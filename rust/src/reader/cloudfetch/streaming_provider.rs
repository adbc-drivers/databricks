// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Channel-based StreamingCloudFetchProvider for orchestrating CloudFetch downloads.
//!
//! This module replaces the DashMap-based coordination with a two-channel pipeline:
//!
//! - **Scheduler** fetches links and dispatches `ChunkDownloadTask` / `ChunkHandle`
//!   pairs through `download_channel` and `result_channel` respectively.
//! - **Download workers** pull tasks from `download_channel`, download chunks, and
//!   deliver results via oneshot channels.
//! - **Consumer** (`next_batch`) reads `ChunkHandle` values from `result_channel`
//!   in chunk-index order and awaits each handle's oneshot receiver.
//!
//! This design eliminates DashMap shard locks, Notify-based polling, and manual
//! memory counters. Backpressure is handled by the bounded `result_channel`.

use crate::error::{DatabricksErrorHelper, Result};
use crate::reader::cloudfetch::chunk_downloader::{spawn_download_workers, ChunkDownload};
use crate::reader::cloudfetch::link_fetcher::ChunkLinkFetcher;
use crate::reader::cloudfetch::pipeline_types::ChunkHandle;
use crate::reader::cloudfetch::scheduler::spawn_scheduler;
use crate::types::cloudfetch::CloudFetchConfig;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use driverbase::error::ErrorHelper;
use std::collections::VecDeque;
use std::sync::{Mutex, OnceLock};
use tokio::sync::Mutex as TokioMutex;
use tokio_util::sync::CancellationToken;
use tracing::debug;

/// Orchestrates link fetching and chunk downloading for CloudFetch.
///
/// This provider manages a channel-based pipeline:
/// - A scheduler task fetches links and creates chunk pairs
/// - Download workers process chunks in parallel
/// - The consumer reads results in sequential chunk order
///
/// Backpressure is automatic: the bounded result channel limits how far
/// ahead the scheduler can get relative to the consumer.
pub struct StreamingCloudFetchProvider {
    /// Pipeline output — consumer reads ChunkHandles in order.
    /// Uses a tokio async mutex because `recv()` is held across await points.
    result_rx: TokioMutex<tokio::sync::mpsc::Receiver<ChunkHandle>>,

    /// Schema (extracted from the first batch).
    schema: OnceLock<SchemaRef>,

    /// Batch buffer (drains current ChunkHandle's batches before advancing).
    batch_buffer: Mutex<VecDeque<RecordBatch>>,

    /// Cancellation token — triggers cooperative shutdown of the entire pipeline.
    cancel_token: CancellationToken,
}

impl std::fmt::Debug for StreamingCloudFetchProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingCloudFetchProvider")
            .field("has_schema", &self.schema.get().is_some())
            .finish()
    }
}

impl StreamingCloudFetchProvider {
    /// Create a new provider, spawning the scheduler and download worker pool.
    ///
    /// # Arguments
    /// * `config` - CloudFetch configuration (concurrency, retries, etc.)
    /// * `link_fetcher` - Trait object for fetching chunk links
    /// * `downloader` - Trait object for downloading chunks from cloud storage
    pub fn new(
        config: CloudFetchConfig,
        link_fetcher: Arc<dyn ChunkLinkFetcher>,
        downloader: Arc<dyn ChunkDownload>,
    ) -> Self {
        let cancel_token = CancellationToken::new();

        // 1. Spawn the scheduler — creates channels and starts fetching links
        let channels = spawn_scheduler(
            Arc::clone(&link_fetcher),
            config.max_chunks_in_memory,
            cancel_token.clone(),
        );

        // 2. Spawn download workers — they pull from the download channel
        let _worker_handles = spawn_download_workers(
            channels.download_rx,
            &config,
            downloader,
            link_fetcher,
            cancel_token.clone(),
        );

        // 3. Store the result channel receiver for the consumer
        Self {
            result_rx: TokioMutex::new(channels.result_rx),
            schema: OnceLock::new(),
            batch_buffer: Mutex::new(VecDeque::new()),
            cancel_token,
        }
    }

    /// Get next record batch. Main consumer interface.
    ///
    /// 1. Drains `batch_buffer` first (current chunk's remaining batches).
    /// 2. Calls `result_rx.recv()` to get the next `ChunkHandle`.
    /// 3. Awaits `handle.result_rx` to get the downloaded batches.
    /// 4. Returns `Ok(None)` when the result channel is closed (end of stream).
    /// 5. Uses `tokio::select!` on `cancel_token.cancelled()`.
    pub async fn next_batch(&self) -> Result<Option<RecordBatch>> {
        // Drain batch buffer first
        {
            let mut buffer = self.batch_buffer.lock().unwrap();
            if let Some(batch) = buffer.pop_front() {
                // Capture schema from first batch
                let _ = self.schema.get_or_init(|| batch.schema());
                return Ok(Some(batch));
            }
        }

        // Buffer empty — get next ChunkHandle from result channel
        let handle = {
            let mut rx = self.result_rx.lock().await;
            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    return Err(
                        DatabricksErrorHelper::invalid_state()
                            .message("Operation cancelled")
                    );
                }
                result = rx.recv() => result,
            }
        };

        let handle = match handle {
            Some(h) => h,
            None => {
                // Channel closed — end of stream
                debug!("End of stream: result channel closed");
                return Ok(None);
            }
        };

        debug!("Consumer: received handle for chunk {}", handle.chunk_index);

        // Await the oneshot receiver to get downloaded batches
        let batches_result = tokio::select! {
            _ = self.cancel_token.cancelled() => {
                return Err(
                    DatabricksErrorHelper::invalid_state()
                        .message("Operation cancelled while awaiting chunk download")
                );
            }
            result = handle.result_rx => result,
        };

        // Handle oneshot channel errors (sender dropped without sending)
        let batches_result = batches_result.map_err(|_| {
            DatabricksErrorHelper::io().message(format!(
                "Download worker dropped without completing chunk {}",
                handle.chunk_index
            ))
        })?;

        // Handle download errors
        let batches = batches_result?;

        debug!(
            "Consumer: chunk {} delivered {} batches",
            handle.chunk_index,
            batches.len()
        );

        // Store batches in buffer and return the first one
        let first_batch = {
            let mut buffer = self.batch_buffer.lock().unwrap();
            for batch in batches {
                buffer.push_back(batch);
            }
            buffer.pop_front()
        };

        if let Some(batch) = first_batch {
            let _ = self.schema.get_or_init(|| batch.schema());
            Ok(Some(batch))
        } else {
            // Empty chunk — recurse to get next chunk
            Box::pin(self.next_batch()).await
        }
    }

    /// Cancel all pending operations.
    pub fn cancel(&self) {
        debug!("Cancelling CloudFetch provider");
        self.cancel_token.cancel();
    }

    /// Get the schema, waiting if necessary.
    ///
    /// If the schema hasn't been extracted yet, this calls `next_batch()`
    /// to read the first batch (which populates the schema), then stores
    /// it in the buffer for later consumption.
    pub async fn get_schema(&self) -> Result<SchemaRef> {
        if let Some(schema) = self.schema.get() {
            return Ok(schema.clone());
        }

        // Need to read the first batch to get the schema
        match self.next_batch().await? {
            Some(batch) => {
                let schema = batch.schema();
                let _ = self.schema.get_or_init(|| schema.clone());

                // Put the batch back in the buffer so it isn't lost
                self.batch_buffer.lock().unwrap().push_front(batch);

                Ok(schema)
            }
            None => Err(DatabricksErrorHelper::invalid_state()
                .message("No data available to determine schema")),
        }
    }
}

impl Drop for StreamingCloudFetchProvider {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

// Arc is needed for the constructor to accept trait objects
use std::sync::Arc;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ChunkLinkFetchResult;
    use crate::types::cloudfetch::CloudFetchLink;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{timeout, Duration};

    // ── Test helpers ────────────────────────────────────────────────

    fn test_link(chunk_index: i64) -> CloudFetchLink {
        CloudFetchLink {
            url: format!("https://storage.example.com/chunk{}", chunk_index),
            chunk_index,
            row_offset: chunk_index * 1000,
            row_count: 1000,
            byte_count: 50000,
            expiration: chrono::Utc::now() + chrono::Duration::hours(1),
            http_headers: HashMap::new(),
            next_chunk_index: Some(chunk_index + 1),
        }
    }

    fn test_record_batch(values: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Int32, false)]));
        let array = Int32Array::from(values.to_vec());
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    // ── Mock ChunkLinkFetcher ───────────────────────────────────────

    #[derive(Debug)]
    struct MockLinkFetcher {
        links_by_index: HashMap<i64, CloudFetchLink>,
        total_chunks: i64,
        refetch_count: AtomicUsize,
    }

    impl MockLinkFetcher {
        fn new(total_chunks: i64) -> Self {
            let mut links_by_index = HashMap::new();
            for i in 0..total_chunks {
                links_by_index.insert(i, test_link(i));
            }
            Self {
                links_by_index,
                total_chunks,
                refetch_count: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl ChunkLinkFetcher for MockLinkFetcher {
        async fn fetch_links(
            &self,
            start_chunk_index: i64,
            _start_row_offset: i64,
        ) -> crate::error::Result<ChunkLinkFetchResult> {
            if start_chunk_index >= self.total_chunks {
                return Ok(ChunkLinkFetchResult::end_of_stream());
            }

            let links: Vec<CloudFetchLink> = (start_chunk_index..self.total_chunks)
                .take(3)
                .filter_map(|i| self.links_by_index.get(&i).cloned())
                .collect();

            let has_more = start_chunk_index + (links.len() as i64) < self.total_chunks;
            let next = if has_more {
                Some(start_chunk_index + links.len() as i64)
            } else {
                None
            };

            Ok(ChunkLinkFetchResult {
                links,
                has_more,
                next_chunk_index: next,
                next_row_offset: None,
            })
        }

        async fn refetch_link(
            &self,
            chunk_index: i64,
            _row_offset: i64,
        ) -> crate::error::Result<CloudFetchLink> {
            self.refetch_count.fetch_add(1, Ordering::SeqCst);
            self.links_by_index
                .get(&chunk_index)
                .cloned()
                .ok_or_else(|| {
                    DatabricksErrorHelper::invalid_state()
                        .message(format!("Link not found for chunk {}", chunk_index))
                })
        }
    }

    // ── Mock ChunkDownload ──────────────────────────────────────────

    #[derive(Debug)]
    struct MockDownloader {
        call_count: AtomicUsize,
    }

    impl MockDownloader {
        fn new() -> Self {
            Self {
                call_count: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl crate::reader::cloudfetch::chunk_downloader::ChunkDownload for MockDownloader {
        async fn download(&self, _link: &CloudFetchLink) -> crate::error::Result<Vec<RecordBatch>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Ok(vec![test_record_batch(&[1, 2, 3])])
        }
    }

    // ── Tests ───────────────────────────────────────────────────────

    #[test]
    fn test_chunk_link_fetch_result_end_of_stream() {
        let result = ChunkLinkFetchResult::end_of_stream();
        assert!(result.links.is_empty());
        assert!(!result.has_more);
        assert!(result.next_chunk_index.is_none());
    }

    #[tokio::test]
    async fn test_next_batch_returns_batches_in_order() {
        let link_fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(MockLinkFetcher::new(3));
        let downloader: Arc<dyn crate::reader::cloudfetch::chunk_downloader::ChunkDownload> =
            Arc::new(MockDownloader::new());

        let config = CloudFetchConfig::default();
        let provider = StreamingCloudFetchProvider::new(config, link_fetcher, downloader);

        // Should get 3 chunks worth of batches
        let mut batch_count = 0;
        while let Some(_batch) = timeout(Duration::from_secs(5), provider.next_batch())
            .await
            .expect("should not time out")
            .expect("should not error")
        {
            batch_count += 1;
        }

        assert_eq!(batch_count, 3, "Expected 3 batches (one per chunk)");
    }

    #[tokio::test]
    async fn test_next_batch_returns_none_at_end_of_stream() {
        let link_fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(MockLinkFetcher::new(1));
        let downloader: Arc<dyn crate::reader::cloudfetch::chunk_downloader::ChunkDownload> =
            Arc::new(MockDownloader::new());

        let config = CloudFetchConfig::default();
        let provider = StreamingCloudFetchProvider::new(config, link_fetcher, downloader);

        // First batch should succeed
        let batch = timeout(Duration::from_secs(5), provider.next_batch())
            .await
            .expect("should not time out")
            .expect("should not error");
        assert!(batch.is_some());

        // Second call should return None (end of stream)
        let batch = timeout(Duration::from_secs(5), provider.next_batch())
            .await
            .expect("should not time out")
            .expect("should not error");
        assert!(batch.is_none());
    }

    #[tokio::test]
    async fn test_get_schema_from_first_batch() {
        let link_fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(MockLinkFetcher::new(2));
        let downloader: Arc<dyn crate::reader::cloudfetch::chunk_downloader::ChunkDownload> =
            Arc::new(MockDownloader::new());

        let config = CloudFetchConfig::default();
        let provider = StreamingCloudFetchProvider::new(config, link_fetcher, downloader);

        // Schema should be extractable
        let schema = timeout(Duration::from_secs(5), provider.get_schema())
            .await
            .expect("should not time out")
            .expect("should not error");

        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "col");

        // After get_schema, we should still be able to read all batches
        // (the first batch was put back in the buffer)
        let mut batch_count = 0;
        while let Some(_batch) = timeout(Duration::from_secs(5), provider.next_batch())
            .await
            .expect("should not time out")
            .expect("should not error")
        {
            batch_count += 1;
        }

        assert_eq!(
            batch_count, 2,
            "Expected 2 batches (first was buffered, plus second chunk)"
        );
    }

    #[tokio::test]
    async fn test_cancel_stops_pipeline() {
        let link_fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(MockLinkFetcher::new(100));
        let downloader: Arc<dyn crate::reader::cloudfetch::chunk_downloader::ChunkDownload> =
            Arc::new(MockDownloader::new());

        let config = CloudFetchConfig::default();
        let provider = StreamingCloudFetchProvider::new(config, link_fetcher, downloader);

        // Cancel immediately
        provider.cancel();

        // next_batch should return an error or None
        let result = timeout(Duration::from_secs(2), provider.next_batch())
            .await
            .expect("should not time out");

        // After cancellation, we should get either an error or None
        // (depends on timing — if cancel fires before recv, we get error;
        // if channel closes first, we get None)
        match result {
            Err(_) => {}   // Expected: operation cancelled
            Ok(None) => {} // Also acceptable: channel closed due to cancellation
            Ok(Some(_)) => panic!("Should not get a batch after cancellation"),
        }
    }

    #[tokio::test]
    async fn test_drop_cancels_token() {
        let cancel_token_clone;
        {
            let link_fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(MockLinkFetcher::new(10));
            let downloader: Arc<dyn crate::reader::cloudfetch::chunk_downloader::ChunkDownload> =
                Arc::new(MockDownloader::new());

            let config = CloudFetchConfig::default();
            let provider = StreamingCloudFetchProvider::new(config, link_fetcher, downloader);
            cancel_token_clone = provider.cancel_token.clone();

            assert!(!cancel_token_clone.is_cancelled());
        }
        // After drop, the cancel token should be cancelled
        assert!(cancel_token_clone.is_cancelled());
    }

    #[tokio::test]
    async fn test_empty_result_set() {
        // Zero chunks — the scheduler should close immediately
        let link_fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(MockLinkFetcher::new(0));
        let downloader: Arc<dyn crate::reader::cloudfetch::chunk_downloader::ChunkDownload> =
            Arc::new(MockDownloader::new());

        let config = CloudFetchConfig::default();
        let provider = StreamingCloudFetchProvider::new(config, link_fetcher, downloader);

        let batch = timeout(Duration::from_secs(5), provider.next_batch())
            .await
            .expect("should not time out")
            .expect("should not error");
        assert!(batch.is_none(), "Empty result set should return None");
    }
}
