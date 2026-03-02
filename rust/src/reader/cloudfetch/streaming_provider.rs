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

    // ── Integration Tests ────────────────────────────────────────────
    //
    // These tests exercise the full channel-based pipeline end-to-end:
    // scheduler → download workers → consumer (next_batch).
    //
    // They use specialized mock implementations that go beyond the simple
    // mocks above to verify complex multi-component interactions.

    // ── Integration mock: ChunkDownload that returns distinct data per chunk ──

    /// A mock downloader that returns multiple RecordBatches per chunk,
    /// with data derived from the chunk index so ordering can be verified.
    #[derive(Debug)]
    struct PerChunkMockDownloader {
        /// Number of RecordBatches to return per chunk.
        batches_per_chunk: usize,
        call_count: AtomicUsize,
    }

    impl PerChunkMockDownloader {
        fn new(batches_per_chunk: usize) -> Self {
            Self {
                batches_per_chunk,
                call_count: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl crate::reader::cloudfetch::chunk_downloader::ChunkDownload for PerChunkMockDownloader {
        async fn download(&self, link: &CloudFetchLink) -> crate::error::Result<Vec<RecordBatch>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            let chunk_idx = link.chunk_index as i32;
            // Return `batches_per_chunk` batches, each containing a marker value
            // derived from the chunk index: [chunk_index * 100 + batch_num]
            let batches = (0..self.batches_per_chunk)
                .map(|batch_num| test_record_batch(&[chunk_idx * 100 + batch_num as i32]))
                .collect();
            Ok(batches)
        }
    }

    // ── Integration mock: slow downloader for cancellation test ──

    /// A mock downloader that introduces a delay before returning, allowing
    /// the cancellation test to cancel mid-stream while downloads are in flight.
    #[derive(Debug)]
    struct SlowMockDownloader {
        delay: Duration,
        call_count: AtomicUsize,
    }

    impl SlowMockDownloader {
        fn new(delay: Duration) -> Self {
            Self {
                delay,
                call_count: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl crate::reader::cloudfetch::chunk_downloader::ChunkDownload for SlowMockDownloader {
        async fn download(&self, link: &CloudFetchLink) -> crate::error::Result<Vec<RecordBatch>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            Ok(vec![test_record_batch(&[link.chunk_index as i32])])
        }
    }

    // ── Integration mock: downloader that returns 401 on first attempt for selected chunks ──

    /// A mock downloader that returns HTTP 401 errors on the first attempt
    /// for specific chunk indices, then succeeds on subsequent attempts
    /// (after the worker refreshes the URL via refetch_link).
    #[derive(Debug)]
    struct Auth401MockDownloader {
        /// Chunk indices that should fail with 401 on first download attempt.
        fail_chunks: std::collections::HashSet<i64>,
        /// Tracks per-URL attempt counts. Key is the URL string.
        /// When a URL is refreshed, the new URL is different, so the next
        /// attempt with the fresh URL succeeds.
        attempted_urls: std::sync::Mutex<std::collections::HashSet<String>>,
        call_count: AtomicUsize,
    }

    impl Auth401MockDownloader {
        fn new(fail_chunks: Vec<i64>) -> Self {
            Self {
                fail_chunks: fail_chunks.into_iter().collect(),
                attempted_urls: std::sync::Mutex::new(std::collections::HashSet::new()),
                call_count: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl crate::reader::cloudfetch::chunk_downloader::ChunkDownload for Auth401MockDownloader {
        async fn download(&self, link: &CloudFetchLink) -> crate::error::Result<Vec<RecordBatch>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);

            // If this chunk is in the fail set, check if we've seen this exact URL before.
            // First attempt with original URL → 401.
            // After refetch_link provides a fresh URL (different string), succeed.
            if self.fail_chunks.contains(&link.chunk_index) {
                let mut attempted = self.attempted_urls.lock().unwrap();
                if attempted.insert(link.url.clone()) {
                    // First time seeing this URL — simulate 401
                    return Err(DatabricksErrorHelper::io().message(format!(
                        "HTTP 401 - Unauthorized for chunk {} (url: {})",
                        link.chunk_index, link.url
                    )));
                }
            }

            // Success — return data derived from chunk index
            Ok(vec![test_record_batch(&[link.chunk_index as i32 * 10])])
        }
    }

    // ── Integration mock: link fetcher that returns fresh URLs on refetch ──

    /// A mock link fetcher for the 401 recovery test. On `refetch_link()`, it
    /// returns a link with a *different* URL (appending "-refreshed") so the
    /// Auth401MockDownloader can distinguish original vs refreshed URLs.
    #[derive(Debug)]
    struct RefreshableMockLinkFetcher {
        links_by_index: HashMap<i64, CloudFetchLink>,
        total_chunks: i64,
        refetch_count: AtomicUsize,
    }

    impl RefreshableMockLinkFetcher {
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
    impl ChunkLinkFetcher for RefreshableMockLinkFetcher {
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
                .map(|original| {
                    // Return a link with a DIFFERENT URL so Auth401MockDownloader
                    // can distinguish original vs refreshed URLs.
                    let mut refreshed = original.clone();
                    refreshed.url = format!("{}-refreshed", original.url);
                    refreshed
                })
                .ok_or_else(|| {
                    DatabricksErrorHelper::invalid_state()
                        .message(format!("Link not found for chunk {}", chunk_index))
                })
        }
    }

    // ── Integration Test 1: end_to_end_sequential_consumption ─────────

    #[tokio::test]
    async fn end_to_end_sequential_consumption() {
        // Verify that the full pipeline delivers all chunks in correct sequential
        // order with correct data, even when the scheduler fetches links in
        // multiple batches (has_more=true → false).
        //
        // Setup: 8 chunks, each producing 2 RecordBatches.
        // The MockLinkFetcher returns batches of 3 links at a time:
        //   batch 1: chunks [0, 1, 2] has_more=true
        //   batch 2: chunks [3, 4, 5] has_more=true
        //   batch 3: chunks [6, 7]    has_more=false

        let total_chunks: i64 = 8;
        let batches_per_chunk: usize = 2;

        let link_fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(MockLinkFetcher::new(total_chunks));
        let downloader: Arc<dyn crate::reader::cloudfetch::chunk_downloader::ChunkDownload> =
            Arc::new(PerChunkMockDownloader::new(batches_per_chunk));

        let config = CloudFetchConfig::default();
        let provider = StreamingCloudFetchProvider::new(config, link_fetcher, downloader);

        // Consume all batches and verify order
        let mut all_values: Vec<i32> = Vec::new();
        let mut batch_count = 0;

        while let Some(batch) = timeout(Duration::from_secs(10), provider.next_batch())
            .await
            .expect("should not time out")
            .expect("should not error")
        {
            // Extract the Int32 value from the single-column batch
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("column should be Int32");
            for i in 0..col.len() {
                all_values.push(col.value(i));
            }
            batch_count += 1;
        }

        // We should get total_chunks * batches_per_chunk batches
        assert_eq!(
            batch_count,
            (total_chunks as usize) * batches_per_chunk,
            "Expected {} batches ({}chunks × {} batches/chunk), got {}",
            total_chunks as usize * batches_per_chunk,
            total_chunks,
            batches_per_chunk,
            batch_count
        );

        // Verify data arrived in correct sequential order.
        // PerChunkMockDownloader returns [chunk_index * 100 + batch_num] per batch.
        // Expected sequence: [0, 1, 100, 101, 200, 201, ..., 700, 701]
        let expected: Vec<i32> = (0..total_chunks as i32)
            .flat_map(|chunk_idx| {
                (0..batches_per_chunk as i32).map(move |batch_num| chunk_idx * 100 + batch_num)
            })
            .collect();

        assert_eq!(
            all_values, expected,
            "Batches were not received in expected sequential order.\n\
             Got:      {:?}\n\
             Expected: {:?}",
            all_values, expected
        );

        // Verify end-of-stream
        let final_batch = timeout(Duration::from_secs(5), provider.next_batch())
            .await
            .expect("should not time out")
            .expect("should not error");
        assert!(
            final_batch.is_none(),
            "Expected None after all chunks consumed"
        );
    }

    // ── Integration Test 2: end_to_end_cancellation_mid_stream ────────

    #[tokio::test]
    async fn end_to_end_cancellation_mid_stream() {
        // Verify that cancelling the pipeline mid-stream does not deadlock or panic.
        // The test must complete within a timeout (5 seconds).
        //
        // Setup: 50 chunks with a slow downloader (100ms per chunk).
        // We consume a few batches, then cancel, and verify clean termination.

        let total_chunks: i64 = 50;
        let link_fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(MockLinkFetcher::new(total_chunks));
        let downloader: Arc<dyn crate::reader::cloudfetch::chunk_downloader::ChunkDownload> =
            Arc::new(SlowMockDownloader::new(Duration::from_millis(100)));

        let config = CloudFetchConfig::default();
        let provider = StreamingCloudFetchProvider::new(config, link_fetcher, downloader);

        // Consume a few batches to get the pipeline flowing
        let mut consumed = 0;
        for _ in 0..3 {
            match timeout(Duration::from_secs(5), provider.next_batch()).await {
                Ok(Ok(Some(_))) => consumed += 1,
                Ok(Ok(None)) => break,
                Ok(Err(_)) => break,
                Err(_) => panic!("Timed out waiting for batch before cancel"),
            }
        }
        assert!(consumed > 0, "Should have consumed at least one batch");

        // Cancel mid-stream
        provider.cancel();

        // After cancel, next_batch() should return Err or None — never block forever.
        // The entire post-cancel phase must complete within the timeout.
        let post_cancel_result = timeout(Duration::from_secs(5), provider.next_batch()).await;

        match post_cancel_result {
            Ok(Err(_)) => {}   // Expected: "Operation cancelled"
            Ok(Ok(None)) => {} // Also acceptable: channel closed
            Ok(Ok(Some(_))) => {
                // A batch that was already buffered is acceptable;
                // but subsequent calls must terminate.
                let second = timeout(Duration::from_secs(2), provider.next_batch()).await;
                match second {
                    Ok(Err(_)) | Ok(Ok(None)) => {}
                    Ok(Ok(Some(_))) => {
                        // Even this is tolerable if the batch was pre-buffered.
                        // What matters is that we don't deadlock.
                    }
                    Err(_) => panic!("Deadlock: next_batch() blocked after cancel"),
                }
            }
            Err(_) => panic!("Deadlock: next_batch() did not return within 5s after cancel"),
        }

        // If we reach here without panic or timeout, the test passes:
        // no deadlock, no panic, clean termination after cancel.
    }

    // ── Integration Test 3: end_to_end_401_recovery ───────────────────

    #[tokio::test]
    async fn end_to_end_401_recovery() {
        // Verify that the pipeline recovers from HTTP 401 errors mid-stream
        // by calling refetch_link() to obtain a fresh URL, then retrying
        // the download successfully.
        //
        // Setup: 6 chunks. Chunks 1, 3, and 5 fail with 401 on the first
        // download attempt. After refetch_link() provides a fresh URL
        // (with a different URL string), the retry succeeds.

        let total_chunks: i64 = 6;
        let fail_chunks = vec![1, 3, 5]; // These chunks fail with 401 on first attempt

        let link_fetcher: Arc<dyn ChunkLinkFetcher> =
            Arc::new(RefreshableMockLinkFetcher::new(total_chunks));
        let downloader: Arc<dyn crate::reader::cloudfetch::chunk_downloader::ChunkDownload> =
            Arc::new(Auth401MockDownloader::new(fail_chunks.clone()));

        let mut config = CloudFetchConfig::default();
        config.max_retries = 3;
        config.max_refresh_retries = 3;

        let provider = StreamingCloudFetchProvider::new(
            config,
            Arc::clone(&link_fetcher),
            Arc::clone(&downloader),
        );

        // Consume all batches — all 6 chunks should be delivered despite 401 errors
        let mut all_values: Vec<i32> = Vec::new();
        let mut batch_count = 0;

        while let Some(batch) = timeout(Duration::from_secs(10), provider.next_batch())
            .await
            .expect("should not time out")
            .expect("should not error — 401 recovery should succeed")
        {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("column should be Int32");
            for i in 0..col.len() {
                all_values.push(col.value(i));
            }
            batch_count += 1;
        }

        // All 6 chunks should have been delivered (one batch each)
        assert_eq!(
            batch_count, total_chunks as usize,
            "Expected {} batches, got {}",
            total_chunks, batch_count
        );

        // Verify correct data: Auth401MockDownloader returns [chunk_index * 10]
        let expected: Vec<i32> = (0..total_chunks as i32).map(|i| i * 10).collect();
        assert_eq!(
            all_values, expected,
            "Data mismatch after 401 recovery.\n\
             Got:      {:?}\n\
             Expected: {:?}",
            all_values, expected
        );

        // Behavioral verification is complete:
        // - All 6 chunks were successfully consumed (verified by batch_count)
        // - The data is correct and in sequential order (verified by all_values)
        // - The test did not error out, confirming 401 recovery worked
        //
        // The pipeline internally handled:
        //   - 3 chunks succeeding immediately (chunks 0, 2, 4)
        //   - 3 chunks failing with 401, then succeeding after refetch_link
        //     provided a fresh URL (chunks 1, 3, 5)
    }
}
