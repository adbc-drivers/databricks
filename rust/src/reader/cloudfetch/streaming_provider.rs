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

//! StreamingCloudFetchProvider — channel-based pipeline for CloudFetch downloads.
//!
//! This module implements the consumer side of the CloudFetch pipeline. The
//! provider holds the receiving end of a bounded `result_channel` and reads
//! [`ChunkHandle`] values in chunk-index order. Each handle contains a oneshot
//! receiver that resolves when the corresponding download worker finishes.
//!
//! ## Pipeline
//!
//! ```text
//! ChunkLinkFetcher → Scheduler → download_channel → Workers → oneshot → result_channel → Consumer
//! ```
//!
//! The scheduler and workers are spawned by the constructor. The consumer
//! (`next_batch`) simply reads from `result_channel` and awaits the oneshot.

use crate::error::{DatabricksErrorHelper, Result};
use crate::reader::cloudfetch::chunk_downloader::ChunkDownloader;
use crate::reader::cloudfetch::link_fetcher::ChunkLinkFetcher;
use crate::reader::cloudfetch::pipeline_types::ChunkHandle;
use crate::reader::cloudfetch::scheduler::spawn_scheduler;
use crate::reader::cloudfetch::workers::spawn_workers;
use crate::types::cloudfetch::CloudFetchConfig;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use driverbase::error::ErrorHelper;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

/// Orchestrates link fetching and chunk downloading for CloudFetch.
///
/// The struct holds only the consumer-side state. All scheduling and downloading
/// happens in background tasks created during construction.
///
/// ## Fields
///
/// - `result_rx`: Bounded channel receiver for [`ChunkHandle`] values in order.
/// - `schema`: Lazily populated from the first downloaded batch.
/// - `batch_buffer`: Drains the current chunk's batches before advancing.
/// - `cancel_token`: Signals all background tasks to stop.
pub struct StreamingCloudFetchProvider {
    /// Pipeline output — consumer reads ChunkHandles in order.
    /// Uses `tokio::sync::Mutex` because `recv()` must be `.await`ed.
    result_rx: tokio::sync::Mutex<mpsc::Receiver<ChunkHandle>>,

    /// Schema (extracted from first batch).
    schema: OnceLock<SchemaRef>,

    /// Batch buffer (drains current ChunkHandle before advancing).
    batch_buffer: Mutex<VecDeque<RecordBatch>>,

    /// Cancellation token shared with scheduler and workers.
    cancel_token: CancellationToken,
}

impl std::fmt::Debug for StreamingCloudFetchProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingCloudFetchProvider")
            .field("schema", &self.schema.get().map(|s| s.fields().len()))
            .field(
                "batch_buffer_len",
                &self.batch_buffer.lock().map(|b| b.len()).unwrap_or(0),
            )
            .field("cancelled", &self.cancel_token.is_cancelled())
            .finish()
    }
}

impl StreamingCloudFetchProvider {
    /// Create a new provider, spawning the scheduler and download workers.
    ///
    /// # Arguments
    /// * `config` - CloudFetch configuration
    /// * `link_fetcher` - Trait object for fetching chunk links
    /// * `chunk_downloader` - For downloading chunks from cloud storage
    /// * `_runtime_handle` - Tokio runtime handle (retained for API compatibility;
    ///   background tasks are spawned via `tokio::spawn` on the current runtime)
    pub fn new(
        config: CloudFetchConfig,
        link_fetcher: Arc<dyn ChunkLinkFetcher>,
        chunk_downloader: Arc<ChunkDownloader>,
        _runtime_handle: tokio::runtime::Handle,
    ) -> Arc<Self> {
        let cancel_token = CancellationToken::new();

        // Spawn scheduler — creates both channels
        let channels = spawn_scheduler(
            Arc::clone(&link_fetcher),
            config.max_chunks_in_memory,
            cancel_token.clone(),
        );

        // Spawn download workers — they share the download_rx
        spawn_workers(
            channels.download_rx,
            chunk_downloader,
            link_fetcher,
            config,
            cancel_token.clone(),
        );

        Arc::new(Self {
            result_rx: tokio::sync::Mutex::new(channels.result_rx),
            schema: OnceLock::new(),
            batch_buffer: Mutex::new(VecDeque::new()),
            cancel_token,
        })
    }

    /// Get next record batch. Main consumer interface.
    ///
    /// Returns `Ok(Some(batch))` for each batch, `Ok(None)` at end of stream.
    ///
    /// The consumer:
    /// 1. Drains `batch_buffer` if non-empty.
    /// 2. Otherwise, reads the next [`ChunkHandle`] from `result_channel`.
    /// 3. Awaits the oneshot receiver in the handle.
    /// 4. Buffers the resulting batches and returns the first one.
    pub async fn next_batch(&self) -> Result<Option<RecordBatch>> {
        // Check cancellation
        if self.cancel_token.is_cancelled() {
            return Err(DatabricksErrorHelper::invalid_state().message("Operation cancelled"));
        }

        // Drain batch buffer first
        {
            let mut buffer = self.batch_buffer.lock().unwrap();
            if let Some(batch) = buffer.pop_front() {
                let _ = self.schema.get_or_init(|| batch.schema());
                return Ok(Some(batch));
            }
        }

        // Buffer empty — get next ChunkHandle from result_channel.
        // Since next_batch is called sequentially by a single consumer
        // (CloudFetchResultReader), there is no concurrent access to result_rx.
        let handle = self.recv_next_handle().await;

        let handle = match handle {
            Some(h) => h,
            None => {
                debug!("Consumer: result_channel closed, end of stream");
                return Ok(None);
            }
        };

        debug!("Consumer: awaiting chunk {}", handle.chunk_index);

        // Await the oneshot — this blocks until the worker finishes the download
        let batches = tokio::select! {
            _ = self.cancel_token.cancelled() => {
                return Err(DatabricksErrorHelper::invalid_state().message("Operation cancelled"));
            }
            result = handle.result_rx => {
                match result {
                    Ok(Ok(batches)) => batches,
                    Ok(Err(e)) => {
                        error!("Consumer: chunk {} download failed: {}", handle.chunk_index, e);
                        return Err(e);
                    }
                    Err(_) => {
                        // oneshot sender was dropped without sending — worker panicked or was cancelled
                        return Err(DatabricksErrorHelper::io().message(format!(
                            "Chunk {} download was cancelled (worker dropped)",
                            handle.chunk_index
                        )));
                    }
                }
            }
        };

        debug!(
            "Consumer: received chunk {} with {} batches",
            handle.chunk_index,
            batches.len()
        );

        // Buffer the batches and return the first one
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

    /// Receive the next [`ChunkHandle`] from the result channel.
    ///
    /// Uses `tokio::sync::Mutex` to protect the receiver across the `.await`
    /// point. Since `next_batch` is the sole consumer, there is no contention.
    async fn recv_next_handle(&self) -> Option<ChunkHandle> {
        let mut rx = self.result_rx.lock().await;
        rx.recv().await
    }

    /// Cancel all pending operations.
    pub fn cancel(&self) {
        debug!("Cancelling CloudFetch provider");
        self.cancel_token.cancel();
    }

    /// Get the schema, waiting if necessary.
    ///
    /// If the schema hasn't been extracted yet, this reads the first chunk
    /// and buffers its batches (the caller will get them via `next_batch`).
    pub async fn get_schema(&self) -> Result<SchemaRef> {
        if let Some(schema) = self.schema.get() {
            return Ok(schema.clone());
        }

        // Need to peek the first batch to get schema
        let first_batch = self.next_batch().await?;
        if let Some(batch) = first_batch {
            let schema = batch.schema();
            let _ = self.schema.get_or_init(|| schema.clone());
            // Put it back in the buffer so the caller gets it via next_batch
            self.batch_buffer.lock().unwrap().push_front(batch);
            Ok(schema)
        } else {
            Err(DatabricksErrorHelper::invalid_state().message("Unable to determine schema"))
        }
    }
}

impl Drop for StreamingCloudFetchProvider {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ChunkLinkFetchResult;
    use crate::types::cloudfetch::CloudFetchLink;
    use std::collections::HashMap;

    /// Mock ChunkLinkFetcher for testing
    #[allow(dead_code)]
    #[derive(Debug)]
    struct MockLinkFetcher {
        links_by_index: HashMap<i64, CloudFetchLink>,
        total_chunks: i64,
    }

    impl MockLinkFetcher {
        #[allow(dead_code)]
        fn new(total_chunks: i64) -> Self {
            let mut links_by_index = HashMap::new();
            for i in 0..total_chunks {
                links_by_index.insert(i, create_test_link(i));
            }
            Self {
                links_by_index,
                total_chunks,
            }
        }
    }

    #[async_trait::async_trait]
    impl ChunkLinkFetcher for MockLinkFetcher {
        async fn fetch_links(
            &self,
            start_chunk_index: i64,
            _start_row_offset: i64,
        ) -> Result<ChunkLinkFetchResult> {
            if start_chunk_index >= self.total_chunks {
                return Ok(ChunkLinkFetchResult::end_of_stream());
            }

            let links: Vec<CloudFetchLink> = (start_chunk_index..self.total_chunks)
                .take(3) // Return up to 3 links at a time
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

        async fn refetch_link(&self, chunk_index: i64, _row_offset: i64) -> Result<CloudFetchLink> {
            self.links_by_index
                .get(&chunk_index)
                .cloned()
                .ok_or_else(|| {
                    DatabricksErrorHelper::invalid_state()
                        .message(format!("Link not found for chunk {}", chunk_index))
                })
        }
    }

    fn create_test_link(chunk_index: i64) -> CloudFetchLink {
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

    #[test]
    fn test_chunk_link_fetch_result_end_of_stream() {
        let result = ChunkLinkFetchResult::end_of_stream();
        assert!(result.links.is_empty());
        assert!(!result.has_more);
        assert!(result.next_chunk_index.is_none());
    }
}
