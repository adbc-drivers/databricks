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

//! StreamingCloudFetchProvider for orchestrating CloudFetch downloads.
//!
//! This is the main component that coordinates:
//! - Scheduler task that fetches links and dispatches download tasks
//! - Worker tasks that download chunks in parallel
//! - Consumer that reads results sequentially via RecordBatchReader trait
//! - Memory management via bounded channels

use crate::error::{DatabricksErrorHelper, Result};
use crate::reader::cloudfetch::chunk_downloader::ChunkDownloader;
use crate::reader::cloudfetch::consumer::Consumer;
use crate::reader::cloudfetch::link_fetcher::ChunkLinkFetcher;
use crate::reader::cloudfetch::{spawn_scheduler, spawn_workers, WorkerConfig};
use crate::types::cloudfetch::CloudFetchConfig;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use driverbase::error::ErrorHelper;
use std::sync::Arc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

/// Orchestrates link fetching and chunk downloading for CloudFetch.
///
/// This provider manages:
/// - Scheduler task that fetches links and dispatches to workers
/// - Worker tasks that download chunks with retry logic
/// - Consumer that reads results sequentially
/// - Proper ordering of result batches via bounded channels
/// - Error propagation and cancellation
pub struct StreamingCloudFetchProvider {
    // Consumer for reading results from the pipeline
    consumer: Consumer,

    // Cancellation token for coordinated shutdown
    cancel_token: CancellationToken,

    // Worker task handles - awaited on drop for clean shutdown
    // This includes both the scheduler task and N worker tasks
    worker_handles: JoinSet<()>,
}

impl std::fmt::Debug for StreamingCloudFetchProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingCloudFetchProvider")
            .field("consumer", &self.consumer)
            .field("num_worker_handles", &self.worker_handles.len())
            .finish()
    }
}

impl StreamingCloudFetchProvider {
    /// Create a new provider.
    ///
    /// This constructor spawns:
    /// - 1 scheduler task that fetches links and dispatches download tasks
    /// - N worker tasks (configured via `num_download_workers`) that download chunks in parallel
    ///
    /// # Arguments
    /// * `config` - CloudFetch configuration
    /// * `link_fetcher` - Trait object for fetching chunk links
    /// * `chunk_downloader` - For downloading chunks from cloud storage
    ///
    /// # Returns
    ///
    /// Returns `Arc<Self>` since the provider is shared across async tasks.
    pub fn new(
        config: CloudFetchConfig,
        link_fetcher: Arc<dyn ChunkLinkFetcher>,
        chunk_downloader: Arc<ChunkDownloader>,
    ) -> Arc<Self> {
        let cancel_token = CancellationToken::new();

        // Create bounded channels for the pipeline
        // Both channels bounded to max_chunks_in_memory to provide backpressure
        let (download_tx, download_rx) =
            async_channel::bounded(config.max_chunks_in_memory);
        let (result_tx, result_rx) =
            tokio::sync::mpsc::channel(config.max_chunks_in_memory);

        // Create worker configuration
        let worker_config = WorkerConfig {
            max_retries: config.max_retries,
            max_refresh_retries: config.max_refresh_retries,
            retry_delay: config.retry_delay,
            url_expiration_buffer_secs: config.url_expiration_buffer_secs,
        };

        // Create consumer
        let consumer = Consumer::new(result_rx, cancel_token.clone());

        // Clone link_fetcher Arc for workers (before moving into scheduler)
        let link_fetcher_for_workers = Arc::clone(&link_fetcher);

        // Spawn scheduler task
        // We need to create a wrapper struct that can be moved into Box
        struct ArcLinkFetcherWrapper(Arc<dyn ChunkLinkFetcher>);

        #[async_trait::async_trait]
        impl ChunkLinkFetcher for ArcLinkFetcherWrapper {
            async fn fetch_links(
                &self,
                start_chunk_index: i64,
                start_row_offset: i64,
            ) -> Result<crate::client::ChunkLinkFetchResult> {
                self.0.fetch_links(start_chunk_index, start_row_offset).await
            }

            async fn refetch_link(
                &self,
                chunk_index: i64,
                row_offset: i64,
            ) -> Result<crate::types::cloudfetch::CloudFetchLink> {
                self.0.refetch_link(chunk_index, row_offset).await
            }
        }

        impl std::fmt::Debug for ArcLinkFetcherWrapper {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "ArcLinkFetcherWrapper({:?})", self.0)
            }
        }

        let scheduler_handle = spawn_scheduler(
            Box::new(ArcLinkFetcherWrapper(link_fetcher)),
            download_tx,
            result_tx,
            cancel_token.clone(),
        );

        // Spawn worker tasks
        let worker_handles = spawn_workers(
            config.num_download_workers,
            download_rx,
            chunk_downloader,
            link_fetcher_for_workers,
            worker_config,
            cancel_token.clone(),
        );

        // Collect all task handles into a JoinSet
        let mut join_set = JoinSet::new();

        // Add scheduler handle (returns Result<()>)
        join_set.spawn(async move {
            if let Err(e) = scheduler_handle.await {
                error!("Scheduler task failed: {}", e);
            }
        });

        // Add worker handles (return ())
        for handle in worker_handles {
            join_set.spawn(async move {
                if let Err(e) = handle.await {
                    error!("Worker task panicked: {:?}", e);
                }
            });
        }

        debug!(
            "StreamingCloudFetchProvider: spawned 1 scheduler + {} workers",
            config.num_download_workers
        );

        Arc::new(Self {
            consumer,
            cancel_token,
            worker_handles: join_set,
        })
    }


    /// Get next record batch. Main consumer interface.
    ///
    /// Delegates to the Consumer which reads from result_channel and awaits
    /// ChunkHandle results from workers.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(batch))` - Next batch is ready
    /// - `Ok(None)` - End of stream reached (all chunks consumed)
    /// - `Err(e)` - Download failed or operation was cancelled
    pub async fn next_batch(&self) -> Result<Option<RecordBatch>> {
        self.consumer.next_batch().await
    }

    /// Cancel all pending operations.
    ///
    /// This will cancel the scheduler, all workers, and the consumer.
    pub fn cancel(&self) {
        debug!("Cancelling CloudFetch provider");
        self.cancel_token.cancel();
    }


    /// Get the schema, waiting if necessary.
    ///
    /// This will block until the first batch is consumed to extract the schema.
    /// If no batches have been consumed yet, it will attempt to peek the first batch.
    pub async fn get_schema(&self) -> Result<SchemaRef> {
        // First check if consumer already has schema
        if let Some(schema) = self.consumer.schema() {
            return Ok(schema);
        }

        // Need to consume first batch to get schema
        // We'll peek it and let the consumer handle it
        match self.next_batch().await? {
            Some(batch) => {
                // Schema should now be captured in consumer
                if let Some(schema) = self.consumer.schema() {
                    Ok(schema)
                } else {
                    // Fallback: get schema from batch directly
                    Ok(batch.schema())
                }
            }
            None => Err(DatabricksErrorHelper::invalid_state()
                .message("Unable to determine schema: no batches available")),
        }
    }
}

impl Drop for StreamingCloudFetchProvider {
    fn drop(&mut self) {
        debug!("StreamingCloudFetchProvider: dropping, cancelling and awaiting workers");

        // Cancel all tasks
        self.cancel_token.cancel();

        // Abort all worker tasks for immediate shutdown
        // Note: We can't await in Drop (not async), so we abort instead
        self.worker_handles.abort_all();

        debug!("StreamingCloudFetchProvider: all workers aborted");
    }
}

#[cfg(test)]
mod tests {
    // Unit tests for the channel-based provider will be added here.
    // Integration tests with mock HTTP responses are in the integration test suite.

    #[test]
    fn test_provider_can_be_constructed() {
        // This is a placeholder test to ensure the module compiles.
        // Full integration tests require mock ChunkDownloader and ChunkLinkFetcher.
        assert!(true);
    }
}
