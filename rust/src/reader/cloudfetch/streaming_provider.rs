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
//! - On-demand link fetching from the link fetcher (which handles prefetching)
//! - Parallel chunk downloads from cloud storage
//! - Memory management via chunk limits (via bounded result_channel)
//! - Consumer API via RecordBatchReader trait
//!
//! ## Architecture
//!
//! The provider uses a two-channel pipeline:
//! - `download_channel` - scheduler pushes download tasks; workers pull from it
//! - `result_channel` - scheduler pushes ChunkHandle in order; consumer reads from it
//!
//! The consumer reads ChunkHandle from result_channel, then awaits the embedded
//! oneshot receiver to get the download result. End of stream is detected when
//! result_channel closes (returns None).

use crate::error::{DatabricksErrorHelper, Result};
use crate::reader::cloudfetch::chunk_downloader::ChunkDownloader;
use crate::reader::cloudfetch::download_workers::spawn_download_workers;
use crate::reader::cloudfetch::link_fetcher::ChunkLinkFetcher;
use crate::reader::cloudfetch::pipeline_types::ChunkHandle;
use crate::reader::cloudfetch::scheduler::spawn_scheduler;
use crate::types::cloudfetch::CloudFetchConfig;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use driverbase::error::ErrorHelper;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::debug;

/// Orchestrates link fetching and chunk downloading for CloudFetch.
///
/// This provider manages:
/// - Background link prefetching to stay ahead of consumption
/// - Parallel downloads with memory-bounded concurrency
/// - Proper ordering of result batches via the result_channel
/// - Error propagation and cancellation
pub struct StreamingCloudFetchProvider {
    // Pipeline output - consumer reads ChunkHandles in order from result_channel
    // Uses tokio::sync::Mutex because we need to hold the lock across await points
    result_rx: tokio::sync::Mutex<mpsc::Receiver<ChunkHandle>>,

    // Schema - extracted from first Arrow batch
    schema: OnceLock<SchemaRef>,

    // Batch buffer - drains current ChunkHandle before advancing
    // Uses std::sync::Mutex because we only access it synchronously
    batch_buffer: Mutex<VecDeque<RecordBatch>>,

    // Cancellation
    cancel_token: CancellationToken,

    // Pipeline task handles for lifecycle management
    #[allow(dead_code)]
    scheduler_handle: JoinHandle<()>,
    #[allow(dead_code)]
    worker_handles: Vec<JoinHandle<()>>,
}

impl std::fmt::Debug for StreamingCloudFetchProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingCloudFetchProvider")
            .field("schema", &self.schema.get().is_some())
            .field("cancel_token_cancelled", &self.cancel_token.is_cancelled())
            .finish()
    }
}

impl StreamingCloudFetchProvider {
    /// Create a new provider.
    ///
    /// This spawns the scheduler and download workers, which start processing immediately.
    /// The scheduler fetches links and creates download tasks, while workers download
    /// chunks from cloud storage.
    ///
    /// # Arguments
    /// * `config` - CloudFetch configuration
    /// * `link_fetcher` - Trait object for fetching chunk links (should be a SeaChunkLinkFetcherHandle
    ///   that already has initial links cached and handles prefetching)
    /// * `chunk_downloader` - For downloading chunks from cloud storage
    /// * `_runtime_handle` - Tokio runtime handle (kept for API compatibility, not used in new implementation)
    pub fn new(
        config: CloudFetchConfig,
        link_fetcher: Arc<dyn ChunkLinkFetcher>,
        chunk_downloader: Arc<ChunkDownloader>,
        _runtime_handle: tokio::runtime::Handle,
    ) -> Arc<Self> {
        let cancel_token = CancellationToken::new();

        // Spawn the scheduler - it creates the pipeline channels
        let scheduler_channels =
            spawn_scheduler(Arc::clone(&link_fetcher), &config, cancel_token.clone());

        // Spawn download workers - they pull from download_rx and process tasks
        let worker_handles = spawn_download_workers(
            scheduler_channels.download_rx,
            &config,
            chunk_downloader,
            link_fetcher,
            cancel_token.clone(),
        );

        debug!(
            "StreamingCloudFetchProvider created: {} workers, max_chunks_in_memory={}",
            worker_handles.len(),
            config.max_chunks_in_memory
        );

        Arc::new(Self {
            result_rx: tokio::sync::Mutex::new(scheduler_channels.result_rx),
            schema: OnceLock::new(),
            batch_buffer: Mutex::new(VecDeque::new()),
            cancel_token,
            scheduler_handle: scheduler_channels.scheduler_handle,
            worker_handles,
        })
    }

    /// Get next record batch. Main consumer interface.
    ///
    /// Returns the next batch from the current chunk, or fetches the next chunk
    /// from the result channel when the batch buffer is empty.
    ///
    /// Returns `Ok(None)` when the stream is exhausted (result channel closed).
    pub async fn next_batch(&self) -> Result<Option<RecordBatch>> {
        // Check for cancellation first
        if self.cancel_token.is_cancelled() {
            return Err(DatabricksErrorHelper::invalid_state().message("Operation cancelled"));
        }

        // Drain batch buffer first
        if let Some(batch) = self.batch_buffer.lock().unwrap().pop_front() {
            // Capture schema from first batch
            let _ = self.schema.get_or_init(|| batch.schema());
            return Ok(Some(batch));
        }

        // Buffer empty - need next chunk from result channel
        // Receive next ChunkHandle with cancellation support
        let handle = {
            let mut rx = self.result_rx.lock().await;

            tokio::select! {
                biased;

                _ = self.cancel_token.cancelled() => {
                    return Err(DatabricksErrorHelper::invalid_state().message("Operation cancelled"));
                }
                result = rx.recv() => {
                    result
                }
            }
        };

        // Process the handle
        let handle = match handle {
            Some(h) => h,
            None => {
                // Channel closed = end of stream
                debug!("Result channel closed, end of stream");
                return Ok(None);
            }
        };

        debug!("Received handle for chunk {}", handle.chunk_index);

        // Await the oneshot result with cancellation support
        let batches_result = tokio::select! {
            biased;

            _ = self.cancel_token.cancelled() => {
                return Err(DatabricksErrorHelper::invalid_state().message("Operation cancelled"));
            }
            result = handle.result_rx => {
                result
            }
        };

        // Handle oneshot recv error (sender dropped = download task cancelled/crashed)
        let batches = batches_result
            .map_err(|_| {
                DatabricksErrorHelper::io().message("Download task was cancelled or failed")
            })?
            // Propagate any download error
            ?;

        // Store batches in buffer
        {
            let mut buffer = self.batch_buffer.lock().unwrap();
            for batch in batches {
                buffer.push_back(batch);
            }
        }

        // Return first batch from buffer (if any)
        if let Some(batch) = self.batch_buffer.lock().unwrap().pop_front() {
            let _ = self.schema.get_or_init(|| batch.schema());
            return Ok(Some(batch));
        }

        // Empty chunk (unusual but valid)
        // Recurse to get next chunk
        Box::pin(self.next_batch()).await
    }

    /// Cancel all pending operations.
    pub fn cancel(&self) {
        debug!("Cancelling CloudFetch provider");
        self.cancel_token.cancel();
    }

    /// Get the schema, waiting if necessary.
    ///
    /// This fetches the first batch to extract the schema.
    pub async fn get_schema(&self) -> Result<SchemaRef> {
        if let Some(schema) = self.schema.get() {
            return Ok(schema.clone());
        }

        // Need to peek the first batch to get schema
        // The next_batch call will populate the schema
        // We can't easily peek without consuming, so we let next_batch set it
        Err(DatabricksErrorHelper::invalid_state()
            .message("Schema not yet available - call next_batch() first"))
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
    use crate::reader::cloudfetch::pipeline_types::create_chunk_pair;
    use crate::types::cloudfetch::CloudFetchLink;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::time::Duration;
    use tokio::time::timeout;

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

    fn create_test_batch(id: i32) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![id, id + 1, id + 2])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_sequential_consumption() {
        // Create channels directly to test consumer behavior
        let (result_tx, result_rx) = mpsc::channel::<ChunkHandle>(10);
        let cancel_token = CancellationToken::new();

        // Create provider struct directly for testing (bypassing new())
        let provider = StreamingCloudFetchProvider {
            result_rx: tokio::sync::Mutex::new(result_rx),
            schema: OnceLock::new(),
            batch_buffer: Mutex::new(VecDeque::new()),
            cancel_token: cancel_token.clone(),
            scheduler_handle: tokio::spawn(async {}),
            worker_handles: vec![],
        };

        // Send 3 chunk handles with results
        for i in 0..3 {
            let link = create_test_link(i);
            let (task, handle) = create_chunk_pair(i, link);

            // Send handle to result channel
            result_tx.send(handle).await.unwrap();

            // Simulate worker sending result
            let batch = create_test_batch(i as i32);
            task.result_tx.send(Ok(vec![batch])).unwrap();
        }

        // Drop sender to signal end of stream
        drop(result_tx);

        // Consume all batches in order
        let mut chunk_indices = Vec::new();
        while let Ok(Some(batch)) = provider.next_batch().await {
            let id_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            chunk_indices.push(id_array.value(0));
        }

        // Verify we got batches in order (ids 0, 1, 2)
        assert_eq!(chunk_indices, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn test_end_of_stream_detection() {
        // Create channels
        let (result_tx, result_rx) = mpsc::channel::<ChunkHandle>(10);
        let cancel_token = CancellationToken::new();

        let provider = StreamingCloudFetchProvider {
            result_rx: tokio::sync::Mutex::new(result_rx),
            schema: OnceLock::new(),
            batch_buffer: Mutex::new(VecDeque::new()),
            cancel_token: cancel_token.clone(),
            scheduler_handle: tokio::spawn(async {}),
            worker_handles: vec![],
        };

        // Send one chunk
        let link = create_test_link(0);
        let (task, handle) = create_chunk_pair(0, link);
        result_tx.send(handle).await.unwrap();
        task.result_tx.send(Ok(vec![create_test_batch(0)])).unwrap();

        // Drop sender to close channel
        drop(result_tx);

        // First call should return the batch
        let result = provider.next_batch().await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());

        // Second call should return None (end of stream)
        let result = provider.next_batch().await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_cancellation_during_await() {
        // Create channels
        let (_result_tx, result_rx) = mpsc::channel::<ChunkHandle>(10);
        let cancel_token = CancellationToken::new();

        let provider = StreamingCloudFetchProvider {
            result_rx: tokio::sync::Mutex::new(result_rx),
            schema: OnceLock::new(),
            batch_buffer: Mutex::new(VecDeque::new()),
            cancel_token: cancel_token.clone(),
            scheduler_handle: tokio::spawn(async {}),
            worker_handles: vec![],
        };

        // Spawn a task that calls next_batch (which will block waiting for result)
        let provider = Arc::new(provider);
        let provider_clone = Arc::clone(&provider);

        let next_batch_task = tokio::spawn(async move {
            // This will block because there's nothing in the channel
            provider_clone.next_batch().await
        });

        // Give the task time to start waiting
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Cancel the provider
        cancel_token.cancel();

        // The task should complete with a cancellation error
        let result = timeout(Duration::from_millis(500), next_batch_task)
            .await
            .expect("Task should complete")
            .expect("Task should not panic");

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cancelled"));
    }

    #[tokio::test]
    async fn test_download_error_propagation() {
        // Create channels
        let (result_tx, result_rx) = mpsc::channel::<ChunkHandle>(10);
        let cancel_token = CancellationToken::new();

        let provider = StreamingCloudFetchProvider {
            result_rx: tokio::sync::Mutex::new(result_rx),
            schema: OnceLock::new(),
            batch_buffer: Mutex::new(VecDeque::new()),
            cancel_token: cancel_token.clone(),
            scheduler_handle: tokio::spawn(async {}),
            worker_handles: vec![],
        };

        // Send a chunk handle that will return an error
        let link = create_test_link(0);
        let (task, handle) = create_chunk_pair(0, link);
        result_tx.send(handle).await.unwrap();

        // Send an error result
        let error = DatabricksErrorHelper::io().message("Download failed");
        task.result_tx.send(Err(error)).unwrap();

        // next_batch should return the error
        let result = provider.next_batch().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Download failed"));
    }

    #[tokio::test]
    async fn test_sender_dropped_error() {
        // Create channels
        let (result_tx, result_rx) = mpsc::channel::<ChunkHandle>(10);
        let cancel_token = CancellationToken::new();

        let provider = StreamingCloudFetchProvider {
            result_rx: tokio::sync::Mutex::new(result_rx),
            schema: OnceLock::new(),
            batch_buffer: Mutex::new(VecDeque::new()),
            cancel_token: cancel_token.clone(),
            scheduler_handle: tokio::spawn(async {}),
            worker_handles: vec![],
        };

        // Send a chunk handle but drop the task (simulating worker crash)
        let link = create_test_link(0);
        let (task, handle) = create_chunk_pair(0, link);
        result_tx.send(handle).await.unwrap();

        // Drop the task without sending a result
        drop(task);

        // next_batch should return an error
        let result = provider.next_batch().await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("cancelled or failed"));
    }

    #[tokio::test]
    async fn test_schema_extraction() {
        // Create channels
        let (result_tx, result_rx) = mpsc::channel::<ChunkHandle>(10);
        let cancel_token = CancellationToken::new();

        let provider = StreamingCloudFetchProvider {
            result_rx: tokio::sync::Mutex::new(result_rx),
            schema: OnceLock::new(),
            batch_buffer: Mutex::new(VecDeque::new()),
            cancel_token: cancel_token.clone(),
            scheduler_handle: tokio::spawn(async {}),
            worker_handles: vec![],
        };

        // Initially schema is not available
        let schema_result = provider.get_schema().await;
        assert!(schema_result.is_err());

        // Send a chunk with batches
        let link = create_test_link(0);
        let (task, handle) = create_chunk_pair(0, link);
        result_tx.send(handle).await.unwrap();
        task.result_tx.send(Ok(vec![create_test_batch(0)])).unwrap();

        // Read one batch to populate schema
        let batch = provider.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.num_columns(), 2);

        // Now schema should be available
        let schema = provider.get_schema().await.unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[tokio::test]
    async fn test_multiple_batches_per_chunk() {
        // Create channels
        let (result_tx, result_rx) = mpsc::channel::<ChunkHandle>(10);
        let cancel_token = CancellationToken::new();

        let provider = StreamingCloudFetchProvider {
            result_rx: tokio::sync::Mutex::new(result_rx),
            schema: OnceLock::new(),
            batch_buffer: Mutex::new(VecDeque::new()),
            cancel_token: cancel_token.clone(),
            scheduler_handle: tokio::spawn(async {}),
            worker_handles: vec![],
        };

        // Send a chunk with multiple batches
        let link = create_test_link(0);
        let (task, handle) = create_chunk_pair(0, link);
        result_tx.send(handle).await.unwrap();

        // Send 3 batches for this chunk
        let batches = vec![
            create_test_batch(10),
            create_test_batch(20),
            create_test_batch(30),
        ];
        task.result_tx.send(Ok(batches)).unwrap();

        // Drop sender to close channel after this chunk
        drop(result_tx);

        // Should receive all 3 batches
        let mut batch_ids = Vec::new();
        while let Ok(Some(batch)) = provider.next_batch().await {
            let id_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            batch_ids.push(id_array.value(0));
        }

        assert_eq!(batch_ids, vec![10, 20, 30]);
    }

    #[test]
    fn test_chunk_link_fetch_result_end_of_stream() {
        let result = ChunkLinkFetchResult::end_of_stream();
        assert!(result.links.is_empty());
        assert!(!result.has_more);
        assert!(result.next_chunk_index.is_none());
    }
}
