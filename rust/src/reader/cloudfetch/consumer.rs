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

//! Consumer for reading results from the CloudFetch pipeline.
//!
//! The Consumer receives `ChunkHandle` items from the `result_channel` in chunk-index order
//! and awaits the embedded `result_rx` to get downloaded batches. This replaces the previous
//! `wait_for_chunk()` Notify-based polling approach with direct future await.
//!
//! # Architecture
//!
//! ```text
//! Consumer (single task) - sequential consumption:
//!   loop {
//!     // First drain batch_buffer
//!     if let Some(batch) = batch_buffer.pop_front() {
//!       return Ok(Some(batch))
//!     }
//!
//!     // Receive next ChunkHandle
//!     handle = result_channel.recv() → ChunkHandle
//!
//!     // Await result from worker
//!     match handle.result_rx.await {
//!       Ok(Ok(batches)) => {
//!         // Populate batch_buffer, return first batch
//!         batch_buffer.extend(batches)
//!         return Ok(Some(batch_buffer.pop_front()))
//!       }
//!       Ok(Err(e)) => return Err(e),
//!       Err(_) => return Err("worker dropped"),
//!     }
//!
//!     // Handle channel closed (None) → return Ok(None)
//!   }
//! ```
//!
//! # Key Features
//!
//! - **Sequential ordering**: Reads from `result_channel` in chunk-index order
//! - **No polling**: Direct await on `oneshot::Receiver`, no timeout loop
//! - **Buffer management**: Drains `batch_buffer` before fetching next chunk
//! - **Schema capture**: Extracts schema from first batch using `OnceLock`
//! - **Cancellation support**: Respects `CancellationToken` for clean shutdown

use crate::error::{DatabricksErrorHelper, Result};
use crate::reader::cloudfetch::pipeline_types::ChunkHandle;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use driverbase::error::ErrorHelper;
use std::collections::VecDeque;
use std::sync::OnceLock;
use tokio::sync::{mpsc::Receiver as MpscReceiver, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// Consumer for reading results from the CloudFetch pipeline.
///
/// The Consumer receives `ChunkHandle` items from `result_channel` in sequential order
/// and awaits the embedded `result_rx` to get downloaded batches from workers.
///
/// # Example
///
/// ```rust,ignore
/// let consumer = Consumer::new(result_rx, cancel_token);
///
/// while let Some(batch) = consumer.next_batch().await? {
///     // Process batch
/// }
/// ```
pub struct Consumer {
    /// Channel for receiving ChunkHandles in chunk-index order.
    result_rx: Mutex<MpscReceiver<ChunkHandle>>,

    /// Buffer for batches from current chunk.
    /// When a chunk contains multiple batches, they are stored here
    /// and returned one at a time on subsequent `next_batch()` calls.
    #[cfg_attr(test, allow(dead_code))]
    pub(crate) batch_buffer: Mutex<VecDeque<RecordBatch>>,

    /// Schema extracted from the first batch.
    /// Captured lazily on first `next_batch()` call.
    schema: OnceLock<SchemaRef>,

    /// Cancellation token for coordinated shutdown.
    cancel_token: CancellationToken,
}

impl Consumer {
    /// Create a new Consumer.
    ///
    /// # Arguments
    ///
    /// * `result_rx` - Channel for receiving ChunkHandles in order
    /// * `cancel_token` - Cancellation token for coordinated shutdown
    pub fn new(result_rx: MpscReceiver<ChunkHandle>, cancel_token: CancellationToken) -> Self {
        Self {
            result_rx: Mutex::new(result_rx),
            batch_buffer: Mutex::new(VecDeque::new()),
            schema: OnceLock::new(),
            cancel_token,
        }
    }

    /// Get the next record batch from the pipeline.
    ///
    /// This method:
    /// 1. Drains `batch_buffer` first before fetching a new chunk
    /// 2. Receives the next `ChunkHandle` from `result_channel`
    /// 3. Awaits `handle.result_rx` to get the download result
    /// 4. On success: populates `batch_buffer` with batches and returns the first one
    /// 5. On error: propagates the error from the worker
    /// 6. On channel closed: returns `Ok(None)` to signal end-of-stream
    ///
    /// # Returns
    ///
    /// - `Ok(Some(batch))` - Next batch is ready
    /// - `Ok(None)` - End of stream reached (all chunks consumed)
    /// - `Err(e)` - Download failed or operation was cancelled
    ///
    /// # Cancellation
    ///
    /// Respects `cancel_token` and returns an error if cancellation is requested.
    pub async fn next_batch(&self) -> Result<Option<RecordBatch>> {
        loop {
            // Check for cancellation
            if self.cancel_token.is_cancelled() {
                return Err(DatabricksErrorHelper::invalid_state().message("Operation cancelled"));
            }

            // Step 1: Drain batch_buffer first
            {
                let mut buffer = self.batch_buffer.lock().await;
                if let Some(batch) = buffer.pop_front() {
                    // Capture schema from first batch
                    let _ = self.schema.get_or_init(|| batch.schema());
                    debug!("Consumer: returning batch from buffer");
                    return Ok(Some(batch));
                }
            }

            // Step 2: Receive next ChunkHandle from result_channel
            let handle = {
                let mut rx = self.result_rx.lock().await;

                tokio::select! {
                    result = rx.recv() => {
                        match result {
                            Some(handle) => handle,
                            None => {
                                // Channel closed - end of stream
                                debug!("Consumer: result_channel closed, end of stream");
                                return Ok(None);
                            }
                        }
                    }
                    _ = self.cancel_token.cancelled() => {
                        debug!("Consumer: cancellation requested during recv");
                        return Err(DatabricksErrorHelper::invalid_state()
                            .message("Operation cancelled"));
                    }
                }
            };

            let chunk_index = handle.chunk_index;
            debug!("Consumer: received ChunkHandle for chunk {}", chunk_index);

            // Step 3: Await result_rx to get Result<Vec<RecordBatch>> from worker
            let download_result = tokio::select! {
                result = handle.result_rx => {
                    match result {
                        Ok(res) => res,
                        Err(_) => {
                            // Worker dropped without sending - this indicates panic or bug
                            warn!("Consumer: worker dropped result_tx for chunk {} without sending", chunk_index);
                            return Err(DatabricksErrorHelper::invalid_state()
                                .message(format!("Worker dropped result for chunk {} without sending", chunk_index))
                                .context("oneshot receiver error"));
                        }
                    }
                }
                _ = self.cancel_token.cancelled() => {
                    debug!("Consumer: cancellation requested during result_rx.await");
                    return Err(DatabricksErrorHelper::invalid_state()
                        .message("Operation cancelled"));
                }
            };

            // Step 4: Handle download result
            match download_result {
                Ok(batches) => {
                    if batches.is_empty() {
                        warn!("Consumer: chunk {} returned empty batches, trying next chunk", chunk_index);
                        // This is unusual but not an error - loop to try next chunk
                        continue;
                    }

                    debug!(
                        "Consumer: chunk {} downloaded successfully ({} batches)",
                        chunk_index,
                        batches.len()
                    );

                    // Step 5: Populate batch_buffer and return first batch
                    let mut buffer = self.batch_buffer.lock().await;
                    for batch in batches {
                        buffer.push_back(batch);
                    }

                    if let Some(batch) = buffer.pop_front() {
                        // Capture schema from first batch
                        let _ = self.schema.get_or_init(|| batch.schema());
                        return Ok(Some(batch));
                    } else {
                        // Should not happen since we checked !batches.is_empty()
                        warn!("Consumer: batch_buffer empty after populating from chunk {}", chunk_index);
                        return Ok(None);
                    }
                }
                Err(e) => {
                    // Step 6: Propagate error from worker
                    debug!(
                        "Consumer: chunk {} download failed: {}",
                        chunk_index, e
                    );
                    return Err(e);
                }
            }
        }
    }

    /// Get the schema of the result set.
    ///
    /// Returns the schema if it has been captured from the first batch,
    /// or `None` if no batches have been consumed yet.
    pub fn schema(&self) -> Option<SchemaRef> {
        self.schema.get().cloned()
    }

    /// Cancel the consumer.
    ///
    /// This will cause any in-progress or future `next_batch()` calls to return
    /// an error. This is typically called during shutdown or when an error occurs
    /// that requires stopping consumption.
    pub fn cancel(&self) {
        debug!("Consumer: cancellation requested");
        self.cancel_token.cancel();
    }
}

impl std::fmt::Debug for Consumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Note: We can't lock in Debug since it's not async
        // Just show minimal info without buffer length
        f.debug_struct("Consumer")
            .field("has_schema", &self.schema.get().is_some())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::cloudfetch::pipeline_types::ChunkHandle;
    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    use tokio::sync::{mpsc, oneshot};

    /// Helper to create a test RecordBatch with a simple schema.
    fn create_test_batch(num_rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let array = arrow_array::Int32Array::from(vec![1; num_rows]);
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    #[tokio::test]
    async fn test_consumer_drains_buffer_before_fetching_new_chunk() {
        // Test that next_batch drains batch_buffer before receiving next ChunkHandle
        let (_result_tx, result_rx) = mpsc::channel(10);
        let cancel_token = CancellationToken::new();
        let consumer = Consumer::new(result_rx, cancel_token.clone());

        // Pre-populate batch_buffer with 3 batches
        {
            let mut buffer = consumer.batch_buffer.lock().await;
            buffer.push_back(create_test_batch(10));
            buffer.push_back(create_test_batch(20));
            buffer.push_back(create_test_batch(30));
        }

        // First call should drain buffer
        let batch1 = consumer.next_batch().await.unwrap().unwrap();
        assert_eq!(batch1.num_rows(), 10);

        // Second call should continue draining buffer
        let batch2 = consumer.next_batch().await.unwrap().unwrap();
        assert_eq!(batch2.num_rows(), 20);

        // Third call should drain last batch
        let batch3 = consumer.next_batch().await.unwrap().unwrap();
        assert_eq!(batch3.num_rows(), 30);

        // Buffer should be empty now
        assert_eq!(consumer.batch_buffer.lock().await.len(), 0);

        // Cancel to avoid waiting forever for next ChunkHandle
        cancel_token.cancel();
    }

    #[tokio::test]
    async fn test_consumer_awaits_chunk_handle_correctly() {
        // Test that next_batch correctly awaits ChunkHandle.result_rx
        let (result_tx, result_rx) = mpsc::channel(10);
        let cancel_token = CancellationToken::new();
        let consumer = Consumer::new(result_rx, cancel_token);

        // Spawn a task to send a ChunkHandle
        tokio::spawn(async move {
            let (tx, rx) = oneshot::channel();
            let handle = ChunkHandle {
                chunk_index: 0,
                result_rx: rx,
            };

            // Send handle first
            result_tx.send(handle).await.unwrap();

            // Send result after a short delay
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            let batches = vec![create_test_batch(100)];
            tx.send(Ok(batches)).unwrap();
        });

        // Consumer should block until result is available
        let batch = consumer.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 100);
    }

    #[tokio::test]
    async fn test_consumer_propagates_error_from_worker() {
        // Test that errors from download workers are propagated correctly
        let (result_tx, result_rx) = mpsc::channel(10);
        let cancel_token = CancellationToken::new();
        let consumer = Consumer::new(result_rx, cancel_token);

        // Spawn a task to send an error
        tokio::spawn(async move {
            let (tx, rx) = oneshot::channel();
            let handle = ChunkHandle {
                chunk_index: 0,
                result_rx: rx,
            };

            result_tx.send(handle).await.unwrap();

            // Send error
            let error = DatabricksErrorHelper::io()
                .message("network timeout")
                .context("download chunk 0");
            tx.send(Err(error)).unwrap();
        });

        // Consumer should receive the error
        let result = consumer.next_batch().await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("network timeout") || err_msg.contains("download chunk 0"));
    }

    #[tokio::test]
    async fn test_consumer_handles_end_of_stream() {
        // Test that channel closed (None) returns Ok(None)
        let (result_tx, result_rx) = mpsc::channel(10);
        let cancel_token = CancellationToken::new();
        let consumer = Consumer::new(result_rx, cancel_token);

        // Close the channel by dropping sender
        drop(result_tx);

        // Consumer should return Ok(None) for end-of-stream
        let result = consumer.next_batch().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_consumer_handles_cancellation() {
        // Test that cancellation is respected
        let (_result_tx, result_rx) = mpsc::channel(10);
        let cancel_token = CancellationToken::new();
        let consumer = Consumer::new(result_rx, cancel_token.clone());

        // Cancel immediately
        cancel_token.cancel();

        // Consumer should return error
        let result = consumer.next_batch().await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("cancelled") || err_msg.contains("Cancelled"));
    }

    #[tokio::test]
    async fn test_consumer_captures_schema() {
        // Test that schema is captured from first batch
        let (result_tx, result_rx) = mpsc::channel(10);
        let cancel_token = CancellationToken::new();
        let consumer = Consumer::new(result_rx, cancel_token);

        // Spawn a task to send batches
        tokio::spawn(async move {
            let (tx, rx) = oneshot::channel();
            let handle = ChunkHandle {
                chunk_index: 0,
                result_rx: rx,
            };

            result_tx.send(handle).await.unwrap();

            let batches = vec![create_test_batch(100)];
            tx.send(Ok(batches)).unwrap();
        });

        // Schema should be None initially
        assert!(consumer.schema().is_none());

        // After first batch
        let batch = consumer.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 100);

        // Schema should be captured now
        let schema = consumer.schema().unwrap();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "id");
    }

    #[tokio::test]
    async fn test_consumer_handles_multiple_batches_per_chunk() {
        // Test that multiple batches from a single chunk are buffered correctly
        let (result_tx, result_rx) = mpsc::channel(10);
        let cancel_token = CancellationToken::new();
        let consumer = Consumer::new(result_rx, cancel_token);

        // Spawn a task to send a chunk with 3 batches
        tokio::spawn(async move {
            let (tx, rx) = oneshot::channel();
            let handle = ChunkHandle {
                chunk_index: 0,
                result_rx: rx,
            };

            result_tx.send(handle).await.unwrap();

            let batches = vec![
                create_test_batch(100),
                create_test_batch(200),
                create_test_batch(300),
            ];
            tx.send(Ok(batches)).unwrap();
        });

        // First call should return first batch from chunk 0
        let batch1 = consumer.next_batch().await.unwrap().unwrap();
        assert_eq!(batch1.num_rows(), 100);

        // Second call should return second batch from buffer
        let batch2 = consumer.next_batch().await.unwrap().unwrap();
        assert_eq!(batch2.num_rows(), 200);

        // Third call should return third batch from buffer
        let batch3 = consumer.next_batch().await.unwrap().unwrap();
        assert_eq!(batch3.num_rows(), 300);
    }

    #[tokio::test]
    async fn test_consumer_handles_worker_drop() {
        // Test that worker dropping result_tx without sending is handled
        let (result_tx, result_rx) = mpsc::channel(10);
        let cancel_token = CancellationToken::new();
        let consumer = Consumer::new(result_rx, cancel_token);

        // Spawn a task to send a handle but drop tx without sending
        tokio::spawn(async move {
            let (tx, rx) = oneshot::channel::<Result<Vec<RecordBatch>>>();
            let handle = ChunkHandle {
                chunk_index: 0,
                result_rx: rx,
            };

            result_tx.send(handle).await.unwrap();

            // Drop tx without sending - simulates worker panic
            drop(tx);
        });

        // Consumer should receive an error
        let result = consumer.next_batch().await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("dropped") || err_msg.contains("without sending"));
    }
}
