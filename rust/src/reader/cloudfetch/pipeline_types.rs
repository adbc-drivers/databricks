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

//! Pipeline types for the channel-based CloudFetch architecture.
//!
//! This module defines the core types used in the CloudFetch download pipeline:
//!
//! - [`ChunkDownloadTask`]: Sent through `download_channel`, consumed by download workers
//! - [`ChunkHandle`]: Sent through `result_channel` in scheduler order, consumed by the consumer
//!
//! These types replace the previous `ChunkEntry`/`ChunkState` pattern, eliminating the need
//! for a shared `DashMap` and enabling a cleaner separation of concerns.
//!
//! # Architecture
//!
//! ```text
//! Scheduler creates oneshot pair (tx, rx) for each chunk:
//!   - Sends ChunkDownloadTask { tx, link, ... } → download_channel → Worker
//!   - Sends ChunkHandle { rx, ... } → result_channel → Consumer
//!
//! Worker downloads chunk and sends result via tx.
//! Consumer awaits result via rx.
//! ```
//!
//! This mirrors the C# driver's `IDownloadResult` with `TaskCompletionSource`.

use crate::error::Result;
use crate::types::cloudfetch::CloudFetchLink;
use arrow_array::RecordBatch;
use tokio::sync::oneshot;

/// Download task sent to workers via `download_channel`.
///
/// Each worker receives a `ChunkDownloadTask`, downloads the chunk from the presigned URL,
/// and sends the result (success or error) back through the `result_tx` channel.
///
/// This struct is **not** `Clone` — it is owned entirely by a single download worker.
///
/// # Fields
///
/// - `chunk_index`: Sequential index of this chunk in the result set (0-based)
/// - `link`: Presigned URL and metadata for downloading the chunk
/// - `result_tx`: Channel to send the download result back to the consumer
///
/// # Example
///
/// ```rust,ignore
/// // Scheduler creates the task and sends it to workers
/// let (result_tx, result_rx) = oneshot::channel();
/// let task = ChunkDownloadTask {
///     chunk_index: 0,
///     link: CloudFetchLink { /* ... */ },
///     result_tx,
/// };
/// download_channel.send(task).await?;
/// ```
pub struct ChunkDownloadTask {
    /// Sequential index of this chunk (0-based).
    pub chunk_index: i64,

    /// Presigned URL and metadata for downloading this chunk.
    pub link: CloudFetchLink,

    /// Channel to send the download result (success or error) back to the consumer.
    ///
    /// The worker calls `result_tx.send(Ok(batches))` on success or
    /// `result_tx.send(Err(error))` on failure.
    pub result_tx: oneshot::Sender<Result<Vec<RecordBatch>>>,
}

/// Handle to a chunk result, sent to consumers via `result_channel` in order.
///
/// The scheduler sends `ChunkHandle` items to `result_channel` in chunk-index order
/// (before the download starts), allowing the consumer to read results sequentially
/// even when downloads complete out of order.
///
/// The consumer awaits the embedded `result_rx` to get the downloaded batches when ready.
///
/// This struct is **not** `Clone` — it is owned entirely by the consumer.
///
/// # Fields
///
/// - `chunk_index`: Sequential index of this chunk (for debugging/logging)
/// - `result_rx`: Channel to receive the download result from the worker
///
/// # Example
///
/// ```rust,ignore
/// // Consumer receives handle in order and awaits result
/// let handle = result_channel.recv().await?;
/// match handle.result_rx.await {
///     Ok(Ok(batches)) => {
///         // Process batches
///     }
///     Ok(Err(e)) => {
///         // Download failed
///     }
///     Err(_) => {
///         // Worker dropped without sending (should not happen)
///     }
/// }
/// ```
pub struct ChunkHandle {
    /// Sequential index of this chunk (0-based).
    ///
    /// Used for debugging and logging. The consumer implicitly knows the order
    /// by reading from `result_channel` sequentially.
    pub chunk_index: i64,

    /// Channel to receive the download result from the worker.
    ///
    /// The consumer awaits this receiver to get the downloaded batches.
    /// - `Ok(Ok(batches))` — download succeeded
    /// - `Ok(Err(error))` — download failed after retries
    /// - `Err(RecvError)` — worker dropped without sending (indicates panic or cancellation)
    pub result_rx: oneshot::Receiver<Result<Vec<RecordBatch>>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::cloudfetch::CloudFetchLink;
    use chrono::{DateTime, Utc};
    use std::collections::HashMap;

    /// Helper to create a test CloudFetchLink.
    fn create_test_link(chunk_index: i64) -> CloudFetchLink {
        CloudFetchLink {
            url: format!("https://storage.example.com/chunk{}", chunk_index),
            chunk_index,
            row_offset: chunk_index * 1000,
            row_count: 1000,
            byte_count: 50000,
            expiration: DateTime::parse_from_rfc3339("2099-01-01T12:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            http_headers: HashMap::new(),
            next_chunk_index: Some(chunk_index + 1),
        }
    }

    #[test]
    fn test_chunk_download_task_construction() {
        // Verify we can construct a ChunkDownloadTask with valid fields
        let (result_tx, _result_rx) = oneshot::channel();
        let link = create_test_link(0);

        let task = ChunkDownloadTask {
            chunk_index: 0,
            link: link.clone(),
            result_tx,
        };

        assert_eq!(task.chunk_index, 0);
        assert_eq!(task.link.chunk_index, 0);
        assert_eq!(task.link.url, "https://storage.example.com/chunk0");
    }

    #[test]
    fn test_chunk_handle_construction() {
        // Verify we can construct a ChunkHandle with valid fields
        let (_result_tx, result_rx) = oneshot::channel();

        let handle = ChunkHandle {
            chunk_index: 0,
            result_rx,
        };

        assert_eq!(handle.chunk_index, 0);
    }

    #[tokio::test]
    async fn test_oneshot_channel_pair_works() {
        // Verify the oneshot channel pair works correctly for communicating results
        let (result_tx, result_rx) = oneshot::channel();

        // Create a ChunkDownloadTask and ChunkHandle
        let link = create_test_link(0);
        let _task = ChunkDownloadTask {
            chunk_index: 0,
            link,
            result_tx,
        };

        let handle = ChunkHandle {
            chunk_index: 0,
            result_rx,
        };

        // Simulate worker sending result (using _task.result_tx, but we already moved it)
        // So we'll create a fresh pair for this test
        let (tx, rx) = oneshot::channel();

        // Simulate worker sending success
        let batches: Vec<RecordBatch> = vec![];
        tx.send(Ok(batches)).unwrap();

        // Simulate consumer receiving result
        let result: Result<Vec<RecordBatch>> = rx.await.unwrap();
        assert!(result.is_ok());

        // Verify handle was constructed correctly
        assert_eq!(handle.chunk_index, 0);
    }

    #[tokio::test]
    async fn test_oneshot_send_error() {
        // Verify error propagation through oneshot channel
        let (tx, rx) = oneshot::channel();

        // Simulate worker sending error
        use crate::error::DatabricksErrorHelper;
        use driverbase::error::ErrorHelper;

        let error = DatabricksErrorHelper::io()
            .message("network timeout")
            .context("download chunk");

        tx.send(Err(error)).unwrap();

        // Consumer receives the error
        let result: Result<Vec<RecordBatch>> = rx.await.unwrap();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_oneshot_recv_error_when_sender_dropped() {
        // Verify RecvError when worker drops without sending
        let (tx, rx) = oneshot::channel::<Result<Vec<RecordBatch>>>();

        // Drop sender without sending
        drop(tx);

        // Consumer gets RecvError
        let result = rx.await;
        assert!(result.is_err());
    }
}
