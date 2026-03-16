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

//! Pipeline types for the two-channel CloudFetch download pipeline.
//!
//! This module defines the types that flow through the download pipeline:
//! - `ChunkDownloadTask` - Sent through download_channel, owned by download workers
//! - `ChunkHandle` - Sent through result_channel in chunk-index order, awaited by consumer
//!
//! These types mirror C#'s `IDownloadResult` with `DownloadCompletedTask` (TaskCompletionSource).
//!
//! ## Pipeline Architecture
//!
//! ```text
//! [Link Fetcher] --> download_channel --> [Download Workers]
//!                          |                     |
//!              ChunkDownloadTask          (downloads data)
//!                                                |
//!                                                v
//! [Consumer] <-- result_channel <-- ChunkHandle (in order)
//! ```
//!
//! The `create_chunk_pair()` function creates a connected (task, handle) pair where:
//! - The task contains a oneshot sender for the download worker to send results
//! - The handle contains a oneshot receiver for the consumer to await results

use crate::error::Result;
use crate::types::cloudfetch::CloudFetchLink;
use arrow_array::RecordBatch;
use tokio::sync::oneshot;

/// A download task sent to download workers via download_channel.
///
/// Contains all information needed by a download worker to fetch and parse
/// a chunk from cloud storage, plus a oneshot sender to communicate the result
/// back to the consumer through the associated `ChunkHandle`.
///
/// ## Ownership Model
///
/// - Sent through `download_channel` to be processed by download workers
/// - Owned by the download worker during download
/// - The `result_tx` sender is consumed when sending the download result
///
/// ## Example
///
/// ```ignore
/// // Download worker processes the task
/// let batches = downloader.download(&task.link).await?;
/// task.result_tx.send(Ok(batches)).ok();
/// ```
#[derive(Debug)]
pub struct ChunkDownloadTask {
    /// Index of this chunk in the result set (used for ordering).
    pub chunk_index: i64,
    /// CloudFetch link containing URL, headers, and metadata for the download.
    pub link: CloudFetchLink,
    /// Oneshot sender to communicate the download result to the consumer.
    /// The download worker sends either Ok(Vec<RecordBatch>) on success
    /// or Err(Error) on failure.
    pub result_tx: oneshot::Sender<Result<Vec<RecordBatch>>>,
}

/// A handle for the consumer to await download results.
///
/// Sent through `result_channel` in chunk-index order so the consumer
/// can await results in the correct sequence regardless of download
/// completion order.
///
/// ## Ownership Model
///
/// - Created alongside `ChunkDownloadTask` via `create_chunk_pair()`
/// - Sent through `result_channel` in chunk-index order
/// - Owned by the consumer which awaits the oneshot receiver
///
/// ## Example
///
/// ```ignore
/// // Consumer awaits results in order
/// let batches = handle.result_rx.await
///     .map_err(|_| DatabricksErrorHelper::io().message("Download task cancelled"))?;
/// ```
#[derive(Debug)]
pub struct ChunkHandle {
    /// Index of this chunk in the result set (used for debugging/logging).
    pub chunk_index: i64,
    /// Oneshot receiver to await the download result from the worker.
    pub result_rx: oneshot::Receiver<Result<Vec<RecordBatch>>>,
}

/// Creates a connected (ChunkDownloadTask, ChunkHandle) pair.
///
/// The task and handle are connected via a tokio oneshot channel:
/// - The task's `result_tx` sender is used by the download worker to send results
/// - The handle's `result_rx` receiver is used by the consumer to await results
///
/// ## Arguments
///
/// * `chunk_index` - The index of this chunk in the result set
/// * `link` - The CloudFetch link for downloading this chunk
///
/// ## Returns
///
/// A tuple of (ChunkDownloadTask, ChunkHandle) that are connected via a oneshot channel.
///
/// ## Example
///
/// ```ignore
/// let (task, handle) = create_chunk_pair(0, link);
///
/// // Send task to download worker
/// download_tx.send(task).await?;
///
/// // Send handle to result channel (maintains order)
/// result_tx.send(handle).await?;
///
/// // Later, consumer awaits the handle
/// let batches = handle.result_rx.await??;
/// ```
pub fn create_chunk_pair(chunk_index: i64, link: CloudFetchLink) -> (ChunkDownloadTask, ChunkHandle) {
    let (result_tx, result_rx) = oneshot::channel();

    let task = ChunkDownloadTask {
        chunk_index,
        link,
        result_tx,
    };

    let handle = ChunkHandle {
        chunk_index,
        result_rx,
    };

    (task, handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::DatabricksErrorHelper;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use driverbase::error::ErrorHelper;
    use std::collections::HashMap;
    use std::sync::Arc;

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

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_create_chunk_pair_returns_connected_pair() {
        let link = create_test_link(42);
        let (task, handle) = create_chunk_pair(42, link);

        // Both should have the same chunk index
        assert_eq!(task.chunk_index, 42);
        assert_eq!(handle.chunk_index, 42);

        // Task should have the link
        assert_eq!(task.link.chunk_index, 42);
        assert!(task.link.url.contains("chunk42"));
    }

    #[tokio::test]
    async fn test_oneshot_channel_communication_success() {
        let link = create_test_link(0);
        let (task, handle) = create_chunk_pair(0, link);

        // Simulate download worker sending result
        let batch = create_test_batch();
        let batches = vec![batch];
        task.result_tx.send(Ok(batches)).unwrap();

        // Consumer receives result
        let result = handle.result_rx.await.unwrap();
        assert!(result.is_ok());
        let received_batches = result.unwrap();
        assert_eq!(received_batches.len(), 1);
        assert_eq!(received_batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_oneshot_channel_communication_error() {
        let link = create_test_link(0);
        let (task, handle) = create_chunk_pair(0, link);

        // Simulate download worker sending error
        let error = DatabricksErrorHelper::io().message("Download failed");
        task.result_tx.send(Err(error)).unwrap();

        // Consumer receives error
        let result = handle.result_rx.await.unwrap();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Download failed"));
    }

    #[tokio::test]
    async fn test_oneshot_channel_sender_dropped() {
        let link = create_test_link(0);
        let (task, handle) = create_chunk_pair(0, link);

        // Drop the sender without sending (simulates worker crash/cancellation)
        drop(task.result_tx);

        // Receiver should get RecvError
        let result = handle.result_rx.await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_multiple_chunk_pairs_independent() {
        let link1 = create_test_link(0);
        let link2 = create_test_link(1);

        let (task1, handle1) = create_chunk_pair(0, link1);
        let (task2, handle2) = create_chunk_pair(1, link2);

        // Send results out of order (chunk 1 completes first)
        let batch2 = create_test_batch();
        task2.result_tx.send(Ok(vec![batch2])).unwrap();

        let batch1 = create_test_batch();
        task1.result_tx.send(Ok(vec![batch1])).unwrap();

        // Both handles should receive their respective results
        let result1 = handle1.result_rx.await.unwrap().unwrap();
        let result2 = handle2.result_rx.await.unwrap().unwrap();

        assert_eq!(result1.len(), 1);
        assert_eq!(result2.len(), 1);
    }
}
