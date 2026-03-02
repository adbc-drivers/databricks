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

//! Pipeline types for the CloudFetch download pipeline.
//!
//! This module defines the two core types that flow through the pipeline channels:
//!
//! - [`ChunkDownloadTask`]: Sent through `download_channel` to download workers.
//!   Contains the presigned URL link and a oneshot sender for delivering results.
//! - [`ChunkHandle`]: Sent through `result_channel` in chunk-index order.
//!   The consumer awaits the embedded oneshot receiver to get downloaded batches.
//!
//! The [`create_chunk_pair()`] factory function creates a connected pair of these
//! types, linked by a oneshot channel.

use arrow_array::RecordBatch;
use tokio::sync::oneshot;

use crate::error::Result;
use crate::types::cloudfetch::CloudFetchLink;

/// A download task sent through the `download_channel` to a download worker.
///
/// Each task contains everything the worker needs to download and parse a single
/// chunk: the chunk index, the presigned URL link, and a oneshot sender to deliver
/// the result back to the consumer.
///
/// Owned entirely by the download worker — no shared state.
pub struct ChunkDownloadTask {
    /// The index of this chunk in the result set.
    pub chunk_index: i64,
    /// The CloudFetch link containing the presigned URL and headers.
    pub link: CloudFetchLink,
    /// Oneshot sender to deliver the download result (success or error).
    pub result_tx: oneshot::Sender<Result<Vec<RecordBatch>>>,
}

/// A handle to a pending chunk download, sent through the `result_channel`.
///
/// The consumer reads handles from the result channel in chunk-index order and
/// awaits each handle's oneshot receiver to get the downloaded batches. This
/// preserves sequential ordering even when downloads complete out of order.
pub struct ChunkHandle {
    /// The index of this chunk in the result set.
    pub chunk_index: i64,
    /// Oneshot receiver to await the download result.
    pub result_rx: oneshot::Receiver<Result<Vec<RecordBatch>>>,
}

/// Creates a connected `(ChunkDownloadTask, ChunkHandle)` pair.
///
/// Internally creates a `tokio::sync::oneshot` channel and wires the sender
/// into the task and the receiver into the handle. The caller then sends the
/// task to the download channel and the handle to the result channel.
///
/// # Arguments
///
/// * `chunk_index` - The index of this chunk in the result set.
/// * `link` - The CloudFetch link containing the presigned URL.
///
/// # Returns
///
/// A tuple of `(ChunkDownloadTask, ChunkHandle)` connected via a oneshot channel.
pub fn create_chunk_pair(
    chunk_index: i64,
    link: CloudFetchLink,
) -> (ChunkDownloadTask, ChunkHandle) {
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
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    /// Helper to create a test CloudFetchLink.
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

    /// Helper to create a test RecordBatch.
    fn test_record_batch(values: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Int32, false)]));
        let array = Int32Array::from(values.to_vec());
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    #[tokio::test]
    async fn test_create_chunk_pair_ok_result() {
        let link = test_link(42);
        let (task, handle) = create_chunk_pair(42, link);

        // Send Ok(batches) through the task's result_tx
        let batches = vec![test_record_batch(&[1, 2, 3])];
        task.result_tx.send(Ok(batches)).unwrap();

        // Receive on the handle's result_rx
        let result = handle.result_rx.await.unwrap();
        assert!(result.is_ok());
        let received_batches = result.unwrap();
        assert_eq!(received_batches.len(), 1);
        assert_eq!(received_batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_create_chunk_pair_err_result() {
        use crate::error::DatabricksErrorHelper;
        use driverbase::error::ErrorHelper;

        let link = test_link(7);
        let (task, handle) = create_chunk_pair(7, link);

        // Send Err through the task's result_tx
        let err = DatabricksErrorHelper::io().message("download failed");
        task.result_tx.send(Err(err)).unwrap();

        // Receive on the handle's result_rx
        let result = handle.result_rx.await.unwrap();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("download failed"),
            "Expected 'download failed' in error, got: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_create_chunk_pair_chunk_index_propagation() {
        for idx in [0, 1, 42, 999, -1] {
            let link = test_link(idx);
            let (task, handle) = create_chunk_pair(idx, link);

            assert_eq!(
                task.chunk_index, idx,
                "ChunkDownloadTask chunk_index should be {}",
                idx
            );
            assert_eq!(
                handle.chunk_index, idx,
                "ChunkHandle chunk_index should be {}",
                idx
            );

            // Verify the link was correctly assigned to the task
            assert_eq!(task.link.chunk_index, idx);
        }
    }

    #[test]
    fn test_create_chunk_pair_link_preserved() {
        let link = test_link(5);
        let expected_url = link.url.clone();
        let expected_row_count = link.row_count;

        let (task, _handle) = create_chunk_pair(5, link);

        assert_eq!(task.link.url, expected_url);
        assert_eq!(task.link.row_count, expected_row_count);
        assert_eq!(task.link.chunk_index, 5);
    }

    #[tokio::test]
    async fn test_create_chunk_pair_sender_dropped_without_send() {
        let link = test_link(0);
        let (_task, handle) = create_chunk_pair(0, link);

        // Drop the task (and its result_tx) without sending
        drop(_task);

        // The receiver should get a RecvError
        let result = handle.result_rx.await;
        assert!(
            result.is_err(),
            "Expected RecvError when sender is dropped without sending"
        );
    }
}
