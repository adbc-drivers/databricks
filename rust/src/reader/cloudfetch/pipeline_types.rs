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

//! Pipeline types for the channel-based CloudFetch download pipeline.
//!
//! These types connect the scheduler (which dispatches download tasks) to the
//! download workers (which execute them) and back to the consumer (which reads
//! results in order). Each chunk gets a oneshot channel pair: the scheduler
//! sends a `ChunkDownloadTask` to a worker and hands back a `ChunkHandle` to
//! the consumer for awaiting the result.

use crate::error::Result;
use crate::types::cloudfetch::CloudFetchLink;
use arrow_array::RecordBatch;
use tokio::sync::oneshot;

/// A download task dispatched to a worker.
///
/// Contains the chunk link to download and a oneshot sender to deliver the result
/// back to the consumer. The worker downloads the chunk data, parses it into
/// Arrow RecordBatches, and sends the result through `result_tx`.
#[derive(Debug)]
pub struct ChunkDownloadTask {
    /// Index of the chunk in the result set.
    pub chunk_index: i64,
    /// The presigned link for downloading this chunk.
    pub link: CloudFetchLink,
    /// Channel to send the download result back to the consumer.
    pub result_tx: oneshot::Sender<Result<Vec<RecordBatch>>>,
}

/// A handle for the consumer to await a chunk's download result.
///
/// The consumer holds `ChunkHandle`s in order and awaits each one sequentially,
/// ensuring results are delivered in chunk order regardless of which worker
/// finishes first.
#[derive(Debug)]
pub struct ChunkHandle {
    /// Index of the chunk in the result set.
    pub chunk_index: i64,
    /// Channel to receive the download result from the worker.
    pub result_rx: oneshot::Receiver<Result<Vec<RecordBatch>>>,
}

/// Create a connected (task, handle) pair for a chunk download.
///
/// The returned `ChunkDownloadTask` should be sent to a download worker via
/// an mpsc channel, and the returned `ChunkHandle` should be held by the
/// consumer to await the result.
///
/// # Arguments
/// * `chunk_index` - Index of the chunk in the result set
/// * `link` - The presigned CloudFetch link for downloading this chunk
pub fn create_chunk_pair(
    chunk_index: i64,
    link: CloudFetchLink,
) -> (ChunkDownloadTask, ChunkHandle) {
    let (tx, rx) = oneshot::channel();
    let task = ChunkDownloadTask {
        chunk_index,
        link,
        result_tx: tx,
    };
    let handle = ChunkHandle {
        chunk_index,
        result_rx: rx,
    };
    (task, handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use chrono::Utc;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_test_link(chunk_index: i64) -> CloudFetchLink {
        CloudFetchLink {
            url: format!("https://storage.example.com/chunk{}", chunk_index),
            chunk_index,
            row_offset: chunk_index * 1000,
            row_count: 1000,
            byte_count: 50000,
            expiration: Utc::now() + chrono::Duration::hours(1),
            http_headers: HashMap::new(),
            next_chunk_index: Some(chunk_index + 1),
        }
    }

    fn create_test_batch() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
        vec![batch]
    }

    #[tokio::test]
    async fn test_create_chunk_pair() {
        let link = create_test_link(42);
        let (task, handle) = create_chunk_pair(42, link);

        // Verify chunk indices match
        assert_eq!(task.chunk_index, 42);
        assert_eq!(handle.chunk_index, 42);

        // Verify the oneshot channel is connected: send through task, receive through handle
        let batches = create_test_batch();
        let expected_len = batches.len();
        task.result_tx.send(Ok(batches)).unwrap();

        let result = handle.result_rx.await.unwrap();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), expected_len);
    }

    #[tokio::test]
    async fn test_create_chunk_pair_error_propagation() {
        let link = create_test_link(7);
        let (task, handle) = create_chunk_pair(7, link);

        // Send an error through the channel
        use crate::error::DatabricksErrorHelper;
        use driverbase::error::ErrorHelper;
        let err = DatabricksErrorHelper::io().message("download failed");
        task.result_tx.send(Err(err)).unwrap();

        let result = handle.result_rx.await.unwrap();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_chunk_pair_dropped_sender() {
        let link = create_test_link(0);
        let (task, handle) = create_chunk_pair(0, link);

        // Drop the task (and its sender) without sending
        drop(task);

        // The receiver should get a RecvError
        let result = handle.result_rx.await;
        assert!(result.is_err());
    }

    #[test]
    fn test_chunk_download_task_has_correct_link() {
        let link = create_test_link(5);
        let expected_url = link.url.clone();
        let (task, _handle) = create_chunk_pair(5, link);

        assert_eq!(task.link.url, expected_url);
        assert_eq!(task.link.chunk_index, 5);
        assert_eq!(task.link.row_count, 1000);
    }
}
