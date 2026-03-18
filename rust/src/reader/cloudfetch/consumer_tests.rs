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

//! Comprehensive tests for the Consumer module.
//!
//! These tests verify all exit criteria from the implementation spec:
//! 1. next_batch() drains batch_buffer first
//! 2. Receives ChunkHandle from result_channel
//! 3. Awaits result_rx.await to get Result<Vec<RecordBatch>>
//! 4. Handles Ok(batches): populate batch_buffer, return first batch
//! 5. Handles Err(e): propagate error
//! 6. Handles channel closed (None): return Ok(None)
//! 7. Captures schema from first batch
//! 8. Handles cancellation via CancellationToken

use crate::error::{DatabricksErrorHelper, Result};
use crate::reader::cloudfetch::consumer::Consumer;
use crate::reader::cloudfetch::pipeline_types::ChunkHandle;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use driverbase::error::ErrorHelper;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

/// Helper to create a test RecordBatch with a simple schema.
fn create_test_batch(num_rows: usize, field_name: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        field_name,
        DataType::Int32,
        false,
    )]));
    let array = arrow_array::Int32Array::from(vec![1; num_rows]);
    RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
}

/// Helper to create a ChunkHandle with a result channel.
fn create_chunk_handle(
    chunk_index: i64,
) -> (ChunkHandle, oneshot::Sender<Result<Vec<RecordBatch>>>) {
    let (tx, rx) = oneshot::channel();
    let handle = ChunkHandle {
        chunk_index,
        result_rx: rx,
    };
    (handle, tx)
}

#[tokio::test]
async fn test_next_batch_drains_buffer_before_fetching_new_chunk() {
    // EXIT CRITERION 1: next_batch() drains batch_buffer first
    let (_result_tx, result_rx) = mpsc::channel(10);
    let cancel_token = CancellationToken::new();
    let consumer = Consumer::new(result_rx, cancel_token.clone());

    // Pre-populate batch_buffer with 3 batches
    // Note: In production, batch_buffer is private and only populated via next_batch()
    // This direct manipulation is only for testing the buffer drain logic
    {
        let mut buffer = consumer.batch_buffer.lock().await;
        buffer.push_back(create_test_batch(10, "id"));
        buffer.push_back(create_test_batch(20, "id"));
        buffer.push_back(create_test_batch(30, "id"));
    }

    // All three calls should drain buffer without touching result_channel
    let batch1 = consumer.next_batch().await.unwrap().unwrap();
    assert_eq!(batch1.num_rows(), 10);

    let batch2 = consumer.next_batch().await.unwrap().unwrap();
    assert_eq!(batch2.num_rows(), 20);

    let batch3 = consumer.next_batch().await.unwrap().unwrap();
    assert_eq!(batch3.num_rows(), 30);

    // Buffer should be empty now
    assert_eq!(consumer.batch_buffer.lock().await.len(), 0);

    cancel_token.cancel();
}

#[tokio::test]
async fn test_receives_chunk_handle_from_result_channel() {
    // EXIT CRITERION 2: Receives ChunkHandle from result_channel
    let (result_tx, result_rx) = mpsc::channel(10);
    let cancel_token = CancellationToken::new();
    let consumer = Consumer::new(result_rx, cancel_token);

    // Send three ChunkHandles sequentially
    tokio::spawn(async move {
        for i in 0..3 {
            let (handle, tx) = create_chunk_handle(i);
            result_tx.send(handle).await.unwrap();

            // Send result immediately
            let batches = vec![create_test_batch(100 * (i as usize + 1), "id")];
            tx.send(Ok(batches)).unwrap();
        }
    });

    // Consumer should receive all three in order
    let batch1 = consumer.next_batch().await.unwrap().unwrap();
    assert_eq!(batch1.num_rows(), 100);

    let batch2 = consumer.next_batch().await.unwrap().unwrap();
    assert_eq!(batch2.num_rows(), 200);

    let batch3 = consumer.next_batch().await.unwrap().unwrap();
    assert_eq!(batch3.num_rows(), 300);
}

#[tokio::test]
async fn test_awaits_result_rx_to_get_result() {
    // EXIT CRITERION 3: Awaits result_rx.await to get Result<Vec<RecordBatch>>
    let (result_tx, result_rx) = mpsc::channel(10);
    let cancel_token = CancellationToken::new();
    let consumer = Consumer::new(result_rx, cancel_token);

    // Send handle first, then result after delay
    let (handle, tx) = create_chunk_handle(0);
    result_tx.send(handle).await.unwrap();

    // Spawn task to send result after delay
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        let batches = vec![create_test_batch(500, "id")];
        tx.send(Ok(batches)).unwrap();
    });

    // Consumer should block until result is available
    let start = tokio::time::Instant::now();
    let batch = consumer.next_batch().await.unwrap().unwrap();
    let elapsed = start.elapsed();

    assert_eq!(batch.num_rows(), 500);
    assert!(elapsed >= tokio::time::Duration::from_millis(50));
}

#[tokio::test]
async fn test_handles_ok_batches_populates_buffer_returns_first() {
    // EXIT CRITERION 4: Handles Ok(batches): populate batch_buffer, return first batch
    let (result_tx, result_rx) = mpsc::channel(10);
    let cancel_token = CancellationToken::new();
    let consumer = Consumer::new(result_rx, cancel_token);

    // Send a chunk with 4 batches
    tokio::spawn(async move {
        let (handle, tx) = create_chunk_handle(0);
        result_tx.send(handle).await.unwrap();

        let batches = vec![
            create_test_batch(100, "id"),
            create_test_batch(200, "id"),
            create_test_batch(300, "id"),
            create_test_batch(400, "id"),
        ];
        tx.send(Ok(batches)).unwrap();
    });

    // First call should return first batch and populate buffer with remaining 3
    let batch1 = consumer.next_batch().await.unwrap().unwrap();
    assert_eq!(batch1.num_rows(), 100);
    assert_eq!(consumer.batch_buffer.lock().await.len(), 3);

    // Subsequent calls should drain buffer
    let batch2 = consumer.next_batch().await.unwrap().unwrap();
    assert_eq!(batch2.num_rows(), 200);

    let batch3 = consumer.next_batch().await.unwrap().unwrap();
    assert_eq!(batch3.num_rows(), 300);

    let batch4 = consumer.next_batch().await.unwrap().unwrap();
    assert_eq!(batch4.num_rows(), 400);

    // Buffer should be empty
    assert_eq!(consumer.batch_buffer.lock().await.len(), 0);
}

#[tokio::test]
async fn test_handles_error_propagates_correctly() {
    // EXIT CRITERION 5: Handles Err(e): propagate error
    let (result_tx, result_rx) = mpsc::channel(10);
    let cancel_token = CancellationToken::new();
    let consumer = Consumer::new(result_rx, cancel_token);

    // Send error from worker
    tokio::spawn(async move {
        let (handle, tx) = create_chunk_handle(0);
        result_tx.send(handle).await.unwrap();

        let error = DatabricksErrorHelper::io()
            .message("chunk download failed: connection reset")
            .context("worker retry exhausted");
        tx.send(Err(error)).unwrap();
    });

    // Consumer should propagate the error
    let result = consumer.next_batch().await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("connection reset") || err_msg.contains("chunk download failed"),
        "Error message: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_handles_channel_closed_returns_ok_none() {
    // EXIT CRITERION 6: Handles channel closed (None): return Ok(None)
    let (result_tx, result_rx) = mpsc::channel(10);
    let cancel_token = CancellationToken::new();
    let consumer = Consumer::new(result_rx, cancel_token);

    // Close channel by dropping sender
    drop(result_tx);

    // Consumer should return Ok(None) for end-of-stream
    let result = consumer.next_batch().await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[tokio::test]
async fn test_captures_schema_from_first_batch() {
    // EXIT CRITERION 7: Captures schema from first batch
    let (result_tx, result_rx) = mpsc::channel(10);
    let cancel_token = CancellationToken::new();
    let consumer = Consumer::new(result_rx, cancel_token);

    // Schema should be None initially
    assert!(consumer.schema().is_none());

    // Send first chunk
    tokio::spawn(async move {
        let (handle, tx) = create_chunk_handle(0);
        result_tx.send(handle).await.unwrap();

        let batches = vec![create_test_batch(100, "user_id")];
        tx.send(Ok(batches)).unwrap();
    });

    // After first batch, schema should be captured
    let batch = consumer.next_batch().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 100);

    let schema = consumer.schema().unwrap();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "user_id");
}

#[tokio::test]
async fn test_handles_cancellation_via_cancellation_token() {
    // EXIT CRITERION 8: Handles cancellation via CancellationToken
    let (_result_tx, result_rx) = mpsc::channel(10);
    let cancel_token = CancellationToken::new();
    let consumer = Consumer::new(result_rx, cancel_token.clone());

    // Cancel before calling next_batch
    cancel_token.cancel();

    // Should return error immediately
    let result = consumer.next_batch().await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("cancelled") || err_msg.contains("Cancelled"),
        "Error message: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_cancellation_during_recv() {
    // Test cancellation while waiting for ChunkHandle
    let (_result_tx, result_rx) = mpsc::channel(10);
    let cancel_token = CancellationToken::new();
    let consumer = Consumer::new(result_rx, cancel_token.clone());

    // Spawn task to cancel after delay
    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        cancel_clone.cancel();
    });

    // Should be cancelled during recv
    let result = consumer.next_batch().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_cancellation_during_result_rx_await() {
    // Test cancellation while waiting for download result
    let (result_tx, result_rx) = mpsc::channel(10);
    let cancel_token = CancellationToken::new();
    let consumer = Consumer::new(result_rx, cancel_token.clone());

    // Send handle but don't send result yet
    let (handle, _tx) = create_chunk_handle(0);
    result_tx.send(handle).await.unwrap();

    // Spawn task to cancel after delay
    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        cancel_clone.cancel();
    });

    // Should be cancelled during result_rx.await
    let result = consumer.next_batch().await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("cancelled") || err_msg.contains("Cancelled"));
}

#[tokio::test]
async fn test_empty_batches_skipped() {
    // Test that empty batches are handled gracefully
    let (result_tx, result_rx) = mpsc::channel(10);
    let cancel_token = CancellationToken::new();
    let consumer = Consumer::new(result_rx, cancel_token);

    // Send chunk with empty batches, then a valid chunk
    tokio::spawn(async move {
        // First chunk: empty batches
        let (handle1, tx1) = create_chunk_handle(0);
        result_tx.send(handle1).await.unwrap();
        tx1.send(Ok(vec![])).unwrap();

        // Second chunk: valid batch
        let (handle2, tx2) = create_chunk_handle(1);
        result_tx.send(handle2).await.unwrap();
        let batches = vec![create_test_batch(100, "id")];
        tx2.send(Ok(batches)).unwrap();
    });

    // Should skip empty chunk and return valid batch
    let batch = consumer.next_batch().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 100);
}

#[tokio::test]
async fn test_worker_drop_without_sending() {
    // Test that worker dropping result_tx without sending is handled
    let (result_tx, result_rx) = mpsc::channel(10);
    let cancel_token = CancellationToken::new();
    let consumer = Consumer::new(result_rx, cancel_token);

    // Send handle but drop tx without sending
    tokio::spawn(async move {
        let (handle, tx) = create_chunk_handle(0);
        result_tx.send(handle).await.unwrap();

        // Drop tx without sending - simulates worker panic
        drop(tx);
    });

    // Should receive an error
    let result = consumer.next_batch().await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("dropped") || err_msg.contains("without sending"),
        "Error message: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_sequential_chunk_consumption() {
    // Test that multiple chunks are consumed in order
    let (result_tx, result_rx) = mpsc::channel(10);
    let cancel_token = CancellationToken::new();
    let consumer = Consumer::new(result_rx, cancel_token);

    // Send 5 chunks, each with 2 batches
    tokio::spawn(async move {
        for i in 0..5 {
            let (handle, tx) = create_chunk_handle(i);
            result_tx.send(handle).await.unwrap();

            let batches = vec![
                create_test_batch(100 * (i as usize + 1), "id"),
                create_test_batch(100 * (i as usize + 1) + 50, "id"),
            ];
            tx.send(Ok(batches)).unwrap();
        }
    });

    // Consume all 10 batches (5 chunks * 2 batches)
    let mut batch_count = 0;
    let expected_rows = vec![100, 150, 200, 250, 300, 350, 400, 450, 500, 550];

    for expected in expected_rows.iter() {
        let batch = consumer.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), *expected);
        batch_count += 1;
    }

    assert_eq!(batch_count, 10);
}

#[tokio::test]
async fn test_concurrent_cancellation_and_send() {
    // Test race condition: cancellation while chunk is being sent
    let (result_tx, result_rx) = mpsc::channel(10);
    let cancel_token = CancellationToken::new();
    let consumer = Consumer::new(result_rx, cancel_token.clone());

    // Spawn sender task
    let sender_handle = tokio::spawn(async move {
        for i in 0..100 {
            let (handle, tx) = create_chunk_handle(i);
            if result_tx.send(handle).await.is_err() {
                break;
            }
            let batches = vec![create_test_batch(100, "id")];
            let _ = tx.send(Ok(batches));
        }
    });

    // Spawn cancellation task
    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        cancel_clone.cancel();
    });

    // Consume until cancelled
    loop {
        match consumer.next_batch().await {
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(_) => break, // Cancelled
        }
    }

    sender_handle.await.unwrap();
}

#[tokio::test]
async fn test_schema_persists_across_batches() {
    // Test that schema captured from first batch persists
    let (result_tx, result_rx) = mpsc::channel(10);
    let cancel_token = CancellationToken::new();
    let consumer = Consumer::new(result_rx, cancel_token);

    // Send two chunks with different data but same schema
    tokio::spawn(async move {
        let (handle1, tx1) = create_chunk_handle(0);
        result_tx.send(handle1).await.unwrap();
        let batches1 = vec![create_test_batch(100, "column_a")];
        tx1.send(Ok(batches1)).unwrap();

        let (handle2, tx2) = create_chunk_handle(1);
        result_tx.send(handle2).await.unwrap();
        let batches2 = vec![create_test_batch(200, "column_a")];
        tx2.send(Ok(batches2)).unwrap();
    });

    // Get first batch
    let _batch1 = consumer.next_batch().await.unwrap().unwrap();
    let schema1 = consumer.schema().unwrap();

    // Get second batch
    let _batch2 = consumer.next_batch().await.unwrap().unwrap();
    let schema2 = consumer.schema().unwrap();

    // Schema should be the same (Arc pointer equality)
    assert!(Arc::ptr_eq(&schema1, &schema2));
}
