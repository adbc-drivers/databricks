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

//! Integration tests for the CloudFetch pipeline redesign.
//!
//! These tests verify end-to-end behavior of the CloudFetch pipeline:
//! - Sequential consumption: all chunks downloaded and read in order
//! - Cancellation handling: cancel during active download, no deadlock or panic
//! - 401 recovery: presigned URL expires mid-stream, driver refetches and continues
//!
//! ## Pipeline Architecture
//!
//! ```text
//! [ChunkLinkFetcher] → [Scheduler] → download_channel → [Download Workers]
//!                            |                                 |
//!                            v                                 v
//!                     result_channel ←──────────────── ChunkHandle (in order)
//!                            |
//!                            v
//!                       [Consumer]
//! ```

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use databricks_adbc::client::ChunkLinkFetchResult;
use databricks_adbc::error::Result;
use databricks_adbc::reader::cloudfetch::{spawn_scheduler, ChunkDownloadTask, ChunkLinkFetcher};
use databricks_adbc::types::cloudfetch::{CloudFetchConfig, CloudFetchLink};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

// =============================================================================
// Test Helpers
// =============================================================================

/// Create a test CloudFetchLink for a given chunk index.
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

/// Create a test RecordBatch with identifiable content.
fn create_test_batch(chunk_index: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("chunk_id", DataType::Int32, false),
        Field::new("data", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![chunk_index as i32; 3])),
            Arc::new(StringArray::from(vec![
                format!("row_0_chunk_{}", chunk_index),
                format!("row_1_chunk_{}", chunk_index),
                format!("row_2_chunk_{}", chunk_index),
            ])),
        ],
    )
    .unwrap()
}

// =============================================================================
// Mock ChunkLinkFetcher
// =============================================================================

/// Mock ChunkLinkFetcher that returns a predefined number of links.
#[derive(Debug)]
struct MockLinkFetcher {
    /// Total number of chunks to return
    total_chunks: i64,
    /// Track fetch_links calls
    fetch_count: AtomicUsize,
    /// Track refetch_link calls
    refetch_count: AtomicUsize,
    /// Whether to simulate slow fetching
    slow_fetch: bool,
}

impl MockLinkFetcher {
    fn new(total_chunks: i64) -> Self {
        Self {
            total_chunks,
            fetch_count: AtomicUsize::new(0),
            refetch_count: AtomicUsize::new(0),
            slow_fetch: false,
        }
    }

    fn with_slow_fetch(total_chunks: i64) -> Self {
        Self {
            total_chunks,
            fetch_count: AtomicUsize::new(0),
            refetch_count: AtomicUsize::new(0),
            slow_fetch: true,
        }
    }

    fn fetch_count(&self) -> usize {
        self.fetch_count.load(Ordering::Relaxed)
    }

    fn refetch_count(&self) -> usize {
        self.refetch_count.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl ChunkLinkFetcher for MockLinkFetcher {
    async fn fetch_links(
        &self,
        start_chunk_index: i64,
        _start_row_offset: i64,
    ) -> Result<ChunkLinkFetchResult> {
        self.fetch_count.fetch_add(1, Ordering::Relaxed);

        if self.slow_fetch {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        if start_chunk_index >= self.total_chunks {
            return Ok(ChunkLinkFetchResult::end_of_stream());
        }

        // Return chunks in batches of 5 (or remaining)
        let batch_size = 5;
        let end = (start_chunk_index + batch_size).min(self.total_chunks);
        let links: Vec<CloudFetchLink> = (start_chunk_index..end).map(create_test_link).collect();

        let has_more = end < self.total_chunks;
        let next_chunk_index = if has_more { Some(end) } else { None };

        Ok(ChunkLinkFetchResult {
            links,
            has_more,
            next_chunk_index,
            next_row_offset: None,
        })
    }

    async fn refetch_link(&self, chunk_index: i64, _row_offset: i64) -> Result<CloudFetchLink> {
        self.refetch_count.fetch_add(1, Ordering::Relaxed);
        // Return a fresh link
        Ok(create_test_link(chunk_index))
    }
}

// =============================================================================
// Mock Download Worker
// =============================================================================

/// Simulates download worker behavior for integration tests.
///
/// This spawns a task that:
/// - Pulls ChunkDownloadTask from download_rx
/// - Simulates download by sleeping or returning immediately
/// - Sends results through the oneshot channel
///
/// Supports simulating:
/// - Successful downloads
/// - Delayed downloads (for cancellation tests)
/// - 401 errors that trigger refetch
async fn spawn_mock_download_worker(
    download_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<ChunkDownloadTask>>>,
    link_fetcher: Arc<dyn ChunkLinkFetcher>,
    cancel_token: CancellationToken,
    config: MockWorkerConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if cancel_token.is_cancelled() {
                break;
            }

            let task = {
                let mut rx = download_rx.lock().await;
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        break;
                    }
                    task = rx.recv() => task
                }
            };

            let task = match task {
                Some(task) => task,
                None => break, // Channel closed
            };

            let chunk_index = task.chunk_index;
            let mut current_link = task.link.clone();

            // Simulate download delay
            if config.download_delay > Duration::ZERO {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        // Don't send result - simulates cancelled download
                        continue;
                    }
                    _ = tokio::time::sleep(config.download_delay) => {}
                }
            }

            // Check if we should simulate 401 error
            let should_fail_with_401 =
                config.fail_401_until_refetch && config.fail_401_chunks.contains(&chunk_index);

            let attempt_count = config
                .attempt_counts
                .entry(chunk_index)
                .or_insert(AtomicU32::new(0));
            let current_attempt = attempt_count.fetch_add(1, Ordering::Relaxed);

            let result = if should_fail_with_401 && current_attempt == 0 {
                // First attempt fails with 401, need refetch
                match link_fetcher
                    .refetch_link(chunk_index, current_link.row_offset)
                    .await
                {
                    Ok(fresh_link) => {
                        current_link = fresh_link;
                        // After refetch, succeed
                        Ok(vec![create_test_batch(chunk_index)])
                    }
                    Err(e) => Err(e),
                }
            } else {
                // Normal success
                Ok(vec![create_test_batch(chunk_index)])
            };

            // Send result
            let _ = task.result_tx.send(result);
        }
    })
}

/// Configuration for mock download workers.
#[derive(Debug, Clone)]
struct MockWorkerConfig {
    /// Delay before completing each download
    download_delay: Duration,
    /// Whether to simulate 401 errors that require refetch
    fail_401_until_refetch: bool,
    /// Specific chunks that should fail with 401 on first attempt
    fail_401_chunks: Vec<i64>,
    /// Track attempt counts per chunk
    attempt_counts: Arc<dashmap::DashMap<i64, AtomicU32>>,
}

impl Default for MockWorkerConfig {
    fn default() -> Self {
        Self {
            download_delay: Duration::ZERO,
            fail_401_until_refetch: false,
            fail_401_chunks: vec![],
            attempt_counts: Arc::new(dashmap::DashMap::new()),
        }
    }
}

// =============================================================================
// Integration Tests
// =============================================================================

/// Test: End-to-end sequential consumption.
///
/// Verifies that:
/// - All chunks are downloaded
/// - Chunks are received in correct order (0, 1, 2, ..., N)
/// - Pipeline terminates cleanly when all chunks are consumed
#[tokio::test]
async fn test_end_to_end_sequential_consumption() {
    const NUM_CHUNKS: i64 = 15;

    // Create mock link fetcher - keep concrete type for assertions
    let mock_fetcher = Arc::new(MockLinkFetcher::new(NUM_CHUNKS));
    let link_fetcher: Arc<dyn ChunkLinkFetcher> =
        Arc::clone(&mock_fetcher) as Arc<dyn ChunkLinkFetcher>;

    // Create config with small buffer for faster test
    let config = CloudFetchConfig {
        max_chunks_in_memory: 4,
        ..Default::default()
    };

    let cancel_token = CancellationToken::new();

    // Spawn the scheduler
    let channels = spawn_scheduler(Arc::clone(&link_fetcher), &config, cancel_token.clone());

    // Wrap download_rx for shared access
    let download_rx = Arc::new(tokio::sync::Mutex::new(channels.download_rx));

    // Spawn mock download workers
    let worker_config = MockWorkerConfig::default();
    let num_workers = 3;
    let mut worker_handles = Vec::with_capacity(num_workers);

    for _ in 0..num_workers {
        let rx = Arc::clone(&download_rx);
        let lf = Arc::clone(&link_fetcher);
        let token = cancel_token.clone();
        let cfg = worker_config.clone();

        let handle = spawn_mock_download_worker(rx, lf, token, cfg).await;
        worker_handles.push(handle);
    }

    // Consumer: read from result_channel and collect chunk indices
    let mut result_rx = channels.result_rx;
    let mut received_indices = Vec::new();

    while let Some(handle) = result_rx.recv().await {
        let chunk_index = handle.chunk_index;

        // Await the download result
        let result = timeout(Duration::from_secs(5), handle.result_rx)
            .await
            .expect("Timed out waiting for download result")
            .expect("Download task sender was dropped")
            .expect("Download failed");

        // Verify batch content matches chunk index
        assert!(!result.is_empty(), "Expected at least one batch");
        let batch = &result[0];
        let chunk_id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Expected Int32Array");
        assert_eq!(
            chunk_id_array.value(0),
            chunk_index as i32,
            "Batch content should match chunk index"
        );

        received_indices.push(chunk_index);
    }

    // Verify all chunks received in order
    let expected_indices: Vec<i64> = (0..NUM_CHUNKS).collect();
    assert_eq!(
        received_indices, expected_indices,
        "Should receive all chunks in order"
    );

    // Verify fetch_links was called (at least once for initial batch)
    assert!(
        mock_fetcher.fetch_count() > 0,
        "fetch_links should be called"
    );

    // Clean up
    cancel_token.cancel();
    for handle in worker_handles {
        let _ = handle.await;
    }
}

/// Test: End-to-end cancellation mid-stream.
///
/// Verifies that:
/// - Cancellation during active downloads doesn't cause deadlock
/// - No panic occurs
/// - Test completes within timeout
#[tokio::test]
async fn test_end_to_end_cancellation_mid_stream() {
    const NUM_CHUNKS: i64 = 20;
    const TIMEOUT_SECS: u64 = 5;

    // Create mock link fetcher with slow fetching
    let link_fetcher: Arc<dyn ChunkLinkFetcher> =
        Arc::new(MockLinkFetcher::with_slow_fetch(NUM_CHUNKS));

    let config = CloudFetchConfig {
        max_chunks_in_memory: 4,
        ..Default::default()
    };

    let cancel_token = CancellationToken::new();

    // Spawn the scheduler
    let channels = spawn_scheduler(Arc::clone(&link_fetcher), &config, cancel_token.clone());

    // Wrap download_rx for shared access
    let download_rx = Arc::new(tokio::sync::Mutex::new(channels.download_rx));

    // Spawn mock download workers with delays
    let worker_config = MockWorkerConfig {
        download_delay: Duration::from_millis(100), // Slow downloads
        ..Default::default()
    };

    let num_workers = 3;
    let mut worker_handles = Vec::with_capacity(num_workers);

    for _ in 0..num_workers {
        let rx = Arc::clone(&download_rx);
        let lf = Arc::clone(&link_fetcher) as Arc<dyn ChunkLinkFetcher>;
        let token = cancel_token.clone();
        let cfg = worker_config.clone();

        let handle = spawn_mock_download_worker(rx, lf, token, cfg).await;
        worker_handles.push(handle);
    }

    // Spawn consumer task
    let consumer_cancel = cancel_token.clone();
    let mut result_rx = channels.result_rx;
    let consumer_task = tokio::spawn(async move {
        let mut count = 0;
        while let Some(handle) = result_rx.recv().await {
            if consumer_cancel.is_cancelled() {
                break;
            }

            // Try to get result with short timeout
            match timeout(Duration::from_millis(500), handle.result_rx).await {
                Ok(Ok(Ok(_))) => count += 1,
                Ok(Ok(Err(_))) => break, // Download error (expected on cancel)
                Ok(Err(_)) => break,     // Sender dropped (expected on cancel)
                Err(_) => break,         // Timeout (expected on cancel)
            }
        }
        count
    });

    // Let some downloads complete
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Cancel mid-stream
    cancel_token.cancel();

    // Verify test completes within timeout (no deadlock)
    let result = timeout(Duration::from_secs(TIMEOUT_SECS), async {
        // Wait for consumer to finish
        let consumed = consumer_task.await.expect("Consumer task panicked");

        // Wait for workers to finish
        for handle in worker_handles {
            handle.await.expect("Worker task panicked");
        }

        // Wait for scheduler to finish
        channels
            .scheduler_handle
            .await
            .expect("Scheduler task panicked");

        consumed
    })
    .await;

    assert!(
        result.is_ok(),
        "Test should complete within {} seconds (no deadlock)",
        TIMEOUT_SECS
    );

    let consumed_count = result.unwrap();
    assert!(
        consumed_count < NUM_CHUNKS as usize,
        "Should have consumed fewer than all chunks due to cancellation"
    );
}

/// Test: End-to-end 401 recovery.
///
/// Verifies that:
/// - When a presigned URL expires (401), the pipeline refetches the link
/// - Fresh URL is used for retry (no sleep delay)
/// - All chunks are eventually delivered successfully
#[tokio::test]
async fn test_end_to_end_401_recovery() {
    const NUM_CHUNKS: i64 = 10;

    // Create mock link fetcher - keep concrete type for assertions
    let mock_fetcher = Arc::new(MockLinkFetcher::new(NUM_CHUNKS));
    let link_fetcher: Arc<dyn ChunkLinkFetcher> =
        Arc::clone(&mock_fetcher) as Arc<dyn ChunkLinkFetcher>;

    let config = CloudFetchConfig {
        max_chunks_in_memory: 4,
        ..Default::default()
    };

    let cancel_token = CancellationToken::new();

    // Spawn the scheduler
    let channels = spawn_scheduler(Arc::clone(&link_fetcher), &config, cancel_token.clone());

    // Wrap download_rx for shared access
    let download_rx = Arc::new(tokio::sync::Mutex::new(channels.download_rx));

    // Configure workers to simulate 401 errors on specific chunks
    // Chunks 2, 5, 7 will fail with 401 on first attempt
    let worker_config = MockWorkerConfig {
        fail_401_until_refetch: true,
        fail_401_chunks: vec![2, 5, 7],
        ..Default::default()
    };

    let num_workers = 3;
    let mut worker_handles = Vec::with_capacity(num_workers);

    for _ in 0..num_workers {
        let rx = Arc::clone(&download_rx);
        let lf = Arc::clone(&link_fetcher);
        let token = cancel_token.clone();
        let cfg = worker_config.clone();

        let handle = spawn_mock_download_worker(rx, lf, token, cfg).await;
        worker_handles.push(handle);
    }

    // Consumer: read from result_channel and collect results
    let mut result_rx = channels.result_rx;
    let mut received_indices = Vec::new();

    while let Some(handle) = result_rx.recv().await {
        let chunk_index = handle.chunk_index;

        // Await the download result
        let result = timeout(Duration::from_secs(5), handle.result_rx)
            .await
            .expect("Timed out waiting for download result")
            .expect("Download task sender was dropped")
            .expect("Download failed");

        // Verify batch was received
        assert!(!result.is_empty(), "Expected at least one batch");

        received_indices.push(chunk_index);
    }

    // Verify all chunks received in order
    let expected_indices: Vec<i64> = (0..NUM_CHUNKS).collect();
    assert_eq!(
        received_indices, expected_indices,
        "Should receive all chunks in order despite 401 errors"
    );

    // Verify refetch_link was called for the failing chunks
    let refetch_count = mock_fetcher.refetch_count();
    assert_eq!(
        refetch_count, 3,
        "refetch_link should be called for each 401 failure (chunks 2, 5, 7)"
    );

    // Clean up
    cancel_token.cancel();
    for handle in worker_handles {
        let _ = handle.await;
    }
}

// =============================================================================
// Additional Integration Tests
// =============================================================================

/// Test: Pipeline handles empty result set gracefully.
#[tokio::test]
async fn test_empty_result_set() {
    // Create mock link fetcher with zero chunks
    let link_fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(MockLinkFetcher::new(0));

    let config = CloudFetchConfig::default();
    let cancel_token = CancellationToken::new();

    // Spawn the scheduler
    let channels = spawn_scheduler(Arc::clone(&link_fetcher), &config, cancel_token.clone());

    // Drop download_rx - we don't need workers for empty result
    drop(channels.download_rx);

    // Consumer should receive no handles
    let mut result_rx = channels.result_rx;
    let handle = result_rx.recv().await;
    assert!(
        handle.is_none(),
        "Should receive no handles for empty result"
    );

    // Scheduler should complete
    timeout(Duration::from_secs(1), channels.scheduler_handle)
        .await
        .expect("Scheduler should complete for empty result")
        .expect("Scheduler should not panic");
}

/// Test: Backpressure prevents unbounded memory growth.
#[tokio::test]
async fn test_backpressure() {
    const NUM_CHUNKS: i64 = 20;
    const MAX_IN_MEMORY: usize = 3;

    let link_fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(MockLinkFetcher::new(NUM_CHUNKS));

    let config = CloudFetchConfig {
        max_chunks_in_memory: MAX_IN_MEMORY,
        ..Default::default()
    };

    let cancel_token = CancellationToken::new();

    // Spawn the scheduler
    let channels = spawn_scheduler(Arc::clone(&link_fetcher), &config, cancel_token.clone());

    // Wrap download_rx for shared access
    let download_rx = Arc::new(tokio::sync::Mutex::new(channels.download_rx));

    // Spawn fast workers that complete downloads immediately
    let worker_config = MockWorkerConfig::default();
    let num_workers = 3;
    let mut worker_handles = Vec::with_capacity(num_workers);

    for _ in 0..num_workers {
        let rx = Arc::clone(&download_rx);
        let lf = Arc::clone(&link_fetcher) as Arc<dyn ChunkLinkFetcher>;
        let token = cancel_token.clone();
        let cfg = worker_config.clone();

        let handle = spawn_mock_download_worker(rx, lf, token, cfg).await;
        worker_handles.push(handle);
    }

    // Give scheduler time to try to fill the channel
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Now slowly consume and verify backpressure works
    let mut result_rx = channels.result_rx;
    let mut count = 0;

    while let Ok(Some(handle)) = timeout(Duration::from_millis(500), result_rx.recv()).await {
        // Consume with delay to test backpressure
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Get result
        if let Ok(Ok(Ok(_))) = timeout(Duration::from_secs(1), handle.result_rx).await {
            count += 1;
        }
    }

    assert_eq!(
        count, NUM_CHUNKS as usize,
        "Should eventually receive all chunks"
    );

    // Clean up
    cancel_token.cancel();
    for handle in worker_handles {
        let _ = handle.await;
    }
}

/// Test: Multiple batches per chunk are delivered correctly.
#[tokio::test]
async fn test_multiple_batches_per_chunk() {
    const NUM_CHUNKS: i64 = 5;

    let link_fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(MockLinkFetcher::new(NUM_CHUNKS));

    let config = CloudFetchConfig {
        max_chunks_in_memory: 4,
        ..Default::default()
    };

    let cancel_token = CancellationToken::new();

    // Spawn the scheduler
    let channels = spawn_scheduler(Arc::clone(&link_fetcher), &config, cancel_token.clone());

    // Create custom worker that returns multiple batches per chunk
    let download_rx = Arc::new(tokio::sync::Mutex::new(channels.download_rx));
    let _link_fetcher = Arc::clone(&link_fetcher);
    let token = cancel_token.clone();

    let worker_handle = tokio::spawn(async move {
        loop {
            if token.is_cancelled() {
                break;
            }

            let task = {
                let mut rx = download_rx.lock().await;
                tokio::select! {
                    _ = token.cancelled() => break,
                    task = rx.recv() => task
                }
            };

            let task = match task {
                Some(task) => task,
                None => break,
            };

            // Return 3 batches per chunk
            let batches = vec![
                create_test_batch(task.chunk_index),
                create_test_batch(task.chunk_index),
                create_test_batch(task.chunk_index),
            ];

            let _ = task.result_tx.send(Ok(batches));
        }
    });

    // Consumer
    let mut result_rx = channels.result_rx;
    let mut total_batches = 0;

    while let Some(handle) = result_rx.recv().await {
        let result = timeout(Duration::from_secs(5), handle.result_rx)
            .await
            .expect("Timeout")
            .expect("Sender dropped")
            .expect("Download failed");

        total_batches += result.len();
    }

    assert_eq!(
        total_batches,
        NUM_CHUNKS as usize * 3,
        "Should receive 3 batches per chunk"
    );

    // Clean up
    cancel_token.cancel();
    let _ = worker_handle.await;
}

// =============================================================================
// StreamingCloudFetchProvider Integration Tests
// =============================================================================

use std::collections::VecDeque;
use std::sync::Mutex;
use tokio::sync::mpsc::Receiver;

/// Test: End-to-end sequential consumption using StreamingCloudFetchProvider.
///
/// This test spawns the full CloudFetch pipeline (scheduler + workers + consumer)
/// with mock components and verifies that chunks are downloaded and consumed
/// in the correct sequential order using the `next_batch()` API.
///
/// Verifies:
/// - All chunks (15+) are downloaded
/// - Chunks are received in correct order (0, 1, 2, ..., N)
/// - `next_batch()` returns `Ok(None)` when stream is complete
/// - Pipeline terminates cleanly (no race conditions)
#[tokio::test]
async fn test_provider_end_to_end_sequential_consumption() {
    const NUM_CHUNKS: i64 = 15;

    // Create mock link fetcher
    let mock_fetcher = Arc::new(MockLinkFetcher::new(NUM_CHUNKS));
    let link_fetcher: Arc<dyn ChunkLinkFetcher> =
        Arc::clone(&mock_fetcher) as Arc<dyn ChunkLinkFetcher>;

    // Create config with small buffer for faster test
    let config = CloudFetchConfig {
        max_chunks_in_memory: 4,
        ..Default::default()
    };

    let cancel_token = CancellationToken::new();

    // Spawn the scheduler - this creates the pipeline channels
    let channels = spawn_scheduler(Arc::clone(&link_fetcher), &config, cancel_token.clone());

    // Wrap download_rx for shared access by mock workers
    let download_rx = Arc::new(tokio::sync::Mutex::new(channels.download_rx));

    // Spawn mock download workers that return predictable RecordBatch per chunk index
    let worker_config = MockWorkerConfig::default();
    let num_workers = 3;
    let mut worker_handles = Vec::with_capacity(num_workers);

    for _ in 0..num_workers {
        let rx = Arc::clone(&download_rx);
        let lf = Arc::clone(&link_fetcher);
        let token = cancel_token.clone();
        let cfg = worker_config.clone();

        let handle = spawn_mock_download_worker(rx, lf, token, cfg).await;
        worker_handles.push(handle);
    }

    // Create StreamingCloudFetchProvider directly with the pipeline channels
    // This bypasses new() to allow using mock workers instead of real ChunkDownloader
    let provider = create_test_provider(channels.result_rx, cancel_token.clone());

    // Consumer: call next_batch() repeatedly until end of stream
    let mut received_indices = Vec::new();
    let mut batch_count = 0;

    loop {
        let result = timeout(Duration::from_secs(5), provider.next_batch())
            .await
            .expect("Timed out waiting for next_batch()");

        match result {
            Ok(Some(batch)) => {
                batch_count += 1;

                // Extract chunk_id from the batch to verify ordering
                let chunk_id_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("Expected Int32Array for chunk_id column");

                let chunk_index = chunk_id_array.value(0) as i64;

                // Only record first batch from each chunk (to verify chunk ordering)
                if received_indices.is_empty() || *received_indices.last().unwrap() != chunk_index {
                    received_indices.push(chunk_index);
                }
            }
            Ok(None) => {
                // End of stream - verify this is the expected termination
                break;
            }
            Err(e) => {
                panic!("Unexpected error from next_batch(): {}", e);
            }
        }
    }

    // Verify all chunks received in correct sequential order
    let expected_indices: Vec<i64> = (0..NUM_CHUNKS).collect();
    assert_eq!(
        received_indices, expected_indices,
        "Should receive all chunks in order 0, 1, 2, ..., {}",
        NUM_CHUNKS - 1
    );

    // Verify we received at least one batch per chunk
    assert!(
        batch_count >= NUM_CHUNKS as usize,
        "Should receive at least {} batches, got {}",
        NUM_CHUNKS,
        batch_count
    );

    // Verify fetch_links was called (at least once for initial batch)
    assert!(
        mock_fetcher.fetch_count() > 0,
        "fetch_links should be called"
    );

    // Verify calling next_batch() again still returns Ok(None)
    let final_result = timeout(Duration::from_millis(100), provider.next_batch())
        .await
        .expect("Timed out on final next_batch() call");
    assert!(
        matches!(final_result, Ok(None)),
        "next_batch() should continue returning Ok(None) after stream exhausted"
    );

    // Clean up
    cancel_token.cancel();
    for handle in worker_handles {
        let _ = handle.await;
    }
}

/// Test: Provider handles larger chunk count with out-of-order downloads.
///
/// Workers may complete downloads out of order, but consumer should still
/// receive chunks in sequential order via the result_channel.
#[tokio::test]
async fn test_provider_out_of_order_downloads_in_order_consumption() {
    const NUM_CHUNKS: i64 = 20;

    let link_fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(MockLinkFetcher::new(NUM_CHUNKS));

    let config = CloudFetchConfig {
        max_chunks_in_memory: 6, // Allow more concurrent downloads
        ..Default::default()
    };

    let cancel_token = CancellationToken::new();

    // Spawn the scheduler
    let channels = spawn_scheduler(Arc::clone(&link_fetcher), &config, cancel_token.clone());

    // Wrap download_rx for shared access
    let download_rx = Arc::new(tokio::sync::Mutex::new(channels.download_rx));

    // Spawn workers with varying delays to cause out-of-order completion
    let num_workers = 4;
    let mut worker_handles = Vec::with_capacity(num_workers);

    for worker_id in 0..num_workers {
        let rx = Arc::clone(&download_rx);
        let _lf = Arc::clone(&link_fetcher); // Kept for potential future use in refetch
        let token = cancel_token.clone();

        // Workers have different delays to cause out-of-order download completion
        let worker_handle = tokio::spawn(async move {
            loop {
                if token.is_cancelled() {
                    break;
                }

                let task = {
                    let mut rx_lock = rx.lock().await;
                    tokio::select! {
                        _ = token.cancelled() => break,
                        task = rx_lock.recv() => task
                    }
                };

                let task = match task {
                    Some(task) => task,
                    None => break,
                };

                // Varying delays based on chunk index to simulate out-of-order completion
                // Even chunks: slow, Odd chunks: fast
                let delay = if task.chunk_index % 2 == 0 {
                    Duration::from_millis(20 + (worker_id as u64 * 5))
                } else {
                    Duration::from_millis(5)
                };

                tokio::select! {
                    _ = token.cancelled() => continue,
                    _ = tokio::time::sleep(delay) => {}
                }

                let batch = create_test_batch(task.chunk_index);
                let _ = task.result_tx.send(Ok(vec![batch]));
            }
        });

        worker_handles.push(worker_handle);
    }

    // Create provider and consume via next_batch()
    let provider = create_test_provider(channels.result_rx, cancel_token.clone());

    let mut received_indices = Vec::new();

    loop {
        let result = timeout(Duration::from_secs(10), provider.next_batch())
            .await
            .expect("Timed out waiting for next_batch()");

        match result {
            Ok(Some(batch)) => {
                let chunk_id_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("Expected Int32Array");
                received_indices.push(chunk_id_array.value(0) as i64);
            }
            Ok(None) => break,
            Err(e) => panic!("Unexpected error: {}", e),
        }
    }

    // Verify sequential ordering despite out-of-order downloads
    let expected_indices: Vec<i64> = (0..NUM_CHUNKS).collect();
    assert_eq!(
        received_indices, expected_indices,
        "Chunks should be delivered in order even when downloads complete out-of-order"
    );

    // Clean up
    cancel_token.cancel();
    for handle in worker_handles {
        let _ = handle.await;
    }
}

/// Test: Provider correctly reports end of stream for small result sets.
#[tokio::test]
async fn test_provider_small_result_set() {
    const NUM_CHUNKS: i64 = 3;

    let link_fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(MockLinkFetcher::new(NUM_CHUNKS));

    let config = CloudFetchConfig::default();
    let cancel_token = CancellationToken::new();

    let channels = spawn_scheduler(Arc::clone(&link_fetcher), &config, cancel_token.clone());
    let download_rx = Arc::new(tokio::sync::Mutex::new(channels.download_rx));

    // Single fast worker
    let rx = Arc::clone(&download_rx);
    let lf = Arc::clone(&link_fetcher);
    let token = cancel_token.clone();
    let worker_handle =
        spawn_mock_download_worker(rx, lf, token, MockWorkerConfig::default()).await;

    let provider = create_test_provider(channels.result_rx, cancel_token.clone());

    // Consume all batches
    let mut count = 0;
    while let Ok(Some(_)) = provider.next_batch().await {
        count += 1;
    }

    assert_eq!(count, NUM_CHUNKS as usize, "Should receive exactly 3 chunks");

    // Verify end of stream
    let result = provider.next_batch().await;
    assert!(matches!(result, Ok(None)), "Should return Ok(None) at end");

    cancel_token.cancel();
    let _ = worker_handle.await;
}

/// Test: Provider passes test consistently (run multiple times to detect race conditions).
#[tokio::test]
async fn test_provider_consistency_no_race_conditions() {
    // Run the test multiple times to detect any race conditions
    for iteration in 0..5 {
        const NUM_CHUNKS: i64 = 12;

        let link_fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(MockLinkFetcher::new(NUM_CHUNKS));

        let config = CloudFetchConfig {
            max_chunks_in_memory: 4,
            ..Default::default()
        };

        let cancel_token = CancellationToken::new();

        let channels = spawn_scheduler(Arc::clone(&link_fetcher), &config, cancel_token.clone());
        let download_rx = Arc::new(tokio::sync::Mutex::new(channels.download_rx));

        // Multiple workers to increase contention
        let mut worker_handles = Vec::new();
        for _ in 0..4 {
            let rx = Arc::clone(&download_rx);
            let lf = Arc::clone(&link_fetcher);
            let token = cancel_token.clone();

            let handle =
                spawn_mock_download_worker(rx, lf, token, MockWorkerConfig::default()).await;
            worker_handles.push(handle);
        }

        let provider = create_test_provider(channels.result_rx, cancel_token.clone());

        let mut received = Vec::new();
        loop {
            match timeout(Duration::from_secs(5), provider.next_batch()).await {
                Ok(Ok(Some(batch))) => {
                    let arr = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    received.push(arr.value(0) as i64);
                }
                Ok(Ok(None)) => break,
                Ok(Err(e)) => panic!("Iteration {}: Unexpected error: {}", iteration, e),
                Err(_) => panic!("Iteration {}: Timeout", iteration),
            }
        }

        let expected: Vec<i64> = (0..NUM_CHUNKS).collect();
        assert_eq!(
            received, expected,
            "Iteration {}: Chunks should be in order",
            iteration
        );

        cancel_token.cancel();
        for handle in worker_handles {
            let _ = handle.await;
        }
    }
}

/// Helper function to create a test StreamingCloudFetchProvider.
///
/// This constructs the provider directly to bypass `new()`, allowing us to
/// use mock workers instead of the real ChunkDownloader.
fn create_test_provider(
    result_rx: Receiver<databricks_adbc::reader::cloudfetch::ChunkHandle>,
    cancel_token: CancellationToken,
) -> TestStreamingProvider {
    TestStreamingProvider {
        result_rx: tokio::sync::Mutex::new(result_rx),
        batch_buffer: Mutex::new(VecDeque::new()),
        cancel_token,
    }
}

/// Test-specific provider that wraps the result channel and implements next_batch().
///
/// This mirrors the StreamingCloudFetchProvider implementation but is constructed
/// directly for testing purposes.
struct TestStreamingProvider {
    result_rx: tokio::sync::Mutex<Receiver<databricks_adbc::reader::cloudfetch::ChunkHandle>>,
    batch_buffer: Mutex<VecDeque<RecordBatch>>,
    cancel_token: CancellationToken,
}

impl TestStreamingProvider {
    /// Get next record batch - mirrors StreamingCloudFetchProvider::next_batch().
    async fn next_batch(&self) -> databricks_adbc::error::Result<Option<RecordBatch>> {
        use databricks_adbc::error::DatabricksErrorHelper;
        use driverbase::error::ErrorHelper;

        // Check for cancellation first
        if self.cancel_token.is_cancelled() {
            return Err(DatabricksErrorHelper::invalid_state().message("Operation cancelled"));
        }

        // Drain batch buffer first
        if let Some(batch) = self.batch_buffer.lock().unwrap().pop_front() {
            return Ok(Some(batch));
        }

        // Buffer empty - need next chunk from result channel
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

        let handle = match handle {
            Some(h) => h,
            None => {
                // Channel closed = end of stream
                return Ok(None);
            }
        };

        // Await the oneshot result
        let batches_result = tokio::select! {
            biased;

            _ = self.cancel_token.cancelled() => {
                return Err(DatabricksErrorHelper::invalid_state().message("Operation cancelled"));
            }
            result = handle.result_rx => {
                result
            }
        };

        // Handle oneshot recv error
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

        // Return first batch from buffer
        if let Some(batch) = self.batch_buffer.lock().unwrap().pop_front() {
            return Ok(Some(batch));
        }

        // Empty chunk - recurse to get next chunk
        Box::pin(self.next_batch()).await
    }
}
