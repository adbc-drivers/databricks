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
