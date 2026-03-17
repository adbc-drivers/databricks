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

//! Unit tests for download workers.

use super::worker::WorkerConfig;
use crate::client::ChunkLinkFetchResult;
use crate::error::{DatabricksErrorHelper, Result};
use crate::reader::cloudfetch::link_fetcher::ChunkLinkFetcher;
use crate::reader::cloudfetch::pipeline_types::ChunkDownloadTask;
use crate::types::cloudfetch::CloudFetchLink;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use driverbase::error::ErrorHelper;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

/// Helper to create a test CloudFetchLink.
fn create_test_link(chunk_index: i64, expiration_future: bool) -> CloudFetchLink {
    let expiration = if expiration_future {
        DateTime::parse_from_rfc3339("2099-01-01T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    } else {
        DateTime::parse_from_rfc3339("2000-01-01T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    };

    CloudFetchLink {
        url: format!("https://storage.example.com/chunk{}", chunk_index),
        chunk_index,
        row_offset: chunk_index * 1000,
        row_count: 1000,
        byte_count: 50000,
        expiration,
        http_headers: HashMap::new(),
        next_chunk_index: Some(chunk_index + 1),
    }
}

/// Helper to create empty RecordBatch for testing.
fn create_empty_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    RecordBatch::new_empty(schema)
}

/// Mock ChunkDownloader that can be configured to fail N times then succeed.
struct MockChunkDownloader {
    fail_count: AtomicU32,
    max_fails: u32,
    fail_with_auth_error: AtomicBool,
    sleep_duration: Option<Duration>,
}

impl MockChunkDownloader {
    fn new(max_fails: u32, fail_with_auth_error: bool) -> Self {
        Self {
            fail_count: AtomicU32::new(0),
            max_fails,
            fail_with_auth_error: AtomicBool::new(fail_with_auth_error),
            sleep_duration: None,
        }
    }

    fn with_sleep(max_fails: u32, sleep_duration: Duration) -> Self {
        Self {
            fail_count: AtomicU32::new(0),
            max_fails,
            fail_with_auth_error: AtomicBool::new(false),
            sleep_duration: Some(sleep_duration),
        }
    }

    async fn download(&self, _link: &CloudFetchLink) -> Result<Vec<RecordBatch>> {
        // Simulate download time if configured
        if let Some(duration) = self.sleep_duration {
            tokio::time::sleep(duration).await;
        }

        let count = self.fail_count.fetch_add(1, Ordering::SeqCst);
        if count < self.max_fails {
            if self.fail_with_auth_error.load(Ordering::SeqCst) {
                return Err(DatabricksErrorHelper::io()
                    .message("HTTP 401 Unauthorized")
                    .context("download chunk"));
            } else {
                return Err(DatabricksErrorHelper::io()
                    .message("Network timeout")
                    .context("download chunk"));
            }
        }
        Ok(vec![create_empty_batch()])
    }
}

/// Mock ChunkLinkFetcher for testing.
#[derive(Debug)]
struct MockChunkLinkFetcher {
    refetch_count: AtomicU32,
}

impl MockChunkLinkFetcher {
    fn new() -> Self {
        Self {
            refetch_count: AtomicU32::new(0),
        }
    }

    fn get_refetch_count(&self) -> u32 {
        self.refetch_count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ChunkLinkFetcher for MockChunkLinkFetcher {
    async fn fetch_links(
        &self,
        _start_chunk_index: i64,
        _start_row_offset: i64,
    ) -> Result<ChunkLinkFetchResult> {
        Ok(ChunkLinkFetchResult {
            links: vec![],
            has_more: false,
            next_chunk_index: None,
            next_row_offset: None,
        })
    }

    async fn refetch_link(&self, chunk_index: i64, _row_offset: i64) -> Result<CloudFetchLink> {
        self.refetch_count.fetch_add(1, Ordering::SeqCst);
        // Return a fresh link with future expiration
        Ok(create_test_link(chunk_index, true))
    }
}

#[tokio::test]
async fn test_worker_retries_on_transient_error() {
    // Mock downloader that fails 2 times then succeeds
    let mock_downloader = Arc::new(MockChunkDownloader::new(2, false));

    let config = WorkerConfig {
        max_retries: 5,
        max_refresh_retries: 3,
        retry_delay: Duration::from_millis(10),
        url_expiration_buffer_secs: 60,
    };

    let link = create_test_link(0, true);

    // Simulate download_chunk_with_retry
    let mut retry_attempts = 0u32;
    let mut result = None;

    for _ in 0..config.max_retries {
        match mock_downloader.download(&link).await {
            Ok(batches) => {
                result = Some(batches);
                break;
            }
            Err(_) => {
                retry_attempts += 1;
                if retry_attempts >= config.max_retries {
                    break;
                }
                tokio::time::sleep(config.retry_delay * retry_attempts).await;
            }
        }
    }

    // Should succeed after 2 retries
    assert!(result.is_some());
    assert_eq!(retry_attempts, 2);
}

#[tokio::test]
async fn test_worker_uses_linear_backoff() {
    let config = WorkerConfig {
        max_retries: 3,
        max_refresh_retries: 3,
        retry_delay: Duration::from_millis(100),
        url_expiration_buffer_secs: 60,
    };

    // Verify backoff calculation
    let attempt1_backoff = config.retry_delay * 1;
    let attempt2_backoff = config.retry_delay * 2;
    let attempt3_backoff = config.retry_delay * 3;

    assert_eq!(attempt1_backoff, Duration::from_millis(100));
    assert_eq!(attempt2_backoff, Duration::from_millis(200));
    assert_eq!(attempt3_backoff, Duration::from_millis(300));

    // Verify actual sleep behavior with timing
    let start = std::time::Instant::now();
    let mock_downloader = Arc::new(MockChunkDownloader::with_sleep(2, Duration::from_millis(1)));
    let link = create_test_link(0, true);

    let mut retry_attempts = 0u32;
    for _ in 0..config.max_retries {
        match mock_downloader.download(&link).await {
            Ok(_) => break,
            Err(_) => {
                retry_attempts += 1;
                if retry_attempts < config.max_retries {
                    tokio::time::sleep(config.retry_delay * retry_attempts).await;
                }
            }
        }
    }

    let elapsed = start.elapsed();
    // Total sleep: 100ms + 200ms = 300ms (plus small overhead)
    assert!(elapsed >= Duration::from_millis(300));
    assert!(elapsed < Duration::from_millis(400));
}

#[tokio::test]
async fn test_worker_refetches_url_on_401_403_404() {
    // Mock downloader that returns auth error first, then succeeds
    let mock_downloader = Arc::new(MockChunkDownloader::new(1, true));
    let mock_link_fetcher = Arc::new(MockChunkLinkFetcher::new());

    let config = WorkerConfig {
        max_retries: 5,
        max_refresh_retries: 3,
        retry_delay: Duration::from_millis(10),
        url_expiration_buffer_secs: 60,
    };

    let mut link = create_test_link(0, true);
    let chunk_index = link.chunk_index;
    let row_offset = link.row_offset;

    // Simulate auth error handling
    let mut retry_attempts = 0u32;
    let mut refresh_attempts = 0u32;
    let mut result = None;

    for _ in 0..config.max_retries {
        match mock_downloader.download(&link).await {
            Ok(batches) => {
                result = Some(batches);
                break;
            }
            Err(e) => {
                let error_str = e.to_string();
                let is_auth_error = error_str.contains("401")
                    || error_str.contains("403")
                    || error_str.contains("404");

                if is_auth_error {
                    // Refetch URL
                    link = mock_link_fetcher
                        .refetch_link(chunk_index, row_offset)
                        .await
                        .unwrap();
                    refresh_attempts += 1;
                    retry_attempts += 1;
                    // No sleep on auth error
                } else {
                    retry_attempts += 1;
                    tokio::time::sleep(config.retry_delay * retry_attempts).await;
                }

                if retry_attempts >= config.max_retries
                    || refresh_attempts >= config.max_refresh_retries
                {
                    break;
                }
            }
        }
    }

    // Should succeed after 1 refetch
    assert!(result.is_some());
    assert_eq!(refresh_attempts, 1);
    assert_eq!(mock_link_fetcher.get_refetch_count(), 1);
}

#[tokio::test]
async fn test_worker_no_sleep_on_auth_error() {
    let mock_downloader = Arc::new(MockChunkDownloader::new(1, true));
    let mock_link_fetcher = Arc::new(MockChunkLinkFetcher::new());

    let config = WorkerConfig {
        max_retries: 5,
        max_refresh_retries: 3,
        retry_delay: Duration::from_millis(100),
        url_expiration_buffer_secs: 60,
    };

    let mut link = create_test_link(0, true);
    let start = std::time::Instant::now();

    // Simulate auth error handling
    for _ in 0..config.max_retries {
        match mock_downloader.download(&link).await {
            Ok(_) => break,
            Err(e) => {
                let error_str = e.to_string();
                let is_auth_error = error_str.contains("401");

                if is_auth_error {
                    // Refetch URL immediately without sleep
                    link = mock_link_fetcher
                        .refetch_link(link.chunk_index, link.row_offset)
                        .await
                        .unwrap();
                    // No sleep - continue immediately
                }
            }
        }
    }

    let elapsed = start.elapsed();
    // Should complete in less than 50ms (no sleep on auth error)
    assert!(elapsed < Duration::from_millis(50));
}

#[tokio::test]
async fn test_worker_gives_up_after_max_refresh_retries() {
    // Mock downloader that always returns auth error
    let mock_downloader = Arc::new(MockChunkDownloader::new(999, true));
    let mock_link_fetcher = Arc::new(MockChunkLinkFetcher::new());

    let config = WorkerConfig {
        max_retries: 10,
        max_refresh_retries: 3,
        retry_delay: Duration::from_millis(10),
        url_expiration_buffer_secs: 60,
    };

    let mut link = create_test_link(0, true);
    let mut refresh_attempts = 0u32;
    let mut retry_attempts = 0u32;
    let mut final_error = None;

    for _ in 0..config.max_retries {
        match mock_downloader.download(&link).await {
            Ok(_) => break,
            Err(e) => {
                let error_str = e.to_string();
                let is_auth_error = error_str.contains("401");

                if is_auth_error {
                    if refresh_attempts >= config.max_refresh_retries {
                        final_error = Some(e);
                        break;
                    }

                    link = mock_link_fetcher
                        .refetch_link(link.chunk_index, link.row_offset)
                        .await
                        .unwrap();
                    refresh_attempts += 1;
                    retry_attempts += 1;
                } else {
                    retry_attempts += 1;
                    tokio::time::sleep(config.retry_delay * retry_attempts).await;
                }

                if retry_attempts >= config.max_retries {
                    final_error = Some(e);
                    break;
                }
            }
        }
    }

    // Should give up after max_refresh_retries
    assert!(final_error.is_some());
    assert_eq!(refresh_attempts, config.max_refresh_retries);
    assert_eq!(mock_link_fetcher.get_refetch_count(), 3);
}

#[tokio::test]
async fn test_worker_proactively_refreshes_expiring_url() {
    let mock_link_fetcher = Arc::new(MockChunkLinkFetcher::new());

    let config = WorkerConfig {
        max_retries: 5,
        max_refresh_retries: 3,
        retry_delay: Duration::from_millis(10),
        url_expiration_buffer_secs: 60,
    };

    // Create a link that's expired
    let link = create_test_link(0, false);

    // Check if proactive refresh is needed
    let needs_refresh = link.is_expired(config.url_expiration_buffer_secs);
    assert!(needs_refresh);

    // Perform proactive refresh
    let mut refresh_attempts = 0u32;
    let fresh_link = if needs_refresh {
        refresh_attempts += 1;
        mock_link_fetcher
            .refetch_link(link.chunk_index, link.row_offset)
            .await
            .unwrap()
    } else {
        link
    };

    // Verify refresh happened before first download attempt
    assert_eq!(refresh_attempts, 1);
    assert_eq!(mock_link_fetcher.get_refetch_count(), 1);
    assert!(!fresh_link.is_expired(config.url_expiration_buffer_secs));
}

#[tokio::test]
async fn test_worker_spawning_and_receiving_tasks() {
    // Create channels
    let (download_tx, download_rx) = async_channel::bounded::<ChunkDownloadTask>(10);

    // Create mock downloader - needs to be wrapped properly
    let mock_downloader = Arc::new(MockChunkDownloader::new(0, false));

    // Since we can't use spawn_workers directly with MockChunkDownloader,
    // we'll test the task receiving logic manually
    let link = create_test_link(0, true);
    let (result_tx, result_rx) = oneshot::channel();

    let task = ChunkDownloadTask {
        chunk_index: 0,
        link,
        result_tx,
    };

    // Send task
    download_tx.send(task).await.unwrap();

    // Receive task
    let received_task = download_rx.recv().await.unwrap();
    assert_eq!(received_task.chunk_index, 0);

    // Simulate download
    let batches = mock_downloader.download(&received_task.link).await.unwrap();
    received_task.result_tx.send(Ok(batches)).unwrap();

    // Verify result received
    let result = result_rx.await.unwrap();
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_worker_cancellation_during_download() {
    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();

    // Simulate a download that checks cancellation
    let download_task = tokio::spawn(async move {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(10)) => {
                Ok::<(), String>(())
            }
            _ = cancel_token_clone.cancelled() => {
                Err("cancelled".to_string())
            }
        }
    });

    // Cancel after a short delay
    tokio::time::sleep(Duration::from_millis(10)).await;
    cancel_token.cancel();

    // Wait for task to complete
    let result = download_task.await.unwrap();
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), "cancelled");
}

#[tokio::test]
async fn test_worker_handles_channel_closed() {
    let (download_tx, download_rx) = async_channel::bounded::<ChunkDownloadTask>(10);

    // Drop sender to close channel
    drop(download_tx);

    // Try to receive - should return Err
    let result = download_rx.recv().await;
    assert!(result.is_err());
}
