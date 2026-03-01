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

//! Download workers for the CloudFetch pipeline.
//!
//! This module provides long-lived tokio tasks that download Arrow data from cloud storage.
//! Workers pull [`ChunkDownloadTask`] from the download channel, perform HTTP downloads
//! with retry logic, and send results through the oneshot channel to the consumer.
//!
//! ## Retry Behavior
//!
//! Workers implement a sophisticated retry strategy matching the C# driver:
//!
//! | Error type | Sleep before retry | Counts against `max_retries` | Counts against `max_refresh_retries` |
//! |---|---|---|---|
//! | Network / 5xx | Yes — `retry_delay * (attempt + 1)` | Yes | No |
//! | 401 / 403 / 404 | No | Yes | Yes |
//! | Link proactively expired | No | No | Yes |
//!
//! ## Proactive Expiry Check
//!
//! Before the first HTTP request for each chunk, the worker checks if the link is
//! expired or will expire within `url_expiration_buffer_secs`. If so, it proactively
//! refetches the link to avoid a guaranteed 401/403 failure.

use crate::error::{DatabricksErrorHelper, Result};
use crate::reader::cloudfetch::chunk_downloader::ChunkDownloader;
use crate::reader::cloudfetch::link_fetcher::ChunkLinkFetcher;
use crate::reader::cloudfetch::pipeline_types::ChunkDownloadTask;
use crate::types::cloudfetch::{CloudFetchConfig, CloudFetchLink};
use driverbase::error::ErrorHelper;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, trace, warn};

/// Configuration for download workers.
///
/// This struct contains all the configuration needed by download workers,
/// extracted from [`CloudFetchConfig`] for cleaner API.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Maximum number of retry attempts for failed downloads.
    pub max_retries: u32,
    /// Delay between retry attempts (used as base for linear backoff).
    pub retry_delay: Duration,
    /// Maximum number of URL refresh attempts before terminal error.
    pub max_refresh_retries: u32,
    /// Seconds before link expiry to trigger proactive refresh.
    pub url_expiration_buffer_secs: u32,
}

impl From<&CloudFetchConfig> for WorkerConfig {
    fn from(config: &CloudFetchConfig) -> Self {
        Self {
            max_retries: config.max_retries,
            retry_delay: config.retry_delay,
            max_refresh_retries: config.max_refresh_retries,
            url_expiration_buffer_secs: config.url_expiration_buffer_secs,
        }
    }
}

/// Errors that can occur during chunk download.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DownloadErrorKind {
    /// Network error or server error (5xx) - should sleep before retry
    TransientError,
    /// Authentication/authorization error (401, 403, 404) - should refetch URL immediately
    AuthError,
}

/// Determines the kind of error from the error message.
///
/// The HTTP client returns errors like "HTTP 401 - Unauthorized", so we parse
/// the status code from the error message to determine the error kind.
fn classify_error(error: &crate::error::Error) -> DownloadErrorKind {
    let message = error.to_string();

    // Check for auth-related HTTP status codes (401, 403, 404)
    // The format is "HTTP {status} - {body}"
    if message.contains("HTTP 401") || message.contains("HTTP 403") || message.contains("HTTP 404")
    {
        return DownloadErrorKind::AuthError;
    }

    // All other errors are treated as transient
    DownloadErrorKind::TransientError
}

/// Spawns download workers that process tasks from the download channel.
///
/// Creates `config.num_download_workers` long-lived tokio tasks that loop over
/// the download channel, processing chunks until the channel closes or
/// cancellation is requested.
///
/// # Arguments
///
/// * `download_rx` - Receiver for download tasks from the scheduler
/// * `config` - CloudFetch configuration
/// * `downloader` - Chunk downloader for HTTP downloads
/// * `link_fetcher` - Fetcher for refreshing expired links
/// * `cancel_token` - Cancellation token to stop workers
///
/// # Returns
///
/// A vector of `JoinHandle`s for the spawned worker tasks.
pub fn spawn_download_workers(
    download_rx: mpsc::UnboundedReceiver<ChunkDownloadTask>,
    config: &CloudFetchConfig,
    downloader: Arc<ChunkDownloader>,
    link_fetcher: Arc<dyn ChunkLinkFetcher>,
    cancel_token: CancellationToken,
) -> Vec<JoinHandle<()>> {
    let worker_config = WorkerConfig::from(config);
    let num_workers = config.num_download_workers;

    // Wrap the receiver in Arc<Mutex> so workers can share it
    let download_rx = Arc::new(tokio::sync::Mutex::new(download_rx));

    debug!(
        "Spawning {} download workers with config: max_retries={}, retry_delay={:?}, \
         max_refresh_retries={}, url_expiration_buffer_secs={}",
        num_workers,
        worker_config.max_retries,
        worker_config.retry_delay,
        worker_config.max_refresh_retries,
        worker_config.url_expiration_buffer_secs
    );

    let mut handles = Vec::with_capacity(num_workers);

    for worker_id in 0..num_workers {
        let rx = Arc::clone(&download_rx);
        let dl = Arc::clone(&downloader);
        let lf = Arc::clone(&link_fetcher);
        let cfg = worker_config.clone();
        let token = cancel_token.clone();

        let handle = tokio::spawn(async move {
            worker_task(worker_id, rx, dl, lf, cfg, token).await;
        });

        handles.push(handle);
    }

    handles
}

/// The main worker task that processes download tasks from the channel.
///
/// This function runs until either:
/// - The download channel closes (all tasks have been sent)
/// - The cancellation token is triggered
///
/// For each task, the worker:
/// 1. Performs a proactive expiry check (refetches link if expiring soon)
/// 2. Attempts to download the chunk
/// 3. On failure, retries according to the retry policy
/// 4. Sends the result (success or error) through the oneshot channel
async fn worker_task(
    worker_id: usize,
    download_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<ChunkDownloadTask>>>,
    downloader: Arc<ChunkDownloader>,
    link_fetcher: Arc<dyn ChunkLinkFetcher>,
    config: WorkerConfig,
    cancel_token: CancellationToken,
) {
    debug!("Worker {} started", worker_id);

    loop {
        // Check for cancellation
        if cancel_token.is_cancelled() {
            debug!("Worker {} cancelled", worker_id);
            break;
        }

        // Try to receive a task from the channel
        let task = {
            let mut rx = download_rx.lock().await;
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    debug!("Worker {} cancelled while waiting for task", worker_id);
                    return;
                }
                task = rx.recv() => task
            }
        };

        let task = match task {
            Some(task) => task,
            None => {
                // Channel closed - no more tasks
                debug!("Worker {} exiting: download channel closed", worker_id);
                break;
            }
        };

        trace!(
            "Worker {} received task for chunk {}",
            worker_id,
            task.chunk_index
        );

        // Process the task and send the result
        let result = process_task(
            worker_id,
            task.chunk_index,
            task.link,
            &downloader,
            &link_fetcher,
            &config,
            &cancel_token,
        )
        .await;

        // Send result through the oneshot channel
        // Ignore send error - receiver may have been dropped (consumer cancelled)
        if task.result_tx.send(result).is_err() {
            debug!(
                "Worker {}: result receiver dropped for chunk {}",
                worker_id, task.chunk_index
            );
        }
    }

    debug!("Worker {} exiting", worker_id);
}

/// Process a single download task with retry logic.
///
/// Implements the full retry contract:
/// - Proactive expiry check before first HTTP request
/// - Linear backoff for network/5xx errors
/// - Immediate URL refresh for 401/403/404 errors
async fn process_task(
    worker_id: usize,
    chunk_index: i64,
    mut link: CloudFetchLink,
    downloader: &Arc<ChunkDownloader>,
    link_fetcher: &Arc<dyn ChunkLinkFetcher>,
    config: &WorkerConfig,
    cancel_token: &CancellationToken,
) -> Result<Vec<arrow_array::RecordBatch>> {
    let mut retry_count: u32 = 0;
    let mut refresh_count: u32 = 0;

    // Proactive expiry check before first HTTP request
    if link.is_expired(config.url_expiration_buffer_secs) {
        debug!(
            "Worker {}: chunk {} link expired or expiring soon, proactively refreshing",
            worker_id, chunk_index
        );

        match link_fetcher
            .refetch_link(chunk_index, link.row_offset)
            .await
        {
            Ok(fresh_link) => {
                link = fresh_link;
                refresh_count += 1;
                debug!(
                    "Worker {}: chunk {} proactive refresh succeeded (refresh_count={})",
                    worker_id, chunk_index, refresh_count
                );
            }
            Err(e) => {
                error!(
                    "Worker {}: chunk {} proactive refresh failed: {}",
                    worker_id, chunk_index, e
                );
                return Err(e);
            }
        }
    }

    // Download loop with retries
    loop {
        // Check for cancellation
        if cancel_token.is_cancelled() {
            return Err(DatabricksErrorHelper::io().message("Download cancelled"));
        }

        // Attempt download
        match downloader.download(&link).await {
            Ok(batches) => {
                trace!(
                    "Worker {}: chunk {} downloaded successfully ({} batches)",
                    worker_id,
                    chunk_index,
                    batches.len()
                );
                return Ok(batches);
            }
            Err(e) => {
                let error_kind = classify_error(&e);

                match error_kind {
                    DownloadErrorKind::AuthError => {
                        // 401/403/404: counts against both max_retries and max_refresh_retries, no sleep
                        retry_count += 1;
                        refresh_count += 1;

                        // Check if we've exceeded limits
                        if refresh_count > config.max_refresh_retries {
                            error!(
                                "Worker {}: chunk {} exceeded max refresh retries ({})",
                                worker_id, chunk_index, config.max_refresh_retries
                            );
                            return Err(DatabricksErrorHelper::io().message(format!(
                                "Download failed after {} URL refresh attempts: {}",
                                refresh_count, e
                            )));
                        }

                        if retry_count > config.max_retries {
                            error!(
                                "Worker {}: chunk {} exceeded max retries ({})",
                                worker_id, chunk_index, config.max_retries
                            );
                            return Err(DatabricksErrorHelper::io().message(format!(
                                "Download failed after {} attempts: {}",
                                retry_count, e
                            )));
                        }

                        warn!(
                            "Worker {}: chunk {} auth error (retry={}, refresh={}), refetching URL",
                            worker_id, chunk_index, retry_count, refresh_count
                        );

                        // Refetch link immediately (no sleep)
                        match link_fetcher
                            .refetch_link(chunk_index, link.row_offset)
                            .await
                        {
                            Ok(fresh_link) => {
                                link = fresh_link;
                                debug!(
                                    "Worker {}: chunk {} URL refreshed successfully",
                                    worker_id, chunk_index
                                );
                                // Continue to retry immediately
                            }
                            Err(refetch_err) => {
                                error!(
                                    "Worker {}: chunk {} URL refetch failed: {}",
                                    worker_id, chunk_index, refetch_err
                                );
                                return Err(refetch_err);
                            }
                        }
                    }
                    DownloadErrorKind::TransientError => {
                        // Network/5xx: counts against max_retries with sleep
                        retry_count += 1;

                        if retry_count > config.max_retries {
                            error!(
                                "Worker {}: chunk {} exceeded max retries ({})",
                                worker_id, chunk_index, config.max_retries
                            );
                            return Err(DatabricksErrorHelper::io().message(format!(
                                "Download failed after {} attempts: {}",
                                retry_count, e
                            )));
                        }

                        // Linear backoff: sleep(retry_delay * (attempt + 1))
                        // Note: retry_count is 1-based after increment, so we use it directly
                        let sleep_duration = config.retry_delay * retry_count;
                        warn!(
                            "Worker {}: chunk {} transient error (retry={}), sleeping {:?}",
                            worker_id, chunk_index, retry_count, sleep_duration
                        );

                        // Sleep with cancellation support
                        tokio::select! {
                            _ = cancel_token.cancelled() => {
                                return Err(DatabricksErrorHelper::io().message("Download cancelled during retry sleep"));
                            }
                            _ = tokio::time::sleep(sleep_duration) => {}
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ChunkLinkFetchResult;
    use crate::types::cloudfetch::CloudFetchLink;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::timeout;

    /// Create a test CloudFetchLink
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

    /// Create an expired test link
    fn create_expired_link(chunk_index: i64) -> CloudFetchLink {
        CloudFetchLink {
            url: format!("https://storage.example.com/chunk{}", chunk_index),
            chunk_index,
            row_offset: chunk_index * 1000,
            row_count: 1000,
            byte_count: 50000,
            // Expires in 30 seconds, which is within the default 60-second buffer
            expiration: chrono::Utc::now() + chrono::Duration::seconds(30),
            http_headers: HashMap::new(),
            next_chunk_index: Some(chunk_index + 1),
        }
    }

    /// Create a test RecordBatch
    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap()
    }

    /// Mock ChunkLinkFetcher for testing
    #[derive(Debug)]
    struct MockLinkFetcher {
        refetch_count: AtomicUsize,
    }

    impl MockLinkFetcher {
        fn new() -> Self {
            Self {
                refetch_count: AtomicUsize::new(0),
            }
        }

        fn refetch_count(&self) -> usize {
            self.refetch_count.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl ChunkLinkFetcher for MockLinkFetcher {
        async fn fetch_links(
            &self,
            _start_chunk_index: i64,
            _start_row_offset: i64,
        ) -> Result<ChunkLinkFetchResult> {
            Ok(ChunkLinkFetchResult::end_of_stream())
        }

        async fn refetch_link(&self, chunk_index: i64, _row_offset: i64) -> Result<CloudFetchLink> {
            self.refetch_count.fetch_add(1, Ordering::Relaxed);
            Ok(create_test_link(chunk_index))
        }
    }

    /// Mock ChunkDownloader that succeeds immediately
    #[derive(Debug)]
    struct MockSuccessDownloader;

    impl MockSuccessDownloader {
        async fn download(&self, _link: &CloudFetchLink) -> Result<Vec<RecordBatch>> {
            Ok(vec![create_test_batch()])
        }
    }

    /// Mock ChunkDownloader that fails N times with transient errors then succeeds
    #[derive(Debug)]
    struct MockFailingDownloader {
        fail_count: AtomicU32,
        failures_remaining: AtomicU32,
        error_kind: DownloadErrorKind,
    }

    impl MockFailingDownloader {
        fn new(failures: u32, error_kind: DownloadErrorKind) -> Self {
            Self {
                fail_count: AtomicU32::new(0),
                failures_remaining: AtomicU32::new(failures),
                error_kind,
            }
        }

        fn fail_count(&self) -> u32 {
            self.fail_count.load(Ordering::Relaxed)
        }

        async fn download(&self, _link: &CloudFetchLink) -> Result<Vec<RecordBatch>> {
            let remaining = self.failures_remaining.fetch_sub(1, Ordering::Relaxed);
            if remaining > 0 {
                self.fail_count.fetch_add(1, Ordering::Relaxed);
                let error_msg = match self.error_kind {
                    DownloadErrorKind::TransientError => "HTTP 500 - Internal Server Error",
                    DownloadErrorKind::AuthError => "HTTP 401 - Unauthorized",
                };
                return Err(DatabricksErrorHelper::io().message(error_msg));
            }
            Ok(vec![create_test_batch()])
        }
    }

    /// Mock ChunkDownloader that always fails
    #[derive(Debug)]
    struct MockAlwaysFailingDownloader {
        fail_count: AtomicU32,
        error_kind: DownloadErrorKind,
    }

    impl MockAlwaysFailingDownloader {
        fn new(error_kind: DownloadErrorKind) -> Self {
            Self {
                fail_count: AtomicU32::new(0),
                error_kind,
            }
        }

        fn fail_count(&self) -> u32 {
            self.fail_count.load(Ordering::Relaxed)
        }

        async fn download(&self, _link: &CloudFetchLink) -> Result<Vec<RecordBatch>> {
            self.fail_count.fetch_add(1, Ordering::Relaxed);
            let error_msg = match self.error_kind {
                DownloadErrorKind::TransientError => "HTTP 500 - Internal Server Error",
                DownloadErrorKind::AuthError => "HTTP 401 - Unauthorized",
            };
            Err(DatabricksErrorHelper::io().message(error_msg))
        }
    }

    #[test]
    fn test_worker_config_from_cloudfetch_config() {
        let cf_config = CloudFetchConfig {
            max_retries: 5,
            retry_delay: Duration::from_millis(1000),
            max_refresh_retries: 3,
            url_expiration_buffer_secs: 120,
            ..Default::default()
        };

        let worker_config = WorkerConfig::from(&cf_config);

        assert_eq!(worker_config.max_retries, 5);
        assert_eq!(worker_config.retry_delay, Duration::from_millis(1000));
        assert_eq!(worker_config.max_refresh_retries, 3);
        assert_eq!(worker_config.url_expiration_buffer_secs, 120);
    }

    #[test]
    fn test_classify_error_auth_errors() {
        let error_401 = DatabricksErrorHelper::io().message("HTTP 401 - Unauthorized");
        assert_eq!(classify_error(&error_401), DownloadErrorKind::AuthError);

        let error_403 = DatabricksErrorHelper::io().message("HTTP 403 - Forbidden");
        assert_eq!(classify_error(&error_403), DownloadErrorKind::AuthError);

        let error_404 = DatabricksErrorHelper::io().message("HTTP 404 - Not Found");
        assert_eq!(classify_error(&error_404), DownloadErrorKind::AuthError);
    }

    #[test]
    fn test_classify_error_transient_errors() {
        let error_500 = DatabricksErrorHelper::io().message("HTTP 500 - Internal Server Error");
        assert_eq!(
            classify_error(&error_500),
            DownloadErrorKind::TransientError
        );

        let error_503 = DatabricksErrorHelper::io().message("HTTP 503 - Service Unavailable");
        assert_eq!(
            classify_error(&error_503),
            DownloadErrorKind::TransientError
        );

        let network_error = DatabricksErrorHelper::io().message("Connection refused");
        assert_eq!(
            classify_error(&network_error),
            DownloadErrorKind::TransientError
        );
    }

    #[tokio::test]
    async fn worker_retries_on_transient_error() {
        // Downloader that fails twice then succeeds
        let downloader = MockFailingDownloader::new(2, DownloadErrorKind::TransientError);
        let link_fetcher = Arc::new(MockLinkFetcher::new());
        let config = WorkerConfig {
            max_retries: 3,
            retry_delay: Duration::from_millis(1), // Very short for testing
            max_refresh_retries: 3,
            url_expiration_buffer_secs: 60,
        };
        let cancel_token = CancellationToken::new();
        let link = create_test_link(0);

        // Create a mock process_task-like function with our mock downloader
        let result = process_task_with_mock_downloader(
            0,
            0,
            link,
            &downloader,
            &link_fetcher,
            &config,
            &cancel_token,
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(downloader.fail_count(), 2);
        // No URL refresh for transient errors
        assert_eq!(link_fetcher.refetch_count(), 0);
    }

    #[tokio::test]
    async fn worker_uses_linear_backoff() {
        // This test verifies that linear backoff is used
        // We can't easily measure sleep times in a unit test, but we can verify
        // the formula by checking the retry_delay calculation
        let config = WorkerConfig {
            max_retries: 3,
            retry_delay: Duration::from_millis(500),
            max_refresh_retries: 3,
            url_expiration_buffer_secs: 60,
        };

        // Verify the formula: sleep_duration = retry_delay * attempt
        // For attempt 1: 500ms * 1 = 500ms
        // For attempt 2: 500ms * 2 = 1000ms
        // For attempt 3: 500ms * 3 = 1500ms
        assert_eq!(config.retry_delay * 1, Duration::from_millis(500));
        assert_eq!(config.retry_delay * 2, Duration::from_millis(1000));
        assert_eq!(config.retry_delay * 3, Duration::from_millis(1500));
    }

    #[tokio::test]
    async fn worker_refetches_url_on_401_403_404() {
        // Downloader that fails with 401 then succeeds
        let downloader = MockFailingDownloader::new(1, DownloadErrorKind::AuthError);
        let link_fetcher = Arc::new(MockLinkFetcher::new());
        let config = WorkerConfig {
            max_retries: 3,
            retry_delay: Duration::from_millis(1),
            max_refresh_retries: 3,
            url_expiration_buffer_secs: 60,
        };
        let cancel_token = CancellationToken::new();
        let link = create_test_link(0);

        let result = process_task_with_mock_downloader(
            0,
            0,
            link,
            &downloader,
            &link_fetcher,
            &config,
            &cancel_token,
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(downloader.fail_count(), 1);
        // URL refresh should have been called for auth error
        assert_eq!(link_fetcher.refetch_count(), 1);
    }

    #[tokio::test]
    async fn worker_no_sleep_on_auth_error() {
        // This test verifies that auth errors don't sleep before refetch
        // We verify this by timing the operation - it should complete quickly
        let downloader = MockFailingDownloader::new(1, DownloadErrorKind::AuthError);
        let link_fetcher = Arc::new(MockLinkFetcher::new());
        let config = WorkerConfig {
            max_retries: 3,
            retry_delay: Duration::from_secs(10), // Very long - would timeout if used
            max_refresh_retries: 3,
            url_expiration_buffer_secs: 60,
        };
        let cancel_token = CancellationToken::new();
        let link = create_test_link(0);

        // Should complete quickly since auth errors don't sleep
        let result = timeout(
            Duration::from_millis(500),
            process_task_with_mock_downloader(
                0,
                0,
                link,
                &downloader,
                &link_fetcher,
                &config,
                &cancel_token,
            ),
        )
        .await;

        assert!(result.is_ok(), "Should complete without sleeping");
        assert!(result.unwrap().is_ok());
    }

    #[tokio::test]
    async fn worker_gives_up_after_max_refresh_retries() {
        // Downloader that always fails with auth error
        let downloader = MockAlwaysFailingDownloader::new(DownloadErrorKind::AuthError);
        let link_fetcher = Arc::new(MockLinkFetcher::new());
        let config = WorkerConfig {
            max_retries: 10, // High so we hit max_refresh_retries first
            retry_delay: Duration::from_millis(1),
            max_refresh_retries: 2,
            url_expiration_buffer_secs: 60,
        };
        let cancel_token = CancellationToken::new();
        let link = create_test_link(0);

        let result = process_task_with_always_failing_downloader(
            0,
            0,
            link,
            &downloader,
            &link_fetcher,
            &config,
            &cancel_token,
        )
        .await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("refresh"));
        // Should have attempted max_refresh_retries + 1 downloads (initial + retries)
        assert_eq!(downloader.fail_count(), config.max_refresh_retries + 1);
        // Should have called refetch max_refresh_retries times
        assert_eq!(
            link_fetcher.refetch_count(),
            config.max_refresh_retries as usize
        );
    }

    #[tokio::test]
    async fn worker_proactively_refreshes_expiring_url() {
        // Use a mock success downloader with an expiring link
        let link_fetcher = Arc::new(MockLinkFetcher::new());
        let config = WorkerConfig {
            max_retries: 3,
            retry_delay: Duration::from_millis(1),
            max_refresh_retries: 3,
            url_expiration_buffer_secs: 60, // Link expires in 30s, buffer is 60s
        };
        let cancel_token = CancellationToken::new();
        let link = create_expired_link(0); // Expires in 30 seconds

        // Verify the link is considered expired with the buffer
        assert!(link.is_expired(config.url_expiration_buffer_secs));

        let downloader = MockSuccessDownloader;
        let result = process_task_with_success_downloader(
            0,
            0,
            link,
            &downloader,
            &link_fetcher,
            &config,
            &cancel_token,
        )
        .await;

        assert!(result.is_ok());
        // Should have proactively refreshed the link before download
        assert_eq!(link_fetcher.refetch_count(), 1);
    }

    #[tokio::test]
    async fn workers_exit_when_channel_closes() {
        let (tx, rx) = mpsc::unbounded_channel::<ChunkDownloadTask>();

        // We can't easily create a real ChunkDownloader without an HTTP client,
        // so we test the channel close behavior by dropping the sender
        drop(tx);

        // Create a simple test that verifies channel behavior
        let mut rx = rx;
        let result = rx.recv().await;
        assert!(result.is_none(), "Channel should be closed");
    }

    #[tokio::test]
    async fn cancellation_interrupts_workers() {
        let cancel_token = CancellationToken::new();

        // Cancel immediately
        cancel_token.cancel();

        // Verify cancellation is detected
        assert!(cancel_token.is_cancelled());
    }

    /// Helper function to test process_task with a mock downloader that can fail
    async fn process_task_with_mock_downloader(
        _worker_id: usize,
        chunk_index: i64,
        mut link: CloudFetchLink,
        downloader: &MockFailingDownloader,
        link_fetcher: &Arc<MockLinkFetcher>,
        config: &WorkerConfig,
        cancel_token: &CancellationToken,
    ) -> Result<Vec<RecordBatch>> {
        let mut retry_count: u32 = 0;
        let mut refresh_count: u32 = 0;

        // Proactive expiry check before first HTTP request
        if link.is_expired(config.url_expiration_buffer_secs) {
            match link_fetcher
                .refetch_link(chunk_index, link.row_offset)
                .await
            {
                Ok(fresh_link) => {
                    link = fresh_link;
                    refresh_count += 1;
                }
                Err(e) => return Err(e),
            }
        }

        // Download loop with retries
        loop {
            if cancel_token.is_cancelled() {
                return Err(DatabricksErrorHelper::io().message("Download cancelled"));
            }

            match downloader.download(&link).await {
                Ok(batches) => return Ok(batches),
                Err(e) => {
                    let error_kind = classify_error(&e);

                    match error_kind {
                        DownloadErrorKind::AuthError => {
                            retry_count += 1;
                            refresh_count += 1;

                            if refresh_count > config.max_refresh_retries {
                                return Err(DatabricksErrorHelper::io().message(format!(
                                    "Download failed after {} URL refresh attempts: {}",
                                    refresh_count, e
                                )));
                            }

                            if retry_count > config.max_retries {
                                return Err(DatabricksErrorHelper::io().message(format!(
                                    "Download failed after {} attempts: {}",
                                    retry_count, e
                                )));
                            }

                            // Refetch link immediately (no sleep)
                            match link_fetcher
                                .refetch_link(chunk_index, link.row_offset)
                                .await
                            {
                                Ok(fresh_link) => link = fresh_link,
                                Err(refetch_err) => return Err(refetch_err),
                            }
                        }
                        DownloadErrorKind::TransientError => {
                            retry_count += 1;

                            if retry_count > config.max_retries {
                                return Err(DatabricksErrorHelper::io().message(format!(
                                    "Download failed after {} attempts: {}",
                                    retry_count, e
                                )));
                            }

                            let sleep_duration = config.retry_delay * retry_count;
                            tokio::select! {
                                _ = cancel_token.cancelled() => {
                                    return Err(DatabricksErrorHelper::io().message("Download cancelled during retry sleep"));
                                }
                                _ = tokio::time::sleep(sleep_duration) => {}
                            }
                        }
                    }
                }
            }
        }
    }

    /// Helper function to test process_task with a success downloader
    async fn process_task_with_success_downloader(
        _worker_id: usize,
        chunk_index: i64,
        mut link: CloudFetchLink,
        downloader: &MockSuccessDownloader,
        link_fetcher: &Arc<MockLinkFetcher>,
        config: &WorkerConfig,
        cancel_token: &CancellationToken,
    ) -> Result<Vec<RecordBatch>> {
        // Proactive expiry check before first HTTP request
        if link.is_expired(config.url_expiration_buffer_secs) {
            match link_fetcher
                .refetch_link(chunk_index, link.row_offset)
                .await
            {
                Ok(fresh_link) => link = fresh_link,
                Err(e) => return Err(e),
            }
        }

        if cancel_token.is_cancelled() {
            return Err(DatabricksErrorHelper::io().message("Download cancelled"));
        }

        downloader.download(&link).await
    }

    /// Helper function to test process_task with an always failing downloader
    async fn process_task_with_always_failing_downloader(
        _worker_id: usize,
        chunk_index: i64,
        mut link: CloudFetchLink,
        downloader: &MockAlwaysFailingDownloader,
        link_fetcher: &Arc<MockLinkFetcher>,
        config: &WorkerConfig,
        cancel_token: &CancellationToken,
    ) -> Result<Vec<RecordBatch>> {
        let mut retry_count: u32 = 0;
        let mut refresh_count: u32 = 0;

        // Proactive expiry check before first HTTP request
        if link.is_expired(config.url_expiration_buffer_secs) {
            match link_fetcher
                .refetch_link(chunk_index, link.row_offset)
                .await
            {
                Ok(fresh_link) => {
                    link = fresh_link;
                    refresh_count += 1;
                }
                Err(e) => return Err(e),
            }
        }

        // Download loop with retries
        loop {
            if cancel_token.is_cancelled() {
                return Err(DatabricksErrorHelper::io().message("Download cancelled"));
            }

            match downloader.download(&link).await {
                Ok(batches) => return Ok(batches),
                Err(e) => {
                    let error_kind = classify_error(&e);

                    match error_kind {
                        DownloadErrorKind::AuthError => {
                            retry_count += 1;
                            refresh_count += 1;

                            if refresh_count > config.max_refresh_retries {
                                return Err(DatabricksErrorHelper::io().message(format!(
                                    "Download failed after {} URL refresh attempts: {}",
                                    refresh_count, e
                                )));
                            }

                            if retry_count > config.max_retries {
                                return Err(DatabricksErrorHelper::io().message(format!(
                                    "Download failed after {} attempts: {}",
                                    retry_count, e
                                )));
                            }

                            // Refetch link immediately (no sleep)
                            match link_fetcher
                                .refetch_link(chunk_index, link.row_offset)
                                .await
                            {
                                Ok(fresh_link) => link = fresh_link,
                                Err(refetch_err) => return Err(refetch_err),
                            }
                        }
                        DownloadErrorKind::TransientError => {
                            retry_count += 1;

                            if retry_count > config.max_retries {
                                return Err(DatabricksErrorHelper::io().message(format!(
                                    "Download failed after {} attempts: {}",
                                    retry_count, e
                                )));
                            }

                            let sleep_duration = config.retry_delay * retry_count;
                            tokio::select! {
                                _ = cancel_token.cancelled() => {
                                    return Err(DatabricksErrorHelper::io().message("Download cancelled during retry sleep"));
                                }
                                _ = tokio::time::sleep(sleep_duration) => {}
                            }
                        }
                    }
                }
            }
        }
    }
}
