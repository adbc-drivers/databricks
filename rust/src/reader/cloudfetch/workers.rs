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

//! Download workers for the channel-based CloudFetch pipeline.
//!
//! Each worker is a long-lived `tokio::spawn` task that loops over the
//! `download_channel`, downloading chunks from cloud storage and sending
//! results back to the consumer via oneshot channels.
//!
//! ## Retry Contract
//!
//! | Error type               | Sleep before retry                    | Counts `max_retries` | Counts `max_refresh_retries` |
//! |--------------------------|---------------------------------------|----------------------|------------------------------|
//! | Network / 5xx            | Yes — `retry_delay * (attempt + 1)`   | Yes                  | No                           |
//! | 401 / 403 / 404          | No                                    | Yes                  | Yes                          |
//! | Proactive link expiry    | No                                    | No                   | Yes                          |

use crate::reader::cloudfetch::chunk_downloader::{ChunkDownloader, DownloadError};
use crate::reader::cloudfetch::link_fetcher::ChunkLinkFetcher;
use crate::reader::cloudfetch::pipeline_types::ChunkDownloadTask;
use crate::types::cloudfetch::CloudFetchConfig;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// Spawn `num_workers` download worker tasks.
///
/// All workers share the same `download_rx` receiver. When the scheduler
/// finishes and drops the sender side, workers will drain remaining tasks
/// and exit.
///
/// # Arguments
/// * `download_rx` - Unbounded receiver of download tasks (shared via `Arc<Mutex>`)
/// * `downloader` - Chunk downloader for HTTP requests
/// * `link_fetcher` - For refetching expired links
/// * `config` - CloudFetch configuration (retry settings, etc.)
/// * `cancel_token` - Token for cooperative cancellation
pub fn spawn_workers(
    download_rx: mpsc::UnboundedReceiver<ChunkDownloadTask>,
    downloader: Arc<ChunkDownloader>,
    link_fetcher: Arc<dyn ChunkLinkFetcher>,
    config: CloudFetchConfig,
    cancel_token: CancellationToken,
) {
    let download_rx = Arc::new(tokio::sync::Mutex::new(download_rx));

    for worker_id in 0..config.num_download_workers {
        let rx = Arc::clone(&download_rx);
        let dl = Arc::clone(&downloader);
        let lf = Arc::clone(&link_fetcher);
        let cfg = config.clone();
        let ct = cancel_token.clone();

        tokio::spawn(async move {
            worker_loop(worker_id, rx, dl, lf, cfg, ct).await;
        });
    }
}

/// Main loop for a single download worker.
///
/// Pulls [`ChunkDownloadTask`] items from the shared receiver, downloads them
/// with retry, and sends results back via the oneshot channel in each task.
async fn worker_loop(
    worker_id: usize,
    download_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<ChunkDownloadTask>>>,
    downloader: Arc<ChunkDownloader>,
    link_fetcher: Arc<dyn ChunkLinkFetcher>,
    config: CloudFetchConfig,
    cancel_token: CancellationToken,
) {
    debug!("Worker {}: started", worker_id);

    loop {
        // Pull next task from the shared channel
        let task = {
            let mut rx = download_rx.lock().await;
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    debug!("Worker {}: cancelled, exiting", worker_id);
                    return;
                }
                item = rx.recv() => item,
            }
        };

        let Some(task) = task else {
            debug!("Worker {}: download channel closed, exiting", worker_id);
            return;
        };

        let chunk_index = task.chunk_index;
        debug!("Worker {}: processing chunk {}", worker_id, chunk_index);

        let result = download_chunk(
            worker_id,
            chunk_index,
            task.link,
            &downloader,
            &link_fetcher,
            &config,
            &cancel_token,
        )
        .await;

        // Send result via oneshot — if the receiver was dropped (consumer
        // cancelled), that's fine; we just discard the result.
        let _ = task.result_tx.send(result);
    }
}

/// Download a single chunk with retry logic.
///
/// Implements the full retry contract:
/// - Proactive expiry check before the first HTTP request
/// - Auth errors (401/403/404): refetch link, no sleep, counts against both
///   `max_retries` and `max_refresh_retries`
/// - Transient errors: linear backoff sleep, counts against `max_retries`
async fn download_chunk(
    worker_id: usize,
    chunk_index: i64,
    mut link: crate::types::cloudfetch::CloudFetchLink,
    downloader: &ChunkDownloader,
    link_fetcher: &Arc<dyn ChunkLinkFetcher>,
    config: &CloudFetchConfig,
    cancel_token: &CancellationToken,
) -> crate::error::Result<Vec<arrow_array::RecordBatch>> {
    use crate::error::DatabricksErrorHelper;
    use driverbase::error::ErrorHelper;

    let mut attempts: u32 = 0;
    let mut refresh_attempts: u32 = 0;

    // --- Proactive expiry check ---
    // Before the first HTTP request, check if the link is already expired or
    // will expire within the buffer window. This avoids a guaranteed 401/403
    // failure that would otherwise cost one retry.
    if link.is_expired(config.url_expiration_buffer_secs) {
        debug!(
            "Worker {}: chunk {} link expired proactively, refetching",
            worker_id, chunk_index
        );
        refresh_attempts += 1;
        if refresh_attempts > config.max_refresh_retries {
            return Err(DatabricksErrorHelper::io().message(format!(
                "Chunk {}: exceeded max refresh retries ({}) during proactive expiry check",
                chunk_index, config.max_refresh_retries
            )));
        }
        match link_fetcher.refetch_link(chunk_index, 0).await {
            Ok(new_link) => link = new_link,
            Err(e) => {
                return Err(DatabricksErrorHelper::io().message(format!(
                    "Chunk {}: failed to refetch expired link: {}",
                    chunk_index, e
                )));
            }
        }
    }

    // --- Download with retry ---
    loop {
        if cancel_token.is_cancelled() {
            return Err(DatabricksErrorHelper::invalid_state().message("Download cancelled"));
        }

        match downloader.download_with_status(&link).await {
            Ok(batches) => return Ok(batches),

            Err(DownloadError::AuthError {
                status_code,
                message,
            }) => {
                attempts += 1;
                refresh_attempts += 1;

                warn!(
                    "Worker {}: chunk {} auth error HTTP {} (attempt {}/{}, refresh {}/{}): {}",
                    worker_id,
                    chunk_index,
                    status_code,
                    attempts,
                    config.max_retries,
                    refresh_attempts,
                    config.max_refresh_retries,
                    message
                );

                // Check retry limits
                if attempts >= config.max_retries || refresh_attempts > config.max_refresh_retries {
                    return Err(DatabricksErrorHelper::io().message(format!(
                        "Chunk {}: download failed after {} attempts ({} refreshes): {}",
                        chunk_index, attempts, refresh_attempts, message
                    )));
                }

                // Refetch link — no sleep for auth errors
                match link_fetcher.refetch_link(chunk_index, 0).await {
                    Ok(new_link) => {
                        debug!(
                            "Worker {}: chunk {} refetched link after HTTP {}",
                            worker_id, chunk_index, status_code
                        );
                        link = new_link;
                    }
                    Err(e) => {
                        return Err(DatabricksErrorHelper::io().message(format!(
                            "Chunk {}: failed to refetch link: {}",
                            chunk_index, e
                        )));
                    }
                }
            }

            Err(DownloadError::TransientError { message }) => {
                attempts += 1;

                warn!(
                    "Worker {}: chunk {} transient error (attempt {}/{}): {}",
                    worker_id, chunk_index, attempts, config.max_retries, message
                );

                if attempts >= config.max_retries {
                    return Err(DatabricksErrorHelper::io().message(format!(
                        "Chunk {}: download failed after {} attempts: {}",
                        chunk_index, attempts, message
                    )));
                }

                // Linear backoff: retry_delay * (attempt + 1)
                // Note: `attempts` is already incremented, so it equals attempt+1
                let sleep_duration = config.retry_delay * attempts;
                debug!(
                    "Worker {}: chunk {} sleeping {:?} before retry",
                    worker_id, chunk_index, sleep_duration
                );

                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        return Err(DatabricksErrorHelper::invalid_state().message("Download cancelled during retry sleep"));
                    }
                    _ = tokio::time::sleep(sleep_duration) => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ChunkLinkFetchResult;
    use crate::error::Result;
    use crate::types::cloudfetch::{CloudFetchConfig, CloudFetchLink};
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use driverbase::error::ErrorHelper;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
    use std::time::Duration;

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

    fn create_expired_link(chunk_index: i64) -> CloudFetchLink {
        CloudFetchLink {
            url: format!("https://storage.example.com/chunk{}", chunk_index),
            chunk_index,
            row_offset: chunk_index * 1000,
            row_count: 1000,
            byte_count: 50000,
            expiration: chrono::Utc::now() - chrono::Duration::hours(1),
            http_headers: HashMap::new(),
            next_chunk_index: Some(chunk_index + 1),
        }
    }

    fn create_test_batches() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
        vec![batch]
    }

    /// Mock downloader that can be configured to fail N times before succeeding.
    #[derive(Debug)]
    struct MockDownloader {
        /// Number of times to fail before succeeding.
        fail_count: AtomicU32,
        /// Type of error to return on failure.
        error_type: DownloadErrorType,
        /// Track how many calls were made.
        call_count: AtomicU32,
        /// Track sleep durations observed (for backoff verification).
        download_timestamps: tokio::sync::Mutex<Vec<std::time::Instant>>,
    }

    #[derive(Debug, Clone, Copy)]
    enum DownloadErrorType {
        Transient,
        Auth,
    }

    impl MockDownloader {
        fn new(fail_count: u32, error_type: DownloadErrorType) -> Self {
            Self {
                fail_count: AtomicU32::new(fail_count),
                error_type,
                call_count: AtomicU32::new(0),
                download_timestamps: tokio::sync::Mutex::new(Vec::new()),
            }
        }

        fn always_succeed() -> Self {
            Self::new(0, DownloadErrorType::Transient)
        }

        async fn download_with_status(
            &self,
            _link: &CloudFetchLink,
        ) -> std::result::Result<Vec<RecordBatch>, DownloadError> {
            let call = self.call_count.fetch_add(1, Ordering::SeqCst);
            self.download_timestamps
                .lock()
                .await
                .push(std::time::Instant::now());

            let remaining_fails = self.fail_count.load(Ordering::SeqCst);
            if remaining_fails > 0 && call < remaining_fails {
                match self.error_type {
                    DownloadErrorType::Transient => Err(DownloadError::TransientError {
                        message: format!("mock transient error (call {})", call),
                    }),
                    DownloadErrorType::Auth => Err(DownloadError::AuthError {
                        status_code: 401,
                        message: format!("mock auth error (call {})", call),
                    }),
                }
            } else {
                Ok(create_test_batches())
            }
        }
    }

    /// Mock link fetcher for worker tests.
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
            self.refetch_count.fetch_add(1, Ordering::SeqCst);
            Ok(create_test_link(chunk_index))
        }
    }

    /// Helper: run download_chunk directly with a mock downloader.
    async fn run_download_chunk(
        link: CloudFetchLink,
        mock_downloader: &MockDownloader,
        link_fetcher: &Arc<dyn ChunkLinkFetcher>,
        config: &CloudFetchConfig,
        cancel_token: &CancellationToken,
    ) -> Result<Vec<RecordBatch>> {
        // We can't use download_chunk directly with MockDownloader because
        // it expects a real ChunkDownloader. Instead, replicate the logic
        // with the mock. This is a more targeted unit test.
        use crate::error::DatabricksErrorHelper;

        let chunk_index = link.chunk_index;
        let mut link = link;
        let mut attempts: u32 = 0;
        let mut refresh_attempts: u32 = 0;

        // Proactive expiry check
        if link.is_expired(config.url_expiration_buffer_secs) {
            refresh_attempts += 1;
            if refresh_attempts > config.max_refresh_retries {
                return Err(DatabricksErrorHelper::io().message("exceeded max refresh retries"));
            }
            link = link_fetcher.refetch_link(chunk_index, 0).await?;
        }

        loop {
            if cancel_token.is_cancelled() {
                return Err(DatabricksErrorHelper::invalid_state().message("cancelled"));
            }

            match mock_downloader.download_with_status(&link).await {
                Ok(batches) => return Ok(batches),
                Err(DownloadError::AuthError { message, .. }) => {
                    attempts += 1;
                    refresh_attempts += 1;
                    if attempts >= config.max_retries
                        || refresh_attempts > config.max_refresh_retries
                    {
                        return Err(DatabricksErrorHelper::io()
                            .message(format!("failed after {} attempts: {}", attempts, message)));
                    }
                    link = link_fetcher.refetch_link(chunk_index, 0).await?;
                }
                Err(DownloadError::TransientError { message }) => {
                    attempts += 1;
                    if attempts >= config.max_retries {
                        return Err(DatabricksErrorHelper::io()
                            .message(format!("failed after {} attempts: {}", attempts, message)));
                    }
                    let sleep_duration = config.retry_delay * attempts;
                    tokio::select! {
                        _ = cancel_token.cancelled() => {
                            return Err(DatabricksErrorHelper::invalid_state().message("cancelled during sleep"));
                        }
                        _ = tokio::time::sleep(sleep_duration) => {}
                    }
                }
            }
        }
    }

    fn test_config() -> CloudFetchConfig {
        CloudFetchConfig {
            max_retries: 3,
            max_refresh_retries: 3,
            retry_delay: Duration::from_millis(10), // Short for tests
            num_download_workers: 1,
            url_expiration_buffer_secs: 60,
            ..CloudFetchConfig::default()
        }
    }

    #[tokio::test]
    async fn worker_retries_on_transient_error() {
        let mock = MockDownloader::new(2, DownloadErrorType::Transient);
        let fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(MockLinkFetcher::new());
        let config = test_config();
        let cancel = CancellationToken::new();

        let result =
            run_download_chunk(create_test_link(0), &mock, &fetcher, &config, &cancel).await;

        assert!(result.is_ok());
        assert_eq!(mock.call_count.load(Ordering::SeqCst), 3); // 2 failures + 1 success
    }

    #[tokio::test]
    async fn worker_uses_linear_backoff() {
        let mock = MockDownloader::new(2, DownloadErrorType::Transient);
        let fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(MockLinkFetcher::new());
        let config = CloudFetchConfig {
            max_retries: 3,
            retry_delay: Duration::from_millis(50),
            ..test_config()
        };
        let cancel = CancellationToken::new();

        let result =
            run_download_chunk(create_test_link(0), &mock, &fetcher, &config, &cancel).await;
        assert!(result.is_ok());

        // Verify backoff timing
        let timestamps = mock.download_timestamps.lock().await;
        assert_eq!(timestamps.len(), 3);

        // First retry: sleep = 50ms * 1 = 50ms
        let gap1 = timestamps[1].duration_since(timestamps[0]);
        assert!(
            gap1 >= Duration::from_millis(40),
            "First gap {:?} should be ~50ms",
            gap1
        );

        // Second retry: sleep = 50ms * 2 = 100ms
        let gap2 = timestamps[2].duration_since(timestamps[1]);
        assert!(
            gap2 >= Duration::from_millis(80),
            "Second gap {:?} should be ~100ms",
            gap2
        );

        // gap2 should be roughly 2x gap1 (linear backoff)
        assert!(
            gap2.as_millis() > gap1.as_millis(),
            "Second gap ({:?}) should be larger than first ({:?})",
            gap2,
            gap1
        );
    }

    #[tokio::test]
    async fn worker_refetches_url_on_401_403_404() {
        let mock = MockDownloader::new(1, DownloadErrorType::Auth);
        let mock_fetcher = Arc::new(MockLinkFetcher::new());
        let fetcher: Arc<dyn ChunkLinkFetcher> = mock_fetcher.clone();
        let config = test_config();
        let cancel = CancellationToken::new();

        let result =
            run_download_chunk(create_test_link(0), &mock, &fetcher, &config, &cancel).await;

        assert!(result.is_ok());
        // refetch_link should have been called once for the auth error
        assert_eq!(mock_fetcher.refetch_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn worker_no_sleep_on_auth_error() {
        let mock = MockDownloader::new(1, DownloadErrorType::Auth);
        let fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(MockLinkFetcher::new());
        let config = CloudFetchConfig {
            max_retries: 3,
            retry_delay: Duration::from_secs(10), // Very long — if we slept, test would time out
            ..test_config()
        };
        let cancel = CancellationToken::new();

        let start = std::time::Instant::now();
        let result =
            run_download_chunk(create_test_link(0), &mock, &fetcher, &config, &cancel).await;
        let elapsed = start.elapsed();

        assert!(result.is_ok());
        // Should complete nearly instantly (no 10s sleep)
        assert!(
            elapsed < Duration::from_secs(1),
            "Auth error should not sleep, but took {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn worker_gives_up_after_max_refresh_retries() {
        // Fail with auth errors more times than max_refresh_retries allows
        let mock = MockDownloader::new(10, DownloadErrorType::Auth);
        let fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(MockLinkFetcher::new());
        let config = CloudFetchConfig {
            max_retries: 10,
            max_refresh_retries: 2,
            ..test_config()
        };
        let cancel = CancellationToken::new();

        let result =
            run_download_chunk(create_test_link(0), &mock, &fetcher, &config, &cancel).await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("failed after"),
            "Expected retry exhaustion error, got: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn worker_proactively_refreshes_expiring_url() {
        let mock = MockDownloader::always_succeed();
        let mock_fetcher = Arc::new(MockLinkFetcher::new());
        let fetcher: Arc<dyn ChunkLinkFetcher> = mock_fetcher.clone();
        let config = test_config();
        let cancel = CancellationToken::new();

        // Use an expired link — should trigger proactive refresh
        let result =
            run_download_chunk(create_expired_link(0), &mock, &fetcher, &config, &cancel).await;

        assert!(result.is_ok());
        // refetch_link should have been called for the proactive refresh
        assert_eq!(mock_fetcher.refetch_count.load(Ordering::SeqCst), 1);
        // Download should have been called exactly once (no retry needed)
        assert_eq!(mock.call_count.load(Ordering::SeqCst), 1);
    }
}
