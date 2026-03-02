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

//! ChunkDownloader and download worker pool for cloud storage.
//!
//! This module provides:
//! - [`ChunkDownloader`]: Downloads and parses Arrow data from presigned URLs,
//!   including HTTP header handling, speed monitoring, decompression, and Arrow
//!   IPC parsing.
//! - [`spawn_download_workers()`]: Spawns a pool of long-lived download worker
//!   tasks that pull [`ChunkDownloadTask`] items from the download channel.
//!
//! ## Worker Retry Contract
//!
//! Each worker implements the following retry logic per chunk (matching C#):
//!
//! | Error type             | Sleep before retry                    | Counts `max_retries` | Counts `max_refresh_retries` |
//! |------------------------|---------------------------------------|----------------------|------------------------------|
//! | Network / 5xx          | Yes — `retry_delay * (attempt + 1)`   | Yes                  | No                           |
//! | 401 / 403 / 404        | No                                    | Yes                  | Yes                          |
//! | Link proactively expired | No                                  | No                   | Yes                          |

use crate::client::DatabricksHttpClient;
use crate::error::{DatabricksErrorHelper, Result};
use crate::reader::cloudfetch::arrow_parser::parse_arrow_ipc;
use crate::reader::cloudfetch::link_fetcher::ChunkLinkFetcher;
use crate::reader::cloudfetch::pipeline_types::ChunkDownloadTask;
use crate::types::cloudfetch::{CloudFetchConfig, CloudFetchLink};
use crate::types::sea::CompressionCodec;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use driverbase::error::ErrorHelper;
use reqwest::Method;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

/// Trait for downloading a chunk from a link.
///
/// This trait abstracts the download operation so that the worker retry
/// logic can be tested with mock implementations.
#[async_trait]
pub trait ChunkDownload: Send + Sync + std::fmt::Debug {
    /// Download a chunk from the given link, returning parsed RecordBatches.
    async fn download(&self, link: &CloudFetchLink) -> Result<Vec<RecordBatch>>;
}

/// Downloads Arrow data from cloud storage presigned URLs.
///
/// Handles:
/// - HTTP GET to presigned URL with custom headers
/// - LZ4 decompression (if compression enabled)
/// - Arrow IPC stream parsing into RecordBatches
/// - Download speed monitoring
#[derive(Debug)]
pub struct ChunkDownloader {
    http_client: Arc<DatabricksHttpClient>,
    compression: CompressionCodec,
    speed_threshold_mbps: f64,
}

impl ChunkDownloader {
    /// Create a new chunk downloader.
    ///
    /// # Arguments
    /// * `http_client` - HTTP client for making requests
    /// * `compression` - Compression codec used by the server
    /// * `speed_threshold_mbps` - Log warning if speed falls below this (MB/s)
    pub fn new(
        http_client: Arc<DatabricksHttpClient>,
        compression: CompressionCodec,
        speed_threshold_mbps: f64,
    ) -> Self {
        Self {
            http_client,
            compression,
            speed_threshold_mbps,
        }
    }

    /// Download a chunk from the given link.
    ///
    /// # Arguments
    /// * `link` - CloudFetch link containing URL and headers
    ///
    /// # Returns
    /// Parsed RecordBatches from the downloaded Arrow IPC data
    pub async fn download(&self, link: &CloudFetchLink) -> Result<Vec<RecordBatch>> {
        let start = Instant::now();

        debug!(
            "Downloading chunk {} from {} ({} bytes expected)",
            link.chunk_index, link.url, link.byte_count
        );

        // Build request with custom headers from the link
        let mut request_builder = self.http_client.inner().request(Method::GET, &link.url);

        for (key, value) in &link.http_headers {
            request_builder = request_builder.header(key, value);
        }

        let request = request_builder.build().map_err(|e| {
            DatabricksErrorHelper::io().message(format!("Failed to build download request: {}", e))
        })?;

        // Execute without auth (presigned URLs have their own auth)
        let response = self.http_client.execute_without_auth(request).await?;

        // Read response body
        let bytes = response.bytes().await.map_err(|e| {
            DatabricksErrorHelper::io().message(format!("Failed to read download response: {}", e))
        })?;

        let elapsed = start.elapsed();
        let size_mb = bytes.len() as f64 / 1024.0 / 1024.0;
        let speed_mbps = size_mb / elapsed.as_secs_f64();

        debug!(
            "Downloaded chunk {}: {:.2} MB in {:.2}s ({:.2} MB/s)",
            link.chunk_index,
            size_mb,
            elapsed.as_secs_f64(),
            speed_mbps
        );

        if speed_mbps < self.speed_threshold_mbps {
            warn!(
                "CloudFetch download slower than threshold: {:.2} MB/s (threshold: {:.2} MB/s)",
                speed_mbps, self.speed_threshold_mbps
            );
        }

        // Parse Arrow IPC data
        let batches = parse_arrow_ipc(&bytes, self.compression)?;

        debug!(
            "Parsed chunk {}: {} batches, {} total rows",
            link.chunk_index,
            batches.len(),
            batches.iter().map(|b| b.num_rows()).sum::<usize>()
        );

        Ok(batches)
    }
}

#[async_trait]
impl ChunkDownload for ChunkDownloader {
    async fn download(&self, link: &CloudFetchLink) -> Result<Vec<RecordBatch>> {
        self.download(link).await
    }
}

// ── Auth-error detection ────────────────────────────────────────────────

/// Check whether a download error indicates an authentication/authorization
/// failure (HTTP 401, 403, or 404). These errors trigger an immediate URL
/// refresh without any sleep.
///
/// The [`DatabricksHttpClient`] formats HTTP errors as `"HTTP {code} - ..."`,
/// so we match against that pattern.
fn is_auth_error(err: &crate::error::Error) -> bool {
    let msg = err.to_string();
    msg.contains("HTTP 401") || msg.contains("HTTP 403") || msg.contains("HTTP 404")
}

// ── Download worker pool ────────────────────────────────────────────────

/// Spawns `config.num_download_workers` long-lived download worker tasks.
///
/// Each worker pulls [`ChunkDownloadTask`] items from the shared
/// `download_rx` channel and downloads the corresponding chunk. Workers
/// implement the full retry contract described in the module docs.
///
/// Returns a `Vec<JoinHandle<()>>` so the caller can await worker
/// completion or use it for cleanup.
///
/// # Arguments
///
/// * `download_rx` - Receiver end of the download channel (from scheduler).
/// * `config` - CloudFetch configuration (retry counts, delays, workers, etc.).
/// * `downloader` - The [`ChunkDownloader`] used for HTTP downloads.
/// * `link_fetcher` - The [`ChunkLinkFetcher`] used for URL refresh.
/// * `cancel_token` - Token for cooperative cancellation.
pub fn spawn_download_workers(
    download_rx: mpsc::UnboundedReceiver<ChunkDownloadTask>,
    config: &CloudFetchConfig,
    downloader: Arc<dyn ChunkDownload>,
    link_fetcher: Arc<dyn ChunkLinkFetcher>,
    cancel_token: CancellationToken,
) -> Vec<JoinHandle<()>> {
    let shared_rx = Arc::new(Mutex::new(download_rx));
    let mut handles = Vec::with_capacity(config.num_download_workers);

    for worker_id in 0..config.num_download_workers {
        let rx = Arc::clone(&shared_rx);
        let dl = Arc::clone(&downloader);
        let fetcher = Arc::clone(&link_fetcher);
        let token = cancel_token.clone();
        let max_retries = config.max_retries;
        let max_refresh_retries = config.max_refresh_retries;
        let retry_delay = config.retry_delay;
        let url_expiration_buffer_secs = config.url_expiration_buffer_secs;

        handles.push(tokio::spawn(async move {
            download_worker_loop(
                worker_id,
                rx,
                dl,
                fetcher,
                token,
                max_retries,
                max_refresh_retries,
                retry_delay,
                url_expiration_buffer_secs,
            )
            .await;
        }));
    }

    handles
}

/// Main loop for a single download worker.
///
/// Receives tasks from `download_rx`, downloads each chunk with retry
/// logic, and sends the result (success or failure) through the task's
/// oneshot `result_tx`.
#[allow(clippy::too_many_arguments)]
async fn download_worker_loop(
    worker_id: usize,
    download_rx: Arc<Mutex<mpsc::UnboundedReceiver<ChunkDownloadTask>>>,
    downloader: Arc<dyn ChunkDownload>,
    link_fetcher: Arc<dyn ChunkLinkFetcher>,
    cancel_token: CancellationToken,
    max_retries: u32,
    max_refresh_retries: u32,
    retry_delay: Duration,
    url_expiration_buffer_secs: u32,
) {
    debug!("Worker {}: started", worker_id);

    loop {
        // Receive next task — hold the lock only briefly for the recv().
        let task = {
            let mut rx = download_rx.lock().await;
            rx.recv().await
        };

        let task = match task {
            Some(t) => t,
            None => {
                // Channel closed — scheduler has finished dispatching all tasks.
                debug!("Worker {}: download channel closed, exiting", worker_id);
                break;
            }
        };

        // Check cancellation before starting work on this chunk.
        if cancel_token.is_cancelled() {
            debug!(
                "Worker {}: cancelled before processing chunk {}",
                worker_id, task.chunk_index
            );
            // Drop the task — result_tx is dropped, consumer gets RecvError.
            break;
        }

        let result = download_chunk(
            worker_id,
            &task,
            &downloader,
            &link_fetcher,
            &cancel_token,
            max_retries,
            max_refresh_retries,
            retry_delay,
            url_expiration_buffer_secs,
        )
        .await;

        // Send the result through the oneshot channel.
        // If the consumer has already dropped the receiver, this is a no-op.
        let _ = task.result_tx.send(result);
    }

    debug!("Worker {}: exiting", worker_id);
}

/// Downloads a single chunk with the full retry contract.
///
/// 1. Proactive expiry check — if link is expired or expiring soon,
///    refreshes URL before the first HTTP request.
/// 2. On success: returns `Ok(batches)`.
/// 3. On 401/403/404: refreshes URL and retries immediately (no sleep),
///    counts against both `max_retries` and `max_refresh_retries`.
/// 4. On other errors: sleeps `retry_delay * (attempt + 1)` (linear backoff),
///    counts against `max_retries` only.
/// 5. On `max_retries` or `max_refresh_retries` exceeded: returns `Err`.
#[allow(clippy::too_many_arguments)]
async fn download_chunk(
    worker_id: usize,
    task: &ChunkDownloadTask,
    downloader: &Arc<dyn ChunkDownload>,
    link_fetcher: &Arc<dyn ChunkLinkFetcher>,
    cancel_token: &CancellationToken,
    max_retries: u32,
    max_refresh_retries: u32,
    retry_delay: Duration,
    url_expiration_buffer_secs: u32,
) -> Result<Vec<RecordBatch>> {
    let chunk_index = task.chunk_index;
    let mut link = task.link.clone();
    let mut retries: u32 = 0;
    let mut refresh_retries: u32 = 0;

    // ── Proactive expiry check ──────────────────────────────────────
    // Before the first HTTP request, check if the link is expired or
    // expiring soon. If so, refresh it immediately. This avoids a
    // guaranteed 401/403 failure that would cost one retry.
    if link.is_expired(url_expiration_buffer_secs) {
        debug!(
            "Worker {}: chunk {} link expired proactively, refreshing",
            worker_id, chunk_index
        );

        refresh_retries += 1;
        if refresh_retries > max_refresh_retries {
            return Err(DatabricksErrorHelper::io().message(format!(
                "Chunk {}: max URL refresh retries ({}) exceeded during proactive check",
                chunk_index, max_refresh_retries
            )));
        }

        match link_fetcher
            .refetch_link(chunk_index, link.row_offset)
            .await
        {
            Ok(new_link) => {
                link = new_link;
            }
            Err(e) => {
                return Err(DatabricksErrorHelper::io().message(format!(
                    "Chunk {}: failed to refresh expired link: {}",
                    chunk_index, e
                )));
            }
        }
    }

    // ── Download loop with retry ────────────────────────────────────
    loop {
        if cancel_token.is_cancelled() {
            return Err(DatabricksErrorHelper::invalid_state().message("Download cancelled"));
        }

        match downloader.download(&link).await {
            Ok(batches) => {
                debug!(
                    "Worker {}: chunk {} downloaded successfully ({} batches)",
                    worker_id,
                    chunk_index,
                    batches.len()
                );
                return Ok(batches);
            }
            Err(e) => {
                if is_auth_error(&e) {
                    // ── 401 / 403 / 404 — refresh URL, retry immediately ──
                    retries += 1;
                    refresh_retries += 1;

                    if retries > max_retries {
                        error!(
                            "Worker {}: chunk {} max retries ({}) exceeded on auth error",
                            worker_id, chunk_index, max_retries
                        );
                        return Err(DatabricksErrorHelper::io().message(format!(
                            "Chunk {}: max retries ({}) exceeded: {}",
                            chunk_index, max_retries, e
                        )));
                    }

                    if refresh_retries > max_refresh_retries {
                        error!(
                            "Worker {}: chunk {} max refresh retries ({}) exceeded",
                            worker_id, chunk_index, max_refresh_retries
                        );
                        return Err(DatabricksErrorHelper::io().message(format!(
                            "Chunk {}: max URL refresh retries ({}) exceeded: {}",
                            chunk_index, max_refresh_retries, e
                        )));
                    }

                    warn!(
                        "Worker {}: chunk {} auth error (retry {}/{}, refresh {}/{}): {}, refreshing URL",
                        worker_id, chunk_index, retries, max_retries,
                        refresh_retries, max_refresh_retries, e
                    );

                    match link_fetcher
                        .refetch_link(chunk_index, link.row_offset)
                        .await
                    {
                        Ok(new_link) => {
                            link = new_link;
                        }
                        Err(fetch_err) => {
                            return Err(DatabricksErrorHelper::io().message(format!(
                                "Chunk {}: failed to refresh link after auth error: {}",
                                chunk_index, fetch_err
                            )));
                        }
                    }
                    // No sleep — retry immediately with fresh URL
                } else {
                    // ── Network / 5xx / other — linear backoff ──────────
                    retries += 1;

                    if retries > max_retries {
                        error!(
                            "Worker {}: chunk {} max retries ({}) exceeded on transient error",
                            worker_id, chunk_index, max_retries
                        );
                        return Err(DatabricksErrorHelper::io().message(format!(
                            "Chunk {}: max retries ({}) exceeded: {}",
                            chunk_index, max_retries, e
                        )));
                    }

                    let delay = retry_delay * (retries); // linear: retry_delay * (attempt + 1) where attempt is 0-based, retries is 1-based
                    warn!(
                        "Worker {}: chunk {} transient error (retry {}/{}): {}, sleeping {:?}",
                        worker_id, chunk_index, retries, max_retries, e, delay
                    );

                    // Cancellable sleep
                    tokio::select! {
                        _ = cancel_token.cancelled() => {
                            return Err(
                                DatabricksErrorHelper::invalid_state()
                                    .message("Download cancelled during retry sleep")
                            );
                        }
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::PersonalAccessToken;
    use crate::client::{ChunkLinkFetchResult, HttpClientConfig};
    use crate::reader::cloudfetch::pipeline_types::create_chunk_pair;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{timeout, Duration as TokioDuration};

    // ── Test helpers ────────────────────────────────────────────────

    /// Create a test link with the given chunk_index and a far-future expiry.
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

    /// Create a test link that is already expired (or about to expire).
    fn expiring_test_link(chunk_index: i64) -> CloudFetchLink {
        CloudFetchLink {
            url: format!("https://storage.example.com/chunk{}", chunk_index),
            chunk_index,
            row_offset: chunk_index * 1000,
            row_count: 1000,
            byte_count: 50000,
            // Expires 10 seconds from now — with a 60s buffer this is "expired"
            expiration: chrono::Utc::now() + chrono::Duration::seconds(10),
            http_headers: HashMap::new(),
            next_chunk_index: Some(chunk_index + 1),
        }
    }

    /// Create a test RecordBatch with a single Int32 column.
    fn test_record_batch(values: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Int32, false)]));
        let array = Int32Array::from(values.to_vec());
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    // ── Mock ChunkDownload ──────────────────────────────────────────

    /// A mock downloader that returns pre-configured results in sequence.
    /// Each call to `download()` pops the next result from the list.
    #[derive(Debug)]
    struct MockDownloader {
        /// Results returned by successive download() calls.
        results: std::sync::Mutex<Vec<Result<Vec<RecordBatch>>>>,
        /// Count of download() calls.
        call_count: AtomicUsize,
    }

    impl MockDownloader {
        fn new(results: Vec<Result<Vec<RecordBatch>>>) -> Self {
            Self {
                results: std::sync::Mutex::new(results),
                call_count: AtomicUsize::new(0),
            }
        }

        fn call_count(&self) -> usize {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl ChunkDownload for MockDownloader {
        async fn download(&self, _link: &CloudFetchLink) -> Result<Vec<RecordBatch>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            let mut results = self.results.lock().unwrap();
            if results.is_empty() {
                Ok(vec![test_record_batch(&[1, 2, 3])])
            } else {
                results.remove(0)
            }
        }
    }

    // ── Mock ChunkLinkFetcher ───────────────────────────────────────

    /// A mock link fetcher that tracks refetch_link calls and returns
    /// fresh links with far-future expiration.
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
            self.refetch_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl ChunkLinkFetcher for MockLinkFetcher {
        async fn fetch_links(
            &self,
            _start_chunk_index: i64,
            _start_row_offset: i64,
        ) -> Result<ChunkLinkFetchResult> {
            unimplemented!("fetch_links not needed for worker tests")
        }

        async fn refetch_link(&self, chunk_index: i64, _row_offset: i64) -> Result<CloudFetchLink> {
            self.refetch_count.fetch_add(1, Ordering::SeqCst);
            Ok(test_link(chunk_index))
        }
    }

    // ── Existing test ───────────────────────────────────────────────

    #[test]
    fn test_chunk_downloader_creation() {
        let auth = Arc::new(PersonalAccessToken::new("test-token".to_string()));
        let http_client =
            Arc::new(DatabricksHttpClient::new(HttpClientConfig::default(), auth).unwrap());
        let downloader = ChunkDownloader::new(http_client, CompressionCodec::None, 0.1);

        assert_eq!(downloader.speed_threshold_mbps, 0.1);
    }

    // ── Worker unit tests ───────────────────────────────────────────

    /// Helper: run `download_chunk` directly (bypasses the worker loop)
    /// for fine-grained control in unit tests.
    async fn run_download_chunk(
        task: &ChunkDownloadTask,
        downloader: Arc<dyn ChunkDownload>,
        link_fetcher: Arc<dyn ChunkLinkFetcher>,
        config: &CloudFetchConfig,
    ) -> Result<Vec<RecordBatch>> {
        let cancel_token = CancellationToken::new();
        download_chunk(
            0, // worker_id
            task,
            &downloader,
            &link_fetcher,
            &cancel_token,
            config.max_retries,
            config.max_refresh_retries,
            config.retry_delay,
            config.url_expiration_buffer_secs,
        )
        .await
    }

    #[tokio::test]
    async fn worker_retries_on_transient_error() {
        // Mock downloader fails 2 times with a transient error, then succeeds.
        let transient_err =
            || Err(DatabricksErrorHelper::io().message("HTTP 500 - Internal Server Error"));
        let success = || Ok(vec![test_record_batch(&[1, 2, 3])]);

        let downloader = Arc::new(MockDownloader::new(vec![
            transient_err(),
            transient_err(),
            success(),
        ]));
        let fetcher = Arc::new(MockLinkFetcher::new());

        let mut config = CloudFetchConfig::default();
        config.max_retries = 3;
        config.retry_delay = Duration::from_millis(1); // fast for tests

        let link = test_link(0);
        let (task, handle) = create_chunk_pair(0, link);

        let result = run_download_chunk(&task, downloader.clone(), fetcher.clone(), &config).await;

        assert!(result.is_ok());
        let batches = result.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);

        // download() was called 3 times (2 failures + 1 success)
        assert_eq!(downloader.call_count(), 3);

        // No URL refreshes for transient errors
        assert_eq!(fetcher.refetch_count(), 0);

        // Handle is still valid (result_tx wasn't sent through since we called
        // download_chunk directly, but that's fine for this test)
        drop(handle);
    }

    #[tokio::test]
    async fn worker_uses_linear_backoff() {
        // Verify that transient errors use linear backoff: retry_delay * (attempt + 1).
        // We'll use 3 failures with retry_delay = 100ms and measure elapsed time.
        // Expected total sleep: 100ms * 1 + 100ms * 2 + 100ms * 3 = 600ms
        // (but last attempt succeeds, so we sleep for attempts 1 and 2: 100 + 200 = 300ms)
        let transient_err = || Err(DatabricksErrorHelper::io().message("HTTP 502 - Bad Gateway"));
        let success = || Ok(vec![test_record_batch(&[42])]);

        let downloader = Arc::new(MockDownloader::new(vec![
            transient_err(),
            transient_err(),
            success(),
        ]));
        let fetcher = Arc::new(MockLinkFetcher::new());

        let mut config = CloudFetchConfig::default();
        config.max_retries = 5;
        config.retry_delay = Duration::from_millis(100);

        let link = test_link(0);
        let (task, _handle) = create_chunk_pair(0, link);

        let start = Instant::now();
        let result = run_download_chunk(&task, downloader.clone(), fetcher.clone(), &config).await;
        let elapsed = start.elapsed();

        assert!(result.is_ok());
        assert_eq!(downloader.call_count(), 3);

        // Linear backoff: sleep(100*1) + sleep(100*2) = 300ms total
        // Allow generous tolerance for CI/scheduling variance
        assert!(
            elapsed >= Duration::from_millis(250),
            "Expected at least 250ms of backoff sleep, got {:?}",
            elapsed
        );
        assert!(
            elapsed < Duration::from_millis(800),
            "Expected less than 800ms total, got {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn worker_refetches_url_on_401_403_404() {
        // Each of 401, 403, 404 should trigger a URL refresh via refetch_link.
        for status_code in &[401, 403, 404] {
            let auth_err = || {
                Err(DatabricksErrorHelper::io()
                    .message(format!("HTTP {} - Unauthorized", status_code)))
            };
            let success = || Ok(vec![test_record_batch(&[1])]);

            let downloader = Arc::new(MockDownloader::new(vec![auth_err(), success()]));
            let fetcher = Arc::new(MockLinkFetcher::new());

            let mut config = CloudFetchConfig::default();
            config.max_retries = 3;
            config.max_refresh_retries = 3;
            config.retry_delay = Duration::from_millis(1);

            let link = test_link(0);
            let (task, _handle) = create_chunk_pair(0, link);

            let result =
                run_download_chunk(&task, downloader.clone(), fetcher.clone(), &config).await;

            assert!(
                result.is_ok(),
                "Expected success after auth error retry for HTTP {}, got: {:?}",
                status_code,
                result
            );

            // refetch_link should have been called once
            assert_eq!(
                fetcher.refetch_count(),
                1,
                "Expected 1 refetch for HTTP {}, got {}",
                status_code,
                fetcher.refetch_count()
            );

            // download() called twice (1 failure + 1 success)
            assert_eq!(downloader.call_count(), 2);
        }
    }

    #[tokio::test]
    async fn worker_no_sleep_on_auth_error() {
        // Auth errors (401/403/404) should NOT sleep before retry.
        // With a large retry_delay, the test should complete quickly.
        let auth_err = || Err(DatabricksErrorHelper::io().message("HTTP 401 - Unauthorized"));
        let success = || Ok(vec![test_record_batch(&[99])]);

        let downloader = Arc::new(MockDownloader::new(vec![auth_err(), auth_err(), success()]));
        let fetcher = Arc::new(MockLinkFetcher::new());

        let mut config = CloudFetchConfig::default();
        config.max_retries = 5;
        config.max_refresh_retries = 5;
        // Set a very long retry_delay — if auth errors sleep, this test will time out
        config.retry_delay = Duration::from_secs(60);

        let link = test_link(0);
        let (task, _handle) = create_chunk_pair(0, link);

        let start = Instant::now();
        let result = timeout(
            TokioDuration::from_secs(2),
            run_download_chunk(&task, downloader.clone(), fetcher.clone(), &config),
        )
        .await;
        let elapsed = start.elapsed();

        // Should complete within timeout (auth errors don't sleep)
        assert!(result.is_ok(), "Timed out — auth error likely sleeping");
        assert!(result.unwrap().is_ok());

        // Should be very fast (no sleep)
        assert!(
            elapsed < Duration::from_millis(500),
            "Auth error retry took {:?} — expected no sleep",
            elapsed
        );

        assert_eq!(downloader.call_count(), 3);
        assert_eq!(fetcher.refetch_count(), 2);
    }

    #[tokio::test]
    async fn worker_gives_up_after_max_refresh_retries() {
        // All download attempts return auth errors. Worker should give up
        // after max_refresh_retries and propagate the error via result.
        let auth_err = || Err(DatabricksErrorHelper::io().message("HTTP 403 - Forbidden"));

        // More errors than retries allow
        let downloader = Arc::new(MockDownloader::new(vec![
            auth_err(),
            auth_err(),
            auth_err(),
            auth_err(), // should never reach this one
        ]));
        let fetcher = Arc::new(MockLinkFetcher::new());

        let mut config = CloudFetchConfig::default();
        config.max_retries = 10; // high so max_refresh_retries is the binding constraint
        config.max_refresh_retries = 2;
        config.retry_delay = Duration::from_millis(1);

        let link = test_link(0);
        let (task, _handle) = create_chunk_pair(0, link);

        let result = run_download_chunk(&task, downloader.clone(), fetcher.clone(), &config).await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("max URL refresh retries"),
            "Expected 'max URL refresh retries' in error, got: {}",
            err_msg
        );

        // Should have attempted download max_refresh_retries + 1 times
        // (first attempt + max_refresh_retries retries, but we stop when
        // refresh_retries > max_refresh_retries after incrementing)
        // With max_refresh_retries=2: attempts = 3 (refresh_retries goes 1, 2, 3 > 2 → stop)
        assert_eq!(downloader.call_count(), 3);
        // refetch_link called twice (first two successful refreshes, third fails the check)
        assert_eq!(fetcher.refetch_count(), 2);
    }

    #[tokio::test]
    async fn worker_proactively_refreshes_expiring_url() {
        // Link expires within url_expiration_buffer_secs (10s < 60s buffer).
        // Worker should call refetch_link BEFORE the first download attempt.
        let success = || Ok(vec![test_record_batch(&[7, 8, 9])]);

        let downloader = Arc::new(MockDownloader::new(vec![success()]));
        let fetcher = Arc::new(MockLinkFetcher::new());

        let config = CloudFetchConfig::default(); // url_expiration_buffer_secs = 60

        // Link expires in 10 seconds — with a 60s buffer, this is "expired"
        let link = expiring_test_link(0);
        assert!(
            link.is_expired(config.url_expiration_buffer_secs),
            "Test setup: link should be considered expired with 60s buffer"
        );

        let (task, _handle) = create_chunk_pair(0, link);

        let result = run_download_chunk(&task, downloader.clone(), fetcher.clone(), &config).await;

        assert!(result.is_ok());
        let batches = result.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);

        // refetch_link should have been called once (proactive refresh)
        // BEFORE download was attempted
        assert_eq!(
            fetcher.refetch_count(),
            1,
            "Expected proactive URL refresh before download"
        );
        // download() should be called once with the fresh link
        assert_eq!(downloader.call_count(), 1);
    }

    // ── spawn_download_workers integration test ─────────────────────

    #[tokio::test]
    async fn spawn_workers_process_tasks_and_exit() {
        // Verify that spawned workers process tasks and exit when channel closes.
        let downloader: Arc<dyn ChunkDownload> = Arc::new(MockDownloader::new(vec![
            Ok(vec![test_record_batch(&[1])]),
            Ok(vec![test_record_batch(&[2])]),
            Ok(vec![test_record_batch(&[3])]),
        ]));
        let fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(MockLinkFetcher::new());

        let mut config = CloudFetchConfig::default();
        config.num_download_workers = 2;

        let (tx, rx) = mpsc::unbounded_channel();
        let cancel_token = CancellationToken::new();

        let worker_handles = spawn_download_workers(rx, &config, downloader, fetcher, cancel_token);

        assert_eq!(worker_handles.len(), 2);

        // Send 3 tasks
        let mut receivers = Vec::new();
        for i in 0..3 {
            let (task, handle) = create_chunk_pair(i, test_link(i));
            tx.send(task).unwrap();
            receivers.push(handle);
        }

        // Close the channel (signals workers to exit after draining)
        drop(tx);

        // Collect results
        for (i, handle) in receivers.into_iter().enumerate() {
            let result = timeout(TokioDuration::from_secs(5), handle.result_rx)
                .await
                .expect("should not time out")
                .expect("sender should not be dropped");
            assert!(result.is_ok(), "Chunk {} should succeed", i);
        }

        // Workers should exit
        for wh in worker_handles {
            timeout(TokioDuration::from_secs(5), wh)
                .await
                .expect("worker should exit")
                .expect("worker should not panic");
        }
    }
}
