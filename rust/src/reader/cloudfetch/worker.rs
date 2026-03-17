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

//! Download worker tasks for the CloudFetch pipeline.
//!
//! Each worker is a long-lived tokio task that:
//! 1. Receives ChunkDownloadTask from download_channel (MPMC)
//! 2. Performs proactive URL expiry check before first download attempt
//! 3. Downloads chunks with retry logic (network errors with linear backoff)
//! 4. Handles URL expiration/refresh (401/403/404 without sleep)
//! 5. Sends results via oneshot channel
//!
//! # Retry Contract
//!
//! | Error type | Sleep before retry | Counts against max_retries | Counts against max_refresh_retries |
//! |------------|--------------------|-----------------------------|-------------------------------------|
//! | Network/5xx | Yes - `retry_delay * (attempt + 1)` | Yes | No |
//! | 401/403/404 | No | Yes | Yes |
//! | Proactive expiry | No | No | Yes |
//!
//! # Architecture
//!
//! ```text
//! Worker (N parallel tasks) - each loops on download_channel:
//!   loop {
//!     task = download_channel.recv()
//!
//!     // Proactive expiry check
//!     if link.is_expired(url_expiration_buffer_secs) {
//!       link = refetch_link()
//!       refresh_attempts++
//!     }
//!
//!     // Download with retry
//!     retry_loop {
//!       result = downloader.download(link)
//!       match result {
//!         Ok(batches) => task.result_tx.send(Ok(batches))
//!         Err(401/403/404) => {
//!           link = refetch_link()
//!           refresh_attempts++
//!           continue (no sleep)
//!         }
//!         Err(network/5xx) => {
//!           sleep(retry_delay * (attempt + 1))
//!           retry_attempts++
//!           continue
//!         }
//!       }
//!     }
//!   }
//! ```

use crate::error::{DatabricksErrorHelper, Result};
use crate::reader::cloudfetch::chunk_downloader::ChunkDownloader;
use crate::reader::cloudfetch::link_fetcher::ChunkLinkFetcher;
use crate::reader::cloudfetch::pipeline_types::ChunkDownloadTask;
use crate::types::cloudfetch::CloudFetchLink;
use async_channel::Receiver as AsyncChannelReceiver;
use driverbase::error::ErrorHelper;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

/// Configuration for download workers.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Maximum number of retry attempts for failed downloads.
    pub max_retries: u32,
    /// Maximum number of URL refresh attempts before terminal error.
    pub max_refresh_retries: u32,
    /// Base delay between retry attempts.
    pub retry_delay: Duration,
    /// Seconds before expiry to trigger proactive refresh.
    pub url_expiration_buffer_secs: u32,
}

/// Spawns N download worker tasks.
///
/// Each worker is a long-lived tokio task that loops on `download_channel.recv()`,
/// downloads chunks with retry logic, handles URL expiration/refresh, and sends
/// results via oneshot channels.
///
/// # Arguments
///
/// * `num_workers` - Number of parallel download worker tasks to spawn
/// * `download_channel` - MPMC channel for receiving download tasks
/// * `downloader` - ChunkDownloader for HTTP download
/// * `link_fetcher` - ChunkLinkFetcher for URL refresh
/// * `config` - Worker configuration (retry settings, expiration buffer)
/// * `cancel_token` - Cancellation token for coordinated shutdown
///
/// # Returns
///
/// Returns a vector of `tokio::task::JoinHandle` for the spawned worker tasks.
///
/// # Example
///
/// ```rust,ignore
/// let (download_tx, download_rx) = async_channel::bounded(max_chunks_in_memory);
/// let cancel_token = CancellationToken::new();
///
/// let worker_handles = spawn_workers(
///     3,
///     download_rx,
///     downloader,
///     link_fetcher,
///     worker_config,
///     cancel_token.clone(),
/// );
/// ```
pub fn spawn_workers(
    num_workers: usize,
    download_channel: AsyncChannelReceiver<ChunkDownloadTask>,
    downloader: Arc<ChunkDownloader>,
    link_fetcher: Arc<dyn ChunkLinkFetcher>,
    config: WorkerConfig,
    cancel_token: CancellationToken,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = Vec::with_capacity(num_workers);

    for worker_id in 0..num_workers {
        let download_rx = download_channel.clone();
        let downloader = Arc::clone(&downloader);
        let link_fetcher = Arc::clone(&link_fetcher);
        let config = config.clone();
        let cancel_token = cancel_token.clone();

        let handle = tokio::spawn(async move {
            run_worker(
                worker_id,
                download_rx,
                downloader,
                link_fetcher,
                config,
                cancel_token,
            )
            .await;
        });

        handles.push(handle);
    }

    handles
}

/// Internal worker loop implementation.
///
/// This function runs the main worker logic:
/// - Loops on download_channel.recv() to receive tasks
/// - Performs proactive expiry check before first download attempt
/// - Downloads chunks with retry logic
/// - Handles auth errors (401/403/404) with immediate refetch, no sleep
/// - Handles network/5xx errors with linear backoff sleep
/// - Enforces max_retries and max_refresh_retries limits
/// - Sends result via oneshot channel
async fn run_worker(
    worker_id: usize,
    download_channel: AsyncChannelReceiver<ChunkDownloadTask>,
    downloader: Arc<ChunkDownloader>,
    link_fetcher: Arc<dyn ChunkLinkFetcher>,
    config: WorkerConfig,
    cancel_token: CancellationToken,
) {
    debug!("Worker {}: started", worker_id);

    loop {
        // Receive next task from channel
        let task = tokio::select! {
            result = download_channel.recv() => {
                match result {
                    Ok(task) => task,
                    Err(_) => {
                        // Channel closed - scheduler is done
                        debug!("Worker {}: download_channel closed, exiting", worker_id);
                        break;
                    }
                }
            }
            _ = cancel_token.cancelled() => {
                debug!("Worker {}: cancellation requested, exiting", worker_id);
                break;
            }
        };

        let chunk_index = task.chunk_index;
        debug!(
            "Worker {}: received task for chunk {}",
            worker_id, chunk_index
        );

        // Process the download task with retry logic
        let result = download_chunk_with_retry(
            worker_id,
            task.link,
            &downloader,
            &link_fetcher,
            &config,
            &cancel_token,
        )
        .await;

        // Send result back to consumer via oneshot
        if task.result_tx.send(result).is_err() {
            warn!(
                "Worker {}: failed to send result for chunk {} (receiver dropped)",
                worker_id, chunk_index
            );
        }
    }

    debug!("Worker {}: stopped", worker_id);
}

/// Download a single chunk with retry logic.
///
/// This function implements the retry contract:
/// - Proactive expiry check before first attempt
/// - Auth errors (401/403/404): refetch URL, retry immediately, no sleep
/// - Network/5xx errors: sleep with linear backoff, then retry
/// - Enforces max_retries and max_refresh_retries
async fn download_chunk_with_retry(
    worker_id: usize,
    mut link: CloudFetchLink,
    downloader: &ChunkDownloader,
    link_fetcher: &Arc<dyn ChunkLinkFetcher>,
    config: &WorkerConfig,
    cancel_token: &CancellationToken,
) -> Result<Vec<arrow_array::RecordBatch>> {
    let chunk_index = link.chunk_index;
    let row_offset = link.row_offset;

    let mut retry_attempts = 0u32;
    let mut refresh_attempts = 0u32;

    // Proactive expiry check before first attempt
    if link.is_expired(config.url_expiration_buffer_secs) {
        debug!(
            "Worker {}: chunk {} URL is expired or expiring within {}s buffer, proactively refetching",
            worker_id, chunk_index, config.url_expiration_buffer_secs
        );

        match link_fetcher.refetch_link(chunk_index, row_offset).await {
            Ok(fresh_link) => {
                debug!(
                    "Worker {}: chunk {} proactively refreshed URL",
                    worker_id, chunk_index
                );
                link = fresh_link;
                refresh_attempts += 1;

                if refresh_attempts >= config.max_refresh_retries {
                    error!(
                        "Worker {}: chunk {} exceeded max_refresh_retries ({}) during proactive refresh",
                        worker_id, chunk_index, config.max_refresh_retries
                    );
                    return Err(DatabricksErrorHelper::io()
                        .message(format!(
                            "Exceeded max URL refresh attempts ({}) for chunk {}",
                            config.max_refresh_retries, chunk_index
                        ))
                        .context("proactive URL refresh"));
                }
            }
            Err(e) => {
                error!(
                    "Worker {}: chunk {} failed to proactively refetch URL: {}",
                    worker_id, chunk_index, e
                );
                return Err(e);
            }
        }
    }

    // Retry loop
    loop {
        // Check cancellation before each attempt
        if cancel_token.is_cancelled() {
            debug!(
                "Worker {}: chunk {} cancelled during download",
                worker_id, chunk_index
            );
            return Err(DatabricksErrorHelper::invalid_state()
                .message("Download cancelled")
                .context(format!("chunk {}", chunk_index)));
        }

        // Attempt download
        debug!(
            "Worker {}: chunk {} download attempt {} (refresh_attempts={})",
            worker_id,
            chunk_index,
            retry_attempts + 1,
            refresh_attempts
        );

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
                // Classify error type
                let error_str = e.to_string();
                let is_auth_error = error_str.contains("401")
                    || error_str.contains("403")
                    || error_str.contains("404");

                if is_auth_error {
                    // Auth error - refetch URL immediately without sleep
                    warn!(
                        "Worker {}: chunk {} auth error (likely expired URL), refetching: {}",
                        worker_id, chunk_index, e
                    );

                    // Check refresh retry limit
                    if refresh_attempts >= config.max_refresh_retries {
                        error!(
                            "Worker {}: chunk {} exceeded max_refresh_retries ({}) on auth error",
                            worker_id, chunk_index, config.max_refresh_retries
                        );
                        return Err(DatabricksErrorHelper::io()
                            .message(format!(
                                "Exceeded max URL refresh attempts ({}) for chunk {}",
                                config.max_refresh_retries, chunk_index
                            ))
                            .context("URL refresh on auth error"));
                    }

                    // Check total retry limit (auth errors count against max_retries)
                    if retry_attempts >= config.max_retries {
                        error!(
                            "Worker {}: chunk {} exceeded max_retries ({}) on auth error",
                            worker_id, chunk_index, config.max_retries
                        );
                        return Err(DatabricksErrorHelper::io()
                            .message(format!(
                                "Exceeded max retry attempts ({}) for chunk {}",
                                config.max_retries, chunk_index
                            ))
                            .context("download with retry"));
                    }

                    // Refetch URL
                    match link_fetcher.refetch_link(chunk_index, row_offset).await {
                        Ok(fresh_link) => {
                            debug!(
                                "Worker {}: chunk {} refreshed URL after auth error",
                                worker_id, chunk_index
                            );
                            link = fresh_link;
                            refresh_attempts += 1;
                            retry_attempts += 1;
                            // Continue immediately without sleep
                            continue;
                        }
                        Err(refetch_error) => {
                            error!(
                                "Worker {}: chunk {} failed to refetch URL: {}",
                                worker_id, chunk_index, refetch_error
                            );
                            return Err(refetch_error);
                        }
                    }
                } else {
                    // Network/5xx error - sleep with linear backoff
                    retry_attempts += 1;

                    if retry_attempts >= config.max_retries {
                        error!(
                            "Worker {}: chunk {} exceeded max_retries ({}) on network error",
                            worker_id, chunk_index, config.max_retries
                        );
                        return Err(DatabricksErrorHelper::io()
                            .message(format!(
                                "Exceeded max retry attempts ({}) for chunk {}: {}",
                                config.max_retries, chunk_index, e
                            ))
                            .context("download with retry"));
                    }

                    // Linear backoff: retry_delay * (attempt)
                    let backoff = config.retry_delay * retry_attempts;
                    warn!(
                        "Worker {}: chunk {} network error (attempt {}), retrying after {:?}: {}",
                        worker_id, chunk_index, retry_attempts, backoff, e
                    );

                    // Sleep with cancellation check
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {
                            // Sleep completed, continue to retry
                        }
                        _ = cancel_token.cancelled() => {
                            debug!(
                                "Worker {}: chunk {} cancelled during backoff sleep",
                                worker_id, chunk_index
                            );
                            return Err(DatabricksErrorHelper::invalid_state()
                                .message("Download cancelled during retry backoff")
                                .context(format!("chunk {}", chunk_index)));
                        }
                    }
                }
            }
        }
    }
}
