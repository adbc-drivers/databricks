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

//! Scheduler task for orchestrating the CloudFetch pipeline.
//!
//! The scheduler is a single tokio task that:
//! 1. Fetches links from ChunkLinkFetcher in batches (server-determined size)
//! 2. For each link, creates a oneshot channel pair (tx, rx)
//! 3. Sends ChunkDownloadTask{link, tx} to download_channel (MPMC, bounded)
//! 4. Sends ChunkHandle{rx} to result_channel (MPSC, bounded) in chunk-index order
//!
//! The bounded result_channel provides automatic backpressure - when max_chunks_in_memory
//! is reached, the scheduler blocks on send, preventing unbounded memory growth.
//!
//! # Architecture
//!
//! ```text
//! Scheduler (single task) - sequential loop:
//!   loop {
//!     links = fetch_links() -> Vec<CloudFetchLink> (batch)
//!     for link in links {
//!       (tx, rx) = oneshot::channel()
//!       download_channel.send(ChunkDownloadTask { link, tx })  // MPMC
//!       result_channel.send(ChunkHandle { rx })                // MPSC, backpressure here
//!     }
//!     if !has_more { break }
//!   }
//! ```
//!
//! This matches the C# driver's pattern in `CloudFetchDownloader.cs` where the result
//! is enqueued before the download task is awaited, preserving sequential ordering.

use crate::error::{DatabricksErrorHelper, Result};
use crate::reader::cloudfetch::link_fetcher::ChunkLinkFetcher;
use crate::reader::cloudfetch::pipeline_types::{ChunkDownloadTask, ChunkHandle};
use async_channel::Sender as AsyncChannelSender;
use driverbase::error::ErrorHelper;
use tokio::sync::mpsc::Sender as MpscSender;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

/// Spawns the scheduler task as a single tokio task.
///
/// The scheduler runs a sequential loop that:
/// - Fetches links in batches via `fetch_links()` (server-determined batch size)
/// - Creates oneshot channel pairs for each chunk
/// - Sends `ChunkDownloadTask` to `download_channel` (MPMC, bounded)
/// - Sends `ChunkHandle` to `result_channel` (MPSC, bounded) in chunk-index order
/// - Respects `CancellationToken` for clean shutdown
///
/// # Arguments
///
/// * `link_fetcher` - ChunkLinkFetcher trait object for fetching links
/// * `download_channel` - Bounded MPMC channel for sending tasks to workers
/// * `result_channel` - Bounded MPSC channel for sending handles to consumer
/// * `cancel_token` - Cancellation token for coordinated shutdown
///
/// # Returns
///
/// Returns a `tokio::task::JoinHandle` for the spawned scheduler task.
///
/// # Backpressure
///
/// The bounded `result_channel` provides automatic backpressure. When the channel
/// is full (max_chunks_in_memory reached), `result_channel.send()` will block,
/// preventing the scheduler from fetching and enqueuing more links until the
/// consumer has drained some results.
///
/// # Example
///
/// ```rust,ignore
/// let (download_tx, download_rx) = async_channel::bounded(max_chunks_in_memory);
/// let (result_tx, result_rx) = tokio::sync::mpsc::channel(max_chunks_in_memory);
/// let cancel_token = CancellationToken::new();
///
/// let scheduler_handle = spawn_scheduler(
///     link_fetcher,
///     download_tx,
///     result_tx,
///     cancel_token.clone(),
/// );
/// ```
pub fn spawn_scheduler(
    link_fetcher: Box<dyn ChunkLinkFetcher>,
    download_channel: AsyncChannelSender<ChunkDownloadTask>,
    result_channel: MpscSender<ChunkHandle>,
    cancel_token: CancellationToken,
) -> tokio::task::JoinHandle<Result<()>> {
    tokio::spawn(async move {
        run_scheduler(link_fetcher, download_channel, result_channel, cancel_token).await
    })
}

/// Internal scheduler loop implementation.
///
/// This function runs the main scheduler logic:
/// - Sequential loop calling `fetch_links()` which returns `Vec<CloudFetchLink>` batches
/// - For each link: create oneshot (tx, rx), send task and handle in order
/// - Bounded result_channel blocks when max_chunks_in_memory reached (automatic backpressure)
/// - Exits when `has_more=false` or cancellation is requested
async fn run_scheduler(
    link_fetcher: Box<dyn ChunkLinkFetcher>,
    download_channel: AsyncChannelSender<ChunkDownloadTask>,
    result_channel: MpscSender<ChunkHandle>,
    cancel_token: CancellationToken,
) -> Result<()> {
    info!("Scheduler: starting link fetch and dispatch loop");

    let mut next_chunk_index = 0i64;
    let mut next_row_offset = 0i64;

    loop {
        // Check cancellation before each batch fetch
        if cancel_token.is_cancelled() {
            info!("Scheduler: cancellation requested, stopping");
            break;
        }

        // Fetch next batch of links (server-determined batch size)
        debug!(
            "Scheduler: fetching links starting from chunk_index={}, row_offset={}",
            next_chunk_index, next_row_offset
        );

        let fetch_result = tokio::select! {
            result = link_fetcher.fetch_links(next_chunk_index, next_row_offset) => {
                match result {
                    Ok(r) => r,
                    Err(e) => {
                        error!("Scheduler: failed to fetch links: {}", e);
                        return Err(e);
                    }
                }
            }
            _ = cancel_token.cancelled() => {
                info!("Scheduler: cancellation requested during fetch, stopping");
                break;
            }
        };

        let links = fetch_result.links;
        let has_more = fetch_result.has_more;

        debug!(
            "Scheduler: fetched {} links, has_more={}",
            links.len(),
            has_more
        );

        // Process each link in the batch
        for link in links {
            // Check cancellation before processing each link
            if cancel_token.is_cancelled() {
                info!("Scheduler: cancellation requested, stopping");
                return Ok(());
            }

            let chunk_index = link.chunk_index;
            let row_offset = link.row_offset;

            // Create oneshot channel pair for this chunk
            let (result_tx, result_rx) = oneshot::channel();

            // Send ChunkDownloadTask to download_channel (MPMC, bounded)
            let task = ChunkDownloadTask {
                chunk_index,
                link,
                result_tx,
            };

            tokio::select! {
                result = download_channel.send(task) => {
                    if let Err(e) = result {
                        error!("Scheduler: failed to send download task for chunk {}: {}", chunk_index, e);
                        return Err(DatabricksErrorHelper::invalid_state()
                            .message(format!("Failed to send download task for chunk {}", chunk_index))
                            .context(format!("download_channel send error: {}", e)));
                    }
                }
                _ = cancel_token.cancelled() => {
                    info!("Scheduler: cancellation requested during download_channel send, stopping");
                    return Ok(());
                }
            }

            // Send ChunkHandle to result_channel (MPSC, bounded)
            // This provides backpressure - blocks when channel is full
            let handle = ChunkHandle {
                chunk_index,
                result_rx,
            };

            tokio::select! {
                result = result_channel.send(handle) => {
                    if let Err(e) = result {
                        error!("Scheduler: failed to send result handle for chunk {}: {}", chunk_index, e);
                        return Err(DatabricksErrorHelper::invalid_state()
                            .message(format!("Failed to send result handle for chunk {}", chunk_index))
                            .context(format!("result_channel send error: {}", e)));
                    }
                }
                _ = cancel_token.cancelled() => {
                    info!("Scheduler: cancellation requested during result_channel send, stopping");
                    return Ok(());
                }
            }

            debug!(
                "Scheduler: dispatched chunk {} (row_offset={})",
                chunk_index, row_offset
            );
        }

        // Update next indices for next batch fetch
        if let Some(next_idx) = fetch_result.next_chunk_index {
            next_chunk_index = next_idx;
        }
        if let Some(next_offset) = fetch_result.next_row_offset {
            next_row_offset = next_offset;
        }

        // Exit loop if no more links
        if !has_more {
            info!("Scheduler: no more links to fetch (has_more=false), stopping");
            break;
        }
    }

    info!("Scheduler: completed successfully");
    Ok(())
}
