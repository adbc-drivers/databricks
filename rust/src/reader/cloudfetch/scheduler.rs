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

//! Scheduler task for the CloudFetch download pipeline.
//!
//! The scheduler orchestrates chunk downloads by:
//! 1. Calling [`ChunkLinkFetcher::fetch_links()`] to get batches of [`CloudFetchLink`] values
//! 2. Creating a oneshot channel pair per link via [`create_chunk_pair()`]
//! 3. Sending [`ChunkHandle`] to the bounded `result_channel` (preserving order)
//! 4. Sending [`ChunkDownloadTask`] to the unbounded `download_channel`
//! 5. Looping until `has_more = false` or cancellation
//!
//! The bounded `result_channel` provides automatic backpressure — when
//! `max_chunks_in_memory` handles are buffered, the scheduler blocks until the
//! consumer drains some handles.
//!
//! **Key invariant:** [`ChunkHandle`] is enqueued to `result_channel` *before*
//! the corresponding [`ChunkDownloadTask`] is dispatched to `download_channel`,
//! preserving sequential ordering even when downloads complete out of order.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use super::link_fetcher::ChunkLinkFetcher;
use super::pipeline_types::{create_chunk_pair, ChunkDownloadTask, ChunkHandle};

/// Channels returned by [`spawn_scheduler()`].
///
/// Contains the receiving ends of both pipeline channels. The caller uses:
/// - `result_rx` to consume [`ChunkHandle`] values in chunk-index order
/// - `download_rx` to feed [`ChunkDownloadTask`] values to download workers
pub struct SchedulerChannels {
    /// Receiver for chunk handles (bounded, in chunk-index order).
    pub result_rx: mpsc::Receiver<ChunkHandle>,
    /// Receiver for download tasks (unbounded).
    pub download_rx: mpsc::UnboundedReceiver<ChunkDownloadTask>,
    /// Join handle for the spawned scheduler task.
    pub join_handle: JoinHandle<()>,
}

/// Spawns the scheduler task and returns the pipeline channels.
///
/// Creates:
/// - A **bounded** `result_channel` with capacity `max_chunks_in_memory`
/// - An **unbounded** `download_channel`
///
/// The scheduler task is spawned via [`tokio::spawn`] and runs until all links
/// have been fetched or the [`CancellationToken`] is triggered.
///
/// # Arguments
///
/// * `link_fetcher` - The fetcher used to retrieve batches of chunk links.
/// * `max_chunks_in_memory` - Capacity of the bounded result channel.
/// * `cancel_token` - Token for cooperative cancellation.
pub fn spawn_scheduler(
    link_fetcher: Arc<dyn ChunkLinkFetcher>,
    max_chunks_in_memory: usize,
    cancel_token: CancellationToken,
) -> SchedulerChannels {
    let (result_tx, result_rx) = mpsc::channel::<ChunkHandle>(max_chunks_in_memory);
    let (download_tx, download_rx) = mpsc::unbounded_channel::<ChunkDownloadTask>();

    let join_handle = tokio::spawn(scheduler_task(
        link_fetcher,
        result_tx,
        download_tx,
        cancel_token,
    ));

    SchedulerChannels {
        result_rx,
        download_rx,
        join_handle,
    }
}

/// The scheduler task loop.
///
/// Fetches batches of chunk links from the [`ChunkLinkFetcher`], creates a
/// oneshot pair for each link, and dispatches the handle and task to the
/// respective channels. Exits when `has_more` is `false` or the cancellation
/// token fires.
async fn scheduler_task(
    link_fetcher: Arc<dyn ChunkLinkFetcher>,
    result_tx: mpsc::Sender<ChunkHandle>,
    download_tx: mpsc::UnboundedSender<ChunkDownloadTask>,
    cancel_token: CancellationToken,
) {
    let mut next_chunk_index: i64 = 0;
    let mut next_row_offset: i64 = 0;

    loop {
        // Check cancellation before fetching
        if cancel_token.is_cancelled() {
            debug!("Scheduler: cancelled before fetch_links");
            break;
        }

        // Fetch next batch of links
        let fetch_result = tokio::select! {
            _ = cancel_token.cancelled() => {
                debug!("Scheduler: cancelled during fetch_links");
                break;
            }
            result = link_fetcher.fetch_links(next_chunk_index, next_row_offset) => result,
        };

        let batch = match fetch_result {
            Ok(result) => result,
            Err(e) => {
                error!("Scheduler: fetch_links failed: {}", e);
                break;
            }
        };

        debug!(
            "Scheduler: fetched {} links starting at chunk {}, has_more={}",
            batch.links.len(),
            next_chunk_index,
            batch.has_more,
        );

        // Process each link in the batch
        for link in batch.links {
            // Check cancellation before processing each link
            if cancel_token.is_cancelled() {
                debug!("Scheduler: cancelled during batch processing");
                return;
            }

            let chunk_index = link.chunk_index;
            let row_count = link.row_count;
            let (task, handle) = create_chunk_pair(chunk_index, link);

            // KEY INVARIANT: Send ChunkHandle to result_channel BEFORE
            // sending ChunkDownloadTask to download_channel.
            // This preserves sequential ordering for the consumer.
            let send_result = tokio::select! {
                _ = cancel_token.cancelled() => {
                    debug!("Scheduler: cancelled while sending handle for chunk {}", chunk_index);
                    return;
                }
                result = result_tx.send(handle) => result,
            };

            if send_result.is_err() {
                debug!(
                    "Scheduler: result_channel closed, stopping (chunk {})",
                    chunk_index
                );
                return;
            }

            // Send task to download channel (unbounded, never blocks)
            if download_tx.send(task).is_err() {
                debug!(
                    "Scheduler: download_channel closed, stopping (chunk {})",
                    chunk_index
                );
                return;
            }

            debug!(
                "Scheduler: dispatched chunk {} (rows={})",
                chunk_index, row_count,
            );

            // Advance tracking
            next_chunk_index = chunk_index + 1;
            next_row_offset += row_count;
        }

        // Update next indices from batch metadata if provided
        if let Some(idx) = batch.next_chunk_index {
            next_chunk_index = idx;
        }
        if let Some(offset) = batch.next_row_offset {
            next_row_offset = offset;
        }

        // Exit when no more links to fetch
        if !batch.has_more {
            debug!("Scheduler: no more links (has_more=false), exiting");
            break;
        }
    }

    debug!("Scheduler: task exiting, dropping channel senders");
    // Senders are dropped here, signaling end-of-stream to receivers
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ChunkLinkFetchResult;
    use crate::error::Result;
    use crate::types::cloudfetch::CloudFetchLink;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{timeout, Duration};

    /// Helper to create a test CloudFetchLink.
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

    /// Mock fetcher that returns pre-configured batches.
    #[derive(Debug)]
    struct MockLinkFetcher {
        /// Each element is a batch of links returned by successive fetch_links() calls.
        batches: Vec<(Vec<CloudFetchLink>, bool)>, // (links, has_more)
        call_count: AtomicUsize,
    }

    impl MockLinkFetcher {
        fn new(batches: Vec<(Vec<CloudFetchLink>, bool)>) -> Self {
            Self {
                batches,
                call_count: AtomicUsize::new(0),
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
            let idx = self.call_count.fetch_add(1, Ordering::SeqCst);
            if idx >= self.batches.len() {
                return Ok(ChunkLinkFetchResult {
                    links: vec![],
                    has_more: false,
                    next_chunk_index: None,
                    next_row_offset: None,
                });
            }
            let (links, has_more) = &self.batches[idx];
            Ok(ChunkLinkFetchResult {
                links: links.clone(),
                has_more: *has_more,
                next_chunk_index: None,
                next_row_offset: None,
            })
        }

        async fn refetch_link(
            &self,
            _chunk_index: i64,
            _row_offset: i64,
        ) -> Result<CloudFetchLink> {
            unimplemented!("refetch_link not needed for scheduler tests")
        }
    }

    #[tokio::test]
    async fn scheduler_sends_handles_in_chunk_index_order() {
        let links: Vec<CloudFetchLink> = (0..5).map(test_link).collect();
        let fetcher = Arc::new(MockLinkFetcher::new(vec![(links, false)]));
        let cancel_token = CancellationToken::new();

        let mut channels = spawn_scheduler(fetcher, 10, cancel_token);

        // Verify handles arrive in chunk-index order
        for expected_idx in 0..5 {
            let handle = channels
                .result_rx
                .recv()
                .await
                .expect("should receive handle");
            assert_eq!(handle.chunk_index, expected_idx);
        }

        // Channel should be closed after scheduler exits
        assert!(channels.result_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn scheduler_processes_batch_links() {
        let links: Vec<CloudFetchLink> = (0..3).map(test_link).collect();
        let fetcher = Arc::new(MockLinkFetcher::new(vec![(links, false)]));
        let cancel_token = CancellationToken::new();

        let mut channels = spawn_scheduler(fetcher, 10, cancel_token);

        // Collect all handles
        let mut handles = Vec::new();
        while let Some(handle) = channels.result_rx.recv().await {
            handles.push(handle);
        }
        assert_eq!(handles.len(), 3);

        // Collect all download tasks
        let mut tasks = Vec::new();
        while let Ok(task) = channels.download_rx.try_recv() {
            tasks.push(task);
        }
        assert_eq!(tasks.len(), 3);

        // Verify ordering
        for (i, task) in tasks.iter().enumerate() {
            assert_eq!(task.chunk_index, i as i64);
        }
    }

    #[tokio::test]
    async fn backpressure_blocks_scheduler_at_capacity() {
        // Create 10 links but result_channel capacity is only 2
        let links: Vec<CloudFetchLink> = (0..10).map(test_link).collect();
        let fetcher = Arc::new(MockLinkFetcher::new(vec![(links, false)]));
        let cancel_token = CancellationToken::new();

        let mut channels = spawn_scheduler(fetcher, 2, cancel_token);

        // Give scheduler time to fill the channel
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The scheduler should be blocked — only 2 handles in the channel.
        // Drain one handle at a time and verify we can keep getting more,
        // proving the scheduler is feeding them as space opens up.
        let mut received = 0;
        while let Some(_handle) = timeout(Duration::from_millis(200), channels.result_rx.recv())
            .await
            .ok()
            .flatten()
        {
            received += 1;
        }

        // We should receive all 10 handles eventually
        assert_eq!(received, 10);
    }

    #[tokio::test]
    async fn scheduler_exits_when_has_more_false() {
        let links = vec![test_link(0), test_link(1)];
        let fetcher = Arc::new(MockLinkFetcher::new(vec![(links, false)]));
        let cancel_token = CancellationToken::new();

        let mut channels = spawn_scheduler(fetcher, 10, cancel_token);

        // Drain all handles
        let mut count = 0;
        while let Some(_) = channels.result_rx.recv().await {
            count += 1;
        }
        assert_eq!(count, 2);

        // Scheduler task should have completed
        let join_result = timeout(Duration::from_millis(100), channels.join_handle).await;
        assert!(join_result.is_ok(), "scheduler should exit cleanly");
    }

    #[tokio::test]
    async fn scheduler_cancellation() {
        // Use a fetcher that blocks forever on the second call
        #[derive(Debug)]
        struct BlockingFetcher;

        #[async_trait]
        impl ChunkLinkFetcher for BlockingFetcher {
            async fn fetch_links(
                &self,
                start_chunk_index: i64,
                _start_row_offset: i64,
            ) -> Result<ChunkLinkFetchResult> {
                if start_chunk_index == 0 {
                    Ok(ChunkLinkFetchResult {
                        links: vec![test_link(0)],
                        has_more: true,
                        next_chunk_index: Some(1),
                        next_row_offset: None,
                    })
                } else {
                    // Block forever — cancellation should interrupt
                    futures::future::pending().await
                }
            }

            async fn refetch_link(
                &self,
                _chunk_index: i64,
                _row_offset: i64,
            ) -> Result<CloudFetchLink> {
                unimplemented!()
            }
        }

        let fetcher = Arc::new(BlockingFetcher);
        let cancel_token = CancellationToken::new();

        let mut channels = spawn_scheduler(fetcher, 10, cancel_token.clone());

        // Read the first handle
        let handle = timeout(Duration::from_millis(200), channels.result_rx.recv())
            .await
            .expect("should receive first handle")
            .expect("channel should not be closed");
        assert_eq!(handle.chunk_index, 0);

        // Cancel the scheduler
        cancel_token.cancel();

        // Scheduler should exit promptly
        let join_result = timeout(Duration::from_millis(200), channels.join_handle).await;
        assert!(
            join_result.is_ok(),
            "scheduler should exit after cancellation"
        );

        // Result channel should be closed
        assert!(channels.result_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn scheduler_handle_before_task() {
        // Verify that for each chunk, the ChunkHandle is sent to result_channel
        // BEFORE the corresponding ChunkDownloadTask is sent to download_channel.
        //
        // Strategy: Use a result_channel capacity of 1 so the scheduler blocks
        // after each handle send. This makes the ordering observable: when we
        // drain one handle, the scheduler can proceed to send the task and then
        // the next handle. After draining each handle, the download_rx should
        // have at most as many tasks as handles we've consumed.
        let links: Vec<CloudFetchLink> = (0..5).map(test_link).collect();
        let fetcher = Arc::new(MockLinkFetcher::new(vec![(links, false)]));
        let cancel_token = CancellationToken::new();

        // Capacity = 1 forces scheduler to block after each handle send
        let mut channels = spawn_scheduler(fetcher, 1, cancel_token);

        for expected_idx in 0..5i64 {
            // Receive one handle — the scheduler was blocked waiting for space
            let handle = timeout(Duration::from_millis(500), channels.result_rx.recv())
                .await
                .expect("should not time out")
                .expect("channel should not be closed");
            assert_eq!(handle.chunk_index, expected_idx);

            // Give the scheduler a moment to send the corresponding task
            tokio::task::yield_now().await;

            // The download_rx should have at most one task available
            // (the one matching this handle) because the scheduler blocks
            // on result_tx.send() before it can proceed to the next link.
            let task = channels
                .download_rx
                .try_recv()
                .expect("download task should be available");
            assert_eq!(
                task.chunk_index, expected_idx,
                "task chunk_index should match handle chunk_index"
            );
        }

        // Channel should be closed
        assert!(channels.result_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn scheduler_multiple_batches() {
        let batch1 = (vec![test_link(0), test_link(1)], true); // has_more = true
        let batch2 = (vec![test_link(2), test_link(3)], true); // has_more = true
        let batch3 = (vec![test_link(4)], false); // has_more = false

        let fetcher = Arc::new(MockLinkFetcher::new(vec![batch1, batch2, batch3]));
        let cancel_token = CancellationToken::new();

        let mut channels = spawn_scheduler(fetcher, 10, cancel_token);

        // Collect all handles
        let mut handles = Vec::new();
        while let Some(handle) = channels.result_rx.recv().await {
            handles.push(handle);
        }

        assert_eq!(handles.len(), 5);
        for (i, handle) in handles.iter().enumerate() {
            assert_eq!(handle.chunk_index, i as i64);
        }
    }

    #[tokio::test]
    async fn scheduler_empty_batch() {
        // First batch is empty but has_more=true, second batch has links, third is done
        let batch1 = (vec![], true);
        let batch2 = (vec![test_link(0), test_link(1)], false);

        let fetcher = Arc::new(MockLinkFetcher::new(vec![batch1, batch2]));
        let cancel_token = CancellationToken::new();

        let mut channels = spawn_scheduler(fetcher, 10, cancel_token);

        // Collect all handles
        let mut handles = Vec::new();
        while let Some(handle) = channels.result_rx.recv().await {
            handles.push(handle);
        }

        // Should get 2 handles from the non-empty batch
        assert_eq!(handles.len(), 2);
        assert_eq!(handles[0].chunk_index, 0);
        assert_eq!(handles[1].chunk_index, 1);
    }
}
