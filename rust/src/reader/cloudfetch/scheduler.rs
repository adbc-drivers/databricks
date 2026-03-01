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

//! Scheduler for the CloudFetch download pipeline.
//!
//! The scheduler fetches chunk links from a [`ChunkLinkFetcher`] and creates oneshot
//! channel pairs for each chunk. It sends [`ChunkHandle`] to the result channel first
//! (preserving ordering) and then sends [`ChunkDownloadTask`] to the download channel.
//!
//! ## Pipeline Architecture
//!
//! ```text
//! [ChunkLinkFetcher] --> [Scheduler] --> download_channel --> [Download Workers]
//!                              |
//!                              +--> result_channel --> [Consumer]
//! ```
//!
//! ## Backpressure
//!
//! The bounded `result_channel` provides automatic backpressure. When the channel
//! is full (i.e., `max_chunks_in_memory` handles are awaiting consumption), the
//! scheduler blocks on the send operation until the consumer reads a handle.
//!
//! ## Ordering Invariant
//!
//! The scheduler always sends `ChunkHandle` to `result_channel` BEFORE sending
//! `ChunkDownloadTask` to `download_channel`. This ensures that:
//! 1. The consumer receives handles in chunk-index order
//! 2. Even if downloads complete out of order, results are consumed in order

use crate::reader::cloudfetch::link_fetcher::ChunkLinkFetcher;
use crate::reader::cloudfetch::pipeline_types::{
    create_chunk_pair, ChunkDownloadTask, ChunkHandle,
};
use crate::types::cloudfetch::CloudFetchConfig;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, trace};

/// Channels returned by the scheduler for consumption by other pipeline components.
///
/// The scheduler spawns a background task that fetches links and populates these channels.
/// - `download_rx`: Download workers pull `ChunkDownloadTask` from this channel
/// - `result_rx`: Consumer pulls `ChunkHandle` from this channel in chunk-index order
pub struct SchedulerChannels {
    /// Receiver for download tasks. Download workers should pull from this.
    pub download_rx: mpsc::UnboundedReceiver<ChunkDownloadTask>,
    /// Receiver for chunk handles. Consumer should pull from this in order.
    pub result_rx: mpsc::Receiver<ChunkHandle>,
    /// Handle to the scheduler task for lifecycle management.
    pub scheduler_handle: JoinHandle<()>,
}

/// Spawns the scheduler task and returns the pipeline channels.
///
/// The scheduler task runs until either:
/// - All chunks have been fetched and enqueued (`has_more` becomes false)
/// - The cancellation token is triggered
///
/// # Arguments
///
/// * `link_fetcher` - The chunk link fetcher to use for fetching links
/// * `config` - CloudFetch configuration (used for `max_chunks_in_memory`)
/// * `cancel_token` - Cancellation token to stop the scheduler
///
/// # Returns
///
/// A [`SchedulerChannels`] struct containing:
/// - `download_rx`: Unbounded receiver for download workers
/// - `result_rx`: Bounded receiver (capacity = `max_chunks_in_memory`) for consumer
/// - `scheduler_handle`: JoinHandle for the scheduler task
///
/// # Example
///
/// ```ignore
/// let link_fetcher = Arc::new(my_fetcher);
/// let config = CloudFetchConfig::default();
/// let cancel_token = CancellationToken::new();
///
/// let channels = spawn_scheduler(link_fetcher, &config, cancel_token.clone());
///
/// // Start download workers pulling from channels.download_rx
/// // Consumer reads from channels.result_rx
/// ```
pub fn spawn_scheduler(
    link_fetcher: Arc<dyn ChunkLinkFetcher>,
    config: &CloudFetchConfig,
    cancel_token: CancellationToken,
) -> SchedulerChannels {
    // download_channel is unbounded - backpressure comes from result_channel
    let (download_tx, download_rx) = mpsc::unbounded_channel::<ChunkDownloadTask>();

    // result_channel is bounded to max_chunks_in_memory for automatic backpressure
    let (result_tx, result_rx) = mpsc::channel::<ChunkHandle>(config.max_chunks_in_memory);

    debug!(
        "Spawning scheduler with max_chunks_in_memory={}",
        config.max_chunks_in_memory
    );

    let scheduler_handle = tokio::spawn(scheduler_task(
        link_fetcher,
        download_tx,
        result_tx,
        cancel_token,
    ));

    SchedulerChannels {
        download_rx,
        result_rx,
        scheduler_handle,
    }
}

/// The main scheduler task that fetches links and populates the pipeline channels.
///
/// This function runs in a loop, fetching batches of links from the `ChunkLinkFetcher`
/// and creating oneshot channel pairs for each chunk. It exits when either:
/// - `has_more` is false (all chunks have been fetched)
/// - The cancellation token is triggered
///
/// # Ordering Invariant
///
/// For each link, the scheduler ALWAYS sends `ChunkHandle` to `result_tx` FIRST,
/// before sending `ChunkDownloadTask` to `download_tx`. This ensures consumers
/// receive handles in chunk-index order.
///
/// # Arguments
///
/// * `link_fetcher` - Source of chunk links
/// * `download_tx` - Unbounded sender for download tasks
/// * `result_tx` - Bounded sender for chunk handles (provides backpressure)
/// * `cancel_token` - Token to cancel the scheduler
async fn scheduler_task(
    link_fetcher: Arc<dyn ChunkLinkFetcher>,
    download_tx: mpsc::UnboundedSender<ChunkDownloadTask>,
    result_tx: mpsc::Sender<ChunkHandle>,
    cancel_token: CancellationToken,
) {
    let mut next_chunk_index: i64 = 0;
    let mut next_row_offset: i64 = 0;

    debug!("Scheduler task started");

    loop {
        // Check for cancellation before fetching
        if cancel_token.is_cancelled() {
            debug!("Scheduler cancelled before fetch");
            break;
        }

        // Fetch the next batch of links
        let fetch_result = tokio::select! {
            _ = cancel_token.cancelled() => {
                debug!("Scheduler cancelled during fetch");
                break;
            }
            result = link_fetcher.fetch_links(next_chunk_index, next_row_offset) => {
                result
            }
        };

        let batch = match fetch_result {
            Ok(batch) => batch,
            Err(e) => {
                error!("Scheduler: failed to fetch links: {}", e);
                // On error, we stop the scheduler. The consumer will see
                // the channel close and can report the error appropriately.
                break;
            }
        };

        trace!(
            "Scheduler: fetched {} links starting at chunk {}, has_more={}",
            batch.links.len(),
            next_chunk_index,
            batch.has_more
        );

        // Process each link in the batch
        for link in batch.links {
            // Check for cancellation before processing each link
            if cancel_token.is_cancelled() {
                debug!("Scheduler cancelled during batch processing");
                return;
            }

            let chunk_index = link.chunk_index;

            // Create the oneshot channel pair
            let (task, handle) = create_chunk_pair(chunk_index, link);

            // CRITICAL: Send ChunkHandle to result_channel FIRST
            // This preserves the ordering invariant - handles are always sent
            // in chunk-index order, regardless of download completion order.
            //
            // The bounded result_channel provides backpressure here - if the
            // channel is full, we block until the consumer reads a handle.
            let send_result = tokio::select! {
                _ = cancel_token.cancelled() => {
                    debug!("Scheduler cancelled while sending handle for chunk {}", chunk_index);
                    return;
                }
                result = result_tx.send(handle) => {
                    result
                }
            };

            if send_result.is_err() {
                // Receiver dropped - consumer is gone
                debug!(
                    "Scheduler: result channel closed, stopping at chunk {}",
                    chunk_index
                );
                return;
            }

            // Then send ChunkDownloadTask to download_channel (unbounded, won't block)
            if download_tx.send(task).is_err() {
                // Receiver dropped - all workers are gone
                debug!(
                    "Scheduler: download channel closed, stopping at chunk {}",
                    chunk_index
                );
                return;
            }

            trace!(
                "Scheduler: enqueued chunk {} (handle first, then task)",
                chunk_index
            );
        }

        // Check if we're done
        if !batch.has_more {
            debug!("Scheduler: no more chunks, exiting");
            break;
        }

        // Update indices for next fetch
        if let Some(next_idx) = batch.next_chunk_index {
            next_chunk_index = next_idx;
        }
        if let Some(next_offset) = batch.next_row_offset {
            next_row_offset = next_offset;
        }
    }

    debug!("Scheduler task completed");
    // Channels are dropped here, signaling end of stream to workers and consumer
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ChunkLinkFetchResult;
    use crate::error::Result;
    use crate::types::cloudfetch::CloudFetchLink;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::time::timeout;

    /// Helper function to create a test CloudFetchLink
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

    /// Mock ChunkLinkFetcher that returns predefined batches
    #[derive(Debug)]
    struct MockLinkFetcher {
        /// Batches to return, indexed by starting chunk index
        batches: Vec<(Vec<CloudFetchLink>, bool)>, // (links, has_more)
        /// Current batch index
        current_batch: AtomicUsize,
        /// Track number of fetch_links calls
        fetch_count: AtomicUsize,
    }

    impl MockLinkFetcher {
        fn new(batches: Vec<(Vec<CloudFetchLink>, bool)>) -> Self {
            Self {
                batches,
                current_batch: AtomicUsize::new(0),
                fetch_count: AtomicUsize::new(0),
            }
        }

        /// Create a fetcher that returns a single batch with the given links
        fn single_batch(links: Vec<CloudFetchLink>, has_more: bool) -> Self {
            Self::new(vec![(links, has_more)])
        }

        /// Create a fetcher that returns empty results (end of stream)
        fn empty() -> Self {
            Self::new(vec![(vec![], false)])
        }

        fn fetch_count(&self) -> usize {
            self.fetch_count.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl ChunkLinkFetcher for MockLinkFetcher {
        async fn fetch_links(
            &self,
            _start_chunk_index: i64,
            _start_row_offset: i64,
        ) -> Result<ChunkLinkFetchResult> {
            self.fetch_count.fetch_add(1, Ordering::Relaxed);

            let batch_idx = self.current_batch.fetch_add(1, Ordering::Relaxed);

            if batch_idx >= self.batches.len() {
                return Ok(ChunkLinkFetchResult::end_of_stream());
            }

            let (links, has_more) = &self.batches[batch_idx];

            let next_chunk_index = if *has_more {
                links.last().map(|l| l.chunk_index + 1)
            } else {
                None
            };

            Ok(ChunkLinkFetchResult {
                links: links.clone(),
                has_more: *has_more,
                next_chunk_index,
                next_row_offset: None,
            })
        }

        async fn refetch_link(&self, chunk_index: i64, _row_offset: i64) -> Result<CloudFetchLink> {
            Ok(create_test_link(chunk_index))
        }
    }

    /// Mock ChunkLinkFetcher that blocks until cancelled
    #[derive(Debug)]
    struct BlockingLinkFetcher {
        /// Notify when fetch_links is called
        fetch_started: tokio::sync::Notify,
    }

    impl BlockingLinkFetcher {
        fn new() -> Self {
            Self {
                fetch_started: tokio::sync::Notify::new(),
            }
        }
    }

    #[async_trait]
    impl ChunkLinkFetcher for BlockingLinkFetcher {
        async fn fetch_links(
            &self,
            _start_chunk_index: i64,
            _start_row_offset: i64,
        ) -> Result<ChunkLinkFetchResult> {
            self.fetch_started.notify_one();
            // Block forever - will be cancelled
            std::future::pending().await
        }

        async fn refetch_link(&self, chunk_index: i64, _row_offset: i64) -> Result<CloudFetchLink> {
            Ok(create_test_link(chunk_index))
        }
    }

    /// Mock ChunkLinkFetcher that tracks call order for testing backpressure
    #[derive(Debug)]
    struct BackpressureFetcher {
        /// Links to return per batch
        links_per_batch: usize,
        /// Total chunks to return
        total_chunks: i64,
        /// Track the next chunk to return
        next_chunk: AtomicI64,
        /// Track fetch_links calls
        fetch_count: AtomicUsize,
    }

    impl BackpressureFetcher {
        fn new(links_per_batch: usize, total_chunks: i64) -> Self {
            Self {
                links_per_batch,
                total_chunks,
                next_chunk: AtomicI64::new(0),
                fetch_count: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl ChunkLinkFetcher for BackpressureFetcher {
        async fn fetch_links(
            &self,
            _start_chunk_index: i64,
            _start_row_offset: i64,
        ) -> Result<ChunkLinkFetchResult> {
            self.fetch_count.fetch_add(1, Ordering::Relaxed);

            let start = self.next_chunk.load(Ordering::Relaxed);

            if start >= self.total_chunks {
                return Ok(ChunkLinkFetchResult::end_of_stream());
            }

            let end = (start + self.links_per_batch as i64).min(self.total_chunks);
            let links: Vec<CloudFetchLink> = (start..end).map(create_test_link).collect();

            self.next_chunk.store(end, Ordering::Relaxed);

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
            Ok(create_test_link(chunk_index))
        }
    }

    #[tokio::test]
    async fn scheduler_sends_handles_in_chunk_index_order() {
        // Create a fetcher that returns 5 links in one batch
        let links: Vec<CloudFetchLink> = (0..5).map(create_test_link).collect();
        let fetcher = Arc::new(MockLinkFetcher::single_batch(links, false));
        let config = CloudFetchConfig::default();
        let cancel_token = CancellationToken::new();

        let mut channels = spawn_scheduler(fetcher, &config, cancel_token);

        // Verify handles arrive in chunk-index order
        for expected_index in 0..5 {
            let handle = channels
                .result_rx
                .recv()
                .await
                .expect("Should receive handle");
            assert_eq!(
                handle.chunk_index, expected_index,
                "Handles should arrive in order"
            );
        }

        // No more handles
        assert!(channels.result_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn scheduler_processes_batch_links() {
        // Create a fetcher that returns 3 links in a batch
        let links: Vec<CloudFetchLink> = (0..3).map(create_test_link).collect();
        let fetcher = Arc::new(MockLinkFetcher::single_batch(links, false));
        let config = CloudFetchConfig::default();
        let cancel_token = CancellationToken::new();

        let mut channels = spawn_scheduler(fetcher.clone(), &config, cancel_token);

        // Collect all handles
        let mut handles = Vec::new();
        while let Some(handle) = channels.result_rx.recv().await {
            handles.push(handle);
        }

        assert_eq!(handles.len(), 3, "Should receive all 3 links as handles");

        // Verify all tasks were also sent
        let mut tasks = Vec::new();
        while let Ok(task) = channels.download_rx.try_recv() {
            tasks.push(task);
        }

        assert_eq!(tasks.len(), 3, "Should receive all 3 links as tasks");
    }

    #[tokio::test]
    async fn backpressure_blocks_scheduler_at_capacity() {
        // Create a fetcher that returns 10 links
        let fetcher = Arc::new(BackpressureFetcher::new(10, 10));

        // Use a small capacity to trigger backpressure
        let config = CloudFetchConfig {
            max_chunks_in_memory: 2,
            ..Default::default()
        };
        let cancel_token = CancellationToken::new();

        let mut channels = spawn_scheduler(fetcher, &config, cancel_token.clone());

        // Give scheduler time to fill the channel
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The scheduler should be blocked after sending 2 handles (channel capacity)
        // Read one handle to unblock
        let _handle1 = channels
            .result_rx
            .recv()
            .await
            .expect("Should receive first handle");

        // Give scheduler time to send another
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Read all remaining handles
        let mut count = 1; // Already read one
        while let Ok(Some(_)) = timeout(Duration::from_millis(100), channels.result_rx.recv()).await
        {
            count += 1;
        }

        // Should have received all 10 handles
        assert_eq!(count, 10, "Should receive all handles eventually");

        cancel_token.cancel();
    }

    #[tokio::test]
    async fn scheduler_exits_when_has_more_false() {
        // Create a fetcher that returns 2 links with has_more=false
        let links: Vec<CloudFetchLink> = (0..2).map(create_test_link).collect();
        let fetcher = Arc::new(MockLinkFetcher::single_batch(links, false));
        let config = CloudFetchConfig::default();
        let cancel_token = CancellationToken::new();

        let mut channels = spawn_scheduler(fetcher.clone(), &config, cancel_token);

        // Read all handles
        let mut count = 0;
        while channels.result_rx.recv().await.is_some() {
            count += 1;
        }

        assert_eq!(count, 2);

        // Scheduler should have exited cleanly
        let result = timeout(Duration::from_millis(100), channels.scheduler_handle).await;
        assert!(
            result.is_ok(),
            "Scheduler should exit when has_more is false"
        );

        // Verify fetch_links was called exactly once
        assert_eq!(fetcher.fetch_count(), 1);
    }

    #[tokio::test]
    async fn scheduler_cancellation() {
        // Create a fetcher that blocks forever
        let fetcher = Arc::new(BlockingLinkFetcher::new());
        let config = CloudFetchConfig::default();
        let cancel_token = CancellationToken::new();

        let channels = spawn_scheduler(fetcher.clone(), &config, cancel_token.clone());

        // Wait for scheduler to start fetching
        fetcher.fetch_started.notified().await;

        // Cancel the scheduler
        cancel_token.cancel();

        // Scheduler should exit promptly
        let result = timeout(Duration::from_millis(500), channels.scheduler_handle).await;
        assert!(
            result.is_ok(),
            "Scheduler should exit when cancelled during fetch"
        );
    }

    #[tokio::test]
    async fn scheduler_handle_before_task() {
        // This test verifies the ordering invariant: handle is sent before task
        // We do this by checking that we can receive a handle even if we haven't
        // processed any download tasks yet.

        let links: Vec<CloudFetchLink> = (0..3).map(create_test_link).collect();
        let fetcher = Arc::new(MockLinkFetcher::single_batch(links, false));
        let config = CloudFetchConfig::default();
        let cancel_token = CancellationToken::new();

        let mut channels = spawn_scheduler(fetcher, &config, cancel_token);

        // Immediately receive handles without touching download_rx
        // This proves handles are sent first and independently
        let handle0 = channels
            .result_rx
            .recv()
            .await
            .expect("Should get handle 0");
        let handle1 = channels
            .result_rx
            .recv()
            .await
            .expect("Should get handle 1");
        let handle2 = channels
            .result_rx
            .recv()
            .await
            .expect("Should get handle 2");

        assert_eq!(handle0.chunk_index, 0);
        assert_eq!(handle1.chunk_index, 1);
        assert_eq!(handle2.chunk_index, 2);

        // Now verify tasks are also available
        let task0 = channels
            .download_rx
            .recv()
            .await
            .expect("Should get task 0");
        let task1 = channels
            .download_rx
            .recv()
            .await
            .expect("Should get task 1");
        let task2 = channels
            .download_rx
            .recv()
            .await
            .expect("Should get task 2");

        assert_eq!(task0.chunk_index, 0);
        assert_eq!(task1.chunk_index, 1);
        assert_eq!(task2.chunk_index, 2);
    }

    #[tokio::test]
    async fn scheduler_multiple_batches() {
        // Create a fetcher that returns multiple batches
        let batch1: Vec<CloudFetchLink> = (0..3).map(create_test_link).collect();
        let batch2: Vec<CloudFetchLink> = (3..6).map(create_test_link).collect();
        let batch3: Vec<CloudFetchLink> = (6..8).map(create_test_link).collect();

        let fetcher = Arc::new(MockLinkFetcher::new(vec![
            (batch1, true),  // has_more = true
            (batch2, true),  // has_more = true
            (batch3, false), // has_more = false (last batch)
        ]));
        let config = CloudFetchConfig::default();
        let cancel_token = CancellationToken::new();

        let mut channels = spawn_scheduler(fetcher.clone(), &config, cancel_token);

        // Collect all handles
        let mut indices = Vec::new();
        while let Some(handle) = channels.result_rx.recv().await {
            indices.push(handle.chunk_index);
        }

        // Should have received all 8 chunks in order
        assert_eq!(indices, vec![0, 1, 2, 3, 4, 5, 6, 7]);

        // Verify all 3 batches were fetched
        assert_eq!(fetcher.fetch_count(), 3);
    }

    #[tokio::test]
    async fn scheduler_empty_batch() {
        // Create a fetcher that returns an empty batch (end of stream)
        let fetcher = Arc::new(MockLinkFetcher::empty());
        let config = CloudFetchConfig::default();
        let cancel_token = CancellationToken::new();

        let mut channels = spawn_scheduler(fetcher.clone(), &config, cancel_token);

        // Should receive no handles
        let handle = channels.result_rx.recv().await;
        assert!(handle.is_none(), "Empty batch should result in no handles");

        // Scheduler should have exited
        let result = timeout(Duration::from_millis(100), channels.scheduler_handle).await;
        assert!(result.is_ok(), "Scheduler should exit on empty batch");
    }

    #[tokio::test]
    async fn scheduler_cancellation_during_batch_processing() {
        // Create a fetcher that returns many links to give us time to cancel mid-batch
        let fetcher = Arc::new(BackpressureFetcher::new(100, 100));

        // Use capacity of 1 to ensure we can cancel between chunks
        let config = CloudFetchConfig {
            max_chunks_in_memory: 1,
            ..Default::default()
        };
        let cancel_token = CancellationToken::new();

        let mut channels = spawn_scheduler(fetcher, &config, cancel_token.clone());

        // Read a few handles
        let _ = channels.result_rx.recv().await;
        let _ = channels.result_rx.recv().await;

        // Cancel mid-stream
        cancel_token.cancel();

        // Scheduler should exit promptly
        let result = timeout(Duration::from_millis(500), channels.scheduler_handle).await;
        assert!(
            result.is_ok(),
            "Scheduler should exit when cancelled during batch"
        );
    }

    #[tokio::test]
    async fn scheduler_result_channel_closed_stops_scheduler() {
        let links: Vec<CloudFetchLink> = (0..10).map(create_test_link).collect();
        let fetcher = Arc::new(MockLinkFetcher::single_batch(links, false));
        let config = CloudFetchConfig::default();
        let cancel_token = CancellationToken::new();

        let channels = spawn_scheduler(fetcher, &config, cancel_token);

        // Drop the result receiver - this should cause scheduler to exit
        drop(channels.result_rx);

        // Scheduler should exit promptly
        let result = timeout(Duration::from_millis(500), channels.scheduler_handle).await;
        assert!(
            result.is_ok(),
            "Scheduler should exit when result channel is closed"
        );
    }

    #[tokio::test]
    async fn scheduler_download_channel_closed_stops_scheduler() {
        let links: Vec<CloudFetchLink> = (0..10).map(create_test_link).collect();
        let fetcher = Arc::new(MockLinkFetcher::single_batch(links, false));
        let config = CloudFetchConfig {
            max_chunks_in_memory: 100, // Large enough to not block on result_tx
            ..Default::default()
        };
        let cancel_token = CancellationToken::new();

        let channels = spawn_scheduler(fetcher, &config, cancel_token);

        // Drop the download receiver - this should cause scheduler to exit
        drop(channels.download_rx);

        // Give scheduler time to detect the closed channel
        let result = timeout(Duration::from_millis(500), channels.scheduler_handle).await;
        assert!(
            result.is_ok(),
            "Scheduler should exit when download channel is closed"
        );
    }
}
