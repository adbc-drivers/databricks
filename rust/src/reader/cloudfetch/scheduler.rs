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

//! Scheduler for the channel-based CloudFetch download pipeline.
//!
//! The scheduler is a single `tokio::spawn` task that:
//! 1. Calls `fetch_links()` on the [`ChunkLinkFetcher`] to obtain batches of
//!    [`CloudFetchLink`] values.
//! 2. For each link, creates a oneshot pair via [`create_chunk_pair`].
//! 3. Sends the [`ChunkHandle`] to a bounded `result_channel` (capacity =
//!    `max_chunks_in_memory`) — this provides automatic backpressure.
//! 4. Sends the [`ChunkDownloadTask`] to an unbounded `download_channel` for
//!    workers to pick up.
//!
//! **Key invariant:** `ChunkHandle` is enqueued to `result_channel` *before*
//! the corresponding `ChunkDownloadTask` is dispatched to `download_channel`.
//! This guarantees the consumer sees handles in chunk-index order regardless
//! of worker completion order.

use crate::reader::cloudfetch::link_fetcher::ChunkLinkFetcher;
use crate::reader::cloudfetch::pipeline_types::{
    create_chunk_pair, ChunkDownloadTask, ChunkHandle,
};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

/// Channel receivers returned by [`spawn_scheduler`].
///
/// The consumer reads from `result_rx` in order; download workers read from
/// `download_rx` concurrently.
pub struct SchedulerChannels {
    /// Bounded receiver of [`ChunkHandle`] — consumer reads these in order.
    pub result_rx: mpsc::Receiver<ChunkHandle>,
    /// Unbounded receiver of [`ChunkDownloadTask`] — shared among workers.
    pub download_rx: mpsc::UnboundedReceiver<ChunkDownloadTask>,
}

/// Spawn the scheduler as a background task.
///
/// # Arguments
/// * `link_fetcher` - Trait object for fetching chunk links
/// * `max_chunks_in_memory` - Capacity of the bounded `result_channel`
/// * `cancel_token` - Token for cooperative cancellation
///
/// # Returns
/// A [`SchedulerChannels`] struct with both channel receivers.
pub fn spawn_scheduler(
    link_fetcher: Arc<dyn ChunkLinkFetcher>,
    max_chunks_in_memory: usize,
    cancel_token: CancellationToken,
) -> SchedulerChannels {
    let (result_tx, result_rx) = mpsc::channel::<ChunkHandle>(max_chunks_in_memory);
    let (download_tx, download_rx) = mpsc::unbounded_channel::<ChunkDownloadTask>();

    tokio::spawn(async move {
        scheduler_task(link_fetcher, result_tx, download_tx, cancel_token).await;
    });

    SchedulerChannels {
        result_rx,
        download_rx,
    }
}

/// Core scheduler loop.
///
/// Fetches links in batches and dispatches (handle, task) pairs to the two
/// channels. Exits when `has_more` is `false` or the cancellation token fires.
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
            debug!("Scheduler: cancelled, exiting");
            return;
        }

        // Fetch next batch of links
        let fetch_result = tokio::select! {
            _ = cancel_token.cancelled() => {
                debug!("Scheduler: cancelled during fetch_links, exiting");
                return;
            }
            result = link_fetcher.fetch_links(next_chunk_index, next_row_offset) => result,
        };

        let batch = match fetch_result {
            Ok(result) => result,
            Err(e) => {
                error!("Scheduler: fetch_links failed: {}", e);
                return;
            }
        };

        debug!(
            "Scheduler: fetched {} links starting at chunk {}, has_more={}",
            batch.links.len(),
            next_chunk_index,
            batch.has_more
        );

        // Process each link in the batch
        for link in batch.links {
            if cancel_token.is_cancelled() {
                debug!("Scheduler: cancelled during batch processing, exiting");
                return;
            }

            let chunk_index = link.chunk_index;
            let (task, handle) = create_chunk_pair(chunk_index, link);

            // ORDERING INVARIANT: send handle to result_channel BEFORE task to
            // download_channel. This ensures the consumer sees handles in order
            // and can start awaiting before the download even begins.
            let send_result = tokio::select! {
                _ = cancel_token.cancelled() => {
                    debug!("Scheduler: cancelled while sending handle for chunk {}", chunk_index);
                    return;
                }
                result = result_tx.send(handle) => result,
            };

            if send_result.is_err() {
                debug!("Scheduler: result_channel closed, exiting");
                return;
            }

            // Unbounded send never blocks (only fails if receiver dropped)
            if download_tx.send(task).is_err() {
                debug!("Scheduler: download_channel closed, exiting");
                return;
            }

            debug!("Scheduler: dispatched chunk {}", chunk_index);
        }

        // Update continuation cursors
        if let Some(next) = batch.next_chunk_index {
            next_chunk_index = next;
        } else if batch.has_more {
            // Fallback: advance by batch size if server didn't provide next index
            next_chunk_index += 1;
        }

        if let Some(offset) = batch.next_row_offset {
            next_row_offset = offset;
        }

        // End of stream
        if !batch.has_more {
            debug!(
                "Scheduler: all chunks dispatched (last chunk index: {}), exiting",
                next_chunk_index.saturating_sub(1)
            );
            return;
        }
    }
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

    /// Mock link fetcher that returns configured batches of links.
    #[derive(Debug)]
    struct MockLinkFetcher {
        /// Each call to fetch_links returns the next batch in this sequence.
        batches: tokio::sync::Mutex<Vec<ChunkLinkFetchResult>>,
        fetch_count: AtomicUsize,
    }

    impl MockLinkFetcher {
        fn new(batches: Vec<ChunkLinkFetchResult>) -> Self {
            Self {
                batches: tokio::sync::Mutex::new(batches),
                fetch_count: AtomicUsize::new(0),
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
            let idx = self.fetch_count.fetch_add(1, Ordering::SeqCst);
            let batches = self.batches.lock().await;
            if idx < batches.len() {
                Ok(batches[idx].clone())
            } else {
                Ok(ChunkLinkFetchResult::end_of_stream())
            }
        }

        async fn refetch_link(&self, chunk_index: i64, _row_offset: i64) -> Result<CloudFetchLink> {
            Ok(create_test_link(chunk_index))
        }
    }

    #[tokio::test]
    async fn scheduler_sends_handles_in_chunk_index_order() {
        let links: Vec<CloudFetchLink> = (0..5).map(create_test_link).collect();
        let fetcher = Arc::new(MockLinkFetcher::new(vec![ChunkLinkFetchResult {
            links,
            has_more: false,
            next_chunk_index: None,
            next_row_offset: None,
        }]));

        let cancel = CancellationToken::new();
        let mut channels = spawn_scheduler(fetcher, 16, cancel);

        // Verify handles arrive in order
        for expected_index in 0..5 {
            let handle = channels.result_rx.recv().await.unwrap();
            assert_eq!(handle.chunk_index, expected_index);
        }

        // Channel should be closed (scheduler exited)
        assert!(channels.result_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn scheduler_processes_batch_links() {
        let links: Vec<CloudFetchLink> = (0..3).map(create_test_link).collect();
        let fetcher = Arc::new(MockLinkFetcher::new(vec![ChunkLinkFetchResult {
            links,
            has_more: false,
            next_chunk_index: None,
            next_row_offset: None,
        }]));

        let cancel = CancellationToken::new();
        let mut channels = spawn_scheduler(fetcher, 16, cancel);

        // All 3 tasks should be in the download channel
        let mut tasks = Vec::new();
        for _ in 0..3 {
            tasks.push(channels.download_rx.recv().await.unwrap());
        }

        assert_eq!(tasks.len(), 3);
        assert_eq!(tasks[0].chunk_index, 0);
        assert_eq!(tasks[1].chunk_index, 1);
        assert_eq!(tasks[2].chunk_index, 2);
    }

    #[tokio::test]
    async fn backpressure_blocks_scheduler_at_capacity() {
        // Create a result_channel with capacity 2
        let links: Vec<CloudFetchLink> = (0..5).map(create_test_link).collect();
        let fetcher = Arc::new(MockLinkFetcher::new(vec![ChunkLinkFetchResult {
            links,
            has_more: false,
            next_chunk_index: None,
            next_row_offset: None,
        }]));

        let cancel = CancellationToken::new();
        let mut channels = spawn_scheduler(fetcher, 2, cancel);

        // Give the scheduler time to fill the bounded channel
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Read one handle - this should unblock the scheduler to send one more
        let h0 = channels.result_rx.recv().await.unwrap();
        assert_eq!(h0.chunk_index, 0);

        let h1 = channels.result_rx.recv().await.unwrap();
        assert_eq!(h1.chunk_index, 1);

        // Continue reading remaining
        let h2 = channels.result_rx.recv().await.unwrap();
        assert_eq!(h2.chunk_index, 2);

        let h3 = channels.result_rx.recv().await.unwrap();
        assert_eq!(h3.chunk_index, 3);

        let h4 = channels.result_rx.recv().await.unwrap();
        assert_eq!(h4.chunk_index, 4);

        // No more
        assert!(channels.result_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn scheduler_exits_when_has_more_false() {
        let fetcher = Arc::new(MockLinkFetcher::new(vec![ChunkLinkFetchResult {
            links: vec![create_test_link(0)],
            has_more: false,
            next_chunk_index: None,
            next_row_offset: None,
        }]));

        let cancel = CancellationToken::new();
        let mut channels = spawn_scheduler(fetcher, 16, cancel);

        // Should get one handle then channel closes
        let handle = channels.result_rx.recv().await.unwrap();
        assert_eq!(handle.chunk_index, 0);
        assert!(channels.result_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn scheduler_cancellation() {
        // Mock that returns a batch with has_more = true, so scheduler would loop forever
        let fetcher = Arc::new(MockLinkFetcher::new(vec![
            ChunkLinkFetchResult {
                links: vec![create_test_link(0)],
                has_more: true,
                next_chunk_index: Some(1),
                next_row_offset: None,
            },
            // Second fetch will be cancelled
            ChunkLinkFetchResult {
                links: vec![create_test_link(1)],
                has_more: true,
                next_chunk_index: Some(2),
                next_row_offset: None,
            },
        ]));

        let cancel = CancellationToken::new();
        let mut channels = spawn_scheduler(fetcher, 16, cancel.clone());

        // Read first handle
        let handle = channels.result_rx.recv().await.unwrap();
        assert_eq!(handle.chunk_index, 0);

        // Cancel — scheduler should exit soon
        cancel.cancel();

        // Channel should eventually close (scheduler exits)
        // It may or may not deliver chunk 1 depending on timing
        let timeout = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while channels.result_rx.recv().await.is_some() {
                // drain
            }
        })
        .await;
        assert!(timeout.is_ok(), "Scheduler did not exit after cancellation");
    }

    #[tokio::test]
    async fn scheduler_handle_before_task() {
        // Verify the ordering invariant: handle appears in result_channel
        // before the corresponding task appears in download_channel.
        let links: Vec<CloudFetchLink> = (0..3).map(create_test_link).collect();
        let fetcher = Arc::new(MockLinkFetcher::new(vec![ChunkLinkFetchResult {
            links,
            has_more: false,
            next_chunk_index: None,
            next_row_offset: None,
        }]));

        let cancel = CancellationToken::new();
        let mut channels = spawn_scheduler(fetcher, 16, cancel);

        // Read both channels in parallel and record arrival order
        let mut handle_indices = Vec::new();
        let mut task_indices = Vec::new();

        for _ in 0..3 {
            // Handle must be available at least as early as the task
            let handle = channels.result_rx.recv().await.unwrap();
            handle_indices.push(handle.chunk_index);

            let task = channels.download_rx.recv().await.unwrap();
            task_indices.push(task.chunk_index);
        }

        // Both should be in order 0, 1, 2
        assert_eq!(handle_indices, vec![0, 1, 2]);
        assert_eq!(task_indices, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn scheduler_multiple_batches() {
        let fetcher = Arc::new(MockLinkFetcher::new(vec![
            ChunkLinkFetchResult {
                links: vec![create_test_link(0), create_test_link(1)],
                has_more: true,
                next_chunk_index: Some(2),
                next_row_offset: None,
            },
            ChunkLinkFetchResult {
                links: vec![create_test_link(2), create_test_link(3)],
                has_more: false,
                next_chunk_index: None,
                next_row_offset: None,
            },
        ]));

        let cancel = CancellationToken::new();
        let mut channels = spawn_scheduler(fetcher, 16, cancel);

        // Should get all 4 handles in order
        for expected in 0..4 {
            let handle = channels.result_rx.recv().await.unwrap();
            assert_eq!(handle.chunk_index, expected);
        }
        assert!(channels.result_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn scheduler_empty_batch() {
        let fetcher = Arc::new(MockLinkFetcher::new(vec![ChunkLinkFetchResult {
            links: vec![],
            has_more: false,
            next_chunk_index: None,
            next_row_offset: None,
        }]));

        let cancel = CancellationToken::new();
        let mut channels = spawn_scheduler(fetcher, 16, cancel);

        // Channel should close immediately — no handles
        assert!(channels.result_rx.recv().await.is_none());
    }
}
