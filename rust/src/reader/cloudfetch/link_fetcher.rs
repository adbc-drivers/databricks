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

//! ChunkLinkFetcher trait and implementations.
//!
//! This module provides a statement-scoped abstraction for fetching chunk links.
//! The `ChunkLinkFetcher` trait wraps a `DatabricksClient` and binds it to a
//! specific statement, providing a simpler interface for `StreamingCloudFetchProvider`.
//!
//! ## Prefetch Architecture
//!
//! The `SeaChunkLinkFetcher` implements proactive link prefetching similar to the
//! JDBC driver. It maintains a prefetch window ahead of the consumer position:
//!
//! ```text
//! Consumer Position     Prefetch Window (128 chunks)
//!       |                      |
//!       v                      v
//! [0][1][2]...[current]...[current + 128]
//!       ^                      ^
//!       |                      |
//!  Links cached          Prefetch triggers when
//!  and available         we approach this boundary
//! ```
//!
//! When a link is requested:
//! 1. Return immediately from cache if available
//! 2. Update consumer position tracking
//! 3. Check if prefetch should be triggered (async, non-blocking)
//! 4. Background task fetches links and caches them

use crate::client::{ChunkLinkFetchResult, DatabricksClient};
use crate::error::{DatabricksErrorHelper, Result};
use crate::types::cloudfetch::CloudFetchLink;
use async_trait::async_trait;
use dashmap::DashMap;
use driverbase::error::ErrorHelper;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;
use tracing::{debug, error};

/// Statement-scoped abstraction for fetching chunk links.
///
/// This trait wraps a `DatabricksClient` and binds it to a specific statement,
/// providing a simpler interface for `StreamingCloudFetchProvider`. Each
/// implementation delegates to `DatabricksClient::get_result_chunks()` but
/// handles protocol-specific details (e.g., which continuation parameter to use).
///
/// # Design Note
///
/// Different backends use different continuation mechanisms:
/// - SEA uses `chunk_index`
/// - Thrift uses `row_offset`
///
/// Both parameters are passed to methods; implementations use the relevant one.
#[async_trait]
pub trait ChunkLinkFetcher: Send + Sync + std::fmt::Debug {
    /// Fetch the next batch of chunk links.
    ///
    /// # Parameters
    /// * `start_chunk_index` - Used by chunk-index based backends (SEA)
    /// * `start_row_offset` - Used by row-offset based backends (Thrift)
    ///
    /// Each implementation uses the parameter relevant to its protocol.
    async fn fetch_links(
        &self,
        start_chunk_index: i64,
        start_row_offset: i64,
    ) -> Result<ChunkLinkFetchResult>;

    /// Refetch a specific chunk link (used when link expires before download).
    async fn refetch_link(&self, chunk_index: i64, row_offset: i64) -> Result<CloudFetchLink>;
}

/// SEA implementation of ChunkLinkFetcher with caching and proactive prefetching.
///
/// This fetcher:
/// - Makes API calls to the SEA get_result_chunks endpoint
/// - Caches fetched links in a concurrent DashMap
/// - Tracks the highest chunk index fetched to avoid redundant API calls
/// - Uses `total_chunk_count` from the manifest to determine end-of-stream
/// - **Proactively prefetches links** when consumer position approaches the
///   prefetch boundary (similar to JDBC's `linkPrefetchWindow`)
///
/// ## Prefetch Behavior
///
/// When `fetch_links()` is called:
/// 1. Consumer position is updated
/// 2. If `next_server_fetch_index <= consumer_position + link_prefetch_window`,
///    a background prefetch task is spawned
/// 3. The prefetch task fetches links until caught up with the window
/// 4. This ensures links are ready before they're needed
///
/// Uses `chunk_index` for pagination (ignores `row_offset`).
pub struct SeaChunkLinkFetcher {
    /// Databricks client for making API calls
    client: Arc<dyn DatabricksClient>,
    /// Statement ID this fetcher is bound to
    statement_id: String,
    /// Cached links by chunk index
    links: DashMap<i64, CloudFetchLink>,
    /// Highest chunk index we've fetched + 1 (next index to fetch from server)
    next_server_fetch_index: AtomicI64,
    /// Total number of chunks from manifest
    total_chunk_count: Option<i64>,
    /// Whether we've fetched all links
    all_links_fetched: AtomicBool,
    /// Current consumer position (highest chunk index requested)
    current_consumer_index: AtomicI64,
    /// Number of chunks to prefetch ahead of consumer position
    link_prefetch_window: i64,
    /// Whether a prefetch task is currently running
    prefetch_in_progress: AtomicBool,
    /// Tokio runtime handle for spawning prefetch tasks
    runtime_handle: tokio::runtime::Handle,
    /// Lock for coordinating prefetch operations
    prefetch_lock: TokioMutex<()>,
    /// Next row offset for pagination (used by some backends)
    next_row_offset: AtomicI64,
}

impl std::fmt::Debug for SeaChunkLinkFetcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeaChunkLinkFetcher")
            .field("statement_id", &self.statement_id)
            .field("links_count", &self.links.len())
            .field(
                "next_server_fetch_index",
                &self.next_server_fetch_index.load(Ordering::Relaxed),
            )
            .field("total_chunk_count", &self.total_chunk_count)
            .field(
                "all_links_fetched",
                &self.all_links_fetched.load(Ordering::Relaxed),
            )
            .field(
                "current_consumer_index",
                &self.current_consumer_index.load(Ordering::Relaxed),
            )
            .field("link_prefetch_window", &self.link_prefetch_window)
            .field(
                "prefetch_in_progress",
                &self.prefetch_in_progress.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl SeaChunkLinkFetcher {
    /// Create a new SEA link fetcher with caching and prefetching.
    ///
    /// # Arguments
    /// * `client` - The Databricks client to use for API calls
    /// * `statement_id` - The statement ID to fetch links for
    /// * `initial_links` - Links from the initial execute response
    /// * `total_chunk_count` - Total number of chunks from result manifest
    /// * `link_prefetch_window` - Number of chunks to prefetch ahead of consumer
    /// * `runtime_handle` - Tokio runtime handle for spawning prefetch tasks
    pub fn new(
        client: Arc<dyn DatabricksClient>,
        statement_id: impl Into<String>,
        initial_links: &[CloudFetchLink],
        total_chunk_count: Option<i64>,
        link_prefetch_window: i64,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        let statement_id = statement_id.into();
        let links = DashMap::new();
        let mut max_chunk_index = -1i64;

        // Store initial links
        for link in initial_links {
            max_chunk_index = max_chunk_index.max(link.chunk_index);
            links.insert(link.chunk_index, link.clone());
        }

        let next_server_fetch_index = max_chunk_index + 1;

        // Determine if we already have all links
        let all_links_fetched = if let Some(total) = total_chunk_count {
            next_server_fetch_index >= total
        } else {
            false
        };

        debug!(
            "SeaChunkLinkFetcher: {} initial links, next_server_fetch={}, total_chunks={:?}, \
             all_fetched={}, prefetch_window={}",
            initial_links.len(),
            next_server_fetch_index,
            total_chunk_count,
            all_links_fetched,
            link_prefetch_window
        );

        Self {
            client,
            statement_id,
            links,
            next_server_fetch_index: AtomicI64::new(next_server_fetch_index),
            total_chunk_count,
            all_links_fetched: AtomicBool::new(all_links_fetched),
            current_consumer_index: AtomicI64::new(0),
            link_prefetch_window,
            prefetch_in_progress: AtomicBool::new(false),
            runtime_handle,
            prefetch_lock: TokioMutex::new(()),
            next_row_offset: AtomicI64::new(0),
        }
    }

    /// Check if a link is cached.
    pub fn has_link(&self, chunk_index: i64) -> bool {
        self.links.contains_key(&chunk_index)
    }

    /// Get a cached link if available.
    pub fn get_link(&self, chunk_index: i64) -> Option<CloudFetchLink> {
        self.links.get(&chunk_index).map(|r| r.clone())
    }

    /// Check if all links have been fetched.
    pub fn all_links_fetched(&self) -> bool {
        self.all_links_fetched.load(Ordering::Acquire)
    }

    /// Check if prefetch should be triggered and spawn background task if needed.
    ///
    /// Prefetch is triggered when:
    /// - Not all links have been fetched yet
    /// - No prefetch is currently in progress
    /// - `next_server_fetch_index <= current_consumer_index + link_prefetch_window`
    fn maybe_trigger_prefetch(self: &Arc<Self>) {
        // Quick checks without locks
        if self.all_links_fetched.load(Ordering::Acquire) {
            return;
        }

        // Try to claim prefetch (compare_exchange returns Ok if we won)
        if self
            .prefetch_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            // Another prefetch is already in progress
            return;
        }

        let consumer_pos = self.current_consumer_index.load(Ordering::Acquire);
        let server_fetch_idx = self.next_server_fetch_index.load(Ordering::Acquire);
        let target_index = consumer_pos + self.link_prefetch_window;

        // Check if we need to prefetch
        if server_fetch_idx > target_index {
            // We're already ahead of the window, release the flag
            self.prefetch_in_progress.store(false, Ordering::Release);
            return;
        }

        debug!(
            "Triggering async prefetch: consumer={}, server_fetch={}, target={}, window={}",
            consumer_pos, server_fetch_idx, target_index, self.link_prefetch_window
        );

        // Spawn background prefetch task
        let fetcher = Arc::clone(self);
        self.runtime_handle.spawn(async move {
            fetcher.run_prefetch_loop().await;
        });
    }

    /// Background prefetch loop - fetches links until caught up with window.
    async fn run_prefetch_loop(self: Arc<Self>) {
        // Acquire the prefetch lock to prevent concurrent prefetch operations
        let _guard = self.prefetch_lock.lock().await;

        loop {
            // Check if we're done
            if self.all_links_fetched.load(Ordering::Acquire) {
                debug!("Prefetch loop: all links fetched, stopping");
                break;
            }

            let consumer_pos = self.current_consumer_index.load(Ordering::Acquire);
            let server_fetch_idx = self.next_server_fetch_index.load(Ordering::Acquire);
            let target_index = consumer_pos + self.link_prefetch_window;

            // Check if we're caught up
            if server_fetch_idx > target_index {
                debug!(
                    "Prefetch loop: caught up (server_fetch={} > target={}), stopping",
                    server_fetch_idx, target_index
                );
                break;
            }

            // Fetch next batch
            let row_offset = self.next_row_offset.load(Ordering::Acquire);
            debug!(
                "Prefetch loop: fetching from index {}, row_offset={}",
                server_fetch_idx, row_offset
            );

            match self
                .client
                .get_result_chunks(&self.statement_id, server_fetch_idx, row_offset)
                .await
            {
                Ok(result) => {
                    // Cache the new links and track highest index
                    let mut max_chunk_index = server_fetch_idx - 1;
                    for link in &result.links {
                        max_chunk_index = max_chunk_index.max(link.chunk_index);
                        self.links.insert(link.chunk_index, link.clone());
                    }

                    // Update next server fetch index
                    let new_next_index = max_chunk_index + 1;
                    self.next_server_fetch_index
                        .fetch_max(new_next_index, Ordering::Release);

                    // Update row offset if provided
                    if let Some(offset) = result.next_row_offset {
                        self.next_row_offset.store(offset, Ordering::Release);
                    }

                    debug!(
                        "Prefetch loop: fetched {} links (chunks up to {}), next_server_fetch={}",
                        result.links.len(),
                        max_chunk_index,
                        new_next_index
                    );

                    // Check if we've fetched all links
                    if let Some(total) = self.total_chunk_count {
                        if new_next_index >= total {
                            debug!(
                                "Prefetch loop: all links fetched (up to chunk {}, total={})",
                                max_chunk_index, total
                            );
                            self.all_links_fetched.store(true, Ordering::Release);
                            break;
                        }
                    } else if !result.has_more {
                        debug!("Prefetch loop: no more links (has_more=false)");
                        self.all_links_fetched.store(true, Ordering::Release);
                        break;
                    }
                }
                Err(e) => {
                    error!("Prefetch loop: failed to fetch links: {}", e);
                    // Don't propagate error - let the main fetch_links handle it
                    break;
                }
            }
        }

        // Release prefetch flag
        self.prefetch_in_progress.store(false, Ordering::Release);
    }
}

/// Wrapper to allow calling `maybe_trigger_prefetch` from the trait implementation.
///
/// Since the trait takes `&self`, we need a way to get an `Arc<Self>` to spawn
/// the background prefetch task. This wrapper holds the Arc internally.
pub struct SeaChunkLinkFetcherHandle {
    inner: Arc<SeaChunkLinkFetcher>,
}

impl std::fmt::Debug for SeaChunkLinkFetcherHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl SeaChunkLinkFetcherHandle {
    /// Create a new handle wrapping a SeaChunkLinkFetcher.
    ///
    /// Immediately triggers async prefetch to start fetching links proactively,
    /// giving the prefetch task a head start before the consumer starts reading.
    pub fn new(fetcher: SeaChunkLinkFetcher) -> Self {
        let handle = Self {
            inner: Arc::new(fetcher),
        };

        // Trigger prefetch immediately on creation - don't wait for first next_batch()
        handle.inner.maybe_trigger_prefetch();

        handle
    }
}

#[async_trait]
impl ChunkLinkFetcher for SeaChunkLinkFetcherHandle {
    async fn fetch_links(
        &self,
        start_chunk_index: i64,
        start_row_offset: i64,
    ) -> Result<ChunkLinkFetchResult> {
        let fetcher = &self.inner;

        // Update consumer position (highest chunk index requested)
        fetcher
            .current_consumer_index
            .fetch_max(start_chunk_index, Ordering::Release);

        // Trigger async prefetch if needed (non-blocking)
        self.inner.maybe_trigger_prefetch();

        // If we already have all links, return what we have from cache
        if fetcher.all_links_fetched.load(Ordering::Acquire) {
            // Return cached links starting from start_chunk_index
            let mut links = Vec::new();
            let total = fetcher.total_chunk_count.unwrap_or(i64::MAX);
            for i in start_chunk_index..total {
                if let Some(link) = fetcher.links.get(&i) {
                    links.push(link.clone());
                } else {
                    break;
                }
            }
            return Ok(ChunkLinkFetchResult {
                links,
                has_more: false,
                next_chunk_index: None,
                next_row_offset: None,
            });
        }

        // Check if we need to fetch from server
        let server_fetch_index = fetcher.next_server_fetch_index.load(Ordering::Acquire);

        // If request is for links we should already have, return from cache
        if start_chunk_index < server_fetch_index {
            let mut links = Vec::new();
            for i in start_chunk_index..server_fetch_index {
                if let Some(link) = fetcher.links.get(&i) {
                    links.push(link.clone());
                }
            }
            let has_more = !fetcher.all_links_fetched.load(Ordering::Acquire);
            return Ok(ChunkLinkFetchResult {
                links,
                has_more,
                next_chunk_index: if has_more {
                    Some(server_fetch_index)
                } else {
                    None
                },
                next_row_offset: None,
            });
        }

        // Fetch from server (synchronous fallback if prefetch hasn't caught up)
        //
        // TODO: This can cause duplicate API calls. There are two independent prefetch
        // loops that race: (1) `SeaChunkLinkFetcher::run_prefetch_loop` triggered by
        // `SeaChunkLinkFetcherHandle::new()`, and (2) `StreamingCloudFetchProvider::
        // run_link_prefetch_loop` which also calls `fetch_links(32, ...)` because its
        // `next_link_fetch_index` (set from initial cached links) is less than its
        // prefetch window. Both end up calling `get_result_chunks(32)` simultaneously.
        // Consider consolidating link prefetch into a single layer — either let the
        // link fetcher handle all prefetching (and have the provider just request links
        // on demand), or remove the fetcher's async prefetch and let the provider drive
        // all fetching.
        debug!(
            "SeaChunkLinkFetcher: cache miss for chunk {}, fetching synchronously",
            start_chunk_index
        );
        let result = fetcher
            .client
            .get_result_chunks(&fetcher.statement_id, start_chunk_index, start_row_offset)
            .await?;

        // Cache the new links and track highest index
        let mut max_chunk_index = start_chunk_index - 1;
        for link in &result.links {
            max_chunk_index = max_chunk_index.max(link.chunk_index);
            fetcher.links.insert(link.chunk_index, link.clone());
        }

        // Update next server fetch index
        let new_next_index = max_chunk_index + 1;
        fetcher
            .next_server_fetch_index
            .fetch_max(new_next_index, Ordering::Release);

        // Update row offset if provided
        if let Some(offset) = result.next_row_offset {
            fetcher.next_row_offset.store(offset, Ordering::Release);
        }

        // Check if we've fetched all links based on total_chunk_count
        if let Some(total) = fetcher.total_chunk_count {
            if new_next_index >= total {
                debug!(
                    "SeaChunkLinkFetcher: all links fetched (up to chunk {}, total={})",
                    max_chunk_index, total
                );
                fetcher.all_links_fetched.store(true, Ordering::Release);
            }
        } else if !result.has_more {
            fetcher.all_links_fetched.store(true, Ordering::Release);
        }

        // Return result with corrected has_more based on our tracking
        Ok(ChunkLinkFetchResult {
            links: result.links,
            has_more: !fetcher.all_links_fetched.load(Ordering::Acquire),
            next_chunk_index: if fetcher.all_links_fetched.load(Ordering::Acquire) {
                None
            } else {
                Some(new_next_index)
            },
            next_row_offset: result.next_row_offset,
        })
    }

    async fn refetch_link(&self, chunk_index: i64, _row_offset: i64) -> Result<CloudFetchLink> {
        let result = self
            .inner
            .client
            .get_result_chunks(&self.inner.statement_id, chunk_index, 0)
            .await?;

        let link = result
            .links
            .into_iter()
            .find(|l| l.chunk_index == chunk_index)
            .ok_or_else(|| {
                DatabricksErrorHelper::invalid_state()
                    .message(format!("Link not found for chunk_index {}", chunk_index))
            })?;

        // Update cache with fresh link
        self.inner.links.insert(chunk_index, link.clone());
        Ok(link)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{ExecuteResult, SessionInfo};
    use crate::reader::EmptyReader;
    use crate::types::sea::ExecuteParams;
    use arrow_schema::Schema;
    use std::collections::HashMap;
    use std::sync::Arc as StdArc;

    /// Mock DatabricksClient for testing
    #[derive(Debug)]
    struct MockDatabricksClient {
        chunks: Vec<Vec<CloudFetchLink>>,
    }

    impl MockDatabricksClient {
        fn new(chunks: Vec<Vec<CloudFetchLink>>) -> Self {
            Self { chunks }
        }
    }

    #[async_trait]
    impl DatabricksClient for MockDatabricksClient {
        async fn create_session(
            &self,
            _catalog: Option<&str>,
            _schema: Option<&str>,
            _session_config: HashMap<String, String>,
        ) -> Result<SessionInfo> {
            Ok(SessionInfo {
                session_id: "mock-session".to_string(),
            })
        }

        async fn delete_session(&self, _session_id: &str) -> Result<()> {
            Ok(())
        }

        async fn execute_statement(
            &self,
            _session_id: &str,
            _sql: &str,
            _params: &ExecuteParams,
        ) -> Result<ExecuteResult> {
            Ok(ExecuteResult {
                statement_id: "mock-statement".to_string(),
                reader: Box::new(EmptyReader::new(StdArc::new(Schema::empty()))),
            })
        }

        async fn get_result_chunks(
            &self,
            _statement_id: &str,
            chunk_index: i64,
            _row_offset: i64,
        ) -> Result<ChunkLinkFetchResult> {
            let index = chunk_index as usize;
            if index >= self.chunks.len() {
                return Ok(ChunkLinkFetchResult::end_of_stream());
            }

            let links = self.chunks[index].clone();
            let has_more = index + 1 < self.chunks.len();
            let next_chunk_index = if has_more {
                Some((index + 1) as i64)
            } else {
                None
            };

            Ok(ChunkLinkFetchResult {
                links,
                has_more,
                next_chunk_index,
                next_row_offset: None,
            })
        }

        async fn cancel_statement(&self, _statement_id: &str) -> Result<()> {
            Ok(())
        }

        async fn close_statement(&self, _statement_id: &str) -> Result<()> {
            Ok(())
        }

        async fn list_catalogs(&self, _session_id: &str) -> Result<ExecuteResult> {
            self.execute_statement("", "", &ExecuteParams::default())
                .await
        }

        async fn list_schemas(
            &self,
            _session_id: &str,
            _catalog: Option<&str>,
            _schema_pattern: Option<&str>,
        ) -> Result<ExecuteResult> {
            self.execute_statement("", "", &ExecuteParams::default())
                .await
        }

        async fn list_tables(
            &self,
            _session_id: &str,
            _catalog: Option<&str>,
            _schema_pattern: Option<&str>,
            _table_pattern: Option<&str>,
            _table_types: Option<&[&str]>,
        ) -> Result<ExecuteResult> {
            self.execute_statement("", "", &ExecuteParams::default())
                .await
        }

        async fn list_columns(
            &self,
            _session_id: &str,
            _catalog: &str,
            _schema_pattern: Option<&str>,
            _table_pattern: Option<&str>,
            _column_pattern: Option<&str>,
        ) -> Result<ExecuteResult> {
            self.execute_statement("", "", &ExecuteParams::default())
                .await
        }

        fn list_table_types(&self) -> Vec<String> {
            vec!["TABLE".to_string()]
        }
    }

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

    #[tokio::test]
    async fn test_sea_link_fetcher_fetch_links() {
        let links = vec![
            vec![create_test_link(0)],
            vec![create_test_link(1)],
            vec![create_test_link(2)],
        ];
        let client = Arc::new(MockDatabricksClient::new(links));
        let fetcher = SeaChunkLinkFetcher::new(
            client,
            "test-statement",
            &[], // no initial links
            Some(3),
            128,
            tokio::runtime::Handle::current(),
        );
        let handle = SeaChunkLinkFetcherHandle::new(fetcher);

        // Fetch first chunk
        let result = handle.fetch_links(0, 0).await.unwrap();
        assert_eq!(result.links.len(), 1);
        assert_eq!(result.links[0].chunk_index, 0);
        assert!(result.has_more);
        assert_eq!(result.next_chunk_index, Some(1));

        // Fetch last chunk
        let result = handle.fetch_links(2, 0).await.unwrap();
        assert_eq!(result.links.len(), 1);
        assert_eq!(result.links[0].chunk_index, 2);
        assert!(!result.has_more);
        assert_eq!(result.next_chunk_index, None);
    }

    #[tokio::test]
    async fn test_sea_link_fetcher_with_initial_links() {
        let links = vec![
            vec![create_test_link(0)],
            vec![create_test_link(1)],
            vec![create_test_link(2)],
        ];
        let client = Arc::new(MockDatabricksClient::new(links));

        // Provide initial links (chunk 0)
        let initial_links = vec![create_test_link(0)];
        let fetcher = SeaChunkLinkFetcher::new(
            client,
            "test-statement",
            &initial_links,
            Some(3),
            128,
            tokio::runtime::Handle::current(),
        );
        let handle = SeaChunkLinkFetcherHandle::new(fetcher);

        // Chunk 0 should be cached
        assert!(handle.inner.has_link(0));

        // Fetch chunk 0 should return from cache
        let result = handle.fetch_links(0, 0).await.unwrap();
        assert_eq!(result.links.len(), 1);
        assert_eq!(result.links[0].chunk_index, 0);
    }

    #[tokio::test]
    async fn test_sea_link_fetcher_fetch_links_end_of_stream() {
        let links = vec![vec![create_test_link(0)]];
        let client = Arc::new(MockDatabricksClient::new(links));
        let fetcher = SeaChunkLinkFetcher::new(
            client,
            "test-statement",
            &[],
            Some(1),
            128,
            tokio::runtime::Handle::current(),
        );
        let handle = SeaChunkLinkFetcherHandle::new(fetcher);

        // Fetch beyond available chunks
        let result = handle.fetch_links(5, 0).await.unwrap();
        assert!(result.links.is_empty());
        assert!(!result.has_more);
    }

    #[tokio::test]
    async fn test_sea_link_fetcher_refetch_link() {
        // Structure: chunks is indexed by chunk_index, each element contains links for that chunk
        // When refetch_link(chunk_index, _) is called, it fetches chunk[chunk_index] from the server
        // and searches for a link with matching chunk_index
        let links = vec![
            vec![create_test_link(0)], // chunk 0 contains link for chunk 0
            vec![create_test_link(1)], // chunk 1 contains link for chunk 1
        ];
        let client = Arc::new(MockDatabricksClient::new(links));
        let fetcher = SeaChunkLinkFetcher::new(
            client,
            "test-statement",
            &[],
            Some(2),
            128,
            tokio::runtime::Handle::current(),
        );
        let handle = SeaChunkLinkFetcherHandle::new(fetcher);

        // Refetch specific link (calls get_result_chunks(1, _) which returns chunk 1)
        let link = handle.refetch_link(1, 0).await.unwrap();
        assert_eq!(link.chunk_index, 1);

        // Link should now be cached
        assert!(handle.inner.has_link(1));
    }

    #[tokio::test]
    async fn test_sea_link_fetcher_refetch_link_not_found() {
        let links = vec![vec![create_test_link(0)]];
        let client = Arc::new(MockDatabricksClient::new(links));
        let fetcher = SeaChunkLinkFetcher::new(
            client,
            "test-statement",
            &[],
            Some(1),
            128,
            tokio::runtime::Handle::current(),
        );
        let handle = SeaChunkLinkFetcherHandle::new(fetcher);

        // Try to refetch non-existent link
        let result = handle.refetch_link(5, 0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sea_link_fetcher_all_links_fetched_flag() {
        let links = vec![vec![create_test_link(0)]];
        let client = Arc::new(MockDatabricksClient::new(links));

        // Provide all initial links (total_chunk_count = 1)
        let initial_links = vec![create_test_link(0)];
        let fetcher = SeaChunkLinkFetcher::new(
            client,
            "test-statement",
            &initial_links,
            Some(1), // total_chunk_count = 1, and we have chunk 0
            128,
            tokio::runtime::Handle::current(),
        );

        // Should be marked as all fetched since we have all chunks
        assert!(fetcher.all_links_fetched());
    }
}
