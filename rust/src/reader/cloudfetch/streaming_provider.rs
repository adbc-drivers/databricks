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

//! StreamingCloudFetchProvider for orchestrating CloudFetch downloads.
//!
//! This is the main component that coordinates:
//! - Link prefetching from the backend
//! - Parallel chunk downloads from cloud storage
//! - Memory management via chunk limits
//! - Consumer API via RecordBatchReader trait

use crate::error::{DatabricksErrorHelper, Result};
use crate::reader::cloudfetch::chunk_downloader::ChunkDownloader;
use crate::reader::cloudfetch::link_fetcher::ChunkLinkFetcher;
use crate::types::cloudfetch::{
    ChunkEntry, ChunkState, CloudFetchConfig, DEFAULT_CHUNK_READY_TIMEOUT_SECS,
};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use dashmap::DashMap;
use driverbase::error::ErrorHelper;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

/// Orchestrates link fetching and chunk downloading for CloudFetch.
///
/// This provider manages:
/// - Background link prefetching to stay ahead of consumption
/// - Parallel downloads with memory-bounded concurrency
/// - Proper ordering of result batches
/// - Error propagation and cancellation
pub struct StreamingCloudFetchProvider {
    // Dependencies (injected)
    link_fetcher: Arc<dyn ChunkLinkFetcher>,
    chunk_downloader: Arc<ChunkDownloader>,
    config: CloudFetchConfig,

    // Schema - extracted from first Arrow batch
    schema: OnceLock<SchemaRef>,

    // Consumer state
    current_chunk_index: AtomicI64,
    current_batch_buffer: Mutex<VecDeque<RecordBatch>>,

    // Download scheduling state
    next_download_index: AtomicI64,
    next_link_fetch_index: AtomicI64,
    end_of_stream: AtomicBool,

    // Storage - unified chunks map with state and data
    // Wrapped in Arc so spawned tasks share the same map
    chunks: Arc<DashMap<i64, ChunkEntry>>,

    // Memory control
    chunks_in_memory: AtomicUsize,
    max_chunks_in_memory: usize,

    // Coordination signals
    consumer_advanced: Arc<Notify>,
    chunk_state_changed: Arc<Notify>,

    // Error propagation from background tasks
    prefetch_error: Arc<Mutex<Option<crate::error::Error>>>,

    // Cancellation
    cancel_token: CancellationToken,

    // Tokio runtime handle for spawning tasks
    runtime_handle: tokio::runtime::Handle,
}

impl std::fmt::Debug for StreamingCloudFetchProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingCloudFetchProvider")
            .field("current_chunk_index", &self.current_chunk_index)
            .field("next_download_index", &self.next_download_index)
            .field("end_of_stream", &self.end_of_stream)
            .field("chunks_in_memory", &self.chunks_in_memory)
            .finish()
    }
}

impl StreamingCloudFetchProvider {
    /// Create a new provider.
    ///
    /// # Arguments
    /// * `config` - CloudFetch configuration
    /// * `link_fetcher` - Trait object for fetching chunk links (should be a SeaChunkLinkFetcherHandle
    ///   that already has initial links cached and handles prefetching)
    /// * `chunk_downloader` - For downloading chunks from cloud storage
    /// * `runtime_handle` - Tokio runtime handle for spawning background tasks
    pub fn new(
        config: CloudFetchConfig,
        link_fetcher: Arc<dyn ChunkLinkFetcher>,
        chunk_downloader: Arc<ChunkDownloader>,
        runtime_handle: tokio::runtime::Handle,
    ) -> Arc<Self> {
        let provider = Arc::new(Self {
            link_fetcher,
            chunk_downloader,
            config: config.clone(),
            schema: OnceLock::new(),
            current_chunk_index: AtomicI64::new(0),
            current_batch_buffer: Mutex::new(VecDeque::new()),
            next_download_index: AtomicI64::new(0),
            next_link_fetch_index: AtomicI64::new(0),
            end_of_stream: AtomicBool::new(false),
            chunks: Arc::new(DashMap::new()),
            chunks_in_memory: AtomicUsize::new(0),
            max_chunks_in_memory: config.max_chunks_in_memory,
            consumer_advanced: Arc::new(Notify::new()),
            chunk_state_changed: Arc::new(Notify::new()),
            prefetch_error: Arc::new(Mutex::new(None)),
            cancel_token: CancellationToken::new(),
            runtime_handle,
        });

        // Start background initialization that fetches links and schedules downloads
        let provider_clone = Arc::clone(&provider);
        provider.runtime_handle.spawn(async move {
            provider_clone.initialize().await;
        });

        provider
    }

    /// Initialize the provider by fetching initial links and starting downloads.
    ///
    /// This runs as a background task immediately after creation to:
    /// 1. Fetch initial links from the link fetcher (which has them cached)
    /// 2. Populate the chunks map
    /// 3. Schedule downloads to start immediately
    /// 4. Start the ongoing link prefetch loop
    async fn initialize(self: &Arc<Self>) {
        // Fetch initial batch of links (should be instant if cached by link fetcher)
        debug!("Initializing provider: fetching initial links");

        match self.link_fetcher.fetch_links(0, 0).await {
            Ok(result) => {
                // Store fetched links and track the highest chunk index
                let mut max_chunk_index: i64 = -1;
                for link in result.links {
                    let chunk_index = link.chunk_index;
                    if chunk_index > max_chunk_index {
                        max_chunk_index = chunk_index;
                    }
                    self.chunks
                        .entry(chunk_index)
                        .or_insert_with(|| ChunkEntry::with_link(link));
                }

                // Update next fetch index
                let next_fetch_index = max_chunk_index + 1;
                self.next_link_fetch_index
                    .store(next_fetch_index, Ordering::Release);

                if !result.has_more {
                    self.end_of_stream.store(true, Ordering::Release);
                }

                debug!(
                    "Provider initialized: {} links cached, next_link_fetch_index={}, end_of_stream={}",
                    self.chunks.len(),
                    next_fetch_index,
                    !result.has_more
                );

                // Now schedule downloads - chunks map has links ready
                self.schedule_downloads();
            }
            Err(e) => {
                error!("Failed to fetch initial links: {}", e);
                *self.prefetch_error.lock().unwrap() = Some(e);
                self.chunk_state_changed.notify_one();
                return;
            }
        }

        // Continue with ongoing link prefetch loop
        self.run_link_prefetch_loop().await;
    }

    /// Get next record batch. Main consumer interface.
    ///
    /// Blocks until the next batch is available or end of stream.
    /// First drains current_batch_buffer, then fetches next chunk.
    pub async fn next_batch(&self) -> Result<Option<RecordBatch>> {
        // Check for errors from background tasks
        if let Some(err) = self.prefetch_error.lock().unwrap().take() {
            return Err(err);
        }

        // Check for cancellation
        if self.cancel_token.is_cancelled() {
            return Err(DatabricksErrorHelper::invalid_state().message("Operation cancelled"));
        }

        // Drain batch buffer first
        if let Some(batch) = self.current_batch_buffer.lock().unwrap().pop_front() {
            // Capture schema from first batch
            let _ = self.schema.get_or_init(|| batch.schema());
            return Ok(Some(batch));
        }

        // Buffer empty - need next chunk
        let chunk_index = self.current_chunk_index.load(Ordering::Acquire);

        // Check if we've reached end of stream
        if self.end_of_stream.load(Ordering::Acquire) && !self.chunks.contains_key(&chunk_index) {
            debug!("End of stream reached at chunk {}", chunk_index);
            return Ok(None);
        }

        // Wait for the chunk to be downloaded
        self.wait_for_chunk(chunk_index).await?;

        // Get the chunk and move batches to buffer
        if let Some((_, mut entry)) = self.chunks.remove(&chunk_index) {
            if let Some(batches) = entry.batches.take() {
                let mut buffer = self.current_batch_buffer.lock().unwrap();
                for batch in batches {
                    buffer.push_back(batch);
                }
            }

            // Mark chunk as released and decrement counter
            let new_count = self.chunks_in_memory.fetch_sub(1, Ordering::Release) - 1;

            // Move to next chunk
            let next_chunk = self.current_chunk_index.fetch_add(1, Ordering::Release) + 1;

            debug!(
                "Released chunk {}: chunks_in_memory={}/{}, consumer advancing to {}",
                chunk_index, new_count, self.max_chunks_in_memory, next_chunk
            );

            // Signal that consumer advanced (for link prefetch)
            self.consumer_advanced.notify_one();

            // Schedule more downloads
            self.schedule_downloads();

            // Return first batch from buffer
            if let Some(batch) = self.current_batch_buffer.lock().unwrap().pop_front() {
                let _ = self.schema.get_or_init(|| batch.schema());
                return Ok(Some(batch));
            }
        }

        // Check end of stream again
        if self.end_of_stream.load(Ordering::Acquire) {
            return Ok(None);
        }

        Err(DatabricksErrorHelper::invalid_state()
            .message(format!("Chunk {} not found after wait", chunk_index)))
    }

    /// Cancel all pending operations.
    pub fn cancel(&self) {
        debug!("Cancelling CloudFetch provider");
        self.cancel_token.cancel();
        self.chunk_state_changed.notify_one();
    }

    // --- Internal methods ---

    /// Background task: fetch links ahead of consumer position.
    async fn run_link_prefetch_loop(&self) {
        let mut next_row_offset: i64 = 0;

        loop {
            // Check cancellation
            if self.cancel_token.is_cancelled() {
                debug!("Link prefetch loop cancelled");
                break;
            }

            // Check if we need to fetch more links
            let current = self.current_chunk_index.load(Ordering::Acquire);
            let prefetched = self.next_link_fetch_index.load(Ordering::Acquire);
            let window = self.config.link_prefetch_window as i64;

            let need_more_links =
                prefetched - current < window && !self.end_of_stream.load(Ordering::Acquire);

            if need_more_links {
                debug!(
                    "Fetching more links: current={}, prefetched={}, window={}",
                    current, prefetched, window
                );

                match self
                    .link_fetcher
                    .fetch_links(prefetched, next_row_offset)
                    .await
                {
                    Ok(result) => {
                        // Store fetched links and track the highest chunk index
                        let mut max_chunk_index = prefetched - 1;
                        for link in result.links {
                            let chunk_index = link.chunk_index;
                            if chunk_index > max_chunk_index {
                                max_chunk_index = chunk_index;
                            }
                            self.chunks
                                .entry(chunk_index)
                                .or_insert_with(|| ChunkEntry::with_link(link));
                        }

                        // Update next fetch index based on actual links received
                        let next_fetch_index = max_chunk_index + 1;
                        self.next_link_fetch_index
                            .store(next_fetch_index, Ordering::Release);

                        // The CachingLinkFetcher handles end-of-stream detection based on
                        // total_chunk_count. We just use the has_more from the result.
                        if !result.has_more {
                            debug!("End of links: has_more=false");
                            self.end_of_stream.store(true, Ordering::Release);
                        }

                        if let Some(offset) = result.next_row_offset {
                            next_row_offset = offset;
                        }

                        // Trigger downloads for newly fetched links
                        self.schedule_downloads();
                    }
                    Err(e) => {
                        error!("Failed to fetch links: {}", e);
                        *self.prefetch_error.lock().unwrap() = Some(e);
                        self.chunk_state_changed.notify_one();
                        break;
                    }
                }
            }

            // Wait for consumer to advance or cancellation
            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    debug!("Link prefetch loop cancelled during wait");
                    break;
                }
                _ = self.consumer_advanced.notified() => {
                    // Consumer moved forward, check if we need more links
                }
            }
        }
    }

    /// Spawn download tasks for available links up to concurrency limit.
    fn schedule_downloads(&self) {
        loop {
            // Check if we have room for more downloads
            let current_in_memory = self.chunks_in_memory.load(Ordering::Acquire);
            if current_in_memory >= self.max_chunks_in_memory {
                debug!(
                    "Memory limit reached: {}/{} chunks in memory, pausing downloads",
                    current_in_memory, self.max_chunks_in_memory
                );
                break;
            }

            // Get next chunk index to download
            let next_index = self.next_download_index.load(Ordering::Acquire);

            // Check if we have a link for this chunk
            let should_download = {
                if let Some(entry) = self.chunks.get(&next_index) {
                    matches!(entry.state, ChunkState::UrlFetched)
                } else {
                    false
                }
            };

            if !should_download {
                // No link available yet, or already downloading/downloaded
                break;
            }

            // Try to claim this chunk for download
            let claimed = {
                if let Some(mut entry) = self.chunks.get_mut(&next_index) {
                    if matches!(entry.state, ChunkState::UrlFetched) {
                        entry.state = ChunkState::Downloading;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            };

            if !claimed {
                break;
            }

            // Increment counters
            let new_count = self.chunks_in_memory.fetch_add(1, Ordering::Release) + 1;
            self.next_download_index.fetch_add(1, Ordering::Release);

            debug!(
                "Scheduling download for chunk {}: chunks_in_memory={}/{}, consumer_at={}",
                next_index,
                new_count,
                self.max_chunks_in_memory,
                self.current_chunk_index.load(Ordering::Acquire)
            );

            // Spawn download task
            let chunk_index = next_index;
            let downloader = Arc::clone(&self.chunk_downloader);
            let link_fetcher = Arc::clone(&self.link_fetcher);
            let chunks = self.chunks.clone();
            let chunk_state_changed = Arc::clone(&self.chunk_state_changed);
            let cancel_token = self.cancel_token.clone();
            let max_retries = self.config.max_retries;
            let retry_delay = self.config.retry_delay;

            self.runtime_handle.spawn(async move {
                let result = Self::download_chunk_with_retry(
                    chunk_index,
                    &downloader,
                    &link_fetcher,
                    &chunks,
                    max_retries,
                    retry_delay,
                    &cancel_token,
                )
                .await;

                match result {
                    Ok(batches) => {
                        if let Some(mut entry) = chunks.get_mut(&chunk_index) {
                            entry.batches = Some(batches);
                            entry.state = ChunkState::Downloaded;
                        }
                    }
                    Err(e) => {
                        error!("Failed to download chunk {}: {}", chunk_index, e);
                        if let Some(mut entry) = chunks.get_mut(&chunk_index) {
                            entry.state = ChunkState::DownloadFailed(e.to_string());
                        }
                    }
                }

                chunk_state_changed.notify_one();
            });
        }
    }

    /// Download a single chunk with retry and link refresh on expiry.
    async fn download_chunk_with_retry(
        chunk_index: i64,
        downloader: &ChunkDownloader,
        link_fetcher: &Arc<dyn ChunkLinkFetcher>,
        chunks: &Arc<DashMap<i64, ChunkEntry>>,
        max_retries: u32,
        retry_delay: std::time::Duration,
        cancel_token: &CancellationToken,
    ) -> Result<Vec<RecordBatch>> {
        let mut attempts = 0;

        loop {
            if cancel_token.is_cancelled() {
                return Err(DatabricksErrorHelper::invalid_state().message("Download cancelled"));
            }

            // Get link (may need to refetch if expired)
            let link = {
                let entry = chunks.get(&chunk_index);
                let stored_link = entry.as_ref().and_then(|e| e.link.clone());

                match stored_link {
                    Some(link) if !link.is_expired() => link,
                    _ => {
                        // Link missing or expired - refetch it
                        debug!("Refetching expired link for chunk {}", chunk_index);
                        let new_link = link_fetcher.refetch_link(chunk_index, 0).await?;

                        // Store the new link
                        if let Some(mut entry) = chunks.get_mut(&chunk_index) {
                            entry.link = Some(new_link.clone());
                        }

                        new_link
                    }
                }
            };

            // Attempt download
            match downloader.download(&link).await {
                Ok(batches) => return Ok(batches),
                Err(e) => {
                    attempts += 1;
                    if attempts >= max_retries {
                        return Err(e);
                    }
                    warn!(
                        "Chunk {} download failed (attempt {}/{}): {}, retrying...",
                        chunk_index, attempts, max_retries, e
                    );

                    // Update state to retry
                    if let Some(mut entry) = chunks.get_mut(&chunk_index) {
                        entry.state = ChunkState::DownloadRetry;
                    }

                    tokio::time::sleep(retry_delay).await;
                }
            }
        }
    }

    /// Wait for a specific chunk to be ready (Downloaded state).
    async fn wait_for_chunk(&self, chunk_index: i64) -> Result<()> {
        loop {
            // Check cancellation
            if self.cancel_token.is_cancelled() {
                return Err(DatabricksErrorHelper::invalid_state().message("Operation cancelled"));
            }

            // Check for prefetch errors
            if let Some(err) = self.prefetch_error.lock().unwrap().take() {
                return Err(err);
            }

            // Check if chunk is ready
            if let Some(entry) = self.chunks.get(&chunk_index) {
                match &entry.state {
                    ChunkState::Downloaded => return Ok(()),
                    ChunkState::DownloadFailed(e) => {
                        return Err(DatabricksErrorHelper::io().message(e.clone()))
                    }
                    ChunkState::ProcessingFailed(e) => {
                        return Err(DatabricksErrorHelper::io().message(e.clone()))
                    }
                    ChunkState::Cancelled => {
                        return Err(
                            DatabricksErrorHelper::invalid_state().message("Operation cancelled")
                        )
                    }
                    _ => {} // Keep waiting
                }
            } else if self.end_of_stream.load(Ordering::Acquire) {
                // Chunk doesn't exist and we're at end of stream
                return Err(DatabricksErrorHelper::invalid_state()
                    .message(format!("Chunk {} not available", chunk_index)));
            }

            // Wait for any chunk state change with timeout
            let timeout =
                self.config
                    .chunk_ready_timeout
                    .unwrap_or(std::time::Duration::from_secs(
                        DEFAULT_CHUNK_READY_TIMEOUT_SECS,
                    ));

            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    return Err(DatabricksErrorHelper::invalid_state().message("Operation cancelled"));
                }
                _ = self.chunk_state_changed.notified() => {
                    // State changed, loop to check
                }
                _ = tokio::time::sleep(timeout) => {
                    // Timeout - check state again and continue
                    debug!("Timeout waiting for chunk {}, continuing...", chunk_index);
                }
            }
        }
    }

    /// Get the schema, waiting if necessary.
    pub async fn get_schema(&self) -> Result<SchemaRef> {
        if let Some(schema) = self.schema.get() {
            return Ok(schema.clone());
        }

        // Need to peek the first batch to get schema
        let chunk_index = 0i64;

        // Ensure chunk 0 is being downloaded
        self.schedule_downloads();

        // Wait for chunk 0
        self.wait_for_chunk(chunk_index).await?;

        // Get schema from the first batch
        if let Some(entry) = self.chunks.get(&chunk_index) {
            if let Some(ref batches) = entry.batches {
                if let Some(batch) = batches.first() {
                    let schema = batch.schema();
                    let _ = self.schema.get_or_init(|| schema.clone());
                    return Ok(schema);
                }
            }
        }

        Err(DatabricksErrorHelper::invalid_state().message("Unable to determine schema"))
    }
}

impl Drop for StreamingCloudFetchProvider {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ChunkLinkFetchResult;
    use crate::types::cloudfetch::CloudFetchLink;
    use std::collections::HashMap;

    /// Mock ChunkLinkFetcher for testing
    #[allow(dead_code)]
    #[derive(Debug)]
    struct MockLinkFetcher {
        links_by_index: HashMap<i64, CloudFetchLink>,
        total_chunks: i64,
    }

    impl MockLinkFetcher {
        #[allow(dead_code)]
        fn new(total_chunks: i64) -> Self {
            let mut links_by_index = HashMap::new();
            for i in 0..total_chunks {
                links_by_index.insert(i, create_test_link(i));
            }
            Self {
                links_by_index,
                total_chunks,
            }
        }
    }

    #[async_trait::async_trait]
    impl ChunkLinkFetcher for MockLinkFetcher {
        async fn fetch_links(
            &self,
            start_chunk_index: i64,
            _start_row_offset: i64,
        ) -> Result<ChunkLinkFetchResult> {
            if start_chunk_index >= self.total_chunks {
                return Ok(ChunkLinkFetchResult::end_of_stream());
            }

            let links: Vec<CloudFetchLink> = (start_chunk_index..self.total_chunks)
                .take(3) // Return up to 3 links at a time
                .filter_map(|i| self.links_by_index.get(&i).cloned())
                .collect();

            let has_more = start_chunk_index + (links.len() as i64) < self.total_chunks;
            let next = if has_more {
                Some(start_chunk_index + links.len() as i64)
            } else {
                None
            };

            Ok(ChunkLinkFetchResult {
                links,
                has_more,
                next_chunk_index: next,
                next_row_offset: None,
            })
        }

        async fn refetch_link(&self, chunk_index: i64, _row_offset: i64) -> Result<CloudFetchLink> {
            self.links_by_index
                .get(&chunk_index)
                .cloned()
                .ok_or_else(|| {
                    DatabricksErrorHelper::invalid_state()
                        .message(format!("Link not found for chunk {}", chunk_index))
                })
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

    #[test]
    fn test_chunk_link_fetch_result_end_of_stream() {
        let result = ChunkLinkFetchResult::end_of_stream();
        assert!(result.links.is_empty());
        assert!(!result.has_more);
        assert!(result.next_chunk_index.is_none());
    }

    // Note: Full streaming provider tests require integration test setup
    // with mock HTTP responses for the chunk downloader
}
