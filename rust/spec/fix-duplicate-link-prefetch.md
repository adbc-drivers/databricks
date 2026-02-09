<!--
  Copyright (c) 2025 ADBC Drivers Contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Fix Duplicate Link Prefetch: Consolidate into Single Layer

## Context

There are two independent prefetch loops that race to fetch the same chunk links, causing duplicate `get_result_chunks` API calls:

1. **`SeaChunkLinkFetcher::run_prefetch_loop`** — spawned by `SeaChunkLinkFetcherHandle::new()` on creation, fetches links ahead of consumer into a `DashMap` cache
2. **`StreamingCloudFetchProvider::run_link_prefetch_loop`** — runs after `initialize()`, calls `link_fetcher.fetch_links(prefetched, ...)` which hits a cache miss and issues a second API call for the same chunk index

The fix: remove the provider's prefetch loop entirely. The `SeaChunkLinkFetcher` already handles all prefetching. Make `schedule_downloads()` pull links on-demand from the fetcher.

---

## Changes

### 1. `streaming_provider.rs` — Remove provider prefetch loop, add on-demand link fetching

**Remove fields:**
- `next_link_fetch_index: AtomicI64` — only tracked what the prefetch loop fetched
- `consumer_advanced: Arc<Notify>` — only woke the prefetch loop
- `prefetch_error: Arc<Mutex<Option<Error>>>` — only captured prefetch loop errors

**Remove methods:**
- `run_link_prefetch_loop()` — the redundant prefetch loop

**Modify `initialize()`:**
- Keep `link_fetcher.fetch_links(0, 0)` to populate `chunks` map with initial links
- Keep `schedule_downloads()` call
- Set `end_of_stream` from `has_more`
- **Remove** the `self.run_link_prefetch_loop().await` call at the end
- Errors go directly to `chunk_state_changed` (already happens on line 189)

**Modify `schedule_downloads()`:**
- Currently breaks when `next_download_index` chunk isn't in the `chunks` map (line 392-395)
- New behavior: when the chunk isn't in the map, call `self.fetch_and_store_links(next_download_index)` to pull from the fetcher cache (or trigger a server fetch via the fetcher)
- If that returns no links for `next_download_index`, break as before

**Add `fetch_and_store_links()` private async helper:**
- Calls `self.link_fetcher.fetch_links(start_index, 0).await`
- Stores returned links in `self.chunks` map as `ChunkEntry::with_link()`
- Sets `self.end_of_stream` if `has_more == false`
- Returns whether `start_index` is now available

**Problem: `schedule_downloads()` is sync but `fetch_links` is async.**
- `schedule_downloads()` is called from both sync (`next_batch` via `block_on`) and async (`initialize`) contexts
- Solution: make `schedule_downloads()` async, and update callers:
  - `initialize()` — already async, just `.await`
  - `next_batch()` — already async, just `.await`

**Modify `next_batch()`:**
- Remove the `self.consumer_advanced.notify_one()` call (no prefetch loop to wake)
- Remove `prefetch_error` check at the top
- Keep everything else the same

**Modify `new()`:**
- Remove `next_link_fetch_index`, `consumer_advanced`, `prefetch_error` from constructor

### 2. `link_fetcher.rs` — Remove the TODO comment

- Remove the TODO at lines 467-478 since the fix eliminates the race

### 3. No changes to `link_fetcher.rs` logic

The `SeaChunkLinkFetcher` already works correctly:
- `SeaChunkLinkFetcherHandle::new()` triggers async prefetch on creation
- `fetch_links()` returns from cache or falls back to sync fetch
- `maybe_trigger_prefetch()` keeps links ahead of consumer
- All of this stays as-is

---

## Files Changed

| File | Change |
|------|--------|
| `reader/cloudfetch/streaming_provider.rs` | Remove prefetch loop + 3 fields, make `schedule_downloads` async, add `fetch_and_store_links` |
| `reader/cloudfetch/link_fetcher.rs` | Remove TODO comment |

## Verification

```bash
cargo build && cargo test && cargo clippy -- -D warnings
cargo run --example large_chunk_test 2>&1 | grep -E "get_result_chunks|Fetching chunk links|Chunks response|cache miss"
```

Verify in debug logs that `/result/chunks/32` is called only **once** (by the fetcher's prefetch), not twice.
