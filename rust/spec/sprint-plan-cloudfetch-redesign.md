# Sprint Plan: CloudFetch Pipeline Redesign (DashMap → Channel-Based)

**Date:** 2026-02-28
**Sprint Duration:** 2 weeks
**JIRA Story:** [PECO-2926](https://databricks.atlassian.net/browse/PECO-2926)
**Epic:** [PECO-2888](https://databricks.atlassian.net/browse/PECO-2888) — Databricks Driver Test Framework
**Spec:** `rust/spec/cloudfetch-pipeline-redesign.md`

---

## Sprint Goal

Replace the `DashMap<i64, ChunkEntry>` coordination structure in `StreamingCloudFetchProvider` with a two-channel pipeline (`download_channel` + `result_channel`) aligned with the C# reference implementation, eliminating lock discipline issues, large data in concurrent maps, and poll-based consumer synchronisation.

---

## Background

The current implementation uses a single `DashMap` as the central coordination point for three unrelated concerns: presigned URL storage, download state machine, and result buffering (potentially 100 MB+). This causes:

- Lock discipline burden (must not hold DashMap shard locks across `.await` points)
- Unnatural indirection for URL refresh (two-`get_mut` dance)
- Large Arrow data stored in a concurrent map with `Clone`-derivation risk
- Poll-based consumer synchronisation via `Notify` loop

The C# driver avoids all of these by flowing each chunk through two channels. This sprint implements the same pattern in Rust.

---

## Corrections from C# Reference Comparison

Before implementation, the spec was audited against `csharp/src/Reader/CloudFetch/`. The following discrepancies were found and corrected in the spec:

| Area | Original Spec | Corrected |
|---|---|---|
| `fetch_links()` return type | Implied single link per call | Returns `Vec<CloudFetchLink>` (batch) — scheduler must iterate |
| Retry backoff | Said "exponential backoff is a follow-up" | **Linear**: `retry_delay * (attempt + 1)` matching C# `Task.Delay(_retryDelayMs * (retry + 1))` |
| `url_expiration_buffer_secs` | Hardcoded constant `LINK_EXPIRY_BUFFER_SECS = 30` | Configurable field, default **60s** (matching C# `UrlExpirationBufferSeconds = 60`) |
| `max_refresh_retries` | Referenced in spec but not defined | New `CloudFetchConfig` field, default **3** (matching C# `MaxUrlRefreshAttempts = 3`) |
| `num_download_workers` | "N workers" with no source for N | New `CloudFetchConfig` field, default **3** (matching C# `ParallelDownloads = 3`) |
| `max_retries` default | 5 | **3** (matching C# `MaxRetries = 3`) |
| `retry_delay` default | 1500ms | **500ms** (matching C# `RetryDelayMs = 500`) |
| `chunk_ready_timeout` | Not mentioned for removal | **Removed** — only served the `wait_for_chunk` Notify loop (deleted); C# has no equivalent wait-layer timeout |

---

## Architecture Overview

```
ChunkLinkFetcher → Scheduler → download_channel → Worker 1
                            ↘                  → Worker 2
                              result_channel      Worker N
                                    ↓               ↓ (oneshot)
                               Consumer       result_channel
```

**Two channels replace the DashMap:**
- `download_channel` — unbounded; scheduler pushes `ChunkDownloadTask`, workers pull
- `result_channel` — bounded to `max_chunks_in_memory`; scheduler pushes `ChunkHandle` in chunk-index order; consumer reads sequentially

**Key new types:**

```rust
struct ChunkDownloadTask {
    chunk_index: i64,
    link: CloudFetchLink,
    result_tx: oneshot::Sender<Result<Vec<RecordBatch>>>,
}

struct ChunkHandle {
    chunk_index: i64,
    result_rx: oneshot::Receiver<Result<Vec<RecordBatch>>>,
}
```

---

## Sub-Tasks

### PECO-2927 — Update `CloudFetchConfig` and remove legacy types ✅ COMPLETED

**Scope:** `rust/src/types/cloudfetch.rs`

**Status:** Completed (commit c6e8251)

**Changes:**
- Add 3 new fields to `CloudFetchConfig`:
  - `max_refresh_retries: u32` — default `3`
  - `num_download_workers: usize` — default `3`
  - `url_expiration_buffer_secs: u32` — default `60`
- Remove `chunk_ready_timeout: Option<Duration>`
- Correct defaults: `max_retries` 5→3, `retry_delay` 1500ms→500ms
- Remove `LINK_EXPIRY_BUFFER_SECS` constant (replaced by `url_expiration_buffer_secs`)
- Remove `ChunkEntry` and `ChunkState` types entirely
- Update `CloudFetchLink::is_expired()` to accept a buffer parameter (in seconds) instead of using the hardcoded constant
- Update all tests in this file

**Implementation Notes:**
- ChunkEntry and ChunkState were moved temporarily to `streaming_provider.rs` as local types to keep the build working
- These local types will be removed when PECO-2928 through PECO-2931 implement the channel-based pipeline
- Added new config options in database.rs: `max_refresh_retries`, `num_download_workers`, `url_expiration_buffer_secs`
- Removed deprecated `chunk_ready_timeout_ms` option from database.rs

---

### PECO-2928 — Implement `ChunkDownloadTask`, `ChunkHandle`, and Scheduler ✅ COMPLETED

**Scope:** New types + new scheduler task in `rust/src/reader/cloudfetch/`

**Status:** Completed

**Changes (Types - COMPLETED):**
- Add `ChunkDownloadTask` struct with `chunk_index`, `link`, `result_tx` (oneshot sender)
- Add `ChunkHandle` struct with `chunk_index`, `result_rx` (oneshot receiver)
- Add `create_chunk_pair()` helper function for creating connected task/handle pairs
- Types defined in new module: `rust/src/reader/cloudfetch/pipeline_types.rs`
- Types exported from `mod.rs`

**Changes (Scheduler - COMPLETED):**
- Implemented scheduler as a `tokio::spawn` task in `rust/src/reader/cloudfetch/scheduler.rs`:
  - `spawn_scheduler()` function creates channels and spawns scheduler task
  - `scheduler_task()` calls `fetch_links()` which returns `Vec<CloudFetchLink>` (batch)
  - For each link in the batch: creates `oneshot::channel()` via `create_chunk_pair()`, sends `ChunkHandle` to `result_channel`, then sends `ChunkDownloadTask` to `download_channel`
  - `download_channel`: unbounded mpsc for `ChunkDownloadTask`
  - `result_channel`: bounded mpsc (capacity = `max_chunks_in_memory`) for `ChunkHandle`
  - Bounded `result_channel` provides automatic backpressure (blocks scheduler when `max_chunks_in_memory` reached)
  - Exits when `has_more = false`
  - Supports cancellation via `CancellationToken`
- Returns `SchedulerChannels` struct containing both channel receivers

**Key invariant:** `ChunkHandle` is enqueued to `result_channel` _before_ the corresponding `ChunkDownloadTask` is dispatched to `download_channel`, preserving sequential ordering even when downloads complete out of order.

**Tests implemented:**
- `scheduler_sends_handles_in_chunk_index_order` — verify handles received in sequence
- `scheduler_processes_batch_links` — verify all tasks/handles enqueued from batch
- `backpressure_blocks_scheduler_at_capacity` — verify bounded channel blocks scheduler
- `scheduler_exits_when_has_more_false` — verify clean exit
- `scheduler_cancellation` — verify early termination on cancel
- `scheduler_handle_before_task` — verify ordering invariant
- `scheduler_multiple_batches` — verify multiple fetch_links calls
- `scheduler_empty_batch` — verify empty batch handling

---

### PECO-2929 — Implement Download Workers ✅ COMPLETED

**Scope:** `rust/src/reader/cloudfetch/` (replaces `download_chunk_with_retry`)

**Status:** Completed

**Changes:**
- Spawn `config.num_download_workers` long-lived `tokio::spawn` tasks
- Each worker loops over `download_channel`:
  1. **Proactive expiry check** using `url_expiration_buffer_secs` buffer — if link is expired or expiring soon, call `refetch_link()` before first HTTP request
  2. `GET presigned_url`
  3. On success: parse Arrow IPC → `result_tx.send(Ok(batches))`
  4. On 401/403/404: call `refetch_link()`, retry immediately (no sleep), count against `max_refresh_retries`
  5. On other errors: `sleep(retry_delay * (attempt + 1))` (linear backoff), retry, count against `max_retries`
  6. On `max_retries` exceeded: `result_tx.send(Err(...))`
- All sleeps use `tokio::select!` on `cancel_token.cancelled()` for cancellation

**Retry contract:**

| Error type | Sleep before retry | Counts against `max_retries` | Counts against `max_refresh_retries` |
|---|---|---|---|
| Network / 5xx | Yes — `retry_delay * (attempt + 1)` | Yes | No |
| 401 / 403 / 404 | No | Yes | Yes |
| Link proactively expired | No | No | Yes |

**Implementation Notes:**
- `download_workers.rs` module implements the worker pool with `spawn_download_workers()`
- `WorkerConfig` struct extracts relevant configuration from `CloudFetchConfig`
- Uses `DownloadError` enum from `chunk_downloader.rs` for categorized error handling
- All tests pass including: `test_worker_retries_on_transient_error`, `test_worker_uses_linear_backoff`, `test_worker_refetches_url_on_401_403_404`, `test_worker_no_sleep_on_auth_error`, `test_worker_gives_up_after_max_refresh_retries`, `test_worker_proactively_refreshes_expiring_url`

---

### PECO-2930 — Implement Consumer (`next_batch`) ✅ COMPLETED

**Scope:** `rust/src/reader/cloudfetch/streaming_provider.rs`

**Status:** Completed

**Changes:**
- Replace `wait_for_chunk` + Notify poll loop with:
  ```rust
  let handle = result_rx.recv().await?;  // get next ChunkHandle in order
  let batches = handle.result_rx.await?;  // await the oneshot directly
  ```
- `Ok(None)` when `result_rx` returns `None` (channel closed = end of stream)
- Consumer selects on `cancel_token.cancelled()` when awaiting `result_rx`

**Implementation Notes:**
- `next_batch()` now uses `tokio::select!` with `biased;` to check cancellation first
- Uses `tokio::sync::Mutex` for `result_rx` to allow holding lock across await
- Properly awaits `handle.result_rx` oneshot for download results
- Error propagation: oneshot recv error = "Download task was cancelled or failed"
- Recursive call via `Box::pin()` for empty chunks

**Tests implemented:**
- `test_sequential_consumption` — verify batches received in chunk order
- `test_end_of_stream_detection` — verify Ok(None) when channel closes
- `test_cancellation_during_await` — verify cancellation error returned
- `test_download_error_propagation` — verify errors passed through
- `test_sender_dropped_error` — verify error when worker crashes
- `test_schema_extraction` — verify schema captured from first batch
- `test_multiple_batches_per_chunk` — verify all batches returned

---

### PECO-2931 — Refactor `StreamingCloudFetchProvider` struct ✅ COMPLETED

**Scope:** `rust/src/reader/cloudfetch/streaming_provider.rs`

**Status:** Completed

**Remove these fields:**

| Field | Replaced by |
|---|---|
| `chunks: Arc<DashMap<i64, ChunkEntry>>` | `download_channel` + `result_channel` |
| `chunks_in_memory: AtomicUsize` | Bounded `result_channel` capacity |
| `max_chunks_in_memory: usize` | `mpsc::channel(max_chunks_in_memory)` bound |
| `chunk_state_changed: Arc<Notify>` | `oneshot` receivers per chunk |
| `next_download_index: AtomicI64` | Sequential counter owned by scheduler task |
| `current_chunk_index: AtomicI64` | Implicit in sequential `result_rx.recv()` calls |
| `end_of_stream: AtomicBool` | Implicit when `result_rx` returns `None` |
| `link_fetcher: Arc<dyn ChunkLinkFetcher>` | Passed to scheduler/workers |
| `chunk_downloader: Arc<ChunkDownloader>` | Passed to workers |
| `config: CloudFetchConfig` | Used during initialization only |
| `runtime_handle: tokio::runtime::Handle` | No longer needed (scheduler spawns internally) |

**New struct:**
```rust
pub struct StreamingCloudFetchProvider {
    result_rx: tokio::sync::Mutex<mpsc::Receiver<ChunkHandle>>,
    schema: OnceLock<SchemaRef>,
    batch_buffer: Mutex<VecDeque<RecordBatch>>,
    cancel_token: CancellationToken,
    scheduler_handle: JoinHandle<()>,
    worker_handles: Vec<JoinHandle<()>>,
}
```

**Implementation Notes:**
- `new()` spawns scheduler and workers immediately via `spawn_scheduler()` and `spawn_download_workers()`
- Uses `tokio::sync::Mutex` for `result_rx` (needs to hold across await)
- Uses `std::sync::Mutex` for `batch_buffer` (only accessed synchronously)
- Removed `initialize()` method — scheduler handles initial link fetch
- Removed `schedule_downloads()` method — scheduler handles scheduling
- Removed `wait_for_chunk()` method — direct await on oneshot
- Removed `fetch_and_store_links()` method — scheduler handles this
- Removed `download_chunk_with_retry()` method — workers handle this
- Removed `ChunkState`, `ChunkEntry` local types — no longer needed

---

### PECO-2932 — Unit Tests (9 tests)

| Test | What it verifies |
|---|---|
| `scheduler_sends_handles_in_chunk_index_order` | `result_channel` receives handles in sequence |
| `scheduler_processes_batch_links` | `fetch_links()` returns 3 links; all 3 tasks enqueued in order |
| `worker_retries_on_transient_error` | Mock downloader fails N times then succeeds |
| `worker_uses_linear_backoff` | Sleep duration is `retry_delay * (attempt + 1)`, not constant or exponential |
| `worker_refetches_url_on_401_403_404` | Auth error triggers `refetch_link` inline |
| `worker_no_sleep_on_auth_error` | Auth error does not sleep before refetch |
| `worker_gives_up_after_max_refresh_retries` | Terminal error propagated via `result_tx` |
| `worker_proactively_refreshes_expiring_url` | Link expiring within buffer triggers `refetch_link` before first HTTP request |
| `backpressure_blocks_scheduler_at_capacity` | Full `result_channel` blocks scheduler, not consumer |

---

### PECO-2933 — Integration Tests (3 tests)

| Test | What it verifies |
|---|---|
| `end_to_end_sequential_consumption` | All chunks downloaded and read in order |
| `end_to_end_cancellation_mid_stream` | Cancel during active download — no deadlock or panic |
| `end_to_end_401_recovery` | Presigned URL expires mid-stream; driver refetches and continues |

---

## Concurrency Model

| Component | Primitive | Reason |
|---|---|---|
| Scheduler | Single `tokio::spawn` | Sequential chunk ordering required |
| Download workers | N `tokio::spawn` sharing `download_channel` | Parallel downloads |
| Consumer | Caller's task (no spawn) | Sequential result consumption |
| `result_channel` | Bounded `mpsc` (capacity = `max_chunks_in_memory`) | Backpressure without manual counter |
| URL refresh in worker | Local variable mutation | No shared state, no lock needed |

---

## Error Handling

| Scenario | Behaviour |
|---|---|
| Download fails after `max_retries` | `result_tx.send(Err(...))` — consumer sees error on `result_rx.await` |
| URL refresh fails after `max_refresh_retries` | Same as above |
| `refetch_link()` itself returns error | Propagated via `result_tx.send(Err(...))` |
| Cancellation during sleep | `tokio::select!` on `cancel_token.cancelled()` interrupts sleep |
| Cancellation during `result_rx.await` | Consumer selects on `cancel_token.cancelled()` |
| Scheduler exits (all chunks sent) | `download_channel` sender drops → workers drain then exit |
| All workers exit | `oneshot` senders drop → `result_rx.await` returns `Err(RecvError)` |

---

## Files to Modify

| File | Change |
|---|---|
| `rust/src/types/cloudfetch.rs` | Update `CloudFetchConfig`, remove `ChunkEntry`/`ChunkState` |
| `rust/src/reader/cloudfetch/streaming_provider.rs` | Full rewrite of struct + pipeline |
| `rust/src/reader/cloudfetch/chunk_downloader.rs` | Replace with worker loop |
| `rust/src/reader/cloudfetch/link_fetcher.rs` | No changes (internal DashMap cache retained) |

---

## Definition of Done

- [x] `cargo build` passes with no warnings
- [x] `cargo clippy -- -D warnings` passes
- [x] All 9 unit tests pass (56 cloudfetch tests total)
- [ ] All 3 integration tests pass (pending E2E testing)
- [x] `cargo fmt` applied
- [x] `DashMap` dependency removed from `streaming_provider.rs`
- [x] `ChunkEntry`, `ChunkState` types deleted
- [x] `chunk_ready_timeout` config field removed
- [x] `LINK_EXPIRY_BUFFER_SECS` constant removed
