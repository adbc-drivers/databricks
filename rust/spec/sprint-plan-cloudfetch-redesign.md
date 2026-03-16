# Sprint Plan: CloudFetch Pipeline Redesign (DashMap → Channel-Based)

**Date:** 2026-02-28
**Sprint Duration:** 2 weeks
**JIRA Story:** [PECO-2926](https://databricks.atlassian.net/browse/PECO-2926)
**Epic:** [PECO-2888](https://databricks.atlassian.net/browse/PECO-2888) — Databricks Driver Test Framework
**Spec:** `rust/spec/cloudfetch-pipeline-redesign.md`

---

## Orchestration Settings

**E2E task:** When `rust/spec/e2e-test-spec.md` is present, the generator always appends a final E2E task to the sprint. This task runs after all implementation tasks are complete. The E2E Agent reads the design doc and e2e-test-spec.md only — it has no knowledge of individual task implementations. See `rust/spec/orchestration_spec.md` for the E2E Agent protocol.

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

### PECO-2927 — Update `CloudFetchConfig` and remove legacy types

**Scope:** `rust/src/types/cloudfetch.rs`

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

---

### PECO-2928 — Implement `ChunkDownloadTask`, `ChunkHandle`, and Scheduler ✅

**Status:** Complete

**Scope:** New types + new scheduler task in `rust/src/reader/cloudfetch/`

**Changes (Types):**
- Add `ChunkDownloadTask` struct with `chunk_index`, `link`, `result_tx` (oneshot sender)
- Add `ChunkHandle` struct with `chunk_index`, `result_rx` (oneshot receiver)
- Add `create_chunk_pair()` helper function for creating connected task/handle pairs
- Types defined in new module: `rust/src/reader/cloudfetch/pipeline_types.rs`
- Types exported from `mod.rs`

**Changes (Scheduler):**
- Implemented scheduler as a `tokio::spawn` task in `rust/src/reader/cloudfetch/scheduler.rs`:
  - `spawn_scheduler()` function creates channels and spawns scheduler task
  - `scheduler_task()` calls `fetch_links()` which returns `Vec<CloudFetchLink>` (batch)
  - For each link in the batch: creates `oneshot::channel()` via `create_chunk_pair()`, sends `ChunkHandle` to `result_channel`, then sends `ChunkDownloadTask` to `download_channel`
  - `download_channel`: unbounded mpsc for `ChunkDownloadTask`
  - `result_channel`: bounded mpsc (capacity = `max_chunks_in_memory`) for `ChunkHandle`
  - Bounded `result_channel` provides automatic backpressure (blocks scheduler when `max_chunks_in_memory` reached)
  - Exits when `has_more = false`
  - Supports cancellation via `CancellationToken`
- Returns `SchedulerChannels` struct containing both channel receivers and the JoinHandle
- `SchedulerChannels` struct includes `join_handle: JoinHandle<()>` for awaiting scheduler completion

**Key invariant:** `ChunkHandle` is enqueued to `result_channel` _before_ the corresponding `ChunkDownloadTask` is dispatched to `download_channel`, preserving sequential ordering even when downloads complete out of order.

**Implementation notes:**
- `scheduler_task()` is private; only `spawn_scheduler()` is public API
- Cancellation is checked at 4 points: before fetch, during fetch (via `tokio::select!`), before each link, and during handle send
- On fetch_links error, scheduler logs and exits (drops senders, signaling end-of-stream)
- Tracks `next_chunk_index` and `next_row_offset` from both link iteration and batch metadata

**Tests implemented (all 8 passing):**
- `scheduler_sends_handles_in_chunk_index_order` — verify handles received in sequence
- `scheduler_processes_batch_links` — verify all tasks/handles enqueued from batch
- `backpressure_blocks_scheduler_at_capacity` — verify bounded channel blocks scheduler
- `scheduler_exits_when_has_more_false` — verify clean exit
- `scheduler_cancellation` — verify early termination on cancel
- `scheduler_handle_before_task` — verify ordering invariant (uses capacity=1 to make ordering observable)
- `scheduler_multiple_batches` — verify multiple fetch_links calls
- `scheduler_empty_batch` — verify empty batch handling

---

### PECO-2929 — Implement Download Workers ✅

**Status:** Complete

**Scope:** `rust/src/reader/cloudfetch/chunk_downloader.rs` (replaces `download_chunk_with_retry`)

**Changes:**
- Added `ChunkDownload` trait to abstract the download operation for testability
- `ChunkDownloader` implements `ChunkDownload` via `#[async_trait]`
- `spawn_download_workers()` spawns `config.num_download_workers` long-lived `tokio::spawn` tasks
- Workers share the download channel receiver via `Arc<Mutex<mpsc::UnboundedReceiver<ChunkDownloadTask>>>`
- Each worker loops over `download_channel`:
  1. **Proactive expiry check** using `url_expiration_buffer_secs` buffer — if link is expired or expiring soon, calls `refetch_link()` before first HTTP request
  2. Downloads via `ChunkDownload.download()`
  3. On success: returns `Ok(batches)` via `result_tx.send()`
  4. On 401/403/404: calls `refetch_link()`, retries immediately (no sleep), counts against both `max_retries` and `max_refresh_retries`
  5. On other errors: `sleep(retry_delay * (attempt + 1))` (linear backoff), retries, counts against `max_retries` only
  6. On `max_retries` or `max_refresh_retries` exceeded: sends `Err(...)` via `result_tx`
- All sleeps use `tokio::select!` on `cancel_token.cancelled()` for cancellation
- Auth error detection via `is_auth_error()` inspects error message for "HTTP 401", "HTTP 403", "HTTP 404" patterns
- Exported `spawn_download_workers` and `ChunkDownload` from `mod.rs`

**Implementation notes:**
- `download_chunk()` is a standalone async function (not a method) for clean testability
- Worker loop holds the `Mutex<UnboundedReceiver>` lock only briefly during `recv()`, not during processing
- Proactive expiry check counts against `max_refresh_retries` only (not `max_retries`), matching C# parity
- `refetch_link()` failure during retry is treated as a terminal error (no further retries)

**Tests implemented (all 8 passing):**
- `worker_retries_on_transient_error` — mock downloader fails 2 times then succeeds
- `worker_uses_linear_backoff` — measures elapsed time to verify linear backoff (100ms * 1 + 100ms * 2 = 300ms)
- `worker_refetches_url_on_401_403_404` — tests each of 401, 403, 404 triggers refetch_link
- `worker_no_sleep_on_auth_error` — uses 60s retry_delay, verifies completes in <500ms (no sleep)
- `worker_gives_up_after_max_refresh_retries` — verifies terminal error with `max_refresh_retries=2`
- `worker_proactively_refreshes_expiring_url` — link expires in 10s with 60s buffer, verifies refetch before download
- `spawn_workers_process_tasks_and_exit` — end-to-end test with 2 workers processing 3 tasks
- `test_chunk_downloader_creation` — existing test preserved

**Retry contract:**

| Error type | Sleep before retry | Counts against `max_retries` | Counts against `max_refresh_retries` |
|---|---|---|---|
| Network / 5xx | Yes — `retry_delay * (attempt + 1)` | Yes | No |
| 401 / 403 / 404 | No | Yes | Yes |
| Link proactively expired | No | No | Yes |

---

### PECO-2930 — Implement Consumer (`next_batch`) ✅

**Status:** Complete

**Scope:** `rust/src/reader/cloudfetch/streaming_provider.rs`

**Changes:**
- Replaced `wait_for_chunk` + Notify poll loop with channel-based pattern:
  ```rust
  let handle = rx.recv().await;     // get next ChunkHandle in order
  let batches = handle.result_rx.await;  // await the oneshot directly
  ```
- `Ok(None)` returned when `result_rx` returns `None` (channel closed = end of stream)
- Consumer selects on `cancel_token.cancelled()` when awaiting both `result_rx.recv()` and `handle.result_rx`
- Empty chunks handled via recursive `Box::pin(self.next_batch()).await`

**Implementation notes:**
- `result_rx` uses `tokio::sync::Mutex` (not `std::sync::Mutex`) because the guard is held across the `tokio::select!` await point
- `batch_buffer` uses `std::sync::Mutex` since it is only held briefly and never across await points
- `get_schema()` reads the first batch to extract the schema, then pushes it back into `batch_buffer` so it isn't lost
- Constructor signature simplified: `new(config, link_fetcher, downloader)` — no `runtime_handle` parameter, returns `Self` not `Arc<Self>`

**Tests implemented (all 7 passing):**
- `test_chunk_link_fetch_result_end_of_stream` — verify ChunkLinkFetchResult end-of-stream helper
- `test_next_batch_returns_batches_in_order` — 3 chunks consumed sequentially
- `test_next_batch_returns_none_at_end_of_stream` — Ok(None) after last chunk
- `test_get_schema_from_first_batch` — schema extracted and batch preserved
- `test_cancel_stops_pipeline` — cancel prevents further consumption
- `test_drop_cancels_token` — Drop impl triggers cancellation
- `test_empty_result_set` — zero chunks returns Ok(None) immediately

---

### PECO-2931 — Refactor `StreamingCloudFetchProvider` struct ✅

**Status:** Complete

**Scope:** `rust/src/reader/cloudfetch/streaming_provider.rs`, `rust/src/reader/mod.rs`

**Removed fields:**

| Field | Replaced by |
|---|---|
| `chunks: Arc<DashMap<i64, ChunkEntry>>` | `download_channel` + `result_channel` |
| `chunks_in_memory: AtomicUsize` | Bounded `result_channel` capacity |
| `max_chunks_in_memory: usize` | `mpsc::channel(max_chunks_in_memory)` bound |
| `chunk_state_changed: Arc<Notify>` | `oneshot` receivers per chunk |
| `next_download_index: AtomicI64` | Sequential counter owned by scheduler task |
| `current_chunk_index: AtomicI64` | Implicit in sequential `result_rx.recv()` calls |
| `end_of_stream: AtomicBool` | Implicit when `result_rx` returns `None` |
| `link_fetcher: Arc<dyn ChunkLinkFetcher>` | Passed to scheduler/workers during construction |
| `chunk_downloader: Arc<ChunkDownloader>` | Passed to workers during construction |
| `config: CloudFetchConfig` | Used during construction only |
| `runtime_handle: tokio::runtime::Handle` | No longer needed (pipeline is self-contained) |

**Also removed:**
- `ChunkState` and `ChunkEntry` local stubs (no longer needed)
- `initialize()`, `fetch_and_store_links()`, `schedule_downloads()`, `download_chunk_with_retry()`, `wait_for_chunk()` — all replaced by pipeline

**New struct:**
```rust
pub struct StreamingCloudFetchProvider {
    result_rx: TokioMutex<tokio::sync::mpsc::Receiver<ChunkHandle>>,
    schema: OnceLock<SchemaRef>,
    batch_buffer: Mutex<VecDeque<RecordBatch>>,
    cancel_token: CancellationToken,
}
```

**`reader/mod.rs` changes:**
- `create_cloudfetch_reader()` updated to call `StreamingCloudFetchProvider::new(config, link_fetcher, chunk_downloader)` (3 parameters)
- `ChunkDownloader` wrapped as `Arc<dyn ChunkDownload>` (uses trait object)
- Provider wrapped in `Arc::new(provider)` for `CloudFetchResultReader`

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

- [ ] `cargo build` passes with no warnings
- [ ] `cargo clippy -- -D warnings` passes
- [ ] All 9 unit tests pass
- [ ] All 3 integration tests pass
- [ ] `cargo fmt` applied
- [ ] `DashMap` dependency removed from `streaming_provider.rs`
- [ ] `ChunkEntry`, `ChunkState` types deleted
- [ ] `chunk_ready_timeout` config field removed
- [ ] `LINK_EXPIRY_BUFFER_SECS` constant removed

E2E validation is handled by the auto-appended E2E task. See `rust/spec/e2e-test-spec.md` and `rust/spec/orchestration_spec.md`.
