# Gap Report — PECO-3022 SEA Telemetry Integration

**Report date:** 2026-05-14
**Branch under review:** `stack/pr-phase5-sea-statement-telemetry`
**Design doc:** [`docs/designs/PECO-3022-sea-telemetry-integration-design.md`](../../docs/designs/PECO-3022-sea-telemetry-integration-design.md)
**Sprint plan:** [`csharp/doc/sprint-plan-PECO-3022-sea-telemetry-2026-05-14.md`](./sprint-plan-PECO-3022-sea-telemetry-2026-05-14.md)
**Jira:** [PECO-3022](https://databricks.atlassian.net/browse/PECO-3022)

---

## 1. Bottom line

Foundation work (T1, T2, T4, T5) is complete and matches the design. T3 (`SeaResultFormatMapper`) was never started, and T6 (`StatementExecutionStatement` hookpoints) is **4 of 7 wired**. The combination produces a **critical functional gap**: no SEA telemetry record reaches the wire today, because `OnFinalized` — the only path that enqueues a `OssSqlDriverTelemetryLog` — is not called from `StatementExecutionStatement.Dispose`.

Closing the gap is straightforward and bounded — roughly 1.5–2 days of focused work, all in `StatementExecutionStatement.cs` and one new mapper file.

---

## 2. What's complete and on-spec

| Item | Status | Evidence |
|---|---|---|
| `IStatementOperationObserver` interface | DONE | `csharp/src/Telemetry/IStatementOperationObserver.cs` — 8 methods, fail-open contract documented per design §5.1 |
| `TelemetryObserver` impl | DONE | `csharp/src/Telemetry/TelemetryObserver.cs` — `Safe(Action)` helper per design §12, idempotent `OnFinalized` via `Interlocked.CompareExchange` |
| `NullObserver` singleton | DONE | `csharp/src/Telemetry/NullObserver.cs` |
| `SafeObserver` decorator | DONE | `csharp/src/Telemetry/SafeObserver.cs` |
| T1: `ConnectionTelemetry.Create` refactor (`string sessionId` + `DriverMode mode`) | DONE | `csharp/src/Telemetry/ConnectionTelemetry.cs:73-83`; hardcoded `Thrift` removed at lines 466, 651 |
| T1: Thrift caller converts at boundary | DONE | `csharp/src/DatabricksConnection.cs:737-747` — `new Guid(SessionHandle.SessionId.Guid).ToString()`, `mode: Thrift` |
| T4: `DatabricksStatement` observer field | DONE | `csharp/src/DatabricksStatement.cs:81`, injected from `DatabricksConnection.CreateStatement` at line 537-541 |
| T4: Private telemetry hooks removed | DONE | `CreateTelemetryContext`, `RecordSuccess`, `RecordError`, `EmitTelemetry` no longer present |
| T5: `StatementExecutionConnection._telemetry` field + lifecycle | DONE | `StatementExecutionConnection.cs:66`; `InitializeTelemetry` at line 457; `EmitCreateSessionTelemetry` at line 459 |
| T5: `DriverMode.Sea` passed for SEA | DONE | `StatementExecutionConnection.cs:487` |
| T5: 5-second dispose timeout | DONE | `StatementExecutionConnection.cs:1104` — `_telemetry.DisposeAsync().Wait(TimeSpan.FromSeconds(5))` in try/catch |
| T5: Observer construction in `CreateStatement` | DONE | `StatementExecutionConnection.cs:577-587` — `TelemetryObserver(session)` when telemetry enabled, else `NullObserver.Instance` |

---

## 3. Critical gaps

### G1 — `OnFinalized` not wired into `StatementExecutionStatement.Dispose` ⚠️ **Blocking**

**Severity:** Critical. `TelemetryObserver.OnFinalized` is the only path that builds a `OssSqlDriverTelemetryLog` and enqueues it for export. Without this call, every observer method up to this point only mutates an in-memory `StatementTelemetryContext` that is garbage-collected when the statement disposes. Zero SEA telemetry records reach `eng_lumberjack`.

**Location:** `csharp/src/StatementExecution/StatementExecutionStatement.cs:887` (`Dispose`).

**Fix:** Add `_observer.OnFinalized()` in the dispose path. The observer's idempotency gate (CAS on `_emitted`) guarantees exactly-once even if the error path later also calls `OnFinalized`.

**Effort:** ~30 minutes including a unit test that confirms exactly-once after both error and dispose.

---

### G2 — `SeaResultFormatMapper` (T3) not created

**Severity:** High. Without the mapper, `OnExecuteSucceeded` cannot populate `result_format` — currently passes `ExecutionResultFormat.Unspecified` as a placeholder. Result format is one of the sprint-plan success-criteria fields.

**Locations:**
- Missing file: `csharp/src/StatementExecution/SeaResultFormatMapper.cs`
- Placeholder usage: `csharp/src/StatementExecution/StatementExecutionStatement.cs:389-391` (comment notes the helper "is being added in a parallel phase 6 PR")

**Fix:** Implement per design §8 four-cell mapping table. Pure-function static helper. Add unit tests for all four cells.

**Effort:** ~½ day.

---

### G3 — Reader-side observer hookpoints not wired

**Severity:** High. Three of the seven hookpoints from design §6 are missing. Each one corresponds to a specific telemetry field that will be absent or null on the wire.

| Hookpoint | Design § | Expected location | Field affected | Status |
|---|---|---|---|---|
| `OnFirstBatchReady` | §6 row 4 | `CreateCloudFetchReader` (line 612) + `InlineArrowStreamReader` ctor (line 970) | `result_latency.result_set_ready_latency_millis` | Missing |
| `OnConsumed` | §6 row 5 | Reader Dispose for both paths | `result_latency.result_set_consumption_latency_millis` | Missing |
| `OnChunksDownloaded` | §6 row 5 | CloudFetch reader Dispose | `chunk_details` (initial/slowest/sum/totals) | Missing |

**Fix:** Thread the observer through the relevant reader classes (`InlineArrowStreamReader`, `CloudFetchReader`/`StatementExecutionResultFetcher`). For chunk metrics, depends on the gap-fix workstream's `ChunkMetrics` plumbing — if not available, pass `ChunkMetrics.Empty`.

**Effort:** ~1 day for `OnFirstBatchReady` + `OnConsumed`. `OnChunksDownloaded` adds ~0.5 day if the chunk-metrics aggregator from gap-fix is not yet exposed.

---

## 4. Hookpoint wiring matrix

For quick reference — current state of all 7 hookpoints in `StatementExecutionStatement`:

| # | Hookpoint | Design § | Current state | File:Line |
|---|---|---|---|---|
| 1 | `OnExecuteStarted` | §6 row 2 | ✅ Wired | `StatementExecutionStatement.cs:360` |
| 2 | `OnExecuteSucceeded` | §6 row 3 | ⚠️ Wired but passes `Unspecified` (blocked on G2) | `StatementExecutionStatement.cs:391` |
| 3 | `OnPollCompleted` | §6 row 4 | ✅ Wired | `StatementExecutionStatement.cs:554` (accumulated, emitted once on terminal) |
| 4 | `OnFirstBatchReady` | §6 row 5 | ❌ Missing | n/a |
| 5 | `OnConsumed` | §6 row 6 | ❌ Missing | n/a |
| 6 | `OnChunksDownloaded` | §6 row 6 | ❌ Missing | n/a |
| 7 | `OnError` | §6 row 7 | ✅ Wired | `StatementExecutionStatement.cs:462` |
| 8 | `OnFinalized` | §6 row 8 | ❌ **Missing — blocks all SEA telemetry emission** | n/a |

---

## 5. Minor divergences (non-blocking)

### D1 — `connectTimeoutMilliseconds` semantically mislabeled

**Location:** `StatementExecutionConnection.cs:490` — passes `(int)TimeSpan.FromSeconds(_waitTimeoutSeconds).TotalMilliseconds`.

**Issue:** `_waitTimeoutSeconds` is the SEA query-wait (CONTINUE) timeout, not a connection timeout. The Thrift path passes `ConnectTimeoutMilliseconds` which is a different concept. Dashboards filtering on `socket_timeout` will see SEA records mislabeled.

**Fix:** Read from the connection-timeout connection-string property (or a sensible default), not from `_waitTimeoutSeconds`. ~15 minutes.

---

### D2 — `PendingTelemetryContext` leaks observer internals back to statement

**Location:** `csharp/src/DatabricksStatement.cs:112` — getter does `(_observer as TelemetryObserver)?.Context`.

**Issue:** The statement still reaches into the observer's internal `StatementTelemetryContext` from the reader-finalize block at lines 425-431 to override `IsCompressed` / `ResultFormat`. Comment notes this preserves byte-identical output for PECO-2988/2978.

**Fix (optional cleanup):** Formalize as an observer method (e.g. `OnReaderInspected(format, compressed)`) and remove the downcast. Not blocking; document or fix in a follow-up.

---

### D3 — Design §6 row "Session open" signature drift

**Location:** `StatementExecutionConnection.cs:517-522` calls `EmitOperationTelemetry(CreateSession, …)` directly.

**Issue:** Design row says `_telemetry.EmitCreateSessionTelemetry(activity)`. Same observable effect; just a method-name mismatch with the spec.

**Fix:** None required. Update the design row to match the actual call, or leave as-is.

---

### D4 — Sprint plan file paths stale

**Issue:** Sprint plan references `csharp/src/Drivers/Databricks/...` but the actual repo layout dropped that prefix — files live under `csharp/src/Telemetry/` and `csharp/src/StatementExecution/`.

**Fix:** Update the sprint plan paths in a small docs commit. Not blocking.

---

## 6. Action plan (ordered)

| Step | Action | Effort | Blocks |
|---|---|---|---|
| 1 | **G1**: Add `_observer.OnFinalized()` in `StatementExecutionStatement.Dispose` (line 887). Test exactly-once after error+dispose. | 30 min | All SEA telemetry emission |
| 2 | **G2**: Create `SeaResultFormatMapper` per design §8; update `OnExecuteSucceeded` callsite (line 391) to use it. | ½ day | `result_format` field correctness |
| 3 | **G3a**: Wire `OnFirstBatchReady` at reader construction in both paths (cloud-fetch + inline). | ½ day | `result_set_ready_latency_millis` |
| 4 | **G3b**: Wire `OnConsumed` at reader Dispose; thread observer into reader classes as needed. | ½ day | `result_set_consumption_latency_millis` |
| 5 | **G3c**: Wire `OnChunksDownloaded` for cloud-fetch path. If gap-fix `ChunkMetrics` aggregator not yet exposed, pass `ChunkMetrics.Empty` and note as follow-up. | ½ day | `chunk_details` |
| 6 | **D1**: Fix `connectTimeoutMilliseconds` source. | 15 min | `socket_timeout` field correctness |
| 7 | End-to-end smoke against real SQL warehouse: run a SELECT via REST, confirm `DRIVER_MODE_SEA` record in `eng_lumberjack.prod_frontend_log_sql_driver_log`. | ~1 hour | Sprint demo |
| 8 | T7 integration tests per design §15. | 2 days | Definition-of-done |

**Critical path:** Steps 1 → 2 → 7 (about 1 day) gets a SEA record on the wire end-to-end. Steps 3–6 round out field coverage to full Thrift parity.

---

## 7. Risks introduced or remaining

- **Gap-fix `ChunkMetrics` plumbing dependency** — still applies. If not landed by the time G3c is reached, ship with `ChunkMetrics.Empty`. Backfill in a follow-up sprint. Proto fields are nullable.
- **OnFinalized exactly-once semantics** — interface guarantees idempotence via the `_emitted` CAS in `TelemetryObserver`. When wiring, both error path and dispose path should be allowed to call it; only the first wins. Already covered by `TelemetryObserverTests.OnFinalized_CalledTwice_EnqueuesOnce`.
- **Reader threading** — observer must be passed into `InlineArrowStreamReader` and `CloudFetchReader` for the reader-Dispose hookpoints. Watch for constructor signature changes affecting Thrift call sites.
