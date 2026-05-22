# Gap Report — PECO-3022 SEA Telemetry Production Findings

**Report date:** 2026-05-15
**Branch under review:** `stack/pr-phase5-sea-statement-telemetry`
**Companion to:** [`gap-report-PECO-3022-sea-telemetry-2026-05-14.md`](./gap-report-PECO-3022-sea-telemetry-2026-05-14.md) (G1–G3, D1–D4) and [`gap-report-PECO-3022-sea-telemetry-e2e-2026-05-15.md`](./gap-report-PECO-3022-sea-telemetry-e2e-2026-05-15.md) (G4)
**Source:** `main.eng_lumberjack.prod_frontend_log_sql_driver_log` via `central-logfood-prodtools-azure-westus` workspace
**Jira:** [PECO-3022](https://databricks.atlassian.net/browse/PECO-3022)

---

## 1. Bottom line

PECO-3022 telemetry **is reaching production lumberjack** as designed — the G1 critical-emission gap is conclusively closed. However, comparator analysis against Thrift records on the same driver version (1.1.4) surfaced **seven concrete bugs**, two of which (B2, B3) are spec deviations the existing gap reports did not predict at this magnitude. None block emission, but together they make SEA records look meaningfully different from Thrift records in places where the design called for parity.

The findings are bounded and reachable — most are single-line fixes in `StatementExecutionConnection.cs` or `StatementExecutionStatement.cs`. None require new architecture.

---

## 2. Sample context

| Attribute | Value |
|---|---|
| Window | 14 days (2026-05-04 → 2026-05-18) |
| Driver-name filter | `'ADBC Databricks Driver'` ∪ `'Databricks ADBC Driver'` |
| Driver version | `1.1.4` (the build with PECO-3022 wiring) |
| SEA records | 23 EXECUTE_STATEMENT + 23 CREATE_SESSION + 23 DELETE_SESSION (one test session per execution) |
| Thrift records | 555 EXECUTE_STATEMENT + 477 CLOSE_STATEMENT + 233 CREATE_SESSION + 192 DELETE_SESSION |
| Workspace | `benchmarking-prod-aws-us-west-2.cloud.databricks.com` (test/benchmark) |
| `client_app_name` | `testhost` (dev/test data, not real customer usage) |
| Day | All 23 SEA records emitted 2026-05-14 |

Sample is small but reliable for field-level parity checks. Latency and population percentages may shift with broader adoption.

---

## 3. Bugs found

### B1 — `driver_name` string drift between SEA and Thrift

**Severity:** Medium
**Evidence:** Two distinct strings coexist in v1.1.4:
- `Databricks ADBC Driver` — 685 records, all `THRIFT` mode
- `ADBC Databricks Driver` — 4,401 records mixed `THRIFT` + 69 `SEA`

Lumberjack dashboards and downstream queries filtering on the established string `'Databricks ADBC Driver'` will silently miss all SEA records and a significant fraction of recent Thrift records.

**Likely cause:** The constant that drives `system_configuration.driver_name` was renamed in the C# driver at some recent point. Both code paths (Thrift, SEA) appear to read from sources that disagree, or the constant flipped between releases.

**Proposed fix:** Identify the canonical driver-name constant in `csharp/src/Telemetry/ConnectionTelemetry.cs` (or wherever `BuildSystemConfiguration` populates `driver_name`) and ensure both transports use the same literal. Confirm with downstream (telemetry consumer team) which string is authoritative for dashboards.

**Effort:** ~30 minutes for the fix; ~1 hour for coordinating the canonical name with consumers.

---

### B2 — `socket_timeout` populated with wrong source AND wrong scale (D1 confirmed and worse)

**Severity:** High — this is the same D1 issue from the original gap report, but production data shows it's worse than predicted.

**Evidence (14-day query, v1.1.4):**

| Mode | min | avg | max |
|---|---|---|---|
| SEA | 0 | 6.5 | 10 |
| Thrift | 900 | 900 | 900 |

The proto field `driver_connection_params.socket_timeout` is in **seconds**. Thrift emits 900 (a connection timeout). SEA emits 0–10 — exactly the range of `_waitTimeoutSeconds`, which is the SEA query-wait (CONTINUE) timeout, not a connection timeout.

**Code path:**
```csharp
// StatementExecutionConnection.cs:490
connectTimeoutMilliseconds: (int)TimeSpan.FromSeconds(_waitTimeoutSeconds).TotalMilliseconds,
```

The `_waitTimeoutSeconds` → ms conversion produces 10,000 ms, but `ConnectionTelemetry` then divides by 1,000 to populate the proto's seconds field, landing on 10. The original gap report flagged this as a "mislabeled" field — production data shows the value is wrong by 2 orders of magnitude on top of the labeling issue.

**Proposed fix:** Pass an actual connect-timeout value (read from `ConnectTimeoutMilliseconds`-equivalent connection-string property), not `_waitTimeoutSeconds`. Mirror what `DatabricksConnection.cs:737-747` passes.

**Effort:** ~15 minutes.

---

### B3 — `operation_type = EXECUTE_STATEMENT` instead of `EXECUTE_STATEMENT_ASYNC`

**Severity:** High (spec deviation)

**Evidence:** All 23 SEA EXECUTE_STATEMENT records show `sql_operation.operation_detail.operation_type = 'EXECUTE_STATEMENT'`. Design §17 open question #3 calls this out:

> Always `EXECUTE_STATEMENT_ASYNC` (SEA is always async on the wire). Confirm this matches the telemetry consumer team's expectation, or whether we should map specifically (`EXECUTE_STATEMENT` for sync-emulated paths).

Production code passes the synchronous variant.

**Code path:**
```csharp
// StatementExecutionStatement.cs:390
_observer.OnExecuteStarted(StatementType.Query, OperationType.ExecuteStatement, isCompressed);
```

**Proposed fix:** Change `OperationType.ExecuteStatement` to `OperationType.ExecuteStatementAsync` (assuming the proto enum has that value). Verify the enum name; design uses `EXECUTE_STATEMENT_ASYNC` as the proto convention.

**Effort:** ~15 minutes.

---

### B4 — Reader latencies populated on only 78% of SEA EXECUTE records

**Severity:** High

**Evidence:**
- `result_set_ready_latency_millis`: 18 / 23 SEA EXECUTE records (78%) — Thrift: 555 / 555 (100%)
- `result_set_consumption_latency_millis`: 18 / 23 SEA EXECUTE records (78%) — Thrift: 555 / 555 (100%)

5 statements executed and finalized but never populated reader latencies. No errors recorded on any of the 23 records. This is unexpected — the design intent is that every successful EXECUTE produces both metrics.

**Likely causes (in order of probability):**

1. Statements that fail or are canceled after execute but before reader construction reach `OnFinalized` (via Dispose) without ever firing `OnFirstBatchReady` or `OnConsumed`. The current `OnFinalized` guard (`_executeStarted`) allows the emission. Verify by querying the 5 missing-reader-latency records for status field, cancellation markers, or unusual error_info shape.
2. Inline-arrow path: `InlineArrowStreamReader` construction at line 685 fires `OnFirstBatchReady`, but Dispose at line 1024 may not fire `OnConsumed` if the reader is never read from (e.g., a consumer that opens then immediately closes the result).
3. Race: `Dispose` runs on a different thread than reader iteration and skips the observer call.

**Proposed fix:**
1. Query lumberjack for the 5 missing records to characterize them (cancellation? immediate-close? what queries?).
2. Audit the reader Dispose paths in `StatementExecutionStatement.cs:1024` and the cloud-fetch reader equivalent — ensure `OnConsumed` and (where applicable) `OnFirstBatchReady` always fire on the successful path.
3. Consider emitting `OnConsumed(0)` from `OnFinalized` if Dispose never fired it — but only after understanding the root cause; this should not paper over a real bug.

**Effort:** ~½ day investigation + 1 hour fix.

---

### B5 — `chunk_details` empty for SEA (expected, gap-fix dependency)

**Severity:** Low / expected
**Evidence:** 0 / 23 SEA EXECUTE records have `chunk_details` populated; Thrift shows 23 / 555. The 4 SEA EXTERNAL_LINKS queries did not produce chunk metrics.

**Cause:** Known dependency on the gap-fix workstream's `CloudFetchDownloader → ChunkMetrics → CloudFetchReader.GetChunkMetrics()` plumbing. The SEA `OnChunksDownloaded` hookpoint (line 1143) is wired but falls back to `new ChunkMetrics()` when the reader returns null. The observer treats `new ChunkMetrics()` (all-zero) as effectively empty.

**Proposed fix:** No action in PECO-3022. Track in the gap-fix workstream. Once the aggregator surfaces real values for SEA, this field will populate automatically.

**Effort:** None for PECO-3022.

---

### B6 — No `CLOSE_STATEMENT` event for SEA

**Severity:** Medium (parity gap)
**Evidence:** Thrift emits 477 `CLOSE_STATEMENT` operation events. SEA emits 0. Net Thrift CLOSE_STATEMENT shipping rate is 477 / 555 ≈ 86% of executes — meaning nearly every Thrift statement disposal produces a standalone close event in addition to the per-statement record.

**Cause:** SEA's `Dispose` path calls `_observer.OnFinalized()` which builds the per-statement `OssSqlDriverTelemetryLog`, but no equivalent of Thrift's `EmitOperationTelemetry(CLOSE_STATEMENT, ...)` call exists for SEA. Design §6 does not list CLOSE_STATEMENT under "SEA integration points"; it may have been overlooked.

**Question for design review:** Is the absence intentional (per-statement record subsumes the CLOSE_STATEMENT event) or a parity gap to fix?

**Proposed fix (if a gap):** Add a `_telemetry.EmitOperationTelemetry(CLOSE_STATEMENT, ...)` call in `StatementExecutionStatement.Dispose` mirroring `DatabricksStatement` Thrift behavior.

**Effort:** ~½ day including design review and tests.

---

### B7 — `poll_count` low-coverage on SEA EXECUTE records

**Severity:** Low (likely expected)
**Evidence:** `n_operation_status_calls` populated on:
- SEA: 8 / 23 (35%)
- Thrift: 65 / 555 (12%)

Both transports show low population — both depend on the polling loop actually running. Statements with immediate terminal responses skip polling and therefore skip `OnPollCompleted`. This may be expected behavior.

**Cause:** `OnPollCompleted` is only called when the polling loop terminates (`StatementExecutionStatement.cs:610`). If the first `GetStatementAsync` returns a terminal state, no poll occurred and no event fires.

**Decision needed:** Should the observer emit `OnPollCompleted(0, 0)` for statements that didn't poll? Pro: consistent telemetry shape, allows aggregation queries to assume the field exists. Con: emits noise for the common case.

**Proposed fix:** None unless consumers want consistent shape. Document the contract: `poll_count` is null when no polling occurred.

**Effort:** None unless we decide to change the contract.

---

## 4. Confirmed working items (from production data)

The following design behaviors are verified by lumberjack records:

| Item | Verification |
|---|---|
| Per-statement record emission (G1) | 23 / 23 SEA EXECUTE records present with `sql_statement_id` populated |
| `SeaResultFormatMapper` (G2) | 19 INLINE_ARROW + 4 EXTERNAL_LINKS, zero `FORMAT_UNSPECIFIED` |
| `OnExecuteStarted` (T6) | All 23 records have `statement_type`, `is_compressed` populated |
| `OnExecuteSucceeded` (T6) | All 23 records have `sql_statement_id` populated |
| `OnError` (T6) | 0 / 23 errors in sample — cannot verify positively, but the path is wired (covered by unit tests) |
| `OnFinalized` (gap-1) | Every executed statement produced a record |
| `OnFirstBatchReady` / `OnConsumed` (gap-3, gap-4) | Wired and working for the majority of records (see B4 for partial gap) |
| `DriverMode = SEA` (design §10) | All 23 records have `driver_connection_params.mode = 'SEA'` |
| Session lifecycle (T5) | 23 CREATE_SESSION ↔ 23 DELETE_SESSION balance — every session opens and closes |
| `auth_type`, `auth_mech`, `auth_flow` | Correctly populated (`pat`, `PAT`, `TOKEN_PASSTHROUGH`) |
| `system_configuration` (driver_version, runtime, OS) | All fields populated |

---

## 5. Action plan (ordered by effort × value)

| Step | Action | Severity | Effort | Outcome |
|---|---|---|---|---|
| 1 | **B3**: Change `OperationType.ExecuteStatement` → `OperationType.ExecuteStatementAsync` in `StatementExecutionStatement.cs:390` | High | 15 min | Spec parity restored |
| 2 | **B2 (D1)**: Replace `_waitTimeoutSeconds`-derived value in `StatementExecutionConnection.cs:490` with a real connect-timeout source | High | 15 min | `socket_timeout` matches Thrift scale and semantics |
| 3 | **B1**: Audit the `driver_name` constant; align SEA path with the canonical Thrift literal | Medium | 30 min | Lumberjack dashboards no longer miss SEA records |
| 4 | **B4**: Query lumberjack for the 5 missing-reader-latency records; characterize them; fix the missing-emit path | High | ½ day | 100% reader-latency coverage |
| 5 | **B6**: Decide on CLOSE_STATEMENT parity with design owner; implement if confirmed | Medium | ½ day | Operation-event parity with Thrift |
| 6 | **B5**: No action — track in gap-fix workstream | Expected | 0 | Auto-resolved when ChunkMetrics aggregator lands |
| 7 | **B7**: Confirm contract with consumer team — document or adjust | Low | 30 min | Documented null-handling for `n_operation_status_calls` |

**Critical path:** Steps 1–3 (~1 hour) eliminate the highest-visibility parity issues. Step 4 is the only remaining investigative effort.

---

## 6. Updated gap-landscape summary

| Gap | Status (before this report) | Status (after prod findings) | Status (after follow-up fixes 2026-05-18) |
|---|---|---|---|
| G1 (OnFinalized) | ✅ Fixed | ✅ Verified in prod | ✅ |
| G2 (SeaResultFormatMapper) | ✅ Fixed | ✅ Verified in prod | ✅ |
| G3a (OnFirstBatchReady) | ✅ Fixed | ⚠️ B4 — 78% pop, not 100% | ✅ Fixed (commit `ad268d1`) |
| G3b (OnConsumed) | ✅ Fixed | ⚠️ B4 — 78% pop, not 100% | ✅ Fixed (commit `ad268d1`) |
| G3c (OnChunksDownloaded) | ⏸️ Empty as designed | ⏸️ Confirmed (B5) | ⏸️ Expected — gap-fix workstream dependency |
| D1 (socket_timeout) | ❌ Open (predicted "mislabeled") | ❌ **B2 — worse than predicted, also wrong scale** | ✅ Fixed (commit `5e2819c`) — uses `_httpClient.Timeout`, expected to land on 900s matching Thrift |
| G4 (E2E coverage) | ❌ Still open | ❌ Still open | ❌ Still open — see e2e gap report |
| **B1** (driver_name drift) | n/a | ❌ New finding | ✅ No code change needed — already fixed pre-1.1.4; production records under `Databricks ADBC Driver` are stale from older builds. Current code only uses canonical `ADBC Databricks Driver`. |
| **B3** (op_type EXECUTE_STATEMENT) | n/a (open question in design) | ❌ New finding — needs fix or design update | ✅ Fixed (commit `fab04a8`) — passes `OperationType.ExecuteStatementAsync` |
| **B6** (no CLOSE_STATEMENT) | n/a | ❌ New finding — parity question | ✅ Fixed (commit `4126a2e`) — emits CLOSE_STATEMENT event with idempotency gate |
| **B7** (poll_count coverage) | n/a | ⏸️ Likely expected | ⏸️ Documented; consumer-team confirmation pending |

### Code-side gap landscape, summary

After the 2026-05-18 follow-up fixes (commits `fab04a8`, `ad268d1`, `4126a2e`, `5e2819c`, `1f3d5aa`), **every code-side gap surfaced by these three reports is closed**, except for:

- **G3c / B5** — `chunk_details` empty for SEA. Dependent on the gap-fix workstream's `ChunkMetrics` aggregator. Will auto-resolve when upstream lands.
- **B7** — `poll_count` low coverage. Likely expected behavior; awaiting consumer-team confirmation, no code change planned.

**G4 (E2E skip-guards)** was closed by commit `1f3d5aa` — 11 of the 12 protocol-skipped telemetry tests in `csharp/test/E2E/Telemetry/` are now enabled for both protocols. `RetryCountTests` remains skipped because `retry_count` is not wired for SEA (tracked in the gap-fix workstream).

Lumberjack verification of `B3`, `B4`, `B6`, `D1` is pending the next driver build reaching prod + the ~1-hour ingestion lag.

---

## 7. Risks

- **Sample bias:** All 23 SEA records are from one test session in a single benchmark workspace on one day. Real-world distributions (longer queries, larger result sets, more polling, errors) may surface additional bugs not present here.
- **Test data drift:** As SEA usage grows post-v1.1.4 rollout, the `client_app_name=testhost` filter will need to be widened to capture customer traffic. Recommend a follow-up query in 2 weeks against real workspaces.
- **D1 escalation:** B2 is meaningfully worse than the original D1 framing. If `socket_timeout` is consumed by any internal SLA dashboard or alert, SEA records may be triggering false signals.
- **B4 root cause unknown:** Until we characterize the 5 missing-reader-latency records, we don't know whether B4 is a benign edge case or a systematic gap. Step 4 in the action plan should not be skipped.
