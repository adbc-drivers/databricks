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

# Sprint Plan — PECO-3022 SEA Telemetry Integration

**Sprint window:** 2026-05-14 → 2026-05-28 (2 weeks)
**Implementer:** Jade Wang (sole)
**Design doc:** [`docs/designs/PECO-3022-sea-telemetry-integration-design.md`](../../docs/designs/PECO-3022-sea-telemetry-integration-design.md)
**Design PR:** https://github.com/adbc-drivers/databricks/pull/455
**Jira:** [PECO-3022](https://databricks.atlassian.net/browse/PECO-3022)

---

## Sprint Goal

Ship end-to-end SEA client telemetry to production parity with Thrift — connection session events, per-statement operation events, error events, chunk metrics — verified against a real SQL warehouse. Includes the mechanical refactor of `DatabricksStatement` to consume the new observer interface so both transports use the same telemetry seam.

### Success criteria

- A statement executed via `adbc.databricks.protocol=rest` emits a `OssSqlDriverTelemetryLog` carrying `driver_connection_params.mode = DRIVER_MODE_SEA`, populated session id, statement id, operation latency, result format, poll count, first-batch and consumed latencies.
- Error in a SEA statement produces an `error_info` record with `error_name` populated.
- Thrift telemetry output is byte-identical to current main (regression-tested).
- SEA telemetry visible in `eng_lumberjack.prod_frontend_log_sql_driver_log` after sprint deploys.

### Dependency

- The gap-fix workstream's `CloudFetchDownloader → ChunkMetrics → CloudFetchReader.GetChunkMetrics()` plumbing. If it lands in-sprint, wire it in. If not, ship with `ChunkMetrics.Empty` and backfill in a follow-up.

---

## Task Breakdown (7 tasks, ~11.5 person-days)

### T1 — Refactor `ConnectionTelemetry.Create` signature (1 day)

Replace `TSessionHandle? sessionHandle` with `string sessionId`. Add `DriverMode.Types.Type mode` parameter. Remove the hardcoded `DriverMode.Types.Type.Thrift` at `ConnectionTelemetry.cs:458` and `:642`.

**Files touched:**
- `csharp/src/Telemetry/ConnectionTelemetry.cs`
- `csharp/src/DatabricksConnection.cs` (single Thrift call site, convert `sessionHandle.SessionId.Guid.ToString()` at boundary)

**Acceptance criteria:**
- All existing telemetry unit tests pass unchanged.
- Thrift integration test emits `driver_connection_params.mode = DRIVER_MODE_THRIFT` (regression check).
- New unit test: `Create_AcceptsStringSessionId`, `Create_ThriftMode_SetsDriverModeThrift`, `Create_SeaMode_SetsDriverModeSea`.

**Risks:** Low. Mechanical refactor with one Thrift caller to update.

---

### T2 — Introduce `IStatementOperationObserver` + impls (2 days)

Create the interface and three implementations per design §5.1 and §12.

**New files:**
- `csharp/src/Telemetry/IStatementOperationObserver.cs`
- `csharp/src/Telemetry/TelemetryObserver.cs` (uses `Safe(Action)` helper pattern from design §12)
- `csharp/src/Telemetry/NullObserver.cs` (singleton)
- `csharp/src/Telemetry/SafeObserver.cs` (decorator)

**Acceptance criteria:**
- Interface contract documented: methods MUST NOT throw, thread-safe, `OnFinalized` is terminal and idempotent.
- `TelemetryObserver` writes into a `StatementTelemetryContext`; on `OnFinalized` builds `OssSqlDriverTelemetryLog` and enqueues via `IConnectionTelemetry`.
- Unit tests per design §15:
  - `NullObserver_AllMethods_AreNoOps` / `NullObserver_IsSingleton`
  - `TelemetryObserver_OnExecuteStarted_PopulatesContext`
  - `TelemetryObserver_OnExecuteSucceeded_RecordsStatementId`
  - `TelemetryObserver_OnFinalized_EnqueuesExactlyOnce`
  - `TelemetryObserver_OnFinalized_CalledTwice_EnqueuesOnce`
  - `TelemetryObserver_OnError_RecordsErrorAndFinalizes`
  - `TelemetryObserver_AllMethods_NeverThrow_WhenContextCorrupted`
  - `TelemetryObserver_OnChunksDownloaded_MergesIntoChunkDetails`
  - `SafeObserver_PropagatesNormalCallsToInner`
  - `SafeObserver_SwallowsExceptionsFromInner_LogsAtTrace`

**Risks:** Low. New code, no existing callers yet.

---

### T3 — Add `SeaResultFormatMapper` (1 day)

Static helper that maps wire `disposition` × manifest state → proto `ExecutionResult.Format`. Per design §8.

**New files:**
- `csharp/src/StatementExecution/SeaResultFormatMapper.cs`

**Acceptance criteria:**
- Unit tests covering all four cells in §8 table:
  - `Map_InlineDisposition_ReturnsInlineArrow`
  - `Map_ExternalLinksDisposition_ReturnsExternalLinks`
  - `Map_AutoDisposition_WithInlineResult_ReturnsInlineArrow`
  - `Map_AutoDisposition_WithExternalLinks_ReturnsExternalLinks`

**Risks:** Low. Isolated pure-function helper.

---

### T4 — Refactor `DatabricksStatement` to use observer (1 day)

Mechanical refactor: replace the private telemetry methods (`CreateTelemetryContext`, `CreateMetadataTelemetryContext`, `RecordSuccess`, `RecordError`, `EmitTelemetry`) with `_observer: IStatementOperationObserver` field calls. Behavior unchanged.

**Files touched:**
- `csharp/src/DatabricksStatement.cs`

**Acceptance criteria:**
- All existing Thrift telemetry unit tests pass unchanged.
- Manual diff check: byte-equivalent `OssSqlDriverTelemetryLog` for a known statement before/after the refactor.
- `((DatabricksConnection)Connection).TelemetrySession` cast eliminated; observer is injected at statement construction from `DatabricksConnection.CreateStatement()`.

**Risks:** Medium. The refactor is mechanical but the existing Thrift test suite is the safety net. Allocate buffer time for any subtle behavior differences (e.g. `PendingTelemetryContext` exposure used by external callers).

**Depends on:** T1 (Create signature), T2 (observer types).

---

### T5 — Wire telemetry into `StatementExecutionConnection` (1.5 days)

Mirror the Thrift pattern at `DatabricksConnection.cs:594-724`. Add `_telemetry: IConnectionTelemetry` field. Call `ConnectionTelemetry.Create(...)` in `OpenAsync` after `CreateSessionAsync` succeeds, emit `CREATE_SESSION` event, then on `Dispose` emit `DELETE_SESSION` and run `DisposeAsync` with 5-second timeout.

**Files touched:**
- `csharp/src/StatementExecution/StatementExecutionConnection.cs`

**Acceptance criteria:**
- `OpenAsync` succeeds even if telemetry initialization throws (telemetry is fail-open; falls back to `NullConnectionTelemetry`).
- `Dispose` completes within 5 seconds even if exporter is wedged.
- Observer is created in `CreateStatement()` using `_telemetry.Session`; falls back to `NullObserver.Instance` if telemetry is disabled or `Session` is null.
- Manual test: open + close a REST connection, verify `CREATE_SESSION` and `DELETE_SESSION` records arrive in lumberjack.

**Risks:** Medium. New telemetry surface on a class that has never had it. Watch for null-handling around `_telemetry` and `Session`.

**Depends on:** T1 (Create signature).

---

### T6 — Wire telemetry into `StatementExecutionStatement` (3 days)

The meatiest task. Add `_observer: IStatementOperationObserver` field (defaults to `NullObserver.Instance`, set by `StatementExecutionConnection.CreateStatement`). Call observer methods at all 7 hookpoints per design §6:

1. `OnExecuteStarted` — `ExecuteQueryInternalAsync` before `_client.ExecuteStatementAsync` (line 345)
2. `OnExecuteSucceeded` — after response received, using `SeaResultFormatMapper`
3. `OnPollCompleted` — in `PollUntilCompleteAsync` (line 453), accumulate count/ms across the loop, emit once on terminal state
4. `OnFirstBatchReady` — at `CreateCloudFetchReader` (line 542) and `InlineArrowStreamReader` construction (nested at line 900)
5. `OnConsumed` + `OnChunksDownloaded` — at reader Dispose
6. `OnError` — `ExecuteQueryInternalAsync` catch block
7. `OnFinalized` — `Dispose` (line 817)

**Files touched:**
- `csharp/src/StatementExecution/StatementExecutionStatement.cs`

**Acceptance criteria:**
- Manual test: execute a SELECT via REST, verify a telemetry record arrives with `statement_id`, `result_format`, `operation_latency_ms`, `poll_count`, `result_set_ready_latency_millis`, `result_set_consumption_latency_millis` populated.
- Manual test: execute a bad SQL via REST, verify `error_info.error_name` populated.
- `OnFinalized` exactly-once even when both error path and dispose path fire.
- `ChunkMetrics`: wire to `OnChunksDownloaded` if gap-fix plumbing is available, else pass `ChunkMetrics.Empty`.

**Risks:** Medium-high. Largest scope; touches Execute, Poll loop, both reader construction paths, Dispose, and error catch. Highest chance of edge-case regressions.

**Depends on:** T1, T2, T3.

---

### T7 — SEA integration tests against real SQL warehouse (2 days)

Mirror the Thrift integration test set per design §15.

**New files:**
- `csharp/test/E2E/Telemetry/SeaTelemetryIntegrationTests.cs` (or similar)

**Test cases:**
- `Sea_ExecuteQuery_EmitsTelemetryWithStatementId`
- `Sea_ExecuteQuery_WithSyntaxError_EmitsErrorTelemetry`
- `Sea_ExecuteQuery_CloudFetch_RecordsChunkMetrics` (skipped if gap-fix plumbing not present)
- `Sea_ExecuteQuery_InlineResults_RecordsInlineFormat`
- `Sea_OpenConnection_EmitsCreateSession`
- `Sea_CloseConnection_EmitsDeleteSessionAndFlushes`
- `Sea_TelemetryDisabledByFeatureFlag_EmitsZeroEvents`
- `Sea_TelemetryDisabledByProperty_EmitsZeroEvents`
- `Sea_TelemetryExporterFails_DoesNotAffectQueryExecution`
- `Sea_TelemetryRecord_HasDriverModeSea`
- `Sea_ConcurrentStatements_EachEmitsExactlyOneRecord`

**Acceptance criteria:**
- All tests pass against a dev/staging Databricks SQL warehouse.
- Test infrastructure verifies records via either a local capture exporter or by querying `eng_lumberjack.prod_frontend_log_sql_driver_log` after a settling delay.

**Risks:** Medium. Real-warehouse tests are slow and flaky; allocate time for retry/stabilization.

**Depends on:** T5, T6.

---

## Sequencing

```
Week 1 (Mon-Fri):  T1 → T2 → T3 → T4
                   (T2 and T3 parallelizable if context allows)

Week 2 (Mon-Fri):  T5 → T6 → T7
                   (T5 in parallel with start of T6 if discipline holds)
```

**Critical path:** T1 → T6 → T7 (≈6 days).
**Slack:** ~1.5 days for review iteration, unexpected edge cases, gap-fix integration if it lands.

---

## Definition of Done

- All 7 tasks merged to `main`.
- Design PR (#455) approved and merged.
- SEA telemetry records visible in `eng_lumberjack.prod_frontend_log_sql_driver_log` via the [client-telemetry-query](https://databricks.atlassian.net/) skill.
- Thrift telemetry regression test green.
- Sprint demo: show side-by-side Thrift vs SEA telemetry records for the same query.

---

## Risks and Mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| Gap-fix `ChunkMetrics` plumbing slips | Medium | Ship with `ChunkMetrics.Empty`; backfill in follow-up sprint |
| `DatabricksStatement` refactor (T4) hits subtle regression | Medium | Cross-transport byte-identical regression test in T1, dry-run in T4 |
| SEA integration tests flaky in CI | Medium | Tag as `[Trait("Category", "Integration")]`; run on-demand initially |
| Sprint overflow (11.5d est in 10d capacity) | High | T7 can slip to follow-up sprint if T5/T6 take longer than estimated; foundation is the priority |
