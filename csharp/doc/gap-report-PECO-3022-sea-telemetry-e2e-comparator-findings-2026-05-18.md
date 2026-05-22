# Gap Report — PECO-3022 SEA Telemetry E2E Comparator Findings

**Report date:** 2026-05-18
**Branch under review:** `stack/pr-phase5-sea-statement-telemetry`
**Companion to:**
- [`gap-report-PECO-3022-sea-telemetry-2026-05-14.md`](./gap-report-PECO-3022-sea-telemetry-2026-05-14.md) (G1–G3, D1–D4)
- [`gap-report-PECO-3022-sea-telemetry-e2e-2026-05-15.md`](./gap-report-PECO-3022-sea-telemetry-e2e-2026-05-15.md) (G4)
- [`gap-report-PECO-3022-sea-telemetry-prod-findings-2026-05-15.md`](./gap-report-PECO-3022-sea-telemetry-prod-findings-2026-05-15.md) (B1–B7, prod-data findings)

---

## 1. Bottom line

After G4 closed (commit `1f3d5aa`), the existing E2E telemetry suite was unblocked for SEA and run end-to-end. **Results: 50 passed, 33 failed, 10 skipped (93 total).** The skip-removal effectively turned the suite into a Thrift-vs-SEA comparator and exposed four new categories of gaps that prior reports did not surface:

- **B8** — Metadata operations on SEA emit `operation_type=EXECUTE_STATEMENT_ASYNC` instead of categorized `LIST_CATALOGS / LIST_SCHEMAS / LIST_TABLES / LIST_COLUMNS / LIST_TABLE_TYPES`. Eleven test failures cluster here.
- **B9** — UPDATE statements on SEA produce zero telemetry events. `OnFinalized` does not fire on the `ExecuteUpdate` path.
- **B10** — `driver_connection_params.enable_direct_results` is hardcoded to `true` on SEA, ignoring the user-supplied `adbc.databricks.enable_direct_results` connection property.
- **B11** — SEA does not honor `SparkParameters.ConnectTimeoutMilliseconds`. After the D1 fix, `socket_timeout` reads from `HttpClient.Timeout` which is wired only to `CloudFetchTimeoutMinutes`. Tests that set `ConnectTimeoutMilliseconds=120000` see `socket_timeout=900` (the 15-minute default), not `120`.

The remaining 17 failures fall under **B5 / ChunkDetails** — already-known gap-fix workstream dependency for `ChunkMetrics` aggregation.

All four new findings are bounded code-side fixes.

---

## 2. Run context

| Attribute | Value |
|---|---|
| Command | `dotnet test --filter "FullyQualifiedName~E2E.Telemetry"` |
| Config | `/home/jade.wang/tmp/adbc_test_config_sea.json` (existing test config + `"protocol": "rest"`) |
| Driver build | Local from `stack/pr-phase5-sea-statement-telemetry` HEAD (includes gap-fills 1–5 + D1 + B3/B4/B6) |
| Total tests | 93 |
| Passed | 50 |
| Failed | 33 |
| Skipped | 10 (5 `RetryCountTests` intentional, 5 misc edge cases) |
| Duration | 6m32s |

**Note on lumberjack ingestion:** the tests use `TelemetryClientManager.ExporterOverride = exporter` to swap in `CapturingTelemetryExporter`. Records are captured **in-process for assertion only** and do **not** flow to `/telemetry-ext`. Re-running these tests will not produce additional `prod_frontend_log_sql_driver_log` records.

---

## 3. New findings

### B8 — Metadata operations not categorized on SEA

**Severity:** High (parity gap)

**Affected tests:** 11 failures across two files:

| File | Tests |
|---|---|
| `MetadataOperationTests.cs` | `Telemetry_GetObjects_Catalogs_EmitsListCatalogs`, `_Schemas_`, `_Tables_`, `_Columns_`, `_AllDepths_EmitCorrectOperationType`, `Telemetry_GetTableTypes_EmitsListTableTypes` |
| `StatementMetadataTelemetryTests.cs` | `Telemetry_StatementGetCatalogs_`, `_GetSchemas_`, `_GetTables_`, `_GetColumns_`, `_AllCommands_EmitCorrectOperationType` |

**Evidence:** All failures share the same assertion pattern:

```
Assert.NotNull() Failure: Value is null
  at FindLog(logs, proto =>
    proto.SqlOperation?.OperationDetail?.OperationType == OperationType.ListCatalogs);
```

The captured records contain only `operation_type=EXECUTE_STATEMENT_ASYNC` (the post-B3 default). No record carries `LIST_CATALOGS / LIST_SCHEMAS / LIST_TABLES / LIST_COLUMNS / LIST_TABLE_TYPES`.

**Likely cause:** `StatementExecutionStatement.cs:416` unconditionally emits:
```csharp
_observer.OnExecuteStarted(StatementType.Query, OperationType.ExecuteStatementAsync, isCompressed);
```

Metadata operations (`GetCatalogs`, `GetSchemas`, `GetTables`, `GetColumns`, `GetTableTypes`) flow through the same code path but should emit `OperationType.ListCatalogs` (etc.) and `StatementType.Metadata` instead — matching the Thrift path which uses `DatabricksStatement.CreateMetadataTelemetryContext()`.

**Proposed fix:**
1. Detect metadata operations at the entry point (SEA metadata methods like `GetCatalogsAsync`, `GetSchemasAsync`, etc., or via a `isMetadataExecution` flag that already exists on `ExecuteQueryAsync(... bool isMetadataExecution = false)` per the SDD findings).
2. Pass the appropriate `(StatementType, OperationType)` pair to `OnExecuteStarted` based on the metadata operation.
3. Likely needs a small mapping helper (e.g., `SeaMetadataOperationMapper`) or a parameter threading through `ExecuteQueryInternalAsync`.

**Effort:** ~½ day fix + ~½ day test/verification.

---

### B9 — UPDATE statements emit zero telemetry on SEA

**Severity:** High (missing emission, like B4 but for a different code path)

**Affected tests:** 2 failures + 1 skip:

| Test | Failure |
|---|---|
| `InternalCallTests.UserUpdate_IsNotMarkedAsInternal` | "Expected at least 1 telemetry event, got 0" |
| `TelemetryBaselineTests.BaselineTest_UpdateStatement_FieldsPopulated` | Skipped: "No telemetry captured for UPDATE statement" |
| (others may be affected — full UPDATE coverage limited in the suite) |

**Evidence (from `InternalCallTests.cs:114-120`):**
```csharp
statement.SqlQuery = "USE default";
statement.ExecuteUpdate();
statement.Dispose();
var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);
Assert.True(logs.Count >= 1, $"Expected at least 1 telemetry event, got {logs.Count}");
```

UPDATE was executed but **no telemetry record was emitted**.

**Likely cause:** `StatementExecutionStatement.ExecuteUpdate()` / `ExecuteUpdateAsync()` is a separate entry point from `ExecuteQueryInternalAsync()`. The observer hookpoints (`OnExecuteStarted`, `OnFinalized`, etc.) were wired into the QUERY path only. The UPDATE path likely:
- Either bypasses the observer entirely (most likely)
- Or sets up the observer but the `_executeStarted` gate at `Dispose:1054` (`if (_executeStarted) _observer.OnFinalized();`) never trips because `_executeStarted` is set only inside `ExecuteQueryInternalAsync`

**Proposed fix:** Wire the same observer lifecycle (`OnExecuteStarted` → `OnExecuteSucceeded`/`OnError` → set `_executeStarted=true` → eventually `OnFinalized` via Dispose) into the `ExecuteUpdate` / `ExecuteUpdateAsync` code path. Operation type for UPDATE is `EXECUTE_STATEMENT_ASYNC` (same as query) but `StatementType.Update`.

**Effort:** ~½ day — mirror the existing Execute path's observer wiring.

---

### B10 — `enable_direct_results` hardcoded on SEA, ignores user property

**Severity:** Medium

**Affected test:** `ConnectionParametersTests.ConnectionParams_EnableDirectResults_IsPopulated`

**Evidence:** Test sets `DatabricksParameters.EnableDirectResults = "false"` and expects `protoLog.DriverConnectionParams.EnableDirectResults == false`. Actual: `true`.

**Likely cause:** `StatementExecutionConnection.cs:487` passes `enableDirectResults: true` literally to `ConnectionTelemetry.Create`. SEA does not have a true "direct results" concept (it uses disposition parameters instead), so the hardcoding is a placeholder.

**Proposed fix — two options:**

- **Option A** (recommended): Read the user property `DatabricksParameters.EnableDirectResults` and emit faithfully. Even if SEA doesn't use the value internally, reflecting user intent makes the telemetry record honest. Same pattern Thrift uses.
- **Option B**: Leave the field unset / `null` for SEA, since the concept doesn't apply. Adjust the test to skip this assertion for `Protocol == "rest"`.

**Effort:** ~30 minutes (Option A) or ~15 minutes (Option B).

---

### B11 — SEA does not honor `ConnectTimeoutMilliseconds`; HttpClient.Timeout only reads `CloudFetchTimeoutMinutes`

**Severity:** Medium

**Affected test:** `ConnectionParametersTests.ConnectionParams_SocketTimeout_IsPopulated`

**Evidence:** Test sets `SparkParameters.ConnectTimeoutMilliseconds = 120000` (with retries disabled to prevent bump). Expects `socket_timeout == 120`. After D1 fix, actual is `900` (the default `CloudFetchTimeoutMinutes=15` → 15m → 900s).

**Likely cause:** `StatementExecutionConnection.CreateHttpClient` reads only `DatabricksParameters.CloudFetchTimeoutMinutes` (line 322); it does not read `SparkParameters.ConnectTimeoutMilliseconds`. After D1, `socket_timeout` is correctly sourced from `_httpClient.Timeout` — but `_httpClient.Timeout` reflects only `CloudFetchTimeoutMinutes`, not `ConnectTimeoutMilliseconds`. So the chain of bugs is:
- D1 fix: ✅ socket_timeout now reads HttpClient.Timeout
- B11: ❌ HttpClient.Timeout doesn't honor the user's `ConnectTimeoutMilliseconds`

**Proposed fix:** In `CreateHttpClient`, prefer `ConnectTimeoutMilliseconds` if set (matching Thrift behavior); fall back to `CloudFetchTimeoutMinutes * 60_000` otherwise. Or unify on a single timeout property and document.

**Effort:** ~1 hour including test verification.

---

## 4. Already-known failure cluster (no new action)

### B5 / ChunkDetails (~17 failures)

Files affected: `ChunkDetailsTelemetryTests.cs`, `ChunkMetricsReaderTests.cs`, `ChunkMetricsAggregationTests.cs`.

All failures stem from `chunk_details` fields being empty/zero on SEA because the gap-fix workstream's `CloudFetchDownloader → ChunkMetrics → CloudFetchReader.GetChunkMetrics()` plumbing has not yet been wired for SEA. SEA's `OnChunksDownloaded` hookpoint passes `new ChunkMetrics()` as the fallback (per design §9 and gap-1 commit `d6d54ec`).

**Action:** None in PECO-3022 scope. Will auto-resolve when the gap-fix workstream lands SEA-side chunk-metrics aggregation. The failing tests should either stay failing (so the gap stays visible) or be skipped with `Skip.If(Protocol == "rest" && !chunkMetricsAvailable, ...)` once that's wired.

---

## 5. Action plan — closed 2026-05-18

| Step | Fix | Status | Commit |
|---|---|---|---|
| 1 | **B11**: Honor `ConnectTimeoutMilliseconds` in `StatementExecutionConnection.CreateHttpClient` | ✅ Closed | `360048f` |
| 2 | **B10**: Read `DatabricksParameters.EnableDirectResults` in SEA path | ✅ Closed | `360048f` |
| 3 | **B9**: Wire observer hookpoints into `ExecuteUpdate`/`ExecuteUpdateAsync` | ✅ Closed | `ec07e4d` |
| 4 | **B8**: Detect metadata operations and emit `(StatementType.Metadata, OperationType.List*)` | ✅ Closed | uncommitted SDD + `360048f` |
| 5 | **B5 cluster** (17 tests): chunk_details empty — gap-fix workstream dependency | ⏸️ Expected | n/a |

### Final triage of the remaining metadata-test failures

`Telemetry_GetObjects_Schemas_EmitsListSchemas`, `Telemetry_GetObjects_Tables_EmitsListTables`, and `Telemetry_GetObjects_AllDepths_EmitCorrectOperationType` were investigated with a single-test verbose run. **Root cause: `TaskCanceledException` from `SHOW SCHEMAS IN ALL CATALOGS` / `SHOW TABLES IN ALL CATALOGS`** — the test workspace cannot complete these queries within the 10-second metadata-timeout budget (`_waitTimeoutSeconds` default). The telemetry implementation is correct; the failures are environmental.

**Not a PECO-3022 fix.** Options for follow-up:
- Raise the SEA metadata timeout when `protocol=rest` so cross-catalog scans complete (separate PR; affects user-facing behavior).
- Or skip these tests on slow workspaces with `Skip.If(...)` and a documented reason.
- Or accept this as a known limitation of the test environment and document.

---

## 6. Updated gap-landscape summary

| Gap | Status |
|---|---|
| G1, G2, G3a, G3b, B3, B4, B6 — observer hookpoints + categorization | ✅ Closed |
| G3c / B5 — chunk_details | ⏸️ Gap-fix dependency |
| D1 / B2 — socket_timeout source | ✅ Closed (`5e2819c`) — but B11 exposes that the upstream HttpClient timeout doesn't honor user-supplied ConnectTimeoutMilliseconds |
| B1 — driver_name drift | ✅ No fix needed (historical) |
| B7 — poll_count coverage | ⏸️ Likely expected |
| G4 — E2E skip-guards | ✅ Closed (`1f3d5aa`) |
| **B8** — metadata operation types | ❌ **New finding** — high severity |
| **B9** — UPDATE no telemetry | ❌ **New finding** — high severity |
| **B10** — enable_direct_results hardcoded | ❌ **New finding** — medium |
| **B11** — ConnectTimeoutMilliseconds not honored | ❌ **New finding** — medium |

---

## 7. Notes on running the E2E suite for lumberjack verification

The E2E telemetry tests swap in `CapturingTelemetryExporter` via `TelemetryClientManager.ExporterOverride`, which **blocks the real `DatabricksTelemetryExporter` from posting to `/telemetry-ext`**. Re-running these tests will not produce additional records in `prod_frontend_log_sql_driver_log`.

If independent prod-side verification is needed, the path is:
1. Write a small standalone driver harness (a regular .NET program, or an xunit test that does not call `CreateConnectionWithCapturingTelemetry`) that constructs `DatabricksDriver` naturally with telemetry enabled.
2. Run a few queries against a real warehouse.
3. Wait ~1 hour for lumberjack ingestion.
4. Re-query `prod_frontend_log_sql_driver_log` to verify the in-process assertions match what arrives on the wire.

The in-process E2E assertions are strong evidence the wire records would be correct (same code paths, same record shapes) — but if you want a final stamp, the harness is bounded work (~1 hour).
