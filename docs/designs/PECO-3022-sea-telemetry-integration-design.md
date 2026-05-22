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

# PECO-3022 — SEA Telemetry Integration Design

**Status:** Draft
**Author:** Jade Wang
**Related:** [`fix-telemetry-gaps-design.md`](./fix-telemetry-gaps-design.md), [`csharp/doc/telemetry-design.md`](../../csharp/doc/telemetry-design.md)
**Jira:** [PECO-3022](https://databricks.atlassian.net/browse/PECO-3022)

---

## 1. Summary

Today the C# ADBC Databricks driver emits client telemetry only on the Thrift code path. When a connection is opened with `adbc.databricks.protocol=rest` (the Statement Execution API / SEA backend), **zero telemetry is produced** — no session events, no per-statement operation events, no error events, no chunk metrics. SEA traffic is invisible to the existing `eng_lumberjack` driver-telemetry pipeline.

This design closes that gap by introducing a small **observer interface** (`IStatementOperationObserver`) between the statement classes and the telemetry implementation, refactoring `ConnectionTelemetry.Create` to be protocol-agnostic, and wiring observer callbacks at the SEA hookpoints. All existing telemetry infrastructure (`TelemetryClient`, `TelemetryClientManager`, `CircuitBreakerTelemetryExporter`, `DatabricksTelemetryExporter`, `FeatureFlagCache`) is reused as-is.

---

## 2. Goals and Non-Goals

### Goals

- SEA path emits the same set of telemetry events as the Thrift path: `CREATE_SESSION`, per-statement operation event, `DELETE_SESSION`, error events.
- Telemetry records carry `driver_connection_params.mode = DRIVER_MODE_SEA`, enabling backend dashboards to distinguish transports.
- Statement classes have no direct dependency on telemetry types — they call observer methods in their own language ("OnExecuteStarted", "OnPollCompleted").
- Telemetry must never throw to the driver caller (fail-open contract enforced by the observer interface).
- No new connection-string options; no new feature flags.

### Non-Goals

- Fixing the other Thrift-side gaps catalogued in [`fix-telemetry-gaps-design.md`](./fix-telemetry-gaps-design.md) (workspace_id, auth_type, metadata-ops instrumentation, retry_count, etc.). These are tracked in the existing gap-fix workstream and will land separately.
- Changing the proto schema (`OssSqlDriverTelemetryLog`) or the transport endpoint.
- Adding OpenTelemetry tracing — the observer interface is designed to admit a future `TracingObserver` impl, but that is out of scope here.

---

## 3. Current State

| Aspect | Thrift path | SEA path |
|---|---|---|
| Connection class | `DatabricksConnection : SparkHttpConnection : … : TracingConnection` | `StatementExecutionConnection : TracingConnection` |
| Statement class | `DatabricksStatement : SparkStatement : HiveServer2Statement : TracingStatement` | `StatementExecutionStatement : TracingStatement` |
| Connection telemetry field | `_telemetry : IConnectionTelemetry` (created in `InitializeTelemetry`, line 724) | none |
| Session lifecycle events | `EmitCreateSessionTelemetry` / `EmitDeleteSessionTelemetry` | none |
| Per-statement context | `private CreateTelemetryContext()` / `private EmitTelemetry()` hooks | none |
| Result reader | `DatabricksCompositeReader` wraps inline + cloud-fetch | direct `CloudFetchReader` (no composite wrapper) |
| Chunk metrics source | `CloudFetchDownloader` (shared, has `Stopwatch` instrumentation) | same `CloudFetchDownloader` (shared) |
| `DriverMode` | hardcoded `Thrift` in `ConnectionTelemetry.BuildDriverConnectionParams` (lines 458, 642) | n/a — no telemetry initialized |

The two connection/statement hierarchies are **disjoint siblings** under `TracingConnection`/`TracingStatement`. C# single inheritance + the Apache common base classes (which we do not own) make a shared base class impractical. Reuse must come through composition.

---

## 4. Architecture

### 4.1 Component overview

```mermaid
classDiagram
    class IConnectionTelemetry {
        <<interface>>
        +Session: TelemetrySessionContext
        +EmitCreateSessionTelemetry(Activity)
        +EmitDeleteSessionTelemetry(long, Exception)
        +DisposeAsync() Task
    }

    class ConnectionTelemetry {
        +static Create(properties, host, version, oauth, sessionId: string, ...)
    }

    class IStatementOperationObserver {
        <<interface>>
        +OnExecuteStarted(stmtType, opType, isCompressed)
        +OnStatementSubmitted(statementId)
        +OnPollCompleted(count, latencyMs, resultFormat)
        +OnFirstBatchReady(latencyMs)
        +OnConsumed(latencyMs)
        +OnChunksDownloaded(ChunkMetrics)
        +OnError(Exception)
        +OnFinalized()
    }

    class TelemetryObserver {
        -ctx: StatementTelemetryContext
        -session: TelemetrySessionContext
        -emitted: bool
    }

    class NullObserver {
        <<singleton>>
    }

    class SafeObserver {
        -inner: IStatementOperationObserver
    }

    class StatementExecutionConnection {
        -_telemetry: IConnectionTelemetry
        +CreateStatement() AdbcStatement
        +OpenAsync()
        +Dispose()
    }

    class StatementExecutionStatement {
        -_observer: IStatementOperationObserver
        +ExecuteQueryAsync(CancellationToken) Task~QueryResult~
        +Dispose()
    }

    class DatabricksConnection {
        -_telemetry: IConnectionTelemetry
    }

    class DatabricksStatement {
        -_observer: IStatementOperationObserver
    }

    IConnectionTelemetry <|.. ConnectionTelemetry
    IStatementOperationObserver <|.. TelemetryObserver
    IStatementOperationObserver <|.. NullObserver
    IStatementOperationObserver <|.. SafeObserver
    SafeObserver --> IStatementOperationObserver : wraps
    StatementExecutionConnection --> IConnectionTelemetry
    DatabricksConnection --> IConnectionTelemetry
    StatementExecutionStatement --> IStatementOperationObserver
    DatabricksStatement --> IStatementOperationObserver
    TelemetryObserver --> ConnectionTelemetry : enqueue log
```

### 4.2 Reused as-is

These components already exist for the Thrift path and are protocol-agnostic. They are reused with no changes:

- `TelemetryClient` / `ITelemetryClient` — per-host batched enqueue + flush
- `TelemetryClientManager` — global singleton, ref-counted client-per-host
- `CircuitBreakerTelemetryExporter` / `CircuitBreaker` / `CircuitBreakerManager`
- `DatabricksTelemetryExporter` — HTTP POST to `/telemetry-ext` / `/telemetry-unauth`
- `FeatureFlagCache` / `FeatureFlagContext` — per-host singleton, controls enable/disable
- `TelemetrySessionContext` / `StatementTelemetryContext` — data containers
- `TelemetryConfiguration` — batch size, flush interval, circuit-breaker thresholds

### 4.3 New components

- **`IStatementOperationObserver`** — small interface, ~8 methods, fail-open contract.
- **`TelemetryObserver`** — default implementation that translates observer calls into `StatementTelemetryContext` mutations and enqueues a `OssSqlDriverTelemetryLog` on finalize.
- **`NullObserver`** — singleton no-op, used as the default field value so callsites never need null checks.
- **`SafeObserver`** (optional) — decorator that swallows any exception thrown by an inner observer. Belt-and-suspenders.

### 4.4 Modified components

- **`ConnectionTelemetry.Create`** — signature change: accept `string sessionId` instead of `TSessionHandle? sessionHandle`. Thrift caller converts at the boundary.
- **`ConnectionTelemetry.BuildDriverConnectionParams`** — new `DriverMode mode` parameter. Caller sets `Thrift` or `Sea`. Removes the hardcoded `DriverMode.Types.Type.Thrift` at lines 458 and 642.
- **`StatementExecutionConnection`** — adds `_telemetry: IConnectionTelemetry` field, calls `InitializeTelemetry` in `OpenAsync`, emits `CREATE_SESSION` on success, emits `DELETE_SESSION` and disposes in `Dispose`.
- **`StatementExecutionStatement`** — adds `_observer: IStatementOperationObserver` field, calls observer methods at lifecycle hookpoints.
- **`DatabricksStatement`** — private telemetry hooks replaced with `_observer` field calls. Behaviorally identical; this is mechanical refactor.

### 4.5 Reader-observer wiring (decorator)

`CloudFetchReader` and `InlineArrowStreamReader` are **shared between Thrift and SEA** today. To keep the shared readers protocol-agnostic, the observer is **not** injected into reader constructors. Instead `StatementExecutionStatement` wraps the underlying reader in a thin `ObservingArrowReader` decorator that it owns:

```csharp
internal sealed class ObservingArrowReader : IArrowArrayStream
{
    private readonly IArrowArrayStream _inner;
    private readonly IStatementOperationObserver _observer;
    private long _readerCreatedTicks;
    private bool _firstBatchEmitted;

    public ObservingArrowReader(IArrowArrayStream inner, IStatementOperationObserver observer)
    {
        _inner = inner;
        _observer = observer;
        _readerCreatedTicks = Stopwatch.GetTimestamp();
    }

    public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken ct = default)
    {
        var task = _inner.ReadNextRecordBatchAsync(ct);
        if (!_firstBatchEmitted)
        {
            _firstBatchEmitted = true;
            _observer.OnFirstBatchReady(ElapsedMs(_readerCreatedTicks));
        }
        return task;
    }

    public ValueTask DisposeAsync()
    {
        _observer.OnConsumed(ElapsedMs(_readerCreatedTicks));
        if (_inner is CloudFetchReader cfr) _observer.OnChunksDownloaded(cfr.GetChunkMetrics());
        return _inner.DisposeAsync();
    }
}
```

- **Zero changes to shared reader classes.** `CloudFetchReader` / `InlineArrowStreamReader` remain unaware of observers.
- **Lifetime matches the result stream.** The decorator is constructed in `StatementExecutionStatement.ExecuteQueryInternalAsync` once the underlying reader is ready, and disposed when the caller disposes the returned `QueryResult.Stream`.
- **Thrift path uses the same decorator** if/when `DatabricksStatement` wants to emit `OnConsumed` / `OnChunksDownloaded` via the observer interface — keeps both transports symmetric without polluting shared readers.

---

## 5. Public Interfaces

### 5.1 `IStatementOperationObserver`

```csharp
internal interface IStatementOperationObserver
{
    // Contract: implementations MUST NOT throw. All methods are fail-open.
    // Contract: callers MUST serialize calls per-statement — a single statement is
    //           driven by one ExecuteQueryAsync caller plus its reader's Dispose, and
    //           statements are not shared across threads. Implementations are NOT
    //           required to be thread-safe for the lifecycle methods.
    // Contract: OnFinalized() is the terminal call. It is the ONE method that may
    //           race (error-path and dispose-path can both reach it), so it MUST be
    //           idempotent — typically via Interlocked.Exchange on an `emitted` flag.

    void OnExecuteStarted(Statement.Types.Type stmtType, Operation.Types.Type opType, bool isCompressed);
    void OnStatementSubmitted(string statementId);
    void OnPollCompleted(int count, long latencyMs, ExecutionResult.Types.Format resultFormat);
    void OnFirstBatchReady(long latencyMs);
    void OnConsumed(long latencyMs);
    void OnChunksDownloaded(ChunkMetrics metrics);
    void OnError(Exception ex);
    void OnFinalized();
}
```

**Naming rationale (`OnStatementSubmitted`):** SEA's `ExecuteStatementAsync` returns immediately with a `statementId` and status commonly `PENDING`/`RUNNING` — the statement has been *accepted by the server*, not *succeeded*. `OnStatementSubmitted` names the actual event. Terminal success (or failure) is observable from `OnPollCompleted` / `OnError`.

**Result format at terminal poll, not submit:** The SEA manifest that carries result-format information is only fully populated when polling reaches `SUCCEEDED`. At the initial submit response it can be null. Result format is therefore resolved at the terminal poll and folded into `OnPollCompleted`'s args, rather than being passed at submit time.

**Why these methods (and not others):** they mirror the existing Thrift integration points enumerated in `csharp/doc/telemetry-design.md` §5.2, restated in the caller's language rather than the proto's.

### 5.2 Refactored `ConnectionTelemetry.Create`

```csharp
public static IConnectionTelemetry Create(
    IReadOnlyDictionary<string, string> properties,
    string host,
    string assemblyVersion,
    OAuthClientCredentialsProvider? oauthTokenProvider,
    string sessionId,                          // CHANGED: was TSessionHandle? sessionHandle
    DriverMode.Types.Type mode,                // NEW: Thrift or Sea
    bool enableDirectResults,
    bool useDescTableExtended,
    int connectTimeoutMilliseconds,
    Activity? activity);
```

Thrift caller converts at the boundary: `sessionHandle.SessionId.Guid.ToString()` (with a null-check; an empty string is mapped to `null` `SessionId` inside `Create` to match the prior behavior). SEA caller passes its `_sessionId` directly.

The `mode` parameter is also threaded through `BuildDriverConnectionParams` and `SafeBuildDriverConnectionParams` — both methods previously hardcoded `Mode = DriverMode.Types.Type.Thrift`. The literal is gone from `ConnectionTelemetry.cs`; only the `DatabricksConnection` (Thrift) call site supplies it.

### 5.3 `IConnectionTelemetry` — no surface change

The interface itself is unchanged; only the static factory above is modified. The fail-open contract on existing methods (`EmitCreateSessionTelemetry`, `EmitDeleteSessionTelemetry`, `EmitOperationTelemetry`, `DisposeAsync`) is **strengthened in documentation** to match the observer's: these methods must never throw to the caller. Today they happen to already swallow via internal try/catch; this design makes that contractually required.

---

## 6. Integration Points (SEA)

| Hookpoint | Class.Method | Observer call |
|---|---|---|
| Statement created | `StatementExecutionConnection.CreateStatement` | constructor injects `_observer` from session |
| Execute issued | `StatementExecutionStatement.ExecuteQueryInternalAsync` (line 323) — before `_client.ExecuteStatementAsync` (line 345) | `OnExecuteStarted(stmtType, opType, compressed)` |
| Execute accepted | `StatementExecutionStatement.ExecuteQueryInternalAsync` — after response received (status typically `PENDING`/`RUNNING`) | `OnStatementSubmitted(statementId)` |
| Terminal poll | `StatementExecutionStatement.PollUntilCompleteAsync` (line 453) — after final `_client.GetStatementAsync` (status=`SUCCEEDED`) | accumulate count/ms across all polls; on terminal state run `SeaResultFormatMapper.Map` against the now-populated manifest and emit `OnPollCompleted(count, latencyMs, resultFormat)` |
| First batch ready | reader-observer decorator (see §4.4) wraps `CreateCloudFetchReader` (line 542) or `InlineArrowStreamReader`; fires on first `ReadNextRecordBatchAsync` | `OnFirstBatchReady(latencyMs)` |
| Results consumed | reader-observer decorator's `DisposeAsync` — wraps the underlying reader's Dispose | `OnConsumed(latencyMs)`; `OnChunksDownloaded(metrics)` for cloud-fetch (metrics pulled from underlying `CloudFetchReader.GetChunkMetrics()`) |
| Error in any of the above | `StatementExecutionStatement.ExecuteQueryInternalAsync` catch block | `OnError(ex)` |
| Statement dispose | `StatementExecutionStatement.Dispose` (line 817) | `OnFinalized()` |
| Session open | `StatementExecutionConnection.OpenAsync` (line 373) — after `CreateSessionAsync` succeeds | `_telemetry.EmitCreateSessionTelemetry(activity)` |
| Session close | `StatementExecutionConnection.Dispose` (line 895) — after `DeleteSessionAsync` | `_telemetry.EmitDeleteSessionTelemetry(elapsedMs, error)`; `await _telemetry.DisposeAsync()` |

---

## 7. SEA Execute Flow (Sequence)

```mermaid
sequenceDiagram
    participant Caller
    participant Stmt as StatementExecutionStatement
    participant Obs as IStatementOperationObserver
    participant Client as IStatementExecutionClient
    participant Reader as CloudFetchReader

    Caller->>+Stmt: ExecuteQueryAsync(ct)
    Stmt->>Obs: OnExecuteStarted(stmt, op, compressed)
    Stmt->>+Client: ExecuteStatementAsync(request)
    Client-->>-Stmt: ExecuteStatementResponse(statementId, status=PENDING/RUNNING)
    Stmt->>Obs: OnStatementSubmitted(statementId)

    loop until terminal state
        Stmt->>+Client: GetStatementAsync(statementId)
        Client-->>-Stmt: GetStatementResponse(status, manifest)
    end
    Note over Stmt: terminal poll: manifest now populated;<br/>SeaResultFormatMapper.Map(disposition, manifest, response)
    Stmt->>Obs: OnPollCompleted(count, totalMs, resultFormat)

    Stmt->>+Reader: construct underlying reader (inline or cloud-fetch)
    Reader-->>-Stmt: IArrowArrayStream
    Note over Stmt: wrap in ObservingArrowReader(reader, observer)
    Stmt-->>-Caller: QueryResult(ObservingArrowReader)

    Note over Caller,Reader: consumer reads batches...
    Caller->>Reader: ReadNextRecordBatchAsync (first call)
    Reader->>Obs: OnFirstBatchReady(ms)

    Caller->>+Reader: DisposeAsync
    Reader->>Obs: OnConsumed(ms)
    Reader->>Obs: OnChunksDownloaded(metrics) [cloud-fetch only]
    Reader-->>-Caller: done

    Caller->>+Stmt: Dispose
    Stmt->>Client: CloseStatementAsync(statementId)
    Stmt->>Obs: OnFinalized
    Note right of Obs: TelemetryObserver builds log,<br/>enqueues to TelemetryClient
    Stmt-->>-Caller: done
```

---

## 8. Result-Format Mapping

SEA does not expose a typed `ResultFormat` field on the statement — it uses a wire-level disposition string. The mapper:

| Wire `disposition` (request) | Wire `format` (request) | Actual result | Proto `ExecutionResult.Format` |
|---|---|---|---|
| `INLINE` | `ARROW_STREAM` | inline attachment | `INLINE_ARROW` |
| `EXTERNAL_LINKS` | `ARROW_STREAM` | external_links populated | `EXTERNAL_LINKS` |
| `INLINE_OR_EXTERNAL_LINKS` (default) | `ARROW_STREAM` | manifest indicates external_links | `EXTERNAL_LINKS` |
| `INLINE_OR_EXTERNAL_LINKS` (default) | `ARROW_STREAM` | inline attachment | `INLINE_ARROW` |

Implemented as a static helper `SeaResultFormatMapper.Map(disposition, manifest, response)` called once at **terminal poll** time (status=`SUCCEEDED`) — the manifest is only fully populated then. The resolved format is then passed to `OnPollCompleted(count, latencyMs, resultFormat)`. No need to peek inside the reader.

---

## 9. Chunk Metrics

SEA's cloud-fetch path uses the **same shared `CloudFetchDownloader`** as Thrift (Stopwatch instrumentation already present at lines 316, 499, 550, 712, 788, 809). The chunk-aggregation work to surface a `ChunkMetrics` value from `CloudFetchReader` is **shared with the gap-fix workstream** — both Thrift and SEA depend on it.

**Dependency:** This design assumes the gap-fix's `CloudFetchDownloader → ChunkMetrics → CloudFetchReader.GetChunkMetrics()` plumbing lands first or concurrently. If gap-fix is delayed, the SEA implementation can ship with `OnChunksDownloaded(ChunkMetrics.Empty)` and backfill metrics in a follow-up — the proto fields are nullable.

---

## 10. `DriverMode.Sea` Wiring (Cross-Cutting)

The only cross-cutting change pulled into this design. Two existing callsites in `ConnectionTelemetry.cs` hardcode `Mode = DriverMode.Types.Type.Thrift`:
- line 458 (in `BuildDriverConnectionParams` static method)
- line 642 (in `Create` factory)

Change: thread a `DriverMode.Types.Type mode` parameter through these two methods. Thrift call site passes `Thrift`; SEA call site passes `Sea`. This is the minimum change needed for SEA telemetry to be distinguishable; deferring it would silently mislabel SEA records as Thrift.

---

## 11. Concurrency and Thread Safety

### Observer contract

- **Lifecycle methods are NOT required to be thread-safe.** A statement is driven by a single `ExecuteQueryAsync` caller (and its reader Dispose, which happens after the result stream is consumed) — statements are not shared across threads. `StatementTelemetryContext` mutations therefore use plain scalar writes; no `Interlocked`/`volatile`/`lock`. This matches the actual usage pattern and avoids gratuitous synchronization cost.
- **`OnFinalized` is the one exception — it MUST be idempotent.** It is the only method that can race: the error-path and the dispose-path can both reach it on a failed statement, potentially on different threads. `TelemetryObserver` enforces "exactly once" via `Interlocked.Exchange` on a single `_emitted` flag; subsequent calls become no-ops.
- **All methods are non-blocking.** `OnFinalized` enqueues to a `BlockingCollection<TelemetryFrontendLog>` (existing `TelemetryClient`); the actual HTTP export runs on a background timer thread owned by `TelemetryClient`.
- **Happens-before for the final read.** `OnFinalized` reads all context fields and builds the log on the calling thread. Because lifecycle calls preceding `OnFinalized` are serialized by the caller (single-threaded statement execution), the writes from `OnExecuteStarted` / `OnStatementSubmitted` / `OnPollCompleted` / etc. are visible to the read with no memory-barrier work needed. Only the `_emitted` flag itself needs an interlocked op to handle the error-vs-dispose race.

### Async patterns

```mermaid
sequenceDiagram
    participant Caller
    participant Stmt
    participant Observer
    participant Client as TelemetryClient
    participant Timer as flush-timer thread

    Caller->>+Stmt: ExecuteQueryAsync()
    Stmt->>Observer: OnExecuteStarted (sync, non-blocking)
    Stmt->>Client: enqueue (via Observer) on OnFinalized
    Stmt-->>-Caller: Task completes

    Note over Client,Timer: HTTP export decoupled from caller
    Timer->>+Client: every 5s
    Client->>Client: ExportAsync(batch) via circuit breaker
    Client-->>-Timer: done
```

### Connection-level concurrency

`IConnectionTelemetry.DisposeAsync` is called from `StatementExecutionConnection.Dispose` synchronously (consistent with existing Thrift pattern): `_telemetry.DisposeAsync().Wait(TimeSpan.FromSeconds(5))`. This flushes any pending events with a hard timeout so connection-close cannot hang on a stuck exporter.

> **Implementation note (T5):** `IConnectionTelemetry.DisposeAsync` returns `Task` (not `ValueTask`), so the call is `_telemetry.DisposeAsync().Wait(TimeSpan.FromSeconds(5))` rather than `.AsTask().Wait(...)`. The result is the same: a hard-bounded synchronous flush.

---

## 12. Error Handling and Failure Modes

### Fail-open contract

Every method on `IStatementOperationObserver` and `IConnectionTelemetry` must swallow all exceptions. Callsites contain **zero try/catch around observer/telemetry calls** — the contract pushes that concern into the implementations exactly once.

**Implementation pattern:** `TelemetryObserver` uses a single private helper rather than per-method try/catch boilerplate, keeping method bodies as one-line action delegates:

```csharp
private static void Safe(Action action) {
    try { action(); }
    catch (Exception ex) { Log.Trace(ex, "telemetry observer suppressed exception"); }
}

public void OnExecuteStarted(Statement.Types.Type stmt, Operation.Types.Type op, bool compressed) =>
    Safe(() => {
        _ctx.StatementType = stmt;
        _ctx.OperationType = op;
        _ctx.IsCompressed   = compressed;
    });

public void OnPollCompleted(int count, long latencyMs, ExecutionResult.Types.Format resultFormat) =>
    Safe(() => {
        _ctx.PollCount     = count;
        _ctx.PollLatencyMs = latencyMs;
        _ctx.ResultFormat  = resultFormat;
    });
```

This concentrates the try/catch in exactly one place per observer impl. The tiny per-call lambda allocation is acceptable — these methods are called O(1) times per statement.

The optional `SafeObserver` decorator is available for future third-party observer implementations that may not honor the contract; it wraps any inner observer with a defensive try/catch per method.

### Circuit breaker reuse

The existing `CircuitBreakerTelemetryExporter` is reused unchanged. Behavior in failure:

| Scenario | Behavior |
|---|---|
| Single export fails | Logged at `TRACE`, batch dropped, next batch attempted |
| 5 consecutive failures | Circuit opens; subsequent enqueue → drop; circuit-breaker event sent to server |
| 60 seconds elapsed | Circuit half-opens; 2 consecutive successes close it |
| `OperationCanceledException` | Propagated (caller cancellation), not counted as failure |

### NullObserver default

`StatementExecutionStatement._observer` defaults to `NullObserver.Instance` — never null. If `TelemetrySessionContext` is null (telemetry disabled by feature flag or property), the statement constructor leaves `_observer` as `NullObserver`. Callsites never check for null.

### Connection telemetry initialization failure

If `ConnectionTelemetry.Create` throws during `OpenAsync` (e.g. feature-flag fetch fails), the exception is caught locally in `StatementExecutionConnection`, logged at `TRACE`, and `_telemetry` is set to a `NoOpConnectionTelemetry` singleton (already exists for Thrift; the name in the codebase is `NoOpConnectionTelemetry`). The connection open succeeds; only telemetry is disabled for the connection.

> **Implementation note (T5):** `ConnectionTelemetry.Create` already swallows internally and returns `NoOpConnectionTelemetry.Instance` on any failure, so the outer try/catch in `StatementExecutionConnection.InitializeTelemetry` is belt-and-suspenders — it only runs if `Create` is later modified to throw in a refactor. Keeping it makes the fail-open contract explicit at the call site.

---

## 13. Configuration

No new configuration parameters. All existing knobs apply unchanged:

| Parameter | Type | Default | Description |
|---|---|---|---|
| `adbc.databricks.telemetry.enabled` | bool | `true` | Driver-side opt-out |
| `adbc.databricks.client_app_name` | string | `null` | Reported in `system_configuration.client_app_name` |

**Server-side feature flag:** `databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForAdbc` (existing). Disabled flag → no events emitted for either transport.

**Priority:** user property > server feature flag > driver default (`true`).

---

## 14. Alternatives Considered

### A. Pure inheritance (have SEA classes inherit from `DatabricksStatement`/`DatabricksConnection`)
**Rejected.** Would drag the entire Thrift base chain (`TCLIService` client, `TSessionHandle`, Hive polling, Thrift transport) into SEA. SEA would be typed as a Thrift class while overriding half of it to no-op. Semantically wrong.

### B. Extract a shared base above both (`DatabricksTelemetryAwareConnection : TracingConnection`)
**Rejected.** Would need to sit between `TracingConnection` and `HiveServer2Connection`. We don't own `HiveServer2Connection`'s parent — it's in the Apache Arrow common package. Blocked structurally.

### C. Decorator at the `AdbcConnection` boundary
**Rejected.** Would wrap the connection externally and intercept ADBC calls. The deep telemetry signals (chunk timing, poll count, first-batch latency, result-format inference) live inside the statement classes — a wrapper from outside cannot see them. You'd still need internal hooks, so you'd pay double.

### D. Static `TelemetryHelper` (as `fix-telemetry-gaps-design.md` proposed)
**Rejected in favor of observer.** Static functions force every callsite to thread `(ctx, session)` parameters. Observer's instance shape gives one-line callsites (`_observer.OnPollCompleted(...)`). Test seam is also cleaner (inject a fake observer vs. mock static methods). Same reuse, better ergonomics. The static-helper proposal in the gap-fix doc is superseded by this design for the new SEA work.

### E. Instance-based `StatementTelemetryEmitter` (intermediate option)
**Rejected in favor of observer.** Emitter's interface is shaped around telemetry's data model (`RecordExecuted`, `RecordPolled`). Observer's interface is shaped around the statement's lifecycle (`OnExecuteStarted`, `OnPollCompleted`). The observer shape decouples statement classes from telemetry types and admits future non-telemetry observers (tracing, audit) without changing statement code. The extra layer of indirection is justified by this decoupling.

---

## 15. Test Strategy

### Unit tests

**`IStatementOperationObserver` implementations:**
- `NullObserver_AllMethods_AreNoOps`
- `NullObserver_IsSingleton`
- `SafeObserver_PropagatesNormalCallsToInner`
- `SafeObserver_SwallowsExceptionsFromInner_LogsAtTrace`
- `TelemetryObserver_OnExecuteStarted_PopulatesContext`
- `TelemetryObserver_OnStatementSubmitted_RecordsStatementId`
- `TelemetryObserver_OnPollCompleted_RecordsCountLatencyAndResultFormat`
- `TelemetryObserver_OnFinalized_EnqueuesExactlyOnce`
- `TelemetryObserver_OnFinalized_CalledFromErrorAndDispose_EnqueuesOnce` (covers the documented race in §11)
- `TelemetryObserver_OnError_RecordsErrorAndFinalizes`
- `TelemetryObserver_AllMethods_NeverThrow_WhenContextCorrupted`
- `TelemetryObserver_OnChunksDownloaded_MergesIntoChunkDetails`
- `ObservingArrowReader_FirstReadNextRecordBatch_EmitsOnFirstBatchReady`
- `ObservingArrowReader_DisposeAsync_EmitsOnConsumed`
- `ObservingArrowReader_DisposeAsync_WithCloudFetchInner_EmitsOnChunksDownloaded`
- `ObservingArrowReader_DisposeAsync_WithInlineInner_DoesNotEmitOnChunksDownloaded`

**`ConnectionTelemetry.Create` refactor:**
- `Create_AcceptsStringSessionId`
- `Create_ThriftMode_SetsDriverModeThrift`
- `Create_SeaMode_SetsDriverModeSea`
- `Create_ThrowingHttpClient_ReturnsNullConnectionTelemetry`

**`SeaResultFormatMapper`:**
- `Map_InlineDisposition_ReturnsInlineArrow`
- `Map_ExternalLinksDisposition_ReturnsExternalLinks`
- `Map_AutoDisposition_WithInlineResult_ReturnsInlineArrow`
- `Map_AutoDisposition_WithExternalLinks_ReturnsExternalLinks`

### Integration tests (SEA path, against a real or mocked SQL warehouse)

- `Sea_ExecuteQuery_EmitsTelemetryWithStatementId`
- `Sea_ExecuteQuery_WithSyntaxError_EmitsErrorTelemetry`
- `Sea_ExecuteQuery_CloudFetch_RecordsChunkMetrics`
- `Sea_ExecuteQuery_InlineResults_RecordsInlineFormat`
- `Sea_OpenConnection_EmitsCreateSession`
- `Sea_CloseConnection_EmitsDeleteSessionAndFlushes`
- `Sea_TelemetryDisabledByFeatureFlag_EmitsZeroEvents`
- `Sea_TelemetryDisabledByProperty_EmitsZeroEvents`
- `Sea_TelemetryExporterFails_DoesNotAffectQueryExecution`
- `Sea_TelemetryRecord_HasDriverModeSea` (parity check vs Thrift's `DRIVER_MODE_THRIFT`)
- `Sea_ConcurrentStatements_EachEmitsExactlyOneRecord`

### Cross-transport regression

- `Thrift_Telemetry_StillEmitsAfterRefactor` — confirm the `string sessionId` refactor and `_observer` field substitution on `DatabricksStatement` produce byte-identical telemetry to current main.
- `Thrift_DriverMode_StillReportedAsThrift` — confirm the new `mode` parameter defaults / passes through correctly.

---

## 16. Rollout

### Dependencies

- **Gap-fix `ChunkMetrics` plumbing** (`CloudFetchDownloader → CloudFetchReader.GetChunkMetrics()`) — shared with this design's `OnChunksDownloaded` callsite. If not ready, ship with `ChunkMetrics.Empty`; backfill in follow-up. Proto fields are nullable.
- **No backend changes required.** The telemetry endpoint, proto schema, and `DriverMode.Sea` enum value already exist (`sql_driver_telemetry.proto:338-344`).

### PR sequencing

1. Refactor `ConnectionTelemetry.Create` signature (string sessionId, `DriverMode mode` param). Thrift call site updates only. Verifies no behavioral change.
2. Introduce `IStatementOperationObserver`, `TelemetryObserver`, `NullObserver`, `SafeObserver`. No callers yet.
3. Refactor `DatabricksStatement` to use `_observer` field instead of private hooks. Mechanical; behavior unchanged.
4. Wire telemetry into `StatementExecutionConnection` (`_telemetry`, `OpenAsync`, `Dispose`).
5. Wire telemetry into `StatementExecutionStatement` (`_observer` field + hookpoint calls).
6. Add `SeaResultFormatMapper`.
7. Add integration tests against a real SQL warehouse.

Each PR is independently reviewable and revertible. Steps 1–3 land in any order; 4–6 depend on 1–3; 7 depends on 4–6.

### Backward compatibility

- Connection-string options: unchanged.
- Proto schema: unchanged.
- Public ADBC API: unchanged.
- Thrift telemetry output: byte-identical (verified by regression test above).

---

## 17. Open Questions

1. **Polling-granularity semantics:** Thrift `PollCount` counts `GetOperationStatusAsync` calls. SEA `PollCount` will count `GetStatementAsync` calls. These are semantically equivalent (both are status-polls during async execution) but the wire shapes differ. Confirm with the telemetry consumer team that this is acceptable; otherwise introduce a separate `sea_poll_count` field.
2. **`is_internal_call` for SEA `USE SCHEMA`:** SEA does not have a separate `SetSchema` method (uses statement execution). Should `is_internal_call=true` propagate the same way it does (or will, post gap-fix) for Thrift? Suggest defer to follow-up.
3. **`operation_type` for SEA:** Always `EXECUTE_STATEMENT_ASYNC` (SEA is always async on the wire). Confirm this matches the telemetry consumer team's expectation, or whether we should map specifically (`EXECUTE_STATEMENT` for sync-emulated paths).
