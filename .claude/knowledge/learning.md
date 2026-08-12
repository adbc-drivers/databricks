# Engineer-Bot Learning Log

Append-only log of durable, non-obvious engineering lessons from fixes the
engineer-bot makes in this repo, so future runs (and humans) don't relearn them.
The bot reads this as system-prompt context (newest entries first) on every run —
**consult past entries before fixing a similar area** (grep by keyword or scan
titles below).

> Lives at the repo-root `.claude/` for now (one log for the whole repo); we can
> split it per-language (`csharp/`, `go/`, `rust/`) later if it grows.

## When to add an entry

Add an entry when a fix surfaced a **durable, reusable** lesson:

- A recurring gotcha in the driver (Thrift vs SEA path, metadata handling, etc.).
- A protocol/server behavior that contradicted the obvious mental model.
- A fix pattern worth reusing — or an anti-pattern to avoid.
- A cross-driver parity rule (e.g. how the JDBC driver or the vendored
  `hiveserver2` base behaves) confirmed against real source.

Skip one-shots: typos, stale builds, local-machine flukes. Entries are for things
the next fix will hit again.

## Format

```
### YYYY-MM-DD: <area> — <brief headline>

**Issue:** what went wrong
**Root Cause:** why it happened
**Fix:** what changed, and where (file/symbol)
**Rule:** the one-line do/don't takeaway — write it greppable
```

The `**Rule:**` line is what makes an entry searchable — phrase it so a future
reader can grep for it.

## Entries

### 2026-07-22: SEA metadata — catalog "%" match-all must honor escape_pattern_wildcards

**Issue:** With `escape_pattern_wildcards=true`, `GetColumns`/`GetTables`/`GetSchemas` with `catalog="%"` returned rows from ALL catalogs on the SEA/REST path but 0 rows on Thrift — a cross-protocol row-set divergence (Issue #593).
**Root Cause:** `StatementExecutionStatement.EffectiveCatalog` intercepted a bare `%`/`*` catalog via `IsMatchAllCatalogPattern` and rewrote it to `null` ("all catalogs") *unconditionally*, before considering the escape flag. Thrift instead escapes `%` → `\%` (a literal catalog matching nothing).
**Fix:** Gate the interception on the flag: `if (IsMatchAllCatalogPattern(catalog) && !_escapePatternWildcards) catalog = null;`. With escaping on, `%` flows as a literal backtick-quoted identifier; the resulting `SCHEMA_NOT_FOUND` is already caught by the existing `IsObjectNotFoundException` handlers in each metadata method and mapped to an empty result — matching Thrift.
**Rule:** Match-all catalog `%`/`*` interception on the SEA path must respect `_escapePatternWildcards` — when escaping is on, treat `%` literally to match the Thrift contract, not as "all catalogs".

### Note on testing SEA-specific bugs
The E2E suite runs on whichever protocol the run configures (Thrift by default in this env). A SEA-only bug will NOT reproduce unless the test forces it: pass `{ DatabricksParameters.Protocol, "rest" }` when creating the connection (see `SeaMetadataE2ETests` `RestProtocol` / `StatementExecutionDriverE2ETests.CreateRestConnection`).
**Rule:** To reproduce/verify a SEA/StatementExecution-path bug in an E2E test, force `adbc.databricks.protocol=rest` on the connection — don't rely on the run's default protocol.

### 2026-08-03: learnings since 2026-08-02T17:43:51Z
- **Context:** PR #614 de-flaked the C# E2E test `PartialRead_DisposeStatement_ShouldNotHangOrLeak`, which had used a `GC.GetTotalMemory` before/after delta to detect a CloudFetch pipeline leak.
  **Rule:** Don't detect managed-memory leaks with `GC.GetTotalMemory()` deltas when result buffers exceed 85KB (Large Object Heap) — the LOH grows to a working-set high-water-mark and is not returned to the OS, so the delta plateaus even with zero leak; instead assert collectability directly via a `WeakReference` that must be dead after dispose + a forced full GC.
- **Context:** PR #614 tightened a WeakReference-based leak assertion; the tracked object had to be allocated/disposed inside a `[MethodImpl(MethodImplOptions.NoInlining)]` helper (`RunPartialReadCycleAsync`) to make `aliveReaders == 0` build-config-independent.
  **Rule:** WeakReference collectability tests must create and dispose the tracked object inside a `NoInlining` helper that has fully returned before the GC — in Debug builds the JIT keeps a method's local slots reported as GC roots until the method returns, so an inline reader/statement local stays rooted and inflates the alive count.
- **Context:** PR #604 added `LikePattern` (Layer 2) on top of `ConvertPattern` (JDBC LIKE → Hive glob) for the SEA `SHOW ... LIKE` metadata commands.
  **Rule:** When embedding a LIKE glob into a single-quoted SQL string literal, double backslashes (`\` → `\\`) — the server's string-literal parser consumes one backslash layer before the LIKE matcher sees the pattern, so an un-doubled backslash collapses and never matches an identifier that literally contains a backslash (the JDBC reference driver has the same bug, databricks-jdbc#1598).
- **Context:** PR #604 classified SEA errors in `DatabricksException.IsObjectNotFoundException` / `IsDescTableExtendedUnsupported` for Thrift-parity fallbacks.
  **Rule:** On the SEA/StatementExecution execute path `AdbcException.SqlState` is frequently null and the SQLSTATE is only embedded in the error message text — check `SqlState` first, then scan the message for `"SQLSTATE: NNNNN"` when classifying SEA errors.
- **Context:** PR #604 made the SEA FAILED-state path throw its own `DatabricksException` (with SqlState/nativeError set) rather than matching Thrift's `HiveServer2Exception` type.
  **Rule:** Cross-protocol exception parity is enforced on the ADBC contract (Status + SqlState that consumers branch on), NOT on the concrete `AdbcException` subclass — SEA may throw `DatabricksException` where Thrift throws `HiveServer2Exception`; but this parity only holds for errors delivered as HTTP 200 + `status.state=FAILED`, since the HTTP-error path (`EnsureSuccessStatusCodeAsync`) still surfaces `DatabricksException` for 4xx.

### 2026-08-04: learnings since 2026-08-03T18:01:33Z
- **Context:** PR #623 fixed a flaky CloudFetch test that asserted `Assert.Empty(downloadCancelledFlags)` in sequential fallback mode.
  **Rule:** Sequential fallback (maxStragglersBeforeFallback==0) only changes scheduling (one download at a time via the sequential semaphore) — it does NOT disable straggler detection/cancellation; a slow tail download can still straggle and trigger a cancel+re-download, so never assume "zero cancellations" in sequential mode.
- **Context:** PR #623 replaced a `Task.Delay(4000)` + zero-cancellation assert with draining the result queue until the end-of-results signal (null) and asserting the full download set was delivered.
  **Rule:** For concurrent downloader/queue tests, wait on the actual end-of-results/completion signal and assert delivery completeness — never a fixed sleep plus a "nothing was cancelled/retried" assert, which races monitor ticks and flakes on loaded CI.
- **Context:** PR #622 fixed SEA `GetPrimaryKeys` echoing the caller's input casing for TABLE_CAT/TABLE_SCHEM/TABLE_NAME instead of the server's stored casing.
  **Rule:** Metadata identifier columns (TABLE_CAT/TABLE_SCHEM/TABLE_NAME) must be read back from the server response (SHOW KEYS: catalogName/namespace/tableName) to reflect canonical stored casing — matching Thrift and the JDBC reference driver — with fallback to input args only when the server omits a value; never echo the caller's input case.
- **Context:** PR #618 removed a `throw` from the TCloseOperationReq catch block in `DatabricksCompositeReader.Dispose`.
  **Rule:** Teardown cleanup RPCs issued during Dispose (e.g. CloseOperation) must be best-effort — record the error (telemetry/trace) but never rethrow out of Dispose; throwing violates the .NET dispose contract and skips remaining teardown, leaking still-undisposed resources.
- **Context:** PR #609 added client-side exact-match arg validation to SEA GetPrimaryKeys/GetCrossReference, refactored (per maintainer) into public validating wrappers delegating to non-throwing internal cores.
  **Rule:** When a metadata op needs client-side arg rejection but an internal reuse path (e.g. GetColumnsExtended) legitimately passes unspecified args, split into a public wrapper that validates+throws and a non-throwing internal core that returns empty — don't use a validateArgs flag or a broad try/catch (catching DatabricksException/InternalError/42000 would swallow real server errors); and check ShouldReturnEmptyPKFKResult (disabled-feature → empty) BEFORE the validation throws, consistently across sibling PK/FK methods.
- **Context:** PR #609 made SEA throw its own DatabricksException for missing exact-match args while matching the Thrift server's rejection contract.
  **Rule:** Cross-protocol exception parity is checked on Status + SqlState, not the concrete exception type — a SEA-path exception replacing a Thrift server rejection must set the same AdbcStatusCode and SqlState (e.g. InternalError + "42000"), but may be any AdbcException subclass.

### 2026-08-05: learnings since 2026-08-04T18:00:56Z
- **Context:** De-flaking `RepeatedLargeCloudFetch_MemoryShouldPlateau` (PR #628): a 1M-row CloudFetch allocates large buffers on the .NET Large Object Heap, and a normal `GC.Collect(2, Forced, blocking)` reclaims dead LOH objects but does NOT return the freed segments, so `GC.GetTotalMemory` carried tens of MB of iteration-to-iteration high-water-mark/fragmentation noise that swamped any real leak signal.
  **Rule:** For .NET memory-plateau/leak tests over large allocations, request a one-shot LOH compaction (`GCSettings.LargeObjectHeapCompactionMode = CompactOnce`) before the forced gen-2 GC so `GetTotalMemory` reflects live bytes, and anchor the growth baseline to the MINIMUM post-warmup sample (not the first) so allocator jitter doesn't manufacture phantom growth.
- **Context:** PR #628's `IsTransientColdStartError` originally gated on `ex is DatabricksException`, but the same server execute error (`BAD_REQUEST … sparkSession is null`) surfaces as `HiveServer2Exception` on the default Thrift/HiveServer2 path and only as `DatabricksException` on the SEA path — the two are siblings both extending `AdbcException`, so the type check silently missed the Thrift case and would have let the very transient it targeted fail the test.
  **Rule:** When classifying a server-side execute error that can arrive on either protocol, gate on the common base type `AdbcException` (not a protocol-specific subclass like `DatabricksException` or `HiveServer2Exception`) and let message-substring checks do the discrimination — Thrift and SEA raise different concrete exception types for the same server error.
- **Context:** PR #628 concurrency stress test flaked because a freshly-provisioned Databricks warehouse can return a transient server-side `BAD_REQUEST … sparkSession is null` when several threads fire their FIRST query simultaneously against a not-yet-warm SparkSession.
  **Rule:** Before a concurrent query burst in E2E/stress tests, warm up the session serially with one trivial query (with bounded retry, since the warm-up itself can hit the race); tolerate a small bounded count of cold-start transients but require a specific predicate (BAD_REQUEST + sparkSession + is null) so the tolerance can't mask genuine concurrency/isolation defects.
- **Context:** Issue #568 / PR #627: `GetColumns` on a GEOMETRY/GEOGRAPHY column threw `NotSupportedException` because `SqlTypeNameParser` had no entry for geospatial (or other unmodeled) types; the SEA `ColumnMetadataHelper.GetBaseTypeName` needed to report the same stripped BASE_TYPE_NAME as the Thrift path.
  **Rule:** For unmodeled/new server types, derive BASE_TYPE_NAME from the shared parser's stripped-base-name fallback (e.g. `geometry(0)` -> `GEOMETRY`) via `Parse` rather than returning the alias verbatim, so both the SEA and Thrift (`SparkConnection`) metadata paths report an identical stripped base name — check Databricks-specific aliases first since those names aren't parseable.

### 2026-08-06: learnings since 2026-08-05T17:56:06Z
- **Context:** PR #626 aligned empty-vs-null `tableTypes` handling with databricks-jdbc (METADATA-035) across getTables, and a reviewer caught that only 3 of 4 surfaces were fixed.
  **Rule:** For metadata getTables, `tableTypes=null` means all types while `tableTypes=[]` (empty, non-null) must match NONE (zero rows) per JDBC parity — and this has FOUR surfaces (`GetObjects` and the `is_metadata_command "gettables"` shim, each on Thrift and SEA); fixing only some leaves a cross-protocol divergence, so change all four together.
- **Context:** PR #626's Thrift path delegates to the vendored `csharp/hiveserver2` base (`HiveServer2Connection.GetTablesAsync`/`HiveServer2Statement.GetTablesAsync`), which can't be edited in this repo.
  **Rule:** The shared HiveServer2 base forwards `tableTypes` into `TGetTablesReq.TableTypes` only when `Count>0` (and `HiveServer2Statement` uses `string.IsNullOrEmpty`), so it treats an empty list as "all types" — the opposite of match-none; to enforce empty→none on the Thrift path without forking the base's nested-result builder, substitute an unmatchable sentinel table-type token, and keep it plain ASCII (no NUL/control chars) so strict server-side string validation can't turn an empty filter into a hard error.
- **Context:** In PR #626 the Thrift sentinel-substitution path was covered only by `EmptyTableTypesE2ETests`, a `SkippableTheory` gated on `Skip.IfNot(Utils.CanExecuteTestConfig(...))`.
  **Rule:** Live-workspace-gated E2E tests (SkippableFact/SkippableTheory) do NOT run in ordinary CI, so logic that depends on server response behavior (e.g. a sentinel forwarded into `TGetTablesReq` returning zero rows) has no regression gate unless you add a mock-server assertion (`HiveServer2.TestServer`/`MockServerMetadataCommandTests`) alongside the E2E.

### 2026-08-08: learnings since 2026-08-07T17:38:04Z
- **Context:** PR #636 (csharp SEA/StatementExecution) fixed failed-statement error handling by reading `status.sql_state` instead of `status.error.sql_state`. Verified live against a warehouse: a FAILED SEA response carries `sql_state` at the status level (a sibling of `error`), while the `error` object holds only `error_code` and `message` — no `sql_state`. The fix threads the whole `StatementStatus` into the exception builder and falls back to `error.SqlState` only if the status-level value is absent.
  **Rule:** On the SEA/StatementExecution path, read the failed-statement SQLSTATE from `status.sql_state` (sibling of `error`), not `status.error.sql_state` — and mirror that real shape (sql_state at status level, error carrying only error_code+message) in unit-test fixtures, since a fixture with sql_state nested under error masks the bug.

### 2026-08-11: learnings since 2026-08-10T17:39:39Z
- **Context:** PR #635 (csharp catalog scoping) issued a standalone `USE CATALOG` before a query; reviewers repeatedly flagged the SEA path as fire-and-forget — it `await`s only the initial `ExecuteStatementAsync` POST and never polls to a terminal state, unlike the Thrift path which routes through `ExecuteUpdateAsync` and waits for completion.
  **Rule:** On the SEA/StatementExecution path a submitted statement is NOT guaranteed executed on return — `ExecuteStatementAsync` throws only on an *immediately* `FAILED` initial response, and with direct results disabled (`_waitTimeout="0s"`, `OnWaitTimeout="CONTINUE"`) it returns `PENDING`/`RUNNING` without having run; any statement whose side effect a later statement depends on must be polled to a terminal `SUCCEEDED` state (throwing on `FAILED`/`CANCELED`) before proceeding and before recording success, or its effect races and its failure is silently swallowed. Note the default direct-results-on path (`_waitTimeout=null`, ~10s server wait) usually resolves trivial statements synchronously, so this only bites the async config and E2E tests won't catch it.
- **Context:** PR #635 needed a native query using a bare 2-level `schema`.`table` name to resolve against a drilled-in catalog; the fix issues a session-level `USE CATALOG` rather than setting a per-statement catalog on the execute request.
  **Rule:** The server honors no per-statement catalog on the execute request (Thrift `StatementConf.InitialNamespace` is ignored), so scoping a bare 2-level name to a non-default catalog requires issuing a standalone `USE CATALOG` on the session before executing — matching the ODBC/Simba and JDBC reference drivers, which issue it on change (not per query).
- **Context:** PR #635 tracked the session's current catalog in memory and mutated it via `USE CATALOG` on a connection's shared session; reviewers flagged that the switch is sticky and the check-then-USE-then-track sequence is not atomic.
  **Rule:** The server-side current catalog is per-session and every statement on one connection shares that session, so a `USE CATALOG` (or any session `SET`) is session-global and persists for all later statements — it is best-effort, not per-statement isolated: concurrent statements on one connection can interleave their `USE CATALOG` and query, and an in-memory issue-on-change tracker goes stale if a user's own inline `USE CATALOG` changes the session out of band. Never assume per-statement isolation of session state on a shared connection.

### 2026-08-12: learnings since 2026-08-11T17:42:31Z
- **Context:** PR #644 / run #31539108512 fixed DATATYPE-042 by remapping the untyped-NULL SQL type (VOID/NULL) from Arrow `NullType` to `StringType` on the SEA/StatementExecution path (to match Thrift's STRING) — but the IPC bytes still arrive as a `NullArray`, so a new `NullColumnSerializingStream` was added to convert it to an all-null `StringArray`, mirroring the sibling `IntervalSerializingStream`/`ComplexTypeSerializingStream`.
  **Rule:** On the SEA path, `IArrowArrayStream` is a strongly-typed contract where the declared manifest schema and each batch's column arrays MUST agree on type — so whenever `ArrowTypeParser` maps a SQL type to an Arrow type that differs from what the IPC data delivers (e.g. VOID→Utf8 but bytes are a `NullArray`), you must also insert a serializing stream that rewrites the batch arrays to the declared type; changing only the type mapping produces a self-inconsistent schema-vs-batch result.
- **Context:** In run #31539108512 the author guessed the C# test project path (`dotnet test csharp/test/Apache.Arrow.Adbc.Tests.Drivers.Databricks.csproj`), which failed, then had to glob `csharp/test/*.csproj` and retry with the actual name before tests would run.
  **Rule:** The csharp test project in this repo is `csharp/test/AdbcDrivers.Databricks.Tests.csproj` — glob `csharp/test/*.csproj` to confirm the path before invoking `dotnet test`, rather than guessing an Apache.Arrow-style csproj name and wasting a build/test cycle.
