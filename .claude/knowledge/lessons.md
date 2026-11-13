# Lessons

<!-- distilled-through: none -->

## SEA metadata [phase=author,followup,review]
- L-0001 (1×, log:2026-07-22): On SEA, a bare catalog `%`/`*` means all catalogs only when escape_pattern_wildcards is off; with escaping on it is a literal name, as on Thrift.
- L-0002 (1×, #604): When a LIKE glob goes into a single-quoted SQL literal, double each backslash (LikePattern); the server's string parser eats one layer before LIKE matches.
- L-0003 (1×, #622): SEA GetPrimaryKeys must read TABLE_CAT/TABLE_SCHEM/TABLE_NAME from SHOW KEYS (catalogName/namespace/tableName), using input args only as fallback, never the caller's casing.
- L-0004 (1×, #609): Split PK/FK metadata ops into a validating public wrapper over a non-throwing core; check ShouldReturnEmptyPKFKResult before validating; no validateArgs flag or broad catch.
- L-0005 (1×, #626): Non-null empty tableTypes must match zero rows (null matches all) on GetObjects and gettables, Thrift and SEA; Thrift relies on the hiveserver2 base, SEA checks are in-driver.
- L-0006 [phase=author,followup] (1×, #627): For unmodeled server types, BASE_TYPE_NAME comes from SqlTypeNameParser.Parse's stripped base (geometry(0) -> GEOMETRY) after Databricks aliases, so SEA and Thrift match.

## SEA errors [phase=author,followup,review]
- L-0007 (1×, #636): On SEA, read a failed statement's SQLSTATE from status.sql_state (sibling of error), falling back to error.sql_state; unit fixtures must put sql_state at status level, not under error.
- L-0008 (1×, #604): Classify SEA errors by SqlState first, then by message text (SQLSTATE: NNNNN or names like SCHEMA_NOT_FOUND), as DatabricksException.IsObjectNotFoundException does.
- L-0009 (2×, #604 #609): Cross-protocol parity is on AdbcStatusCode and SqlState, not the AdbcException subclass; client-side rejections must match Thrift's Status and SqlState (InternalError + 42000).
- L-0010 [phase=author,followup; lang=csharp] (1×, #628): Gate classification of an execute error that can arrive on either protocol on base AdbcException, not DatabricksException or HiveServer2Exception, and discriminate by message substring.

## SEA execution and sessions [phase=author,followup,review]
- L-0011 (1×, #635): On SEA, ExecuteStatementAsync can return PENDING/RUNNING without having run (wait_timeout 0s); poll to a terminal state before any dependent statement or recording success.
- L-0012 [phase=author,followup] (1×, #635): The server ignores a per-statement catalog on execute, so a bare 2-level name needs a session USE CATALOG first, issued on change as the ODBC and JDBC drivers do.
- L-0013 (1×, #635): USE CATALOG and SET are session-global on a shared connection: concurrent statements can interleave, and the in-memory current-catalog tracker goes stale after a user's own USE CATALOG.

## SEA result schema [phase=author,followup]
- L-0014 (1×, #644): When ArrowTypeParser maps a SQL type to an Arrow type the IPC bytes lack (VOID to Utf8 but NullArray bytes), add a stream that rewrites batch arrays; the schema change alone is inconsistent.

## CloudFetch [phase=author,followup,review]
- L-0015 (1×, #623): Sequential fallback (maxStragglersBeforeFallback 0) only serializes downloads; straggler detection and cancel still run, so never assert zero cancellations in sequential mode.

## Tests and E2E [phase=author,followup,review]
- L-0016 [phase=author,followup] (1×, log:2026-07-22): To reproduce a SEA-only bug in an E2E test, force adbc.databricks.protocol=rest on the connection; the default protocol is thrift and hides it.
- L-0017 (1×, #623): Wait on the end-of-results signal (EndOfResultsGuard) and assert full delivery; never a fixed Task.Delay plus a "nothing cancelled" assert, which flakes on loaded CI.
- L-0018 [phase=author,followup] (1×, #628): Before a concurrent query burst in E2E tests, warm the session with one serial query (bounded retry); tolerate only BAD_REQUEST + sparkSession + is null, in a bounded count.
- L-0019 (1×, #626): Skip.IfNot-gated E2E tests skip without a live workspace, so server-response logic also needs a unit test (e.g. StatementExecutionEmptyTableTypesTests).
- L-0020 [phase=author,followup; lang=csharp] (1×, log:2026-08-12): The C# test project is csharp/test/AdbcDrivers.Databricks.Tests.csproj; glob csharp/test/*.csproj before dotnet test instead of guessing a name.
- L-0021 [phase=author,followup; lang=csharp] (2×, #614 #628): Don't gate leak tests on GC.GetTotalMemory deltas with LOH (>85KB) buffers; assert a WeakReference is dead after dispose and a full GC, or use CompactOnce and the minimum baseline.
- L-0022 [phase=author,followup; lang=csharp] (1×, #614): Create and dispose the tracked object inside a NoInlining helper that has returned before the GC; Debug-build JIT keeps inline locals rooted and inflates the alive count.

## Dispose and teardown [phase=author,followup; lang=csharp]
- L-0023 (1×, #618): Teardown RPCs in Dispose (e.g. CloseOperation) must be best-effort: record the error, never rethrow, since a throw skips remaining teardown and breaks the dispose contract.
