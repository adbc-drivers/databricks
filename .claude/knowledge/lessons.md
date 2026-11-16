# Lessons

<!-- distilled-through: none -->

## SEA metadata
- L-0001 (1×, #593): On SEA a match-all catalog `%`/`*` means all catalogs only when escape_pattern_wildcards is off; with escaping on, treat it literally (empty result), as Thrift does.
- L-0002 (1×, #604): When putting a LIKE glob into a single-quoted SQL literal, double every backslash: the string parser strips one layer before LIKE sees the pattern.
- L-0003 (1×, #622): Read metadata identifier columns (TABLE_CAT, TABLE_SCHEM, TABLE_NAME) from the server response to keep stored casing; fall back to input args only when the server omits them.
- L-0004 (1×, #609): For client-side metadata arg validation, use a public validating wrapper over a non-throwing internal core; check ShouldReturnEmptyPKFKResult first, in every sibling PK/FK method.
- L-0005 (1×, #626): getTables: tableTypes=null means all types and [] means none; fix all four surfaces together (GetObjects and the gettables shim, on Thrift and SEA).
- L-0006 (1×, #626): The vendored hiveserver2 base treats an empty tableTypes list as all types; on Thrift enforce empty-means-none with an unmatchable plain-ASCII sentinel type.
- L-0007 (2×, #660 #645): SHOW FOREIGN KEYS returns FKs to every parent: filter to a caller-specified parent client-side (case-insensitive); a null requested field means no constraint.
- L-0008 (1×, #645): When a field is both a filter key and filled with a fallback to the requested value, filter on the raw server value first; apply the fallback only for row population.
- L-0009 (1×, #660): A field seeded from the connection's default catalog doesn't mean the caller set the arg; track an explicit was-set flag, and use fixtures whose parent catalog differs from the seed.
- L-0010 (1×, #654): GetTables must not return an empty or null TABLE_TYPE: default it to "TABLE" (JDBC parity) by wrapping the result stream; pass other encodings and a null stream through.
- L-0011 (1×, #692): SEA treats `IN CATALOG` as a LIKE pattern: filter native rows client-side, exact match for GetTables/GetColumns/GetTableSchema and pattern match for GetObjects.
- L-0012 (1×, #692): In a per-catalog fanout loop never `catch { continue; }`; filter with `when (ex.IsObjectNotFoundException())` so cancellation and timeouts propagate instead of truncating results.
- L-0013 (1×, #627): For unmodeled server types derive BASE_TYPE_NAME via the shared parser's stripped-base-name fallback (geometry(0) to GEOMETRY) so SEA and Thrift match; check Databricks aliases first.
- L-0014 (1×, #760): With multi-catalog on, GetColumns on the SPARK catalog scans the whole metastore; scope E2E metadata tests to the per-run table (ApacheParameters.TableName).

## Cross-protocol parity
- L-0015 (3×, #628 #609 #604): Error parity is on AdbcStatusCode + SqlState, not the exception type: classify execute errors on base AdbcException (Thrift throws HiveServer2Exception, SEA DatabricksException).
- L-0016 (1×, #604): On SEA, AdbcException.SqlState is often null: check SqlState first, then scan the message for "SQLSTATE: NNNNN".
- L-0017 (1×, #636): A FAILED SEA response carries sql_state at the status level, beside `error` (which holds only error_code and message); mirror that shape in test fixtures.
- L-0018 (1×, #767): CloudFetch reading differs by protocol (Thrift per-file limits, SEA manifest TotalRowCount): run large or boundary CloudFetch E2E on both the thrift and rest legs.
- L-0019 (1×, #593): E2E runs default to Thrift; to reproduce a SEA-only bug, force `adbc.databricks.protocol=rest` on the connection.

## SEA execution and results
- L-0020 (1×, #635): On SEA, ExecuteStatementAsync can return PENDING/RUNNING without running the statement; poll to SUCCEEDED before relying on its side effect (direct-results E2E won't catch it).
- L-0021 (1×, #635): The server ignores a per-statement catalog; scope a bare schema.table by issuing USE CATALOG on the session, on change, like the ODBC and JDBC drivers.
- L-0022 (1×, #635): Session state (USE CATALOG, SET) is shared by every statement on a connection; never assume per-statement isolation, and expect an in-memory tracker to go stale.
- L-0023 (1×, #644): On SEA the declared Arrow schema and each batch must agree: if the type mapping differs from the IPC data (VOID to Utf8 over a NullArray), add a serializing stream that rewrites batches.
- L-0024 (1×, #701): SEA's manifest schema lacks complex columns' physical child types; when a rewrite needs them, buffer the first IPC batch, keep other paths lazy, and document the eager read.
- L-0025 (1×, #701): A substring match on SQL type_text also hits nested field names; use it only as a cheap pre-filter and confirm by parsing the type before acting.
- L-0026 (1×, #751): SEA FAST session_version: read it from CreateSession, Execute and GetStatement (FAILED too), keep the maximum seen, echo it on later executes, one tracker per connection.
- L-0027 (1×, #693): JsonSerializer.Deserialize over bytes assumes UTF-8 and keeps a BOM, unlike the string and stream paths; use it only for raw UTF-8, or skip a leading BOM first.
- L-0028 (1×, #678): AutomaticDecompression=None also drops Accept-Encoding; send `Accept-Encoding: identity` explicitly, and scope the change to the one client that needs it.

## CloudFetch and streams
- L-0029 (3×, #770 #764): With concatenated LZ4 frames (K4os) a 0 read before the input's end is a frame boundary: read while Position < Length, require a seekable stream, guard no-progress; recheck on upgrades.
- L-0030 (3×, #770 #767 #764): .NET Framework CopyTo reads 81,920 bytes, .NET 8 131,072: test read-boundary code with explicit buffer sizes, and run CloudFetch on net472 (Power BI and the gateway use it).
- L-0031 (1×, #764): A cancel while awaiting the next file can return null like end-of-results; gate row-shortfall and completeness checks on !cancellationToken.IsCancellationRequested.
- L-0032 (1×, #623): Sequential CloudFetch fallback changes scheduling only, not straggler cancellation: never assert zero cancellations; wait for end-of-results and assert completeness, not a sleep.
- L-0033 (1×, #681): UnobservedTaskException fires only for faulted tasks, not canceled ones; observe an abandoned prefetch task on the fault path that runs in production, not in an unused Dispose.

## .NET tests
- L-0034 (2×, #628 #614): Don't detect leaks with GC.GetTotalMemory deltas over large buffers (the LOH keeps a high-water mark); assert a WeakReference dies after dispose and a full GC, or compact the LOH.
- L-0035 (1×, #614): Create and dispose a WeakReference-tracked object inside a returned [MethodImpl(NoInlining)] helper; the Debug JIT keeps locals rooted until the method returns.
- L-0036 (1×, #628): Before a concurrent E2E burst, warm the session with one serial query (bounded retry); tolerate only the precise cold-start transient (BAD_REQUEST sparkSession is null), bounded.
- L-0037 (1×, #626): Live-workspace (Skippable) E2E tests don't run in ordinary CI; add a mock-server assertion (HiveServer2.TestServer) for logic that depends on server behavior.
- L-0038 (1×, #662): MAP key order is unspecified: compare maps order-insensitively by sorting top-level keys only, since nested STRUCT field order matters.
- L-0039 (1×, #684): The workspace default catalog drifts (hive_metastore to main) and depends on multi-catalog mode; discover it with current_catalog() instead of hard-coding it in tests.
- L-0040 (1×, #684): To test catalog scoping, assert current_catalog() changed or not, against a target that differs from the default; don't rely on TABLE_OR_VIEW_NOT_FOUND.
- L-0041 (1×, #684): TestBase.NewConnection(testConfig) returns an untracked connection: wrap it in `using`. xUnit builds a new class per theory case, so cache invariants in static fields.
- L-0042 (2×, #764 #700): CI runs only csharp unit tests named AdbcDrivers.Databricks.Tests.Unit*, so put new tests there; a zero-match `dotnet test --filter` passes unless TreatNoTestsAsError=true.
- L-0043 (1×, #767): Skip.If/IfNot inside a test skips only under [SkippableFact]/[SkippableTheory]; under plain [Fact]/[Theory] it becomes a failure.
- L-0044 (1×, #689): A shared SetOption test that checks only throw/no-throw doesn't prove the option takes effect; assert it's honored (SEA validated but discarded QueryTimeoutSeconds).
- L-0045 (1×, #509): When replacing a reflection serializer with a hand-written one, add a round-trip test against the reflection output and a Debug.Assert in the switch's default arm.
- L-0046 (1×, #509): Utf8JsonWriter.WriteRawValue needs a valid JSON token; use WriteStringValue for quoted values such as times, and WriteRawValue only for raw numbers.

## Connections, caches and resources
- L-0047 (1×, #618): Cleanup RPCs in Dispose (e.g. CloseOperation) are best effort: record the error, never rethrow out of Dispose.
- L-0048 (1×, #656): Key a per-connect fetch lock per host, not process-wide, so cold connects to different hosts don't queue; dedup same-host fetches with a post-lock double check.
- L-0049 (1×, #656): A per-key lock map beside a cache must TryRemove its entry in the eviction callback (don't Dispose it: holders may still use it), or it grows forever.
- L-0050 (1×, #670): Warehouse ids are unique only within a workspace: key process-wide caches on (lowercased host, warehouseId).
- L-0051 (1×, #670): When a factory opens a resource that owns an HTTP client, dispose it if the open fails, on every open path, fallbacks included.
- L-0052 (1×, #670): If a transparent fallback also fails, throw an AggregateException with both errors, primary first for the unwrap; emit a telemetry event for every silent downgrade.
- L-0053 (1×, #670): Reyden/Lakehouse-RT warehouses reject Thrift OpenSession with SQLSTATE KP001: walk the whole exception chain (AggregateException too) and cache the rejection per host+warehouse with a TTL.
- L-0054 (1×, #689): Bound timeout options to what the consumer accepts: CancellationTokenSource(TimeSpan.FromSeconds(n)) needs total ms at most int.MaxValue.
- L-0055 (1×, #678): A strictly typed connection parameter read with a throwing GetXxxPropertyWithValidation, and settable by the connector service, needs [FeatureFlagType] or a bad server value breaks connects.
- L-0056 (1×, #706): On Thrift HTTP, a cancelled request can surface as TTransportException with Type=Interrupted rather than OperationCanceledException; handle both.
- L-0057 (1×, #706): One Thrift transport is shared per connection, so never abort an in-flight write early (partial bytes corrupt the next RPC); let it drain to the request timeout.
- L-0058 (1×, #706): A decorator that overrides every base method breaks silently when the base adds one; add a reflection test that every Write*Async on TProtocol is overridden.
- L-0059 (1×, #706): A cancellation guard before error telemetry swallows every shutdown error; swallow only the expected exception and still emit an event for the rest.
- L-0060 (1×, #701): Record statement telemetry success only after every post-processing step that can throw, or a later throw records both success and error.
- L-0061 (1×, #760): When racing a non-cancelable task against Task.Delay, cancel the delay's CTS on success and observe the abandoned task's fault on timeout.

## Result contract
- L-0062 (1×, #700): A flag that changes an existing column's Arrow wire type is a breaking change: default it off (opt-in), like the sibling flags and the ODBC/JDBC drivers.
- L-0063 (1×, #699): Geospatial: ISO WKB for a fixed SRID, EWKB for ANY-SRID (a known geoarrow.wkb deviation; GDAL rejects it). GEOGRAPHY allows only SRID 4326 or ANY; GEOMETRY any nonnegative SRID.

## Go driver
- L-0064 (1×, #686): arrow.Schema.Equal compares field names and metadata; to check per-row schema compatibility compare column count and types, and emit typed nulls.
- L-0065 (1×, #686): Spark SQL backtick identifiers escape a backtick by doubling it, never with a backslash; scope backslash escaping in SQL scanners to string literals.
- L-0066 (1×, #686): The Go driver ignores connector options on the URI/DSN path; apply connection fixes to both the option and the DSN key, and test that the DSN key takes effect.
- L-0067 (1×, #686): Arrow timestamps with an empty TimeZone (NTZ) format as UTC instants with RFC3339; decide and document how naive timestamps are treated, and test it.
- L-0068 (2×, #718 #716): The Go driver has two Arrow type dispatches (parameter_binding.go, bulk_ingest.go): add new type support to both, unit-tested warehouse-free with in-memory Arrow builders.
- L-0069 (1×, #716): Bind binary hex-encoded with UNHEX(?) placeholders, for every Arrow binary type. Databricks timestamps are microsecond (ns truncates); DECIMAL scale must be 0..precision.
- L-0070 (2×, #727 #716): Map SQLSTATE to ADBC status with an ErrorInspector and the Wrap* helpers, looping the error chain; DBExecutionError is private, so tests use a struct implementing its interface.
- L-0071 (1×, #727): Don't write tests that only re-enumerate a lookup map; test a few behaviors instead (mapped, unmapped default, wrapped unwrap).
- L-0072 (1×, #720): Go switch cases don't fall through, so an empty case is a no-op: write `case "warn", "warning":`. Don't bury behavior changes in chore or dependency PRs.
- L-0073 (1×, #753): Connection-URI parse errors must not echo the URI: use driverbase.SafeParseURL, never format user option values into messages, and test with sentinel secrets.

## Repo tooling
- L-0074 (1×, #709): Go CI workflows are generated by adbc-gen-workflow (DO NOT EDIT): fix pins upstream in adbc-drivers/dev, and after regenerating, diff for dropped customizations.
- L-0075 (1×, #723): In pixi.lock the digest in the PyPI URL path is BLAKE2b, not the sha256 field; regenerate with `pixi update <pkg>`, never copy it.
- L-0076 (1×, #767): zizmor ignores are anchored by file:line; after changing a workflow's line count, regenerate the anchors and verify offline (actionlint doesn't check them).

## Finding things in this repo [phase=author,followup]
- L-0077 (1×, log:2026-08-12): The csharp test project is csharp/test/AdbcDrivers.Databricks.Tests.csproj; glob csharp/test/*.csproj before running dotnet test.
- L-0078 (1×, log:2026-09-17): ADBC base classes come from the Apache.Arrow.Adbc NuGet pinned in csharp/Directory.Packages.props; read them from apache/arrow-adbc at the matching tag, not locally.
