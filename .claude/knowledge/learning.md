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

### 2026-07-08: metadata (Thrift vs SEA) — empty-string identifier arg must map to empty result, not an exception

**Issue:** #524 — passing an empty-string identifier (catalog="", schema="", table="", foreign_schema="", foreign_table="") to a metadata op threw `HiveServer2Exception` on the Thrift path but returned an empty `QueryResult` on SEA (1,763 outcome diffs across list_columns/list_tables/get_primary_keys/get_cross_reference).
**Root Cause:** `HiveServer2Statement.SetOption` stores the empty string verbatim into `CatalogName`/`SchemaName`/`TableName`/`Foreign*` and the Thrift metadata RPCs pass it straight through; the server rejects `""` (e.g. `ListTableSummaries` "name \"\" is not a valid name", GetPrimaryKeys "Can not create a Path from an empty string"). SEA already short-circuited via its `string.IsNullOrEmpty(...)` checks. The existing Thrift guards only covered `catalog != null` (multi-catalog) and PK/FK invalid-catalog — none caught a non-null empty string on schema/table/foreign args. `HandleSparkCatalog("")` returns `""` (only "SPARK"→null), so empty catalog also slipped through.
**Fix:** Added `MetadataUtilities.HasEmptyStringIdentifier(params string?[])` (true only for a non-null zero-length string; null = "no filter" untouched) and guarded the Databricks Thrift overrides in `DatabricksStatement.cs` (GetTables/GetColumns/GetPrimaryKeys/GetCrossReference/GetCrossReferenceAsForeignTable) to return the empty result *before* calling `base.*`, mirroring SEA. Note: on Thrift, GetColumns with catalog=""/schema="" already returned empty for some param combos (SHOW COLUMNS tolerates it) — only GetTables/PK/FK actually threw — but guarding all ops uniformly is the parity-correct behavior.
**Rule:** Normalize empty-string metadata identifier args to an empty result set up front in the editable Databricks overrides — never let `""` reach a base HiveServer2 RPC (it throws); null means "no filter" and must pass through.
