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
