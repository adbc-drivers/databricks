<!--
  Copyright (c) 2025 ADBC Drivers Contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0
-->

# E2E Test Isolation Guidance

Guidance for writing and reviewing E2E tests that share a Databricks workspace. Prevents the flaky merge-queue failures tracked in PECO-3000.

## TL;DR

- The CI workspace is shared across parallel jobs and across PRs. Any tables your test writes to are visible — and writable — by other jobs.
- Tests must isolate **mutable** state to a per-run schema, and treat **fixture** tables as read-only.
- Never assert on metadata that depends on the entire schema (table counts, schema counts) — those reflect concurrent jobs' state.

## Why this matters

E2E tests run against a single shared Databricks workspace. Two sources of concurrency cause flake:

1. **Within one workflow run** — the `thrift` and `rest` matrix jobs run in parallel and hit the same workspace.
2. **Across PRs** — every merge-queue Tests Workflow on `gh-readonly-queue/main/pr-*` runs against the same workspace at the same time.

Assembly-level serialization (`csharp/test/AssemblyInfo.cs`: `[assembly: CollectionBehavior(DisableTestParallelization = true)]`) prevents races *inside* one job. It cannot prevent races *between* jobs.

Observed failure patterns:

| Symptom | Mechanism |
|---|---|
| `parsed records (13) differ from expected (12)` | Job A finishes inserting 13 rows; Job B reads before Job A's `DELETE` drops it to 12. |
| `DELTA_METADATA_CHANGED: ... CREATE OR REPLACE TABLE` conflict | Job A reads while Job B's `CREATE OR REPLACE TABLE` rewrites the Delta log. |
| `GetTables count: 221 vs 220` | Two `GetTables` calls separated by milliseconds; another job creates/drops a table in the schema in between. |

## Schema model

Two schemas with different roles:

| Schema | Role | Lifecycle |
|---|---|---|
| `main.adbc_testing` | Stable read-only fixture tables (`all_column_types`, `cross_ref_customers`, `cross_ref_orders`, `fk_test_ref_table`, …) | Hand-managed in the workspace. Tests **read** from it; never write. |
| `main.adbc_testing_run_${run_id}_${attempt}_${proto}` | Per-run mutable state (`adbc_testing_table` and any other table the test creates) | `CREATE SCHEMA IF NOT EXISTS` at job start; `DROP SCHEMA … CASCADE` at job end (with `if: always()`). |

Tests get the writable schema name via `TestConfiguration.Metadata.Schema` (CI substitutes the per-run schema into `connection.json`). Fixture lookups use the constant `DatabricksTestEnvironment.FixtureSchema`.

> Historical note: `main.adbc_testing` was originally the only schema and held both fixtures and mutable state, which caused the cross-job races tracked by PECO-3000. After this refactor, only fixtures remain there; all mutation moved to the per-run schema. The fixture schema's name was kept as-is to avoid a workspace-rename operation.

## Rules for engineers

### Writing tests

- **Don't hard-code `"adbc_testing"` or `"adbc_testing_table"` as string literals.** Use `TestConfiguration.Metadata.Schema` (writable) or `DatabricksTestEnvironment.FixtureSchema` (read-only).
- **Any `CREATE`, `DROP`, `INSERT`, `UPDATE`, `DELETE` must target the per-run schema.** Any DML on `DatabricksTestEnvironment.FixtureSchema` is a bug.
- **Cleanup must be idempotent.** Prefer `DROP TABLE IF EXISTS` over `DROP TABLE`. The schema-level `DROP SCHEMA … CASCADE` runs automatically; per-test cleanup is for fast-feedback only.
- **Avoid schema-wide assertions.** Don't compare `GetTables` counts across protocols, don't assert on total row counts in shared fixtures, don't compare schema lists. These reflect every concurrent job's state.

### Assertions to prefer

```csharp
// BAD — depends on the entire schema:
Assert.Equal(thriftRows.Count, seaRows.Count);
Assert.Equal(12, allRows.Count);

// GOOD — scoped to the test's own table:
Assert.Contains(rows, r => r["TABLE_NAME"] == myTestTable);
Assert.Equal(12, myTestTableRowCount);  // OK because myTestTable is per-run-unique
```

### Adding a new test class

- If it writes anything: route through `TestConfiguration.Metadata.Schema`.
- If it only reads fixtures: use `DatabricksTestEnvironment.FixtureSchema`.
- Keep `[assembly: CollectionBehavior(DisableTestParallelization = true)]` in place. New test assemblies that hit the workspace need the same attribute.

### Adding a new fixture

- Don't create it lazily inside a test. Add it to the hand-managed fixture schema (`DatabricksTestEnvironment.FixtureSchema` → `main.adbc_testing`) and document the row counts and structure.
- Don't put new fixtures in the per-run schema "for safety" — that's wasted setup time on every job.

## Code review checklist

When reviewing PRs that touch E2E tests, the workspace setup, or CI workflows:

### Schema and table naming
- [ ] No new hard-coded `"adbc_testing"` or `"adbc_testing_table"` literals in test code (use `DatabricksTestEnvironment.FixtureSchema` or `TestConfiguration.Metadata.{Schema,Table}`).
- [ ] New tests that mutate state use `TestConfiguration.Metadata.Schema`.
- [ ] New SQL files use `{ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE}` placeholders, not literal names.

### Setup and teardown
- [ ] Tests that `CREATE` a table also `DROP IF EXISTS` it, or rely on the per-run schema cleanup.
- [ ] No DML against the fixture schema.
- [ ] No assumption that a previous test left state behind — each test owns its setup.

### Assertions
- [ ] No `Assert.Equal(thriftCount, seaCount)` or other cross-schema-wide equality assertions.
- [ ] Row-count assertions are scoped to a per-run-unique table.
- [ ] Metadata assertions (`GetTables`, `GetSchemas`) use `Assert.Contains` for the specific entity, not equality on the full list.

### CI workflow changes
- [ ] `.github/workflows/e2e-tests.yml` schema/table fields stay templated from `${{ github.run_id }}` / `${{ matrix.protocol }}` / `${{ github.run_attempt }}`.
- [ ] Cleanup steps use `if: always()` so they run on test failure too.
- [ ] New matrix dimensions are reflected in the schema suffix to keep parallel jobs isolated.

### Telemetry tests
- [ ] Tests asserting on telemetry payload values (operation type, statement count) only count operations they themselves triggered — not catalog/schema-wide totals — because metadata operations from concurrent jobs are not visible in this driver's telemetry but related counts (e.g., schema sizes) can still drift.

## References

- PECO-3000 — Flaky CI: Tests Workflow merge-queue E2E tests fail intermittently due to test-workspace data drift
- `csharp/test/AssemblyInfo.cs` — assembly-level parallelization disable
- `csharp/test/E2E/DatabricksTestEnvironment.cs` — `FixtureSchema` constant
- `csharp/test/Resources/Databricks.sql` — shared setup script (templated against the per-run schema)
- `.github/workflows/e2e-tests.yml` — CI matrix and per-run schema lifecycle
