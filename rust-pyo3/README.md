<!--
  Copyright (c) 2026 ADBC Drivers Contributors

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

# `databricks-adbc-pyo3` — POC Python bindings (DRAFT)

PyO3-based Python bindings for the Databricks ADBC Rust kernel. **This is a
proof of concept.** It exists to validate the multi-language strategy outlined
in the kernel-strategy design doc — primary Python binding via PyO3 + abi3
rather than via `adbc_driver_manager`. See the parent design doc for the full
context.

## Status

- ✅ End-to-end smoke against a live SEA warehouse (PAT auth, inline + CloudFetch results).
- ✅ Returns `pyarrow.Table` via Arrow C Data Interface (zero-copy where the schema permits).
- ✅ Performance at parity with or better than the existing Thrift-based path on most query sizes (see benchmark in the companion PR description).
- ❌ PAT-only — no OAuth M2M, U2M, Azure SP, or external credential providers yet.
- ❌ No metadata methods (`get_objects`, `get_table_schema`, `get_table_types`).
- ❌ No async execution, no `cancel()`, no Ctrl-C signal handling, no logging bridge into Python `logging`.
- ❌ No prepared statements / parameter binding from the Python side.
- ❌ No production tests, no CI integration, not packaged for PyPI.

## Surface

```python
import databricks_adbc_pyo3 as dbx

conn  = dbx.Connection(host, http_path, access_token, *, catalog=None, schema=None)
rs    = conn.execute(sql)            # → ResultSet
rs.num_columns(); rs.column_names(); rs.arrow_schema()
batch = rs.fetch_next_batch()        # → pyarrow.RecordBatch | None (streaming)
table = rs.fetch_all_arrow()         # → pyarrow.Table (drains the rest)
```

## Design notes

- Wraps the kernel's ADBC `Optionable` layer (string-keyed config) for now.
  The kernel-strategy design proposes typed Rust config structs
  (`DatabricksConfig`, `AuthConfig`, …) as a Phase 0a item; once that lands
  this binding should switch to typed construction.
- Calls a new `Statement::execute_owned` inherent method that returns
  `Box<dyn RecordBatchReader + Send + 'static>`, decoupling the reader's
  lifetime from the Statement so the binding can hold the reader past the
  Statement's drop.
- Releases the GIL during all Rust-side work (`execute_owned`, batch drain).

## Building

```bash
cd rust-pyo3
python3 -m venv .venv && source .venv/bin/activate
pip install 'maturin>=1.5,<2.0' pyarrow
maturin develop --release
```

Build emits an abi3-py39 wheel — one wheel covers all Python 3.9+ versions.

## Running the smoke tests

Set `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`, then:

```bash
python3 examples/e2e_smoke.py            # standalone PyO3 surface
```
