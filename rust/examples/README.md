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

# Examples

Integration examples that run queries against a live Databricks SQL warehouse.

## Setup

Credentials are configured in `rust/.cargo/config.toml` (gitignored). Set the
following environment variables there:

```toml
[env]
DATABRICKS_HOST = "https://your-workspace.cloud.databricks.com"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/<warehouse-id>"
DATABRICKS_TOKEN = "<your-pat-token>"
```

## Running

```bash
cargo run --example <example_name>
```

Most examples initialize `tracing_subscriber` with `DEBUG` level, so you will
see detailed debug logs by default. To filter logs, set `RUST_LOG` in
`.cargo/config.toml`:

```toml
RUST_LOG = "databricks_adbc=debug"
```

## Logging

The `logging_test` example demonstrates the driver's built-in logging support.
Instead of manually initializing `tracing_subscriber`, you can configure logging
via ADBC database options:

- `databricks.log_level` — `off`, `error`, `warn`, `info`, `debug`, `trace`
- `databricks.log_file` — file path (logs go to stderr if unset)

```bash
cargo run --example logging_test
```

Alternatively, use the `RUST_LOG` environment variable (takes effect when no
`databricks.log_level` option is set):

```bash
RUST_LOG=databricks_adbc=debug cargo run --example logging_test
```
