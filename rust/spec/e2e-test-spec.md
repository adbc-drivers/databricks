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

# E2E Test Specification — Rust ADBC Driver (CloudFetch)

This document captures connection parameters and execution instructions for
CloudFetch end-to-end testing of the Databricks Rust ADBC driver.
A separate agent reads this document to generate test implementations.

---

## Connection Parameters

Source: `/home/e.wang/databricks-driver-test/databricks-test-config.json`

| Config field | ADBC option key | Env var | Value |
|---|---|---|---|
| `uri` / `hostName` | `OptionDatabase::Uri` | `DATABRICKS_HOST` | `https://adb-6436897454825492.12.azuredatabricks.net` |
| `path` | `databricks.http_path` | `DATABRICKS_HTTP_PATH` | `/sql/1.0/warehouses/2f03dd43e35e2aa0` |
| `token` | `databricks.access_token` | `DATABRICKS_TOKEN` | *(read from config)* |

> **Note:** `uri` is the full warehouse URL. Pass only the host portion to the driver;
> `path` maps separately to `databricks.http_path`.

---

## CloudFetch Test Scenarios

### Large result sets
- **1M rows**: all rows received across multiple batches.
- **10M rows**: exercises the link-prefetch loop beyond the server's 32-link-per-response limit; all rows received.

### Fault injection (external harness only, via mitmproxy)

| Scenario | What to verify |
|---|---|
| Link expiry | Proxy returns 403 on presigned-URL download; driver refetches and retries |
| Download timeout | Proxy delays response past `chunk_ready_timeout_ms`; driver surfaces timeout error |
| Partial download | Proxy closes connection mid-stream; driver retries or fails cleanly |
| SEA 500 on chunk-link fetch | `GET /result/chunks` returns 500; driver retries up to `max_retries` then fails |
| Slow consumer | Consumer reads with deliberate delay; `chunks_in_memory` backpressure prevents unbounded memory use |

---

## How to Run

### Option 1: Native cargo tests

Set credentials in `rust/.cargo/config.toml` (gitignored):

```toml
[env]
DATABRICKS_HOST      = "https://adb-6436897454825492.12.azuredatabricks.net"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/2f03dd43e35e2aa0"
DATABRICKS_TOKEN     = "<pat-token>"
```

```bash
cargo test --test e2e -- --include-ignored
```

### Option 2: External harness (required for fault injection)

```bash
cd /home/e.wang/databricks-driver-test

# Test against local driver
DATABRICKS_DRIVER_PATH=/home/e.wang/databricks/rust \
DATABRICKS_TEST_CONFIG_FILE=./databricks-test-config.json \
  ./adbc_rust_tests.sh

# Filter by test name
DATABRICKS_DRIVER_PATH=/home/e.wang/databricks/rust \
DATABRICKS_TEST_CONFIG_FILE=./databricks-test-config.json \
  ./adbc_rust_tests.sh --filter "test_cloudfetch"

# Run a specific test binary
DATABRICKS_DRIVER_PATH=/home/e.wang/databricks/rust \
DATABRICKS_TEST_CONFIG_FILE=./databricks-test-config.json \
  ./adbc_rust_tests.sh --test cloud_fetch
```

> If `./driver` symlink exists from a previous run: `rm ./driver`

**How it works:** `DATABRICKS_DRIVER_PATH` causes the script to symlink `./driver →
/home/e.wang/databricks/rust` instead of cloning from GitHub, then runs
`cargo build --release` in `$DRIVER_DIR/rust`.

**All env vars:**

| Variable | Required | Description |
|---|---|---|
| `DATABRICKS_TEST_CONFIG_FILE` | **Yes** | Path to `databricks-test-config.json` |
| `DATABRICKS_DRIVER_PATH` | No | Local `rust/` dir; skips GitHub clone |
| `DATABRICKS_DRIVER_REPO` | No | Git URL (default: `https://github.com/adbc-drivers/databricks.git`) |
| `RUST_TEST_FILTER` | No | Test name substring filter |
| `PROXY_PORT` | No | mitmproxy port (default: `8080`) |
| `PROXY_CONTROL_PORT` | No | Proxy control API port (default: `18081`) |
