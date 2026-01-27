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

# Databricks ADBC Driver for Rust

A Rust implementation of an ADBC (Arrow Database Connectivity) driver for Databricks SQL endpoints.

## Project Structure

```
rust/
├── Cargo.toml                  # Package configuration and dependencies
├── Makefile                    # Build automation targets
├── src/
│   ├── lib.rs                  # Library root with public exports
│   ├── error.rs                # Error types for the driver
│   ├── driver.rs               # ADBC Driver entry point
│   ├── database.rs             # Database configuration and connection factory
│   ├── connection.rs           # Connection to Databricks SQL endpoint
│   ├── statement.rs            # SQL statement execution
│   │
│   ├── auth/                   # Authentication mechanisms
│   │   ├── mod.rs              # AuthProvider trait definition
│   │   ├── pat.rs              # Personal Access Token authentication
│   │   └── oauth.rs            # OAuth 2.0 client credentials flow
│   │
│   ├── client/                 # HTTP client layer
│   │   ├── mod.rs              # Module exports
│   │   └── http.rs             # HTTP client with retry and timeout config
│   │
│   ├── reader/                 # Result readers
│   │   ├── mod.rs              # Module exports
│   │   └── cloudfetch.rs       # CloudFetch for high-performance result downloads
│   │
│   ├── result/                 # Result set handling
│   │   └── mod.rs              # ResultSet with Arrow RecordBatch support
│   │
│   └── telemetry/              # Metrics and telemetry
│       └── mod.rs              # TelemetryCollector for driver metrics
│
└── tests/
    └── integration.rs          # Integration tests
```

## Module Overview

| Module | Description |
|--------|-------------|
| `driver` | Entry point for creating database instances |
| `database` | Holds connection configuration (host, HTTP path, catalog, schema) |
| `connection` | Active connection to a Databricks SQL endpoint |
| `statement` | Executes SQL queries and returns results |
| `auth` | Authentication providers (PAT, OAuth 2.0) |
| `client` | HTTP client for communicating with Databricks APIs |
| `reader` | Result readers including CloudFetch for cloud storage downloads |
| `result` | Result set abstraction over Arrow RecordBatches |
| `telemetry` | Driver metrics and performance tracking |

## Building

```bash
cargo build
```

## Testing

```bash
cargo test
```

## Development

Format code:
```bash
cargo fmt
```

Run linter:
```bash
cargo clippy -- -D warnings
```

Run all checks (format, lint, test):
```bash
make check
```

## Dependencies

- `arrow` - Apache Arrow for columnar data handling
- `thiserror` - Ergonomic error type definitions
- `tracing` - Structured logging and diagnostics
