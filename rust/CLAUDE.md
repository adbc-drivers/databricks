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

# CLAUDE.md - LLM Development Guide

This document provides context for LLMs (and developers) working on the Databricks ADBC Rust driver.

## Project Overview

This is a Rust implementation of an ADBC (Arrow Database Connectivity) driver for Databricks SQL endpoints. The driver enables high-performance data access using Apache Arrow as the data format.

**Sister implementations exist in:**
- Go: `../go/` - Production driver with full feature set
- C#: `../csharp/` - Production driver with full feature set

When implementing features, reference these drivers for behavior and API design.

## Build Commands

```bash
cargo build           # Build the library
cargo test            # Run all tests
cargo fmt             # Format code
cargo clippy -- -D warnings  # Lint with warnings as errors
```

## Pre-commit Checklist

Before committing or pushing, always run these checks:
```bash
cargo fmt             # Format code (CI will reject unformatted code)
cargo clippy -- -D warnings  # Lint
cargo test            # Run all tests
```

## Pull Requests

When creating PRs, always target the upstream repository `adbc-drivers/databricks`, not the personal fork. Use:
```bash
gh pr create --repo adbc-drivers/databricks
```

## Architecture

### Core ADBC Flow

```
Driver -> Database -> Connection -> Statement -> ResultSet
```

1. **Driver** (`driver.rs`): Entry point, creates Database instances
2. **Database** (`database.rs`): Holds configuration (host, http_path, credentials), creates Connections
3. **Connection** (`connection.rs`): Active session with Databricks, creates Statements
4. **Statement** (`statement.rs`): Executes SQL, returns ResultSet
5. **ResultSet** (`result/mod.rs`): Iterator over Arrow RecordBatches

### Module Responsibilities

| Module | Purpose |
|--------|---------|
| `auth/` | Authentication (PAT, OAuth) |
| `client/` | HTTP communication with Databricks |
| `reader/` | Result fetching (CloudFetch) |
| `result/` | Result set abstraction |
| `telemetry/` | Metrics collection |

Reference the Go (`../go/`) and C# (`../csharp/`) drivers for implementation patterns.

## Key Types

### Error Handling

The driver uses the `driverbase` error framework for consistent error handling.

**Internal code** (within modules like `auth/`, `client/`, `reader/`) uses `crate::Result<T>`:
```rust
use crate::error::{DatabricksErrorHelper, Result};
use driverbase::error::ErrorHelper;

// Create errors using the helper
Err(DatabricksErrorHelper::invalid_argument().message("invalid host URL"))
Err(DatabricksErrorHelper::io().message("connection refused").context("connect to server"))
Err(DatabricksErrorHelper::not_implemented().message("feature_name"))
```

**ADBC trait implementations** (in `connection.rs`, `database.rs`, `statement.rs`) must return `adbc_core::error::Result`:
```rust
use adbc_core::error::Result;

// Convert to ADBC error using .to_adbc()
Err(DatabricksErrorHelper::not_implemented().message("get_objects").to_adbc())
```

### Authentication

Implement the `AuthProvider` trait:

```rust
pub trait AuthProvider: Send + Sync + Debug {
    fn get_auth_header(&self) -> Result<String>;
}
```

- `PersonalAccessToken`: Returns `Bearer {token}` - fully implemented
- `OAuthCredentials`: Needs token fetch/refresh logic - stub only

### Arrow Integration

Results use Apache Arrow types:

```rust
use arrow_schema::Schema;
use arrow_array::RecordBatch;
```

## Implementation Status

### Implemented (working)
- [x] Project structure and module organization
- [x] Error types
- [x] Driver/Database/Connection/Statement scaffolding
- [x] PAT authentication
- [x] CloudFetch configuration types
- [x] ResultSet with RecordBatch iteration
- [x] Telemetry collector structure

### Not Yet Implemented (stubs)
- [ ] OAuth token fetching and refresh
- [ ] HTTP client actual requests (reqwest/hyper integration)
- [ ] Statement execution (SQL API calls)
- [ ] CloudFetch downloads from cloud storage
- [ ] Arrow IPC parsing from CloudFetch responses
- [ ] Connection session management
- [ ] Telemetry reporting

## Coding Conventions

### File Headers

All files must have Apache 2.0 license headers:

```rust
// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// ...
```

### Documentation

- Add `//!` module-level docs at the top of each file
- Add `///` doc comments to all public types and functions
- Include `# Example` sections for complex APIs

### Testing

- Unit tests go in `#[cfg(test)] mod tests { }` at the bottom of each file
- Integration tests go in `tests/` directory
- Test names: `test_<function>_<scenario>`

### Error Handling

- Use `driverbase::error::ErrorHelper` for creating errors
- Use `DatabricksErrorHelper` methods: `invalid_argument()`, `io()`, `not_implemented()`, `invalid_state()`, etc.
- Chain `.message("...")` and `.context("...")` for details
- Use `.to_adbc()` when returning from ADBC trait methods
- Propagate errors with `?` operator

### Async Considerations

The driver is currently synchronous. When adding async:
- Use `tokio` as the async runtime
- Provide both sync and async APIs where possible
- Use `async-trait` for async trait methods

## Adding New Features

### 1. Adding a new authentication method

1. Create `src/auth/<method>.rs`
2. Implement `AuthProvider` trait
3. Add `pub mod <method>;` to `src/auth/mod.rs`
4. Add `pub use <method>::<Type>;` to `src/auth/mod.rs`
5. Add tests in the new file

### 2. Adding HTTP functionality

1. Add dependency to `Cargo.toml` (e.g., `reqwest`)
2. Implement in `src/client/http.rs`
3. Reference `../csharp/src/Http/` for retry logic, error handling

### 3. Implementing CloudFetch

Reference implementations:
- C#: `../csharp/src/Reader/CloudFetchReader.cs`
- Go: `../go/ipc_reader_adapter.go`

Key steps:
1. Parse CloudFetch links from statement response
2. Download Arrow IPC files from presigned URLs
3. Parse IPC files into RecordBatches
4. Handle parallel downloads and prefetching

### 4. Implementing Statement Execution

Key steps:
1. POST to SQL statements endpoint
2. Poll for completion or use async mode
3. Parse response for result links (CloudFetch) or inline data
4. Return ResultSet

Reference the [Databricks SQL Statement Execution API documentation](https://docs.databricks.com/api/workspace/statementexecution) for endpoint details and request/response formats.

## Testing Against Databricks

Environment variables for E2E tests:

```bash
export DATABRICKS_HOST="https://your-workspace.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/abc123"
export DATABRICKS_TOKEN="your-pat-token"
```

## Common Patterns

### Builder Pattern (used throughout)

```rust
let database = Database::new()
    .with_host("https://example.databricks.com")
    .with_http_path("/sql/1.0/warehouses/abc123")
    .with_catalog("main")
    .with_schema("default");
```

### Configuration Structs

```rust
#[derive(Debug, Clone)]
pub struct SomeConfig {
    pub field: Type,
}

impl Default for SomeConfig {
    fn default() -> Self {
        Self {
            field: sensible_default,
        }
    }
}
```

### Trait Objects for Extensibility

```rust
pub struct Client {
    auth_provider: Option<Arc<dyn AuthProvider>>,
}
```

## Dependencies to Consider Adding

When implementing features, consider these crates:

| Crate | Purpose |
|-------|---------|
| `reqwest` | HTTP client with async support |
| `tokio` | Async runtime |
| `serde` / `serde_json` | JSON serialization |
| `url` | URL parsing |
| `base64` | Base64 encoding |
| `chrono` | Date/time handling |
| `uuid` | UUID generation |
| `async-trait` | Async trait methods |

## Useful References

- [ADBC Specification](https://arrow.apache.org/adbc/)
- [Databricks SQL Statement API](https://docs.databricks.com/api/workspace/statementexecution)
- [Apache Arrow Rust](https://docs.rs/arrow/latest/arrow/)
- Go driver: `../go/`
- C# driver: `../csharp/`
