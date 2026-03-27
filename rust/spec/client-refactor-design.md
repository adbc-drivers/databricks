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

# DatabricksClient Trait Refactor Design

## Overview

This document proposes refactoring `DatabricksClient.execute_statement` to absorb polling and result reader creation internally, returning `Box<dyn ResultReader + Send>` instead of the raw `ExecuteResponse`. This simplifies all consumers (Statement, future metadata) and positions the trait to support both SEA and Thrift backends cleanly.

## Motivation

Today the execution flow spreads across three layers:

```
SeaClient.execute_statement()       → ExecuteResponse (raw API response)
Statement.wait_for_completion()     → ExecuteResponse (polls until done)
ResultReaderFactory.create_reader() → Box<dyn ResultReader> (picks CloudFetch/inline/empty)
```

This causes two problems:

1. **Any consumer of `execute_statement` must re-implement polling + reader creation.** The planned metadata implementation (`Connection.get_objects`, etc.) would need the same polling and reader logic that `Statement` already has.

2. **`ExecuteResponse` is a protocol-level detail leaking to consumers.** A future Thrift backend wouldn't produce `ExecuteResponse` at all — it returns results through a completely different mechanism. Consumers shouldn't need to know.

## Proposed Change

Move polling and reader creation into the client. `execute_statement` returns a ready-to-read `ResultReader`:

```
SeaClient.execute_statement()
  → internally: API call → poll → ResultReaderFactory → Box<dyn ResultReader>
```

### Before

```rust
#[async_trait]
pub trait DatabricksClient: Send + Sync + Debug {
    async fn execute_statement(
        &self, session_id: &str, sql: &str, params: &ExecuteParams,
    ) -> Result<ExecuteResponse>;

    async fn get_statement_status(&self, statement_id: &str) -> Result<ExecuteResponse>;

    async fn get_result_chunks(...) -> Result<ChunkLinkFetchResult>;

    async fn cancel_statement(&self, statement_id: &str) -> Result<()>;
    async fn close_statement(&self, statement_id: &str) -> Result<()>;

    // session methods...
}
```

### After

```rust
#[async_trait]
pub trait DatabricksClient: Send + Sync + Debug {
    /// Execute SQL and return a reader over the results.
    /// Handles polling, result format detection, and reader creation internally.
    async fn execute_statement(
        &self,
        session_id: &str,
        sql: &str,
        params: &ExecuteParams,
    ) -> Result<ExecuteResult>;

    /// Fetch chunk links for CloudFetch streaming.
    /// Used by SeaChunkLinkFetcher to fetch additional result chunks during streaming.
    async fn get_result_chunks(
        &self,
        statement_id: &str,
        chunk_index: i64,
        row_offset: i64,
    ) -> Result<ChunkLinkFetchResult>;

    /// Cancel a running statement.
    async fn cancel_statement(&self, statement_id: &str) -> Result<()>;

    /// Close/cleanup a statement (release server resources).
    async fn close_statement(&self, statement_id: &str) -> Result<()>;

    // session methods stay the same...
    async fn create_session(...) -> Result<SessionInfo>;
    async fn delete_session(&self, session_id: &str) -> Result<()>;
}

/// Result from execute_statement. Contains the statement ID (for cancellation/cleanup)
/// and a reader over the result data.
pub struct ExecuteResult {
    pub statement_id: String,
    pub reader: Box<dyn ResultReader + Send>,
}
```

**Key changes:**
- `execute_statement` returns `ExecuteResult` (statement_id + reader) instead of `ExecuteResponse`
- `get_statement_status` removed from the trait (polling is internal to SeaClient)
- `get_result_chunks` stays on the trait (needed by `SeaChunkLinkFetcher` which holds `Arc<dyn DatabricksClient>`)
- `ExecuteResponse` becomes an internal type inside the SEA client module

### Why `ExecuteResult` instead of just `Box<dyn ResultReader>`?

`Statement` needs the `statement_id` to support:
- `cancel()` — calls `client.cancel_statement(statement_id)`
- `Drop` — calls `client.close_statement(statement_id)`

If we only returned the reader, we'd lose the ability to cancel or clean up.

## Impact Analysis

### What Changes

#### 1. `client/mod.rs` — Trait definition

- `ExecuteResponse`, `ExecuteResultData` move out of the public API
- New `ExecuteResult` struct added
- `get_statement_status` removed from trait (polling is internal)
- `get_result_chunks` and `ChunkLinkFetchResult` stay on the trait (needed by `SeaChunkLinkFetcher`)

```rust
// Removed from public trait:
// - get_statement_status (internal to polling)

// Stays on trait:
// - get_result_chunks (used by SeaChunkLinkFetcher via Arc<dyn DatabricksClient>)

// New:
pub struct ExecuteResult {
    pub statement_id: String,
    pub reader: Box<dyn ResultReader + Send>,
}
```

#### 2. `client/sea.rs` — SeaClient implementation

The biggest change. SeaClient absorbs:
- **Polling logic** (currently in `Statement.wait_for_completion`)
- **Reader creation** (currently in `ResultReaderFactory.create_reader`)

```rust
impl DatabricksClient for SeaClient {
    async fn execute_statement(
        &self,
        session_id: &str,
        sql: &str,
        params: &ExecuteParams,
    ) -> Result<ExecuteResult> {
        // 1. Call SEA API
        let response = self.call_execute_api(session_id, sql, params).await?;

        // 2. Poll until completion (moved from Statement)
        let response = self.wait_for_completion(response).await?;

        // 3. Create appropriate reader (moved from ResultReaderFactory)
        let reader = self.reader_factory.create_reader(
            &response.statement_id,
            &response,
        )?;

        Ok(ExecuteResult {
            statement_id: response.statement_id,
            reader,
        })
    }
}
```

SeaClient now needs access to `ResultReaderFactory` (or its dependencies: `http_client`, `CloudFetchConfig`, `runtime_handle`). These are injected at construction time.

`get_statement_status` becomes a private method on SeaClient (only needed internally for polling). `get_result_chunks` stays on the `DatabricksClient` trait since `SeaChunkLinkFetcher` holds `Arc<dyn DatabricksClient>` and calls it during CloudFetch streaming.

#### 3. `statement.rs` — Statement simplification

Statement becomes a thin ADBC wrapper:

```rust
impl adbc_core::Statement for Statement {
    fn execute(&mut self) -> Result<impl RecordBatchReader + Send> {
        let query = self.query.as_ref().ok_or_else(|| {
            DatabricksErrorHelper::invalid_state().message("No query set").to_adbc()
        })?;

        let result = self.runtime_handle
            .block_on(self.client.execute_statement(
                &self.session_id, query, &ExecuteParams::default(),
            ))
            .map_err(|e| e.to_adbc())?;

        self.current_statement_id = Some(result.statement_id);

        ResultReaderAdapter::new(result.reader).map_err(|e| e.to_adbc())
    }
}
```

**Removed from Statement:**
- `wait_for_completion()` — moved into SeaClient
- `reader_factory` field — no longer needed
- `poll_timeout` / `poll_interval` fields — move to client config

#### 4. `reader/mod.rs` — ResultReaderFactory

`ResultReaderFactory` stays as-is internally but is no longer used by `Statement`. It's used by `SeaClient` instead. Its public API (`create_reader`) still takes an `ExecuteResponse`, but that's now an internal type within the client module.

`ResultReader` trait, `ResultReaderAdapter`, and all reader implementations (`CloudFetchResultReader`, `InlineArrowReader`, `EmptyReader`) stay unchanged.

#### 5. `connection.rs` — Connection

Connection no longer needs to create `ResultReaderFactory` for Statement. It simplifies:

```rust
fn new_statement(&mut self) -> Result<Self::StatementType> {
    Ok(Statement::new(
        self.client.clone(),
        self.session_id.clone(),
        self.runtime.handle().clone(),
    ))
}
```

`http_client` and `cloudfetch_config` move to client construction (in Database).

#### 6. `database.rs` — Database

Database creates `SeaClient` with the reader factory dependencies. The `Arc<dyn DatabricksClient>` is passed into the `ResultReaderFactory` so it can be forwarded to `SeaChunkLinkFetcher` for CloudFetch:

```rust
let client: Arc<dyn DatabricksClient> = Arc::new(SeaClient::new(
    http_client.clone(),
    host,
    warehouse_id,
    client_config,  // NEW: DatabricksClientConfig with polling + CloudFetch config
));

// ResultReaderFactory gets the Arc<dyn DatabricksClient> for CloudFetch link fetching
let reader_factory = ResultReaderFactory::new(
    client.clone(),
    http_client,
    client_config.cloudfetch_config,
    runtime_handle,
);
// reader_factory is passed into SeaClient (or stored alongside it)
```

### What Doesn't Change

- `reader/cloudfetch/` — Link fetcher, streaming provider, and chunk downloader stay the same
- `SeaChunkLinkFetcher` — still holds `Arc<dyn DatabricksClient>`, calls `get_result_chunks` on it
- `ResultReader` trait — unchanged
- `ResultReaderAdapter` — unchanged
- `types/sea.rs` — unchanged (these are SEA-specific API types)
- `auth/` — unchanged
- `error.rs` — unchanged

### CloudFetch Link Fetcher — Unchanged

`SeaChunkLinkFetcher` continues to hold `Arc<dyn DatabricksClient>` and call `get_result_chunks` on it, exactly as it does today. `get_result_chunks` stays on the `DatabricksClient` trait.

**Why keep it on the trait?** Removing `get_result_chunks` from the trait and putting it only on `SeaClient` would require `SeaChunkLinkFetcher` to hold `Arc<SeaClient>` (the concrete type). But `SeaClient.execute_statement(&self)` creates the link fetcher internally and only has `&self`, not `Arc<SeaClient>`. Getting an `Arc<Self>` from `&self` in Rust requires either `Arc::new_cyclic` with `Weak<Self>`, two-phase initialization, or other ownership gymnastics. Keeping `get_result_chunks` on the trait avoids all of this — `ResultReaderFactory` already receives `Arc<dyn DatabricksClient>` at construction time and passes it to `SeaChunkLinkFetcher`.

The existing `ChunkLinkFetcher` trait remains the public interface consumed by `StreamingCloudFetchProvider` — unchanged:

```rust
#[async_trait]
pub trait ChunkLinkFetcher: Send + Sync {
    async fn fetch_links(&self, start_chunk_index: i64, start_row_offset: i64) -> Result<ChunkLinkFetchResult>;
    async fn refetch_link(&self, chunk_index: i64, row_offset: i64) -> Result<CloudFetchLink>;
}
```

The reader assembly within `SeaClient.execute_statement()` becomes:

```
SeaClient.execute_statement()
  → poll to completion
  → ResultReaderFactory.create_reader()
      → create_cloudfetch_reader()
          → SeaChunkLinkFetcher::new(Arc<dyn DatabricksClient>, statement_id, ...)
          → StreamingCloudFetchProvider::new(link_fetcher, ...)
          → CloudFetchResultReader
```

## How This Enables Metadata

With this refactor, the metadata design becomes straightforward. Metadata methods are added to `DatabricksClient`:

```rust
#[async_trait]
pub trait DatabricksClient: Send + Sync + Debug {
    async fn execute_statement(...) -> Result<ExecuteResult>;
    async fn cancel_statement(...) -> Result<()>;
    async fn close_statement(...) -> Result<()>;

    // Session
    async fn create_session(...) -> Result<SessionInfo>;
    async fn delete_session(...) -> Result<()>;

    // Metadata
    async fn list_catalogs(&self, session_id: &str) -> Result<ExecuteResult>;
    async fn list_schemas(
        &self, session_id: &str, catalog: Option<&str>,
        schema_pattern: Option<&str>,
    ) -> Result<ExecuteResult>;
    async fn list_tables(
        &self, session_id: &str, catalog: Option<&str>,
        schema_pattern: Option<&str>, table_pattern: Option<&str>,
        table_types: Option<&[&str]>,
    ) -> Result<ExecuteResult>;
    async fn list_columns(
        &self, session_id: &str, catalog: &str,
        schema_pattern: Option<&str>, table_pattern: Option<&str>,
        column_pattern: Option<&str>,
    ) -> Result<ExecuteResult>;
    async fn list_primary_keys(
        &self, session_id: &str, catalog: &str, schema: &str, table: &str,
    ) -> Result<ExecuteResult>;
    async fn list_foreign_keys(
        &self, session_id: &str, catalog: &str, schema: &str, table: &str,
    ) -> Result<ExecuteResult>;
    async fn list_table_types(&self, session_id: &str) -> Result<ExecuteResult>;
}
```

**SEA implementation:** Each metadata method builds a `SHOW` SQL command and calls `self.execute_statement()` internally. Zero duplication.

**Future Thrift implementation:** Uses native Thrift RPCs (`TGetCatalogsReq`, etc.) and wraps results in a `ResultReader`. No SQL needed.

**Connection:** Calls `client.list_catalogs()`, iterates the reader, parses into structs, builds the ADBC-spec Arrow response. Backend-agnostic.

## Configuration Changes

Polling and reader config moves from Statement/Connection to client construction. The config is backend-agnostic since both SEA and Thrift need polling and CloudFetch:

```rust
pub struct DatabricksClientConfig {
    pub poll_timeout: Duration,          // Default: 600s (moved from Statement)
    pub poll_interval: Duration,         // Default: 500ms (moved from Statement)
    pub cloudfetch_config: CloudFetchConfig,  // Moved from Connection
}
```

## Implementation Plan

### Phase 1: Move polling into SeaClient

1. Move `Statement.wait_for_completion()` logic into `SeaClient` as a private `async fn wait_for_completion()`
2. Make `get_statement_status` a private method on SeaClient
3. `execute_statement` now polls internally and returns completed `ExecuteResponse` (still internal type)
4. Statement stops polling, relies on client
5. Move `poll_timeout` / `poll_interval` from Statement to `SeaClientConfig`

### Phase 2: Move reader creation into SeaClient

1. Move `ResultReaderFactory` into SeaClient (injected at construction)
2. `SeaChunkLinkFetcher` keeps `Arc<dyn DatabricksClient>` — no change to link fetcher
3. `get_result_chunks` stays on `DatabricksClient` trait — avoids `Arc<Self>` ownership complexity
4. `execute_statement` returns `ExecuteResult` (statement_id + reader)
5. Remove `reader_factory` from Statement
6. Simplify Connection (no longer passes http_client/cloudfetch_config to Statement)

Metadata methods on `DatabricksClient` are covered in the [connection metadata design](rust/spec/connection-metadata-design.md) and will be implemented separately after this refactor lands.

## Alternatives Considered

### Keep execute_statement as-is, add a separate MetadataClient trait

**Pros:** No refactor of existing code
**Cons:** Metadata implementation duplicates polling/reader logic, or awkwardly depends on Statement

**Decision:** Refactor is worth it — it simplifies the architecture and makes the trait usable for both SQL execution and metadata.

### Return Box<dyn RecordBatchReader> instead of ExecuteResult

**Pros:** Standard Arrow interface
**Cons:** Loses statement_id needed for cancel/close; `RecordBatchReader` is the ADBC-facing interface while `ResultReader` is the internal one that `ResultReaderAdapter` wraps

**Decision:** Return `ExecuteResult` with both statement_id and `ResultReader`. Statement wraps in `ResultReaderAdapter` for ADBC.

### Remove get_result_chunks from trait, use Arc<SeaClient> directly

**Pros:** Smaller `DatabricksClient` trait; `SeaChunkLinkFetcher` holds concrete `Arc<SeaClient>` instead of trait object
**Cons:** `SeaClient.execute_statement(&self)` needs `Arc<SeaClient>` to pass to the link fetcher, but only has `&self`. Requires `Arc::new_cyclic` with `Weak<Self>` or two-phase initialization — adds real Rust ownership complexity for a minor API cleanliness win.

**Decision:** Keep `get_result_chunks` on the trait. The trait is slightly larger but the implementation stays simple — `ResultReaderFactory` already receives `Arc<dyn DatabricksClient>` and passes it through to `SeaChunkLinkFetcher`.

### Return Vec<RecordBatch> (fully materialized)

**Pros:** Simpler interface
**Cons:** Breaks CloudFetch streaming for large results; forces full materialization in memory

**Decision:** Keep lazy `ResultReader` for streaming. Metadata results are small and can be collected by the caller.

## References

- [Current DatabricksClient trait](rust/src/client/mod.rs)
- [Current Statement implementation](rust/src/statement.rs)
- [ResultReaderFactory](rust/src/reader/mod.rs)
- [Connection metadata design](rust/spec/connection-metadata-design.md)
- [databricks-jdbc IDatabricksMetadataClient](~/databricks-jdbc/src/main/java/com/databricks/jdbc/dbclient/IDatabricksMetadataClient.java)
