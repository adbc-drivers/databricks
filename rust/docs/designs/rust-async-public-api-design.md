# Rust Kernel — Async Public API Design

**Status:** Proposed
**Scope:** `rust/` (Databricks ADBC Rust driver)
**Author:** (to be filled in on PR)
**Ticket:** (associate with Jira ticket before submitting PR)

---

## 1. Summary

This design proposes adding a **parallel, native-async public API** to the `databricks-adbc` Rust kernel. The existing synchronous ADBC trait surface (which the ODBC C++ driver consumes via a C FFI) remains byte-identical. The new async surface unlocks a Node.js binding (and future Python/JNI bindings) that cannot tolerate the `block_on` / `block_in_place` bridges currently used everywhere sync-public-wraps-async-internal.

**One-sentence framing:** publish the async layer that already exists inside the kernel, as an additive public contract, without touching the sync layer ODBC depends on.

---

## 2. Context & Motivation

### 2.1 Why this design exists

Databricks is adding SEA support to the Node.js SQL driver (`databricks-sql-nodejs`). The chosen strategy is a napi-rs native addon that wraps this Rust kernel — the same kernel ODBC uses. This gives one implementation of SEA protocol logic across all native clients.

The kernel today cannot be consumed by napi-rs as-is. Every public method is `sync-wrapping-async-with-block_on`. Calling such a method from a napi-rs `#[napi] async fn` (which itself runs on napi-rs's shared tokio runtime) triggers nested-runtime panics or deadlocks.

### 2.2 Goals

1. Expose an async public API on `Driver`, `Database`, `Connection`, `Statement`, and the result-reader layer.
2. Allow napi-rs to drive the kernel with **zero `block_on` on its caller thread**.
3. Preserve the existing ADBC trait implementations and the C FFI surface **byte-identically**.
4. Support runtime sharing: the napi-rs addon provides its own `tokio::runtime::Handle`; the kernel uses it instead of creating a per-connection Runtime.
5. Maintain backward compatibility for ODBC, JDBC, Python ADBC, and any other existing consumers.

### 2.3 Non-goals

- Rewriting the sync ADBC surface.
- Changing the C FFI at `src/ffi/*.rs` or the header `include/databricks_metadata_ffi.h`.
- Exposing async through the C FFI (C has no `async`; ODBC continues to consume sync).
- Adding new protocol features (SEA extensions, new auth types, new result formats).
- Refactoring the internal async machinery — it works; we're just publishing it.

---

## 3. Current state

### 3.1 Architectural pattern today

```mermaid
classDiagram
    direction LR

    class adbc_core_Statement {
        <<trait, sync>>
        +execute() Result~RecordBatchReader~
        +cancel() Result
    }

    class Statement {
        -client: Arc~dyn DatabricksClient~
        -runtime_handle: Handle
        +execute() Result~RecordBatchReader~
        +cancel() Result
    }

    class DatabricksClient {
        <<trait, async>>
        +create_session() async
        +execute_statement() async
        +list_catalogs() async
        +cancel_statement() async
        +close_statement() async
    }

    class StreamingCloudFetchProvider {
        -cancel_token: CancellationToken
        -chunks_in_memory: AtomicUsize
        -max_chunks_in_memory: usize
        +next_batch() async
        +get_schema() async
        +cancel()
    }

    class CloudFetchResultReader {
        <<sync via block_on>>
        +next_batch() Result
    }

    adbc_core_Statement <|.. Statement : sync trait impl
    Statement --> DatabricksClient : block_on(async)
    CloudFetchResultReader --> StreamingCloudFetchProvider : block_on(async)

    note for Statement "Every public method\nblock_ons an async call"
    note for CloudFetchResultReader "Per-batch block_on\nduring streaming"
```

**What's already async internally:**

- `DatabricksClient` trait — `#[async_trait]` over all SEA RPCs (`execute_statement`, `list_catalogs`, etc.).
- `StreamingCloudFetchProvider` — async, with a `CancellationToken`, memory-bounded concurrency (atomic counter + max), and parallel chunk downloads.
- `ChunkLinkFetcher` trait — `#[async_trait]`.
- `ChunkDownloader` — async, uses tokio HTTP concurrency.
- `DatabricksHttpClient` — async, full retry policy, token refresh.
- OAuth `ClientCredentialsProvider::new` and `AuthorizationCodeProvider::new` — async constructors.
- `TokenStore` — Fresh/Stale/Expired state machine, background refresh via `spawn_blocking`, atomic coordination.

**Where async is hidden behind sync:**

| Layer | File | Sites |
|---|---|---|
| `Statement` ADBC impl | `src/statement.rs` | execute (142), cancel (188), Drop (201) |
| `Connection` ADBC impl | `src/connection.rs` | create_session (98), list_catalogs (233), list_schemas (248), list_tables (264, 292, 396), list_columns (331, 420), Drop (514) |
| `Database::new_connection` | `src/database.rs` | Runtime::new (821), M2M auth init (859), U2M auth init (891) |
| `CloudFetchResultReader` | `src/reader/mod.rs` | schema (319), next_batch (323) |
| `AuthProvider::get_auth_header` | `src/auth/oauth/m2m.rs` / `u2m.rs` | `block_in_place + Handle::current().block_on(...)` on every call that needs refresh |

**Runtime ownership:** `Connection` today owns a `tokio::runtime::Runtime` (not a `Handle`). Each connection creates its own thread pool.

### 3.2 Why this is incompatible with napi-rs

```mermaid
sequenceDiagram
    participant JS as JS thread (V8)
    participant NAPI as napi-rs tokio worker
    participant S as Statement (sync ADBC)
    participant C as DatabricksClient (async)

    JS->>NAPI: await addon.execute(sql)
    NAPI->>S: stmt.execute()
    Note over S: self.runtime_handle.block_on(...)
    S--xNAPI: PANIC — "Cannot start a runtime from within a runtime"
    NAPI--xJS: Promise rejects with native error
```

napi-rs's `#[napi] async fn` schedules futures onto its own tokio runtime's worker threads. When the kernel's sync method calls `block_on` on that same thread, tokio detects re-entrance and panics. `block_in_place` (used by auth) doesn't panic but starves the worker pool.

---

## 4. Proposed architecture

### 4.1 Dual-surface contract

```mermaid
classDiagram
    direction LR

    class adbc_core_Statement {
        <<trait, sync>>
        +execute()
        +cancel()
    }

    class AsyncStatement {
        <<new, async>>
        +execute_async() async
        +cancel_async() async
        +close_async(self) async
    }

    class Statement {
        +execute() ← sync ADBC path
        +execute_async() async ← new
        +cancel_async() async ← new
    }

    class AsyncResultReader {
        <<new trait, async>>
        +schema() async
        +next_batch() async
        +cancel()
    }

    class ResultReader {
        <<existing trait, sync>>
        +schema()
        +next_batch()
    }

    adbc_core_Statement <|.. Statement : unchanged
    AsyncStatement <|.. Statement : new inherent impl
    Statement ..> AsyncResultReader : returns from execute_async
    Statement ..> ResultReader : returns from execute (unchanged)
```

Two surfaces on the same structs. The sync surface preserves every existing behavior; the async surface is purely additive.

### 4.2 End-to-end async flow

```mermaid
sequenceDiagram
    participant JS as JS thread
    participant NAPI as napi-rs worker (tokio)
    participant K as Statement.execute_async
    participant H as DatabricksHttpClient
    participant SEA as Databricks SEA

    JS->>NAPI: await stmt.execute(sql)
    NAPI->>K: stmt.execute_async(sql).await
    K->>H: client.execute_statement(...).await
    H->>SEA: POST /api/2.0/sql/statements
    SEA-->>H: 200 OK (statement_id)
    loop poll until SUCCEEDED
        H->>SEA: GET /statements/{id}
        SEA-->>H: {state}
    end
    H-->>K: ExecuteResult { reader, manifest }
    K-->>NAPI: Box<dyn AsyncResultReader + Send>
    NAPI-->>JS: Promise resolves
    Note over NAPI,K: Zero block_on anywhere.
```

### 4.3 Runtime ownership

```mermaid
classDiagram
    class RuntimeMode {
        <<enum>>
        +Owned(Runtime)
        +Borrowed(Handle)
    }

    class Database {
        +new_connection() Connection ← creates Owned runtime
        +new_connection_async(Handle) Connection ← uses Borrowed
    }

    class Connection {
        -runtime_mode: RuntimeMode
        +handle() &Handle
    }

    Database --> RuntimeMode : chooses per constructor
    Connection --> RuntimeMode : stores one variant
```

- **Owned mode** — ODBC path. `Database::new_connection` creates a `Runtime`, hands it to `Connection`. Today's behavior, unchanged.
- **Borrowed mode** — Node path. `Database::new_connection_async(handle)` stores a `Handle`. No Runtime created; napi-rs's runtime drives everything.

---

## 5. Interface specifications

All signatures below are **inherent methods** (not trait methods) on the existing structs, unless explicitly marked as a new trait. This keeps the sync ADBC traits untouched.

### 5.1 Database — async constructors

```rust
impl Database {
    /// Construct a Connection using the caller's tokio runtime handle.
    /// Replaces the internal `Runtime::new()` + auth `block_on` path.
    ///
    /// Contract:
    /// - Awaits auth provider construction (no block_on).
    /// - Awaits session creation (no block_on).
    /// - Connection stores the handle in Borrowed mode.
    /// - Drop on the resulting Connection is spawn-and-forget (§6.3).
    pub async fn new_connection_async(
        &self,
        handle: tokio::runtime::Handle,
    ) -> Result<Connection>;
}
```

### 5.2 Connection — async constructor + async metadata mirrors

```rust
impl Connection {
    pub(crate) async fn new_async(
        config: ConnectionConfig,
        handle: tokio::runtime::Handle,
    ) -> crate::Result<Self>;

    pub async fn list_catalogs_async(&self) -> Result<ExecuteResult>;

    pub async fn list_schemas_async(
        &self,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
    ) -> Result<ExecuteResult>;

    pub async fn list_tables_async(
        &self,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        table_pattern: Option<&str>,
        table_types: Option<&[&str]>,
    ) -> Result<ExecuteResult>;

    pub async fn list_columns_async(
        &self,
        catalog: &str,
        schema_pattern: Option<&str>,
        table_pattern: Option<&str>,
        column_pattern: Option<&str>,
    ) -> Result<ExecuteResult>;

    pub async fn get_objects_async(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<Box<dyn AsyncResultReader + Send>>;

    pub async fn get_table_schema_async(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema>;

    pub async fn get_info_async(
        &self,
        codes: Option<HashSet<InfoCode>>,
    ) -> Result<Box<dyn AsyncResultReader + Send>>;

    /// Deterministic async shutdown. Preferred over Drop for the async path.
    pub async fn close_async(self) -> Result<()>;
}
```

**Contract for every `*_async` method:**
- Must not call `block_on` or `block_in_place` anywhere in its call chain.
- Must be `Send` (the returned Future is `Send`; can be `tokio::spawn`ed).
- Safe to invoke concurrently from any tokio runtime.
- Auth header fetching on the `DatabricksHttpClient` goes through the new async auth path (§7).

### 5.3 Statement — async methods

```rust
impl Statement {
    /// Execute and return an async reader. No block_on.
    pub async fn execute_async(
        &mut self,
    ) -> Result<Box<dyn AsyncResultReader + Send>>;

    /// Fire the CancellationToken and issue DELETE /statements/{id}.
    pub async fn cancel_async(&mut self) -> Result<()>;

    /// Explicit deterministic close. Sets an internal `closed` flag so Drop skips I/O.
    pub async fn close_async(self) -> Result<()>;
}
```

### 5.4 AsyncResultReader — new trait

```rust
#[async_trait]
pub trait AsyncResultReader: Send {
    /// Returns the Arrow schema. May wait on the first batch if the reader
    /// has not yet observed it (CloudFetch).
    async fn schema(&self) -> Result<SchemaRef>;

    /// Returns the next batch, or None at end-of-stream.
    /// Respects the reader's CancellationToken.
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>>;

    /// Synchronous cancel — flips the CancellationToken.
    /// Safe to call from any thread at any time, including mid-`next_batch`.
    fn cancel(&self);
}
```

**Contract:**

- `schema()`: idempotent; safe to call before, during, or after `next_batch()`.
- `next_batch()`: returns `Ok(None)` once at EOS; subsequent calls also return `Ok(None)`.
- `cancel()`: after call, subsequent `next_batch()` returns an error whose `to_adbc()` yields a cancelled status. Does not block.
- Dropping the reader before EOS releases all in-memory chunks and in-flight downloads.

**Concrete implementations shipped with the kernel:**

| Impl | Wraps | Notes |
|---|---|---|
| `AsyncCloudFetchResultReader` | `Arc<StreamingCloudFetchProvider>` | Delegates to existing `pub async fn next_batch` — no block_on. Inherits CancellationToken + memory-bounded concurrency. |
| `AsyncInlineArrowReader` | `InlineArrowProvider` | Inline bytes parsed in constructor; `next_batch` is a trivial `async { pop_front() }`. |
| `AsyncEmptyReader` | schema only | For zero-row queries (e.g., `SELECT ... WHERE 1=0`). |

### 5.5 AsyncAuthProvider — new trait

```rust
#[async_trait]
pub trait AsyncAuthProvider: Send + Sync + std::fmt::Debug {
    /// Async auth header retrieval. Must not block_in_place.
    async fn get_auth_header_async(&self) -> Result<String>;
}
```

**Concrete impls:**

- `PersonalAccessToken` — trivially `async { Ok(format!("Bearer {}", self.token)) }`.
- `ClientCredentialsProvider` — uses the existing TokenStore state machine; calls its existing async refresh path directly (no `block_in_place`).
- `AuthorizationCodeProvider` — same.

**Contract:** every type that implements `AuthProvider` (sync) also implements `AsyncAuthProvider`. The two methods return identical values for the same state; they differ only in how they wait.

### 5.6 DatabricksHttpClient — async auth path

`DatabricksHttpClient` must be able to fetch auth headers asynchronously when driven from an async context. Add an internal branch that uses `AsyncAuthProvider::get_auth_header_async` when the caller is async; keep the sync path for sync callers.

No public API change here — it's an internal wiring tweak. Called out because `block_in_place` today lives inside the HTTP client's outbound request path (via the sync `AuthProvider`), and a genuinely async Node call must avoid it.

### 5.7 ExecuteResult — shape preserved

```rust
pub struct ExecuteResult {
    pub reader: Box<dyn ResultReader + Send>,           // sync; existing
    pub async_reader: Option<Box<dyn AsyncResultReader + Send>>, // new; async
    pub manifest: Option<ResultManifest>,
    pub statement_id: String,
}
```

Async callers populate `async_reader`, sync callers populate `reader`. An internal helper can synthesize the sync reader from the async one (wrapping with `block_on`) for the sync code path, preserving byte-identical behavior for ODBC.

---

## 6. Concurrency model

### 6.1 Thread safety

| Type | Guarantee |
|---|---|
| `Arc<Connection>` | Shared across threads. All `*_async` methods are `&self`, safe to invoke concurrently. |
| `Statement` | `Send` but not `Sync`; one active consumer per statement (mirrors current semantics). |
| `Box<dyn AsyncResultReader>` | `Send` but not `Sync`; single-consumer. |
| `Arc<dyn DatabricksClient>` | `Send + Sync`; already today. |
| `Arc<dyn AsyncAuthProvider>` | `Send + Sync`; new, same semantics as sync trait. |

### 6.2 Backpressure

Already implemented inside `StreamingCloudFetchProvider`:

- `chunks_in_memory: AtomicUsize` tracks outstanding downloaded-but-unconsumed chunks.
- `max_chunks_in_memory: usize` caps it.
- `schedule_downloads()` breaks early when the cap is reached.
- `next_batch()` decrements the counter as the consumer drains.

**No new code needed.** Exposing the async surface gives callers direct access to this mechanism — a slow napi-rs consumer naturally parks the kernel's producer without any additional wiring.

### 6.3 Cancellation

```mermaid
sequenceDiagram
    participant JS as JS thread
    participant NAPI as napi-rs worker
    participant S as Statement
    participant R as AsyncResultReader
    participant P as StreamingCloudFetchProvider
    participant SEA as Databricks SEA

    Note over JS,P: Long-running query in flight
    NAPI->>R: next_batch().await
    activate R
    R->>P: provider.next_batch().await
    activate P
    P->>SEA: GET chunk 7
    Note over JS: user calls operation.cancel()
    JS->>S: stmt.cancel_async()
    S->>P: cancel()
    Note over P: CancellationToken flipped
    P-->>R: Err(cancelled)
    deactivate P
    R-->>NAPI: Err(cancelled)
    deactivate R
    S->>SEA: DELETE /statements/{id}
    SEA-->>S: 200
    S-->>JS: cancel_async resolves
```

**Flow:**
1. `cancel_async` flips the shared `CancellationToken` (sync) *and* issues the kernel-side DELETE RPC (async).
2. Any in-flight `next_batch()` on an `AsyncResultReader` observes the token at its next `.await` checkpoint and returns an error.
3. Existing `CancellationToken` instrumentation inside `StreamingCloudFetchProvider::next_batch`, `wait_for_chunk`, `download_chunk_with_retry` handles this today — we just expose the trigger.

**Caveat:** cooperative cancellation only fires at `.await` boundaries. Synchronous CPU chunks between awaits (Arrow decode, LZ4 decompress) run to completion. Typical SEA workloads have frequent awaits; not expected to be a problem in practice.

### 6.4 Drop semantics

```mermaid
stateDiagram-v2
    [*] --> Active
    Active --> Closed: close_async().await
    Active --> DroppedOwnedRT: drop (RuntimeMode::Owned)
    Active --> DroppedBorrowedRT: drop (RuntimeMode::Borrowed)
    Closed --> [*]: Drop runs but skips I/O

    DroppedOwnedRT --> [*]: block_on(delete_session)
    DroppedBorrowedRT --> [*]: handle.spawn(delete_session)
```

- **Owned mode** (ODBC): Drop keeps today's `runtime.block_on(delete_session())`. Zero change.
- **Borrowed mode** (Node): Drop spawns cleanup onto the borrowed `Handle` and returns immediately. JS thread is never blocked.
- **Explicit `close_async`**: sets a `closed: bool` flag. Drop observes it and skips all I/O — avoids double-close.

### 6.5 Runtime re-entrance safety

Every `*_async` method must be safe to call from:
- A fresh tokio runtime (Rust application using the kernel directly).
- A napi-rs worker on a shared tokio runtime.
- A `tokio::spawn`ed task from inside another `*_async` method (reentrancy via fanout).

The only requirement on the caller is that a tokio runtime is entered. There is no assumption about runtime flavor, worker count, or thread-local state.

---

## 7. Authentication

### 7.1 Sync vs async provider parity

```mermaid
classDiagram
    class AuthProvider {
        <<trait, sync>>
        +get_auth_header() Result
    }

    class AsyncAuthProvider {
        <<trait, async, new>>
        +get_auth_header_async() async
    }

    class PersonalAccessToken {
        +get_auth_header() Result
        +get_auth_header_async() async
    }

    class ClientCredentialsProvider {
        -token_store: Arc~TokenStore~
        -http_client: Arc~DatabricksHttpClient~
        +get_auth_header() ← block_in_place
        +get_auth_header_async() ← direct await
    }

    AuthProvider <|.. PersonalAccessToken
    AuthProvider <|.. ClientCredentialsProvider
    AsyncAuthProvider <|.. PersonalAccessToken
    AsyncAuthProvider <|.. ClientCredentialsProvider
```

Every concrete auth type implements both traits. TokenStore's existing state machine (Fresh/Stale/Expired + background refresh) is untouched — it's already async-native internally.

### 7.2 External token injection (future extension, non-blocking)

Out of scope for this design. A follow-up design may add a `TokenProvider` callback mechanism so the Node binding can push externally-resolved tokens (PAT from env, externally-issued JWT, federation). Sketch:

```rust
#[async_trait]
pub trait ExternalTokenSource: Send + Sync {
    async fn fetch_token(&self) -> Result<String>;
}
```

Mentioned here only so the async API shape doesn't preclude it.

---

## 8. What doesn't change (compatibility guarantees)

The following surfaces are **byte-identical** after this change:

| Surface | File | Guarantee |
|---|---|---|
| `impl adbc_core::Driver for Driver` | `src/driver.rs` | Unchanged — no I/O |
| `impl adbc_core::Database for Database` | `src/database.rs` | `new_connection()` body unchanged |
| `impl adbc_core::Connection for Connection` | `src/connection.rs` | All trait bodies unchanged; still `block_on` internally |
| `impl adbc_core::Statement for Statement` | `src/statement.rs` | Same |
| `ResultReader` trait and all impls | `src/reader/mod.rs` | Unchanged |
| `ResultReaderAdapter` | `src/reader/mod.rs` | Unchanged — Arrow `RecordBatchReader` remains sync |
| C FFI — `extern "C"` functions | `src/ffi/*.rs` | Zero change |
| C header | `include/databricks_metadata_ffi.h` | Zero change |
| `[lib] crate-type = ["lib", "cdylib", "staticlib"]` | `Cargo.toml` | Zero change |
| ABI and binary layout | | ODBC keeps linking against the same symbols |

**Verification:** the existing ODBC integration test suite (in the `../../databricks-odbc` repo) must pass unchanged against a kernel built from this branch. This is the tripwire.

---

## 9. Feature flag strategy

Add a Cargo feature `async-api` that gates the new surface:

```toml
[features]
default = ["async-api"]
async-api = []           # can be disabled to shave binary size for pure-ODBC consumers
metadata-ffi = []        # existing — no change
```

Rationale for enabling by default: the async surface adds ~50 KB to the compiled library but enables all non-ODBC consumers. Disabling is available for downstream packagers who want minimal surface.

---

## 10. Phased implementation plan

Each phase is an atomic, independently-testable step. Dependencies are explicit.

### Phase 1 — Runtime ownership refactor

- Introduce `RuntimeMode` enum with `Owned(Runtime)` and `Borrowed(Handle)` variants.
- Replace `runtime: tokio::runtime::Runtime` field on `Connection` with `runtime_mode: RuntimeMode`.
- Every existing `self.runtime.block_on(...)` call site becomes `self.runtime_mode.block_on(...)` (delegates appropriately).
- `new_with_runtime` continues to build `Owned` mode; no observable behavior change.

**Exit:** every existing test passes. `cargo clippy --all-targets -- -D warnings` clean.

### Phase 2 — Async auth providers

- Define `AsyncAuthProvider` trait (§5.5).
- Implement for `PersonalAccessToken`, `ClientCredentialsProvider`, `AuthorizationCodeProvider`.
- Add `DatabricksHttpClient::auth_header_async(&self) -> Result<String>` internal method that dispatches to `AsyncAuthProvider` when wired.
- Sync `AuthProvider` path unchanged (still uses `block_in_place`).

**Exit:** a `#[tokio::test]` verifies each async auth impl returns the same header as the sync one.

### Phase 3 — Async constructors

- `Connection::new_async(config, handle) -> Result<Self>` — mirror of `new_with_runtime` with `.await` substituted, stores `RuntimeMode::Borrowed(handle)`.
- `Database::new_connection_async(&self, handle) -> Result<Connection>` — builds auth provider async, then `Connection::new_async`.
- OAuth provider construction uses existing `pub async fn new_with_full_config` directly (no block_on).

**Exit:** a `#[tokio::test]` constructs a Connection against a live warehouse (gated on env vars, `#[ignore]` by default) via `new_connection_async`.

### Phase 4 — Async metadata methods on Connection

- Add inherent `*_async` methods (§5.2): `list_catalogs_async`, `list_schemas_async`, `list_tables_async`, `list_columns_async`, `get_table_schema_async`, `get_info_async`, `get_objects_async`.
- Each body is the existing ADBC trait body with `.await` substituted for `block_on(...)`.
- ADBC trait impls unchanged.

**Exit:** parity test — each async method returns identical content to its sync counterpart.

### Phase 5 — AsyncResultReader trait + impls

- Define `AsyncResultReader` trait (§5.4).
- Implement `AsyncCloudFetchResultReader` — wraps `Arc<StreamingCloudFetchProvider>`, delegates directly (no block_on).
- Implement `AsyncInlineArrowReader` — wraps `InlineArrowProvider`, trivial async shims.
- Implement `AsyncEmptyReader`.
- Factory: `ResultReaderFactory::create_async(...)` — decides inline-vs-cloudfetch from response.
- Augment `ExecuteResult` with optional `async_reader` field.

**Exit:** a `#[tokio::test]` executes a query via `execute_async`, iterates with `next_batch`, asserts row content matches the sync path.

### Phase 6 — Async Statement methods

- Add `execute_async`, `cancel_async`, `close_async` (§5.3).
- `execute_async` body: `self.client.execute_statement(...).await` → wrap result in `AsyncResultReader`.
- `cancel_async`: flip local `CancellationToken` + issue `client.cancel_statement(...).await`.
- `close_async(self)`: sets `closed: bool`, calls `client.close_statement(...).await`.

**Exit:** cancellation test — issue long query, call `cancel_async`, confirm next-batch returns cancelled error promptly.

### Phase 7 — Drop semantics

- `Connection::drop`: branch on `runtime_mode`. Owned → today's `block_on`. Borrowed → `handle.spawn(cleanup_future)`.
- `Statement::drop`: same pattern. Skip if `closed` flag is set.
- Add a smoke test: spawn 100 Connections in Borrowed mode, drop them all, assert the tokio runtime processes the spawned cleanup within a timeout.

**Exit:** process exits cleanly under napi-rs-like usage patterns.

### Phase 8 — Integration with napi-rs spike

- In the `databricks-sql-nodejs` worktree, build a minimal napi-rs crate that:
  - Depends on this kernel via path dep.
  - Exposes `#[napi] async fn execute(sql: String) -> Buffer` that serializes the first batch to Arrow IPC and returns.
- Write a Node harness that calls it against a live warehouse, measures event-loop delay via `perf_hooks.monitorEventLoopDelay`.

**Exit:** E2E `SELECT 1` through napi-rs; max event-loop delay < 20 ms during the call.

### Phase 9 — Documentation & release prep

- Update `src/lib.rs` rustdoc module overview.
- Add `examples/async_query.rs` demonstrating `Database::new_connection_async`.
- CHANGELOG entry under "Added — async public API (behind `async-api` feature flag, enabled by default)."
- Ensure `cargo test` (all features) green; `cargo test --no-default-features` green.

---

## 11. Testing strategy

### Unit tests (per phase)

- `RuntimeMode_dispatches_correctly_to_Owned`
- `RuntimeMode_dispatches_correctly_to_Borrowed`
- `PersonalAccessToken_async_matches_sync`
- `ClientCredentialsProvider_async_matches_sync`
- `TokenStore_async_refresh_no_block_in_place`
- `AsyncInlineArrowReader_yields_all_batches`
- `AsyncCloudFetchResultReader_respects_cancel_token`
- `Connection_new_async_constructs_without_block_on`
- `Statement_cancel_async_fires_token_and_issues_delete`

### Integration tests (live warehouse, `#[ignore]`)

- `async_select_one_returns_one_row`
- `async_metadata_catalogs_matches_sync`
- `async_large_query_streams_without_oom`
- `async_cancel_interrupts_long_query_promptly`
- `async_auth_m2m_refreshes_expired_token`

### Parity tests (run against live warehouse, both backends)

- Same SQL → same row content via sync ADBC path and async path.
- Same metadata query → identical ExecuteResult shape.

### ODBC compatibility verification

- Run the existing `databricks-odbc` test suite against a locally-built kernel from this branch. Must pass unchanged. This is the single most important regression gate.

### Benchmarks (informational, not gates)

- Throughput of `AsyncCloudFetchResultReader` on a 1M-row query, async path vs sync path. Expect approximate parity.
- Event-loop delay during streaming in the napi-rs spike.

---

## 12. Edge cases & failure modes

| Scenario | Current (sync) behavior | Async behavior |
|---|---|---|
| Network failure during `execute` | block_on returns Err; caller handles | Future resolves to Err; caller handles |
| Token expiry mid-stream | HTTP retry fires with refreshed token | Same — async auth path also goes through TokenStore |
| Consumer stops iterating mid-stream | Sync reader dropped → block_on(close) | Async reader dropped → spawn(close) on borrowed runtime |
| `cancel_async` during a `block_on` poll | N/A — there is no poll | CancellationToken observed at next `.await`; typically within tens of ms for SEA |
| Drop on JS thread (Borrowed mode) | N/A | spawn(cleanup); JS thread never blocked |
| Kernel crate with `async-api` disabled | N/A | New async symbols not compiled; ADBC/ODBC path byte-identical |
| Nested tokio runtime (caller in sync context calls async path) | N/A | Caller must enter a runtime; Owned mode's `block_on` remains the sync bridge |
| CPU-bound work inside `next_batch` (LZ4, Arrow decode) | Blocks caller's thread | Blocks a tokio worker; recommend `spawn_blocking` for large batches if profiling shows pool starvation |

---

## 13. Configuration

| Config | Scope | Default | Purpose |
|---|---|---|---|
| Cargo feature `async-api` | Compile-time | enabled | Gate the new public surface |
| `RuntimeMode` | Per-Connection | `Owned` when built via ADBC path; `Borrowed` when built via `new_connection_async` | Selects runtime ownership |
| `max_chunks_in_memory` | `CloudFetchConfig` | existing default | Backpressure cap (unchanged) |
| `cloudfetch_parallelism` | `CloudFetchConfig` | existing default | Per-statement download concurrency (unchanged) |

No new user-facing configuration is introduced. All knobs are existing.

---

## 14. Alternatives considered

### A. Make the ADBC trait implementations themselves async

**Rejected.** The `adbc_core` traits are defined as sync by the ADBC spec. Changing them would break every ADBC consumer (ODBC, Python ADBC, JDBC-via-ADBC). This is a multi-project breaking change we cannot unilaterally make.

### B. Remove `block_on` from existing sync methods entirely

**Rejected.** The sync ADBC methods are consumed by sync-only C++/C callers that cannot `.await`. Removing the bridge would force those callers to build their own runtime — worse ergonomics than today.

### C. Separate crate for the async surface

**Rejected.** The async methods share all state (client, session, runtime handle) with the sync methods. Splitting into two crates would require lifting every shared type into a third "core" crate and adding `pub` exposures that feel arbitrary. The inherent-impl approach on the same structs is cleaner.

### D. Replace the internal Runtime with a lazy singleton

**Rejected in this scope.** Would be a separate improvement to the sync path. Orthogonal to exposing async.

### E. Expose the async surface via a C FFI for future WASM/Python

**Rejected for this scope.** C has no `async`. Any async FFI requires either callback-based continuations (huge surface churn) or caller-side `block_on` (pointless). Each language binding (napi-rs, PyO3, JNI) speaks directly to the Rust-native async API.

---

## 15. Open questions

1. **Does `async-api` stay on by default, or opt-in?**
   - Recommendation: on. Removes a foot-gun for downstream consumers who would otherwise see "symbol not found" for async methods they expected.

2. **Should `ExecuteResult` carry both `reader` and `async_reader`, or use an enum?**
   - Recommendation: both as `Option`, as sketched in §5.7. Allows zero-cost upgrade paths and is simpler for consumers to inspect.

3. **Do we need `tokio::sync::Mutex` vs `std::sync::Mutex` anywhere in the new code?**
   - Recommendation: std::sync::Mutex for state held briefly (< 1 µs); tokio::sync::Mutex for any guard held across `.await`. Decide per site during implementation.

4. **Should the async auth trait supersede the sync one eventually?**
   - Recommendation: no. Sync trait stays forever — it's the ADBC contract. Async trait is additive.

5. **What's the upstream coordination plan?**
   - Need to raise this as an RFC-style discussion with the `adbc-drivers/databricks` maintainers before opening a 700-line PR. The design itself is conservative; the coordination overhead is mostly social.

---

## 16. Review focus areas

Reviewers, please focus on:

1. **ADBC compatibility guarantee (§8).** Does the plan actually leave ODBC byte-identical? Anything I missed?
2. **Runtime ownership model (§4.3, §6.5).** Is the `Owned | Borrowed` enum the right abstraction, or should we just allow `Database` to take an optional `Handle` and always use Borrowed when provided?
3. **AsyncResultReader shape (§5.4).** Does this trait cover every real consumer need? Any missing methods (size hints, row counts, schema metadata propagation)?
4. **Drop semantics (§6.4).** Is "spawn-and-forget in Borrowed mode" acceptable, or do we need a stronger guarantee (e.g., a reaper task that awaits all outstanding cleanups on kernel shutdown)?
5. **Feature flag default (§9, question 1).** On or off?
6. **Upstream coordination (question 5).** Who owns talking to the kernel maintainers, and on what timeline?

---

## 17. Appendix: file-by-file change map

| File | Change |
|---|---|
| `src/lib.rs` | Add `pub use` for async traits; add module-level rustdoc |
| `src/driver.rs` | No change |
| `src/database.rs` | Add `pub async fn new_connection_async` |
| `src/connection.rs` | Replace `runtime` field with `runtime_mode` enum; add ~7 `pub async fn` methods; update Drop |
| `src/statement.rs` | Add 3 `pub async fn` methods; add `closed` flag; update Drop |
| `src/reader/mod.rs` | Add `AsyncResultReader` trait; add async impls for CloudFetch, Inline, Empty; update factory |
| `src/auth/mod.rs` | Add `AsyncAuthProvider` trait |
| `src/auth/pat.rs` | Implement `AsyncAuthProvider` (trivial) |
| `src/auth/oauth/m2m.rs` | Implement `AsyncAuthProvider`; route through existing TokenStore without `block_in_place` |
| `src/auth/oauth/u2m.rs` | Same |
| `src/client/http.rs` | Add internal async auth-header path |
| `src/ffi/*.rs` | No change |
| `Cargo.toml` | Add `async-api` feature (enabled by default) |
| `tests/` | Add async integration tests (gated on env + `#[ignore]`) |
| `examples/async_query.rs` | New example |

Estimated net addition: ~700 LOC. Estimated net deletion: ~0 LOC.

---

## 18. Auth + TLS integration addendum

Added after initial design circulated. A deep audit of how the Node driver's auth/transport layer interacts with the proposed kernel async surface surfaced five concrete gaps that the original §7 (Authentication) understated. This addendum documents them and proposes additive kernel changes beyond the core async exposure.

### 18.1 Responsibility split — Node resolves, Rust consumes

The Node binding will use the kernel **exclusively in access-token (PAT) mode**. All OAuth flows, federation, external-token callbacks, custom `IAuthentication` subclasses, and token caching stay in TypeScript — where they already exist in the Thrift driver and do not need reimplementing.

```mermaid
sequenceDiagram
    participant TS as lib/sea/SeaBackend (TS)
    participant Auth as IAuthentication chain (TS)<br/>Federation → Cached → External / OAuth / PAT
    participant NAPI as napi-rs binding
    participant Rust as Rust kernel<br/>(PersonalAccessToken mode only)
    participant DBX as Databricks SEA

    Note over TS,Auth: Token resolution, refresh, federation, caching — all TS-side
    TS->>Auth: authenticate()
    Auth-->>TS: Bearer token (JWT)
    TS->>NAPI: stmt.updateAccessToken(token)
    TS->>NAPI: stmt.execute_async(sql)
    NAPI->>Rust: Statement.execute_async
    Rust->>DBX: POST /api/2.0/sql/statements<br/>Authorization: Bearer ...
    DBX-->>Rust: ExecuteResult
    Rust-->>NAPI: AsyncResultReader
    NAPI-->>TS: Promise<Buffer>
```

**Invariant:** the kernel's OAuth providers (`ClientCredentialsProvider`, `AuthorizationCodeProvider`) are never constructed by the Node binding. Only `PersonalAccessToken` is used. This avoids:

- Duplicate OAuth flows in Rust and TS.
- Port collision between the two U2M callback servers (both bind localhost port 8030 by default).
- Duplicate token caches on disk with divergent content.

### 18.2 Kernel gap — `PersonalAccessToken` needs interior mutability

`src/client/http.rs:272-280` uses `OnceLock::set()` for auth-provider installation — second call returns `Err` and today's code panics. Combined with `PersonalAccessToken::token: String` being immutable, this means the Node binding **cannot rotate the bearer token after construction**. Every OAuth token expiry would require tearing down the kernel `Connection` — unusable for long-lived sessions.

**Proposed kernel change:**

```rust
// src/auth/pat.rs (new)
pub struct PersonalAccessToken {
    token: Arc<RwLock<String>>,
}

impl PersonalAccessToken {
    pub fn new(token: impl Into<String>) -> Self {
        Self { token: Arc::new(RwLock::new(token.into())) }
    }

    /// Atomically replace the held token. Safe to call from any thread
    /// concurrently with `get_auth_header` calls.
    pub fn update_token(&self, new_token: impl Into<String>) {
        *self.token.write().unwrap() = new_token.into();
    }
}

impl AuthProvider for PersonalAccessToken {
    fn get_auth_header(&self) -> Result<String> {
        Ok(format!("Bearer {}", self.token.read().unwrap()))
    }
}
```

Same addition on `AsyncAuthProvider` impl. Zero change to the trait signatures.

**Estimated cost:** ~20 LOC.

### 18.3 Token-refresh strategy across the FFI

With §18.2 in place, the Node binding drives token freshness with two mechanisms:

1. **Pre-emptive push (primary).** After resolving a token via `IAuthentication`, TS parses the JWT `exp` claim and starts a timer for `exp − 30s`. On fire: re-authenticate, call `nativeConnection.updateAccessToken(fresh)`. Same cadence used by the Thrift driver's `CachedTokenProvider`.
2. **401 retry (safety net).** If the kernel returns a status-401 error (observable via `napi::Error` mapping), the TS wrapper re-authenticates and retries the operation once. Safe for idempotent metadata ops (`list_catalogs`, etc.); **not** invoked for `execute_statement` — a re-executed query could double-apply side effects.

**CloudFetch chunk downloads use presigned URLs** and require no `Authorization` header, so mid-stream expiry of the Databricks-issued bearer token does not break in-flight downloads. Only the statement-polling loop inside Rust is affected, and that loop picks up the fresh token on its next HTTP request because `get_auth_header()` reads from the `RwLock` on every call.

### 18.4 TLS trust store — the biggest behavioral divergence

The Node driver and the Rust kernel load trust anchors from **different sources**:

| Aspect | Node driver today | Rust kernel |
|---|---|---|
| TLS backend | Node's built-in OpenSSL | `reqwest` + `native-tls` (`Cargo.toml:42`) |
| Default trust anchors | Node's bundled Mozilla CA list | OS system store (macOS Keychain / Linux `/etc/ssl/certs` / Windows SChannel) |
| `NODE_EXTRA_CA_CERTS` env var | Automatically appended | **Ignored** |
| `ca` ConnectionOption | `Buffer \| string` — used as full replacement | **Not supported** (kernel accepts file path only) |
| Config field | `ca` (in-memory) | `TlsConfig.trusted_certificate_path` (file path) |
| `rejectUnauthorized` default | **`false`** (permissive, see §18.4.1) | `true` (strict) |

**Failure scenarios that break users migrating Thrift → SEA:**

1. Corporate MITM proxy with `NODE_EXTRA_CA_CERTS=/etc/corp-ca.pem` — Thrift works, SEA fails with cert verification error.
2. User passes `ca: fs.readFileSync('cert.pem')` as Buffer — Thrift works, SEA fails because kernel expects a file path.
3. CA trusted in Keychain (macOS enterprise auto-installed) but not in Node's bundle — Thrift fails, SEA works (opposite divergence).

**Required changes:**

1. **Node binding (TS side):**
   - New `ConnectionOptions.caPath: string` — forwarded to Rust's `trusted_certificate_path`.
   - When user supplies in-memory `ca: Buffer | string`, serialize to a securely-created temp file (`0o600`, auto-cleanup on process exit) and pass the path through.
   - When `NODE_EXTRA_CA_CERTS` is set and `useSEA: true`, emit a warning: *"SEA backend uses a separate TLS trust store; NODE_EXTRA_CA_CERTS is not applied. Use `caPath` option instead."*
   - Forward `rejectUnauthorized: false` through to Rust's `allow_self_signed=true` + `allow_hostname_mismatch=true` (both already in `TlsConfig`).

2. **Kernel change (separate PR, outside this design):**
   - Add `reqwest::Certificate::from_pem(bytes: &[u8])` support so `TlsConfig` can accept in-memory CA bytes, not just file paths. Removes the temp-file dance on the TS side.

#### 18.4.1 `rejectUnauthorized: false` as the Thrift default

`lib/connection/connections/HttpConnection.ts` sets `rejectUnauthorized: false` unconditionally today. That means **the Thrift driver disables server cert verification by default**. Surprising but documented behavior.

To preserve parity, the SEA path must match this default. Flag for discussion with reviewers: do we take this opportunity to introduce a `strictTls: boolean` option (default `false` for parity, move to `true` in a later major) so users who've been unknowingly running without verification can opt into strict mode?

### 18.5 mTLS — a hard gap

| | Node driver today | Rust kernel |
|---|---|---|
| Client certificate | Supported (`cert`/`key`/`pfx`/`passphrase`) | **Not supported** |
| TLS backend hook | `tls.createSecureContext` | `reqwest::Identity` — not currently wired in |

Any user relying on mTLS under Thrift cannot migrate to SEA until the kernel adds client-cert support. **Proposed as a known limitation for the initial SEA GA**, tracked as a separate kernel issue:

> Add `TlsConfig.client_identity_pem_path: Option<String>` (and the corresponding in-memory variant) routed into `reqwest::Identity::from_pem` and `ClientBuilder::identity(identity)`.

Estimated kernel cost: ~30 LOC. Not included in this design's scope.

### 18.6 Proxy divergence

| Aspect | Node | Rust |
|---|---|---|
| HTTP proxy | ✓ (via `proxy-agent`) | ✓ |
| HTTPS proxy | ✓ | ✓ |
| SOCKS4 / SOCKS5 | ✓ | **✗** |
| `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` env | ✓ | ✓ |
| Basic-auth credentials | ✓ | ✓ |
| ConnectionOption config | `proxy: { protocol, host, port, auth }` | `ProxyConfig { url, username, password, bypass_hosts }` |

**Required:** the SeaBackend must marshal the Node `proxy` ConnectionOption into the kernel's `ProxyConfig`. For `protocol: 'socks4' | 'socks5'`, fail fast with a clear error message at `DBSQLClient.connect()` — do not silently fall back to direct connection.

### 18.7 User-Agent passthrough

Current kernel default at `HttpClientConfig::user_agent`:

```
Databricks JDBC Driver/14.8.1 JDK/11.0.16 ADBC/1.2.0
```

A comment in `src/client/http.rs` explains this masquerade triggers Databricks server-side INLINE_OR_EXTERNAL_LINKS disposition. But it misrepresents the client — customer analytics dashboards would categorize every SEA-path query from Node as JDBC-from-JVM, breaking attribution.

**Required:**

1. `HttpClientConfig::user_agent` must be a public settable field.
2. The Node binding passes `NodejsDatabricksSqlConnector/{version} napi ({nodeVersion}, {os})` derived from the existing `lib/utils/buildUserAgentString.ts` helper.

Flag for reviewers: should the kernel default remain the JDBC masquerade, or move to `DatabricksADBC/{version}` once the server-side disposition-triggering logic no longer depends on UA sniffing?

### 18.8 Read timeout default

`HttpClientConfig::read_timeout` defaults to **60 seconds**. This aborts any SEA statement polling loop where the query runs longer than a minute — i.e., the exact workloads SEA exists to serve (large warehouse queries that run tens of seconds to minutes).

**Required:** the Node binding must override this to a value driven by `ConnectionOptions.socketTimeout` (or a new `requestTimeout`) with a floor of 15 minutes. Document the setting prominently.

### 18.9 Updated open questions

In addition to §15's six questions, add:

7. **Mutable `PersonalAccessToken` on the main design PR or a separate kernel PR?**
   - Recommendation: separate kernel PR (small, focused, orthogonal). This design PR references it as a prerequisite.

8. **`strictTls` default — match Thrift's permissive default, or make SEA strict from day one?**
   - Recommendation: match for v1. Introduce strict-by-default in a later major.

9. **User-Agent kernel default — keep the JDBC masquerade, or switch to ADBC branding?**
   - Needs confirmation from server-side team about whether UA sniffing is still load-bearing for disposition.

10. **mTLS gap — known-limitation for SEA v1, or block GA?**
    - Recommendation: known-limitation. File a kernel issue and communicate clearly.

11. **Migration warning for `NODE_EXTRA_CA_CERTS` users — runtime log, or fail-fast with documentation link?**
    - Recommendation: warn-and-proceed at connect time; add to migration docs.

### 18.10 Updated file-by-file change map

Additions to §17:

| File | Change |
|---|---|
| `src/auth/pat.rs` | `PersonalAccessToken` gains `Arc<RwLock<String>>` token + `update_token(&self, String)` setter |
| `src/client/http.rs` | `user_agent` made public-settable on `HttpClientConfig` (no default change for this PR) |
| `src/client/http.rs` | **(separate PR)** `TlsConfig` gains in-memory CA bytes support via `Certificate::from_pem(&[u8])` |
| `src/client/http.rs` | **(separate PR)** `TlsConfig` gains `client_identity_pem_path` for mTLS |

Updated LOC estimate for this design: still ~700 LOC net additive. Separate kernel PRs for in-memory CA + mTLS add another ~60 LOC combined.

### 18.11 Updated phased plan

Insert between §10 Phase 2 (Async auth providers) and Phase 3 (Async constructors):

**Phase 2b — Mutable PersonalAccessToken + `update_token` setter.**

- Change `PersonalAccessToken::token` to `Arc<RwLock<String>>`.
- Add `update_token` method.
- Verify `get_auth_header` / `get_auth_header_async` read from the lock on every call (not cached).
- Unit test: concurrent `update_token` + `get_auth_header_async` calls produce a consistent view (never a half-updated token).

**Exit:** a stress test firing `update_token` at 100 Hz while 10 concurrent tasks call `get_auth_header_async` observes no torn reads and no performance cliff.

### 18.12 Updated review focus areas

Added to §16:

7. **Mutable PAT approach (§18.2).** Is `Arc<RwLock<String>>` the right primitive, or would a simpler `ArcSwap<String>` (lock-free) be preferable given the read-heavy access pattern?
8. **TLS temp-file bridge (§18.4).** Should we accept the temp-file hack for in-memory CA certs in v1 and fix properly in a follow-up, or gate SEA behind the kernel supporting in-memory bytes from day one?
9. **`rejectUnauthorized: false` parity (§18.4.1).** Ship permissive-by-default (mirroring Thrift), or take the migration as an opportunity to flip to strict?

---

**End of design.**
