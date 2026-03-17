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

# OAuth Authentication Design: U2M and M2M Flows

## Overview

This document describes the design for adding OAuth 2.0 authentication to the Databricks Rust ADBC driver, covering:

- **U2M (User-to-Machine)**: Authorization Code flow with PKCE for interactive browser-based login
- **M2M (Machine-to-Machine)**: Client Credentials flow for service principal authentication
- **Shared infrastructure**: Token management, refresh, caching, and OIDC discovery

The Python Databricks SDK (`databricks-sdk-python`) serves as the reference implementation.

### Motivation

The driver currently only supports Personal Access Token (PAT) authentication. OAuth support is required for:
- Service principal authentication (M2M) in production workloads
- Interactive user authentication (U2M) for development and BI tools
- Parity with the Python, Java, and Go Databricks SDKs

---

## Architecture

### Module Structure

```mermaid
graph TD
    DB[Database] -->|creates| AP[AuthProvider]
    AP -->|trait impl| PAT[PersonalAccessToken]
    AP -->|trait impl| M2M[ClientCredentialsProvider]
    AP -->|trait impl| U2M[AuthorizationCodeProvider]

    M2M --> TS[TokenStore]
    U2M --> TS
    U2M --> TC[TokenCache]
    U2M --> CB[CallbackServer]

    M2M --> OIDC[OidcDiscovery]
    U2M --> OIDC

    M2M --> OC[oauth2::BasicClient]
    U2M --> OC

    TS --> TK[OAuthToken]
    TC --> TK

    HC[DatabricksHttpClient] -->|execute: calls| AP
    M2M -->|execute_without_auth: token fetches| HC
    U2M -->|execute_without_auth: token fetches| HC
```

### File Layout

```
src/auth/
  mod.rs              -- AuthProvider trait, re-exports (existing, modified)
  pat.rs              -- PersonalAccessToken (existing, unchanged)
  oauth/
    mod.rs            -- Module root, re-exports
    token.rs          -- OAuthToken struct, expiry/stale logic
    token_store.rs    -- Thread-safe token container with refresh state machine
    oidc.rs           -- OIDC endpoint discovery
    cache.rs          -- File-based token persistence
    callback.rs       -- Localhost HTTP server for U2M browser redirect
    m2m.rs            -- ClientCredentialsProvider (implements AuthProvider)
    u2m.rs            -- AuthorizationCodeProvider (implements AuthProvider)
```

> **Note:** No `pkce.rs` -- PKCE generation is handled by the `oauth2` crate's
> `PkceCodeChallenge::new_random_sha256()` method.

---

## Interfaces and Contracts

### AuthProvider Trait (existing, unchanged)

```rust
pub trait AuthProvider: Send + Sync + Debug {
    fn get_auth_header(&self) -> Result<String>;
}
```

**Contract:**
- Called on every HTTP request attempt (including retries)
- Must return a valid `"Bearer {access_token}"` string
- Must be thread-safe (`Send + Sync`)
- May block briefly for token refresh; must not block indefinitely

### OAuthToken

```rust
pub struct OAuthToken {
    pub access_token: String,
    pub token_type: String,
    pub refresh_token: Option<String>,
    pub expires_at: DateTime<Utc>,
    pub scopes: Vec<String>,
}
```

**Contract:**
- `is_expired()`: Returns true when `expires_at - 40s < now` (40s buffer matches Python SDK; Azure rejects tokens within 30s of expiry)
- `is_stale()`: Returns true when remaining TTL < stale threshold, where stale threshold = `min(initial_TTL * 0.5, 20 minutes)`, computed once at token acquisition (matching Python SDK)
- Serializable to/from JSON for disk caching

### OidcEndpoints

```rust
pub struct OidcEndpoints {
    pub authorization_endpoint: String,
    pub token_endpoint: String,
}
```

Discovered via `GET {host}/oidc/.well-known/oauth-authorization-server`.

### Usage of the `oauth2` Crate

Both U2M and M2M providers use [`oauth2::BasicClient`](https://docs.rs/oauth2/latest/oauth2/struct.Client.html) for protocol-level operations. The crate handles:

- **PKCE generation**: `PkceCodeChallenge::new_random_sha256()` generates the verifier/challenge pair (RFC 7636 S256)
- **Authorization URL construction**: `client.authorize_url(...)` with state, scopes, and PKCE challenge
- **Token exchange**: `client.exchange_code(code).set_pkce_verifier(verifier)` for U2M
- **Client credentials exchange**: `client.exchange_client_credentials()` for M2M
- **Refresh token exchange**: `client.exchange_refresh_token(refresh_token)` for U2M
- **HTTP client integration**: Pluggable async HTTP client via `oauth2::reqwest::async_http_client`

The `oauth2::BasicClient` is constructed from OIDC-discovered endpoints:

```rust
let client = BasicClient::new(ClientId::new(client_id))
    .set_client_secret(ClientSecret::new(client_secret))  // M2M only
    .set_auth_uri(AuthUrl::new(endpoints.authorization_endpoint)?)
    .set_token_uri(TokenUrl::new(endpoints.token_endpoint)?)
    .set_redirect_uri(RedirectUrl::new(redirect_uri)?);    // U2M only
```

**What the `oauth2` crate does NOT handle** (we implement ourselves):
- OIDC endpoint discovery (`oidc.rs`)
- Token lifecycle management -- FRESH/STALE/EXPIRED state machine (`token_store.rs`)
- File-based token caching (`cache.rs`)
- Browser launch and localhost callback server (`callback.rs`)
- Integration with the driver's `AuthProvider` trait

---

## Token Refresh State Machine

```mermaid
stateDiagram-v2
    [*] --> Empty: initial state

    Empty --> Fresh: fetch token (blocking)

    Fresh --> Stale: TTL drops below stale threshold

    Stale --> Refreshing: get_auth_header() called
    note right of Refreshing: Background refresh spawned\nCurrent token returned immediately

    Refreshing --> Fresh: refresh succeeds
    Refreshing --> Stale: refresh fails (retry next call)

    Stale --> Expired: TTL drops below 40s
    Fresh --> Expired: TTL drops below 40s

    Expired --> Fresh: blocking refresh succeeds
    Expired --> Error: blocking refresh fails
```

**Stale threshold:** `min(initial_TTL * 0.5, 20 minutes)` -- computed once when the token is first acquired (using the full TTL at that moment, not the current remaining time). Stored alongside the token in `TokenStore`.

### TokenStore

```rust
pub(crate) struct TokenStore {
    token: RwLock<Option<OAuthToken>>,
    refreshing: AtomicBool,
}
```

**Contract:**
- `get_or_refresh(refresh_fn)`: Returns a valid token. If STALE, spawns background refresh via `tokio::spawn` and returns current token. If EXPIRED, blocks caller until refresh completes.
- Thread-safe: `RwLock` for read-heavy access, `AtomicBool` to prevent concurrent refresh.
- Only one refresh runs at a time; concurrent callers receive the current (stale) token.

---

## U2M Flow (Authorization Code + PKCE)

```mermaid
sequenceDiagram
    participant App as Application
    participant DB as Database
    participant U2M as AuthCodeProvider
    participant Cache as TokenCache
    participant Browser as Browser
    participant CB as CallbackServer
    participant IDP as Databricks IdP

    App->>DB: new_connection()
    DB->>U2M: new(host, client_id, scopes)
    U2M->>Cache: load(host, client_id, scopes)

    alt Cached token with valid refresh_token
        Cache-->>U2M: OAuthToken
        U2M->>U2M: Store in TokenStore
        Note over U2M: On first get_auth_header(),<br/>refresh if stale/expired
    else No cache or expired refresh_token
        U2M->>U2M: PkceCodeChallenge::new_random_sha256()
        U2M->>U2M: client.authorize_url() with PKCE + state
        U2M->>CB: Start on localhost:8020
        U2M->>Browser: Open authorization URL
        Browser->>IDP: User authenticates
        IDP->>CB: Redirect with code + state
        CB-->>U2M: Authorization code
        U2M->>IDP: client.exchange_code(code).set_pkce_verifier(verifier)
        IDP-->>U2M: access_token + refresh_token
        U2M->>Cache: save(token)
    end

    DB-->>App: Connection

    Note over App,IDP: Subsequent requests
    App->>U2M: get_auth_header()
    U2M->>U2M: TokenStore returns token<br/>(refreshes if stale/expired)
    U2M-->>App: "Bearer {access_token}"
```

### CallbackServer

```rust
pub(crate) struct CallbackServer { /* ... */ }

impl CallbackServer {
    pub fn redirect_uri(&self) -> String;
    pub async fn wait_for_code(
        &self,
        expected_state: &str,
        timeout: Duration,
    ) -> Result<String>;
}
```

**Contract:**
- Binds to `localhost:{port}` (default 8020)
- Validates `state` parameter matches expected value (CSRF protection)
- Returns HTML response "You can close this tab" to the browser
- Times out after configurable duration (default: 120s)
- Returns the authorization code extracted from the callback query parameters

### Token Exchange (U2M)

Both exchanges are handled by the `oauth2` crate, routed through `DatabricksHttpClient`:

```rust
// Adapter function: converts between oauth2 crate's HttpRequest/HttpResponse
// and DatabricksHttpClient's reqwest types, routing through execute_without_auth()
let http_fn = |request: oauth2::HttpRequest| {
    let http_client = http_client.clone();
    async move {
        let reqwest_request = convert_to_reqwest(request);  // thin conversion layer
        let response = http_client.execute_without_auth(reqwest_request).await?;
        convert_to_oauth2_response(response)  // thin conversion layer
    }
};

// Authorization code exchange (with PKCE verifier)
let token_response = client
    .exchange_code(AuthorizationCode::new(code))
    .set_pkce_verifier(pkce_verifier)
    .request_async(&http_fn)
    .await?;

// Refresh token exchange
let token_response = client
    .exchange_refresh_token(&RefreshToken::new(refresh_token))
    .request_async(&http_fn)
    .await?;
```

The `oauth2` crate constructs the correct `POST` request with `grant_type`, `code_verifier`, `redirect_uri`, `client_id`, and `refresh_token` parameters. A thin adapter converts between the `oauth2` crate's HTTP types and `reqwest` types. We use `execute_without_auth()` since the `oauth2` crate adds its own auth (Basic or form-encoded) -- this avoids the circular `get_auth_header()` call.

---

## M2M Flow (Client Credentials)

```mermaid
sequenceDiagram
    participant App as Application
    participant DB as Database
    participant M2M as ClientCredsProvider
    participant OIDC as OIDC Discovery
    participant TokenEP as Token Endpoint

    App->>DB: new_connection()
    DB->>M2M: new(host, client_id, client_secret, scopes)
    M2M->>OIDC: GET {host}/oidc/.well-known/oauth-authorization-server
    OIDC-->>M2M: OidcEndpoints

    Note over App,TokenEP: First get_auth_header() call
    App->>M2M: get_auth_header()
    M2M->>TokenEP: client.exchange_client_credentials()
    TokenEP-->>M2M: access_token (no refresh_token)
    M2M-->>App: "Bearer {access_token}"

    Note over App,TokenEP: Subsequent calls (token FRESH)
    App->>M2M: get_auth_header()
    M2M-->>App: "Bearer {cached_token}"

    Note over App,TokenEP: Token becomes STALE
    App->>M2M: get_auth_header()
    M2M->>M2M: Spawn background refresh
    M2M-->>App: "Bearer {current_token}"
    M2M->>TokenEP: client.exchange_client_credentials() (background)
    TokenEP-->>M2M: new access_token
```

### Token Exchange (M2M)

```rust
let token_response = client
    .exchange_client_credentials()
    .add_scope(Scope::new("all-apis".to_string()))
    .request_async(&http_fn)  // routes through DatabricksHttpClient::execute_without_auth()
    .await?;
```

The `oauth2` crate sends `POST {token_endpoint}` with `grant_type=client_credentials`, `Authorization: Basic base64(client_id:client_secret)`, and the requested scopes. The request goes through `DatabricksHttpClient::execute_without_auth()` to get retry logic and connection pooling.

**Contract:**
- M2M tokens have no `refresh_token`; re-authentication uses the same client credentials
- No disk caching for M2M (credentials are always available; tokens are short-lived)
- Token endpoint discovered via OIDC, or overridden with `databricks.auth.token_endpoint`

---

## Token Cache (U2M only)

**Location:** `~/.config/databricks-adbc/oauth/`

**Filename:** `SHA256(json({"host": ..., "client_id": ..., "scopes": [...]}))).json`

**File permissions:** `0o600` (owner read/write only)

```rust
pub(crate) struct TokenCache { /* ... */ }

impl TokenCache {
    pub fn load(host: &str, client_id: &str, scopes: &[String]) -> Option<OAuthToken>;
    pub fn save(host: &str, client_id: &str, scopes: &[String], token: &OAuthToken) -> Result<()>;
}
```

**Contract:**
- Cache is separate from the Python SDK cache (`~/.config/databricks-sdk-py/oauth/`). Cross-SDK cache sharing is fragile and not worth the compatibility risk.
- Cache I/O errors are logged as warnings but never block authentication.
- Tokens are saved after every successful acquisition or refresh.
- On load, expired tokens with a valid `refresh_token` are still returned (refresh will be attempted).

---

## Configuration Options

Authentication is configured via a single `databricks.auth.type` string option that selects the authentication method. This replaces the ODBC-style two-level `AuthMech`/`Auth_Flow` numeric scheme with self-describing string values.

### Rust Enum

```rust
/// Authentication type -- single selector for the authentication method.
#[derive(Debug, Clone, PartialEq)]
pub enum AuthType {
    /// Personal access token.
    AccessToken,
    /// M2M: client credentials grant for service principals.
    OAuthM2m,
    /// U2M: browser-based authorization code + PKCE.
    OAuthU2m,
}
```

### Authentication Selection

| Option | Type | Values | Required | Description |
|--------|------|--------|----------|-------------|
| `databricks.auth.type` | String | `access_token`, `oauth_m2m`, `oauth_u2m` | **Yes** | Authentication method |

| Value | Description |
|-------|-------------|
| `access_token` | Personal access token |
| `oauth_m2m` | M2M: client credentials for service principals |
| `oauth_u2m` | U2M: browser-based authorization code + PKCE |

### Credential and OAuth Options

| Option | Type | Default | Required For | Description |
|--------|------|---------|-------------|-------------|
| `databricks.access_token` | String | -- | `access_token` | Personal access token |
| `databricks.auth.client_id` | String | `"databricks-cli"` (`oauth_u2m`) | `oauth_m2m` (required), `oauth_u2m` (optional) | OAuth client ID |
| `databricks.auth.client_secret` | String | -- | `oauth_m2m` | OAuth client secret |
| `databricks.auth.scopes` | String | `"all-apis offline_access"` (`oauth_u2m`), `"all-apis"` (`oauth_m2m`) | No | Space-separated OAuth scopes |
| `databricks.auth.token_endpoint` | String | Auto-discovered via OIDC | No | Override OIDC-discovered token endpoint |
| `databricks.auth.redirect_port` | String | `"8020"` | No | Localhost port for browser callback server |

`databricks.auth.type` is mandatory -- no auto-detection. This makes configuration explicit and predictable.

---

## Concurrency Model

### Sync/Async Bridge

The `AuthProvider::get_auth_header()` trait method is synchronous, but OAuth token fetches require HTTP calls (async via `reqwest`).

**Approach:** OAuth providers reuse `DatabricksHttpClient` for all HTTP calls (including token endpoint requests), bridged to the sync `get_auth_header()`:
- Inside a tokio runtime (which the driver always has): use `tokio::task::block_in_place` + `Handle::block_on`
- For background stale-refresh: use `std::thread::spawn` with a captured `tokio::runtime::Handle`

**Avoiding circular dependency with `DatabricksHttpClient`:**

`DatabricksHttpClient` currently requires an `AuthProvider` at construction time and calls `get_auth_header()` on every `execute()` call. This creates a circular dependency if the OAuth provider also needs the HTTP client to fetch tokens. The solution is a two-phase initialization:

1. **Decouple auth from HTTP client construction.** Change `DatabricksHttpClient` to accept `auth_provider` via `OnceLock<Arc<dyn AuthProvider>>` (matching the existing `SeaClient` pattern for `reader_factory`). The client is created first without auth.
2. **OAuth providers use `execute_without_auth()`** for token endpoint calls. Token endpoints authenticate via form-encoded `client_id`/`client_secret` or `Authorization: Basic` header -- not Bearer tokens. The OAuth provider manually adds these credentials to the request before calling `execute_without_auth()`.
3. **Auth provider is set on the HTTP client after creation.**

```rust
// In database.rs new_connection():
// 1. Create HTTP client (no auth yet)
let http_client = Arc::new(DatabricksHttpClient::new(self.http_config.clone())?);

// 2. Create auth provider based on auth type
//    (see database.rs section below for full match logic)
let auth_provider: Arc<dyn AuthProvider> = /* match on AuthType */;

// 3. Set auth on HTTP client
http_client.set_auth_provider(auth_provider);
```

This gives us a single HTTP client with shared retry logic, timeouts, and connection pooling for both API calls and token endpoint calls.

**Changes to `DatabricksHttpClient`:**

```rust
pub struct DatabricksHttpClient {
    client: Client,
    config: HttpClientConfig,
    auth_provider: OnceLock<Arc<dyn AuthProvider>>,  // was: Arc<dyn AuthProvider>
}

impl DatabricksHttpClient {
    pub fn new(config: HttpClientConfig) -> Result<Self>;  // no auth_provider param
    pub fn set_auth_provider(&self, provider: Arc<dyn AuthProvider>);
    pub fn auth_header(&self) -> Result<String>;  // reads from OnceLock, errors if not set
}
```

### Thread Safety

| Component | Mechanism | Guarantee |
|-----------|-----------|-----------|
| `TokenStore.token` | `std::sync::RwLock` | Multiple readers, single writer |
| `TokenStore.refreshing` | `AtomicBool` | Lock-free single-refresh coordination |
| `TokenCache` file I/O | File-level atomicity | Write to temp file, then rename |
| `CallbackServer` | Single-use, owned by provider | No concurrent access |

---

## Error Handling

| Scenario | Error Kind | Behavior |
|----------|-----------|----------|
| Missing `databricks.auth.type` | `invalid_argument()` | Fail at `new_connection()` |
| Invalid value for `databricks.auth.type` | `invalid_argument()` | Fail at `set_option()` |
| Missing `client_id` or `client_secret` for `oauth_m2m` | `invalid_argument()` | Fail at `new_connection()` |
| Missing `access_token` for `access_token` type | `invalid_argument()` | Fail at `new_connection()` |
| OIDC discovery HTTP failure | `io()` | Fail at provider creation |
| Token endpoint returns error | `io()` | Fail at `get_auth_header()` |
| Browser callback timeout (120s) | `io()` | Fail at provider creation |
| User denies consent in browser | `io()` | Fail at provider creation with IdP error message |
| Refresh token expired (U2M) | Falls through to browser | Re-launches browser flow |
| Token cache read/write failure | Logged as warning | Never blocks auth flow |
| Callback port already in use | `io()` | Fail at provider creation |

---

## Changes to Existing Code

### `src/client/http.rs`

Two-phase auth initialization via `OnceLock` (see [Concurrency Model](#concurrency-model) for details and struct definition).

### `src/auth/mod.rs`

- Remove `pub use oauth::OAuthCredentials`
- Add `pub use oauth::{ClientCredentialsProvider, AuthorizationCodeProvider}`

### `src/database.rs`

**New fields on `Database`:**
```rust
auth_config: AuthConfig,  // groups all auth-related options
```

`set_option` parses the auth type string:
```rust
"databricks.auth.type" => {
    let v = value.as_ref();
    self.auth_config.auth_type = Some(AuthType::try_from(v)?);
    // "access_token" -> AccessToken, "oauth_m2m" -> OAuthM2m, "oauth_u2m" -> OAuthU2m
}
```

**Modified `new_connection()`:** Two-phase initialization with auth type matching:

```rust
// Phase 1: Create HTTP client (no auth yet)
let http_client = Arc::new(DatabricksHttpClient::new(self.http_config.clone())?);

// Phase 2: Create auth provider based on auth type
let auth_type = self.auth_config.validate(&self.access_token)?;

let auth_provider: Arc<dyn AuthProvider> = match auth_type {
    AuthType::AccessToken => {
        let token = self.access_token.as_ref()
            .ok_or_else(|| /* error: access_token required */)?;
        Arc::new(PersonalAccessToken::new(token))
    }
    AuthType::OAuthM2m => Arc::new(
        ClientCredentialsProvider::new(host, client_id, client_secret, http_client.clone())?
    ),
    AuthType::OAuthU2m => Arc::new(
        AuthorizationCodeProvider::new(host, client_id, http_client.clone())?
    ),
};

// Phase 3: Wire auth into HTTP client
http_client.set_auth_provider(auth_provider);
```

**`access_token` becomes optional:** Only required when mechanism=`0` (Pat) or flow=`0` (TokenPassthrough).

### `Cargo.toml`

New dependencies:
```toml
oauth2 = "5"         # OAuth 2.0 protocol (PKCE, token exchange, client credentials)
sha2 = "0.10"        # SHA-256 for token cache key generation
open = "5"           # Cross-platform browser launch
dirs = "5"           # Cross-platform config directory (~/.config/)
```

---

## Alternatives Considered

### 1. Reuse Python SDK's token cache directory
**Rejected.** Cross-SDK cache sharing is fragile -- different serialization formats, token field naming, and security implications. Each SDK/driver should manage its own cache. The JSON format is compatible enough to enable future sharing if explicitly desired.

### 2. Make AuthProvider trait async
**Rejected.** Would require changes across the entire call chain (`DatabricksHttpClient`, `SeaClient`, `Connection`, `Statement`). The sync interface with internal async bridge is simpler, matches how the Python SDK wraps async refresh behind a sync API, and `block_in_place` is efficient in a tokio multi-threaded runtime.

### 3. Implement OAuth protocol from scratch (no `oauth2` crate)
**Rejected.** While the Python, Java, and Go SDKs implement OAuth from scratch, the Rust ecosystem has a mature, well-maintained `oauth2` crate (87M+ downloads) that handles PKCE generation, authorization URL construction, token exchange, client credentials flow, and refresh token flow with strongly-typed APIs. Using it eliminates ~200 lines of hand-rolled protocol code, reduces risk of spec compliance bugs, and provides free support for edge cases like token response parsing. We still implement OIDC discovery, token lifecycle management, caching, and the browser callback server ourselves.

### 4. Separate `reqwest::Client` for token endpoint calls
**Rejected.** An earlier design proposed using a standalone `reqwest::Client` for OAuth token endpoint calls to avoid a circular dependency with `DatabricksHttpClient`. This would duplicate retry logic, timeout configuration, and connection pooling. Instead, we use two-phase initialization (HTTP client created first, auth provider set later via `OnceLock`) and route token endpoint calls through `execute_without_auth()`. This gives a single HTTP client with unified behavior.

### 5. Single OAuthProvider struct handling both U2M and M2M
**Rejected.** U2M and M2M have fundamentally different flows (browser vs. direct token exchange), different refresh strategies (refresh_token vs. re-authenticate), and different caching needs (disk cache vs. none). Separate types are clearer and match the Python SDK's `SessionCredentials` vs `ClientCredentials` split.

---

## New Dependencies

| Crate | Version | Purpose | Size Impact |
|-------|---------|---------|-------------|
| `oauth2` | 5 | OAuth 2.0 protocol: PKCE, token exchange, client credentials, refresh | ~200KB (brings `url`, `sha2`, `rand` as transitive deps) |
| `open` | 5 | Cross-platform `open::that(url)` for browser launch | ~10KB |
| `dirs` | 5 | Cross-platform `config_dir()` for cache path | ~15KB |
| `sha2` | 0.10 | SHA-256 for token cache key generation | Already transitive dep via `oauth2` |

---

## Test Strategy

### Unit Tests

**token.rs:**
- `test_token_fresh_not_expired` -- token with >20min TTL is not stale or expired
- `test_token_stale_threshold` -- token within stale window is stale but not expired
- `test_token_expired_within_buffer` -- token within 40s of expiry is expired
- `test_token_serialization_roundtrip` -- JSON serialize/deserialize preserves all fields

**oidc.rs:**
- `test_discover_workspace_endpoints` -- mock well-known endpoint, verify parsed endpoints
- `test_discover_invalid_response` -- malformed JSON returns error
- `test_discover_http_error` -- 404/500 returns descriptive error

**cache.rs:**
- `test_cache_key_deterministic` -- same inputs produce same filename
- `test_cache_save_load_roundtrip` -- save then load returns same token
- `test_cache_missing_file` -- load returns None for nonexistent cache
- `test_cache_file_permissions` -- saved file has 0o600 permissions
- `test_cache_corrupted_file` -- malformed JSON returns None (not error)

**token_store.rs:**
- `test_store_fresh_token_no_refresh` -- FRESH token returned without calling refresh
- `test_store_expired_triggers_blocking_refresh` -- EXPIRED token blocks and refreshes
- `test_store_concurrent_refresh_single_fetch` -- multiple threads, only one refresh runs
- `test_store_stale_returns_current_token` -- STALE returns current token immediately

**callback.rs:**
- `test_callback_captures_code` -- simulated HTTP GET with code/state returns code
- `test_callback_validates_state` -- mismatched state returns error
- `test_callback_timeout` -- no callback within timeout returns error

**m2m.rs:**
- `test_m2m_token_exchange` -- mock token endpoint, verify grant_type and auth header
- `test_m2m_auto_refresh` -- expired token triggers new client_credentials exchange
- `test_m2m_oidc_discovery` -- discovers token endpoint before exchange

**u2m.rs:**
- `test_u2m_refresh_token_flow` -- mock token endpoint with grant_type=refresh_token
- `test_u2m_cache_hit` -- cached token skips browser flow
- `test_u2m_cache_miss_with_expired_refresh` -- falls through to browser flow

### Wiremock Integration Tests (mocked HTTP, full flow)

Tests in `tests/` using `wiremock` to simulate OIDC discovery and token endpoints. These test the complete wiring from `Database` config through `new_connection()` to `get_auth_header()`.

**M2M full flow:**
- `test_m2m_full_flow_discovery_and_token_exchange` -- wiremock serves OIDC discovery + token endpoint; verify `get_auth_header()` returns valid Bearer token
- `test_m2m_token_refresh_on_expiry` -- first token expires after 1s; verify second `get_auth_header()` call triggers re-exchange and returns new token
- `test_m2m_discovery_failure_propagates` -- wiremock returns 500 on discovery; verify `new_connection()` fails with descriptive error

**U2M refresh flow (no browser -- only tests refresh path):**
- `test_u2m_refresh_token_full_flow` -- pre-populate cache with token that has a refresh_token; wiremock serves token endpoint; verify `get_auth_header()` refreshes via grant_type=refresh_token

### Database Config Validation Tests

Tests in `database.rs` `#[cfg(test)]` verifying the new auth config options are parsed and validated correctly.

**Parsing:**
- `test_set_auth_mechanism_valid` -- setting mechanism to `0` and `11` succeeds, stored as correct enum variant
- `test_set_auth_mechanism_invalid` -- setting mechanism to `99` or a non-integer returns `invalid_argument` error
- `test_set_auth_flow_valid` -- setting flow to `0`, `1`, `2` succeeds
- `test_set_auth_flow_invalid` -- setting flow to `5` or a string returns error

**Validation at `new_connection()`:**
- `test_new_connection_missing_mechanism` -- no mechanism set returns clear error
- `test_new_connection_oauth_missing_flow` -- mechanism=`11` without flow returns error
- `test_new_connection_client_credentials_missing_secret` -- mechanism=`11`, flow=`1` with client_id but no client_secret returns error
- `test_new_connection_pat_missing_token` -- mechanism=`0` without access_token returns error
- `test_new_connection_token_passthrough_missing_token` -- mechanism=`11`, flow=`0` without access_token returns error

### HTTP Client Two-Phase Init Tests

Tests in `client/http.rs` `#[cfg(test)]` verifying the `OnceLock`-based auth provider lifecycle.

- `test_execute_without_auth_works_before_auth_set` -- `execute_without_auth()` succeeds even when no auth provider is set
- `test_execute_fails_before_auth_set` -- `execute()` returns error when auth provider not yet set via `OnceLock`
- `test_execute_succeeds_after_auth_set` -- `set_auth_provider()` then `execute()` succeeds with correct Bearer header
- `test_set_auth_provider_twice_panics_or_errors` -- calling `set_auth_provider()` a second time is rejected (OnceLock semantics)

### End-to-End Tests

End-to-end tests in `tests/oauth_e2e.rs` verify the complete OAuth implementation against real Databricks workspaces:

**M2M (Service Principal) Test:**
- `test_m2m_end_to_end` -- Connects to real Databricks workspace using service principal credentials
- Requires environment variables: `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, `DATABRICKS_WAREHOUSE_ID`
- Marked with `#[ignore]` to prevent running in CI by default
- Verifies: (1) Database config with mechanism=11, flow=1, (2) OIDC discovery, (3) Client credentials token exchange, (4) Connection creation, (5) Query execution (SELECT 1), (6) Result validation
- Run with: `cargo test --test oauth_e2e test_m2m_end_to_end -- --ignored --nocapture`

**U2M (Browser-Based) Test:**
- `test_u2m_end_to_end` -- Manual test requiring interactive browser authentication
- Requires environment variables: `DATABRICKS_HOST`, `DATABRICKS_WAREHOUSE_ID`, `DATABRICKS_CLIENT_ID` (optional, defaults to "databricks-cli")
- Always marked with `#[ignore]` (manual test only)
- Verifies: (1) Database config with mechanism=11, flow=2, (2) OIDC discovery, (3) Token cache check, (4) Browser launch (if needed), (5) Authorization code exchange, (6) Connection creation, (7) Query execution (SELECT 1), (8) Result validation, (9) Token caching to disk
- Run with: `cargo test --test oauth_e2e test_u2m_end_to_end -- --ignored --nocapture`
- Note: Will launch default web browser for user to complete authentication

**Configuration Validation Test:**
- `test_oauth_config_validation` -- Verifies proper error messages for missing/invalid OAuth configuration
- Runs in standard test suite (not ignored)
- Tests: missing auth.flow when mechanism=OAuth, missing client_secret for M2M, invalid mechanism/flow values

---

## Implementation Phases

| Phase | Scope | Dependencies |
|-------|-------|-------------|
| **1. Foundation** | `token.rs`, `oidc.rs`, `cache.rs`, Cargo.toml updates (`oauth2`, `sha2`, `open`, `dirs`) | None |
| **2. M2M** | `token_store.rs`, `m2m.rs`, `oauth/mod.rs` | Phase 1 |
| **3. U2M** | `callback.rs`, `u2m.rs` | Phase 1 + 2 |
| **4. Integration** | `database.rs` changes, `auth/mod.rs` re-exports, config options | Phase 1 + 2 + 3 |
