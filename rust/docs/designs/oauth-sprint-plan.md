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

# Sprint Plan: OAuth U2M + M2M Implementation

**Sprint dates:** 2026-03-07 to 2026-03-20
**Sprint goal:** Implement complete OAuth 2.0 authentication (U2M + M2M) for the Rust ADBC driver as described in the [OAuth design doc](oauth-u2m-m2m-design.md).

---

## Story

**Title:** Implement OAuth U2M and M2M authentication

**Description:**
Add OAuth 2.0 support to the Rust ADBC driver, covering:
- M2M (Client Credentials) flow for service principal authentication
- U2M (Authorization Code + PKCE) flow for interactive browser-based login
- Shared infrastructure: OIDC discovery, token lifecycle management, file-based token caching
- Integration with `Database` config and `DatabricksHttpClient` (two-phase init via `OnceLock`)
- ODBC-aligned numeric config values (`AuthMech`/`Auth_Flow` scheme)

**Acceptance Criteria:**
- [ ] M2M flow works end-to-end: `Database` config -> `new_connection()` -> `get_auth_header()` returns valid Bearer token from client credentials exchange
- [ ] U2M flow works end-to-end: browser launch -> callback server captures code -> PKCE token exchange -> cached token reused on subsequent connections
- [ ] Token refresh state machine works: FRESH (no-op) -> STALE (background refresh) -> EXPIRED (blocking refresh)
- [ ] File-based token cache persists U2M tokens across connections with correct permissions (0o600)
- [ ] `DatabricksHttpClient` supports two-phase init: created without auth, auth set later via `OnceLock`
- [ ] All config options from the design doc are parseable via `set_option()`
- [ ] Invalid config combinations fail with clear error messages at `new_connection()`
- [ ] `cargo fmt`, `cargo clippy -- -D warnings`, `cargo test` all pass

---

## Sub-Tasks

### Task 1: Foundation + HTTP Client Changes

**Scope:** Shared OAuth infrastructure and modifications to existing code to support two-phase auth initialization.

**Files to create:**
- `src/auth/oauth/mod.rs` — module root, re-exports
- `src/auth/oauth/token.rs` — `OAuthToken` struct with `is_expired()`, `is_stale()`, JSON serialization
- `src/auth/oauth/oidc.rs` — `OidcEndpoints` struct, `discover()` function hitting `{host}/oidc/.well-known/oauth-authorization-server`
- `src/auth/oauth/cache.rs` — `TokenCache` with `load()`/`save()`, SHA-256 hashed filenames, 0o600 permissions
- `src/auth/oauth/token_store.rs` — `TokenStore` with `RwLock<Option<OAuthToken>>`, `AtomicBool` for refresh coordination, FRESH/STALE/EXPIRED state machine

**Files to modify:**
- `Cargo.toml` — add `oauth2 = "5"`, `sha2 = "0.10"`, `open = "5"`, `dirs = "5"`
- `src/client/http.rs` — change `auth_provider` from `Arc<dyn AuthProvider>` to `OnceLock<Arc<dyn AuthProvider>>`, add `set_auth_provider()`, update `execute()` to read from `OnceLock`
- `src/database.rs` — add `AuthMechanism`/`AuthFlow` enums (with `TryFrom<i64>`), new config fields (`auth_mechanism`, `auth_flow`, `auth_client_id`, `auth_client_secret`, `auth_scopes`, `auth_token_endpoint`, `auth_redirect_port`), `set_option()`/`get_option_string()` for all new keys
- `src/auth/mod.rs` — add `pub mod oauth;`, remove old `pub use oauth::OAuthCredentials`, delete `src/auth/oauth.rs` (replaced by directory module)

**Tests:**
- `token.rs`: `test_token_fresh_not_expired`, `test_token_stale_threshold`, `test_token_expired_within_buffer`, `test_token_serialization_roundtrip`
- `oidc.rs`: `test_discover_workspace_endpoints`, `test_discover_invalid_response`, `test_discover_http_error`
- `cache.rs`: `test_cache_key_deterministic`, `test_cache_save_load_roundtrip`, `test_cache_missing_file`, `test_cache_file_permissions`, `test_cache_corrupted_file`
- `token_store.rs`: `test_store_fresh_token_no_refresh`, `test_store_expired_triggers_blocking_refresh`, `test_store_concurrent_refresh_single_fetch`, `test_store_stale_returns_current_token`
- `http.rs`: `test_execute_without_auth_works_before_auth_set`, `test_execute_fails_before_auth_set`, `test_execute_succeeds_after_auth_set`, `test_set_auth_provider_twice_panics_or_errors`
- `database.rs`: `test_set_auth_mechanism_valid`, `test_set_auth_mechanism_invalid`, `test_set_auth_flow_valid`, `test_set_auth_flow_invalid`, `test_new_connection_missing_mechanism`, `test_new_connection_oauth_missing_flow`, `test_new_connection_pat_missing_token`

**Definition of done:** All foundation modules compile, unit tests pass, `DatabricksHttpClient` two-phase init works, database config parsing works for all auth options.

---

### Task 2: M2M Provider (Client Credentials)

**Scope:** Implement the M2M OAuth flow using `oauth2::BasicClient` for client credentials exchange.

**Files to create:**
- `src/auth/oauth/m2m.rs` — `ClientCredentialsProvider` implementing `AuthProvider`

**Files to modify:**
- `src/auth/oauth/mod.rs` — add `pub mod m2m;`, re-export `ClientCredentialsProvider`
- `src/auth/mod.rs` — re-export `ClientCredentialsProvider`
- `src/database.rs` — wire `AuthFlow::ClientCredentials` match arm in `new_connection()` to create `ClientCredentialsProvider`

**Implementation details:**
- Construct `oauth2::BasicClient` from OIDC-discovered endpoints
- Implement `oauth2` HTTP adapter that routes through `DatabricksHttpClient::execute_without_auth()`
- Use `TokenStore` for token lifecycle (no disk cache for M2M)
- `get_auth_header()` → `TokenStore::get_or_refresh()` → `client.exchange_client_credentials()` if needed

**Tests:**
- `m2m.rs`: `test_m2m_token_exchange`, `test_m2m_auto_refresh`, `test_m2m_oidc_discovery`
- Wiremock integration (`tests/`): `test_m2m_full_flow_discovery_and_token_exchange`, `test_m2m_token_refresh_on_expiry`, `test_m2m_discovery_failure_propagates`
- Database validation: `test_new_connection_client_credentials_missing_secret`

**Definition of done:** M2M flow works end-to-end from `Database` config through `new_connection()` to `get_auth_header()`. Wiremock tests verify the full OIDC discovery -> token exchange -> refresh cycle.

---

### Task 3: U2M Provider (Authorization Code + PKCE)

**Scope:** Implement the U2M OAuth flow with browser-based login, PKCE, callback server, and token caching.

**Files to create:**
- `src/auth/oauth/callback.rs` — `CallbackServer` with localhost HTTP listener, state validation, timeout
- `src/auth/oauth/u2m.rs` — `AuthorizationCodeProvider` implementing `AuthProvider`

**Files to modify:**
- `src/auth/oauth/mod.rs` — add `pub mod callback;`, `pub mod u2m;`, re-export `AuthorizationCodeProvider`
- `src/auth/mod.rs` — re-export `AuthorizationCodeProvider`
- `src/database.rs` — wire `AuthFlow::Browser` match arm in `new_connection()` to create `AuthorizationCodeProvider`

**Implementation details:**
- On creation: try loading cached token via `TokenCache::load()`
- If cached token has valid `refresh_token`: store in `TokenStore`, refresh on first `get_auth_header()` if stale/expired
- If no cache: generate PKCE via `PkceCodeChallenge::new_random_sha256()`, build auth URL via `client.authorize_url()`, start `CallbackServer`, launch browser via `open::that()`, wait for callback, exchange code via `client.exchange_code().set_pkce_verifier()`
- Save token to cache after every successful acquisition/refresh
- Refresh uses `client.exchange_refresh_token()` through `DatabricksHttpClient::execute_without_auth()`
- If refresh token is expired, fall back to full browser flow

**Tests:**
- `callback.rs`: `test_callback_captures_code`, `test_callback_validates_state`, `test_callback_timeout`
- `u2m.rs`: `test_u2m_refresh_token_flow`, `test_u2m_cache_hit`, `test_u2m_cache_miss_with_expired_refresh`
- Wiremock integration: `test_u2m_refresh_token_full_flow`
- Database validation: `test_new_connection_token_passthrough_missing_token`
- E2E (ignored): `test_m2m_end_to_end`, `test_u2m_end_to_end`

**Definition of done:** U2M flow works end-to-end. Token cache persists across connections. Refresh path works via wiremock tests. Browser flow verified manually via `#[ignore]` E2E test.
