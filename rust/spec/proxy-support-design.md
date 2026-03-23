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

# HTTP Proxy Support Design

## Overview

Add HTTP/HTTPS proxy support to the Rust ADBC driver, enabling use in corporate network environments that require outbound traffic to route through a proxy server. The driver will support proxy configuration via environment variables (`HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY`) and explicit ADBC configuration parameters, including authenticated proxies.

## Motivation

Enterprise deployments commonly require all outbound HTTP traffic to pass through a corporate proxy. Without explicit proxy support, the driver cannot be used in these environments. The C# driver already has full proxy support via `adbc.proxy_options.*` parameters; this design brings the Rust driver to parity.

## Current State

### reqwest's Built-in Proxy Behavior

By default, `reqwest::Client` automatically reads `HTTP_PROXY`, `HTTPS_PROXY`, and `NO_PROXY` environment variables and routes requests accordingly. **This means basic proxy support via env vars already works today with zero code changes.** However:

- There is no way to configure proxy via ADBC parameters (required for programmatic use)
- There is no support for authenticated proxies (username/password)
- There is no way to override or disable the env var behavior
- There is no logging/visibility into whether a proxy is being used

### Existing Architecture

All HTTP requests flow through `DatabricksHttpClient` (`src/client/http.rs`), which wraps a single `reqwest::Client`. This includes:

- **SEA API calls** (`execute`, with auth) — session management, statement execution, chunk link fetching
- **CloudFetch downloads** (`execute_without_auth`) — presigned URL downloads
- **OAuth token requests** (`execute_without_auth`) — M2M/U2M token endpoint calls

Because everything goes through one client, proxy configuration only needs to be applied in one place: `DatabricksHttpClient::new()`.

## Design

### Configuration Parameters

New ADBC database options under the `databricks.http.proxy` namespace:

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| `databricks.http.proxy.url` | String | Proxy URL (e.g., `http://proxy:8080`) | None (use env vars) |
| `databricks.http.proxy.username` | String | Proxy authentication username | None |
| `databricks.http.proxy.password` | String | Proxy authentication password | None |
| `databricks.http.proxy.bypass_hosts` | String | Comma-separated list of hosts/domains to bypass | None (use env var) |

#### Behavior Matrix

| `proxy.url` set? | `HTTP_PROXY` env set? | Behavior |
|-------------------|-----------------------|----------|
| No | No | Direct connection (no proxy) |
| No | Yes | reqwest uses env var automatically |
| Yes | Don't care | Explicit config takes precedence; env vars ignored |

When `databricks.http.proxy.url` is set, the driver calls `reqwest::Client::builder().proxy(...)` explicitly, which overrides reqwest's default env var behavior. When `databricks.http.proxy.bypass_hosts` is set alongside `proxy.url`, those hosts bypass the explicit proxy.

When no explicit proxy config is provided, reqwest's default env var behavior is preserved — this is the zero-config path for users who already have `HTTP_PROXY`/`HTTPS_PROXY` set in their environment.

### Code Changes

#### 1. `HttpClientConfig` — Add proxy fields

```rust
// src/client/http.rs

#[derive(Debug, Clone, Default)]
pub struct ProxyConfig {
    /// Proxy URL (e.g., "http://proxy.corp.example.com:8080").
    /// When set, overrides HTTP_PROXY/HTTPS_PROXY environment variables.
    pub url: Option<String>,
    /// Username for proxy authentication.
    pub username: Option<String>,
    /// Password for proxy authentication.
    pub password: Option<String>,
    /// Comma-separated list of hosts/domains to bypass the proxy
    /// (e.g., "localhost,*.internal.corp,.example.com").
    pub bypass_hosts: Option<String>,
}

#[derive(Debug, Clone)]
pub struct HttpClientConfig {
    // ... existing fields ...
    pub proxy: ProxyConfig,
}
```

#### 2. `DatabricksHttpClient::new()` — Apply proxy to reqwest builder

```rust
// src/client/http.rs

pub fn new(config: HttpClientConfig) -> Result<Self> {
    let mut builder = Client::builder()
        .connect_timeout(config.connect_timeout)
        .timeout(config.read_timeout)
        .pool_max_idle_per_host(config.max_connections_per_host)
        .user_agent(&config.user_agent);

    // Apply proxy configuration
    if let Some(ref proxy_url) = config.proxy.url {
        let mut proxy = reqwest::Proxy::all(proxy_url).map_err(|e| {
            DatabricksErrorHelper::invalid_argument()
                .message(format!("Invalid proxy URL '{}': {}", proxy_url, e))
        })?;

        // Add basic auth if credentials provided
        if let Some(ref username) = config.proxy.username {
            let password = config.proxy.password.as_deref().unwrap_or("");
            proxy = proxy.basic_auth(username, password);
        }

        // Apply bypass_hosts list to the proxy
        if let Some(ref bypass_hosts) = config.proxy.bypass_hosts {
            proxy = proxy.no_proxy(reqwest::NoProxy::from_string(bypass_hosts));
        }

        builder = builder.proxy(proxy);

        debug!(
            "HTTP client configured with proxy: {} (bypass_hosts: {:?})",
            proxy_url, config.proxy.bypass_hosts
        );
    } else {
        debug!("HTTP client using default proxy behavior (env vars)");
    }

    let client = builder.build().map_err(|e| {
        DatabricksErrorHelper::io()
            .message(format!("Failed to create HTTP client: {}", e))
    })?;

    Ok(Self {
        client,
        config,
        auth_provider: OnceLock::new(),
    })
}
```

#### 3. `Database` — Parse proxy options

```rust
// src/database.rs — inside set_option() match block

// Proxy configuration
"databricks.http.proxy.url" => {
    if let OptionValue::String(v) = value {
        self.http_config.proxy.url = Some(v);
        Ok(())
    } else {
        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
    }
}
"databricks.http.proxy.username" => {
    if let OptionValue::String(v) = value {
        self.http_config.proxy.username = Some(v);
        Ok(())
    } else {
        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
    }
}
"databricks.http.proxy.password" => {
    if let OptionValue::String(v) = value {
        self.http_config.proxy.password = Some(v);
        Ok(())
    } else {
        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
    }
}
"databricks.http.proxy.bypass_hosts" => {
    if let OptionValue::String(v) = value {
        self.http_config.proxy.bypass_hosts = Some(v);
        Ok(())
    } else {
        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
    }
}
```

Also add corresponding `get_option_string` entries for reading back proxy config.

### No Cargo.toml Changes Required

`reqwest::Proxy` and `reqwest::NoProxy` are available with no additional feature flags. The `socks` feature is not needed since we're only supporting HTTP/HTTPS proxies.

## Validation

- `proxy.url` must use `http://` or `https://` scheme. Other schemes (e.g., `socks5://`) produce `InvalidArgument` errors.
- `proxy.url` must be a valid URL parseable by `reqwest::Proxy::all()`. Invalid URLs produce `InvalidArgument` errors at client construction time (i.e., during `Database::new_connection()`), not at first request.
- `proxy.username` without `proxy.url` is ignored with a `warn!` log.
- `proxy.password` without `proxy.username` is ignored with a `warn!` log.
- `proxy.bypass_hosts` without `proxy.url` is ignored with a `warn!` log.
- If the proxy URL contains embedded credentials (e.g., `http://user:pass@proxy:8080`) AND explicit `proxy.username`/`proxy.password` are also set, the explicit parameters take precedence (with a `warn!` log).
- `proxy.bypass_hosts` entries are trimmed of whitespace to handle `"host1, host2"` style input.

## Testing

### Unit Tests (`src/client/http.rs`)

1. **`test_proxy_config_default`** — `ProxyConfig::default()` has all `None` fields.
2. **`test_http_client_with_proxy`** — Client builds successfully with a proxy URL.
3. **`test_http_client_with_authenticated_proxy`** — Client builds with proxy URL + credentials.
4. **`test_http_client_with_invalid_proxy_url`** — Returns `InvalidArgument` error.
5. **`test_http_client_with_bypass_hosts`** — Client builds with proxy URL + bypass_hosts list.

### Unit Tests (`src/database.rs`)

6. **`test_set_proxy_options`** — All four proxy options parse correctly via `set_option`.
7. **`test_get_proxy_options`** — Proxy options are readable via `get_option_string`.

### E2E Proxy Tests (CI workflow)

A new GitHub Actions workflow (`rust-e2e.yml`) provides a home for Rust e2e tests that require external infrastructure. The proxy tests are the first job; other e2e jobs (e.g., CloudFetch, metadata, OAuth) can be added to the same workflow later.

#### Workflow Design

```yaml
# .github/workflows/rust-e2e.yml
name: Rust E2E Tests

on:
  push:
    branches: [main]
    paths:
      - "rust/**"
      - .github/workflows/rust-e2e.yml
  pull_request:
    branches: [main]
    paths:
      - "rust/**"
      - .github/workflows/rust-e2e.yml

jobs:
  proxy:
    name: "E2E/Proxy"
    runs-on: ubuntu-latest
    env:
      DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
      DATABRICKS_HTTP_PATH: ${{ secrets.TEST_PECO_WAREHOUSE_HTTP_PATH }}
      DATABRICKS_TEST_CLIENT_ID: ${{ secrets.DATABRICKS_TEST_CLIENT_ID }}
      DATABRICKS_TEST_CLIENT_SECRET: ${{ secrets.DATABRICKS_TEST_CLIENT_SECRET }}

    services:
      # Unauthenticated proxy
      squid:
        image: ubuntu/squid:latest
        ports:
          - 3128:3128

      # Authenticated proxy (basic auth)
      squid-auth:
        image: ubuntu/squid:latest
        ports:
          - 3129:3128
        volumes:
          - ./rust/ci/proxy/squid-auth.conf:/etc/squid/squid.conf
          - ./rust/ci/proxy/htpasswd:/etc/squid/htpasswd

    steps:
      - uses: actions/checkout@v4
      - uses: actions-rust-lang/setup-rust-toolchain@v1

      - name: Run proxy e2e tests
        working-directory: rust
        run: cargo test --test proxy_e2e -- --ignored

      - name: Verify proxy was used (unauthenticated)
        run: |
          docker compose logs squid | grep "CONNECT $DATABRICKS_HOST" \
            || (echo "FAIL: No CONNECT entry in squid access log" && exit 1)

      - name: Verify proxy was used (authenticated)
        run: |
          docker compose logs squid-auth | grep "CONNECT $DATABRICKS_HOST" \
            || (echo "FAIL: No CONNECT entry in squid-auth access log" && exit 1)

  # Future e2e jobs can be added here, e.g.:
  # cloudfetch:
  #   name: "E2E/CloudFetch"
  #   ...
```

#### Proxy Verification Strategy

Tests verify that requests actually routed through the proxy by checking **Squid's access log** after each test. Squid logs every CONNECT tunnel it establishes, e.g.:

```
1711234567.890    200 TCP_TUNNEL/200 12345 CONNECT my-workspace.databricks.com:443 - HIER_DIRECT/1.2.3.4 -
```

The CI step reads the container logs after tests complete and asserts the Databricks host appears in a CONNECT entry. If the driver connected directly (bypassing the proxy), the log would be empty and the step fails.

#### E2E Test Cases

8. **`test_connection_through_proxy`** — Set `databricks.http.proxy.url = http://localhost:3128`, connect to Databricks, run `SELECT 1`. CI step verifies Squid access log contains a CONNECT to the Databricks host.

9. **`test_connection_through_authenticated_proxy`** — Set `proxy.url = http://localhost:3129` with `proxy.username`/`proxy.password`, connect and query. Verifies authenticated proxy works (Squid returns 200, not 407).

10. **`test_proxy_bypass_hosts`** — Set `proxy.url = http://localhost:3128` and `proxy.bypass_hosts` to include the Databricks host. Connect and query. Verify the Squid access log does **not** contain a CONNECT for the Databricks host (traffic went direct).

#### Proxy Configuration Files

```
rust/ci/proxy/
├── squid-auth.conf    # Squid config requiring basic auth
└── htpasswd           # Test credentials (testuser:testpass)
```

`squid-auth.conf`:
```
auth_param basic program /usr/lib/squid/basic_ncsa_auth /etc/squid/htpasswd
auth_param basic realm Proxy Auth
acl authenticated proxy_auth REQUIRED
http_access allow authenticated
http_access deny all
```

#### Files Added

| File | Purpose |
|------|---------|
| `.github/workflows/rust-e2e.yml` | CI workflow with Squid sidecar |
| `rust/ci/proxy/squid-auth.conf` | Squid config for authenticated proxy |
| `rust/ci/proxy/htpasswd` | htpasswd file with test credentials |
| `rust/tests/proxy_e2e.rs` | E2E test cases (marked `#[ignore]`) |

## Cross-Driver Comparison

| Feature | Rust (this design) | C# | Go |
|---------|-------------------|----|----|
| Env var proxy | Yes (reqwest default) | Yes | Yes (`ProxyFromEnvironment`) |
| Explicit proxy URL | `databricks.http.proxy.url` | `adbc.proxy_options.proxy_host/port` | No |
| Authenticated proxy | `proxy.username` / `proxy.password` | `proxy_uid` / `proxy_pwd` | No |
| Proxy bypass list | `proxy.bypass_hosts` | `proxy_ignore_list` | `NO_PROXY` env only |
| SOCKS5 | No | No | No |
| Enable/disable toggle | No (absence of URL = disabled) | `use_proxy` boolean | No |

### Design Difference from C#

The C# driver uses separate `proxy_host` and `proxy_port` fields plus a `use_proxy` boolean toggle. This design uses a single `proxy.url` field instead because:

1. A URL naturally encodes scheme + host + port (e.g., `http://proxy:8080`)
2. No toggle needed — absence of URL means no explicit proxy
3. Simpler config surface (4 params vs 7)

## Files Changed

| File | Change |
|------|--------|
| `rust/src/client/http.rs` | Add `ProxyConfig` struct, update `HttpClientConfig`, update `DatabricksHttpClient::new()`, add unit tests |
| `rust/src/database.rs` | Parse and get proxy options in `set_option`/`get_option_string` |
| `.github/workflows/rust-e2e.yml` | New CI workflow with Squid proxy sidecar |
| `rust/ci/proxy/squid-auth.conf` | Squid config for authenticated proxy testing |
| `rust/ci/proxy/htpasswd` | Test credentials for authenticated proxy |
| `rust/tests/proxy_e2e.rs` | E2E proxy test cases |

## Scope Exclusions

- **SOCKS5 proxy**: Not needed per requirements. Can be added later by enabling the `socks` reqwest feature.
- **Digest, NTLM, or Kerberos proxy auth**: Only basic auth is supported. These could be added in the future via reqwest's `custom_http_auth` on `Proxy` if enterprise demand exists.
- **Per-request proxy**: All requests use the same proxy. No mechanism for routing different requests through different proxies.
- **Custom TLS for proxy**: No support for custom CA certificates to trust the proxy's TLS interception cert. This could be a follow-up (similar to C#'s `tls.trusted_certificate_path`).
- **Proxy auto-configuration (PAC)**: Not supported. Users must provide explicit proxy URLs.
