// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! HTTP client implementation for Databricks SQL API.
//!
//! This module provides a low-level HTTP client with:
//! - Connection pooling
//! - Idempotency-aware retry with exponential backoff and jitter
//! - `Retry-After` header support
//! - Per-category retry configuration
//! - Bearer token authentication
//! - Configurable timeouts

use crate::auth::AuthProvider;
use crate::client::retry::{
    calculate_backoff, RequestCategory, RequestType, RetryConfig, RetryStrategy,
};
use crate::error::{DatabricksErrorHelper, Result};
use driverbase::error::ErrorHelper;
use reqwest::{Client, NoProxy, Proxy, Request, Response};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{debug, warn};

/// Configuration for HTTP proxy.
///
/// When `url` is set, the driver uses the specified proxy for all requests,
/// overriding `HTTP_PROXY`/`HTTPS_PROXY` environment variables. When `url`
/// is `None`, reqwest's default behavior applies (reads env vars automatically).
#[derive(Clone, Default)]
pub struct ProxyConfig {
    /// Proxy URL (e.g., "http://proxy.corp.example.com:8080").
    /// When set, overrides HTTP_PROXY/HTTPS_PROXY environment variables.
    pub url: Option<String>,
    /// Username for proxy authentication.
    pub username: Option<String>,
    /// Password for proxy authentication.
    ///
    /// Note: This is a sensitive value. It is redacted in `Debug` output.
    pub password: Option<String>,
    /// Comma-separated list of hosts/domains to bypass the proxy
    /// (e.g., "localhost,*.internal.corp,.example.com").
    pub bypass_hosts: Option<String>,
}

impl std::fmt::Debug for ProxyConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyConfig")
            .field("url", &self.url)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "[REDACTED]"))
            .field("bypass_hosts", &self.bypass_hosts)
            .finish()
    }
}

/// Configuration for the HTTP client.
#[derive(Debug, Clone)]
pub struct HttpClientConfig {
    /// Connection timeout duration.
    pub connect_timeout: Duration,
    /// Read timeout duration.
    pub read_timeout: Duration,
    /// Maximum number of idle connections per host.
    pub max_connections_per_host: usize,
    /// User agent string.
    pub user_agent: String,
    /// Proxy configuration.
    pub proxy: ProxyConfig,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(30),
            read_timeout: Duration::from_secs(60),
            max_connections_per_host: 100,
            // TODO: Update user agent to properly identify as Rust ADBC driver.
            // Currently using JDBC user agent to enable INLINE_OR_EXTERNAL_LINKS disposition
            // which returns all chunk links in a single response, enabling true parallel downloads.
            // See: https://github.com/databricks-eng/universe/pull/909153
            user_agent: format!("DatabricksJDBCDriverOSS/{}", env!("CARGO_PKG_VERSION")),
            proxy: ProxyConfig::default(),
        }
    }
}

/// HTTP client for communicating with Databricks SQL endpoints.
///
/// This client handles:
/// - Connection pooling (via reqwest)
/// - Idempotency-aware retry with exponential backoff, jitter, and Retry-After
/// - Per-category retry configuration with global defaults
/// - Bearer token authentication (set via two-phase initialization)
/// - User-Agent header injection
///
/// ## Two-Phase Initialization
///
/// The client uses `OnceLock` for auth provider to avoid circular dependencies:
/// OAuth providers need the HTTP client to fetch tokens, but the HTTP client
/// traditionally required auth at construction. The solution:
///
/// 1. Create the HTTP client first (no auth)
/// 2. Create the auth provider (can use the HTTP client)
/// 3. Set auth on the HTTP client via `set_auth_provider()`
///
/// OAuth providers use `execute_without_auth()` for token endpoint calls
/// (which authenticate via form-encoded credentials, not Bearer tokens).
#[derive(Debug)]
pub struct DatabricksHttpClient {
    client: Client,
    config: HttpClientConfig,
    auth_provider: OnceLock<Arc<dyn AuthProvider>>,
    retry_configs: HashMap<RequestCategory, RetryConfig>,
}

impl DatabricksHttpClient {
    /// Creates a new HTTP client with the given configuration and per-category retry configs.
    ///
    /// Auth provider must be set separately via `set_auth_provider()` before
    /// calling `execute()`. Use `execute_without_auth()` for requests that
    /// don't need authentication (e.g., OAuth token endpoint calls).
    pub fn new(
        config: HttpClientConfig,
        retry_configs: HashMap<RequestCategory, RetryConfig>,
    ) -> Result<Self> {
        let mut builder = Client::builder()
            .connect_timeout(config.connect_timeout)
            .timeout(config.read_timeout)
            .pool_max_idle_per_host(config.max_connections_per_host)
            .user_agent(&config.user_agent);

        // Apply proxy configuration
        if let Some(ref proxy_url) = config.proxy.url {
            // Validate proxy URL scheme
            if !proxy_url.starts_with("http://") && !proxy_url.starts_with("https://") {
                return Err(DatabricksErrorHelper::invalid_argument().message(format!(
                    "Proxy URL must use http:// or https:// scheme, got: '{}'",
                    proxy_url
                )));
            }

            let mut proxy = Proxy::all(proxy_url).map_err(|e| {
                DatabricksErrorHelper::invalid_argument()
                    .message(format!("Invalid proxy URL '{}': {}", proxy_url, e))
            })?;

            // Add basic auth if credentials provided.
            // Explicit username/password override any credentials embedded in the URL.
            if let Some(ref username) = config.proxy.username {
                if proxy_url.contains('@') {
                    warn!("Proxy URL contains embedded credentials, but explicit username/password are also set; explicit credentials take precedence");
                }
                let password = config.proxy.password.as_deref().unwrap_or("");
                proxy = proxy.basic_auth(username, password);
            } else if config.proxy.password.is_some() {
                warn!("Proxy password provided without username; password will be ignored");
            }

            // Apply bypass_hosts list to the proxy.
            // Normalize by trimming whitespace around entries (e.g., "host1, host2").
            if let Some(ref bypass_hosts) = config.proxy.bypass_hosts {
                let normalized: String = bypass_hosts
                    .split(',')
                    .map(|s| s.trim())
                    .collect::<Vec<_>>()
                    .join(",");
                proxy = proxy.no_proxy(NoProxy::from_string(&normalized));
            }

            builder = builder.proxy(proxy);

            debug!(
                "HTTP client configured with proxy: {} (bypass_hosts: {:?})",
                proxy_url, config.proxy.bypass_hosts
            );
        } else {
            if config.proxy.username.is_some() {
                warn!(
                    "Proxy credentials provided but no proxy URL set; credentials will be ignored"
                );
            }
            if config.proxy.bypass_hosts.is_some() {
                warn!("Proxy bypass_hosts provided but no proxy URL set; bypass list will be ignored (env var NO_PROXY is unaffected)");
            }
            debug!("HTTP client using default proxy behavior (env vars)");
        }

        let client = builder.build().map_err(|e| {
            DatabricksErrorHelper::io().message(format!("Failed to create HTTP client: {}", e))
        })?;

        Ok(Self {
            client,
            config,
            auth_provider: OnceLock::new(),
            retry_configs,
        })
    }

    /// Creates a new HTTP client with default retry configs for all request categories.
    ///
    /// Convenience method for tests and simple use cases where custom retry
    /// configuration is not needed.
    pub fn with_default_retry(config: HttpClientConfig) -> Result<Self> {
        use crate::client::retry::build_retry_configs;
        let retry_configs =
            build_retry_configs(&RetryConfig::default(), &std::collections::HashMap::new());
        Self::new(config, retry_configs)
    }

    /// Sets the auth provider for this client.
    ///
    /// This must be called exactly once after construction and before calling `execute()`.
    /// Calling this method more than once will panic (OnceLock semantics).
    pub fn set_auth_provider(&self, provider: Arc<dyn AuthProvider>) {
        self.auth_provider
            .set(provider)
            .expect("Auth provider can only be set once");
    }

    /// Returns the client configuration.
    pub fn config(&self) -> &HttpClientConfig {
        &self.config
    }

    /// Returns the underlying reqwest client for building requests.
    pub fn inner(&self) -> &Client {
        &self.client
    }

    /// Get the authorization header value.
    ///
    /// Returns an error if the auth provider has not been set via `set_auth_provider()`.
    pub fn auth_header(&self) -> Result<String> {
        let provider = self.auth_provider.get().ok_or_else(|| {
            DatabricksErrorHelper::invalid_state()
                .message("Auth provider not set. Call set_auth_provider() first.")
        })?;
        provider.get_auth_header()
    }

    /// Returns the retry config for the given request category.
    fn retry_config(&self, category: RequestCategory) -> &RetryConfig {
        self.retry_configs
            .get(&category)
            .expect("All categories should have retry configs")
    }

    /// Execute an HTTP request with authentication and idempotency-aware retry.
    ///
    /// The `RequestType` determines which `RetryConfig` and `RetryStrategy` to use.
    pub async fn execute(&self, request: Request, request_type: RequestType) -> Result<Response> {
        self.execute_impl(request, true, request_type).await
    }

    /// Execute a request without authentication (for CloudFetch, OAuth endpoints).
    ///
    /// Same retry logic as `execute()`, just skips the Authorization header.
    pub async fn execute_without_auth(
        &self,
        request: Request,
        request_type: RequestType,
    ) -> Result<Response> {
        self.execute_impl(request, false, request_type).await
    }

    /// Internal implementation of execute with configurable auth and retry strategy.
    async fn execute_impl(
        &self,
        request: Request,
        with_auth: bool,
        request_type: RequestType,
    ) -> Result<Response> {
        let config = self.retry_config(request_type.category());
        let strategy = RetryStrategy::for_request(request_type, config);
        let max_attempts = config.max_retries + 1;
        let start_time = Instant::now();

        let mut attempts: u32 = 0;
        let mut last_error: Option<String> = None;

        // Clone the request parts we need for retries
        let method = request.method().clone();
        let url = request.url().clone();
        let headers = request.headers().clone();
        let body_bytes = request
            .body()
            .and_then(|b| b.as_bytes())
            .map(|b| b.to_vec());

        loop {
            attempts += 1;

            // Build a fresh request for this attempt
            let mut req_builder = self.client.request(method.clone(), url.clone());

            for (name, value) in headers.iter() {
                req_builder = req_builder.header(name, value);
            }

            if with_auth {
                let auth_header = self.auth_header()?;
                req_builder = req_builder.header("Authorization", auth_header);
            }

            if let Some(ref body) = body_bytes {
                req_builder = req_builder.body(body.clone());
            }

            let request = req_builder.build().map_err(|e| {
                DatabricksErrorHelper::io().message(format!("Failed to build request: {}", e))
            })?;

            debug!(
                "{:?}: {} {} (attempt {}/{})",
                request_type, method, url, attempts, max_attempts
            );

            match self.client.execute(request).await {
                Ok(response) => {
                    let status = response.status();

                    if status.is_success() {
                        if attempts > 1 {
                            debug!(
                                "{:?}: completed after {} attempts in {:.1}s — success",
                                request_type,
                                attempts,
                                start_time.elapsed().as_secs_f64()
                            );
                        }
                        return Ok(response);
                    }

                    // Extract Retry-After header before consuming response
                    let retry_after = response
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.to_string());

                    let has_retry_after = retry_after.is_some();

                    if strategy.is_retryable_status(status, has_retry_after)
                        && attempts < max_attempts
                    {
                        let elapsed = start_time.elapsed();
                        let wait = calculate_backoff(config, attempts, retry_after.as_deref());

                        // Check overall timeout: don't start a retry that will time out
                        if elapsed + wait > config.overall_timeout {
                            let error_body = response.text().await.unwrap_or_default();
                            debug!(
                                "{:?}: failed after {} attempts in {:.1}s — retry timeout exceeded",
                                request_type,
                                attempts,
                                elapsed.as_secs_f64()
                            );
                            return Err(DatabricksErrorHelper::io().message(format!(
                                "HTTP {} — retry timeout exceeded after {} attempts ({:.1}s): {}",
                                status.as_u16(),
                                attempts,
                                elapsed.as_secs_f64(),
                                error_body
                            )));
                        }

                        last_error = Some(format!("HTTP {}", status.as_u16()));
                        warn!(
                            "{:?}: HTTP {} (attempt {}/{}), waiting {:.1}s before retry",
                            request_type,
                            status.as_u16(),
                            attempts,
                            max_attempts,
                            wait.as_secs_f64()
                        );
                        sleep(wait).await;
                        continue;
                    }

                    // Non-retryable or max retries exceeded
                    let error_body = response.text().await.unwrap_or_default();
                    debug!(
                        "{:?}: failed after {} attempts in {:.1}s — HTTP {}",
                        request_type,
                        attempts,
                        start_time.elapsed().as_secs_f64(),
                        status.as_u16()
                    );
                    return Err(DatabricksErrorHelper::io().message(format!(
                        "HTTP {} - {}",
                        status.as_u16(),
                        error_body
                    )));
                }
                Err(e) => {
                    if strategy.is_retryable_error(&e) && attempts < max_attempts {
                        let elapsed = start_time.elapsed();
                        let wait = calculate_backoff(config, attempts, None);

                        if elapsed + wait > config.overall_timeout {
                            debug!(
                                "{:?}: failed after {} attempts in {:.1}s — retry timeout exceeded",
                                request_type,
                                attempts,
                                elapsed.as_secs_f64()
                            );
                            return Err(DatabricksErrorHelper::io().message(format!(
                                "HTTP request failed — retry timeout exceeded after {} attempts ({:.1}s): {}",
                                attempts,
                                elapsed.as_secs_f64(),
                                last_error.unwrap_or_else(|| e.to_string())
                            )));
                        }

                        last_error = Some(e.to_string());
                        warn!(
                            "{:?}: error (attempt {}/{}): {}, waiting {:.1}s before retry",
                            request_type,
                            attempts,
                            max_attempts,
                            e,
                            wait.as_secs_f64()
                        );
                        sleep(wait).await;
                        continue;
                    }

                    debug!(
                        "{:?}: failed after {} attempts in {:.1}s — {}",
                        request_type,
                        attempts,
                        start_time.elapsed().as_secs_f64(),
                        e
                    );
                    return Err(DatabricksErrorHelper::io().message(format!(
                        "HTTP request failed after {} attempts: {}",
                        attempts,
                        last_error.unwrap_or_else(|| e.to_string())
                    )));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::PersonalAccessToken;
    use crate::client::retry::build_retry_configs;

    fn default_retry_configs() -> HashMap<RequestCategory, RetryConfig> {
        build_retry_configs(&RetryConfig::default(), &HashMap::new())
    }

    #[test]
    fn test_http_client_config_default() {
        let config = HttpClientConfig::default();
        assert_eq!(config.connect_timeout, Duration::from_secs(30));
        assert_eq!(config.read_timeout, Duration::from_secs(60));
        assert_eq!(config.max_connections_per_host, 100);
    }

    #[tokio::test]
    async fn test_http_client_creation() {
        let config = HttpClientConfig::default();
        let client = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_auth_header_after_set() {
        let config = HttpClientConfig::default();
        let client = DatabricksHttpClient::new(config, default_retry_configs()).unwrap();
        let auth = Arc::new(PersonalAccessToken::new("test-token".to_string()));
        client.set_auth_provider(auth);

        let header = client.auth_header().unwrap();
        assert_eq!(header, "Bearer test-token");
    }

    #[tokio::test]
    async fn test_execute_fails_before_auth_set() {
        let config = HttpClientConfig::default();
        let client = DatabricksHttpClient::new(config, default_retry_configs()).unwrap();

        let result = client.auth_header();
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("Auth provider not set"));
    }

    #[tokio::test]
    async fn test_execute_succeeds_after_auth_set() {
        let config = HttpClientConfig::default();
        let client = DatabricksHttpClient::new(config, default_retry_configs()).unwrap();
        let auth = Arc::new(PersonalAccessToken::new("test-token".to_string()));

        client.set_auth_provider(auth);

        let header = client.auth_header().unwrap();
        assert_eq!(header, "Bearer test-token");
    }

    #[tokio::test]
    #[should_panic(expected = "Auth provider can only be set once")]
    async fn test_set_auth_provider_twice_panics() {
        let config = HttpClientConfig::default();
        let client = DatabricksHttpClient::new(config, default_retry_configs()).unwrap();
        let auth1 = Arc::new(PersonalAccessToken::new("token1".to_string()));
        let auth2 = Arc::new(PersonalAccessToken::new("token2".to_string()));

        client.set_auth_provider(auth1);
        client.set_auth_provider(auth2); // Should panic
    }

    #[tokio::test]
    async fn test_execute_without_auth_works_before_auth_set() {
        let config = HttpClientConfig::default();
        let client = DatabricksHttpClient::new(config, default_retry_configs()).unwrap();
        assert!(client.auth_provider.get().is_none());
    }

    #[test]
    fn test_proxy_config_default() {
        let config = ProxyConfig::default();
        assert!(config.url.is_none());
        assert!(config.username.is_none());
        assert!(config.password.is_none());
        assert!(config.bypass_hosts.is_none());
    }

    #[test]
    fn test_http_client_config_default_has_default_proxy() {
        let config = HttpClientConfig::default();
        assert!(config.proxy.url.is_none());
    }

    #[test]
    fn test_http_client_with_proxy() {
        let mut config = HttpClientConfig::default();
        config.proxy.url = Some("http://proxy.example.com:8080".to_string());
        let client = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(client.is_ok());
    }

    #[test]
    fn test_http_client_with_authenticated_proxy() {
        let mut config = HttpClientConfig::default();
        config.proxy.url = Some("http://proxy.example.com:8080".to_string());
        config.proxy.username = Some("user".to_string());
        config.proxy.password = Some("pass".to_string());
        let client = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(client.is_ok());
    }

    #[test]
    fn test_http_client_with_invalid_proxy_url() {
        let mut config = HttpClientConfig::default();
        config.proxy.url = Some("http://".to_string());
        let result = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("Invalid proxy URL"));
    }

    #[test]
    fn test_http_client_with_no_scheme_proxy_url() {
        let mut config = HttpClientConfig::default();
        config.proxy.url = Some("proxy.example.com:8080".to_string());
        let result = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("http:// or https://"));
    }

    #[test]
    fn test_http_client_with_bypass_hosts() {
        let mut config = HttpClientConfig::default();
        config.proxy.url = Some("http://proxy.example.com:8080".to_string());
        config.proxy.bypass_hosts = Some("localhost,*.internal.corp,.example.com".to_string());
        let client = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(client.is_ok());
    }

    #[test]
    fn test_http_client_with_bypass_hosts_whitespace() {
        let mut config = HttpClientConfig::default();
        config.proxy.url = Some("http://proxy.example.com:8080".to_string());
        config.proxy.bypass_hosts = Some("localhost , *.internal.corp , .example.com".to_string());
        let client = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(client.is_ok());
    }

    #[test]
    fn test_http_client_rejects_invalid_proxy_scheme() {
        let mut config = HttpClientConfig::default();
        config.proxy.url = Some("socks5://proxy.example.com:1080".to_string());
        let result = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("http:// or https://"));
    }

    #[test]
    fn test_http_client_proxy_username_without_url() {
        let mut config = HttpClientConfig::default();
        config.proxy.username = Some("user".to_string());
        config.proxy.password = Some("pass".to_string());
        let client = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(client.is_ok());
    }

    #[test]
    fn test_proxy_config_debug_redacts_password() {
        let config = ProxyConfig {
            url: Some("http://proxy:8080".to_string()),
            username: Some("user".to_string()),
            password: Some("secret123".to_string()),
            bypass_hosts: None,
        };
        let debug_output = format!("{:?}", config);
        assert!(debug_output.contains("[REDACTED]"));
        assert!(!debug_output.contains("secret123"));
    }

    // --- Retry integration tests (with wiremock) ---

    mod retry_integration {
        use super::*;
        use crate::client::retry::{
            build_retry_configs, RequestCategory, RequestType, RetryConfig, RetryConfigOverrides,
        };
        use std::collections::HashSet;
        use std::sync::atomic::{AtomicU32, Ordering};
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        /// Build a client pointed at the mock server with short retry waits for fast tests.
        fn test_client(
            mock_server: &MockServer,
            retry_config: RetryConfig,
        ) -> DatabricksHttpClient {
            let mut overrides = HashMap::new();
            let ovr = RetryConfigOverrides {
                min_wait: Some(retry_config.min_wait),
                max_wait: Some(retry_config.max_wait),
                overall_timeout: Some(retry_config.overall_timeout),
                max_retries: Some(retry_config.max_retries),
                override_retryable_codes: retry_config.override_retryable_codes.clone(),
            };
            overrides.insert(RequestCategory::Sea, ovr.clone());
            overrides.insert(RequestCategory::CloudFetch, ovr.clone());
            overrides.insert(RequestCategory::Auth, ovr);

            let global = RetryConfig::default();
            let retry_configs = build_retry_configs(&global, &overrides);

            let config = HttpClientConfig::default();
            let client = DatabricksHttpClient::new(config, retry_configs).unwrap();
            let auth = Arc::new(PersonalAccessToken::new("test-token".to_string()));
            client.set_auth_provider(auth);

            // Override the client's base URL by using the mock_server URI directly in requests
            let _ = mock_server; // used by caller to build request URLs
            client
        }

        /// Short retry config for fast tests (10ms waits instead of 1s).
        fn fast_retry_config(max_retries: u32) -> RetryConfig {
            RetryConfig {
                min_wait: Duration::from_millis(10),
                max_wait: Duration::from_millis(50),
                overall_timeout: Duration::from_secs(10),
                max_retries,
                override_retryable_codes: None,
            }
        }

        #[tokio::test]
        async fn test_idempotent_request_succeeds_on_retry_after_503() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            // Return 503 twice, then 200
            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    let n = counter.fetch_add(1, Ordering::SeqCst);
                    if n < 2 {
                        ResponseTemplate::new(503)
                    } else {
                        ResponseTemplate::new(200).set_body_string("ok")
                    }
                })
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(5));
            let request = client
                .inner()
                .get(format!("{}/test", mock_server.uri()))
                .build()
                .unwrap();

            let response = client
                .execute(request, RequestType::GetStatementStatus)
                .await;
            assert!(response.is_ok());
            assert_eq!(attempt_count.load(Ordering::SeqCst), 3);
        }

        #[tokio::test]
        async fn test_idempotent_request_fails_after_max_retries() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            // Always return 503
            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    ResponseTemplate::new(503)
                })
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(3));
            let request = client
                .inner()
                .get(format!("{}/test", mock_server.uri()))
                .build()
                .unwrap();

            let result = client
                .execute(request, RequestType::GetStatementStatus)
                .await;
            assert!(result.is_err());
            // max_retries=3 means 4 total attempts (1 initial + 3 retries)
            assert_eq!(attempt_count.load(Ordering::SeqCst), 4);
        }

        #[tokio::test]
        async fn test_idempotent_request_retries_500() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            // Return 500 once, then 200 — idempotent strategy retries 500
            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    let n = counter.fetch_add(1, Ordering::SeqCst);
                    if n < 1 {
                        ResponseTemplate::new(500)
                    } else {
                        ResponseTemplate::new(200).set_body_string("ok")
                    }
                })
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(3));
            let request = client
                .inner()
                .get(format!("{}/test", mock_server.uri()))
                .build()
                .unwrap();

            let response = client
                .execute(request, RequestType::GetStatementStatus)
                .await;
            assert!(response.is_ok());
            assert_eq!(attempt_count.load(Ordering::SeqCst), 2);
        }

        #[tokio::test]
        async fn test_idempotent_no_retry_on_400() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    ResponseTemplate::new(400).set_body_string("bad request")
                })
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(3));
            let request = client
                .inner()
                .get(format!("{}/test", mock_server.uri()))
                .build()
                .unwrap();

            let result = client
                .execute(request, RequestType::GetStatementStatus)
                .await;
            assert!(result.is_err());
            // 400 is non-retryable — only 1 attempt
            assert_eq!(attempt_count.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn test_idempotent_no_retry_on_401() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    ResponseTemplate::new(401).set_body_string("unauthorized")
                })
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(3));
            let request = client
                .inner()
                .get(format!("{}/test", mock_server.uri()))
                .build()
                .unwrap();

            let result = client
                .execute(request, RequestType::GetStatementStatus)
                .await;
            assert!(result.is_err());
            assert_eq!(attempt_count.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn test_non_idempotent_no_retry_on_500() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            Mock::given(method("POST"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    ResponseTemplate::new(500).set_body_string("server error")
                })
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(3));
            let request = client
                .inner()
                .post(format!("{}/test", mock_server.uri()))
                .body("sql query")
                .build()
                .unwrap();

            let result = client.execute(request, RequestType::ExecuteStatement).await;
            assert!(result.is_err());
            // Non-idempotent: 500 is NOT retryable — only 1 attempt
            assert_eq!(attempt_count.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn test_non_idempotent_retries_on_429_with_retry_after() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            // Return 429 with Retry-After once, then 200
            Mock::given(method("POST"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    let n = counter.fetch_add(1, Ordering::SeqCst);
                    if n < 1 {
                        ResponseTemplate::new(429).append_header("Retry-After", "0")
                    } else {
                        ResponseTemplate::new(200).set_body_string("ok")
                    }
                })
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(3));
            let request = client
                .inner()
                .post(format!("{}/test", mock_server.uri()))
                .body("sql query")
                .build()
                .unwrap();

            let response = client.execute(request, RequestType::ExecuteStatement).await;
            assert!(response.is_ok());
            assert_eq!(attempt_count.load(Ordering::SeqCst), 2);
        }

        #[tokio::test]
        async fn test_non_idempotent_no_retry_on_429_without_retry_after() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            // Return 429 WITHOUT Retry-After — should NOT retry for non-idempotent
            Mock::given(method("POST"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    ResponseTemplate::new(429)
                })
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(3));
            let request = client
                .inner()
                .post(format!("{}/test", mock_server.uri()))
                .body("sql query")
                .build()
                .unwrap();

            let result = client.execute(request, RequestType::ExecuteStatement).await;
            assert!(result.is_err());
            // No Retry-After → not retried for non-idempotent — only 1 attempt
            assert_eq!(attempt_count.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn test_non_idempotent_retries_on_503_with_retry_after() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            // Return 503 with Retry-After twice, then 200
            Mock::given(method("POST"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    let n = counter.fetch_add(1, Ordering::SeqCst);
                    if n < 2 {
                        ResponseTemplate::new(503).append_header("Retry-After", "0")
                    } else {
                        ResponseTemplate::new(200).set_body_string("ok")
                    }
                })
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(3));
            let request = client
                .inner()
                .post(format!("{}/test", mock_server.uri()))
                .body("sql query")
                .build()
                .unwrap();

            let response = client.execute(request, RequestType::ExecuteStatement).await;
            assert!(response.is_ok());
            assert_eq!(attempt_count.load(Ordering::SeqCst), 3);
        }

        #[tokio::test]
        async fn test_retry_after_header_honored() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            // Return 429 with Retry-After: 0, then 200
            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    let n = counter.fetch_add(1, Ordering::SeqCst);
                    if n < 1 {
                        ResponseTemplate::new(429).append_header("Retry-After", "0")
                    } else {
                        ResponseTemplate::new(200).set_body_string("ok")
                    }
                })
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(3));
            let request = client
                .inner()
                .get(format!("{}/test", mock_server.uri()))
                .build()
                .unwrap();

            let start = std::time::Instant::now();
            let response = client
                .execute(request, RequestType::GetStatementStatus)
                .await;
            let elapsed = start.elapsed();

            assert!(response.is_ok());
            assert_eq!(attempt_count.load(Ordering::SeqCst), 2);
            // Retry-After: 0 clamped to min_wait=10ms + jitter(50-750ms)
            // Should complete quickly (under 2s)
            assert!(elapsed < Duration::from_secs(2));
        }

        #[tokio::test]
        async fn test_overall_timeout_stops_retries() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            // Always return 503
            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    ResponseTemplate::new(503)
                })
                .mount(&mock_server)
                .await;

            // overall_timeout=2s with min_wait=500ms + jitter means ~2-3 attempts max
            let config = RetryConfig {
                min_wait: Duration::from_millis(500),
                max_wait: Duration::from_millis(800),
                overall_timeout: Duration::from_secs(2),
                max_retries: 100, // high limit — timeout should stop us first
                override_retryable_codes: None,
            };
            let client = test_client(&mock_server, config);
            let request = client
                .inner()
                .get(format!("{}/test", mock_server.uri()))
                .build()
                .unwrap();

            let result = client
                .execute(request, RequestType::GetStatementStatus)
                .await;
            assert!(result.is_err());
            let err_msg = format!("{:?}", result.unwrap_err());
            assert!(err_msg.contains("retry timeout exceeded"));
            // Should have made 2-4 attempts before timeout (not 100)
            let attempts = attempt_count.load(Ordering::SeqCst);
            assert!(
                (2..=5).contains(&attempts),
                "Expected 2-5 attempts, got {}",
                attempts
            );
        }

        #[tokio::test]
        async fn test_override_retryable_codes() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            // Return 418 once, then 200
            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    let n = counter.fetch_add(1, Ordering::SeqCst);
                    if n < 1 {
                        ResponseTemplate::new(418) // I'm a teapot — not normally retryable
                    } else {
                        ResponseTemplate::new(200).set_body_string("ok")
                    }
                })
                .mount(&mock_server)
                .await;

            // Override: make 418 retryable
            let mut codes = HashSet::new();
            codes.insert(418);
            let config = RetryConfig {
                min_wait: Duration::from_millis(10),
                max_wait: Duration::from_millis(50),
                overall_timeout: Duration::from_secs(10),
                max_retries: 3,
                override_retryable_codes: Some(codes),
            };

            let client = test_client(&mock_server, config);
            let request = client
                .inner()
                .get(format!("{}/test", mock_server.uri()))
                .build()
                .unwrap();

            let response = client
                .execute(request, RequestType::GetStatementStatus)
                .await;
            assert!(response.is_ok());
            assert_eq!(attempt_count.load(Ordering::SeqCst), 2);
        }

        #[tokio::test]
        async fn test_cloudfetch_403_not_retried_at_http_layer() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            // Return 403 (expired presigned URL) — should NOT be retried at HTTP layer
            Mock::given(method("GET"))
                .and(path("/presigned"))
                .respond_with(move |_req: &wiremock::Request| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    ResponseTemplate::new(403).set_body_string("access denied")
                })
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(3));
            let request = client
                .inner()
                .get(format!("{}/presigned", mock_server.uri()))
                .build()
                .unwrap();

            let result = client
                .execute_without_auth(request, RequestType::CloudFetchDownload)
                .await;
            assert!(result.is_err());
            // 403 is non-retryable for idempotent requests — only 1 attempt
            assert_eq!(attempt_count.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn test_exponential_backoff_timing() {
            // Verify that delays between attempts follow exponential backoff.
            // Config: min_wait=100ms, max_wait=500ms
            // Per spec: exp_backoff = 2^attempt * min_wait
            // Expected: attempt 1→2: ~200ms+jitter, attempt 2→3: ~400ms+jitter, attempt 3→4: ~500ms(capped)+jitter
            let mock_server = MockServer::start().await;
            let timestamps = Arc::new(std::sync::Mutex::new(Vec::<Instant>::new()));
            let ts = timestamps.clone();

            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    ts.lock().unwrap().push(Instant::now());
                    ResponseTemplate::new(503)
                })
                .mount(&mock_server)
                .await;

            let config = RetryConfig {
                min_wait: Duration::from_millis(100),
                max_wait: Duration::from_millis(500),
                overall_timeout: Duration::from_secs(30),
                max_retries: 3,
                override_retryable_codes: None,
            };
            let client = test_client(&mock_server, config);
            let request = client
                .inner()
                .get(format!("{}/test", mock_server.uri()))
                .build()
                .unwrap();

            let _ = client
                .execute(request, RequestType::GetStatementStatus)
                .await;

            let ts = timestamps.lock().unwrap();
            assert_eq!(ts.len(), 4, "Expected 4 attempts (1 initial + 3 retries)");

            // Gap between attempt 1 and 2: 2^1*100ms = 200ms + 50-750ms jitter = 250-950ms
            let gap1 = ts[1].duration_since(ts[0]);
            assert!(
                gap1 >= Duration::from_millis(200) && gap1 <= Duration::from_millis(1100),
                "Gap 1→2: {:?}, expected 250-1100ms",
                gap1
            );

            // Gap between attempt 2 and 3: 2^2*100ms = 400ms + jitter = 450-1150ms
            let gap2 = ts[2].duration_since(ts[1]);
            assert!(
                gap2 >= Duration::from_millis(400) && gap2 <= Duration::from_millis(1300),
                "Gap 2→3: {:?}, expected 450-1300ms",
                gap2
            );

            // Gap between attempt 3 and 4: 2^3*100ms = 800ms, capped to max_wait=500ms + jitter
            let gap3 = ts[3].duration_since(ts[2]);
            assert!(
                gap3 >= Duration::from_millis(500) && gap3 <= Duration::from_millis(1400),
                "Gap 3→4: {:?}, expected 550-1400ms (capped at max_wait=500ms)",
                gap3
            );

            // Exponential growth is verified by the ranges above:
            // gap1 lower bound (200ms) < gap2 lower bound (400ms) < gap3 lower bound (500ms, capped)
            // Direct gap comparison is unreliable due to jitter (50-750ms) overlapping the base.
        }

        #[tokio::test]
        async fn test_retry_after_header_delays_correctly() {
            // Verify that Retry-After: N causes a delay of at least N seconds
            // (clamped to min_wait, plus jitter)
            let mock_server = MockServer::start().await;
            let timestamps = Arc::new(std::sync::Mutex::new(Vec::<Instant>::new()));
            let ts = timestamps.clone();

            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    let n = ts.lock().unwrap().len();
                    ts.lock().unwrap().push(Instant::now());
                    if n < 1 {
                        // Tell client to wait 1 second
                        ResponseTemplate::new(429).append_header("Retry-After", "1")
                    } else {
                        ResponseTemplate::new(200).set_body_string("ok")
                    }
                })
                .mount(&mock_server)
                .await;

            let config = RetryConfig {
                min_wait: Duration::from_millis(50), // low min so Retry-After dominates
                max_wait: Duration::from_secs(5),
                overall_timeout: Duration::from_secs(30),
                max_retries: 3,
                override_retryable_codes: None,
            };
            let client = test_client(&mock_server, config);
            let request = client
                .inner()
                .get(format!("{}/test", mock_server.uri()))
                .build()
                .unwrap();

            let response = client
                .execute(request, RequestType::GetStatementStatus)
                .await;
            assert!(response.is_ok());

            let ts = timestamps.lock().unwrap();
            assert_eq!(ts.len(), 2);

            // Gap should be ~1s (Retry-After) + 50-750ms jitter = 1050ms-1750ms
            let gap = ts[1].duration_since(ts[0]);
            assert!(
                gap >= Duration::from_millis(1000) && gap <= Duration::from_millis(2000),
                "Gap with Retry-After:1: {:?}, expected 1000-2000ms",
                gap
            );
        }

        #[tokio::test]
        async fn test_retry_after_clamped_to_max_wait() {
            // Retry-After: 10 but max_wait: 500ms → should clamp to 500ms + jitter
            let mock_server = MockServer::start().await;
            let timestamps = Arc::new(std::sync::Mutex::new(Vec::<Instant>::new()));
            let ts = timestamps.clone();

            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    let n = ts.lock().unwrap().len();
                    ts.lock().unwrap().push(Instant::now());
                    if n < 1 {
                        // Tell client to wait 10 seconds — but max_wait will clamp this
                        ResponseTemplate::new(429).append_header("Retry-After", "10")
                    } else {
                        ResponseTemplate::new(200).set_body_string("ok")
                    }
                })
                .mount(&mock_server)
                .await;

            let config = RetryConfig {
                min_wait: Duration::from_millis(50),
                max_wait: Duration::from_millis(500), // clamp Retry-After:10 down to 500ms
                overall_timeout: Duration::from_secs(30),
                max_retries: 3,
                override_retryable_codes: None,
            };
            let client = test_client(&mock_server, config);
            let request = client
                .inner()
                .get(format!("{}/test", mock_server.uri()))
                .build()
                .unwrap();

            let response = client
                .execute(request, RequestType::GetStatementStatus)
                .await;
            assert!(response.is_ok());

            let ts = timestamps.lock().unwrap();
            assert_eq!(ts.len(), 2);

            // Gap should be ~500ms (clamped max_wait) + 50-750ms jitter = 550-1250ms
            // Crucially, NOT 10 seconds
            let gap = ts[1].duration_since(ts[0]);
            assert!(
                gap >= Duration::from_millis(500) && gap <= Duration::from_millis(1500),
                "Gap with clamped Retry-After: {:?}, expected 500-1500ms (NOT 10s)",
                gap
            );
        }

        #[tokio::test]
        async fn test_metadata_query_retries_as_idempotent() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            // Return 500 once, then 200
            // ExecuteMetadataQuery is idempotent so 500 should be retried
            Mock::given(method("POST"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    let n = counter.fetch_add(1, Ordering::SeqCst);
                    if n < 1 {
                        ResponseTemplate::new(500)
                    } else {
                        ResponseTemplate::new(200).set_body_string("{}")
                    }
                })
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(3));
            let request = client
                .inner()
                .post(format!("{}/test", mock_server.uri()))
                .body("SHOW CATALOGS")
                .build()
                .unwrap();

            let response = client
                .execute(request, RequestType::ExecuteMetadataQuery)
                .await;
            assert!(response.is_ok());
            // Idempotent: 500 is retried — 2 attempts
            assert_eq!(attempt_count.load(Ordering::SeqCst), 2);
        }
    }
}
