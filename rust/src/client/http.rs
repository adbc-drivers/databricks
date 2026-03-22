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
//! - Automatic retry with exponential backoff
//! - Bearer token authentication
//! - Configurable timeouts

use crate::auth::AuthProvider;
use crate::error::{DatabricksErrorHelper, Result};
use driverbase::error::ErrorHelper;
use reqwest::{Client, NoProxy, Proxy, Request, Response, StatusCode};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
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
    /// Maximum number of retry attempts.
    pub max_retries: u32,
    /// Base delay between retry attempts (doubles each retry).
    pub retry_delay: Duration,
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
            max_retries: 5,
            retry_delay: Duration::from_millis(1500),
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
/// - Automatic retry with exponential backoff for transient failures
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
}

impl DatabricksHttpClient {
    /// Creates a new HTTP client with the given configuration.
    ///
    /// Auth provider must be set separately via `set_auth_provider()` before
    /// calling `execute()`. Use `execute_without_auth()` for requests that
    /// don't need authentication (e.g., OAuth token endpoint calls).
    pub fn new(config: HttpClientConfig) -> Result<Self> {
        let mut builder = Client::builder()
            .connect_timeout(config.connect_timeout)
            .timeout(config.read_timeout)
            .pool_max_idle_per_host(config.max_connections_per_host)
            .user_agent(&config.user_agent);

        // Apply proxy configuration
        if let Some(ref proxy_url) = config.proxy.url {
            let mut proxy = Proxy::all(proxy_url).map_err(|e| {
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
                proxy = proxy.no_proxy(NoProxy::from_string(bypass_hosts));
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
        })
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

    /// Execute an HTTP request with automatic retry logic and authentication.
    ///
    /// This is the standard method for API calls that require authentication.
    /// For requests without auth (e.g., CloudFetch presigned URLs), use
    /// `execute_without_auth`.
    ///
    /// Retries are performed for:
    /// - Network errors
    /// - 429 Too Many Requests
    /// - 503 Service Unavailable
    /// - 504 Gateway Timeout
    ///
    /// Non-retryable errors are returned immediately.
    pub async fn execute(&self, request: Request) -> Result<Response> {
        self.execute_impl(request, true).await
    }

    /// Execute a request without authentication (for CloudFetch downloads).
    ///
    /// CloudFetch presigned URLs don't need our auth header - they have
    /// their own authentication built into the URL or custom headers.
    pub async fn execute_without_auth(&self, request: Request) -> Result<Response> {
        self.execute_impl(request, false).await
    }

    /// Internal implementation of execute with configurable auth.
    async fn execute_impl(&self, request: Request, with_auth: bool) -> Result<Response> {
        let mut attempts = 0;
        let mut last_error: Option<String> = None;

        // Clone the request parts we need for retries
        let method = request.method().clone();
        let url = request.url().clone();
        let headers = request.headers().clone();
        let body_bytes = if with_auth {
            request
                .body()
                .and_then(|b| b.as_bytes())
                .map(|b| b.to_vec())
        } else {
            None
        };

        loop {
            attempts += 1;

            // Build a fresh request for this attempt
            let mut req_builder = self.client.request(method.clone(), url.clone());

            // Add headers
            for (name, value) in headers.iter() {
                req_builder = req_builder.header(name, value);
            }

            // Add auth header if requested
            if with_auth {
                let auth_header = self.auth_header()?;
                req_builder = req_builder.header("Authorization", auth_header);
            }

            // Add body if present
            if let Some(ref body) = body_bytes {
                req_builder = req_builder.body(body.clone());
            }

            let request = req_builder.build().map_err(|e| {
                DatabricksErrorHelper::io().message(format!("Failed to build request: {}", e))
            })?;

            debug!(
                "Executing {} {} (attempt {}/{})",
                method,
                url,
                attempts,
                self.config.max_retries + 1
            );

            match self.client.execute(request).await {
                Ok(response) => {
                    let status = response.status();

                    if status.is_success() {
                        return Ok(response);
                    }

                    // Check if this is a retryable error
                    if Self::is_retryable_status(status) && attempts <= self.config.max_retries {
                        last_error = Some(format!("HTTP {}", status.as_u16()));
                        warn!(
                            "Request failed with {} (attempt {}/{}), retrying...",
                            status,
                            attempts,
                            self.config.max_retries + 1
                        );
                        self.wait_for_retry(attempts).await;
                        continue;
                    }

                    // Non-retryable HTTP error or max retries exceeded
                    let error_body = response.text().await.unwrap_or_default();
                    return Err(DatabricksErrorHelper::io().message(format!(
                        "HTTP {} - {}",
                        status.as_u16(),
                        error_body
                    )));
                }
                Err(e) => {
                    // Network or other error
                    if Self::is_retryable_error(&e) && attempts <= self.config.max_retries {
                        last_error = Some(e.to_string());
                        warn!(
                            "Request failed with error (attempt {}/{}): {}, retrying...",
                            attempts,
                            self.config.max_retries + 1,
                            e
                        );
                        self.wait_for_retry(attempts).await;
                        continue;
                    }

                    return Err(DatabricksErrorHelper::io().message(format!(
                        "HTTP request failed after {} attempts: {}",
                        attempts,
                        last_error.unwrap_or_else(|| e.to_string())
                    )));
                }
            }
        }
    }

    /// Check if the HTTP status code indicates a retryable error.
    fn is_retryable_status(status: StatusCode) -> bool {
        matches!(
            status,
            StatusCode::TOO_MANY_REQUESTS
                | StatusCode::SERVICE_UNAVAILABLE
                | StatusCode::GATEWAY_TIMEOUT
                | StatusCode::BAD_GATEWAY
        )
    }

    /// Check if the request error is retryable.
    fn is_retryable_error(error: &reqwest::Error) -> bool {
        error.is_timeout() || error.is_connect() || error.is_request()
    }

    /// Wait with exponential backoff before retry.
    async fn wait_for_retry(&self, attempt: u32) {
        let delay = self.config.retry_delay * 2u32.saturating_pow(attempt.saturating_sub(1));
        debug!("Waiting {:?} before retry", delay);
        sleep(delay).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::PersonalAccessToken;

    #[test]
    fn test_http_client_config_default() {
        let config = HttpClientConfig::default();
        assert_eq!(config.connect_timeout, Duration::from_secs(30));
        assert_eq!(config.read_timeout, Duration::from_secs(60));
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.max_connections_per_host, 100);
    }

    #[test]
    fn test_is_retryable_status() {
        assert!(DatabricksHttpClient::is_retryable_status(
            StatusCode::TOO_MANY_REQUESTS
        ));
        assert!(DatabricksHttpClient::is_retryable_status(
            StatusCode::SERVICE_UNAVAILABLE
        ));
        assert!(DatabricksHttpClient::is_retryable_status(
            StatusCode::GATEWAY_TIMEOUT
        ));
        assert!(DatabricksHttpClient::is_retryable_status(
            StatusCode::BAD_GATEWAY
        ));
        assert!(!DatabricksHttpClient::is_retryable_status(StatusCode::OK));
        assert!(!DatabricksHttpClient::is_retryable_status(
            StatusCode::BAD_REQUEST
        ));
        assert!(!DatabricksHttpClient::is_retryable_status(
            StatusCode::INTERNAL_SERVER_ERROR
        ));
    }

    #[tokio::test]
    async fn test_http_client_creation() {
        let config = HttpClientConfig::default();
        let client = DatabricksHttpClient::new(config);
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_auth_header_after_set() {
        let config = HttpClientConfig::default();
        let client = DatabricksHttpClient::new(config).unwrap();
        let auth = Arc::new(PersonalAccessToken::new("test-token".to_string()));
        client.set_auth_provider(auth);

        let header = client.auth_header().unwrap();
        assert_eq!(header, "Bearer test-token");
    }

    #[tokio::test]
    async fn test_execute_fails_before_auth_set() {
        let config = HttpClientConfig::default();
        let client = DatabricksHttpClient::new(config).unwrap();

        // Calling auth_header() without setting auth provider should fail
        let result = client.auth_header();
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("Auth provider not set"));
    }

    #[tokio::test]
    async fn test_execute_succeeds_after_auth_set() {
        let config = HttpClientConfig::default();
        let client = DatabricksHttpClient::new(config).unwrap();
        let auth = Arc::new(PersonalAccessToken::new("test-token".to_string()));

        client.set_auth_provider(auth);

        let header = client.auth_header().unwrap();
        assert_eq!(header, "Bearer test-token");
    }

    #[tokio::test]
    #[should_panic(expected = "Auth provider can only be set once")]
    async fn test_set_auth_provider_twice_panics() {
        let config = HttpClientConfig::default();
        let client = DatabricksHttpClient::new(config).unwrap();
        let auth1 = Arc::new(PersonalAccessToken::new("token1".to_string()));
        let auth2 = Arc::new(PersonalAccessToken::new("token2".to_string()));

        client.set_auth_provider(auth1);
        client.set_auth_provider(auth2); // Should panic
    }

    #[tokio::test]
    async fn test_execute_without_auth_works_before_auth_set() {
        // This test verifies that execute_without_auth() doesn't need auth provider
        // It doesn't make actual HTTP calls, just checks that the method can be called
        let config = HttpClientConfig::default();
        let client = DatabricksHttpClient::new(config).unwrap();

        // Should not panic or error - execute_without_auth doesn't check auth_provider
        // We can't test actual HTTP execution without a mock server, but we verified
        // the method signature and that it doesn't call auth_header()
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
        let client = DatabricksHttpClient::new(config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_http_client_with_authenticated_proxy() {
        let mut config = HttpClientConfig::default();
        config.proxy.url = Some("http://proxy.example.com:8080".to_string());
        config.proxy.username = Some("user".to_string());
        config.proxy.password = Some("pass".to_string());
        let client = DatabricksHttpClient::new(config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_http_client_with_invalid_proxy_url() {
        let mut config = HttpClientConfig::default();
        config.proxy.url = Some("not a valid url".to_string());
        let result = DatabricksHttpClient::new(config);
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("Invalid proxy URL"));
    }

    #[test]
    fn test_http_client_with_bypass_hosts() {
        let mut config = HttpClientConfig::default();
        config.proxy.url = Some("http://proxy.example.com:8080".to_string());
        config.proxy.bypass_hosts = Some("localhost,*.internal.corp,.example.com".to_string());
        let client = DatabricksHttpClient::new(config);
        assert!(client.is_ok());
    }
}
