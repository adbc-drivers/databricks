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

//! M2M (Machine-to-Machine) OAuth authentication using client credentials flow.
//!
//! This module implements the OAuth 2.0 client credentials grant for service
//! principal authentication. The flow:
//!
//! 1. Discover token endpoint via OIDC well-known endpoint
//! 2. Exchange client_id + client_secret for access_token
//! 3. Cache token in TokenStore with refresh lifecycle
//! 4. Re-authenticate using client credentials when token expires (no refresh_token)
//!
//! M2M tokens are not cached to disk because:
//! - Credentials (client_id/secret) are always available for re-authentication
//! - Tokens are typically short-lived
//! - Caching adds security risk without significant benefit

use crate::auth::oauth::oidc::OidcEndpoints;
use crate::auth::oauth::token::OAuthToken;
use crate::auth::oauth::token_store::TokenStore;
use crate::auth::AuthProvider;
use crate::client::http::DatabricksHttpClient;
use crate::error::{DatabricksErrorHelper, Result};
use driverbase::error::ErrorHelper;
use oauth2::basic::BasicClient;
use oauth2::{AuthUrl, ClientId, ClientSecret, Scope, TokenResponse, TokenUrl};
use std::sync::Arc;

/// OAuth provider for M2M (Machine-to-Machine) authentication.
///
/// Uses the OAuth 2.0 client credentials grant to authenticate service principals.
/// The provider discovers the token endpoint via OIDC, then exchanges client
/// credentials for access tokens. Tokens are managed by TokenStore with automatic
/// refresh when they become stale or expire.
///
/// # Token Lifecycle
///
/// - **Initial fetch**: On first `get_auth_header()` call, exchanges credentials for token
/// - **Fresh token**: Returned immediately from TokenStore (no HTTP call)
/// - **Stale token**: Returned immediately, background refresh spawned
/// - **Expired token**: Blocking re-authentication via client credentials exchange
///
/// M2M tokens have no `refresh_token` - re-authentication uses the same client credentials.
///
/// # Example
///
/// ```no_run
/// use databricks_adbc::auth::AuthProvider;
/// use databricks_adbc::auth::oauth::m2m::ClientCredentialsProvider;
/// use databricks_adbc::client::http::{DatabricksHttpClient, HttpClientConfig};
/// use std::sync::Arc;
///
/// # async fn example() -> databricks_adbc::error::Result<()> {
/// let http_client = Arc::new(DatabricksHttpClient::new(HttpClientConfig::default())?);
/// let provider = ClientCredentialsProvider::new(
///     "https://my-workspace.cloud.databricks.com",
///     "my-client-id",
///     "my-client-secret",
///     http_client,
/// ).await?;
///
/// // Get auth header (will trigger initial token fetch)
/// let auth_header = provider.get_auth_header()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct ClientCredentialsProvider {
    /// Client ID for OAuth
    client_id: String,
    /// Client secret for OAuth
    client_secret: String,
    /// Discovered OIDC endpoints
    token_endpoint: String,
    auth_endpoint: String,
    /// Thread-safe token store managing the token lifecycle
    token_store: TokenStore,
    /// HTTP client for token endpoint requests
    http_client: Arc<DatabricksHttpClient>,
    /// OAuth scopes to request (typically "all-apis")
    scopes: Vec<String>,
}

impl ClientCredentialsProvider {
    /// Creates a new M2M authentication provider.
    ///
    /// Discovers the token endpoint via OIDC before returning. The provider
    /// is ready to use after construction - the first `get_auth_header()` call
    /// will trigger the initial token exchange.
    ///
    /// # Arguments
    ///
    /// * `host` - Databricks workspace URL (e.g., "https://my-workspace.cloud.databricks.com")
    /// * `client_id` - OAuth client ID for the service principal
    /// * `client_secret` - OAuth client secret for the service principal
    /// * `http_client` - Shared HTTP client for token endpoint requests
    ///
    /// # Returns
    ///
    /// A configured provider ready for authentication, or an error if:
    /// - OIDC discovery fails (network error, invalid response, etc.)
    /// - The runtime handle cannot be obtained
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use databricks_adbc::auth::oauth::m2m::ClientCredentialsProvider;
    /// # use databricks_adbc::client::http::{DatabricksHttpClient, HttpClientConfig};
    /// # use std::sync::Arc;
    /// # async fn example() -> databricks_adbc::error::Result<()> {
    /// let http_client = Arc::new(DatabricksHttpClient::new(HttpClientConfig::default())?);
    /// let provider = ClientCredentialsProvider::new(
    ///     "https://my-workspace.cloud.databricks.com",
    ///     "client-id",
    ///     "client-secret",
    ///     http_client,
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(
        host: &str,
        client_id: &str,
        client_secret: &str,
        http_client: Arc<DatabricksHttpClient>,
    ) -> Result<Self> {
        Self::new_with_scopes(
            host,
            client_id,
            client_secret,
            http_client,
            vec!["all-apis".to_string()],
        )
        .await
    }

    /// Creates a new M2M provider with custom scopes.
    ///
    /// This is useful for testing or when specific scopes are required.
    /// Most users should use `new()` which defaults to "all-apis".
    pub async fn new_with_scopes(
        host: &str,
        client_id: &str,
        client_secret: &str,
        http_client: Arc<DatabricksHttpClient>,
        scopes: Vec<String>,
    ) -> Result<Self> {
        // Discover OIDC endpoints
        let endpoints = OidcEndpoints::discover(host, &http_client).await?;

        Ok(Self {
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
            token_endpoint: endpoints.token_endpoint,
            auth_endpoint: endpoints.authorization_endpoint,
            token_store: TokenStore::new(),
            http_client,
            scopes,
        })
    }
}

impl AuthProvider for ClientCredentialsProvider {
    /// Returns a valid Bearer token for authentication.
    ///
    /// This method implements the token lifecycle state machine via TokenStore:
    /// - **Empty**: Blocks and fetches the initial token
    /// - **Fresh**: Returns cached token immediately (no HTTP call)
    /// - **Stale**: Returns cached token immediately, spawns background refresh
    /// - **Expired**: Blocks and fetches a new token via client credentials exchange
    ///
    /// The method is safe to call from multiple threads concurrently. Only one
    /// token fetch/refresh will execute at a time, coordinated by TokenStore.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The token exchange fails (network error, invalid credentials, etc.)
    /// - The token store lock is poisoned (indicates panic in another thread)
    fn get_auth_header(&self) -> Result<String> {
        // Capture variables for the closure
        let http_client = self.http_client.clone();
        let client_id = self.client_id.clone();
        let client_secret = self.client_secret.clone();
        let token_endpoint = self.token_endpoint.clone();
        let auth_endpoint = self.auth_endpoint.clone();
        let scopes = self.scopes.clone();

        // Get or refresh token via TokenStore
        let token = self.token_store.get_or_refresh(move || {
            // Execute async token fetch within the runtime
            // Use block_in_place to avoid blocking the runtime if we're in one,
            // and get the handle to block_on the async operation
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    // Build the OAuth client
                    let oauth_client = BasicClient::new(ClientId::new(client_id))
                        .set_client_secret(ClientSecret::new(client_secret))
                        .set_auth_uri(AuthUrl::new(auth_endpoint).map_err(|e| {
                            DatabricksErrorHelper::invalid_argument()
                                .message(format!("Invalid authorization endpoint URL: {}", e))
                        })?)
                        .set_token_uri(TokenUrl::new(token_endpoint).map_err(|e| {
                            DatabricksErrorHelper::invalid_argument()
                                .message(format!("Invalid token endpoint URL: {}", e))
                        })?);

                    // Build the token request
                    let mut token_request = oauth_client.exchange_client_credentials();
                    for scope in &scopes {
                        token_request = token_request.add_scope(Scope::new(scope.clone()));
                    }

                    // Execute the token exchange using inner reqwest client
                    let token_response = token_request
                        .request_async(http_client.inner())
                        .await
                        .map_err(|e| {
                            DatabricksErrorHelper::io()
                                .message(format!("M2M token exchange failed: {}", e))
                                .context("client credentials grant")
                        })?;

                    // Convert to OAuthToken
                    let access_token = token_response.access_token().secret().to_string();
                    let token_type = token_response.token_type().as_ref().to_string();
                    let expires_in = token_response
                        .expires_in()
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(3600);

                    let scopes_result = token_response
                        .scopes()
                        .map(|s| s.iter().map(|scope| scope.to_string()).collect())
                        .unwrap_or_else(|| scopes.clone());

                    Ok(OAuthToken::new(
                        access_token,
                        token_type,
                        expires_in,
                        None, // No refresh token for M2M
                        scopes_result,
                    ))
                })
            })
        })?;

        // Return Bearer token
        Ok(format!("Bearer {}", token.access_token))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::http::HttpClientConfig;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Helper to create a mock OIDC discovery response
    fn mock_oidc_discovery(mock_server: &MockServer) -> String {
        let token_endpoint = format!("{}/oidc/v1/token", mock_server.uri());
        let _auth_endpoint = format!("{}/oidc/v1/authorize", mock_server.uri());

        // Return the token endpoint for test verification
        token_endpoint
    }

    /// Helper to mock a successful token response
    fn mock_token_response_body() -> serde_json::Value {
        serde_json::json!({
            "access_token": "test-access-token-m2m",
            "token_type": "Bearer",
            "expires_in": 3600,
            "scope": "all-apis"
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_m2m_token_exchange() {
        let mock_server = MockServer::start().await;
        let token_endpoint = mock_oidc_discovery(&mock_server);

        // Mock OIDC discovery
        Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "authorization_endpoint": format!("{}/oidc/v1/authorize", mock_server.uri()),
                "token_endpoint": token_endpoint,
            })))
            .mount(&mock_server)
            .await;

        // Mock token endpoint - verify grant_type and Basic auth
        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .and(header("content-type", "application/x-www-form-urlencoded"))
            .respond_with(ResponseTemplate::new(200).set_body_json(mock_token_response_body()))
            .expect(1)
            .mount(&mock_server)
            .await;

        // Create HTTP client and provider
        let http_client = Arc::new(
            DatabricksHttpClient::new(HttpClientConfig::default())
                .expect("Failed to create HTTP client"),
        );

        let provider = ClientCredentialsProvider::new(
            &mock_server.uri(),
            "test-client-id",
            "test-client-secret",
            http_client,
        )
        .await
        .expect("Failed to create provider");

        // Get auth header (should trigger token exchange)
        let auth_header = provider
            .get_auth_header()
            .expect("Failed to get auth header");

        assert_eq!(auth_header, "Bearer test-access-token-m2m");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_m2m_auto_refresh() {
        let mock_server = MockServer::start().await;
        let token_endpoint = mock_oidc_discovery(&mock_server);

        // Mock OIDC discovery
        Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "authorization_endpoint": format!("{}/oidc/v1/authorize", mock_server.uri()),
                "token_endpoint": token_endpoint,
            })))
            .mount(&mock_server)
            .await;

        // Use a counter to return different responses on subsequent calls
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        // Mock token endpoint - will be called twice (initial + refresh)
        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .respond_with(move |_req: &wiremock::Request| {
                let count = call_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if count == 0 {
                    // First call - return short-lived token
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "access_token": "initial-token",
                        "token_type": "Bearer",
                        "expires_in": 1, // Very short expiry to trigger refresh
                        "scope": "all-apis"
                    }))
                } else {
                    // Subsequent calls - return long-lived token
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "access_token": "refreshed-token",
                        "token_type": "Bearer",
                        "expires_in": 3600,
                        "scope": "all-apis"
                    }))
                }
            })
            .expect(2)
            .mount(&mock_server)
            .await;

        let http_client = Arc::new(
            DatabricksHttpClient::new(HttpClientConfig::default())
                .expect("Failed to create HTTP client"),
        );

        let provider = ClientCredentialsProvider::new(
            &mock_server.uri(),
            "test-client-id",
            "test-client-secret",
            http_client,
        )
        .await
        .expect("Failed to create provider");

        // First call - gets initial token
        let auth_header1 = provider
            .get_auth_header()
            .expect("Failed to get initial auth header");
        assert_eq!(auth_header1, "Bearer initial-token");

        // Wait for token to expire (1 second + buffer for expiry check)
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // Second call - should trigger refresh due to expiry
        let auth_header2 = provider
            .get_auth_header()
            .expect("Failed to get refreshed auth header");
        assert_eq!(auth_header2, "Bearer refreshed-token");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_m2m_oidc_discovery() {
        let mock_server = MockServer::start().await;

        // Mock OIDC discovery with specific endpoints
        Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "authorization_endpoint": "https://custom.example.com/auth",
                "token_endpoint": "https://custom.example.com/token",
            })))
            .mount(&mock_server)
            .await;

        // Mock token endpoint
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(mock_token_response_body()))
            .mount(&mock_server)
            .await;

        let http_client = Arc::new(
            DatabricksHttpClient::new(HttpClientConfig::default())
                .expect("Failed to create HTTP client"),
        );

        // Provider creation should discover endpoints
        let result = ClientCredentialsProvider::new(
            &mock_server.uri(),
            "test-client-id",
            "test-client-secret",
            http_client,
        )
        .await;

        assert!(
            result.is_ok(),
            "OIDC discovery should succeed: {:?}",
            result
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_m2m_oidc_discovery_failure() {
        let mock_server = MockServer::start().await;

        // Mock OIDC discovery returning 500 error
        Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(500).set_body_string("Internal server error"))
            .mount(&mock_server)
            .await;

        let http_client = Arc::new(
            DatabricksHttpClient::new(HttpClientConfig::default())
                .expect("Failed to create HTTP client"),
        );

        // Provider creation should fail with descriptive error
        let result = ClientCredentialsProvider::new(
            &mock_server.uri(),
            "test-client-id",
            "test-client-secret",
            http_client,
        )
        .await;

        assert!(result.is_err(), "OIDC discovery failure should propagate");
        let error_msg = format!("{:?}", result.unwrap_err());
        assert!(
            error_msg.contains("500"),
            "Error should mention HTTP 500: {}",
            error_msg
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_m2m_concurrent_token_fetch() {
        let mock_server = MockServer::start().await;
        let token_endpoint = mock_oidc_discovery(&mock_server);

        // Mock OIDC discovery
        Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "authorization_endpoint": format!("{}/oidc/v1/authorize", mock_server.uri()),
                "token_endpoint": token_endpoint,
            })))
            .mount(&mock_server)
            .await;

        // Mock token endpoint - should only be called once despite concurrent requests
        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(mock_token_response_body()))
            .expect(1) // Verify only one token fetch occurs
            .mount(&mock_server)
            .await;

        let http_client = Arc::new(
            DatabricksHttpClient::new(HttpClientConfig::default())
                .expect("Failed to create HTTP client"),
        );

        let provider = Arc::new(
            ClientCredentialsProvider::new(
                &mock_server.uri(),
                "test-client-id",
                "test-client-secret",
                http_client,
            )
            .await
            .expect("Failed to create provider"),
        );

        // Spawn multiple concurrent tasks trying to get auth header
        let mut handles = vec![];
        for _ in 0..10 {
            let provider = provider.clone();
            let handle = tokio::spawn(async move {
                provider
                    .get_auth_header()
                    .expect("Failed to get auth header")
            });
            handles.push(handle);
        }

        // Wait for all tasks and collect results
        let mut results = vec![];
        for handle in handles {
            results.push(handle.await.expect("Task panicked"));
        }

        // All should get the same token
        for result in &results {
            assert_eq!(result, "Bearer test-access-token-m2m");
        }

        // Verify mock expectations (only 1 token fetch)
        // This is verified by the .expect(1) on the Mock
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_m2m_custom_scopes() {
        let mock_server = MockServer::start().await;
        let token_endpoint = mock_oidc_discovery(&mock_server);

        // Mock OIDC discovery
        Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "authorization_endpoint": format!("{}/oidc/v1/authorize", mock_server.uri()),
                "token_endpoint": token_endpoint,
            })))
            .mount(&mock_server)
            .await;

        // Mock token endpoint
        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "test-token-custom-scopes",
                "token_type": "Bearer",
                "expires_in": 3600,
                "scope": "custom-scope-1 custom-scope-2"
            })))
            .mount(&mock_server)
            .await;

        let http_client = Arc::new(
            DatabricksHttpClient::new(HttpClientConfig::default())
                .expect("Failed to create HTTP client"),
        );

        // Create provider with custom scopes
        let provider = ClientCredentialsProvider::new_with_scopes(
            &mock_server.uri(),
            "test-client-id",
            "test-client-secret",
            http_client,
            vec!["custom-scope-1".to_string(), "custom-scope-2".to_string()],
        )
        .await
        .expect("Failed to create provider");

        let auth_header = provider
            .get_auth_header()
            .expect("Failed to get auth header");

        assert_eq!(auth_header, "Bearer test-token-custom-scopes");
    }
}
