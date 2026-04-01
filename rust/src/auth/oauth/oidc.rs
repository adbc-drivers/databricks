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

//! OIDC endpoint discovery for Databricks OAuth flows.
//!
//! This module provides discovery of OAuth 2.0 authorization and token endpoints
//! via the OIDC well-known configuration endpoint. Both M2M (client credentials)
//! and U2M (authorization code + PKCE) flows require these endpoints to construct
//! the `oauth2::BasicClient`.

use crate::client::http::DatabricksHttpClient;
use crate::client::retry::RequestType;
use crate::error::{DatabricksErrorHelper, Result};
use driverbase::error::ErrorHelper;
use serde::Deserialize;
use std::sync::Arc;

/// OAuth 2.0 authorization and token endpoints discovered via OIDC.
///
/// These endpoints are discovered by fetching the OIDC well-known configuration
/// from `{host}/oidc/.well-known/oauth-authorization-server`. The endpoints are
/// required to construct an `oauth2::BasicClient` for both M2M and U2M flows.
#[derive(Debug, Clone, PartialEq)]
pub struct OidcEndpoints {
    /// The OAuth 2.0 authorization endpoint URL.
    /// Used by U2M flow to initiate browser-based authorization.
    pub authorization_endpoint: String,

    /// The OAuth 2.0 token endpoint URL.
    /// Used by both M2M and U2M flows to exchange credentials/codes for access tokens.
    pub token_endpoint: String,
}

/// Internal struct for deserializing the OIDC discovery response.
/// The well-known endpoint returns additional fields, but we only need these two.
#[derive(Debug, Deserialize)]
struct OidcDiscoveryResponse {
    authorization_endpoint: String,
    token_endpoint: String,
}

impl OidcEndpoints {
    /// Discovers OAuth 2.0 endpoints for a Databricks workspace.
    ///
    /// Fetches the OIDC well-known configuration from:
    /// `{host}/oidc/.well-known/oauth-authorization-server`
    ///
    /// The discovery endpoint returns JSON with (at minimum) these fields:
    /// ```json
    /// {
    ///   "authorization_endpoint": "https://{host}/oidc/v1/authorize",
    ///   "token_endpoint": "https://{host}/oidc/v1/token",
    ///   ...
    /// }
    /// ```
    ///
    /// # Arguments
    ///
    /// * `host` - The Databricks workspace host (e.g., "https://my-workspace.cloud.databricks.com")
    /// * `http_client` - The HTTP client to use for the discovery request
    ///
    /// # Returns
    ///
    /// `Ok(OidcEndpoints)` with the discovered endpoints, or an error if:
    /// - The HTTP request fails (network error, 404, 5xx, etc.)
    /// - The response body is not valid JSON
    /// - The response JSON is missing required fields
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use databricks_adbc::auth::oauth::oidc::OidcEndpoints;
    /// use databricks_adbc::client::http::{DatabricksHttpClient, HttpClientConfig};
    /// use std::sync::Arc;
    ///
    /// # async fn example() -> databricks_adbc::error::Result<()> {
    /// let http_client = Arc::new(DatabricksHttpClient::with_default_retry(HttpClientConfig::default())?);
    /// let endpoints = OidcEndpoints::discover(
    ///     "https://my-workspace.cloud.databricks.com",
    ///     &http_client
    /// ).await?;
    ///
    /// println!("Authorization endpoint: {}", endpoints.authorization_endpoint);
    /// println!("Token endpoint: {}", endpoints.token_endpoint);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn discover(host: &str, http_client: &Arc<DatabricksHttpClient>) -> Result<Self> {
        // Construct the well-known endpoint URL (strip trailing slash to avoid double-slash)
        let host = host.trim_end_matches('/');
        let discovery_url = format!("{}/oidc/.well-known/oauth-authorization-server", host);

        // Build the HTTP request
        let request = http_client
            .inner()
            .get(&discovery_url)
            .build()
            .map_err(|e| {
                DatabricksErrorHelper::io()
                    .message(format!("Failed to build OIDC discovery request: {}", e))
            })?;

        // Execute the request without authentication (discovery endpoint is public)
        let response = http_client
            .execute_without_auth(request, RequestType::AuthDiscovery)
            .await?;

        // Check if the response status is successful
        let status = response.status();
        if !status.is_success() {
            let error_body = response.text().await.unwrap_or_default();
            return Err(DatabricksErrorHelper::io().message(format!(
                "OIDC discovery failed with HTTP {}: {}",
                status.as_u16(),
                error_body
            )));
        }

        // Parse the JSON response
        let discovery_response: OidcDiscoveryResponse = response.json().await.map_err(|e| {
            DatabricksErrorHelper::io().message(format!(
                "Failed to parse OIDC discovery response as JSON: {}",
                e
            ))
        })?;

        // Return the endpoints
        Ok(Self {
            authorization_endpoint: discovery_response.authorization_endpoint,
            token_endpoint: discovery_response.token_endpoint,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::http::HttpClientConfig;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_discover_workspace_endpoints() {
        // Start a mock server
        let mock_server = MockServer::start().await;

        // Mock the OIDC discovery endpoint
        let response_body = serde_json::json!({
            "authorization_endpoint": "https://example.com/oidc/v1/authorize",
            "token_endpoint": "https://example.com/oidc/v1/token",
            "issuer": "https://example.com/oidc",
            "jwks_uri": "https://example.com/oidc/v1/keys"
        });

        Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
            .mount(&mock_server)
            .await;

        // Create HTTP client
        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                .expect("Failed to create HTTP client"),
        );

        // Discover endpoints
        let endpoints = OidcEndpoints::discover(&mock_server.uri(), &http_client)
            .await
            .expect("Discovery should succeed");

        // Verify the parsed endpoints
        assert_eq!(
            endpoints.authorization_endpoint,
            "https://example.com/oidc/v1/authorize"
        );
        assert_eq!(
            endpoints.token_endpoint,
            "https://example.com/oidc/v1/token"
        );
    }

    #[tokio::test]
    async fn test_discover_invalid_response() {
        // Start a mock server
        let mock_server = MockServer::start().await;

        // Mock with invalid JSON
        Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not valid json"))
            .mount(&mock_server)
            .await;

        // Create HTTP client
        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                .expect("Failed to create HTTP client"),
        );

        // Discovery should fail with JSON parse error
        let result = OidcEndpoints::discover(&mock_server.uri(), &http_client).await;

        assert!(result.is_err(), "Discovery should fail with invalid JSON");
        let error_msg = format!("{:?}", result.unwrap_err());
        assert!(
            error_msg.contains("Failed to parse OIDC discovery response"),
            "Error should mention JSON parsing failure: {}",
            error_msg
        );
    }

    #[tokio::test]
    async fn test_discover_missing_required_fields() {
        // Start a mock server
        let mock_server = MockServer::start().await;

        // Mock with JSON missing required fields
        let response_body = serde_json::json!({
            "issuer": "https://example.com/oidc",
            "jwks_uri": "https://example.com/oidc/v1/keys"
            // Missing authorization_endpoint and token_endpoint
        });

        Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
            .mount(&mock_server)
            .await;

        // Create HTTP client
        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                .expect("Failed to create HTTP client"),
        );

        // Discovery should fail with deserialization error
        let result = OidcEndpoints::discover(&mock_server.uri(), &http_client).await;

        assert!(result.is_err(), "Discovery should fail with missing fields");
        let error_msg = format!("{:?}", result.unwrap_err());
        assert!(
            error_msg.contains("Failed to parse OIDC discovery response"),
            "Error should mention parsing failure: {}",
            error_msg
        );
    }

    #[tokio::test]
    async fn test_discover_http_error() {
        // Start a mock server
        let mock_server = MockServer::start().await;

        // Mock with 404 response
        Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(404).set_body_string("Endpoint not found"))
            .mount(&mock_server)
            .await;

        // Create HTTP client
        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                .expect("Failed to create HTTP client"),
        );

        // Discovery should fail with HTTP error
        let result = OidcEndpoints::discover(&mock_server.uri(), &http_client).await;

        assert!(result.is_err(), "Discovery should fail with HTTP 404");
        let error_msg = format!("{:?}", result.unwrap_err());
        assert!(
            error_msg.contains("404"),
            "Error should mention HTTP 404: {}",
            error_msg
        );
    }

    #[tokio::test]
    async fn test_discover_http_500_error() {
        // Start a mock server
        let mock_server = MockServer::start().await;

        // Mock with 500 response
        Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(500).set_body_string("Internal server error"))
            .mount(&mock_server)
            .await;

        // Create HTTP client
        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                .expect("Failed to create HTTP client"),
        );

        // Discovery should fail with HTTP error
        let result = OidcEndpoints::discover(&mock_server.uri(), &http_client).await;

        assert!(result.is_err(), "Discovery should fail with HTTP 500");
        let error_msg = format!("{:?}", result.unwrap_err());
        assert!(
            error_msg.contains("500"),
            "Error should mention HTTP 500: {}",
            error_msg
        );
    }

    #[tokio::test]
    async fn test_discover_with_extra_fields() {
        // Start a mock server
        let mock_server = MockServer::start().await;

        // Mock with response containing extra fields (should be ignored)
        let response_body = serde_json::json!({
            "authorization_endpoint": "https://example.com/oidc/v1/authorize",
            "token_endpoint": "https://example.com/oidc/v1/token",
            "issuer": "https://example.com/oidc",
            "jwks_uri": "https://example.com/oidc/v1/keys",
            "response_types_supported": ["code"],
            "grant_types_supported": ["authorization_code", "client_credentials"],
            "token_endpoint_auth_methods_supported": ["client_secret_basic"],
            "extra_field": "should be ignored"
        });

        Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
            .mount(&mock_server)
            .await;

        // Create HTTP client
        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                .expect("Failed to create HTTP client"),
        );

        // Discovery should succeed and ignore extra fields
        let endpoints = OidcEndpoints::discover(&mock_server.uri(), &http_client)
            .await
            .expect("Discovery should succeed with extra fields");

        // Verify only the required fields are parsed
        assert_eq!(
            endpoints.authorization_endpoint,
            "https://example.com/oidc/v1/authorize"
        );
        assert_eq!(
            endpoints.token_endpoint,
            "https://example.com/oidc/v1/token"
        );
    }
}
