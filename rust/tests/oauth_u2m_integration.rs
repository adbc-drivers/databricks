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

//! Wiremock integration tests for OAuth U2M refresh token flow.
//!
//! These tests use the wiremock crate to simulate OIDC discovery and token endpoints,
//! allowing us to test the complete U2M refresh flow without requiring a real OAuth server
//! or browser interaction.

use databricks_adbc::auth::oauth::u2m::AuthorizationCodeProvider;
use databricks_adbc::auth::AuthProvider;
use databricks_adbc::client::http::{DatabricksHttpClient, HttpClientConfig};
use std::sync::Arc;
use std::time::Duration;
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Helper function to mock OIDC discovery endpoint.
///
/// Sets up a wiremock endpoint at `/oidc/.well-known/oauth-authorization-server`
/// that returns the authorization and token endpoint URLs.
async fn mock_oidc_discovery(mock_server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/oidc/.well-known/oauth-authorization-server"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "authorization_endpoint": format!("{}/oidc/v1/authorize", mock_server.uri()),
            "token_endpoint": format!("{}/oidc/v1/token", mock_server.uri()),
        })))
        .mount(mock_server)
        .await;
}

/// Wiremock integration test for U2M refresh token flow.
///
/// This test verifies the complete refresh token path:
/// 1. Pre-populate TokenCache with a token that has a valid refresh_token
/// 2. Wiremock serves the OIDC discovery and token endpoints
/// 3. Verify that get_auth_header() triggers refresh via grant_type=refresh_token
/// 4. Returns a valid Bearer token with the new access token
///
/// This tests the refresh path without requiring actual browser flow.
#[tokio::test(flavor = "multi_thread")]
async fn test_u2m_refresh_token_full_flow() {
    // Setup: Start wiremock server
    let mock_server = MockServer::start().await;

    // Step 1: Mock OIDC discovery endpoint
    mock_oidc_discovery(&mock_server).await;

    // Step 2: Mock token endpoint - expects grant_type=refresh_token
    // This validates that the refresh flow is being triggered correctly
    Mock::given(method("POST"))
        .and(path("/oidc/v1/token"))
        .and(body_string_contains("grant_type=refresh_token"))
        .and(body_string_contains("refresh_token=old-refresh-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "new-refreshed-access-token",
            "token_type": "Bearer",
            "expires_in": 3600,
            "refresh_token": "new-refresh-token",
            "scope": "all-apis offline_access"
        })))
        .expect(1) // Expect exactly one refresh call
        .mount(&mock_server)
        .await;

    // Step 3: Create HTTP client
    let http_client = Arc::new(
        DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
            .expect("Failed to create HTTP client"),
    );

    // Step 4: Pre-populate TokenCache with an expired token that has a refresh_token
    // This simulates a cached token from a previous session
    use databricks_adbc::auth::oauth::cache::TokenCache;
    use databricks_adbc::auth::oauth::token::OAuthToken;

    let cached_token = OAuthToken::new(
        "old-access-token".to_string(),
        "Bearer".to_string(),
        1, // Expire immediately (< 40s) to trigger refresh
        Some("old-refresh-token".to_string()),
        vec!["all-apis".to_string(), "offline_access".to_string()],
    );

    // Save to cache (will be loaded by AuthorizationCodeProvider)
    TokenCache::save(
        &mock_server.uri(),
        "test-client-id",
        &["all-apis".to_string(), "offline_access".to_string()],
        &cached_token,
    );

    // Step 5: Create AuthorizationCodeProvider - should load cached token
    let provider = AuthorizationCodeProvider::new_with_config(
        &mock_server.uri(),
        "test-client-id",
        http_client,
        vec!["all-apis".to_string(), "offline_access".to_string()],
        8020,
        Duration::from_secs(120),
    )
    .await
    .expect("Failed to create AuthorizationCodeProvider");

    // Wait a bit to ensure token is definitely expired
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Step 6: Call get_auth_header() - should trigger refresh via grant_type=refresh_token
    let auth_header = provider
        .get_auth_header()
        .expect("Failed to get auth header");

    // Step 7: Verify the response
    assert_eq!(
        auth_header, "Bearer new-refreshed-access-token",
        "Should return Bearer token with new access token"
    );

    // Wiremock will automatically verify that the token endpoint was called exactly once
    // with the correct grant_type=refresh_token and refresh_token parameters
}

/// Test U2M refresh token flow with stale token (immediate return).
///
/// This test verifies that when a token is STALE (not expired, but past the stale threshold),
/// the current token is returned immediately without blocking.
///
/// Note: Background refresh is not tested here due to runtime context limitations in test env.
/// The refresh mechanism itself is thoroughly tested in test_u2m_refresh_token_full_flow.
#[tokio::test(flavor = "multi_thread")]
async fn test_u2m_refresh_token_stale_immediate_return() {
    let mock_server = MockServer::start().await;
    mock_oidc_discovery(&mock_server).await;

    let http_client = Arc::new(
        DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
            .expect("Failed to create HTTP client"),
    );

    // Create a STALE token: long expiry but past the stale threshold
    use chrono::{Duration as ChronoDuration, Utc};
    use databricks_adbc::auth::oauth::cache::TokenCache;
    use databricks_adbc::auth::oauth::token::OAuthToken;

    // Create token with 25 minutes initial TTL (1500s)
    // Stale threshold will be min(1500/2, 1200) = 750s
    let mut stale_token = OAuthToken::new(
        "stale-access-token".to_string(),
        "Bearer".to_string(),
        1500,
        Some("stale-refresh-token".to_string()),
        vec!["all-apis".to_string(), "offline_access".to_string()],
    );
    // Manually set expires_at to 10 minutes from now (600s)
    // This is less than threshold (750s) but more than expiry buffer (40s)
    stale_token.expires_at = Utc::now() + ChronoDuration::seconds(600);

    TokenCache::save(
        &mock_server.uri(),
        "test-client-id-stale",
        &["all-apis".to_string(), "offline_access".to_string()],
        &stale_token,
    );

    let provider = AuthorizationCodeProvider::new_with_config(
        &mock_server.uri(),
        "test-client-id-stale",
        http_client,
        vec!["all-apis".to_string(), "offline_access".to_string()],
        8021,
        Duration::from_secs(120),
    )
    .await
    .expect("Failed to create provider");

    // Measure time to get auth header - should be immediate (not blocked waiting for refresh)
    let start = std::time::Instant::now();
    let auth_header = provider
        .get_auth_header()
        .expect("Failed to get auth header");
    let elapsed = start.elapsed();

    // Should return the stale token immediately (not blocked)
    assert_eq!(
        auth_header, "Bearer stale-access-token",
        "Should return current stale token immediately"
    );

    // Should complete in less than 500ms (immediate return, not waiting for refresh)
    assert!(
        elapsed.as_millis() < 500,
        "Should return immediately without blocking on refresh, took {:?}",
        elapsed
    );
}

/// Test U2M refresh token flow when refresh token is invalid/expired.
///
/// This test verifies that when the refresh token is invalid or expired,
/// the provider falls back to the browser flow (which will fail in this test,
/// as expected, since we can't simulate a real browser).
#[tokio::test(flavor = "multi_thread")]
async fn test_u2m_refresh_token_expired_fallback() {
    let mock_server = MockServer::start().await;
    mock_oidc_discovery(&mock_server).await;

    // Mock token endpoint - return error for refresh_token grant
    Mock::given(method("POST"))
        .and(path("/oidc/v1/token"))
        .and(body_string_contains("grant_type=refresh_token"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "error": "invalid_grant",
            "error_description": "Refresh token expired or invalid"
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    let http_client = Arc::new(
        DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
            .expect("Failed to create HTTP client"),
    );

    use databricks_adbc::auth::oauth::cache::TokenCache;
    use databricks_adbc::auth::oauth::token::OAuthToken;

    let expired_token = OAuthToken::new(
        "expired-access-token".to_string(),
        "Bearer".to_string(),
        1, // Expire immediately
        Some("expired-refresh-token".to_string()),
        vec!["all-apis".to_string(), "offline_access".to_string()],
    );

    TokenCache::save(
        &mock_server.uri(),
        "test-client-id-expired",
        &["all-apis".to_string(), "offline_access".to_string()],
        &expired_token,
    );

    let mut provider = AuthorizationCodeProvider::new_with_config(
        &mock_server.uri(),
        "test-client-id-expired",
        http_client,
        vec!["all-apis".to_string(), "offline_access".to_string()],
        0,                      // Use port 0 to avoid conflicts
        Duration::from_secs(5), // Short timeout for test
    )
    .await
    .expect("Failed to create provider");
    provider.suppress_browser(); // Don't open a real browser in tests

    // Wait for token to expire
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Get auth header - should attempt refresh, fail, then try browser flow
    // Browser flow will fail (no actual browser), which is expected
    let result = provider.get_auth_header();

    assert!(
        result.is_err(),
        "Should fail when falling back to browser flow in test"
    );

    // Wiremock will verify that refresh was attempted
}

/// Test that TokenCache is updated after successful refresh.
///
/// This test verifies that after a successful token refresh, the new token
/// is saved to the cache for future use.
#[tokio::test(flavor = "multi_thread")]
async fn test_u2m_refresh_updates_cache() {
    let mock_server = MockServer::start().await;
    mock_oidc_discovery(&mock_server).await;

    Mock::given(method("POST"))
        .and(path("/oidc/v1/token"))
        .and(body_string_contains("grant_type=refresh_token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "cache-updated-token",
            "token_type": "Bearer",
            "expires_in": 3600,
            "refresh_token": "new-cached-refresh-token",
            "scope": "all-apis offline_access"
        })))
        .mount(&mock_server)
        .await;

    let http_client = Arc::new(
        DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
            .expect("Failed to create HTTP client"),
    );

    use databricks_adbc::auth::oauth::cache::TokenCache;
    use databricks_adbc::auth::oauth::token::OAuthToken;

    let old_token = OAuthToken::new(
        "old-cached-token".to_string(),
        "Bearer".to_string(),
        1, // Expire immediately
        Some("old-cached-refresh".to_string()),
        vec!["all-apis".to_string(), "offline_access".to_string()],
    );

    TokenCache::save(
        &mock_server.uri(),
        "test-client-id-cache",
        &["all-apis".to_string(), "offline_access".to_string()],
        &old_token,
    );

    let provider = AuthorizationCodeProvider::new_with_config(
        &mock_server.uri(),
        "test-client-id-cache",
        http_client,
        vec!["all-apis".to_string(), "offline_access".to_string()],
        8023,
        Duration::from_secs(120),
    )
    .await
    .expect("Failed to create provider");

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Trigger refresh
    let _auth_header = provider
        .get_auth_header()
        .expect("Failed to get auth header");

    // Verify cache was updated by loading it again
    let cached_token = TokenCache::load(
        &mock_server.uri(),
        "test-client-id-cache",
        &["all-apis".to_string(), "offline_access".to_string()],
    );

    assert!(cached_token.is_some(), "Cache should contain updated token");
    let cached_token = cached_token.unwrap();
    assert_eq!(
        cached_token.access_token, "cache-updated-token",
        "Cache should contain the new access token"
    );
    assert_eq!(
        cached_token.refresh_token,
        Some("new-cached-refresh-token".to_string()),
        "Cache should contain the new refresh token"
    );
}
