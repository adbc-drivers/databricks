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

//! OAuth token representation and lifecycle management.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

/// Expiry buffer: tokens are considered expired when remaining TTL < 40 seconds.
/// This 40s buffer matches the Python SDK and accounts for clock skew and network latency.
/// Azure rejects tokens within 30s of expiry, so 40s provides a safe margin.
const EXPIRY_BUFFER_SECONDS: i64 = 40;

/// Maximum stale threshold: 20 minutes.
/// The stale threshold is min(initial_TTL * 0.5, 20 minutes).
const MAX_STALE_THRESHOLD_MINUTES: i64 = 20;

/// OAuth 2.0 access token with metadata and lifecycle state.
///
/// This struct represents an OAuth access token with its associated metadata.
/// It provides methods to check if the token is expired or stale, which are
/// used by the token refresh state machine to determine when to refresh tokens.
///
/// # Token Lifecycle States
///
/// - **FRESH**: Token has significant remaining TTL (not stale or expired)
/// - **STALE**: Remaining TTL < stale threshold; eligible for background refresh
/// - **EXPIRED**: Remaining TTL < 40 seconds; must be refreshed before use
///
/// # Stale Threshold Calculation
///
/// The stale threshold is computed once when the token is first acquired as:
/// `min(initial_TTL * 0.5, 20 minutes)`
///
/// This means:
/// - A token with 1 hour initial TTL becomes stale after 30 minutes (50%)
/// - A token with 2 hour initial TTL becomes stale after 20 minutes (capped at max)
///
/// The threshold is computed using the full TTL at acquisition time, not the
/// current remaining time, and is stored alongside the token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthToken {
    /// The OAuth access token string.
    pub access_token: String,

    /// Token type (typically "Bearer").
    pub token_type: String,

    /// Optional refresh token for obtaining new access tokens.
    /// Only present for U2M (authorization code) flow; M2M (client credentials) doesn't provide refresh tokens.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,

    /// Absolute timestamp when the token expires (UTC).
    pub expires_at: DateTime<Utc>,

    /// Scopes granted to this token.
    #[serde(default)]
    pub scopes: Vec<String>,

    /// Stale threshold in seconds: min(initial_TTL * 0.5, 20 minutes).
    /// Computed once when the token is acquired and stored with the token.
    /// When remaining TTL drops below this threshold, the token becomes STALE
    /// and eligible for background refresh.
    pub stale_threshold_seconds: i64,
}

impl OAuthToken {
    /// Creates a new OAuth token.
    ///
    /// The `expires_in` parameter is the token lifetime in seconds from now.
    /// The stale threshold is automatically computed as `min(expires_in * 0.5, 20 minutes)`.
    ///
    /// # Arguments
    ///
    /// * `access_token` - The OAuth access token string
    /// * `token_type` - Token type (typically "Bearer")
    /// * `expires_in` - Token lifetime in seconds from now
    /// * `refresh_token` - Optional refresh token
    /// * `scopes` - List of granted scopes
    pub fn new(
        access_token: String,
        token_type: String,
        expires_in: i64,
        refresh_token: Option<String>,
        scopes: Vec<String>,
    ) -> Self {
        let now = Utc::now();
        let expires_at = now + Duration::seconds(expires_in);

        // Compute stale threshold: min(initial_TTL * 0.5, 20 minutes)
        let stale_threshold_seconds = (expires_in / 2).min(MAX_STALE_THRESHOLD_MINUTES * 60);

        Self {
            access_token,
            token_type,
            refresh_token,
            expires_at,
            scopes,
            stale_threshold_seconds,
        }
    }

    /// Checks if the token is expired.
    ///
    /// A token is considered expired when its remaining TTL is less than 40 seconds.
    /// This buffer accounts for clock skew and network latency. The 40-second value
    /// matches the Python Databricks SDK and provides a safe margin, as Azure rejects
    /// tokens within 30 seconds of their actual expiry time.
    ///
    /// # Returns
    ///
    /// `true` if the token has less than 40 seconds of remaining lifetime.
    pub fn is_expired(&self) -> bool {
        let now = Utc::now();
        let remaining = self.expires_at - now;
        remaining.num_seconds() < EXPIRY_BUFFER_SECONDS
    }

    /// Checks if the token is stale and eligible for background refresh.
    ///
    /// A token is stale when its remaining TTL drops below the stale threshold,
    /// which is `min(initial_TTL * 0.5, 20 minutes)`. The threshold is computed
    /// once when the token is acquired and stored with the token.
    ///
    /// Stale tokens are still valid for use, but a background refresh should be
    /// initiated to obtain a fresh token before the current one expires.
    ///
    /// # Returns
    ///
    /// `true` if the remaining TTL is less than the stale threshold.
    pub fn is_stale(&self) -> bool {
        let now = Utc::now();
        let remaining = self.expires_at - now;
        remaining.num_seconds() < self.stale_threshold_seconds
    }

    /// Returns the remaining time-to-live in seconds.
    ///
    /// This is useful for debugging and logging. Returns 0 if the token has already expired.
    pub fn remaining_seconds(&self) -> i64 {
        let now = Utc::now();
        let remaining = self.expires_at - now;
        remaining.num_seconds().max(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_fresh_not_expired() {
        // Token with >20 minutes TTL should be neither stale nor expired
        let token = OAuthToken::new(
            "access-token".to_string(),
            "Bearer".to_string(),
            3600, // 1 hour
            Some("refresh-token".to_string()),
            vec!["all-apis".to_string()],
        );

        assert!(!token.is_expired(), "Fresh token should not be expired");
        assert!(!token.is_stale(), "Fresh token should not be stale");
        assert!(
            token.remaining_seconds() > 3500,
            "Token should have most of its TTL remaining"
        );
    }

    #[test]
    fn test_token_stale_threshold() {
        // Token with 1 hour TTL: stale threshold = min(3600/2, 1200) = 1200 seconds (20 min)
        let token = OAuthToken::new(
            "access-token".to_string(),
            "Bearer".to_string(),
            3600,
            None,
            vec![],
        );

        assert_eq!(
            token.stale_threshold_seconds, 1200,
            "Stale threshold for 1h token should be 20 minutes (capped)"
        );

        // Token with 30 minutes TTL: stale threshold = min(1800/2, 1200) = 900 seconds (15 min)
        let token2 = OAuthToken::new(
            "access-token".to_string(),
            "Bearer".to_string(),
            1800,
            None,
            vec![],
        );

        assert_eq!(
            token2.stale_threshold_seconds, 900,
            "Stale threshold for 30min token should be 15 minutes (50%)"
        );

        // Token with 10 minutes TTL: stale threshold = min(600/2, 1200) = 300 seconds (5 min)
        let token3 = OAuthToken::new(
            "access-token".to_string(),
            "Bearer".to_string(),
            600,
            None,
            vec![],
        );

        assert_eq!(
            token3.stale_threshold_seconds, 300,
            "Stale threshold for 10min token should be 5 minutes (50%)"
        );
    }

    #[test]
    fn test_token_expired_within_buffer() {
        // Create a token that expires in 30 seconds (less than 40s buffer)
        // With 30s TTL, stale_threshold = 30/2 = 15s
        let token = OAuthToken::new(
            "access-token".to_string(),
            "Bearer".to_string(),
            30,
            None,
            vec![],
        );

        assert!(
            token.is_expired(),
            "Token with <40s remaining should be expired"
        );
        // Note: With 30s remaining and stale threshold of 15s, the token is NOT stale yet.
        // A token is stale when remaining < threshold. Here 30 > 15, so not stale.
        // However, expired tokens are always considered problematic, so we don't test stale here.

        // Token expiring in 41 seconds should NOT be expired (just above the 40s buffer)
        let token2 = OAuthToken::new(
            "access-token".to_string(),
            "Bearer".to_string(),
            41,
            None,
            vec![],
        );

        assert!(
            !token2.is_expired(),
            "Token with 41s remaining should not be expired"
        );

        // Create a token with 10 seconds remaining - should be both expired and stale
        // With 10s TTL, stale_threshold = 10/2 = 5s, and 10 < 40 (expired), 10 > 5 (not stale)
        // Let's create a token and then manually adjust expires_at to test expired+stale
        let mut token3 = OAuthToken::new(
            "access-token".to_string(),
            "Bearer".to_string(),
            100,
            None,
            vec![],
        );
        // stale_threshold = 50s
        // Set expires_at to 20 seconds from now (< 40s buffer, < 50s threshold)
        token3.expires_at = Utc::now() + Duration::seconds(20);

        assert!(
            token3.is_expired(),
            "Token with 20s remaining should be expired (< 40s)"
        );
        assert!(
            token3.is_stale(),
            "Token with 20s remaining should be stale (< 50s threshold)"
        );
    }

    #[test]
    fn test_token_serialization_roundtrip() {
        let original = OAuthToken::new(
            "my-access-token".to_string(),
            "Bearer".to_string(),
            3600,
            Some("my-refresh-token".to_string()),
            vec!["all-apis".to_string(), "offline_access".to_string()],
        );

        // Serialize to JSON
        let json = serde_json::to_string(&original).expect("serialization should succeed");

        // Deserialize back
        let deserialized: OAuthToken =
            serde_json::from_str(&json).expect("deserialization should succeed");

        // Verify all fields match
        assert_eq!(deserialized.access_token, original.access_token);
        assert_eq!(deserialized.token_type, original.token_type);
        assert_eq!(deserialized.refresh_token, original.refresh_token);
        assert_eq!(deserialized.expires_at, original.expires_at);
        assert_eq!(deserialized.scopes, original.scopes);
        assert_eq!(
            deserialized.stale_threshold_seconds,
            original.stale_threshold_seconds
        );
    }

    #[test]
    fn test_token_without_refresh_token_serialization() {
        // Test that tokens without refresh_token serialize correctly (field omitted)
        let token = OAuthToken::new(
            "access-token".to_string(),
            "Bearer".to_string(),
            3600,
            None, // No refresh token (M2M flow)
            vec!["all-apis".to_string()],
        );

        let json = serde_json::to_string(&token).expect("serialization should succeed");

        // Verify refresh_token field is not present in JSON
        assert!(
            !json.contains("refresh_token"),
            "JSON should not contain refresh_token field when None"
        );

        // Verify deserialization works
        let deserialized: OAuthToken =
            serde_json::from_str(&json).expect("deserialization should succeed");
        assert_eq!(deserialized.refresh_token, None);
    }

    #[test]
    fn test_token_stale_but_not_expired() {
        // Create a token with 25 minutes TTL (1500 seconds)
        // Stale threshold = min(1500/2, 1200) = 750 seconds
        // Then manually adjust expires_at to simulate time passing
        let mut token = OAuthToken::new(
            "access-token".to_string(),
            "Bearer".to_string(),
            1500,
            None,
            vec![],
        );

        // Move expires_at to 10 minutes from now (600 seconds)
        // This is less than stale threshold (750s) but more than expiry buffer (40s)
        token.expires_at = Utc::now() + Duration::seconds(600);

        assert!(
            token.is_stale(),
            "Token with 600s remaining should be stale (threshold 750s)"
        );
        assert!(
            !token.is_expired(),
            "Token with 600s remaining should not be expired"
        );
    }

    #[test]
    fn test_remaining_seconds_non_negative() {
        // Create an already-expired token
        let mut token = OAuthToken::new(
            "access-token".to_string(),
            "Bearer".to_string(),
            3600,
            None,
            vec![],
        );

        // Set expires_at to the past
        token.expires_at = Utc::now() - Duration::seconds(100);

        assert_eq!(
            token.remaining_seconds(),
            0,
            "remaining_seconds should return 0 for expired tokens"
        );
    }
}
