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

//! Retry logic for the Databricks HTTP client.
//!
//! Implements the standardized DBSQL connector retry specification with:
//! - Idempotency-aware retry strategies (idempotent vs non-idempotent)
//! - `Retry-After` header support with min/max clamping
//! - Exponential backoff with random jitter (50ms–750ms)
//! - Cumulative overall timeout
//! - Per-category configuration with global defaults

use rand::Rng;
use reqwest::StatusCode;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

/// Identifies the type of request being made.
///
/// The HTTP client uses this to look up the correct `RetryConfig` (from category)
/// and select the correct `RetryStrategy` (from idempotency). This is the single
/// source of truth for request classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RequestType {
    // SEA
    CreateSession,
    CloseSession,
    ExecuteStatement,
    ExecuteMetadataQuery,
    GetStatementStatus,
    CancelStatement,
    CloseStatement,
    GetResultChunks,
    // CloudFetch
    CloudFetchDownload,
    // Auth
    AuthTokenRequest,
    AuthDiscovery,
}

/// Category of request, used to look up per-category retry configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RequestCategory {
    Sea,
    CloudFetch,
    Auth,
}

/// Whether a request is safe to retry after a failure where the server
/// may have processed the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestIdempotency {
    Idempotent,
    NonIdempotent,
}

impl RequestType {
    /// Returns the category for retry config lookup.
    pub fn category(self) -> RequestCategory {
        match self {
            Self::CreateSession
            | Self::CloseSession
            | Self::ExecuteStatement
            | Self::ExecuteMetadataQuery
            | Self::GetStatementStatus
            | Self::CancelStatement
            | Self::CloseStatement
            | Self::GetResultChunks => RequestCategory::Sea,

            Self::CloudFetchDownload => RequestCategory::CloudFetch,

            Self::AuthTokenRequest | Self::AuthDiscovery => RequestCategory::Auth,
        }
    }

    /// Returns the idempotency classification for strategy selection.
    pub fn idempotency(self) -> RequestIdempotency {
        match self {
            Self::ExecuteStatement => RequestIdempotency::NonIdempotent,
            _ => RequestIdempotency::Idempotent,
        }
    }
}

/// Configuration for retry behavior. Each request category can have its own config,
/// with unset fields falling back to global defaults.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Minimum wait time per retry. Clamps both exponential backoff floor
    /// and Retry-After minimum.
    pub min_wait: Duration,

    /// Maximum wait time per retry. Clamps both exponential backoff ceiling
    /// and Retry-After maximum.
    pub max_wait: Duration,

    /// Cumulative wall-clock timeout for all retry attempts.
    /// Stops retrying when total elapsed time exceeds this.
    pub overall_timeout: Duration,

    /// Maximum number of retry attempts. A request is attempted at most
    /// `max_retries + 1` times (1 initial + N retries).
    pub max_retries: u32,

    /// Override the set of HTTP codes that are retryable.
    /// When set, this becomes the **exhaustive** set — only these codes are
    /// retried, regardless of the strategy's default logic.
    pub override_retryable_codes: Option<HashSet<u16>>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            min_wait: Duration::from_secs(1),
            max_wait: Duration::from_secs(60),
            overall_timeout: Duration::from_secs(900),
            max_retries: 5,
            override_retryable_codes: None,
        }
    }
}

/// Builds a per-category retry config map from global defaults and optional overrides.
pub fn build_retry_configs(
    global: &RetryConfig,
    overrides: &HashMap<RequestCategory, RetryConfigOverrides>,
) -> HashMap<RequestCategory, RetryConfig> {
    let categories = [
        RequestCategory::Sea,
        RequestCategory::CloudFetch,
        RequestCategory::Auth,
    ];

    categories
        .into_iter()
        .map(|cat| {
            let config = if let Some(ovr) = overrides.get(&cat) {
                ovr.apply(global)
            } else {
                global.clone()
            };
            (cat, config)
        })
        .collect()
}

/// Per-category overrides. `None` fields inherit from the global config.
#[derive(Debug, Clone, Default)]
pub struct RetryConfigOverrides {
    pub min_wait: Option<Duration>,
    pub max_wait: Option<Duration>,
    pub overall_timeout: Option<Duration>,
    pub max_retries: Option<u32>,
    pub override_retryable_codes: Option<HashSet<u16>>,
}

impl RetryConfigOverrides {
    /// Apply overrides on top of a base config.
    pub fn apply(&self, base: &RetryConfig) -> RetryConfig {
        RetryConfig {
            min_wait: self.min_wait.unwrap_or(base.min_wait),
            max_wait: self.max_wait.unwrap_or(base.max_wait),
            overall_timeout: self.overall_timeout.unwrap_or(base.overall_timeout),
            max_retries: self.max_retries.unwrap_or(base.max_retries),
            override_retryable_codes: self
                .override_retryable_codes
                .clone()
                .or_else(|| base.override_retryable_codes.clone()),
        }
    }
}

// --- Retry Strategies ---

/// Non-retryable HTTP status codes for idempotent requests.
const IDEMPOTENT_NON_RETRYABLE: &[u16] = &[
    400, 401, 403, 404, 405, 409, 410, 411, 412, 413, 414, 415, 416,
];

/// Retryable HTTP status codes for non-idempotent requests.
const NON_IDEMPOTENT_RETRYABLE: &[u16] = &[429, 503];

/// Determines whether a request should be retried based on the error type
/// and the request's idempotency classification.
pub struct RetryStrategy {
    idempotency: RequestIdempotency,
    override_codes: Option<HashSet<u16>>,
}

impl RetryStrategy {
    /// Create a strategy for the given request type and config.
    pub fn for_request(request_type: RequestType, config: &RetryConfig) -> Self {
        Self {
            idempotency: request_type.idempotency(),
            override_codes: config.override_retryable_codes.clone(),
        }
    }

    /// Whether the given HTTP status code is retryable.
    ///
    /// For non-idempotent requests, 429/503 are only retryable when the server
    /// sends a `Retry-After` header — this is the explicit signal that it is
    /// safe to retry (per Mehmet's review feedback on the spec).
    pub fn is_retryable_status(&self, status: StatusCode, has_retry_after: bool) -> bool {
        let code = status.as_u16();

        // Success codes are never retryable (the caller checks success first,
        // but guard here for safety if the method is called directly)
        if status.is_success() || status.is_redirection() {
            return false;
        }

        if let Some(ref codes) = self.override_codes {
            return codes.contains(&code);
        }

        match self.idempotency {
            // Idempotent: retry everything except known non-retryable codes
            RequestIdempotency::Idempotent => !IDEMPOTENT_NON_RETRYABLE.contains(&code),
            // Non-idempotent: retry only when the server explicitly signals retry
            // via Retry-After header on 429/503
            RequestIdempotency::NonIdempotent => {
                NON_IDEMPOTENT_RETRYABLE.contains(&code) && has_retry_after
            }
        }
    }

    /// Whether the given network/client error is retryable.
    pub fn is_retryable_error(&self, error: &reqwest::Error) -> bool {
        match self.idempotency {
            // Idempotent: all network errors are retryable
            RequestIdempotency::Idempotent => {
                error.is_timeout() || error.is_connect() || error.is_request()
            }
            // Non-idempotent: only connection-level errors (request never reached server)
            RequestIdempotency::NonIdempotent => error.is_connect(),
        }
    }
}

// --- Backoff Calculator ---

/// Calculate the backoff duration for a retry attempt.
///
/// Honors `Retry-After` header if present, otherwise uses exponential backoff.
/// Both are clamped to `[min_wait, max_wait]` and jitter (50ms–750ms) is added.
pub fn calculate_backoff(
    config: &RetryConfig,
    attempt: u32,
    retry_after_header: Option<&str>,
) -> Duration {
    let base_wait = if let Some(wait) = retry_after_header.and_then(parse_retry_after) {
        // Clamp Retry-After to [min_wait, max_wait]
        wait.clamp(config.min_wait, config.max_wait)
    } else {
        // Exponential backoff: min_wait * 2^attempt (per spec)
        let exp = config.min_wait.saturating_mul(2u32.saturating_pow(attempt));
        exp.min(config.max_wait)
    };

    // Add random jitter between 50ms and 750ms
    let jitter_ms = rand::rng().random_range(50..=750);
    base_wait + Duration::from_millis(jitter_ms)
}

/// Parse the `Retry-After` header value.
///
/// Supports:
/// - Seconds: `Retry-After: 120` → 120 seconds
/// - HTTP-date: `Retry-After: Wed, 25 Mar 2026 10:30:00 GMT` → delta from now
fn parse_retry_after(value: &str) -> Option<Duration> {
    // Try seconds first
    if let Ok(seconds) = value.trim().parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }

    // Try HTTP-date (RFC 7231 / RFC 2822)
    if let Ok(date) = chrono::DateTime::parse_from_rfc2822(value.trim()) {
        let now = chrono::Utc::now();
        let target = date.with_timezone(&chrono::Utc);
        if target > now {
            let delta = target - now;
            return Some(Duration::from_secs(delta.num_seconds() as u64));
        }
        // Date is in the past — treat as 0
        return Some(Duration::ZERO);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- RequestType mapping tests ---

    #[test]
    fn test_request_type_category_mapping() {
        assert_eq!(RequestType::CreateSession.category(), RequestCategory::Sea);
        assert_eq!(RequestType::CloseSession.category(), RequestCategory::Sea);
        assert_eq!(
            RequestType::ExecuteStatement.category(),
            RequestCategory::Sea
        );
        assert_eq!(
            RequestType::ExecuteMetadataQuery.category(),
            RequestCategory::Sea
        );
        assert_eq!(
            RequestType::GetStatementStatus.category(),
            RequestCategory::Sea
        );
        assert_eq!(
            RequestType::CancelStatement.category(),
            RequestCategory::Sea
        );
        assert_eq!(RequestType::CloseStatement.category(), RequestCategory::Sea);
        assert_eq!(
            RequestType::GetResultChunks.category(),
            RequestCategory::Sea
        );
        assert_eq!(
            RequestType::CloudFetchDownload.category(),
            RequestCategory::CloudFetch
        );
        assert_eq!(
            RequestType::AuthTokenRequest.category(),
            RequestCategory::Auth
        );
        assert_eq!(RequestType::AuthDiscovery.category(), RequestCategory::Auth);
    }

    #[test]
    fn test_request_type_idempotency_mapping() {
        // Only ExecuteStatement is non-idempotent
        assert_eq!(
            RequestType::ExecuteStatement.idempotency(),
            RequestIdempotency::NonIdempotent
        );

        // Everything else is idempotent
        let idempotent_types = [
            RequestType::CreateSession,
            RequestType::CloseSession,
            RequestType::ExecuteMetadataQuery,
            RequestType::GetStatementStatus,
            RequestType::CancelStatement,
            RequestType::CloseStatement,
            RequestType::GetResultChunks,
            RequestType::CloudFetchDownload,
            RequestType::AuthTokenRequest,
            RequestType::AuthDiscovery,
        ];
        for rt in idempotent_types {
            assert_eq!(
                rt.idempotency(),
                RequestIdempotency::Idempotent,
                "{:?} should be idempotent",
                rt
            );
        }
    }

    #[test]
    fn test_execute_statement_is_non_idempotent() {
        assert_eq!(
            RequestType::ExecuteStatement.idempotency(),
            RequestIdempotency::NonIdempotent
        );
    }

    #[test]
    fn test_execute_metadata_query_is_idempotent() {
        assert_eq!(
            RequestType::ExecuteMetadataQuery.idempotency(),
            RequestIdempotency::Idempotent
        );
    }

    // --- Idempotent strategy tests ---

    #[test]
    fn test_idempotent_strategy_retries_5xx() {
        let strategy = RetryStrategy {
            idempotency: RequestIdempotency::Idempotent,
            override_codes: None,
        };
        // Idempotent: Retry-After presence doesn't matter
        assert!(strategy.is_retryable_status(StatusCode::INTERNAL_SERVER_ERROR, false));
        assert!(strategy.is_retryable_status(StatusCode::BAD_GATEWAY, false));
        assert!(strategy.is_retryable_status(StatusCode::SERVICE_UNAVAILABLE, false));
        assert!(strategy.is_retryable_status(StatusCode::GATEWAY_TIMEOUT, false));
    }

    #[test]
    fn test_idempotent_strategy_retries_429() {
        let strategy = RetryStrategy {
            idempotency: RequestIdempotency::Idempotent,
            override_codes: None,
        };
        assert!(strategy.is_retryable_status(StatusCode::TOO_MANY_REQUESTS, false));
    }

    #[test]
    fn test_idempotent_strategy_no_retry_on_client_errors() {
        let strategy = RetryStrategy {
            idempotency: RequestIdempotency::Idempotent,
            override_codes: None,
        };
        assert!(!strategy.is_retryable_status(StatusCode::BAD_REQUEST, false));
        assert!(!strategy.is_retryable_status(StatusCode::UNAUTHORIZED, false));
        assert!(!strategy.is_retryable_status(StatusCode::FORBIDDEN, false));
        assert!(!strategy.is_retryable_status(StatusCode::NOT_FOUND, false));
        assert!(!strategy.is_retryable_status(StatusCode::METHOD_NOT_ALLOWED, false));
        assert!(!strategy.is_retryable_status(StatusCode::CONFLICT, false));
        assert!(!strategy.is_retryable_status(StatusCode::GONE, false));
    }

    #[test]
    fn test_idempotent_strategy_does_not_retry_success() {
        let strategy = RetryStrategy {
            idempotency: RequestIdempotency::Idempotent,
            override_codes: None,
        };
        // 2xx and 3xx codes are never retryable
        assert!(!strategy.is_retryable_status(StatusCode::OK, false));
        assert!(!strategy.is_retryable_status(StatusCode::CREATED, false));
        assert!(!strategy.is_retryable_status(StatusCode::MOVED_PERMANENTLY, false));
    }

    #[test]
    fn test_idempotent_strategy_override_codes() {
        let mut override_codes = HashSet::new();
        override_codes.insert(503);
        let strategy = RetryStrategy {
            idempotency: RequestIdempotency::Idempotent,
            override_codes: Some(override_codes),
        };
        // Only 503 is retryable now
        assert!(strategy.is_retryable_status(StatusCode::SERVICE_UNAVAILABLE, false));
        assert!(!strategy.is_retryable_status(StatusCode::INTERNAL_SERVER_ERROR, false));
        assert!(!strategy.is_retryable_status(StatusCode::BAD_GATEWAY, false));
        assert!(!strategy.is_retryable_status(StatusCode::TOO_MANY_REQUESTS, false));
    }

    // --- Non-idempotent strategy tests ---

    #[test]
    fn test_non_idempotent_strategy_retries_429_503_with_retry_after() {
        let strategy = RetryStrategy {
            idempotency: RequestIdempotency::NonIdempotent,
            override_codes: None,
        };
        // With Retry-After header: 429/503 are retryable
        assert!(strategy.is_retryable_status(StatusCode::TOO_MANY_REQUESTS, true));
        assert!(strategy.is_retryable_status(StatusCode::SERVICE_UNAVAILABLE, true));
    }

    #[test]
    fn test_non_idempotent_strategy_no_retry_429_503_without_retry_after() {
        let strategy = RetryStrategy {
            idempotency: RequestIdempotency::NonIdempotent,
            override_codes: None,
        };
        // Without Retry-After header: 429/503 are NOT retryable for non-idempotent
        assert!(!strategy.is_retryable_status(StatusCode::TOO_MANY_REQUESTS, false));
        assert!(!strategy.is_retryable_status(StatusCode::SERVICE_UNAVAILABLE, false));
    }

    #[test]
    fn test_non_idempotent_strategy_no_retry_on_5xx() {
        let strategy = RetryStrategy {
            idempotency: RequestIdempotency::NonIdempotent,
            override_codes: None,
        };
        // Even with Retry-After, 500/502/504 are not in the retryable set
        assert!(!strategy.is_retryable_status(StatusCode::INTERNAL_SERVER_ERROR, true));
        assert!(!strategy.is_retryable_status(StatusCode::BAD_GATEWAY, true));
        assert!(!strategy.is_retryable_status(StatusCode::GATEWAY_TIMEOUT, true));
    }

    #[test]
    fn test_non_idempotent_strategy_no_retry_on_client_errors() {
        let strategy = RetryStrategy {
            idempotency: RequestIdempotency::NonIdempotent,
            override_codes: None,
        };
        assert!(!strategy.is_retryable_status(StatusCode::BAD_REQUEST, true));
        assert!(!strategy.is_retryable_status(StatusCode::UNAUTHORIZED, true));
        assert!(!strategy.is_retryable_status(StatusCode::FORBIDDEN, true));
        assert!(!strategy.is_retryable_status(StatusCode::NOT_FOUND, true));
    }

    // --- Backoff calculation tests ---

    #[test]
    fn test_exponential_backoff_increases() {
        let config = RetryConfig {
            min_wait: Duration::from_secs(1),
            max_wait: Duration::from_secs(60),
            ..Default::default()
        };
        // Per spec: exp_backoff = 2^attempt * min_wait
        // Attempt 1: 2^1 * 1s = 2s + jitter, Attempt 2: 2^2 * 1s = 4s + jitter,
        // Attempt 3: 2^3 * 1s = 8s + jitter
        let b1 = calculate_backoff(&config, 1, None);
        let b2 = calculate_backoff(&config, 2, None);
        let b3 = calculate_backoff(&config, 3, None);
        assert!(b1 >= Duration::from_millis(2050));
        assert!(b1 <= Duration::from_millis(2750));
        assert!(b2 >= Duration::from_millis(4050));
        assert!(b2 <= Duration::from_millis(4750));
        assert!(b3 >= Duration::from_millis(8050));
        assert!(b3 <= Duration::from_millis(8750));
    }

    #[test]
    fn test_backoff_capped_at_max_wait() {
        let config = RetryConfig {
            min_wait: Duration::from_secs(1),
            max_wait: Duration::from_secs(10),
            ..Default::default()
        };
        // Attempt 10: 2^9 = 512s, should be capped to 10s + jitter
        let backoff = calculate_backoff(&config, 10, None);
        assert!(backoff <= Duration::from_millis(10_750));
    }

    #[test]
    fn test_backoff_respects_min_wait() {
        let config = RetryConfig {
            min_wait: Duration::from_secs(2),
            max_wait: Duration::from_secs(60),
            ..Default::default()
        };
        // Attempt 1: min_wait * 2^1 = 4s + jitter
        let backoff = calculate_backoff(&config, 1, None);
        assert!(backoff >= Duration::from_millis(4050));
    }

    #[test]
    fn test_retry_after_header_seconds() {
        let config = RetryConfig::default();
        let backoff = calculate_backoff(&config, 1, Some("5"));
        // 5s clamped to [1s, 60s] = 5s + jitter
        assert!(backoff >= Duration::from_millis(5050));
        assert!(backoff <= Duration::from_millis(5750));
    }

    #[test]
    fn test_retry_after_clamped_to_min_max() {
        let config = RetryConfig {
            min_wait: Duration::from_secs(3),
            max_wait: Duration::from_secs(10),
            ..Default::default()
        };
        // Retry-After: 1 should be clamped up to min_wait (3s)
        let backoff = calculate_backoff(&config, 1, Some("1"));
        assert!(backoff >= Duration::from_millis(3050));

        // Retry-After: 120 should be clamped down to max_wait (10s)
        let backoff = calculate_backoff(&config, 1, Some("120"));
        assert!(backoff <= Duration::from_millis(10_750));
    }

    #[test]
    fn test_retry_after_invalid_falls_back_to_exponential() {
        let config = RetryConfig {
            min_wait: Duration::from_secs(1),
            ..Default::default()
        };
        // Invalid Retry-After should fall back to exponential: 2^1 * 1s = 2s + jitter
        let backoff = calculate_backoff(&config, 1, Some("not-a-number"));
        assert!(backoff >= Duration::from_millis(2050));
        assert!(backoff <= Duration::from_millis(2750));
    }

    #[test]
    fn test_jitter_in_range_50ms_750ms() {
        let config = RetryConfig {
            min_wait: Duration::from_secs(1),
            max_wait: Duration::from_secs(60),
            ..Default::default()
        };
        // Run multiple times and check jitter range
        // Attempt 1: 2^1 * 1s = 2s base + 50ms to 750ms jitter
        for _ in 0..100 {
            let backoff = calculate_backoff(&config, 1, None);
            assert!(backoff >= Duration::from_millis(2050));
            assert!(backoff <= Duration::from_millis(2750));
        }
    }

    // --- RetryConfig tests ---

    #[test]
    fn test_retry_config_defaults() {
        let config = RetryConfig::default();
        assert_eq!(config.min_wait, Duration::from_secs(1));
        assert_eq!(config.max_wait, Duration::from_secs(60));
        assert_eq!(config.overall_timeout, Duration::from_secs(900));
        assert_eq!(config.max_retries, 5);
        assert!(config.override_retryable_codes.is_none());
    }

    #[test]
    fn test_category_config_overrides_global() {
        let global = RetryConfig::default();
        let mut overrides = HashMap::new();
        overrides.insert(
            RequestCategory::Auth,
            RetryConfigOverrides {
                max_retries: Some(3),
                overall_timeout: Some(Duration::from_secs(30)),
                ..Default::default()
            },
        );

        let configs = build_retry_configs(&global, &overrides);

        // Auth should have overridden values
        let auth = &configs[&RequestCategory::Auth];
        assert_eq!(auth.max_retries, 3);
        assert_eq!(auth.overall_timeout, Duration::from_secs(30));
        // But inherit min_wait/max_wait from global
        assert_eq!(auth.min_wait, Duration::from_secs(1));
        assert_eq!(auth.max_wait, Duration::from_secs(60));

        // SEA should have global defaults
        let sea = &configs[&RequestCategory::Sea];
        assert_eq!(sea.max_retries, 5);
        assert_eq!(sea.overall_timeout, Duration::from_secs(900));
    }

    // --- parse_retry_after tests ---

    #[test]
    fn test_parse_retry_after_seconds() {
        assert_eq!(parse_retry_after("120"), Some(Duration::from_secs(120)));
        assert_eq!(parse_retry_after("0"), Some(Duration::from_secs(0)));
        assert_eq!(parse_retry_after(" 5 "), Some(Duration::from_secs(5)));
    }

    #[test]
    fn test_parse_retry_after_http_date() {
        // Use a future date relative to test execution
        let future = chrono::Utc::now() + chrono::Duration::seconds(30);
        let formatted = future.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        let result = parse_retry_after(&formatted);
        assert!(result.is_some());
        let duration = result.unwrap();
        // Allow some tolerance for test execution time
        assert!(
            duration.as_secs() >= 28 && duration.as_secs() <= 32,
            "Expected ~30s, got {:?}",
            duration
        );
    }

    #[test]
    fn test_parse_retry_after_past_date() {
        // A date in the past should return Duration::ZERO
        let past = chrono::Utc::now() - chrono::Duration::seconds(60);
        let formatted = past.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        let result = parse_retry_after(&formatted);
        assert_eq!(result, Some(Duration::ZERO));
    }

    #[test]
    fn test_parse_retry_after_invalid() {
        assert_eq!(parse_retry_after("not-a-number"), None);
        assert_eq!(parse_retry_after(""), None);
    }
}
