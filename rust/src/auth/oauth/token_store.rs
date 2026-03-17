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

//! Token lifecycle state machine for OAuth token refresh.
//!
//! This module implements a thread-safe token store that manages the token
//! refresh lifecycle with three states:
//!
//! - **FRESH**: Token has significant remaining TTL (not stale or expired).
//!   Returned immediately without any refresh attempt.
//!
//! - **STALE**: Token's remaining TTL has dropped below the stale threshold
//!   (computed as `min(initial_TTL * 0.5, 20 minutes)` when the token was
//!   first acquired). The current token is still valid but eligible for
//!   background refresh. The token is returned immediately to the caller,
//!   and a background refresh is spawned via `tokio::task::spawn_blocking`.
//!
//! - **EXPIRED**: Token's remaining TTL is less than 40 seconds. The caller
//!   is blocked until a refresh completes. This ensures no requests are made
//!   with a token that might be rejected by the server.
//!
//! The state machine ensures that only one refresh runs at a time, even with
//! concurrent callers. This is coordinated via an `AtomicBool` flag.

use crate::auth::oauth::token::OAuthToken;
use crate::error::{DatabricksErrorHelper, Result};
use driverbase::error::ErrorHelper;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

/// RAII guard that resets the `refreshing` flag on drop.
/// Ensures the flag is cleared even if the refresh function panics.
struct RefreshGuard {
    refreshing: Arc<AtomicBool>,
}

impl Drop for RefreshGuard {
    fn drop(&mut self) {
        self.refreshing.store(false, Ordering::SeqCst);
    }
}

/// Thread-safe token container with refresh state machine.
///
/// The `TokenStore` manages the lifecycle of an OAuth token, coordinating
/// refreshes based on the token's current state (FRESH, STALE, or EXPIRED).
///
/// # Thread Safety
///
/// - `RwLock` provides read-heavy access patterns: multiple readers can
///   access the token concurrently, but only one writer can refresh it.
/// - `AtomicBool` prevents concurrent refresh attempts: the first caller
///   to successfully set `refreshing` to `true` wins, and others either
///   wait (if token is EXPIRED) or return the current token (if STALE).
///
/// # Example
///
/// ```rust,ignore
/// // This is a private internal module - example shown for documentation only
/// use databricks_adbc::auth::oauth::token_store::TokenStore;
/// use databricks_adbc::auth::oauth::token::OAuthToken;
///
/// let store = TokenStore::new();
///
/// // Get or refresh the token
/// let token = store.get_or_refresh(|| {
///     // Fetch a fresh token from the OAuth provider
///     Ok(OAuthToken::new(
///         "access-token".to_string(),
///         "Bearer".to_string(),
///         3600, // 1 hour TTL
///         Some("refresh-token".to_string()),
///         vec!["all-apis".to_string()],
///     ))
/// })?;
/// ```

#[derive(Debug)]
pub(crate) struct TokenStore {
    /// The current token, protected by a read-write lock.
    token: Arc<RwLock<Option<OAuthToken>>>,
    /// Flag indicating whether a refresh is currently in progress.
    /// This prevents concurrent refresh attempts.
    refreshing: Arc<AtomicBool>,
}

impl TokenStore {
    /// Creates a new empty token store.
    pub fn new() -> Self {
        Self {
            token: Arc::new(RwLock::new(None)),
            refreshing: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Gets a valid token, refreshing if necessary.
    ///
    /// This method implements the token lifecycle state machine:
    ///
    /// - **Empty (no token)**: Blocks and calls `refresh_fn` to fetch the initial token.
    /// - **FRESH**: Returns the token immediately without calling `refresh_fn`.
    /// - **STALE**: Returns the current token immediately and spawns a background
    ///   refresh via `tokio::task::spawn_blocking`. Only one background refresh runs at a time.
    /// - **EXPIRED**: Blocks the caller and calls `refresh_fn` to obtain a fresh token
    ///   before returning.
    ///
    /// # Arguments
    ///
    /// * `refresh_fn` - A function that fetches a fresh token. This function is
    ///   called when the token is missing, expired, or stale (background only).
    ///   The function must be `Send + 'static` to support background refresh.
    ///
    /// # Returns
    ///
    /// A valid `OAuthToken` that is not expired.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The `refresh_fn` fails to fetch a token
    /// - The lock is poisoned (unlikely, indicates a panic in another thread)
    ///
    /// # Concurrency
    ///
    /// Multiple concurrent callers are handled as follows:
    /// - **FRESH token**: All callers get the token immediately (read lock only)
    /// - **STALE token**: First caller spawns background refresh; others get current token
    /// - **EXPIRED token**: First caller blocks and refreshes; others wait and get the new token
    pub fn get_or_refresh<F>(&self, refresh_fn: F) -> Result<OAuthToken>
    where
        F: FnOnce() -> Result<OAuthToken> + Send + 'static,
    {
        // Fast path: check if we have a FRESH token (read lock only, no contention)
        {
            let token = self.token.read().map_err(|e| {
                DatabricksErrorHelper::invalid_state()
                    .message("token store lock poisoned")
                    .context(format!("read lock error: {}", e))
            })?;

            if let Some(ref t) = *token {
                if !t.is_expired() && !t.is_stale() {
                    // FRESH: return immediately
                    return Ok(t.clone());
                }
            }
        }

        // Slow path: token is missing, stale, or expired
        // Check state again under read lock to determine action
        let token_state = {
            let token = self.token.read().map_err(|e| {
                DatabricksErrorHelper::invalid_state()
                    .message("token store lock poisoned")
                    .context(format!("read lock error: {}", e))
            })?;

            match *token {
                None => TokenState::Empty,
                Some(ref t) if t.is_expired() => TokenState::Expired,
                Some(ref t) if t.is_stale() => TokenState::Stale(t.clone()),
                Some(ref t) => TokenState::Fresh(t.clone()), // Race: became fresh between checks
            }
        };

        match token_state {
            TokenState::Fresh(t) => {
                // Race condition: token became fresh between our two checks
                Ok(t)
            }
            TokenState::Empty | TokenState::Expired => {
                // EXPIRED or EMPTY: block caller and refresh synchronously
                self.blocking_refresh(refresh_fn)
            }
            TokenState::Stale(current_token) => {
                // STALE: return current token immediately, spawn background refresh
                self.spawn_background_refresh(refresh_fn);
                Ok(current_token)
            }
        }
    }

    /// Performs a blocking refresh, ensuring only one refresh runs at a time.
    ///
    /// This method is called when the token is EXPIRED or missing. It blocks
    /// the caller until the refresh completes.
    ///
    /// Uses `AtomicBool` to coordinate concurrent callers:
    /// - First caller to set `refreshing` to `true` performs the refresh
    /// - Other callers spin-wait until the refresh completes, then read the new token
    fn blocking_refresh<F>(&self, refresh_fn: F) -> Result<OAuthToken>
    where
        F: FnOnce() -> Result<OAuthToken>,
    {
        // Try to acquire the refresh lock
        if self
            .refreshing
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            // Guard ensures refreshing is reset even if refresh_fn panics
            let _guard = RefreshGuard {
                refreshing: self.refreshing.clone(),
            };

            // We won the race: perform the refresh
            let result = refresh_fn();

            // Store the new token if successful
            if let Ok(ref new_token) = result {
                let mut token = self.token.write().map_err(|e| {
                    DatabricksErrorHelper::invalid_state()
                        .message("token store lock poisoned")
                        .context(format!("write lock error: {}", e))
                })?;
                *token = Some(new_token.clone());
            }

            // _guard drops here, resetting refreshing to false

            result
        } else {
            // Another thread is already refreshing: wait for it to complete
            self.wait_for_refresh()
        }
    }

    /// Spawns a background refresh task on the tokio blocking thread pool.
    ///
    /// This is called when the token is STALE. The background task will
    /// attempt to refresh the token without blocking the caller.
    ///
    /// If a refresh is already in progress, this method does nothing (no-op).
    fn spawn_background_refresh<F>(&self, refresh_fn: F)
    where
        F: FnOnce() -> Result<OAuthToken> + Send + 'static,
    {
        // Try to acquire the refresh lock
        if self
            .refreshing
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            // Clone Arc pointers to move into the task
            let token = self.token.clone();
            let refreshing = self.refreshing.clone();

            // Spawn on tokio's blocking thread pool (reuses threads, cheaper than std::thread::spawn)
            tokio::task::spawn_blocking(move || {
                // Guard ensures refreshing is reset even if refresh_fn panics
                let _guard = RefreshGuard {
                    refreshing: refreshing.clone(),
                };

                let result = refresh_fn();

                // Store the new token if successful (ignore errors in background)
                if let Ok(new_token) = result {
                    if let Ok(mut token_guard) = token.write() {
                        *token_guard = Some(new_token);
                    }
                }

                // _guard drops here, resetting refreshing to false
            });
        }
        // If another task is already refreshing, do nothing
    }

    /// Waits for an in-progress refresh to complete, then returns the new token.
    ///
    /// This method is called by threads that lost the race to refresh an EXPIRED token.
    /// It polls until the `refreshing` flag is cleared, sleeping briefly between checks
    /// to avoid burning CPU.
    fn wait_for_refresh(&self) -> Result<OAuthToken> {
        // Poll until the refresh completes, sleeping to avoid busy-waiting
        while self.refreshing.load(Ordering::SeqCst) {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        // Read the refreshed token
        let token = self.token.read().map_err(|e| {
            DatabricksErrorHelper::invalid_state()
                .message("token store lock poisoned")
                .context(format!("read lock error: {}", e))
        })?;

        token.clone().ok_or_else(|| {
            DatabricksErrorHelper::invalid_state()
                .message("token refresh failed: no token available after refresh")
        })
    }
}

/// Internal enum representing the token's current state.
#[derive(Debug)]
enum TokenState {
    /// No token is present in the store.
    Empty,
    /// Token is fresh and can be used immediately.
    Fresh(OAuthToken),
    /// Token is stale but still valid; eligible for background refresh.
    Stale(OAuthToken),
    /// Token is expired and must be refreshed before use.
    Expired,
}

impl Default for TokenStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use std::sync::atomic::AtomicUsize;

    /// Helper to create a token with a specific TTL in seconds
    fn create_token_with_ttl(ttl_seconds: i64) -> OAuthToken {
        OAuthToken::new(
            format!("token-{}", ttl_seconds),
            "Bearer".to_string(),
            ttl_seconds,
            Some("refresh-token".to_string()),
            vec!["all-apis".to_string()],
        )
    }

    /// Helper to create a token and manually set its expiry (for testing STALE state)
    fn create_token_with_expiry(expires_at: chrono::DateTime<Utc>, threshold: i64) -> OAuthToken {
        let mut token = create_token_with_ttl(3600);
        token.expires_at = expires_at;
        token.stale_threshold_seconds = threshold;
        token
    }

    #[test]
    fn test_store_fresh_token_no_refresh() {
        let store = TokenStore::new();
        let refresh_count = Arc::new(AtomicUsize::new(0));
        let refresh_count_clone = refresh_count.clone();

        // First call: should fetch token (store is empty)
        let token1 = store
            .get_or_refresh(move || {
                refresh_count_clone.fetch_add(1, Ordering::SeqCst);
                Ok(create_token_with_ttl(3600)) // 1 hour = FRESH
            })
            .expect("first refresh should succeed");

        assert_eq!(refresh_count.load(Ordering::SeqCst), 1);
        assert!(!token1.is_stale());
        assert!(!token1.is_expired());

        let refresh_count_clone2 = refresh_count.clone();
        // Second call: should return cached token without refresh
        let token2 = store
            .get_or_refresh(move || {
                refresh_count_clone2.fetch_add(1, Ordering::SeqCst);
                Ok(create_token_with_ttl(7200)) // Should not be called
            })
            .expect("second call should return cached token");

        assert_eq!(refresh_count.load(Ordering::SeqCst), 1); // No additional refresh
        assert_eq!(token2.access_token, token1.access_token);
    }

    #[test]
    fn test_store_expired_triggers_blocking_refresh() {
        let store = TokenStore::new();
        let refresh_count = Arc::new(AtomicUsize::new(0));
        let refresh_count_clone = refresh_count.clone();

        // Store an expired token (< 40s remaining)
        {
            let mut token = store.token.write().unwrap();
            *token = Some(create_token_with_ttl(30)); // 30s = EXPIRED
        }

        // get_or_refresh should block and call refresh_fn
        let token = store
            .get_or_refresh(move || {
                refresh_count_clone.fetch_add(1, Ordering::SeqCst);
                Ok(create_token_with_ttl(3600)) // Return fresh token
            })
            .expect("refresh should succeed");

        assert_eq!(refresh_count.load(Ordering::SeqCst), 1);
        assert!(!token.is_expired());
        assert_eq!(token.access_token, "token-3600");
    }

    #[test]
    fn test_store_concurrent_refresh_single_fetch() {
        let store = Arc::new(TokenStore::new());
        let refresh_count = Arc::new(AtomicUsize::new(0));

        // Store an expired token
        {
            let mut token = store.token.write().unwrap();
            *token = Some(create_token_with_ttl(30)); // EXPIRED
        }

        // Spawn multiple threads trying to refresh concurrently
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let store = store.clone();
                let refresh_count = refresh_count.clone();
                std::thread::spawn(move || {
                    store
                        .get_or_refresh(move || {
                            refresh_count.fetch_add(1, Ordering::SeqCst);
                            // Simulate slow refresh
                            std::thread::sleep(std::time::Duration::from_millis(50));
                            Ok(create_token_with_ttl(3600))
                        })
                        .expect("refresh should succeed")
                })
            })
            .collect();

        // Wait for all threads
        let tokens: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Only one refresh should have occurred
        assert_eq!(refresh_count.load(Ordering::SeqCst), 1);

        // All threads should get the same token
        for token in &tokens {
            assert_eq!(token.access_token, "token-3600");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_store_stale_returns_current_token() {
        let store = TokenStore::new();
        let refresh_count = Arc::new(AtomicUsize::new(0));
        let refresh_count_clone = refresh_count.clone();

        // Create a STALE token: TTL=25min (1500s), threshold=20min (1200s)
        // Set expires_at = now + 600s (10 minutes)
        // This is less than threshold (1200s) but more than expiry buffer (40s)
        let stale_token = create_token_with_expiry(
            Utc::now() + Duration::seconds(600), // 10 min remaining
            1200,                                // 20 min threshold
        );

        {
            let mut token = store.token.write().unwrap();
            *token = Some(stale_token.clone());
        }

        // get_or_refresh should return current token immediately
        let token = store
            .get_or_refresh(move || {
                refresh_count_clone.fetch_add(1, Ordering::SeqCst);
                // Simulate slow refresh
                std::thread::sleep(std::time::Duration::from_millis(100));
                Ok(create_token_with_ttl(3600))
            })
            .expect("should return current token");

        // Should return the stale token immediately (not the refreshed one)
        assert_eq!(token.access_token, stale_token.access_token);

        // The refresh should happen in the background, but we shouldn't wait for it
        // To verify it happened, we can wait a bit and check the count
        std::thread::sleep(std::time::Duration::from_millis(200));

        // Verify background refresh completed
        assert_eq!(refresh_count.load(Ordering::SeqCst), 1);

        // Verify that the method returned immediately with the stale token
        assert!(token.is_stale());
        assert!(!token.is_expired());
    }

    #[test]
    fn test_store_empty_triggers_blocking_refresh() {
        let store = TokenStore::new();
        let refresh_count = Arc::new(AtomicUsize::new(0));
        let refresh_count_clone = refresh_count.clone();

        // get_or_refresh on empty store should block and fetch
        let token = store
            .get_or_refresh(move || {
                refresh_count_clone.fetch_add(1, Ordering::SeqCst);
                Ok(create_token_with_ttl(3600))
            })
            .expect("initial fetch should succeed");

        assert_eq!(refresh_count.load(Ordering::SeqCst), 1);
        assert_eq!(token.access_token, "token-3600");
    }

    #[test]
    fn test_store_refresh_failure_propagates_error() {
        let store = TokenStore::new();

        // Refresh function that fails
        let result = store.get_or_refresh(|| {
            Err(DatabricksErrorHelper::io()
                .message("network error")
                .context("token refresh"))
        });

        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("network error"));
    }
}
