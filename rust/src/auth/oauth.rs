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

//! OAuth 2.0 client credentials authentication.

use super::AuthProvider;
use crate::error::{DatabricksErrorHelper, Result};
use driverbase::error::ErrorHelper;

/// OAuth 2.0 client credentials authentication provider.
///
/// This provider implements the OAuth 2.0 client credentials flow for
/// authenticating with Databricks using a service principal.
#[derive(Debug, Clone)]
pub struct OAuthCredentials {
    client_id: String,
    client_secret: String,
    token_endpoint: Option<String>,
}

impl OAuthCredentials {
    /// Creates a new OAuth credentials provider.
    pub fn new(client_id: impl Into<String>, client_secret: impl Into<String>) -> Self {
        Self {
            client_id: client_id.into(),
            client_secret: client_secret.into(),
            token_endpoint: None,
        }
    }

    /// Sets a custom token endpoint URL.
    pub fn with_token_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.token_endpoint = Some(endpoint.into());
        self
    }

    /// Returns the client ID.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the client secret.
    pub fn client_secret(&self) -> &str {
        &self.client_secret
    }

    /// Returns the token endpoint.
    pub fn token_endpoint(&self) -> Option<&str> {
        self.token_endpoint.as_deref()
    }
}

impl AuthProvider for OAuthCredentials {
    fn get_auth_header(&self) -> Result<String> {
        // TODO: Implement token fetching and caching
        Err(DatabricksErrorHelper::not_implemented()
            .message("OAuth token fetching not yet implemented"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oauth_credentials() {
        let oauth = OAuthCredentials::new("client-id", "client-secret")
            .with_token_endpoint("https://example.com/oauth/token");

        assert_eq!(oauth.client_id(), "client-id");
        assert_eq!(
            oauth.token_endpoint(),
            Some("https://example.com/oauth/token")
        );
    }
}
