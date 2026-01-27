// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! HTTP client implementation for Databricks SQL API.

use crate::auth::AuthProvider;
use crate::Result;
use std::sync::Arc;
use std::time::Duration;

/// Configuration for the HTTP client.
#[derive(Debug, Clone)]
pub struct HttpClientConfig {
    /// Request timeout duration.
    pub timeout: Duration,
    /// Maximum number of retry attempts.
    pub max_retries: u32,
    /// User agent string.
    pub user_agent: String,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            max_retries: 3,
            user_agent: format!("databricks-adbc-rust/{}", env!("CARGO_PKG_VERSION")),
        }
    }
}

/// HTTP client for communicating with Databricks SQL endpoints.
#[derive(Debug)]
pub struct HttpClient {
    config: HttpClientConfig,
    auth_provider: Option<Arc<dyn AuthProvider>>,
}

impl HttpClient {
    /// Creates a new HTTP client with the given configuration.
    pub fn new(config: HttpClientConfig) -> Self {
        Self {
            config,
            auth_provider: None,
        }
    }

    /// Sets the authentication provider.
    pub fn with_auth(mut self, auth_provider: Arc<dyn AuthProvider>) -> Self {
        self.auth_provider = Some(auth_provider);
        self
    }

    /// Returns the client configuration.
    pub fn config(&self) -> &HttpClientConfig {
        &self.config
    }

    /// Executes a SQL statement.
    pub fn execute_statement(&self, _statement: &str) -> Result<()> {
        // TODO: Implement HTTP request to Databricks SQL API
        Ok(())
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new(HttpClientConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_client_default_config() {
        let client = HttpClient::default();
        assert_eq!(client.config().timeout, Duration::from_secs(30));
        assert_eq!(client.config().max_retries, 3);
    }

    #[test]
    fn test_http_client_custom_config() {
        let config = HttpClientConfig {
            timeout: Duration::from_secs(60),
            max_retries: 5,
            user_agent: "custom-agent".to_string(),
        };
        let client = HttpClient::new(config);
        assert_eq!(client.config().timeout, Duration::from_secs(60));
        assert_eq!(client.config().max_retries, 5);
    }
}
