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

//! Database implementation for the Databricks ADBC driver.

use crate::auth::config::{AuthConfig, AuthType};
use crate::auth::{AuthProvider, AuthorizationCodeProvider, PersonalAccessToken};
use crate::client::{
    DatabricksClient, DatabricksClientConfig, DatabricksHttpClient, HttpClientConfig, SeaClient,
};
use crate::connection::{Connection, ConnectionConfig};
use crate::error::DatabricksErrorHelper;
use crate::logging::{self, LogConfig};
use crate::reader::ResultReaderFactory;
use crate::types::cloudfetch::CloudFetchConfig;
use adbc_core::error::Result;
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::Optionable;
use driverbase::error::ErrorHelper;
use std::sync::Arc;
use std::time::Duration;

/// Represents a database instance that holds connection configuration.
///
/// A Database is created from a Driver and is used to establish Connections.
/// Configuration options like host, credentials, and HTTP path are set on
/// the Database before creating connections.
#[derive(Debug, Default)]
pub struct Database {
    // Core configuration
    uri: Option<String>,
    warehouse_id: Option<String>,
    access_token: Option<String>,
    catalog: Option<String>,
    schema: Option<String>,

    // HTTP client configuration
    http_config: HttpClientConfig,

    // CloudFetch configuration
    cloudfetch_config: CloudFetchConfig,

    // Logging configuration
    log_level: Option<String>,
    log_file: Option<String>,

    // Authentication configuration
    auth_config: AuthConfig,
}

impl Database {
    /// Creates a new Database instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the configured URI.
    pub fn uri(&self) -> Option<&str> {
        self.uri.as_deref()
    }

    /// Returns the configured warehouse ID.
    pub fn warehouse_id(&self) -> Option<&str> {
        self.warehouse_id.as_deref()
    }

    /// Returns the configured catalog.
    pub fn catalog(&self) -> Option<&str> {
        self.catalog.as_deref()
    }

    /// Returns the configured schema.
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    /// Extract warehouse ID from HTTP path if provided.
    /// Supports both `/sql/1.0/warehouses/{id}` and `/sql/1.0/endpoints/{id}`
    /// formats (they are equivalent on the server side).
    fn extract_warehouse_id(http_path: &str) -> Option<String> {
        http_path
            .strip_prefix("/sql/1.0/warehouses/")
            .or_else(|| http_path.strip_prefix("sql/1.0/warehouses/"))
            .or_else(|| http_path.strip_prefix("/sql/1.0/endpoints/"))
            .or_else(|| http_path.strip_prefix("sql/1.0/endpoints/"))
            .map(|s| s.trim_end_matches('/').to_string())
    }

    /// Parse a boolean option value.
    fn parse_bool_option(value: &OptionValue) -> Option<bool> {
        match value {
            OptionValue::String(s) => match s.to_lowercase().as_str() {
                "true" | "1" | "yes" => Some(true),
                "false" | "0" | "no" => Some(false),
                _ => None,
            },
            _ => None,
        }
    }

    /// Parse an integer option value.
    fn parse_int_option(value: &OptionValue) -> Option<i64> {
        match value {
            OptionValue::String(s) => s.parse().ok(),
            OptionValue::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// Parse a float option value.
    fn parse_float_option(value: &OptionValue) -> Option<f64> {
        match value {
            OptionValue::String(s) => s.parse().ok(),
            OptionValue::Double(d) => Some(*d),
            _ => None,
        }
    }
}

impl Optionable for Database {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match key {
            OptionDatabase::Uri => {
                if let OptionValue::String(s) = value {
                    self.uri = Some(s);
                    Ok(())
                } else {
                    Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                }
            }
            OptionDatabase::Other(ref s) => match s.as_str() {
                // Core options
                "databricks.http_path" => {
                    if let OptionValue::String(v) = value {
                        // Extract warehouse ID from HTTP path
                        if let Some(wid) = Self::extract_warehouse_id(&v) {
                            self.warehouse_id = Some(wid);
                        }
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }
                "databricks.warehouse_id" => {
                    if let OptionValue::String(v) = value {
                        self.warehouse_id = Some(v);
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }
                "databricks.access_token" => {
                    if let OptionValue::String(v) = value {
                        self.access_token = Some(v);
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }
                "databricks.catalog" => {
                    if let OptionValue::String(v) = value {
                        self.catalog = Some(v);
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }
                "databricks.schema" => {
                    if let OptionValue::String(v) = value {
                        self.schema = Some(v);
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }

                // CloudFetch options
                "databricks.cloudfetch.enabled" => {
                    if let Some(v) = Self::parse_bool_option(&value) {
                        self.cloudfetch_config.enabled = v;
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }
                "databricks.cloudfetch.link_prefetch_window" => {
                    if let Some(v) = Self::parse_int_option(&value) {
                        self.cloudfetch_config.link_prefetch_window = v as usize;
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }
                "databricks.cloudfetch.max_chunks_in_memory" => {
                    if let Some(v) = Self::parse_int_option(&value) {
                        self.cloudfetch_config.max_chunks_in_memory = v as usize;
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }
                "databricks.cloudfetch.max_retries" => {
                    if let Some(v) = Self::parse_int_option(&value) {
                        self.cloudfetch_config.max_retries = v as u32;
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }
                "databricks.cloudfetch.retry_delay_ms" => {
                    if let Some(v) = Self::parse_int_option(&value) {
                        self.cloudfetch_config.retry_delay = Duration::from_millis(v as u64);
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }
                "databricks.cloudfetch.chunk_ready_timeout_ms" => {
                    if let Some(v) = Self::parse_int_option(&value) {
                        self.cloudfetch_config.chunk_ready_timeout =
                            Some(Duration::from_millis(v as u64));
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }
                "databricks.cloudfetch.speed_threshold_mbps" => {
                    if let Some(v) = Self::parse_float_option(&value) {
                        self.cloudfetch_config.speed_threshold_mbps = v;
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }

                // Logging options
                "databricks.log_level" => {
                    if let OptionValue::String(v) = value {
                        self.log_level = Some(v);
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }
                "databricks.log_file" => {
                    if let OptionValue::String(v) = value {
                        self.log_file = Some(v);
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }

                // Authentication configuration options
                "databricks.auth.type" => {
                    if let OptionValue::String(v) = value {
                        self.auth_config.auth_type = Some(AuthType::try_from(v.as_str())?);
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }
                "databricks.auth.client_id" => {
                    if let OptionValue::String(v) = value {
                        self.auth_config.client_id = Some(v);
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }
                "databricks.auth.client_secret" => {
                    if let OptionValue::String(v) = value {
                        self.auth_config.client_secret = Some(v);
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }
                "databricks.auth.scopes" => {
                    if let OptionValue::String(v) = value {
                        self.auth_config.scopes = Some(v);
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }
                "databricks.auth.token_endpoint" => {
                    if let OptionValue::String(v) = value {
                        self.auth_config.token_endpoint = Some(v);
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }
                "databricks.auth.redirect_port" => {
                    if let Some(v) = Self::parse_int_option(&value) {
                        if !(0..=65535).contains(&v) {
                            return Err(DatabricksErrorHelper::invalid_argument()
                                .message(format!(
                                    "Invalid redirect port: {}. Port must be between 0 and 65535",
                                    v
                                ))
                                .to_adbc());
                        }
                        self.auth_config.redirect_port = Some(v as u16);
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }

                // HTTP client options
                "databricks.http.connect_timeout_ms" => {
                    if let Some(v) = Self::parse_int_option(&value) {
                        self.http_config.connect_timeout = Duration::from_millis(v as u64);
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }
                "databricks.http.read_timeout_ms" => {
                    if let Some(v) = Self::parse_int_option(&value) {
                        self.http_config.read_timeout = Duration::from_millis(v as u64);
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }
                "databricks.http.max_retries" => {
                    if let Some(v) = Self::parse_int_option(&value) {
                        self.http_config.max_retries = v as u32;
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }

                _ => Err(DatabricksErrorHelper::set_unknown_option(&key).to_adbc()),
            },
            _ => Err(DatabricksErrorHelper::set_unknown_option(&key).to_adbc()),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key {
            OptionDatabase::Uri => self.uri.clone().ok_or_else(|| {
                DatabricksErrorHelper::invalid_state()
                    .message("option 'uri' is not set")
                    .to_adbc()
            }),
            OptionDatabase::Other(ref s) => match s.as_str() {
                "databricks.warehouse_id" => self.warehouse_id.clone().ok_or_else(|| {
                    DatabricksErrorHelper::invalid_state()
                        .message("option 'databricks.warehouse_id' is not set")
                        .to_adbc()
                }),
                "databricks.catalog" => self.catalog.clone().ok_or_else(|| {
                    DatabricksErrorHelper::invalid_state()
                        .message("option 'databricks.catalog' is not set")
                        .to_adbc()
                }),
                "databricks.schema" => self.schema.clone().ok_or_else(|| {
                    DatabricksErrorHelper::invalid_state()
                        .message("option 'databricks.schema' is not set")
                        .to_adbc()
                }),
                "databricks.log_level" => self.log_level.clone().ok_or_else(|| {
                    DatabricksErrorHelper::invalid_state()
                        .message("option 'databricks.log_level' is not set")
                        .to_adbc()
                }),
                "databricks.log_file" => self.log_file.clone().ok_or_else(|| {
                    DatabricksErrorHelper::invalid_state()
                        .message("option 'databricks.log_file' is not set")
                        .to_adbc()
                }),
                "databricks.auth.type" => self
                    .auth_config
                    .auth_type
                    .ok_or_else(|| {
                        DatabricksErrorHelper::invalid_state()
                            .message("option 'databricks.auth.type' is not set")
                            .to_adbc()
                    })
                    .map(|t| t.to_string()),
                "databricks.auth.client_id" => {
                    self.auth_config.client_id.clone().ok_or_else(|| {
                        DatabricksErrorHelper::invalid_state()
                            .message("option 'databricks.auth.client_id' is not set")
                            .to_adbc()
                    })
                }
                "databricks.auth.client_secret" => {
                    self.auth_config.client_secret.clone().ok_or_else(|| {
                        DatabricksErrorHelper::invalid_state()
                            .message("option 'databricks.auth.client_secret' is not set")
                            .to_adbc()
                    })
                }
                "databricks.auth.scopes" => self.auth_config.scopes.clone().ok_or_else(|| {
                    DatabricksErrorHelper::invalid_state()
                        .message("option 'databricks.auth.scopes' is not set")
                        .to_adbc()
                }),
                "databricks.auth.token_endpoint" => {
                    self.auth_config.token_endpoint.clone().ok_or_else(|| {
                        DatabricksErrorHelper::invalid_state()
                            .message("option 'databricks.auth.token_endpoint' is not set")
                            .to_adbc()
                    })
                }
                _ => Err(DatabricksErrorHelper::get_unknown_option(&key).to_adbc()),
            },
            _ => Err(DatabricksErrorHelper::get_unknown_option(&key).to_adbc()),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        Err(DatabricksErrorHelper::get_unknown_option(&key).to_adbc())
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        match key {
            OptionDatabase::Other(ref s) => match s.as_str() {
                "databricks.cloudfetch.link_prefetch_window" => {
                    Ok(self.cloudfetch_config.link_prefetch_window as i64)
                }
                "databricks.cloudfetch.max_chunks_in_memory" => {
                    Ok(self.cloudfetch_config.max_chunks_in_memory as i64)
                }
                "databricks.cloudfetch.max_retries" => {
                    Ok(self.cloudfetch_config.max_retries as i64)
                }
                "databricks.auth.redirect_port" => self
                    .auth_config
                    .redirect_port
                    .ok_or_else(|| {
                        DatabricksErrorHelper::invalid_state()
                            .message("option 'databricks.auth.redirect_port' is not set")
                            .to_adbc()
                    })
                    .map(|p| p as i64),
                _ => Err(DatabricksErrorHelper::get_unknown_option(&key).to_adbc()),
            },
            _ => Err(DatabricksErrorHelper::get_unknown_option(&key).to_adbc()),
        }
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        match key {
            OptionDatabase::Other(ref s) => match s.as_str() {
                "databricks.cloudfetch.speed_threshold_mbps" => {
                    Ok(self.cloudfetch_config.speed_threshold_mbps)
                }
                _ => Err(DatabricksErrorHelper::get_unknown_option(&key).to_adbc()),
            },
            _ => Err(DatabricksErrorHelper::get_unknown_option(&key).to_adbc()),
        }
    }
}

impl adbc_core::Database for Database {
    type ConnectionType = Connection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        // Initialize logging (first call wins, subsequent calls are no-ops)
        logging::init_logging(&LogConfig {
            level: self.log_level.clone(),
            file: self.log_file.clone(),
        });

        // Validate required options
        let host = self.uri.as_ref().ok_or_else(|| {
            DatabricksErrorHelper::invalid_argument()
                .message("uri not set")
                .to_adbc()
        })?;
        let warehouse_id = self.warehouse_id.as_ref().ok_or_else(|| {
            DatabricksErrorHelper::invalid_argument()
                .message("warehouse_id not set (set via databricks.http_path or databricks.warehouse_id)")
                .to_adbc()
        })?;

        // Validate auth configuration
        let auth_type = self
            .auth_config
            .validate(&self.access_token)
            .map_err(|e| e.to_adbc())?;

        // Get access_token if needed (for AccessToken type)
        let access_token = self.access_token.as_ref();

        // Create HTTP client (without auth provider - two-phase initialization)
        let http_client =
            Arc::new(DatabricksHttpClient::new(self.http_config.clone()).map_err(|e| e.to_adbc())?);

        // Create tokio runtime for async operations (needed before auth provider creation for U2M)
        let runtime = tokio::runtime::Runtime::new().map_err(|e| {
            DatabricksErrorHelper::io()
                .message(format!("Failed to create async runtime: {}", e))
                .to_adbc()
        })?;

        // Create auth provider based on auth type
        let auth_provider: Arc<dyn AuthProvider> = match auth_type {
            AuthType::AccessToken => Arc::new(PersonalAccessToken::new(
                access_token
                    .ok_or_else(|| {
                        DatabricksErrorHelper::invalid_argument()
                            .message(
                                "databricks.access_token is required for auth type 'access_token'",
                            )
                            .to_adbc()
                    })?
                    .clone(),
            )),
            AuthType::OAuthM2m => {
                // Client credentials flow (M2M) - create ClientCredentialsProvider
                let client_id = self.auth_config.client_id.as_ref().ok_or_else(|| {
                    DatabricksErrorHelper::invalid_argument()
                        .message("databricks.auth.client_id is required for auth type 'oauth_m2m'")
                        .to_adbc()
                })?;
                let client_secret = self.auth_config.client_secret.as_ref().ok_or_else(|| {
                    DatabricksErrorHelper::invalid_argument()
                        .message(
                            "databricks.auth.client_secret is required for auth type 'oauth_m2m'",
                        )
                        .to_adbc()
                })?;

                // Default scope for M2M is "all-apis" (no offline_access since M2M has no refresh token)
                let scopes_str = self.auth_config.scopes.as_deref().unwrap_or("all-apis");
                let scopes: Vec<String> = scopes_str.split_whitespace().map(String::from).collect();

                let provider = runtime
                    .block_on(
                        crate::auth::ClientCredentialsProvider::new_with_full_config(
                            host,
                            client_id,
                            client_secret,
                            http_client.clone(),
                            scopes,
                            self.auth_config.token_endpoint.clone(),
                        ),
                    )
                    .map_err(|e| e.to_adbc())?;

                Arc::new(provider)
            }
            AuthType::OAuthU2m => {
                // U2M flow - create AuthorizationCodeProvider
                let client_id = self
                    .auth_config
                    .client_id
                    .as_deref()
                    .unwrap_or("databricks-cli");
                let scopes_str = self
                    .auth_config
                    .scopes
                    .as_deref()
                    .unwrap_or("all-apis offline_access");
                let scopes: Vec<String> = scopes_str.split_whitespace().map(String::from).collect();
                let redirect_port = self.auth_config.redirect_port.unwrap_or(8020);
                let callback_timeout = Duration::from_secs(120); // 2 minutes default

                // Create the provider (async operation - needs runtime)
                let provider = runtime
                    .block_on(AuthorizationCodeProvider::new_with_full_config(
                        host,
                        client_id,
                        http_client.clone(),
                        scopes,
                        redirect_port,
                        callback_timeout,
                        self.auth_config.token_endpoint.clone(),
                    ))
                    .map_err(|e| e.to_adbc())?;

                Arc::new(provider)
            }
        };

        // Set auth provider on HTTP client (phase 2)
        http_client.set_auth_provider(auth_provider);

        // Two-step initialization required because ResultReaderFactory needs
        // Arc<dyn DatabricksClient>, which requires wrapping SeaClient in Arc first.
        //
        // 1. Create SeaClient (without reader_factory — uses OnceLock)
        // 2. Wrap in Arc<SeaClient>, coerce to Arc<dyn DatabricksClient>
        // 3. Create ResultReaderFactory with that Arc
        // 4. Set the factory on SeaClient via OnceLock::set()
        let client_config = DatabricksClientConfig {
            cloudfetch_config: self.cloudfetch_config.clone(),
            ..Default::default()
        };
        let sea_client = Arc::new(SeaClient::new(
            http_client.clone(),
            host,
            warehouse_id,
            client_config,
        ));
        let client: Arc<dyn DatabricksClient> = sea_client.clone();

        let reader_factory = ResultReaderFactory::new(
            client.clone(),
            http_client,
            self.cloudfetch_config.clone(),
            runtime.handle().clone(),
        );
        sea_client.set_reader_factory(reader_factory, runtime.handle().clone());

        // Create connection (passes runtime ownership to Connection)
        Connection::new_with_runtime(
            ConnectionConfig {
                host: host.clone(),
                warehouse_id: warehouse_id.clone(),
                catalog: self.catalog.clone(),
                schema: self.schema.clone(),
                client,
            },
            runtime,
        )
        .map_err(|e| e.to_adbc())
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let mut connection = self.new_connection()?;
        for (key, value) in opts {
            connection.set_option(key, value)?;
        }
        Ok(connection)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_set_options() {
        let mut db = Database::new();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("https://example.databricks.com".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.http_path".into()),
            OptionValue::String("/sql/1.0/warehouses/abc123".into()),
        )
        .unwrap();

        assert_eq!(db.uri(), Some("https://example.databricks.com"));
        assert_eq!(db.warehouse_id(), Some("abc123"));
    }

    #[test]
    fn test_database_extract_warehouse_id() {
        assert_eq!(
            Database::extract_warehouse_id("/sql/1.0/warehouses/abc123"),
            Some("abc123".to_string())
        );
        assert_eq!(
            Database::extract_warehouse_id("sql/1.0/warehouses/abc123"),
            Some("abc123".to_string())
        );
        assert_eq!(
            Database::extract_warehouse_id("/sql/1.0/warehouses/abc123/"),
            Some("abc123".to_string())
        );
        assert_eq!(Database::extract_warehouse_id("/other/path"), None);

        // /sql/1.0/endpoints/ format (Simba-compatible)
        assert_eq!(
            Database::extract_warehouse_id("/sql/1.0/endpoints/abc123"),
            Some("abc123".to_string())
        );
        assert_eq!(
            Database::extract_warehouse_id("sql/1.0/endpoints/abc123"),
            Some("abc123".to_string())
        );
        assert_eq!(
            Database::extract_warehouse_id("/sql/1.0/endpoints/abc123/"),
            Some("abc123".to_string())
        );
    }

    #[test]
    fn test_database_cloudfetch_options() {
        let mut db = Database::new();
        db.set_option(
            OptionDatabase::Other("databricks.cloudfetch.enabled".into()),
            OptionValue::String("true".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.cloudfetch.max_chunks_in_memory".into()),
            OptionValue::String("8".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.cloudfetch.speed_threshold_mbps".into()),
            OptionValue::String("0.5".into()),
        )
        .unwrap();

        assert!(db.cloudfetch_config.enabled);
        assert_eq!(db.cloudfetch_config.max_chunks_in_memory, 8);
        assert_eq!(db.cloudfetch_config.speed_threshold_mbps, 0.5);
    }

    #[test]
    fn test_database_new_connection_missing_uri() {
        use adbc_core::Database as _;

        let db = Database::new();
        let result = db.new_connection();
        assert!(result.is_err());
    }

    #[test]
    fn test_database_log_level_option() {
        let mut db = Database::new();
        db.set_option(
            OptionDatabase::Other("databricks.log_level".into()),
            OptionValue::String("DEBUG".into()),
        )
        .unwrap();

        assert_eq!(db.log_level, Some("DEBUG".to_string()));
        assert_eq!(
            db.get_option_string(OptionDatabase::Other("databricks.log_level".into()))
                .unwrap(),
            "DEBUG"
        );
    }

    #[test]
    fn test_database_log_file_option() {
        let mut db = Database::new();
        db.set_option(
            OptionDatabase::Other("databricks.log_file".into()),
            OptionValue::String("/tmp/test.log".into()),
        )
        .unwrap();

        assert_eq!(db.log_file, Some("/tmp/test.log".to_string()));
        assert_eq!(
            db.get_option_string(OptionDatabase::Other("databricks.log_file".into()))
                .unwrap(),
            "/tmp/test.log"
        );
    }

    #[test]
    fn test_database_log_options_reject_non_string() {
        let mut db = Database::new();
        let result = db.set_option(
            OptionDatabase::Other("databricks.log_level".into()),
            OptionValue::Int(5),
        );
        assert!(result.is_err());

        let result = db.set_option(
            OptionDatabase::Other("databricks.log_file".into()),
            OptionValue::Int(5),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_database_log_options_unset_returns_error() {
        let db = Database::new();
        assert!(db
            .get_option_string(OptionDatabase::Other("databricks.log_level".into()))
            .is_err());
        assert!(db
            .get_option_string(OptionDatabase::Other("databricks.log_file".into()))
            .is_err());
    }

    // Note: AuthType enum tests are in auth::config::tests

    #[test]
    fn test_database_set_auth_type_option() {
        let mut db = Database::new();

        db.set_option(
            OptionDatabase::Other("databricks.auth.type".into()),
            OptionValue::String("access_token".into()),
        )
        .unwrap();
        assert_eq!(db.auth_config.auth_type, Some(AuthType::AccessToken));

        db.set_option(
            OptionDatabase::Other("databricks.auth.type".into()),
            OptionValue::String("oauth_m2m".into()),
        )
        .unwrap();
        assert_eq!(db.auth_config.auth_type, Some(AuthType::OAuthM2m));

        db.set_option(
            OptionDatabase::Other("databricks.auth.type".into()),
            OptionValue::String("oauth_u2m".into()),
        )
        .unwrap();
        assert_eq!(db.auth_config.auth_type, Some(AuthType::OAuthU2m));

        // Verify get_option_string round-trip
        assert_eq!(
            db.get_option_string(OptionDatabase::Other("databricks.auth.type".into()))
                .unwrap(),
            "oauth_u2m"
        );
    }

    #[test]
    fn test_database_set_auth_type_invalid() {
        let mut db = Database::new();

        let result = db.set_option(
            OptionDatabase::Other("databricks.auth.type".into()),
            OptionValue::String("invalid".into()),
        );
        assert!(result.is_err());

        // Non-string should fail
        let result = db.set_option(
            OptionDatabase::Other("databricks.auth.type".into()),
            OptionValue::Int(0),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_database_set_auth_client_id() {
        let mut db = Database::new();
        db.set_option(
            OptionDatabase::Other("databricks.auth.client_id".into()),
            OptionValue::String("test-client-id".into()),
        )
        .unwrap();

        assert_eq!(db.auth_config.client_id, Some("test-client-id".to_string()));
        assert_eq!(
            db.get_option_string(OptionDatabase::Other("databricks.auth.client_id".into()))
                .unwrap(),
            "test-client-id"
        );
    }

    #[test]
    fn test_database_set_auth_client_secret() {
        let mut db = Database::new();
        db.set_option(
            OptionDatabase::Other("databricks.auth.client_secret".into()),
            OptionValue::String("test-secret".into()),
        )
        .unwrap();

        assert_eq!(
            db.auth_config.client_secret,
            Some("test-secret".to_string())
        );
        assert_eq!(
            db.get_option_string(OptionDatabase::Other(
                "databricks.auth.client_secret".into()
            ))
            .unwrap(),
            "test-secret"
        );
    }

    #[test]
    fn test_database_set_auth_scopes() {
        let mut db = Database::new();
        db.set_option(
            OptionDatabase::Other("databricks.auth.scopes".into()),
            OptionValue::String("all-apis offline_access".into()),
        )
        .unwrap();

        assert_eq!(
            db.auth_config.scopes,
            Some("all-apis offline_access".to_string())
        );
        assert_eq!(
            db.get_option_string(OptionDatabase::Other("databricks.auth.scopes".into()))
                .unwrap(),
            "all-apis offline_access"
        );
    }

    #[test]
    fn test_database_set_auth_token_endpoint() {
        let mut db = Database::new();
        db.set_option(
            OptionDatabase::Other("databricks.auth.token_endpoint".into()),
            OptionValue::String("https://example.com/token".into()),
        )
        .unwrap();

        assert_eq!(
            db.auth_config.token_endpoint,
            Some("https://example.com/token".to_string())
        );
        assert_eq!(
            db.get_option_string(OptionDatabase::Other(
                "databricks.auth.token_endpoint".into()
            ))
            .unwrap(),
            "https://example.com/token"
        );
    }

    #[test]
    fn test_database_set_auth_redirect_port() {
        let mut db = Database::new();

        // Test valid port
        db.set_option(
            OptionDatabase::Other("databricks.auth.redirect_port".into()),
            OptionValue::String("8020".into()),
        )
        .unwrap();
        assert_eq!(db.auth_config.redirect_port, Some(8020));
        assert_eq!(
            db.get_option_int(OptionDatabase::Other(
                "databricks.auth.redirect_port".into()
            ))
            .unwrap(),
            8020
        );

        // Test with OptionValue::Int
        db.set_option(
            OptionDatabase::Other("databricks.auth.redirect_port".into()),
            OptionValue::Int(9000),
        )
        .unwrap();
        assert_eq!(db.auth_config.redirect_port, Some(9000));

        // Test port 0 (OS-assigned)
        db.set_option(
            OptionDatabase::Other("databricks.auth.redirect_port".into()),
            OptionValue::Int(0),
        )
        .unwrap();
        assert_eq!(db.auth_config.redirect_port, Some(0));

        // Test max port
        db.set_option(
            OptionDatabase::Other("databricks.auth.redirect_port".into()),
            OptionValue::Int(65535),
        )
        .unwrap();
        assert_eq!(db.auth_config.redirect_port, Some(65535));
    }

    #[test]
    fn test_database_set_auth_redirect_port_invalid() {
        let mut db = Database::new();

        // Test negative port
        let result = db.set_option(
            OptionDatabase::Other("databricks.auth.redirect_port".into()),
            OptionValue::Int(-1),
        );
        assert!(result.is_err());

        // Test port > 65535
        let result = db.set_option(
            OptionDatabase::Other("databricks.auth.redirect_port".into()),
            OptionValue::Int(65536),
        );
        assert!(result.is_err());

        // Test non-integer string
        let result = db.set_option(
            OptionDatabase::Other("databricks.auth.redirect_port".into()),
            OptionValue::String("invalid".into()),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_database_oauth_options_reject_non_string() {
        let mut db = Database::new();

        // client_id should reject non-string
        let result = db.set_option(
            OptionDatabase::Other("databricks.auth.client_id".into()),
            OptionValue::Int(123),
        );
        assert!(result.is_err());

        // client_secret should reject non-string
        let result = db.set_option(
            OptionDatabase::Other("databricks.auth.client_secret".into()),
            OptionValue::Int(123),
        );
        assert!(result.is_err());

        // scopes should reject non-string
        let result = db.set_option(
            OptionDatabase::Other("databricks.auth.scopes".into()),
            OptionValue::Int(123),
        );
        assert!(result.is_err());

        // token_endpoint should reject non-string
        let result = db.set_option(
            OptionDatabase::Other("databricks.auth.token_endpoint".into()),
            OptionValue::Int(123),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_database_get_auth_options_unset_returns_error() {
        let db = Database::new();

        assert!(db
            .get_option_string(OptionDatabase::Other("databricks.auth.type".into()))
            .is_err());
        assert!(db
            .get_option_string(OptionDatabase::Other("databricks.auth.client_id".into()))
            .is_err());
        assert!(db
            .get_option_string(OptionDatabase::Other(
                "databricks.auth.client_secret".into()
            ))
            .is_err());
        assert!(db
            .get_option_string(OptionDatabase::Other("databricks.auth.scopes".into()))
            .is_err());
        assert!(db
            .get_option_string(OptionDatabase::Other(
                "databricks.auth.token_endpoint".into()
            ))
            .is_err());
        assert!(db
            .get_option_int(OptionDatabase::Other(
                "databricks.auth.redirect_port".into()
            ))
            .is_err());
    }

    // Config validation tests for new_connection()

    #[test]
    fn test_new_connection_missing_auth_type() {
        use adbc_core::Database as _;

        let mut db = Database::new();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("https://example.databricks.com".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.warehouse_id".into()),
            OptionValue::String("test123".into()),
        )
        .unwrap();

        let result = db.new_connection();
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(
            err_msg.contains("databricks.auth.type is required"),
            "Expected error message about missing auth type, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_new_connection_oauth_m2m_missing_client_id() {
        use adbc_core::Database as _;

        let mut db = Database::new();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("https://example.databricks.com".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.warehouse_id".into()),
            OptionValue::String("test123".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.auth.type".into()),
            OptionValue::String("oauth_m2m".into()),
        )
        .unwrap();

        let result = db.new_connection();
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(
            err_msg.contains("databricks.auth.client_id is required"),
            "Expected error message about missing client_id, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_new_connection_oauth_m2m_missing_secret() {
        use adbc_core::Database as _;

        let mut db = Database::new();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("https://example.databricks.com".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.warehouse_id".into()),
            OptionValue::String("test123".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.auth.type".into()),
            OptionValue::String("oauth_m2m".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.auth.client_id".into()),
            OptionValue::String("test-client-id".into()),
        )
        .unwrap();

        let result = db.new_connection();
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(
            err_msg.contains("databricks.auth.client_secret is required"),
            "Expected error message about missing client_secret, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_new_connection_access_token_missing_token() {
        use adbc_core::Database as _;

        let mut db = Database::new();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("https://example.databricks.com".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.warehouse_id".into()),
            OptionValue::String("test123".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.auth.type".into()),
            OptionValue::String("access_token".into()),
        )
        .unwrap();

        let result = db.new_connection();
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(
            err_msg.contains("databricks.access_token is required"),
            "Expected error message about missing access_token, got: {}",
            err_msg
        );
    }

    // U2M (OAuth) configuration tests

    #[test]
    fn test_database_oauth_u2m_config_with_defaults() {
        let mut db = Database::new();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("https://example.databricks.com".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.warehouse_id".into()),
            OptionValue::String("test123".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.auth.type".into()),
            OptionValue::String("oauth_u2m".into()),
        )
        .unwrap();

        assert_eq!(db.auth_config.auth_type, Some(AuthType::OAuthU2m));
        assert_eq!(db.auth_config.client_id, None); // Will use default "databricks-cli"
        assert_eq!(db.auth_config.scopes, None); // Will use default "all-apis offline_access"
        assert_eq!(db.auth_config.redirect_port, None); // Will use default 8020
    }

    #[test]
    fn test_database_oauth_u2m_config_with_overrides() {
        let mut db = Database::new();
        db.set_option(
            OptionDatabase::Other("databricks.auth.type".into()),
            OptionValue::String("oauth_u2m".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.auth.client_id".into()),
            OptionValue::String("custom-client-id".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.auth.scopes".into()),
            OptionValue::String("custom-scope other-scope".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.auth.redirect_port".into()),
            OptionValue::Int(9000),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.auth.token_endpoint".into()),
            OptionValue::String("https://custom.endpoint/token".into()),
        )
        .unwrap();

        assert_eq!(db.auth_config.auth_type, Some(AuthType::OAuthU2m));
        assert_eq!(
            db.auth_config.client_id,
            Some("custom-client-id".to_string())
        );
        assert_eq!(
            db.auth_config.scopes,
            Some("custom-scope other-scope".to_string())
        );
        assert_eq!(db.auth_config.redirect_port, Some(9000));
        assert_eq!(
            db.auth_config.token_endpoint,
            Some("https://custom.endpoint/token".to_string())
        );
    }

    #[test]
    fn test_database_oauth_u2m_validation_passes() {
        let config = AuthConfig {
            auth_type: Some(AuthType::OAuthU2m),
            ..Default::default()
        };
        // U2M doesn't require client_id, scopes, etc. (all have defaults)
        let result = config.validate(&None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), AuthType::OAuthU2m);
    }

    #[test]
    fn test_database_m2m_config_validation() {
        let mut db = Database::new();
        db.set_option(
            OptionDatabase::Other("databricks.auth.type".into()),
            OptionValue::String("oauth_m2m".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.auth.client_id".into()),
            OptionValue::String("test-client-id".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.auth.client_secret".into()),
            OptionValue::String("test-secret".into()),
        )
        .unwrap();

        assert_eq!(db.auth_config.auth_type, Some(AuthType::OAuthM2m));
        assert_eq!(db.auth_config.client_id, Some("test-client-id".to_string()));
        assert_eq!(
            db.auth_config.client_secret,
            Some("test-secret".to_string())
        );
    }

    #[test]
    fn test_database_m2m_config_with_custom_scopes() {
        let mut db = Database::new();
        db.set_option(
            OptionDatabase::Other("databricks.auth.type".into()),
            OptionValue::String("oauth_m2m".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.auth.client_id".into()),
            OptionValue::String("test-client-id".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.auth.client_secret".into()),
            OptionValue::String("test-secret".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.auth.scopes".into()),
            OptionValue::String("custom-scope-1 custom-scope-2".into()),
        )
        .unwrap();

        assert_eq!(
            db.auth_config.scopes,
            Some("custom-scope-1 custom-scope-2".to_string())
        );
    }

    #[test]
    fn test_database_m2m_config_with_token_endpoint_override() {
        let mut db = Database::new();
        db.set_option(
            OptionDatabase::Other("databricks.auth.type".into()),
            OptionValue::String("oauth_m2m".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.auth.client_id".into()),
            OptionValue::String("test-client-id".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.auth.client_secret".into()),
            OptionValue::String("test-secret".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.auth.token_endpoint".into()),
            OptionValue::String("https://custom.endpoint/token".into()),
        )
        .unwrap();

        assert_eq!(
            db.auth_config.token_endpoint,
            Some("https://custom.endpoint/token".to_string())
        );
    }
}
