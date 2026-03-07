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

use crate::auth::PersonalAccessToken;
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

/// Authentication mechanism -- top-level selector.
/// Config values match the ODBC driver's AuthMech numeric codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AuthMechanism {
    /// Personal access token (no OAuth). Config value: 0
    Pat = 0,
    /// OAuth 2.0 -- requires AuthFlow to select the specific flow. Config value: 11
    OAuth = 11,
}

impl TryFrom<i64> for AuthMechanism {
    type Error = crate::error::Error;

    fn try_from(value: i64) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(AuthMechanism::Pat),
            11 => Ok(AuthMechanism::OAuth),
            _ => Err(DatabricksErrorHelper::invalid_argument()
                .message(format!(
                    "Invalid auth mechanism value: {}. Valid values are 0 (PAT) or 11 (OAuth)",
                    value
                ))
                .into()),
        }
    }
}

/// OAuth authentication flow -- selects the specific OAuth grant type.
/// Config values match the ODBC driver's Auth_Flow numeric codes.
/// Only applicable when AuthMechanism is OAuth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AuthFlow {
    /// Use a pre-obtained OAuth access token directly. Config value: 0
    TokenPassthrough = 0,
    /// M2M: client credentials grant for service principals. Config value: 1
    ClientCredentials = 1,
    /// U2M: browser-based authorization code + PKCE. Config value: 2
    Browser = 2,
}

impl TryFrom<i64> for AuthFlow {
    type Error = crate::error::Error;

    fn try_from(value: i64) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(AuthFlow::TokenPassthrough),
            1 => Ok(AuthFlow::ClientCredentials),
            2 => Ok(AuthFlow::Browser),
            _ => Err(DatabricksErrorHelper::invalid_argument()
                .message(format!(
                    "Invalid auth flow value: {}. Valid values are 0 (token passthrough), 1 (client credentials), or 2 (browser)",
                    value
                ))
                .into()),
        }
    }
}

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

    // OAuth configuration
    auth_mechanism: Option<AuthMechanism>,
    auth_flow: Option<AuthFlow>,
    auth_client_id: Option<String>,
    auth_client_secret: Option<String>,
    auth_scopes: Option<String>,
    auth_token_endpoint: Option<String>,
    auth_redirect_port: Option<u16>,
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
    /// Format: /sql/1.0/warehouses/{warehouse_id}
    fn extract_warehouse_id(http_path: &str) -> Option<String> {
        http_path
            .strip_prefix("/sql/1.0/warehouses/")
            .or_else(|| http_path.strip_prefix("sql/1.0/warehouses/"))
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

                // OAuth configuration options
                "databricks.auth.mechanism" => {
                    if let Some(v) = Self::parse_int_option(&value) {
                        self.auth_mechanism = Some(AuthMechanism::try_from(v)?);
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }
                "databricks.auth.flow" => {
                    if let Some(v) = Self::parse_int_option(&value) {
                        self.auth_flow = Some(AuthFlow::try_from(v)?);
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }
                "databricks.auth.client_id" => {
                    if let OptionValue::String(v) = value {
                        self.auth_client_id = Some(v);
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }
                "databricks.auth.client_secret" => {
                    if let OptionValue::String(v) = value {
                        self.auth_client_secret = Some(v);
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }
                "databricks.auth.scopes" => {
                    if let OptionValue::String(v) = value {
                        self.auth_scopes = Some(v);
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }
                "databricks.auth.token_endpoint" => {
                    if let OptionValue::String(v) = value {
                        self.auth_token_endpoint = Some(v);
                        Ok(())
                    } else {
                        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
                    }
                }
                "databricks.auth.redirect_port" => {
                    if let Some(v) = Self::parse_int_option(&value) {
                        if v < 0 || v > 65535 {
                            return Err(DatabricksErrorHelper::invalid_argument()
                                .message(format!(
                                    "Invalid redirect port: {}. Port must be between 0 and 65535",
                                    v
                                ))
                                .to_adbc());
                        }
                        self.auth_redirect_port = Some(v as u16);
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
                "databricks.auth.client_id" => self.auth_client_id.clone().ok_or_else(|| {
                    DatabricksErrorHelper::invalid_state()
                        .message("option 'databricks.auth.client_id' is not set")
                        .to_adbc()
                }),
                "databricks.auth.client_secret" => self.auth_client_secret.clone().ok_or_else(|| {
                    DatabricksErrorHelper::invalid_state()
                        .message("option 'databricks.auth.client_secret' is not set")
                        .to_adbc()
                }),
                "databricks.auth.scopes" => self.auth_scopes.clone().ok_or_else(|| {
                    DatabricksErrorHelper::invalid_state()
                        .message("option 'databricks.auth.scopes' is not set")
                        .to_adbc()
                }),
                "databricks.auth.token_endpoint" => {
                    self.auth_token_endpoint.clone().ok_or_else(|| {
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
                "databricks.auth.mechanism" => self
                    .auth_mechanism
                    .ok_or_else(|| {
                        DatabricksErrorHelper::invalid_state()
                            .message("option 'databricks.auth.mechanism' is not set")
                            .to_adbc()
                    })
                    .map(|m| m as i64),
                "databricks.auth.flow" => self
                    .auth_flow
                    .ok_or_else(|| {
                        DatabricksErrorHelper::invalid_state()
                            .message("option 'databricks.auth.flow' is not set")
                            .to_adbc()
                    })
                    .map(|f| f as i64),
                "databricks.auth.redirect_port" => self
                    .auth_redirect_port
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
        let access_token = self.access_token.as_ref().ok_or_else(|| {
            DatabricksErrorHelper::invalid_argument()
                .message("access_token not set")
                .to_adbc()
        })?;

        // Create HTTP client (without auth provider - two-phase initialization)
        let http_client =
            Arc::new(DatabricksHttpClient::new(self.http_config.clone()).map_err(|e| e.to_adbc())?);

        // Create auth provider
        let auth_provider = Arc::new(PersonalAccessToken::new(access_token.clone()));

        // Set auth provider on HTTP client (phase 2)
        http_client.set_auth_provider(auth_provider);

        // Create tokio runtime for async operations
        let runtime = tokio::runtime::Runtime::new().map_err(|e| {
            DatabricksErrorHelper::io()
                .message(format!("Failed to create async runtime: {}", e))
                .to_adbc()
        })?;

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

    #[test]
    fn test_set_auth_mechanism_valid() {
        // Test valid PAT value (0)
        let mechanism = AuthMechanism::try_from(0).unwrap();
        assert_eq!(mechanism, AuthMechanism::Pat);
        assert_eq!(mechanism as u8, 0);

        // Test valid OAuth value (11)
        let mechanism = AuthMechanism::try_from(11).unwrap();
        assert_eq!(mechanism, AuthMechanism::OAuth);
        assert_eq!(mechanism as u8, 11);
    }

    #[test]
    fn test_set_auth_mechanism_invalid() {
        // Test invalid values
        assert!(AuthMechanism::try_from(1).is_err());
        assert!(AuthMechanism::try_from(10).is_err());
        assert!(AuthMechanism::try_from(12).is_err());
        assert!(AuthMechanism::try_from(-1).is_err());
        assert!(AuthMechanism::try_from(100).is_err());
    }

    #[test]
    fn test_set_auth_flow_valid() {
        // Test valid TokenPassthrough value (0)
        let flow = AuthFlow::try_from(0).unwrap();
        assert_eq!(flow, AuthFlow::TokenPassthrough);
        assert_eq!(flow as u8, 0);

        // Test valid ClientCredentials value (1)
        let flow = AuthFlow::try_from(1).unwrap();
        assert_eq!(flow, AuthFlow::ClientCredentials);
        assert_eq!(flow as u8, 1);

        // Test valid Browser value (2)
        let flow = AuthFlow::try_from(2).unwrap();
        assert_eq!(flow, AuthFlow::Browser);
        assert_eq!(flow as u8, 2);
    }

    #[test]
    fn test_set_auth_flow_invalid() {
        // Test invalid values
        assert!(AuthFlow::try_from(3).is_err());
        assert!(AuthFlow::try_from(-1).is_err());
        assert!(AuthFlow::try_from(10).is_err());
        assert!(AuthFlow::try_from(100).is_err());
    }

    #[test]
    fn test_database_set_auth_mechanism_option() {
        let mut db = Database::new();

        // Test valid PAT value (0)
        db.set_option(
            OptionDatabase::Other("databricks.auth.mechanism".into()),
            OptionValue::String("0".into()),
        )
        .unwrap();
        assert_eq!(db.auth_mechanism, Some(AuthMechanism::Pat));

        // Test valid OAuth value (11)
        db.set_option(
            OptionDatabase::Other("databricks.auth.mechanism".into()),
            OptionValue::String("11".into()),
        )
        .unwrap();
        assert_eq!(db.auth_mechanism, Some(AuthMechanism::OAuth));

        // Test with OptionValue::Int
        db.set_option(
            OptionDatabase::Other("databricks.auth.mechanism".into()),
            OptionValue::Int(0),
        )
        .unwrap();
        assert_eq!(db.auth_mechanism, Some(AuthMechanism::Pat));
    }

    #[test]
    fn test_database_set_auth_mechanism_invalid() {
        let mut db = Database::new();

        // Test invalid integer value
        let result = db.set_option(
            OptionDatabase::Other("databricks.auth.mechanism".into()),
            OptionValue::String("99".into()),
        );
        assert!(result.is_err());

        // Test invalid non-integer string
        let result = db.set_option(
            OptionDatabase::Other("databricks.auth.mechanism".into()),
            OptionValue::String("invalid".into()),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_database_set_auth_flow_option() {
        let mut db = Database::new();

        // Test TokenPassthrough (0)
        db.set_option(
            OptionDatabase::Other("databricks.auth.flow".into()),
            OptionValue::String("0".into()),
        )
        .unwrap();
        assert_eq!(db.auth_flow, Some(AuthFlow::TokenPassthrough));

        // Test ClientCredentials (1)
        db.set_option(
            OptionDatabase::Other("databricks.auth.flow".into()),
            OptionValue::String("1".into()),
        )
        .unwrap();
        assert_eq!(db.auth_flow, Some(AuthFlow::ClientCredentials));

        // Test Browser (2)
        db.set_option(
            OptionDatabase::Other("databricks.auth.flow".into()),
            OptionValue::String("2".into()),
        )
        .unwrap();
        assert_eq!(db.auth_flow, Some(AuthFlow::Browser));

        // Test with OptionValue::Int
        db.set_option(
            OptionDatabase::Other("databricks.auth.flow".into()),
            OptionValue::Int(1),
        )
        .unwrap();
        assert_eq!(db.auth_flow, Some(AuthFlow::ClientCredentials));
    }

    #[test]
    fn test_database_set_auth_flow_invalid() {
        let mut db = Database::new();

        // Test invalid integer value
        let result = db.set_option(
            OptionDatabase::Other("databricks.auth.flow".into()),
            OptionValue::String("5".into()),
        );
        assert!(result.is_err());

        // Test invalid non-integer string
        let result = db.set_option(
            OptionDatabase::Other("databricks.auth.flow".into()),
            OptionValue::String("browser".into()),
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

        assert_eq!(db.auth_client_id, Some("test-client-id".to_string()));
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

        assert_eq!(db.auth_client_secret, Some("test-secret".to_string()));
        assert_eq!(
            db.get_option_string(OptionDatabase::Other("databricks.auth.client_secret".into()))
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
            db.auth_scopes,
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
            db.auth_token_endpoint,
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
        assert_eq!(db.auth_redirect_port, Some(8020));
        assert_eq!(
            db.get_option_int(OptionDatabase::Other("databricks.auth.redirect_port".into()))
                .unwrap(),
            8020
        );

        // Test with OptionValue::Int
        db.set_option(
            OptionDatabase::Other("databricks.auth.redirect_port".into()),
            OptionValue::Int(9000),
        )
        .unwrap();
        assert_eq!(db.auth_redirect_port, Some(9000));

        // Test port 0 (OS-assigned)
        db.set_option(
            OptionDatabase::Other("databricks.auth.redirect_port".into()),
            OptionValue::Int(0),
        )
        .unwrap();
        assert_eq!(db.auth_redirect_port, Some(0));

        // Test max port
        db.set_option(
            OptionDatabase::Other("databricks.auth.redirect_port".into()),
            OptionValue::Int(65535),
        )
        .unwrap();
        assert_eq!(db.auth_redirect_port, Some(65535));
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
            .get_option_string(OptionDatabase::Other("databricks.auth.client_id".into()))
            .is_err());
        assert!(db
            .get_option_string(OptionDatabase::Other("databricks.auth.client_secret".into()))
            .is_err());
        assert!(db
            .get_option_string(OptionDatabase::Other("databricks.auth.scopes".into()))
            .is_err());
        assert!(db
            .get_option_string(OptionDatabase::Other("databricks.auth.token_endpoint".into()))
            .is_err());
        assert!(db
            .get_option_int(OptionDatabase::Other("databricks.auth.mechanism".into()))
            .is_err());
        assert!(db
            .get_option_int(OptionDatabase::Other("databricks.auth.flow".into()))
            .is_err());
        assert!(db
            .get_option_int(OptionDatabase::Other("databricks.auth.redirect_port".into()))
            .is_err());
    }

    #[test]
    fn test_database_get_auth_mechanism_as_int() {
        let mut db = Database::new();

        // Set PAT mechanism
        db.set_option(
            OptionDatabase::Other("databricks.auth.mechanism".into()),
            OptionValue::Int(0),
        )
        .unwrap();
        assert_eq!(
            db.get_option_int(OptionDatabase::Other("databricks.auth.mechanism".into()))
                .unwrap(),
            0
        );

        // Set OAuth mechanism
        db.set_option(
            OptionDatabase::Other("databricks.auth.mechanism".into()),
            OptionValue::Int(11),
        )
        .unwrap();
        assert_eq!(
            db.get_option_int(OptionDatabase::Other("databricks.auth.mechanism".into()))
                .unwrap(),
            11
        );
    }

    #[test]
    fn test_database_get_auth_flow_as_int() {
        let mut db = Database::new();

        // Set TokenPassthrough flow
        db.set_option(
            OptionDatabase::Other("databricks.auth.flow".into()),
            OptionValue::Int(0),
        )
        .unwrap();
        assert_eq!(
            db.get_option_int(OptionDatabase::Other("databricks.auth.flow".into()))
                .unwrap(),
            0
        );

        // Set ClientCredentials flow
        db.set_option(
            OptionDatabase::Other("databricks.auth.flow".into()),
            OptionValue::Int(1),
        )
        .unwrap();
        assert_eq!(
            db.get_option_int(OptionDatabase::Other("databricks.auth.flow".into()))
                .unwrap(),
            1
        );

        // Set Browser flow
        db.set_option(
            OptionDatabase::Other("databricks.auth.flow".into()),
            OptionValue::Int(2),
        )
        .unwrap();
        assert_eq!(
            db.get_option_int(OptionDatabase::Other("databricks.auth.flow".into()))
                .unwrap(),
            2
        );
    }
}
