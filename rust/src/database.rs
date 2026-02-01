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
use crate::client::{DatabricksClient, DatabricksHttpClient, HttpClientConfig, SeaClient};
use crate::connection::{Connection, ConnectionConfig};
use crate::error::DatabricksErrorHelper;
use crate::types::cloudfetch::CloudFetchConfig;
use adbc_core::error::Result;
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::Optionable;
use driverbase::error::ErrorHelper;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

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

        debug!(
            "Creating connection to {} with warehouse {}",
            host, warehouse_id
        );

        // Create auth provider
        let auth_provider = Arc::new(PersonalAccessToken::new(access_token.clone()));

        // Create HTTP client
        let http_client = Arc::new(
            DatabricksHttpClient::new(self.http_config.clone(), auth_provider)
                .map_err(|e| e.to_adbc())?,
        );

        // Create DatabricksClient (currently always SEA)
        let client: Arc<dyn DatabricksClient> =
            Arc::new(SeaClient::new(http_client.clone(), host, warehouse_id));

        // Create connection
        Connection::new(ConnectionConfig {
            host: host.clone(),
            warehouse_id: warehouse_id.clone(),
            catalog: self.catalog.clone(),
            schema: self.schema.clone(),
            client,
            http_client,
            cloudfetch_config: self.cloudfetch_config.clone(),
        })
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
}
