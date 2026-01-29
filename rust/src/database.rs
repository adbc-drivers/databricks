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

use crate::connection::Connection;
use crate::error::DatabricksErrorHelper;
use adbc_core::error::Result;
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::Optionable;
use driverbase::error::ErrorHelper;

/// Represents a database instance that holds connection configuration.
///
/// A Database is created from a Driver and is used to establish Connections.
/// Configuration options like host, credentials, and HTTP path are set on
/// the Database before creating connections.
#[derive(Debug, Default)]
pub struct Database {
    uri: Option<String>,
    http_path: Option<String>,
    access_token: Option<String>,
    catalog: Option<String>,
    schema: Option<String>,
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

    /// Returns the configured HTTP path.
    pub fn http_path(&self) -> Option<&str> {
        self.http_path.as_deref()
    }

    /// Returns the configured catalog.
    pub fn catalog(&self) -> Option<&str> {
        self.catalog.as_deref()
    }

    /// Returns the configured schema.
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
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
                "databricks.http_path" => {
                    if let OptionValue::String(v) = value {
                        self.http_path = Some(v);
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
                "databricks.http_path" => self.http_path.clone().ok_or_else(|| {
                    DatabricksErrorHelper::invalid_state()
                        .message("option 'databricks.http_path' is not set")
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
        Err(DatabricksErrorHelper::get_unknown_option(&key).to_adbc())
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        Err(DatabricksErrorHelper::get_unknown_option(&key).to_adbc())
    }
}

impl adbc_core::Database for Database {
    type ConnectionType = Connection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        Ok(Connection::new(
            self.uri.clone(),
            self.http_path.clone(),
            self.access_token.clone(),
            self.catalog.clone(),
            self.schema.clone(),
        ))
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
        assert_eq!(db.http_path(), Some("/sql/1.0/warehouses/abc123"));
    }

    #[test]
    fn test_database_new_connection() {
        use adbc_core::Database as _;

        let db = Database::new();
        let connection = db.new_connection();
        assert!(connection.is_ok());
    }
}
