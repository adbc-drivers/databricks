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

//! Database implementation for the Databricks ADBC driver.

use crate::connection::Connection;
use crate::Result;

/// Represents a database instance that holds connection configuration.
///
/// A Database is created from a Driver and is used to establish Connections.
/// Configuration options like host, credentials, and HTTP path are set on
/// the Database before creating connections.
#[derive(Debug, Default)]
pub struct Database {
    host: Option<String>,
    http_path: Option<String>,
    catalog: Option<String>,
    schema: Option<String>,
}

impl Database {
    /// Creates a new Database instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the Databricks host URL.
    pub fn with_host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    /// Sets the HTTP path for the SQL warehouse or cluster.
    pub fn with_http_path(mut self, http_path: impl Into<String>) -> Self {
        self.http_path = Some(http_path.into());
        self
    }

    /// Sets the default catalog.
    pub fn with_catalog(mut self, catalog: impl Into<String>) -> Self {
        self.catalog = Some(catalog.into());
        self
    }

    /// Sets the default schema.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Returns the configured host.
    pub fn host(&self) -> Option<&str> {
        self.host.as_deref()
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

    /// Creates a new Connection from this Database.
    pub fn connect(&self) -> Result<Connection> {
        Ok(Connection::new(
            self.host.clone(),
            self.http_path.clone(),
            self.catalog.clone(),
            self.schema.clone(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_builder() {
        let db = Database::new()
            .with_host("https://example.databricks.com")
            .with_http_path("/sql/1.0/warehouses/abc123")
            .with_catalog("main")
            .with_schema("default");

        assert_eq!(db.host(), Some("https://example.databricks.com"));
        assert_eq!(db.http_path(), Some("/sql/1.0/warehouses/abc123"));
        assert_eq!(db.catalog(), Some("main"));
        assert_eq!(db.schema(), Some("default"));
    }

    #[test]
    fn test_database_connect() {
        let db = Database::new();
        assert!(db.connect().is_ok());
    }
}
