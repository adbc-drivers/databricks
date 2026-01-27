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

//! Connection implementation for the Databricks ADBC driver.

use crate::statement::Statement;
use crate::Result;

/// Represents an active connection to a Databricks SQL endpoint.
///
/// A Connection is created from a Database and is used to create Statements
/// for executing SQL queries.
#[derive(Debug)]
pub struct Connection {
    host: Option<String>,
    http_path: Option<String>,
    catalog: Option<String>,
    schema: Option<String>,
}

impl Connection {
    /// Creates a new Connection with the given configuration.
    pub(crate) fn new(
        host: Option<String>,
        http_path: Option<String>,
        catalog: Option<String>,
        schema: Option<String>,
    ) -> Self {
        Self {
            host,
            http_path,
            catalog,
            schema,
        }
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

    /// Creates a new Statement for executing queries.
    pub fn new_statement(&self) -> Result<Statement> {
        Ok(Statement::new())
    }

    /// Closes the connection.
    pub fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_new_statement() {
        let conn = Connection::new(None, None, None, None);
        assert!(conn.new_statement().is_ok());
    }

    #[test]
    fn test_connection_close() {
        let mut conn = Connection::new(None, None, None, None);
        assert!(conn.close().is_ok());
    }
}
