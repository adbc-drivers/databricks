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

//! Statement implementation for the Databricks ADBC driver.

use crate::error::Error;
use crate::result::ResultSet;
use crate::Result;

/// Represents a SQL statement that can be executed against Databricks.
///
/// A Statement is created from a Connection and is used to execute SQL
/// queries and retrieve results.
#[derive(Debug, Default)]
pub struct Statement {
    query: Option<String>,
}

impl Statement {
    /// Creates a new Statement.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the SQL query to execute.
    pub fn set_sql_query(&mut self, query: impl Into<String>) -> &mut Self {
        self.query = Some(query.into());
        self
    }

    /// Returns the current SQL query.
    pub fn sql_query(&self) -> Option<&str> {
        self.query.as_deref()
    }

    /// Executes the statement and returns a result set.
    pub fn execute_query(&self) -> Result<ResultSet> {
        if self.query.is_none() {
            return Err(Error::Statement("No query set".to_string()));
        }
        Err(Error::NotImplemented("execute_query".to_string()))
    }

    /// Executes the statement for its side effects (e.g., INSERT, UPDATE).
    pub fn execute_update(&self) -> Result<i64> {
        if self.query.is_none() {
            return Err(Error::Statement("No query set".to_string()));
        }
        Err(Error::NotImplemented("execute_update".to_string()))
    }

    /// Closes the statement and releases resources.
    pub fn close(&mut self) -> Result<()> {
        self.query = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_statement_set_query() {
        let mut stmt = Statement::new();
        stmt.set_sql_query("SELECT 1");
        assert_eq!(stmt.sql_query(), Some("SELECT 1"));
    }

    #[test]
    fn test_statement_execute_without_query() {
        let stmt = Statement::new();
        assert!(stmt.execute_query().is_err());
    }

    #[test]
    fn test_statement_close() {
        let mut stmt = Statement::new();
        stmt.set_sql_query("SELECT 1");
        assert!(stmt.close().is_ok());
        assert!(stmt.sql_query().is_none());
    }
}
