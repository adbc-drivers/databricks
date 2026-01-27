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

//! Statement implementation for the Databricks ADBC driver.

use crate::error::DatabricksErrorHelper;
use adbc_core::error::Result;
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::Optionable;
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, Schema};
use driverbase::error::ErrorHelper;

/// Type alias for our empty reader used in stub implementations.
type EmptyReader =
    RecordBatchIterator<std::vec::IntoIter<std::result::Result<RecordBatch, ArrowError>>>;

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

    /// Returns the current SQL query.
    pub fn sql_query(&self) -> Option<&str> {
        self.query.as_deref()
    }
}

impl Optionable for Statement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, _value: OptionValue) -> Result<()> {
        Err(DatabricksErrorHelper::set_unknown_option(&key).to_adbc())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        Err(DatabricksErrorHelper::get_unknown_option(&key).to_adbc())
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

impl adbc_core::Statement for Statement {
    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.query = Some(query.as_ref().to_string());
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Err(DatabricksErrorHelper::not_implemented()
            .message("Substrait plans")
            .to_adbc())
    }

    fn prepare(&mut self) -> Result<()> {
        Err(DatabricksErrorHelper::not_implemented()
            .message("prepare")
            .to_adbc())
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        Err(DatabricksErrorHelper::not_implemented()
            .message("get_parameter_schema")
            .to_adbc())
    }

    fn bind(&mut self, _batch: arrow_array::RecordBatch) -> Result<()> {
        Err(DatabricksErrorHelper::not_implemented()
            .message("bind parameters")
            .to_adbc())
    }

    fn bind_stream(&mut self, _stream: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        Err(DatabricksErrorHelper::not_implemented()
            .message("bind_stream")
            .to_adbc())
    }

    fn execute(&mut self) -> Result<impl RecordBatchReader + Send> {
        Err::<EmptyReader, _>(
            DatabricksErrorHelper::not_implemented()
                .message("execute")
                .to_adbc(),
        )
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        Err(DatabricksErrorHelper::not_implemented()
            .message("execute_update")
            .to_adbc())
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        Err(DatabricksErrorHelper::not_implemented()
            .message("execute_schema")
            .to_adbc())
    }

    fn execute_partitions(&mut self) -> Result<adbc_core::PartitionedResult> {
        Err(DatabricksErrorHelper::not_implemented()
            .message("execute_partitions")
            .to_adbc())
    }

    fn cancel(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::Statement as _;

    #[test]
    fn test_statement_set_query() {
        let mut stmt = Statement::new();
        stmt.set_sql_query("SELECT 1").unwrap();
        assert_eq!(stmt.sql_query(), Some("SELECT 1"));
    }

    #[test]
    fn test_statement_execute() {
        let mut stmt = Statement::new();
        stmt.set_sql_query("SELECT 1").unwrap();
        // Should fail with "not implemented"
        let result = stmt.execute();
        assert!(result.is_err());
    }
}
