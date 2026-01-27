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

//! Connection implementation for the Databricks ADBC driver.

use crate::error::DatabricksErrorHelper;
use crate::statement::Statement;
use adbc_core::error::Result;
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue};
use adbc_core::Optionable;
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, Schema};
use driverbase::error::ErrorHelper;
use std::collections::HashSet;
use std::sync::Arc;

/// Represents an active connection to a Databricks SQL endpoint.
///
/// A Connection is created from a Database and is used to create Statements
/// for executing SQL queries.
#[derive(Debug)]
pub struct Connection {
    uri: Option<String>,
    http_path: Option<String>,
    #[allow(dead_code)]
    access_token: Option<String>,
    catalog: Option<String>,
    schema: Option<String>,
}

/// Type alias for our empty reader used in stub implementations.
type EmptyReader =
    RecordBatchIterator<std::vec::IntoIter<std::result::Result<RecordBatch, ArrowError>>>;

/// Creates an empty RecordBatchIterator for stub implementations.
#[allow(dead_code)]
fn empty_reader() -> EmptyReader {
    let schema = Arc::new(Schema::empty());
    RecordBatchIterator::new(vec![].into_iter(), schema)
}

impl Connection {
    /// Creates a new Connection with the given configuration.
    pub(crate) fn new(
        uri: Option<String>,
        http_path: Option<String>,
        access_token: Option<String>,
        catalog: Option<String>,
        schema: Option<String>,
    ) -> Self {
        Self {
            uri,
            http_path,
            access_token,
            catalog,
            schema,
        }
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

impl Optionable for Connection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, _value: OptionValue) -> Result<()> {
        match key {
            OptionConnection::AutoCommit => {
                // Databricks SQL doesn't support transactions in the traditional sense
                // Just accept and ignore this option
                Ok(())
            }
            _ => Err(DatabricksErrorHelper::set_unknown_option(&key).to_adbc()),
        }
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

impl adbc_core::Connection for Connection {
    type StatementType = Statement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Ok(Statement::new())
    }

    fn cancel(&mut self) -> Result<()> {
        // TODO: Implement cancellation
        Ok(())
    }

    fn get_info(&self, codes: Option<HashSet<InfoCode>>) -> Result<impl RecordBatchReader + Send> {
        use driverbase::InfoBuilder;

        let mut builder = InfoBuilder::new();

        // Filter by requested codes or return all if none specified
        let return_all = codes.is_none();
        let codes = codes.unwrap_or_default();

        if return_all || codes.contains(&InfoCode::DriverName) {
            builder.add_string(InfoCode::DriverName as u32, "Databricks ADBC Driver");
        }
        if return_all || codes.contains(&InfoCode::DriverVersion) {
            builder.add_string(InfoCode::DriverVersion as u32, env!("CARGO_PKG_VERSION"));
        }
        if return_all || codes.contains(&InfoCode::VendorName) {
            builder.add_string(InfoCode::VendorName as u32, "Databricks");
        }

        Ok(builder.build())
    }

    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> Result<impl RecordBatchReader + Send> {
        Err::<EmptyReader, _>(
            DatabricksErrorHelper::not_implemented()
                .message("get_objects")
                .to_adbc(),
        )
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> Result<Schema> {
        Err(DatabricksErrorHelper::not_implemented()
            .message("get_table_schema")
            .to_adbc())
    }

    fn get_table_types(&self) -> Result<impl RecordBatchReader + Send> {
        Err::<EmptyReader, _>(
            DatabricksErrorHelper::not_implemented()
                .message("get_table_types")
                .to_adbc(),
        )
    }

    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<impl RecordBatchReader + Send> {
        Err::<EmptyReader, _>(
            DatabricksErrorHelper::not_implemented()
                .message("read_partition")
                .to_adbc(),
        )
    }

    fn commit(&mut self) -> Result<()> {
        // Databricks SQL is auto-commit only
        Ok(())
    }

    fn rollback(&mut self) -> Result<()> {
        // Databricks SQL doesn't support rollback
        Err(DatabricksErrorHelper::not_implemented()
            .message("rollback - Databricks SQL is auto-commit only")
            .to_adbc())
    }

    fn get_statistic_names(&self) -> Result<impl RecordBatchReader + Send> {
        Err::<EmptyReader, _>(
            DatabricksErrorHelper::not_implemented()
                .message("get_statistic_names")
                .to_adbc(),
        )
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<impl RecordBatchReader + Send> {
        Err::<EmptyReader, _>(
            DatabricksErrorHelper::not_implemented()
                .message("get_statistics")
                .to_adbc(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::Connection as _;

    #[test]
    fn test_connection_new_statement() {
        let mut conn = Connection::new(None, None, None, None, None);
        assert!(conn.new_statement().is_ok());
    }

    #[test]
    fn test_connection_get_info() {
        let conn = Connection::new(None, None, None, None, None);
        let result = conn.get_info(None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_connection_commit() {
        let mut conn = Connection::new(None, None, None, None, None);
        assert!(conn.commit().is_ok());
    }

    #[test]
    fn test_connection_rollback() {
        let mut conn = Connection::new(None, None, None, None, None);
        assert!(conn.rollback().is_err());
    }
}
