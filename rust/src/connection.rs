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

use crate::client::{DatabricksClient, DatabricksHttpClient};
use crate::error::DatabricksErrorHelper;
use crate::reader::ResultReaderFactory;
use crate::statement::Statement;
use crate::types::cloudfetch::CloudFetchConfig;
use adbc_core::error::Result;
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue};
use adbc_core::Optionable;
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, Schema};
use driverbase::error::ErrorHelper;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::debug;

/// Configuration passed from Database to Connection.
pub struct ConnectionConfig {
    pub host: String,
    pub warehouse_id: String,
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub client: Arc<dyn DatabricksClient>,
    pub http_client: Arc<DatabricksHttpClient>,
    pub cloudfetch_config: CloudFetchConfig,
}

/// Represents an active connection to a Databricks SQL endpoint.
///
/// A Connection is created from a Database and is used to create Statements
/// for executing SQL queries. It maintains a session with the Databricks
/// server and manages shared resources like the HTTP client.
#[derive(Debug)]
pub struct Connection {
    // Configuration
    host: String,
    warehouse_id: String,

    // Databricks client (trait object for backend flexibility)
    client: Arc<dyn DatabricksClient>,

    // HTTP client for CloudFetch downloads
    http_client: Arc<DatabricksHttpClient>,

    // Session ID (created on connection initialization)
    session_id: String,

    // CloudFetch settings
    cloudfetch_config: CloudFetchConfig,

    // Tokio runtime for async operations
    runtime: tokio::runtime::Runtime,
}

/// Type alias for our empty reader used in stub implementations.
type EmptyReader =
    RecordBatchIterator<std::vec::IntoIter<std::result::Result<RecordBatch, ArrowError>>>;

impl Connection {
    /// Called by Database::new_connection().
    ///
    /// Connection receives the DatabricksClient from Database - it does NOT
    /// create the client itself. This keeps the client selection logic in
    /// Database where configuration is set.
    pub(crate) fn new(config: ConnectionConfig) -> crate::error::Result<Self> {
        // Create tokio runtime
        let runtime = tokio::runtime::Runtime::new().map_err(|e| {
            DatabricksErrorHelper::io().message(format!("Failed to create async runtime: {}", e))
        })?;

        // Create session using the client provided by Database
        let session_info = runtime.block_on(config.client.create_session(
            config.catalog.as_deref(),
            config.schema.as_deref(),
            HashMap::new(),
        ))?;

        debug!("Created session: {}", session_info.session_id);

        Ok(Self {
            host: config.host,
            warehouse_id: config.warehouse_id,
            client: config.client,
            http_client: config.http_client,
            session_id: session_info.session_id,
            cloudfetch_config: config.cloudfetch_config,
            runtime,
        })
    }

    /// Returns the Databricks host URL.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the warehouse ID.
    pub fn warehouse_id(&self) -> &str {
        &self.warehouse_id
    }

    /// Returns the session ID.
    pub fn session_id(&self) -> &str {
        &self.session_id
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
        let reader_factory = ResultReaderFactory::new(
            self.client.clone(),
            self.http_client.clone(),
            self.cloudfetch_config.clone(),
            self.runtime.handle().clone(),
        );

        Ok(Statement::new(
            self.client.clone(),
            self.session_id.clone(),
            reader_factory,
            self.runtime.handle().clone(),
        ))
    }

    fn cancel(&mut self) -> Result<()> {
        // TODO: Implement connection-level cancellation
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

impl Drop for Connection {
    fn drop(&mut self) {
        // Clean up session on connection close
        debug!("Closing session: {}", self.session_id);
        let _ = self
            .runtime
            .block_on(self.client.delete_session(&self.session_id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Full connection tests require mock DatabricksClient
    // Integration tests should be added in a separate test module
}
