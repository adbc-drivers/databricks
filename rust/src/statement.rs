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

use crate::client::DatabricksClient;
use crate::error::DatabricksErrorHelper;
use crate::reader::ResultReaderAdapter;
use crate::types::sea::ExecuteParams;
use adbc_core::error::Result;
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::Optionable;
use arrow_array::RecordBatchReader;
use arrow_schema::Schema;
use driverbase::error::ErrorHelper;
use std::sync::Arc;
use tokio::runtime::Handle as RuntimeHandle;
use tracing::debug;

/// Represents a SQL statement that can be executed against Databricks.
///
/// A Statement is created from a Connection and is used to execute SQL
/// queries and retrieve results.
#[derive(Debug)]
pub struct Statement {
    /// The SQL query to execute.
    query: Option<String>,
    /// Databricks client for executing queries.
    client: Arc<dyn DatabricksClient>,
    /// Session ID for this statement's connection.
    session_id: String,
    /// Tokio runtime handle for async operations.
    runtime_handle: RuntimeHandle,
    /// Current statement ID (set after execution).
    current_statement_id: Option<String>,
}

impl Statement {
    /// Creates a new Statement.
    pub(crate) fn new(
        client: Arc<dyn DatabricksClient>,
        session_id: String,
        runtime_handle: RuntimeHandle,
    ) -> Self {
        Self {
            query: None,
            client,
            session_id,
            runtime_handle,
            current_statement_id: None,
        }
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
        let query = self.query.as_ref().ok_or_else(|| {
            DatabricksErrorHelper::invalid_state()
                .message("No query set")
                .to_adbc()
        })?;

        debug!("Executing query: {}", query);

        // Execute statement via DatabricksClient (handles polling and reader creation)
        let result = self
            .runtime_handle
            .block_on(self.client.execute_statement(
                &self.session_id,
                query,
                &ExecuteParams::default(),
            ))
            .map_err(|e| e.to_adbc())?;

        // Store statement ID for cancellation/cleanup
        self.current_statement_id = Some(result.statement_id);

        // Wrap in adapter for RecordBatchReader trait
        ResultReaderAdapter::new(result.reader).map_err(|e| e.to_adbc())
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        // Execute and count affected rows
        let reader = self.execute()?;

        // For UPDATE/INSERT/DELETE, there are no result rows
        // but the manifest might have row count info
        // Drain the reader (shouldn't have data for DML)
        for _batch in reader {
            // Consume batches
        }

        // TODO: Extract affected row count from response manifest
        Ok(None)
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        // Execute and get schema from reader
        let reader = self.execute()?;
        Ok((*reader.schema()).clone())
    }

    fn execute_partitions(&mut self) -> Result<adbc_core::PartitionedResult> {
        Err(DatabricksErrorHelper::not_implemented()
            .message("execute_partitions")
            .to_adbc())
    }

    fn cancel(&mut self) -> Result<()> {
        if let Some(ref statement_id) = self.current_statement_id {
            debug!("Canceling statement: {}", statement_id);
            self.runtime_handle
                .block_on(self.client.cancel_statement(statement_id))
                .map_err(|e| e.to_adbc())?;
        }
        Ok(())
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        // Clean up statement resources
        if let Some(ref statement_id) = self.current_statement_id {
            let _ = self
                .runtime_handle
                .block_on(self.client.close_statement(statement_id));
        }
    }
}

#[cfg(test)]
mod tests {
    // Note: Full statement tests require mock DatabricksClient
    // Integration tests should be added in a separate test module
}
