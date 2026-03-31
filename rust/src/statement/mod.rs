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
//!
//! Supports prepared statements with parameter binding via the SEA API's
//! positional parameters. See `params` submodule for conversion utilities.

pub mod params;

use crate::client::DatabricksClient;
use crate::error::DatabricksErrorHelper;
use crate::reader::ResultReaderAdapter;
use crate::types::sea::ExecuteParams;
use adbc_core::error::Result;
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::Optionable;
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{DataType, Field, Schema};
use driverbase::error::ErrorHelper;
use params::{
    count_parameter_markers, describe_result_to_schema, record_batch_to_sea_parameters,
    replace_parameter_markers_with_literals,
};
use std::sync::Arc;
use tokio::runtime::Handle as RuntimeHandle;
use tracing::debug;

/// Represents a SQL statement that can be executed against Databricks.
///
/// A Statement is created from a Connection and is used to execute SQL
/// queries and retrieve results. Supports prepared statements with
/// parameter binding.
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
    /// Whether the statement has been prepared.
    is_prepared: bool,
    /// Number of parameter markers (?) found in the SQL query (set by prepare()).
    parameter_count: Option<usize>,
    /// Bound parameters (single row) from bind().
    bound_params: Option<RecordBatch>,
    /// Bound parameter stream from bind_stream().
    bound_stream: Option<Box<dyn RecordBatchReader + Send>>,
}

// Statement holds a Box<dyn RecordBatchReader + Send> which is Send but
// the compiler can't verify it automatically through the trait object.
// The field is only accessed from the thread that owns the Statement.
unsafe impl Send for Statement {}

impl std::fmt::Debug for Statement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Statement")
            .field("query", &self.query)
            .field("session_id", &self.session_id)
            .field("current_statement_id", &self.current_statement_id)
            .field("is_prepared", &self.is_prepared)
            .field("parameter_count", &self.parameter_count)
            .field("has_bound_params", &self.bound_params.is_some())
            .field("has_bound_stream", &self.bound_stream.is_some())
            .finish()
    }
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
            is_prepared: false,
            parameter_count: None,
            bound_params: None,
            bound_stream: None,
        }
    }

    /// Returns the current SQL query.
    pub fn sql_query(&self) -> Option<&str> {
        self.query.as_deref()
    }

    /// Execute a single query with the given parameters and return the result.
    fn execute_single(
        &mut self,
        query: &str,
        exec_params: &ExecuteParams,
    ) -> Result<ResultReaderAdapter> {
        let result = self
            .runtime_handle
            .block_on(
                self.client
                    .execute_statement(&self.session_id, query, exec_params),
            )
            .map_err(|e| e.to_adbc())?;

        self.current_statement_id = Some(result.statement_id);

        ResultReaderAdapter::new(result.reader, result.manifest.as_ref()).map_err(|e| e.to_adbc())
    }

    /// Execute a parameterized query once per row in the bound stream.
    ///
    /// Each row in the stream becomes a separate SEA API call with its own
    /// set of parameters. Results from all executions are concatenated.
    fn execute_with_stream(
        &mut self,
        query: &str,
        stream: Box<dyn RecordBatchReader + Send>,
    ) -> Result<impl RecordBatchReader + Send> {
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        let mut result_schema: Option<Arc<Schema>> = None;

        for batch_result in stream {
            let batch = batch_result.map_err(|e| {
                DatabricksErrorHelper::io()
                    .message(format!("Error reading parameter stream: {e}"))
                    .to_adbc()
            })?;

            // Execute once per row in this batch
            for row_idx in 0..batch.num_rows() {
                let row_batch = batch.slice(row_idx, 1);
                let parameters =
                    record_batch_to_sea_parameters(&row_batch).map_err(|e| e.to_adbc())?;

                let exec_params = ExecuteParams {
                    parameters,
                    ..ExecuteParams::default()
                };

                let reader = self.execute_single(query, &exec_params)?;

                if result_schema.is_none() {
                    result_schema = Some(reader.schema());
                }

                for rb in reader {
                    let rb = rb.map_err(|e| {
                        DatabricksErrorHelper::io()
                            .message(format!("Error reading results: {e}"))
                            .to_adbc()
                    })?;
                    all_batches.push(rb);
                }
            }
        }

        let schema = result_schema.unwrap_or_else(|| Arc::new(Schema::empty()));
        Ok(arrow_array::RecordBatchIterator::new(
            all_batches.into_iter().map(Ok),
            schema,
        ))
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
        // Setting a new query resets prepared state
        self.is_prepared = false;
        self.parameter_count = None;
        self.bound_params = None;
        self.bound_stream = None;
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Err(DatabricksErrorHelper::not_implemented()
            .message("Substrait plans")
            .to_adbc())
    }

    fn prepare(&mut self) -> Result<()> {
        let query = self.query.as_ref().ok_or_else(|| {
            DatabricksErrorHelper::invalid_state()
                .message("No query set")
                .to_adbc()
        })?;

        self.parameter_count = Some(count_parameter_markers(query));
        self.is_prepared = true;

        // Clear any previously bound parameters
        self.bound_params = None;
        self.bound_stream = None;

        debug!(
            "Prepared statement with {} parameter markers",
            self.parameter_count.unwrap()
        );

        Ok(())
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        // If parameters are bound, return the bound schema
        if let Some(ref batch) = self.bound_params {
            return Ok((*batch.schema()).clone());
        }
        if let Some(ref stream) = self.bound_stream {
            return Ok((*stream.schema()).clone());
        }

        // If prepared but not bound, return a schema with Null-typed fields
        // (parameter count is known, but types are not until bind)
        if let Some(count) = self.parameter_count {
            let fields: Vec<Field> = (0..count)
                .map(|i| Field::new(format!("{i}"), DataType::Null, true))
                .collect();
            return Ok(Schema::new(fields));
        }

        Err(DatabricksErrorHelper::invalid_state()
            .message("Statement not prepared and no parameters bound")
            .to_adbc())
    }

    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        // Validate single row
        if batch.num_rows() != 1 {
            return Err(DatabricksErrorHelper::invalid_argument()
                .message(format!(
                    "bind() expects a single-row RecordBatch, got {} rows",
                    batch.num_rows()
                ))
                .to_adbc());
        }

        // Validate column count matches parameter markers if prepared
        if let Some(expected) = self.parameter_count {
            if batch.num_columns() != expected {
                return Err(DatabricksErrorHelper::invalid_argument()
                    .message(format!(
                        "Expected {expected} parameters but got {} columns",
                        batch.num_columns()
                    ))
                    .to_adbc());
            }
        }

        self.bound_params = Some(batch);
        self.bound_stream = None; // bind() clears any prior bind_stream()
        Ok(())
    }

    fn bind_stream(&mut self, stream: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        // Validate column count matches parameter markers if prepared
        if let Some(expected) = self.parameter_count {
            if stream.schema().fields().len() != expected {
                return Err(DatabricksErrorHelper::invalid_argument()
                    .message(format!(
                        "Expected {expected} parameters but got {} columns in stream schema",
                        stream.schema().fields().len()
                    ))
                    .to_adbc());
            }
        }

        self.bound_stream = Some(stream);
        self.bound_params = None; // bind_stream() clears any prior bind()
        Ok(())
    }

    fn execute(&mut self) -> Result<impl RecordBatchReader + Send> {
        let query = self.query.as_ref().ok_or_else(|| {
            DatabricksErrorHelper::invalid_state()
                .message("No query set")
                .to_adbc()
        })?;
        let query = query.clone();

        debug!("Executing query: {}", query);

        // Handle bind_stream: execute once per row in the stream
        if let Some(stream) = self.bound_stream.take() {
            let iter_reader = self.execute_with_stream(&query, stream)?;
            // Collect into batches so both branches return the same type
            let schema = iter_reader.schema();
            let batches: Vec<RecordBatch> = iter_reader
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| {
                    DatabricksErrorHelper::io()
                        .message(format!("Error collecting stream results: {e}"))
                        .to_adbc()
                })?;
            return Ok(ResultReaderAdapter::from_batches(schema, batches));
        }

        // Convert bound parameters (if any) to SEA format
        let parameters = match self.bound_params.take() {
            Some(batch) => record_batch_to_sea_parameters(&batch).map_err(|e| e.to_adbc())?,
            None => vec![],
        };

        let exec_params = ExecuteParams {
            parameters,
            ..ExecuteParams::default()
        };

        self.execute_single(&query, &exec_params)
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
        let query = self.query.as_ref().ok_or_else(|| {
            DatabricksErrorHelper::invalid_state()
                .message("No query set")
                .to_adbc()
        })?;

        // Use DESCRIBE QUERY to get the result schema without executing.
        // Replace ? markers with '?' to avoid parse errors on the server.
        let safe_query = replace_parameter_markers_with_literals(query);
        let describe_sql = format!("DESCRIBE QUERY {safe_query}");

        debug!("Getting schema via: {}", describe_sql);

        let result = self
            .runtime_handle
            .block_on(self.client.execute_statement(
                &self.session_id,
                &describe_sql,
                &ExecuteParams::default(),
            ))
            .map_err(|e| e.to_adbc())?;

        let reader = ResultReaderAdapter::new(result.reader, result.manifest.as_ref())
            .map_err(|e| e.to_adbc())?;

        describe_result_to_schema(reader).map_err(|e| e.to_adbc())
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
