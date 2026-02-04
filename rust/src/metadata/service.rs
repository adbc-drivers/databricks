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

//! Metadata service for executing metadata queries via SEA.
//!
//! This module provides the [`MetadataService`] struct that executes SQL-based
//! metadata queries (SHOW CATALOGS, SHOW SCHEMAS, etc.) via the SEA client
//! and parses the Arrow results into structured metadata types.
//!
//! ## Example
//!
//! ```ignore
//! use databricks_adbc::metadata::MetadataService;
//!
//! let service = MetadataService::new(client, session_id, runtime);
//! let catalogs = service.list_catalogs()?;
//! let schemas = service.list_schemas(Some("main"), Some("default%"))?;
//! ```

use crate::client::DatabricksClient;
use crate::error::{DatabricksErrorHelper, Result};
use crate::metadata::sql::SqlCommandBuilder;
use crate::metadata::types::{CatalogInfo, SchemaInfo};
use crate::types::sea::{ExecuteParams, StatementState};
use arrow_array::{Array, RecordBatch, StringArray};
use driverbase::error::ErrorHelper;
use std::sync::Arc;
use tracing::debug;

/// Service for executing metadata queries via SEA.
///
/// This service wraps the Databricks client and provides methods for
/// querying database metadata (catalogs, schemas, tables, columns).
/// It executes SQL commands like `SHOW CATALOGS` and `SHOW SCHEMAS`
/// and parses the Arrow results into structured metadata types.
#[derive(Debug)]
pub struct MetadataService {
    /// The Databricks client for executing SQL statements.
    client: Arc<dyn DatabricksClient>,
    /// The session ID for the current connection.
    session_id: String,
    /// Runtime handle for async execution.
    runtime: tokio::runtime::Handle,
}

impl MetadataService {
    /// Create a new MetadataService.
    ///
    /// # Arguments
    ///
    /// * `client` - The Databricks client for executing SQL statements.
    /// * `session_id` - The session ID for the current connection.
    /// * `runtime` - Runtime handle for async execution.
    pub fn new(
        client: Arc<dyn DatabricksClient>,
        session_id: String,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        Self {
            client,
            session_id,
            runtime,
        }
    }

    /// List all catalogs.
    ///
    /// Executes `SHOW CATALOGS` and returns the list of available catalogs.
    ///
    /// # Returns
    ///
    /// A vector of [`CatalogInfo`] containing catalog names.
    ///
    /// # Errors
    ///
    /// Returns an error if the query execution fails.
    pub fn list_catalogs(&self) -> Result<Vec<CatalogInfo>> {
        let sql = SqlCommandBuilder::new().build_show_catalogs();
        debug!("Executing metadata query: {}", sql);

        let batches = self.execute_metadata_query(&sql)?;

        let mut catalogs = Vec::new();
        for batch in batches {
            // SHOW CATALOGS returns a single column "catalog" with catalog names
            let catalog_names = self.extract_string_column(&batch, 0)?;
            for name in catalog_names {
                catalogs.push(CatalogInfo { catalog_name: name });
            }
        }

        debug!("Found {} catalogs", catalogs.len());
        Ok(catalogs)
    }

    /// List schemas, optionally filtered by catalog and pattern.
    ///
    /// Executes `SHOW SCHEMAS IN {catalog}` or `SHOW SCHEMAS IN ALL CATALOGS`
    /// and returns the list of schemas.
    ///
    /// # Arguments
    ///
    /// * `catalog` - Optional catalog to filter by. If None or "%", queries all catalogs.
    /// * `schema_pattern` - Optional LIKE pattern for schema names.
    ///
    /// # Returns
    ///
    /// A vector of [`SchemaInfo`] containing catalog and schema names.
    ///
    /// # Errors
    ///
    /// Returns an error if the query execution fails.
    pub fn list_schemas(
        &self,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
    ) -> Result<Vec<SchemaInfo>> {
        let sql = SqlCommandBuilder::new()
            .with_catalog(catalog)
            .with_schema_pattern(schema_pattern)
            .build_show_schemas();
        debug!("Executing metadata query: {}", sql);

        let batches = self.execute_metadata_query(&sql)?;

        let mut schemas = Vec::new();
        for batch in batches {
            // SHOW SCHEMAS IN ALL CATALOGS returns: catalog, databaseName
            // SHOW SCHEMAS IN `catalog` returns: databaseName (single column)
            let is_all_catalogs = catalog.is_none() || catalog == Some("") || catalog == Some("%");

            if is_all_catalogs && batch.num_columns() >= 2 {
                // All catalogs: first column is catalog, second is schema
                let catalog_names = self.extract_string_column(&batch, 0)?;
                let schema_names = self.extract_string_column(&batch, 1)?;

                for (cat_name, schema_name) in catalog_names.into_iter().zip(schema_names) {
                    schemas.push(SchemaInfo {
                        catalog_name: cat_name,
                        schema_name,
                    });
                }
            } else {
                // Single catalog: only schema names returned
                let schema_names = self.extract_string_column(&batch, 0)?;
                let cat_name = catalog.unwrap_or("").to_string();

                for schema_name in schema_names {
                    schemas.push(SchemaInfo {
                        catalog_name: cat_name.clone(),
                        schema_name,
                    });
                }
            }
        }

        debug!("Found {} schemas", schemas.len());
        Ok(schemas)
    }

    /// Execute a metadata SQL query and return the result batches.
    ///
    /// This is a helper method that handles:
    /// - Executing the SQL statement via the SEA client
    /// - Polling for completion if the statement is async
    /// - Extracting inline data (metadata queries typically return small results)
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL query to execute.
    ///
    /// # Returns
    ///
    /// A vector of [`RecordBatch`] containing the query results.
    fn execute_metadata_query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let params = ExecuteParams {
            wait_timeout: Some("30s".to_string()),
            on_wait_timeout: Some("CONTINUE".to_string()),
            ..Default::default()
        };

        // Execute the statement
        let response = self.runtime.block_on(async {
            self.client
                .execute_statement(&self.session_id, sql, &params)
                .await
        })?;

        // Check for execution errors
        match response.status.state {
            StatementState::Failed => {
                let error_msg = response
                    .status
                    .error
                    .as_ref()
                    .and_then(|e| e.message.as_deref())
                    .unwrap_or("Unknown error");
                return Err(DatabricksErrorHelper::io()
                    .message(format!("Metadata query failed: {}", error_msg)));
            }
            StatementState::Canceled => {
                return Err(DatabricksErrorHelper::io().message("Metadata query was canceled"));
            }
            StatementState::Pending | StatementState::Running => {
                // Poll for completion
                let mut current_response = response;
                loop {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    current_response = self.runtime.block_on(async {
                        self.client
                            .get_statement_status(&current_response.statement_id)
                            .await
                    })?;

                    match current_response.status.state {
                        StatementState::Succeeded => break,
                        StatementState::Pending | StatementState::Running => continue,
                        StatementState::Failed => {
                            let error_msg = current_response
                                .status
                                .error
                                .as_ref()
                                .and_then(|e| e.message.as_deref())
                                .unwrap_or("Unknown error");
                            return Err(DatabricksErrorHelper::io()
                                .message(format!("Metadata query failed: {}", error_msg)));
                        }
                        StatementState::Canceled => {
                            return Err(
                                DatabricksErrorHelper::io().message("Metadata query was canceled")
                            );
                        }
                        StatementState::Closed => {
                            return Err(DatabricksErrorHelper::io().message("Statement was closed"));
                        }
                    }
                }
                // Continue with the final response
                return self.extract_result_batches(&current_response.statement_id);
            }
            StatementState::Succeeded => {
                // Continue to extract results
            }
            StatementState::Closed => {
                return Err(DatabricksErrorHelper::io().message("Statement was closed"));
            }
        }

        self.extract_result_batches(&response.statement_id)
    }

    /// Extract result batches from a completed statement.
    ///
    /// For metadata queries, results are typically small and returned inline.
    /// This method handles extracting the inline data and converting it to Arrow RecordBatches.
    fn extract_result_batches(&self, statement_id: &str) -> Result<Vec<RecordBatch>> {
        // Get the final response with results
        let response = self
            .runtime
            .block_on(async { self.client.get_statement_status(statement_id).await })?;

        // Check if we have a manifest with schema information
        let manifest = response
            .manifest
            .ok_or_else(|| DatabricksErrorHelper::io().message("No result manifest available"))?;

        // For metadata queries, we expect inline data or very small results
        // If external links are present, we need to fetch them
        if let Some(ref result) = response.result {
            if let Some(ref external_links) = result.external_links {
                if !external_links.is_empty() {
                    // For metadata queries, we typically don't expect CloudFetch
                    // But we should handle it if it happens
                    return Err(DatabricksErrorHelper::not_implemented()
                        .message("CloudFetch for metadata queries not yet supported"));
                }
            }
        }

        // Build schema from manifest
        let schema = self.build_schema_from_manifest(&manifest)?;

        // Check if there are any rows
        if manifest.total_row_count == Some(0) {
            // Empty result set - return empty vector
            return Ok(vec![]);
        }

        // For inline results, we need to parse the data_array
        // The SEA API can return results in data_array format for small results
        if let Some(ref result) = response.result {
            if let Some(ref data_array) = result.data_array {
                return self.convert_data_array_to_batches(data_array, &schema, &manifest);
            }
        }

        // If no inline data and no external links, return empty
        Ok(vec![])
    }

    /// Build an Arrow schema from the result manifest.
    fn build_schema_from_manifest(
        &self,
        manifest: &crate::types::sea::ResultManifest,
    ) -> Result<arrow_schema::Schema> {
        let fields: Vec<arrow_schema::Field> = manifest
            .schema
            .columns
            .iter()
            .map(|col| {
                let data_type = self.map_databricks_type(&col.type_name);
                arrow_schema::Field::new(&col.name, data_type, true)
            })
            .collect();

        Ok(arrow_schema::Schema::new(fields))
    }

    /// Map Databricks SQL type names to Arrow DataTypes.
    fn map_databricks_type(&self, type_name: &str) -> arrow_schema::DataType {
        match type_name.to_uppercase().as_str() {
            "BOOLEAN" => arrow_schema::DataType::Boolean,
            "BYTE" | "TINYINT" => arrow_schema::DataType::Int8,
            "SHORT" | "SMALLINT" => arrow_schema::DataType::Int16,
            "INT" | "INTEGER" => arrow_schema::DataType::Int32,
            "LONG" | "BIGINT" => arrow_schema::DataType::Int64,
            "FLOAT" | "REAL" => arrow_schema::DataType::Float32,
            "DOUBLE" => arrow_schema::DataType::Float64,
            "STRING" => arrow_schema::DataType::Utf8,
            "BINARY" => arrow_schema::DataType::Binary,
            "DATE" => arrow_schema::DataType::Date32,
            "TIMESTAMP" | "TIMESTAMP_NTZ" => {
                arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
            }
            _ => arrow_schema::DataType::Utf8, // Default fallback
        }
    }

    /// Convert inline data_array to Arrow RecordBatches.
    ///
    /// The data_array is a Vec<Vec<String>> where each inner Vec is a row.
    fn convert_data_array_to_batches(
        &self,
        data_array: &[Vec<String>],
        schema: &arrow_schema::Schema,
        _manifest: &crate::types::sea::ResultManifest,
    ) -> Result<Vec<RecordBatch>> {
        if data_array.is_empty() {
            return Ok(vec![]);
        }

        let num_columns = schema.fields().len();
        let num_rows = data_array.len();

        // Transpose row-oriented data to column-oriented
        let mut columns: Vec<Vec<Option<&str>>> = vec![Vec::with_capacity(num_rows); num_columns];

        for row in data_array {
            for (col_idx, value) in row.iter().enumerate() {
                if col_idx < num_columns {
                    // Treat empty strings as None for nullable columns
                    if value.is_empty() {
                        columns[col_idx].push(None);
                    } else {
                        columns[col_idx].push(Some(value.as_str()));
                    }
                }
            }
        }

        // Build Arrow arrays - for metadata queries, all columns are typically strings
        let arrays: Vec<Arc<dyn Array>> = columns
            .into_iter()
            .map(|col| {
                let array = StringArray::from(col);
                Arc::new(array) as Arc<dyn Array>
            })
            .collect();

        let batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays).map_err(|e| {
            DatabricksErrorHelper::io().message(format!("Failed to create RecordBatch: {}", e))
        })?;

        Ok(vec![batch])
    }

    /// Extract string values from a column in a RecordBatch.
    fn extract_string_column(
        &self,
        batch: &RecordBatch,
        column_index: usize,
    ) -> Result<Vec<String>> {
        if column_index >= batch.num_columns() {
            return Err(DatabricksErrorHelper::invalid_argument()
                .message(format!("Column index {} out of bounds", column_index)));
        }

        let column = batch.column(column_index);
        let string_array = column
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DatabricksErrorHelper::io().message(format!(
                    "Expected string column at index {}, got {:?}",
                    column_index,
                    column.data_type()
                ))
            })?;

        let mut values = Vec::with_capacity(string_array.len());
        for i in 0..string_array.len() {
            if string_array.is_null(i) {
                values.push(String::new());
            } else {
                values.push(string_array.value(i).to_string());
            }
        }

        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{ChunkLinkFetchResult, ExecuteResponse, ExecuteResultData, SessionInfo};
    use crate::types::sea::{ColumnInfo, ResultManifest, ResultSchema, StatementStatus};
    use async_trait::async_trait;
    use std::collections::HashMap;

    /// Mock client for testing MetadataService.
    #[derive(Debug)]
    struct MockClient {
        /// The responses to return for execute_statement calls.
        responses: std::sync::Mutex<Vec<ExecuteResponse>>,
    }

    impl MockClient {
        fn new(responses: Vec<ExecuteResponse>) -> Self {
            Self {
                responses: std::sync::Mutex::new(responses),
            }
        }

        fn with_catalogs(catalogs: Vec<&str>) -> Self {
            let data_array: Vec<Vec<String>> =
                catalogs.iter().map(|c| vec![c.to_string()]).collect();

            let response = ExecuteResponse {
                statement_id: "stmt-1".to_string(),
                status: StatementStatus {
                    state: StatementState::Succeeded,
                    error: None,
                },
                manifest: Some(ResultManifest {
                    format: "JSON_ARRAY".to_string(),
                    schema: ResultSchema {
                        column_count: 1,
                        columns: vec![ColumnInfo {
                            name: "catalog".to_string(),
                            type_name: "STRING".to_string(),
                            type_text: "STRING".to_string(),
                            position: 0,
                        }],
                    },
                    total_chunk_count: Some(1),
                    total_row_count: Some(catalogs.len() as i64),
                    total_byte_count: None,
                    truncated: false,
                    chunks: None,
                    result_compression: None,
                }),
                result: Some(ExecuteResultData {
                    chunk_index: Some(0),
                    row_offset: Some(0),
                    row_count: Some(catalogs.len() as i64),
                    byte_count: None,
                    next_chunk_index: None,
                    next_chunk_internal_link: None,
                    external_links: None,
                    has_inline_data: true,
                    data_array: Some(data_array.clone()),
                }),
            };

            // Create a second response for get_statement_status with inline data
            let status_response = ExecuteResponse {
                statement_id: "stmt-1".to_string(),
                status: StatementStatus {
                    state: StatementState::Succeeded,
                    error: None,
                },
                manifest: Some(ResultManifest {
                    format: "JSON_ARRAY".to_string(),
                    schema: ResultSchema {
                        column_count: 1,
                        columns: vec![ColumnInfo {
                            name: "catalog".to_string(),
                            type_name: "STRING".to_string(),
                            type_text: "STRING".to_string(),
                            position: 0,
                        }],
                    },
                    total_chunk_count: Some(1),
                    total_row_count: Some(catalogs.len() as i64),
                    total_byte_count: None,
                    truncated: false,
                    chunks: None,
                    result_compression: None,
                }),
                result: Some(ExecuteResultData {
                    chunk_index: Some(0),
                    row_offset: Some(0),
                    row_count: Some(catalogs.len() as i64),
                    byte_count: None,
                    next_chunk_index: None,
                    next_chunk_internal_link: None,
                    external_links: None,
                    has_inline_data: true,
                    data_array: Some(data_array),
                }),
            };

            Self::new(vec![response, status_response])
        }

        fn with_schemas(schemas: Vec<(&str, &str)>) -> Self {
            let data_array: Vec<Vec<String>> = schemas
                .iter()
                .map(|(cat, schema)| vec![cat.to_string(), schema.to_string()])
                .collect();

            let response = ExecuteResponse {
                statement_id: "stmt-1".to_string(),
                status: StatementStatus {
                    state: StatementState::Succeeded,
                    error: None,
                },
                manifest: Some(ResultManifest {
                    format: "JSON_ARRAY".to_string(),
                    schema: ResultSchema {
                        column_count: 2,
                        columns: vec![
                            ColumnInfo {
                                name: "catalog".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 0,
                            },
                            ColumnInfo {
                                name: "databaseName".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 1,
                            },
                        ],
                    },
                    total_chunk_count: Some(1),
                    total_row_count: Some(schemas.len() as i64),
                    total_byte_count: None,
                    truncated: false,
                    chunks: None,
                    result_compression: None,
                }),
                result: Some(ExecuteResultData {
                    chunk_index: Some(0),
                    row_offset: Some(0),
                    row_count: Some(schemas.len() as i64),
                    byte_count: None,
                    next_chunk_index: None,
                    next_chunk_internal_link: None,
                    external_links: None,
                    has_inline_data: true,
                    data_array: Some(data_array.clone()),
                }),
            };

            let status_response = ExecuteResponse {
                statement_id: "stmt-1".to_string(),
                status: StatementStatus {
                    state: StatementState::Succeeded,
                    error: None,
                },
                manifest: Some(ResultManifest {
                    format: "JSON_ARRAY".to_string(),
                    schema: ResultSchema {
                        column_count: 2,
                        columns: vec![
                            ColumnInfo {
                                name: "catalog".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 0,
                            },
                            ColumnInfo {
                                name: "databaseName".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 1,
                            },
                        ],
                    },
                    total_chunk_count: Some(1),
                    total_row_count: Some(schemas.len() as i64),
                    total_byte_count: None,
                    truncated: false,
                    chunks: None,
                    result_compression: None,
                }),
                result: Some(ExecuteResultData {
                    chunk_index: Some(0),
                    row_offset: Some(0),
                    row_count: Some(schemas.len() as i64),
                    byte_count: None,
                    next_chunk_index: None,
                    next_chunk_internal_link: None,
                    external_links: None,
                    has_inline_data: true,
                    data_array: Some(data_array),
                }),
            };

            Self::new(vec![response, status_response])
        }
    }

    #[async_trait]
    impl DatabricksClient for MockClient {
        async fn create_session(
            &self,
            _catalog: Option<&str>,
            _schema: Option<&str>,
            _session_config: HashMap<String, String>,
        ) -> Result<SessionInfo> {
            Ok(SessionInfo {
                session_id: "test-session".to_string(),
            })
        }

        async fn delete_session(&self, _session_id: &str) -> Result<()> {
            Ok(())
        }

        async fn execute_statement(
            &self,
            _session_id: &str,
            _sql: &str,
            _params: &ExecuteParams,
        ) -> Result<ExecuteResponse> {
            let mut responses = self.responses.lock().unwrap();
            if responses.is_empty() {
                Err(DatabricksErrorHelper::io().message("No more mock responses"))
            } else {
                Ok(responses.remove(0))
            }
        }

        async fn get_statement_status(&self, _statement_id: &str) -> Result<ExecuteResponse> {
            let mut responses = self.responses.lock().unwrap();
            if responses.is_empty() {
                Err(DatabricksErrorHelper::io().message("No more mock responses"))
            } else {
                Ok(responses.remove(0))
            }
        }

        async fn get_result_chunks(
            &self,
            _statement_id: &str,
            _chunk_index: i64,
            _row_offset: i64,
        ) -> Result<ChunkLinkFetchResult> {
            Ok(ChunkLinkFetchResult::end_of_stream())
        }

        async fn cancel_statement(&self, _statement_id: &str) -> Result<()> {
            Ok(())
        }

        async fn close_statement(&self, _statement_id: &str) -> Result<()> {
            Ok(())
        }
    }

    fn create_test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn test_metadata_service_new() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::new(vec![]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        assert_eq!(service.session_id, "session-1");
    }

    #[test]
    fn test_extract_string_column() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::new(vec![]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        // Create a simple RecordBatch with string data
        let array = StringArray::from(vec!["main", "catalog1", "catalog2"]);
        let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "catalog",
            arrow_schema::DataType::Utf8,
            true,
        )]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        let values = service.extract_string_column(&batch, 0).unwrap();
        assert_eq!(values, vec!["main", "catalog1", "catalog2"]);
    }

    #[test]
    fn test_extract_string_column_with_nulls() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::new(vec![]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        // Create a RecordBatch with null values
        let array = StringArray::from(vec![Some("main"), None, Some("catalog2")]);
        let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "catalog",
            arrow_schema::DataType::Utf8,
            true,
        )]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        let values = service.extract_string_column(&batch, 0).unwrap();
        assert_eq!(values, vec!["main", "", "catalog2"]);
    }

    #[test]
    fn test_extract_string_column_out_of_bounds() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::new(vec![]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        let array = StringArray::from(vec!["main"]);
        let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "catalog",
            arrow_schema::DataType::Utf8,
            true,
        )]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        let result = service.extract_string_column(&batch, 5);
        assert!(result.is_err());
    }

    #[test]
    fn test_convert_data_array_to_batches() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::new(vec![]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        let data_array = vec![
            vec!["main".to_string()],
            vec!["catalog1".to_string()],
            vec!["catalog2".to_string()],
        ];

        let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "catalog",
            arrow_schema::DataType::Utf8,
            true,
        )]);

        let manifest = ResultManifest {
            format: "JSON_ARRAY".to_string(),
            schema: ResultSchema {
                column_count: 1,
                columns: vec![ColumnInfo {
                    name: "catalog".to_string(),
                    type_name: "STRING".to_string(),
                    type_text: "STRING".to_string(),
                    position: 0,
                }],
            },
            total_chunk_count: Some(1),
            total_row_count: Some(3),
            total_byte_count: None,
            truncated: false,
            chunks: None,
            result_compression: None,
        };

        let batches = service
            .convert_data_array_to_batches(&data_array, &schema, &manifest)
            .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[0].num_columns(), 1);

        let values = service.extract_string_column(&batches[0], 0).unwrap();
        assert_eq!(values, vec!["main", "catalog1", "catalog2"]);
    }

    #[test]
    fn test_convert_data_array_empty() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::new(vec![]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        let data_array: Vec<Vec<String>> = vec![];

        let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "catalog",
            arrow_schema::DataType::Utf8,
            true,
        )]);

        let manifest = ResultManifest {
            format: "JSON_ARRAY".to_string(),
            schema: ResultSchema {
                column_count: 1,
                columns: vec![ColumnInfo {
                    name: "catalog".to_string(),
                    type_name: "STRING".to_string(),
                    type_text: "STRING".to_string(),
                    position: 0,
                }],
            },
            total_chunk_count: Some(0),
            total_row_count: Some(0),
            total_byte_count: None,
            truncated: false,
            chunks: None,
            result_compression: None,
        };

        let batches = service
            .convert_data_array_to_batches(&data_array, &schema, &manifest)
            .unwrap();

        assert!(batches.is_empty());
    }

    #[test]
    fn test_map_databricks_type() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::new(vec![]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        assert_eq!(
            service.map_databricks_type("BOOLEAN"),
            arrow_schema::DataType::Boolean
        );
        assert_eq!(
            service.map_databricks_type("STRING"),
            arrow_schema::DataType::Utf8
        );
        assert_eq!(
            service.map_databricks_type("INT"),
            arrow_schema::DataType::Int32
        );
        assert_eq!(
            service.map_databricks_type("BIGINT"),
            arrow_schema::DataType::Int64
        );
        // Unknown types should fall back to Utf8
        assert_eq!(
            service.map_databricks_type("UNKNOWN"),
            arrow_schema::DataType::Utf8
        );
    }

    #[test]
    fn test_build_schema_from_manifest() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::new(vec![]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        let manifest = ResultManifest {
            format: "JSON_ARRAY".to_string(),
            schema: ResultSchema {
                column_count: 2,
                columns: vec![
                    ColumnInfo {
                        name: "catalog".to_string(),
                        type_name: "STRING".to_string(),
                        type_text: "STRING".to_string(),
                        position: 0,
                    },
                    ColumnInfo {
                        name: "count".to_string(),
                        type_name: "BIGINT".to_string(),
                        type_text: "BIGINT".to_string(),
                        position: 1,
                    },
                ],
            },
            total_chunk_count: Some(1),
            total_row_count: Some(10),
            total_byte_count: None,
            truncated: false,
            chunks: None,
            result_compression: None,
        };

        let schema = service.build_schema_from_manifest(&manifest).unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "catalog");
        assert_eq!(schema.field(0).data_type(), &arrow_schema::DataType::Utf8);
        assert_eq!(schema.field(1).name(), "count");
        assert_eq!(schema.field(1).data_type(), &arrow_schema::DataType::Int64);
    }

    #[test]
    fn test_list_catalogs_with_mock() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::with_catalogs(vec![
            "main",
            "hive_metastore",
            "system",
        ]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        let catalogs = service.list_catalogs().unwrap();
        assert_eq!(catalogs.len(), 3);
        assert_eq!(catalogs[0].catalog_name, "main");
        assert_eq!(catalogs[1].catalog_name, "hive_metastore");
        assert_eq!(catalogs[2].catalog_name, "system");
    }

    #[test]
    fn test_list_schemas_with_mock() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::with_schemas(vec![
            ("main", "default"),
            ("main", "information_schema"),
            ("hive_metastore", "public"),
        ]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        let schemas = service.list_schemas(None, None).unwrap();
        assert_eq!(schemas.len(), 3);
        assert_eq!(schemas[0].catalog_name, "main");
        assert_eq!(schemas[0].schema_name, "default");
        assert_eq!(schemas[1].catalog_name, "main");
        assert_eq!(schemas[1].schema_name, "information_schema");
        assert_eq!(schemas[2].catalog_name, "hive_metastore");
        assert_eq!(schemas[2].schema_name, "public");
    }

    #[test]
    fn test_list_catalogs_empty() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::with_catalogs(vec![]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        let catalogs = service.list_catalogs().unwrap();
        assert!(catalogs.is_empty());
    }

    #[test]
    fn test_list_schemas_empty() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::with_schemas(vec![]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        let schemas = service.list_schemas(None, None).unwrap();
        assert!(schemas.is_empty());
    }
}
