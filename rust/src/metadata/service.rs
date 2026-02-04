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
use crate::metadata::types::{
    CatalogInfo, ColumnInfo, ForeignKeyInfo, PrimaryKeyInfo, SchemaInfo, TableInfo,
};
use crate::types::sea::{ExecuteParams, StatementState};
use arrow_array::{Array, RecordBatch, StringArray};
use driverbase::error::ErrorHelper;
use std::sync::Arc;
use tracing::debug;

/// Supported table types in Databricks.
/// From databricks-jdbc MetadataResultConstants.java
const TABLE_TYPES: &[&str] = &["SYSTEM TABLE", "TABLE", "VIEW", "METRIC_VIEW"];

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

    /// List tables, optionally filtered by catalog, schema, table pattern, and table types.
    ///
    /// Executes `SHOW TABLES IN CATALOG {catalog}` or `SHOW TABLES IN ALL CATALOGS`
    /// and returns the list of tables.
    ///
    /// # Arguments
    ///
    /// * `catalog` - Optional catalog to filter by. If None or "%", queries all catalogs.
    /// * `schema_pattern` - Optional LIKE pattern for schema names.
    /// * `table_pattern` - Optional LIKE pattern for table names.
    /// * `table_types` - Optional list of table types to filter by (e.g., ["TABLE", "VIEW"]).
    ///
    /// # Returns
    ///
    /// A vector of [`TableInfo`] containing table metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the query execution fails.
    pub fn list_tables(
        &self,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        table_pattern: Option<&str>,
        table_types: Option<&[&str]>,
    ) -> Result<Vec<TableInfo>> {
        let sql = SqlCommandBuilder::new()
            .with_catalog(catalog)
            .with_schema_pattern(schema_pattern)
            .with_table_pattern(table_pattern)
            .build_show_tables();
        debug!("Executing metadata query: {}", sql);

        let batches = self.execute_metadata_query(&sql)?;

        let mut tables = Vec::new();
        for batch in batches {
            // SHOW TABLES returns columns: catalog, database/schema, tableName, tableType, isTemporary
            // Column indices may vary based on query type, but we need at least 4 columns
            if batch.num_columns() < 4 {
                continue;
            }

            let catalog_names = self.extract_string_column(&batch, 0)?;
            let schema_names = self.extract_string_column(&batch, 1)?;
            let table_names = self.extract_string_column(&batch, 2)?;
            let table_type_values = self.extract_string_column(&batch, 3)?;

            for i in 0..catalog_names.len() {
                let table_type = self.normalize_table_type(&table_type_values[i]);

                // Filter by table_types if provided
                if let Some(types) = table_types {
                    if !types.is_empty()
                        && !types.iter().any(|t| t.eq_ignore_ascii_case(&table_type))
                    {
                        continue;
                    }
                }

                tables.push(TableInfo {
                    catalog_name: catalog_names[i].clone(),
                    schema_name: schema_names[i].clone(),
                    table_name: table_names[i].clone(),
                    table_type,
                    remarks: None, // SHOW TABLES doesn't return remarks
                });
            }
        }

        debug!("Found {} tables", tables.len());
        Ok(tables)
    }

    /// Normalizes table type strings to standard ADBC values.
    ///
    /// Databricks may return various forms like "MANAGED", "EXTERNAL", "VIEW", etc.
    /// This method normalizes them to standard ADBC table types.
    fn normalize_table_type(&self, table_type: &str) -> String {
        let upper = table_type.to_uppercase();
        match upper.as_str() {
            "MANAGED" | "EXTERNAL" | "TABLE" => "TABLE".to_string(),
            "VIEW" | "MATERIALIZED_VIEW" => "VIEW".to_string(),
            "SYSTEM TABLE" => "SYSTEM TABLE".to_string(),
            "METRIC_VIEW" => "METRIC_VIEW".to_string(),
            _ => upper, // Return as-is for unknown types
        }
    }

    /// List columns, optionally filtered by catalog, schema, table, and column patterns.
    ///
    /// Executes `SHOW COLUMNS IN CATALOG {catalog}` and returns detailed column metadata
    /// including all XDBC fields.
    ///
    /// # Arguments
    ///
    /// * `catalog` - Required catalog to query (SHOW COLUMNS requires a catalog).
    /// * `schema_pattern` - Optional LIKE pattern for schema names.
    /// * `table_pattern` - Optional LIKE pattern for table names.
    /// * `column_pattern` - Optional LIKE pattern for column names.
    ///
    /// # Returns
    ///
    /// A vector of [`ColumnInfo`] containing detailed column metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the query execution fails or if catalog is not provided.
    pub fn list_columns(
        &self,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        table_pattern: Option<&str>,
        column_pattern: Option<&str>,
    ) -> Result<Vec<ColumnInfo>> {
        let sql = SqlCommandBuilder::new()
            .with_catalog(catalog)
            .with_schema_pattern(schema_pattern)
            .with_table_pattern(table_pattern)
            .with_column_pattern(column_pattern)
            .build_show_columns()?;
        debug!("Executing metadata query: {}", sql);

        let batches = self.execute_metadata_query(&sql)?;

        let mut columns = Vec::new();
        for batch in batches {
            // SHOW COLUMNS returns: catalog, database/schema, tableName, columnName, dataType,
            // description, partition, comment, ... (varies by version)
            // We need at least 5 columns for basic info
            if batch.num_columns() < 5 {
                continue;
            }

            let catalog_names = self.extract_string_column(&batch, 0)?;
            let schema_names = self.extract_string_column(&batch, 1)?;
            let table_names = self.extract_string_column(&batch, 2)?;
            let column_names = self.extract_string_column(&batch, 3)?;
            let data_types = self.extract_string_column(&batch, 4)?;

            // Optional columns - try to extract if available
            let comments = if batch.num_columns() > 5 {
                self.extract_string_column(&batch, 5).ok()
            } else {
                None
            };

            for i in 0..catalog_names.len() {
                let type_name = data_types[i].clone();
                let (column_size, decimal_digits, num_prec_radix) =
                    self.parse_column_type_info(&type_name);

                // For SHOW COLUMNS, we assume columns are nullable by default
                // since the query doesn't return nullability info
                let nullable: i16 = 1; // Assume nullable
                let is_nullable = "YES".to_string();

                let remarks = comments.as_ref().and_then(|c| {
                    let comment = &c[i];
                    if comment.is_empty() {
                        None
                    } else {
                        Some(comment.clone())
                    }
                });

                columns.push(ColumnInfo {
                    catalog_name: catalog_names[i].clone(),
                    schema_name: schema_names[i].clone(),
                    table_name: table_names[i].clone(),
                    column_name: column_names[i].clone(),
                    ordinal_position: (i + 1) as i32, // 1-based position within this result
                    data_type: type_name.clone(),
                    type_name: type_name.clone(),
                    column_size,
                    decimal_digits,
                    num_prec_radix,
                    nullable,
                    remarks,
                    column_def: None, // Not provided by SHOW COLUMNS
                    is_nullable,
                    is_autoincrement: None,   // Not provided by SHOW COLUMNS
                    is_generatedcolumn: None, // Not provided by SHOW COLUMNS
                });
            }
        }

        // Recompute ordinal positions per table
        self.recompute_ordinal_positions(&mut columns);

        debug!("Found {} columns", columns.len());
        Ok(columns)
    }

    /// Recomputes ordinal positions to be correct per table.
    ///
    /// SHOW COLUMNS returns columns in order, but the ordinal position needs
    /// to be relative to each table, starting from 1.
    fn recompute_ordinal_positions(&self, columns: &mut [ColumnInfo]) {
        use std::collections::HashMap;

        // Group columns by (catalog, schema, table) and assign positions
        let mut position_counters: HashMap<(String, String, String), i32> = HashMap::new();

        for col in columns.iter_mut() {
            let key = (
                col.catalog_name.clone(),
                col.schema_name.clone(),
                col.table_name.clone(),
            );
            let counter = position_counters.entry(key).or_insert(0);
            *counter += 1;
            col.ordinal_position = *counter;
        }
    }

    /// Parse column type information to extract size, precision, and radix.
    ///
    /// For numeric types, extracts precision and scale from type names like "DECIMAL(10,2)".
    /// For string types, estimates maximum length.
    fn parse_column_type_info(&self, type_name: &str) -> (Option<i32>, Option<i16>, Option<i16>) {
        let type_upper = type_name.to_uppercase();
        let base_type = type_upper
            .split(['(', '<'])
            .next()
            .unwrap_or(&type_upper)
            .trim();

        match base_type {
            // Integer types - return bit width as column size
            "TINYINT" | "BYTE" => (Some(3), Some(0), Some(10)), // 8-bit, 3 digits
            "SMALLINT" | "SHORT" => (Some(5), Some(0), Some(10)), // 16-bit, 5 digits
            "INT" | "INTEGER" => (Some(10), Some(0), Some(10)), // 32-bit, 10 digits
            "BIGINT" | "LONG" => (Some(19), Some(0), Some(10)), // 64-bit, 19 digits

            // Floating point - IEEE 754 precision
            "FLOAT" | "REAL" => (Some(7), None, Some(2)), // ~7 significant digits
            "DOUBLE" => (Some(15), None, Some(2)),        // ~15 significant digits

            // Decimal - parse precision and scale
            "DECIMAL" | "DEC" | "NUMERIC" => {
                let (precision, scale) =
                    crate::metadata::type_mapping::parse_decimal_params(type_name);
                (Some(precision as i32), Some(scale as i16), Some(10))
            }

            // String types
            "STRING" | "TEXT" => (Some(i32::MAX), None, None),
            "VARCHAR" | "CHAR" => {
                // Try to parse length from VARCHAR(n)
                let size = self.parse_type_length(type_name).unwrap_or(i32::MAX);
                (Some(size), None, None)
            }

            // Binary
            "BINARY" | "VARBINARY" => (Some(i32::MAX), None, None),

            // Boolean
            "BOOLEAN" | "BOOL" => (Some(1), None, None),

            // Date/Time
            "DATE" => (Some(10), None, None), // YYYY-MM-DD
            "TIMESTAMP" | "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" => (Some(29), None, None), // Full timestamp

            // Complex types
            "ARRAY" | "MAP" | "STRUCT" => (None, None, None),

            // Unknown
            _ => (None, None, None),
        }
    }

    /// Parse type length from parameterized type strings like VARCHAR(255).
    fn parse_type_length(&self, type_name: &str) -> Option<i32> {
        let start = type_name.find('(')?;
        let end = type_name.rfind(')')?;
        if start >= end {
            return None;
        }
        let params = &type_name[start + 1..end];
        params.trim().parse::<i32>().ok()
    }

    /// List supported table types.
    ///
    /// Returns a static list of table types supported by Databricks:
    /// - `SYSTEM TABLE`
    /// - `TABLE`
    /// - `VIEW`
    /// - `METRIC_VIEW`
    ///
    /// # Returns
    ///
    /// A vector of table type strings.
    pub fn list_table_types(&self) -> Vec<String> {
        TABLE_TYPES.iter().map(|s| s.to_string()).collect()
    }

    /// List primary keys for a specific table.
    ///
    /// Executes `SHOW KEYS IN CATALOG {catalog} IN SCHEMA {schema} IN TABLE {table}`
    /// and returns the primary key information.
    ///
    /// # Arguments
    ///
    /// * `catalog` - The catalog containing the table.
    /// * `schema` - The schema containing the table.
    /// * `table` - The table name.
    ///
    /// # Returns
    ///
    /// A vector of [`PrimaryKeyInfo`] containing primary key columns.
    ///
    /// # Errors
    ///
    /// Returns an error if the query execution fails.
    pub fn list_primary_keys(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<PrimaryKeyInfo>> {
        let sql = SqlCommandBuilder::build_show_primary_keys(catalog, schema, table);
        debug!("Executing metadata query: {}", sql);

        let batches = self.execute_metadata_query(&sql)?;

        let mut keys = Vec::new();
        for batch in batches {
            // SHOW KEYS returns: database, tableName, primaryKey
            // or potentially more columns depending on version
            // The primaryKey column contains the key column name
            if batch.num_columns() < 3 {
                continue;
            }

            // Try to parse based on expected column structure
            // Some versions may return: pk_name, column_name, key_sequence
            // Others may return: database, tableName, primaryKey
            let column_names = self.extract_string_column(&batch, 2)?;

            for (i, col_name) in column_names.iter().enumerate() {
                if col_name.is_empty() {
                    continue;
                }
                keys.push(PrimaryKeyInfo {
                    catalog_name: catalog.to_string(),
                    schema_name: schema.to_string(),
                    table_name: table.to_string(),
                    column_name: col_name.clone(),
                    key_seq: (i + 1) as i16, // 1-based sequence
                    pk_name: None,           // Not typically provided
                });
            }
        }

        debug!("Found {} primary key columns", keys.len());
        Ok(keys)
    }

    /// List foreign keys for a specific table.
    ///
    /// Executes `SHOW FOREIGN KEYS IN CATALOG {catalog} IN SCHEMA {schema} IN TABLE {table}`
    /// and returns the foreign key information.
    ///
    /// # Arguments
    ///
    /// * `catalog` - The catalog containing the table.
    /// * `schema` - The schema containing the table.
    /// * `table` - The table name.
    ///
    /// # Returns
    ///
    /// A vector of [`ForeignKeyInfo`] containing foreign key relationships.
    ///
    /// # Errors
    ///
    /// Returns an error if the query execution fails.
    pub fn list_foreign_keys(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        let sql = SqlCommandBuilder::build_show_foreign_keys(catalog, schema, table);
        debug!("Executing metadata query: {}", sql);

        let batches = self.execute_metadata_query(&sql)?;

        let mut keys = Vec::new();
        for batch in batches {
            // SHOW FOREIGN KEYS returns relationship information
            // Expected columns vary by version, but typically include:
            // fk_database, fk_table, fk_column, pk_database, pk_table, pk_column
            if batch.num_columns() < 6 {
                continue;
            }

            let fk_schemas = self.extract_string_column(&batch, 0)?;
            let fk_tables = self.extract_string_column(&batch, 1)?;
            let fk_columns = self.extract_string_column(&batch, 2)?;
            let pk_schemas = self.extract_string_column(&batch, 3)?;
            let pk_tables = self.extract_string_column(&batch, 4)?;
            let pk_columns = self.extract_string_column(&batch, 5)?;

            for i in 0..fk_schemas.len() {
                keys.push(ForeignKeyInfo {
                    pk_catalog: catalog.to_string(), // Assume same catalog
                    pk_schema: pk_schemas[i].clone(),
                    pk_table: pk_tables[i].clone(),
                    pk_column: pk_columns[i].clone(),
                    fk_catalog: catalog.to_string(),
                    fk_schema: fk_schemas[i].clone(),
                    fk_table: fk_tables[i].clone(),
                    fk_column: fk_columns[i].clone(),
                    key_seq: (i + 1) as i16, // 1-based sequence
                    fk_name: None,           // Not typically provided
                    pk_name: None,           // Not typically provided
                });
            }
        }

        debug!("Found {} foreign key relationships", keys.len());
        Ok(keys)
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

        fn with_tables(tables: Vec<(&str, &str, &str, &str)>) -> Self {
            // tables: (catalog, schema, table_name, table_type)
            let data_array: Vec<Vec<String>> = tables
                .iter()
                .map(|(cat, schema, table, table_type)| {
                    vec![
                        cat.to_string(),
                        schema.to_string(),
                        table.to_string(),
                        table_type.to_string(),
                        "false".to_string(), // isTemporary
                    ]
                })
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
                        column_count: 5,
                        columns: vec![
                            ColumnInfo {
                                name: "catalog".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 0,
                            },
                            ColumnInfo {
                                name: "database".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 1,
                            },
                            ColumnInfo {
                                name: "tableName".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 2,
                            },
                            ColumnInfo {
                                name: "tableType".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 3,
                            },
                            ColumnInfo {
                                name: "isTemporary".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 4,
                            },
                        ],
                    },
                    total_chunk_count: Some(1),
                    total_row_count: Some(tables.len() as i64),
                    total_byte_count: None,
                    truncated: false,
                    chunks: None,
                    result_compression: None,
                }),
                result: Some(ExecuteResultData {
                    chunk_index: Some(0),
                    row_offset: Some(0),
                    row_count: Some(tables.len() as i64),
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
                        column_count: 5,
                        columns: vec![
                            ColumnInfo {
                                name: "catalog".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 0,
                            },
                            ColumnInfo {
                                name: "database".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 1,
                            },
                            ColumnInfo {
                                name: "tableName".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 2,
                            },
                            ColumnInfo {
                                name: "tableType".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 3,
                            },
                            ColumnInfo {
                                name: "isTemporary".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 4,
                            },
                        ],
                    },
                    total_chunk_count: Some(1),
                    total_row_count: Some(tables.len() as i64),
                    total_byte_count: None,
                    truncated: false,
                    chunks: None,
                    result_compression: None,
                }),
                result: Some(ExecuteResultData {
                    chunk_index: Some(0),
                    row_offset: Some(0),
                    row_count: Some(tables.len() as i64),
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

        fn with_columns(columns: Vec<(&str, &str, &str, &str, &str)>) -> Self {
            // columns: (catalog, schema, table, column_name, data_type)
            let data_array: Vec<Vec<String>> = columns
                .iter()
                .map(|(cat, schema, table, col, dtype)| {
                    vec![
                        cat.to_string(),
                        schema.to_string(),
                        table.to_string(),
                        col.to_string(),
                        dtype.to_string(),
                        "".to_string(), // comment
                    ]
                })
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
                        column_count: 6,
                        columns: vec![
                            ColumnInfo {
                                name: "catalog".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 0,
                            },
                            ColumnInfo {
                                name: "database".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 1,
                            },
                            ColumnInfo {
                                name: "tableName".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 2,
                            },
                            ColumnInfo {
                                name: "columnName".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 3,
                            },
                            ColumnInfo {
                                name: "dataType".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 4,
                            },
                            ColumnInfo {
                                name: "comment".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 5,
                            },
                        ],
                    },
                    total_chunk_count: Some(1),
                    total_row_count: Some(columns.len() as i64),
                    total_byte_count: None,
                    truncated: false,
                    chunks: None,
                    result_compression: None,
                }),
                result: Some(ExecuteResultData {
                    chunk_index: Some(0),
                    row_offset: Some(0),
                    row_count: Some(columns.len() as i64),
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
                        column_count: 6,
                        columns: vec![
                            ColumnInfo {
                                name: "catalog".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 0,
                            },
                            ColumnInfo {
                                name: "database".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 1,
                            },
                            ColumnInfo {
                                name: "tableName".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 2,
                            },
                            ColumnInfo {
                                name: "columnName".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 3,
                            },
                            ColumnInfo {
                                name: "dataType".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 4,
                            },
                            ColumnInfo {
                                name: "comment".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 5,
                            },
                        ],
                    },
                    total_chunk_count: Some(1),
                    total_row_count: Some(columns.len() as i64),
                    total_byte_count: None,
                    truncated: false,
                    chunks: None,
                    result_compression: None,
                }),
                result: Some(ExecuteResultData {
                    chunk_index: Some(0),
                    row_offset: Some(0),
                    row_count: Some(columns.len() as i64),
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

        fn with_primary_keys(keys: Vec<(&str, &str, &str)>) -> Self {
            // keys: (database, tableName, primaryKey column)
            let data_array: Vec<Vec<String>> = keys
                .iter()
                .map(|(db, table, pk_col)| {
                    vec![db.to_string(), table.to_string(), pk_col.to_string()]
                })
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
                        column_count: 3,
                        columns: vec![
                            ColumnInfo {
                                name: "database".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 0,
                            },
                            ColumnInfo {
                                name: "tableName".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 1,
                            },
                            ColumnInfo {
                                name: "primaryKey".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 2,
                            },
                        ],
                    },
                    total_chunk_count: Some(1),
                    total_row_count: Some(keys.len() as i64),
                    total_byte_count: None,
                    truncated: false,
                    chunks: None,
                    result_compression: None,
                }),
                result: Some(ExecuteResultData {
                    chunk_index: Some(0),
                    row_offset: Some(0),
                    row_count: Some(keys.len() as i64),
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
                        column_count: 3,
                        columns: vec![
                            ColumnInfo {
                                name: "database".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 0,
                            },
                            ColumnInfo {
                                name: "tableName".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 1,
                            },
                            ColumnInfo {
                                name: "primaryKey".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 2,
                            },
                        ],
                    },
                    total_chunk_count: Some(1),
                    total_row_count: Some(keys.len() as i64),
                    total_byte_count: None,
                    truncated: false,
                    chunks: None,
                    result_compression: None,
                }),
                result: Some(ExecuteResultData {
                    chunk_index: Some(0),
                    row_offset: Some(0),
                    row_count: Some(keys.len() as i64),
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

        fn with_foreign_keys(keys: Vec<(&str, &str, &str, &str, &str, &str)>) -> Self {
            // keys: (fk_schema, fk_table, fk_column, pk_schema, pk_table, pk_column)
            let data_array: Vec<Vec<String>> = keys
                .iter()
                .map(
                    |(fk_schema, fk_table, fk_col, pk_schema, pk_table, pk_col)| {
                        vec![
                            fk_schema.to_string(),
                            fk_table.to_string(),
                            fk_col.to_string(),
                            pk_schema.to_string(),
                            pk_table.to_string(),
                            pk_col.to_string(),
                        ]
                    },
                )
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
                        column_count: 6,
                        columns: vec![
                            ColumnInfo {
                                name: "fk_database".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 0,
                            },
                            ColumnInfo {
                                name: "fk_table".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 1,
                            },
                            ColumnInfo {
                                name: "fk_column".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 2,
                            },
                            ColumnInfo {
                                name: "pk_database".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 3,
                            },
                            ColumnInfo {
                                name: "pk_table".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 4,
                            },
                            ColumnInfo {
                                name: "pk_column".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 5,
                            },
                        ],
                    },
                    total_chunk_count: Some(1),
                    total_row_count: Some(keys.len() as i64),
                    total_byte_count: None,
                    truncated: false,
                    chunks: None,
                    result_compression: None,
                }),
                result: Some(ExecuteResultData {
                    chunk_index: Some(0),
                    row_offset: Some(0),
                    row_count: Some(keys.len() as i64),
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
                        column_count: 6,
                        columns: vec![
                            ColumnInfo {
                                name: "fk_database".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 0,
                            },
                            ColumnInfo {
                                name: "fk_table".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 1,
                            },
                            ColumnInfo {
                                name: "fk_column".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 2,
                            },
                            ColumnInfo {
                                name: "pk_database".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 3,
                            },
                            ColumnInfo {
                                name: "pk_table".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 4,
                            },
                            ColumnInfo {
                                name: "pk_column".to_string(),
                                type_name: "STRING".to_string(),
                                type_text: "STRING".to_string(),
                                position: 5,
                            },
                        ],
                    },
                    total_chunk_count: Some(1),
                    total_row_count: Some(keys.len() as i64),
                    total_byte_count: None,
                    truncated: false,
                    chunks: None,
                    result_compression: None,
                }),
                result: Some(ExecuteResultData {
                    chunk_index: Some(0),
                    row_offset: Some(0),
                    row_count: Some(keys.len() as i64),
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

    // =========================================================================
    // Tests for list_tables
    // =========================================================================

    #[test]
    fn test_list_tables_with_mock() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::with_tables(vec![
            ("main", "default", "users", "MANAGED"),
            ("main", "default", "orders", "MANAGED"),
            ("main", "analytics", "events_view", "VIEW"),
        ]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        let tables = service.list_tables(None, None, None, None).unwrap();
        assert_eq!(tables.len(), 3);
        assert_eq!(tables[0].catalog_name, "main");
        assert_eq!(tables[0].schema_name, "default");
        assert_eq!(tables[0].table_name, "users");
        assert_eq!(tables[0].table_type, "TABLE"); // MANAGED normalized to TABLE
        assert_eq!(tables[2].table_type, "VIEW");
    }

    #[test]
    fn test_list_tables_with_table_type_filter() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::with_tables(vec![
            ("main", "default", "users", "MANAGED"),
            ("main", "default", "orders", "MANAGED"),
            ("main", "analytics", "events_view", "VIEW"),
        ]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        // Filter for only TABLEs (MANAGED maps to TABLE)
        let tables = service
            .list_tables(None, None, None, Some(&["TABLE"]))
            .unwrap();
        assert_eq!(tables.len(), 2);
        assert!(tables.iter().all(|t| t.table_type == "TABLE"));
    }

    #[test]
    fn test_list_tables_with_multiple_table_types() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::with_tables(vec![
            ("main", "default", "users", "MANAGED"),
            ("main", "default", "orders", "VIEW"),
            ("main", "analytics", "system_info", "SYSTEM TABLE"),
        ]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        // Filter for TABLEs and VIEWs
        let tables = service
            .list_tables(None, None, None, Some(&["TABLE", "VIEW"]))
            .unwrap();
        assert_eq!(tables.len(), 2);
    }

    #[test]
    fn test_list_tables_empty() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::with_tables(vec![]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        let tables = service.list_tables(None, None, None, None).unwrap();
        assert!(tables.is_empty());
    }

    #[test]
    fn test_normalize_table_type() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::new(vec![]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        assert_eq!(service.normalize_table_type("MANAGED"), "TABLE");
        assert_eq!(service.normalize_table_type("EXTERNAL"), "TABLE");
        assert_eq!(service.normalize_table_type("TABLE"), "TABLE");
        assert_eq!(service.normalize_table_type("VIEW"), "VIEW");
        assert_eq!(service.normalize_table_type("MATERIALIZED_VIEW"), "VIEW");
        assert_eq!(service.normalize_table_type("SYSTEM TABLE"), "SYSTEM TABLE");
        assert_eq!(service.normalize_table_type("METRIC_VIEW"), "METRIC_VIEW");
        assert_eq!(service.normalize_table_type("unknown"), "UNKNOWN");
    }

    // =========================================================================
    // Tests for list_columns
    // =========================================================================

    #[test]
    fn test_list_columns_with_mock() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::with_columns(vec![
            ("main", "default", "users", "id", "BIGINT"),
            ("main", "default", "users", "name", "STRING"),
            ("main", "default", "users", "email", "STRING"),
        ]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        let columns = service
            .list_columns(Some("main"), None, None, None)
            .unwrap();
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0].catalog_name, "main");
        assert_eq!(columns[0].schema_name, "default");
        assert_eq!(columns[0].table_name, "users");
        assert_eq!(columns[0].column_name, "id");
        assert_eq!(columns[0].data_type, "BIGINT");
        assert_eq!(columns[0].ordinal_position, 1);
        assert_eq!(columns[1].ordinal_position, 2);
        assert_eq!(columns[2].ordinal_position, 3);
    }

    #[test]
    fn test_list_columns_with_multiple_tables() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::with_columns(vec![
            ("main", "default", "users", "id", "BIGINT"),
            ("main", "default", "users", "name", "STRING"),
            ("main", "default", "orders", "id", "BIGINT"),
            ("main", "default", "orders", "user_id", "BIGINT"),
        ]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        let columns = service
            .list_columns(Some("main"), None, None, None)
            .unwrap();
        assert_eq!(columns.len(), 4);

        // Check that ordinal positions are per-table
        assert_eq!(columns[0].table_name, "users");
        assert_eq!(columns[0].ordinal_position, 1);
        assert_eq!(columns[1].table_name, "users");
        assert_eq!(columns[1].ordinal_position, 2);
        assert_eq!(columns[2].table_name, "orders");
        assert_eq!(columns[2].ordinal_position, 1);
        assert_eq!(columns[3].table_name, "orders");
        assert_eq!(columns[3].ordinal_position, 2);
    }

    #[test]
    fn test_list_columns_type_parsing() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::with_columns(vec![
            ("main", "default", "test", "int_col", "INT"),
            ("main", "default", "test", "decimal_col", "DECIMAL(10,2)"),
            ("main", "default", "test", "varchar_col", "VARCHAR(255)"),
        ]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        let columns = service
            .list_columns(Some("main"), None, None, None)
            .unwrap();
        assert_eq!(columns.len(), 3);

        // INT type
        assert_eq!(columns[0].column_size, Some(10));
        assert_eq!(columns[0].decimal_digits, Some(0));
        assert_eq!(columns[0].num_prec_radix, Some(10));

        // DECIMAL(10,2) type
        assert_eq!(columns[1].column_size, Some(10));
        assert_eq!(columns[1].decimal_digits, Some(2));
        assert_eq!(columns[1].num_prec_radix, Some(10));

        // VARCHAR(255) type
        assert_eq!(columns[2].column_size, Some(255));
        assert!(columns[2].decimal_digits.is_none());
    }

    #[test]
    fn test_list_columns_empty() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::with_columns(vec![]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        let columns = service
            .list_columns(Some("main"), None, None, None)
            .unwrap();
        assert!(columns.is_empty());
    }

    #[test]
    fn test_list_columns_requires_catalog() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::with_columns(vec![]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        // Calling without catalog should fail
        let result = service.list_columns(None, None, None, None);
        assert!(result.is_err());
    }

    // =========================================================================
    // Tests for list_table_types
    // =========================================================================

    #[test]
    fn test_list_table_types() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::new(vec![]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        let types = service.list_table_types();
        assert_eq!(types.len(), 4);
        assert!(types.contains(&"SYSTEM TABLE".to_string()));
        assert!(types.contains(&"TABLE".to_string()));
        assert!(types.contains(&"VIEW".to_string()));
        assert!(types.contains(&"METRIC_VIEW".to_string()));
    }

    // =========================================================================
    // Tests for list_primary_keys
    // =========================================================================

    #[test]
    fn test_list_primary_keys_with_mock() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::with_primary_keys(vec![(
            "default", "users", "id",
        )]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        let keys = service
            .list_primary_keys("main", "default", "users")
            .unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].catalog_name, "main");
        assert_eq!(keys[0].schema_name, "default");
        assert_eq!(keys[0].table_name, "users");
        assert_eq!(keys[0].column_name, "id");
        assert_eq!(keys[0].key_seq, 1);
    }

    #[test]
    fn test_list_primary_keys_composite() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::with_primary_keys(vec![
            ("default", "order_items", "order_id"),
            ("default", "order_items", "item_id"),
        ]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        let keys = service
            .list_primary_keys("main", "default", "order_items")
            .unwrap();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].column_name, "order_id");
        assert_eq!(keys[0].key_seq, 1);
        assert_eq!(keys[1].column_name, "item_id");
        assert_eq!(keys[1].key_seq, 2);
    }

    #[test]
    fn test_list_primary_keys_empty() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::with_primary_keys(vec![]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        let keys = service
            .list_primary_keys("main", "default", "no_pk_table")
            .unwrap();
        assert!(keys.is_empty());
    }

    // =========================================================================
    // Tests for list_foreign_keys
    // =========================================================================

    #[test]
    fn test_list_foreign_keys_with_mock() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::with_foreign_keys(vec![(
            "default", "orders", "user_id", "default", "users", "id",
        )]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        let keys = service
            .list_foreign_keys("main", "default", "orders")
            .unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].fk_catalog, "main");
        assert_eq!(keys[0].fk_schema, "default");
        assert_eq!(keys[0].fk_table, "orders");
        assert_eq!(keys[0].fk_column, "user_id");
        assert_eq!(keys[0].pk_schema, "default");
        assert_eq!(keys[0].pk_table, "users");
        assert_eq!(keys[0].pk_column, "id");
        assert_eq!(keys[0].key_seq, 1);
    }

    #[test]
    fn test_list_foreign_keys_multiple() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::with_foreign_keys(vec![
            ("default", "orders", "user_id", "default", "users", "id"),
            (
                "default",
                "orders",
                "product_id",
                "default",
                "products",
                "id",
            ),
        ]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        let keys = service
            .list_foreign_keys("main", "default", "orders")
            .unwrap();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].fk_column, "user_id");
        assert_eq!(keys[0].pk_table, "users");
        assert_eq!(keys[1].fk_column, "product_id");
        assert_eq!(keys[1].pk_table, "products");
    }

    #[test]
    fn test_list_foreign_keys_empty() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::with_foreign_keys(vec![]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        let keys = service
            .list_foreign_keys("main", "default", "users")
            .unwrap();
        assert!(keys.is_empty());
    }

    // =========================================================================
    // Tests for parse_column_type_info
    // =========================================================================

    #[test]
    fn test_parse_column_type_info_integers() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::new(vec![]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        let (size, digits, radix) = service.parse_column_type_info("TINYINT");
        assert_eq!(size, Some(3));
        assert_eq!(digits, Some(0));
        assert_eq!(radix, Some(10));

        let (size, digits, radix) = service.parse_column_type_info("INT");
        assert_eq!(size, Some(10));
        assert_eq!(digits, Some(0));
        assert_eq!(radix, Some(10));

        let (size, digits, radix) = service.parse_column_type_info("BIGINT");
        assert_eq!(size, Some(19));
        assert_eq!(digits, Some(0));
        assert_eq!(radix, Some(10));
    }

    #[test]
    fn test_parse_column_type_info_decimal() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::new(vec![]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        let (size, digits, radix) = service.parse_column_type_info("DECIMAL(10,2)");
        assert_eq!(size, Some(10));
        assert_eq!(digits, Some(2));
        assert_eq!(radix, Some(10));

        let (size, digits, radix) = service.parse_column_type_info("DECIMAL(38,18)");
        assert_eq!(size, Some(38));
        assert_eq!(digits, Some(18));
        assert_eq!(radix, Some(10));
    }

    #[test]
    fn test_parse_column_type_info_string() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::new(vec![]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        let (size, digits, radix) = service.parse_column_type_info("STRING");
        assert_eq!(size, Some(i32::MAX));
        assert!(digits.is_none());
        assert!(radix.is_none());

        let (size, digits, radix) = service.parse_column_type_info("VARCHAR(255)");
        assert_eq!(size, Some(255));
        assert!(digits.is_none());
        assert!(radix.is_none());
    }

    #[test]
    fn test_parse_type_length() {
        let runtime = create_test_runtime();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient::new(vec![]));
        let service =
            MetadataService::new(client, "session-1".to_string(), runtime.handle().clone());

        assert_eq!(service.parse_type_length("VARCHAR(255)"), Some(255));
        assert_eq!(service.parse_type_length("CHAR(10)"), Some(10));
        assert_eq!(service.parse_type_length("STRING"), None);
        assert_eq!(service.parse_type_length("VARCHAR"), None);
        assert_eq!(service.parse_type_length("VARCHAR()"), None);
    }
}
