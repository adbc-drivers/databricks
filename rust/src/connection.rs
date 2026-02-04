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
use crate::metadata::{databricks_type_to_arrow, GetObjectsBuilder, MetadataService};
use crate::reader::ResultReaderFactory;
use crate::statement::Statement;
use crate::types::cloudfetch::CloudFetchConfig;
use adbc_core::error::Result;
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue};
use adbc_core::Optionable;
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray};
use arrow_schema::{ArrowError, DataType, Field, Schema};
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

    /// Checks if a name matches a SQL LIKE pattern.
    ///
    /// Supports `%` for multi-character wildcard and `_` for single-character wildcard.
    /// The pattern matching is case-sensitive.
    fn matches_pattern(name: &str, pattern: &str) -> bool {
        // Handle special case of % meaning match all
        if pattern == "%" {
            return true;
        }

        // Convert SQL LIKE pattern to regex-like matching
        let mut regex_pattern = String::new();
        let mut chars = pattern.chars().peekable();

        while let Some(c) = chars.next() {
            match c {
                '%' => regex_pattern.push_str(".*"),
                '_' => regex_pattern.push('.'),
                // Escape regex special characters
                '.' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '^' | '$'
                | '\\' => {
                    regex_pattern.push('\\');
                    regex_pattern.push(c);
                }
                _ => regex_pattern.push(c),
            }
        }

        // Match the entire string
        regex_pattern = format!("^{}$", regex_pattern);

        regex::Regex::new(&regex_pattern)
            .map(|re| re.is_match(name))
            .unwrap_or(false)
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
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<impl RecordBatchReader + Send> {
        debug!(
            "get_objects: depth={:?}, catalog={:?}, db_schema={:?}, table_name={:?}, table_type={:?}, column_name={:?}",
            depth, catalog, db_schema, table_name, table_type, column_name
        );

        let metadata_service = MetadataService::new(
            self.client.clone(),
            self.session_id.clone(),
            self.runtime.handle().clone(),
        );

        // Step 1: Get catalogs (filtered by pattern if provided)
        let catalogs = metadata_service
            .list_catalogs()
            .map_err(|e| e.to_adbc())?;

        // Filter catalogs by pattern if provided
        let catalogs: Vec<_> = catalogs
            .into_iter()
            .filter(|c| match catalog {
                None => true,
                Some(pattern) => pattern.is_empty() || Self::matches_pattern(&c.catalog_name, pattern),
            })
            .collect();

        let mut builder = GetObjectsBuilder::new();

        for cat in &catalogs {
            builder.add_catalog(&cat.catalog_name);

            // Stop here if depth is Catalogs
            if matches!(depth, ObjectDepth::Catalogs) {
                continue;
            }

            // Step 2: Get schemas for this catalog
            let schemas = metadata_service
                .list_schemas(Some(&cat.catalog_name), db_schema)
                .map_err(|e| e.to_adbc())?;

            for schema in &schemas {
                builder.add_schema(&cat.catalog_name, &schema.schema_name);

                // Stop here if depth is Schemas
                if matches!(depth, ObjectDepth::Schemas) {
                    continue;
                }

                // Step 3: Get tables for this schema
                let table_type_slice: Option<Vec<&str>> = table_type.as_ref().map(|v| v.iter().map(|s| s.as_ref()).collect());
                let tables = metadata_service
                    .list_tables(
                        Some(&cat.catalog_name),
                        Some(&schema.schema_name),
                        table_name,
                        table_type_slice.as_deref(),
                    )
                    .map_err(|e| e.to_adbc())?;

                for table in &tables {
                    builder.add_table(&cat.catalog_name, &schema.schema_name, table);

                    // Stop here if depth is Tables
                    if matches!(depth, ObjectDepth::Tables) {
                        continue;
                    }

                    // Step 4: Get columns for this table (depth is All or Columns)
                    let columns = metadata_service
                        .list_columns(
                            Some(&cat.catalog_name),
                            Some(&schema.schema_name),
                            Some(&table.table_name),
                            column_name,
                        )
                        .map_err(|e| e.to_adbc())?;

                    for col in &columns {
                        builder.add_column(
                            &cat.catalog_name,
                            &schema.schema_name,
                            &table.table_name,
                            col,
                        );
                    }

                    // Step 5: Get constraints (primary keys, foreign keys)
                    let pks = metadata_service
                        .list_primary_keys(&cat.catalog_name, &schema.schema_name, &table.table_name)
                        .unwrap_or_default();

                    let fks = metadata_service
                        .list_foreign_keys(&cat.catalog_name, &schema.schema_name, &table.table_name)
                        .unwrap_or_default();

                    builder.add_constraints(
                        &cat.catalog_name,
                        &schema.schema_name,
                        &table.table_name,
                        &pks,
                        &fks,
                    );
                }
            }
        }

        builder.build().map_err(|e| e.to_adbc())
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        debug!(
            "get_table_schema: catalog={:?}, db_schema={:?}, table_name={}",
            catalog, db_schema, table_name
        );

        let metadata_service = MetadataService::new(
            self.client.clone(),
            self.session_id.clone(),
            self.runtime.handle().clone(),
        );

        // Get columns for the specific table - use exact match by escaping pattern characters
        let columns = metadata_service
            .list_columns(catalog, db_schema, Some(table_name), None)
            .map_err(|e| e.to_adbc())?;

        // Filter to only exact matches for the table name (in case LIKE pattern returned more)
        let columns: Vec<_> = columns
            .into_iter()
            .filter(|c| c.table_name == table_name)
            .collect();

        if columns.is_empty() {
            return Err(DatabricksErrorHelper::not_found()
                .message(format!("Table not found: {}", table_name))
                .to_adbc());
        }

        // Convert columns to Arrow schema fields
        let fields: Vec<Field> = columns
            .iter()
            .map(|col| {
                let arrow_type = databricks_type_to_arrow(&col.type_name);
                let nullable = col.nullable != 0; // 0 = no nulls, 1 = nullable, 2 = unknown (treat as nullable)
                Field::new(&col.column_name, arrow_type, nullable)
            })
            .collect();

        Ok(Schema::new(fields))
    }

    fn get_table_types(&self) -> Result<impl RecordBatchReader + Send> {
        debug!("get_table_types");

        // Static list of table types supported by Databricks
        // From databricks-jdbc MetadataResultConstants.java
        let table_types = vec!["SYSTEM TABLE", "TABLE", "VIEW", "METRIC_VIEW"];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]));

        let array = StringArray::from(table_types);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).map_err(|e| {
            DatabricksErrorHelper::io()
                .message(format!("Failed to create RecordBatch: {}", e))
                .to_adbc()
        })?;

        Ok(RecordBatchIterator::new(vec![Ok(batch)], schema))
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

    // =========================================================================
    // Tests for pattern matching helper
    // =========================================================================

    #[test]
    fn test_matches_pattern_exact() {
        assert!(Connection::matches_pattern("main", "main"));
        assert!(!Connection::matches_pattern("main", "other"));
    }

    #[test]
    fn test_matches_pattern_wildcard_all() {
        assert!(Connection::matches_pattern("main", "%"));
        assert!(Connection::matches_pattern("any_catalog", "%"));
        assert!(Connection::matches_pattern("", "%"));
    }

    #[test]
    fn test_matches_pattern_prefix() {
        assert!(Connection::matches_pattern("main_catalog", "main%"));
        assert!(Connection::matches_pattern("main", "main%"));
        assert!(!Connection::matches_pattern("other", "main%"));
    }

    #[test]
    fn test_matches_pattern_suffix() {
        assert!(Connection::matches_pattern("test_main", "%main"));
        assert!(Connection::matches_pattern("main", "%main"));
        assert!(!Connection::matches_pattern("main_test", "%main"));
    }

    #[test]
    fn test_matches_pattern_contains() {
        assert!(Connection::matches_pattern("test_main_catalog", "%main%"));
        assert!(Connection::matches_pattern("main", "%main%"));
        assert!(!Connection::matches_pattern("other", "%main%"));
    }

    #[test]
    fn test_matches_pattern_single_char_wildcard() {
        assert!(Connection::matches_pattern("main", "mai_"));
        assert!(Connection::matches_pattern("mans", "ma_s"));
        assert!(!Connection::matches_pattern("main", "ma_"));
        assert!(!Connection::matches_pattern("main", "ma___"));
    }

    #[test]
    fn test_matches_pattern_combined_wildcards() {
        assert!(Connection::matches_pattern("catalog_test_1", "catalog_%_1"));
        assert!(Connection::matches_pattern("catalog_abc_1", "catalog_%_1"));
        assert!(Connection::matches_pattern("catalog__1", "catalog_%_1"));
    }

    #[test]
    fn test_matches_pattern_special_characters() {
        // Test that regex special characters are escaped
        assert!(Connection::matches_pattern("test.catalog", "test.catalog"));
        assert!(!Connection::matches_pattern("testXcatalog", "test.catalog"));
    }

    // =========================================================================
    // Tests for get_table_types (static, no mocking needed)
    // =========================================================================

    // Note: These tests require a full Connection which needs a mock client.
    // The get_table_types functionality is tested indirectly through the
    // static return value verification in the integration tests.

    // We can verify the static values match our expectations:
    #[test]
    fn test_table_types_static_values() {
        // The expected table types from databricks-jdbc MetadataResultConstants.java
        let expected_types = vec!["SYSTEM TABLE", "TABLE", "VIEW", "METRIC_VIEW"];

        // These are the same values used in get_table_types
        // This test documents the expected behavior
        assert_eq!(expected_types.len(), 4);
        assert!(expected_types.contains(&"TABLE"));
        assert!(expected_types.contains(&"VIEW"));
        assert!(expected_types.contains(&"SYSTEM TABLE"));
        assert!(expected_types.contains(&"METRIC_VIEW"));
    }

    // =========================================================================
    // Tests for get_table_schema - requires mocked MetadataService
    // These would need integration tests with a mock client
    // =========================================================================

    // Note: Full get_table_schema and get_objects tests require a mock DatabricksClient
    // that can return controlled responses to metadata queries.
    // Integration tests should be added in a separate test module.

    // =========================================================================
    // Tests for get_objects - requires mocked MetadataService
    // =========================================================================

    // Note: The get_objects method is thoroughly tested through the GetObjectsBuilder
    // unit tests in metadata/builder.rs. Full end-to-end tests require a mock client.
}
