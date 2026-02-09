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

use crate::client::DatabricksClient;
use crate::error::DatabricksErrorHelper;
use crate::metadata::{parse_columns_as_fields, parse_tables};
use crate::statement::Statement;
use adbc_core::error::Result;
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue};
use adbc_core::Optionable;
use adbc_core::schemas::GET_TABLE_TYPES_SCHEMA;
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray};
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

    // Session ID (created on connection initialization)
    session_id: String,

    // Tokio runtime for async operations
    runtime: tokio::runtime::Runtime,
}

/// Type alias for our empty reader used in stub implementations.
type EmptyReader =
    RecordBatchIterator<std::vec::IntoIter<std::result::Result<RecordBatch, ArrowError>>>;

impl Connection {
    /// Called by Database::new_connection().
    ///
    /// Connection receives the DatabricksClient and runtime from Database.
    /// The runtime is created by Database so it can share the handle with
    /// SeaClient and ResultReaderFactory before Connection is created.
    pub(crate) fn new_with_runtime(
        config: ConnectionConfig,
        runtime: tokio::runtime::Runtime,
    ) -> crate::error::Result<Self> {
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
            session_id: session_info.session_id,
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
        Ok(Statement::new(
            self.client.clone(),
            self.session_id.clone(),
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
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        // SHOW COLUMNS IN CATALOG `{cat}` requires a catalog.
        // If catalog is not provided, discover it via list_tables first.
        let catalog = match catalog {
            Some(c) => c.to_string(),
            None => {
                let result = self
                    .runtime
                    .block_on(self.client.list_tables(
                        &self.session_id,
                        None,
                        db_schema,
                        Some(table_name),
                        None,
                    ))
                    .map_err(|e| e.to_adbc())?;
                let tables = parse_tables(result).map_err(|e| e.to_adbc())?;
                tables
                    .first()
                    .map(|t| t.catalog_name.clone())
                    .ok_or_else(|| {
                        DatabricksErrorHelper::not_found()
                            .message(format!("Table not found: {}", table_name))
                            .to_adbc()
                    })?
            }
        };

        let result = self
            .runtime
            .block_on(self.client.list_columns(
                &self.session_id,
                &catalog,
                db_schema,
                Some(table_name),
                None, // all columns
            ))
            .map_err(|e| e.to_adbc())?;
        let fields = parse_columns_as_fields(result).map_err(|e| e.to_adbc())?;

        if fields.is_empty() {
            return Err(DatabricksErrorHelper::not_found()
                .message(format!("Table not found: {}", table_name))
                .to_adbc());
        }

        Ok(Schema::new(fields))
    }

    fn get_table_types(&self) -> Result<impl RecordBatchReader + Send> {
        let table_types = self.client.list_table_types();
        let array = StringArray::from(table_types);
        let batch = RecordBatch::try_new(GET_TABLE_TYPES_SCHEMA.clone(), vec![Arc::new(array)])
            .map_err(|e| {
                DatabricksErrorHelper::io()
                    .message(format!("Failed to build get_table_types result: {}", e))
                    .to_adbc()
            })?;

        Ok(RecordBatchIterator::new(
            vec![Ok(batch)],
            GET_TABLE_TYPES_SCHEMA.clone(),
        ))
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
    use crate::client::{
        ChunkLinkFetchResult, DatabricksClient, ExecuteResult, SessionInfo,
    };
    use crate::types::sea::ExecuteParams;
    use arrow_array::cast::AsArray;
    use arrow_array::Array;
    use async_trait::async_trait;

    /// Minimal mock client for connection tests.
    #[derive(Debug)]
    struct MockClient;

    #[async_trait]
    impl DatabricksClient for MockClient {
        async fn create_session(
            &self,
            _catalog: Option<&str>,
            _schema: Option<&str>,
            _session_config: HashMap<String, String>,
        ) -> crate::error::Result<SessionInfo> {
            Ok(SessionInfo {
                session_id: "mock-session".to_string(),
            })
        }

        async fn delete_session(&self, _session_id: &str) -> crate::error::Result<()> {
            Ok(())
        }

        async fn execute_statement(
            &self,
            _session_id: &str,
            _sql: &str,
            _params: &ExecuteParams,
        ) -> crate::error::Result<ExecuteResult> {
            unimplemented!("not needed for get_table_types test")
        }

        async fn get_result_chunks(
            &self,
            _statement_id: &str,
            _chunk_index: i64,
            _row_offset: i64,
        ) -> crate::error::Result<ChunkLinkFetchResult> {
            unimplemented!("not needed for get_table_types test")
        }

        async fn cancel_statement(&self, _statement_id: &str) -> crate::error::Result<()> {
            Ok(())
        }

        async fn close_statement(&self, _statement_id: &str) -> crate::error::Result<()> {
            Ok(())
        }

        async fn list_catalogs(
            &self,
            _session_id: &str,
        ) -> crate::error::Result<ExecuteResult> {
            unimplemented!("not needed for get_table_types test")
        }

        async fn list_schemas(
            &self,
            _session_id: &str,
            _catalog: Option<&str>,
            _schema_pattern: Option<&str>,
        ) -> crate::error::Result<ExecuteResult> {
            unimplemented!("not needed for get_table_types test")
        }

        async fn list_tables(
            &self,
            _session_id: &str,
            _catalog: Option<&str>,
            _schema_pattern: Option<&str>,
            _table_pattern: Option<&str>,
            _table_types: Option<&[&str]>,
        ) -> crate::error::Result<ExecuteResult> {
            unimplemented!("not needed for get_table_types test")
        }

        async fn list_columns(
            &self,
            _session_id: &str,
            _catalog: &str,
            _schema_pattern: Option<&str>,
            _table_pattern: Option<&str>,
            _column_pattern: Option<&str>,
        ) -> crate::error::Result<ExecuteResult> {
            unimplemented!("not needed for get_table_types test")
        }

        async fn list_primary_keys(
            &self,
            _session_id: &str,
            _catalog: &str,
            _schema: &str,
            _table: &str,
        ) -> crate::error::Result<ExecuteResult> {
            unimplemented!("not needed for get_table_types test")
        }

        async fn list_foreign_keys(
            &self,
            _session_id: &str,
            _catalog: &str,
            _schema: &str,
            _table: &str,
        ) -> crate::error::Result<ExecuteResult> {
            unimplemented!("not needed for get_table_types test")
        }

        fn list_table_types(&self) -> Vec<String> {
            vec![
                "SYSTEM TABLE".to_string(),
                "TABLE".to_string(),
                "VIEW".to_string(),
                "METRIC_VIEW".to_string(),
            ]
        }
    }

    /// Helper to create a Connection with the mock client (bypasses session creation).
    fn create_test_connection() -> Connection {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let client: Arc<dyn DatabricksClient> = Arc::new(MockClient);
        Connection {
            host: "https://test.databricks.com".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            client,
            session_id: "mock-session".to_string(),
            runtime,
        }
    }

    #[test]
    fn test_get_table_types_returns_correct_types() {
        use adbc_core::Connection as _;

        let conn = create_test_connection();
        let mut reader = conn.get_table_types().unwrap();

        // Verify schema matches GET_TABLE_TYPES_SCHEMA
        let schema = reader.schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "table_type");

        // Read batches and collect all values
        let batch = reader.next().unwrap().unwrap();
        let table_type_col = batch.column(0).as_string::<i32>();
        let values: Vec<&str> = (0..table_type_col.len())
            .map(|i| table_type_col.value(i))
            .collect();

        assert_eq!(
            values,
            vec!["SYSTEM TABLE", "TABLE", "VIEW", "METRIC_VIEW"]
        );

        // No more batches
        assert!(reader.next().is_none());
    }

    // --- get_table_schema tests ---

    use crate::reader::ResultReader;
    use arrow_array::{BooleanArray, Int32Array};
    use arrow_schema::{DataType, Field, TimeUnit};

    /// A mock ResultReader that returns pre-built RecordBatches.
    struct MockReader {
        batches: Vec<RecordBatch>,
        index: usize,
        schema: arrow_schema::SchemaRef,
    }

    impl MockReader {
        fn new(batches: Vec<RecordBatch>) -> Self {
            let schema = if batches.is_empty() {
                Arc::new(arrow_schema::Schema::empty())
            } else {
                batches[0].schema()
            };
            Self {
                batches,
                index: 0,
                schema,
            }
        }
    }

    impl ResultReader for MockReader {
        fn schema(&self) -> crate::error::Result<arrow_schema::SchemaRef> {
            Ok(self.schema.clone())
        }

        fn next_batch(&mut self) -> crate::error::Result<Option<RecordBatch>> {
            if self.index < self.batches.len() {
                let batch = self.batches[self.index].clone();
                self.index += 1;
                Ok(Some(batch))
            } else {
                Ok(None)
            }
        }
    }

    fn make_execute_result(batches: Vec<RecordBatch>) -> ExecuteResult {
        ExecuteResult {
            statement_id: "test-stmt-id".to_string(),
            reader: Box::new(MockReader::new(batches)),
        }
    }

    /// Build a mock SHOW COLUMNS result batch.
    fn make_columns_batch(
        col_names: Vec<&str>,
        catalog_names: Vec<&str>,
        namespaces: Vec<&str>,
        table_names: Vec<&str>,
        column_types: Vec<&str>,
        is_nullables: Vec<Option<&str>>,
    ) -> RecordBatch {
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("col_name", DataType::Utf8, false),
            Field::new("catalogName", DataType::Utf8, true),
            Field::new("namespace", DataType::Utf8, false),
            Field::new("tableName", DataType::Utf8, false),
            Field::new("columnType", DataType::Utf8, false),
            Field::new("columnSize", DataType::Int32, true),
            Field::new("decimalDigits", DataType::Int32, true),
            Field::new("radix", DataType::Int32, true),
            Field::new("isNullable", DataType::Utf8, true),
            Field::new("remarks", DataType::Utf8, true),
            Field::new("ordinalPosition", DataType::Int32, true),
            Field::new("isAutoIncrement", DataType::Utf8, true),
            Field::new("isGenerated", DataType::Utf8, true),
        ]));
        let n = col_names.len();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(col_names)),
                Arc::new(StringArray::from(catalog_names)),
                Arc::new(StringArray::from(namespaces)),
                Arc::new(StringArray::from(table_names)),
                Arc::new(StringArray::from(column_types)),
                Arc::new(Int32Array::from(vec![None::<i32>; n])),
                Arc::new(Int32Array::from(vec![None::<i32>; n])),
                Arc::new(Int32Array::from(vec![None::<i32>; n])),
                Arc::new(StringArray::from(is_nullables)),
                Arc::new(StringArray::from(vec![None::<&str>; n])),
                Arc::new(Int32Array::from((1..=n as i32).map(Some).collect::<Vec<_>>())),
                Arc::new(StringArray::from(vec![None::<&str>; n])),
                Arc::new(StringArray::from(vec![None::<&str>; n])),
            ],
        )
        .unwrap()
    }

    /// Build a mock SHOW TABLES result batch.
    fn make_tables_batch(
        catalog_names: Vec<&str>,
        namespaces: Vec<&str>,
        table_names: Vec<&str>,
        table_types: Vec<&str>,
    ) -> RecordBatch {
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("namespace", DataType::Utf8, false),
            Field::new("tableName", DataType::Utf8, false),
            Field::new("isTemporary", DataType::Boolean, false),
            Field::new("information", DataType::Utf8, true),
            Field::new("catalogName", DataType::Utf8, false),
            Field::new("tableType", DataType::Utf8, false),
            Field::new("remarks", DataType::Utf8, true),
        ]));
        let n = catalog_names.len();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(namespaces)),
                Arc::new(StringArray::from(table_names)),
                Arc::new(BooleanArray::from(vec![false; n])),
                Arc::new(StringArray::from(vec![None::<&str>; n])),
                Arc::new(StringArray::from(catalog_names)),
                Arc::new(StringArray::from(table_types)),
                Arc::new(StringArray::from(vec![None::<&str>; n])),
            ],
        )
        .unwrap()
    }

    /// Mock client that supports list_columns and list_tables for get_table_schema tests.
    #[derive(Debug)]
    struct MockMetadataClient {
        tables_batch: std::sync::Mutex<Option<RecordBatch>>,
        columns_batch: std::sync::Mutex<Option<RecordBatch>>,
    }

    impl MockMetadataClient {
        fn new(tables_batch: Option<RecordBatch>, columns_batch: Option<RecordBatch>) -> Self {
            Self {
                tables_batch: std::sync::Mutex::new(tables_batch),
                columns_batch: std::sync::Mutex::new(columns_batch),
            }
        }
    }

    #[async_trait]
    impl DatabricksClient for MockMetadataClient {
        async fn create_session(
            &self,
            _catalog: Option<&str>,
            _schema: Option<&str>,
            _session_config: HashMap<String, String>,
        ) -> crate::error::Result<SessionInfo> {
            Ok(SessionInfo {
                session_id: "mock-session".to_string(),
            })
        }

        async fn delete_session(&self, _session_id: &str) -> crate::error::Result<()> {
            Ok(())
        }

        async fn execute_statement(
            &self,
            _session_id: &str,
            _sql: &str,
            _params: &ExecuteParams,
        ) -> crate::error::Result<ExecuteResult> {
            unimplemented!()
        }

        async fn get_result_chunks(
            &self,
            _statement_id: &str,
            _chunk_index: i64,
            _row_offset: i64,
        ) -> crate::error::Result<ChunkLinkFetchResult> {
            unimplemented!()
        }

        async fn cancel_statement(&self, _statement_id: &str) -> crate::error::Result<()> {
            Ok(())
        }

        async fn close_statement(&self, _statement_id: &str) -> crate::error::Result<()> {
            Ok(())
        }

        async fn list_catalogs(
            &self,
            _session_id: &str,
        ) -> crate::error::Result<ExecuteResult> {
            unimplemented!()
        }

        async fn list_schemas(
            &self,
            _session_id: &str,
            _catalog: Option<&str>,
            _schema_pattern: Option<&str>,
        ) -> crate::error::Result<ExecuteResult> {
            unimplemented!()
        }

        async fn list_tables(
            &self,
            _session_id: &str,
            _catalog: Option<&str>,
            _schema_pattern: Option<&str>,
            _table_pattern: Option<&str>,
            _table_types: Option<&[&str]>,
        ) -> crate::error::Result<ExecuteResult> {
            let batch = self.tables_batch.lock().unwrap().take();
            Ok(make_execute_result(batch.into_iter().collect()))
        }

        async fn list_columns(
            &self,
            _session_id: &str,
            _catalog: &str,
            _schema_pattern: Option<&str>,
            _table_pattern: Option<&str>,
            _column_pattern: Option<&str>,
        ) -> crate::error::Result<ExecuteResult> {
            let batch = self.columns_batch.lock().unwrap().take();
            Ok(make_execute_result(batch.into_iter().collect()))
        }

        async fn list_primary_keys(
            &self,
            _session_id: &str,
            _catalog: &str,
            _schema: &str,
            _table: &str,
        ) -> crate::error::Result<ExecuteResult> {
            unimplemented!()
        }

        async fn list_foreign_keys(
            &self,
            _session_id: &str,
            _catalog: &str,
            _schema: &str,
            _table: &str,
        ) -> crate::error::Result<ExecuteResult> {
            unimplemented!()
        }

        fn list_table_types(&self) -> Vec<String> {
            vec![
                "SYSTEM TABLE".to_string(),
                "TABLE".to_string(),
                "VIEW".to_string(),
                "METRIC_VIEW".to_string(),
            ]
        }
    }

    fn create_metadata_test_connection(client: MockMetadataClient) -> Connection {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let client: Arc<dyn DatabricksClient> = Arc::new(client);
        Connection {
            host: "https://test.databricks.com".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            client,
            session_id: "mock-session".to_string(),
            runtime,
        }
    }

    #[test]
    fn test_get_table_schema_builds_correct_schema() {
        use adbc_core::Connection as _;

        let columns_batch = make_columns_batch(
            vec!["id", "name", "created_at", "price"],
            vec!["main", "main", "main", "main"],
            vec!["default", "default", "default", "default"],
            vec!["users", "users", "users", "users"],
            vec!["BIGINT", "STRING", "TIMESTAMP", "DECIMAL(10,2)"],
            vec![Some("false"), Some("true"), Some("true"), Some("false")],
        );

        let client = MockMetadataClient::new(None, Some(columns_batch));
        let conn = create_metadata_test_connection(client);

        let schema = conn
            .get_table_schema(Some("main"), Some("default"), "users")
            .unwrap();

        assert_eq!(schema.fields().len(), 4);

        // id: BIGINT, not nullable
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(*schema.field(0).data_type(), DataType::Int64);
        assert!(!schema.field(0).is_nullable());

        // name: STRING, nullable
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
        assert!(schema.field(1).is_nullable());

        // created_at: TIMESTAMP, nullable
        assert_eq!(schema.field(2).name(), "created_at");
        assert_eq!(
            *schema.field(2).data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert!(schema.field(2).is_nullable());

        // price: DECIMAL(10,2), not nullable
        assert_eq!(schema.field(3).name(), "price");
        assert_eq!(*schema.field(3).data_type(), DataType::Decimal128(10, 2));
        assert!(!schema.field(3).is_nullable());
    }

    #[test]
    fn test_get_table_schema_discovers_catalog_via_list_tables() {
        use adbc_core::Connection as _;

        let tables_batch = make_tables_batch(
            vec!["discovered_catalog"],
            vec!["default"],
            vec!["users"],
            vec!["TABLE"],
        );
        let columns_batch = make_columns_batch(
            vec!["id", "name"],
            vec!["discovered_catalog", "discovered_catalog"],
            vec!["default", "default"],
            vec!["users", "users"],
            vec!["INT", "STRING"],
            vec![Some("false"), Some("true")],
        );

        let client = MockMetadataClient::new(Some(tables_batch), Some(columns_batch));
        let conn = create_metadata_test_connection(client);

        // Call without catalog — should discover it via list_tables
        let schema = conn
            .get_table_schema(None, Some("default"), "users")
            .unwrap();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(*schema.field(0).data_type(), DataType::Int32);
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
    }

    #[test]
    fn test_get_table_schema_not_found_empty_columns() {
        use adbc_core::Connection as _;

        // list_columns returns empty — table exists but no columns (treated as not found)
        let client = MockMetadataClient::new(None, None);
        let conn = create_metadata_test_connection(client);

        let result = conn.get_table_schema(Some("main"), Some("default"), "nonexistent");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, adbc_core::error::Status::NotFound);
        assert!(err.message.contains("Table not found"));
    }

    #[test]
    fn test_get_table_schema_not_found_no_catalog_discovered() {
        use adbc_core::Connection as _;

        // list_tables returns empty — no table found in any catalog
        let client = MockMetadataClient::new(None, None);
        let conn = create_metadata_test_connection(client);

        let result = conn.get_table_schema(None, Some("default"), "nonexistent");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, adbc_core::error::Status::NotFound);
        assert!(err.message.contains("Table not found"));
    }
}
