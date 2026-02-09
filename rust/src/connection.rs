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
use crate::metadata::builder::{
    build_get_objects_all, build_get_objects_catalogs, build_get_objects_schemas,
    build_get_objects_tables, collect_reader, filter_by_pattern, group_schemas_by_catalog,
    group_tables_and_columns, group_tables_by_catalog_schema,
};
use crate::metadata::parse::{
    parse_catalogs, parse_columns, parse_columns_as_fields, parse_schemas, parse_tables,
};
use crate::statement::Statement;
use adbc_core::error::Result;
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue};
use adbc_core::schemas::GET_TABLE_TYPES_SCHEMA;
use adbc_core::Optionable;
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
            builder.add_string(u32::from(&InfoCode::DriverName), "Databricks ADBC Driver");
        }
        if return_all || codes.contains(&InfoCode::DriverVersion) {
            builder.add_string(
                u32::from(&InfoCode::DriverVersion),
                env!("CARGO_PKG_VERSION"),
            );
        }
        if return_all || codes.contains(&InfoCode::VendorName) {
            builder.add_string(u32::from(&InfoCode::VendorName), "Databricks");
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
        match depth {
            ObjectDepth::Catalogs => {
                let result = self
                    .runtime
                    .block_on(self.client.list_catalogs(&self.session_id))
                    .map_err(|e| e.to_adbc())?;
                let catalogs = parse_catalogs(result).map_err(|e| e.to_adbc())?;

                // Client-side catalog pattern filtering (SHOW CATALOGS has no LIKE clause)
                let catalogs = filter_by_pattern(catalogs, catalog, |c| &c.catalog_name);
                debug!("get_objects(Catalogs): found {} catalogs", catalogs.len());

                let reader = build_get_objects_catalogs(catalogs).map_err(|e| e.to_adbc())?;
                collect_reader(reader).map_err(|e| e.to_adbc())
            }

            ObjectDepth::Schemas => {
                let result = self
                    .runtime
                    .block_on(
                        self.client
                            .list_schemas(&self.session_id, catalog, db_schema),
                    )
                    .map_err(|e| e.to_adbc())?;
                let schemas = parse_schemas(result, catalog).map_err(|e| e.to_adbc())?;
                debug!("get_objects(Schemas): found {} schemas", schemas.len());

                let grouped = group_schemas_by_catalog(schemas);
                let reader = build_get_objects_schemas(grouped).map_err(|e| e.to_adbc())?;
                collect_reader(reader).map_err(|e| e.to_adbc())
            }

            ObjectDepth::Tables => {
                let result = self
                    .runtime
                    .block_on(self.client.list_tables(
                        &self.session_id,
                        catalog,
                        db_schema,
                        table_name,
                        table_type.as_deref(),
                    ))
                    .map_err(|e| e.to_adbc())?;
                let mut tables = parse_tables(result).map_err(|e| e.to_adbc())?;

                // Client-side table type filtering
                if let Some(ref types) = table_type {
                    tables.retain(|t| types.iter().any(|tt| t.table_type.eq_ignore_ascii_case(tt)));
                }
                debug!(
                    "get_objects(Tables): found {} tables after filtering",
                    tables.len()
                );

                let grouped = group_tables_by_catalog_schema(tables);
                let reader = build_get_objects_tables(grouped).map_err(|e| e.to_adbc())?;
                collect_reader(reader).map_err(|e| e.to_adbc())
            }

            ObjectDepth::Columns | ObjectDepth::All => {
                // Step 1: Get tables (for table_type info)
                let tables_result = self
                    .runtime
                    .block_on(self.client.list_tables(
                        &self.session_id,
                        catalog,
                        db_schema,
                        table_name,
                        table_type.as_deref(),
                    ))
                    .map_err(|e| e.to_adbc())?;
                let mut tables = parse_tables(tables_result).map_err(|e| e.to_adbc())?;

                // Client-side table type filtering
                if let Some(ref types) = table_type {
                    tables.retain(|t| types.iter().any(|tt| t.table_type.eq_ignore_ascii_case(tt)));
                }
                debug!(
                    "get_objects(All): found {} tables after filtering",
                    tables.len()
                );

                // Step 2: Get distinct catalogs from tables result
                let distinct_catalogs: Vec<String> = {
                    let mut cats: Vec<String> = tables
                        .iter()
                        .map(|t| t.catalog_name.clone())
                        .collect::<HashSet<_>>()
                        .into_iter()
                        .collect();
                    cats.sort();
                    cats
                };

                // Step 3: Fetch columns per catalog (parallel)
                debug!(
                    "get_objects(All): fetching columns for {} distinct catalogs: {:?}",
                    distinct_catalogs.len(),
                    distinct_catalogs
                );
                let all_columns = self
                    .runtime
                    .block_on(async {
                        let mut handles = Vec::new();
                        for cat in &distinct_catalogs {
                            let client = self.client.clone();
                            let session_id = self.session_id.clone();
                            let cat = cat.clone();
                            let schema_pattern = db_schema.map(|s| s.to_string());
                            let table_pattern = table_name.map(|s| s.to_string());
                            let col_pattern = column_name.map(|s| s.to_string());

                            handles.push(tokio::spawn(async move {
                                client
                                    .list_columns(
                                        &session_id,
                                        &cat,
                                        schema_pattern.as_deref(),
                                        table_pattern.as_deref(),
                                        col_pattern.as_deref(),
                                    )
                                    .await
                            }));
                        }

                        let mut all_columns = Vec::new();
                        for handle in handles {
                            let result = handle.await.map_err(|e| {
                                crate::error::DatabricksErrorHelper::io()
                                    .message(format!("Column fetch task failed: {}", e))
                            })?;
                            let cols = parse_columns(result?)?;
                            all_columns.extend(cols);
                        }
                        Ok::<_, crate::error::Error>(all_columns)
                    })
                    .map_err(|e| e.to_adbc())?;

                // Step 4: Group and build
                debug!(
                    "get_objects(All): fetched {} total columns",
                    all_columns.len()
                );
                let grouped = group_tables_and_columns(tables, all_columns);
                let reader = build_get_objects_all(grouped).map_err(|e| e.to_adbc())?;
                collect_reader(reader).map_err(|e| e.to_adbc())
            }
        }
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        debug!(
            "get_table_schema: catalog={:?}, db_schema={:?}, table_name={:?}",
            catalog, db_schema, table_name
        );
        // SHOW COLUMNS IN CATALOG `{cat}` requires a catalog.
        // If catalog is not provided, discover it via list_tables.
        let resolved_catalog = match catalog {
            Some(c) if !c.is_empty() => c.to_string(),
            _ => {
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

        debug!("get_table_schema: resolved catalog={:?}", resolved_catalog);

        let result = self
            .runtime
            .block_on(self.client.list_columns(
                &self.session_id,
                &resolved_catalog,
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

        debug!(
            "get_table_schema: found {} fields for {}.{}",
            fields.len(),
            resolved_catalog,
            table_name
        );
        Ok(Schema::new(fields))
    }

    fn get_table_types(&self) -> Result<impl RecordBatchReader + Send> {
        let table_types = self.client.list_table_types();

        let array = StringArray::from(table_types);
        let batch = RecordBatch::try_new(GET_TABLE_TYPES_SCHEMA.clone(), vec![Arc::new(array)])
            .map_err(|e| {
                DatabricksErrorHelper::io()
                    .message(format!("Failed to build table types result: {}", e))
                    .to_adbc()
            })?;

        Ok(RecordBatchIterator::new(
            vec![Ok(batch)].into_iter(),
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
    // Note: Full connection tests require mock DatabricksClient
    // Integration tests should be added in a separate test module
}
