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

//! Metadata service returning raw Arrow result sets from Databricks.
//!
//! Each method executes a SQL metadata query via the Databricks client and
//! returns the raw Arrow `RecordBatch` without reshaping. The caller (e.g.
//! the ODBC wrapper) is responsible for column renaming, type mapping, and
//! any other transformations.

use crate::client::{DatabricksClient, ExecuteResult};
use crate::error::{DatabricksErrorHelper, Result};
use crate::metadata::parse::parse_catalogs;
use crate::metadata::sql::SqlCommandBuilder;
use crate::types::sea::ExecuteParams;
use arrow_array::RecordBatch;
use arrow_select::concat::concat_batches;
use driverbase::error::ErrorHelper;
use std::sync::Arc;

/// Collect all batches from an `ExecuteResult` into a single `RecordBatch`.
fn collect_batches(result: ExecuteResult) -> Result<RecordBatch> {
    let mut reader = result.reader;
    let schema = reader.schema()?;
    let mut batches = Vec::new();

    while let Some(batch) = reader.next_batch()? {
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }

    if batches.is_empty() {
        Ok(RecordBatch::new_empty(schema))
    } else {
        concat_batches(&schema, &batches).map_err(|e| {
            DatabricksErrorHelper::io().message(format!("Failed to concat metadata batches: {}", e))
        })
    }
}

/// Metadata service backed by a Databricks connection.
///
/// Executes metadata SQL queries and returns raw Arrow `RecordBatch` results
/// directly from the server, without intermediate parsing or schema reshaping.
pub struct ConnectionMetadataService {
    client: Arc<dyn DatabricksClient>,
    session_id: String,
    runtime: tokio::runtime::Handle,
}

impl ConnectionMetadataService {
    /// Create a new metadata service from connection components.
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

    /// List catalogs. Returns raw Arrow data from `SHOW CATALOGS`.
    pub fn get_catalogs(&self) -> Result<RecordBatch> {
        let result = self
            .runtime
            .block_on(self.client.list_catalogs(&self.session_id))?;
        collect_batches(result)
    }

    /// List schemas. Returns raw Arrow data from `SHOW SCHEMAS`.
    pub fn get_schemas(
        &self,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
    ) -> Result<RecordBatch> {
        let result = self.runtime.block_on(self.client.list_schemas(
            &self.session_id,
            catalog,
            schema_pattern,
        ))?;
        collect_batches(result)
    }

    /// List tables. Returns raw Arrow data from `SHOW TABLES`.
    pub fn get_tables(
        &self,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        table_pattern: Option<&str>,
        table_types: Option<&[&str]>,
    ) -> Result<RecordBatch> {
        let result = self.runtime.block_on(self.client.list_tables(
            &self.session_id,
            catalog,
            schema_pattern,
            table_pattern,
            table_types,
        ))?;
        collect_batches(result)
    }

    /// List columns. Returns raw Arrow data from `SHOW COLUMNS`.
    ///
    /// `SHOW COLUMNS` requires a catalog. When catalog is `None`, empty, or a
    /// wildcard (`%` / `*`), all catalogs are discovered first and each is
    /// queried individually; the results are concatenated.
    pub fn get_columns(
        &self,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        table_pattern: Option<&str>,
        column_pattern: Option<&str>,
    ) -> Result<RecordBatch> {
        let catalogs_to_query: Vec<String> =
            if let Some(cat) = catalog.filter(|c| !c.is_empty() && *c != "%" && *c != "*") {
                vec![cat.to_string()]
            } else {
                let result = self
                    .runtime
                    .block_on(self.client.list_catalogs(&self.session_id))?;
                let cats = parse_catalogs(result)?;
                cats.into_iter().map(|c| c.catalog_name).collect()
            };

        let mut all_batches = Vec::new();
        let mut schema = None;

        for cat in &catalogs_to_query {
            let result = self.runtime.block_on(self.client.list_columns(
                &self.session_id,
                cat,
                schema_pattern,
                table_pattern,
                column_pattern,
            ))?;
            let batch = collect_batches(result)?;
            if schema.is_none() {
                schema = Some(batch.schema());
            }
            if batch.num_rows() > 0 {
                all_batches.push(batch);
            }
        }

        match schema {
            Some(s) if all_batches.is_empty() => Ok(RecordBatch::new_empty(s)),
            Some(s) => concat_batches(&s, &all_batches).map_err(|e| {
                DatabricksErrorHelper::io()
                    .message(format!("Failed to concat column batches: {}", e))
            }),
            None => Err(DatabricksErrorHelper::invalid_state()
                .message("No catalogs found for column listing")),
        }
    }

    /// List primary keys. Returns raw Arrow data from `SHOW KEYS`.
    pub fn get_primary_keys(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<RecordBatch> {
        let sql = SqlCommandBuilder::build_show_primary_keys(catalog, schema, table);
        let result = self.runtime.block_on(self.client.execute_statement(
            &self.session_id,
            &sql,
            &ExecuteParams::default(),
        ))?;
        collect_batches(result)
    }

    /// List foreign keys. Returns raw Arrow data from `SHOW FOREIGN KEYS`.
    pub fn get_foreign_keys(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<RecordBatch> {
        let sql = SqlCommandBuilder::build_show_foreign_keys(catalog, schema, table);
        let result = self.runtime.block_on(self.client.execute_statement(
            &self.session_id,
            &sql,
            &ExecuteParams::default(),
        ))?;
        collect_batches(result)
    }
}
