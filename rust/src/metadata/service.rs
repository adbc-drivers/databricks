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

//! Metadata service returning streaming Arrow result readers from Databricks.
//!
//! Each method executes a SQL metadata query via the Databricks client and
//! returns a streaming `ResultReader`. The caller (e.g. the ODBC wrapper) is
//! responsible for column renaming, type mapping, and any other transformations.

use crate::client::DatabricksClient;
use crate::error::Result;
use crate::metadata::sql::SqlCommandBuilder;
use crate::reader::{EmptyReader, ResultReader};
use crate::types::sea::ExecuteParams;
use arrow_schema::Schema;
use std::sync::Arc;

/// Metadata service backed by a Databricks connection.
///
/// Executes metadata SQL queries and returns streaming `ResultReader` instances
/// directly from the server, without intermediate buffering or schema reshaping.
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

    /// List catalogs. Returns a streaming reader over `SHOW CATALOGS` results.
    pub fn get_catalogs(&self) -> Result<Box<dyn ResultReader + Send>> {
        let result = self
            .runtime
            .block_on(self.client.list_catalogs(&self.session_id))?;
        Ok(result.reader)
    }

    /// List schemas. Returns a streaming reader over `SHOW SCHEMAS` results.
    pub fn get_schemas(
        &self,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
    ) -> Result<Box<dyn ResultReader + Send>> {
        let result = self.runtime.block_on(self.client.list_schemas(
            &self.session_id,
            catalog,
            schema_pattern,
        ))?;
        Ok(result.reader)
    }

    /// List tables. Returns a streaming reader over `SHOW TABLES` results.
    pub fn get_tables(
        &self,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        table_pattern: Option<&str>,
        table_types: Option<&[&str]>,
    ) -> Result<Box<dyn ResultReader + Send>> {
        let result = self.runtime.block_on(self.client.list_tables(
            &self.session_id,
            catalog,
            schema_pattern,
            table_pattern,
            table_types,
        ))?;
        Ok(result.reader)
    }

    /// List columns. Returns a streaming reader over `SHOW COLUMNS` results.
    ///
    /// Catalog is required for `SHOW COLUMNS`. When catalog is `None` or empty,
    /// returns an empty result set.
    pub fn get_columns(
        &self,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        table_pattern: Option<&str>,
        column_pattern: Option<&str>,
    ) -> Result<Box<dyn ResultReader + Send>> {
        let catalog = match catalog.filter(|c| !c.is_empty()) {
            Some(c) => c,
            None => return Ok(Box::new(EmptyReader::new(Arc::new(Schema::empty())))),
        };
        let result = self.runtime.block_on(self.client.list_columns(
            &self.session_id,
            Some(catalog),
            schema_pattern,
            table_pattern,
            column_pattern,
        ))?;
        Ok(result.reader)
    }

    /// List primary keys. Returns a streaming reader over `SHOW KEYS` results.
    pub fn get_primary_keys(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<Box<dyn ResultReader + Send>> {
        let sql = SqlCommandBuilder::build_show_primary_keys(catalog, schema, table);
        let result = self.runtime.block_on(self.client.execute_statement(
            &self.session_id,
            &sql,
            &ExecuteParams::default(),
        ))?;
        Ok(result.reader)
    }

    /// List foreign keys. Returns a streaming reader over `SHOW FOREIGN KEYS` results.
    pub fn get_foreign_keys(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<Box<dyn ResultReader + Send>> {
        let sql = SqlCommandBuilder::build_show_foreign_keys(catalog, schema, table);
        let result = self.runtime.block_on(self.client.execute_statement(
            &self.session_id,
            &sql,
            &ExecuteParams::default(),
        ))?;
        Ok(result.reader)
    }
}
