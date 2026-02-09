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

//! SQL command builder for metadata queries.
//!
//! Builds SHOW SQL commands based on the Databricks SQL dialect, matching
//! the patterns used by the JDBC driver (`CommandConstants.java`).

use crate::error::{DatabricksErrorHelper, Result};
use driverbase::error::ErrorHelper;

/// Builds SQL commands for metadata queries.
///
/// Uses a builder pattern to set optional filters before generating the SQL.
#[derive(Default)]
pub struct SqlCommandBuilder {
    catalog: Option<String>,
    schema_pattern: Option<String>,
    table_pattern: Option<String>,
    column_pattern: Option<String>,
}

impl SqlCommandBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_catalog(mut self, catalog: Option<&str>) -> Self {
        self.catalog = catalog.map(|s| s.to_string());
        self
    }

    pub fn with_schema_pattern(mut self, pattern: Option<&str>) -> Self {
        self.schema_pattern = pattern.map(Self::escape_like_pattern);
        self
    }

    pub fn with_table_pattern(mut self, pattern: Option<&str>) -> Self {
        self.table_pattern = pattern.map(Self::escape_like_pattern);
        self
    }

    pub fn with_column_pattern(mut self, pattern: Option<&str>) -> Self {
        self.column_pattern = pattern.map(Self::escape_like_pattern);
        self
    }

    /// Escape pattern for use in LIKE clause.
    /// ADBC/JDBC uses % for multi-char wildcard and _ for single char,
    /// which match SQL LIKE syntax. We only need to escape single quotes.
    fn escape_like_pattern(pattern: &str) -> String {
        pattern.replace('\'', "''")
    }

    /// Escape identifier for use in SQL (backtick-quote).
    fn escape_identifier(name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
    }

    /// Returns true if the value is None, empty, or a wildcard ("%" or "*").
    fn is_null_or_wildcard(value: &Option<String>) -> bool {
        match value {
            None => true,
            Some(v) => v.is_empty() || v == "%" || v == "*",
        }
    }

    pub fn build_show_catalogs(&self) -> String {
        "SHOW CATALOGS".to_string()
    }

    pub fn build_show_schemas(&self) -> String {
        let mut sql = if Self::is_null_or_wildcard(&self.catalog) {
            "SHOW SCHEMAS IN ALL CATALOGS".to_string()
        } else {
            format!(
                "SHOW SCHEMAS IN {}",
                Self::escape_identifier(self.catalog.as_ref().unwrap())
            )
        };

        if let Some(ref pattern) = self.schema_pattern {
            sql.push_str(&format!(" LIKE '{}'", pattern));
        }

        sql
    }

    pub fn build_show_tables(&self) -> String {
        let mut sql = if Self::is_null_or_wildcard(&self.catalog) {
            "SHOW TABLES IN ALL CATALOGS".to_string()
        } else {
            format!(
                "SHOW TABLES IN CATALOG {}",
                Self::escape_identifier(self.catalog.as_ref().unwrap())
            )
        };

        if let Some(ref pattern) = self.schema_pattern {
            sql.push_str(&format!(" SCHEMA LIKE '{}'", pattern));
        }

        if let Some(ref pattern) = self.table_pattern {
            sql.push_str(&format!(" LIKE '{}'", pattern));
        }

        sql
    }

    /// Build SHOW COLUMNS command. Requires a catalog — `SHOW COLUMNS IN ALL CATALOGS`
    /// is not yet available server-side.
    pub fn build_show_columns(&self) -> Result<String> {
        let catalog = self.catalog.as_ref().ok_or_else(|| {
            DatabricksErrorHelper::invalid_argument()
                .message("catalog is required for SHOW COLUMNS (ALL CATALOGS not yet supported)")
        })?;

        let mut sql = format!(
            "SHOW COLUMNS IN CATALOG {}",
            Self::escape_identifier(catalog)
        );

        if let Some(ref pattern) = self.schema_pattern {
            sql.push_str(&format!(" SCHEMA LIKE '{}'", pattern));
        }

        if let Some(ref pattern) = self.table_pattern {
            sql.push_str(&format!(" TABLE LIKE '{}'", pattern));
        }

        if let Some(ref pattern) = self.column_pattern {
            sql.push_str(&format!(" LIKE '{}'", pattern));
        }

        Ok(sql)
    }

    pub fn build_show_primary_keys(catalog: &str, schema: &str, table: &str) -> String {
        format!(
            "SHOW KEYS IN CATALOG {} IN SCHEMA {} IN TABLE {}",
            Self::escape_identifier(catalog),
            Self::escape_identifier(schema),
            Self::escape_identifier(table)
        )
    }

    pub fn build_show_foreign_keys(catalog: &str, schema: &str, table: &str) -> String {
        format!(
            "SHOW FOREIGN KEYS IN CATALOG {} IN SCHEMA {} IN TABLE {}",
            Self::escape_identifier(catalog),
            Self::escape_identifier(schema),
            Self::escape_identifier(table)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_show_catalogs() {
        let sql = SqlCommandBuilder::new().build_show_catalogs();
        assert_eq!(sql, "SHOW CATALOGS");
    }

    #[test]
    fn test_show_schemas_all_catalogs() {
        let sql = SqlCommandBuilder::new().build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN ALL CATALOGS");
    }

    #[test]
    fn test_show_schemas_with_catalog() {
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("main"))
            .build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN `main`");
    }

    #[test]
    fn test_show_schemas_with_pattern() {
        let sql = SqlCommandBuilder::new()
            .with_schema_pattern(Some("default%"))
            .build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN ALL CATALOGS LIKE 'default%'");
    }

    #[test]
    fn test_show_schemas_wildcard_catalog() {
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("%"))
            .build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN ALL CATALOGS");
    }

    #[test]
    fn test_show_schemas_empty_catalog() {
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some(""))
            .build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN ALL CATALOGS");
    }

    #[test]
    fn test_show_schemas_star_wildcard() {
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("*"))
            .build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN ALL CATALOGS");
    }

    #[test]
    fn test_show_tables_all_catalogs() {
        let sql = SqlCommandBuilder::new().build_show_tables();
        assert_eq!(sql, "SHOW TABLES IN ALL CATALOGS");
    }

    #[test]
    fn test_show_tables_with_catalog() {
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("main"))
            .build_show_tables();
        assert_eq!(sql, "SHOW TABLES IN CATALOG `main`");
    }

    #[test]
    fn test_show_tables_with_patterns() {
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("main"))
            .with_schema_pattern(Some("default"))
            .with_table_pattern(Some("my_table%"))
            .build_show_tables();
        assert_eq!(
            sql,
            "SHOW TABLES IN CATALOG `main` SCHEMA LIKE 'default' LIKE 'my_table%'"
        );
    }

    #[test]
    fn test_show_columns_requires_catalog() {
        let result = SqlCommandBuilder::new().build_show_columns();
        assert!(result.is_err());
    }

    #[test]
    fn test_show_columns_with_catalog() {
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("main"))
            .build_show_columns()
            .unwrap();
        assert_eq!(sql, "SHOW COLUMNS IN CATALOG `main`");
    }

    #[test]
    fn test_show_columns_with_all_patterns() {
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("main"))
            .with_schema_pattern(Some("default"))
            .with_table_pattern(Some("my_table"))
            .with_column_pattern(Some("id%"))
            .build_show_columns()
            .unwrap();
        assert_eq!(
            sql,
            "SHOW COLUMNS IN CATALOG `main` SCHEMA LIKE 'default' TABLE LIKE 'my_table' LIKE 'id%'"
        );
    }

    #[test]
    fn test_escape_identifier_with_backtick() {
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("my`catalog"))
            .build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN `my``catalog`");
    }

    #[test]
    fn test_escape_pattern_with_single_quote() {
        let sql = SqlCommandBuilder::new()
            .with_schema_pattern(Some("it's"))
            .build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN ALL CATALOGS LIKE 'it''s'");
    }

    #[test]
    fn test_show_primary_keys() {
        let sql = SqlCommandBuilder::build_show_primary_keys("main", "default", "my_table");
        assert_eq!(
            sql,
            "SHOW KEYS IN CATALOG `main` IN SCHEMA `default` IN TABLE `my_table`"
        );
    }

    #[test]
    fn test_show_foreign_keys() {
        let sql = SqlCommandBuilder::build_show_foreign_keys("main", "default", "my_table");
        assert_eq!(
            sql,
            "SHOW FOREIGN KEYS IN CATALOG `main` IN SCHEMA `default` IN TABLE `my_table`"
        );
    }
}
