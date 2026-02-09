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
//! Builds SHOW SQL commands (SHOW CATALOGS, SHOW SCHEMAS, SHOW TABLES,
//! SHOW COLUMNS, SHOW KEYS, SHOW FOREIGN KEYS) with optional pattern
//! filters and identifier escaping.
//!
//! SQL commands match the `databricks-jdbc` `CommandConstants.java` patterns.

use crate::error::{DatabricksErrorHelper, Result};
use driverbase::error::ErrorHelper;

/// Builds SQL commands for metadata queries.
///
/// Uses the builder pattern to construct SHOW SQL commands with optional
/// catalog, schema, table, and column pattern filters.
///
/// # Examples
///
/// ```ignore
/// let sql = SqlCommandBuilder::new()
///     .with_catalog(Some("my_catalog"))
///     .with_schema_pattern(Some("my_schema%"))
///     .build_show_tables();
/// assert_eq!(sql, "SHOW TABLES IN CATALOG `my_catalog` SCHEMA LIKE 'my_schema%'");
/// ```
pub struct SqlCommandBuilder {
    catalog: Option<String>,
    schema_pattern: Option<String>,
    table_pattern: Option<String>,
    column_pattern: Option<String>,
}

impl SqlCommandBuilder {
    /// Create a new builder with no filters set.
    pub fn new() -> Self {
        Self {
            catalog: None,
            schema_pattern: None,
            table_pattern: None,
            column_pattern: None,
        }
    }

    /// Set the catalog name for the query.
    pub fn with_catalog(mut self, catalog: Option<&str>) -> Self {
        self.catalog = catalog.map(|s| s.to_string());
        self
    }

    /// Set the schema pattern filter (JDBC/SQL LIKE syntax with `%` and `_`).
    pub fn with_schema_pattern(mut self, pattern: Option<&str>) -> Self {
        self.schema_pattern = pattern.map(|p| Self::jdbc_to_sql_pattern(p));
        self
    }

    /// Set the table pattern filter (JDBC/SQL LIKE syntax with `%` and `_`).
    pub fn with_table_pattern(mut self, pattern: Option<&str>) -> Self {
        self.table_pattern = pattern.map(|p| Self::jdbc_to_sql_pattern(p));
        self
    }

    /// Set the column pattern filter (JDBC/SQL LIKE syntax with `%` and `_`).
    pub fn with_column_pattern(mut self, pattern: Option<&str>) -> Self {
        self.column_pattern = pattern.map(|p| Self::jdbc_to_sql_pattern(p));
        self
    }

    /// Build `SHOW CATALOGS` command. No filters are applied.
    pub fn build_show_catalogs(&self) -> String {
        "SHOW CATALOGS".to_string()
    }

    /// Build `SHOW SCHEMAS` command.
    ///
    /// When catalog is `None` or a wildcard (`%`, `*`, empty), uses
    /// `SHOW SCHEMAS IN ALL CATALOGS`. Otherwise uses `SHOW SCHEMAS IN \`{catalog}\``.
    /// Appends `LIKE '{pattern}'` if a schema pattern is set.
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

    /// Build `SHOW TABLES` command.
    ///
    /// When catalog is `None` or a wildcard, uses `SHOW TABLES IN ALL CATALOGS`.
    /// Otherwise uses `SHOW TABLES IN CATALOG \`{catalog}\``.
    /// Appends `SCHEMA LIKE` and `LIKE` filters if set.
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

    /// Build `SHOW COLUMNS` command.
    ///
    /// Requires a catalog — `SHOW COLUMNS IN ALL CATALOGS` is not yet available
    /// server-side. Returns an error if catalog is `None`.
    /// Appends `SCHEMA LIKE`, `TABLE LIKE`, and `LIKE` filters if set.
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

    /// Build `SHOW KEYS` command for primary keys.
    ///
    /// This is a static method since primary key queries require all three
    /// identifiers (catalog, schema, table) and no patterns.
    pub fn build_show_primary_keys(catalog: &str, schema: &str, table: &str) -> String {
        format!(
            "SHOW KEYS IN CATALOG {} IN SCHEMA {} IN TABLE {}",
            Self::escape_identifier(catalog),
            Self::escape_identifier(schema),
            Self::escape_identifier(table)
        )
    }

    /// Build `SHOW FOREIGN KEYS` command.
    ///
    /// This is a static method since foreign key queries require all three
    /// identifiers (catalog, schema, table) and no patterns.
    pub fn build_show_foreign_keys(catalog: &str, schema: &str, table: &str) -> String {
        format!(
            "SHOW FOREIGN KEYS IN CATALOG {} IN SCHEMA {} IN TABLE {}",
            Self::escape_identifier(catalog),
            Self::escape_identifier(schema),
            Self::escape_identifier(table)
        )
    }

    /// Returns `true` if the value is `None`, empty, or a wildcard (`%` or `*`).
    ///
    /// Matches the behavior of `WildcardUtil.isNullOrWildcard()` in the JDBC driver.
    fn is_null_or_wildcard(value: &Option<String>) -> bool {
        match value {
            None => true,
            Some(v) => v.is_empty() || v == "%" || v == "*",
        }
    }

    /// Escape an identifier for use in SQL by wrapping in backticks.
    ///
    /// Any backticks within the identifier are doubled (`` ` `` → `` `` ``).
    fn escape_identifier(name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
    }

    /// Convert a JDBC/ADBC pattern to a SQL LIKE pattern.
    ///
    /// JDBC/ADBC patterns use `%` for multi-char wildcard and `_` for
    /// single-char wildcard, which is the same as SQL LIKE syntax.
    /// This function escapes backticks within the pattern.
    fn jdbc_to_sql_pattern(pattern: &str) -> String {
        pattern.replace('`', "``")
    }
}

impl Default for SqlCommandBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_command_builder_show_catalogs() {
        let sql = SqlCommandBuilder::new().build_show_catalogs();
        assert_eq!(sql, "SHOW CATALOGS");
    }

    #[test]
    fn test_sql_command_builder_show_schemas_with_catalog() {
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("my_catalog"))
            .build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN `my_catalog`");

        // With schema pattern
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("my_catalog"))
            .with_schema_pattern(Some("my_schema%"))
            .build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN `my_catalog` LIKE 'my_schema%'");
    }

    #[test]
    fn test_sql_command_builder_show_schemas_all_catalogs() {
        let sql = SqlCommandBuilder::new().build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN ALL CATALOGS");

        // With schema pattern
        let sql = SqlCommandBuilder::new()
            .with_schema_pattern(Some("test_%"))
            .build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN ALL CATALOGS LIKE 'test_%'");
    }

    #[test]
    fn test_sql_command_builder_show_schemas_null_or_wildcard() {
        // None catalog → ALL CATALOGS
        let sql = SqlCommandBuilder::new()
            .with_catalog(None)
            .build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN ALL CATALOGS");

        // Empty catalog → ALL CATALOGS
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some(""))
            .build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN ALL CATALOGS");

        // "%" catalog → ALL CATALOGS
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("%"))
            .build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN ALL CATALOGS");

        // "*" catalog → ALL CATALOGS
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("*"))
            .build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN ALL CATALOGS");
    }

    #[test]
    fn test_sql_command_builder_show_tables_with_patterns() {
        // No filters → ALL CATALOGS
        let sql = SqlCommandBuilder::new().build_show_tables();
        assert_eq!(sql, "SHOW TABLES IN ALL CATALOGS");

        // With catalog
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("my_catalog"))
            .build_show_tables();
        assert_eq!(sql, "SHOW TABLES IN CATALOG `my_catalog`");

        // With all filters
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("my_catalog"))
            .with_schema_pattern(Some("my_schema%"))
            .with_table_pattern(Some("my_table%"))
            .build_show_tables();
        assert_eq!(
            sql,
            "SHOW TABLES IN CATALOG `my_catalog` SCHEMA LIKE 'my_schema%' LIKE 'my_table%'"
        );

        // Wildcard catalog with patterns
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("%"))
            .with_schema_pattern(Some("default"))
            .with_table_pattern(Some("users%"))
            .build_show_tables();
        assert_eq!(
            sql,
            "SHOW TABLES IN ALL CATALOGS SCHEMA LIKE 'default' LIKE 'users%'"
        );
    }

    #[test]
    fn test_sql_command_builder_show_columns_requires_catalog() {
        // No catalog → error
        let result = SqlCommandBuilder::new().build_show_columns();
        assert!(result.is_err());
        let err = result.unwrap_err();
        let display = format!("{err}");
        assert!(display.contains("catalog is required"));

        // With catalog → success
        let result = SqlCommandBuilder::new()
            .with_catalog(Some("my_catalog"))
            .build_show_columns();
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            "SHOW COLUMNS IN CATALOG `my_catalog`"
        );
    }

    #[test]
    fn test_sql_command_builder_show_columns_with_patterns() {
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("my_catalog"))
            .with_schema_pattern(Some("my_schema"))
            .with_table_pattern(Some("my_table"))
            .with_column_pattern(Some("col%"))
            .build_show_columns()
            .unwrap();
        assert_eq!(
            sql,
            "SHOW COLUMNS IN CATALOG `my_catalog` SCHEMA LIKE 'my_schema' TABLE LIKE 'my_table' LIKE 'col%'"
        );

        // With only some patterns
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("my_catalog"))
            .with_column_pattern(Some("id"))
            .build_show_columns()
            .unwrap();
        assert_eq!(sql, "SHOW COLUMNS IN CATALOG `my_catalog` LIKE 'id'");
    }

    #[test]
    fn test_sql_command_builder_escape_identifiers() {
        // Normal identifier
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("my_catalog"))
            .build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN `my_catalog`");

        // Identifier with backtick
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("my`catalog"))
            .build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN `my``catalog`");

        // Primary keys with backtick escaping
        let sql = SqlCommandBuilder::build_show_primary_keys("cat`1", "sch`2", "tbl`3");
        assert_eq!(
            sql,
            "SHOW KEYS IN CATALOG `cat``1` IN SCHEMA `sch``2` IN TABLE `tbl``3`"
        );

        // Foreign keys with backtick escaping
        let sql = SqlCommandBuilder::build_show_foreign_keys("cat`1", "sch`2", "tbl`3");
        assert_eq!(
            sql,
            "SHOW FOREIGN KEYS IN CATALOG `cat``1` IN SCHEMA `sch``2` IN TABLE `tbl``3`"
        );
    }

    #[test]
    fn test_sql_command_builder_show_primary_keys() {
        let sql = SqlCommandBuilder::build_show_primary_keys("my_catalog", "my_schema", "my_table");
        assert_eq!(
            sql,
            "SHOW KEYS IN CATALOG `my_catalog` IN SCHEMA `my_schema` IN TABLE `my_table`"
        );
    }

    #[test]
    fn test_sql_command_builder_show_foreign_keys() {
        let sql =
            SqlCommandBuilder::build_show_foreign_keys("my_catalog", "my_schema", "my_table");
        assert_eq!(
            sql,
            "SHOW FOREIGN KEYS IN CATALOG `my_catalog` IN SCHEMA `my_schema` IN TABLE `my_table`"
        );
    }

    #[test]
    fn test_is_null_or_wildcard() {
        assert!(SqlCommandBuilder::is_null_or_wildcard(&None));
        assert!(SqlCommandBuilder::is_null_or_wildcard(&Some("".to_string())));
        assert!(SqlCommandBuilder::is_null_or_wildcard(&Some("%".to_string())));
        assert!(SqlCommandBuilder::is_null_or_wildcard(&Some("*".to_string())));
        assert!(!SqlCommandBuilder::is_null_or_wildcard(&Some(
            "my_catalog".to_string()
        )));
        assert!(!SqlCommandBuilder::is_null_or_wildcard(&Some(
            "main".to_string()
        )));
    }

    #[test]
    fn test_escape_identifier() {
        assert_eq!(SqlCommandBuilder::escape_identifier("simple"), "`simple`");
        assert_eq!(
            SqlCommandBuilder::escape_identifier("with`backtick"),
            "`with``backtick`"
        );
        assert_eq!(
            SqlCommandBuilder::escape_identifier("multiple``backticks"),
            "`multiple````backticks`"
        );
        assert_eq!(SqlCommandBuilder::escape_identifier(""), "``");
    }

    #[test]
    fn test_jdbc_to_sql_pattern() {
        // Normal pattern passes through
        assert_eq!(SqlCommandBuilder::jdbc_to_sql_pattern("test%"), "test%");
        assert_eq!(SqlCommandBuilder::jdbc_to_sql_pattern("test_"), "test_");
        assert_eq!(SqlCommandBuilder::jdbc_to_sql_pattern("%"), "%");

        // Backticks are escaped
        assert_eq!(
            SqlCommandBuilder::jdbc_to_sql_pattern("test`pattern"),
            "test``pattern"
        );
    }
}
