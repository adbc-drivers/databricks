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
//! This module provides the [`SqlCommandBuilder`] struct that generates SQL commands
//! for metadata queries such as `SHOW CATALOGS`, `SHOW SCHEMAS`, `SHOW TABLES`, etc.
//! The builder handles pattern escaping, identifier quoting, and constructing
//! proper SQL filters based on the databricks-jdbc implementation.
//!
//! ## Example
//!
//! ```
//! use databricks_adbc::metadata::SqlCommandBuilder;
//!
//! let builder = SqlCommandBuilder::new()
//!     .with_catalog(Some("main"))
//!     .with_schema_pattern(Some("default%"));
//!
//! let sql = builder.build_show_schemas();
//! assert_eq!(sql, "SHOW SCHEMAS IN `main` LIKE 'default%'");
//! ```

use crate::error::{DatabricksErrorHelper, Result};
use driverbase::error::ErrorHelper;

/// Builds SQL commands for metadata queries.
///
/// This struct uses a builder pattern to configure filters for metadata queries
/// and then generate the appropriate SQL commands.
///
/// ## SQL Commands Generated
///
/// - `SHOW CATALOGS` - List all catalogs
/// - `SHOW SCHEMAS IN \`{catalog}\`` - List schemas in a specific catalog
/// - `SHOW SCHEMAS IN ALL CATALOGS` - List schemas across all catalogs
/// - `SHOW TABLES IN CATALOG \`{catalog}\`` - List tables in a catalog
/// - `SHOW TABLES IN ALL CATALOGS` - List tables across all catalogs
/// - `SHOW COLUMNS IN CATALOG \`{catalog}\`` - List columns in a catalog
/// - `SHOW KEYS IN CATALOG \`{catalog}\` IN SCHEMA \`{schema}\` IN TABLE \`{table}\`` - List primary keys
/// - `SHOW FOREIGN KEYS IN CATALOG \`{catalog}\` IN SCHEMA \`{schema}\` IN TABLE \`{table}\`` - List foreign keys
#[derive(Debug, Clone, Default)]
pub struct SqlCommandBuilder {
    /// The catalog to filter by.
    catalog: Option<String>,
    /// The schema pattern for LIKE filtering.
    schema_pattern: Option<String>,
    /// The table pattern for LIKE filtering.
    table_pattern: Option<String>,
    /// The column pattern for LIKE filtering.
    column_pattern: Option<String>,
}

impl SqlCommandBuilder {
    /// Creates a new `SqlCommandBuilder` with no filters set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the catalog filter.
    ///
    /// When set, queries will be scoped to the specified catalog.
    /// If `None` or set to "%", queries will operate on all catalogs.
    pub fn with_catalog(mut self, catalog: Option<&str>) -> Self {
        self.catalog = catalog.map(|s| s.to_string());
        self
    }

    /// Sets the schema pattern for LIKE filtering.
    ///
    /// The pattern uses SQL LIKE syntax where `%` matches any sequence of
    /// characters and `_` matches any single character.
    pub fn with_schema_pattern(mut self, pattern: Option<&str>) -> Self {
        self.schema_pattern = pattern.map(Self::jdbc_to_sql_pattern);
        self
    }

    /// Sets the table pattern for LIKE filtering.
    ///
    /// The pattern uses SQL LIKE syntax where `%` matches any sequence of
    /// characters and `_` matches any single character.
    pub fn with_table_pattern(mut self, pattern: Option<&str>) -> Self {
        self.table_pattern = pattern.map(Self::jdbc_to_sql_pattern);
        self
    }

    /// Sets the column pattern for LIKE filtering.
    ///
    /// The pattern uses SQL LIKE syntax where `%` matches any sequence of
    /// characters and `_` matches any single character.
    pub fn with_column_pattern(mut self, pattern: Option<&str>) -> Self {
        self.column_pattern = pattern.map(Self::jdbc_to_sql_pattern);
        self
    }

    /// Converts a JDBC/ADBC pattern to a SQL LIKE pattern.
    ///
    /// JDBC uses `%` for multi-character wildcard and `_` for single character,
    /// which is the same as SQL LIKE. This method escapes backticks in the pattern
    /// to prevent SQL injection.
    pub fn jdbc_to_sql_pattern(pattern: &str) -> String {
        // Escape backticks in the pattern
        pattern.replace('`', "``")
    }

    /// Escapes an identifier for use in SQL by backtick-quoting it.
    ///
    /// This method handles identifiers that contain backticks by doubling them.
    ///
    /// # Examples
    ///
    /// - `my_table` becomes `` `my_table` ``
    /// - `my`table` becomes `` `my``table` ``
    pub fn escape_identifier(name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
    }

    /// Builds the SQL command to list all catalogs.
    ///
    /// Returns `SHOW CATALOGS`.
    pub fn build_show_catalogs(&self) -> String {
        "SHOW CATALOGS".to_string()
    }

    /// Builds the SQL command to list schemas.
    ///
    /// If a catalog is set and is not empty or "%", returns:
    /// `SHOW SCHEMAS IN \`{catalog}\` [LIKE '{pattern}']`
    ///
    /// Otherwise, returns:
    /// `SHOW SCHEMAS IN ALL CATALOGS [LIKE '{pattern}']`
    pub fn build_show_schemas(&self) -> String {
        let mut sql = match &self.catalog {
            Some(cat) if !cat.is_empty() && cat != "%" => {
                format!("SHOW SCHEMAS IN {}", Self::escape_identifier(cat))
            }
            _ => "SHOW SCHEMAS IN ALL CATALOGS".to_string(),
        };

        if let Some(ref pattern) = self.schema_pattern {
            sql.push_str(&format!(" LIKE '{}'", pattern));
        }

        sql
    }

    /// Builds the SQL command to list tables.
    ///
    /// If a catalog is set and is not empty or "%", returns:
    /// `SHOW TABLES IN CATALOG \`{catalog}\` [SCHEMA LIKE '{schema_pattern}'] [LIKE '{table_pattern}']`
    ///
    /// Otherwise, returns:
    /// `SHOW TABLES IN ALL CATALOGS [SCHEMA LIKE '{schema_pattern}'] [LIKE '{table_pattern}']`
    pub fn build_show_tables(&self) -> String {
        let mut sql = match &self.catalog {
            Some(cat) if !cat.is_empty() && cat != "%" => {
                format!("SHOW TABLES IN CATALOG {}", Self::escape_identifier(cat))
            }
            _ => "SHOW TABLES IN ALL CATALOGS".to_string(),
        };

        if let Some(ref pattern) = self.schema_pattern {
            sql.push_str(&format!(" SCHEMA LIKE '{}'", pattern));
        }

        if let Some(ref pattern) = self.table_pattern {
            sql.push_str(&format!(" LIKE '{}'", pattern));
        }

        sql
    }

    /// Builds the SQL command to list columns.
    ///
    /// Returns:
    /// `SHOW COLUMNS IN CATALOG \`{catalog}\` [SCHEMA LIKE '{schema_pattern}'] [TABLE LIKE '{table_pattern}'] [LIKE '{column_pattern}']`
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog is not set, as it is required for `SHOW COLUMNS`.
    pub fn build_show_columns(&self) -> Result<String> {
        let catalog = self.catalog.as_ref().ok_or_else(|| {
            DatabricksErrorHelper::invalid_argument()
                .message("Catalog is required for SHOW COLUMNS")
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

    /// Builds the SQL command to list primary keys for a specific table.
    ///
    /// Returns:
    /// `SHOW KEYS IN CATALOG \`{catalog}\` IN SCHEMA \`{schema}\` IN TABLE \`{table}\``
    ///
    /// This is a static method as it requires all three identifiers.
    pub fn build_show_primary_keys(catalog: &str, schema: &str, table: &str) -> String {
        format!(
            "SHOW KEYS IN CATALOG {} IN SCHEMA {} IN TABLE {}",
            Self::escape_identifier(catalog),
            Self::escape_identifier(schema),
            Self::escape_identifier(table)
        )
    }

    /// Builds the SQL command to list foreign keys for a specific table.
    ///
    /// Returns:
    /// `SHOW FOREIGN KEYS IN CATALOG \`{catalog}\` IN SCHEMA \`{schema}\` IN TABLE \`{table}\``
    ///
    /// This is a static method as it requires all three identifiers.
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
    fn test_sql_command_builder_show_catalogs() {
        let builder = SqlCommandBuilder::new();
        assert_eq!(builder.build_show_catalogs(), "SHOW CATALOGS");

        // Should be the same regardless of any filters set
        let builder_with_filters = SqlCommandBuilder::new()
            .with_catalog(Some("main"))
            .with_schema_pattern(Some("test%"));
        assert_eq!(builder_with_filters.build_show_catalogs(), "SHOW CATALOGS");
    }

    #[test]
    fn test_sql_command_builder_show_schemas_with_catalog() {
        let builder = SqlCommandBuilder::new().with_catalog(Some("main"));
        assert_eq!(builder.build_show_schemas(), "SHOW SCHEMAS IN `main`");

        // With schema pattern
        let builder_with_pattern = SqlCommandBuilder::new()
            .with_catalog(Some("main"))
            .with_schema_pattern(Some("default%"));
        assert_eq!(
            builder_with_pattern.build_show_schemas(),
            "SHOW SCHEMAS IN `main` LIKE 'default%'"
        );
    }

    #[test]
    fn test_sql_command_builder_show_schemas_all_catalogs() {
        // No catalog set
        let builder = SqlCommandBuilder::new();
        assert_eq!(builder.build_show_schemas(), "SHOW SCHEMAS IN ALL CATALOGS");

        // Empty catalog
        let builder_empty = SqlCommandBuilder::new().with_catalog(Some(""));
        assert_eq!(
            builder_empty.build_show_schemas(),
            "SHOW SCHEMAS IN ALL CATALOGS"
        );

        // Wildcard catalog
        let builder_wildcard = SqlCommandBuilder::new().with_catalog(Some("%"));
        assert_eq!(
            builder_wildcard.build_show_schemas(),
            "SHOW SCHEMAS IN ALL CATALOGS"
        );

        // With pattern
        let builder_with_pattern =
            SqlCommandBuilder::new().with_schema_pattern(Some("information%"));
        assert_eq!(
            builder_with_pattern.build_show_schemas(),
            "SHOW SCHEMAS IN ALL CATALOGS LIKE 'information%'"
        );
    }

    #[test]
    fn test_sql_command_builder_show_tables_with_patterns() {
        // Catalog only
        let builder = SqlCommandBuilder::new().with_catalog(Some("main"));
        assert_eq!(builder.build_show_tables(), "SHOW TABLES IN CATALOG `main`");

        // Catalog + schema pattern
        let builder_with_schema = SqlCommandBuilder::new()
            .with_catalog(Some("main"))
            .with_schema_pattern(Some("default"));
        assert_eq!(
            builder_with_schema.build_show_tables(),
            "SHOW TABLES IN CATALOG `main` SCHEMA LIKE 'default'"
        );

        // Catalog + schema + table patterns
        let builder_full = SqlCommandBuilder::new()
            .with_catalog(Some("main"))
            .with_schema_pattern(Some("default"))
            .with_table_pattern(Some("user%"));
        assert_eq!(
            builder_full.build_show_tables(),
            "SHOW TABLES IN CATALOG `main` SCHEMA LIKE 'default' LIKE 'user%'"
        );

        // All catalogs with patterns
        let builder_all = SqlCommandBuilder::new()
            .with_schema_pattern(Some("public"))
            .with_table_pattern(Some("%_events"));
        assert_eq!(
            builder_all.build_show_tables(),
            "SHOW TABLES IN ALL CATALOGS SCHEMA LIKE 'public' LIKE '%_events'"
        );
    }

    #[test]
    fn test_sql_command_builder_show_columns() {
        // With catalog only
        let builder = SqlCommandBuilder::new().with_catalog(Some("main"));
        assert_eq!(
            builder.build_show_columns().unwrap(),
            "SHOW COLUMNS IN CATALOG `main`"
        );

        // With all filters
        let builder_full = SqlCommandBuilder::new()
            .with_catalog(Some("main"))
            .with_schema_pattern(Some("default"))
            .with_table_pattern(Some("users"))
            .with_column_pattern(Some("email%"));
        assert_eq!(
            builder_full.build_show_columns().unwrap(),
            "SHOW COLUMNS IN CATALOG `main` SCHEMA LIKE 'default' TABLE LIKE 'users' LIKE 'email%'"
        );

        // Without catalog should fail
        let builder_no_catalog = SqlCommandBuilder::new();
        let result = builder_no_catalog.build_show_columns();
        assert!(result.is_err());
    }

    #[test]
    fn test_sql_command_builder_escape_identifiers() {
        // Simple identifier
        assert_eq!(
            SqlCommandBuilder::escape_identifier("my_table"),
            "`my_table`"
        );

        // Identifier with backtick
        assert_eq!(
            SqlCommandBuilder::escape_identifier("my`table"),
            "`my``table`"
        );

        // Identifier with multiple backticks
        assert_eq!(
            SqlCommandBuilder::escape_identifier("my``table"),
            "`my````table`"
        );

        // Empty identifier
        assert_eq!(SqlCommandBuilder::escape_identifier(""), "``");

        // Identifier with spaces
        assert_eq!(
            SqlCommandBuilder::escape_identifier("my table"),
            "`my table`"
        );
    }

    #[test]
    fn test_sql_command_builder_with_special_characters() {
        // Catalog with special characters
        let builder = SqlCommandBuilder::new().with_catalog(Some("my`catalog"));
        assert_eq!(
            builder.build_show_schemas(),
            "SHOW SCHEMAS IN `my``catalog`"
        );

        // Pattern with special SQL characters
        let builder_pattern = SqlCommandBuilder::new()
            .with_catalog(Some("main"))
            .with_schema_pattern(Some("test_schema%"));
        assert_eq!(
            builder_pattern.build_show_schemas(),
            "SHOW SCHEMAS IN `main` LIKE 'test_schema%'"
        );

        // Pattern with backtick
        let builder_backtick = SqlCommandBuilder::new()
            .with_catalog(Some("main"))
            .with_schema_pattern(Some("schema`name"));
        assert_eq!(
            builder_backtick.build_show_schemas(),
            "SHOW SCHEMAS IN `main` LIKE 'schema``name'"
        );
    }

    #[test]
    fn test_build_show_primary_keys() {
        let sql = SqlCommandBuilder::build_show_primary_keys("main", "default", "users");
        assert_eq!(
            sql,
            "SHOW KEYS IN CATALOG `main` IN SCHEMA `default` IN TABLE `users`"
        );

        // With special characters
        let sql_special =
            SqlCommandBuilder::build_show_primary_keys("my`catalog", "my`schema", "my`table");
        assert_eq!(
            sql_special,
            "SHOW KEYS IN CATALOG `my``catalog` IN SCHEMA `my``schema` IN TABLE `my``table`"
        );
    }

    #[test]
    fn test_build_show_foreign_keys() {
        let sql = SqlCommandBuilder::build_show_foreign_keys("main", "default", "orders");
        assert_eq!(
            sql,
            "SHOW FOREIGN KEYS IN CATALOG `main` IN SCHEMA `default` IN TABLE `orders`"
        );

        // With special characters
        let sql_special =
            SqlCommandBuilder::build_show_foreign_keys("my`catalog", "my`schema", "my`table");
        assert_eq!(
            sql_special,
            "SHOW FOREIGN KEYS IN CATALOG `my``catalog` IN SCHEMA `my``schema` IN TABLE `my``table`"
        );
    }

    #[test]
    fn test_jdbc_to_sql_pattern() {
        // Regular pattern
        assert_eq!(SqlCommandBuilder::jdbc_to_sql_pattern("test%"), "test%");
        assert_eq!(SqlCommandBuilder::jdbc_to_sql_pattern("test_"), "test_");

        // Pattern with backticks
        assert_eq!(
            SqlCommandBuilder::jdbc_to_sql_pattern("test`pattern"),
            "test``pattern"
        );

        // Empty pattern
        assert_eq!(SqlCommandBuilder::jdbc_to_sql_pattern(""), "");

        // Pattern with only wildcard
        assert_eq!(SqlCommandBuilder::jdbc_to_sql_pattern("%"), "%");
    }

    #[test]
    fn test_builder_default() {
        let builder = SqlCommandBuilder::default();
        assert_eq!(builder.build_show_catalogs(), "SHOW CATALOGS");
        assert_eq!(builder.build_show_schemas(), "SHOW SCHEMAS IN ALL CATALOGS");
    }

    #[test]
    fn test_builder_clone() {
        let builder = SqlCommandBuilder::new()
            .with_catalog(Some("main"))
            .with_schema_pattern(Some("default"));

        let cloned = builder.clone();
        assert_eq!(cloned.build_show_schemas(), builder.build_show_schemas());
    }

    #[test]
    fn test_builder_debug() {
        let builder = SqlCommandBuilder::new()
            .with_catalog(Some("main"))
            .with_schema_pattern(Some("test%"));

        let debug_str = format!("{:?}", builder);
        assert!(debug_str.contains("SqlCommandBuilder"));
        assert!(debug_str.contains("main"));
        assert!(debug_str.contains("test%"));
    }
}
