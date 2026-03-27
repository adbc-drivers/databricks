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

/// Builds SQL commands for metadata queries.
///
/// Uses a builder pattern to set optional filters before generating the SQL.
#[derive(Default)]
pub(crate) struct SqlCommandBuilder {
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
        self.schema_pattern = pattern.map(Self::jdbc_pattern_to_hive);
        self
    }

    pub fn with_table_pattern(mut self, pattern: Option<&str>) -> Self {
        self.table_pattern = pattern.map(Self::jdbc_pattern_to_hive);
        self
    }

    pub fn with_column_pattern(mut self, pattern: Option<&str>) -> Self {
        self.column_pattern = pattern.map(Self::jdbc_pattern_to_hive);
        self
    }

    /// Convert JDBC/ADBC-style pattern to Hive-style pattern for `SHOW` commands.
    ///
    /// Use this for `SHOW TABLES/SCHEMAS/COLUMNS` patterns, which use Hive-style wildcards.
    /// For `information_schema` SQL `LIKE` patterns, use [`escape_sql_string`] instead.
    ///
    /// Databricks SHOW commands use Hive-style wildcards, not SQL LIKE wildcards:
    /// - `%` → `*` (multi-char wildcard)
    /// - `_` → `.` (single-char wildcard)
    /// - `\%` → `%` (escaped literal percent)
    /// - `\_` → `_` (escaped literal underscore)
    /// - `\\` → `\\` (escaped backslash)
    ///
    /// Also escapes single quotes for safe SQL embedding.
    /// Matches JDBC driver's `WildcardUtil.jdbcPatternToHive`.
    fn jdbc_pattern_to_hive(pattern: &str) -> String {
        let mut result = String::with_capacity(pattern.len());
        let mut escape_next = false;
        for ch in pattern.chars() {
            if ch == '\\' && !escape_next {
                escape_next = true;
                continue;
            }
            if escape_next {
                // Escaped character: emit literally
                if ch == '\\' {
                    result.push_str("\\\\");
                } else {
                    result.push(ch);
                }
                escape_next = false;
            } else {
                match ch {
                    '%' => result.push('*'),
                    '_' => result.push('.'),
                    '\'' => result.push_str("''"),
                    _ => result.push(ch),
                }
            }
        }
        result
    }

    /// Escape identifier for use in SQL (backtick-quote).
    ///
    /// Use for catalog, schema, table names in SQL statements (e.g., `` `my_catalog` ``).
    /// For string literal values inside `LIKE` clauses, use [`escape_sql_string`] instead.
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

    /// Build SHOW COLUMNS command.
    ///
    /// Catalog is a required parameter (not taken from builder state) because
    /// `SHOW COLUMNS IN ALL CATALOGS` is not supported by Databricks.
    pub fn build_show_columns(&self, catalog: &str) -> String {
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

        sql
    }

    /// Escape a value for use in a SQL string literal (inside single quotes).
    ///
    /// Use for `LIKE` pattern values in `information_schema` queries.
    /// For identifiers (catalog/schema/table names), use [`escape_identifier`] instead.
    /// For `SHOW` command patterns, use [`jdbc_pattern_to_hive`] instead.
    ///
    /// Databricks SQL treats backslash as an escape character in string literals
    /// by default, so both single quotes and backslashes must be escaped.
    ///
    /// Order matters: backslashes must be escaped first, then quotes.
    /// Reversing the order would double-escape `\'` → `\''` → `\\''`.
    fn escape_sql_string(value: &str) -> String {
        value.replace('\\', "\\\\").replace('\'', "''")
    }

    /// Resolve the catalog prefix for `information_schema` queries.
    ///
    /// - `None` → `system` (cross-catalog query)
    /// - `Some("main")` → `` `main` `` (scoped to catalog)
    ///
    /// Empty string catalog must be handled by the caller before reaching
    /// the SQL builder (return an empty result at the FFI/client layer).
    fn resolve_catalog_prefix(catalog: Option<&str>) -> String {
        match catalog {
            None => "system".to_string(),
            Some(c) => Self::escape_identifier(c),
        }
    }

    /// Build a SELECT query against `information_schema.routines` for getProcedures.
    ///
    /// Catalog resolution:
    /// - `None` → queries `system.information_schema.routines` (cross-catalog)
    /// - `Some("main")` → queries `` `main`.information_schema.routines ``
    ///
    /// Empty string catalog must be handled by the caller (return empty result
    /// at the FFI/client layer) before calling this method.
    ///
    /// Schema and procedure patterns use SQL LIKE syntax directly (no Hive conversion).
    pub fn build_get_procedures(
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        procedure_pattern: Option<&str>,
    ) -> String {
        let prefix = Self::resolve_catalog_prefix(catalog);
        let mut sql = format!(
            "SELECT routine_catalog, routine_schema, routine_name, comment, specific_name \
             FROM {}.information_schema.routines \
             WHERE routine_type = 'PROCEDURE'",
            prefix
        );

        if let Some(pattern) = schema_pattern {
            sql.push_str(&format!(
                " AND routine_schema LIKE '{}'",
                Self::escape_sql_string(pattern)
            ));
        }

        if let Some(pattern) = procedure_pattern {
            sql.push_str(&format!(
                " AND routine_name LIKE '{}'",
                Self::escape_sql_string(pattern)
            ));
        }

        sql.push_str(" ORDER BY routine_catalog, routine_schema, routine_name");
        sql
    }

    /// Build a SELECT query against `information_schema.parameters` joined with
    /// `information_schema.routines` for getProcedureColumns.
    ///
    /// Same catalog resolution as `build_get_procedures`.
    pub fn build_get_procedure_columns(
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        procedure_pattern: Option<&str>,
        column_pattern: Option<&str>,
    ) -> String {
        let prefix = Self::resolve_catalog_prefix(catalog);
        let mut sql = format!(
            "SELECT \
             p.specific_catalog, p.specific_schema, p.specific_name, \
             p.parameter_name, p.parameter_mode, p.is_result, \
             p.data_type, p.full_data_type, \
             p.numeric_precision, p.numeric_precision_radix, p.numeric_scale, \
             p.character_maximum_length, p.character_octet_length, \
             p.ordinal_position, p.parameter_default, p.comment \
             FROM {prefix}.information_schema.parameters p \
             JOIN {prefix}.information_schema.routines r \
             ON p.specific_catalog = r.specific_catalog \
             AND p.specific_schema = r.specific_schema \
             AND p.specific_name = r.specific_name \
             WHERE r.routine_type = 'PROCEDURE'"
        );

        if let Some(pattern) = schema_pattern {
            sql.push_str(&format!(
                " AND p.specific_schema LIKE '{}'",
                Self::escape_sql_string(pattern)
            ));
        }

        if let Some(pattern) = procedure_pattern {
            sql.push_str(&format!(
                " AND p.specific_name LIKE '{}'",
                Self::escape_sql_string(pattern)
            ));
        }

        if let Some(pattern) = column_pattern {
            sql.push_str(&format!(
                " AND p.parameter_name LIKE '{}'",
                Self::escape_sql_string(pattern)
            ));
        }

        sql.push_str(
            " ORDER BY p.specific_catalog, p.specific_schema, p.specific_name, p.ordinal_position",
        );
        sql
    }

    #[allow(dead_code)] // Used by ffi/metadata.rs when metadata-ffi feature is enabled
    pub fn build_show_primary_keys(catalog: &str, schema: &str, table: &str) -> String {
        format!(
            "SHOW KEYS IN CATALOG {} IN SCHEMA {} IN TABLE {}",
            Self::escape_identifier(catalog),
            Self::escape_identifier(schema),
            Self::escape_identifier(table)
        )
    }

    #[allow(dead_code)] // Used by ffi/metadata.rs when metadata-ffi feature is enabled
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
        assert_eq!(sql, "SHOW SCHEMAS IN ALL CATALOGS LIKE 'default*'");
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
            "SHOW TABLES IN CATALOG `main` SCHEMA LIKE 'default' LIKE 'my.table*'"
        );
    }

    #[test]
    fn test_show_columns_with_catalog() {
        let sql = SqlCommandBuilder::new().build_show_columns("main");
        assert_eq!(sql, "SHOW COLUMNS IN CATALOG `main`");
    }

    #[test]
    fn test_show_columns_with_all_patterns() {
        let sql = SqlCommandBuilder::new()
            .with_schema_pattern(Some("default"))
            .with_table_pattern(Some("my_table"))
            .with_column_pattern(Some("id%"))
            .build_show_columns("main");
        assert_eq!(
            sql,
            "SHOW COLUMNS IN CATALOG `main` SCHEMA LIKE 'default' TABLE LIKE 'my.table' LIKE 'id*'"
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

    // --- getProcedures / getProcedureColumns SQL builder tests ---

    #[test]
    fn test_get_procedures_null_catalog() {
        let sql = SqlCommandBuilder::build_get_procedures(None, None, None);
        assert!(sql.starts_with("SELECT routine_catalog"));
        assert!(sql.contains("FROM system.information_schema.routines"));
        assert!(sql.contains("WHERE routine_type = 'PROCEDURE'"));
        assert!(sql.ends_with("ORDER BY routine_catalog, routine_schema, routine_name"));
    }

    #[test]
    fn test_get_procedures_specific_catalog() {
        let sql = SqlCommandBuilder::build_get_procedures(Some("main"), None, None);
        assert!(sql.contains("FROM `main`.information_schema.routines"));
    }

    #[test]
    fn test_get_procedures_with_schema_pattern() {
        let sql = SqlCommandBuilder::build_get_procedures(None, Some("default%"), None);
        assert!(sql.contains("AND routine_schema LIKE 'default%'"));
    }

    #[test]
    fn test_get_procedures_with_procedure_pattern() {
        let sql = SqlCommandBuilder::build_get_procedures(None, None, Some("my_proc%"));
        assert!(sql.contains("AND routine_name LIKE 'my_proc%'"));
    }

    #[test]
    fn test_get_procedures_with_all_filters() {
        let sql =
            SqlCommandBuilder::build_get_procedures(Some("prod"), Some("public"), Some("sp_%"));
        assert!(sql.contains("FROM `prod`.information_schema.routines"));
        assert!(sql.contains("AND routine_schema LIKE 'public'"));
        assert!(sql.contains("AND routine_name LIKE 'sp_%'"));
    }

    #[test]
    fn test_get_procedures_sql_injection_escaped() {
        let sql = SqlCommandBuilder::build_get_procedures(None, Some("it's"), None);
        assert!(sql.contains("AND routine_schema LIKE 'it''s'"));
    }

    #[test]
    fn test_get_procedures_backslash_escaped() {
        let sql = SqlCommandBuilder::build_get_procedures(None, Some(r"foo\bar"), None);
        assert!(
            sql.contains(r"AND routine_schema LIKE 'foo\\bar'"),
            "Backslash should be escaped: {}",
            sql
        );
    }

    #[test]
    fn test_get_procedures_trailing_backslash_escaped() {
        let sql = SqlCommandBuilder::build_get_procedures(None, Some(r"foo\"), None);
        assert!(
            sql.contains(r"AND routine_schema LIKE 'foo\\'"),
            "Trailing backslash should be escaped: {}",
            sql
        );
    }

    #[test]
    fn test_get_procedures_catalog_with_backtick() {
        let sql = SqlCommandBuilder::build_get_procedures(Some("my`catalog"), None, None);
        assert!(sql.contains("FROM `my``catalog`.information_schema.routines"));
    }

    #[test]
    fn test_get_procedure_columns_null_catalog() {
        let sql = SqlCommandBuilder::build_get_procedure_columns(None, None, None, None);
        assert!(sql.contains("FROM system.information_schema.parameters p"));
        assert!(sql.contains("JOIN system.information_schema.routines r"));
        assert!(sql.contains("WHERE r.routine_type = 'PROCEDURE'"));
        assert!(sql.ends_with(
            "ORDER BY p.specific_catalog, p.specific_schema, p.specific_name, p.ordinal_position"
        ));
    }

    #[test]
    fn test_get_procedure_columns_specific_catalog() {
        let sql = SqlCommandBuilder::build_get_procedure_columns(Some("main"), None, None, None);
        assert!(sql.contains("FROM `main`.information_schema.parameters p"));
        assert!(sql.contains("JOIN `main`.information_schema.routines r"));
    }

    #[test]
    fn test_get_procedure_columns_with_all_filters() {
        let sql = SqlCommandBuilder::build_get_procedure_columns(
            Some("prod"),
            Some("public"),
            Some("my_proc"),
            Some("param%"),
        );
        assert!(sql.contains("FROM `prod`.information_schema.parameters p"));
        assert!(sql.contains("AND p.specific_schema LIKE 'public'"));
        assert!(sql.contains("AND p.specific_name LIKE 'my_proc'"));
        assert!(sql.contains("AND p.parameter_name LIKE 'param%'"));
    }

    #[test]
    fn test_get_procedure_columns_selects_all_needed_columns() {
        let sql = SqlCommandBuilder::build_get_procedure_columns(None, None, None, None);
        // Verify key columns are selected
        assert!(sql.contains("p.specific_catalog"));
        assert!(sql.contains("p.parameter_name"));
        assert!(sql.contains("p.parameter_mode"));
        assert!(sql.contains("p.is_result"));
        assert!(sql.contains("p.data_type"));
        assert!(sql.contains("p.full_data_type"));
        assert!(sql.contains("p.numeric_precision"));
        assert!(sql.contains("p.numeric_scale"));
        assert!(sql.contains("p.character_maximum_length"));
        assert!(sql.contains("p.character_octet_length"));
        assert!(sql.contains("p.ordinal_position"));
        assert!(sql.contains("p.parameter_default"));
        assert!(sql.contains("p.comment"));
    }

    // --- resolve_catalog_prefix edge cases ---

    #[test]
    fn test_resolve_catalog_prefix_none() {
        assert_eq!(SqlCommandBuilder::resolve_catalog_prefix(None), "system");
    }

    #[test]
    fn test_resolve_catalog_prefix_specific() {
        assert_eq!(
            SqlCommandBuilder::resolve_catalog_prefix(Some("main")),
            "`main`"
        );
    }

    #[test]
    fn test_resolve_catalog_prefix_special_chars() {
        assert_eq!(
            SqlCommandBuilder::resolve_catalog_prefix(Some("my`catalog")),
            "`my``catalog`"
        );
    }

    // --- escape_sql_string edge cases ---

    #[test]
    fn test_escape_sql_string_single_quote() {
        assert_eq!(SqlCommandBuilder::escape_sql_string("it's"), "it''s");
    }

    #[test]
    fn test_escape_sql_string_backslash() {
        assert_eq!(
            SqlCommandBuilder::escape_sql_string(r"foo\bar"),
            r"foo\\bar"
        );
    }

    #[test]
    fn test_escape_sql_string_trailing_backslash() {
        assert_eq!(SqlCommandBuilder::escape_sql_string(r"foo\"), r"foo\\");
    }

    #[test]
    fn test_escape_sql_string_both_quote_and_backslash() {
        assert_eq!(
            SqlCommandBuilder::escape_sql_string(r"it's\here"),
            r"it''s\\here"
        );
    }

    #[test]
    fn test_escape_sql_string_percent_underscore_passthrough() {
        // % and _ are SQL LIKE wildcards — they should NOT be escaped by escape_sql_string.
        // They are meaningful in LIKE patterns and passed through as-is.
        assert_eq!(
            SqlCommandBuilder::escape_sql_string("foo%bar_baz"),
            "foo%bar_baz"
        );
    }

    #[test]
    fn test_escape_sql_string_empty() {
        assert_eq!(SqlCommandBuilder::escape_sql_string(""), "");
    }

    // --- procedure_columns SQL injection in column_pattern ---

    #[test]
    fn test_get_procedure_columns_column_pattern_injection() {
        let sql = SqlCommandBuilder::build_get_procedure_columns(
            None,
            None,
            None,
            Some("'; DROP TABLE --"),
        );
        // Input: '; DROP TABLE --
        // After escaping: ''; DROP TABLE --
        // Embedded in LIKE '...': LIKE '''; DROP TABLE --'
        assert!(
            sql.contains("AND p.parameter_name LIKE '''; DROP TABLE --'"),
            "Single quotes in column_pattern should be escaped: {}",
            sql
        );
    }

    #[test]
    fn test_get_procedure_columns_column_pattern_backslash() {
        let sql =
            SqlCommandBuilder::build_get_procedure_columns(None, None, None, Some(r"param\name"));
        assert!(
            sql.contains(r"AND p.parameter_name LIKE 'param\\name'"),
            "Backslash in column_pattern should be escaped: {}",
            sql
        );
    }

    // --- foreign keys with special characters ---

    #[test]
    fn test_show_foreign_keys_special_chars_in_catalog() {
        let sql = SqlCommandBuilder::build_show_foreign_keys("my`cat", "my`sch", "my`tbl");
        assert_eq!(
            sql,
            "SHOW FOREIGN KEYS IN CATALOG `my``cat` IN SCHEMA `my``sch` IN TABLE `my``tbl`"
        );
    }

    // --- jdbc_pattern_to_hive tests (matches JDBC WildcardUtil) ---

    #[test]
    fn test_pattern_percent_to_star() {
        assert_eq!(SqlCommandBuilder::jdbc_pattern_to_hive("abc%"), "abc*");
        assert_eq!(SqlCommandBuilder::jdbc_pattern_to_hive("%abc"), "*abc");
        assert_eq!(SqlCommandBuilder::jdbc_pattern_to_hive("%abc%"), "*abc*");
    }

    #[test]
    fn test_pattern_underscore_to_dot() {
        assert_eq!(SqlCommandBuilder::jdbc_pattern_to_hive("abc_"), "abc.");
        assert_eq!(SqlCommandBuilder::jdbc_pattern_to_hive("_abc"), ".abc");
    }

    #[test]
    fn test_pattern_escaped_percent_literal() {
        assert_eq!(SqlCommandBuilder::jdbc_pattern_to_hive(r"abc\%"), "abc%");
    }

    #[test]
    fn test_pattern_escaped_underscore_literal() {
        assert_eq!(SqlCommandBuilder::jdbc_pattern_to_hive(r"abc\_"), "abc_");
    }

    #[test]
    fn test_pattern_escaped_backslash() {
        assert_eq!(SqlCommandBuilder::jdbc_pattern_to_hive(r"abc\\"), r"abc\\");
    }

    #[test]
    fn test_pattern_no_wildcards() {
        assert_eq!(SqlCommandBuilder::jdbc_pattern_to_hive("exact"), "exact");
    }

    #[test]
    fn test_pattern_single_quote_escaped() {
        assert_eq!(SqlCommandBuilder::jdbc_pattern_to_hive("it's"), "it''s");
    }
}
