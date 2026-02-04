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

//! Arrow schema definitions for ADBC get_objects() result.
//!
//! This module defines the nested Arrow schemas required by the ADBC specification
//! for the `get_objects()` method. The schemas follow a hierarchical structure:
//!
//! - GetObjectsResult (catalog_name, catalog_db_schemas)
//!   - DbSchemaSchema (db_schema_name, db_schema_tables)
//!     - TableSchema (table_name, table_type, table_columns, table_constraints)
//!       - ColumnSchema (column_name, ordinal_position, remarks, xdbc_* fields)
//!       - ConstraintSchema (constraint_name, constraint_type, constraint_column_names, constraint_column_usage)
//!         - UsageSchema (fk_catalog, fk_db_schema, fk_table, fk_column_name)

use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

/// Returns the schema for the top-level get_objects() result.
///
/// The result schema has two fields:
/// - `catalog_name`: Utf8 (nullable) - name of the catalog
/// - `catalog_db_schemas`: List of DbSchemaSchema (nullable)
///
/// # Returns
///
/// An Arrow Schema with the get_objects() result structure.
pub fn get_objects_schema() -> Schema {
    Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, true),
        Field::new(
            "catalog_db_schemas",
            DataType::List(Arc::new(db_schema_field())),
            true,
        ),
    ])
}

/// Returns the field definition for a database schema entry.
fn db_schema_field() -> Field {
    Field::new("item", DataType::Struct(db_schema_schema().into()), true)
}

/// Returns the schema for a database schema entry.
///
/// Each database schema has:
/// - `db_schema_name`: Utf8 (nullable) - name of the schema
/// - `db_schema_tables`: List of TableSchema (nullable)
pub fn db_schema_schema() -> Vec<Field> {
    vec![
        Field::new("db_schema_name", DataType::Utf8, true),
        Field::new(
            "db_schema_tables",
            DataType::List(Arc::new(table_field())),
            true,
        ),
    ]
}

/// Returns the field definition for a table entry.
fn table_field() -> Field {
    Field::new("item", DataType::Struct(table_schema().into()), true)
}

/// Returns the schema for a table entry.
///
/// Each table has:
/// - `table_name`: Utf8 NOT NULL - name of the table
/// - `table_type`: Utf8 NOT NULL - type of the table (TABLE, VIEW, etc.)
/// - `table_columns`: List of ColumnSchema (nullable)
/// - `table_constraints`: List of ConstraintSchema (nullable)
pub fn table_schema() -> Vec<Field> {
    vec![
        Field::new("table_name", DataType::Utf8, false),
        Field::new("table_type", DataType::Utf8, false),
        Field::new(
            "table_columns",
            DataType::List(Arc::new(column_field())),
            true,
        ),
        Field::new(
            "table_constraints",
            DataType::List(Arc::new(constraint_field())),
            true,
        ),
    ]
}

/// Returns the field definition for a column entry.
fn column_field() -> Field {
    Field::new("item", DataType::Struct(column_schema().into()), true)
}

/// Returns the schema for a column entry.
///
/// Each column has basic fields plus all XDBC (Extended Database Connectivity) fields
/// as defined by the ADBC specification:
///
/// - `column_name`: Utf8 NOT NULL - name of the column
/// - `ordinal_position`: Int32 (nullable) - 1-based position in the table
/// - `remarks`: Utf8 (nullable) - comments about the column
/// - `xdbc_data_type`: Int16 (nullable) - XDBC/JDBC data type code
/// - `xdbc_type_name`: Utf8 (nullable) - database-specific type name
/// - `xdbc_column_size`: Int32 (nullable) - column size/precision
/// - `xdbc_decimal_digits`: Int16 (nullable) - decimal digits/scale
/// - `xdbc_num_prec_radix`: Int16 (nullable) - numeric precision radix
/// - `xdbc_nullable`: Int16 (nullable) - nullability (0=no, 1=yes, 2=unknown)
/// - `xdbc_column_def`: Utf8 (nullable) - default value
/// - `xdbc_sql_data_type`: Int16 (nullable) - SQL data type code
/// - `xdbc_datetime_sub`: Int16 (nullable) - datetime subcode
/// - `xdbc_char_octet_length`: Int32 (nullable) - max bytes for char types
/// - `xdbc_is_nullable`: Utf8 (nullable) - "YES", "NO", or ""
/// - `xdbc_scope_catalog`: Utf8 (nullable) - scope catalog for REF types
/// - `xdbc_scope_schema`: Utf8 (nullable) - scope schema for REF types
/// - `xdbc_scope_table`: Utf8 (nullable) - scope table for REF types
/// - `xdbc_is_autoincrement`: Bool (nullable) - whether auto-increment
/// - `xdbc_is_generatedcolumn`: Bool (nullable) - whether generated
pub fn column_schema() -> Vec<Field> {
    vec![
        Field::new("column_name", DataType::Utf8, false),
        Field::new("ordinal_position", DataType::Int32, true),
        Field::new("remarks", DataType::Utf8, true),
        // XDBC fields
        Field::new("xdbc_data_type", DataType::Int16, true),
        Field::new("xdbc_type_name", DataType::Utf8, true),
        Field::new("xdbc_column_size", DataType::Int32, true),
        Field::new("xdbc_decimal_digits", DataType::Int16, true),
        Field::new("xdbc_num_prec_radix", DataType::Int16, true),
        Field::new("xdbc_nullable", DataType::Int16, true),
        Field::new("xdbc_column_def", DataType::Utf8, true),
        Field::new("xdbc_sql_data_type", DataType::Int16, true),
        Field::new("xdbc_datetime_sub", DataType::Int16, true),
        Field::new("xdbc_char_octet_length", DataType::Int32, true),
        Field::new("xdbc_is_nullable", DataType::Utf8, true),
        Field::new("xdbc_scope_catalog", DataType::Utf8, true),
        Field::new("xdbc_scope_schema", DataType::Utf8, true),
        Field::new("xdbc_scope_table", DataType::Utf8, true),
        Field::new("xdbc_is_autoincrement", DataType::Boolean, true),
        Field::new("xdbc_is_generatedcolumn", DataType::Boolean, true),
    ]
}

/// Returns the field definition for a constraint entry.
fn constraint_field() -> Field {
    Field::new("item", DataType::Struct(constraint_schema().into()), true)
}

/// Returns the schema for a constraint entry.
///
/// Each constraint has:
/// - `constraint_name`: Utf8 (nullable) - name of the constraint
/// - `constraint_type`: Utf8 NOT NULL - type: "PRIMARY KEY", "FOREIGN KEY", "UNIQUE"
/// - `constraint_column_names`: List of Utf8 NOT NULL - columns in the constraint
/// - `constraint_column_usage`: List of UsageSchema (nullable) - for foreign keys
pub fn constraint_schema() -> Vec<Field> {
    vec![
        Field::new("constraint_name", DataType::Utf8, true),
        Field::new("constraint_type", DataType::Utf8, false),
        Field::new(
            "constraint_column_names",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
            false,
        ),
        Field::new(
            "constraint_column_usage",
            DataType::List(Arc::new(usage_field())),
            true,
        ),
    ]
}

/// Returns the field definition for a constraint usage entry.
fn usage_field() -> Field {
    Field::new("item", DataType::Struct(usage_schema().into()), true)
}

/// Returns the schema for a constraint usage entry (foreign key references).
///
/// Each usage entry has:
/// - `fk_catalog`: Utf8 (nullable) - catalog of the referenced table
/// - `fk_db_schema`: Utf8 (nullable) - schema of the referenced table
/// - `fk_table`: Utf8 NOT NULL - name of the referenced table
/// - `fk_column_name`: Utf8 NOT NULL - name of the referenced column
pub fn usage_schema() -> Vec<Field> {
    vec![
        Field::new("fk_catalog", DataType::Utf8, true),
        Field::new("fk_db_schema", DataType::Utf8, true),
        Field::new("fk_table", DataType::Utf8, false),
        Field::new("fk_column_name", DataType::Utf8, false),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_objects_schema_has_correct_fields() {
        let schema = get_objects_schema();

        assert_eq!(schema.fields().len(), 2);

        // Check catalog_name field
        let catalog_name = schema.field(0);
        assert_eq!(catalog_name.name(), "catalog_name");
        assert_eq!(catalog_name.data_type(), &DataType::Utf8);
        assert!(catalog_name.is_nullable());

        // Check catalog_db_schemas field is a list
        let db_schemas = schema.field(1);
        assert_eq!(db_schemas.name(), "catalog_db_schemas");
        assert!(matches!(db_schemas.data_type(), DataType::List(_)));
        assert!(db_schemas.is_nullable());
    }

    #[test]
    fn test_db_schema_schema_has_correct_fields() {
        let fields = db_schema_schema();

        assert_eq!(fields.len(), 2);

        // Check db_schema_name field
        assert_eq!(fields[0].name(), "db_schema_name");
        assert_eq!(fields[0].data_type(), &DataType::Utf8);
        assert!(fields[0].is_nullable());

        // Check db_schema_tables field is a list
        assert_eq!(fields[1].name(), "db_schema_tables");
        assert!(matches!(fields[1].data_type(), DataType::List(_)));
        assert!(fields[1].is_nullable());
    }

    #[test]
    fn test_table_schema_has_correct_fields() {
        let fields = table_schema();

        assert_eq!(fields.len(), 4);

        // Check table_name field (NOT NULL)
        assert_eq!(fields[0].name(), "table_name");
        assert_eq!(fields[0].data_type(), &DataType::Utf8);
        assert!(!fields[0].is_nullable());

        // Check table_type field (NOT NULL)
        assert_eq!(fields[1].name(), "table_type");
        assert_eq!(fields[1].data_type(), &DataType::Utf8);
        assert!(!fields[1].is_nullable());

        // Check table_columns field is a list
        assert_eq!(fields[2].name(), "table_columns");
        assert!(matches!(fields[2].data_type(), DataType::List(_)));
        assert!(fields[2].is_nullable());

        // Check table_constraints field is a list
        assert_eq!(fields[3].name(), "table_constraints");
        assert!(matches!(fields[3].data_type(), DataType::List(_)));
        assert!(fields[3].is_nullable());
    }

    #[test]
    fn test_column_schema_has_all_xdbc_fields() {
        let fields = column_schema();

        // Should have 19 fields total
        assert_eq!(fields.len(), 19);

        // Required fields
        assert_eq!(fields[0].name(), "column_name");
        assert!(!fields[0].is_nullable());

        // Check for all XDBC fields
        let field_names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"xdbc_data_type"));
        assert!(field_names.contains(&"xdbc_type_name"));
        assert!(field_names.contains(&"xdbc_column_size"));
        assert!(field_names.contains(&"xdbc_decimal_digits"));
        assert!(field_names.contains(&"xdbc_num_prec_radix"));
        assert!(field_names.contains(&"xdbc_nullable"));
        assert!(field_names.contains(&"xdbc_column_def"));
        assert!(field_names.contains(&"xdbc_sql_data_type"));
        assert!(field_names.contains(&"xdbc_datetime_sub"));
        assert!(field_names.contains(&"xdbc_char_octet_length"));
        assert!(field_names.contains(&"xdbc_is_nullable"));
        assert!(field_names.contains(&"xdbc_scope_catalog"));
        assert!(field_names.contains(&"xdbc_scope_schema"));
        assert!(field_names.contains(&"xdbc_scope_table"));
        assert!(field_names.contains(&"xdbc_is_autoincrement"));
        assert!(field_names.contains(&"xdbc_is_generatedcolumn"));
    }

    #[test]
    fn test_constraint_schema_has_correct_fields() {
        let fields = constraint_schema();

        assert_eq!(fields.len(), 4);

        // Check constraint_name (nullable)
        assert_eq!(fields[0].name(), "constraint_name");
        assert!(fields[0].is_nullable());

        // Check constraint_type (NOT NULL)
        assert_eq!(fields[1].name(), "constraint_type");
        assert!(!fields[1].is_nullable());

        // Check constraint_column_names (NOT NULL list)
        assert_eq!(fields[2].name(), "constraint_column_names");
        assert!(!fields[2].is_nullable());
        assert!(matches!(fields[2].data_type(), DataType::List(_)));

        // Check constraint_column_usage (nullable list)
        assert_eq!(fields[3].name(), "constraint_column_usage");
        assert!(fields[3].is_nullable());
        assert!(matches!(fields[3].data_type(), DataType::List(_)));
    }

    #[test]
    fn test_usage_schema_has_correct_fields() {
        let fields = usage_schema();

        assert_eq!(fields.len(), 4);

        // Check fk_catalog (nullable)
        assert_eq!(fields[0].name(), "fk_catalog");
        assert!(fields[0].is_nullable());

        // Check fk_db_schema (nullable)
        assert_eq!(fields[1].name(), "fk_db_schema");
        assert!(fields[1].is_nullable());

        // Check fk_table (NOT NULL)
        assert_eq!(fields[2].name(), "fk_table");
        assert!(!fields[2].is_nullable());

        // Check fk_column_name (NOT NULL)
        assert_eq!(fields[3].name(), "fk_column_name");
        assert!(!fields[3].is_nullable());
    }

    #[test]
    fn test_schema_matches_adbc_specification() {
        // Verify the complete nested structure matches ADBC spec
        let schema = get_objects_schema();

        // Get the nested struct type for db_schemas
        if let DataType::List(field) = schema.field(1).data_type() {
            if let DataType::Struct(db_schema_fields) = field.data_type() {
                // Verify db_schema has tables list
                assert_eq!(db_schema_fields.len(), 2);
                let tables_field = &db_schema_fields[1];
                assert_eq!(tables_field.name(), "db_schema_tables");

                if let DataType::List(table_field) = tables_field.data_type() {
                    if let DataType::Struct(table_fields) = table_field.data_type() {
                        // Verify table has columns and constraints
                        assert_eq!(table_fields.len(), 4);
                        assert_eq!(table_fields[2].name(), "table_columns");
                        assert_eq!(table_fields[3].name(), "table_constraints");
                    } else {
                        panic!("Expected Struct type for table");
                    }
                } else {
                    panic!("Expected List type for db_schema_tables");
                }
            } else {
                panic!("Expected Struct type for db_schema");
            }
        } else {
            panic!("Expected List type for catalog_db_schemas");
        }
    }
}
