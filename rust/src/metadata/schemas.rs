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

//! Arrow schema definitions for flat ODBC/JDBC metadata result sets.
//!
//! Each schema matches the JDBC/ODBC specification for the corresponding
//! catalog function. These are separate from `adbc_core::schemas::*` which
//! define the nested ADBC `get_objects` format.

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::sync::{Arc, LazyLock};

/// Schema for SQLTables / get_catalogs result.
/// Columns: TABLE_CAT
pub static CATALOGS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "TABLE_CAT",
        DataType::Utf8,
        true,
    )]))
});

/// Schema for SQLTables / get_schemas result.
/// Columns: TABLE_SCHEM, TABLE_CATALOG
pub static SCHEMAS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("TABLE_SCHEM", DataType::Utf8, true),
        Field::new("TABLE_CATALOG", DataType::Utf8, true),
    ]))
});

/// Schema for SQLTables result.
/// Columns: TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS
pub static TABLES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("TABLE_CAT", DataType::Utf8, true),
        Field::new("TABLE_SCHEM", DataType::Utf8, true),
        Field::new("TABLE_NAME", DataType::Utf8, false),
        Field::new("TABLE_TYPE", DataType::Utf8, false),
        Field::new("REMARKS", DataType::Utf8, true),
    ]))
});

/// Schema for SQLColumns result (JDBC-compatible 24-column schema).
pub static COLUMNS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("TABLE_CAT", DataType::Utf8, true),
        Field::new("TABLE_SCHEM", DataType::Utf8, true),
        Field::new("TABLE_NAME", DataType::Utf8, false),
        Field::new("COLUMN_NAME", DataType::Utf8, false),
        Field::new("DATA_TYPE", DataType::Int16, false),
        Field::new("TYPE_NAME", DataType::Utf8, false),
        Field::new("COLUMN_SIZE", DataType::Int32, true),
        Field::new("BUFFER_LENGTH", DataType::Int32, true),
        Field::new("DECIMAL_DIGITS", DataType::Int16, true),
        Field::new("NUM_PREC_RADIX", DataType::Int16, true),
        Field::new("NULLABLE", DataType::Int16, false),
        Field::new("REMARKS", DataType::Utf8, true),
        Field::new("COLUMN_DEF", DataType::Utf8, true),
        Field::new("SQL_DATA_TYPE", DataType::Int16, false),
        Field::new("SQL_DATETIME_SUB", DataType::Int16, true),
        Field::new("CHAR_OCTET_LENGTH", DataType::Int32, true),
        Field::new("ORDINAL_POSITION", DataType::Int32, false),
        Field::new("IS_NULLABLE", DataType::Utf8, true),
        Field::new("SCOPE_CATALOG", DataType::Utf8, true),
        Field::new("SCOPE_SCHEMA", DataType::Utf8, true),
        Field::new("SCOPE_TABLE", DataType::Utf8, true),
        Field::new("SOURCE_DATA_TYPE", DataType::Int16, true),
        Field::new("IS_AUTOINCREMENT", DataType::Utf8, true),
        Field::new("IS_GENERATEDCOLUMN", DataType::Utf8, true),
    ]))
});

/// Schema for SQLPrimaryKeys result.
pub static PRIMARY_KEYS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("TABLE_CAT", DataType::Utf8, true),
        Field::new("TABLE_SCHEM", DataType::Utf8, true),
        Field::new("TABLE_NAME", DataType::Utf8, false),
        Field::new("COLUMN_NAME", DataType::Utf8, false),
        Field::new("KEY_SEQ", DataType::Int16, false),
        Field::new("PK_NAME", DataType::Utf8, true),
    ]))
});

/// Schema for SQLForeignKeys result.
pub static FOREIGN_KEYS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("PKTABLE_CAT", DataType::Utf8, true),
        Field::new("PKTABLE_SCHEM", DataType::Utf8, true),
        Field::new("PKTABLE_NAME", DataType::Utf8, false),
        Field::new("PKCOLUMN_NAME", DataType::Utf8, false),
        Field::new("FKTABLE_CAT", DataType::Utf8, true),
        Field::new("FKTABLE_SCHEM", DataType::Utf8, true),
        Field::new("FKTABLE_NAME", DataType::Utf8, false),
        Field::new("FKCOLUMN_NAME", DataType::Utf8, false),
        Field::new("KEY_SEQ", DataType::Int16, false),
        Field::new("UPDATE_RULE", DataType::Int16, false),
        Field::new("DELETE_RULE", DataType::Int16, false),
        Field::new("FK_NAME", DataType::Utf8, true),
        Field::new("PK_NAME", DataType::Utf8, true),
        Field::new("DEFERRABILITY", DataType::Int16, false),
    ]))
});

/// Schema for SQLTableTypes result.
pub static TABLE_TYPES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "TABLE_TYPE",
        DataType::Utf8,
        false,
    )]))
});

/// Schema for SQLStatistics result.
pub static STATISTICS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("TABLE_CAT", DataType::Utf8, true),
        Field::new("TABLE_SCHEM", DataType::Utf8, true),
        Field::new("TABLE_NAME", DataType::Utf8, false),
        Field::new("NON_UNIQUE", DataType::Boolean, true),
        Field::new("INDEX_QUALIFIER", DataType::Utf8, true),
        Field::new("INDEX_NAME", DataType::Utf8, true),
        Field::new("TYPE", DataType::Int16, false),
        Field::new("ORDINAL_POSITION", DataType::Int16, true),
        Field::new("COLUMN_NAME", DataType::Utf8, true),
        Field::new("ASC_OR_DESC", DataType::Utf8, true),
        Field::new("CARDINALITY", DataType::Int64, true),
        Field::new("PAGES", DataType::Int64, true),
        Field::new("FILTER_CONDITION", DataType::Utf8, true),
    ]))
});

/// Schema for SQLSpecialColumns result.
pub static SPECIAL_COLUMNS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("SCOPE", DataType::Int16, true),
        Field::new("COLUMN_NAME", DataType::Utf8, false),
        Field::new("DATA_TYPE", DataType::Int16, false),
        Field::new("TYPE_NAME", DataType::Utf8, false),
        Field::new("COLUMN_SIZE", DataType::Int32, true),
        Field::new("BUFFER_LENGTH", DataType::Int32, true),
        Field::new("DECIMAL_DIGITS", DataType::Int16, true),
        Field::new("PSEUDO_COLUMN", DataType::Int16, true),
    ]))
});

/// Schema for SQLProcedures result.
pub static PROCEDURES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("PROCEDURE_CAT", DataType::Utf8, true),
        Field::new("PROCEDURE_SCHEM", DataType::Utf8, true),
        Field::new("PROCEDURE_NAME", DataType::Utf8, false),
        Field::new("NUM_INPUT_PARAMS", DataType::Int32, true),
        Field::new("NUM_OUTPUT_PARAMS", DataType::Int32, true),
        Field::new("NUM_RESULT_SETS", DataType::Int32, true),
        Field::new("REMARKS", DataType::Utf8, true),
        Field::new("PROCEDURE_TYPE", DataType::Int16, false),
    ]))
});

/// Schema for SQLProcedureColumns result.
pub static PROCEDURE_COLUMNS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("PROCEDURE_CAT", DataType::Utf8, true),
        Field::new("PROCEDURE_SCHEM", DataType::Utf8, true),
        Field::new("PROCEDURE_NAME", DataType::Utf8, false),
        Field::new("COLUMN_NAME", DataType::Utf8, false),
        Field::new("COLUMN_TYPE", DataType::Int16, false),
        Field::new("DATA_TYPE", DataType::Int16, false),
        Field::new("TYPE_NAME", DataType::Utf8, false),
        Field::new("COLUMN_SIZE", DataType::Int32, true),
        Field::new("BUFFER_LENGTH", DataType::Int32, true),
        Field::new("DECIMAL_DIGITS", DataType::Int16, true),
        Field::new("NUM_PREC_RADIX", DataType::Int16, true),
        Field::new("NULLABLE", DataType::Int16, false),
        Field::new("REMARKS", DataType::Utf8, true),
        Field::new("COLUMN_DEF", DataType::Utf8, true),
        Field::new("SQL_DATA_TYPE", DataType::Int16, false),
        Field::new("SQL_DATETIME_SUB", DataType::Int16, true),
        Field::new("CHAR_OCTET_LENGTH", DataType::Int32, true),
        Field::new("ORDINAL_POSITION", DataType::Int32, false),
        Field::new("IS_NULLABLE", DataType::Utf8, true),
    ]))
});

/// Schema for SQLTablePrivileges result.
pub static TABLE_PRIVILEGES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("TABLE_CAT", DataType::Utf8, true),
        Field::new("TABLE_SCHEM", DataType::Utf8, true),
        Field::new("TABLE_NAME", DataType::Utf8, false),
        Field::new("GRANTOR", DataType::Utf8, true),
        Field::new("GRANTEE", DataType::Utf8, true),
        Field::new("PRIVILEGE", DataType::Utf8, false),
        Field::new("IS_GRANTABLE", DataType::Utf8, true),
    ]))
});

/// Schema for SQLColumnPrivileges result.
pub static COLUMN_PRIVILEGES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("TABLE_CAT", DataType::Utf8, true),
        Field::new("TABLE_SCHEM", DataType::Utf8, true),
        Field::new("TABLE_NAME", DataType::Utf8, false),
        Field::new("COLUMN_NAME", DataType::Utf8, false),
        Field::new("GRANTOR", DataType::Utf8, true),
        Field::new("GRANTEE", DataType::Utf8, true),
        Field::new("PRIVILEGE", DataType::Utf8, false),
        Field::new("IS_GRANTABLE", DataType::Utf8, true),
    ]))
});

/// Create an empty RecordBatch with the given schema (zero rows).
pub fn empty_batch(schema: &SchemaRef) -> arrow_array::RecordBatch {
    arrow_array::RecordBatch::new_empty(schema.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schemas_have_correct_column_counts() {
        assert_eq!(CATALOGS_SCHEMA.fields().len(), 1);
        assert_eq!(SCHEMAS_SCHEMA.fields().len(), 2);
        assert_eq!(TABLES_SCHEMA.fields().len(), 5);
        assert_eq!(COLUMNS_SCHEMA.fields().len(), 24);
        assert_eq!(PRIMARY_KEYS_SCHEMA.fields().len(), 6);
        assert_eq!(FOREIGN_KEYS_SCHEMA.fields().len(), 14);
        assert_eq!(TABLE_TYPES_SCHEMA.fields().len(), 1);
        assert_eq!(STATISTICS_SCHEMA.fields().len(), 13);
        assert_eq!(SPECIAL_COLUMNS_SCHEMA.fields().len(), 8);
        assert_eq!(PROCEDURES_SCHEMA.fields().len(), 8);
        assert_eq!(PROCEDURE_COLUMNS_SCHEMA.fields().len(), 19);
        assert_eq!(TABLE_PRIVILEGES_SCHEMA.fields().len(), 7);
        assert_eq!(COLUMN_PRIVILEGES_SCHEMA.fields().len(), 8);
    }

    #[test]
    fn test_empty_batch() {
        let batch = empty_batch(&TABLES_SCHEMA);
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), *TABLES_SCHEMA);
    }
}
