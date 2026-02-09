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

//! Result parsing for metadata queries.
//!
//! Parses `ExecuteResult` readers into intermediate structs that can be
//! used by the Arrow builder to construct the nested `get_objects` response.
//! Column names match the verified result schemas from live Databricks queries.

use crate::client::ExecuteResult;
use crate::error::{DatabricksErrorHelper, Result};
use crate::metadata::type_mapping::databricks_type_to_arrow;
use arrow_array::cast::AsArray;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field};
use driverbase::error::ErrorHelper;

/// Parsed catalog info from SHOW CATALOGS.
#[derive(Debug, Clone)]
pub struct CatalogInfo {
    pub catalog_name: String,
}

/// Parsed schema info from SHOW SCHEMAS.
#[derive(Debug, Clone)]
pub struct SchemaInfo {
    pub catalog_name: String,
    pub schema_name: String,
}

/// Parsed table info from SHOW TABLES.
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub table_type: String,
    pub remarks: Option<String>,
}

/// Parsed column info from SHOW COLUMNS.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub column_name: String,
    pub column_type: String,
    pub column_size: Option<i32>,
    pub decimal_digits: Option<i32>,
    pub radix: Option<i32>,
    pub is_nullable: Option<String>,
    pub remarks: Option<String>,
    pub ordinal_position: Option<i32>,
    pub is_auto_increment: Option<String>,
    pub is_generated: Option<String>,
}

/// Get the index of a column by name, or return an error.
fn column_index(batch: &RecordBatch, name: &str) -> Result<usize> {
    batch.schema().index_of(name).map_err(|_| {
        DatabricksErrorHelper::invalid_state()
            .message(format!("Expected column '{}' in metadata result", name))
    })
}

/// Get a string value from a column at a given row, returning error if not string type.
fn get_string_value(batch: &RecordBatch, col_idx: usize, row: usize) -> Result<String> {
    let array = batch.column(col_idx);
    if array.is_null(row) {
        return Ok(String::new());
    }
    match array.data_type() {
        DataType::Utf8 => Ok(array.as_string::<i32>().value(row).to_string()),
        DataType::LargeUtf8 => Ok(array.as_string::<i64>().value(row).to_string()),
        dt => Err(DatabricksErrorHelper::invalid_state()
            .message(format!("Expected string column, got {:?}", dt))),
    }
}

/// Get an optional string value from a column at a given row.
fn get_optional_string_value(
    batch: &RecordBatch,
    col_idx: usize,
    row: usize,
) -> Result<Option<String>> {
    let array = batch.column(col_idx);
    if array.is_null(row) {
        return Ok(None);
    }
    Ok(Some(get_string_value(batch, col_idx, row)?))
}

/// Get an optional i32 value from a column at a given row.
fn get_optional_int32_value(
    batch: &RecordBatch,
    col_idx: usize,
    row: usize,
) -> Result<Option<i32>> {
    let array = batch.column(col_idx);
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Int32 => Ok(Some(
            array
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .unwrap()
                .value(row),
        )),
        DataType::Int64 => Ok(Some(
            array
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap()
                .value(row) as i32,
        )),
        dt => Err(DatabricksErrorHelper::invalid_state()
            .message(format!("Expected int column, got {:?}", dt))),
    }
}

/// Parse catalogs from SHOW CATALOGS result.
/// Result columns: `catalog: Utf8`
pub fn parse_catalogs(result: ExecuteResult) -> Result<Vec<CatalogInfo>> {
    let mut catalogs = Vec::new();
    let mut reader = result.reader;

    while let Some(batch) = reader.next_batch()? {
        let cat_idx = column_index(&batch, "catalog")?;
        for row in 0..batch.num_rows() {
            catalogs.push(CatalogInfo {
                catalog_name: get_string_value(&batch, cat_idx, row)?,
            });
        }
    }
    Ok(catalogs)
}

/// Parse schemas from SHOW SCHEMAS result.
/// Result columns: `databaseName: Utf8`, `catalog: Utf8`
pub fn parse_schemas(result: ExecuteResult) -> Result<Vec<SchemaInfo>> {
    let mut schemas = Vec::new();
    let mut reader = result.reader;

    while let Some(batch) = reader.next_batch()? {
        let cat_idx = column_index(&batch, "catalog")?;
        let db_idx = column_index(&batch, "databaseName")?;
        for row in 0..batch.num_rows() {
            schemas.push(SchemaInfo {
                catalog_name: get_string_value(&batch, cat_idx, row)?,
                schema_name: get_string_value(&batch, db_idx, row)?,
            });
        }
    }
    Ok(schemas)
}

/// Parse tables from SHOW TABLES result.
/// Result columns: `namespace: Utf8`, `tableName: Utf8`, `isTemporary: Boolean`,
///                 `information: Utf8?`, `catalogName: Utf8`, `tableType: Utf8`, `remarks: Utf8?`
pub fn parse_tables(result: ExecuteResult) -> Result<Vec<TableInfo>> {
    let mut tables = Vec::new();
    let mut reader = result.reader;

    while let Some(batch) = reader.next_batch()? {
        let cat_idx = column_index(&batch, "catalogName")?;
        let ns_idx = column_index(&batch, "namespace")?;
        let name_idx = column_index(&batch, "tableName")?;
        let type_idx = column_index(&batch, "tableType")?;
        let remarks_idx = batch.schema().index_of("remarks").ok();
        for row in 0..batch.num_rows() {
            let remarks = if let Some(idx) = remarks_idx {
                get_optional_string_value(&batch, idx, row)?
            } else {
                None
            };
            tables.push(TableInfo {
                catalog_name: get_string_value(&batch, cat_idx, row)?,
                schema_name: get_string_value(&batch, ns_idx, row)?,
                table_name: get_string_value(&batch, name_idx, row)?,
                table_type: get_string_value(&batch, type_idx, row)?,
                remarks,
            });
        }
    }
    Ok(tables)
}

/// Parse columns from SHOW COLUMNS result.
/// Result columns: `col_name: Utf8`, `catalogName: Utf8?`, `namespace: Utf8`,
///                 `tableName: Utf8`, `columnType: Utf8`, `columnSize: Int32?`,
///                 `decimalDigits: Int32?`, `radix: Int32?`, `isNullable: Utf8?`,
///                 `remarks: Utf8?`, `ordinalPosition: Int32?`,
///                 `isAutoIncrement: Utf8?`, `isGenerated: Utf8?`
pub fn parse_columns(result: ExecuteResult) -> Result<Vec<ColumnInfo>> {
    let mut columns = Vec::new();
    let mut reader = result.reader;

    while let Some(batch) = reader.next_batch()? {
        let name_idx = column_index(&batch, "col_name")?;
        let cat_idx = column_index(&batch, "catalogName")?;
        let ns_idx = column_index(&batch, "namespace")?;
        let tbl_idx = column_index(&batch, "tableName")?;
        let type_idx = column_index(&batch, "columnType")?;

        // Optional columns — use index_of().ok() to handle their absence
        let size_idx = batch.schema().index_of("columnSize").ok();
        let digits_idx = batch.schema().index_of("decimalDigits").ok();
        let radix_idx = batch.schema().index_of("radix").ok();
        let nullable_idx = batch.schema().index_of("isNullable").ok();
        let remarks_idx = batch.schema().index_of("remarks").ok();
        let ordinal_idx = batch.schema().index_of("ordinalPosition").ok();
        let auto_inc_idx = batch.schema().index_of("isAutoIncrement").ok();
        let generated_idx = batch.schema().index_of("isGenerated").ok();

        for row in 0..batch.num_rows() {
            columns.push(ColumnInfo {
                catalog_name: get_string_value(&batch, cat_idx, row).unwrap_or_default(),
                schema_name: get_string_value(&batch, ns_idx, row)?,
                table_name: get_string_value(&batch, tbl_idx, row)?,
                column_name: get_string_value(&batch, name_idx, row)?,
                column_type: get_string_value(&batch, type_idx, row)?,
                column_size: size_idx
                    .and_then(|i| get_optional_int32_value(&batch, i, row).ok())
                    .flatten(),
                decimal_digits: digits_idx
                    .and_then(|i| get_optional_int32_value(&batch, i, row).ok())
                    .flatten(),
                radix: radix_idx
                    .and_then(|i| get_optional_int32_value(&batch, i, row).ok())
                    .flatten(),
                is_nullable: nullable_idx
                    .and_then(|i| get_optional_string_value(&batch, i, row).ok())
                    .flatten(),
                remarks: remarks_idx
                    .and_then(|i| get_optional_string_value(&batch, i, row).ok())
                    .flatten(),
                ordinal_position: ordinal_idx
                    .and_then(|i| get_optional_int32_value(&batch, i, row).ok())
                    .flatten(),
                is_auto_increment: auto_inc_idx
                    .and_then(|i| get_optional_string_value(&batch, i, row).ok())
                    .flatten(),
                is_generated: generated_idx
                    .and_then(|i| get_optional_string_value(&batch, i, row).ok())
                    .flatten(),
            });
        }
    }
    Ok(columns)
}

/// Parse columns directly into Arrow Fields for `get_table_schema`.
/// Uses `col_name`, `columnType`, and `isNullable` to build the schema.
pub fn parse_columns_as_fields(result: ExecuteResult) -> Result<Vec<Field>> {
    let mut fields = Vec::new();
    let mut reader = result.reader;

    while let Some(batch) = reader.next_batch()? {
        let name_idx = column_index(&batch, "col_name")?;
        let type_idx = column_index(&batch, "columnType")?;
        let nullable_idx = batch.schema().index_of("isNullable").ok();

        for row in 0..batch.num_rows() {
            let col_name = get_string_value(&batch, name_idx, row)?;
            let col_type = get_string_value(&batch, type_idx, row)?;
            let arrow_type = databricks_type_to_arrow(&col_type);

            let nullable = nullable_idx
                .and_then(|i| get_optional_string_value(&batch, i, row).ok())
                .flatten()
                .map(|v| v == "true" || v == "YES")
                .unwrap_or(true);

            fields.push(Field::new(&col_name, arrow_type, nullable));
        }
    }
    Ok(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::ResultReader;
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use std::sync::Arc;

    /// A simple mock reader that returns predefined batches.
    struct MockReader {
        batches: Vec<RecordBatch>,
        index: usize,
        schema: SchemaRef,
    }

    impl MockReader {
        fn new(batches: Vec<RecordBatch>) -> Self {
            let schema = if batches.is_empty() {
                Arc::new(Schema::empty())
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
        fn schema(&self) -> Result<SchemaRef> {
            Ok(self.schema.clone())
        }

        fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
            if self.index >= self.batches.len() {
                Ok(None)
            } else {
                let batch = self.batches[self.index].clone();
                self.index += 1;
                Ok(Some(batch))
            }
        }
    }

    fn make_execute_result(batches: Vec<RecordBatch>) -> ExecuteResult {
        ExecuteResult {
            statement_id: "test-stmt".to_string(),
            reader: Box::new(MockReader::new(batches)),
        }
    }

    #[test]
    fn test_parse_catalogs() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "catalog",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["main", "hive_metastore"]))],
        )
        .unwrap();

        let result = make_execute_result(vec![batch]);
        let catalogs = parse_catalogs(result).unwrap();

        assert_eq!(catalogs.len(), 2);
        assert_eq!(catalogs[0].catalog_name, "main");
        assert_eq!(catalogs[1].catalog_name, "hive_metastore");
    }

    #[test]
    fn test_parse_schemas() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("databaseName", DataType::Utf8, false),
            Field::new("catalog", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["default", "information_schema"])),
                Arc::new(StringArray::from(vec!["main", "main"])),
            ],
        )
        .unwrap();

        let result = make_execute_result(vec![batch]);
        let schemas = parse_schemas(result).unwrap();

        assert_eq!(schemas.len(), 2);
        assert_eq!(schemas[0].catalog_name, "main");
        assert_eq!(schemas[0].schema_name, "default");
        assert_eq!(schemas[1].schema_name, "information_schema");
    }

    #[test]
    fn test_parse_tables() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("namespace", DataType::Utf8, false),
            Field::new("tableName", DataType::Utf8, false),
            Field::new("isTemporary", DataType::Boolean, false),
            Field::new("catalogName", DataType::Utf8, false),
            Field::new("tableType", DataType::Utf8, false),
            Field::new("remarks", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["default"])),
                Arc::new(StringArray::from(vec!["my_table"])),
                Arc::new(arrow_array::BooleanArray::from(vec![false])),
                Arc::new(StringArray::from(vec!["main"])),
                Arc::new(StringArray::from(vec!["TABLE"])),
                Arc::new(StringArray::from(vec![Some("A test table")])),
            ],
        )
        .unwrap();

        let result = make_execute_result(vec![batch]);
        let tables = parse_tables(result).unwrap();

        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].catalog_name, "main");
        assert_eq!(tables[0].schema_name, "default");
        assert_eq!(tables[0].table_name, "my_table");
        assert_eq!(tables[0].table_type, "TABLE");
        assert_eq!(tables[0].remarks, Some("A test table".to_string()));
    }

    #[test]
    fn test_parse_columns() {
        let schema = Arc::new(Schema::new(vec![
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
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["id"])),
                Arc::new(StringArray::from(vec![Some("main")])),
                Arc::new(StringArray::from(vec!["default"])),
                Arc::new(StringArray::from(vec!["my_table"])),
                Arc::new(StringArray::from(vec!["INT"])),
                Arc::new(Int32Array::from(vec![Some(10)])),
                Arc::new(Int32Array::from(vec![Some(0)])),
                Arc::new(Int32Array::from(vec![Some(10)])),
                Arc::new(StringArray::from(vec![Some("false")])),
                Arc::new(StringArray::from(vec![Some("Primary key")])),
                Arc::new(Int32Array::from(vec![Some(1)])),
                Arc::new(StringArray::from(vec![Some("NO")])),
                Arc::new(StringArray::from(vec![Some("NO")])),
            ],
        )
        .unwrap();

        let result = make_execute_result(vec![batch]);
        let columns = parse_columns(result).unwrap();

        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].catalog_name, "main");
        assert_eq!(columns[0].schema_name, "default");
        assert_eq!(columns[0].table_name, "my_table");
        assert_eq!(columns[0].column_name, "id");
        assert_eq!(columns[0].column_type, "INT");
        assert_eq!(columns[0].column_size, Some(10));
        assert_eq!(columns[0].is_nullable, Some("false".to_string()));
        assert_eq!(columns[0].ordinal_position, Some(1));
    }

    #[test]
    fn test_parse_columns_as_fields() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_name", DataType::Utf8, false),
            Field::new("columnType", DataType::Utf8, false),
            Field::new("isNullable", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["id", "name", "value"])),
                Arc::new(StringArray::from(vec!["INT", "STRING", "DOUBLE"])),
                Arc::new(StringArray::from(vec![
                    Some("false"),
                    Some("true"),
                    Some("true"),
                ])),
            ],
        )
        .unwrap();

        let result = make_execute_result(vec![batch]);
        let fields = parse_columns_as_fields(result).unwrap();

        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].name(), "id");
        assert_eq!(fields[0].data_type(), &DataType::Int32);
        assert!(!fields[0].is_nullable());
        assert_eq!(fields[1].name(), "name");
        assert_eq!(fields[1].data_type(), &DataType::Utf8);
        assert!(fields[1].is_nullable());
        assert_eq!(fields[2].name(), "value");
        assert_eq!(fields[2].data_type(), &DataType::Float64);
        assert!(fields[2].is_nullable());
    }

    #[test]
    fn test_parse_empty_result() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "catalog",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(Vec::<&str>::new()))],
        )
        .unwrap();

        let result = make_execute_result(vec![batch]);
        let catalogs = parse_catalogs(result).unwrap();
        assert!(catalogs.is_empty());
    }
}
