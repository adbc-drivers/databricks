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
//! Parses `ExecuteResult` readers from SHOW SQL commands into intermediate
//! structs (`CatalogInfo`, `SchemaInfo`, `TableInfo`, `ColumnInfo`) that
//! the builder module uses to construct nested Arrow responses.

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::Field;
use driverbase::error::ErrorHelper;

use crate::client::ExecuteResult;
use crate::error::{DatabricksErrorHelper, Result};
use crate::metadata::type_mapping::databricks_type_to_arrow;

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

/// Extract a required string column from a `RecordBatch` by name.
///
/// Returns an error if the column is not found or is not a string type.
fn get_string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    let idx = batch.schema().index_of(name).map_err(|_| {
        DatabricksErrorHelper::invalid_state()
            .message(format!("Column '{}' not found in result", name))
    })?;
    let col = batch.column(idx);
    col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        DatabricksErrorHelper::invalid_state()
            .message(format!("Column '{}' is not a string type", name))
    })
}

/// Extract an optional string column from a `RecordBatch` by name.
///
/// Returns `Ok(None)` if the column does not exist.
/// Returns an error only if the column exists but is not a string type.
fn get_optional_string_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<Option<&'a StringArray>> {
    match batch.schema().index_of(name) {
        Ok(idx) => {
            let col = batch.column(idx);
            let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                DatabricksErrorHelper::invalid_state()
                    .message(format!("Column '{}' is not a string type", name))
            })?;
            Ok(Some(arr))
        }
        Err(_) => Ok(None),
    }
}

/// Extract an optional Int32 column from a `RecordBatch` by name.
///
/// Returns `Ok(None)` if the column does not exist.
fn get_optional_int32_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<Option<&'a arrow_array::Int32Array>> {
    match batch.schema().index_of(name) {
        Ok(idx) => {
            let col = batch.column(idx);
            let arr = col
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .ok_or_else(|| {
                    DatabricksErrorHelper::invalid_state()
                        .message(format!("Column '{}' is not an Int32 type", name))
                })?;
            Ok(Some(arr))
        }
        Err(_) => Ok(None),
    }
}

/// Read all batches from an `ExecuteResult` reader, applying a function to each batch.
fn read_all_batches<T, F>(mut result: ExecuteResult, mut process_batch: F) -> Result<Vec<T>>
where
    F: FnMut(&RecordBatch) -> Result<Vec<T>>,
{
    let mut items = Vec::new();
    loop {
        match result.reader.next_batch()? {
            Some(batch) => {
                let batch_items = process_batch(&batch)?;
                items.extend(batch_items);
            }
            None => break,
        }
    }
    Ok(items)
}

/// Parse catalogs from SHOW CATALOGS result.
///
/// Reads the `catalog` column from each batch.
pub fn parse_catalogs(result: ExecuteResult) -> Result<Vec<CatalogInfo>> {
    read_all_batches(result, |batch| {
        let catalog_col = get_string_column(batch, "catalog")?;
        let mut catalogs = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            catalogs.push(CatalogInfo {
                catalog_name: catalog_col.value(i).to_string(),
            });
        }
        Ok(catalogs)
    })
}

/// Parse schemas from SHOW SCHEMAS result.
///
/// Works for both `SHOW SCHEMAS IN ALL CATALOGS` and `SHOW SCHEMAS IN \`catalog\``.
/// Reads `catalog` and `databaseName` columns.
pub fn parse_schemas(result: ExecuteResult) -> Result<Vec<SchemaInfo>> {
    read_all_batches(result, |batch| {
        let catalog_col = get_string_column(batch, "catalog")?;
        let schema_col = get_string_column(batch, "databaseName")?;
        let mut schemas = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            schemas.push(SchemaInfo {
                catalog_name: catalog_col.value(i).to_string(),
                schema_name: schema_col.value(i).to_string(),
            });
        }
        Ok(schemas)
    })
}

/// Parse tables from SHOW TABLES result.
///
/// Reads `catalogName`, `namespace`, `tableName`, `tableType`, and `remarks` columns.
pub fn parse_tables(result: ExecuteResult) -> Result<Vec<TableInfo>> {
    read_all_batches(result, |batch| {
        let catalog_col = get_string_column(batch, "catalogName")?;
        let namespace_col = get_string_column(batch, "namespace")?;
        let table_name_col = get_string_column(batch, "tableName")?;
        let table_type_col = get_string_column(batch, "tableType")?;
        let remarks_col = get_optional_string_column(batch, "remarks")?;

        let mut tables = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let remarks = remarks_col.and_then(|col| {
                if col.is_null(i) {
                    None
                } else {
                    Some(col.value(i).to_string())
                }
            });
            tables.push(TableInfo {
                catalog_name: catalog_col.value(i).to_string(),
                schema_name: namespace_col.value(i).to_string(),
                table_name: table_name_col.value(i).to_string(),
                table_type: table_type_col.value(i).to_string(),
                remarks,
            });
        }
        Ok(tables)
    })
}

/// Parse columns from SHOW COLUMNS result.
///
/// Reads all 13 columns: `col_name`, `catalogName`, `namespace`, `tableName`,
/// `columnType`, `columnSize`, `decimalDigits`, `radix`, `isNullable`, `remarks`,
/// `ordinalPosition`, `isAutoIncrement`, `isGenerated`.
pub fn parse_columns(result: ExecuteResult) -> Result<Vec<ColumnInfo>> {
    read_all_batches(result, |batch| {
        let col_name = get_string_column(batch, "col_name")?;
        let catalog_col = get_string_column(batch, "catalogName")?;
        let namespace_col = get_string_column(batch, "namespace")?;
        let table_name_col = get_string_column(batch, "tableName")?;
        let column_type_col = get_string_column(batch, "columnType")?;

        let column_size_col = get_optional_int32_column(batch, "columnSize")?;
        let decimal_digits_col = get_optional_int32_column(batch, "decimalDigits")?;
        let radix_col = get_optional_int32_column(batch, "radix")?;
        let is_nullable_col = get_optional_string_column(batch, "isNullable")?;
        let remarks_col = get_optional_string_column(batch, "remarks")?;
        let ordinal_position_col = get_optional_int32_column(batch, "ordinalPosition")?;
        let is_auto_increment_col = get_optional_string_column(batch, "isAutoIncrement")?;
        let is_generated_col = get_optional_string_column(batch, "isGenerated")?;

        let mut columns = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            columns.push(ColumnInfo {
                catalog_name: catalog_col.value(i).to_string(),
                schema_name: namespace_col.value(i).to_string(),
                table_name: table_name_col.value(i).to_string(),
                column_name: col_name.value(i).to_string(),
                column_type: column_type_col.value(i).to_string(),
                column_size: column_size_col.and_then(|c| {
                    if c.is_null(i) {
                        None
                    } else {
                        Some(c.value(i))
                    }
                }),
                decimal_digits: decimal_digits_col.and_then(|c| {
                    if c.is_null(i) {
                        None
                    } else {
                        Some(c.value(i))
                    }
                }),
                radix: radix_col.and_then(|c| {
                    if c.is_null(i) {
                        None
                    } else {
                        Some(c.value(i))
                    }
                }),
                is_nullable: is_nullable_col.and_then(|c| {
                    if c.is_null(i) {
                        None
                    } else {
                        Some(c.value(i).to_string())
                    }
                }),
                remarks: remarks_col.and_then(|c| {
                    if c.is_null(i) {
                        None
                    } else {
                        Some(c.value(i).to_string())
                    }
                }),
                ordinal_position: ordinal_position_col.and_then(|c| {
                    if c.is_null(i) {
                        None
                    } else {
                        Some(c.value(i))
                    }
                }),
                is_auto_increment: is_auto_increment_col.and_then(|c| {
                    if c.is_null(i) {
                        None
                    } else {
                        Some(c.value(i).to_string())
                    }
                }),
                is_generated: is_generated_col.and_then(|c| {
                    if c.is_null(i) {
                        None
                    } else {
                        Some(c.value(i).to_string())
                    }
                }),
            });
        }
        Ok(columns)
    })
}

/// Parse columns directly into Arrow `Field`s for `get_table_schema`.
///
/// Uses `col_name` for field name, `columnType` mapped via `databricks_type_to_arrow()`
/// for field data type, and `isNullable` for nullability.
pub fn parse_columns_as_fields(result: ExecuteResult) -> Result<Vec<Field>> {
    read_all_batches(result, |batch| {
        let name_col = get_string_column(batch, "col_name")?;
        let type_col = get_string_column(batch, "columnType")?;
        let nullable_col = get_optional_string_column(batch, "isNullable")?;

        let mut fields = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let arrow_type = databricks_type_to_arrow(type_col.value(i));
            let nullable = nullable_col
                .map(|c| {
                    if c.is_null(i) {
                        true
                    } else {
                        let val = c.value(i);
                        val.eq_ignore_ascii_case("true") || val.eq_ignore_ascii_case("yes")
                    }
                })
                .unwrap_or(true);
            fields.push(Field::new(name_col.value(i), arrow_type, nullable));
        }
        Ok(fields)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use std::sync::Arc;

    use crate::reader::ResultReader;

    /// A mock ResultReader that returns pre-built RecordBatches.
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
            if self.index < self.batches.len() {
                let batch = self.batches[self.index].clone();
                self.index += 1;
                Ok(Some(batch))
            } else {
                Ok(None)
            }
        }
    }

    fn make_execute_result(batches: Vec<RecordBatch>) -> ExecuteResult {
        ExecuteResult {
            statement_id: "test-stmt-id".to_string(),
            reader: Box::new(MockReader::new(batches)),
        }
    }

    #[test]
    fn test_parse_catalogs_from_reader() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "catalog",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                "main",
                "hive_metastore",
                "system",
            ]))],
        )
        .unwrap();

        let result = make_execute_result(vec![batch]);
        let catalogs = parse_catalogs(result).unwrap();

        assert_eq!(catalogs.len(), 3);
        assert_eq!(catalogs[0].catalog_name, "main");
        assert_eq!(catalogs[1].catalog_name, "hive_metastore");
        assert_eq!(catalogs[2].catalog_name, "system");
    }

    #[test]
    fn test_parse_catalogs_multiple_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "catalog",
            DataType::Utf8,
            false,
        )]));

        let batch1 =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["main"]))])
                .unwrap();
        let batch2 = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["hive_metastore"]))],
        )
        .unwrap();

        let result = make_execute_result(vec![batch1, batch2]);
        let catalogs = parse_catalogs(result).unwrap();

        assert_eq!(catalogs.len(), 2);
        assert_eq!(catalogs[0].catalog_name, "main");
        assert_eq!(catalogs[1].catalog_name, "hive_metastore");
    }

    #[test]
    fn test_parse_catalogs_empty_result() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "catalog",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(Vec::<&str>::new()))],
        )
        .unwrap();

        let result = make_execute_result(vec![batch]);
        let catalogs = parse_catalogs(result).unwrap();
        assert!(catalogs.is_empty());
    }

    #[test]
    fn test_parse_schemas_from_reader() {
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
        assert_eq!(schemas[1].catalog_name, "main");
        assert_eq!(schemas[1].schema_name, "information_schema");
    }

    #[test]
    fn test_parse_tables_from_reader() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("namespace", DataType::Utf8, false),
            Field::new("tableName", DataType::Utf8, false),
            Field::new("isTemporary", DataType::Boolean, false),
            Field::new("information", DataType::Utf8, true),
            Field::new("catalogName", DataType::Utf8, false),
            Field::new("tableType", DataType::Utf8, false),
            Field::new("remarks", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["default", "default"])),
                Arc::new(StringArray::from(vec!["users", "orders"])),
                Arc::new(arrow_array::BooleanArray::from(vec![false, false])),
                Arc::new(StringArray::from(vec![None::<&str>, None])),
                Arc::new(StringArray::from(vec!["main", "main"])),
                Arc::new(StringArray::from(vec!["TABLE", "TABLE"])),
                Arc::new(StringArray::from(vec![
                    Some("user table"),
                    None::<&str>,
                ])),
            ],
        )
        .unwrap();

        let result = make_execute_result(vec![batch]);
        let tables = parse_tables(result).unwrap();

        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].catalog_name, "main");
        assert_eq!(tables[0].schema_name, "default");
        assert_eq!(tables[0].table_name, "users");
        assert_eq!(tables[0].table_type, "TABLE");
        assert_eq!(tables[0].remarks, Some("user table".to_string()));
        assert_eq!(tables[1].table_name, "orders");
        assert_eq!(tables[1].remarks, None);
    }

    #[test]
    fn test_parse_columns_from_reader() {
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
                Arc::new(StringArray::from(vec!["id", "name", "price"])),
                Arc::new(StringArray::from(vec!["main", "main", "main"])),
                Arc::new(StringArray::from(vec!["default", "default", "default"])),
                Arc::new(StringArray::from(vec!["products", "products", "products"])),
                Arc::new(StringArray::from(vec!["BIGINT", "STRING", "DECIMAL(10,2)"])),
                Arc::new(Int32Array::from(vec![
                    Some(19),
                    Some(2147483647),
                    Some(10),
                ])),
                Arc::new(Int32Array::from(vec![Some(0), None, Some(2)])),
                Arc::new(Int32Array::from(vec![Some(10), None, Some(10)])),
                Arc::new(StringArray::from(vec![
                    Some("false"),
                    Some("true"),
                    Some("true"),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("primary key"),
                    None::<&str>,
                    None,
                ])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
                Arc::new(StringArray::from(vec![
                    Some("YES"),
                    Some("NO"),
                    Some("NO"),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("NO"),
                    Some("NO"),
                    Some("NO"),
                ])),
            ],
        )
        .unwrap();

        let result = make_execute_result(vec![batch]);
        let columns = parse_columns(result).unwrap();

        assert_eq!(columns.len(), 3);

        // Check first column (id)
        assert_eq!(columns[0].catalog_name, "main");
        assert_eq!(columns[0].schema_name, "default");
        assert_eq!(columns[0].table_name, "products");
        assert_eq!(columns[0].column_name, "id");
        assert_eq!(columns[0].column_type, "BIGINT");
        assert_eq!(columns[0].column_size, Some(19));
        assert_eq!(columns[0].decimal_digits, Some(0));
        assert_eq!(columns[0].radix, Some(10));
        assert_eq!(columns[0].is_nullable, Some("false".to_string()));
        assert_eq!(columns[0].remarks, Some("primary key".to_string()));
        assert_eq!(columns[0].ordinal_position, Some(1));
        assert_eq!(columns[0].is_auto_increment, Some("YES".to_string()));
        assert_eq!(columns[0].is_generated, Some("NO".to_string()));

        // Check second column (name) — some nullable fields
        assert_eq!(columns[1].column_name, "name");
        assert_eq!(columns[1].column_type, "STRING");
        assert_eq!(columns[1].decimal_digits, None);
        assert_eq!(columns[1].radix, None);
        assert_eq!(columns[1].is_nullable, Some("true".to_string()));
        assert_eq!(columns[1].remarks, None);

        // Check third column (price) — DECIMAL with digits
        assert_eq!(columns[2].column_name, "price");
        assert_eq!(columns[2].column_type, "DECIMAL(10,2)");
        assert_eq!(columns[2].column_size, Some(10));
        assert_eq!(columns[2].decimal_digits, Some(2));
        assert_eq!(columns[2].ordinal_position, Some(3));
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
                Arc::new(StringArray::from(vec!["id", "name", "created_at", "price"])),
                Arc::new(StringArray::from(vec![
                    "BIGINT",
                    "STRING",
                    "TIMESTAMP",
                    "DECIMAL(10,2)",
                ])),
                Arc::new(StringArray::from(vec![
                    Some("false"),
                    Some("true"),
                    Some("YES"),
                    None::<&str>,
                ])),
            ],
        )
        .unwrap();

        let result = make_execute_result(vec![batch]);
        let fields = parse_columns_as_fields(result).unwrap();

        assert_eq!(fields.len(), 4);

        // id: BIGINT, not nullable
        assert_eq!(fields[0].name(), "id");
        assert_eq!(*fields[0].data_type(), DataType::Int64);
        assert!(!fields[0].is_nullable());

        // name: STRING, nullable
        assert_eq!(fields[1].name(), "name");
        assert_eq!(*fields[1].data_type(), DataType::Utf8);
        assert!(fields[1].is_nullable());

        // created_at: TIMESTAMP, nullable (YES maps to true)
        assert_eq!(fields[2].name(), "created_at");
        assert_eq!(
            *fields[2].data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert!(fields[2].is_nullable());

        // price: DECIMAL(10,2), nullable (null isNullable defaults to true)
        assert_eq!(fields[3].name(), "price");
        assert_eq!(*fields[3].data_type(), DataType::Decimal128(10, 2));
        assert!(fields[3].is_nullable());
    }

    #[test]
    fn test_parse_columns_as_fields_no_nullable_column() {
        // Test when isNullable column is not present at all
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_name", DataType::Utf8, false),
            Field::new("columnType", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["id"])),
                Arc::new(StringArray::from(vec!["INT"])),
            ],
        )
        .unwrap();

        let result = make_execute_result(vec![batch]);
        let fields = parse_columns_as_fields(result).unwrap();

        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name(), "id");
        assert_eq!(*fields[0].data_type(), DataType::Int32);
        // When isNullable column is missing, defaults to nullable
        assert!(fields[0].is_nullable());
    }

    #[test]
    fn test_parse_empty_result() {
        let result = make_execute_result(vec![]);
        let catalogs = parse_catalogs(result).unwrap();
        assert!(catalogs.is_empty());
    }

    #[test]
    fn test_get_string_column_missing() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["x"]))]).unwrap();

        let err = get_string_column(&batch, "nonexistent");
        assert!(err.is_err());
        let msg = format!("{}", err.unwrap_err());
        assert!(msg.contains("not found"));
    }
}
