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

//! Arrow builder for `get_objects` response.
//!
//! Builds the deeply nested Arrow struct required by the ADBC `get_objects` spec.
//! The hierarchy is: catalog → db_schema → table → column/constraint.
//!
//! Uses `adbc_core::schemas::GET_OBJECTS_SCHEMA` for the output schema.
//!
//! Instead of nested `StructBuilder` (which has difficulty with deep nesting),
//! we build arrays bottom-up: columns → tables → schemas → catalogs, then
//! assemble the final `RecordBatch`.

use crate::error::{DatabricksErrorHelper, Result};
use crate::metadata::parse::{CatalogInfo, ColumnInfo, SchemaInfo, TableInfo};
use crate::metadata::type_mapping::databricks_type_to_xdbc;
use adbc_core::schemas::{
    COLUMN_SCHEMA, CONSTRAINT_SCHEMA, GET_OBJECTS_SCHEMA, OBJECTS_DB_SCHEMA_SCHEMA, TABLE_SCHEMA,
};
use arrow_array::builder::{BooleanBuilder, Int16Builder, Int32Builder, StringBuilder};
use arrow_array::{
    Array, ArrayRef, ListArray, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray,
    StructArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Fields};
use driverbase::error::ErrorHelper;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Grouped hierarchy types used by the builder.
pub struct CatalogNode {
    pub catalog_name: String,
    pub schemas: Option<Vec<SchemaNode>>,
}

pub struct SchemaNode {
    pub schema_name: String,
    pub tables: Option<Vec<TableNode>>,
}

pub struct TableNode {
    pub table_name: String,
    pub table_type: String,
    pub columns: Option<Vec<ColumnInfo>>,
}

// ─── Grouping helpers ──────────────────────────────────────────────────────────

/// Group schemas by catalog name.
pub fn group_schemas_by_catalog(schemas: Vec<SchemaInfo>) -> Vec<CatalogNode> {
    let mut map: BTreeMap<String, Vec<SchemaNode>> = BTreeMap::new();
    for s in schemas {
        map.entry(s.catalog_name.clone())
            .or_default()
            .push(SchemaNode {
                schema_name: s.schema_name,
                tables: None,
            });
    }
    map.into_iter()
        .map(|(cat, schemas)| CatalogNode {
            catalog_name: cat,
            schemas: Some(schemas),
        })
        .collect()
}

/// Group tables by catalog → schema.
pub fn group_tables_by_catalog_schema(tables: Vec<TableInfo>) -> Vec<CatalogNode> {
    let mut map: BTreeMap<String, BTreeMap<String, Vec<TableNode>>> = BTreeMap::new();
    for t in tables {
        map.entry(t.catalog_name.clone())
            .or_default()
            .entry(t.schema_name.clone())
            .or_default()
            .push(TableNode {
                table_name: t.table_name,
                table_type: t.table_type,
                columns: None,
            });
    }

    map.into_iter()
        .map(|(cat, schema_map)| CatalogNode {
            catalog_name: cat,
            schemas: Some(
                schema_map
                    .into_iter()
                    .map(|(schema, tables)| SchemaNode {
                        schema_name: schema,
                        tables: Some(tables),
                    })
                    .collect(),
            ),
        })
        .collect()
}

/// Group tables and columns into the full hierarchy for `All` depth.
pub fn group_tables_and_columns(
    tables: Vec<TableInfo>,
    columns: Vec<ColumnInfo>,
) -> Vec<CatalogNode> {
    let mut col_map: BTreeMap<(String, String, String), Vec<ColumnInfo>> = BTreeMap::new();
    for c in columns {
        col_map
            .entry((
                c.catalog_name.clone(),
                c.schema_name.clone(),
                c.table_name.clone(),
            ))
            .or_default()
            .push(c);
    }

    let mut map: BTreeMap<String, BTreeMap<String, Vec<TableNode>>> = BTreeMap::new();
    for t in tables {
        let key = (
            t.catalog_name.clone(),
            t.schema_name.clone(),
            t.table_name.clone(),
        );
        let cols = col_map.remove(&key);
        map.entry(t.catalog_name.clone())
            .or_default()
            .entry(t.schema_name.clone())
            .or_default()
            .push(TableNode {
                table_name: t.table_name,
                table_type: t.table_type,
                columns: Some(cols.unwrap_or_default()),
            });
    }

    map.into_iter()
        .map(|(cat, schema_map)| CatalogNode {
            catalog_name: cat,
            schemas: Some(
                schema_map
                    .into_iter()
                    .map(|(schema, tables)| SchemaNode {
                        schema_name: schema,
                        tables: Some(tables),
                    })
                    .collect(),
            ),
        })
        .collect()
}

// ─── Pattern matching ─────────────────────────────────────────────────────────

/// Filter items by a LIKE-style pattern (% for multi-char, _ for single-char).
pub fn filter_by_pattern<T, F>(items: Vec<T>, pattern: Option<&str>, get_name: F) -> Vec<T>
where
    F: Fn(&T) -> &str,
{
    match pattern {
        None => items,
        Some(p) if p == "%" || p == "*" || p.is_empty() => items,
        Some(p) => items
            .into_iter()
            .filter(|item| like_match(p, get_name(item)))
            .collect(),
    }
}

/// Simple LIKE-style pattern matching.
fn like_match(pattern: &str, value: &str) -> bool {
    let pattern_chars: Vec<char> = pattern.chars().collect();
    let value_chars: Vec<char> = value.chars().collect();
    like_match_impl(&pattern_chars, &value_chars)
}

fn like_match_impl(pattern: &[char], value: &[char]) -> bool {
    match (pattern.first(), value.first()) {
        (None, None) => true,
        (Some(&'%'), _) => {
            like_match_impl(&pattern[1..], value)
                || (!value.is_empty() && like_match_impl(pattern, &value[1..]))
        }
        (Some(&'_'), Some(_)) => like_match_impl(&pattern[1..], &value[1..]),
        (Some(&pc), Some(&vc)) if pc == vc => like_match_impl(&pattern[1..], &value[1..]),
        _ => false,
    }
}

// ─── Arrow builders (bottom-up approach) ──────────────────────────────────────

/// Get the Fields from a DataType::Struct.
fn struct_fields(dt: &DataType) -> &Fields {
    match dt {
        DataType::Struct(fields) => fields,
        _ => panic!("Expected Struct DataType"),
    }
}

/// Build an empty ListArray with the given list item type (all nulls).
fn null_list_array(item_type: DataType, len: usize) -> ArrayRef {
    let field = Arc::new(Field::new("item", item_type, true));
    let empty_values: ArrayRef = arrow_array::new_empty_array(&field.data_type().clone());
    let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(0, len));
    let nulls = NullBuffer::new_null(len);
    Arc::new(ListArray::try_new(field, offsets, empty_values, Some(nulls)).unwrap())
}

/// Build a columns StructArray from a flat list of ColumnInfo.
fn build_columns_array(columns: &[ColumnInfo]) -> ArrayRef {
    let fields = struct_fields(&COLUMN_SCHEMA);
    let n = columns.len();

    let mut col_name = StringBuilder::with_capacity(n, n * 16);
    let mut ordinal = Int32Builder::with_capacity(n);
    let mut remarks = StringBuilder::with_capacity(n, n * 16);
    let mut xdbc_data_type = Int16Builder::with_capacity(n);
    let mut xdbc_type_name = StringBuilder::with_capacity(n, n * 16);
    let mut xdbc_column_size = Int32Builder::with_capacity(n);
    let mut xdbc_decimal_digits = Int16Builder::with_capacity(n);
    let mut xdbc_num_prec_radix = Int16Builder::with_capacity(n);
    let mut xdbc_nullable = Int16Builder::with_capacity(n);
    let mut xdbc_column_def = StringBuilder::with_capacity(n, 0);
    let mut xdbc_sql_data_type = Int16Builder::with_capacity(n);
    let mut xdbc_datetime_sub = Int16Builder::with_capacity(n);
    let mut xdbc_char_octet_length = Int32Builder::with_capacity(n);
    let mut xdbc_is_nullable_str = StringBuilder::with_capacity(n, n * 8);
    let mut xdbc_scope_catalog = StringBuilder::with_capacity(n, 0);
    let mut xdbc_scope_schema = StringBuilder::with_capacity(n, 0);
    let mut xdbc_scope_table = StringBuilder::with_capacity(n, 0);
    let mut xdbc_is_autoincrement = BooleanBuilder::with_capacity(n);
    let mut xdbc_is_generatedcolumn = BooleanBuilder::with_capacity(n);

    for c in columns {
        let xdbc = databricks_type_to_xdbc(&c.column_type);

        col_name.append_value(&c.column_name);
        append_opt_i32(&mut ordinal, c.ordinal_position);
        append_opt_str(&mut remarks, c.remarks.as_deref());
        xdbc_data_type.append_value(xdbc);
        xdbc_type_name.append_value(&c.column_type);
        append_opt_i32(&mut xdbc_column_size, c.column_size);
        append_opt_i16(&mut xdbc_decimal_digits, c.decimal_digits.map(|v| v as i16));
        append_opt_i16(&mut xdbc_num_prec_radix, c.radix.map(|v| v as i16));
        match &c.is_nullable {
            Some(v) if v == "true" || v == "YES" => xdbc_nullable.append_value(1),
            Some(_) => xdbc_nullable.append_value(0),
            None => xdbc_nullable.append_null(),
        }
        xdbc_column_def.append_null();
        xdbc_sql_data_type.append_value(xdbc);
        xdbc_datetime_sub.append_null();
        xdbc_char_octet_length.append_null();
        append_opt_str(&mut xdbc_is_nullable_str, c.is_nullable.as_deref());
        xdbc_scope_catalog.append_null();
        xdbc_scope_schema.append_null();
        xdbc_scope_table.append_null();
        match &c.is_auto_increment {
            Some(v) => xdbc_is_autoincrement.append_value(v == "YES"),
            None => xdbc_is_autoincrement.append_null(),
        }
        match &c.is_generated {
            Some(v) => xdbc_is_generatedcolumn.append_value(v == "YES"),
            None => xdbc_is_generatedcolumn.append_null(),
        }
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(col_name.finish()),
        Arc::new(ordinal.finish()),
        Arc::new(remarks.finish()),
        Arc::new(xdbc_data_type.finish()),
        Arc::new(xdbc_type_name.finish()),
        Arc::new(xdbc_column_size.finish()),
        Arc::new(xdbc_decimal_digits.finish()),
        Arc::new(xdbc_num_prec_radix.finish()),
        Arc::new(xdbc_nullable.finish()),
        Arc::new(xdbc_column_def.finish()),
        Arc::new(xdbc_sql_data_type.finish()),
        Arc::new(xdbc_datetime_sub.finish()),
        Arc::new(xdbc_char_octet_length.finish()),
        Arc::new(xdbc_is_nullable_str.finish()),
        Arc::new(xdbc_scope_catalog.finish()),
        Arc::new(xdbc_scope_schema.finish()),
        Arc::new(xdbc_scope_table.finish()),
        Arc::new(xdbc_is_autoincrement.finish()),
        Arc::new(xdbc_is_generatedcolumn.finish()),
    ];

    Arc::new(StructArray::new(fields.clone(), arrays, None))
}

/// Build a tables StructArray + surrounding ListArray for one schema.
fn build_tables_list(tables: &[TableNode], include_columns: bool) -> ArrayRef {
    let table_fields = struct_fields(&TABLE_SCHEMA);
    let n = tables.len();

    let mut table_name_builder = StringBuilder::with_capacity(n, n * 32);
    let mut table_type_builder = StringBuilder::with_capacity(n, n * 16);

    // For columns: collect per-table column arrays and offsets
    let mut all_columns: Vec<ColumnInfo> = Vec::new();
    let mut col_offsets: Vec<usize> = vec![0];
    let mut col_nulls: Vec<bool> = Vec::new(); // true = valid

    // For constraints: always empty list per table
    let mut constraint_offsets: Vec<usize> = vec![0];

    for t in tables {
        table_name_builder.append_value(&t.table_name);
        table_type_builder.append_value(&t.table_type);

        if include_columns {
            if let Some(ref cols) = t.columns {
                all_columns.extend(cols.iter().cloned());
                col_offsets.push(all_columns.len());
                col_nulls.push(true);
            } else {
                col_offsets.push(all_columns.len());
                col_nulls.push(false);
            }
        } else {
            col_offsets.push(0);
            col_nulls.push(false);
        }
        // constraints: empty list
        constraint_offsets.push(0);
    }

    // Build columns ListArray
    let columns_array: ArrayRef = if include_columns {
        let col_struct = build_columns_array(&all_columns);
        let col_field = Arc::new(Field::new("item", COLUMN_SCHEMA.clone(), true));
        let offsets = OffsetBuffer::from_lengths(col_offsets.windows(2).map(|w| w[1] - w[0]));
        let nulls = NullBuffer::from(col_nulls.clone());
        Arc::new(ListArray::try_new(col_field, offsets, col_struct, Some(nulls)).unwrap())
    } else {
        null_list_array(COLUMN_SCHEMA.clone(), n)
    };

    // Build constraints ListArray (always empty lists)
    let constraint_array: ArrayRef = {
        let constraint_field = Arc::new(Field::new("item", CONSTRAINT_SCHEMA.clone(), true));
        let empty_values: ArrayRef = arrow_array::new_empty_array(&CONSTRAINT_SCHEMA.clone());
        let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(0, n));
        Arc::new(ListArray::try_new(constraint_field, offsets, empty_values, None).unwrap())
    };

    let table_struct = StructArray::new(
        table_fields.clone(),
        vec![
            Arc::new(table_name_builder.finish()) as ArrayRef,
            Arc::new(table_type_builder.finish()) as ArrayRef,
            columns_array,
            constraint_array,
        ],
        None,
    );

    // Wrap in ListArray (one list element for this schema)
    let table_field = Arc::new(Field::new("item", TABLE_SCHEMA.clone(), true));
    let offsets = OffsetBuffer::from_lengths(vec![n]);
    Arc::new(ListArray::try_new(table_field, offsets, Arc::new(table_struct), None).unwrap())
}

/// Build the db_schemas ListArray for a set of CatalogNodes.
fn build_db_schemas_list_array(catalogs: &[CatalogNode]) -> ArrayRef {
    let schema_fields = struct_fields(&OBJECTS_DB_SCHEMA_SCHEMA);

    // Flatten all schemas across all catalogs
    let mut schema_names: Vec<Option<String>> = Vec::new();
    let mut tables_arrays: Vec<ArrayRef> = Vec::new();
    let mut catalog_offsets: Vec<usize> = vec![0];
    let mut catalog_nulls: Vec<bool> = Vec::new();

    for cat in catalogs {
        if let Some(ref schemas) = cat.schemas {
            for s in schemas {
                schema_names.push(Some(s.schema_name.clone()));
                if let Some(ref tables) = s.tables {
                    tables_arrays.push(build_tables_list(tables, has_columns(schemas)));
                } else {
                    // null tables list
                    tables_arrays.push(null_list_array(TABLE_SCHEMA.clone(), 0));
                }
            }
            catalog_offsets.push(schema_names.len());
            catalog_nulls.push(true);
        } else {
            catalog_offsets.push(schema_names.len());
            catalog_nulls.push(false);
        }
    }

    if schema_names.is_empty() {
        // No schemas at all — return properly typed null arrays
        let db_schema_field = Arc::new(Field::new("item", OBJECTS_DB_SCHEMA_SCHEMA.clone(), true));
        let empty_names = Arc::new(StringArray::from(Vec::<Option<&str>>::new())) as ArrayRef;
        let table_list_field = Arc::new(Field::new("item", TABLE_SCHEMA.clone(), true));
        let empty_table_values: ArrayRef = arrow_array::new_empty_array(&TABLE_SCHEMA.clone());
        let empty_tables_list = Arc::new(
            ListArray::try_new(
                table_list_field,
                OffsetBuffer::from_lengths(Vec::<usize>::new()),
                empty_table_values,
                None,
            )
            .unwrap(),
        ) as ArrayRef;
        let empty_struct = Arc::new(StructArray::new(
            schema_fields.clone(),
            vec![empty_names, empty_tables_list],
            None,
        )) as ArrayRef;
        let offsets = OffsetBuffer::from_lengths(catalog_offsets.windows(2).map(|w| w[1] - w[0]));
        let nulls = NullBuffer::from(catalog_nulls);
        return Arc::new(
            ListArray::try_new(db_schema_field, offsets, empty_struct, Some(nulls)).unwrap(),
        );
    }

    // Build schema_name StringArray
    let names_array = Arc::new(StringArray::from(
        schema_names
            .iter()
            .map(|s| s.as_deref())
            .collect::<Vec<_>>(),
    )) as ArrayRef;

    // Concatenate all tables ListArrays into one
    let total_schemas = tables_arrays.len();
    let tables_list_array = if total_schemas > 0 {
        // Each tables_arrays[i] is a ListArray with exactly 1 element (the list for that schema).
        // We need to combine them into a single ListArray with total_schemas elements.
        let mut all_table_structs: Vec<ArrayRef> = Vec::new();
        let mut schema_to_table_offsets: Vec<usize> = vec![0];
        let mut schema_nulls: Vec<bool> = Vec::new();

        for ta in &tables_arrays {
            let la = ta.as_any().downcast_ref::<ListArray>().unwrap();
            if la.len() == 0 {
                // null tables for this schema
                schema_to_table_offsets.push(schema_to_table_offsets.last().copied().unwrap());
                schema_nulls.push(false);
            } else {
                let values = la.values();
                let n_tables = la.value_length(0) as usize;
                if n_tables > 0 {
                    all_table_structs.push(values.slice(la.value_offsets()[0] as usize, n_tables));
                }
                schema_to_table_offsets
                    .push(schema_to_table_offsets.last().copied().unwrap() + n_tables);
                schema_nulls.push(!la.is_null(0));
            }
        }

        let table_field = Arc::new(Field::new("item", TABLE_SCHEMA.clone(), true));
        let combined_tables: ArrayRef = if all_table_structs.is_empty() {
            arrow_array::new_empty_array(&TABLE_SCHEMA.clone())
        } else {
            let refs: Vec<&dyn Array> = all_table_structs.iter().map(|a| a.as_ref()).collect();
            arrow_select::concat::concat(&refs).unwrap()
        };

        let offsets =
            OffsetBuffer::from_lengths(schema_to_table_offsets.windows(2).map(|w| w[1] - w[0]));
        let nulls = NullBuffer::from(schema_nulls);
        Arc::new(ListArray::try_new(table_field, offsets, combined_tables, Some(nulls)).unwrap())
            as ArrayRef
    } else {
        let table_field = Arc::new(Field::new("item", TABLE_SCHEMA.clone(), true));
        let empty_values = arrow_array::new_empty_array(&TABLE_SCHEMA.clone());
        Arc::new(
            ListArray::try_new(
                table_field,
                OffsetBuffer::from_lengths(Vec::<usize>::new()),
                empty_values,
                None,
            )
            .unwrap(),
        ) as ArrayRef
    };

    // Build schema struct
    let schema_struct = Arc::new(StructArray::new(
        schema_fields.clone(),
        vec![names_array, tables_list_array],
        None,
    )) as ArrayRef;

    // Wrap in ListArray per catalog
    let db_schema_field = Arc::new(Field::new("item", OBJECTS_DB_SCHEMA_SCHEMA.clone(), true));
    let offsets = OffsetBuffer::from_lengths(catalog_offsets.windows(2).map(|w| w[1] - w[0]));
    let nulls = NullBuffer::from(catalog_nulls);
    Arc::new(ListArray::try_new(db_schema_field, offsets, schema_struct, Some(nulls)).unwrap())
}

/// Check if any schema in the list has table nodes with columns.
fn has_columns(schemas: &[SchemaNode]) -> bool {
    schemas.iter().any(|s| {
        s.tables
            .as_ref()
            .is_some_and(|tables| tables.iter().any(|t| t.columns.is_some()))
    })
}

fn make_result(catalogs: &[CatalogNode]) -> Result<impl RecordBatchReader + Send> {
    let schema = GET_OBJECTS_SCHEMA.clone();

    let catalog_names = Arc::new(StringArray::from(
        catalogs
            .iter()
            .map(|c| Some(c.catalog_name.as_str()))
            .collect::<Vec<_>>(),
    )) as ArrayRef;

    let db_schemas = build_db_schemas_list_array(catalogs);

    let batch =
        RecordBatch::try_new(schema.clone(), vec![catalog_names, db_schemas]).map_err(|e| {
            DatabricksErrorHelper::io()
                .message(format!("Failed to build get_objects result: {}", e))
        })?;

    Ok(RecordBatchIterator::new(
        vec![Ok(batch)].into_iter(),
        schema,
    ))
}

/// Build get_objects result at Catalogs depth.
pub fn build_get_objects_catalogs(
    catalogs: Vec<CatalogInfo>,
) -> Result<impl RecordBatchReader + Send> {
    let nodes: Vec<CatalogNode> = catalogs
        .into_iter()
        .map(|c| CatalogNode {
            catalog_name: c.catalog_name,
            schemas: None,
        })
        .collect();
    make_result(&nodes)
}

/// Build get_objects result at Schemas depth.
pub fn build_get_objects_schemas(
    catalogs: Vec<CatalogNode>,
) -> Result<impl RecordBatchReader + Send> {
    make_result(&catalogs)
}

/// Build get_objects result at Tables depth.
pub fn build_get_objects_tables(
    catalogs: Vec<CatalogNode>,
) -> Result<impl RecordBatchReader + Send> {
    make_result(&catalogs)
}

/// Build get_objects result at All depth (includes columns).
pub fn build_get_objects_all(catalogs: Vec<CatalogNode>) -> Result<impl RecordBatchReader + Send> {
    make_result(&catalogs)
}

/// Collect any `impl RecordBatchReader` into a concrete `RecordBatchIterator`.
pub fn collect_reader(
    reader: impl RecordBatchReader + Send,
) -> Result<
    RecordBatchIterator<
        std::vec::IntoIter<std::result::Result<RecordBatch, arrow_schema::ArrowError>>,
    >,
> {
    let schema = reader.schema();
    let batches: Vec<_> = reader.collect();
    Ok(RecordBatchIterator::new(batches.into_iter(), schema))
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn append_opt_str(builder: &mut StringBuilder, val: Option<&str>) {
    match val {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

fn append_opt_i32(builder: &mut Int32Builder, val: Option<i32>) {
    match val {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

fn append_opt_i16(builder: &mut Int16Builder, val: Option<i16>) {
    match val {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::RecordBatchReader;

    #[test]
    fn test_like_match() {
        assert!(like_match("%", "anything"));
        assert!(like_match("%", ""));
        assert!(like_match("main", "main"));
        assert!(!like_match("main", "other"));
        assert!(like_match("main%", "main"));
        assert!(like_match("main%", "main_extra"));
        assert!(like_match("%main", "my_main"));
        assert!(like_match("m_in", "main"));
        assert!(!like_match("m_in", "matin"));
        assert!(like_match("d%t", "default"));
        assert!(like_match("d%t", "dt"));
        assert!(!like_match("d%t", "data"));
    }

    #[test]
    fn test_filter_by_pattern() {
        let items = vec![
            CatalogInfo {
                catalog_name: "main".to_string(),
            },
            CatalogInfo {
                catalog_name: "hive_metastore".to_string(),
            },
            CatalogInfo {
                catalog_name: "system".to_string(),
            },
        ];

        let filtered = filter_by_pattern(items.clone(), None, |c| &c.catalog_name);
        assert_eq!(filtered.len(), 3);

        let filtered = filter_by_pattern(items.clone(), Some("%"), |c| &c.catalog_name);
        assert_eq!(filtered.len(), 3);

        let filtered = filter_by_pattern(items.clone(), Some("main"), |c| &c.catalog_name);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].catalog_name, "main");

        let filtered = filter_by_pattern(items.clone(), Some("%store"), |c| &c.catalog_name);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].catalog_name, "hive_metastore");
    }

    #[test]
    fn test_group_schemas_by_catalog() {
        let schemas = vec![
            SchemaInfo {
                catalog_name: "main".to_string(),
                schema_name: "default".to_string(),
            },
            SchemaInfo {
                catalog_name: "main".to_string(),
                schema_name: "information_schema".to_string(),
            },
            SchemaInfo {
                catalog_name: "hive".to_string(),
                schema_name: "default".to_string(),
            },
        ];

        let grouped = group_schemas_by_catalog(schemas);
        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped[0].catalog_name, "hive");
        assert_eq!(grouped[0].schemas.as_ref().unwrap().len(), 1);
        assert_eq!(grouped[1].catalog_name, "main");
        assert_eq!(grouped[1].schemas.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn test_group_tables_by_catalog_schema() {
        let tables = vec![
            TableInfo {
                catalog_name: "main".to_string(),
                schema_name: "default".to_string(),
                table_name: "t1".to_string(),
                table_type: "TABLE".to_string(),
                remarks: None,
            },
            TableInfo {
                catalog_name: "main".to_string(),
                schema_name: "default".to_string(),
                table_name: "t2".to_string(),
                table_type: "VIEW".to_string(),
                remarks: None,
            },
            TableInfo {
                catalog_name: "main".to_string(),
                schema_name: "other".to_string(),
                table_name: "t3".to_string(),
                table_type: "TABLE".to_string(),
                remarks: None,
            },
        ];

        let grouped = group_tables_by_catalog_schema(tables);
        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped[0].catalog_name, "main");
        let schemas = grouped[0].schemas.as_ref().unwrap();
        assert_eq!(schemas.len(), 2);
        assert_eq!(schemas[0].schema_name, "default");
        assert_eq!(schemas[0].tables.as_ref().unwrap().len(), 2);
        assert_eq!(schemas[1].schema_name, "other");
        assert_eq!(schemas[1].tables.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn test_build_get_objects_catalogs() {
        let catalogs = vec![
            CatalogInfo {
                catalog_name: "main".to_string(),
            },
            CatalogInfo {
                catalog_name: "hive_metastore".to_string(),
            },
        ];

        let reader = build_get_objects_catalogs(catalogs).unwrap();
        let schema = reader.schema();
        assert_eq!(schema, *GET_OBJECTS_SCHEMA);
        let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn test_build_get_objects_schemas() {
        let catalogs = vec![CatalogNode {
            catalog_name: "main".to_string(),
            schemas: Some(vec![
                SchemaNode {
                    schema_name: "default".to_string(),
                    tables: None,
                },
                SchemaNode {
                    schema_name: "info".to_string(),
                    tables: None,
                },
            ]),
        }];

        let reader = build_get_objects_schemas(catalogs).unwrap();
        assert_eq!(reader.schema(), *GET_OBJECTS_SCHEMA);
        let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn test_build_get_objects_tables() {
        let catalogs = vec![CatalogNode {
            catalog_name: "main".to_string(),
            schemas: Some(vec![SchemaNode {
                schema_name: "default".to_string(),
                tables: Some(vec![
                    TableNode {
                        table_name: "t1".to_string(),
                        table_type: "TABLE".to_string(),
                        columns: None,
                    },
                    TableNode {
                        table_name: "t2".to_string(),
                        table_type: "VIEW".to_string(),
                        columns: None,
                    },
                ]),
            }]),
        }];

        let reader = build_get_objects_tables(catalogs).unwrap();
        let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn test_build_get_objects_all() {
        let catalogs = vec![CatalogNode {
            catalog_name: "main".to_string(),
            schemas: Some(vec![SchemaNode {
                schema_name: "default".to_string(),
                tables: Some(vec![TableNode {
                    table_name: "t1".to_string(),
                    table_type: "TABLE".to_string(),
                    columns: Some(vec![ColumnInfo {
                        catalog_name: "main".to_string(),
                        schema_name: "default".to_string(),
                        table_name: "t1".to_string(),
                        column_name: "id".to_string(),
                        column_type: "INT".to_string(),
                        column_size: Some(10),
                        decimal_digits: Some(0),
                        radix: Some(10),
                        is_nullable: Some("false".to_string()),
                        remarks: Some("Primary key".to_string()),
                        ordinal_position: Some(1),
                        is_auto_increment: Some("NO".to_string()),
                        is_generated: Some("NO".to_string()),
                    }]),
                }]),
            }]),
        }];

        let reader = build_get_objects_all(catalogs).unwrap();
        assert_eq!(reader.schema(), *GET_OBJECTS_SCHEMA);
        let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn test_build_get_objects_empty() {
        let reader = build_get_objects_catalogs(vec![]).unwrap();
        let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);
    }
}
