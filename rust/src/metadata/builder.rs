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

//! Arrow builder for `get_objects` responses.
//!
//! Constructs the nested Arrow struct hierarchy required by the ADBC
//! `get_objects` specification (`GET_OBJECTS_SCHEMA`). Groups parsed
//! metadata into catalog -> schema -> table -> column hierarchy and
//! builds the corresponding `RecordBatch`.

use std::collections::BTreeMap;
use std::sync::Arc;

use adbc_core::schemas::GET_OBJECTS_SCHEMA;
use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Int16Builder, Int32Builder, ListBuilder, StringBuilder,
    StructBuilder,
};
use arrow_array::{ArrayRef, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray};
use arrow_schema::{ArrowError, DataType, Fields};

use crate::metadata::parse::{CatalogInfo, ColumnInfo, SchemaInfo, TableInfo};
use crate::metadata::type_mapping::databricks_type_to_xdbc;

// ---------------------------------------------------------------------------
// Hierarchy types
// ---------------------------------------------------------------------------

/// A catalog with optional nested schemas (None at Catalogs depth).
pub struct CatalogNode {
    pub catalog_name: String,
    pub schemas: Option<Vec<SchemaNode>>,
}

/// A schema within a catalog, with optional nested tables (None at Schemas depth).
pub struct SchemaNode {
    pub schema_name: String,
    pub tables: Option<Vec<TableNode>>,
}

/// A table within a schema, with optional nested columns (None at Tables depth).
pub struct TableNode {
    pub table_name: String,
    pub table_type: String,
    pub columns: Option<Vec<ColumnInfo>>,
}

// ---------------------------------------------------------------------------
// Grouping helpers
// ---------------------------------------------------------------------------

/// Group flat schema results by catalog name.
///
/// Returns a list of `CatalogNode`s with `schemas` populated (tables are None
/// within each schema since this is used at Schemas depth).
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
        .map(|(catalog_name, schemas)| CatalogNode {
            catalog_name,
            schemas: Some(schemas),
        })
        .collect()
}

/// Group flat table results by catalog -> schema.
///
/// Returns a list of `CatalogNode`s with `schemas.tables` populated
/// (columns are None within each table since this is used at Tables depth).
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
        .map(|(catalog_name, schema_map)| {
            let schemas = schema_map
                .into_iter()
                .map(|(schema_name, tables)| SchemaNode {
                    schema_name,
                    tables: Some(tables),
                })
                .collect();
            CatalogNode {
                catalog_name,
                schemas: Some(schemas),
            }
        })
        .collect()
}

/// Group tables and columns into the full hierarchy (for All depth).
///
/// Tables provide `table_type` (which is not in the columns result).
/// Columns are matched to tables by (catalog, schema, table) key.
pub fn group_tables_and_columns(
    tables: Vec<TableInfo>,
    columns: Vec<ColumnInfo>,
) -> Vec<CatalogNode> {
    // Build a lookup: (catalog, schema, table) -> Vec<ColumnInfo>
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
        let cols = col_map.remove(&key).unwrap_or_default();
        map.entry(t.catalog_name.clone())
            .or_default()
            .entry(t.schema_name.clone())
            .or_default()
            .push(TableNode {
                table_name: t.table_name,
                table_type: t.table_type,
                columns: Some(cols),
            });
    }

    map.into_iter()
        .map(|(catalog_name, schema_map)| {
            let schemas = schema_map
                .into_iter()
                .map(|(schema_name, tables)| SchemaNode {
                    schema_name,
                    tables: Some(tables),
                })
                .collect();
            CatalogNode {
                catalog_name,
                schemas: Some(schemas),
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Schema extraction helpers
// ---------------------------------------------------------------------------

/// Extract the inner Fields of the db_schema struct from GET_OBJECTS_SCHEMA.
fn db_schema_struct_fields() -> Fields {
    let schema = GET_OBJECTS_SCHEMA.clone();
    let list_field = schema.field(1); // catalog_db_schemas
    match list_field.data_type() {
        DataType::List(inner) => match inner.data_type() {
            DataType::Struct(fields) => fields.clone(),
            _ => unreachable!("Expected struct inside catalog_db_schemas list"),
        },
        _ => unreachable!("Expected list for catalog_db_schemas"),
    }
}

// ---------------------------------------------------------------------------
// Builder functions
// ---------------------------------------------------------------------------

/// Build `get_objects` result at **Catalogs** depth.
///
/// `catalog_db_schemas` is null for each catalog.
pub fn build_get_objects_catalogs(
    catalogs: Vec<CatalogInfo>,
) -> Result<impl RecordBatchReader + Send, adbc_core::error::Error> {
    let schema = GET_OBJECTS_SCHEMA.clone();
    let num_rows = catalogs.len();

    let catalog_names: Vec<Option<&str>> = catalogs
        .iter()
        .map(|c| Some(c.catalog_name.as_str()))
        .collect();
    let catalog_name_array = Arc::new(StringArray::from(catalog_names)) as ArrayRef;

    // catalog_db_schemas: all null (Catalogs depth)
    let db_schemas_array = arrow_array::new_null_array(schema.field(1).data_type(), num_rows);

    let batch = RecordBatch::try_new(schema.clone(), vec![catalog_name_array, db_schemas_array])
        .map_err(|e| to_adbc_error("build_get_objects_catalogs", e))?;

    Ok(RecordBatchIterator::new(vec![Ok(batch)], schema))
}

/// Build `get_objects` result at **Schemas** depth.
///
/// `db_schema_tables` is null for each schema.
pub fn build_get_objects_schemas(
    catalogs: Vec<CatalogNode>,
) -> Result<impl RecordBatchReader + Send, adbc_core::error::Error> {
    let schema = GET_OBJECTS_SCHEMA.clone();
    let db_schema_fields = db_schema_struct_fields();

    let num_catalogs = catalogs.len();
    let mut catalog_names = StringBuilder::with_capacity(num_catalogs, num_catalogs * 32);

    // When using StructBuilder::from_fields, nested list fields are created as
    // ListBuilder<Box<dyn ArrayBuilder>>. We must use that type for field_builder<>
    // and then downcast .values() to the concrete builder type.
    let mut db_schema_list_builder =
        ListBuilder::new(StructBuilder::from_fields(db_schema_fields, 0));

    for cat in &catalogs {
        catalog_names.append_value(&cat.catalog_name);

        match &cat.schemas {
            Some(schemas) => {
                let struct_builder = db_schema_list_builder.values();
                for s in schemas {
                    // db_schema_name (field 0: StringBuilder)
                    struct_builder
                        .field_builder::<StringBuilder>(0)
                        .unwrap()
                        .append_value(&s.schema_name);

                    // db_schema_tables (field 1): null at Schemas depth
                    // This is a List field, created as ListBuilder<Box<dyn ArrayBuilder>>
                    struct_builder
                        .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(1)
                        .unwrap()
                        .append_null();

                    struct_builder.append(true);
                }
                db_schema_list_builder.append(true);
            }
            None => {
                db_schema_list_builder.append_null();
            }
        }
    }

    let catalog_name_array = Arc::new(catalog_names.finish()) as ArrayRef;
    let db_schemas_array = Arc::new(db_schema_list_builder.finish()) as ArrayRef;

    let batch = RecordBatch::try_new(schema.clone(), vec![catalog_name_array, db_schemas_array])
        .map_err(|e| to_adbc_error("build_get_objects_schemas", e))?;

    Ok(RecordBatchIterator::new(vec![Ok(batch)], schema))
}

/// Build `get_objects` result at **Tables** depth.
///
/// `table_columns` and `table_constraints` are null for each table.
pub fn build_get_objects_tables(
    catalogs: Vec<CatalogNode>,
) -> Result<impl RecordBatchReader + Send, adbc_core::error::Error> {
    let schema = GET_OBJECTS_SCHEMA.clone();
    let db_schema_fields = db_schema_struct_fields();

    let num_catalogs = catalogs.len();
    let mut catalog_names = StringBuilder::with_capacity(num_catalogs, num_catalogs * 32);

    let mut db_schema_list_builder =
        ListBuilder::new(StructBuilder::from_fields(db_schema_fields, 0));

    for cat in &catalogs {
        catalog_names.append_value(&cat.catalog_name);

        match &cat.schemas {
            Some(schemas) => {
                let db_struct_builder = db_schema_list_builder.values();
                for s in schemas {
                    // db_schema_name
                    db_struct_builder
                        .field_builder::<StringBuilder>(0)
                        .unwrap()
                        .append_value(&s.schema_name);

                    // db_schema_tables (field 1)
                    let tables_list_builder = db_struct_builder
                        .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(1)
                        .unwrap();

                    match &s.tables {
                        Some(tables) => {
                            let table_struct_builder = tables_list_builder
                                .values()
                                .as_any_mut()
                                .downcast_mut::<StructBuilder>()
                                .unwrap();

                            for t in tables {
                                // table_name (field 0)
                                table_struct_builder
                                    .field_builder::<StringBuilder>(0)
                                    .unwrap()
                                    .append_value(&t.table_name);
                                // table_type (field 1)
                                table_struct_builder
                                    .field_builder::<StringBuilder>(1)
                                    .unwrap()
                                    .append_value(&t.table_type);
                                // table_columns (field 2): null at Tables depth
                                table_struct_builder
                                    .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(2)
                                    .unwrap()
                                    .append_null();
                                // table_constraints (field 3): null at Tables depth
                                table_struct_builder
                                    .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(3)
                                    .unwrap()
                                    .append_null();
                                table_struct_builder.append(true);
                            }
                            tables_list_builder.append(true);
                        }
                        None => {
                            tables_list_builder.append_null();
                        }
                    }

                    db_struct_builder.append(true);
                }
                db_schema_list_builder.append(true);
            }
            None => {
                db_schema_list_builder.append_null();
            }
        }
    }

    let catalog_name_array = Arc::new(catalog_names.finish()) as ArrayRef;
    let db_schemas_array = Arc::new(db_schema_list_builder.finish()) as ArrayRef;

    let batch = RecordBatch::try_new(schema.clone(), vec![catalog_name_array, db_schemas_array])
        .map_err(|e| to_adbc_error("build_get_objects_tables", e))?;

    Ok(RecordBatchIterator::new(vec![Ok(batch)], schema))
}

/// Build `get_objects` result at **All** depth.
///
/// Includes columns with xdbc fields. Constraints are empty lists.
pub fn build_get_objects_all(
    catalogs: Vec<CatalogNode>,
) -> Result<impl RecordBatchReader + Send, adbc_core::error::Error> {
    let schema = GET_OBJECTS_SCHEMA.clone();
    let db_schema_fields = db_schema_struct_fields();

    let num_catalogs = catalogs.len();
    let mut catalog_names = StringBuilder::with_capacity(num_catalogs, num_catalogs * 32);

    let mut db_schema_list_builder =
        ListBuilder::new(StructBuilder::from_fields(db_schema_fields, 0));

    for cat in &catalogs {
        catalog_names.append_value(&cat.catalog_name);

        match &cat.schemas {
            Some(schemas) => {
                let db_struct_builder = db_schema_list_builder.values();
                for s in schemas {
                    // db_schema_name
                    db_struct_builder
                        .field_builder::<StringBuilder>(0)
                        .unwrap()
                        .append_value(&s.schema_name);

                    // db_schema_tables (field 1)
                    let tables_list_builder = db_struct_builder
                        .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(1)
                        .unwrap();

                    match &s.tables {
                        Some(tables) => {
                            let table_struct_builder = tables_list_builder
                                .values()
                                .as_any_mut()
                                .downcast_mut::<StructBuilder>()
                                .unwrap();

                            for t in tables {
                                // table_name (field 0)
                                table_struct_builder
                                    .field_builder::<StringBuilder>(0)
                                    .unwrap()
                                    .append_value(&t.table_name);
                                // table_type (field 1)
                                table_struct_builder
                                    .field_builder::<StringBuilder>(1)
                                    .unwrap()
                                    .append_value(&t.table_type);

                                // table_columns (field 2)
                                let columns_list_builder = table_struct_builder
                                    .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(2)
                                    .unwrap();

                                match &t.columns {
                                    Some(cols) => {
                                        let col_struct = columns_list_builder
                                            .values()
                                            .as_any_mut()
                                            .downcast_mut::<StructBuilder>()
                                            .unwrap();
                                        for c in cols {
                                            append_column_fields(col_struct, c);
                                            col_struct.append(true);
                                        }
                                        columns_list_builder.append(true);
                                    }
                                    None => {
                                        columns_list_builder.append_null();
                                    }
                                }

                                // table_constraints (field 3): empty list
                                table_struct_builder
                                    .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(3)
                                    .unwrap()
                                    .append(true); // empty list (not null)

                                table_struct_builder.append(true);
                            }
                            tables_list_builder.append(true);
                        }
                        None => {
                            tables_list_builder.append_null();
                        }
                    }

                    db_struct_builder.append(true);
                }
                db_schema_list_builder.append(true);
            }
            None => {
                db_schema_list_builder.append_null();
            }
        }
    }

    let catalog_name_array = Arc::new(catalog_names.finish()) as ArrayRef;
    let db_schemas_array = Arc::new(db_schema_list_builder.finish()) as ArrayRef;

    let batch = RecordBatch::try_new(schema.clone(), vec![catalog_name_array, db_schemas_array])
        .map_err(|e| to_adbc_error("build_get_objects_all", e))?;

    Ok(RecordBatchIterator::new(vec![Ok(batch)], schema))
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Append all 19 column fields from a `ColumnInfo` into a column struct builder.
///
/// Field order matches `COLUMN_SCHEMA` from `adbc_core::schemas`:
///  0: column_name (Utf8)
///  1: ordinal_position (Int32)
///  2: remarks (Utf8)
///  3: xdbc_data_type (Int16)
///  4: xdbc_type_name (Utf8)
///  5: xdbc_column_size (Int32)
///  6: xdbc_decimal_digits (Int16)
///  7: xdbc_num_prec_radix (Int16)
///  8: xdbc_nullable (Int16)
///  9: xdbc_column_def (Utf8)
/// 10: xdbc_sql_data_type (Int16)
/// 11: xdbc_datetime_sub (Int16)
/// 12: xdbc_char_octet_length (Int32)
/// 13: xdbc_is_nullable (Utf8)
/// 14: xdbc_scope_catalog (Utf8)
/// 15: xdbc_scope_schema (Utf8)
/// 16: xdbc_scope_table (Utf8)
/// 17: xdbc_is_autoincrement (Boolean)
/// 18: xdbc_is_generatedcolumn (Boolean)
fn append_column_fields(builder: &mut StructBuilder, col: &ColumnInfo) {
    // 0: column_name
    builder
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_value(&col.column_name);

    // 1: ordinal_position
    append_optional_i32(builder, 1, col.ordinal_position);

    // 2: remarks
    append_optional_string(builder, 2, col.remarks.as_deref());

    // 3: xdbc_data_type
    let xdbc_type = databricks_type_to_xdbc(&col.column_type);
    builder
        .field_builder::<Int16Builder>(3)
        .unwrap()
        .append_value(xdbc_type);

    // 4: xdbc_type_name
    builder
        .field_builder::<StringBuilder>(4)
        .unwrap()
        .append_value(&col.column_type);

    // 5: xdbc_column_size
    append_optional_i32(builder, 5, col.column_size);

    // 6: xdbc_decimal_digits (Int16 from Int32)
    append_optional_i16(builder, 6, col.decimal_digits.map(|v| v as i16));

    // 7: xdbc_num_prec_radix (Int16 from Int32)
    append_optional_i16(builder, 7, col.radix.map(|v| v as i16));

    // 8: xdbc_nullable — map "true"->1, "false"->0
    let xdbc_nullable = col.is_nullable.as_ref().and_then(|v| {
        if v.eq_ignore_ascii_case("true") {
            Some(1i16)
        } else if v.eq_ignore_ascii_case("false") {
            Some(0i16)
        } else {
            None
        }
    });
    append_optional_i16(builder, 8, xdbc_nullable);

    // 9: xdbc_column_def — not available
    append_optional_string(builder, 9, None);

    // 10: xdbc_sql_data_type — same as xdbc_data_type
    builder
        .field_builder::<Int16Builder>(10)
        .unwrap()
        .append_value(xdbc_type);

    // 11: xdbc_datetime_sub — not available
    append_optional_i16(builder, 11, None);

    // 12: xdbc_char_octet_length — not available
    append_optional_i32(builder, 12, None);

    // 13: xdbc_is_nullable — direct string
    append_optional_string(builder, 13, col.is_nullable.as_deref());

    // 14-16: xdbc_scope_catalog, xdbc_scope_schema, xdbc_scope_table — not available
    append_optional_string(builder, 14, None);
    append_optional_string(builder, 15, None);
    append_optional_string(builder, 16, None);

    // 17: xdbc_is_autoincrement — "YES"->true, else false
    match &col.is_auto_increment {
        Some(v) => builder
            .field_builder::<BooleanBuilder>(17)
            .unwrap()
            .append_value(v.eq_ignore_ascii_case("YES")),
        None => builder
            .field_builder::<BooleanBuilder>(17)
            .unwrap()
            .append_null(),
    }

    // 18: xdbc_is_generatedcolumn — "YES"->true, else false
    match &col.is_generated {
        Some(v) => builder
            .field_builder::<BooleanBuilder>(18)
            .unwrap()
            .append_value(v.eq_ignore_ascii_case("YES")),
        None => builder
            .field_builder::<BooleanBuilder>(18)
            .unwrap()
            .append_null(),
    }
}

fn append_optional_string(builder: &mut StructBuilder, idx: usize, val: Option<&str>) {
    let b = builder.field_builder::<StringBuilder>(idx).unwrap();
    match val {
        Some(v) => b.append_value(v),
        None => b.append_null(),
    }
}

fn append_optional_i16(builder: &mut StructBuilder, idx: usize, val: Option<i16>) {
    let b = builder.field_builder::<Int16Builder>(idx).unwrap();
    match val {
        Some(v) => b.append_value(v),
        None => b.append_null(),
    }
}

fn append_optional_i32(builder: &mut StructBuilder, idx: usize, val: Option<i32>) {
    let b = builder.field_builder::<Int32Builder>(idx).unwrap();
    match val {
        Some(v) => b.append_value(v),
        None => b.append_null(),
    }
}

fn to_adbc_error(context: &str, err: ArrowError) -> adbc_core::error::Error {
    use driverbase::error::ErrorHelper;
    crate::error::DatabricksErrorHelper::io()
        .message(format!("Failed to {}: {}", context, err))
        .to_adbc()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::{Array, StructArray};

    fn make_catalogs(names: &[&str]) -> Vec<CatalogInfo> {
        names
            .iter()
            .map(|n| CatalogInfo {
                catalog_name: n.to_string(),
            })
            .collect()
    }

    fn make_schemas(entries: &[(&str, &str)]) -> Vec<SchemaInfo> {
        entries
            .iter()
            .map(|(cat, sch)| SchemaInfo {
                catalog_name: cat.to_string(),
                schema_name: sch.to_string(),
            })
            .collect()
    }

    fn make_tables(entries: &[(&str, &str, &str, &str)]) -> Vec<TableInfo> {
        entries
            .iter()
            .map(|(cat, sch, tbl, typ)| TableInfo {
                catalog_name: cat.to_string(),
                schema_name: sch.to_string(),
                table_name: tbl.to_string(),
                table_type: typ.to_string(),
                remarks: None,
            })
            .collect()
    }

    fn make_column(
        cat: &str,
        sch: &str,
        tbl: &str,
        col: &str,
        col_type: &str,
        ordinal: i32,
    ) -> ColumnInfo {
        ColumnInfo {
            catalog_name: cat.to_string(),
            schema_name: sch.to_string(),
            table_name: tbl.to_string(),
            column_name: col.to_string(),
            column_type: col_type.to_string(),
            column_size: Some(19),
            decimal_digits: Some(0),
            radix: Some(10),
            is_nullable: Some("true".to_string()),
            remarks: Some("test column".to_string()),
            ordinal_position: Some(ordinal),
            is_auto_increment: Some("NO".to_string()),
            is_generated: Some("NO".to_string()),
        }
    }

    // -----------------------------------------------------------------------
    // Grouping helper tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_group_schemas_by_catalog() {
        let schemas = make_schemas(&[
            ("main", "default"),
            ("main", "information_schema"),
            ("hive_metastore", "default"),
        ]);

        let result = group_schemas_by_catalog(schemas);

        assert_eq!(result.len(), 2);

        // BTreeMap ordering: hive_metastore < main
        assert_eq!(result[0].catalog_name, "hive_metastore");
        let hive_schemas = result[0].schemas.as_ref().unwrap();
        assert_eq!(hive_schemas.len(), 1);
        assert_eq!(hive_schemas[0].schema_name, "default");
        assert!(hive_schemas[0].tables.is_none());

        assert_eq!(result[1].catalog_name, "main");
        let main_schemas = result[1].schemas.as_ref().unwrap();
        assert_eq!(main_schemas.len(), 2);
        assert_eq!(main_schemas[0].schema_name, "default");
        assert_eq!(main_schemas[1].schema_name, "information_schema");
    }

    #[test]
    fn test_group_tables_by_catalog_schema() {
        let tables = make_tables(&[
            ("main", "default", "users", "TABLE"),
            ("main", "default", "orders", "TABLE"),
            ("main", "analytics", "events", "VIEW"),
            ("hive_metastore", "default", "legacy", "TABLE"),
        ]);

        let result = group_tables_by_catalog_schema(tables);

        assert_eq!(result.len(), 2);

        // hive_metastore
        assert_eq!(result[0].catalog_name, "hive_metastore");
        let hive_schemas = result[0].schemas.as_ref().unwrap();
        assert_eq!(hive_schemas.len(), 1);
        assert_eq!(hive_schemas[0].schema_name, "default");
        let hive_tables = hive_schemas[0].tables.as_ref().unwrap();
        assert_eq!(hive_tables.len(), 1);
        assert_eq!(hive_tables[0].table_name, "legacy");
        assert!(hive_tables[0].columns.is_none());

        // main
        assert_eq!(result[1].catalog_name, "main");
        let main_schemas = result[1].schemas.as_ref().unwrap();
        assert_eq!(main_schemas.len(), 2);
        assert_eq!(main_schemas[0].schema_name, "analytics");
        assert_eq!(main_schemas[0].tables.as_ref().unwrap().len(), 1);
        assert_eq!(main_schemas[1].schema_name, "default");
        assert_eq!(main_schemas[1].tables.as_ref().unwrap().len(), 2);
    }

    // -----------------------------------------------------------------------
    // Builder tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_build_get_objects_catalogs_depth() {
        let catalogs = make_catalogs(&["main", "hive_metastore", "system"]);
        let mut reader = build_get_objects_catalogs(catalogs).unwrap();

        // Verify schema
        let schema = reader.schema();
        assert_eq!(*schema, **GET_OBJECTS_SCHEMA);

        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);

        let catalog_col = batch.column(0).as_string::<i32>();
        assert_eq!(catalog_col.value(0), "main");
        assert_eq!(catalog_col.value(1), "hive_metastore");
        assert_eq!(catalog_col.value(2), "system");

        // catalog_db_schemas is null for all rows
        let db_schemas_col = batch.column(1);
        for i in 0..3 {
            assert!(db_schemas_col.is_null(i), "Row {} should be null", i);
        }

        assert!(reader.next().is_none());
    }

    #[test]
    fn test_build_get_objects_schemas_depth() {
        let schemas = make_schemas(&[
            ("main", "default"),
            ("main", "information_schema"),
            ("hive_metastore", "default"),
        ]);
        let catalog_nodes = group_schemas_by_catalog(schemas);
        let mut reader = build_get_objects_schemas(catalog_nodes).unwrap();

        let schema = reader.schema();
        assert_eq!(*schema, **GET_OBJECTS_SCHEMA);

        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2); // 2 catalogs

        let catalog_col = batch.column(0).as_string::<i32>();
        assert_eq!(catalog_col.value(0), "hive_metastore");
        assert_eq!(catalog_col.value(1), "main");

        let db_schemas_col = batch.column(1).as_list::<i32>();
        assert!(!db_schemas_col.is_null(0));
        assert!(!db_schemas_col.is_null(1));

        // hive_metastore has 1 schema
        let hive_schemas = db_schemas_col.value(0);
        let hive_struct = hive_schemas.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(hive_struct.len(), 1);
        let hive_schema_names = hive_struct.column(0).as_string::<i32>();
        assert_eq!(hive_schema_names.value(0), "default");
        // db_schema_tables should be null at Schemas depth
        assert!(hive_struct.column(1).is_null(0));

        // main has 2 schemas
        let main_schemas = db_schemas_col.value(1);
        let main_struct = main_schemas.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(main_struct.len(), 2);
        let main_schema_names = main_struct.column(0).as_string::<i32>();
        assert_eq!(main_schema_names.value(0), "default");
        assert_eq!(main_schema_names.value(1), "information_schema");

        assert!(reader.next().is_none());
    }

    #[test]
    fn test_build_get_objects_tables_depth() {
        let tables = make_tables(&[
            ("main", "default", "users", "TABLE"),
            ("main", "default", "orders", "TABLE"),
            ("main", "analytics", "events", "VIEW"),
        ]);
        let catalog_nodes = group_tables_by_catalog_schema(tables);
        let mut reader = build_get_objects_tables(catalog_nodes).unwrap();

        let schema = reader.schema();
        assert_eq!(*schema, **GET_OBJECTS_SCHEMA);

        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1); // 1 catalog

        let catalog_col = batch.column(0).as_string::<i32>();
        assert_eq!(catalog_col.value(0), "main");

        // Navigate into catalog_db_schemas
        let db_schemas_col = batch.column(1).as_list::<i32>();
        let schemas_arr = db_schemas_col.value(0);
        let schemas_struct = schemas_arr.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(schemas_struct.len(), 2); // analytics, default

        let schema_names = schemas_struct.column(0).as_string::<i32>();
        assert_eq!(schema_names.value(0), "analytics");
        assert_eq!(schema_names.value(1), "default");

        // Navigate into db_schema_tables for "default" (index 1)
        let tables_list = schemas_struct.column(1).as_list::<i32>();
        let default_tables_arr = tables_list.value(1);
        let default_tables = default_tables_arr
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(default_tables.len(), 2);

        let table_names = default_tables.column(0).as_string::<i32>();
        assert_eq!(table_names.value(0), "users");
        assert_eq!(table_names.value(1), "orders");

        let table_types = default_tables.column(1).as_string::<i32>();
        assert_eq!(table_types.value(0), "TABLE");
        assert_eq!(table_types.value(1), "TABLE");

        // table_columns and table_constraints should be null at Tables depth
        assert!(default_tables.column(2).is_null(0));
        assert!(default_tables.column(2).is_null(1));
        assert!(default_tables.column(3).is_null(0));
        assert!(default_tables.column(3).is_null(1));

        // analytics schema has 1 table (VIEW)
        let analytics_tables_arr = tables_list.value(0);
        let analytics_tables = analytics_tables_arr
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(analytics_tables.len(), 1);
        assert_eq!(
            analytics_tables.column(0).as_string::<i32>().value(0),
            "events"
        );
        assert_eq!(
            analytics_tables.column(1).as_string::<i32>().value(0),
            "VIEW"
        );

        assert!(reader.next().is_none());
    }

    #[test]
    fn test_build_get_objects_all_depth() {
        let tables = make_tables(&[
            ("main", "default", "users", "TABLE"),
            ("main", "default", "orders", "TABLE"),
        ]);
        let mut columns = vec![
            make_column("main", "default", "users", "id", "BIGINT", 1),
            make_column("main", "default", "users", "name", "STRING", 2),
            make_column("main", "default", "orders", "order_id", "INT", 1),
        ];
        // Override specific fields for testing xdbc mappings
        columns[0].is_nullable = Some("false".to_string());
        columns[0].is_auto_increment = Some("YES".to_string());
        columns[1].is_nullable = Some("true".to_string());
        columns[1].is_generated = Some("YES".to_string());

        let catalog_nodes = group_tables_and_columns(tables, columns);
        let mut reader = build_get_objects_all(catalog_nodes).unwrap();

        let schema = reader.schema();
        assert_eq!(*schema, **GET_OBJECTS_SCHEMA);

        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);

        // Navigate: catalog -> db_schemas -> tables -> columns
        let db_schemas_col = batch.column(1).as_list::<i32>();
        let schemas_arr = db_schemas_col.value(0);
        let schemas_struct = schemas_arr.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(schemas_struct.len(), 1); // just "default"

        let tables_list = schemas_struct.column(1).as_list::<i32>();
        let tables_arr = tables_list.value(0);
        let tables_struct = tables_arr
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(tables_struct.len(), 2); // users, orders (input order)

        // Check columns for "users" table (index 0, same as input order)
        let columns_list = tables_struct.column(2).as_list::<i32>();
        let users_cols_arr = columns_list.value(0);
        let users_cols = users_cols_arr
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(users_cols.len(), 2);

        // column_name
        let col_names = users_cols.column(0).as_string::<i32>();
        assert_eq!(col_names.value(0), "id");
        assert_eq!(col_names.value(1), "name");

        // ordinal_position
        let ordinals = users_cols
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .unwrap();
        assert_eq!(ordinals.value(0), 1);
        assert_eq!(ordinals.value(1), 2);

        // remarks
        let remarks = users_cols.column(2).as_string::<i32>();
        assert_eq!(remarks.value(0), "test column");

        // xdbc_data_type (BIGINT -> -5)
        let xdbc_data_type = users_cols
            .column(3)
            .as_any()
            .downcast_ref::<arrow_array::Int16Array>()
            .unwrap();
        assert_eq!(xdbc_data_type.value(0), -5); // BIGINT

        // xdbc_type_name
        let xdbc_type_name = users_cols.column(4).as_string::<i32>();
        assert_eq!(xdbc_type_name.value(0), "BIGINT");
        assert_eq!(xdbc_type_name.value(1), "STRING");

        // xdbc_nullable: "false"->0, "true"->1
        let xdbc_nullable = users_cols
            .column(8)
            .as_any()
            .downcast_ref::<arrow_array::Int16Array>()
            .unwrap();
        assert_eq!(xdbc_nullable.value(0), 0);
        assert_eq!(xdbc_nullable.value(1), 1);

        // xdbc_is_autoincrement: "YES"->true for id, "NO"->false for name
        let xdbc_autoincrement = users_cols
            .column(17)
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap();
        assert!(xdbc_autoincrement.value(0));
        assert!(!xdbc_autoincrement.value(1));

        // xdbc_is_generatedcolumn: "NO"->false for id, "YES"->true for name
        let xdbc_generated = users_cols
            .column(18)
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap();
        assert!(!xdbc_generated.value(0));
        assert!(xdbc_generated.value(1));

        // table_constraints is an empty list (not null)
        let constraints_list = tables_struct.column(3).as_list::<i32>();
        assert!(!constraints_list.is_null(0));
        assert_eq!(constraints_list.value(0).len(), 0);

        // Check orders table has 1 column (index 1)
        let orders_cols_arr = columns_list.value(1);
        let orders_cols = orders_cols_arr
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(orders_cols.len(), 1);
        assert_eq!(
            orders_cols.column(0).as_string::<i32>().value(0),
            "order_id"
        );

        assert!(reader.next().is_none());
    }

    #[test]
    fn test_build_get_objects_catalogs_empty() {
        let catalogs: Vec<CatalogInfo> = vec![];
        let mut reader = build_get_objects_catalogs(catalogs).unwrap();

        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert!(reader.next().is_none());
    }

    #[test]
    fn test_group_tables_and_columns_missing_columns() {
        let tables = make_tables(&[("main", "default", "users", "TABLE")]);
        let columns: Vec<ColumnInfo> = vec![];
        let result = group_tables_and_columns(tables, columns);

        assert_eq!(result.len(), 1);
        let table = &result[0].schemas.as_ref().unwrap()[0]
            .tables
            .as_ref()
            .unwrap()[0];
        assert_eq!(table.columns.as_ref().unwrap().len(), 0);
    }
}
