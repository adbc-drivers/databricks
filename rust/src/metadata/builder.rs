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

//! Builder for constructing the nested Arrow structure for get_objects().
//!
//! The `GetObjectsBuilder` accumulates catalog, schema, table, column, and constraint
//! metadata and then constructs the complex nested Arrow structure required by the
//! ADBC specification.
//!
//! # Example
//!
//! ```ignore
//! use databricks_adbc::metadata::builder::GetObjectsBuilder;
//! use databricks_adbc::metadata::types::{TableInfo, ColumnInfo};
//!
//! let mut builder = GetObjectsBuilder::new();
//!
//! builder.add_catalog("main");
//! builder.add_schema("main", "default");
//! builder.add_table("main", "default", &TableInfo { ... });
//! builder.add_column("main", "default", "users", &ColumnInfo { ... });
//!
//! let reader = builder.build()?;
//! ```

use crate::error::{DatabricksErrorHelper, Result};
use crate::metadata::schemas::get_objects_schema;
use crate::metadata::type_mapping::databricks_type_to_xdbc;
use crate::metadata::types::{ColumnInfo, ForeignKeyInfo, PrimaryKeyInfo, TableInfo};

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int16Array, Int32Array, ListArray, RecordBatch,
    RecordBatchIterator, RecordBatchReader, StringArray, StructArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{Field, Fields, SchemaRef};
use driverbase::error::ErrorHelper;
use std::collections::HashMap;
use std::sync::Arc;

/// Builder for constructing get_objects() result as a nested Arrow structure.
///
/// This builder accumulates metadata at different levels (catalogs, schemas, tables,
/// columns, constraints) and produces a properly nested Arrow RecordBatch when
/// `build()` is called.
pub struct GetObjectsBuilder {
    /// Map from catalog name to catalog entry.
    catalogs: HashMap<String, CatalogEntry>,
    /// Ordered list of catalog names (to preserve insertion order).
    catalog_order: Vec<String>,
}

/// Internal representation of a catalog entry.
#[derive(Default)]
struct CatalogEntry {
    /// Map from schema name to schema entry.
    schemas: HashMap<String, SchemaEntry>,
    /// Ordered list of schema names.
    schema_order: Vec<String>,
}

/// Internal representation of a schema entry.
#[derive(Default)]
struct SchemaEntry {
    /// Map from table name to table entry.
    tables: HashMap<String, TableEntry>,
    /// Ordered list of table names.
    table_order: Vec<String>,
}

/// Internal representation of a table entry.
struct TableEntry {
    /// Table metadata.
    info: TableInfo,
    /// Columns in this table.
    columns: Vec<ColumnInfo>,
    /// Constraints on this table.
    constraints: Vec<ConstraintEntry>,
}

/// Internal representation of a constraint entry.
struct ConstraintEntry {
    /// Name of the constraint (may be None).
    name: Option<String>,
    /// Type of constraint: "PRIMARY KEY", "FOREIGN KEY", "UNIQUE".
    constraint_type: String,
    /// Column names involved in the constraint.
    column_names: Vec<String>,
    /// Foreign key usage info (only for FOREIGN KEY constraints).
    usage: Vec<UsageEntry>,
}

/// Internal representation of a foreign key usage entry.
struct UsageEntry {
    /// Catalog of the referenced table.
    fk_catalog: Option<String>,
    /// Schema of the referenced table.
    fk_db_schema: Option<String>,
    /// Name of the referenced table.
    fk_table: String,
    /// Name of the referenced column.
    fk_column_name: String,
}

impl GetObjectsBuilder {
    /// Creates a new empty builder.
    pub fn new() -> Self {
        Self {
            catalogs: HashMap::new(),
            catalog_order: Vec::new(),
        }
    }

    /// Adds a catalog to the builder.
    ///
    /// If the catalog already exists, this is a no-op.
    pub fn add_catalog(&mut self, catalog_name: &str) {
        if !self.catalogs.contains_key(catalog_name) {
            self.catalogs
                .insert(catalog_name.to_string(), CatalogEntry::default());
            self.catalog_order.push(catalog_name.to_string());
        }
    }

    /// Adds a schema to a catalog.
    ///
    /// The catalog must already exist (added via `add_catalog`).
    /// If the schema already exists, this is a no-op.
    pub fn add_schema(&mut self, catalog_name: &str, schema_name: &str) {
        // Ensure catalog exists
        self.add_catalog(catalog_name);

        if let Some(catalog) = self.catalogs.get_mut(catalog_name) {
            if !catalog.schemas.contains_key(schema_name) {
                catalog
                    .schemas
                    .insert(schema_name.to_string(), SchemaEntry::default());
                catalog.schema_order.push(schema_name.to_string());
            }
        }
    }

    /// Adds a table to a schema.
    ///
    /// The catalog and schema must already exist.
    /// If the table already exists, this is a no-op.
    pub fn add_table(&mut self, catalog_name: &str, schema_name: &str, table_info: &TableInfo) {
        // Ensure schema exists
        self.add_schema(catalog_name, schema_name);

        if let Some(catalog) = self.catalogs.get_mut(catalog_name) {
            if let Some(schema) = catalog.schemas.get_mut(schema_name) {
                if !schema.tables.contains_key(&table_info.table_name) {
                    schema.tables.insert(
                        table_info.table_name.clone(),
                        TableEntry {
                            info: table_info.clone(),
                            columns: Vec::new(),
                            constraints: Vec::new(),
                        },
                    );
                    schema.table_order.push(table_info.table_name.clone());
                }
            }
        }
    }

    /// Adds a column to a table.
    ///
    /// The catalog, schema, and table must already exist.
    pub fn add_column(
        &mut self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        column_info: &ColumnInfo,
    ) {
        if let Some(catalog) = self.catalogs.get_mut(catalog_name) {
            if let Some(schema) = catalog.schemas.get_mut(schema_name) {
                if let Some(table) = schema.tables.get_mut(table_name) {
                    table.columns.push(column_info.clone());
                }
            }
        }
    }

    /// Adds constraints (primary keys and foreign keys) to a table.
    ///
    /// The catalog, schema, and table must already exist.
    pub fn add_constraints(
        &mut self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        primary_keys: &[PrimaryKeyInfo],
        foreign_keys: &[ForeignKeyInfo],
    ) {
        if let Some(catalog) = self.catalogs.get_mut(catalog_name) {
            if let Some(schema) = catalog.schemas.get_mut(schema_name) {
                if let Some(table) = schema.tables.get_mut(table_name) {
                    // Process primary keys - group by pk_name
                    let mut pk_groups: HashMap<Option<String>, Vec<&PrimaryKeyInfo>> =
                        HashMap::new();
                    for pk in primary_keys {
                        pk_groups.entry(pk.pk_name.clone()).or_default().push(pk);
                    }

                    for (pk_name, pks) in pk_groups {
                        // Sort by key_seq to get columns in order
                        let mut sorted_pks = pks;
                        sorted_pks.sort_by_key(|pk| pk.key_seq);

                        let column_names: Vec<String> =
                            sorted_pks.iter().map(|pk| pk.column_name.clone()).collect();

                        table.constraints.push(ConstraintEntry {
                            name: pk_name,
                            constraint_type: "PRIMARY KEY".to_string(),
                            column_names,
                            usage: Vec::new(),
                        });
                    }

                    // Process foreign keys - group by fk_name
                    let mut fk_groups: HashMap<Option<String>, Vec<&ForeignKeyInfo>> =
                        HashMap::new();
                    for fk in foreign_keys {
                        fk_groups.entry(fk.fk_name.clone()).or_default().push(fk);
                    }

                    for (fk_name, fks) in fk_groups {
                        // Sort by key_seq to get columns in order
                        let mut sorted_fks = fks;
                        sorted_fks.sort_by_key(|fk| fk.key_seq);

                        let column_names: Vec<String> =
                            sorted_fks.iter().map(|fk| fk.fk_column.clone()).collect();

                        let usage: Vec<UsageEntry> = sorted_fks
                            .iter()
                            .map(|fk| UsageEntry {
                                fk_catalog: Some(fk.pk_catalog.clone()),
                                fk_db_schema: Some(fk.pk_schema.clone()),
                                fk_table: fk.pk_table.clone(),
                                fk_column_name: fk.pk_column.clone(),
                            })
                            .collect();

                        table.constraints.push(ConstraintEntry {
                            name: fk_name,
                            constraint_type: "FOREIGN KEY".to_string(),
                            column_names,
                            usage,
                        });
                    }
                }
            }
        }
    }

    /// Builds and returns a RecordBatchReader containing the nested Arrow structure.
    ///
    /// # Returns
    ///
    /// A `RecordBatchReader` that yields a single RecordBatch with the complete
    /// get_objects() result.
    ///
    /// # Errors
    ///
    /// Returns an error if Arrow array construction fails.
    pub fn build(self) -> Result<impl RecordBatchReader + Send> {
        let schema = Arc::new(get_objects_schema());
        let batch = self.build_record_batch(schema.clone())?;

        Ok(RecordBatchIterator::new(
            vec![Ok(batch)].into_iter(),
            schema,
        ))
    }

    /// Builds the RecordBatch containing all metadata.
    fn build_record_batch(&self, schema: SchemaRef) -> Result<RecordBatch> {
        // Build catalog_name array
        let catalog_names: Vec<Option<&str>> = self
            .catalog_order
            .iter()
            .map(|s| Some(s.as_str()))
            .collect();
        let catalog_name_array = Arc::new(StringArray::from(catalog_names)) as ArrayRef;

        // Build db_schemas list array
        let db_schemas_array = self.build_db_schemas_array()?;

        RecordBatch::try_new(schema, vec![catalog_name_array, db_schemas_array])
            .map_err(|e| DatabricksErrorHelper::io().message(format!("Arrow error: {}", e)))
    }

    /// Builds the catalog_db_schemas List array.
    fn build_db_schemas_array(&self) -> Result<ArrayRef> {
        let mut all_schemas: Vec<ArrayRef> = Vec::new();
        let mut offsets: Vec<i32> = vec![0];
        let mut current_offset: i32 = 0;

        for catalog_name in &self.catalog_order {
            let catalog = self.catalogs.get(catalog_name).unwrap();
            let num_schemas = catalog.schema_order.len() as i32;

            // Build StructArray for this catalog's schemas
            if num_schemas > 0 {
                let schemas_struct = self.build_schemas_struct(catalog)?;
                all_schemas.push(Arc::new(schemas_struct) as ArrayRef);
            }

            current_offset += num_schemas;
            offsets.push(current_offset);
        }

        // Combine all schema structs into one StructArray
        let combined_schemas = if all_schemas.is_empty() {
            // Create empty struct array with proper schema
            self.create_empty_db_schema_struct()?
        } else {
            self.concat_struct_arrays(&all_schemas)?
        };

        // Get the field definition for the list
        let db_schema_struct_field = self.get_db_schema_field();

        // Build the ListArray
        let offsets_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets.to_vec()));
        let list_array = ListArray::new(
            Arc::new(db_schema_struct_field),
            offsets_buffer,
            Arc::new(combined_schemas),
            None, // no nulls
        );

        Ok(Arc::new(list_array) as ArrayRef)
    }

    /// Gets the field definition for a db_schema entry.
    fn get_db_schema_field(&self) -> Field {
        use crate::metadata::schemas::db_schema_schema;
        Field::new(
            "item",
            arrow_schema::DataType::Struct(Fields::from(db_schema_schema())),
            true,
        )
    }

    /// Creates an empty StructArray for db_schemas.
    fn create_empty_db_schema_struct(&self) -> Result<StructArray> {
        use crate::metadata::schemas::db_schema_schema;

        let fields = db_schema_schema();

        // db_schema_name - empty string array
        let schema_names = Arc::new(StringArray::from(Vec::<Option<&str>>::new())) as ArrayRef;

        // Create empty list with no elements - must have offset [0]
        let empty_tables_list = Arc::new(ListArray::new(
            Arc::new(self.get_table_field()),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32])),
            Arc::new(self.create_empty_table_struct()?),
            None,
        )) as ArrayRef;
        // Slice to get zero length with valid structure
        let sliced_tables_list = empty_tables_list.slice(0, 0);

        let struct_array = StructArray::try_new(
            Fields::from(fields),
            vec![schema_names, sliced_tables_list],
            None,
        )
        .map_err(|e| DatabricksErrorHelper::io().message(format!("Arrow error: {}", e)))?;

        Ok(struct_array)
    }

    /// Builds a StructArray for all schemas in a catalog.
    fn build_schemas_struct(&self, catalog: &CatalogEntry) -> Result<StructArray> {
        use crate::metadata::schemas::db_schema_schema;

        let fields = db_schema_schema();

        // Build db_schema_name array
        let schema_names: Vec<Option<&str>> = catalog
            .schema_order
            .iter()
            .map(|s| Some(s.as_str()))
            .collect();
        let schema_name_array = Arc::new(StringArray::from(schema_names)) as ArrayRef;

        // Build db_schema_tables list array
        let tables_array = self.build_tables_list_array(catalog)?;

        let struct_array = StructArray::try_new(
            Fields::from(fields),
            vec![schema_name_array, tables_array],
            None,
        )
        .map_err(|e| DatabricksErrorHelper::io().message(format!("Arrow error: {}", e)))?;

        Ok(struct_array)
    }

    /// Builds the db_schema_tables List array for a catalog.
    fn build_tables_list_array(&self, catalog: &CatalogEntry) -> Result<ArrayRef> {
        let mut all_tables: Vec<ArrayRef> = Vec::new();
        let mut offsets: Vec<i32> = vec![0];
        let mut current_offset: i32 = 0;

        for schema_name in &catalog.schema_order {
            let schema_entry = catalog.schemas.get(schema_name).unwrap();
            let num_tables = schema_entry.table_order.len() as i32;

            if num_tables > 0 {
                let tables_struct = self.build_tables_struct(schema_entry)?;
                all_tables.push(Arc::new(tables_struct) as ArrayRef);
            }

            current_offset += num_tables;
            offsets.push(current_offset);
        }

        // Combine all table structs
        let combined_tables = if all_tables.is_empty() {
            self.create_empty_table_struct()?
        } else {
            self.concat_struct_arrays(&all_tables)?
        };

        let tables_field = self.get_table_field();
        let offsets_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets.to_vec()));
        let list_array = ListArray::new(
            Arc::new(tables_field),
            offsets_buffer,
            Arc::new(combined_tables),
            None,
        );

        Ok(Arc::new(list_array) as ArrayRef)
    }

    /// Gets the field definition for a table entry.
    fn get_table_field(&self) -> Field {
        use crate::metadata::schemas::table_schema;
        Field::new(
            "item",
            arrow_schema::DataType::Struct(Fields::from(table_schema())),
            true,
        )
    }

    /// Creates an empty StructArray for tables.
    fn create_empty_table_struct(&self) -> Result<StructArray> {
        use crate::metadata::schemas::table_schema;
        let fields = table_schema();

        let table_names = Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef;
        let table_types = Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef;

        // Empty columns list - create with one offset then slice to empty
        let columns_field = self.get_column_field();
        let columns_list = Arc::new(ListArray::new(
            Arc::new(columns_field),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32])),
            Arc::new(self.create_empty_column_struct()?),
            None,
        )) as ArrayRef;
        let empty_columns = columns_list.slice(0, 0);

        // Empty constraints list - create with one offset then slice to empty
        let constraints_field = self.get_constraint_field();
        let constraints_list = Arc::new(ListArray::new(
            Arc::new(constraints_field),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32])),
            Arc::new(self.create_empty_constraint_struct()?),
            None,
        )) as ArrayRef;
        let empty_constraints = constraints_list.slice(0, 0);

        let struct_array = StructArray::try_new(
            Fields::from(fields),
            vec![table_names, table_types, empty_columns, empty_constraints],
            None,
        )
        .map_err(|e| DatabricksErrorHelper::io().message(format!("Arrow error: {}", e)))?;

        Ok(struct_array)
    }

    /// Builds a StructArray for all tables in a schema.
    fn build_tables_struct(&self, schema_entry: &SchemaEntry) -> Result<StructArray> {
        use crate::metadata::schemas::table_schema;
        let fields = table_schema();

        // Build table_name and table_type arrays
        let mut table_names: Vec<&str> = Vec::new();
        let mut table_types: Vec<&str> = Vec::new();

        for table_name in &schema_entry.table_order {
            let table_entry = schema_entry.tables.get(table_name).unwrap();
            table_names.push(&table_entry.info.table_name);
            table_types.push(&table_entry.info.table_type);
        }

        let table_name_array = Arc::new(StringArray::from(table_names)) as ArrayRef;
        let table_type_array = Arc::new(StringArray::from(table_types)) as ArrayRef;

        // Build table_columns list array
        let columns_array = self.build_columns_list_array(schema_entry)?;

        // Build table_constraints list array
        let constraints_array = self.build_constraints_list_array(schema_entry)?;

        let struct_array = StructArray::try_new(
            Fields::from(fields),
            vec![
                table_name_array,
                table_type_array,
                columns_array,
                constraints_array,
            ],
            None,
        )
        .map_err(|e| DatabricksErrorHelper::io().message(format!("Arrow error: {}", e)))?;

        Ok(struct_array)
    }

    /// Gets the field definition for a column entry.
    fn get_column_field(&self) -> Field {
        use crate::metadata::schemas::column_schema;
        Field::new(
            "item",
            arrow_schema::DataType::Struct(Fields::from(column_schema())),
            true,
        )
    }

    /// Creates an empty StructArray for columns.
    fn create_empty_column_struct(&self) -> Result<StructArray> {
        use crate::metadata::schemas::column_schema;
        let fields = column_schema();

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(Vec::<&str>::new())), // column_name
            Arc::new(Int32Array::from(Vec::<i32>::new())),   // ordinal_position
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())), // remarks
            Arc::new(Int16Array::from(Vec::<i16>::new())),   // xdbc_data_type
            Arc::new(StringArray::from(Vec::<&str>::new())), // xdbc_type_name
            Arc::new(Int32Array::from(Vec::<Option<i32>>::new())), // xdbc_column_size
            Arc::new(Int16Array::from(Vec::<Option<i16>>::new())), // xdbc_decimal_digits
            Arc::new(Int16Array::from(Vec::<Option<i16>>::new())), // xdbc_num_prec_radix
            Arc::new(Int16Array::from(Vec::<i16>::new())),   // xdbc_nullable
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())), // xdbc_column_def
            Arc::new(Int16Array::from(Vec::<i16>::new())),   // xdbc_sql_data_type
            Arc::new(Int16Array::from(Vec::<Option<i16>>::new())), // xdbc_datetime_sub
            Arc::new(Int32Array::from(Vec::<Option<i32>>::new())), // xdbc_char_octet_length
            Arc::new(StringArray::from(Vec::<&str>::new())), // xdbc_is_nullable
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())), // xdbc_scope_catalog
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())), // xdbc_scope_schema
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())), // xdbc_scope_table
            Arc::new(BooleanArray::from(Vec::<Option<bool>>::new())), // xdbc_is_autoincrement
            Arc::new(BooleanArray::from(Vec::<Option<bool>>::new())), // xdbc_is_generatedcolumn
        ];

        let struct_array = StructArray::try_new(Fields::from(fields), arrays, None)
            .map_err(|e| DatabricksErrorHelper::io().message(format!("Arrow error: {}", e)))?;

        Ok(struct_array)
    }

    /// Builds the table_columns List array for a schema.
    fn build_columns_list_array(&self, schema_entry: &SchemaEntry) -> Result<ArrayRef> {
        let mut all_columns: Vec<ArrayRef> = Vec::new();
        let mut offsets: Vec<i32> = vec![0];
        let mut current_offset: i32 = 0;

        for table_name in &schema_entry.table_order {
            let table_entry = schema_entry.tables.get(table_name).unwrap();
            let num_columns = table_entry.columns.len() as i32;

            if num_columns > 0 {
                let columns_struct = self.build_columns_struct(&table_entry.columns)?;
                all_columns.push(Arc::new(columns_struct) as ArrayRef);
            }

            current_offset += num_columns;
            offsets.push(current_offset);
        }

        let combined_columns = if all_columns.is_empty() {
            self.create_empty_column_struct()?
        } else {
            self.concat_struct_arrays(&all_columns)?
        };

        let columns_field = self.get_column_field();
        let offsets_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets.to_vec()));
        let list_array = ListArray::new(
            Arc::new(columns_field),
            offsets_buffer,
            Arc::new(combined_columns),
            None,
        );

        Ok(Arc::new(list_array) as ArrayRef)
    }

    /// Builds a StructArray for columns.
    fn build_columns_struct(&self, columns: &[ColumnInfo]) -> Result<StructArray> {
        use crate::metadata::schemas::column_schema;
        let fields = column_schema();

        let mut column_names: Vec<&str> = Vec::new();
        let mut ordinal_positions: Vec<i32> = Vec::new();
        let mut remarks: Vec<Option<&str>> = Vec::new();
        let mut xdbc_data_types: Vec<i16> = Vec::new();
        let mut xdbc_type_names: Vec<&str> = Vec::new();
        let mut xdbc_column_sizes: Vec<Option<i32>> = Vec::new();
        let mut xdbc_decimal_digits: Vec<Option<i16>> = Vec::new();
        let mut xdbc_num_prec_radix: Vec<Option<i16>> = Vec::new();
        let mut xdbc_nullables: Vec<i16> = Vec::new();
        let mut xdbc_column_defs: Vec<Option<&str>> = Vec::new();
        let mut xdbc_sql_data_types: Vec<i16> = Vec::new();
        let mut xdbc_datetime_subs: Vec<Option<i16>> = Vec::new();
        let mut xdbc_char_octet_lengths: Vec<Option<i32>> = Vec::new();
        let mut xdbc_is_nullables: Vec<&str> = Vec::new();
        let mut xdbc_scope_catalogs: Vec<Option<&str>> = Vec::new();
        let mut xdbc_scope_schemas: Vec<Option<&str>> = Vec::new();
        let mut xdbc_scope_tables: Vec<Option<&str>> = Vec::new();
        let mut xdbc_is_autoincrements: Vec<Option<bool>> = Vec::new();
        let mut xdbc_is_generatedcolumns: Vec<Option<bool>> = Vec::new();

        for col in columns {
            column_names.push(&col.column_name);
            ordinal_positions.push(col.ordinal_position);
            remarks.push(col.remarks.as_deref());

            let xdbc_type_code = databricks_type_to_xdbc(&col.data_type);
            xdbc_data_types.push(xdbc_type_code);
            xdbc_type_names.push(&col.type_name);
            xdbc_column_sizes.push(col.column_size);
            xdbc_decimal_digits.push(col.decimal_digits);
            xdbc_num_prec_radix.push(col.num_prec_radix);
            xdbc_nullables.push(col.nullable);
            xdbc_column_defs.push(col.column_def.as_deref());
            xdbc_sql_data_types.push(xdbc_type_code);
            xdbc_datetime_subs.push(None);
            xdbc_char_octet_lengths.push(col.column_size.map(|s| s * 4));
            xdbc_is_nullables.push(&col.is_nullable);
            xdbc_scope_catalogs.push(None);
            xdbc_scope_schemas.push(None);
            xdbc_scope_tables.push(None);
            xdbc_is_autoincrements.push(col.is_autoincrement);
            xdbc_is_generatedcolumns.push(col.is_generatedcolumn);
        }

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(column_names)),
            Arc::new(Int32Array::from(ordinal_positions)),
            Arc::new(StringArray::from(remarks)),
            Arc::new(Int16Array::from(xdbc_data_types)),
            Arc::new(StringArray::from(xdbc_type_names)),
            Arc::new(Int32Array::from(xdbc_column_sizes)),
            Arc::new(Int16Array::from(xdbc_decimal_digits)),
            Arc::new(Int16Array::from(xdbc_num_prec_radix)),
            Arc::new(Int16Array::from(xdbc_nullables)),
            Arc::new(StringArray::from(xdbc_column_defs)),
            Arc::new(Int16Array::from(xdbc_sql_data_types)),
            Arc::new(Int16Array::from(xdbc_datetime_subs)),
            Arc::new(Int32Array::from(xdbc_char_octet_lengths)),
            Arc::new(StringArray::from(xdbc_is_nullables)),
            Arc::new(StringArray::from(xdbc_scope_catalogs)),
            Arc::new(StringArray::from(xdbc_scope_schemas)),
            Arc::new(StringArray::from(xdbc_scope_tables)),
            Arc::new(BooleanArray::from(xdbc_is_autoincrements)),
            Arc::new(BooleanArray::from(xdbc_is_generatedcolumns)),
        ];

        let struct_array = StructArray::try_new(Fields::from(fields), arrays, None)
            .map_err(|e| DatabricksErrorHelper::io().message(format!("Arrow error: {}", e)))?;

        Ok(struct_array)
    }

    /// Gets the field definition for a constraint entry.
    fn get_constraint_field(&self) -> Field {
        use crate::metadata::schemas::constraint_schema;
        Field::new(
            "item",
            arrow_schema::DataType::Struct(Fields::from(constraint_schema())),
            true,
        )
    }

    /// Creates an empty StructArray for constraints.
    fn create_empty_constraint_struct(&self) -> Result<StructArray> {
        use crate::metadata::schemas::constraint_schema;
        let fields = constraint_schema();

        // constraint_column_names list - create with one offset then slice to empty
        let column_names_field = Field::new("item", arrow_schema::DataType::Utf8, false);
        let column_names_list = Arc::new(ListArray::new(
            Arc::new(column_names_field),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32])),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            None,
        )) as ArrayRef;
        let empty_column_names = column_names_list.slice(0, 0);

        // constraint_column_usage list - create with one offset then slice to empty
        let usage_field = self.get_usage_field();
        let usage_list = Arc::new(ListArray::new(
            Arc::new(usage_field),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32])),
            Arc::new(self.create_empty_usage_struct()?),
            None,
        )) as ArrayRef;
        let empty_usage = usage_list.slice(0, 0);

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())), // constraint_name
            Arc::new(StringArray::from(Vec::<&str>::new())),         // constraint_type
            empty_column_names,                                      // constraint_column_names
            empty_usage,                                             // constraint_column_usage
        ];

        let struct_array = StructArray::try_new(Fields::from(fields), arrays, None)
            .map_err(|e| DatabricksErrorHelper::io().message(format!("Arrow error: {}", e)))?;

        Ok(struct_array)
    }

    /// Builds the table_constraints List array for a schema.
    fn build_constraints_list_array(&self, schema_entry: &SchemaEntry) -> Result<ArrayRef> {
        let mut all_constraints: Vec<ArrayRef> = Vec::new();
        let mut offsets: Vec<i32> = vec![0];
        let mut current_offset: i32 = 0;

        for table_name in &schema_entry.table_order {
            let table_entry = schema_entry.tables.get(table_name).unwrap();
            let num_constraints = table_entry.constraints.len() as i32;

            if num_constraints > 0 {
                let constraints_struct = self.build_constraints_struct(&table_entry.constraints)?;
                all_constraints.push(Arc::new(constraints_struct) as ArrayRef);
            }

            current_offset += num_constraints;
            offsets.push(current_offset);
        }

        let combined_constraints = if all_constraints.is_empty() {
            self.create_empty_constraint_struct()?
        } else {
            self.concat_struct_arrays(&all_constraints)?
        };

        let constraints_field = self.get_constraint_field();
        let offsets_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets.to_vec()));
        let list_array = ListArray::new(
            Arc::new(constraints_field),
            offsets_buffer,
            Arc::new(combined_constraints),
            None,
        );

        Ok(Arc::new(list_array) as ArrayRef)
    }

    /// Builds a StructArray for constraints.
    fn build_constraints_struct(&self, constraints: &[ConstraintEntry]) -> Result<StructArray> {
        use crate::metadata::schemas::constraint_schema;
        let fields = constraint_schema();

        let mut constraint_names: Vec<Option<&str>> = Vec::new();
        let mut constraint_types: Vec<&str> = Vec::new();

        // For constraint_column_names - we need to build a ListArray
        let mut column_names_offsets: Vec<i32> = vec![0];
        let mut all_column_names: Vec<&str> = Vec::new();

        // For constraint_column_usage - we need to build a ListArray of structs
        let mut usage_offsets: Vec<i32> = vec![0];
        let mut all_usage: Vec<&UsageEntry> = Vec::new();

        for constraint in constraints {
            constraint_names.push(constraint.name.as_deref());
            constraint_types.push(&constraint.constraint_type);

            for col_name in &constraint.column_names {
                all_column_names.push(col_name);
            }
            column_names_offsets.push(all_column_names.len() as i32);

            for usage in &constraint.usage {
                all_usage.push(usage);
            }
            usage_offsets.push(all_usage.len() as i32);
        }

        // Build constraint_column_names ListArray
        let column_names_field = Field::new("item", arrow_schema::DataType::Utf8, false);
        let column_names_values = Arc::new(StringArray::from(all_column_names)) as ArrayRef;
        let column_names_list = ListArray::new(
            Arc::new(column_names_field),
            OffsetBuffer::new(ScalarBuffer::from(column_names_offsets)),
            column_names_values,
            None,
        );

        // Build constraint_column_usage ListArray
        let usage_struct = self.build_usage_struct(&all_usage)?;
        let usage_field = self.get_usage_field();
        let usage_list = ListArray::new(
            Arc::new(usage_field),
            OffsetBuffer::new(ScalarBuffer::from(usage_offsets)),
            Arc::new(usage_struct),
            None,
        );

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(constraint_names)),
            Arc::new(StringArray::from(constraint_types)),
            Arc::new(column_names_list),
            Arc::new(usage_list),
        ];

        let struct_array = StructArray::try_new(Fields::from(fields), arrays, None)
            .map_err(|e| DatabricksErrorHelper::io().message(format!("Arrow error: {}", e)))?;

        Ok(struct_array)
    }

    /// Gets the field definition for a usage entry.
    fn get_usage_field(&self) -> Field {
        use crate::metadata::schemas::usage_schema;
        Field::new(
            "item",
            arrow_schema::DataType::Struct(Fields::from(usage_schema())),
            true,
        )
    }

    /// Creates an empty StructArray for usage.
    fn create_empty_usage_struct(&self) -> Result<StructArray> {
        use crate::metadata::schemas::usage_schema;
        let fields = usage_schema();

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())), // fk_catalog
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())), // fk_db_schema
            Arc::new(StringArray::from(Vec::<&str>::new())),         // fk_table
            Arc::new(StringArray::from(Vec::<&str>::new())),         // fk_column_name
        ];

        let struct_array = StructArray::try_new(Fields::from(fields), arrays, None)
            .map_err(|e| DatabricksErrorHelper::io().message(format!("Arrow error: {}", e)))?;

        Ok(struct_array)
    }

    /// Builds a StructArray for usage entries.
    fn build_usage_struct(&self, usage_entries: &[&UsageEntry]) -> Result<StructArray> {
        use crate::metadata::schemas::usage_schema;
        let fields = usage_schema();

        if usage_entries.is_empty() {
            return self.create_empty_usage_struct();
        }

        let mut fk_catalogs: Vec<Option<&str>> = Vec::new();
        let mut fk_db_schemas: Vec<Option<&str>> = Vec::new();
        let mut fk_tables: Vec<&str> = Vec::new();
        let mut fk_column_names: Vec<&str> = Vec::new();

        for entry in usage_entries {
            fk_catalogs.push(entry.fk_catalog.as_deref());
            fk_db_schemas.push(entry.fk_db_schema.as_deref());
            fk_tables.push(&entry.fk_table);
            fk_column_names.push(&entry.fk_column_name);
        }

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(fk_catalogs)),
            Arc::new(StringArray::from(fk_db_schemas)),
            Arc::new(StringArray::from(fk_tables)),
            Arc::new(StringArray::from(fk_column_names)),
        ];

        let struct_array = StructArray::try_new(Fields::from(fields), arrays, None)
            .map_err(|e| DatabricksErrorHelper::io().message(format!("Arrow error: {}", e)))?;

        Ok(struct_array)
    }

    /// Concatenates multiple StructArrays into one.
    fn concat_struct_arrays(&self, arrays: &[ArrayRef]) -> Result<StructArray> {
        if arrays.is_empty() {
            return Err(DatabricksErrorHelper::io().message("Cannot concat empty array list"));
        }

        // Get the fields from the first array
        let first = arrays[0]
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("Expected StructArray");
        let fields = first.fields().clone();
        let num_fields = fields.len();

        // Concatenate each field's arrays
        let mut concatenated_fields: Vec<ArrayRef> = Vec::new();
        for i in 0..num_fields {
            let field_arrays: Vec<&dyn Array> = arrays
                .iter()
                .map(|a| {
                    let struct_arr = a.as_any().downcast_ref::<StructArray>().unwrap();
                    struct_arr.column(i).as_ref()
                })
                .collect();

            let concatenated = arrow_select::concat::concat(&field_arrays)
                .map_err(|e| DatabricksErrorHelper::io().message(format!("Arrow error: {}", e)))?;
            concatenated_fields.push(concatenated);
        }

        let struct_array = StructArray::try_new(fields, concatenated_fields, None)
            .map_err(|e| DatabricksErrorHelper::io().message(format!("Arrow error: {}", e)))?;

        Ok(struct_array)
    }
}

impl Default for GetObjectsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::RecordBatchReader;
    use arrow_schema::DataType;

    fn create_test_table_info(name: &str, table_type: &str) -> TableInfo {
        TableInfo {
            catalog_name: "test_catalog".to_string(),
            schema_name: "test_schema".to_string(),
            table_name: name.to_string(),
            table_type: table_type.to_string(),
            remarks: None,
        }
    }

    fn create_test_column_info(name: &str, ordinal: i32, data_type: &str) -> ColumnInfo {
        ColumnInfo {
            catalog_name: "test_catalog".to_string(),
            schema_name: "test_schema".to_string(),
            table_name: "test_table".to_string(),
            column_name: name.to_string(),
            ordinal_position: ordinal,
            data_type: data_type.to_string(),
            type_name: data_type.to_string(),
            column_size: Some(10),
            decimal_digits: None,
            num_prec_radix: Some(10),
            nullable: 1,
            remarks: None,
            column_def: None,
            is_nullable: "YES".to_string(),
            is_autoincrement: Some(false),
            is_generatedcolumn: Some(false),
        }
    }

    #[test]
    fn test_get_objects_builder_catalogs_only() {
        let mut builder = GetObjectsBuilder::new();
        builder.add_catalog("catalog1");
        builder.add_catalog("catalog2");

        let reader = builder.build().expect("build should succeed");

        // Verify schema
        let schema = reader.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "catalog_name");
        assert_eq!(schema.field(1).name(), "catalog_db_schemas");
    }

    #[test]
    fn test_get_objects_builder_with_schemas() {
        let mut builder = GetObjectsBuilder::new();
        builder.add_catalog("main");
        builder.add_schema("main", "default");
        builder.add_schema("main", "information_schema");

        let mut reader = builder.build().expect("build should succeed");

        // Read the batch
        let batch = reader.next().expect("should have one batch").unwrap();
        assert_eq!(batch.num_rows(), 1); // One catalog

        // Verify catalog name
        let catalog_names = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(catalog_names.value(0), "main");
    }

    #[test]
    fn test_get_objects_builder_with_tables() {
        let mut builder = GetObjectsBuilder::new();
        builder.add_catalog("main");
        builder.add_schema("main", "default");

        let table_info = create_test_table_info("users", "TABLE");
        builder.add_table("main", "default", &table_info);

        let table_info2 = create_test_table_info("orders", "TABLE");
        builder.add_table("main", "default", &table_info2);

        let mut reader = builder.build().expect("build should succeed");
        let batch = reader.next().expect("should have one batch").unwrap();

        // Should have one catalog row
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_get_objects_builder_full_depth() {
        let mut builder = GetObjectsBuilder::new();

        // Add catalog
        builder.add_catalog("main");

        // Add schema
        builder.add_schema("main", "default");

        // Add table
        let table_info = TableInfo {
            catalog_name: "main".to_string(),
            schema_name: "default".to_string(),
            table_name: "users".to_string(),
            table_type: "TABLE".to_string(),
            remarks: Some("User accounts".to_string()),
        };
        builder.add_table("main", "default", &table_info);

        // Add columns
        let col1 = create_test_column_info("id", 1, "BIGINT");
        let col2 = create_test_column_info("name", 2, "STRING");
        builder.add_column("main", "default", "users", &col1);
        builder.add_column("main", "default", "users", &col2);

        let mut reader = builder.build().expect("build should succeed");
        let batch = reader.next().expect("should have one batch").unwrap();

        // Verify we got a batch
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_get_objects_builder_with_constraints() {
        let mut builder = GetObjectsBuilder::new();

        builder.add_catalog("main");
        builder.add_schema("main", "default");

        let table_info = create_test_table_info("orders", "TABLE");
        builder.add_table("main", "default", &table_info);

        // Add a column
        let col = create_test_column_info("id", 1, "BIGINT");
        builder.add_column("main", "default", "orders", &col);

        // Add primary key
        let pk = PrimaryKeyInfo {
            catalog_name: "main".to_string(),
            schema_name: "default".to_string(),
            table_name: "orders".to_string(),
            column_name: "id".to_string(),
            key_seq: 1,
            pk_name: Some("pk_orders".to_string()),
        };

        // Add foreign key
        let fk = ForeignKeyInfo {
            pk_catalog: "main".to_string(),
            pk_schema: "default".to_string(),
            pk_table: "users".to_string(),
            pk_column: "id".to_string(),
            fk_catalog: "main".to_string(),
            fk_schema: "default".to_string(),
            fk_table: "orders".to_string(),
            fk_column: "user_id".to_string(),
            key_seq: 1,
            fk_name: Some("fk_orders_users".to_string()),
            pk_name: Some("pk_users".to_string()),
        };

        builder.add_constraints("main", "default", "orders", &[pk], &[fk]);

        let mut reader = builder.build().expect("build should succeed");
        let batch = reader.next().expect("should have one batch").unwrap();

        // Verify we got a batch
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_get_objects_builder_empty() {
        let builder = GetObjectsBuilder::new();
        let mut reader = builder.build().expect("build should succeed");
        let batch = reader.next().expect("should have one batch").unwrap();

        // Empty result should have 0 rows
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_get_objects_builder_multiple_catalogs() {
        let mut builder = GetObjectsBuilder::new();

        builder.add_catalog("catalog_a");
        builder.add_schema("catalog_a", "schema1");

        builder.add_catalog("catalog_b");
        builder.add_schema("catalog_b", "schema2");

        let mut reader = builder.build().expect("build should succeed");
        let batch = reader.next().expect("should have one batch").unwrap();

        // Should have two catalog rows
        assert_eq!(batch.num_rows(), 2);

        let catalog_names = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(catalog_names.value(0), "catalog_a");
        assert_eq!(catalog_names.value(1), "catalog_b");
    }

    #[test]
    fn test_schema_matches_adbc_specification() {
        let builder = GetObjectsBuilder::new();
        let reader = builder.build().expect("build should succeed");
        let schema = reader.schema();

        // Verify top-level schema matches ADBC spec
        assert_eq!(schema.field(0).name(), "catalog_name");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert!(schema.field(0).is_nullable());

        assert_eq!(schema.field(1).name(), "catalog_db_schemas");
        assert!(matches!(schema.field(1).data_type(), DataType::List(_)));
    }

    #[test]
    fn test_add_catalog_idempotent() {
        let mut builder = GetObjectsBuilder::new();
        builder.add_catalog("main");
        builder.add_catalog("main"); // Add again - should be no-op
        builder.add_catalog("main"); // And again

        let mut reader = builder.build().expect("build should succeed");
        let batch = reader.next().expect("should have one batch").unwrap();

        // Should still have only one catalog
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_add_schema_creates_catalog_if_missing() {
        let mut builder = GetObjectsBuilder::new();
        // Add schema without first adding catalog
        builder.add_schema("main", "default");

        let mut reader = builder.build().expect("build should succeed");
        let batch = reader.next().expect("should have one batch").unwrap();

        // Should have the catalog
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_add_table_creates_hierarchy_if_missing() {
        let mut builder = GetObjectsBuilder::new();

        // Add table without first adding catalog and schema
        let table_info = create_test_table_info("users", "TABLE");
        builder.add_table("main", "default", &table_info);

        let mut reader = builder.build().expect("build should succeed");
        let batch = reader.next().expect("should have one batch").unwrap();

        // Should have the catalog
        assert_eq!(batch.num_rows(), 1);
    }
}
