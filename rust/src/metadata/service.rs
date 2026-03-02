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

//! ODBC-style metadata service returning flat Arrow result sets.
//!
//! Each method corresponds to an ODBC catalog function and returns
//! a `RecordBatch` with a well-defined schema matching the JDBC/ODBC specification.
//! This is used by the ODBC FFI layer (`ffi/odbc.rs`), separate from the nested
//! ADBC `get_objects` format.

use crate::client::DatabricksClient;
use crate::error::{DatabricksErrorHelper, Result};
use crate::metadata::builder::filter_by_pattern;
use crate::metadata::parse::{
    parse_catalogs, parse_columns, parse_foreign_keys, parse_primary_keys, parse_schemas,
    parse_tables, ColumnInfo,
};
use crate::metadata::schemas::*;
use crate::metadata::sql::SqlCommandBuilder;
use crate::metadata::type_info::TYPE_INFO_ENTRIES;
use crate::metadata::type_mapping::databricks_type_to_xdbc;
use crate::types::sea::ExecuteParams;
use arrow_array::builder::{BooleanBuilder, Int16Builder, Int32Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch, StringArray};
use driverbase::error::ErrorHelper;
use std::sync::Arc;

/// ODBC-style metadata operations returning flat Arrow result sets.
///
/// Each method corresponds to an ODBC catalog function and returns
/// a RecordBatch with a well-defined schema matching the JDBC/ODBC specification.
pub trait MetadataService {
    /// List catalogs. Schema: [TABLE_CAT]
    fn get_catalogs(&self) -> Result<RecordBatch>;

    /// List schemas. Schema: [TABLE_SCHEM, TABLE_CATALOG]
    fn get_schemas(
        &self,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
    ) -> Result<RecordBatch>;

    /// List tables. Schema: [TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS]
    fn get_tables(
        &self,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        table_pattern: Option<&str>,
        table_types: Option<&[&str]>,
    ) -> Result<RecordBatch>;

    /// List columns (JDBC 24-column schema).
    fn get_columns(
        &self,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        table_pattern: Option<&str>,
        column_pattern: Option<&str>,
    ) -> Result<RecordBatch>;

    /// List supported SQL data types.
    fn get_type_info(&self, data_type: Option<i16>) -> Result<RecordBatch>;

    /// List primary key columns for a table.
    fn get_primary_keys(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<RecordBatch>;

    /// List foreign key columns for a table.
    fn get_foreign_keys(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<RecordBatch>;

    /// List table types.
    fn get_table_types(&self) -> Result<RecordBatch>;

    /// Get statistics (returns empty result — Databricks has no traditional indexes).
    fn get_statistics(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        unique: bool,
    ) -> Result<RecordBatch>;

    /// Get special columns (returns empty result — no ROWID in Databricks).
    fn get_special_columns(
        &self,
        identifier_type: i16,
        catalog: &str,
        schema: &str,
        table: &str,
        scope: i16,
        nullable: i16,
    ) -> Result<RecordBatch>;

    /// List procedures (returns empty result).
    fn get_procedures(
        &self,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        proc_pattern: Option<&str>,
    ) -> Result<RecordBatch>;

    /// List procedure columns (returns empty result).
    fn get_procedure_columns(
        &self,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        proc_pattern: Option<&str>,
        column_pattern: Option<&str>,
    ) -> Result<RecordBatch>;

    /// List table privileges (returns empty result).
    fn get_table_privileges(
        &self,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        table_pattern: Option<&str>,
    ) -> Result<RecordBatch>;

    /// List column privileges (returns empty result).
    fn get_column_privileges(
        &self,
        catalog: Option<&str>,
        schema: Option<&str>,
        table: &str,
        column_pattern: Option<&str>,
    ) -> Result<RecordBatch>;
}

/// Implementation of `MetadataService` backed by a Databricks connection.
///
/// Wraps the connection's client, session ID, and runtime to execute
/// metadata queries and return flat Arrow RecordBatch results.
pub struct ConnectionMetadataService {
    client: Arc<dyn DatabricksClient>,
    session_id: String,
    runtime: tokio::runtime::Handle,
}

impl ConnectionMetadataService {
    /// Create a new metadata service from connection components.
    pub fn new(
        client: Arc<dyn DatabricksClient>,
        session_id: String,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        Self {
            client,
            session_id,
            runtime,
        }
    }
}

impl MetadataService for ConnectionMetadataService {
    fn get_catalogs(&self) -> Result<RecordBatch> {
        let result = self
            .runtime
            .block_on(self.client.list_catalogs(&self.session_id))?;
        let catalogs = parse_catalogs(result)?;
        let catalogs = filter_by_pattern(catalogs, None, |c| &c.catalog_name);

        let cat_array = StringArray::from(
            catalogs
                .iter()
                .map(|c| Some(c.catalog_name.as_str()))
                .collect::<Vec<_>>(),
        );

        RecordBatch::try_new(CATALOGS_SCHEMA.clone(), vec![Arc::new(cat_array)]).map_err(|e| {
            DatabricksErrorHelper::io().message(format!("Failed to build catalogs result: {}", e))
        })
    }

    fn get_schemas(
        &self,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
    ) -> Result<RecordBatch> {
        let result = self.runtime.block_on(
            self.client
                .list_schemas(&self.session_id, catalog, schema_pattern),
        )?;
        let schemas = parse_schemas(result, catalog)?;

        let schem_array = StringArray::from(
            schemas
                .iter()
                .map(|s| Some(s.schema_name.as_str()))
                .collect::<Vec<_>>(),
        );
        let cat_array = StringArray::from(
            schemas
                .iter()
                .map(|s| Some(s.catalog_name.as_str()))
                .collect::<Vec<_>>(),
        );

        RecordBatch::try_new(
            SCHEMAS_SCHEMA.clone(),
            vec![Arc::new(schem_array), Arc::new(cat_array)],
        )
        .map_err(|e| {
            DatabricksErrorHelper::io().message(format!("Failed to build schemas result: {}", e))
        })
    }

    fn get_tables(
        &self,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        table_pattern: Option<&str>,
        table_types: Option<&[&str]>,
    ) -> Result<RecordBatch> {
        let result = self.runtime.block_on(self.client.list_tables(
            &self.session_id,
            catalog,
            schema_pattern,
            table_pattern,
            table_types,
        ))?;
        let mut tables = parse_tables(result)?;

        // Client-side table type filtering
        if let Some(types) = table_types {
            tables.retain(|t| types.iter().any(|tt| t.table_type.eq_ignore_ascii_case(tt)));
        }

        let n = tables.len();
        let cat: Vec<Option<&str>> = tables.iter().map(|t| Some(t.catalog_name.as_str())).collect();
        let schem: Vec<Option<&str>> =
            tables.iter().map(|t| Some(t.schema_name.as_str())).collect();
        let name: Vec<&str> = tables.iter().map(|t| t.table_name.as_str()).collect();
        let ttype: Vec<&str> = tables.iter().map(|t| t.table_type.as_str()).collect();
        let remarks: Vec<Option<&str>> = tables
            .iter()
            .map(|t| t.remarks.as_deref())
            .collect();

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(cat)),
            Arc::new(StringArray::from(schem)),
            Arc::new(StringArray::from(name)),
            Arc::new(StringArray::from(ttype)),
            Arc::new(StringArray::from(remarks)),
        ];

        let _ = n; // suppress unused warning
        RecordBatch::try_new(TABLES_SCHEMA.clone(), arrays).map_err(|e| {
            DatabricksErrorHelper::io().message(format!("Failed to build tables result: {}", e))
        })
    }

    fn get_columns(
        &self,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        table_pattern: Option<&str>,
        column_pattern: Option<&str>,
    ) -> Result<RecordBatch> {
        // SHOW COLUMNS requires a catalog. If none specified, discover catalogs first.
        let catalogs_to_query: Vec<String> = if let Some(cat) = catalog {
            if cat.is_empty() || cat == "%" || cat == "*" {
                // Query all catalogs
                let result = self
                    .runtime
                    .block_on(self.client.list_catalogs(&self.session_id))?;
                let cats = parse_catalogs(result)?;
                cats.into_iter().map(|c| c.catalog_name).collect()
            } else {
                vec![cat.to_string()]
            }
        } else {
            let result = self
                .runtime
                .block_on(self.client.list_catalogs(&self.session_id))?;
            let cats = parse_catalogs(result)?;
            cats.into_iter().map(|c| c.catalog_name).collect()
        };

        let mut all_columns: Vec<ColumnInfo> = Vec::new();
        for cat in &catalogs_to_query {
            let result = self.runtime.block_on(self.client.list_columns(
                &self.session_id,
                cat,
                schema_pattern,
                table_pattern,
                column_pattern,
            ))?;
            let cols = parse_columns(result)?;
            all_columns.extend(cols);
        }

        columns_to_record_batch(&all_columns)
    }

    fn get_type_info(&self, data_type: Option<i16>) -> Result<RecordBatch> {
        let entries: Vec<_> = TYPE_INFO_ENTRIES
            .iter()
            .filter(|e| data_type.is_none() || data_type == Some(e.data_type))
            .collect();

        let n = entries.len();
        let mut type_name = StringBuilder::with_capacity(n, n * 16);
        let mut data_type_col = Int16Builder::with_capacity(n);
        let mut column_size = Int32Builder::with_capacity(n);
        let mut literal_prefix = StringBuilder::with_capacity(n, n * 4);
        let mut literal_suffix = StringBuilder::with_capacity(n, n * 4);
        let mut create_params = StringBuilder::with_capacity(n, n * 16);
        let mut nullable = Int16Builder::with_capacity(n);
        let mut case_sensitive = BooleanBuilder::with_capacity(n);
        let mut searchable = Int16Builder::with_capacity(n);
        let mut unsigned_attribute = BooleanBuilder::with_capacity(n);
        let mut fixed_prec_scale = BooleanBuilder::with_capacity(n);
        let mut auto_unique_value = BooleanBuilder::with_capacity(n);
        let mut local_type_name = StringBuilder::with_capacity(n, n * 16);
        let mut minimum_scale = Int16Builder::with_capacity(n);
        let mut maximum_scale = Int16Builder::with_capacity(n);
        let mut sql_data_type = Int16Builder::with_capacity(n);
        let mut sql_datetime_sub = Int16Builder::with_capacity(n);
        let mut num_prec_radix = Int32Builder::with_capacity(n);

        for e in &entries {
            type_name.append_value(e.type_name);
            data_type_col.append_value(e.data_type);
            append_opt_i32(&mut column_size, e.column_size);
            append_opt_str(&mut literal_prefix, e.literal_prefix);
            append_opt_str(&mut literal_suffix, e.literal_suffix);
            append_opt_str(&mut create_params, e.create_params);
            nullable.append_value(e.nullable);
            case_sensitive.append_value(e.case_sensitive);
            searchable.append_value(e.searchable);
            append_opt_bool(&mut unsigned_attribute, e.unsigned_attribute);
            fixed_prec_scale.append_value(e.fixed_prec_scale);
            append_opt_bool(&mut auto_unique_value, e.auto_unique_value);
            append_opt_str(&mut local_type_name, e.local_type_name);
            append_opt_i16(&mut minimum_scale, e.minimum_scale);
            append_opt_i16(&mut maximum_scale, e.maximum_scale);
            sql_data_type.append_value(e.sql_data_type);
            append_opt_i16(&mut sql_datetime_sub, e.sql_datetime_sub);
            append_opt_i32(&mut num_prec_radix, e.num_prec_radix);
        }

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(type_name.finish()),
            Arc::new(data_type_col.finish()),
            Arc::new(column_size.finish()),
            Arc::new(literal_prefix.finish()),
            Arc::new(literal_suffix.finish()),
            Arc::new(create_params.finish()),
            Arc::new(nullable.finish()),
            Arc::new(case_sensitive.finish()),
            Arc::new(searchable.finish()),
            Arc::new(unsigned_attribute.finish()),
            Arc::new(fixed_prec_scale.finish()),
            Arc::new(auto_unique_value.finish()),
            Arc::new(local_type_name.finish()),
            Arc::new(minimum_scale.finish()),
            Arc::new(maximum_scale.finish()),
            Arc::new(sql_data_type.finish()),
            Arc::new(sql_datetime_sub.finish()),
            Arc::new(num_prec_radix.finish()),
        ];

        RecordBatch::try_new(TYPE_INFO_SCHEMA.clone(), arrays).map_err(|e| {
            DatabricksErrorHelper::io().message(format!("Failed to build type info result: {}", e))
        })
    }

    fn get_primary_keys(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<RecordBatch> {
        let sql = SqlCommandBuilder::build_show_primary_keys(catalog, schema, table);
        let result = self.runtime.block_on(self.client.execute_statement(
            &self.session_id,
            &sql,
            &ExecuteParams::default(),
        ))?;
        let keys = parse_primary_keys(result, catalog, schema, table)?;

        let n = keys.len();
        let cat: Vec<Option<&str>> = keys.iter().map(|k| Some(k.catalog_name.as_str())).collect();
        let schem: Vec<Option<&str>> = keys.iter().map(|k| Some(k.schema_name.as_str())).collect();
        let tbl: Vec<&str> = keys.iter().map(|k| k.table_name.as_str()).collect();
        let col: Vec<&str> = keys.iter().map(|k| k.column_name.as_str()).collect();
        let seq: Vec<i16> = keys.iter().map(|k| k.key_seq).collect();
        let pk_name: Vec<Option<&str>> = keys.iter().map(|k| k.pk_name.as_deref()).collect();

        let _ = n;
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(cat)),
            Arc::new(StringArray::from(schem)),
            Arc::new(StringArray::from(tbl)),
            Arc::new(StringArray::from(col)),
            Arc::new(arrow_array::Int16Array::from(seq)),
            Arc::new(StringArray::from(pk_name)),
        ];

        RecordBatch::try_new(PRIMARY_KEYS_SCHEMA.clone(), arrays).map_err(|e| {
            DatabricksErrorHelper::io()
                .message(format!("Failed to build primary keys result: {}", e))
        })
    }

    fn get_foreign_keys(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<RecordBatch> {
        let sql = SqlCommandBuilder::build_show_foreign_keys(catalog, schema, table);
        let result = self.runtime.block_on(self.client.execute_statement(
            &self.session_id,
            &sql,
            &ExecuteParams::default(),
        ))?;
        let keys = parse_foreign_keys(result, catalog, schema, table)?;

        let n = keys.len();
        let pk_cat: Vec<Option<&str>> = keys.iter().map(|k| Some(k.pk_catalog.as_str())).collect();
        let pk_schem: Vec<Option<&str>> =
            keys.iter().map(|k| Some(k.pk_schema.as_str())).collect();
        let pk_tbl: Vec<&str> = keys.iter().map(|k| k.pk_table.as_str()).collect();
        let pk_col: Vec<&str> = keys.iter().map(|k| k.pk_column.as_str()).collect();
        let fk_cat: Vec<Option<&str>> = keys.iter().map(|k| Some(k.fk_catalog.as_str())).collect();
        let fk_schem: Vec<Option<&str>> =
            keys.iter().map(|k| Some(k.fk_schema.as_str())).collect();
        let fk_tbl: Vec<&str> = keys.iter().map(|k| k.fk_table.as_str()).collect();
        let fk_col: Vec<&str> = keys.iter().map(|k| k.fk_column.as_str()).collect();
        let seq: Vec<i16> = keys.iter().map(|k| k.key_seq).collect();
        let update_rule: Vec<i16> = keys.iter().map(|k| k.update_rule).collect();
        let delete_rule: Vec<i16> = keys.iter().map(|k| k.delete_rule).collect();
        let fk_name: Vec<Option<&str>> = keys.iter().map(|k| k.fk_name.as_deref()).collect();
        let pk_name: Vec<Option<&str>> = keys.iter().map(|k| k.pk_name.as_deref()).collect();
        let deferrability: Vec<i16> = keys.iter().map(|k| k.deferrability).collect();

        let _ = n;
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(pk_cat)),
            Arc::new(StringArray::from(pk_schem)),
            Arc::new(StringArray::from(pk_tbl)),
            Arc::new(StringArray::from(pk_col)),
            Arc::new(StringArray::from(fk_cat)),
            Arc::new(StringArray::from(fk_schem)),
            Arc::new(StringArray::from(fk_tbl)),
            Arc::new(StringArray::from(fk_col)),
            Arc::new(arrow_array::Int16Array::from(seq)),
            Arc::new(arrow_array::Int16Array::from(update_rule)),
            Arc::new(arrow_array::Int16Array::from(delete_rule)),
            Arc::new(StringArray::from(fk_name)),
            Arc::new(StringArray::from(pk_name)),
            Arc::new(arrow_array::Int16Array::from(deferrability)),
        ];

        RecordBatch::try_new(FOREIGN_KEYS_SCHEMA.clone(), arrays).map_err(|e| {
            DatabricksErrorHelper::io()
                .message(format!("Failed to build foreign keys result: {}", e))
        })
    }

    fn get_table_types(&self) -> Result<RecordBatch> {
        let types = self.client.list_table_types();
        let array = StringArray::from(types);
        RecordBatch::try_new(TABLE_TYPES_SCHEMA.clone(), vec![Arc::new(array)]).map_err(|e| {
            DatabricksErrorHelper::io()
                .message(format!("Failed to build table types result: {}", e))
        })
    }

    fn get_statistics(
        &self,
        _catalog: &str,
        _schema: &str,
        _table: &str,
        _unique: bool,
    ) -> Result<RecordBatch> {
        Ok(empty_batch(&STATISTICS_SCHEMA))
    }

    fn get_special_columns(
        &self,
        _identifier_type: i16,
        _catalog: &str,
        _schema: &str,
        _table: &str,
        _scope: i16,
        _nullable: i16,
    ) -> Result<RecordBatch> {
        Ok(empty_batch(&SPECIAL_COLUMNS_SCHEMA))
    }

    fn get_procedures(
        &self,
        _catalog: Option<&str>,
        _schema_pattern: Option<&str>,
        _proc_pattern: Option<&str>,
    ) -> Result<RecordBatch> {
        Ok(empty_batch(&PROCEDURES_SCHEMA))
    }

    fn get_procedure_columns(
        &self,
        _catalog: Option<&str>,
        _schema_pattern: Option<&str>,
        _proc_pattern: Option<&str>,
        _column_pattern: Option<&str>,
    ) -> Result<RecordBatch> {
        Ok(empty_batch(&PROCEDURE_COLUMNS_SCHEMA))
    }

    fn get_table_privileges(
        &self,
        _catalog: Option<&str>,
        _schema_pattern: Option<&str>,
        _table_pattern: Option<&str>,
    ) -> Result<RecordBatch> {
        Ok(empty_batch(&TABLE_PRIVILEGES_SCHEMA))
    }

    fn get_column_privileges(
        &self,
        _catalog: Option<&str>,
        _schema: Option<&str>,
        _table: &str,
        _column_pattern: Option<&str>,
    ) -> Result<RecordBatch> {
        Ok(empty_batch(&COLUMN_PRIVILEGES_SCHEMA))
    }
}

// ─── Helper builders ──────────────────────────────────────────────────────────

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

fn append_opt_bool(builder: &mut BooleanBuilder, val: Option<bool>) {
    match val {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

/// Convert parsed ColumnInfo structs into a flat JDBC-compatible RecordBatch.
fn columns_to_record_batch(columns: &[ColumnInfo]) -> Result<RecordBatch> {
    let n = columns.len();
    let mut table_cat = StringBuilder::with_capacity(n, n * 16);
    let mut table_schem = StringBuilder::with_capacity(n, n * 16);
    let mut table_name = StringBuilder::with_capacity(n, n * 32);
    let mut column_name = StringBuilder::with_capacity(n, n * 32);
    let mut data_type = Int16Builder::with_capacity(n);
    let mut type_name = StringBuilder::with_capacity(n, n * 16);
    let mut column_size = Int32Builder::with_capacity(n);
    let mut buffer_length = Int32Builder::with_capacity(n);
    let mut decimal_digits = Int16Builder::with_capacity(n);
    let mut num_prec_radix = Int16Builder::with_capacity(n);
    let mut nullable_col = Int16Builder::with_capacity(n);
    let mut remarks = StringBuilder::with_capacity(n, n * 16);
    let mut column_def = StringBuilder::with_capacity(n, 0);
    let mut sql_data_type = Int16Builder::with_capacity(n);
    let mut sql_datetime_sub = Int16Builder::with_capacity(n);
    let mut char_octet_length = Int32Builder::with_capacity(n);
    let mut ordinal_position = Int32Builder::with_capacity(n);
    let mut is_nullable = StringBuilder::with_capacity(n, n * 4);
    let mut scope_catalog = StringBuilder::with_capacity(n, 0);
    let mut scope_schema = StringBuilder::with_capacity(n, 0);
    let mut scope_table = StringBuilder::with_capacity(n, 0);
    let mut source_data_type = Int16Builder::with_capacity(n);
    let mut is_autoincrement = StringBuilder::with_capacity(n, n * 4);
    let mut is_generatedcolumn = StringBuilder::with_capacity(n, n * 4);

    for c in columns {
        let xdbc = databricks_type_to_xdbc(&c.column_type);

        table_cat.append_value(&c.catalog_name);
        table_schem.append_value(&c.schema_name);
        table_name.append_value(&c.table_name);
        column_name.append_value(&c.column_name);
        data_type.append_value(xdbc);
        type_name.append_value(&c.column_type);
        append_opt_i32(&mut column_size, c.column_size);
        buffer_length.append_null(); // not available from Databricks
        append_opt_i16(&mut decimal_digits, c.decimal_digits.map(|v| v as i16));
        append_opt_i16(&mut num_prec_radix, c.radix.map(|v| v as i16));

        // NULLABLE: 0=no nulls, 1=nullable, 2=unknown
        match &c.is_nullable {
            Some(v) if v == "true" || v == "YES" => nullable_col.append_value(1),
            Some(_) => nullable_col.append_value(0),
            None => nullable_col.append_value(2), // unknown
        }

        append_opt_str(&mut remarks, c.remarks.as_deref());
        column_def.append_null(); // default value not available
        sql_data_type.append_value(xdbc);
        sql_datetime_sub.append_null();
        char_octet_length.append_null();
        ordinal_position.append_value(c.ordinal_position.unwrap_or(0));

        match &c.is_nullable {
            Some(v) if v == "true" || v == "YES" => is_nullable.append_value("YES"),
            Some(_) => is_nullable.append_value("NO"),
            None => is_nullable.append_value(""),
        }

        scope_catalog.append_null();
        scope_schema.append_null();
        scope_table.append_null();
        source_data_type.append_null();

        match &c.is_auto_increment {
            Some(v) if v == "YES" => is_autoincrement.append_value("YES"),
            Some(_) => is_autoincrement.append_value("NO"),
            None => is_autoincrement.append_value(""),
        }

        match &c.is_generated {
            Some(v) if v == "YES" => is_generatedcolumn.append_value("YES"),
            Some(_) => is_generatedcolumn.append_value("NO"),
            None => is_generatedcolumn.append_value(""),
        }
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(table_cat.finish()),
        Arc::new(table_schem.finish()),
        Arc::new(table_name.finish()),
        Arc::new(column_name.finish()),
        Arc::new(data_type.finish()),
        Arc::new(type_name.finish()),
        Arc::new(column_size.finish()),
        Arc::new(buffer_length.finish()),
        Arc::new(decimal_digits.finish()),
        Arc::new(num_prec_radix.finish()),
        Arc::new(nullable_col.finish()),
        Arc::new(remarks.finish()),
        Arc::new(column_def.finish()),
        Arc::new(sql_data_type.finish()),
        Arc::new(sql_datetime_sub.finish()),
        Arc::new(char_octet_length.finish()),
        Arc::new(ordinal_position.finish()),
        Arc::new(is_nullable.finish()),
        Arc::new(scope_catalog.finish()),
        Arc::new(scope_schema.finish()),
        Arc::new(scope_table.finish()),
        Arc::new(source_data_type.finish()),
        Arc::new(is_autoincrement.finish()),
        Arc::new(is_generatedcolumn.finish()),
    ];

    RecordBatch::try_new(COLUMNS_SCHEMA.clone(), arrays).map_err(|e| {
        DatabricksErrorHelper::io().message(format!("Failed to build columns result: {}", e))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::schemas;

    #[test]
    fn test_type_info_all_types() {
        // Create a minimal mock service to test type_info builder
        let batch = build_type_info_batch(None);
        assert!(batch.is_ok());
        let batch = batch.unwrap();
        assert_eq!(batch.num_rows(), TYPE_INFO_ENTRIES.len());
        assert_eq!(batch.schema(), *TYPE_INFO_SCHEMA);
    }

    #[test]
    fn test_type_info_filtered() {
        let batch = build_type_info_batch(Some(4)); // INTEGER
        assert!(batch.is_ok());
        let batch = batch.unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_columns_to_record_batch_empty() {
        let batch = columns_to_record_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), *schemas::COLUMNS_SCHEMA);
    }

    #[test]
    fn test_columns_to_record_batch() {
        let columns = vec![ColumnInfo {
            catalog_name: "main".to_string(),
            schema_name: "default".to_string(),
            table_name: "t1".to_string(),
            column_name: "id".to_string(),
            column_type: "INT".to_string(),
            column_size: Some(10),
            decimal_digits: Some(0),
            radix: Some(10),
            is_nullable: Some("true".to_string()),
            remarks: None,
            ordinal_position: Some(1),
            is_auto_increment: Some("NO".to_string()),
            is_generated: Some("NO".to_string()),
        }];

        let batch = columns_to_record_batch(&columns).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema(), *schemas::COLUMNS_SCHEMA);
    }

    #[test]
    fn test_empty_batch_returns() {
        let batch = empty_batch(&STATISTICS_SCHEMA);
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), *STATISTICS_SCHEMA);

        let batch = empty_batch(&SPECIAL_COLUMNS_SCHEMA);
        assert_eq!(batch.num_rows(), 0);

        let batch = empty_batch(&PROCEDURES_SCHEMA);
        assert_eq!(batch.num_rows(), 0);

        let batch = empty_batch(&PROCEDURE_COLUMNS_SCHEMA);
        assert_eq!(batch.num_rows(), 0);

        let batch = empty_batch(&TABLE_PRIVILEGES_SCHEMA);
        assert_eq!(batch.num_rows(), 0);

        let batch = empty_batch(&COLUMN_PRIVILEGES_SCHEMA);
        assert_eq!(batch.num_rows(), 0);
    }

    /// Helper to test type_info building without needing a full service.
    fn build_type_info_batch(data_type: Option<i16>) -> Result<RecordBatch> {
        let entries: Vec<_> = TYPE_INFO_ENTRIES
            .iter()
            .filter(|e| data_type.is_none() || data_type == Some(e.data_type))
            .collect();

        let n = entries.len();
        let mut type_name = StringBuilder::with_capacity(n, n * 16);
        let mut data_type_col = Int16Builder::with_capacity(n);
        let mut column_size = Int32Builder::with_capacity(n);
        let mut literal_prefix = StringBuilder::with_capacity(n, n * 4);
        let mut literal_suffix = StringBuilder::with_capacity(n, n * 4);
        let mut create_params = StringBuilder::with_capacity(n, n * 16);
        let mut nullable_col = Int16Builder::with_capacity(n);
        let mut case_sensitive = BooleanBuilder::with_capacity(n);
        let mut searchable = Int16Builder::with_capacity(n);
        let mut unsigned_attribute = BooleanBuilder::with_capacity(n);
        let mut fixed_prec_scale = BooleanBuilder::with_capacity(n);
        let mut auto_unique_value = BooleanBuilder::with_capacity(n);
        let mut local_type_name = StringBuilder::with_capacity(n, n * 16);
        let mut minimum_scale = Int16Builder::with_capacity(n);
        let mut maximum_scale = Int16Builder::with_capacity(n);
        let mut sql_data_type = Int16Builder::with_capacity(n);
        let mut sql_datetime_sub = Int16Builder::with_capacity(n);
        let mut num_prec_radix = Int32Builder::with_capacity(n);

        for e in &entries {
            type_name.append_value(e.type_name);
            data_type_col.append_value(e.data_type);
            append_opt_i32(&mut column_size, e.column_size);
            append_opt_str(&mut literal_prefix, e.literal_prefix);
            append_opt_str(&mut literal_suffix, e.literal_suffix);
            append_opt_str(&mut create_params, e.create_params);
            nullable_col.append_value(e.nullable);
            case_sensitive.append_value(e.case_sensitive);
            searchable.append_value(e.searchable);
            append_opt_bool(&mut unsigned_attribute, e.unsigned_attribute);
            fixed_prec_scale.append_value(e.fixed_prec_scale);
            append_opt_bool(&mut auto_unique_value, e.auto_unique_value);
            append_opt_str(&mut local_type_name, e.local_type_name);
            append_opt_i16(&mut minimum_scale, e.minimum_scale);
            append_opt_i16(&mut maximum_scale, e.maximum_scale);
            sql_data_type.append_value(e.sql_data_type);
            append_opt_i16(&mut sql_datetime_sub, e.sql_datetime_sub);
            append_opt_i32(&mut num_prec_radix, e.num_prec_radix);
        }

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(type_name.finish()),
            Arc::new(data_type_col.finish()),
            Arc::new(column_size.finish()),
            Arc::new(literal_prefix.finish()),
            Arc::new(literal_suffix.finish()),
            Arc::new(create_params.finish()),
            Arc::new(nullable_col.finish()),
            Arc::new(case_sensitive.finish()),
            Arc::new(searchable.finish()),
            Arc::new(unsigned_attribute.finish()),
            Arc::new(fixed_prec_scale.finish()),
            Arc::new(auto_unique_value.finish()),
            Arc::new(local_type_name.finish()),
            Arc::new(minimum_scale.finish()),
            Arc::new(maximum_scale.finish()),
            Arc::new(sql_data_type.finish()),
            Arc::new(sql_datetime_sub.finish()),
            Arc::new(num_prec_radix.finish()),
        ];

        RecordBatch::try_new(TYPE_INFO_SCHEMA.clone(), arrays).map_err(|e| {
            DatabricksErrorHelper::io().message(format!("Failed to build type info: {}", e))
        })
    }
}
