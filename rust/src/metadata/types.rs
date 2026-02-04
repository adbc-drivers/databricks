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

//! Data structures for metadata query results.
//!
//! These types represent the results of metadata queries executed via
//! SQL commands like `SHOW CATALOGS`, `SHOW SCHEMAS`, `SHOW TABLES`, etc.
//! They are used by the `MetadataService` to provide structured results
//! for ADBC Connection interface methods like `get_objects()` and
//! `get_table_schema()`.

/// Catalog information from `SHOW CATALOGS`.
#[derive(Debug, Clone)]
pub struct CatalogInfo {
    /// The name of the catalog.
    pub catalog_name: String,
}

/// Schema information from `SHOW SCHEMAS`.
#[derive(Debug, Clone)]
pub struct SchemaInfo {
    /// The name of the catalog containing this schema.
    pub catalog_name: String,
    /// The name of the schema.
    pub schema_name: String,
}

/// Table information from `SHOW TABLES`.
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// The name of the catalog containing this table.
    pub catalog_name: String,
    /// The name of the schema containing this table.
    pub schema_name: String,
    /// The name of the table.
    pub table_name: String,
    /// The type of the table (e.g., "TABLE", "VIEW", "SYSTEM TABLE").
    pub table_type: String,
    /// Optional remarks/comments about the table.
    pub remarks: Option<String>,
}

/// Column information from `SHOW COLUMNS`.
///
/// This struct contains all XDBC (Extended Database Connectivity) fields
/// as defined by the ADBC specification for column metadata.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// The name of the catalog containing this column's table.
    pub catalog_name: String,
    /// The name of the schema containing this column's table.
    pub schema_name: String,
    /// The name of the table containing this column.
    pub table_name: String,
    /// The name of the column.
    pub column_name: String,
    /// The 1-based position of the column in the table.
    pub ordinal_position: i32,
    /// The SQL data type code.
    pub data_type: String,
    /// The database-specific type name.
    pub type_name: String,
    /// The column size (precision for numeric types, max length for strings).
    pub column_size: Option<i32>,
    /// The number of decimal digits (scale for numeric types).
    pub decimal_digits: Option<i16>,
    /// The radix for numeric precision (usually 10 or 2).
    pub num_prec_radix: Option<i16>,
    /// Nullability indicator: 0=no nulls, 1=nullable, 2=unknown.
    pub nullable: i16,
    /// Optional remarks/comments about the column.
    pub remarks: Option<String>,
    /// The default value for the column.
    pub column_def: Option<String>,
    /// String representation of nullability: "YES", "NO", or "" (unknown).
    pub is_nullable: String,
    /// Whether the column is auto-incrementing.
    pub is_autoincrement: Option<bool>,
    /// Whether the column is generated.
    pub is_generatedcolumn: Option<bool>,
}

/// Primary key information from `SHOW KEYS`.
#[derive(Debug, Clone)]
pub struct PrimaryKeyInfo {
    /// The name of the catalog containing the table.
    pub catalog_name: String,
    /// The name of the schema containing the table.
    pub schema_name: String,
    /// The name of the table.
    pub table_name: String,
    /// The name of the column that is part of the primary key.
    pub column_name: String,
    /// The sequence number of this column in the primary key (1-based).
    pub key_seq: i16,
    /// The name of the primary key constraint, if any.
    pub pk_name: Option<String>,
}

/// Foreign key information from `SHOW FOREIGN KEYS`.
#[derive(Debug, Clone)]
pub struct ForeignKeyInfo {
    /// The catalog of the primary key table being referenced.
    pub pk_catalog: String,
    /// The schema of the primary key table being referenced.
    pub pk_schema: String,
    /// The name of the primary key table being referenced.
    pub pk_table: String,
    /// The column name in the primary key table.
    pub pk_column: String,
    /// The catalog of the foreign key table.
    pub fk_catalog: String,
    /// The schema of the foreign key table.
    pub fk_schema: String,
    /// The name of the foreign key table.
    pub fk_table: String,
    /// The column name in the foreign key table.
    pub fk_column: String,
    /// The sequence number of this column in the foreign key (1-based).
    pub key_seq: i16,
    /// The name of the foreign key constraint, if any.
    pub fk_name: Option<String>,
    /// The name of the primary key constraint being referenced, if any.
    pub pk_name: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_info_instantiation_and_clone() {
        let catalog = CatalogInfo {
            catalog_name: "main".to_string(),
        };
        assert_eq!(catalog.catalog_name, "main");

        let cloned = catalog.clone();
        assert_eq!(cloned.catalog_name, "main");
    }

    #[test]
    fn test_schema_info_instantiation_and_clone() {
        let schema = SchemaInfo {
            catalog_name: "main".to_string(),
            schema_name: "default".to_string(),
        };
        assert_eq!(schema.catalog_name, "main");
        assert_eq!(schema.schema_name, "default");

        let cloned = schema.clone();
        assert_eq!(cloned.catalog_name, "main");
        assert_eq!(cloned.schema_name, "default");
    }

    #[test]
    fn test_table_info_instantiation_and_clone() {
        let table = TableInfo {
            catalog_name: "main".to_string(),
            schema_name: "default".to_string(),
            table_name: "users".to_string(),
            table_type: "TABLE".to_string(),
            remarks: Some("User accounts table".to_string()),
        };
        assert_eq!(table.catalog_name, "main");
        assert_eq!(table.schema_name, "default");
        assert_eq!(table.table_name, "users");
        assert_eq!(table.table_type, "TABLE");
        assert_eq!(table.remarks, Some("User accounts table".to_string()));

        let cloned = table.clone();
        assert_eq!(cloned.table_name, "users");
        assert_eq!(cloned.remarks, Some("User accounts table".to_string()));
    }

    #[test]
    fn test_table_info_without_remarks() {
        let table = TableInfo {
            catalog_name: "main".to_string(),
            schema_name: "default".to_string(),
            table_name: "events".to_string(),
            table_type: "VIEW".to_string(),
            remarks: None,
        };
        assert_eq!(table.table_type, "VIEW");
        assert!(table.remarks.is_none());
    }

    #[test]
    fn test_column_info_instantiation_and_clone() {
        let column = ColumnInfo {
            catalog_name: "main".to_string(),
            schema_name: "default".to_string(),
            table_name: "users".to_string(),
            column_name: "id".to_string(),
            ordinal_position: 1,
            data_type: "BIGINT".to_string(),
            type_name: "BIGINT".to_string(),
            column_size: Some(19),
            decimal_digits: Some(0),
            num_prec_radix: Some(10),
            nullable: 0,
            remarks: None,
            column_def: None,
            is_nullable: "NO".to_string(),
            is_autoincrement: Some(true),
            is_generatedcolumn: Some(false),
        };
        assert_eq!(column.column_name, "id");
        assert_eq!(column.ordinal_position, 1);
        assert_eq!(column.nullable, 0);
        assert_eq!(column.is_nullable, "NO");
        assert_eq!(column.is_autoincrement, Some(true));

        let cloned = column.clone();
        assert_eq!(cloned.column_name, "id");
        assert_eq!(cloned.is_autoincrement, Some(true));
    }

    #[test]
    fn test_column_info_nullable_variants() {
        // Nullable column
        let nullable_col = ColumnInfo {
            catalog_name: "main".to_string(),
            schema_name: "default".to_string(),
            table_name: "users".to_string(),
            column_name: "email".to_string(),
            ordinal_position: 2,
            data_type: "STRING".to_string(),
            type_name: "STRING".to_string(),
            column_size: Some(255),
            decimal_digits: None,
            num_prec_radix: None,
            nullable: 1,
            remarks: Some("User email address".to_string()),
            column_def: None,
            is_nullable: "YES".to_string(),
            is_autoincrement: None,
            is_generatedcolumn: None,
        };
        assert_eq!(nullable_col.nullable, 1);
        assert_eq!(nullable_col.is_nullable, "YES");

        // Unknown nullability
        let unknown_col = ColumnInfo {
            catalog_name: "main".to_string(),
            schema_name: "default".to_string(),
            table_name: "users".to_string(),
            column_name: "metadata".to_string(),
            ordinal_position: 3,
            data_type: "MAP".to_string(),
            type_name: "MAP<STRING,STRING>".to_string(),
            column_size: None,
            decimal_digits: None,
            num_prec_radix: None,
            nullable: 2,
            remarks: None,
            column_def: None,
            is_nullable: "".to_string(),
            is_autoincrement: None,
            is_generatedcolumn: None,
        };
        assert_eq!(unknown_col.nullable, 2);
        assert_eq!(unknown_col.is_nullable, "");
    }

    #[test]
    fn test_primary_key_info_instantiation_and_clone() {
        let pk = PrimaryKeyInfo {
            catalog_name: "main".to_string(),
            schema_name: "default".to_string(),
            table_name: "users".to_string(),
            column_name: "id".to_string(),
            key_seq: 1,
            pk_name: Some("pk_users".to_string()),
        };
        assert_eq!(pk.catalog_name, "main");
        assert_eq!(pk.column_name, "id");
        assert_eq!(pk.key_seq, 1);
        assert_eq!(pk.pk_name, Some("pk_users".to_string()));

        let cloned = pk.clone();
        assert_eq!(cloned.column_name, "id");
        assert_eq!(cloned.pk_name, Some("pk_users".to_string()));
    }

    #[test]
    fn test_primary_key_info_composite_key() {
        // First column in composite key
        let pk1 = PrimaryKeyInfo {
            catalog_name: "main".to_string(),
            schema_name: "default".to_string(),
            table_name: "order_items".to_string(),
            column_name: "order_id".to_string(),
            key_seq: 1,
            pk_name: Some("pk_order_items".to_string()),
        };
        // Second column in composite key
        let pk2 = PrimaryKeyInfo {
            catalog_name: "main".to_string(),
            schema_name: "default".to_string(),
            table_name: "order_items".to_string(),
            column_name: "item_id".to_string(),
            key_seq: 2,
            pk_name: Some("pk_order_items".to_string()),
        };
        assert_eq!(pk1.key_seq, 1);
        assert_eq!(pk2.key_seq, 2);
        assert_eq!(pk1.pk_name, pk2.pk_name);
    }

    #[test]
    fn test_foreign_key_info_instantiation_and_clone() {
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
        assert_eq!(fk.pk_table, "users");
        assert_eq!(fk.pk_column, "id");
        assert_eq!(fk.fk_table, "orders");
        assert_eq!(fk.fk_column, "user_id");
        assert_eq!(fk.key_seq, 1);

        let cloned = fk.clone();
        assert_eq!(cloned.pk_table, "users");
        assert_eq!(cloned.fk_table, "orders");
        assert_eq!(cloned.fk_name, Some("fk_orders_users".to_string()));
    }

    #[test]
    fn test_foreign_key_info_cross_catalog() {
        let fk = ForeignKeyInfo {
            pk_catalog: "catalog_a".to_string(),
            pk_schema: "schema_a".to_string(),
            pk_table: "reference_data".to_string(),
            pk_column: "code".to_string(),
            fk_catalog: "catalog_b".to_string(),
            fk_schema: "schema_b".to_string(),
            fk_table: "transactions".to_string(),
            fk_column: "ref_code".to_string(),
            key_seq: 1,
            fk_name: None,
            pk_name: None,
        };
        assert_ne!(fk.pk_catalog, fk.fk_catalog);
        assert!(fk.fk_name.is_none());
        assert!(fk.pk_name.is_none());
    }

    #[test]
    fn test_debug_trait_implementations() {
        // Verify Debug trait is implemented for all types
        let catalog = CatalogInfo {
            catalog_name: "test".to_string(),
        };
        let debug_str = format!("{:?}", catalog);
        assert!(debug_str.contains("CatalogInfo"));
        assert!(debug_str.contains("test"));

        let schema = SchemaInfo {
            catalog_name: "test".to_string(),
            schema_name: "public".to_string(),
        };
        let debug_str = format!("{:?}", schema);
        assert!(debug_str.contains("SchemaInfo"));

        let table = TableInfo {
            catalog_name: "test".to_string(),
            schema_name: "public".to_string(),
            table_name: "my_table".to_string(),
            table_type: "TABLE".to_string(),
            remarks: None,
        };
        let debug_str = format!("{:?}", table);
        assert!(debug_str.contains("TableInfo"));

        let pk = PrimaryKeyInfo {
            catalog_name: "test".to_string(),
            schema_name: "public".to_string(),
            table_name: "my_table".to_string(),
            column_name: "id".to_string(),
            key_seq: 1,
            pk_name: None,
        };
        let debug_str = format!("{:?}", pk);
        assert!(debug_str.contains("PrimaryKeyInfo"));

        let fk = ForeignKeyInfo {
            pk_catalog: "test".to_string(),
            pk_schema: "public".to_string(),
            pk_table: "ref".to_string(),
            pk_column: "id".to_string(),
            fk_catalog: "test".to_string(),
            fk_schema: "public".to_string(),
            fk_table: "child".to_string(),
            fk_column: "ref_id".to_string(),
            key_seq: 1,
            fk_name: None,
            pk_name: None,
        };
        let debug_str = format!("{:?}", fk);
        assert!(debug_str.contains("ForeignKeyInfo"));
    }
}
