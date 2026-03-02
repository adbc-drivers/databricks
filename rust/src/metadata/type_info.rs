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

//! Type info catalog for SQLGetTypeInfo.
//!
//! Describes all SQL data types supported by Databricks, with their
//! JDBC/ODBC properties. Used by `MetadataService::get_type_info()`.

/// ODBC searchable constants.
const SQL_SEARCHABLE: i16 = 3;
const SQL_PRED_BASIC: i16 = 2;
const SQL_UNSEARCHABLE: i16 = 0;

/// ODBC nullable constant.
const SQL_NULLABLE: i16 = 1;

/// A single type info entry describing a Databricks SQL type.
#[derive(Debug, Clone)]
pub struct TypeInfoEntry {
    pub type_name: &'static str,
    pub data_type: i16,
    pub column_size: Option<i32>,
    pub literal_prefix: Option<&'static str>,
    pub literal_suffix: Option<&'static str>,
    pub create_params: Option<&'static str>,
    pub nullable: i16,
    pub case_sensitive: bool,
    pub searchable: i16,
    pub unsigned_attribute: Option<bool>,
    pub fixed_prec_scale: bool,
    pub auto_unique_value: Option<bool>,
    pub local_type_name: Option<&'static str>,
    pub minimum_scale: Option<i16>,
    pub maximum_scale: Option<i16>,
    pub sql_data_type: i16,
    pub sql_datetime_sub: Option<i16>,
    pub num_prec_radix: Option<i32>,
}

/// Static catalog of all Databricks SQL types.
///
/// Type codes match `type_mapping::databricks_type_to_xdbc()`.
///
/// ## Design notes
///
/// - **INTERVAL**: Mapped to VARCHAR (12) to match the Databricks JDBC driver.
///   ODBC 3.x defines specific `SQL_INTERVAL_*` subtypes, but Databricks represents
///   intervals as strings. Applications needing ODBC interval subtypes should handle
///   the conversion at the ODBC wrapper level.
///
/// - **ARRAY/MAP/STRUCT**: Use JDBC type codes (2003/2000/2002) rather than standard
///   ODBC `SQL_*` constants, matching the Databricks JDBC driver behavior. ODBC
///   applications may see unfamiliar type codes for these complex types.
///
/// - **TIMESTAMP and TIMESTAMP_NTZ** share `data_type: 93` (SQL_TIMESTAMP). A call to
///   `get_type_info(93)` will return both entries, which is valid per the ODBC spec.
pub static TYPE_INFO_ENTRIES: &[TypeInfoEntry] = &[
    TypeInfoEntry {
        type_name: "BOOLEAN",
        data_type: -7, // BIT
        column_size: Some(1),
        literal_prefix: None,
        literal_suffix: None,
        create_params: None,
        nullable: SQL_NULLABLE,
        case_sensitive: false,
        searchable: SQL_PRED_BASIC,
        unsigned_attribute: None,
        fixed_prec_scale: false,
        auto_unique_value: Some(false),
        local_type_name: Some("BOOLEAN"),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: -7,
        sql_datetime_sub: None,
        num_prec_radix: None,
    },
    TypeInfoEntry {
        type_name: "TINYINT",
        data_type: -6, // TINYINT
        column_size: Some(3),
        literal_prefix: None,
        literal_suffix: None,
        create_params: None,
        nullable: SQL_NULLABLE,
        case_sensitive: false,
        searchable: SQL_SEARCHABLE,
        unsigned_attribute: Some(false),
        fixed_prec_scale: false,
        auto_unique_value: Some(false),
        local_type_name: Some("TINYINT"),
        minimum_scale: Some(0),
        maximum_scale: Some(0),
        sql_data_type: -6,
        sql_datetime_sub: None,
        num_prec_radix: Some(10),
    },
    TypeInfoEntry {
        type_name: "SMALLINT",
        data_type: 5, // SMALLINT
        column_size: Some(5),
        literal_prefix: None,
        literal_suffix: None,
        create_params: None,
        nullable: SQL_NULLABLE,
        case_sensitive: false,
        searchable: SQL_SEARCHABLE,
        unsigned_attribute: Some(false),
        fixed_prec_scale: false,
        auto_unique_value: Some(false),
        local_type_name: Some("SMALLINT"),
        minimum_scale: Some(0),
        maximum_scale: Some(0),
        sql_data_type: 5,
        sql_datetime_sub: None,
        num_prec_radix: Some(10),
    },
    TypeInfoEntry {
        type_name: "INT",
        data_type: 4, // INTEGER
        column_size: Some(10),
        literal_prefix: None,
        literal_suffix: None,
        create_params: None,
        nullable: SQL_NULLABLE,
        case_sensitive: false,
        searchable: SQL_SEARCHABLE,
        unsigned_attribute: Some(false),
        fixed_prec_scale: false,
        auto_unique_value: Some(false),
        local_type_name: Some("INT"),
        minimum_scale: Some(0),
        maximum_scale: Some(0),
        sql_data_type: 4,
        sql_datetime_sub: None,
        num_prec_radix: Some(10),
    },
    TypeInfoEntry {
        type_name: "BIGINT",
        data_type: -5, // BIGINT
        column_size: Some(19),
        literal_prefix: None,
        literal_suffix: None,
        create_params: None,
        nullable: SQL_NULLABLE,
        case_sensitive: false,
        searchable: SQL_SEARCHABLE,
        unsigned_attribute: Some(false),
        fixed_prec_scale: false,
        auto_unique_value: Some(false),
        local_type_name: Some("BIGINT"),
        minimum_scale: Some(0),
        maximum_scale: Some(0),
        sql_data_type: -5,
        sql_datetime_sub: None,
        num_prec_radix: Some(10),
    },
    TypeInfoEntry {
        type_name: "FLOAT",
        data_type: 6, // FLOAT
        column_size: Some(7),
        literal_prefix: None,
        literal_suffix: None,
        create_params: None,
        nullable: SQL_NULLABLE,
        case_sensitive: false,
        searchable: SQL_SEARCHABLE,
        unsigned_attribute: Some(false),
        fixed_prec_scale: false,
        auto_unique_value: Some(false),
        local_type_name: Some("FLOAT"),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: 6,
        sql_datetime_sub: None,
        num_prec_radix: Some(10),
    },
    TypeInfoEntry {
        type_name: "DOUBLE",
        data_type: 8, // DOUBLE
        column_size: Some(15),
        literal_prefix: None,
        literal_suffix: None,
        create_params: None,
        nullable: SQL_NULLABLE,
        case_sensitive: false,
        searchable: SQL_SEARCHABLE,
        unsigned_attribute: Some(false),
        fixed_prec_scale: false,
        auto_unique_value: Some(false),
        local_type_name: Some("DOUBLE"),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: 8,
        sql_datetime_sub: None,
        num_prec_radix: Some(10),
    },
    TypeInfoEntry {
        type_name: "DECIMAL",
        data_type: 3, // DECIMAL
        column_size: Some(38),
        literal_prefix: None,
        literal_suffix: None,
        create_params: Some("precision,scale"),
        nullable: SQL_NULLABLE,
        case_sensitive: false,
        searchable: SQL_SEARCHABLE,
        unsigned_attribute: Some(false),
        fixed_prec_scale: false,
        auto_unique_value: Some(false),
        local_type_name: Some("DECIMAL"),
        minimum_scale: Some(0),
        maximum_scale: Some(38),
        sql_data_type: 3,
        sql_datetime_sub: None,
        num_prec_radix: Some(10),
    },
    TypeInfoEntry {
        type_name: "STRING",
        data_type: -1, // LONGVARCHAR
        column_size: Some(2_147_483_647),
        literal_prefix: Some("'"),
        literal_suffix: Some("'"),
        create_params: None,
        nullable: SQL_NULLABLE,
        case_sensitive: true,
        searchable: SQL_SEARCHABLE,
        unsigned_attribute: None,
        fixed_prec_scale: false,
        auto_unique_value: None,
        local_type_name: Some("STRING"),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: -1,
        sql_datetime_sub: None,
        num_prec_radix: None,
    },
    TypeInfoEntry {
        type_name: "VARCHAR",
        data_type: 12, // VARCHAR
        column_size: Some(65535),
        literal_prefix: Some("'"),
        literal_suffix: Some("'"),
        create_params: Some("length"),
        nullable: SQL_NULLABLE,
        case_sensitive: true,
        searchable: SQL_SEARCHABLE,
        unsigned_attribute: None,
        fixed_prec_scale: false,
        auto_unique_value: None,
        local_type_name: Some("VARCHAR"),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: 12,
        sql_datetime_sub: None,
        num_prec_radix: None,
    },
    TypeInfoEntry {
        type_name: "CHAR",
        data_type: 1, // CHAR
        column_size: Some(255),
        literal_prefix: Some("'"),
        literal_suffix: Some("'"),
        create_params: Some("length"),
        nullable: SQL_NULLABLE,
        case_sensitive: true,
        searchable: SQL_SEARCHABLE,
        unsigned_attribute: None,
        fixed_prec_scale: false,
        auto_unique_value: None,
        local_type_name: Some("CHAR"),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: 1,
        sql_datetime_sub: None,
        num_prec_radix: None,
    },
    TypeInfoEntry {
        type_name: "BINARY",
        data_type: -3, // VARBINARY
        column_size: Some(2_147_483_647),
        literal_prefix: Some("X'"),
        literal_suffix: Some("'"),
        create_params: None,
        nullable: SQL_NULLABLE,
        case_sensitive: false,
        searchable: SQL_UNSEARCHABLE,
        unsigned_attribute: None,
        fixed_prec_scale: false,
        auto_unique_value: None,
        local_type_name: Some("BINARY"),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: -3,
        sql_datetime_sub: None,
        num_prec_radix: None,
    },
    TypeInfoEntry {
        type_name: "DATE",
        data_type: 91, // DATE
        column_size: Some(10),
        literal_prefix: Some("'"),
        literal_suffix: Some("'"),
        create_params: None,
        nullable: SQL_NULLABLE,
        case_sensitive: false,
        searchable: SQL_SEARCHABLE,
        unsigned_attribute: None,
        fixed_prec_scale: false,
        auto_unique_value: None,
        local_type_name: Some("DATE"),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: 9, // SQL_DATE -> SQL_DATETIME
        sql_datetime_sub: Some(1), // SQL_CODE_DATE
        num_prec_radix: None,
    },
    TypeInfoEntry {
        type_name: "TIMESTAMP",
        data_type: 93, // TIMESTAMP
        column_size: Some(29),
        literal_prefix: Some("'"),
        literal_suffix: Some("'"),
        create_params: None,
        nullable: SQL_NULLABLE,
        case_sensitive: false,
        searchable: SQL_SEARCHABLE,
        unsigned_attribute: None,
        fixed_prec_scale: false,
        auto_unique_value: None,
        local_type_name: Some("TIMESTAMP"),
        minimum_scale: Some(0),
        maximum_scale: Some(6),
        sql_data_type: 9, // SQL_DATETIME
        sql_datetime_sub: Some(3), // SQL_CODE_TIMESTAMP
        num_prec_radix: None,
    },
    TypeInfoEntry {
        type_name: "TIMESTAMP_NTZ",
        data_type: 93, // TIMESTAMP
        column_size: Some(29),
        literal_prefix: Some("'"),
        literal_suffix: Some("'"),
        create_params: None,
        nullable: SQL_NULLABLE,
        case_sensitive: false,
        searchable: SQL_SEARCHABLE,
        unsigned_attribute: None,
        fixed_prec_scale: false,
        auto_unique_value: None,
        local_type_name: Some("TIMESTAMP_NTZ"),
        minimum_scale: Some(0),
        maximum_scale: Some(6),
        sql_data_type: 9, // SQL_DATETIME
        sql_datetime_sub: Some(3), // SQL_CODE_TIMESTAMP
        num_prec_radix: None,
    },
    TypeInfoEntry {
        type_name: "ARRAY",
        data_type: 2003, // ARRAY
        column_size: None,
        literal_prefix: None,
        literal_suffix: None,
        create_params: None,
        nullable: SQL_NULLABLE,
        case_sensitive: false,
        searchable: SQL_UNSEARCHABLE,
        unsigned_attribute: None,
        fixed_prec_scale: false,
        auto_unique_value: None,
        local_type_name: Some("ARRAY"),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: 2003,
        sql_datetime_sub: None,
        num_prec_radix: None,
    },
    TypeInfoEntry {
        type_name: "MAP",
        data_type: 2000, // JAVA_OBJECT
        column_size: None,
        literal_prefix: None,
        literal_suffix: None,
        create_params: None,
        nullable: SQL_NULLABLE,
        case_sensitive: false,
        searchable: SQL_UNSEARCHABLE,
        unsigned_attribute: None,
        fixed_prec_scale: false,
        auto_unique_value: None,
        local_type_name: Some("MAP"),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: 2000,
        sql_datetime_sub: None,
        num_prec_radix: None,
    },
    TypeInfoEntry {
        type_name: "STRUCT",
        data_type: 2002, // STRUCT
        column_size: None,
        literal_prefix: None,
        literal_suffix: None,
        create_params: None,
        nullable: SQL_NULLABLE,
        case_sensitive: false,
        searchable: SQL_UNSEARCHABLE,
        unsigned_attribute: None,
        fixed_prec_scale: false,
        auto_unique_value: None,
        local_type_name: Some("STRUCT"),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: 2002,
        sql_datetime_sub: None,
        num_prec_radix: None,
    },
    TypeInfoEntry {
        type_name: "INTERVAL",
        data_type: 12, // VARCHAR (matches JDBC driver)
        column_size: Some(65535),
        literal_prefix: Some("'"),
        literal_suffix: Some("'"),
        create_params: None,
        nullable: SQL_NULLABLE,
        case_sensitive: false,
        searchable: SQL_SEARCHABLE,
        unsigned_attribute: None,
        fixed_prec_scale: false,
        auto_unique_value: None,
        local_type_name: Some("INTERVAL"),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: 12,
        sql_datetime_sub: None,
        num_prec_radix: None,
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::type_mapping::databricks_type_to_xdbc;

    #[test]
    fn test_type_info_count() {
        assert_eq!(TYPE_INFO_ENTRIES.len(), 19);
    }

    #[test]
    fn test_type_codes_match_type_mapping() {
        // Verify type codes in TYPE_INFO_ENTRIES are consistent with type_mapping.rs
        for entry in TYPE_INFO_ENTRIES {
            let expected = databricks_type_to_xdbc(entry.type_name);
            assert_eq!(
                entry.data_type, expected,
                "Type code mismatch for {}: entry={}, mapping={}",
                entry.type_name, entry.data_type, expected
            );
        }
    }

    #[test]
    fn test_all_entries_nullable() {
        for entry in TYPE_INFO_ENTRIES {
            assert_eq!(entry.nullable, SQL_NULLABLE, "All types should be nullable");
        }
    }
}
