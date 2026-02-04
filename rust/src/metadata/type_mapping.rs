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

//! Type mapping functions for converting between Databricks SQL types,
//! Arrow DataTypes, and XDBC/JDBC type codes.
//!
//! This module provides functions to map Databricks SQL type names to their
//! corresponding Arrow DataTypes and XDBC (Extended Database Connectivity) type codes.
//! These mappings are essential for implementing `get_table_schema()` and
//! populating XDBC fields in `get_objects()`.
//!
//! ## Type Mappings
//!
//! | Databricks Type | Arrow DataType | XDBC Code |
//! |-----------------|----------------|-----------|
//! | BOOLEAN | Boolean | -7 (BIT) |
//! | TINYINT | Int8 | -6 (TINYINT) |
//! | SMALLINT | Int16 | 5 (SMALLINT) |
//! | INT | Int32 | 4 (INTEGER) |
//! | BIGINT | Int64 | -5 (BIGINT) |
//! | FLOAT | Float32 | 6 (FLOAT) |
//! | DOUBLE | Float64 | 8 (DOUBLE) |
//! | DECIMAL(p,s) | Decimal128(p,s) | 3 (DECIMAL) |
//! | STRING | Utf8 | -1 (LONGVARCHAR) |
//! | BINARY | Binary | -3 (VARBINARY) |
//! | DATE | Date32 | 91 (DATE) |
//! | TIMESTAMP | Timestamp(μs, None) | 93 (TIMESTAMP) |
//!
//! ## Example
//!
//! ```
//! use databricks_adbc::metadata::type_mapping::{databricks_type_to_arrow, databricks_type_to_xdbc};
//! use arrow_schema::DataType;
//!
//! let arrow_type = databricks_type_to_arrow("BIGINT");
//! assert_eq!(arrow_type, DataType::Int64);
//!
//! let xdbc_code = databricks_type_to_xdbc("BIGINT");
//! assert_eq!(xdbc_code, -5); // JDBC BIGINT
//! ```

use arrow_schema::{DataType, IntervalUnit, TimeUnit};

/// Default precision for DECIMAL types when not specified.
const DEFAULT_DECIMAL_PRECISION: u8 = 38;

/// Default scale for DECIMAL types when not specified.
const DEFAULT_DECIMAL_SCALE: i8 = 0;

/// Maps a Databricks SQL type name to an Arrow DataType.
///
/// This function performs case-insensitive matching and handles parameterized
/// types like `DECIMAL(p,s)`, `VARCHAR(n)`, etc.
///
/// # Arguments
///
/// * `type_name` - The Databricks SQL type name (e.g., "BIGINT", "DECIMAL(10,2)")
///
/// # Returns
///
/// The corresponding Arrow DataType. Unknown types are mapped to `Utf8` as a fallback.
///
/// # Type Mappings
///
/// | Databricks Type | Arrow DataType |
/// |-----------------|----------------|
/// | BOOLEAN, BOOL | Boolean |
/// | TINYINT, BYTE | Int8 |
/// | SMALLINT, SHORT | Int16 |
/// | INT, INTEGER | Int32 |
/// | BIGINT, LONG | Int64 |
/// | FLOAT, REAL | Float32 |
/// | DOUBLE | Float64 |
/// | DECIMAL, DEC, NUMERIC | Decimal128(precision, scale) |
/// | STRING, VARCHAR, CHAR, TEXT | Utf8 |
/// | BINARY, VARBINARY | Binary |
/// | DATE | Date32 |
/// | TIMESTAMP, TIMESTAMP_NTZ | Timestamp(Microsecond, None) |
/// | TIMESTAMP_LTZ | Timestamp(Microsecond, Some("UTC")) |
/// | INTERVAL | Interval(DayTime) |
/// | ARRAY, MAP, STRUCT | Utf8 (JSON representation) |
/// | VOID, NULL | Null |
/// | Unknown | Utf8 |
///
/// # Example
///
/// ```
/// use databricks_adbc::metadata::type_mapping::databricks_type_to_arrow;
/// use arrow_schema::DataType;
///
/// assert_eq!(databricks_type_to_arrow("BIGINT"), DataType::Int64);
/// assert_eq!(databricks_type_to_arrow("bigint"), DataType::Int64); // case-insensitive
/// assert_eq!(databricks_type_to_arrow("DECIMAL(10,2)"), DataType::Decimal128(10, 2));
/// assert_eq!(databricks_type_to_arrow("UNKNOWN_TYPE"), DataType::Utf8); // fallback
/// ```
pub fn databricks_type_to_arrow(type_name: &str) -> DataType {
    let type_upper = type_name.to_uppercase();
    // Extract the base type (before any parentheses or angle brackets)
    // This handles both DECIMAL(10,2) and ARRAY<INT> style parameterization
    let base_type = type_upper
        .split(['(', '<'])
        .next()
        .unwrap_or(&type_upper)
        .trim();

    match base_type {
        // Boolean types
        "BOOLEAN" | "BOOL" => DataType::Boolean,

        // Integer types
        "TINYINT" | "BYTE" => DataType::Int8,
        "SMALLINT" | "SHORT" => DataType::Int16,
        "INT" | "INTEGER" => DataType::Int32,
        "BIGINT" | "LONG" => DataType::Int64,

        // Floating-point types
        "FLOAT" | "REAL" => DataType::Float32,
        "DOUBLE" => DataType::Float64,

        // Decimal types
        "DECIMAL" | "DEC" | "NUMERIC" => {
            let (precision, scale) = parse_decimal_params(type_name);
            DataType::Decimal128(precision, scale)
        }

        // String types
        "STRING" | "VARCHAR" | "CHAR" | "TEXT" => DataType::Utf8,

        // Binary types
        "BINARY" | "VARBINARY" => DataType::Binary,

        // Date/time types
        "DATE" => DataType::Date32,
        "TIMESTAMP" | "TIMESTAMP_NTZ" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "TIMESTAMP_LTZ" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "INTERVAL" => DataType::Interval(IntervalUnit::DayTime),

        // Complex types - represented as JSON strings
        "ARRAY" => DataType::Utf8,
        "MAP" => DataType::Utf8,
        "STRUCT" => DataType::Utf8,

        // Null type
        "VOID" | "NULL" => DataType::Null,

        // Unknown types default to Utf8
        _ => DataType::Utf8,
    }
}

/// Parses precision and scale from a DECIMAL type string.
///
/// This function extracts the precision and scale parameters from a DECIMAL
/// type specification like "DECIMAL(10,2)". If parameters are not specified
/// or cannot be parsed, defaults are used.
///
/// # Arguments
///
/// * `type_name` - The full type name (e.g., "DECIMAL(10,2)", "DECIMAL", "DEC(5)")
///
/// # Returns
///
/// A tuple of (precision, scale) where:
/// - `precision`: The maximum number of digits (default: 38)
/// - `scale`: The number of digits to the right of the decimal point (default: 0)
///
/// # Examples
///
/// ```
/// use databricks_adbc::metadata::type_mapping::parse_decimal_params;
///
/// // With explicit precision and scale
/// assert_eq!(parse_decimal_params("DECIMAL(10,2)"), (10, 2));
///
/// // With only precision (scale defaults to 0)
/// assert_eq!(parse_decimal_params("DECIMAL(10)"), (10, 0));
///
/// // Without parameters (both default)
/// assert_eq!(parse_decimal_params("DECIMAL"), (38, 0));
///
/// // Case-insensitive
/// assert_eq!(parse_decimal_params("decimal(15,5)"), (15, 5));
///
/// // With whitespace
/// assert_eq!(parse_decimal_params("DECIMAL( 10 , 2 )"), (10, 2));
/// ```
pub fn parse_decimal_params(type_name: &str) -> (u8, i8) {
    // Find the parameters within parentheses
    let start = type_name.find('(');
    let end = type_name.rfind(')');

    match (start, end) {
        (Some(start_idx), Some(end_idx)) if start_idx < end_idx => {
            let params = &type_name[start_idx + 1..end_idx];
            let parts: Vec<&str> = params.split(',').collect();

            let precision = parts
                .first()
                .and_then(|p| p.trim().parse::<u8>().ok())
                .unwrap_or(DEFAULT_DECIMAL_PRECISION);

            let scale = parts
                .get(1)
                .and_then(|s| s.trim().parse::<i8>().ok())
                .unwrap_or(DEFAULT_DECIMAL_SCALE);

            (precision, scale)
        }
        _ => (DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE),
    }
}

/// Maps a Databricks SQL type name to an XDBC/JDBC type code.
///
/// XDBC type codes are compatible with JDBC `java.sql.Types` constants.
/// These codes are used in the ADBC `get_objects()` method to populate
/// the `xdbc_data_type` and `xdbc_sql_data_type` fields.
///
/// # Arguments
///
/// * `type_name` - The Databricks SQL type name (e.g., "BIGINT", "VARCHAR")
///
/// # Returns
///
/// The corresponding JDBC type code. Unknown types return 12 (VARCHAR).
///
/// # Type Code Mappings
///
/// | Databricks Type | JDBC Code | JDBC Constant |
/// |-----------------|-----------|---------------|
/// | BOOLEAN, BOOL | -7 | BIT |
/// | TINYINT, BYTE | -6 | TINYINT |
/// | SMALLINT, SHORT | 5 | SMALLINT |
/// | INT, INTEGER | 4 | INTEGER |
/// | BIGINT, LONG | -5 | BIGINT |
/// | FLOAT, REAL | 6 | FLOAT |
/// | DOUBLE | 8 | DOUBLE |
/// | DECIMAL, DEC, NUMERIC | 3 | DECIMAL |
/// | STRING, TEXT | -1 | LONGVARCHAR |
/// | VARCHAR | 12 | VARCHAR |
/// | CHAR | 1 | CHAR |
/// | BINARY, VARBINARY | -3 | VARBINARY |
/// | DATE | 91 | DATE |
/// | TIMESTAMP* | 93 | TIMESTAMP |
/// | ARRAY | 2003 | ARRAY |
/// | MAP | 2000 | JAVA_OBJECT |
/// | STRUCT | 2002 | STRUCT |
///
/// # Example
///
/// ```
/// use databricks_adbc::metadata::type_mapping::databricks_type_to_xdbc;
///
/// assert_eq!(databricks_type_to_xdbc("BIGINT"), -5);
/// assert_eq!(databricks_type_to_xdbc("VARCHAR"), 12);
/// assert_eq!(databricks_type_to_xdbc("TIMESTAMP"), 93);
/// ```
pub fn databricks_type_to_xdbc(type_name: &str) -> i16 {
    let type_upper = type_name.to_uppercase();
    // Extract the base type (before any parentheses or angle brackets)
    // This handles both DECIMAL(10,2) and ARRAY<INT> style parameterization
    let base_type = type_upper
        .split(['(', '<'])
        .next()
        .unwrap_or(&type_upper)
        .trim();

    match base_type {
        // Boolean types
        "BOOLEAN" | "BOOL" => -7, // JDBC BIT

        // Integer types
        "TINYINT" | "BYTE" => -6,  // JDBC TINYINT
        "SMALLINT" | "SHORT" => 5, // JDBC SMALLINT
        "INT" | "INTEGER" => 4,    // JDBC INTEGER
        "BIGINT" | "LONG" => -5,   // JDBC BIGINT

        // Floating-point types
        "FLOAT" | "REAL" => 6, // JDBC FLOAT
        "DOUBLE" => 8,         // JDBC DOUBLE

        // Decimal types
        "DECIMAL" | "DEC" | "NUMERIC" => 3, // JDBC DECIMAL

        // String types - each has different JDBC code
        "STRING" | "TEXT" => -1, // JDBC LONGVARCHAR
        "VARCHAR" => 12,         // JDBC VARCHAR
        "CHAR" => 1,             // JDBC CHAR

        // Binary types
        "BINARY" | "VARBINARY" => -3, // JDBC VARBINARY

        // Date/time types
        "DATE" => 91,                                          // JDBC DATE
        "TIMESTAMP" | "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" => 93, // JDBC TIMESTAMP

        // Complex types
        "ARRAY" => 2003,  // JDBC ARRAY
        "MAP" => 2000,    // JDBC JAVA_OBJECT
        "STRUCT" => 2002, // JDBC STRUCT

        // Unknown types default to VARCHAR
        _ => 12, // JDBC VARCHAR
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // Tests for databricks_type_to_arrow
    // =========================================================================

    #[test]
    fn test_boolean_types_to_arrow() {
        assert_eq!(databricks_type_to_arrow("BOOLEAN"), DataType::Boolean);
        assert_eq!(databricks_type_to_arrow("BOOL"), DataType::Boolean);
        assert_eq!(databricks_type_to_arrow("boolean"), DataType::Boolean);
        assert_eq!(databricks_type_to_arrow("Bool"), DataType::Boolean);
    }

    #[test]
    fn test_integer_types_to_arrow() {
        // TINYINT
        assert_eq!(databricks_type_to_arrow("TINYINT"), DataType::Int8);
        assert_eq!(databricks_type_to_arrow("BYTE"), DataType::Int8);
        assert_eq!(databricks_type_to_arrow("tinyint"), DataType::Int8);

        // SMALLINT
        assert_eq!(databricks_type_to_arrow("SMALLINT"), DataType::Int16);
        assert_eq!(databricks_type_to_arrow("SHORT"), DataType::Int16);
        assert_eq!(databricks_type_to_arrow("smallint"), DataType::Int16);

        // INT
        assert_eq!(databricks_type_to_arrow("INT"), DataType::Int32);
        assert_eq!(databricks_type_to_arrow("INTEGER"), DataType::Int32);
        assert_eq!(databricks_type_to_arrow("int"), DataType::Int32);

        // BIGINT
        assert_eq!(databricks_type_to_arrow("BIGINT"), DataType::Int64);
        assert_eq!(databricks_type_to_arrow("LONG"), DataType::Int64);
        assert_eq!(databricks_type_to_arrow("bigint"), DataType::Int64);
    }

    #[test]
    fn test_floating_point_types_to_arrow() {
        assert_eq!(databricks_type_to_arrow("FLOAT"), DataType::Float32);
        assert_eq!(databricks_type_to_arrow("REAL"), DataType::Float32);
        assert_eq!(databricks_type_to_arrow("float"), DataType::Float32);

        assert_eq!(databricks_type_to_arrow("DOUBLE"), DataType::Float64);
        assert_eq!(databricks_type_to_arrow("double"), DataType::Float64);
    }

    #[test]
    fn test_decimal_types_to_arrow() {
        // Default precision and scale
        assert_eq!(
            databricks_type_to_arrow("DECIMAL"),
            DataType::Decimal128(38, 0)
        );
        assert_eq!(databricks_type_to_arrow("DEC"), DataType::Decimal128(38, 0));
        assert_eq!(
            databricks_type_to_arrow("NUMERIC"),
            DataType::Decimal128(38, 0)
        );

        // With explicit precision and scale
        assert_eq!(
            databricks_type_to_arrow("DECIMAL(10,2)"),
            DataType::Decimal128(10, 2)
        );
        assert_eq!(
            databricks_type_to_arrow("DECIMAL(18,6)"),
            DataType::Decimal128(18, 6)
        );
        assert_eq!(
            databricks_type_to_arrow("DEC(5,3)"),
            DataType::Decimal128(5, 3)
        );
        assert_eq!(
            databricks_type_to_arrow("NUMERIC(38,10)"),
            DataType::Decimal128(38, 10)
        );

        // With only precision (scale defaults to 0)
        assert_eq!(
            databricks_type_to_arrow("DECIMAL(10)"),
            DataType::Decimal128(10, 0)
        );

        // Case-insensitive
        assert_eq!(
            databricks_type_to_arrow("decimal(15,5)"),
            DataType::Decimal128(15, 5)
        );

        // With whitespace
        assert_eq!(
            databricks_type_to_arrow("DECIMAL( 10 , 2 )"),
            DataType::Decimal128(10, 2)
        );
    }

    #[test]
    fn test_string_types_to_arrow() {
        assert_eq!(databricks_type_to_arrow("STRING"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("VARCHAR"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("VARCHAR(255)"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("CHAR"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("CHAR(10)"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("TEXT"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("string"), DataType::Utf8);
    }

    #[test]
    fn test_binary_types_to_arrow() {
        assert_eq!(databricks_type_to_arrow("BINARY"), DataType::Binary);
        assert_eq!(databricks_type_to_arrow("VARBINARY"), DataType::Binary);
        assert_eq!(databricks_type_to_arrow("binary"), DataType::Binary);
    }

    #[test]
    fn test_date_types_to_arrow() {
        assert_eq!(databricks_type_to_arrow("DATE"), DataType::Date32);
        assert_eq!(databricks_type_to_arrow("date"), DataType::Date32);
    }

    #[test]
    fn test_timestamp_types_to_arrow() {
        // TIMESTAMP and TIMESTAMP_NTZ have no timezone
        assert_eq!(
            databricks_type_to_arrow("TIMESTAMP"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            databricks_type_to_arrow("TIMESTAMP_NTZ"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            databricks_type_to_arrow("timestamp"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );

        // TIMESTAMP_LTZ has UTC timezone
        assert_eq!(
            databricks_type_to_arrow("TIMESTAMP_LTZ"),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn test_interval_type_to_arrow() {
        assert_eq!(
            databricks_type_to_arrow("INTERVAL"),
            DataType::Interval(IntervalUnit::DayTime)
        );
    }

    #[test]
    fn test_complex_types_to_arrow() {
        // Complex types are represented as JSON strings (Utf8)
        assert_eq!(databricks_type_to_arrow("ARRAY"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("ARRAY<INT>"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("MAP"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("MAP<STRING,INT>"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("STRUCT"), DataType::Utf8);
        assert_eq!(
            databricks_type_to_arrow("STRUCT<id:INT,name:STRING>"),
            DataType::Utf8
        );
    }

    #[test]
    fn test_null_types_to_arrow() {
        assert_eq!(databricks_type_to_arrow("VOID"), DataType::Null);
        assert_eq!(databricks_type_to_arrow("NULL"), DataType::Null);
    }

    #[test]
    fn test_unknown_types_default_to_utf8() {
        assert_eq!(databricks_type_to_arrow("UNKNOWN"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("CUSTOM_TYPE"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow(""), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("SOME_NEW_TYPE"), DataType::Utf8);
    }

    #[test]
    fn test_type_to_arrow_case_insensitivity() {
        // Test various case combinations
        assert_eq!(
            databricks_type_to_arrow("BIGINT"),
            databricks_type_to_arrow("bigint")
        );
        assert_eq!(
            databricks_type_to_arrow("BIGINT"),
            databricks_type_to_arrow("BigInt")
        );
        assert_eq!(
            databricks_type_to_arrow("BIGINT"),
            databricks_type_to_arrow("BiGiNt")
        );
        assert_eq!(
            databricks_type_to_arrow("VARCHAR"),
            databricks_type_to_arrow("varchar")
        );
        assert_eq!(
            databricks_type_to_arrow("TIMESTAMP_LTZ"),
            databricks_type_to_arrow("timestamp_ltz")
        );
    }

    // =========================================================================
    // Tests for parse_decimal_params
    // =========================================================================

    #[test]
    fn test_parse_decimal_with_precision_and_scale() {
        assert_eq!(parse_decimal_params("DECIMAL(10,2)"), (10, 2));
        assert_eq!(parse_decimal_params("DECIMAL(18,6)"), (18, 6));
        assert_eq!(parse_decimal_params("DECIMAL(38,10)"), (38, 10));
        assert_eq!(parse_decimal_params("DECIMAL(1,0)"), (1, 0));
    }

    #[test]
    fn test_parse_decimal_with_only_precision() {
        // Scale defaults to 0 when not specified
        assert_eq!(parse_decimal_params("DECIMAL(10)"), (10, 0));
        assert_eq!(parse_decimal_params("DECIMAL(38)"), (38, 0));
    }

    #[test]
    fn test_parse_decimal_without_params() {
        // Both precision and scale default
        assert_eq!(parse_decimal_params("DECIMAL"), (38, 0));
        assert_eq!(parse_decimal_params("DEC"), (38, 0));
        assert_eq!(parse_decimal_params("NUMERIC"), (38, 0));
    }

    #[test]
    fn test_parse_decimal_case_insensitive() {
        assert_eq!(parse_decimal_params("decimal(15,5)"), (15, 5));
        assert_eq!(parse_decimal_params("Decimal(10,2)"), (10, 2));
    }

    #[test]
    fn test_parse_decimal_with_whitespace() {
        assert_eq!(parse_decimal_params("DECIMAL( 10 , 2 )"), (10, 2));
        assert_eq!(parse_decimal_params("DECIMAL(  10  ,  2  )"), (10, 2));
        assert_eq!(parse_decimal_params("DECIMAL(10 , 2)"), (10, 2));
        assert_eq!(parse_decimal_params("DECIMAL( 10 )"), (10, 0));
    }

    #[test]
    fn test_parse_decimal_with_negative_scale() {
        // Negative scale is technically valid for some DBs
        assert_eq!(parse_decimal_params("DECIMAL(10,-2)"), (10, -2));
    }

    #[test]
    fn test_parse_decimal_invalid_params() {
        // Invalid parameters should use defaults
        assert_eq!(parse_decimal_params("DECIMAL(abc,def)"), (38, 0));
        assert_eq!(parse_decimal_params("DECIMAL(,)"), (38, 0));
        assert_eq!(parse_decimal_params("DECIMAL()"), (38, 0));
    }

    #[test]
    fn test_parse_decimal_malformed() {
        // Malformed type strings should use defaults
        assert_eq!(parse_decimal_params("DECIMAL(10,2"), (38, 0)); // missing closing paren
        assert_eq!(parse_decimal_params("DECIMAL10,2)"), (38, 0)); // missing opening paren
        assert_eq!(parse_decimal_params("DECIMAL)("), (38, 0)); // wrong order
    }

    // =========================================================================
    // Tests for databricks_type_to_xdbc
    // =========================================================================

    #[test]
    fn test_boolean_types_to_xdbc() {
        assert_eq!(databricks_type_to_xdbc("BOOLEAN"), -7);
        assert_eq!(databricks_type_to_xdbc("BOOL"), -7);
        assert_eq!(databricks_type_to_xdbc("boolean"), -7);
    }

    #[test]
    fn test_integer_types_to_xdbc() {
        assert_eq!(databricks_type_to_xdbc("TINYINT"), -6);
        assert_eq!(databricks_type_to_xdbc("BYTE"), -6);

        assert_eq!(databricks_type_to_xdbc("SMALLINT"), 5);
        assert_eq!(databricks_type_to_xdbc("SHORT"), 5);

        assert_eq!(databricks_type_to_xdbc("INT"), 4);
        assert_eq!(databricks_type_to_xdbc("INTEGER"), 4);

        assert_eq!(databricks_type_to_xdbc("BIGINT"), -5);
        assert_eq!(databricks_type_to_xdbc("LONG"), -5);
    }

    #[test]
    fn test_floating_point_types_to_xdbc() {
        assert_eq!(databricks_type_to_xdbc("FLOAT"), 6);
        assert_eq!(databricks_type_to_xdbc("REAL"), 6);

        assert_eq!(databricks_type_to_xdbc("DOUBLE"), 8);
    }

    #[test]
    fn test_decimal_types_to_xdbc() {
        assert_eq!(databricks_type_to_xdbc("DECIMAL"), 3);
        assert_eq!(databricks_type_to_xdbc("DEC"), 3);
        assert_eq!(databricks_type_to_xdbc("NUMERIC"), 3);
        assert_eq!(databricks_type_to_xdbc("DECIMAL(10,2)"), 3);
    }

    #[test]
    fn test_string_types_to_xdbc() {
        // STRING and TEXT map to LONGVARCHAR
        assert_eq!(databricks_type_to_xdbc("STRING"), -1);
        assert_eq!(databricks_type_to_xdbc("TEXT"), -1);

        // VARCHAR maps to VARCHAR
        assert_eq!(databricks_type_to_xdbc("VARCHAR"), 12);
        assert_eq!(databricks_type_to_xdbc("VARCHAR(255)"), 12);

        // CHAR maps to CHAR
        assert_eq!(databricks_type_to_xdbc("CHAR"), 1);
        assert_eq!(databricks_type_to_xdbc("CHAR(10)"), 1);
    }

    #[test]
    fn test_binary_types_to_xdbc() {
        assert_eq!(databricks_type_to_xdbc("BINARY"), -3);
        assert_eq!(databricks_type_to_xdbc("VARBINARY"), -3);
    }

    #[test]
    fn test_date_types_to_xdbc() {
        assert_eq!(databricks_type_to_xdbc("DATE"), 91);
    }

    #[test]
    fn test_timestamp_types_to_xdbc() {
        assert_eq!(databricks_type_to_xdbc("TIMESTAMP"), 93);
        assert_eq!(databricks_type_to_xdbc("TIMESTAMP_NTZ"), 93);
        assert_eq!(databricks_type_to_xdbc("TIMESTAMP_LTZ"), 93);
    }

    #[test]
    fn test_complex_types_to_xdbc() {
        assert_eq!(databricks_type_to_xdbc("ARRAY"), 2003);
        assert_eq!(databricks_type_to_xdbc("ARRAY<INT>"), 2003);
        assert_eq!(databricks_type_to_xdbc("MAP"), 2000);
        assert_eq!(databricks_type_to_xdbc("MAP<STRING,INT>"), 2000);
        assert_eq!(databricks_type_to_xdbc("STRUCT"), 2002);
        assert_eq!(databricks_type_to_xdbc("STRUCT<id:INT>"), 2002);
    }

    #[test]
    fn test_unknown_types_default_to_varchar_xdbc() {
        assert_eq!(databricks_type_to_xdbc("UNKNOWN"), 12);
        assert_eq!(databricks_type_to_xdbc("CUSTOM_TYPE"), 12);
        assert_eq!(databricks_type_to_xdbc(""), 12);
    }

    #[test]
    fn test_type_to_xdbc_case_insensitivity() {
        assert_eq!(
            databricks_type_to_xdbc("BIGINT"),
            databricks_type_to_xdbc("bigint")
        );
        assert_eq!(
            databricks_type_to_xdbc("VARCHAR"),
            databricks_type_to_xdbc("varchar")
        );
        assert_eq!(
            databricks_type_to_xdbc("TIMESTAMP_LTZ"),
            databricks_type_to_xdbc("timestamp_ltz")
        );
    }

    // =========================================================================
    // Integration tests - Arrow and XDBC consistency
    // =========================================================================

    #[test]
    fn test_all_primitive_types_have_both_mappings() {
        // Verify that all common types have both Arrow and XDBC mappings
        let types = [
            "BOOLEAN",
            "TINYINT",
            "SMALLINT",
            "INT",
            "BIGINT",
            "FLOAT",
            "DOUBLE",
            "DECIMAL",
            "STRING",
            "VARCHAR",
            "CHAR",
            "BINARY",
            "DATE",
            "TIMESTAMP",
            "TIMESTAMP_NTZ",
            "TIMESTAMP_LTZ",
        ];

        for type_name in types {
            // Arrow mapping should not panic
            let _arrow = databricks_type_to_arrow(type_name);
            // XDBC mapping should not panic
            let _xdbc = databricks_type_to_xdbc(type_name);
        }
    }

    #[test]
    fn test_decimal_variations() {
        // Test various DECIMAL formats
        let variations = [
            "DECIMAL",
            "DECIMAL(10)",
            "DECIMAL(10,2)",
            "DECIMAL(38,18)",
            "DEC",
            "DEC(5,3)",
            "NUMERIC",
            "NUMERIC(15,5)",
        ];

        for type_name in variations {
            let arrow = databricks_type_to_arrow(type_name);
            assert!(matches!(arrow, DataType::Decimal128(_, _)));

            let xdbc = databricks_type_to_xdbc(type_name);
            assert_eq!(xdbc, 3); // JDBC DECIMAL
        }
    }
}
