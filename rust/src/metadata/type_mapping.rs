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

//! Databricks type → Arrow/XDBC type mapping.
//!
//! Maps Databricks SQL type names (from `SHOW COLUMNS` results) to
//! Arrow `DataType` and XDBC/JDBC type codes.

use arrow_schema::{DataType, IntervalUnit, TimeUnit};

/// Map Databricks SQL type name to Arrow DataType.
pub fn databricks_type_to_arrow(type_name: &str) -> DataType {
    let type_upper = type_name.to_uppercase();
    let base_type = type_upper.split('(').next().unwrap_or(&type_upper).trim();

    match base_type {
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "TINYINT" | "BYTE" => DataType::Int8,
        "SMALLINT" | "SHORT" => DataType::Int16,
        "INT" | "INTEGER" => DataType::Int32,
        "BIGINT" | "LONG" => DataType::Int64,
        "FLOAT" | "REAL" => DataType::Float32,
        "DOUBLE" => DataType::Float64,
        "DECIMAL" | "DEC" | "NUMERIC" => {
            let (precision, scale) = parse_decimal_params(type_name);
            DataType::Decimal128(precision, scale)
        }
        "STRING" | "VARCHAR" | "CHAR" | "TEXT" => DataType::Utf8,
        "BINARY" | "VARBINARY" => DataType::Binary,
        "DATE" => DataType::Date32,
        "TIMESTAMP" | "TIMESTAMP_NTZ" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "TIMESTAMP_LTZ" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "INTERVAL" => DataType::Interval(IntervalUnit::DayTime),
        "ARRAY" => DataType::Utf8,  // Represented as JSON string
        "MAP" => DataType::Utf8,    // Represented as JSON string
        "STRUCT" => DataType::Utf8, // Represented as JSON string
        "VOID" | "NULL" => DataType::Null,
        _ => DataType::Utf8, // Default fallback
    }
}

/// Map Databricks type to XDBC/JDBC type code.
pub fn databricks_type_to_xdbc(type_name: &str) -> i16 {
    let type_upper = type_name.to_uppercase();
    let base_type = type_upper.split('(').next().unwrap_or(&type_upper).trim();

    match base_type {
        "BOOLEAN" | "BOOL" => -7,                              // JDBC BIT
        "TINYINT" | "BYTE" => -6,                              // JDBC TINYINT
        "SMALLINT" | "SHORT" => 5,                             // JDBC SMALLINT
        "INT" | "INTEGER" => 4,                                // JDBC INTEGER
        "BIGINT" | "LONG" => -5,                               // JDBC BIGINT
        "FLOAT" | "REAL" => 6,                                 // JDBC FLOAT
        "DOUBLE" => 8,                                         // JDBC DOUBLE
        "DECIMAL" | "DEC" | "NUMERIC" => 3,                    // JDBC DECIMAL
        "STRING" | "TEXT" => -1,                               // JDBC LONGVARCHAR
        "VARCHAR" => 12,                                       // JDBC VARCHAR
        "CHAR" => 1,                                           // JDBC CHAR
        "BINARY" | "VARBINARY" => -3,                          // JDBC VARBINARY
        "DATE" => 91,                                          // JDBC DATE
        "TIMESTAMP" | "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" => 93, // JDBC TIMESTAMP
        "ARRAY" => 2003,                                       // JDBC ARRAY
        "MAP" => 2000,                                         // JDBC JAVA_OBJECT
        "STRUCT" => 2002,                                      // JDBC STRUCT
        _ => 12,                                               // Default to VARCHAR
    }
}

/// Parse precision and scale from DECIMAL(p,s) type string.
/// Defaults to DECIMAL(38,18) if not specified (Databricks default).
fn parse_decimal_params(type_name: &str) -> (u8, i8) {
    let default_precision = 38u8;
    let default_scale = 18i8;

    let Some(start) = type_name.find('(') else {
        return (default_precision, default_scale);
    };
    let Some(end) = type_name.find(')') else {
        return (default_precision, default_scale);
    };

    let params = &type_name[start + 1..end];
    let parts: Vec<&str> = params.split(',').map(|s| s.trim()).collect();

    let precision = parts
        .first()
        .and_then(|p| p.parse::<u8>().ok())
        .unwrap_or(default_precision);
    let scale = parts
        .get(1)
        .and_then(|s| s.parse::<i8>().ok())
        .unwrap_or(default_scale);

    (precision, scale)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_databricks_type_to_arrow_primitives() {
        assert_eq!(databricks_type_to_arrow("BOOLEAN"), DataType::Boolean);
        assert_eq!(databricks_type_to_arrow("BOOL"), DataType::Boolean);
        assert_eq!(databricks_type_to_arrow("TINYINT"), DataType::Int8);
        assert_eq!(databricks_type_to_arrow("BYTE"), DataType::Int8);
        assert_eq!(databricks_type_to_arrow("SMALLINT"), DataType::Int16);
        assert_eq!(databricks_type_to_arrow("SHORT"), DataType::Int16);
        assert_eq!(databricks_type_to_arrow("INT"), DataType::Int32);
        assert_eq!(databricks_type_to_arrow("INTEGER"), DataType::Int32);
        assert_eq!(databricks_type_to_arrow("BIGINT"), DataType::Int64);
        assert_eq!(databricks_type_to_arrow("LONG"), DataType::Int64);
        assert_eq!(databricks_type_to_arrow("FLOAT"), DataType::Float32);
        assert_eq!(databricks_type_to_arrow("REAL"), DataType::Float32);
        assert_eq!(databricks_type_to_arrow("DOUBLE"), DataType::Float64);
        assert_eq!(databricks_type_to_arrow("STRING"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("VARCHAR"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("CHAR"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("BINARY"), DataType::Binary);
        assert_eq!(databricks_type_to_arrow("DATE"), DataType::Date32);
    }

    #[test]
    fn test_databricks_type_to_arrow_timestamps() {
        assert_eq!(
            databricks_type_to_arrow("TIMESTAMP"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            databricks_type_to_arrow("TIMESTAMP_NTZ"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            databricks_type_to_arrow("TIMESTAMP_LTZ"),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn test_databricks_type_to_arrow_decimal() {
        assert_eq!(
            databricks_type_to_arrow("DECIMAL"),
            DataType::Decimal128(38, 18)
        );
        assert_eq!(
            databricks_type_to_arrow("DECIMAL(10,2)"),
            DataType::Decimal128(10, 2)
        );
        assert_eq!(
            databricks_type_to_arrow("DECIMAL(38, 0)"),
            DataType::Decimal128(38, 0)
        );
        assert_eq!(
            databricks_type_to_arrow("NUMERIC(5,3)"),
            DataType::Decimal128(5, 3)
        );
    }

    #[test]
    fn test_databricks_type_to_arrow_complex() {
        // Complex types represented as JSON strings
        assert_eq!(databricks_type_to_arrow("ARRAY"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("MAP"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("STRUCT"), DataType::Utf8);
    }

    #[test]
    fn test_databricks_type_to_arrow_special() {
        assert_eq!(databricks_type_to_arrow("VOID"), DataType::Null);
        assert_eq!(databricks_type_to_arrow("NULL"), DataType::Null);
        assert_eq!(
            databricks_type_to_arrow("INTERVAL"),
            DataType::Interval(IntervalUnit::DayTime)
        );
    }

    #[test]
    fn test_databricks_type_to_arrow_unknown() {
        assert_eq!(databricks_type_to_arrow("UNKNOWN_TYPE"), DataType::Utf8);
    }

    #[test]
    fn test_databricks_type_to_arrow_case_insensitive() {
        assert_eq!(databricks_type_to_arrow("boolean"), DataType::Boolean);
        assert_eq!(databricks_type_to_arrow("Int"), DataType::Int32);
        assert_eq!(databricks_type_to_arrow("String"), DataType::Utf8);
    }

    #[test]
    fn test_databricks_type_to_xdbc() {
        assert_eq!(databricks_type_to_xdbc("BOOLEAN"), -7);
        assert_eq!(databricks_type_to_xdbc("TINYINT"), -6);
        assert_eq!(databricks_type_to_xdbc("SMALLINT"), 5);
        assert_eq!(databricks_type_to_xdbc("INT"), 4);
        assert_eq!(databricks_type_to_xdbc("BIGINT"), -5);
        assert_eq!(databricks_type_to_xdbc("FLOAT"), 6);
        assert_eq!(databricks_type_to_xdbc("DOUBLE"), 8);
        assert_eq!(databricks_type_to_xdbc("DECIMAL"), 3);
        assert_eq!(databricks_type_to_xdbc("STRING"), -1);
        assert_eq!(databricks_type_to_xdbc("VARCHAR"), 12);
        assert_eq!(databricks_type_to_xdbc("CHAR"), 1);
        assert_eq!(databricks_type_to_xdbc("BINARY"), -3);
        assert_eq!(databricks_type_to_xdbc("DATE"), 91);
        assert_eq!(databricks_type_to_xdbc("TIMESTAMP"), 93);
        assert_eq!(databricks_type_to_xdbc("ARRAY"), 2003);
        assert_eq!(databricks_type_to_xdbc("MAP"), 2000);
        assert_eq!(databricks_type_to_xdbc("STRUCT"), 2002);
    }

    #[test]
    fn test_databricks_type_to_xdbc_with_params() {
        assert_eq!(databricks_type_to_xdbc("DECIMAL(10,2)"), 3);
        assert_eq!(databricks_type_to_xdbc("VARCHAR(255)"), 12);
    }

    #[test]
    fn test_parse_decimal_params() {
        assert_eq!(parse_decimal_params("DECIMAL(10,2)"), (10, 2));
        assert_eq!(parse_decimal_params("DECIMAL(38, 0)"), (38, 0));
        assert_eq!(parse_decimal_params("DECIMAL"), (38, 18));
        assert_eq!(parse_decimal_params("NUMERIC(5,3)"), (5, 3));
    }
}
