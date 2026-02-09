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

//! Databricks type mapping to Arrow and XDBC types.
//!
//! Provides functions to map Databricks SQL type names (e.g., "STRING",
//! "DECIMAL(10,2)") to Arrow `DataType` and XDBC/JDBC type codes.
//!
//! Type mappings mirror the JDBC driver's `MetadataResultConstants.java`.

use arrow_schema::{DataType, TimeUnit};

/// Default precision for DECIMAL when not specified.
const DEFAULT_DECIMAL_PRECISION: u8 = 38;

/// Default scale for DECIMAL when not specified.
const DEFAULT_DECIMAL_SCALE: i8 = 18;

/// Parse precision and scale from a DECIMAL type string like "DECIMAL(10,2)".
///
/// Returns `(precision, scale)` with defaults of (38, 18) when parameters
/// are not specified or cannot be parsed.
///
/// # Examples
///
/// ```ignore
/// assert_eq!(parse_decimal_params("DECIMAL(10,2)"), (10, 2));
/// assert_eq!(parse_decimal_params("DECIMAL"), (38, 18));
/// assert_eq!(parse_decimal_params("NUMERIC(5)"), (5, 18));
/// ```
pub fn parse_decimal_params(type_name: &str) -> (u8, i8) {
    let open = match type_name.find('(') {
        Some(i) => i,
        None => return (DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE),
    };
    let close = match type_name.find(')') {
        Some(i) => i,
        None => return (DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE),
    };

    let inner = &type_name[open + 1..close];
    let parts: Vec<&str> = inner.split(',').collect();

    let precision = parts
        .first()
        .and_then(|s| s.trim().parse::<u8>().ok())
        .unwrap_or(DEFAULT_DECIMAL_PRECISION);

    let scale = parts
        .get(1)
        .and_then(|s| s.trim().parse::<i8>().ok())
        .unwrap_or(DEFAULT_DECIMAL_SCALE);

    (precision, scale)
}

/// Map a Databricks SQL type name to an Arrow `DataType`.
///
/// The base type is extracted by splitting on `(` and trimming, so
/// parameterized types like `DECIMAL(10,2)` or `VARCHAR(255)` are handled.
///
/// Complex types (`ARRAY`, `MAP`, `STRUCT`) are represented as `Utf8`
/// (JSON string representation), matching the JDBC driver behavior.
pub fn databricks_type_to_arrow(type_name: &str) -> DataType {
    let type_upper = type_name.to_uppercase();
    let base_type = type_upper
        .split('(')
        .next()
        .unwrap_or(&type_upper)
        .trim();

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
        "STRING" | "VARCHAR" | "CHAR" => DataType::Utf8,
        "BINARY" => DataType::Binary,
        "DATE" => DataType::Date32,
        "TIMESTAMP" | "TIMESTAMP_NTZ" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "TIMESTAMP_LTZ" => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        "ARRAY" | "MAP" | "STRUCT" => DataType::Utf8,
        "VOID" | "NULL" => DataType::Null,
        _ => DataType::Utf8,
    }
}

/// XDBC/JDBC type code constants.
///
/// These match `java.sql.Types` values used by the JDBC driver.
mod xdbc_type_codes {
    pub const BIT: i16 = -7;
    pub const TINYINT: i16 = -6;
    pub const SMALLINT: i16 = 5;
    pub const INTEGER: i16 = 4;
    pub const BIGINT: i16 = -5;
    pub const FLOAT: i16 = 6;
    pub const DOUBLE: i16 = 8;
    pub const DECIMAL: i16 = 3;
    pub const CHAR: i16 = 1;
    pub const VARCHAR: i16 = 12;
    pub const LONGVARCHAR: i16 = -1;
    pub const VARBINARY: i16 = -3;
    pub const DATE: i16 = 91;
    pub const TIMESTAMP: i16 = 93;
    pub const JAVA_OBJECT: i16 = 2000;
    pub const STRUCT: i16 = 2002;
    pub const ARRAY: i16 = 2003;
}

/// Map a Databricks SQL type name to an XDBC/JDBC type code.
///
/// The base type is extracted by splitting on `(` and trimming, so
/// parameterized types like `DECIMAL(10,2)` are handled.
///
/// Returns JDBC type codes matching `java.sql.Types` constants.
pub fn databricks_type_to_xdbc(type_name: &str) -> i16 {
    let type_upper = type_name.to_uppercase();
    let base_type = type_upper
        .split('(')
        .next()
        .unwrap_or(&type_upper)
        .trim();

    match base_type {
        "BOOLEAN" | "BOOL" => xdbc_type_codes::BIT,
        "TINYINT" | "BYTE" => xdbc_type_codes::TINYINT,
        "SMALLINT" | "SHORT" => xdbc_type_codes::SMALLINT,
        "INT" | "INTEGER" => xdbc_type_codes::INTEGER,
        "BIGINT" | "LONG" => xdbc_type_codes::BIGINT,
        "FLOAT" | "REAL" => xdbc_type_codes::FLOAT,
        "DOUBLE" => xdbc_type_codes::DOUBLE,
        "DECIMAL" | "DEC" | "NUMERIC" => xdbc_type_codes::DECIMAL,
        "STRING" | "TEXT" => xdbc_type_codes::LONGVARCHAR,
        "VARCHAR" => xdbc_type_codes::VARCHAR,
        "CHAR" => xdbc_type_codes::CHAR,
        "BINARY" | "VARBINARY" => xdbc_type_codes::VARBINARY,
        "DATE" => xdbc_type_codes::DATE,
        "TIMESTAMP" | "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" => xdbc_type_codes::TIMESTAMP,
        "ARRAY" => xdbc_type_codes::ARRAY,
        "MAP" => xdbc_type_codes::JAVA_OBJECT,
        "STRUCT" => xdbc_type_codes::STRUCT,
        _ => xdbc_type_codes::VARCHAR,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_decimal_params() {
        // Standard precision and scale
        assert_eq!(parse_decimal_params("DECIMAL(10,2)"), (10, 2));
        assert_eq!(parse_decimal_params("DECIMAL(38,18)"), (38, 18));
        assert_eq!(parse_decimal_params("NUMERIC(5,0)"), (5, 0));
        assert_eq!(parse_decimal_params("DEC(18,6)"), (18, 6));

        // With spaces
        assert_eq!(parse_decimal_params("DECIMAL( 10 , 2 )"), (10, 2));

        // No parameters → defaults
        assert_eq!(
            parse_decimal_params("DECIMAL"),
            (DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE)
        );

        // Precision only → default scale
        assert_eq!(parse_decimal_params("DECIMAL(10)"), (10, DEFAULT_DECIMAL_SCALE));

        // Case insensitive input (parse_decimal_params works on the raw string)
        assert_eq!(parse_decimal_params("decimal(10,2)"), (10, 2));

        // Malformed → defaults
        assert_eq!(
            parse_decimal_params("DECIMAL(abc)"),
            (DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE)
        );
        assert_eq!(
            parse_decimal_params("DECIMAL()"),
            (DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE)
        );
    }

    #[test]
    fn test_type_mapping_databricks_to_arrow() {
        // Boolean types
        assert_eq!(databricks_type_to_arrow("BOOLEAN"), DataType::Boolean);
        assert_eq!(databricks_type_to_arrow("boolean"), DataType::Boolean);
        assert_eq!(databricks_type_to_arrow("BOOL"), DataType::Boolean);

        // Integer types
        assert_eq!(databricks_type_to_arrow("TINYINT"), DataType::Int8);
        assert_eq!(databricks_type_to_arrow("BYTE"), DataType::Int8);
        assert_eq!(databricks_type_to_arrow("SMALLINT"), DataType::Int16);
        assert_eq!(databricks_type_to_arrow("SHORT"), DataType::Int16);
        assert_eq!(databricks_type_to_arrow("INT"), DataType::Int32);
        assert_eq!(databricks_type_to_arrow("INTEGER"), DataType::Int32);
        assert_eq!(databricks_type_to_arrow("BIGINT"), DataType::Int64);
        assert_eq!(databricks_type_to_arrow("LONG"), DataType::Int64);

        // Floating-point types
        assert_eq!(databricks_type_to_arrow("FLOAT"), DataType::Float32);
        assert_eq!(databricks_type_to_arrow("REAL"), DataType::Float32);
        assert_eq!(databricks_type_to_arrow("DOUBLE"), DataType::Float64);

        // Decimal types
        assert_eq!(
            databricks_type_to_arrow("DECIMAL(10,2)"),
            DataType::Decimal128(10, 2)
        );
        assert_eq!(
            databricks_type_to_arrow("DECIMAL"),
            DataType::Decimal128(DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE)
        );
        assert_eq!(
            databricks_type_to_arrow("NUMERIC(5,0)"),
            DataType::Decimal128(5, 0)
        );
        assert_eq!(
            databricks_type_to_arrow("DEC(18,6)"),
            DataType::Decimal128(18, 6)
        );

        // String types
        assert_eq!(databricks_type_to_arrow("STRING"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("VARCHAR"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("VARCHAR(255)"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("CHAR"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("CHAR(10)"), DataType::Utf8);

        // Binary type
        assert_eq!(databricks_type_to_arrow("BINARY"), DataType::Binary);

        // Date/time types
        assert_eq!(databricks_type_to_arrow("DATE"), DataType::Date32);
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

        // Complex types → Utf8 (JSON representation)
        assert_eq!(databricks_type_to_arrow("ARRAY"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("MAP"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("STRUCT"), DataType::Utf8);

        // Null types
        assert_eq!(databricks_type_to_arrow("VOID"), DataType::Null);
        assert_eq!(databricks_type_to_arrow("NULL"), DataType::Null);

        // Unknown type → Utf8 fallback
        assert_eq!(databricks_type_to_arrow("UNKNOWN_TYPE"), DataType::Utf8);
    }

    #[test]
    fn test_type_mapping_databricks_to_xdbc() {
        // Boolean
        assert_eq!(databricks_type_to_xdbc("BOOLEAN"), xdbc_type_codes::BIT);
        assert_eq!(databricks_type_to_xdbc("boolean"), xdbc_type_codes::BIT);
        assert_eq!(databricks_type_to_xdbc("BOOL"), xdbc_type_codes::BIT);

        // Integer types
        assert_eq!(databricks_type_to_xdbc("TINYINT"), xdbc_type_codes::TINYINT);
        assert_eq!(databricks_type_to_xdbc("BYTE"), xdbc_type_codes::TINYINT);
        assert_eq!(databricks_type_to_xdbc("SMALLINT"), xdbc_type_codes::SMALLINT);
        assert_eq!(databricks_type_to_xdbc("SHORT"), xdbc_type_codes::SMALLINT);
        assert_eq!(databricks_type_to_xdbc("INT"), xdbc_type_codes::INTEGER);
        assert_eq!(databricks_type_to_xdbc("INTEGER"), xdbc_type_codes::INTEGER);
        assert_eq!(databricks_type_to_xdbc("BIGINT"), xdbc_type_codes::BIGINT);
        assert_eq!(databricks_type_to_xdbc("LONG"), xdbc_type_codes::BIGINT);

        // Floating-point types
        assert_eq!(databricks_type_to_xdbc("FLOAT"), xdbc_type_codes::FLOAT);
        assert_eq!(databricks_type_to_xdbc("REAL"), xdbc_type_codes::FLOAT);
        assert_eq!(databricks_type_to_xdbc("DOUBLE"), xdbc_type_codes::DOUBLE);

        // Decimal types
        assert_eq!(databricks_type_to_xdbc("DECIMAL"), xdbc_type_codes::DECIMAL);
        assert_eq!(
            databricks_type_to_xdbc("DECIMAL(10,2)"),
            xdbc_type_codes::DECIMAL
        );
        assert_eq!(databricks_type_to_xdbc("NUMERIC"), xdbc_type_codes::DECIMAL);
        assert_eq!(databricks_type_to_xdbc("DEC"), xdbc_type_codes::DECIMAL);

        // String types
        assert_eq!(databricks_type_to_xdbc("STRING"), xdbc_type_codes::LONGVARCHAR);
        assert_eq!(databricks_type_to_xdbc("TEXT"), xdbc_type_codes::LONGVARCHAR);
        assert_eq!(databricks_type_to_xdbc("VARCHAR"), xdbc_type_codes::VARCHAR);
        assert_eq!(databricks_type_to_xdbc("VARCHAR(255)"), xdbc_type_codes::VARCHAR);
        assert_eq!(databricks_type_to_xdbc("CHAR"), xdbc_type_codes::CHAR);
        assert_eq!(databricks_type_to_xdbc("CHAR(10)"), xdbc_type_codes::CHAR);

        // Binary type
        assert_eq!(databricks_type_to_xdbc("BINARY"), xdbc_type_codes::VARBINARY);
        assert_eq!(databricks_type_to_xdbc("VARBINARY"), xdbc_type_codes::VARBINARY);

        // Date/time types
        assert_eq!(databricks_type_to_xdbc("DATE"), xdbc_type_codes::DATE);
        assert_eq!(databricks_type_to_xdbc("TIMESTAMP"), xdbc_type_codes::TIMESTAMP);
        assert_eq!(
            databricks_type_to_xdbc("TIMESTAMP_NTZ"),
            xdbc_type_codes::TIMESTAMP
        );
        assert_eq!(
            databricks_type_to_xdbc("TIMESTAMP_LTZ"),
            xdbc_type_codes::TIMESTAMP
        );

        // Complex types
        assert_eq!(databricks_type_to_xdbc("ARRAY"), xdbc_type_codes::ARRAY);
        assert_eq!(databricks_type_to_xdbc("MAP"), xdbc_type_codes::JAVA_OBJECT);
        assert_eq!(databricks_type_to_xdbc("STRUCT"), xdbc_type_codes::STRUCT);

        // Unknown type → VARCHAR fallback
        assert_eq!(databricks_type_to_xdbc("UNKNOWN_TYPE"), xdbc_type_codes::VARCHAR);
    }

    #[test]
    fn test_type_mapping_case_insensitive() {
        // Arrow mapping is case-insensitive
        assert_eq!(databricks_type_to_arrow("int"), DataType::Int32);
        assert_eq!(databricks_type_to_arrow("Int"), DataType::Int32);
        assert_eq!(databricks_type_to_arrow("INT"), DataType::Int32);
        assert_eq!(databricks_type_to_arrow("string"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("String"), DataType::Utf8);

        // XDBC mapping is case-insensitive
        assert_eq!(databricks_type_to_xdbc("int"), xdbc_type_codes::INTEGER);
        assert_eq!(databricks_type_to_xdbc("Int"), xdbc_type_codes::INTEGER);
        assert_eq!(databricks_type_to_xdbc("string"), xdbc_type_codes::LONGVARCHAR);
    }

    #[test]
    fn test_type_mapping_parameterized_types() {
        // Parameterized types should extract base type correctly
        assert_eq!(databricks_type_to_arrow("VARCHAR(100)"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("CHAR(50)"), DataType::Utf8);
        assert_eq!(
            databricks_type_to_arrow("DECIMAL(10,2)"),
            DataType::Decimal128(10, 2)
        );

        assert_eq!(databricks_type_to_xdbc("VARCHAR(100)"), xdbc_type_codes::VARCHAR);
        assert_eq!(databricks_type_to_xdbc("CHAR(50)"), xdbc_type_codes::CHAR);
        assert_eq!(
            databricks_type_to_xdbc("DECIMAL(10,2)"),
            xdbc_type_codes::DECIMAL
        );
    }
}
