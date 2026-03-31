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

//! Parameter handling utilities for prepared statements.
//!
//! This module provides:
//! - SQL parameter marker (`?`) parsing and transformation
//! - Arrow RecordBatch to SEA statement parameter conversion
//! - Databricks type name to/from Arrow DataType mapping
//! - `DESCRIBE QUERY` result parsing for `execute_schema()`

use crate::error::{DatabricksErrorHelper, Result};
use crate::types::sea::StatementParameter;
use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use driverbase::error::ErrorHelper;

// ---------------------------------------------------------------------------
// SQL parameter marker parsing
// ---------------------------------------------------------------------------

/// States for the SQL parser state machine.
#[derive(Clone, Copy, PartialEq)]
enum SqlParserState {
    Normal,
    InSingleQuote,
    InLineComment,
    InBlockComment,
}

/// Walk a SQL string with a state machine, calling `on_marker` for each `?`
/// found in normal context (outside string literals and comments).
fn walk_sql(sql: &str, mut on_marker: impl FnMut(usize)) {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    let mut state = SqlParserState::Normal;

    while i < len {
        match state {
            SqlParserState::Normal => {
                if bytes[i] == b'?' {
                    on_marker(i);
                    i += 1;
                } else if bytes[i] == b'\'' {
                    state = SqlParserState::InSingleQuote;
                    i += 1;
                } else if i + 1 < len && bytes[i] == b'-' && bytes[i + 1] == b'-' {
                    state = SqlParserState::InLineComment;
                    i += 2;
                } else if i + 1 < len && bytes[i] == b'/' && bytes[i + 1] == b'*' {
                    state = SqlParserState::InBlockComment;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            SqlParserState::InSingleQuote => {
                if i + 1 < len && bytes[i] == b'\'' && bytes[i + 1] == b'\'' {
                    // Escaped single quote ('')
                    i += 2;
                } else if bytes[i] == b'\'' {
                    state = SqlParserState::Normal;
                    i += 1;
                } else {
                    i += 1;
                }
            }
            SqlParserState::InLineComment => {
                if bytes[i] == b'\n' {
                    state = SqlParserState::Normal;
                }
                i += 1;
            }
            SqlParserState::InBlockComment => {
                if i + 1 < len && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                    state = SqlParserState::Normal;
                    i += 2;
                } else {
                    i += 1;
                }
            }
        }
    }
}

/// Count parameter markers (`?`) in a SQL string, ignoring those inside
/// string literals (`'...'`) and comments (`--`, `/* */`).
pub fn count_parameter_markers(sql: &str) -> usize {
    let mut count = 0;
    walk_sql(sql, |_| count += 1);
    count
}

/// Replace parameter markers (`?`) with string literals (`'?'`) for use
/// with `DESCRIBE QUERY`. Markers inside string literals and comments are
/// left untouched.
///
/// Reference: JDBC's `DatabricksPreparedStatement.surroundPlaceholdersWithQuotes()`.
pub fn replace_parameter_markers_with_literals(sql: &str) -> String {
    // Collect marker positions
    let mut positions = Vec::new();
    walk_sql(sql, |pos| positions.push(pos));

    if positions.is_empty() {
        return sql.to_string();
    }

    // Build result string, replacing each `?` at marker positions with `'?'`
    let mut result = String::with_capacity(sql.len() + positions.len() * 2);
    let mut last = 0;
    for pos in positions {
        result.push_str(&sql[last..pos]);
        result.push_str("'?'");
        last = pos + 1;
    }
    result.push_str(&sql[last..]);
    result
}

// ---------------------------------------------------------------------------
// Arrow → SEA parameter conversion
// ---------------------------------------------------------------------------

/// Convert a single-row Arrow RecordBatch into SEA statement parameters.
///
/// Each column in the batch becomes one positional parameter. Arrow types are
/// mapped to Databricks SQL type names and values are serialized to strings.
/// Null values are sent with `value: None` (JSON null).
pub fn record_batch_to_sea_parameters(batch: &RecordBatch) -> Result<Vec<StatementParameter>> {
    if batch.num_rows() != 1 {
        return Err(DatabricksErrorHelper::invalid_argument().message(format!(
            "bind() expects a single-row RecordBatch, got {} rows",
            batch.num_rows()
        )));
    }

    let mut params = Vec::with_capacity(batch.num_columns());
    for (i, column) in batch.columns().iter().enumerate() {
        let param_type = arrow_type_to_databricks_type(column.data_type());
        let value = if column.is_null(0) {
            None
        } else {
            Some(arrow_value_to_string(column, 0)?)
        };

        params.push(StatementParameter {
            ordinal: (i + 1) as i32,
            param_type,
            value,
        });
    }

    Ok(params)
}

/// Map an Arrow DataType to a Databricks SQL type name.
pub fn arrow_type_to_databricks_type(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::UInt8 => "SMALLINT".to_string(),
        DataType::UInt16 => "INT".to_string(),
        DataType::UInt32 => "BIGINT".to_string(),
        DataType::UInt64 => "BIGINT".to_string(),
        DataType::Float16 => "FLOAT".to_string(),
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Decimal128(p, s) => format!("DECIMAL({p},{s})"),
        DataType::Utf8 | DataType::LargeUtf8 => "STRING".to_string(),
        DataType::Binary | DataType::LargeBinary => "BINARY".to_string(),
        DataType::Date32 => "DATE".to_string(),
        DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(_, _) => "TIMESTAMP".to_string(),
        DataType::Null => "STRING".to_string(),
        // Fallback: use STRING for unsupported types
        _ => "STRING".to_string(),
    }
}

/// Extract a scalar value from an Arrow array at the given index as a string.
fn arrow_value_to_string(array: &dyn Array, index: usize) -> Result<String> {
    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_boolean();
            Ok(arr.value(index).to_string())
        }
        DataType::Int8 => Ok(array.as_primitive::<Int8Type>().value(index).to_string()),
        DataType::Int16 => Ok(array.as_primitive::<Int16Type>().value(index).to_string()),
        DataType::Int32 => Ok(array.as_primitive::<Int32Type>().value(index).to_string()),
        DataType::Int64 => Ok(array.as_primitive::<Int64Type>().value(index).to_string()),
        DataType::UInt8 => Ok(array.as_primitive::<UInt8Type>().value(index).to_string()),
        DataType::UInt16 => Ok(array.as_primitive::<UInt16Type>().value(index).to_string()),
        DataType::UInt32 => Ok(array.as_primitive::<UInt32Type>().value(index).to_string()),
        DataType::UInt64 => Ok(array.as_primitive::<UInt64Type>().value(index).to_string()),
        DataType::Float16 => Ok(array
            .as_primitive::<Float16Type>()
            .value(index)
            .to_f64()
            .to_string()),
        DataType::Float32 => Ok(array.as_primitive::<Float32Type>().value(index).to_string()),
        DataType::Float64 => Ok(array.as_primitive::<Float64Type>().value(index).to_string()),
        DataType::Decimal128(_, scale) => {
            let arr = array.as_primitive::<Decimal128Type>();
            let raw = arr.value(index);
            Ok(format_decimal128(raw, *scale))
        }
        DataType::Utf8 => Ok(array.as_string::<i32>().value(index).to_string()),
        DataType::LargeUtf8 => Ok(array.as_string::<i64>().value(index).to_string()),
        DataType::Binary => {
            let arr = array.as_binary::<i32>();
            Ok(hex::encode(arr.value(index)))
        }
        DataType::LargeBinary => {
            let arr = array.as_binary::<i64>();
            Ok(hex::encode(arr.value(index)))
        }
        DataType::Date32 => {
            let arr = array.as_primitive::<Date32Type>();
            let days = arr.value(index);
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163)
                .unwrap_or(chrono::NaiveDate::MIN);
            Ok(date.format("%Y-%m-%d").to_string())
        }
        DataType::Date64 => {
            let arr = array.as_primitive::<Date64Type>();
            let ms = arr.value(index);
            let date = chrono::DateTime::from_timestamp_millis(ms)
                .map(|dt| dt.date_naive())
                .unwrap_or(chrono::NaiveDate::MIN);
            Ok(date.format("%Y-%m-%d").to_string())
        }
        DataType::Timestamp(unit, _tz) => {
            let ts = match unit {
                TimeUnit::Second => {
                    let arr = array.as_primitive::<TimestampSecondType>();
                    chrono::DateTime::from_timestamp(arr.value(index), 0)
                }
                TimeUnit::Millisecond => {
                    let arr = array.as_primitive::<TimestampMillisecondType>();
                    chrono::DateTime::from_timestamp_millis(arr.value(index))
                }
                TimeUnit::Microsecond => {
                    let arr = array.as_primitive::<TimestampMicrosecondType>();
                    chrono::DateTime::from_timestamp_micros(arr.value(index))
                }
                TimeUnit::Nanosecond => {
                    let arr = array.as_primitive::<TimestampNanosecondType>();
                    let nanos = arr.value(index);
                    chrono::DateTime::from_timestamp(
                        nanos.div_euclid(1_000_000_000),
                        nanos.rem_euclid(1_000_000_000) as u32,
                    )
                }
            };
            let dt = ts.unwrap_or(chrono::DateTime::UNIX_EPOCH);
            Ok(dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string())
        }
        _ => {
            // Fallback: use Debug representation
            Err(DatabricksErrorHelper::invalid_argument().message(format!(
                "Unsupported Arrow type for parameter binding: {:?}",
                array.data_type()
            )))
        }
    }
}

/// Format a Decimal128 raw value with the given scale.
fn format_decimal128(raw: i128, scale: i8) -> String {
    if scale <= 0 {
        return raw.to_string();
    }
    let scale = scale as u32;
    let divisor = 10_i128.pow(scale);
    let sign = if raw < 0 { "-" } else { "" };
    let abs_raw = raw.unsigned_abs();
    let whole = abs_raw / divisor as u128;
    let frac = abs_raw % divisor as u128;
    format!("{sign}{whole}.{frac:0>width$}", width = scale as usize)
}

// ---------------------------------------------------------------------------
// DESCRIBE QUERY result → Arrow Schema
// ---------------------------------------------------------------------------

/// Convert a `DESCRIBE QUERY` result into an Arrow Schema.
///
/// `DESCRIBE QUERY` returns rows with columns `col_name` and `data_type`.
/// Each row becomes a field in the output schema.
pub fn describe_result_to_schema(
    reader: impl arrow_array::RecordBatchReader,
) -> crate::error::Result<Schema> {
    let mut fields = Vec::new();

    for batch_result in reader {
        let batch = batch_result.map_err(|e| {
            DatabricksErrorHelper::io().message(format!("Error reading DESCRIBE QUERY result: {e}"))
        })?;

        // DESCRIBE QUERY returns at least 2 columns: col_name, data_type
        if batch.num_columns() < 2 {
            return Err(DatabricksErrorHelper::io().message(format!(
                "DESCRIBE QUERY returned {} columns, expected at least 2",
                batch.num_columns()
            )));
        }

        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| {
                DatabricksErrorHelper::io()
                    .message("DESCRIBE QUERY col_name column is not a StringArray")
            })?;

        let types = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| {
                DatabricksErrorHelper::io()
                    .message("DESCRIBE QUERY data_type column is not a StringArray")
            })?;

        for row in 0..batch.num_rows() {
            let col_name = names.value(row);
            let data_type_str = types.value(row);
            let arrow_type = databricks_type_to_arrow_type(data_type_str);
            fields.push(Field::new(col_name, arrow_type, true));
        }
    }

    Ok(Schema::new(fields))
}

/// Map a Databricks SQL type name to an Arrow DataType.
///
/// Handles types like `INT`, `STRING`, `DECIMAL(10,2)`, `ARRAY<INT>`, etc.
pub fn databricks_type_to_arrow_type(type_name: &str) -> DataType {
    let upper = type_name.trim().to_uppercase();

    match upper.as_str() {
        "BOOLEAN" => DataType::Boolean,
        "TINYINT" | "BYTE" => DataType::Int8,
        "SMALLINT" | "SHORT" => DataType::Int16,
        "INT" | "INTEGER" => DataType::Int32,
        "BIGINT" | "LONG" => DataType::Int64,
        "FLOAT" | "REAL" => DataType::Float32,
        "DOUBLE" => DataType::Float64,
        "STRING" | "VARCHAR" | "CHAR" => DataType::Utf8,
        "BINARY" => DataType::Binary,
        "DATE" => DataType::Date32,
        "TIMESTAMP" | "TIMESTAMP_NTZ" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "VOID" | "NULL" => DataType::Null,
        _ => {
            // Check for parameterized types like DECIMAL(p,s)
            if let Some(inner) = upper.strip_prefix("DECIMAL") {
                if let Some((p, s)) = parse_decimal_params(inner) {
                    return DataType::Decimal128(p, s);
                }
                // Plain DECIMAL without params
                return DataType::Decimal128(38, 18);
            }
            // Complex types (ARRAY<...>, MAP<...>, STRUCT<...>) → opaque Utf8
            if upper.starts_with("ARRAY") || upper.starts_with("MAP") || upper.starts_with("STRUCT")
            {
                return DataType::Utf8;
            }
            // Unknown type → Utf8 fallback
            DataType::Utf8
        }
    }
}

/// Parse `(precision, scale)` from a string like `(10,2)`.
fn parse_decimal_params(s: &str) -> Option<(u8, i8)> {
    let s = s.trim();
    let s = s.strip_prefix('(')?;
    let s = s.strip_suffix(')')?;
    let mut parts = s.split(',');
    let p: u8 = parts.next()?.trim().parse().ok()?;
    let s: i8 = parts.next()?.trim().parse().ok()?;
    Some((p, s))
}

/// Encode bytes as a hex string. Avoids adding a `hex` crate dependency.
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        let mut s = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            s.push_str(&format!("{b:02x}"));
        }
        s
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BooleanArray, Float64Array, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    // --- SQL parser tests ---

    #[test]
    fn test_count_simple() {
        assert_eq!(count_parameter_markers("SELECT * FROM t WHERE id = ?"), 1);
    }

    #[test]
    fn test_count_multiple() {
        assert_eq!(count_parameter_markers("INSERT INTO t VALUES (?, ?, ?)"), 3);
    }

    #[test]
    fn test_count_in_string_literal() {
        assert_eq!(count_parameter_markers("SELECT '?' FROM t"), 0);
    }

    #[test]
    fn test_count_in_line_comment() {
        assert_eq!(
            count_parameter_markers("SELECT -- ? comment\n * FROM t WHERE id = ?"),
            1
        );
    }

    #[test]
    fn test_count_in_block_comment() {
        assert_eq!(
            count_parameter_markers("SELECT /* ? */ * FROM t WHERE id = ?"),
            1
        );
    }

    #[test]
    fn test_count_escaped_quote() {
        assert_eq!(
            count_parameter_markers("SELECT * FROM t WHERE name = '?''s value' AND id = ?"),
            1
        );
    }

    #[test]
    fn test_count_no_markers() {
        assert_eq!(count_parameter_markers("SELECT 1"), 0);
    }

    #[test]
    fn test_count_empty() {
        assert_eq!(count_parameter_markers(""), 0);
    }

    // --- replace markers tests ---

    #[test]
    fn test_replace_simple() {
        assert_eq!(
            replace_parameter_markers_with_literals("SELECT * FROM t WHERE id = ?"),
            "SELECT * FROM t WHERE id = '?'"
        );
    }

    #[test]
    fn test_replace_multiple() {
        assert_eq!(
            replace_parameter_markers_with_literals("SELECT * FROM t WHERE a = ? AND b = ?"),
            "SELECT * FROM t WHERE a = '?' AND b = '?'"
        );
    }

    #[test]
    fn test_replace_in_string_literal_untouched() {
        assert_eq!(
            replace_parameter_markers_with_literals("SELECT '?' FROM t WHERE id = ?"),
            "SELECT '?' FROM t WHERE id = '?'"
        );
    }

    #[test]
    fn test_replace_no_markers() {
        assert_eq!(
            replace_parameter_markers_with_literals("SELECT 1"),
            "SELECT 1"
        );
    }

    // --- Arrow → SEA parameter conversion tests ---

    fn make_single_row_batch(fields: Vec<(&str, Arc<dyn Array>)>) -> RecordBatch {
        let schema_fields: Vec<Field> = fields
            .iter()
            .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(schema_fields));
        let arrays: Vec<Arc<dyn Array>> = fields.into_iter().map(|(_, arr)| arr).collect();
        RecordBatch::try_new(schema, arrays).unwrap()
    }

    #[test]
    fn test_convert_int32() {
        let batch = make_single_row_batch(vec![(
            "p1",
            Arc::new(Int32Array::from(vec![42])) as Arc<dyn Array>,
        )]);
        let params = record_batch_to_sea_parameters(&batch).unwrap();
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].ordinal, 1);
        assert_eq!(params[0].param_type, "INT");
        assert_eq!(params[0].value, Some("42".to_string()));
    }

    #[test]
    fn test_convert_string() {
        let batch = make_single_row_batch(vec![(
            "p1",
            Arc::new(StringArray::from(vec!["hello"])) as Arc<dyn Array>,
        )]);
        let params = record_batch_to_sea_parameters(&batch).unwrap();
        assert_eq!(params[0].param_type, "STRING");
        assert_eq!(params[0].value, Some("hello".to_string()));
    }

    #[test]
    fn test_convert_null_value() {
        let batch = make_single_row_batch(vec![(
            "p1",
            Arc::new(StringArray::from(vec![None::<&str>])) as Arc<dyn Array>,
        )]);
        let params = record_batch_to_sea_parameters(&batch).unwrap();
        assert_eq!(params[0].param_type, "STRING");
        assert_eq!(params[0].value, None);
    }

    #[test]
    fn test_convert_multi_column() {
        let batch = make_single_row_batch(vec![
            (
                "p1",
                Arc::new(Int32Array::from(vec![100])) as Arc<dyn Array>,
            ),
            (
                "p2",
                Arc::new(StringArray::from(vec!["alice"])) as Arc<dyn Array>,
            ),
            (
                "p3",
                Arc::new(Float64Array::from(vec![3.25])) as Arc<dyn Array>,
            ),
        ]);
        let params = record_batch_to_sea_parameters(&batch).unwrap();
        assert_eq!(params.len(), 3);
        assert_eq!(params[0].ordinal, 1);
        assert_eq!(params[0].param_type, "INT");
        assert_eq!(params[1].ordinal, 2);
        assert_eq!(params[1].param_type, "STRING");
        assert_eq!(params[2].ordinal, 3);
        assert_eq!(params[2].param_type, "DOUBLE");
    }

    #[test]
    fn test_convert_multi_row_error() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)])),
            vec![Arc::new(Int32Array::from(vec![1, 2])) as Arc<dyn Array>],
        )
        .unwrap();
        assert!(record_batch_to_sea_parameters(&batch).is_err());
    }

    #[test]
    fn test_convert_boolean() {
        let batch = make_single_row_batch(vec![(
            "p1",
            Arc::new(BooleanArray::from(vec![true])) as Arc<dyn Array>,
        )]);
        let params = record_batch_to_sea_parameters(&batch).unwrap();
        assert_eq!(params[0].param_type, "BOOLEAN");
        assert_eq!(params[0].value, Some("true".to_string()));
    }

    #[test]
    fn test_convert_decimal128() {
        let arr = arrow_array::Decimal128Array::from(vec![12345])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let batch = make_single_row_batch(vec![("p1", Arc::new(arr) as Arc<dyn Array>)]);
        let params = record_batch_to_sea_parameters(&batch).unwrap();
        assert_eq!(params[0].param_type, "DECIMAL(10,2)");
        assert_eq!(params[0].value, Some("123.45".to_string()));
    }

    // --- Type mapping tests ---

    #[test]
    fn test_arrow_to_databricks_types() {
        assert_eq!(arrow_type_to_databricks_type(&DataType::Boolean), "BOOLEAN");
        assert_eq!(arrow_type_to_databricks_type(&DataType::Int8), "TINYINT");
        assert_eq!(arrow_type_to_databricks_type(&DataType::Int16), "SMALLINT");
        assert_eq!(arrow_type_to_databricks_type(&DataType::Int32), "INT");
        assert_eq!(arrow_type_to_databricks_type(&DataType::Int64), "BIGINT");
        assert_eq!(arrow_type_to_databricks_type(&DataType::Float32), "FLOAT");
        assert_eq!(arrow_type_to_databricks_type(&DataType::Float64), "DOUBLE");
        assert_eq!(arrow_type_to_databricks_type(&DataType::Utf8), "STRING");
        assert_eq!(arrow_type_to_databricks_type(&DataType::Binary), "BINARY");
        assert_eq!(arrow_type_to_databricks_type(&DataType::Date32), "DATE");
        assert_eq!(
            arrow_type_to_databricks_type(&DataType::Decimal128(10, 2)),
            "DECIMAL(10,2)"
        );
    }

    #[test]
    fn test_databricks_to_arrow_types() {
        assert_eq!(databricks_type_to_arrow_type("BOOLEAN"), DataType::Boolean);
        assert_eq!(databricks_type_to_arrow_type("TINYINT"), DataType::Int8);
        assert_eq!(databricks_type_to_arrow_type("SMALLINT"), DataType::Int16);
        assert_eq!(databricks_type_to_arrow_type("INT"), DataType::Int32);
        assert_eq!(databricks_type_to_arrow_type("INTEGER"), DataType::Int32);
        assert_eq!(databricks_type_to_arrow_type("BIGINT"), DataType::Int64);
        assert_eq!(databricks_type_to_arrow_type("FLOAT"), DataType::Float32);
        assert_eq!(databricks_type_to_arrow_type("DOUBLE"), DataType::Float64);
        assert_eq!(databricks_type_to_arrow_type("STRING"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow_type("BINARY"), DataType::Binary);
        assert_eq!(databricks_type_to_arrow_type("DATE"), DataType::Date32);
        assert_eq!(
            databricks_type_to_arrow_type("TIMESTAMP"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            databricks_type_to_arrow_type("TIMESTAMP_NTZ"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            databricks_type_to_arrow_type("DECIMAL(10,2)"),
            DataType::Decimal128(10, 2)
        );
        assert_eq!(
            databricks_type_to_arrow_type("DECIMAL"),
            DataType::Decimal128(38, 18)
        );
        // Complex types → Utf8
        assert_eq!(databricks_type_to_arrow_type("ARRAY<INT>"), DataType::Utf8);
        assert_eq!(
            databricks_type_to_arrow_type("MAP<STRING,INT>"),
            DataType::Utf8
        );
        assert_eq!(
            databricks_type_to_arrow_type("STRUCT<a:INT,b:STRING>"),
            DataType::Utf8
        );
    }

    // --- Decimal formatting tests ---

    #[test]
    fn test_format_decimal128() {
        assert_eq!(format_decimal128(12345, 2), "123.45");
        assert_eq!(format_decimal128(100, 2), "1.00");
        assert_eq!(format_decimal128(5, 3), "0.005");
        assert_eq!(format_decimal128(-12345, 2), "-123.45");
        assert_eq!(format_decimal128(-5, 2), "-0.05");
        assert_eq!(format_decimal128(-1, 3), "-0.001");
        assert_eq!(format_decimal128(42, 0), "42");
        assert_eq!(format_decimal128(0, 2), "0.00");
    }

    // --- DESCRIBE QUERY result parsing tests ---

    #[test]
    fn test_describe_result_to_schema() {
        let names = StringArray::from(vec!["id", "name", "amount"]);
        let types = StringArray::from(vec!["INT", "STRING", "DECIMAL(10,2)"]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(names) as Arc<dyn Array>, Arc::new(types)],
        )
        .unwrap();

        let reader = arrow_array::RecordBatchIterator::new(
            vec![Ok(batch)],
            Arc::new(Schema::new(vec![
                Field::new("col_name", DataType::Utf8, false),
                Field::new("data_type", DataType::Utf8, false),
            ])),
        );

        let result_schema = describe_result_to_schema(reader).unwrap();
        assert_eq!(result_schema.fields().len(), 3);
        assert_eq!(result_schema.field(0).name(), "id");
        assert_eq!(result_schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(result_schema.field(1).name(), "name");
        assert_eq!(result_schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(result_schema.field(2).name(), "amount");
        assert_eq!(
            result_schema.field(2).data_type(),
            &DataType::Decimal128(10, 2)
        );
    }

    // --- StatementParameter serialization test ---

    #[test]
    fn test_statement_parameter_serialization() {
        let params = vec![
            StatementParameter {
                ordinal: 1,
                param_type: "INT".to_string(),
                value: Some("100".to_string()),
            },
            StatementParameter {
                ordinal: 2,
                param_type: "STRING".to_string(),
                value: None,
            },
        ];

        let json = serde_json::to_string(&params).unwrap();
        assert!(json.contains(r#""ordinal":1"#));
        assert!(json.contains(r#""type":"INT""#));
        assert!(json.contains(r#""value":"100""#));
        assert!(json.contains(r#""value":null"#));
    }
}
