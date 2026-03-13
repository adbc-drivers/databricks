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

//! Error types for the Databricks ADBC driver.
//!
//! This module uses the driverbase error framework to provide consistent,
//! informative error messages that integrate with the ADBC error model.

use driverbase::error::ErrorHelper;

/// Error helper for Databricks driver errors.
///
/// This type implements the driverbase `ErrorHelper` trait to provide
/// consistent error formatting with the driver name prefix.
#[derive(Clone)]
pub struct DatabricksErrorHelper;

impl ErrorHelper for DatabricksErrorHelper {
    const NAME: &'static str = "Databricks";
}

/// The error type for Databricks ADBC driver operations.
pub type Error = driverbase::error::Error<DatabricksErrorHelper>;

/// A convenient alias for Results with Databricks errors.
pub type Result<T> = std::result::Result<T, Error>;

/// Extract SQLSTATE from error message text.
///
/// Many Databricks error messages include "SQLSTATE: XXXXX" in the text.
/// This function extracts the 5-character SQLSTATE code if present.
pub fn extract_sqlstate_from_message(message: &str) -> Option<[std::os::raw::c_char; 5]> {
    // Look for pattern "SQLSTATE: XXXXX" or "SQLSTATE:XXXXX"
    let sqlstate_pattern = "SQLSTATE:";
    if let Some(start_idx) = message.find(sqlstate_pattern) {
        let after_pattern = &message[start_idx + sqlstate_pattern.len()..];
        // Skip optional whitespace
        let trimmed = after_pattern.trim_start();
        // Extract exactly 5 characters
        if trimmed.len() >= 5 {
            let sqlstate_str = &trimmed[..5];
            // Verify it looks like a SQLSTATE (alphanumeric)
            if sqlstate_str.chars().all(|c| c.is_ascii_alphanumeric()) {
                return sqlstate_str_to_array(sqlstate_str);
            }
        }
    }
    None
}

/// Convert a 5-character SQLSTATE string to a c_char array.
pub fn sqlstate_str_to_array(sqlstate_str: &str) -> Option<[std::os::raw::c_char; 5]> {
    let bytes = sqlstate_str.as_bytes();
    if bytes.len() != 5 {
        return None;
    }
    Some([
        bytes[0] as std::os::raw::c_char,
        bytes[1] as std::os::raw::c_char,
        bytes[2] as std::os::raw::c_char,
        bytes[3] as std::os::raw::c_char,
        bytes[4] as std::os::raw::c_char,
    ])
}

/// Map Databricks server error codes to ANSI SQL SQLSTATE codes.
///
/// This function converts Databricks-specific error codes (e.g., PARSE_SYNTAX_ERROR,
/// TABLE_OR_VIEW_NOT_FOUND) into standardized 5-character SQLSTATE codes as defined
/// by the SQL standard and ODBC specification.
///
/// Returns a 5-byte array suitable for the ADBC error sqlstate field, or `None`
/// if the error code is not recognized.
pub fn map_error_code_to_sqlstate(error_code: &str) -> Option<[std::os::raw::c_char; 5]> {
    let sqlstate_str = match error_code {
        // Syntax errors - SQLSTATE 42601
        "PARSE_SYNTAX_ERROR" => "42601",

        // Table/view not found - SQLSTATE 42S02
        "TABLE_OR_VIEW_NOT_FOUND" => "42S02",

        // Column not found - SQLSTATE 42S22
        "COLUMN_NOT_FOUND" | "UNRESOLVED_COLUMN" => "42S22",

        // Division by zero - SQLSTATE 22012
        "DIVIDE_BY_ZERO" => "22012",

        // Invalid argument/data - SQLSTATE 22023
        "INVALID_PARAMETER_VALUE" | "INVALID_ARGUMENT" => "22023",

        // Data type mismatch - SQLSTATE 42804
        "DATATYPE_MISMATCH" => "42804",

        // Duplicate key - SQLSTATE 23000
        "DUPLICATE_KEY" => "23000",

        // Access denied - SQLSTATE 42000
        "PERMISSION_DENIED" | "ACCESS_DENIED" => "42000",

        // Numeric value out of range - SQLSTATE 22003
        "NUMERIC_VALUE_OUT_OF_RANGE" | "ARITHMETIC_OVERFLOW" => "22003",

        // Unknown/unmapped error codes return None
        _ => return None,
    };

    sqlstate_str_to_array(sqlstate_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let error = DatabricksErrorHelper::invalid_argument().message("invalid host URL");
        let display = format!("{error}");
        assert!(display.contains("Databricks"));
        assert!(display.contains("invalid host URL"));
    }

    #[test]
    fn test_error_with_context() {
        let error = DatabricksErrorHelper::io()
            .message("connection refused")
            .context("connect to server");
        let display = format!("{error}");
        assert!(display.contains("could not connect to server"));
        assert!(display.contains("connection refused"));
    }

    #[test]
    fn test_error_to_adbc() {
        let error = DatabricksErrorHelper::not_implemented().message("bulk ingest");
        let adbc_error = error.to_adbc();
        assert_eq!(adbc_error.status, adbc_core::error::Status::NotImplemented);
        assert!(adbc_error.message.contains("Databricks"));
    }

    #[test]
    fn test_map_error_code_to_sqlstate() {
        use super::map_error_code_to_sqlstate;

        // Test syntax error mapping
        let sqlstate = map_error_code_to_sqlstate("PARSE_SYNTAX_ERROR").unwrap();
        assert_eq!(
            std::str::from_utf8(unsafe {
                std::slice::from_raw_parts(sqlstate.as_ptr() as *const u8, 5)
            })
            .unwrap(),
            "42601"
        );

        // Test table not found mapping
        let sqlstate = map_error_code_to_sqlstate("TABLE_OR_VIEW_NOT_FOUND").unwrap();
        assert_eq!(
            std::str::from_utf8(unsafe {
                std::slice::from_raw_parts(sqlstate.as_ptr() as *const u8, 5)
            })
            .unwrap(),
            "42S02"
        );

        // Test column not found mapping
        let sqlstate = map_error_code_to_sqlstate("COLUMN_NOT_FOUND").unwrap();
        assert_eq!(
            std::str::from_utf8(unsafe {
                std::slice::from_raw_parts(sqlstate.as_ptr() as *const u8, 5)
            })
            .unwrap(),
            "42S22"
        );

        // Test unmapped error code returns None
        assert!(map_error_code_to_sqlstate("UNKNOWN_ERROR_CODE").is_none());
    }

    #[test]
    fn test_error_with_sqlstate() {
        use super::map_error_code_to_sqlstate;

        let sqlstate = map_error_code_to_sqlstate("PARSE_SYNTAX_ERROR").unwrap();
        let error = DatabricksErrorHelper::invalid_argument()
            .message("syntax error near 'SELECT'")
            .sqlstate(sqlstate);

        let adbc_error = error.to_adbc();
        assert_eq!(
            adbc_error.status,
            adbc_core::error::Status::InvalidArguments
        );
        assert!(adbc_error.message.contains("syntax error"));

        // Verify SQLSTATE is set correctly
        let sqlstate_str = std::str::from_utf8(unsafe {
            std::slice::from_raw_parts(adbc_error.sqlstate.as_ptr() as *const u8, 5)
        })
        .unwrap();
        assert_eq!(sqlstate_str, "42601");
    }

    #[test]
    fn test_extract_sqlstate_from_message() {
        use super::extract_sqlstate_from_message;

        // Test extraction with space after colon
        let msg = "Error: Something went wrong. SQLSTATE: 42601 (line 1, pos 26)";
        let sqlstate = extract_sqlstate_from_message(msg).unwrap();
        let sqlstate_str = std::str::from_utf8(unsafe {
            std::slice::from_raw_parts(sqlstate.as_ptr() as *const u8, 5)
        })
        .unwrap();
        assert_eq!(sqlstate_str, "42601");

        // Test extraction without space after colon
        let msg2 = "Error message. SQLSTATE:42S02 some more text";
        let sqlstate2 = extract_sqlstate_from_message(msg2).unwrap();
        let sqlstate_str2 = std::str::from_utf8(unsafe {
            std::slice::from_raw_parts(sqlstate2.as_ptr() as *const u8, 5)
        })
        .unwrap();
        assert_eq!(sqlstate_str2, "42S02");

        // Test message without SQLSTATE
        let msg3 = "Error without sqlstate";
        assert!(extract_sqlstate_from_message(msg3).is_none());
    }
}
