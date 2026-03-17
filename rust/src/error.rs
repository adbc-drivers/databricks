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
