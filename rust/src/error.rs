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
}
