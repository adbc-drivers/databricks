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

//! Databricks ADBC Driver for Rust
//!
//! This crate provides an ADBC (Arrow Database Connectivity) driver for
//! connecting to Databricks SQL endpoints.
//!
//! ## Overview
//!
//! The driver implements the standard ADBC traits from `adbc_core`:
//! - [`Driver`] - Entry point for creating database connections
//! - [`Database`] - Holds connection configuration
//! - [`Connection`] - Active connection to Databricks
//! - [`Statement`] - SQL statement execution
//!
//! ## Features
//!
//! - **CloudFetch**: High-performance result streaming via cloud storage
//! - **SEA (Statement Execution API)**: REST-based query execution
//! - **Arrow IPC**: Native Arrow data format with optional LZ4 compression
//!
//! ## Example
//!
//! ```ignore
//! use databricks_adbc::Driver;
//! use adbc_core::driver::Driver as _;
//! use adbc_core::options::{OptionDatabase, OptionValue};
//! use adbc_core::Optionable;
//!
//! let driver = Driver::new();
//! let mut database = driver.new_database()?;
//! database.set_option(OptionDatabase::Uri, OptionValue::String("https://my-workspace.databricks.com".into()))?;
//! database.set_option(OptionDatabase::Other("databricks.http_path".into()), OptionValue::String("/sql/1.0/warehouses/abc123".into()))?;
//! database.set_option(OptionDatabase::Other("databricks.access_token".into()), OptionValue::String("dapi...".into()))?;
//!
//! let mut connection = database.new_connection()?;
//! let mut statement = connection.new_statement()?;
//! statement.set_sql_query("SELECT * FROM my_table")?;
//! let result = statement.execute()?;
//! ```
//!
//! ## Configuration Options
//!
//! ### Database Options
//!
//! | Option | Description |
//! |--------|-------------|
//! | `uri` | Databricks workspace URL |
//! | `databricks.http_path` | SQL warehouse HTTP path (extracts warehouse_id) |
//! | `databricks.warehouse_id` | SQL warehouse ID directly |
//! | `databricks.access_token` | Personal access token |
//! | `databricks.catalog` | Default catalog |
//! | `databricks.schema` | Default schema |
//! | `databricks.log_level` | Log level: `off`, `error`, `warn`, `info`, `debug`, `trace` |
//! | `databricks.log_file` | Log file path. If unset, logs go to stderr |
//!
//! ### CloudFetch Options
//!
//! | Option | Default | Description |
//! |--------|---------|-------------|
//! | `databricks.cloudfetch.enabled` | true | Enable CloudFetch |
//! | `databricks.cloudfetch.link_prefetch_window` | 10 | Links to prefetch |
//! | `databricks.cloudfetch.max_chunks_in_memory` | 4 | Memory limit (controls parallelism) |
//! | `databricks.cloudfetch.max_retries` | 5 | Download retry attempts |
//! | `databricks.cloudfetch.retry_delay_ms` | 1500 | Retry delay in ms |
//! | `databricks.cloudfetch.speed_threshold_mbps` | 0.1 | Slow download warning threshold |

pub mod auth;
pub mod client;
pub mod connection;
pub mod database;
pub mod driver;
pub mod error;
pub(crate) mod logging;
pub mod metadata;
pub mod reader;
pub mod result;
pub mod statement;
pub mod telemetry;
pub mod types;

// Re-export main types
pub use connection::Connection;
pub use database::Database;
pub use driver::Driver;
pub use error::{DatabricksErrorHelper, Error, Result};
pub use statement::Statement;

// Re-export client types for advanced users
pub use client::{DatabricksClient, DatabricksHttpClient, HttpClientConfig, SeaClient};

// Re-export configuration types
pub use types::cloudfetch::CloudFetchConfig;

// Metadata FFI — additional extern "C" functions for catalog metadata
// when built with `cargo build --features metadata-ffi`
#[cfg(feature = "metadata-ffi")]
pub(crate) mod ffi;

// FFI export — produces AdbcDatabricksInit and AdbcDriverInit symbols
// when built with `cargo build --features ffi`
//
// We provide our own init instead of using `adbc_ffi::export_driver!` because
// the macro hard-rejects ADBC 1.0.0 callers. DuckDB's adbc_scanner (as of v1.5)
// allocates a 1.0.0-sized AdbcDriver struct; writing the full 1.1.0 struct into
// that buffer causes a heap overflow. Our init accepts both versions and only
// writes the fields the caller's buffer can hold.
#[cfg(feature = "ffi")]
mod ffi_init {
    use adbc_ffi::FFIDriver;
    use std::os::raw::{c_int, c_void};

    /// Number of pointer-sized fields in the ADBC 1.0.0 AdbcDriver struct.
    /// Fields: private_data, private_manager, release, then 26 function pointers
    /// (DatabaseInit..StatementSetSubstraitPlan).
    const ADBC_DRIVER_1_0_0_FIELDS: usize = 29;
    const ADBC_DRIVER_1_0_0_SIZE: usize = ADBC_DRIVER_1_0_0_FIELDS * std::mem::size_of::<usize>();

    // ADBC version constants (from the C header)
    const ADBC_VERSION_1_0_0: c_int = 1_000_000;
    const ADBC_VERSION_1_1_0: c_int = 1_001_000;

    unsafe fn driver_init(
        version: c_int,
        driver: *mut c_void,
        error: *mut adbc_ffi::FFI_AdbcError,
    ) -> adbc_core::error::AdbcStatusCode {
        if driver.is_null() {
            return adbc_core::constants::ADBC_STATUS_INVALID_ARGUMENT;
        }

        let ffi_driver = <crate::driver::Driver as FFIDriver>::ffi_driver();

        match version {
            ADBC_VERSION_1_1_0 => {
                // Caller expects full 1.1.0 struct — write everything.
                unsafe {
                    std::ptr::write_unaligned(
                        driver as *mut adbc_ffi::FFI_AdbcDriver,
                        ffi_driver,
                    );
                }
            }
            ADBC_VERSION_1_0_0 => {
                // Caller allocated a 1.0.0-sized buffer — only copy the first
                // 29 fields so we don't overflow.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        &ffi_driver as *const adbc_ffi::FFI_AdbcDriver as *const u8,
                        driver as *mut u8,
                        ADBC_DRIVER_1_0_0_SIZE,
                    );
                }
                // Prevent ffi_driver's Drop from running (it would call release())
                std::mem::forget(ffi_driver);
            }
            _ => {
                if !error.is_null() {
                    let err = adbc_core::error::Error::with_message_and_status(
                        format!(
                            "Unsupported ADBC version: {}. Supported: 1.0.0 and 1.1.0",
                            version,
                        ),
                        adbc_core::error::Status::NotImplemented,
                    );
                    if let Ok(ffi_err) = adbc_ffi::FFI_AdbcError::try_from(err) {
                        unsafe { std::ptr::write_unaligned(error, ffi_err) };
                    }
                }
                return adbc_core::constants::ADBC_STATUS_NOT_IMPLEMENTED;
            }
        }

        adbc_core::constants::ADBC_STATUS_OK
    }

    #[allow(non_snake_case)]
    #[no_mangle]
    pub unsafe extern "C" fn AdbcDatabricksInit(
        version: c_int,
        driver: *mut c_void,
        error: *mut adbc_ffi::FFI_AdbcError,
    ) -> adbc_core::error::AdbcStatusCode {
        unsafe { driver_init(version, driver, error) }
    }

    #[allow(non_snake_case)]
    #[no_mangle]
    pub unsafe extern "C" fn AdbcDriverInit(
        version: c_int,
        driver: *mut c_void,
        error: *mut adbc_ffi::FFI_AdbcError,
    ) -> adbc_core::error::AdbcStatusCode {
        unsafe { driver_init(version, driver, error) }
    }
}
