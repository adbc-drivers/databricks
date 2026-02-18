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

// FFI export — produces AdbcDatabricksInit and AdbcDriverInit symbols
// when built with `cargo build --features ffi`
#[cfg(feature = "ffi")]
adbc_ffi::export_driver!(AdbcDatabricksInit, crate::driver::Driver);
