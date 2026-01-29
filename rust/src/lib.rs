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
//! ## Example
//!
//! ```ignore
//! use databricks_adbc::Driver;
//! use adbc_core::driver::Driver as _;
//!
//! let driver = Driver::new();
//! let mut database = driver.new_database()?;
//! database.set_option("uri", "https://my-workspace.databricks.com")?;
//! database.set_option("http_path", "/sql/1.0/warehouses/abc123")?;
//! database.set_option("access_token", "dapi...")?;
//!
//! let connection = database.new_connection()?;
//! let mut statement = connection.new_statement()?;
//! statement.set_sql_query("SELECT * FROM my_table")?;
//! let result = statement.execute_query()?;
//! ```

pub mod auth;
pub mod client;
pub mod connection;
pub mod database;
pub mod driver;
pub mod error;
pub mod reader;
pub mod result;
pub mod statement;
pub mod telemetry;

pub use connection::Connection;
pub use database::Database;
pub use driver::Driver;
pub use error::{DatabricksErrorHelper, Error, Result};
pub use statement::Statement;
