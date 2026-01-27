// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Databricks ADBC Driver for Rust
//!
//! This crate provides an ADBC (Arrow Database Connectivity) driver for
//! connecting to Databricks SQL endpoints.

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
pub use error::Error;
pub use statement::Statement;

pub type Result<T> = std::result::Result<T, Error>;
