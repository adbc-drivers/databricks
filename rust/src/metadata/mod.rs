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

//! Metadata helpers for ADBC Connection metadata methods.
//!
//! This module provides:
//! - [`SqlCommandBuilder`] — Builds SHOW SQL commands for metadata queries
//! - Result parsing — Parses `ExecuteResult` readers into intermediate structs
//! - Arrow builder — Constructs nested Arrow structs for `get_objects` responses
//! - Type mapping — Maps Databricks types to Arrow/XDBC types

pub mod builder;
pub mod parse;
pub mod sql;
pub mod type_mapping;

pub use sql::SqlCommandBuilder;
