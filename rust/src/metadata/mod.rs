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

//! Metadata service for ADBC Connection interface methods.
//!
//! This module provides functionality for querying database metadata
//! such as catalogs, schemas, tables, and columns via SQL commands
//! executed through the SEA (Statement Execution API).
//!
//! ## Module Structure
//!
//! - `types`: Data structures for metadata query results
//! - `sql`: SQL command builder for metadata queries

pub mod sql;
pub mod types;

// Re-export commonly used types
pub use sql::SqlCommandBuilder;
pub use types::{CatalogInfo, ColumnInfo, ForeignKeyInfo, PrimaryKeyInfo, SchemaInfo, TableInfo};
