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

//! Type definitions for the Databricks ADBC driver.
//!
//! This module contains data structures organized by domain:
//! - `sea`: SEA (Statement Execution API) request/response types
//! - `cloudfetch`: CloudFetch-specific types for streaming downloads

pub mod cloudfetch;
pub mod sea;

// Re-export commonly used types
pub use cloudfetch::{CloudFetchConfig, CloudFetchLink};
pub use sea::{
    ChunkInfo, ColumnInfo, CompressionCodec, ExecuteParams, ExecuteStatementRequest, ExternalLink,
    ResultData, ResultManifest, ResultSchema, ServiceError, StatementExecutionResponse,
    StatementState, StatementStatus,
};
