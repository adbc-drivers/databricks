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

//! Client implementations for communicating with Databricks SQL endpoints.
//!
//! This module provides:
//! - `DatabricksClient` trait: Abstract interface for Databricks backends
//! - `DatabricksHttpClient`: Low-level HTTP client with retry logic
//! - `SeaClient`: Implementation using the Statement Execution API (REST)

pub mod http;
pub mod sea;

use crate::error::Result;
use crate::types::cloudfetch::CloudFetchLink;
use crate::types::sea::{ExecuteParams, ResultManifest, StatementStatus};
use async_trait::async_trait;
use std::collections::HashMap;

pub use http::{DatabricksHttpClient, HttpClientConfig};
pub use sea::SeaClient;

/// Session information returned from create_session.
#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub session_id: String,
}

/// Unified response from statement execution.
#[derive(Debug, Clone)]
pub struct ExecuteResponse {
    pub statement_id: String,
    pub status: StatementStatus,
    pub manifest: Option<ResultManifest>,
    pub result: Option<ExecuteResultData>,
}

/// Result data from execution (simplified view for consumers).
#[derive(Debug, Clone)]
pub struct ExecuteResultData {
    pub chunk_index: Option<i64>,
    pub row_offset: Option<i64>,
    pub row_count: Option<i64>,
    pub byte_count: Option<i64>,
    pub next_chunk_index: Option<i64>,
    pub next_chunk_internal_link: Option<String>,
    pub external_links: Option<Vec<CloudFetchLink>>,
    pub has_inline_data: bool,
}

/// Result of fetching chunk links.
#[derive(Debug, Clone)]
pub struct ChunkLinkFetchResult {
    pub links: Vec<CloudFetchLink>,
    pub has_more: bool,
    /// For chunk-index based backends (SEA).
    pub next_chunk_index: Option<i64>,
    /// For row-offset based backends (Thrift).
    pub next_row_offset: Option<i64>,
}

impl ChunkLinkFetchResult {
    /// Create an empty result indicating end of stream.
    pub fn end_of_stream() -> Self {
        Self {
            links: vec![],
            has_more: false,
            next_chunk_index: None,
            next_row_offset: None,
        }
    }
}

/// Abstract interface for Databricks backends (SEA, Thrift, etc.).
///
/// This trait provides the full client abstraction for session management,
/// statement execution, and result fetching. Implementations handle
/// protocol-specific details.
#[async_trait]
pub trait DatabricksClient: Send + Sync + std::fmt::Debug {
    // --- Session Management ---

    /// Create a new session with the given catalog/schema context.
    async fn create_session(
        &self,
        catalog: Option<&str>,
        schema: Option<&str>,
        session_config: HashMap<String, String>,
    ) -> Result<SessionInfo>;

    /// Delete/close a session.
    async fn delete_session(&self, session_id: &str) -> Result<()>;

    // --- Statement Execution ---

    /// Execute a SQL statement within a session.
    async fn execute_statement(
        &self,
        session_id: &str,
        sql: &str,
        params: &ExecuteParams,
    ) -> Result<ExecuteResponse>;

    /// Poll statement status (for async execution).
    async fn get_statement_status(&self, statement_id: &str) -> Result<ExecuteResponse>;

    // --- Result Fetching (CloudFetch) ---

    /// Fetch chunk links for CloudFetch.
    ///
    /// Different backends use different continuation mechanisms:
    /// - SEA uses `chunk_index`
    /// - Thrift uses `row_offset`
    ///
    /// Both parameters are provided; implementations use the relevant one.
    async fn get_result_chunks(
        &self,
        statement_id: &str,
        chunk_index: i64,
        row_offset: i64,
    ) -> Result<ChunkLinkFetchResult>;

    // --- Statement Lifecycle ---

    /// Cancel a running statement.
    async fn cancel_statement(&self, statement_id: &str) -> Result<()>;

    /// Close/cleanup a statement (release server resources).
    async fn close_statement(&self, statement_id: &str) -> Result<()>;
}
