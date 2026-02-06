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
use crate::reader::ResultReader;
use crate::types::cloudfetch::{CloudFetchConfig, CloudFetchLink};
use crate::types::sea::{ExecuteParams, ResultManifest, StatementStatus};
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::Duration;

pub use http::{DatabricksHttpClient, HttpClientConfig};
pub use sea::SeaClient;

/// Backend-agnostic configuration for DatabricksClient implementations.
///
/// Contains polling and CloudFetch settings shared across SEA and Thrift backends.
#[derive(Debug, Clone)]
pub struct DatabricksClientConfig {
    /// Maximum time to wait for statement completion (default: 600s).
    pub poll_timeout: Duration,
    /// Interval between status polls (default: 500ms).
    pub poll_interval: Duration,
    /// CloudFetch configuration.
    pub cloudfetch_config: CloudFetchConfig,
}

impl Default for DatabricksClientConfig {
    fn default() -> Self {
        Self {
            poll_timeout: Duration::from_secs(600),
            poll_interval: Duration::from_millis(500),
            cloudfetch_config: CloudFetchConfig::default(),
        }
    }
}

/// Session information returned from create_session.
#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub session_id: String,
}

/// Result from `execute_statement`. Contains the statement ID (for cancellation/cleanup)
/// and a reader over the result data.
pub struct ExecuteResult {
    pub statement_id: String,
    pub reader: Box<dyn ResultReader + Send>,
}

impl std::fmt::Debug for ExecuteResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecuteResult")
            .field("statement_id", &self.statement_id)
            .field("reader", &"<dyn ResultReader>")
            .finish()
    }
}

/// Unified response from statement execution (internal to client implementations).
#[derive(Debug, Clone)]
pub(crate) struct ExecuteResponse {
    pub(crate) statement_id: String,
    pub(crate) status: StatementStatus,
    pub(crate) manifest: Option<ResultManifest>,
    pub(crate) result: Option<ExecuteResultData>,
}

/// Result data from execution (internal to client implementations).
#[derive(Debug, Clone)]
pub(crate) struct ExecuteResultData {
    pub(crate) next_chunk_internal_link: Option<String>,
    pub(crate) external_links: Option<Vec<CloudFetchLink>>,
    /// Inline Arrow IPC data (decoded from base64).
    /// Present when server returns inline results instead of CloudFetch.
    pub(crate) inline_arrow_data: Option<Vec<u8>>,
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
    ///
    /// Handles polling, result format detection, and reader creation internally.
    /// Always returns a ready-to-read `ExecuteResult`.
    async fn execute_statement(
        &self,
        session_id: &str,
        sql: &str,
        params: &ExecuteParams,
    ) -> Result<ExecuteResult>;

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
