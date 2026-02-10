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

//! SEA (Statement Execution API) client implementation.
//!
//! This module implements the `DatabricksClient` trait using the Databricks
//! SQL Statement Execution API (REST-based).

use crate::client::{
    ChunkLinkFetchResult, DatabricksClient, DatabricksClientConfig, DatabricksHttpClient,
    ExecuteResponse, ExecuteResult, ExecuteResultData, SessionInfo,
};
use crate::error::{DatabricksErrorHelper, Result};
use crate::reader::ResultReaderFactory;
use crate::types::cloudfetch::CloudFetchLink;
use crate::types::sea::{
    CreateSessionRequest, CreateSessionResponse, ExecuteParams, ExecuteStatementRequest,
    GetChunksResponse, StatementExecutionResponse, StatementState,
};
use async_trait::async_trait;
use driverbase::error::ErrorHelper;
use reqwest::Method;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Handle as RuntimeHandle;
use tracing::debug;

/// SEA client for the Databricks SQL Statement Execution API.
///
/// This client implements the `DatabricksClient` trait using REST endpoints.
/// The `reader_factory` is set after construction via `set_reader_factory()` because
/// `ResultReaderFactory` needs `Arc<dyn DatabricksClient>` which requires wrapping
/// `SeaClient` in `Arc` first.
pub struct SeaClient {
    http_client: Arc<DatabricksHttpClient>,
    host: String,
    warehouse_id: String,
    config: DatabricksClientConfig,
    reader_factory: OnceLock<ResultReaderFactory>,
    runtime_handle: OnceLock<RuntimeHandle>,
}

impl std::fmt::Debug for SeaClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeaClient")
            .field("host", &self.host)
            .field("warehouse_id", &self.warehouse_id)
            .field("config", &self.config)
            .field(
                "reader_factory",
                &if self.reader_factory.get().is_some() {
                    "<set>"
                } else {
                    "<not set>"
                },
            )
            .finish()
    }
}

impl SeaClient {
    /// Create a new SEA client.
    ///
    /// After construction, call `set_reader_factory()` to enable result reader creation.
    /// This two-step initialization is required because `ResultReaderFactory` needs
    /// `Arc<dyn DatabricksClient>`, which is only available after wrapping in `Arc`.
    pub fn new(
        http_client: Arc<DatabricksHttpClient>,
        host: impl Into<String>,
        warehouse_id: impl Into<String>,
        config: DatabricksClientConfig,
    ) -> Self {
        Self {
            http_client,
            host: host.into(),
            warehouse_id: warehouse_id.into(),
            config,
            reader_factory: OnceLock::new(),
            runtime_handle: OnceLock::new(),
        }
    }

    /// Set the reader factory and runtime handle. Must be called once after wrapping in `Arc`.
    pub fn set_reader_factory(
        &self,
        reader_factory: ResultReaderFactory,
        runtime_handle: RuntimeHandle,
    ) {
        let _ = self.reader_factory.set(reader_factory);
        let _ = self.runtime_handle.set(runtime_handle);
    }

    fn reader_factory(&self) -> Result<&ResultReaderFactory> {
        self.reader_factory.get().ok_or_else(|| {
            DatabricksErrorHelper::invalid_state()
                .message("SeaClient reader_factory not initialized")
        })
    }

    /// Build the base URL for API requests.
    fn base_url(&self) -> String {
        format!("{}/api/2.0/sql", self.host.trim_end_matches('/'))
    }

    /// Convert SEA response to internal ExecuteResultData.
    fn convert_result_data(result: &crate::types::sea::ResultData) -> Result<ExecuteResultData> {
        let external_links = if let Some(ref links) = result.external_links {
            let converted: Result<Vec<CloudFetchLink>> = links
                .iter()
                .map(CloudFetchLink::from_external_link)
                .collect();
            Some(converted?)
        } else {
            None
        };

        Ok(ExecuteResultData {
            next_chunk_internal_link: result.next_chunk_internal_link.clone(),
            external_links,
            inline_arrow_data: result.attachment.clone(),
        })
    }

    /// Poll for statement status (private — used by wait_for_completion).
    async fn get_statement_status(&self, statement_id: &str) -> Result<ExecuteResponse> {
        let url = format!("{}/statements/{}", self.base_url(), statement_id);

        debug!("Getting statement status at {}", url);

        let request = self
            .http_client
            .inner()
            .request(Method::GET, &url)
            .build()
            .map_err(|e| {
                DatabricksErrorHelper::io().message(format!("Failed to build request: {}", e))
            })?;

        let response = self.http_client.execute(request).await?;
        let body = response.text().await.map_err(|e| {
            DatabricksErrorHelper::io().message(format!("Failed to read response: {}", e))
        })?;

        let sea_response: StatementExecutionResponse =
            serde_json::from_str(&body).map_err(|e| {
                DatabricksErrorHelper::io().message(format!(
                    "Failed to parse status response: {} - body: {}",
                    e, body
                ))
            })?;

        debug!(
            "Status response: statement_id={}, status={:?}",
            sea_response.statement_id, sea_response.status.state
        );

        Self::convert_response(sea_response)
    }

    /// Wait for statement to complete, polling status.
    async fn wait_for_completion(&self, response: ExecuteResponse) -> Result<ExecuteResponse> {
        let start = std::time::Instant::now();
        let mut current_response = response;

        loop {
            match current_response.status.state {
                StatementState::Succeeded => return Ok(current_response),
                StatementState::Failed => {
                    let error_msg = current_response
                        .status
                        .error
                        .as_ref()
                        .and_then(|e| e.message.clone())
                        .unwrap_or_else(|| "Unknown error".to_string());
                    return Err(DatabricksErrorHelper::io().message(error_msg));
                }
                StatementState::Canceled => {
                    return Err(
                        DatabricksErrorHelper::invalid_state().message("Statement was canceled")
                    );
                }
                StatementState::Closed => {
                    // Closed with result data is valid for inline results — the server
                    // delivers the data and immediately closes the statement since no
                    // further fetching is needed.
                    if current_response.result.is_some() {
                        debug!("Statement closed with inline result data - treating as success");
                        return Ok(current_response);
                    }
                    return Err(
                        DatabricksErrorHelper::invalid_state().message("Statement was closed")
                    );
                }
                StatementState::Pending | StatementState::Running => {
                    if start.elapsed() > self.config.poll_timeout {
                        return Err(
                            DatabricksErrorHelper::io().message("Statement execution timed out")
                        );
                    }

                    tokio::time::sleep(self.config.poll_interval).await;

                    debug!(
                        "Polling statement status: {}",
                        current_response.statement_id
                    );
                    current_response = self
                        .get_statement_status(&current_response.statement_id)
                        .await?;
                }
            }
        }
    }

    /// Call the execute statement API endpoint (without polling or reader creation).
    async fn call_execute_api(
        &self,
        session_id: &str,
        sql: &str,
        params: &ExecuteParams,
    ) -> Result<ExecuteResponse> {
        let url = format!("{}/statements", self.base_url());

        let request_body = ExecuteStatementRequest {
            warehouse_id: self.warehouse_id.clone(),
            statement: sql.to_string(),
            session_id: Some(session_id.to_string()),
            catalog: params.catalog.clone(),
            schema: params.schema.clone(),
            // TODO: INLINE_OR_EXTERNAL_LINKS is not a public API disposition but enables
            // returning all chunk links in a single response (requires JDBC user agent).
            // Once EXTERNAL_LINKS supports multi-link responses, switch back to it.
            disposition: "INLINE_OR_EXTERNAL_LINKS".to_string(),
            format: "ARROW_STREAM".to_string(),
            wait_timeout: params.wait_timeout.clone(),
            on_wait_timeout: params.on_wait_timeout.clone(),
            row_limit: params.row_limit,
        };

        debug!("Executing statement at {}: {}", url, sql);

        let request = self
            .http_client
            .inner()
            .request(Method::POST, &url)
            .header("Content-Type", "application/json")
            .json(&request_body)
            .build()
            .map_err(|e| {
                DatabricksErrorHelper::io().message(format!("Failed to build request: {}", e))
            })?;

        let response = self.http_client.execute(request).await?;
        let body = response.text().await.map_err(|e| {
            DatabricksErrorHelper::io().message(format!("Failed to read response: {}", e))
        })?;

        let sea_response: StatementExecutionResponse =
            serde_json::from_str(&body).map_err(|e| {
                DatabricksErrorHelper::io().message(format!(
                    "Failed to parse execute response: {} - body: {}",
                    e, body
                ))
            })?;

        debug!(
            "Execute response: statement_id={}, status={:?}",
            sea_response.statement_id, sea_response.status.state
        );

        Self::convert_response(sea_response)
    }

    /// Convert SEA response to internal ExecuteResponse.
    fn convert_response(response: StatementExecutionResponse) -> Result<ExecuteResponse> {
        let result = if let Some(ref result) = response.result {
            Some(Self::convert_result_data(result)?)
        } else {
            None
        };

        Ok(ExecuteResponse {
            statement_id: response.statement_id,
            status: response.status,
            manifest: response.manifest,
            result,
        })
    }
}

#[async_trait]
impl DatabricksClient for SeaClient {
    async fn create_session(
        &self,
        catalog: Option<&str>,
        schema: Option<&str>,
        session_config: HashMap<String, String>,
    ) -> Result<SessionInfo> {
        let url = format!("{}/sessions", self.base_url());

        let request_body = CreateSessionRequest {
            warehouse_id: self.warehouse_id.clone(),
            catalog: catalog.map(|s| s.to_string()),
            schema: schema.map(|s| s.to_string()),
            session_configuration: session_config,
        };

        debug!("Creating session at {}", url);

        let request = self
            .http_client
            .inner()
            .request(Method::POST, &url)
            .header("Content-Type", "application/json")
            .json(&request_body)
            .build()
            .map_err(|e| {
                DatabricksErrorHelper::io().message(format!("Failed to build request: {}", e))
            })?;

        let response = self.http_client.execute(request).await?;
        let body = response.text().await.map_err(|e| {
            DatabricksErrorHelper::io().message(format!("Failed to read response: {}", e))
        })?;

        let session_response: CreateSessionResponse = serde_json::from_str(&body).map_err(|e| {
            DatabricksErrorHelper::io().message(format!(
                "Failed to parse session response: {} - body: {}",
                e, body
            ))
        })?;

        debug!("Created session: {}", session_response.session_id);

        Ok(SessionInfo {
            session_id: session_response.session_id,
        })
    }

    async fn delete_session(&self, session_id: &str) -> Result<()> {
        let url = format!("{}/sessions/{}", self.base_url(), session_id);

        debug!("Deleting session at {}", url);

        let request = self
            .http_client
            .inner()
            .request(Method::DELETE, &url)
            .build()
            .map_err(|e| {
                DatabricksErrorHelper::io().message(format!("Failed to build request: {}", e))
            })?;

        // Ignore errors on session deletion (best effort cleanup)
        let _ = self.http_client.execute(request).await;

        debug!("Deleted session: {}", session_id);

        Ok(())
    }

    async fn execute_statement(
        &self,
        session_id: &str,
        sql: &str,
        params: &ExecuteParams,
    ) -> Result<ExecuteResult> {
        // 1. Call SEA API
        let response = self.call_execute_api(session_id, sql, params).await?;

        // 2. Poll until completion
        let response = self.wait_for_completion(response).await?;

        // 3. Create appropriate reader
        let reader_factory = self.reader_factory()?;
        let reader = reader_factory.create_reader(&response.statement_id, &response)?;

        Ok(ExecuteResult {
            statement_id: response.statement_id,
            reader,
        })
    }

    async fn get_result_chunks(
        &self,
        statement_id: &str,
        chunk_index: i64,
        _row_offset: i64, // Ignored - SEA uses chunk_index
    ) -> Result<ChunkLinkFetchResult> {
        // The chunk_index is a path parameter, not a query parameter
        // See: https://docs.databricks.com/api/workspace/statementexecution
        let url = format!(
            "{}/statements/{}/result/chunks/{}",
            self.base_url(),
            statement_id,
            chunk_index
        );

        debug!("Getting result chunks at {}", url);

        let request = self
            .http_client
            .inner()
            .request(Method::GET, &url)
            .build()
            .map_err(|e| {
                DatabricksErrorHelper::io().message(format!("Failed to build request: {}", e))
            })?;

        let response = self.http_client.execute(request).await?;
        let body = response.text().await.map_err(|e| {
            DatabricksErrorHelper::io().message(format!("Failed to read response: {}", e))
        })?;

        let chunks_response: GetChunksResponse = serde_json::from_str(&body).map_err(|e| {
            DatabricksErrorHelper::io().message(format!(
                "Failed to parse chunks response: {} - body: {}",
                e, body
            ))
        })?;

        // Convert external links to CloudFetchLinks
        let links = if let Some(ref external_links) = chunks_response.external_links {
            external_links
                .iter()
                .map(CloudFetchLink::from_external_link)
                .collect::<Result<Vec<_>>>()?
        } else {
            vec![]
        };

        // The next_chunk_index may be:
        // 1. At the top level of the response, OR
        // 2. Inside the external_link (each link knows its next chunk)
        // Prefer top-level, fall back to link-level
        let next_chunk_index = chunks_response.next_chunk_index.or_else(|| {
            chunks_response
                .external_links
                .as_ref()
                .and_then(|links| links.first())
                .and_then(|link| link.next_chunk_index)
        });

        let has_more = next_chunk_index.is_some();

        debug!(
            "Chunks response: {} links, next_chunk_index={:?}, has_more={}",
            links.len(),
            next_chunk_index,
            has_more
        );

        Ok(ChunkLinkFetchResult {
            links,
            has_more,
            next_chunk_index,
            next_row_offset: None, // SEA doesn't use row offset
        })
    }

    async fn cancel_statement(&self, statement_id: &str) -> Result<()> {
        let url = format!("{}/statements/{}/cancel", self.base_url(), statement_id);

        debug!("Canceling statement at {}", url);

        let request = self
            .http_client
            .inner()
            .request(Method::POST, &url)
            .build()
            .map_err(|e| {
                DatabricksErrorHelper::io().message(format!("Failed to build request: {}", e))
            })?;

        self.http_client.execute(request).await?;

        debug!("Canceled statement: {}", statement_id);

        Ok(())
    }

    async fn close_statement(&self, statement_id: &str) -> Result<()> {
        let url = format!("{}/statements/{}", self.base_url(), statement_id);

        debug!("Closing statement at {}", url);

        let request = self
            .http_client
            .inner()
            .request(Method::DELETE, &url)
            .build()
            .map_err(|e| {
                DatabricksErrorHelper::io().message(format!("Failed to build request: {}", e))
            })?;

        // Ignore errors on statement close (best effort cleanup)
        let _ = self.http_client.execute(request).await;

        debug!("Closed statement: {}", statement_id);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::PersonalAccessToken;
    use crate::client::HttpClientConfig;

    fn create_test_client() -> SeaClient {
        let auth = Arc::new(PersonalAccessToken::new("test-token".to_string()));
        let http_client =
            Arc::new(DatabricksHttpClient::new(HttpClientConfig::default(), auth).unwrap());
        SeaClient::new(
            http_client,
            "https://test.databricks.com",
            "warehouse-123",
            DatabricksClientConfig::default(),
        )
    }

    #[test]
    fn test_base_url() {
        let client = create_test_client();
        assert_eq!(client.base_url(), "https://test.databricks.com/api/2.0/sql");
    }

    #[test]
    fn test_base_url_strips_trailing_slash() {
        let auth = Arc::new(PersonalAccessToken::new("test-token".to_string()));
        let http_client =
            Arc::new(DatabricksHttpClient::new(HttpClientConfig::default(), auth).unwrap());
        let client = SeaClient::new(
            http_client,
            "https://test.databricks.com/",
            "warehouse-123",
            DatabricksClientConfig::default(),
        );
        assert_eq!(client.base_url(), "https://test.databricks.com/api/2.0/sql");
    }

    #[test]
    fn test_convert_result_data_without_external_links() {
        let result = crate::types::sea::ResultData {
            chunk_index: Some(0),
            row_offset: Some(0),
            row_count: Some(100),
            byte_count: Some(5000),
            next_chunk_index: Some(1),
            next_chunk_internal_link: None,
            external_links: None,
            data_array: None,
            attachment: None,
        };

        let converted = SeaClient::convert_result_data(&result).unwrap();
        assert!(converted.external_links.is_none());
        assert!(converted.inline_arrow_data.is_none());
    }

    #[test]
    fn test_convert_result_data_with_inline_arrow_data() {
        let test_data = vec![1u8, 2, 3, 4, 5];
        let result = crate::types::sea::ResultData {
            chunk_index: Some(0),
            row_offset: Some(0),
            row_count: Some(10),
            byte_count: Some(500),
            next_chunk_index: None,
            next_chunk_internal_link: None,
            external_links: None,
            data_array: None,
            attachment: Some(test_data.clone()),
        };

        let converted = SeaClient::convert_result_data(&result).unwrap();
        assert!(converted.inline_arrow_data.is_some());
        assert_eq!(converted.inline_arrow_data.unwrap(), test_data);
    }
}
