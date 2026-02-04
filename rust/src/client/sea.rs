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
    ChunkLinkFetchResult, DatabricksClient, DatabricksHttpClient, ExecuteResponse,
    ExecuteResultData, SessionInfo,
};
use crate::error::{DatabricksErrorHelper, Result};
use crate::types::cloudfetch::CloudFetchLink;
use crate::types::sea::{
    CreateSessionRequest, CreateSessionResponse, ExecuteParams, ExecuteStatementRequest,
    GetChunksResponse, StatementExecutionResponse,
};
use async_trait::async_trait;
use driverbase::error::ErrorHelper;
use reqwest::Method;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// SEA client for the Databricks SQL Statement Execution API.
///
/// This client implements the `DatabricksClient` trait using REST endpoints.
#[derive(Debug)]
pub struct SeaClient {
    http_client: Arc<DatabricksHttpClient>,
    host: String,
    warehouse_id: String,
}

impl SeaClient {
    /// Create a new SEA client.
    pub fn new(
        http_client: Arc<DatabricksHttpClient>,
        host: impl Into<String>,
        warehouse_id: impl Into<String>,
    ) -> Self {
        Self {
            http_client,
            host: host.into(),
            warehouse_id: warehouse_id.into(),
        }
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
            chunk_index: result.chunk_index,
            row_offset: result.row_offset,
            row_count: result.row_count,
            byte_count: result.byte_count,
            next_chunk_index: result.next_chunk_index,
            next_chunk_internal_link: result.next_chunk_internal_link.clone(),
            external_links,
            has_inline_data: result.data_array.is_some(),
            data_array: result.data_array.clone(),
        })
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
        SeaClient::new(http_client, "https://test.databricks.com", "warehouse-123")
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
        let client = SeaClient::new(http_client, "https://test.databricks.com/", "warehouse-123");
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
        };

        let converted = SeaClient::convert_result_data(&result).unwrap();
        assert_eq!(converted.chunk_index, Some(0));
        assert_eq!(converted.row_count, Some(100));
        assert!(converted.external_links.is_none());
        assert!(!converted.has_inline_data);
    }

    #[test]
    fn test_convert_result_data_with_inline_data() {
        let result = crate::types::sea::ResultData {
            chunk_index: Some(0),
            row_offset: Some(0),
            row_count: Some(10),
            byte_count: Some(500),
            next_chunk_index: None,
            next_chunk_internal_link: None,
            external_links: None,
            data_array: Some(vec![vec!["value1".to_string()]]),
        };

        let converted = SeaClient::convert_result_data(&result).unwrap();
        assert!(converted.has_inline_data);
    }
}
