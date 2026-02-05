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

//! SEA (Statement Execution API) request/response types.
//!
//! These types map directly to the JSON structures used by the Databricks
//! SQL Statement Execution API. They are primarily used by `SeaClient`.

use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;

/// Response from statement execution or status polling.
#[derive(Debug, Clone, Deserialize)]
pub struct StatementExecutionResponse {
    pub statement_id: String,
    pub status: StatementStatus,
    #[serde(default)]
    pub manifest: Option<ResultManifest>,
    #[serde(default)]
    pub result: Option<ResultData>,
}

/// Status of a statement execution.
#[derive(Debug, Clone, Deserialize)]
pub struct StatementStatus {
    pub state: StatementState,
    #[serde(default)]
    pub error: Option<ServiceError>,
}

/// Possible states of a statement during execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StatementState {
    Pending,
    Running,
    Succeeded,
    Failed,
    Canceled,
    Closed,
}

/// Error information from the service.
#[derive(Debug, Clone, Deserialize)]
pub struct ServiceError {
    #[serde(default)]
    pub error_code: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}

/// Manifest describing the result set structure.
#[derive(Debug, Clone, Deserialize)]
pub struct ResultManifest {
    pub format: String,
    pub schema: ResultSchema,
    #[serde(default)]
    pub total_chunk_count: Option<i64>,
    #[serde(default)]
    pub total_row_count: Option<i64>,
    #[serde(default)]
    pub total_byte_count: Option<i64>,
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub chunks: Option<Vec<ChunkInfo>>,
    /// Compression codec used for result data ("LZ4_FRAME" or absent for none)
    #[serde(default)]
    pub result_compression: Option<String>,
}

/// Schema of the result set.
#[derive(Debug, Clone, Deserialize)]
pub struct ResultSchema {
    pub column_count: i32,
    pub columns: Vec<ColumnInfo>,
}

/// Information about a single column in the result.
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub type_name: String,
    pub type_text: String,
    pub position: i32,
}

/// Information about a result chunk (metadata only).
#[derive(Debug, Clone, Deserialize)]
pub struct ChunkInfo {
    pub chunk_index: i64,
    pub row_offset: i64,
    pub row_count: i64,
    pub byte_count: i64,
}

/// Result data from a chunk fetch or initial execution response.
#[derive(Debug, Clone, Deserialize)]
pub struct ResultData {
    #[serde(default)]
    pub chunk_index: Option<i64>,
    #[serde(default)]
    pub row_offset: Option<i64>,
    #[serde(default)]
    pub row_count: Option<i64>,
    #[serde(default)]
    pub byte_count: Option<i64>,
    #[serde(default)]
    pub next_chunk_index: Option<i64>,
    #[serde(default)]
    pub next_chunk_internal_link: Option<String>,
    #[serde(default)]
    pub external_links: Option<Vec<ExternalLink>>,
    /// Inline data for small results in JSON format (not used with Arrow)
    #[serde(default)]
    pub data_array: Option<Vec<Vec<String>>>,
    /// Inline Arrow IPC data (base64-encoded in JSON, decoded by serde).
    /// Present when server returns inline results instead of CloudFetch.
    #[serde(default, deserialize_with = "deserialize_base64_attachment")]
    pub attachment: Option<Vec<u8>>,
}

/// Deserialize base64-encoded attachment field from JSON.
fn deserialize_base64_attachment<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    match opt {
        Some(s) if !s.is_empty() => STANDARD
            .decode(&s)
            .map(Some)
            .map_err(serde::de::Error::custom),
        _ => Ok(None),
    }
}

/// External link for CloudFetch download.
#[derive(Debug, Clone, Deserialize)]
pub struct ExternalLink {
    pub external_link: String,
    pub expiration: String, // ISO 8601 timestamp
    pub chunk_index: i64,
    pub row_offset: i64,
    pub row_count: i64,
    pub byte_count: i64,
    #[serde(default)]
    pub http_headers: Option<HashMap<String, String>>,
    #[serde(default)]
    pub next_chunk_index: Option<i64>,
}

/// Request body for statement execution.
#[derive(Debug, Clone, Serialize)]
pub struct ExecuteStatementRequest {
    pub warehouse_id: String,
    pub statement: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    pub disposition: String, // "EXTERNAL_LINKS" for CloudFetch
    pub format: String,      // "ARROW_STREAM"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wait_timeout: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_wait_timeout: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_limit: Option<i64>,
}

/// Parameters for statement execution (passed to client methods).
#[derive(Debug, Clone, Default)]
pub struct ExecuteParams {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub wait_timeout: Option<String>,
    pub on_wait_timeout: Option<String>, // "CONTINUE" or "CANCEL"
    pub row_limit: Option<i64>,
}

/// Compression codec for result data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionCodec {
    #[default]
    None,
    Lz4Frame,
}

impl CompressionCodec {
    /// Parse compression codec from manifest field value.
    pub fn from_manifest(value: Option<&str>) -> Self {
        match value {
            Some("LZ4_FRAME") => Self::Lz4Frame,
            _ => Self::None,
        }
    }
}

/// Request body for session creation.
#[derive(Debug, Clone, Serialize)]
pub struct CreateSessionRequest {
    pub warehouse_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub session_configuration: HashMap<String, String>,
}

/// Response from session creation.
#[derive(Debug, Clone, Deserialize)]
pub struct CreateSessionResponse {
    pub session_id: String,
}

/// Response from fetching result chunks.
/// Note: The /result/chunks/{chunk_index} endpoint returns a different format
/// than the main execute response - it doesn't include statement_id.
#[derive(Debug, Clone, Deserialize)]
pub struct GetChunksResponse {
    #[serde(default)]
    pub statement_id: Option<String>,
    #[serde(default)]
    pub chunk_index: Option<i64>,
    #[serde(default)]
    pub row_offset: Option<i64>,
    #[serde(default)]
    pub row_count: Option<i64>,
    #[serde(default)]
    pub next_chunk_index: Option<i64>,
    #[serde(default)]
    pub external_links: Option<Vec<ExternalLink>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_codec_from_manifest() {
        assert_eq!(
            CompressionCodec::from_manifest(Some("LZ4_FRAME")),
            CompressionCodec::Lz4Frame
        );
        assert_eq!(
            CompressionCodec::from_manifest(Some("UNKNOWN")),
            CompressionCodec::None
        );
        assert_eq!(
            CompressionCodec::from_manifest(None),
            CompressionCodec::None
        );
    }

    #[test]
    fn test_statement_state_deserialization() {
        let json = r#""SUCCEEDED""#;
        let state: StatementState = serde_json::from_str(json).unwrap();
        assert_eq!(state, StatementState::Succeeded);

        let json = r#""PENDING""#;
        let state: StatementState = serde_json::from_str(json).unwrap();
        assert_eq!(state, StatementState::Pending);
    }

    #[test]
    fn test_execute_statement_request_serialization() {
        let req = ExecuteStatementRequest {
            warehouse_id: "abc123".to_string(),
            statement: "SELECT 1".to_string(),
            session_id: Some("session-1".to_string()),
            catalog: None,
            schema: None,
            disposition: "EXTERNAL_LINKS".to_string(),
            format: "ARROW_STREAM".to_string(),
            wait_timeout: Some("30s".to_string()),
            on_wait_timeout: None,
            row_limit: None,
        };

        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"warehouse_id\":\"abc123\""));
        assert!(json.contains("\"session_id\":\"session-1\""));
        assert!(!json.contains("\"catalog\"")); // None should be skipped
    }

    #[test]
    fn test_statement_execution_response_deserialization() {
        let json = r#"{
            "statement_id": "stmt-123",
            "status": {
                "state": "SUCCEEDED"
            },
            "manifest": {
                "format": "ARROW_STREAM",
                "schema": {
                    "column_count": 1,
                    "columns": [{"name": "id", "type_name": "INT", "type_text": "INT", "position": 0}]
                },
                "total_chunk_count": 1,
                "total_row_count": 100
            }
        }"#;

        let response: StatementExecutionResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.statement_id, "stmt-123");
        assert_eq!(response.status.state, StatementState::Succeeded);
        assert!(response.manifest.is_some());
        let manifest = response.manifest.unwrap();
        assert_eq!(manifest.total_row_count, Some(100));
    }

    #[test]
    fn test_external_link_deserialization() {
        let json = r#"{
            "external_link": "https://storage.example.com/chunk0",
            "expiration": "2024-01-01T12:00:00Z",
            "chunk_index": 0,
            "row_offset": 0,
            "row_count": 1000,
            "byte_count": 50000,
            "http_headers": {"x-custom": "value"},
            "next_chunk_index": 1
        }"#;

        let link: ExternalLink = serde_json::from_str(json).unwrap();
        assert_eq!(link.chunk_index, 0);
        assert_eq!(link.next_chunk_index, Some(1));
        assert!(link.http_headers.is_some());
        assert_eq!(
            link.http_headers.unwrap().get("x-custom"),
            Some(&"value".to_string())
        );
    }

    #[test]
    fn test_result_data_with_base64_attachment() {
        // "Hello, World!" in base64
        let json = r#"{
            "chunk_index": 0,
            "row_count": 10,
            "attachment": "SGVsbG8sIFdvcmxkIQ=="
        }"#;

        let result: ResultData = serde_json::from_str(json).unwrap();
        assert_eq!(result.chunk_index, Some(0));
        assert!(result.attachment.is_some());
        assert_eq!(result.attachment.unwrap(), b"Hello, World!");
    }

    #[test]
    fn test_result_data_without_attachment() {
        let json = r#"{
            "chunk_index": 0,
            "row_count": 10
        }"#;

        let result: ResultData = serde_json::from_str(json).unwrap();
        assert!(result.attachment.is_none());
    }

    #[test]
    fn test_result_data_with_empty_attachment() {
        let json = r#"{
            "chunk_index": 0,
            "attachment": ""
        }"#;

        let result: ResultData = serde_json::from_str(json).unwrap();
        assert!(result.attachment.is_none());
    }

    #[test]
    fn test_result_data_with_null_attachment() {
        let json = r#"{
            "chunk_index": 0,
            "attachment": null
        }"#;

        let result: ResultData = serde_json::from_str(json).unwrap();
        assert!(result.attachment.is_none());
    }
}
