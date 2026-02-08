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

//! Result readers for fetching query results from Databricks.
//!
//! This module provides:
//! - `ResultReaderFactory`: Creates appropriate reader based on response type
//! - `CloudFetchReader`: Streams Arrow data via cloud storage downloads
//! - `InlineArrowProvider`: Handles inline Arrow data embedded in responses

pub mod cloudfetch;
pub mod inline;

use crate::client::{DatabricksClient, DatabricksHttpClient, ExecuteResponse};
use crate::error::{DatabricksErrorHelper, Result};
use crate::reader::cloudfetch::chunk_downloader::ChunkDownloader;
use crate::reader::cloudfetch::link_fetcher::{SeaChunkLinkFetcher, SeaChunkLinkFetcherHandle};
use crate::reader::cloudfetch::streaming_provider::StreamingCloudFetchProvider;
use crate::reader::inline::InlineArrowProvider;
use crate::types::cloudfetch::{CloudFetchConfig, CloudFetchLink};
use crate::types::sea::{CompressionCodec, ResultManifest, StatementState};
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use driverbase::error::ErrorHelper;
use std::sync::Arc;

pub use cloudfetch::StreamingCloudFetchProvider as CloudFetchReader;

/// Factory that creates the appropriate reader based on the response type.
///
/// Encapsulates the decision of CloudFetch vs inline vs other result formats.
/// Uses `DatabricksClient` trait for backend flexibility - works with SeaClient
/// or any future backend implementation.
#[derive(Debug)]
pub struct ResultReaderFactory {
    client: Arc<dyn DatabricksClient>,
    http_client: Arc<DatabricksHttpClient>,
    config: CloudFetchConfig,
    runtime_handle: tokio::runtime::Handle,
}

impl ResultReaderFactory {
    /// Create a new result reader factory.
    pub fn new(
        client: Arc<dyn DatabricksClient>,
        http_client: Arc<DatabricksHttpClient>,
        config: CloudFetchConfig,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            client,
            http_client,
            config,
            runtime_handle,
        }
    }

    /// Create a reader from a statement execution response.
    ///
    /// Automatically selects CloudFetch, inline, or other reader based on response.
    pub(crate) fn create_reader(
        &self,
        statement_id: &str,
        response: &ExecuteResponse,
    ) -> Result<Box<dyn ResultReader + Send>> {
        // Log manifest info for debugging
        if let Some(ref manifest) = response.manifest {
            tracing::debug!(
                "Result manifest: total_chunks={:?}, total_rows={:?}, total_bytes={:?}, truncated={}",
                manifest.total_chunk_count,
                manifest.total_row_count,
                manifest.total_byte_count,
                manifest.truncated
            );
        }

        // Determine compression from manifest
        let compression = response
            .manifest
            .as_ref()
            .and_then(|m| m.result_compression.as_deref())
            .map(|s| CompressionCodec::from_manifest(Some(s)))
            .unwrap_or_default();

        // Check result data for CloudFetch or inline results
        if let Some(ref result) = response.result {
            // Log what result data we have for debugging the decision path
            tracing::debug!(
                "Result data present: external_links={}, inline_arrow_data={} bytes, next_chunk_internal_link={}",
                result.external_links.as_ref().map(|l| l.len()).unwrap_or(0),
                result.inline_arrow_data.as_ref().map(|d| d.len()).unwrap_or(0),
                result.next_chunk_internal_link.is_some()
            );

            // Priority 1: CloudFetch (external_links present)
            if let Some(ref external_links) = result.external_links {
                if !external_links.is_empty() {
                    let total_chunk_count =
                        response.manifest.as_ref().and_then(|m| m.total_chunk_count);

                    tracing::info!(
                        "Using CloudFetch reader: {} external links, total_chunks={:?}",
                        external_links.len(),
                        total_chunk_count
                    );

                    return self.create_cloudfetch_reader(
                        statement_id,
                        external_links,
                        total_chunk_count,
                        compression,
                    );
                }
            }

            // Priority 2: Inline Arrow (attachment present)
            if let Some(ref inline_data) = result.inline_arrow_data {
                if !inline_data.is_empty() {
                    tracing::info!(
                        "Using inline Arrow reader: {} bytes of Arrow IPC data",
                        inline_data.len()
                    );

                    return self.create_inline_reader(inline_data, compression);
                }
            }

            // Internal links without external = CloudFetch not available
            if result.next_chunk_internal_link.is_some() {
                return Err(DatabricksErrorHelper::not_implemented()
                    .message("CloudFetch not available. Internal links not supported."));
            }
        }

        // No result data - check if this is a valid empty result or an error state
        match response.status.state {
            StatementState::Succeeded | StatementState::Closed => {
                // Valid empty result set - extract schema from manifest
                // Note: Closed state is valid for inline results where the server delivers
                // the data (or empty result) and immediately closes the statement.
                tracing::info!(
                    "Using empty reader: no result data present for {:?} statement",
                    response.status.state
                );
                let schema = self.extract_schema_from_manifest(response.manifest.as_ref())?;
                Ok(Box::new(EmptyReader::new(schema)))
            }
            StatementState::Pending | StatementState::Running => {
                Err(DatabricksErrorHelper::invalid_state()
                    .message("Statement is still executing. Poll for completion first."))
            }
            StatementState::Failed => {
                let error_msg = response
                    .status
                    .error
                    .as_ref()
                    .and_then(|e| e.message.as_deref())
                    .unwrap_or("Unknown error");
                Err(DatabricksErrorHelper::io().message(format!("Statement failed: {}", error_msg)))
            }
            StatementState::Canceled => {
                Err(DatabricksErrorHelper::io().message("Statement was canceled"))
            }
        }
    }

    /// Extract Arrow schema from the result manifest.
    ///
    /// Falls back to an empty schema if no manifest is available.
    fn extract_schema_from_manifest(&self, manifest: Option<&ResultManifest>) -> Result<SchemaRef> {
        let Some(manifest) = manifest else {
            tracing::warn!("No manifest available for empty result set, using empty schema");
            return Ok(Arc::new(Schema::empty()));
        };

        let fields: Vec<Field> = manifest
            .schema
            .columns
            .iter()
            .map(|col| {
                let data_type = Self::map_databricks_type(&col.type_name);
                Field::new(&col.name, data_type, true) // nullable by default
            })
            .collect();

        Ok(Arc::new(Schema::new(fields)))
    }

    /// Map Databricks SQL type names to Arrow DataTypes.
    fn map_databricks_type(type_name: &str) -> DataType {
        // Databricks type names from the API
        match type_name.to_uppercase().as_str() {
            "BOOLEAN" => DataType::Boolean,
            "BYTE" | "TINYINT" => DataType::Int8,
            "SHORT" | "SMALLINT" => DataType::Int16,
            "INT" | "INTEGER" => DataType::Int32,
            "LONG" | "BIGINT" => DataType::Int64,
            "FLOAT" | "REAL" => DataType::Float32,
            "DOUBLE" => DataType::Float64,
            "STRING" => DataType::Utf8,
            "BINARY" => DataType::Binary,
            "DATE" => DataType::Date32,
            "TIMESTAMP" | "TIMESTAMP_NTZ" => {
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
            }
            // For complex types and unknown types, fall back to string
            // The actual Arrow IPC data will have the correct type
            _ => {
                tracing::debug!("Unknown Databricks type '{}', mapping to Utf8", type_name);
                DataType::Utf8
            }
        }
    }

    fn create_cloudfetch_reader(
        &self,
        statement_id: &str,
        external_links: &[CloudFetchLink],
        total_chunk_count: Option<i64>,
        compression: CompressionCodec,
    ) -> Result<Box<dyn ResultReader + Send>> {
        tracing::debug!(
            "Creating CloudFetch reader: {} initial links, total_chunks={:?}, prefetch_window={}",
            external_links.len(),
            total_chunk_count,
            self.config.link_prefetch_window
        );

        // Create link fetcher with caching and prefetching built-in
        let fetcher = SeaChunkLinkFetcher::new(
            self.client.clone(),
            statement_id.to_string(),
            external_links,
            total_chunk_count,
            self.config.link_prefetch_window as i64,
            self.runtime_handle.clone(),
        );
        let link_fetcher: Arc<dyn crate::reader::cloudfetch::ChunkLinkFetcher> =
            Arc::new(SeaChunkLinkFetcherHandle::new(fetcher));

        let chunk_downloader = Arc::new(ChunkDownloader::new(
            self.http_client.clone(),
            compression,
            self.config.speed_threshold_mbps,
        ));

        let provider = StreamingCloudFetchProvider::new(
            self.config.clone(),
            link_fetcher,
            chunk_downloader,
            self.runtime_handle.clone(),
        );

        Ok(Box::new(CloudFetchResultReader::new(
            provider,
            self.runtime_handle.clone(),
        )))
    }

    fn create_inline_reader(
        &self,
        inline_data: &[u8],
        compression: CompressionCodec,
    ) -> Result<Box<dyn ResultReader + Send>> {
        tracing::debug!(
            "Creating inline Arrow reader: {} bytes, compression={:?}",
            inline_data.len(),
            compression
        );

        let provider = InlineArrowProvider::new(inline_data.to_vec(), compression)?;

        Ok(Box::new(InlineArrowReader::new(provider)))
    }
}

/// Trait for result readers.
pub trait ResultReader: Send {
    /// Get the schema of the result.
    fn schema(&self) -> Result<SchemaRef>;

    /// Get the next record batch, or None if end of results.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>>;
}

/// Wrapper for StreamingCloudFetchProvider that implements ResultReader.
struct CloudFetchResultReader {
    provider: Arc<StreamingCloudFetchProvider>,
    runtime_handle: tokio::runtime::Handle,
}

impl CloudFetchResultReader {
    fn new(
        provider: Arc<StreamingCloudFetchProvider>,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            provider,
            runtime_handle,
        }
    }
}

impl ResultReader for CloudFetchResultReader {
    fn schema(&self) -> Result<SchemaRef> {
        self.runtime_handle.block_on(self.provider.get_schema())
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.runtime_handle.block_on(self.provider.next_batch())
    }
}

/// Reader for inline Arrow results.
///
/// Wraps `InlineArrowProvider` to implement the `ResultReader` trait.
struct InlineArrowReader {
    provider: InlineArrowProvider,
    /// Cached schema (extracted from first batch on creation)
    schema: Option<SchemaRef>,
}

impl InlineArrowReader {
    fn new(provider: InlineArrowProvider) -> Self {
        let schema = provider.schema().cloned();
        Self { provider, schema }
    }
}

impl ResultReader for InlineArrowReader {
    fn schema(&self) -> Result<SchemaRef> {
        self.schema.clone().ok_or_else(|| {
            DatabricksErrorHelper::invalid_state().message("Schema not available for inline result")
        })
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.provider.next_batch()
    }
}

/// Empty reader for queries with no results.
///
/// Used for valid queries that return zero rows (e.g., `SELECT * WHERE 1=0`).
/// The schema is preserved from the query's manifest.
pub struct EmptyReader {
    schema: SchemaRef,
}

impl EmptyReader {
    fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl ResultReader for EmptyReader {
    fn schema(&self) -> Result<SchemaRef> {
        Ok(self.schema.clone())
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(None)
    }
}

/// Adapter to make ResultReader work as arrow's RecordBatchReader.
pub struct ResultReaderAdapter {
    inner: Box<dyn ResultReader + Send>,
    schema: SchemaRef,
}

impl ResultReaderAdapter {
    /// Create a new adapter wrapping a ResultReader.
    pub fn new(inner: Box<dyn ResultReader + Send>) -> Result<Self> {
        let schema = inner.schema()?;
        Ok(Self { inner, schema })
    }
}

impl arrow_array::RecordBatchReader for ResultReaderAdapter {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Iterator for ResultReaderAdapter {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next_batch() {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => None,
            Err(e) => Some(Err(ArrowError::ExternalError(Box::new(
                std::io::Error::other(e.to_string()),
            )))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_reader_with_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let mut reader = EmptyReader::new(schema.clone());

        // Schema should be preserved
        let result_schema = reader.schema().unwrap();
        assert_eq!(result_schema.fields().len(), 2);
        assert_eq!(result_schema.field(0).name(), "id");
        assert_eq!(result_schema.field(1).name(), "name");

        // Should return no batches
        assert!(reader.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_empty_reader_with_empty_schema() {
        let schema = Arc::new(Schema::empty());
        let mut reader = EmptyReader::new(schema);

        assert_eq!(reader.schema().unwrap().fields().len(), 0);
        assert!(reader.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_map_databricks_types() {
        assert_eq!(
            ResultReaderFactory::map_databricks_type("BOOLEAN"),
            DataType::Boolean
        );
        assert_eq!(
            ResultReaderFactory::map_databricks_type("INT"),
            DataType::Int32
        );
        assert_eq!(
            ResultReaderFactory::map_databricks_type("BIGINT"),
            DataType::Int64
        );
        assert_eq!(
            ResultReaderFactory::map_databricks_type("STRING"),
            DataType::Utf8
        );
        assert_eq!(
            ResultReaderFactory::map_databricks_type("DOUBLE"),
            DataType::Float64
        );
        // Unknown types fall back to Utf8
        assert_eq!(
            ResultReaderFactory::map_databricks_type("UNKNOWN_TYPE"),
            DataType::Utf8
        );
    }
}
