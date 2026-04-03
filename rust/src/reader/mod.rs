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
#[cfg(test)]
pub(crate) mod test_utils;

use crate::client::{DatabricksClient, DatabricksHttpClient, ExecuteResponse};
use crate::error::{DatabricksErrorHelper, Result};
use crate::reader::cloudfetch::chunk_downloader::ChunkDownloader;
use crate::reader::cloudfetch::link_fetcher::{SeaChunkLinkFetcher, SeaChunkLinkFetcherHandle};
use crate::reader::cloudfetch::streaming_provider::StreamingCloudFetchProvider;
use crate::reader::inline::InlineArrowProvider;
use crate::types::cloudfetch::{CloudFetchConfig, CloudFetchLink};
use crate::types::sea::{CompressionCodec, ResultManifest, StatementState};
use arrow_array::{Array, RecordBatch, StructArray};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use std::collections::HashMap;
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
    /// Keeps the runtime alive as long as any reader exists.
    _runtime_keepalive: Arc<tokio::runtime::Runtime>,
}

impl ResultReaderFactory {
    /// Create a new result reader factory.
    pub fn new(
        client: Arc<dyn DatabricksClient>,
        http_client: Arc<DatabricksHttpClient>,
        config: CloudFetchConfig,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Self {
        Self {
            client,
            http_client,
            config,
            runtime_handle: runtime.handle().clone(),
            _runtime_keepalive: runtime,
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
            self._runtime_keepalive.clone(),
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
    /// Prevents the tokio Runtime from being dropped while this reader is alive.
    /// The reader crosses the FFI boundary (lives inside FFI_ArrowArrayStream)
    /// and may outlive the ADBC Connection that owns the Runtime.
    _runtime_keepalive: Arc<tokio::runtime::Runtime>,
}

impl CloudFetchResultReader {
    fn new(
        provider: Arc<StreamingCloudFetchProvider>,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Self {
        Self {
            provider,
            runtime_handle: runtime.handle().clone(),
            _runtime_keepalive: runtime,
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
    /// Create an empty reader that returns no batches.
    pub fn new(schema: SchemaRef) -> Self {
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
///
/// When `use_geoarrow` is enabled, this adapter detects geometry columns
/// (Struct { srid: Int32, wkb: Binary }) in the first batch, rewrites the
/// schema to use GeoArrow WKB encoding (Binary with ARROW:extension:name =
/// geoarrow.wkb), and transforms subsequent batches by extracting the wkb
/// child from the struct (zero-copy).
pub struct ResultReaderAdapter {
    inner: Box<dyn ResultReader + Send>,
    schema: SchemaRef,
    /// Column indices that contain geometry structs to be converted.
    geo_columns: Vec<usize>,
    /// Whether the schema has been finalized with CRS info from the first batch.
    schema_finalized: bool,
    /// Whether GeoArrow conversion is enabled.
    #[allow(dead_code)]
    use_geoarrow: bool,
}

impl ResultReaderAdapter {
    /// Create a new adapter wrapping a ResultReader.
    ///
    /// When `use_geoarrow` is true, geometry columns (Struct { srid: Int32, wkb: Binary })
    /// will be automatically converted to GeoArrow WKB encoding.
    pub fn new(inner: Box<dyn ResultReader + Send>, use_geoarrow: bool) -> Result<Self> {
        let schema = inner.schema()?;
        let geo_columns = if use_geoarrow {
            Self::detect_geometry_columns(&schema)
        } else {
            Vec::new()
        };

        // If we have geo columns, build initial schema (without CRS -- that comes from first batch)
        let schema = if !geo_columns.is_empty() {
            Arc::new(Self::build_geoarrow_schema(&schema, &geo_columns))
        } else {
            schema
        };

        Ok(Self {
            inner,
            schema,
            geo_columns,
            schema_finalized: false,
            use_geoarrow,
        })
    }

    /// Detect columns that are geometry structs: Struct { srid: Int32, wkb: Binary }.
    fn detect_geometry_columns(schema: &Schema) -> Vec<usize> {
        schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| Self::is_geometry_struct(f.data_type()))
            .map(|(i, _)| i)
            .collect()
    }

    /// Check whether a DataType is a Databricks geometry struct.
    fn is_geometry_struct(dt: &DataType) -> bool {
        if let DataType::Struct(fields) = dt {
            if fields.len() == 2 {
                let has_srid = fields
                    .iter()
                    .any(|f| f.name() == "srid" && f.data_type() == &DataType::Int32);
                let has_wkb = fields
                    .iter()
                    .any(|f| f.name() == "wkb" && f.data_type() == &DataType::Binary);
                return has_srid && has_wkb;
            }
        }
        false
    }

    /// Build a new schema replacing geometry struct columns with GeoArrow WKB Binary.
    fn build_geoarrow_schema(schema: &Schema, geo_columns: &[usize]) -> Schema {
        let fields: Vec<Field> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if geo_columns.contains(&i) {
                    let mut metadata = HashMap::new();
                    metadata.insert(
                        "ARROW:extension:name".to_string(),
                        "geoarrow.wkb".to_string(),
                    );
                    Field::new(f.name(), DataType::Binary, f.is_nullable())
                        .with_metadata(metadata)
                } else {
                    f.as_ref().clone()
                }
            })
            .collect();
        Schema::new(fields).with_metadata(schema.metadata().clone())
    }

    /// Finalize schema with CRS info extracted from the first batch's SRID values.
    fn finalize_schema_with_crs(&mut self, batch: &RecordBatch) {
        if self.schema_finalized || self.geo_columns.is_empty() {
            return;
        }
        self.schema_finalized = true;

        let mut fields: Vec<Field> = self.schema.fields().iter().map(|f| f.as_ref().clone()).collect();

        for &col_idx in &self.geo_columns {
            if let Some(struct_col) = batch.column(col_idx).as_any().downcast_ref::<StructArray>() {
                // Find the srid column
                if let Some(srid_idx) = struct_col
                    .fields()
                    .iter()
                    .position(|f| f.name() == "srid")
                {
                    let srid_array = struct_col.column(srid_idx);
                    if let Some(srid_arr) = srid_array
                        .as_any()
                        .downcast_ref::<arrow_array::Int32Array>()
                    {
                        // Read SRID from first non-null row
                        for row in 0..srid_arr.len() {
                            if !srid_arr.is_null(row) {
                                let srid = srid_arr.value(row);
                                if srid != 0 {
                                    let projjson = Self::srid_to_projjson(srid);
                                    let mut metadata = fields[col_idx].metadata().clone();
                                    metadata.insert(
                                        "ARROW:extension:metadata".to_string(),
                                        projjson,
                                    );
                                    fields[col_idx] = fields[col_idx]
                                        .clone()
                                        .with_metadata(metadata);
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }

        self.schema = Arc::new(
            Schema::new(fields).with_metadata(self.schema.metadata().clone()),
        );
    }

    /// Convert an SRID to a PROJJSON string for GeoArrow metadata.
    fn srid_to_projjson(srid: i32) -> String {
        // Wrap the CRS info as GeoArrow extension metadata JSON
        let crs_json = format!(
            r#"{{"$schema":"https://proj.org/schemas/v0.7/projjson.schema.json","type":"GeographicCRS","name":"WGS 84","id":{{"authority":"EPSG","code":{}}}}}"#,
            srid
        );
        format!(r#"{{"crs":{}}}"#, crs_json)
    }

    /// Transform a batch by extracting the wkb child from geometry struct columns.
    fn transform_batch_for_geoarrow(
        batch: &RecordBatch,
        geo_columns: &[usize],
        schema: &SchemaRef,
    ) -> std::result::Result<RecordBatch, ArrowError> {
        let columns: Vec<Arc<dyn Array>> = batch
            .columns()
            .iter()
            .enumerate()
            .map(|(i, col)| {
                if geo_columns.contains(&i) {
                    // Extract the wkb child from the struct
                    if let Some(struct_col) = col.as_any().downcast_ref::<StructArray>() {
                        if let Some(wkb_idx) = struct_col
                            .fields()
                            .iter()
                            .position(|f| f.name() == "wkb")
                        {
                            return struct_col.column(wkb_idx).clone();
                        }
                    }
                    // Fallback: return original column if extraction fails
                    col.clone()
                } else {
                    col.clone()
                }
            })
            .collect();

        RecordBatch::try_new(schema.clone(), columns)
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
            Ok(Some(batch)) => {
                if !self.geo_columns.is_empty() {
                    // Finalize schema with CRS from first batch
                    if !self.schema_finalized {
                        self.finalize_schema_with_crs(&batch);
                    }
                    Some(Self::transform_batch_for_geoarrow(
                        &batch,
                        &self.geo_columns,
                        &self.schema,
                    ))
                } else {
                    Some(Ok(batch))
                }
            }
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
    use crate::reader::test_utils::{ErrorReader, MockReader, SchemaErrorReader};
    use arrow_array::StringArray;

    fn make_test_batch(values: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))]).unwrap()
    }

    // --- ResultReaderAdapter tests ---

    #[test]
    fn test_adapter_schema() {
        let batch = make_test_batch(vec!["a"]);
        let reader: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch]));
        let adapter = ResultReaderAdapter::new(reader, false).unwrap();
        let schema = arrow_array::RecordBatchReader::schema(&adapter);
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "name");
    }

    #[test]
    fn test_adapter_iterates_batches() {
        let batch1 = make_test_batch(vec!["a", "b"]);
        let batch2 = make_test_batch(vec!["c"]);
        let reader: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch1, batch2]));
        let mut adapter = ResultReaderAdapter::new(reader, false).unwrap();

        let first = adapter.next().unwrap().unwrap();
        assert_eq!(first.num_rows(), 2);

        let second = adapter.next().unwrap().unwrap();
        assert_eq!(second.num_rows(), 1);

        assert!(adapter.next().is_none());
    }

    #[test]
    fn test_adapter_empty_reader() {
        let reader: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![]));
        let mut adapter = ResultReaderAdapter::new(reader, false).unwrap();
        assert!(adapter.next().is_none());
    }

    #[test]
    fn test_adapter_propagates_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let reader: Box<dyn ResultReader + Send> = Box::new(ErrorReader { schema });
        let mut adapter = ResultReaderAdapter::new(reader, false).unwrap();

        let result = adapter.next().unwrap();
        assert!(result.is_err());
    }

    #[test]
    fn test_adapter_schema_error() {
        let reader: Box<dyn ResultReader + Send> = Box::new(SchemaErrorReader);
        let result = ResultReaderAdapter::new(reader, false);
        assert!(result.is_err());
    }

    // --- EmptyReader tests ---

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
