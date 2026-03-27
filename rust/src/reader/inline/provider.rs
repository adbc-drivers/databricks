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

//! InlineArrowProvider for handling inline Arrow results.

use crate::error::{DatabricksErrorHelper, Result};
use crate::reader::cloudfetch::parse_arrow_ipc;
use crate::types::sea::CompressionCodec;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use driverbase::error::ErrorHelper;
use std::collections::VecDeque;

/// Provider for inline Arrow results.
///
/// Unlike CloudFetch, inline results are:
/// - Always a single chunk
/// - Already present in the response (no download needed)
/// - Typically smaller datasets
///
/// This provider parses the Arrow IPC data upfront and holds the
/// resulting RecordBatches in memory for iteration.
#[derive(Debug)]
pub struct InlineArrowProvider {
    /// Pre-parsed record batches from the inline attachment
    batches: VecDeque<RecordBatch>,
    /// Schema extracted from the first batch
    schema: Option<SchemaRef>,
}

impl InlineArrowProvider {
    /// Create a new provider from raw Arrow IPC bytes.
    ///
    /// Decompresses (if needed) and parses the Arrow IPC stream into RecordBatches.
    ///
    /// # Arguments
    /// * `attachment` - Raw Arrow IPC bytes (decoded from base64)
    /// * `compression` - Compression codec used (from manifest.result_compression)
    ///
    /// # Returns
    /// A new provider with parsed batches ready for iteration
    ///
    /// # Errors
    /// - If decompression fails
    /// - If Arrow IPC parsing fails
    pub fn new(attachment: Vec<u8>, compression: CompressionCodec) -> Result<Self> {
        if attachment.is_empty() {
            tracing::debug!("Empty attachment, creating empty provider");
            return Ok(Self {
                batches: VecDeque::new(),
                schema: None,
            });
        }

        tracing::debug!(
            "Parsing inline Arrow data: {} bytes, compression={:?}",
            attachment.len(),
            compression
        );

        // Reuse the shared Arrow IPC parser (same format as CloudFetch)
        let batches = parse_arrow_ipc(&attachment, compression).map_err(|e| {
            DatabricksErrorHelper::io().message(format!("Failed to parse inline Arrow data: {}", e))
        })?;

        let schema = batches.first().map(|b| b.schema());

        tracing::debug!(
            "Parsed inline Arrow data: {} batches, {} total rows",
            batches.len(),
            batches.iter().map(|b| b.num_rows()).sum::<usize>()
        );

        Ok(Self {
            batches: VecDeque::from(batches),
            schema,
        })
    }

    /// Get the schema of the result set.
    ///
    /// Returns `None` if the result set is empty or schema hasn't been determined yet.
    pub fn schema(&self) -> Option<&SchemaRef> {
        self.schema.as_ref()
    }

    /// Get the next record batch, or `None` if all batches have been consumed.
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.batches.pop_front())
    }

    /// Check if there are more batches available.
    pub fn has_more(&self) -> bool {
        !self.batches.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field, Schema};
    use lz4_flex::frame::FrameEncoder;
    use std::io::Write;
    use std::sync::Arc;

    /// Helper to create test Arrow IPC data
    fn create_test_arrow_ipc(batches: &[RecordBatch]) -> Vec<u8> {
        let schema = batches[0].schema();
        let mut buffer = Vec::new();

        {
            let mut writer = StreamWriter::try_new(&mut buffer, &schema).unwrap();
            for batch in batches {
                writer.write(batch).unwrap();
            }
            writer.finish().unwrap();
        }

        buffer
    }

    /// Helper to create a test RecordBatch
    fn create_test_batch(num_rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let ids: Vec<i32> = (0..num_rows as i32).collect();
        let names: Vec<String> = (0..num_rows).map(|i| format!("name_{}", i)).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_inline_provider_with_uncompressed_data() {
        let batch = create_test_batch(100);
        let ipc_data = create_test_arrow_ipc(&[batch]);

        let mut provider = InlineArrowProvider::new(ipc_data, CompressionCodec::None).unwrap();

        // Check schema
        assert!(provider.schema().is_some());
        let schema = provider.schema().unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");

        // Check batches
        assert!(provider.has_more());
        let batch = provider.next_batch().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 100);
        assert_eq!(batch.num_columns(), 2);

        // No more batches
        assert!(!provider.has_more());
        assert!(provider.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_inline_provider_with_compressed_data() {
        let batch = create_test_batch(100);
        let ipc_data = create_test_arrow_ipc(&[batch]);

        // Compress with LZ4 frame format
        let mut compressed = Vec::new();
        {
            let mut encoder = FrameEncoder::new(&mut compressed);
            encoder.write_all(&ipc_data).unwrap();
            encoder.finish().unwrap();
        }

        let mut provider =
            InlineArrowProvider::new(compressed, CompressionCodec::Lz4Frame).unwrap();

        let batch = provider.next_batch().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 100);
    }

    #[test]
    fn test_inline_provider_with_multiple_batches() {
        let batch1 = create_test_batch(50);
        let batch2 = create_test_batch(30);
        let ipc_data = create_test_arrow_ipc(&[batch1, batch2]);

        let mut provider = InlineArrowProvider::new(ipc_data, CompressionCodec::None).unwrap();

        // First batch
        let batch = provider.next_batch().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 50);

        // Second batch
        let batch = provider.next_batch().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 30);

        // No more
        assert!(provider.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_inline_provider_with_empty_data() {
        let provider = InlineArrowProvider::new(vec![], CompressionCodec::None).unwrap();

        assert!(provider.schema().is_none());
        assert!(!provider.has_more());
    }

    #[test]
    fn test_inline_provider_with_invalid_data() {
        let invalid_data = b"this is not valid arrow ipc data".to_vec();

        let result = InlineArrowProvider::new(invalid_data, CompressionCodec::None);
        assert!(result.is_err());
    }
}
