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

//! Arrow IPC parsing utilities for CloudFetch responses.
//!
//! CloudFetch downloads return Arrow IPC Streaming format bytes from cloud storage.
//! This module handles parsing these bytes, including optional LZ4 decompression.

use crate::error::{DatabricksErrorHelper, Result};
use crate::types::sea::CompressionCodec;
use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use driverbase::error::ErrorHelper;
use lz4_flex::frame::FrameDecoder;
use std::io::{Cursor, Read};

/// Parse CloudFetch response into RecordBatches.
///
/// The response is Arrow IPC streaming format, optionally LZ4 compressed.
/// The compression codec is specified in the ResultManifest from the initial
/// statement execution response (manifest.result_compression field).
/// A single chunk may contain multiple RecordBatches.
///
/// # Arguments
/// * `data` - Raw bytes from the CloudFetch download
/// * `compression` - Compression codec used (from manifest.result_compression)
///
/// # Returns
/// Vec of RecordBatches parsed from the Arrow IPC stream
///
/// # Errors
/// - If LZ4 decompression fails
/// - If Arrow IPC parsing fails
pub fn parse_cloudfetch_response(
    data: &[u8],
    compression: CompressionCodec,
) -> Result<Vec<RecordBatch>> {
    // 1. Decompress if needed
    let decompressed: Vec<u8>;
    let bytes: &[u8] = match compression {
        CompressionCodec::Lz4Frame => {
            let mut decoder = FrameDecoder::new(Cursor::new(data));
            let mut buf = Vec::new();
            decoder.read_to_end(&mut buf).map_err(|e| {
                DatabricksErrorHelper::io().message(format!("LZ4 decompression failed: {}", e))
            })?;
            decompressed = buf;
            &decompressed
        }
        CompressionCodec::None => data,
    };

    // 2. Parse Arrow IPC stream
    let cursor = Cursor::new(bytes);
    let reader = StreamReader::try_new(cursor, None).map_err(|e| {
        DatabricksErrorHelper::io().message(format!("Failed to create Arrow IPC reader: {}", e))
    })?;

    // 3. Collect all batches from the stream
    let batches: Vec<RecordBatch> = reader
        .into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| {
            DatabricksErrorHelper::io().message(format!("Failed to read Arrow batches: {}", e))
        })?;

    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field, Schema};
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
    fn test_parse_uncompressed_arrow_ipc() {
        let batch = create_test_batch(100);
        let ipc_data = create_test_arrow_ipc(&[batch.clone()]);

        let result = parse_cloudfetch_response(&ipc_data, CompressionCodec::None).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 100);
        assert_eq!(result[0].num_columns(), 2);
    }

    #[test]
    fn test_parse_multiple_batches() {
        let batch1 = create_test_batch(50);
        let batch2 = create_test_batch(30);
        let ipc_data = create_test_arrow_ipc(&[batch1, batch2]);

        let result = parse_cloudfetch_response(&ipc_data, CompressionCodec::None).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].num_rows(), 50);
        assert_eq!(result[1].num_rows(), 30);
    }

    #[test]
    fn test_parse_compressed_arrow_ipc() {
        use lz4_flex::frame::FrameEncoder;
        use std::io::Write;

        let batch = create_test_batch(100);
        let ipc_data = create_test_arrow_ipc(&[batch.clone()]);

        // Compress with LZ4 frame format
        let mut compressed = Vec::new();
        {
            let mut encoder = FrameEncoder::new(&mut compressed);
            encoder.write_all(&ipc_data).unwrap();
            encoder.finish().unwrap();
        }

        let result = parse_cloudfetch_response(&compressed, CompressionCodec::Lz4Frame).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 100);
    }

    #[test]
    fn test_parse_invalid_data() {
        let invalid_data = b"this is not valid arrow ipc data";

        let result = parse_cloudfetch_response(invalid_data, CompressionCodec::None);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_compressed_data() {
        let invalid_data = b"this is not valid lz4 compressed data";

        let result = parse_cloudfetch_response(invalid_data, CompressionCodec::Lz4Frame);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_empty_stream() {
        // Create valid but empty Arrow IPC stream
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let mut buffer = Vec::new();

        {
            let mut writer = StreamWriter::try_new(&mut buffer, &schema).unwrap();
            writer.finish().unwrap();
        }

        let result = parse_cloudfetch_response(&buffer, CompressionCodec::None).unwrap();
        assert!(result.is_empty());
    }
}
