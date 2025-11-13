/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Databricks.StatementExecution;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Reader
{
    /// <summary>
    /// Reader for inline Arrow result data from Databricks Statement Execution REST API.
    /// Handles INLINE disposition where results are embedded as base64-encoded Arrow IPC stream in response.
    /// </summary>
    internal sealed class InlineReader : IArrowArrayStream
    {
        private readonly List<ResultChunk> _chunks;
        private int _currentChunkIndex;
        private ArrowStreamReader? _currentReader;
        private Schema? _schema;
        private bool _isDisposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="InlineReader"/> class.
        /// </summary>
        /// <param name="manifest">The result manifest containing inline data chunks.</param>
        /// <exception cref="ArgumentNullException">Thrown when manifest is null.</exception>
        /// <exception cref="ArgumentException">Thrown when manifest format is not arrow_stream.</exception>
        public InlineReader(ResultManifest manifest)
        {
            if (manifest == null)
            {
                throw new ArgumentNullException(nameof(manifest));
            }

            if (manifest.Format != "arrow_stream")
            {
                throw new ArgumentException(
                    $"InlineReader only supports arrow_stream format, but received: {manifest.Format}",
                    nameof(manifest));
            }

            // Filter chunks that have attachment data
            _chunks = manifest.Chunks?
                .Where(c => c.Attachment != null && c.Attachment.Length > 0)
                .OrderBy(c => c.ChunkIndex)
                .ToList() ?? new List<ResultChunk>();

            _currentChunkIndex = 0;
        }

        /// <summary>
        /// Gets the Arrow schema for the result set.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when schema cannot be determined.</exception>
        public Schema Schema
        {
            get
            {
                ThrowIfDisposed();

                if (_schema != null)
                {
                    return _schema;
                }

                // Extract schema from the first chunk
                if (_chunks.Count == 0)
                {
                    throw new InvalidOperationException("No chunks with attachment data found in result manifest");
                }

                // Create a reader for the first chunk to extract the schema
                var firstChunk = _chunks[0];
                using (var stream = new MemoryStream(firstChunk.Attachment!))
                using (var reader = new ArrowStreamReader(stream))
                {
                    _schema = reader.Schema;
                }

                return _schema;
            }
        }

        /// <summary>
        /// Reads the next record batch from the inline result data.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The next record batch, or null if there are no more batches.</returns>
        public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();

            while (true)
            {
                // If we have a current reader, try to read the next batch
                if (_currentReader != null)
                {
                    RecordBatch? batch = await _currentReader.ReadNextRecordBatchAsync(cancellationToken);
                    if (batch != null)
                    {
                        return batch;
                    }
                    else
                    {
                        // Clean up the current reader
                        _currentReader.Dispose();
                        _currentReader = null;
                        _currentChunkIndex++;
                    }
                }

                // If we don't have a current reader, move to the next chunk
                if (_currentChunkIndex >= _chunks.Count)
                {
                    // No more chunks
                    return null;
                }

                // Create a reader for the current chunk
                var chunk = _chunks[_currentChunkIndex];
                if (chunk.Attachment == null || chunk.Attachment.Length == 0)
                {
                    // Skip chunks without attachment data
                    _currentChunkIndex++;
                    continue;
                }

                try
                {
                    var stream = new MemoryStream(chunk.Attachment);
                    _currentReader = new ArrowStreamReader(stream, leaveOpen: false);

                    // Ensure schema is set
                    if (_schema == null)
                    {
                        _schema = _currentReader.Schema;
                    }

                    // Continue to read the first batch from this chunk
                    continue;
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException(
                        $"Failed to read Arrow stream from chunk {chunk.ChunkIndex}: {ex.Message}",
                        ex);
                }
            }
        }

        /// <summary>
        /// Disposes the reader and releases all resources.
        /// </summary>
        public void Dispose()
        {
            if (!_isDisposed)
            {
                if (_currentReader != null)
                {
                    _currentReader.Dispose();
                    _currentReader = null;
                }

                _isDisposed = true;
            }
        }

        private void ThrowIfDisposed()
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException(nameof(InlineReader));
            }
        }
    }
}
