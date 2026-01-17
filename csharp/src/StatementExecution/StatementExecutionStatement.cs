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
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Reader.CloudFetch;
using AdbcDrivers.Databricks.Telemetry.TagDefinitions;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tracing;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

namespace AdbcDrivers.Databricks.StatementExecution
{
    /// <summary>
    /// Statement implementation using the Databricks Statement Execution REST API.
    /// Handles query execution, polling, and result retrieval.
    /// Extends TracingStatement for consistent tracing support with Thrift protocol.
    /// </summary>
    internal class StatementExecutionStatement : TracingStatement
    {
        private readonly IStatementExecutionClient _client;
        private readonly string? _sessionId;
        private readonly string _warehouseId;
        private readonly string? _catalog;
        private readonly string? _schema;

        // Result configuration
        private readonly string _resultDisposition;
        private readonly string _resultFormat;
        private readonly string? _resultCompression;
        private readonly int _waitTimeoutSeconds;
        private readonly int _pollingIntervalMs;

        // Connection properties for CloudFetch configuration
        private readonly IReadOnlyDictionary<string, string> _properties;

        // Memory pooling
        private readonly Microsoft.IO.RecyclableMemoryStreamManager _recyclableMemoryStreamManager;
        private readonly System.Buffers.ArrayPool<byte> _lz4BufferPool;

        // HTTP client for CloudFetch downloads
        private readonly HttpClient _httpClient;

        // Statement state
        private string? _currentStatementId;
        private string? _sqlQuery;

        // Telemetry tracking
        private int _pollCount;
        private long _pollLatencyMs;

        public StatementExecutionStatement(
            IStatementExecutionClient client,
            string? sessionId,
            string warehouseId,
            string? catalog,
            string? schema,
            string resultDisposition,
            string resultFormat,
            string? resultCompression,
            int waitTimeoutSeconds,
            int pollingIntervalMs,
            IReadOnlyDictionary<string, string> properties,
            Microsoft.IO.RecyclableMemoryStreamManager recyclableMemoryStreamManager,
            System.Buffers.ArrayPool<byte> lz4BufferPool,
            HttpClient httpClient,
            StatementExecutionConnection connection)
            : base(connection) // Initialize TracingStatement base class with TracingConnection
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _sessionId = sessionId;
            _warehouseId = warehouseId ?? throw new ArgumentNullException(nameof(warehouseId));
            _catalog = catalog;
            _schema = schema;
            _resultDisposition = resultDisposition ?? throw new ArgumentNullException(nameof(resultDisposition));
            _resultFormat = resultFormat ?? throw new ArgumentNullException(nameof(resultFormat));
            _resultCompression = resultCompression;
            _waitTimeoutSeconds = waitTimeoutSeconds;
            _pollingIntervalMs = pollingIntervalMs;
            _properties = properties ?? throw new ArgumentNullException(nameof(properties));
            _recyclableMemoryStreamManager = recyclableMemoryStreamManager ?? throw new ArgumentNullException(nameof(recyclableMemoryStreamManager));
            _lz4BufferPool = lz4BufferPool ?? throw new ArgumentNullException(nameof(lz4BufferPool));
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        }

        /// <summary>
        /// Gets or sets the SQL query to execute.
        /// </summary>
        public override string? SqlQuery
        {
            get => _sqlQuery;
            set => _sqlQuery = value;
        }

        /// <summary>
        /// Executes the query and returns a result set.
        /// </summary>
        public override QueryResult ExecuteQuery()
        {
            return ExecuteQueryAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Executes the query asynchronously and returns a result set.
        /// </summary>
        public async Task<QueryResult> ExecuteQueryAsync(CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(_sqlQuery))
            {
                throw new InvalidOperationException("SQL query is required");
            }

            // Build the execute statement request
            // Note: warehouse_id is always required by the Databricks Statement Execution API
            // Note: catalog/schema cannot be set when session_id is provided (session has context)
            var request = new ExecuteStatementRequest
            {
                Statement = _sqlQuery,
                WarehouseId = _warehouseId,
                SessionId = _sessionId,
                Catalog = string.IsNullOrEmpty(_sessionId) ? _catalog : null,
                Schema = string.IsNullOrEmpty(_sessionId) ? _schema : null,
                Disposition = _resultDisposition,
                Format = _resultFormat,
                ResultCompression = _resultCompression,
                WaitTimeout = $"{_waitTimeoutSeconds}s",
                OnWaitTimeout = "CONTINUE"
            };

            // Execute the statement
            var response = await _client.ExecuteStatementAsync(request, cancellationToken).ConfigureAwait(false);
            _currentStatementId = response.StatementId;

            // Handle query status according to Databricks API documentation:
            // PENDING: waiting for warehouse - continue polling
            // RUNNING: running - continue polling
            // SUCCEEDED: execution was successful, result data available for fetch
            // FAILED: execution failed; reason for failure described in accompanying error message
            // CANCELED: user canceled; can come from explicit cancel call, or timeout with on_wait_timeout=CANCEL
            // CLOSED: execution successful, and statement closed; result no longer available for fetch
            var state = response.Status?.State;
            if (state == "PENDING" || state == "RUNNING")
            {
                response = await PollUntilCompleteAsync(response.StatementId, cancellationToken).ConfigureAwait(false);
                state = response.Status?.State;
            }

            // Check for terminal error states
            if (state == "FAILED")
            {
                var error = response.Status?.Error;
                throw new AdbcException($"Statement execution failed: {error?.Message ?? "Unknown error"} (Error Code: {error?.ErrorCode})");
            }
            if (state == "CANCELED")
            {
                throw new AdbcException("Statement execution was canceled");
            }
            if (state == "CLOSED")
            {
                throw new AdbcException("Statement was closed before results could be retrieved");
            }

            // Check for truncated results warning
            if (response.Manifest?.Truncated == true)
            {
                Activity.Current?.AddEvent(new ActivityEvent("statement.results_truncated",
                    tags: new ActivityTagsCollection
                    {
                        { "total_row_count", response.Manifest.TotalRowCount },
                        { "total_byte_count", response.Manifest.TotalByteCount }
                    }));
            }

            // Add telemetry tags for result metrics
            AddResultTelemetryTags(response);

            // Create appropriate reader based on result disposition
            IArrowArrayStream reader = CreateReader(response, cancellationToken);

            // Get schema from reader
            var schema = reader.Schema;

            // Return query result - use -1 if row count is not available
            long rowCount = response.Manifest?.TotalRowCount ?? -1;
            return new QueryResult(rowCount, reader);
        }

        /// <summary>
        /// Adds telemetry tags for statement execution result metrics.
        /// </summary>
        private void AddResultTelemetryTags(ExecuteStatementResponse response)
        {
            var activity = Activity.Current;
            if (activity == null) return;

            // Add result format tag
            bool hasExternalLinks = (response.Manifest?.Chunks != null &&
                                     response.Manifest.Chunks.Count > 0 &&
                                     response.Manifest.Chunks[0].ExternalLinks != null &&
                                     response.Manifest.Chunks[0].ExternalLinks.Count > 0) ||
                                    (response.Result?.ExternalLinks != null &&
                                     response.Result.ExternalLinks.Count > 0);

            string resultFormat = hasExternalLinks ? "cloudfetch" : "inline";
            activity.SetTag(StatementExecutionEvent.ResultFormat, resultFormat);

            // Add chunk count tag
            int chunkCount = response.Manifest?.Chunks?.Count ?? 0;
            if (chunkCount > 0)
            {
                activity.SetTag(StatementExecutionEvent.ResultChunkCount, chunkCount);
            }

            // Add bytes downloaded tag (total byte count from manifest)
            long? totalBytes = response.Manifest?.TotalByteCount;
            if (totalBytes.HasValue)
            {
                activity.SetTag(StatementExecutionEvent.ResultBytesDownloaded, totalBytes.Value);
            }

            // Add compression enabled tag
            bool compressionEnabled = !string.IsNullOrEmpty(response.Manifest?.ResultCompression);
            activity.SetTag(StatementExecutionEvent.ResultCompressionEnabled, compressionEnabled);

            // Add polling metrics tags
            if (_pollCount > 0)
            {
                activity.SetTag(StatementExecutionEvent.PollCount, _pollCount);
                activity.SetTag(StatementExecutionEvent.PollLatencyMs, _pollLatencyMs);
            }
        }

        /// <summary>
        /// Polls the statement until it reaches a terminal state.
        /// Terminal states: SUCCEEDED, FAILED, CANCELED, CLOSED
        /// Non-terminal states: PENDING, RUNNING
        /// </summary>
        private async Task<ExecuteStatementResponse> PollUntilCompleteAsync(string statementId, CancellationToken cancellationToken)
        {
            while (true)
            {
                // Check for cancellation before each polling iteration
                cancellationToken.ThrowIfCancellationRequested();

                // Wait for polling interval
                await Task.Delay(_pollingIntervalMs, cancellationToken).ConfigureAwait(false);

                // Check for cancellation after delay
                cancellationToken.ThrowIfCancellationRequested();

                // Track poll count and latency for telemetry
                var pollStartTime = Stopwatch.GetTimestamp();
                _pollCount++;

                // Get statement status
                var response = await _client.GetStatementAsync(statementId, cancellationToken).ConfigureAwait(false);

                // Calculate poll latency
                var pollLatency = Stopwatch.GetElapsedTime(pollStartTime);
                _pollLatencyMs += (long)pollLatency.TotalMilliseconds;

                // Convert GetStatementResponse to ExecuteStatementResponse
                var executeResponse = new ExecuteStatementResponse
                {
                    StatementId = response.StatementId,
                    Status = response.Status,
                    Manifest = response.Manifest,
                    Result = response.Result
                };

                // Check if reached a terminal state
                var state = response.Status?.State;
                if (state == "SUCCEEDED" ||
                    state == "FAILED" ||
                    state == "CANCELED" ||
                    state == "CLOSED")
                {
                    return executeResponse;
                }

                // Continue polling for PENDING and RUNNING states
            }
        }

        /// <summary>
        /// Creates an appropriate reader based on the result disposition.
        /// </summary>
        private IArrowArrayStream CreateReader(ExecuteStatementResponse response, CancellationToken cancellationToken)
        {
            if (response.Manifest == null)
            {
                // No results - return empty reader
                return new EmptyArrowArrayStream();
            }

            // Check for external links in manifest chunks or result
            bool hasExternalLinksInChunks = response.Manifest.Chunks != null &&
                                  response.Manifest.Chunks.Count > 0 &&
                                  response.Manifest.Chunks[0].ExternalLinks != null &&
                                  response.Manifest.Chunks[0].ExternalLinks.Count > 0;
            bool hasExternalLinksInResult = response.Result != null &&
                                  response.Result.ExternalLinks != null &&
                                  response.Result.ExternalLinks.Count > 0;
            bool hasExternalLinks = hasExternalLinksInChunks || hasExternalLinksInResult;

            if (hasExternalLinks)
            {
                // Use CloudFetch for external links
                return CreateCloudFetchReader(response);
            }
            else if (response.Result != null && response.Result.Attachment != null && response.Result.Attachment.Length > 0)
            {
                // Check if data is LZ4 compressed
                bool isLz4Compressed = response.Manifest?.ResultCompression?.ToUpperInvariant() == "LZ4_FRAME";

                // Inline results - may be split across multiple chunks
                int totalChunks = response.Manifest?.Chunks?.Count ?? 1;
                return new InlineArrowStreamReader(_client, _currentStatementId!, response.Result.Attachment,
                    isLz4Compressed, totalChunks, _lz4BufferPool, cancellationToken);
            }
            else
            {
                // No inline data - return empty reader
                return new EmptyArrowArrayStream();
            }
        }

        /// <summary>
        /// Creates a CloudFetch reader for external link results.
        /// </summary>
        private IArrowArrayStream CreateCloudFetchReader(ExecuteStatementResponse response)
        {
            var manifest = response.Manifest!;

            // Build schema from manifest
            var schema = GetSchemaFromManifest(manifest);

            // The Statement Execution API response structure:
            // - manifest.chunks: Array of ChunkInfo with metadata for ALL chunks (row counts, offsets, etc.)
            // - result.external_links: Presigned URL for chunk 0 only (subsequent chunks fetched via GetChunk API)
            //
            // We pass the initial external links separately to avoid mutating the manifest.
            var initialExternalLinks = response.Result?.ExternalLinks;

            return CloudFetchReaderFactory.CreateStatementExecutionReader(
                _client,
                _currentStatementId!,
                schema,
                manifest,
                initialExternalLinks,
                _httpClient,
                _properties,
                _recyclableMemoryStreamManager,
                _lz4BufferPool,
                this); // Pass statement as ITracingStatement (via TracingStatement base class)
        }

        /// <summary>
        /// Extracts the Arrow schema from the result manifest.
        /// </summary>
        private Schema GetSchemaFromManifest(ResultManifest manifest)
        {
            if (manifest.Schema == null || manifest.Schema.Columns == null || manifest.Schema.Columns.Count == 0)
            {
                throw new AdbcException("Result manifest does not contain schema information");
            }

            var fields = new List<Field>();
            foreach (var column in manifest.Schema.Columns)
            {
                var arrowType = MapDatabricksTypeToArrowType(column.TypeName);
                fields.Add(new Field(column.Name, arrowType, true));
            }

            return new Schema(fields, null);
        }

        /// <summary>
        /// Maps Databricks SQL type names to Arrow types.
        /// </summary>
        private IArrowType MapDatabricksTypeToArrowType(string typeName)
        {
            // Handle parameterized types (e.g., DECIMAL(10,2), VARCHAR(100))
            var baseType = typeName.Split('(')[0].ToUpperInvariant();

            return baseType switch
            {
                "BOOLEAN" => BooleanType.Default,
                "BYTE" or "TINYINT" => Int8Type.Default,
                "SHORT" or "SMALLINT" => Int16Type.Default,
                "INT" or "INTEGER" => Int32Type.Default,
                "LONG" or "BIGINT" => Int64Type.Default,
                "FLOAT" or "REAL" => FloatType.Default,
                "DOUBLE" => DoubleType.Default,
                "DECIMAL" or "NUMERIC" => ParseDecimalType(typeName),
                "STRING" or "VARCHAR" or "CHAR" => StringType.Default,
                "BINARY" or "VARBINARY" => BinaryType.Default,
                "DATE" => Date32Type.Default,
                "TIMESTAMP" or "TIMESTAMP_NTZ" or "TIMESTAMP_LTZ" => TimestampType.Default,
                "INTERVAL" => StringType.Default, // Intervals as strings for now
                "ARRAY" => StringType.Default, // Complex types as strings for now
                "MAP" => StringType.Default,
                "STRUCT" => StringType.Default,
                "NULL" or "VOID" => NullType.Default,
                _ => StringType.Default // Default to string for unknown types
            };
        }

        /// <summary>
        /// Parses a DECIMAL type to determine precision and scale.
        /// </summary>
        private IArrowType ParseDecimalType(string typeName)
        {
            // Default precision and scale
            int precision = 38;
            int scale = 18;

            // Try to parse DECIMAL(precision, scale)
            var match = System.Text.RegularExpressions.Regex.Match(typeName, @"DECIMAL\((\d+),\s*(\d+)\)", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            if (match.Success)
            {
                precision = int.Parse(match.Groups[1].Value);
                scale = int.Parse(match.Groups[2].Value);
            }

            return new Decimal128Type(precision, scale);
        }

        /// <summary>
        /// Executes an update query and returns the number of affected rows.
        /// </summary>
        public override UpdateResult ExecuteUpdate()
        {
            return ExecuteUpdateAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Executes an update query asynchronously and returns the number of affected rows.
        /// </summary>
        public async Task<UpdateResult> ExecuteUpdateAsync(CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(_sqlQuery))
            {
                throw new InvalidOperationException("SQL query is required");
            }

            // Build the execute statement request
            // Note: catalog/schema cannot be set when session_id is provided (session has context)
            var request = new ExecuteStatementRequest
            {
                Statement = _sqlQuery,
                WarehouseId = _warehouseId,
                SessionId = _sessionId,
                Catalog = string.IsNullOrEmpty(_sessionId) ? _catalog : null,
                Schema = string.IsNullOrEmpty(_sessionId) ? _schema : null,
                Disposition = _resultDisposition,
                Format = _resultFormat,
                ResultCompression = _resultCompression,
                WaitTimeout = $"{_waitTimeoutSeconds}s",
                OnWaitTimeout = "CONTINUE"
            };

            // Execute the statement
            var response = await _client.ExecuteStatementAsync(request, cancellationToken).ConfigureAwait(false);
            _currentStatementId = response.StatementId;

            // Handle query status - poll until complete
            var state = response.Status?.State;
            if (state == "PENDING" || state == "RUNNING")
            {
                response = await PollUntilCompleteAsync(response.StatementId, cancellationToken).ConfigureAwait(false);
                state = response.Status?.State;
            }

            // Check for terminal error states
            if (state == "FAILED")
            {
                var error = response.Status?.Error;
                throw new AdbcException($"Statement execution failed: {error?.Message ?? "Unknown error"} (Error Code: {error?.ErrorCode})");
            }
            if (state == "CANCELED")
            {
                throw new AdbcException("Statement execution was canceled");
            }
            if (state == "CLOSED")
            {
                throw new AdbcException("Statement was closed before results could be retrieved");
            }

            // For updates, we don't need to read the results - just return the row count
            long rowCount = response.Manifest?.TotalRowCount ?? 0;
            return new UpdateResult(rowCount);
        }

        /// <summary>
        /// Disposes the statement and cancels/closes any active statement.
        /// </summary>
        public override void Dispose()
        {
            if (_currentStatementId != null)
            {
                try
                {
                    // Close statement synchronously during dispose
                    Activity.Current?.AddEvent(new ActivityEvent("statement.dispose",
                        tags: new ActivityTagsCollection
                        {
                            { "statement_id", _currentStatementId }
                        }));
                    _client.CloseStatementAsync(_currentStatementId, CancellationToken.None).GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    // Best effort - ignore errors during dispose
                    Activity.Current?.AddEvent(new ActivityEvent("statement.dispose.error",
                        tags: new ActivityTagsCollection
                        {
                            { "error", ex.Message }
                        }));
                }
                finally
                {
                    _currentStatementId = null;
                }
            }
        }

        /// <summary>
        /// Empty Arrow array stream for queries with no results.
        /// </summary>
        private class EmptyArrowArrayStream : IArrowArrayStream
        {
            public Schema Schema => new Schema.Builder().Build();

            public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                return new ValueTask<RecordBatch?>((RecordBatch?)null);
            }

            public void Dispose()
            {
                // Nothing to dispose
            }
        }

        /// <summary>
        /// Reader for inline results in Arrow IPC stream format.
        /// Handles both single-chunk and multi-chunk inline results by fetching
        /// all chunks and concatenating them into a single Arrow stream.
        /// Supports LZ4_FRAME compressed data.
        /// </summary>
        private class InlineArrowStreamReader : IArrowArrayStream
        {
            private readonly ArrowStreamReader _streamReader;
            private readonly System.IO.MemoryStream _memoryStream;
            private bool _disposed;

            public InlineArrowStreamReader(
                IStatementExecutionClient client,
                string statementId,
                byte[] firstChunkData,
                bool isLz4Compressed,
                int totalChunks,
                System.Buffers.ArrayPool<byte> bufferPool,
                CancellationToken cancellationToken)
            {
                if (firstChunkData == null || firstChunkData.Length == 0)
                {
                    throw new ArgumentException("First chunk data cannot be null or empty", nameof(firstChunkData));
                }

                // Fetch and concatenate all chunks
                var allData = FetchAllChunksAsync(client, statementId, firstChunkData, isLz4Compressed, totalChunks, bufferPool, cancellationToken).GetAwaiter().GetResult();

                _memoryStream = new System.IO.MemoryStream(allData);
                _streamReader = new ArrowStreamReader(_memoryStream);
            }

            public Schema Schema => _streamReader.Schema;

            public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(InlineArrowStreamReader));
                }

                return await _streamReader.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false);
            }

            public void Dispose()
            {
                if (!_disposed)
                {
                    _streamReader?.Dispose();
                    _memoryStream?.Dispose();
                    _disposed = true;
                }
            }

            private static async Task<byte[]> FetchAllChunksAsync(
                IStatementExecutionClient client,
                string statementId,
                byte[] firstChunkData,
                bool isLz4Compressed,
                int totalChunks,
                System.Buffers.ArrayPool<byte> bufferPool,
                CancellationToken cancellationToken)
            {
                // Start with the first chunk (already have it inline)
                var chunks = new List<byte[]>();

                // Decompress first chunk if needed
                if (isLz4Compressed)
                {
                    var decompressed = Lz4Utilities.DecompressLz4(firstChunkData, bufferPool);
                    chunks.Add(decompressed.ToArray());
                }
                else
                {
                    chunks.Add(firstChunkData);
                }

                // Fetch remaining chunks (chunks are 0-indexed, chunk 0 is already inline)
                for (int i = 1; i < totalChunks; i++)
                {
                    var chunkResult = await client.GetResultChunkAsync(statementId, i, cancellationToken).ConfigureAwait(false);

                    if (chunkResult.Attachment != null && chunkResult.Attachment.Length > 0)
                    {
                        if (isLz4Compressed)
                        {
                            var decompressed = Lz4Utilities.DecompressLz4(chunkResult.Attachment, bufferPool);
                            chunks.Add(decompressed.ToArray());
                        }
                        else
                        {
                            chunks.Add(chunkResult.Attachment);
                        }
                    }
                }

                // Concatenate all chunks
                int totalLength = chunks.Sum(c => c.Length);
                byte[] result = new byte[totalLength];
                int offset = 0;
                foreach (var chunk in chunks)
                {
                    Buffer.BlockCopy(chunk, 0, result, offset, chunk.Length);
                    offset += chunk.Length;
                }

                return result;
            }
        }

        // TracingStatement implementation
        public override string AssemblyVersion => GetType().Assembly.GetName().Version?.ToString() ?? "1.0.0";
        public override string AssemblyName => "AdbcDrivers.Databricks";
    }
}
