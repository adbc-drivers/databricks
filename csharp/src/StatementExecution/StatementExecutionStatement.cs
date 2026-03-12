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
using AdbcDrivers.Databricks;
using AdbcDrivers.Databricks.Reader.CloudFetch;
using AdbcDrivers.Databricks.StatementExecution.MetadataCommands;
using AdbcDrivers.Databricks.Result;
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.Hive2;
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
        private readonly int _queryTimeoutSeconds; // 0 = no timeout

        // Connection properties for CloudFetch configuration
        private readonly IReadOnlyDictionary<string, string> _properties;

        // Memory pooling
        private readonly Microsoft.IO.RecyclableMemoryStreamManager _recyclableMemoryStreamManager;
        private readonly System.Buffers.ArrayPool<byte> _lz4BufferPool;

        // HTTP client for CloudFetch downloads
        private readonly HttpClient _httpClient;

        // Complex type configuration
        private readonly bool _enableComplexDatatypeSupport;

        // Connection reference for metadata queries
        private readonly StatementExecutionConnection _connection;

        // Statement state
        private string? _currentStatementId;
        private string? _sqlQuery;

        // Metadata command support
        private bool _isMetadataCommand;
        private bool _escapePatternWildcards;
        private string? _metadataCatalogName;
        private string? _metadataSchemaName;
        private string? _metadataTableName;
        private string? _metadataColumnName;
        private string? _metadataTableTypes;
        private string? _metadataForeignCatalogName;
        private string? _metadataForeignSchemaName;
        private string? _metadataForeignTableName;

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
            : base(connection)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
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
            _queryTimeoutSeconds = PropertyHelper.GetIntPropertyWithValidation(
                properties, ApacheParameters.QueryTimeoutSeconds, 0);
            _recyclableMemoryStreamManager = recyclableMemoryStreamManager ?? throw new ArgumentNullException(nameof(recyclableMemoryStreamManager));
            _lz4BufferPool = lz4BufferPool ?? throw new ArgumentNullException(nameof(lz4BufferPool));
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
            _enableComplexDatatypeSupport = connection.EnableComplexDatatypeSupport;

            // Match Thrift: statement starts with connection's default catalog.
            // When enableMultipleCatalogSupport=true, this is the catalog from config (e.g. "main").
            // When false, _catalog is null (not set from config), matching Thrift behavior.
            _metadataCatalogName = catalog;
        }

        /// <summary>
        /// Gets or sets the SQL query to execute.
        /// </summary>
        public override string? SqlQuery
        {
            get => _sqlQuery;
            set => _sqlQuery = value;
        }

        public override void SetOption(string key, string value)
        {
            switch (key)
            {
                case ApacheParameters.IsMetadataCommand:
                    _isMetadataCommand = bool.TryParse(value, out bool b) && b;
                    break;
                case ApacheParameters.CatalogName:
                    _metadataCatalogName = value;
                    break;
                case ApacheParameters.SchemaName:
                    _metadataSchemaName = value;
                    break;
                case ApacheParameters.TableName:
                    _metadataTableName = value;
                    break;
                case ApacheParameters.ColumnName:
                    _metadataColumnName = value;
                    break;
                case ApacheParameters.ForeignCatalogName:
                    _metadataForeignCatalogName = value;
                    break;
                case ApacheParameters.ForeignSchemaName:
                    _metadataForeignSchemaName = value;
                    break;
                case ApacheParameters.ForeignTableName:
                    _metadataForeignTableName = value;
                    break;
                case ApacheParameters.TableTypes:
                    _metadataTableTypes = value;
                    break;
                case ApacheParameters.EscapePatternWildcards:
                    _escapePatternWildcards = bool.TryParse(value, out bool escape) && escape;
                    break;

                // These options are readonly in SEA (set at connection level).
                // Accept but ignore them to avoid NotImplemented exceptions for compatibility.
                case ApacheParameters.PollTimeMilliseconds:
                case ApacheParameters.BatchSize:
                case ApacheParameters.BatchSizeStopCondition:
                case ApacheParameters.QueryTimeoutSeconds:
                    break;

                // DatabricksStatement-specific options: accept but ignore for now.
                // TODO(PECOBLR-2259): Implement query_tags support for SEA. The SEA API uses a
                // JSON array of {key, value} objects in the executestatement request body,
                // unlike Thrift which sends a string in confOverlay.
                case DatabricksParameters.QueryTags:
                case DatabricksParameters.UseCloudFetch:
                case DatabricksParameters.CanDecompressLz4:
                case DatabricksParameters.MaxBytesPerFile:
                case DatabricksParameters.MaxBytesPerFetchRequest:
                    break;

                case AdbcOptions.Telemetry.TraceParent:
                    SetTraceParent(string.IsNullOrEmpty(value) ? null : value);
                    break;

                default:
                    base.SetOption(key, value);
                    break;
            }
        }

        public override QueryResult ExecuteQuery()
        {
            return ExecuteQueryAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Executes the query asynchronously.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <param name="isMetadataExecution">When true, adds the x-databricks-sea-can-run-fully-sync
        /// header for optimized metadata query execution on the server.</param>
        public async Task<QueryResult> ExecuteQueryAsync(
            CancellationToken cancellationToken = default,
            bool isMetadataExecution = false)
        {
            if (_isMetadataCommand)
            {
                return await ExecuteMetadataCommandAsync(cancellationToken).ConfigureAwait(false);
            }

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
                OnWaitTimeout = "CONTINUE",
                IsMetadata = isMetadataExecution
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
                response = await PollWithTimeoutAsync(response.StatementId, cancellationToken).ConfigureAwait(false);
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

            // Create appropriate reader based on result disposition
            IArrowArrayStream reader = CreateReader(response, cancellationToken);

            // When EnableComplexDatatypeSupport=false (default), serialize complex Arrow types to JSON strings
            // so that SEA behavior matches Thrift (which sets ComplexTypesAsArrow=false).
            if (!_enableComplexDatatypeSupport)
            {
                reader = new ComplexTypeSerializingStream(reader);
            }

            // Get schema from reader
            var schema = reader.Schema;

            // Return query result - use 0 if row count is not available
            long rowCount = response.Manifest?.TotalRowCount ?? 0;
            return new QueryResult(rowCount, reader);
        }

        /// <summary>
        /// Wraps PollUntilCompleteAsync with query timeout enforcement.
        /// If _queryTimeoutSeconds > 0, cancels the server-side statement and throws on timeout.
        /// </summary>
        private async Task<ExecuteStatementResponse> PollWithTimeoutAsync(string statementId, CancellationToken cancellationToken)
        {
            if (_queryTimeoutSeconds <= 0)
            {
                return await PollUntilCompleteAsync(statementId, cancellationToken).ConfigureAwait(false);
            }

            using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(_queryTimeoutSeconds));
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

            try
            {
                return await PollUntilCompleteAsync(statementId, linkedCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                // Timeout fired (not caller cancellation) — cancel statement on server, best-effort
                try
                {
                    await _client.CancelStatementAsync(statementId, CancellationToken.None).ConfigureAwait(false);
                }
                catch
                {
                    // Best-effort; ignore cancel errors
                }
                throw new AdbcException(
                    $"Query timed out after {_queryTimeoutSeconds} seconds (statement {statementId}). " +
                    $"Increase timeout via '{ApacheParameters.QueryTimeoutSeconds}' or set to 0 for no timeout.");
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

                // Get statement status
                var response = await _client.GetStatementAsync(statementId, cancellationToken).ConfigureAwait(false);

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
                // No data rows, but the manifest contains schema information.
                // Preserve the schema so callers get correct column metadata even
                // when the queried table is empty — following the same pattern as
                // the JDBC driver where ResultManifest schema is always extracted
                // independently of data presence.
                Schema schema = TryGetSchemaFromManifest(response.Manifest) ?? new Schema.Builder().Build();
                return new EmptyArrowArrayStream(schema);
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
        /// Throws <see cref="AdbcException"/> if the manifest contains no column definitions.
        /// </summary>
        private Schema GetSchemaFromManifest(ResultManifest manifest)
        {
            return TryGetSchemaFromManifest(manifest)
                ?? throw new AdbcException("Result manifest does not contain schema information");
        }

        /// <summary>
        /// Tries to extract the Arrow schema from the result manifest.
        /// Returns <c>null</c> when the manifest contains no column definitions,
        /// allowing callers to decide on a fallback (e.g. empty schema for no-data results).
        /// </summary>
        private Schema? TryGetSchemaFromManifest(ResultManifest manifest)
        {
            if (manifest.Schema == null || manifest.Schema.Columns == null || manifest.Schema.Columns.Count == 0)
            {
                return null;
            }

            var fields = new List<Field>();
            foreach (var column in manifest.Schema.Columns)
            {
                var typeName = column.TypeName ?? string.Empty;
                var arrowType = MapDatabricksTypeToArrowType(typeName);
                // Embed the SQL type name as Arrow field metadata so that consumers
                // (e.g. the PowerBI connector's AdjustNativeTypes) can read it via
                // the "Spark:DataType:SqlName" key — the same metadata the Databricks
                // server embeds in the Arrow IPC stream for non-empty results.
                //
                // Note: the Thrift server also sets "Spark:DataType:JsonType" (the JSON
                // representation of the type, e.g. "{\"type\":\"integer\"}") alongside
                // SqlName. That key is not read by any known consumer today, so we omit
                // it here for now. Add it if a consumer requires it (PECO-2950).
                var metadata = new Dictionary<string, string>
                {
                    ["Spark:DataType:SqlName"] = ColumnMetadataHelper.GetSparkSqlName(typeName)
                };
                fields.Add(new Field(column.Name, arrowType, true, metadata));
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
                OnWaitTimeout = "CONTINUE",
                IsMetadata = false
            };

            // Execute the statement
            var response = await _client.ExecuteStatementAsync(request, cancellationToken).ConfigureAwait(false);
            _currentStatementId = response.StatementId;

            // Handle query status - poll until complete
            var state = response.Status?.State;
            if (state == "PENDING" || state == "RUNNING")
            {
                response = await PollWithTimeoutAsync(response.StatementId, cancellationToken).ConfigureAwait(false);
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

            // For updates, we don't need to read the results - just return the row count.
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
        /// Accepts an optional schema so that column metadata is preserved
        /// even when the result contains zero rows (e.g. querying an empty table).
        /// </summary>
        private class EmptyArrowArrayStream : IArrowArrayStream
        {
            public EmptyArrowArrayStream(Schema? schema = null)
            {
                Schema = schema ?? new Schema.Builder().Build();
            }

            public Schema Schema { get; }

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

        // Metadata command routing

        /// <summary>
        /// Resolves the catalog for metadata SQL commands.
        /// Matches Thrift behavior:
        ///   - SPARK → null (all catalogs)
        ///   - Other values pass through as-is
        ///   - null stays null
        /// When enableMultipleCatalogSupport=false and result is null,
        /// resolves to the session default catalog (SEA SQL requires an explicit
        /// catalog for SHOW commands when not querying all catalogs).
        /// </summary>
        // TODO: Once the backend supports SHOW COLUMNS IN ALL CATALOGS, the
        // ExecuteShowColumnsAsync iterate-all-catalogs fallback can be removed.
        private string? EffectiveCatalog
        {
            get
            {
                // Normalize SPARK → null, same as Thrift's HandleSparkCatalog
                string? catalog = DatabricksConnection.HandleSparkCatalog(_metadataCatalogName);

                if (_connection.EnableMultipleCatalogSupport)
                {
                    // null means "all catalogs" (e.g. SHOW SCHEMAS IN ALL CATALOGS)
                    return catalog;
                }

                // flag=false: null means use session default (SEA SQL needs explicit catalog)
                return catalog ?? _connection.GetSessionDefaultCatalog();
            }
        }

        /// <summary>
        /// Escapes wildcard characters (_ and %) in metadata name parameters when
        /// EscapePatternWildcards is enabled. This prevents literal underscores or
        /// percent signs in identifiers from being treated as pattern wildcards.
        /// </summary>
        private string? EscapePatternWildcardsInName(string? name)
        {
            if (!_escapePatternWildcards || name == null)
                return name;
            return name.Replace("_", "\\_").Replace("%", "\\%");
        }

        private Task<QueryResult> ExecuteMetadataCommandAsync(CancellationToken cancellationToken)
        {
            return _sqlQuery?.ToLowerInvariant() switch
            {
                "getcatalogs" => GetCatalogsAsync(cancellationToken),
                "getschemas" => GetSchemasAsync(cancellationToken),
                "gettables" => GetTablesAsync(cancellationToken),
                "getcolumns" => GetColumnsAsync(cancellationToken),
                "getcolumnsextended" => GetColumnsExtendedAsync(cancellationToken),
                "getprimarykeys" => GetPrimaryKeysAsync(cancellationToken),
                "getcrossreference" => GetCrossReferenceAsync(cancellationToken),
                _ => throw new NotSupportedException($"Metadata command '{_sqlQuery}' is not supported"),
            };
        }

        private async Task<QueryResult> GetCatalogsAsync(CancellationToken cancellationToken)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.SetTag("catalog_pattern", _metadataCatalogName ?? "(none)");
                activity?.SetTag("enable_multiple_catalog_support", _connection.EnableMultipleCatalogSupport);

                // When multiple catalog support is disabled, return a single "SPARK" catalog
                if (!_connection.EnableMultipleCatalogSupport)
                {
                    var catalogSchema = MetadataSchemaFactory.CreateCatalogsSchema();
                    var sparkBuilder = new StringArray.Builder();
                    sparkBuilder.Append("SPARK");
                    return new QueryResult(1, new HiveInfoArrowStream(catalogSchema, new IArrowArray[] { sparkBuilder.Build() }));
                }

                // GetCatalogs returns all catalogs — no filtering by pattern,
                // matching Thrift behavior (Thrift RPC has no catalog filter for GetCatalogs).
                string sql = new ShowCatalogsCommand(null).Build();
                activity?.SetTag("sql_query", sql);
                var batches = await _connection.ExecuteMetadataSqlAsync(sql, cancellationToken).ConfigureAwait(false);

                var tableCatBuilder = new StringArray.Builder();
                int count = 0;
                foreach (var batch in batches)
                {
                    var catalogArray = TryGetColumn<StringArray>(batch, "catalog");
                    if (catalogArray == null) continue;
                    for (int i = 0; i < batch.Length; i++)
                    {
                        if (!catalogArray.IsNull(i))
                        {
                            tableCatBuilder.Append(catalogArray.GetString(i));
                            count++;
                        }
                    }
                }

                activity?.SetTag("result_count", count);
                var schema = MetadataSchemaFactory.CreateCatalogsSchema();
                return new QueryResult(count, new HiveInfoArrowStream(schema, new IArrowArray[] { tableCatBuilder.Build() }));
            }, "GetCatalogs").ConfigureAwait(false);
        }

        private async Task<QueryResult> GetSchemasAsync(CancellationToken cancellationToken)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                var catalog = EffectiveCatalog;
                activity?.SetTag("catalog", catalog ?? "(none)");
                activity?.SetTag("schema_pattern", _metadataSchemaName ?? "(none)");
                activity?.SetTag("enable_multiple_catalog_support", _connection.EnableMultipleCatalogSupport);

                // When flag=false and user specified an explicit non-SPARK catalog, return empty
                if (!_connection.EnableMultipleCatalogSupport
                    && MetadataUtilities.NormalizeSparkCatalog(_metadataCatalogName) != null)
                    return MetadataSchemaFactory.CreateEmptySchemasResult();

                string sql = new ShowSchemasCommand(
                    catalog,
                    EscapePatternWildcardsInName(_metadataSchemaName)).Build();
                activity?.SetTag("sql_query", sql);
                var batches = await _connection.ExecuteMetadataSqlAsync(sql, cancellationToken).ConfigureAwait(false);

                // SHOW SCHEMAS IN ALL CATALOGS returns 2 columns: databaseName, catalog
                // SHOW SCHEMAS IN `catalog` returns 1 column: databaseName
                bool showAllCatalogs = catalog == null;

                var tableSchemaBuilder = new StringArray.Builder();
                var tableCatalogBuilder = new StringArray.Builder();
                int count = 0;
                foreach (var batch in batches)
                {
                    StringArray? catalogArray = null;
                    StringArray? schemaArray = null;

                    if (showAllCatalogs)
                    {
                        schemaArray = batch.Column(0) as StringArray;
                        catalogArray = batch.Column(1) as StringArray;
                    }
                    else
                    {
                        schemaArray = batch.Column(0) as StringArray;
                    }

                    if (schemaArray == null) continue;
                    for (int i = 0; i < batch.Length; i++)
                    {
                        if (schemaArray.IsNull(i)) continue;
                        tableSchemaBuilder.Append(schemaArray.GetString(i));
                        string catalogValue = catalogArray != null && !catalogArray.IsNull(i)
                            ? catalogArray.GetString(i)
                            : catalog ?? "";
                        tableCatalogBuilder.Append(catalogValue);
                        count++;
                    }
                }

                activity?.SetTag("result_count", count);
                var schema = MetadataSchemaFactory.CreateSchemasSchema();
                return new QueryResult(count, new HiveInfoArrowStream(schema, new IArrowArray[]
                {
                    tableSchemaBuilder.Build(), tableCatalogBuilder.Build()
                }));
            }, "GetSchemas").ConfigureAwait(false);
        }

        private async Task<QueryResult> GetTablesAsync(CancellationToken cancellationToken)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                var catalog = EffectiveCatalog;
                activity?.SetTag("catalog", catalog ?? "(none)");
                activity?.SetTag("schema_pattern", _metadataSchemaName ?? "(none)");
                activity?.SetTag("table_pattern", _metadataTableName ?? "(none)");
                activity?.SetTag("enable_multiple_catalog_support", _connection.EnableMultipleCatalogSupport);

                if (!_connection.EnableMultipleCatalogSupport
                    && MetadataUtilities.NormalizeSparkCatalog(_metadataCatalogName) != null)
                    return MetadataSchemaFactory.CreateEmptyTablesResult();

                string sql = new ShowTablesCommand(
                    catalog,
                    EscapePatternWildcardsInName(_metadataSchemaName),
                    EscapePatternWildcardsInName(_metadataTableName)).Build();
                activity?.SetTag("sql_query", sql);
                var batches = await _connection.ExecuteMetadataSqlAsync(sql, cancellationToken).ConfigureAwait(false);

                var tableCatBuilder = new StringArray.Builder();
                var tableSchemaBuilder = new StringArray.Builder();
                var tableNameBuilder = new StringArray.Builder();
                var tableTypeBuilder = new StringArray.Builder();
                var remarksBuilder = new StringArray.Builder();
                var typeCatBuilder = new StringArray.Builder();
                var typeSchemaBuilder = new StringArray.Builder();
                var typeNameBuilder = new StringArray.Builder();
                var selfRefColBuilder = new StringArray.Builder();
                var refGenBuilder = new StringArray.Builder();
                var tableTypeFilter = !string.IsNullOrEmpty(_metadataTableTypes)
                    ? new HashSet<string>(
                        _metadataTableTypes!.Split(',').Select(t => t.Trim()),
                        StringComparer.OrdinalIgnoreCase)
                    : null;

                int count = 0;
                foreach (var batch in batches)
                {
                    var catalogArray = TryGetColumn<StringArray>(batch, "catalogName");
                    var schemaArray = TryGetColumn<StringArray>(batch, "namespace");
                    var tableArray = TryGetColumn<StringArray>(batch, "tableName");
                    var tableTypeArray = TryGetColumn<StringArray>(batch, "tableType");
                    var remarksArray = TryGetColumn<StringArray>(batch, "remarks");
                    if (catalogArray == null || schemaArray == null || tableArray == null) continue;

                    for (int i = 0; i < batch.Length; i++)
                    {
                        if (catalogArray.IsNull(i) || schemaArray.IsNull(i) || tableArray.IsNull(i)) continue;
                        string tableType = tableTypeArray != null && !tableTypeArray.IsNull(i) ? tableTypeArray.GetString(i) : "TABLE";
                        if (tableTypeFilter != null && !tableTypeFilter.Contains(tableType)) continue;
                        tableCatBuilder.Append(catalogArray.GetString(i));
                        tableSchemaBuilder.Append(schemaArray.GetString(i));
                        tableNameBuilder.Append(tableArray.GetString(i));
                        tableTypeBuilder.Append(tableType);
                        remarksBuilder.Append(remarksArray != null && !remarksArray.IsNull(i) ? remarksArray.GetString(i) : "");
                        typeCatBuilder.AppendNull();
                        typeSchemaBuilder.AppendNull();
                        typeNameBuilder.AppendNull();
                        selfRefColBuilder.AppendNull();
                        refGenBuilder.AppendNull();
                        count++;
                    }
                }

                activity?.SetTag("result_count", count);
                var schema = MetadataSchemaFactory.CreateTablesSchema();
                return new QueryResult(count, new HiveInfoArrowStream(schema, new IArrowArray[]
                {
                    tableCatBuilder.Build(), tableSchemaBuilder.Build(), tableNameBuilder.Build(),
                    tableTypeBuilder.Build(), remarksBuilder.Build(), typeCatBuilder.Build(),
                    typeSchemaBuilder.Build(), typeNameBuilder.Build(), selfRefColBuilder.Build(),
                    refGenBuilder.Build()
                }));
            }, "GetTables").ConfigureAwait(false);
        }

        private async Task<QueryResult> GetColumnsAsync(CancellationToken cancellationToken)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                var catalog = EffectiveCatalog;
                activity?.SetTag("catalog", catalog ?? "(none)");
                activity?.SetTag("schema_pattern", _metadataSchemaName ?? "(none)");
                activity?.SetTag("table_pattern", _metadataTableName ?? "(none)");
                activity?.SetTag("column_pattern", _metadataColumnName ?? "(none)");
                activity?.SetTag("enable_multiple_catalog_support", _connection.EnableMultipleCatalogSupport);

                // When EnableMultipleCatalogSupport is false, only the session's default catalog
                // is accessible. If the user specified an explicit non-SPARK catalog, return empty
                // results immediately (matching Thrift behavior in DatabricksStatement).
                if (!_connection.EnableMultipleCatalogSupport
                    && MetadataUtilities.NormalizeSparkCatalog(_metadataCatalogName) != null)
                    return FlatColumnsResultBuilder.BuildFlatColumnsResult(
                        System.Array.Empty<(string, string, string, TableInfo)>());

                var batches = await _connection.ExecuteShowColumnsAsync(
                    catalog,
                    EscapePatternWildcardsInName(_metadataSchemaName),
                    EscapePatternWildcardsInName(_metadataTableName),
                    EscapePatternWildcardsInName(_metadataColumnName),
                    cancellationToken).ConfigureAwait(false);

                var tableInfos = new Dictionary<string, (string catalog, string schema, string table, TableInfo info)>();

                foreach (var batch in batches)
                {
                    var catalogArray = TryGetColumn<StringArray>(batch, "catalogName");
                    var schemaArray = TryGetColumn<StringArray>(batch, "namespace");
                    var tableArray = TryGetColumn<StringArray>(batch, "tableName");
                    var colNameArray = TryGetColumn<StringArray>(batch, "col_name");
                    var columnTypeArray = TryGetColumn<StringArray>(batch, "columnType");
                    var isNullableArray = TryGetColumn<StringArray>(batch, "isNullable");

                    if (catalogArray == null || schemaArray == null || tableArray == null ||
                        colNameArray == null || columnTypeArray == null) continue;

                    for (int i = 0; i < batch.Length; i++)
                    {
                        if (catalogArray.IsNull(i) || schemaArray.IsNull(i) || tableArray.IsNull(i) ||
                            colNameArray.IsNull(i) || columnTypeArray.IsNull(i)) continue;

                        string cat = catalogArray.GetString(i);
                        string sch = schemaArray.GetString(i);
                        string tbl = tableArray.GetString(i);
                        string key = $"{cat}.{sch}.{tbl}";

                        if (!tableInfos.ContainsKey(key))
                            tableInfos[key] = (cat, sch, tbl, new TableInfo("TABLE"));

                        var entry = tableInfos[key];
                        bool nullable = isNullableArray == null || isNullableArray.IsNull(i) ||
                            !isNullableArray.GetString(i).Equals("false", StringComparison.OrdinalIgnoreCase);

                        ColumnMetadataHelper.PopulateTableInfoFromTypeName(
                            entry.info,
                            colNameArray.GetString(i),
                            columnTypeArray.GetString(i),
                            entry.info.ColumnName.Count,
                            nullable);
                    }
                }

                activity?.SetTag("result_tables", tableInfos.Count);
                return FlatColumnsResultBuilder.BuildFlatColumnsResult(tableInfos.Values);
            }, "GetColumns").ConfigureAwait(false);
        }

        private async Task<QueryResult> GetColumnsExtendedAsync(CancellationToken cancellationToken)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                var catalog = EffectiveCatalog;
                activity?.SetTag("catalog", catalog ?? "(none)");
                activity?.SetTag("schema", _metadataSchemaName ?? "(none)");
                activity?.SetTag("table", _metadataTableName ?? "(none)");
                activity?.SetTag("use_desc_table_extended", _connection.UseDescTableExtended);

                if (string.IsNullOrEmpty(_metadataTableName))
                    throw new ArgumentException("Table name is required for GetColumnsExtended");

                if (_connection.UseDescTableExtended)
                    return await GetColumnsExtendedViaDescTableAsync(catalog, cancellationToken).ConfigureAwait(false);

                // Fallback: combine GetColumns + GetPrimaryKeys + GetCrossReference,
                // mirroring the Thrift base (HiveServer2Statement.GetColumnsExtendedAsync).
                return await GetColumnsExtendedViaThreeCalls(cancellationToken).ConfigureAwait(false);

            }, "GetColumnsExtended").ConfigureAwait(false);
        }

        /// <summary>
        /// GetColumnsExtended via DESC TABLE EXTENDED AS JSON (single round-trip, full fidelity).
        /// Only used when UseDescTableExtended=true.
        /// </summary>
        private async Task<QueryResult> GetColumnsExtendedViaDescTableAsync(string? catalog, CancellationToken cancellationToken)
        {
            // For building the qualified table name, use the user-specified catalog
            // (normalized to null for SPARK) rather than EffectiveCatalog. This matches
            // Thrift's BuildTableName which omits the catalog prefix when it's null/SPARK,
            // letting the server resolve to its default catalog.
            string? catalogForTableName = _connection.EnableMultipleCatalogSupport
                ? catalog
                : MetadataUtilities.NormalizeSparkCatalog(_metadataCatalogName);

            string? fullTableName = MetadataUtilities.BuildQualifiedTableName(
                catalogForTableName, _metadataSchemaName, _metadataTableName);

            string query = $"DESC TABLE EXTENDED {fullTableName} AS JSON";
            var batches = await _connection.ExecuteMetadataSqlAsync(query, cancellationToken).ConfigureAwait(false);

            string? resultJson = null;
            foreach (var batch in batches)
            {
                if (batch.Length > 0)
                {
                    resultJson = ((StringArray)batch.Column(0)).GetString(0);
                    break;
                }
            }

            if (string.IsNullOrEmpty(resultJson))
                throw new FormatException($"Empty result from {query}");

            var descResult = System.Text.Json.JsonSerializer.Deserialize<DescTableExtendedResult>(resultJson!);
            if (descResult == null)
                throw new FormatException($"Failed to parse JSON result from {query}");

            return DatabricksStatement.CreateExtendedColumnsResult(
                MetadataSchemaFactory.CreateColumnMetadataSchema(), descResult);
        }

        /// <summary>
        /// GetColumnsExtended fallback: calls GetColumns, GetPrimaryKeys, GetCrossReference
        /// separately and merges the results — identical to the Thrift base implementation.
        /// Used when UseDescTableExtended=false (the default).
        /// </summary>
        private async Task<QueryResult> GetColumnsExtendedViaThreeCalls(CancellationToken cancellationToken)
        {
            var columnsResult = await GetColumnsAsync(cancellationToken).ConfigureAwait(false);
            if (columnsResult.Stream == null)
                return columnsResult;

            var pkResult = await GetPrimaryKeysAsync(cancellationToken).ConfigureAwait(false);

            // Find FKs where the current table is the FK (child) side — null PK params to
            // match any parent, mirroring Thrift's GetCrossReferenceAsForeignTableAsync.
            var fkResult = await FetchCrossReferenceAsync(
                pkCatalog: null, pkSchema: null, pkTable: null,
                fkCatalog: _metadataCatalogName, fkSchema: _metadataSchemaName, fkTable: _metadataTableName,
                cancellationToken).ConfigureAwait(false);

            // Read all column batches into memory
            int colNameIndex;
            List<RecordBatch> columnsBatches;
            int totalRows;
            Schema columnsSchema;
            StringArray columnNames;

            using (var stream = columnsResult.Stream)
            {
                colNameIndex = stream.Schema.GetFieldIndex("COLUMN_NAME");
                if (colNameIndex < 0)
                    return columnsResult;

                var batchResult = await ReadAllBatchesAsync(stream, cancellationToken).ConfigureAwait(false);
                columnsBatches = batchResult.Batches;
                columnsSchema = batchResult.Schema;
                totalRows = batchResult.TotalRows;

                if (columnsBatches.Count == 0)
                    return CreateEmptyExtendedColumnsResult(columnsSchema);

                List<ArrayData> colNameArrayDatas = columnsBatches
                    .Select(b => b.Column(colNameIndex).Data).ToList();
                ArrayData concatenated = ArrayDataConcatenator.Concatenate(colNameArrayDatas)!;
                columnNames = (StringArray)ArrowArrayFactory.BuildArray(concatenated);
            }

            // Build combined schema + data starting from the base columns
            var allFields = new List<Field>(columnsSchema.FieldsList);
            var combinedData = new List<IArrowArray>();

            for (int colIdx = 0; colIdx < columnsSchema.FieldsList.Count; colIdx++)
            {
                var arrays = columnsBatches.Select(b => b.Column(colIdx)).ToList();
                var arrayDatas = arrays.Select(a => a.Data).ToList();
                combinedData.Add(ArrowArrayFactory.BuildArray(ArrayDataConcatenator.Concatenate(arrayDatas)!));
            }

            // Merge PK data (keyed on COLUMN_NAME → PK_COLUMN_NAME)
            await ProcessRelationshipDataSafe(
                pkResult, PrimaryKeyPrefix, "COLUMN_NAME",
                PrimaryKeyFields, columnNames, totalRows,
                allFields, combinedData, cancellationToken).ConfigureAwait(false);

            // Merge FK data (keyed on FKCOLUMN_NAME → FK_* fields)
            await ProcessRelationshipDataSafe(
                fkResult, ForeignKeyPrefix, "FKCOLUMN_NAME",
                ForeignKeyFields, columnNames, totalRows,
                allFields, combinedData, cancellationToken).ConfigureAwait(false);

            var combinedSchema = new Schema(allFields, columnsSchema.Metadata);
            return new QueryResult(totalRows, new HiveInfoArrowStream(combinedSchema, combinedData));
        }

        private async Task<QueryResult> GetPrimaryKeysAsync(CancellationToken cancellationToken)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.SetTag("catalog", _metadataCatalogName ?? "(none)");
                activity?.SetTag("schema", _metadataSchemaName ?? "(none)");
                activity?.SetTag("table", _metadataTableName ?? "(none)");
                activity?.SetTag("pk_fk_enabled", _connection.EnablePKFK);

                if (MetadataUtilities.ShouldReturnEmptyPKFKResult(_metadataCatalogName, null, _connection.EnablePKFK))
                    return MetadataSchemaFactory.CreateEmptyPrimaryKeysResult();

                if (string.IsNullOrEmpty(_metadataCatalogName) || string.IsNullOrEmpty(_metadataSchemaName) ||
                    string.IsNullOrEmpty(_metadataTableName))
                    return MetadataSchemaFactory.CreateEmptyPrimaryKeysResult();

                string sql = new ShowKeysCommand(_metadataCatalogName!, _metadataSchemaName!, _metadataTableName!).Build();
                activity?.SetTag("sql_query", sql);
                List<RecordBatch> batches = await _connection.ExecuteMetadataSqlAsync(sql, cancellationToken).ConfigureAwait(false);

                var keys = new List<(string, string, string, string, int, string)>();
                int seq = 0;
                foreach (var batch in batches)
                {
                    var colNameArray = TryGetColumn<StringArray>(batch, "col_name");
                    var keyNameArray = TryGetColumn<StringArray>(batch, "constraintName");
                    var keySeqArray = TryGetColumn<Int32Array>(batch, "keySeq");
                    if (colNameArray == null) continue;
                    for (int i = 0; i < batch.Length; i++)
                    {
                        if (colNameArray.IsNull(i)) continue;
                        int keySeq = keySeqArray != null && !keySeqArray.IsNull(i) ? keySeqArray.GetValue(i)!.Value : ++seq;
                        string pkName = keyNameArray != null && !keyNameArray.IsNull(i) ? keyNameArray.GetString(i) : "";
                        keys.Add((_metadataCatalogName!, _metadataSchemaName!, _metadataTableName!,
                            colNameArray.GetString(i), keySeq, pkName));
                    }
                }

                activity?.SetTag("result_count", keys.Count);
                return MetadataSchemaFactory.BuildPrimaryKeysResult(keys);
            }, "GetPrimaryKeys").ConfigureAwait(false);
        }

        private async Task<QueryResult> GetCrossReferenceAsync(CancellationToken cancellationToken)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.SetTag("pk_catalog", _metadataCatalogName ?? "(none)");
                activity?.SetTag("pk_schema", _metadataSchemaName ?? "(none)");
                activity?.SetTag("pk_table", _metadataTableName ?? "(none)");
                activity?.SetTag("fk_catalog", _metadataForeignCatalogName ?? "(none)");
                activity?.SetTag("fk_schema", _metadataForeignSchemaName ?? "(none)");
                activity?.SetTag("fk_table", _metadataForeignTableName ?? "(none)");
                activity?.SetTag("pk_fk_enabled", _connection.EnablePKFK);

                var result = await FetchCrossReferenceAsync(
                    _metadataCatalogName, _metadataSchemaName, _metadataTableName,
                    _metadataForeignCatalogName, _metadataForeignSchemaName, _metadataForeignTableName,
                    cancellationToken).ConfigureAwait(false);

                activity?.SetTag("result_count", result.RowCount);
                return result;
            }, "GetCrossReference").ConfigureAwait(false);
        }

        /// <summary>
        /// Core cross-reference fetch with explicit params. Used by both GetCrossReferenceAsync
        /// (user-facing, reads from statement fields) and GetColumnsExtendedViaThreeCalls
        /// (passes current table as the FK side with null PK params to match any parent).
        /// </summary>
        private async Task<QueryResult> FetchCrossReferenceAsync(
            string? pkCatalog, string? pkSchema, string? pkTable,
            string? fkCatalog, string? fkSchema, string? fkTable,
            CancellationToken cancellationToken)
        {
            if (MetadataUtilities.ShouldReturnEmptyPKFKResult(pkCatalog, fkCatalog, _connection.EnablePKFK))
                return MetadataSchemaFactory.CreateEmptyCrossReferenceResult();

            if (string.IsNullOrEmpty(fkCatalog) || string.IsNullOrEmpty(fkSchema) || string.IsNullOrEmpty(fkTable))
                return MetadataSchemaFactory.CreateEmptyCrossReferenceResult();

            string sql = new ShowForeignKeysCommand(fkCatalog!, fkSchema!, fkTable!).Build();
            List<RecordBatch> batches = await _connection.ExecuteMetadataSqlAsync(sql, cancellationToken).ConfigureAwait(false);

            var refs = new List<(string, string, string, string, string, string, string, string, int, int, int, string, string?, int)>();
            int seq = 0;
            foreach (var batch in batches)
            {
                var pkCatalogArray = TryGetColumn<StringArray>(batch, "parentCatalogName");
                var pkSchemaArray = TryGetColumn<StringArray>(batch, "parentNamespace");
                var pkTableArray = TryGetColumn<StringArray>(batch, "parentTableName");
                var pkColArray = TryGetColumn<StringArray>(batch, "parentColName");
                var fkCatalogArray = TryGetColumn<StringArray>(batch, "catalogName");
                var fkSchemaArray = TryGetColumn<StringArray>(batch, "namespace");
                var fkTableArray = TryGetColumn<StringArray>(batch, "tableName");
                var fkColArray = TryGetColumn<StringArray>(batch, "col_name");
                var fkNameArray = TryGetColumn<StringArray>(batch, "constraintName");
                var fkKeySeqArray = TryGetColumn<Int32Array>(batch, "keySeq");
                var fkUpdateRuleArray = TryGetColumn<Int32Array>(batch, "updateRule");
                var fkDeleteRuleArray = TryGetColumn<Int32Array>(batch, "deleteRule");
                var fkDeferrabilityArray = TryGetColumn<Int32Array>(batch, "deferrability");

                if (fkColArray == null) continue;

                for (int i = 0; i < batch.Length; i++)
                {
                    if (fkColArray.IsNull(i)) continue;
                    refs.Add((
                        pkCatalogArray != null && !pkCatalogArray.IsNull(i) ? pkCatalogArray.GetString(i) : pkCatalog ?? "",
                        pkSchemaArray != null && !pkSchemaArray.IsNull(i) ? pkSchemaArray.GetString(i) : pkSchema ?? "",
                        pkTableArray != null && !pkTableArray.IsNull(i) ? pkTableArray.GetString(i) : pkTable ?? "",
                        pkColArray != null && !pkColArray.IsNull(i) ? pkColArray.GetString(i) : "",
                        fkCatalogArray != null && !fkCatalogArray.IsNull(i) ? fkCatalogArray.GetString(i) : fkCatalog!,
                        fkSchemaArray != null && !fkSchemaArray.IsNull(i) ? fkSchemaArray.GetString(i) : fkSchema!,
                        fkTableArray != null && !fkTableArray.IsNull(i) ? fkTableArray.GetString(i) : fkTable!,
                        fkColArray.GetString(i),
                        fkKeySeqArray != null && !fkKeySeqArray.IsNull(i) ? fkKeySeqArray.GetValue(i)!.Value : ++seq,
                        fkUpdateRuleArray != null && !fkUpdateRuleArray.IsNull(i) ? fkUpdateRuleArray.GetValue(i)!.Value : 0,
                        fkDeleteRuleArray != null && !fkDeleteRuleArray.IsNull(i) ? fkDeleteRuleArray.GetValue(i)!.Value : 0,
                        fkNameArray != null && !fkNameArray.IsNull(i) ? fkNameArray.GetString(i) : "",
                        (string?)null,
                        fkDeferrabilityArray != null && !fkDeferrabilityArray.IsNull(i) ? fkDeferrabilityArray.GetValue(i)!.Value : 5
                    ));
                }
            }

            return MetadataSchemaFactory.BuildCrossReferenceResult(refs);
        }

        private static T? TryGetColumn<T>(RecordBatch batch, string name) where T : class, IArrowArray
        {
            try { return batch.Column(name) as T; }
            catch (ArgumentOutOfRangeException) { return null; }
        }

        // TracingStatement implementation
        public override string AssemblyVersion => GetType().Assembly.GetName().Version?.ToString() ?? "1.0.0";
        public override string AssemblyName => "AdbcDrivers.Databricks";

        // ─── Helpers for GetColumnsExtended fallback (mirrors HiveServer2Statement) ──

        private const string PrimaryKeyPrefix = "PK_";
        private const string ForeignKeyPrefix = "FK_";
        private static readonly string[] PrimaryKeyFields = new[] { "COLUMN_NAME" };
        private static readonly string[] ForeignKeyFields = new[] { "PKCOLUMN_NAME", "PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "FKCOLUMN_NAME", "FK_NAME", "KEQ_SEQ" };

        private static async Task<(List<RecordBatch> Batches, Schema Schema, int TotalRows)> ReadAllBatchesAsync(
            IArrowArrayStream stream, CancellationToken cancellationToken)
        {
            var batches = new List<RecordBatch>();
            int totalRows = 0;
            Schema schema = stream.Schema;
            while (true)
            {
                var batch = await stream.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false);
                if (batch == null) break;
                if (batch.Length > 0)
                {
                    batches.Add(batch);
                    totalRows += batch.Length;
                }
            }
            return (batches, schema, totalRows);
        }

        private static QueryResult CreateEmptyExtendedColumnsResult(Schema baseSchema)
        {
            var allFields = new List<Field>(baseSchema.FieldsList);
            foreach (var field in PrimaryKeyFields)
                allFields.Add(new Field(PrimaryKeyPrefix + field, StringType.Default, true));
            foreach (var field in ForeignKeyFields)
            {
                IArrowType fieldType = field != "KEQ_SEQ" ? (IArrowType)StringType.Default : Int32Type.Default;
                allFields.Add(new Field(ForeignKeyPrefix + field, fieldType, true));
            }

            var combinedSchema = new Schema(allFields, baseSchema.Metadata);
            var combinedData = new List<IArrowArray>();
            foreach (var field in allFields)
            {
                switch (field.DataType.TypeId)
                {
                    case ArrowTypeId.String:  combinedData.Add(new StringArray.Builder().Build());  break;
                    case ArrowTypeId.Int8:    combinedData.Add(new Int8Array.Builder().Build());    break;
                    case ArrowTypeId.Int16:   combinedData.Add(new Int16Array.Builder().Build());   break;
                    case ArrowTypeId.Int32:   combinedData.Add(new Int32Array.Builder().Build());   break;
                    case ArrowTypeId.Int64:   combinedData.Add(new Int64Array.Builder().Build());   break;
                    default:
                        throw AdbcException.NotImplemented(
                            $"Data type '{field.DataType}' is not supported for empty extended columns result.");
                }
            }
            return new QueryResult(0, new HiveInfoArrowStream(combinedSchema, combinedData));
        }

        private static async Task ProcessRelationshipDataSafe(
            QueryResult result, string prefix, string relationColNameField,
            string[] includeFields, StringArray colNames, int rowCount,
            List<Field> allFields, List<IArrowArray> combinedData,
            CancellationToken cancellationToken)
        {
            // STEP 1: Add relationship fields to schema
            if (result.Stream != null)
            {
                var schema = result.Stream.Schema;
                foreach (var fieldName in includeFields)
                {
                    int idx = schema.GetFieldIndex(fieldName);
                    IArrowType arrowType = idx >= 0 ? schema.GetFieldByIndex(idx).DataType : (IArrowType)StringType.Default;
                    allFields.Add(new Field(prefix + fieldName, arrowType, true));
                }
            }
            else
            {
                foreach (var fieldName in includeFields)
                    allFields.Add(new Field(prefix + fieldName, StringType.Default, true));
            }

            // STEP 2: Build lookup: fieldName → columnName → value
            var relationData = new Dictionary<string, Dictionary<string, object>>(StringComparer.OrdinalIgnoreCase);
            if (result.Stream != null)
            {
                using var stream = result.Stream;
                int keyColIndex = stream.Schema.GetFieldIndex(relationColNameField);
                if (keyColIndex >= 0)
                {
                    while (true)
                    {
                        var batch = await stream.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false);
                        if (batch == null) break;

                        var fieldIndices = new Dictionary<string, int>();
                        foreach (var fieldName in includeFields)
                        {
                            int idx = stream.Schema.GetFieldIndex(fieldName);
                            if (idx >= 0) fieldIndices[fieldName] = idx;
                        }

                        for (int i = 0; i < batch.Length; i++)
                        {
                            var keyCol = (StringArray)batch.Column(keyColIndex);
                            if (keyCol.IsNull(i)) continue;
                            string keyValue = keyCol.GetString(i);
                            if (string.IsNullOrEmpty(keyValue)) continue;

                            foreach (var pair in fieldIndices)
                            {
                                if (!relationData.TryGetValue(pair.Key, out var fieldData))
                                {
                                    fieldData = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                                    relationData[pair.Key] = fieldData;
                                }
                                IArrowArray fieldArray = batch.Column(pair.Value);
                                if (fieldArray is Int32Array int32Arr)
                                    relationData[pair.Key][keyValue] = int32Arr.GetValue(i).GetValueOrDefault();
                                else
                                    relationData[pair.Key][keyValue] = ((StringArray)fieldArray).GetString(i)!;
                            }
                        }
                    }
                }
            }

            // STEP 3: Build Arrow arrays aligned with colNames
            foreach (var fieldName in includeFields)
            {
                var fieldData = relationData.ContainsKey(fieldName) ? relationData[fieldName] : null;
                IArrowType arrowType = StringType.Default;
                if (result.Stream != null)
                {
                    int fi = result.Stream.Schema.GetFieldIndex(fieldName);
                    if (fi >= 0) arrowType = result.Stream.Schema.GetFieldByIndex(fi).DataType;
                }

                if (arrowType.TypeId == ArrowTypeId.Int32)
                {
                    var builder = new Int32Array.Builder();
                    for (int i = 0; i < colNames.Length; i++)
                    {
                        string? colName = colNames.GetString(i);
                        if (!string.IsNullOrEmpty(colName) && fieldData != null && fieldData.TryGetValue(colName!, out var val))
                        {
                            if (val is int iv) builder.Append(iv);
                            else if (val is string sv && int.TryParse(sv, out int pv)) builder.Append(pv);
                            else builder.AppendNull();
                        }
                        else builder.AppendNull();
                    }
                    combinedData.Add(builder.Build());
                }
                else
                {
                    var builder = new StringArray.Builder();
                    for (int i = 0; i < colNames.Length; i++)
                    {
                        string? colName = colNames.GetString(i);
                        string? value = null;
                        if (!string.IsNullOrEmpty(colName) && fieldData != null && fieldData.TryGetValue(colName!, out var val))
                            value = val as string;
                        builder.Append(value);
                    }
                    combinedData.Add(builder.Build());
                }
            }
        }
    }
}
