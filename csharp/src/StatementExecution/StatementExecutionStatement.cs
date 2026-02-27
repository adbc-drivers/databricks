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

        // Connection properties for CloudFetch configuration
        private readonly IReadOnlyDictionary<string, string> _properties;

        // Memory pooling
        private readonly Microsoft.IO.RecyclableMemoryStreamManager _recyclableMemoryStreamManager;
        private readonly System.Buffers.ArrayPool<byte> _lz4BufferPool;

        // HTTP client for CloudFetch downloads
        private readonly HttpClient _httpClient;

        // Connection reference for metadata queries
        private readonly StatementExecutionConnection _connection;

        // Statement state
        private string? _currentStatementId;
        private string? _sqlQuery;

        // Metadata command support
        private bool _isMetadataCommand;
        private string? _metadataCatalogName;
        private string? _metadataSchemaName;
        private string? _metadataTableName;
        private string? _metadataColumnName;
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
                default:
                    base.SetOption(key, value);
                    break;
            }
        }

        public override QueryResult ExecuteQuery()
        {
            return ExecuteQueryAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        public async Task<QueryResult> ExecuteQueryAsync(CancellationToken cancellationToken = default)
        {
            if (_isMetadataCommand)
            {
                return ExecuteMetadataCommand();
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

            // Create appropriate reader based on result disposition
            IArrowArrayStream reader = CreateReader(response, cancellationToken);

            // Get schema from reader
            var schema = reader.Schema;

            // Return query result - use -1 if row count is not available
            long rowCount = response.Manifest?.TotalRowCount ?? -1;
            return new QueryResult(rowCount, reader);
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

        // Metadata command routing

        private QueryResult ExecuteMetadataCommand()
        {
            return _sqlQuery?.ToLowerInvariant() switch
            {
                "getcatalogs" => GetCatalogs(),
                "getschemas" => GetSchemas(),
                "gettables" => GetTables(),
                "getcolumns" => GetColumns(),
                "getcolumnsextended" => GetColumns(),
                "getprimarykeys" => GetPrimaryKeys(),
                "getcrossreference" => GetCrossReference(),
                _ => throw new NotSupportedException($"Metadata command '{_sqlQuery}' is not supported"),
            };
        }

        private QueryResult GetCatalogs()
        {
            string sql = new ShowCatalogsCommand(_metadataCatalogName).Build();
            var batches = _connection.ExecuteMetadataSql(sql);

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

            var schema = new Schema(new[] { new Field("TABLE_CAT", StringType.Default, true) }, null);
            return new QueryResult(count, new HiveInfoArrowStream(schema, new IArrowArray[] { tableCatBuilder.Build() }));
        }

        private QueryResult GetSchemas()
        {
            string sql = new ShowSchemasCommand(_metadataCatalogName, _metadataSchemaName).Build();
            var batches = _connection.ExecuteMetadataSql(sql);

            var tableSchemaBuilder = new StringArray.Builder();
            var tableCatalogBuilder = new StringArray.Builder();
            int count = 0;
            foreach (var batch in batches)
            {
                var schemaArray = TryGetColumn<StringArray>(batch, "databaseName");
                var catalogArray = TryGetColumn<StringArray>(batch, "catalog_name");
                if (schemaArray == null) continue;
                for (int i = 0; i < batch.Length; i++)
                {
                    if (schemaArray.IsNull(i)) continue;
                    tableSchemaBuilder.Append(schemaArray.GetString(i));
                    string catalog = catalogArray != null && !catalogArray.IsNull(i)
                        ? catalogArray.GetString(i)
                        : _metadataCatalogName ?? "";
                    tableCatalogBuilder.Append(catalog);
                    count++;
                }
            }

            var schema = new Schema(new[]
            {
                new Field("TABLE_SCHEM", StringType.Default, true),
                new Field("TABLE_CATALOG", StringType.Default, true)
            }, null);
            return new QueryResult(count, new HiveInfoArrowStream(schema, new IArrowArray[]
            {
                tableSchemaBuilder.Build(), tableCatalogBuilder.Build()
            }));
        }

        private QueryResult GetTables()
        {
            string sql = new ShowTablesCommand(_metadataCatalogName, _metadataSchemaName, _metadataTableName).Build();
            var batches = _connection.ExecuteMetadataSql(sql);

            var tableCatBuilder = new StringArray.Builder();
            var tableSchemaBuilder = new StringArray.Builder();
            var tableNameBuilder = new StringArray.Builder();
            var tableTypeBuilder = new StringArray.Builder();
            var remarksBuilder = new StringArray.Builder();
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
                    tableCatBuilder.Append(catalogArray.GetString(i));
                    tableSchemaBuilder.Append(schemaArray.GetString(i));
                    tableNameBuilder.Append(tableArray.GetString(i));
                    string tableType = tableTypeArray != null && !tableTypeArray.IsNull(i) ? tableTypeArray.GetString(i) : "TABLE";
                    tableTypeBuilder.Append(tableType);
                    remarksBuilder.Append(remarksArray != null && !remarksArray.IsNull(i) ? remarksArray.GetString(i) : "");
                    count++;
                }
            }

            var schema = new Schema(new[]
            {
                new Field("TABLE_CAT", StringType.Default, true),
                new Field("TABLE_SCHEM", StringType.Default, true),
                new Field("TABLE_NAME", StringType.Default, true),
                new Field("TABLE_TYPE", StringType.Default, true),
                new Field("REMARKS", StringType.Default, true),
            }, null);
            return new QueryResult(count, new HiveInfoArrowStream(schema, new IArrowArray[]
            {
                tableCatBuilder.Build(), tableSchemaBuilder.Build(), tableNameBuilder.Build(),
                tableTypeBuilder.Build(), remarksBuilder.Build()
            }));
        }

        private QueryResult GetColumns()
        {
            string sql = new ShowColumnsCommand(
                _metadataCatalogName, _metadataSchemaName,
                _metadataTableName, _metadataColumnName).Build();
            var batches = _connection.ExecuteMetadataSql(sql);

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
                        entry.info.ColumnName.Count + 1,
                        nullable);
                }
            }

            return GetObjectsResultBuilder.BuildFlatColumnsResult(tableInfos.Values);
        }

        private QueryResult GetPrimaryKeys()
        {
            if (MetadataUtilities.IsInvalidPKFKCatalog(_metadataCatalogName))
                return MetadataSchemaFactory.CreateEmptyPrimaryKeysResult();

            if (string.IsNullOrEmpty(_metadataCatalogName) || string.IsNullOrEmpty(_metadataSchemaName) ||
                string.IsNullOrEmpty(_metadataTableName))
                return MetadataSchemaFactory.CreateEmptyPrimaryKeysResult();

            string sql = new ShowKeysCommand(_metadataCatalogName!, _metadataSchemaName!, _metadataTableName!).Build();
            List<RecordBatch> batches;
            try
            {
                batches = _connection.ExecuteMetadataSql(sql);
            }
            catch
            {
                return MetadataSchemaFactory.CreateEmptyPrimaryKeysResult();
            }

            var tableCatBuilder = new StringArray.Builder();
            var tableSchemaBuilder = new StringArray.Builder();
            var tableNameBuilder = new StringArray.Builder();
            var columnNameBuilder = new StringArray.Builder();
            var keySeqBuilder = new Int32Array.Builder();
            var pkNameBuilder = new StringArray.Builder();
            int count = 0;

            foreach (var batch in batches)
            {
                var colNameArray = TryGetColumn<StringArray>(batch, "col_name");
                var keyNameArray = TryGetColumn<StringArray>(batch, "constraintName");
                var keySeqArray = TryGetColumn<Int32Array>(batch, "keySeq");
                if (colNameArray == null) continue;
                for (int i = 0; i < batch.Length; i++)
                {
                    if (colNameArray.IsNull(i)) continue;
                    tableCatBuilder.Append(_metadataCatalogName!);
                    tableSchemaBuilder.Append(_metadataSchemaName!);
                    tableNameBuilder.Append(_metadataTableName!);
                    columnNameBuilder.Append(colNameArray.GetString(i));
                    keySeqBuilder.Append(keySeqArray != null && !keySeqArray.IsNull(i)
                        ? keySeqArray.GetValue(i)!.Value : count + 1);
                    pkNameBuilder.Append(keyNameArray != null && !keyNameArray.IsNull(i)
                        ? keyNameArray.GetString(i) : "");
                    count++;
                }
            }
            var schema = MetadataSchemaFactory.CreatePrimaryKeysSchema();
            return new QueryResult(count, new HiveInfoArrowStream(schema, new IArrowArray[]
            {
                tableCatBuilder.Build(), tableSchemaBuilder.Build(), tableNameBuilder.Build(),
                columnNameBuilder.Build(), keySeqBuilder.Build(), pkNameBuilder.Build()
            }));
        }

        private QueryResult GetCrossReference()
        {
            if (MetadataUtilities.IsInvalidPKFKCatalog(_metadataForeignCatalogName))
                return MetadataSchemaFactory.CreateEmptyCrossReferenceResult();

            if (string.IsNullOrEmpty(_metadataForeignCatalogName) || string.IsNullOrEmpty(_metadataForeignSchemaName) ||
                string.IsNullOrEmpty(_metadataForeignTableName))
                return MetadataSchemaFactory.CreateEmptyCrossReferenceResult();

            try
            {
                string sql = new ShowForeignKeysCommand(
                    _metadataForeignCatalogName!, _metadataForeignSchemaName!, _metadataForeignTableName!).Build();
                var batches = _connection.ExecuteMetadataSql(sql);

                var pkTableCatBuilder = new StringArray.Builder();
                var pkTableSchemaBuilder = new StringArray.Builder();
                var pkTableNameBuilder = new StringArray.Builder();
                var pkColumnNameBuilder = new StringArray.Builder();
                var fkTableCatBuilder = new StringArray.Builder();
                var fkTableSchemaBuilder = new StringArray.Builder();
                var fkTableNameBuilder = new StringArray.Builder();
                var fkColumnNameBuilder = new StringArray.Builder();
                var keySeqBuilder = new Int32Array.Builder();
                var updateRuleBuilder = new Int32Array.Builder();
                var deleteRuleBuilder = new Int32Array.Builder();
                var fkNameBuilder = new StringArray.Builder();
                var pkNameBuilder = new StringArray.Builder();
                var deferrabilityBuilder = new Int32Array.Builder();
                int count = 0;

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
                        pkTableCatBuilder.Append(pkCatalogArray != null && !pkCatalogArray.IsNull(i) ? pkCatalogArray.GetString(i) : _metadataCatalogName ?? "");
                        pkTableSchemaBuilder.Append(pkSchemaArray != null && !pkSchemaArray.IsNull(i) ? pkSchemaArray.GetString(i) : _metadataSchemaName ?? "");
                        pkTableNameBuilder.Append(pkTableArray != null && !pkTableArray.IsNull(i) ? pkTableArray.GetString(i) : _metadataTableName ?? "");
                        pkColumnNameBuilder.Append(pkColArray != null && !pkColArray.IsNull(i) ? pkColArray.GetString(i) : "");
                        fkTableCatBuilder.Append(fkCatalogArray != null && !fkCatalogArray.IsNull(i) ? fkCatalogArray.GetString(i) : _metadataForeignCatalogName!);
                        fkTableSchemaBuilder.Append(fkSchemaArray != null && !fkSchemaArray.IsNull(i) ? fkSchemaArray.GetString(i) : _metadataForeignSchemaName!);
                        fkTableNameBuilder.Append(fkTableArray != null && !fkTableArray.IsNull(i) ? fkTableArray.GetString(i) : _metadataForeignTableName!);
                        fkColumnNameBuilder.Append(fkColArray.GetString(i));
                        keySeqBuilder.Append(fkKeySeqArray != null && !fkKeySeqArray.IsNull(i) ? fkKeySeqArray.GetValue(i)!.Value : count + 1);
                        updateRuleBuilder.Append(fkUpdateRuleArray != null && !fkUpdateRuleArray.IsNull(i) ? fkUpdateRuleArray.GetValue(i)!.Value : 0);
                        deleteRuleBuilder.Append(fkDeleteRuleArray != null && !fkDeleteRuleArray.IsNull(i) ? fkDeleteRuleArray.GetValue(i)!.Value : 0);
                        fkNameBuilder.Append(fkNameArray != null && !fkNameArray.IsNull(i) ? fkNameArray.GetString(i) : "");
                        pkNameBuilder.AppendNull();
                        deferrabilityBuilder.Append(fkDeferrabilityArray != null && !fkDeferrabilityArray.IsNull(i) ? fkDeferrabilityArray.GetValue(i)!.Value : 5);
                        count++;
                    }
                }

                var schema = MetadataSchemaFactory.CreateCrossReferenceSchema();
                return new QueryResult(count, new HiveInfoArrowStream(schema, new IArrowArray[]
                {
                    pkTableCatBuilder.Build(), pkTableSchemaBuilder.Build(), pkTableNameBuilder.Build(),
                    pkColumnNameBuilder.Build(), fkTableCatBuilder.Build(), fkTableSchemaBuilder.Build(),
                    fkTableNameBuilder.Build(), fkColumnNameBuilder.Build(), keySeqBuilder.Build(),
                    updateRuleBuilder.Build(), deleteRuleBuilder.Build(), fkNameBuilder.Build(),
                    pkNameBuilder.Build(), deferrabilityBuilder.Build()
                }));
            }
            catch
            {
                return MetadataSchemaFactory.CreateEmptyCrossReferenceResult();
            }
        }

        private static T? TryGetColumn<T>(RecordBatch batch, string name) where T : class, IArrowArray
        {
            try { return batch.Column(name) as T; }
            catch (ArgumentOutOfRangeException) { return null; }
        }

        // TracingStatement implementation
        public override string AssemblyVersion => GetType().Assembly.GetName().Version?.ToString() ?? "1.0.0";
        public override string AssemblyName => "AdbcDrivers.Databricks";
    }
}
