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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Databricks.Reader;
using Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch;
using Apache.Arrow.Adbc.Tracing;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc.Drivers.Databricks.StatementExecution
{
    /// <summary>
    /// Statement implementation for Databricks Statement Execution REST API.
    /// Executes queries via REST endpoints and supports both inline and external links result dispositions.
    /// </summary>
    internal class StatementExecutionStatement : AdbcStatement, ITracingStatement
    {
        private readonly IStatementExecutionClient _client;
        private readonly string _warehouseId;
        private readonly string? _sessionId;
        private readonly IReadOnlyDictionary<string, string> _properties;
        private readonly HttpClient _httpClient;

        private string? _statementId;
        private GetStatementResponse? _response;
        private bool _disposed;
        private HttpClient? _cloudFetchHttpClient; // Separate HttpClient for CloudFetch downloads

        // Configuration properties
        private readonly string _resultDisposition;
        private readonly string _resultFormat;
        private readonly string? _resultCompression;
        private readonly int _pollingIntervalMs;
        private readonly string? _waitTimeout;
        private readonly bool _enableDirectResults;
        private readonly long _byteLimit;

        // Statement properties
        private string? _catalogName;
        private string? _schemaName;
        private long _maxRows;
        private int _queryTimeoutSeconds;

        // Tracing support
        private readonly ActivityTrace _trace;
        private readonly string? _traceParent;
        private readonly string _assemblyVersion;
        private readonly string _assemblyName;

        /// <summary>
        /// Initializes a new instance of the StatementExecutionStatement class.
        /// </summary>
        /// <param name="client">The Statement Execution API client.</param>
        /// <param name="warehouseId">The warehouse ID for query execution.</param>
        /// <param name="sessionId">Optional session ID for session-scoped execution.</param>
        /// <param name="properties">Connection properties for configuration.</param>
        /// <param name="httpClient">HTTP client for CloudFetch downloads.</param>
        public StatementExecutionStatement(
            IStatementExecutionClient client,
            string warehouseId,
            string? sessionId,
            IReadOnlyDictionary<string, string> properties,
            HttpClient httpClient)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _warehouseId = warehouseId ?? throw new ArgumentNullException(nameof(warehouseId));
            _sessionId = sessionId;
            _properties = properties ?? throw new ArgumentNullException(nameof(properties));
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));

            // Parse configuration from properties
            _resultDisposition = GetPropertyOrDefault(DatabricksParameters.ResultDisposition, "INLINE_OR_EXTERNAL_LINKS");
            _resultFormat = GetPropertyOrDefault(DatabricksParameters.ResultFormat, "ARROW_STREAM");
            _resultCompression = GetPropertyOrDefault(DatabricksParameters.ResultCompression, null);
            _pollingIntervalMs = int.Parse(GetPropertyOrDefault(DatabricksParameters.PollingInterval, "1000"));
            _waitTimeout = GetPropertyOrDefault(DatabricksParameters.WaitTimeout, null);
            _enableDirectResults = bool.Parse(GetPropertyOrDefault(DatabricksParameters.EnableDirectResults, "true"));
            _byteLimit = long.Parse(GetPropertyOrDefault("adbc.databricks.rest.byte_limit", "0"));

            // Initialize catalog and schema from connection properties
            properties.TryGetValue(AdbcOptions.Connection.CurrentCatalog, out _catalogName);
            properties.TryGetValue(AdbcOptions.Connection.CurrentDbSchema, out _schemaName);

            // Initialize tracing
            var assembly = Assembly.GetExecutingAssembly();
            _assemblyName = assembly.GetName().Name ?? "Apache.Arrow.Adbc.Drivers.Databricks";
            _assemblyVersion = assembly.GetName().Version?.ToString() ?? "1.0.0";
            _trace = new ActivityTrace(_assemblyName, _assemblyVersion);
            _traceParent = Activity.Current?.Id;
        }

        /// <summary>
        /// Gets or sets the catalog name for query execution.
        /// </summary>
        public string? CatalogName
        {
            get => _catalogName;
            set => _catalogName = value;
        }

        /// <summary>
        /// Gets or sets the schema name for query execution.
        /// </summary>
        public string? SchemaName
        {
            get => _schemaName;
            set => _schemaName = value;
        }

        /// <summary>
        /// Gets or sets the maximum number of rows to return.
        /// </summary>
        public long MaxRows
        {
            get => _maxRows;
            set => _maxRows = value;
        }

        /// <summary>
        /// Gets or sets the query timeout in seconds.
        /// </summary>
        public int QueryTimeoutSeconds
        {
            get => _queryTimeoutSeconds;
            set => _queryTimeoutSeconds = value;
        }

        /// <summary>
        /// Gets the activity trace for this statement.
        /// </summary>
        public ActivityTrace Trace => _trace;

        /// <summary>
        /// Gets the trace parent ID.
        /// </summary>
        public string? TraceParent => _traceParent;

        /// <summary>
        /// Gets the assembly version.
        /// </summary>
        public string AssemblyVersion => _assemblyVersion;

        /// <summary>
        /// Gets the assembly name.
        /// </summary>
        public string AssemblyName => _assemblyName;

        /// <summary>
        /// Executes a query and returns the results.
        /// </summary>
        /// <returns>Query results with schema and data.</returns>
        public override QueryResult ExecuteQuery()
        {
            return ExecuteQueryAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Executes a query asynchronously and returns the results.
        /// </summary>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>Query results with schema and data.</returns>
        private async Task<QueryResult> ExecuteQueryAsync(CancellationToken cancellationToken)
        {
            ThrowIfDisposed();

            // Build ExecuteStatementRequest
            var request = new ExecuteStatementRequest
            {
                Statement = SqlQuery,
                Disposition = _resultDisposition,
                Format = _resultFormat,
                // Parameters = ConvertParameters() // TODO: Implement parameter conversion
            };

            // Set warehouse_id or session_id (mutually exclusive)
            if (_sessionId != null)
            {
                request.SessionId = _sessionId;
            }
            else
            {
                request.WarehouseId = _warehouseId;
                request.Catalog = _catalogName;
                request.Schema = _schemaName;
            }

            // Set compression (skip for inline results)
            if (request.Disposition != "INLINE")
            {
                request.ResultCompression = _resultCompression ?? "lz4";
            }

            // Set wait_timeout (skip if direct results mode is enabled)
            if (!_enableDirectResults && _waitTimeout != null)
            {
                request.WaitTimeout = _waitTimeout;
                request.OnWaitTimeout = "CONTINUE";
            }

            // Set row/byte limits
            if (_maxRows > 0)
            {
                request.RowLimit = _maxRows;
            }
            if (_byteLimit > 0)
            {
                request.ByteLimit = _byteLimit;
            }

            // Execute statement
            var executeResponse = await _client.ExecuteStatementAsync(request, cancellationToken).ConfigureAwait(false);
            _statementId = executeResponse.StatementId;

            // Poll until completion if async
            if (executeResponse.Status?.State == "PENDING" || executeResponse.Status?.State == "RUNNING")
            {
                _response = await PollUntilCompleteAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                _response = new GetStatementResponse
                {
                    StatementId = executeResponse.StatementId,
                    Status = executeResponse.Status,
                    Manifest = executeResponse.Manifest,
                    Result = executeResponse.Result
                };
            }

            // Handle errors
            if (_response.Status?.State == "FAILED")
            {
                throw new AdbcException(
                    _response.Status.Error?.Message ?? "Query execution failed",
                    AdbcStatusCode.UnknownError);
            }

            // Check if results were truncated
            if (_response.Manifest?.Truncated == true)
            {
                // Log warning (would need logger instance)
                Debug.WriteLine($"Results truncated by row_limit or byte_limit for statement {_statementId}");
            }

            // Create reader based on actual disposition in response
            IArrowArrayStream reader = CreateReader(_response);

            return new QueryResult(
                _response.Manifest?.TotalRowCount ?? 0,
                reader);
        }

        /// <summary>
        /// Polls the statement until it completes or fails.
        /// </summary>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>The final statement response.</returns>
        private async Task<GetStatementResponse> PollUntilCompleteAsync(CancellationToken cancellationToken)
        {
            int pollCount = 0;
            var startTime = DateTime.UtcNow;

            while (true)
            {
                // First poll happens immediately (no delay)
                if (pollCount > 0)
                {
                    await Task.Delay(_pollingIntervalMs, cancellationToken).ConfigureAwait(false);
                }

                // Check timeout
                if (_queryTimeoutSeconds > 0)
                {
                    var elapsed = (DateTime.UtcNow - startTime).TotalSeconds;
                    if (elapsed > _queryTimeoutSeconds)
                    {
                        await _client.CancelStatementAsync(_statementId!, cancellationToken).ConfigureAwait(false);
                        throw new AdbcException(
                            $"Query timeout exceeded ({_queryTimeoutSeconds}s) for statement {_statementId}",
                            AdbcStatusCode.Timeout);
                    }
                }

                var status = await _client.GetStatementAsync(_statementId!, cancellationToken).ConfigureAwait(false);

                if (status.Status?.State == "SUCCEEDED" ||
                    status.Status?.State == "FAILED" ||
                    status.Status?.State == "CANCELED" ||
                    status.Status?.State == "CLOSED")
                {
                    return status;
                }

                pollCount++;
            }
        }

        /// <summary>
        /// Creates the appropriate reader based on the response disposition.
        /// </summary>
        /// <param name="response">The statement execution response.</param>
        /// <returns>An Arrow array stream reader.</returns>
        private IArrowArrayStream CreateReader(GetStatementResponse response)
        {
            // Determine actual disposition from response
            var hasExternalLinks = response.Manifest?.Chunks?
                .Any(c => c.ExternalLinks != null && c.ExternalLinks.Any()) == true;
            var hasInlineData = response.Manifest?.Chunks?
                .Any(c => c.Attachment != null && c.Attachment.Length > 0) == true;

            if (hasExternalLinks)
            {
                // External links - use CloudFetch pipeline
                return CreateExternalLinksReader(response);
            }
            else if (hasInlineData)
            {
                // Inline data - parse directly
                return CreateInlineReader(response);
            }
            else
            {
                // Empty result set
                return CreateEmptyReader(response);
            }
        }

        /// <summary>
        /// Creates a reader for external links results using the CloudFetch pipeline.
        /// </summary>
        /// <param name="response">The statement execution response.</param>
        /// <returns>A CloudFetch reader.</returns>
        private IArrowArrayStream CreateExternalLinksReader(GetStatementResponse response)
        {
            if (response.Manifest == null)
            {
                throw new InvalidOperationException("Manifest is required for external links disposition");
            }

            // Convert REST API schema to Arrow schema
            var schema = ConvertSchema(response.Manifest.Schema);

            // Determine compression
            bool isLz4Compressed = response.Manifest.ResultCompression?.Equals("lz4", StringComparison.OrdinalIgnoreCase) == true;

            // Create memory manager
            int memoryBufferSizeMB = int.Parse(GetPropertyOrDefault(DatabricksParameters.CloudFetchMemoryBufferSize, "200"));
            var memoryManager = new CloudFetchMemoryBufferManager(memoryBufferSizeMB);

            // Create download and result queues
            var downloadQueue = new BlockingCollection<IDownloadResult>(new ConcurrentQueue<IDownloadResult>(), 10);
            var resultQueue = new BlockingCollection<IDownloadResult>(new ConcurrentQueue<IDownloadResult>(), 10);

            // Create result fetcher
            var resultFetcher = new StatementExecutionResultFetcher(
                _client,
                response.StatementId,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
                response);  // Pass full response to use Result field
>>>>>>> defec99 (fix(csharp): use GetStatementResponse.Result and follow next_chunk_index chain)
=======
                response.Manifest);
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
                response);  // Pass full response to use Result field
>>>>>>> defec99 (fix(csharp): use GetStatementResponse.Result and follow next_chunk_index chain)

            // 2. Parse configuration from REST properties (unified properties work for both Thrift and REST)
            var config = CloudFetchConfiguration.FromProperties(
                _properties,
                schema,
                isLz4Compressed);

            // 3. Create a separate HttpClient for CloudFetch downloads if not already created
            // This allows us to set CloudFetch-specific timeout without affecting API calls
            if (_cloudFetchHttpClient == null)
            {
                _cloudFetchHttpClient = new HttpClient();
            }

            // 4. Create protocol-agnostic download manager
            // Manager creates shared resources and calls Initialize() on the fetcher
            var downloadManager = new CloudFetchDownloadManager(
                resultFetcher,              // Protocol-specific fetcher
                _cloudFetchHttpClient,      // Dedicated HttpClient for CloudFetch
                config,
                this);                      // ITracingStatement for tracing

            // 5. Start the manager
            downloadManager.StartAsync().GetAwaiter().GetResult();

            // 6. Create protocol-agnostic reader

            // 2. Parse configuration from REST properties (unified properties work for both Thrift and REST)
            var config = CloudFetchConfiguration.FromProperties(
                schema,
                isLz4Compressed);

            // Manager creates shared resources and calls Initialize() on the fetcher
            var downloadManager = new CloudFetchDownloadManager(
                config,
                this);                // ITracingStatement for tracing

            downloadManager.StartAsync().GetAwaiter().GetResult();

            return new CloudFetchReader(
                this,                 // ITracingStatement (both Thrift and REST implement this)
                schema,
                null,                 // IResponse (REST doesn't use IResponse)
                downloadManager);
                memoryManager,
                downloadQueue);

            // Create downloader with correct parameters
            int parallelDownloads = int.Parse(GetPropertyOrDefault(DatabricksParameters.CloudFetchParallelDownloads, "3"));
            int maxRetries = int.Parse(GetPropertyOrDefault(DatabricksParameters.CloudFetchMaxRetries, "3"));
            int retryDelayMs = int.Parse(GetPropertyOrDefault(DatabricksParameters.CloudFetchRetryDelayMs, "500"));
            int urlExpirationBufferSeconds = int.Parse(GetPropertyOrDefault(DatabricksParameters.CloudFetchUrlExpirationBufferSeconds, "60"));
            int maxUrlRefreshAttempts = int.Parse(GetPropertyOrDefault(DatabricksParameters.CloudFetchMaxUrlRefreshAttempts, "3"));

            var downloader = new CloudFetchDownloader(
                this, // Pass this as ITracingStatement
                downloadQueue,
                resultQueue,
                memoryManager,
                _httpClient,
                resultFetcher,
                parallelDownloads,
                isLz4Compressed,
                maxRetries,
                retryDelayMs,
                maxUrlRefreshAttempts,
                urlExpirationBufferSeconds);

            // Create download manager
            var downloadManager = new CloudFetchDownloadManager(
                null, // statement parameter is nullable for REST API
                schema,
                isLz4Compressed,
                resultFetcher,
                downloader);

            // Start the download manager
            downloadManager.StartAsync().GetAwaiter().GetResult();

            // Create and return a simple reader that uses the download manager
        }

        /// <summary>
        /// Creates a reader for inline results.
        /// </summary>
        /// <param name="response">The statement execution response.</param>
        /// <returns>An inline reader.</returns>
        private IArrowArrayStream CreateInlineReader(GetStatementResponse response)
        {
            if (response.Manifest == null)
            {
                throw new InvalidOperationException("Manifest is required for inline disposition");
            }

            return new InlineReader(response.Manifest);
        }

        /// <summary>
        /// Creates a reader for empty result sets.
        /// </summary>
        /// <param name="response">The statement execution response.</param>
        /// <returns>An empty reader.</returns>
        private IArrowArrayStream CreateEmptyReader(GetStatementResponse response)
        {
            // For empty results, create a schema with no columns if manifest doesn't have schema
            var schema = response.Manifest?.Schema != null
                ? ConvertSchema(response.Manifest.Schema)
                : new Schema(new List<Field>(), null);

            return new EmptyArrowArrayStream(schema);
        }

        /// <summary>
        /// Converts a REST API result schema to an Arrow schema.
        /// </summary>
        /// <param name="resultSchema">The REST API result schema.</param>
        /// <returns>An Arrow schema.</returns>
        private Schema ConvertSchema(ResultSchema? resultSchema)
        {
            if (resultSchema?.Columns == null || resultSchema.Columns.Count == 0)
            {
                return new Schema(new List<Field>(), null);
            }

            var fields = new List<Field>();
            foreach (var column in resultSchema.Columns)
            {
                // TODO: Implement proper type conversion from REST API types to Arrow types
                // For now, use string type as fallback
                var arrowType = ConvertType(column.TypeText);
                var field = new Field(column.Name ?? $"col_{column.Position}", arrowType, nullable: true);
                fields.Add(field);
            }

            return new Schema(fields, null);
        }

        /// <summary>
        /// Converts a REST API type string to an Arrow type.
        /// </summary>
        /// <param name="typeText">The type text from REST API.</param>
        /// <returns>An Arrow data type.</returns>
        private IArrowType ConvertType(string? typeText)
        {
            // TODO: Implement comprehensive type mapping
            // This is a simplified implementation
            if (string.IsNullOrEmpty(typeText))
            {
                return StringType.Default;
            }

            var lowerType = typeText.ToLowerInvariant();

            if (lowerType.Contains("int")) return Int64Type.Default;
            if (lowerType.Contains("long")) return Int64Type.Default;
            if (lowerType.Contains("double")) return DoubleType.Default;
            if (lowerType.Contains("float")) return FloatType.Default;
            if (lowerType.Contains("bool")) return BooleanType.Default;
            if (lowerType.Contains("string")) return StringType.Default;
            if (lowerType.Contains("binary")) return BinaryType.Default;
            if (lowerType.Contains("date")) return Date64Type.Default;
            if (lowerType.Contains("timestamp")) return new TimestampType(TimeUnit.Microsecond, timezone: (string?)null);

            // Default to string for unknown types
            return StringType.Default;
        }

        /// <summary>
        /// Gets a property value or returns a default value if not found.
        /// </summary>
        /// <param name="key">The property key.</param>
        /// <param name="defaultValue">The default value.</param>
        /// <returns>The property value or default.</returns>
        private string GetPropertyOrDefault(string key, string? defaultValue)
        {
            return _properties.TryGetValue(key, out var value) ? value : defaultValue ?? string.Empty;
        }

        /// <summary>
        /// Executes an update statement (INSERT, UPDATE, DELETE, etc.) and returns affected row count.
        /// </summary>
        /// <returns>Update results with affected row count.</returns>
        public override UpdateResult ExecuteUpdate()
        {
            // Execute the query to get the results
            var queryResult = ExecuteQuery();

            // For DML statements, the manifest should contain the row count
            // If not available, return -1 (unknown)
            long affectedRows = _response?.Manifest?.TotalRowCount ?? -1;

            // Dispose the reader since we don't need the data
            queryResult.Stream?.Dispose();

            return new UpdateResult(affectedRows);
        }

        /// <summary>
        /// Disposes the statement and releases resources.
        /// </summary>
        public override void Dispose()
        {
            if (!_disposed)
            {
                // Close statement if it was created
                if (_statementId != null)
                {
                    try
                    {
                        _client.CloseStatementAsync(_statementId, CancellationToken.None)
                            .GetAwaiter().GetResult();
                    }
                    catch (Exception)
                    {
                        // Swallow exceptions during disposal
                        // TODO: Consider logging this error
                    }
                }

                // Dispose CloudFetch HttpClient if it was created
                _cloudFetchHttpClient?.Dispose();

                base.Dispose();
                _disposed = true;
            }
        }

        /// <summary>
        /// Throws if the statement has been disposed.
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(StatementExecutionStatement));
            }
        }

        /// <summary>
        /// Empty Arrow array stream for empty result sets.
        /// </summary>
        private class EmptyArrowArrayStream : IArrowArrayStream
        {
            private readonly Schema _schema;

            public EmptyArrowArrayStream(Schema schema)
            {
                _schema = schema;
            }

            public Schema Schema => _schema;

            public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                return new ValueTask<RecordBatch?>(Task.FromResult<RecordBatch?>(null));
            }

            public void Dispose()
            {
                // Nothing to dispose
            }
        }

        /// <summary>
        /// Simple reader for CloudFetch results using ICloudFetchDownloadManager.
        /// </summary>
        private class SimpleCloudFetchReader : IArrowArrayStream
        {
            private readonly ICloudFetchDownloadManager _downloadManager;
            private readonly string? _compressionCodec;
            private readonly Schema _schema;
            private bool _disposed;

            public SimpleCloudFetchReader(ICloudFetchDownloadManager downloadManager, string? compressionCodec, Schema schema)
            {
                _downloadManager = downloadManager ?? throw new ArgumentNullException(nameof(downloadManager));
                _compressionCodec = compressionCodec;
                _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            }

            public Schema Schema => _schema;

            public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(SimpleCloudFetchReader));
                }

                var downloadResult = await _downloadManager.GetNextDownloadedFileAsync(cancellationToken).ConfigureAwait(false);

                if (downloadResult == null)
                {
                    return null; // End of stream
                }

                var stream = downloadResult.DataStream;

                // Decompress if needed
                if (!string.IsNullOrEmpty(_compressionCodec) && _compressionCodec.Equals("lz4", StringComparison.OrdinalIgnoreCase))
                {
                    stream = DecompressLz4(stream);
                }

                // Read Arrow IPC format
                using var reader = new ArrowStreamReader(stream);
                var batch = await reader.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false);
                return batch;
            }

            private System.IO.Stream DecompressLz4(System.IO.Stream compressedStream)
            {
                // TODO: Implement LZ4 decompression
                // For now, assume data is not compressed or already decompressed
                return compressedStream;
            }

            public void Dispose()
            {
                if (!_disposed)
                {
                    _downloadManager?.Dispose();
                    _disposed = true;
                }
            }
        }
    }
}
