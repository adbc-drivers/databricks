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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
            _resultDisposition = GetPropertyOrDefault(DatabricksParameters.ResultDisposition, "inline_or_external_links");
            _resultFormat = GetPropertyOrDefault(DatabricksParameters.ResultFormat, "arrow_stream");
<<<<<<< HEAD
=======
            _resultDisposition = GetPropertyOrDefault(DatabricksParameters.ResultDisposition, "INLINE_OR_EXTERNAL_LINKS");
            _resultFormat = GetPropertyOrDefault(DatabricksParameters.ResultFormat, "ARROW_STREAM");
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
            _resultDisposition = GetPropertyOrDefault(DatabricksParameters.ResultDisposition, "inline_or_external_links");
            _resultFormat = GetPropertyOrDefault(DatabricksParameters.ResultFormat, "arrow_stream");
>>>>>>> cd94a4b (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
            _resultDisposition = GetPropertyOrDefault(DatabricksParameters.ResultDisposition, "inline_or_external_links");
            _resultFormat = GetPropertyOrDefault(DatabricksParameters.ResultFormat, "arrow_stream");
>>>>>>> 7c1e247 (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
            _resultDisposition = GetPropertyOrDefault(DatabricksParameters.ResultDisposition, "INLINE_OR_EXTERNAL_LINKS");
            _resultFormat = GetPropertyOrDefault(DatabricksParameters.ResultFormat, "ARROW_STREAM");
>>>>>>> cf3a928 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
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

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
            // Set warehouse_id or session_id (mutually exclusive)
<<<<<<< HEAD
<<<<<<< HEAD
=======
            // Set warehouse_id (always required) and session_id if available
            request.WarehouseId = _warehouseId;

>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
            // Set warehouse_id or session_id (mutually exclusive)
>>>>>>> cd94a4b (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
            // Set warehouse_id (always required) and session_id if available
            request.WarehouseId = _warehouseId;

>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
=======
            // Set warehouse_id (always required) and session_id if available
            request.WarehouseId = _warehouseId;

>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
            // Set warehouse_id or session_id (mutually exclusive)
>>>>>>> cd94a4b (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
            // Set warehouse_id (always required) and session_id if available
            request.WarehouseId = _warehouseId;

>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
            if (_sessionId != null)
            {
                request.SessionId = _sessionId;
            }
            else
            {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
                request.WarehouseId = _warehouseId;
<<<<<<< HEAD
<<<<<<< HEAD
=======
                // Only set catalog/schema when not using a session
                // (sessions have their own catalog/schema)
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
                request.WarehouseId = _warehouseId;
>>>>>>> cd94a4b (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
                // Only set catalog/schema when not using a session
                // (sessions have their own catalog/schema)
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
=======
                // Only set catalog/schema when not using a session
                // (sessions have their own catalog/schema)
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
                request.WarehouseId = _warehouseId;
>>>>>>> cd94a4b (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
                // Only set catalog/schema when not using a session
                // (sessions have their own catalog/schema)
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
                request.Catalog = _catalogName;
                request.Schema = _schemaName;
            }

            // Set compression (skip for inline results)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
            if (request.Disposition != "inline")
<<<<<<< HEAD
=======
            if (request.Disposition != "INLINE")
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
            if (request.Disposition != "inline")
>>>>>>> cd94a4b (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
            if (request.Disposition != "inline")
>>>>>>> 7c1e247 (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
            if (request.Disposition != "INLINE")
>>>>>>> cf3a928 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
            {
                request.ResultCompression = _resultCompression ?? "lz4";
            }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
            // Set wait_timeout (skip if direct results mode is enabled)
            if (!_enableDirectResults)
<<<<<<< HEAD
<<<<<<< HEAD
=======
            // Set wait_timeout (skip if direct results mode is enabled OR using a session)
            // Sessions don't support wait_timeout parameter
            if (!_enableDirectResults && _sessionId == null)
<<<<<<< HEAD
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
            // Set wait_timeout (skip if direct results mode is enabled)
            if (!_enableDirectResults && _waitTimeout != null)
>>>>>>> cd94a4b (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
            // Set wait_timeout (skip if direct results mode is enabled OR using a session)
            // Sessions don't support wait_timeout parameter
            if (!_enableDirectResults && _sessionId == null)
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
=======
            if (!_enableDirectResults && _waitTimeout != null)
>>>>>>> 7c1e247 (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
            {
                request.WaitTimeout = _waitTimeout;
=======
=======
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
            // Set wait_timeout (skip if direct results mode is enabled OR using a session)
            // Sessions don't support wait_timeout parameter
            if (!_enableDirectResults && _sessionId == null)
            if (!_enableDirectResults && _waitTimeout != null)
            {
<<<<<<< HEAD
                request.WaitTimeout = _waitTimeout ?? "10s";  // Default to 10s if not specified
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
            // Set wait_timeout (skip if direct results mode is enabled)
            if (!_enableDirectResults && _waitTimeout != null)
            {
                request.WaitTimeout = _waitTimeout;
<<<<<<< HEAD
>>>>>>> cd94a4b (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
                request.WaitTimeout = _waitTimeout;
>>>>>>> 7c1e247 (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
            // Set wait_timeout (skip if direct results mode is enabled OR using a session)
            // Sessions don't support wait_timeout parameter
            if (!_enableDirectResults && _sessionId == null)
            {
                request.WaitTimeout = _waitTimeout ?? "10s";  // Default to 10s if not specified
>>>>>>> cf3a928 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
            // Check if response is in JSON_ARRAY format (fallback when Arrow not supported)
            bool isJsonFormat = response.Manifest?.Format?.Equals("JSON_ARRAY", StringComparison.OrdinalIgnoreCase) == true;
=======
            // Check if response is in JSON_ARRAY format (fallback when Arrow not supported)
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
=======
            // Check if response is in JSON_ARRAY format (fallback when Arrow not supported)
            bool isJsonFormat = response.Manifest?.Format?.Equals("JSON_ARRAY", StringComparison.OrdinalIgnoreCase) == true;
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
            // Check if response is in JSON_ARRAY format (fallback when Arrow not supported)
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)

            if (isJsonFormat)
            {
                // JSON format - convert to Arrow
                return CreateJsonArrayReader(response);
            }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
            // Determine actual disposition from response
            // Check Result field first (contains actual data for this response)
            var hasExternalLinks = (response.Result?.ExternalLinks != null && response.Result.ExternalLinks.Any()) ||
<<<<<<< HEAD
                (response.Manifest?.Chunks?.Any(c => c.ExternalLinks != null && c.ExternalLinks.Any()) == true);
<<<<<<< HEAD
<<<<<<< HEAD
            var hasInlineData = response.Manifest?.Chunks?
=======
=======
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
=======
>>>>>>> 7c1e247 (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
=======
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
            // Determine actual disposition from response
            // Check Result field first (contains actual data for this response)
            var hasExternalLinks = (response.Result?.ExternalLinks != null && response.Result.ExternalLinks.Any()) ||
<<<<<<< HEAD
                (response.Manifest?.Chunks?.Any(c => c.ExternalLinks != null && c.ExternalLinks.Any()) == true);
<<<<<<< HEAD
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
=======
>>>>>>> 7c1e247 (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)

            // Check for inline data in Result field (INLINE disposition with Arrow bytes)
            var hasInlineResult = response.Result?.Attachment != null && response.Result.Attachment.Length > 0;

            // Check for inline data in Manifest chunks (INLINE_OR_EXTERNAL_LINKS with Arrow bytes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
            var hasInlineManifest = response.Manifest?.Chunks?
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
            // Determine actual disposition from response
            var hasExternalLinks = response.Manifest?.Chunks?
                .Any(c => c.ExternalLinks != null && c.ExternalLinks.Any()) == true;
            var hasInlineData = response.Manifest?.Chunks?
>>>>>>> cd94a4b (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
=======
            // Determine actual disposition from response
                .Any(c => c.ExternalLinks != null && c.ExternalLinks.Any()) == true;
>>>>>>> 7c1e247 (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
            var hasInlineManifest = response.Manifest?.Chunks?
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
            // Determine actual disposition from response
            var hasExternalLinks = response.Manifest?.Chunks?
                .Any(c => c.ExternalLinks != null && c.ExternalLinks.Any()) == true;
            var hasInlineData = response.Manifest?.Chunks?
>>>>>>> cd94a4b (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
=======
            // Determine actual disposition from response
                .Any(c => c.ExternalLinks != null && c.ExternalLinks.Any()) == true;
>>>>>>> 7c1e247 (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
                .Any(c => c.Attachment != null && c.Attachment.Length > 0) == true;

            if (hasExternalLinks)
            {
                // External links - use CloudFetch pipeline
                return CreateExternalLinksReader(response);
            }
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
            else if (hasInlineData)
=======
            else if (hasInlineResult || hasInlineManifest)
>>>>>>> e791f48 (feat(csharp): add JSON_ARRAY format support and complete GetObjects implementation for   Statement Execution API)
            {
                // Inline data - parse directly
<<<<<<< HEAD
=======
            else if (hasInlineResult || hasInlineManifest)
            {
<<<<<<< HEAD
                // Inline Arrow data - parse directly
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
            else if (hasInlineData)
=======
            else if (hasInlineResult || hasInlineManifest)
>>>>>>> e791f48 (feat(csharp): add JSON_ARRAY format support and complete GetObjects implementation for   Statement Execution API)
            {
                // Inline data - parse directly
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cd94a4b (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
            else if (hasInlineResult || hasInlineManifest)
            {
<<<<<<< HEAD
                // Inline Arrow data - parse directly
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
=======
                // Inline data - parse directly
<<<<<<< HEAD
>>>>>>> 7c1e247 (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
                // Inline Arrow data - parse directly
>>>>>>> e791f48 (feat(csharp): add JSON_ARRAY format support and complete GetObjects implementation for   Statement Execution API)
=======
            else if (hasInlineResult || hasInlineManifest)
            {
                // Inline Arrow data - parse directly
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
            else if (hasInlineData)
            {
                // Inline data - parse directly
>>>>>>> cd94a4b (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
            else if (hasInlineResult || hasInlineManifest)
            {
                // Inline Arrow data - parse directly
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
=======
                // Inline data - parse directly
>>>>>>> 7c1e247 (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
                // Inline Arrow data - parse directly
>>>>>>> e791f48 (feat(csharp): add JSON_ARRAY format support and complete GetObjects implementation for   Statement Execution API)
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
        /// Follows the protocol-agnostic pattern: Create fetcher → Parse config → Create manager → Start → Create reader.
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

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7c1e247 (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
>>>>>>> cd94a4b (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
>>>>>>> 7c1e247 (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
            // Create memory manager
            int memoryBufferSizeMB = int.Parse(GetPropertyOrDefault(DatabricksParameters.CloudFetchMemoryBufferSize, "200"));
            var memoryManager = new CloudFetchMemoryBufferManager(memoryBufferSizeMB);

            // Create download and result queues
            var downloadQueue = new BlockingCollection<IDownloadResult>(new ConcurrentQueue<IDownloadResult>(), 10);
            var resultQueue = new BlockingCollection<IDownloadResult>(new ConcurrentQueue<IDownloadResult>(), 10);

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 14b246a (fix(csharp): implement RefreshUrlsAsync for REST API with 1-hour URL expiration)
=======
>>>>>>> 14b246a (fix(csharp): implement RefreshUrlsAsync for REST API with 1-hour URL expiration)
            // If Result field has external links, add them to the download queue first
            // (Result contains the first chunk, Manifest may not include it for large results)
            if (response.Result?.ExternalLinks != null && response.Result.ExternalLinks.Any())
            {
                foreach (var link in response.Result.ExternalLinks)
                {
                    var expirationTime = DateTime.UtcNow.AddHours(1);
                    if (!string.IsNullOrEmpty(link.Expiration))
                    {
                        try
                        {
                            expirationTime = DateTime.Parse(link.Expiration, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind);
                        }
                        catch (FormatException) { }
                    }

                    var downloadResult = new DownloadResult(
                        chunkIndex: link.ChunkIndex,
                        fileUrl: link.ExternalLinkUrl,
                        startRowOffset: link.RowOffset,
                        rowCount: link.RowCount,
                        byteCount: link.ByteCount,
                        expirationTime: expirationTime,
                        memoryManager: memoryManager,
                        httpHeaders: link.HttpHeaders);

                    downloadQueue.Add(downloadResult);
                }
            }

<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 77c7a19 (fix(csharp): implement RefreshUrlsAsync for REST API with 1-hour URL expiration)
=======
>>>>>>> 14b246a (fix(csharp): implement RefreshUrlsAsync for REST API with 1-hour URL expiration)
=======
>>>>>>> 14b246a (fix(csharp): implement RefreshUrlsAsync for REST API with 1-hour URL expiration)
            // Create result fetcher
=======
            // 1. Create REST-specific result fetcher
            // Resources (memory manager, download queue) will be initialized by CloudFetchDownloadManager
>>>>>>> d40cb8b (fix(csharp): update StatementExecutionStatement to use protocol-agnostic CloudFetch pattern)
=======
            // Create result fetcher
>>>>>>> 7c1e247 (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
            // 1. Create REST-specific result fetcher
            // Resources (memory manager, download queue) will be initialized by CloudFetchDownloadManager
>>>>>>> 1f5f8a2 (fix(csharp): update StatementExecutionStatement to use protocol-agnostic CloudFetch pattern)
=======
            // 1. Create REST-specific result fetcher
            // Resources (memory manager, download queue) will be initialized by CloudFetchDownloadManager
>>>>>>> d40cb8b (fix(csharp): update StatementExecutionStatement to use protocol-agnostic CloudFetch pattern)
=======
            // Create result fetcher
>>>>>>> 7c1e247 (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
            // 1. Create REST-specific result fetcher
            // Resources (memory manager, download queue) will be initialized by CloudFetchDownloadManager
>>>>>>> 1f5f8a2 (fix(csharp): update StatementExecutionStatement to use protocol-agnostic CloudFetch pattern)
            var resultFetcher = new StatementExecutionResultFetcher(
                _client,
                response.StatementId,
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
                response.Manifest);
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
                response);  // Pass full response to use Result field
>>>>>>> defec99 (fix(csharp): use GetStatementResponse.Result and follow next_chunk_index chain)
=======
            // 1. Create REST-specific result fetcher
            // Resources (memory manager, download queue) will be initialized by CloudFetchDownloadManager
=======
            // Create result fetcher
>>>>>>> cd94a4b (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
            var resultFetcher = new StatementExecutionResultFetcher(
                _client,
                response.StatementId,
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d876949 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
=======
>>>>>>> d876949 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)

            // 2. Parse configuration from REST properties (unified properties work for both Thrift and REST)
            var config = CloudFetchConfiguration.FromProperties(
                schema,
                isLz4Compressed);

            // Manager creates shared resources and calls Initialize() on the fetcher
            var downloadManager = new CloudFetchDownloadManager(
                config,
                this);                // ITracingStatement for tracing

            downloadManager.StartAsync().GetAwaiter().GetResult();

<<<<<<< HEAD
<<<<<<< HEAD
            return new CloudFetchReader(
                this,                 // ITracingStatement (both Thrift and REST implement this)
                schema,
                null,                 // IResponse (REST doesn't use IResponse)
                downloadManager);
                memoryManager,
                downloadQueue);

            // 2. Parse configuration from REST properties (unified properties work for both Thrift and REST)
            var config = CloudFetchConfiguration.FromProperties(
                _properties,
                schema,
                isLz4Compressed);

            // 3. Create protocol-agnostic download manager
            // Manager creates shared resources and calls Initialize() on the fetcher
            var downloadManager = new CloudFetchDownloadManager(
                resultFetcher,        // Protocol-specific fetcher
                _httpClient,
                config,
                this);                // ITracingStatement for tracing

            // 4. Start the manager
            downloadManager.StartAsync().GetAwaiter().GetResult();

            // Create and return a simple reader that uses the download manager
            // 5. Create protocol-agnostic reader
=======
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
>>>>>>> d876949 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
            return new CloudFetchReader(
                this,                 // ITracingStatement (both Thrift and REST implement this)
                schema,
                null,                 // IResponse (REST doesn't use IResponse)
                downloadManager);
                memoryManager,
                downloadQueue);

            // 2. Parse configuration from REST properties (unified properties work for both Thrift and REST)
            var config = CloudFetchConfiguration.FromProperties(
                _properties,

            // 2. Parse configuration from REST properties (unified properties work for both Thrift and REST)
            var config = CloudFetchConfiguration.FromProperties(
                schema,
                isLz4Compressed);
                memoryManager,
                downloadQueue);

            // 2. Parse configuration from REST properties (unified properties work for both Thrift and REST)
            var config = CloudFetchConfiguration.FromProperties(
                _properties,
                schema,
                isLz4Compressed);

            // 3. Create protocol-agnostic download manager
            // Manager creates shared resources and calls Initialize() on the fetcher
            var downloadManager = new CloudFetchDownloadManager(
                resultFetcher,        // Protocol-specific fetcher
                _httpClient,
                config,
                this);                // ITracingStatement for tracing

            // 4. Start the manager
            downloadManager.StartAsync().GetAwaiter().GetResult();

            // Create and return a simple reader that uses the download manager
            // 5. Create protocol-agnostic reader
=======
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
>>>>>>> d876949 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
            return new CloudFetchReader(
                this,                 // ITracingStatement (both Thrift and REST implement this)
                schema,
                null,                 // IResponse (REST doesn't use IResponse)
                downloadManager);
                memoryManager,
                downloadQueue);

            // 2. Parse configuration from REST properties (unified properties work for both Thrift and REST)
            var config = CloudFetchConfiguration.FromProperties(
                _properties,

            // 2. Parse configuration from REST properties (unified properties work for both Thrift and REST)
            var config = CloudFetchConfiguration.FromProperties(
                schema,
                isLz4Compressed);
                memoryManager,
                downloadQueue);

            // 2. Parse configuration from REST properties (unified properties work for both Thrift and REST)
            var config = CloudFetchConfiguration.FromProperties(
                _properties,

            // 2. Parse configuration from REST properties (unified properties work for both Thrift and REST)
            var config = CloudFetchConfiguration.FromProperties(
                schema,
                isLz4Compressed);

            // 3. Create protocol-agnostic download manager
            // Manager creates shared resources and calls Initialize() on the fetcher
            var downloadManager = new CloudFetchDownloadManager(
                resultFetcher,        // Protocol-specific fetcher
                _httpClient,
                config,
                this);                // ITracingStatement for tracing

            // 4. Start the manager
            downloadManager.StartAsync().GetAwaiter().GetResult();

            // Create and return a simple reader that uses the download manager
            // 5. Create protocol-agnostic reader
            return new CloudFetchReader(
                this,                 // ITracingStatement (both Thrift and REST implement this)
                schema,
                null,                 // IResponse (REST doesn't use IResponse)
                downloadManager);
        }

        /// <summary>
        /// Creates a reader for inline results.
        /// </summary>
        /// <param name="response">The statement execution response.</param>
        /// <returns>An inline reader.</returns>
        private IArrowArrayStream CreateInlineReader(GetStatementResponse response)
        {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
            if (response.Manifest == null)
=======
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
=======
            // For INLINE disposition, data is in response.Result
            if (response.Result != null && response.Result.Attachment != null && response.Result.Attachment.Length > 0)
>>>>>>> e791f48 (feat(csharp): add JSON_ARRAY format support and complete GetObjects implementation for   Statement Execution API)
=======
            // For INLINE disposition, data is in response.Result
            if (response.Result != null && response.Result.Attachment != null && response.Result.Attachment.Length > 0)
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
            if (response.Manifest == null)
>>>>>>> cd94a4b (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
=======
            // For INLINE disposition, data is in response.Result
            if (response.Result != null && response.Result.Attachment != null && response.Result.Attachment.Length > 0)
>>>>>>> e791f48 (feat(csharp): add JSON_ARRAY format support and complete GetObjects implementation for   Statement Execution API)
            {
                // Check if data is compressed (manifest contains compression metadata)
                byte[] attachmentData = response.Result.Attachment;
                string? compression = response.Manifest?.ResultCompression;

                // Decompress if necessary
                if (!string.IsNullOrEmpty(compression) && !compression.Equals("none", StringComparison.OrdinalIgnoreCase))
                {
                    if (compression.Equals("lz4", StringComparison.OrdinalIgnoreCase))
                    {
                        var decompressed = Lz4Utilities.DecompressLz4(attachmentData);
                        attachmentData = decompressed.ToArray();
                    }
                    else
                    {
                        throw new NotSupportedException($"Compression type '{compression}' is not supported for inline results");
                    }
                }

                // Convert ResultData to ResultManifest format for InlineReader
                var manifest = new ResultManifest
                {
                    Format = "arrow_stream",
                    Chunks = new List<ResultChunk>
                    {
                        new ResultChunk
                        {
                            ChunkIndex = (int)(response.Result.ChunkIndex ?? 0),
                            RowCount = response.Result.RowCount ?? 0,
                            RowOffset = response.Result.RowOffset ?? 0,
                            ByteCount = response.Result.ByteCount ?? 0,
                            Attachment = attachmentData  // Use decompressed data
                        }
                    }
                };

                return new InlineReader(manifest);
            }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
            return new InlineReader(response.Manifest);
=======
            // For INLINE disposition, data is in response.Result
            if (response.Result != null && response.Result.Attachment != null && response.Result.Attachment.Length > 0)
=======
            if (response.Manifest == null)
>>>>>>> cd94a4b (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
            {
                throw new InvalidOperationException("Manifest is required for inline disposition");
            }

<<<<<<< HEAD
=======
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
            // For INLINE_OR_EXTERNAL_LINKS disposition with inline data, data is in response.Manifest
            // These chunks should already be decompressed by the server or need similar handling
            if (response.Manifest != null)
            {
                // Check if manifest chunks need decompression
                if (response.Manifest.Chunks != null && response.Manifest.Chunks.Count > 0)
                {
                    string? compression = response.Manifest.ResultCompression;
                    if (!string.IsNullOrEmpty(compression) && !compression.Equals("none", StringComparison.OrdinalIgnoreCase))
                    {
                        // Decompress each chunk's attachment
                        foreach (var chunk in response.Manifest.Chunks)
<<<<<<< HEAD
=======
=======
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
            // For INLINE disposition, data is in response.Result
            if (response.Result != null && response.Result.Attachment != null && response.Result.Attachment.Length > 0)
            {
                // Check if data is compressed (manifest contains compression metadata)
                byte[] attachmentData = response.Result.Attachment;

                // Decompress if necessary
                if (!string.IsNullOrEmpty(compression) && !compression.Equals("none", StringComparison.OrdinalIgnoreCase))
                {
                    if (compression.Equals("lz4", StringComparison.OrdinalIgnoreCase))
                    {
                        var decompressed = Lz4Utilities.DecompressLz4(attachmentData);
                        attachmentData = decompressed.ToArray();
                    }
                    else
                    {
                        throw new NotSupportedException($"Compression type '{compression}' is not supported for inline results");
                    }
                }

                // Convert ResultData to ResultManifest format for InlineReader
                var manifest = new ResultManifest
                {
                    Format = "arrow_stream",
                    Chunks = new List<ResultChunk>
                    {
                        new ResultChunk
                        {
                            ChunkIndex = (int)(response.Result.ChunkIndex ?? 0),
                            RowCount = response.Result.RowCount ?? 0,
                            RowOffset = response.Result.RowOffset ?? 0,
                            ByteCount = response.Result.ByteCount ?? 0,
                            Attachment = attachmentData  // Use decompressed data
                        }
                    }
                };

                return new InlineReader(manifest);
            }

            // These chunks should already be decompressed by the server or need similar handling
            {
                // Check if manifest chunks need decompression
                {
                    if (!string.IsNullOrEmpty(compression) && !compression.Equals("none", StringComparison.OrdinalIgnoreCase))
                    {
                        // Decompress each chunk's attachment
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
=======
=======
>>>>>>> e791f48 (feat(csharp): add JSON_ARRAY format support and complete GetObjects implementation for   Statement Execution API)
            // These chunks should already be decompressed by the server or need similar handling
            {
                // Check if manifest chunks need decompression
                {
                    if (!string.IsNullOrEmpty(compression) && !compression.Equals("none", StringComparison.OrdinalIgnoreCase))
                    {
                        // Decompress each chunk's attachment
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e791f48 (feat(csharp): add JSON_ARRAY format support and complete GetObjects implementation for   Statement Execution API)
=======
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
=======
>>>>>>> e791f48 (feat(csharp): add JSON_ARRAY format support and complete GetObjects implementation for   Statement Execution API)
=======
            // These chunks should already be decompressed by the server or need similar handling
            {
                // Check if manifest chunks need decompression
                {
                    if (!string.IsNullOrEmpty(compression) && !compression.Equals("none", StringComparison.OrdinalIgnoreCase))
                    {
                        // Decompress each chunk's attachment
>>>>>>> cf3a928 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
                        {
                            if (chunk.Attachment != null && chunk.Attachment.Length > 0)
                            {
                                if (compression.Equals("lz4", StringComparison.OrdinalIgnoreCase))
                                {
                                    var decompressed = Lz4Utilities.DecompressLz4(chunk.Attachment);
                                    chunk.Attachment = decompressed.ToArray();
                                }
                            }
                        }
                    }
                }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
                return new InlineReader(response.Manifest);
            }

<<<<<<< HEAD
<<<<<<< HEAD
            throw new InvalidOperationException("No inline data found in response.Result or response.Manifest");
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
            return new InlineReader(response.Manifest);
>>>>>>> cd94a4b (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
            }

>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
=======
=======
>>>>>>> 7c1e247 (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
            {
                throw new InvalidOperationException("Manifest is required for inline disposition");
            }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7c1e247 (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
            }

<<<<<<< HEAD
>>>>>>> e791f48 (feat(csharp): add JSON_ARRAY format support and complete GetObjects implementation for   Statement Execution API)
=======
                return new InlineReader(response.Manifest);
            }

            throw new InvalidOperationException("No inline data found in response.Result or response.Manifest");
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
            return new InlineReader(response.Manifest);
>>>>>>> cd94a4b (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
            }

>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
=======
>>>>>>> 7c1e247 (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
            }

>>>>>>> e791f48 (feat(csharp): add JSON_ARRAY format support and complete GetObjects implementation for   Statement Execution API)
=======
            }

>>>>>>> cf3a928 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
=======
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
        /// Creates a reader for JSON_ARRAY format results.
        /// </summary>
        /// <param name="response">The statement execution response.</param>
        /// <returns>A JSON array reader that converts JSON to Arrow format.</returns>
        private IArrowArrayStream CreateJsonArrayReader(GetStatementResponse response)
        {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
            if (response.Manifest == null)
=======
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
=======
            if (response.Manifest == null)
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
            {
                throw new InvalidOperationException("Manifest is required for JSON_ARRAY format");
            }

            // Extract data_array from the response
            List<List<string>> data;

            if (response.Result?.DataArray != null && response.Result.DataArray.Count > 0)
            {
                // Data is in result.data_array - convert List<List<object>> to List<List<string>>
                data = response.Result.DataArray
                    .Select(row => row.Select(cell => cell?.ToString() ?? string.Empty).ToList())
                    .ToList();
            }
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
            else if (response.Manifest.Chunks != null && response.Manifest.Chunks.Count > 0)
            {
                // Try to get data from manifest chunks
                data = new List<List<string>>();
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
                foreach (var chunk in response.Manifest.Chunks)
=======
            {
                // Try to get data from manifest chunks
                data = new List<List<string>>();
<<<<<<< HEAD
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
=======
            {
                // Try to get data from manifest chunks
                data = new List<List<string>>();
>>>>>>> e791f48 (feat(csharp): add JSON_ARRAY format support and complete GetObjects implementation for   Statement Execution API)
=======
                foreach (var chunk in response.Manifest.Chunks)
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
            {
                // Try to get data from manifest chunks
                data = new List<List<string>>();
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
=======
            {
                // Try to get data from manifest chunks
                data = new List<List<string>>();
>>>>>>> e791f48 (feat(csharp): add JSON_ARRAY format support and complete GetObjects implementation for   Statement Execution API)
=======
            {
                // Try to get data from manifest chunks
                data = new List<List<string>>();
>>>>>>> cf3a928 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
                {
                    if (chunk.DataArray != null)
                    {
                        // Convert List<List<object>> to List<List<string>>
                        var chunkData = chunk.DataArray
                            .Select(row => row.Select(cell => cell?.ToString() ?? string.Empty).ToList())
                            .ToList();
                        data.AddRange(chunkData);
                    }
                }
            }
            else
            {
                // Empty result
                data = new List<List<string>>();
            }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
            return new JsonArrayReader(response.Manifest, data);
        }

        /// <summary>
<<<<<<< HEAD
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
>>>>>>> cd94a4b (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
        }

        /// <summary>
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
=======
>>>>>>> 6c543ed (refactor(csharp): use separate HttpClient for CloudFetch downloads)
=======
>>>>>>> cd94a4b (feat(csharp): implement StatementExecutionStatement with hybrid disposition support)
=======
        }

        /// <summary>
>>>>>>> c7082c9 (feat(csharp): implement StatementExecutionStatement with CloudFetch support)
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
