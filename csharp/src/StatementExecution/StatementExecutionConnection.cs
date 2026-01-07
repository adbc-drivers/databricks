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
using AdbcDrivers.Databricks.Http;
using AdbcDrivers.Databricks.Result;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Adbc.Telemetry.Traces.Listeners;
using Apache.Arrow.Adbc.Telemetry.Traces.Listeners.FileListener;
using Apache.Arrow.Adbc.Tracing;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

namespace AdbcDrivers.Databricks.StatementExecution
{
    /// <summary>
    /// Connection implementation using the Databricks Statement Execution REST API.
    /// Manages session lifecycle and creates statements for query execution.
    /// Extends TracingConnection for consistent tracing support with Thrift protocol.
    /// </summary>
    internal class StatementExecutionConnection : TracingConnection
    {
        private readonly IStatementExecutionClient _client;
        private readonly string _warehouseId;
        private readonly string? _catalog;
        private readonly string? _schema;
        private readonly HttpClient _httpClient;
        private readonly HttpClient _cloudFetchHttpClient; // Separate HttpClient without auth headers for CloudFetch downloads
        private readonly IReadOnlyDictionary<string, string> _properties;
        private readonly bool _ownsHttpClient;

        // Session management
        private string? _sessionId;
        private readonly SemaphoreSlim _sessionLock = new SemaphoreSlim(1, 1);

        // Configuration for statement creation
        private readonly string _resultDisposition;
        private readonly string _resultFormat;
        private readonly string? _resultCompression;
        private readonly int _waitTimeoutSeconds;
        private readonly int _pollingIntervalMs;

        // Memory pooling (shared across connection)
        private readonly Microsoft.IO.RecyclableMemoryStreamManager _recyclableMemoryStreamManager;
        private readonly System.Buffers.ArrayPool<byte> _lz4BufferPool;

        // Tracing propagation configuration
        private readonly bool _tracePropagationEnabled;
        private readonly string _traceParentHeaderName;
        private readonly bool _traceStateEnabled;

        // Authentication support
        private HttpClient? _authHttpClient;
        private readonly string? _identityFederationClientId;

        // Trace exporter
        private readonly FileActivityListener? _fileActivityListener;
        private readonly string _traceInstanceId = Guid.NewGuid().ToString();

        /// <summary>
        /// Creates a new Statement Execution connection with internally managed HTTP client.
        /// The connection will create and manage its own HTTP client with proper tracing and retry handlers.
        /// </summary>
        /// <param name="properties">Connection properties.</param>
        /// <param name="memoryStreamManager">Optional shared memory stream manager.</param>
        /// <param name="lz4BufferPool">Optional shared LZ4 buffer pool.</param>
        public StatementExecutionConnection(
            IReadOnlyDictionary<string, string> properties,
            Microsoft.IO.RecyclableMemoryStreamManager? memoryStreamManager = null,
            System.Buffers.ArrayPool<byte>? lz4BufferPool = null)
            : this(properties, httpClient: null, memoryStreamManager, lz4BufferPool, ownsHttpClient: true)
        {
        }

        /// <summary>
        /// Creates a new Statement Execution connection with externally provided HTTP client.
        /// Used for testing or advanced scenarios where caller manages the HTTP client.
        /// </summary>
        /// <param name="properties">Connection properties.</param>
        /// <param name="httpClient">Externally managed HTTP client.</param>
        /// <param name="memoryStreamManager">Optional shared memory stream manager.</param>
        /// <param name="lz4BufferPool">Optional shared LZ4 buffer pool.</param>
        public StatementExecutionConnection(
            IReadOnlyDictionary<string, string> properties,
            HttpClient httpClient,
            Microsoft.IO.RecyclableMemoryStreamManager? memoryStreamManager = null,
            System.Buffers.ArrayPool<byte>? lz4BufferPool = null)
            : this(properties, httpClient, memoryStreamManager, lz4BufferPool, ownsHttpClient: false)
        {
        }

        private StatementExecutionConnection(
            IReadOnlyDictionary<string, string> properties,
            HttpClient? httpClient,
            Microsoft.IO.RecyclableMemoryStreamManager? memoryStreamManager,
            System.Buffers.ArrayPool<byte>? lz4BufferPool,
            bool ownsHttpClient)
            : base(properties) // Initialize TracingConnection base class
        {
            _properties = properties ?? throw new ArgumentNullException(nameof(properties));
            _ownsHttpClient = ownsHttpClient;

            // Parse configuration - check for URI first (same as Thrift protocol)
            properties.TryGetValue(AdbcOptions.Uri, out var uri);
            properties.TryGetValue(SparkParameters.HostName, out var hostName);
            properties.TryGetValue(SparkParameters.Path, out var path);

            Uri? parsedUri = null;
            if (!string.IsNullOrEmpty(uri) && Uri.TryCreate(uri, UriKind.Absolute, out parsedUri))
            {
                // Extract host and path from URI if not provided separately
                if (string.IsNullOrEmpty(hostName))
                {
                    hostName = parsedUri.Host;
                }
                if (string.IsNullOrEmpty(path))
                {
                    path = parsedUri.AbsolutePath;
                }
            }

            // Try to get warehouse ID from explicit parameter first
            string? warehouseId = PropertyHelper.GetStringProperty(properties, DatabricksParameters.WarehouseId, string.Empty);
            // If not provided explicitly, try to extract from path
            // Path format: /sql/1.0/warehouses/{warehouse_id} or /sql/1.0/endpoints/{warehouse_id}
            if (string.IsNullOrEmpty(warehouseId) && !string.IsNullOrEmpty(path))
            {
                // Validate path pattern using regex
                // Match: /sql/1.0/warehouses/{id} or /sql/1.0/endpoints/{id}
                // Reject: /sql/protocolv1/o/{orgId}/{clusterId} (general cluster)
                var warehousePathPattern = new System.Text.RegularExpressions.Regex(@"^/sql/1\.0/(warehouses|endpoints)/([^/]+)/?$");
                var match = warehousePathPattern.Match(path);

                if (match.Success)
                {
                    warehouseId = match.Groups[2].Value;
                }
                else
                {
                    // Check if it's a general cluster path (should be rejected)
                    var clusterPathPattern = new System.Text.RegularExpressions.Regex(@"^/sql/protocolv1/o/\d+/[^/]+/?$");
                    if (clusterPathPattern.IsMatch(path))
                    {
                        throw new ArgumentException(
                            "Statement Execution API requires a SQL Warehouse, not a general cluster. " +
                            $"The provided path '{path}' appears to be a general cluster endpoint. " +
                            "Please use a SQL Warehouse path like '/sql/1.0/warehouses/{{warehouse_id}}' or '/sql/1.0/endpoints/{{warehouse_id}}'.",
                            nameof(properties));
                    }
                }
            }

            if (string.IsNullOrEmpty(warehouseId))
            {
                throw new ArgumentException(
                    "Warehouse ID is required for Statement Execution API. " +
                    "Please provide it via 'adbc.databricks.warehouse_id' parameter, include it in the 'path' parameter (e.g., '/sql/1.0/warehouses/your-warehouse-id'), " +
                    "or provide a full URI with the warehouse path.",
                    nameof(properties));
            }
            _warehouseId = warehouseId;

            // Get host URL
            if (string.IsNullOrEmpty(hostName))
            {
                throw new ArgumentException(
                    "Host name is required. Please provide it via 'hostName' parameter or via 'uri' parameter.",
                    nameof(properties));
            }
            string baseUrl = $"https://{hostName}";

            // Session configuration
            properties.TryGetValue(AdbcOptions.Connection.CurrentCatalog, out _catalog);
            properties.TryGetValue(AdbcOptions.Connection.CurrentDbSchema, out _schema);

            // Result configuration
            _resultDisposition = PropertyHelper.GetStringProperty(properties, DatabricksParameters.ResultDisposition, "INLINE_OR_EXTERNAL_LINKS");
            _resultFormat = PropertyHelper.GetStringProperty(properties, DatabricksParameters.ResultFormat, "ARROW_STREAM");
            properties.TryGetValue(DatabricksParameters.ResultCompression, out _resultCompression);

            _waitTimeoutSeconds = PropertyHelper.GetIntPropertyWithValidation(properties, DatabricksParameters.WaitTimeout, 10);
            _pollingIntervalMs = PropertyHelper.GetPositiveIntPropertyWithValidation(properties, DatabricksParameters.PollingInterval, 1000);

            // Memory pooling
            _recyclableMemoryStreamManager = memoryStreamManager ?? new Microsoft.IO.RecyclableMemoryStreamManager();
            _lz4BufferPool = lz4BufferPool ?? System.Buffers.ArrayPool<byte>.Create(maxArrayLength: 4 * 1024 * 1024, maxArraysPerBucket: 10);

            // Tracing propagation configuration
            // Base class (TracingConnection) already handles ActivityTrace initialization
            _tracePropagationEnabled = PropertyHelper.GetBooleanPropertyWithValidation(properties, DatabricksParameters.TracePropagationEnabled, true);
            _traceParentHeaderName = PropertyHelper.GetStringProperty(properties, DatabricksParameters.TraceParentHeaderName, "traceparent");
            _traceStateEnabled = PropertyHelper.GetBooleanPropertyWithValidation(properties, DatabricksParameters.TraceStateEnabled, false);

            // Initialize trace exporter
            TryInitTracerProvider(out _fileActivityListener);

            // Authentication configuration
            if (properties.TryGetValue(DatabricksParameters.IdentityFederationClientId, out string? identityFederationClientId))
            {
                _identityFederationClientId = identityFederationClientId;
            }

            // Create or use provided HTTP client
            if (httpClient != null)
            {
                _httpClient = httpClient;
            }
            else
            {
                _httpClient = CreateHttpClient(properties);
            }

            // Create a separate HTTP client for CloudFetch downloads (without auth headers)
            // This is needed because CloudFetch uses pre-signed URLs from cloud storage (S3, Azure Blob, etc.)
            // and those services reject requests with multiple authentication methods
            int timeoutMinutes = PropertyHelper.GetPositiveIntPropertyWithValidation(properties, DatabricksParameters.CloudFetchTimeoutMinutes, DatabricksConstants.DefaultCloudFetchTimeoutMinutes);
            _cloudFetchHttpClient = new HttpClient()
            {
                Timeout = TimeSpan.FromMinutes(timeoutMinutes)
            };

            // Create REST API client
            _client = new StatementExecutionClient(_httpClient, baseUrl);
        }

        /// <summary>
        /// Creates an HTTP client with proper handler chain for the Statement Execution API.
        /// Handler chain order (outermost to innermost):
        /// 1. OAuthDelegatingHandler (if OAuth M2M) OR TokenRefreshDelegatingHandler (if token refresh) - token management
        /// 2. MandatoryTokenExchangeDelegatingHandler (if OAuth) - workload identity federation
        /// 3. RetryHttpHandler - retries 408, 429, 502, 503, 504 with Retry-After support
        /// 4. TracingDelegatingHandler - propagates W3C trace context (closest to network)
        /// 5. HttpClientHandler - actual network communication
        /// </summary>
        private HttpClient CreateHttpClient(IReadOnlyDictionary<string, string> properties)
        {
            // Retry configuration
            bool temporarilyUnavailableRetry = PropertyHelper.GetBooleanPropertyWithValidation(properties, DatabricksParameters.TemporarilyUnavailableRetry, true);
            bool rateLimitRetry = PropertyHelper.GetBooleanPropertyWithValidation(properties, DatabricksParameters.RateLimitRetry, true);
            int temporarilyUnavailableRetryTimeout = PropertyHelper.GetIntPropertyWithValidation(properties, DatabricksParameters.TemporarilyUnavailableRetryTimeout, DatabricksConstants.DefaultTemporarilyUnavailableRetryTimeout);
            int rateLimitRetryTimeout = PropertyHelper.GetIntPropertyWithValidation(properties, DatabricksParameters.RateLimitRetryTimeout, DatabricksConstants.DefaultRateLimitRetryTimeout);
            int timeoutMinutes = PropertyHelper.GetPositiveIntPropertyWithValidation(properties, DatabricksParameters.CloudFetchTimeoutMinutes, DatabricksConstants.DefaultCloudFetchTimeoutMinutes);

            var config = new HttpHandlerFactory.HandlerConfig
            {
                BaseHandler = new HttpClientHandler(),
                BaseAuthHandler = new HttpClientHandler(),
                Properties = properties,
                Host = GetHost(properties),
                ActivityTracer = this,
                TracePropagationEnabled = _tracePropagationEnabled,
                TraceParentHeaderName = _traceParentHeaderName,
                TraceStateEnabled = _traceStateEnabled,
                IdentityFederationClientId = _identityFederationClientId,
                TemporarilyUnavailableRetry = temporarilyUnavailableRetry,
                TemporarilyUnavailableRetryTimeout = temporarilyUnavailableRetryTimeout,
                RateLimitRetry = rateLimitRetry,
                RateLimitRetryTimeout = rateLimitRetryTimeout,
                TimeoutMinutes = timeoutMinutes,
                AddThriftErrorHandler = false
            };

            var result = HttpHandlerFactory.CreateHandlers(config);

            if (result.AuthHttpClient != null)
            {
                _authHttpClient = result.AuthHttpClient;
            }

            var httpClient = new HttpClient(result.Handler)
            {
                Timeout = TimeSpan.FromMinutes(timeoutMinutes)
            };

            // Set user agent
            string userAgent = GetUserAgent(properties);
            httpClient.DefaultRequestHeaders.UserAgent.ParseAdd(userAgent);

            return httpClient;
        }

        /// <summary>
        /// Initializes the trace provider/exporter if configured.
        /// </summary>
        /// <param name="fileActivityListener">The created file activity listener, if any.</param>
        /// <returns>True if a trace provider was successfully initialized, false otherwise.</returns>
        private bool TryInitTracerProvider(out FileActivityListener? fileActivityListener)
        {
            _properties.TryGetValue(ListenersOptions.Exporter, out string? exporterOption);
            // This listener will only listen for activity from this specific connection instance.
            bool shouldListenTo(ActivitySource source) => source.Tags?.Any(t => ReferenceEquals(t.Key, _traceInstanceId)) == true;
            return FileActivityListener.TryActivateFileListener(AssemblyName, exporterOption, out fileActivityListener, shouldListenTo: shouldListenTo);
        }

        /// <summary>
        /// Gets tags for the activity source to enable per-instance trace filtering.
        /// </summary>
        public override IEnumerable<KeyValuePair<string, object?>>? GetActivitySourceTags(IReadOnlyDictionary<string, string> properties)
        {
            IEnumerable<KeyValuePair<string, object?>>? tags = base.GetActivitySourceTags(properties);
            tags ??= [];
            tags = tags.Concat([new(_traceInstanceId, null)]);
            return tags;
        }

        /// <summary>
        /// Gets the host from the connection properties.
        /// </summary>
        /// <param name="properties">Connection properties.</param>
        /// <returns>The host URL.</returns>
        private static string GetHost(IReadOnlyDictionary<string, string> properties)
        {
            if (properties.TryGetValue(SparkParameters.HostName, out string? host) && !string.IsNullOrEmpty(host))
            {
                return host;
            }

            if (properties.TryGetValue(AdbcOptions.Uri, out string? uri) && !string.IsNullOrEmpty(uri))
            {
                // Parse the URI to extract the host
                if (Uri.TryCreate(uri, UriKind.Absolute, out Uri? parsedUri))
                {
                    return parsedUri.Host;
                }
            }

            throw new ArgumentException("Host not found in connection properties. Please provide a valid host using either 'hostName' or 'uri' property.");
        }

        /// <summary>
        /// Builds the user agent string for HTTP requests.
        /// Format: DatabricksJDBCDriverOSS/{version} (ADBC)
        /// Uses DatabricksJDBCDriverOSS prefix for server-side feature compatibility.
        /// </summary>
        private string GetUserAgent(IReadOnlyDictionary<string, string> properties)
        {
            // Use DatabricksJDBCDriverOSS prefix for server-side feature compatibility
            // (e.g., INLINE_OR_EXTERNAL_LINKS disposition support)
            string baseUserAgent = $"DatabricksJDBCDriverOSS/{AssemblyVersion} (ADBC)";

            // Check if a client has provided a user-agent entry
            string userAgentEntry = PropertyHelper.GetStringProperty(properties, "adbc.spark.user_agent_entry", string.Empty);
            if (!string.IsNullOrWhiteSpace(userAgentEntry))
            {
                return $"{baseUserAgent} {userAgentEntry}";
            }

            return baseUserAgent;
        }

        /// <summary>
        /// Opens the connection and creates a session.
        /// Session management is always enabled for REST API connections.
        /// </summary>
        public async Task OpenAsync(CancellationToken cancellationToken = default)
        {
            if (_sessionId == null)
            {
                await _sessionLock.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    // Double-check after acquiring lock
                    if (_sessionId == null)
                    {
                        var request = new CreateSessionRequest
                        {
                            WarehouseId = _warehouseId,
                            Catalog = _catalog,
                            Schema = _schema
                        };

                        var response = await _client.CreateSessionAsync(request, cancellationToken).ConfigureAwait(false);
                        _sessionId = response.SessionId;
                    }
                }
                finally
                {
                    _sessionLock.Release();
                }
            }
        }

        /// <summary>
        /// Creates a new statement for query execution.
        /// </summary>
        public override AdbcStatement CreateStatement()
        {
            return new StatementExecutionStatement(
                _client,
                _sessionId,
                _warehouseId,
                _catalog,
                _schema,
                _resultDisposition,
                _resultFormat,
                _resultCompression,
                _waitTimeoutSeconds,
                _pollingIntervalMs,
                _properties,
                _recyclableMemoryStreamManager,
                _lz4BufferPool,
                _cloudFetchHttpClient,
                this); // Pass connection as TracingConnection for tracing support
        }

        /// <summary>
        /// Gets objects (metadata) from the database using SQL-based commands.
        /// Uses SHOW CATALOGS, SHOW SCHEMAS, SHOW TABLES, SHOW COLUMNS SQL commands for metadata retrieval.
        /// </summary>
        /// <param name="depth">The depth of metadata to retrieve (Catalogs, DbSchemas, Tables, or All)</param>
        /// <param name="catalogPattern">Pattern to filter catalog names Null means all catalogs.</param>
        /// <param name="schemaPattern">Pattern to filter schema names (supports SQL LIKE wildcards: %, _). Null means all schemas.</param>
        /// <param name="tableNamePattern">Pattern to filter table names (supports SQL LIKE wildcards: %, _). Null means all tables.</param>
        /// <param name="tableTypes">List of table types to include (e.g., "TABLE", "VIEW", "SYSTEM TABLE"). Null means all types.</param>
        /// <param name="columnNamePattern">Pattern to filter column names (supports SQL LIKE wildcards: %, _). Null means all columns. Only used with All depth.</param>
        /// <returns>Arrow stream containing metadata records with schema dependent on depth parameter</returns>
        /// <remarks>
        /// <para>Returned schema depends on depth:</para>
        /// <list type="bullet">
        /// <item><description>Catalogs: Returns (catalog_name)</description></item>
        /// <item><description>DbSchemas: Returns (catalog_name, db_schema_name)</description></item>
        /// <item><description>Tables: Returns (catalog_name, db_schema_name, table_name, table_type)</description></item>
        /// <item><description>All: Returns simplified flat structure</description></item>
        /// </list>
        /// <para>Pattern matching uses SQL LIKE syntax:</para>
        /// <list type="bullet">
        /// <item><description>% - matches any sequence of characters</description></item>
        /// <item><description>_ - matches any single character</description></item>
        /// </list>
        /// <para>Example: "main%" matches all catalogs starting with "main"</para>
        /// </remarks>
        public override IArrowArrayStream GetObjects(GetObjectsDepth depth, string? catalogPattern, string? schemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
        {
            return GetObjectsAsync(depth, catalogPattern, schemaPattern, tableNamePattern, tableTypes, columnNamePattern).GetAwaiter().GetResult();
        }

        private async Task<IArrowArrayStream> GetObjectsAsync(GetObjectsDepth depth, string? catalogPattern, string? schemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.SetTag("depth", depth.ToString());
                activity?.SetTag("catalog_pattern", catalogPattern ?? "(none)");
                activity?.SetTag("schema_pattern", schemaPattern ?? "(none)");
                activity?.SetTag("table_pattern", tableNamePattern ?? "(none)");
                activity?.SetTag("column_pattern", columnNamePattern ?? "(none)");
                activity?.SetTag("table_types", tableTypes != null ? string.Join(",", tableTypes) : "(none)");

                // Unified structure: catalog → schema → table → (tableType, columns)
                var catalogMap = new Dictionary<string, Dictionary<string, Dictionary<string, (string tableType, List<ColumnInfo> columns)>>>();

                // Fetch catalogs
                if (depth == GetObjectsDepth.All || depth >= GetObjectsDepth.Catalogs)
                {
                    // GetCatalogsAsync will return empty list if catalog doesn't exist
                    var catalogs = await GetCatalogsAsync(catalogPattern).ConfigureAwait(false);
                    activity?.SetTag("fetched_catalogs", catalogs.Count);

                    // Handle servers that don't support 'catalog' in the namespace
                    if (catalogs.Count == 0 && string.IsNullOrEmpty(catalogPattern))
                    {
                        catalogs.Add(string.Empty);
                    }

                    // Initialize catalogMap with fetched catalogs
                    foreach (var catalog in catalogs)
                    {
                        catalogMap.Add(catalog, new Dictionary<string, Dictionary<string, (string, List<ColumnInfo>)>>());
                    }

                    // Fetch schemas (needed for DbSchemas and deeper depths)
                    if (depth == GetObjectsDepth.All || depth >= GetObjectsDepth.DbSchemas)
                    {
                        // Get schemas - the underlying GetSchemasAsync handles null catalog with "IN ALL CATALOGS"
                        var allSchemas = await GetSchemasAsync(catalogPattern, schemaPattern).ConfigureAwait(false);
                        activity?.SetTag("fetched_schemas", allSchemas.Count);

                        // Add schemas to catalogMap using null-safe access
                        // This handles Spark edge cases where catalog might be empty string for temporary tables
                        foreach (var (catalog, schema) in allSchemas)
                        {
                            if (catalogMap.TryGetValue(catalog, out var schemaDict))
                            {
                                if (!schemaDict.ContainsKey(schema))
                                {
                                    schemaDict.Add(schema, new Dictionary<string, (string, List<ColumnInfo>)>());
                                }
                            }
                        }
                    }

                    // Fetch tables (needed for Tables and All depths)
                    if (depth == GetObjectsDepth.All || depth >= GetObjectsDepth.Tables)
                    {
                        // Get tables - server handles pattern matching for schema and table names
                        var allTables = await GetTablesAsync(catalogPattern, schemaPattern, tableNamePattern, tableTypes).ConfigureAwait(false);
                        activity?.SetTag("fetched_tables", allTables.Count);

                        // Add tables to catalogMap using null-safe 2-level access
                        foreach (var (catalog, schema, tableName, tableType, _) in allTables)
                        {
                            if (catalogMap.TryGetValue(catalog, out var schemaDict) &&
                                schemaDict.TryGetValue(schema, out var tableDict))
                            {
                                if (!tableDict.ContainsKey(tableName))
                                {
                                    tableDict.Add(tableName, (tableType, new List<ColumnInfo>()));
                                }
                            }
                        }

                        // Fetch columns (only for All depth)
                        if (depth == GetObjectsDepth.All)
                        {
                            // Get columns - server handles all pattern matching
                            var allColumns = await GetColumnsAsync(catalogPattern, schemaPattern, tableNamePattern, columnNamePattern).ConfigureAwait(false);
                            activity?.SetTag("fetched_columns", allColumns.Count);

                            // Add columns to catalogMap using null-safe 3-level access
                            foreach (var column in allColumns)
                            {
                                if (catalogMap.TryGetValue(column.Catalog, out var schemaDict) &&
                                    schemaDict.TryGetValue(column.Schema, out var tableDict) &&
                                    tableDict.TryGetValue(column.Table, out var tableEntry))
                                {
                                    tableEntry.columns.Add(column);
                                }
                            }
                        }
                    }
                }

                // Add result metrics
                activity?.SetTag("result_catalogs", catalogMap.Count);
                activity?.SetTag("result_schemas", catalogMap.Values.Sum(s => s.Count));
                activity?.SetTag("result_tables", catalogMap.Values.SelectMany(s => s.Values).Sum(t => t.Count));

                // Build and return result from unified catalogMap
                return BuildGetObjectsResult(depth, catalogMap);
            });
        }

        /// <summary>
        /// Gets list of catalogs matching the optional pattern.
        /// Thin wrapper around GetCatalogsMetadataAsync for GetObjects usage.
        /// </summary>
        private async Task<List<string>> GetCatalogsAsync(string? catalogPattern)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.AddEvent("metadata.get_catalogs.start");
                activity?.SetTag("catalog_pattern", catalogPattern ?? "null");

                // Call core implementation
                var stream = await GetCatalogsMetadataAsync(catalogPattern).ConfigureAwait(false);

                // Convert Arrow stream to List<string>
                var catalogs = new List<string>();
                using (var reader = stream)
                {
                    RecordBatch? batch;
                    while ((batch = await reader.ReadNextRecordBatchAsync().ConfigureAwait(false)) != null)
                    {
                        var catalogArray = batch.Column("catalog_name") as StringArray;
                        if (catalogArray != null)
                        {
                            for (int i = 0; i < catalogArray.Length; i++)
                            {
                                if (!catalogArray.IsNull(i))
                                    catalogs.Add(catalogArray.GetString(i));
                            }
                        }
                    }
                }

                activity?.SetTag("result_count", catalogs.Count);
                activity?.SetTag(SemanticConventions.Db.Response.ReturnedRows, catalogs.Count);
                activity?.AddEvent("metadata.get_catalogs.complete");
                return catalogs;
            });
        }

        /// <summary>
        /// Gets list of schemas matching the optional pattern.
        /// Thin wrapper around GetSchemasMetadataAsync for GetObjects usage.
        /// </summary>
        private async Task<List<(string catalog, string schema)>> GetSchemasAsync(string? catalog, string? schemaPattern)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.SetTag("catalog", catalog ?? "(none)");
                activity?.SetTag("schema_pattern", schemaPattern ?? "(none)");

                // Call core implementation
                var stream = await GetSchemasMetadataAsync(catalog, schemaPattern).ConfigureAwait(false);

                // Convert Arrow stream to List<(string, string)>
                var schemas = new List<(string catalog, string schema)>();
                using (var reader = stream)
                {
                    RecordBatch? batch;
                    while ((batch = await reader.ReadNextRecordBatchAsync().ConfigureAwait(false)) != null)
                    {
                        var catalogArray = batch.Column("catalog_name") as StringArray;
                        var schemaArray = batch.Column("db_schema_name") as StringArray;

                        if (schemaArray != null)
                        {
                            for (int i = 0; i < schemaArray.Length; i++)
                            {
                                if (!schemaArray.IsNull(i))
                                {
                                    var catalogName = (catalogArray != null && !catalogArray.IsNull(i))
                                        ? catalogArray.GetString(i)
                                        : catalog ?? string.Empty;
                                    var schemaName = schemaArray.GetString(i);
                                    schemas.Add((catalogName, schemaName));
                                }
                            }
                        }
                    }
                }

                activity?.SetTag("result_count", schemas.Count);
                return schemas;
            });
        }

        /// <summary>
        /// Gets list of tables matching the optional patterns.
        /// Thin wrapper around GetTablesMetadataAsync for GetObjects usage.
        /// </summary>
        private async Task<List<(string catalog, string schema, string tableName, string tableType, string remarks)>> GetTablesAsync(
            string? catalog, string? schemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.SetTag("catalog", catalog ?? "(none)");
                activity?.SetTag("schema_pattern", schemaPattern ?? "(none)");
                activity?.SetTag("table_pattern", tableNamePattern ?? "(none)");
                activity?.SetTag("table_types", tableTypes != null ? string.Join(",", tableTypes) : "(none)");

                // Call core implementation
                var stream = await GetTablesMetadataAsync(catalog, schemaPattern, tableNamePattern, tableTypes).ConfigureAwait(false);

                // Convert ADBC stream to List for GetObjects usage
                var tables = new List<(string catalog, string schema, string tableName, string tableType, string remarks)>();
                using (var reader = stream)
                {
                    RecordBatch? batch;
                    while ((batch = await reader.ReadNextRecordBatchAsync().ConfigureAwait(false)) != null)
                    {
                        var catalogArray = batch.Column("catalog_name") as StringArray;
                        var schemaArray = batch.Column("db_schema_name") as StringArray;
                        var tableNameArray = batch.Column("table_name") as StringArray;
                        var tableTypeArray = batch.Column("table_type") as StringArray;
                        var remarksArray = batch.Column("table_remarks") as StringArray;

                        if (catalogArray != null && schemaArray != null && tableNameArray != null && tableTypeArray != null)
                        {
                            for (int i = 0; i < batch.Length; i++)
                            {
                                if (!catalogArray.IsNull(i) && !schemaArray.IsNull(i) &&
                                    !tableNameArray.IsNull(i) && !tableTypeArray.IsNull(i))
                                {
                                    var catalogName = catalogArray.GetString(i);
                                    var schemaName = schemaArray.GetString(i);
                                    var tableName = tableNameArray.GetString(i);
                                    var tableType = tableTypeArray.GetString(i);
                                    var remarks = (remarksArray != null && !remarksArray.IsNull(i))
                                        ? remarksArray.GetString(i)
                                        : "";
                                    tables.Add((catalogName, schemaName, tableName, tableType, remarks));
                                }
                            }
                        }
                    }
                }

                activity?.SetTag("result_count", tables.Count);
                activity?.SetTag(SemanticConventions.Db.Response.ReturnedRows, tables.Count);
                return tables;
            });
        }

        /// <summary>
        /// Gets list of columns matching the optional patterns using SHOW COLUMNS command.
        /// Thin wrapper around GetColumnsMetadataAsync for GetObjects usage.
        /// </summary>
        private async Task<List<ColumnInfo>> GetColumnsAsync(string? catalog, string? schemaPattern, string? tablePattern, string? columnNamePattern)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.SetTag("catalog", catalog ?? "(none)");
                activity?.SetTag("schema_pattern", schemaPattern ?? "(none)");
                activity?.SetTag("table_pattern", tablePattern ?? "(none)");
                activity?.SetTag("column_pattern", columnNamePattern ?? "(none)");

                // Call core implementation
                var stream = await GetColumnsMetadataAsync(catalog, schemaPattern, tablePattern, columnNamePattern).ConfigureAwait(false);

                // Convert ADBC stream to List<ColumnInfo> for GetObjects usage
                var columns = new List<ColumnInfo>();
                using (var reader = stream)
                {
                    RecordBatch? batch;
                    while ((batch = await reader.ReadNextRecordBatchAsync().ConfigureAwait(false)) != null)
                    {
                        // Extract columns from ADBC stream (17 columns)
                        var catalogArray = batch.Column("catalog_name") as StringArray;
                        var schemaArray = batch.Column("db_schema_name") as StringArray;
                        var tableArray = batch.Column("table_name") as StringArray;
                        var columnNameArray = batch.Column("column_name") as StringArray;
                        var typeNameArray = batch.Column("type_name") as StringArray;
                        var positionArray = batch.Column("ordinal_position") as Int32Array;
                        var nullableArray = batch.Column("is_nullable") as BooleanArray;
                        var remarksArray = batch.Column("remarks") as StringArray;
                        var xdbcDataTypeArray = batch.Column("xdbc_data_type") as Int16Array;
                        var xdbcTypeNameArray = batch.Column("xdbc_type_name") as StringArray;
                        var xdbcColumnSizeArray = batch.Column("xdbc_column_size") as Int32Array;
                        var xdbcDecimalDigitsArray = batch.Column("xdbc_decimal_digits") as Int32Array;
                        var xdbcNumPrecRadixArray = batch.Column("xdbc_num_prec_radix") as Int16Array;
                        var xdbcBufferLengthArray = batch.Column("xdbc_buffer_length") as Int32Array;
                        var xdbcCharOctetLengthArray = batch.Column("xdbc_char_octet_length") as Int32Array;
                        var xdbcSqlDataTypeArray = batch.Column("xdbc_sql_data_type") as Int16Array;
                        var xdbcDatetimeSubArray = batch.Column("xdbc_datetime_sub") as Int16Array;

                        if (catalogArray != null && schemaArray != null && tableArray != null &&
                            columnNameArray != null && typeNameArray != null && positionArray != null && nullableArray != null)
                        {
                            for (int i = 0; i < batch.Length; i++)
                            {
                                if (!catalogArray.IsNull(i) && !schemaArray.IsNull(i) && !tableArray.IsNull(i) &&
                                    !columnNameArray.IsNull(i) && !typeNameArray.IsNull(i) &&
                                    !positionArray.IsNull(i) && !nullableArray.IsNull(i))
                                {
                                    columns.Add(new ColumnInfo
                                    {
                                        Catalog = catalogArray.GetString(i),
                                        Schema = schemaArray.GetString(i),
                                        Table = tableArray.GetString(i),
                                        Name = columnNameArray.GetString(i),
                                        TypeName = typeNameArray.GetString(i),
                                        Position = positionArray.GetValue(i)!.Value,
                                        Nullable = nullableArray.GetValue(i)!.Value,
                                        Comment = remarksArray != null && !remarksArray.IsNull(i) ? remarksArray.GetString(i) : null,
                                        XdbcDataType = xdbcDataTypeArray != null && !xdbcDataTypeArray.IsNull(i) ? xdbcDataTypeArray.GetValue(i) : null,
                                        XdbcTypeName = xdbcTypeNameArray != null && !xdbcTypeNameArray.IsNull(i) ? xdbcTypeNameArray.GetString(i) : null,
                                        XdbcColumnSize = xdbcColumnSizeArray != null && !xdbcColumnSizeArray.IsNull(i) ? xdbcColumnSizeArray.GetValue(i) : null,
                                        XdbcDecimalDigits = xdbcDecimalDigitsArray != null && !xdbcDecimalDigitsArray.IsNull(i) ? xdbcDecimalDigitsArray.GetValue(i) : null,
                                        XdbcNumPrecRadix = xdbcNumPrecRadixArray != null && !xdbcNumPrecRadixArray.IsNull(i) ? xdbcNumPrecRadixArray.GetValue(i) : null,
                                        XdbcBufferLength = xdbcBufferLengthArray != null && !xdbcBufferLengthArray.IsNull(i) ? xdbcBufferLengthArray.GetValue(i) : null,
                                        XdbcCharOctetLength = xdbcCharOctetLengthArray != null && !xdbcCharOctetLengthArray.IsNull(i) ? xdbcCharOctetLengthArray.GetValue(i) : null,
                                        XdbcSqlDataType = xdbcSqlDataTypeArray != null && !xdbcSqlDataTypeArray.IsNull(i) ? xdbcSqlDataTypeArray.GetValue(i) : null,
                                        XdbcDatetimeSub = xdbcDatetimeSubArray != null && !xdbcDatetimeSubArray.IsNull(i) ? xdbcDatetimeSubArray.GetValue(i) : null
                                    });
                                }
                            }
                        }
                    }
                }

                activity?.SetTag("result_count", columns.Count);
                activity?.SetTag(SemanticConventions.Db.Response.ReturnedRows, columns.Count);
                return columns;
            });
        }

        /// <summary>
        /// Gets columns matching the optional patterns using SHOW COLUMNS command.
        /// Core implementation returning ADBC-formatted IArrowArrayStream with complete column metadata.
        /// </summary>
        /// <param name="catalog">Catalog name. If null, searches all catalogs.</param>
        /// <param name="schemaPattern">Schema name pattern (SQL LIKE syntax). If null, searches all schemas.</param>
        /// <param name="tablePattern">Table name pattern (SQL LIKE syntax). If null, returns all tables.</param>
        /// <param name="columnNamePattern">Column name pattern (SQL LIKE syntax). If null, returns all columns.</param>
        /// <returns>Arrow stream with ADBC-formatted column metadata (17 columns with lowercase names)</returns>
        /// <remarks>
        /// Returns ADBC schema with lowercase field names. Both GetColumnsAsync and GetColumnsFlat consume this.
        /// Schema: catalog_name, db_schema_name, table_name, column_name, type_name, ordinal_position, is_nullable,
        /// remarks, xdbc_data_type, xdbc_type_name, xdbc_column_size, xdbc_decimal_digits, xdbc_num_prec_radix,
        /// xdbc_buffer_length, xdbc_char_octet_length, xdbc_sql_data_type, xdbc_datetime_sub
        /// </remarks>
        private async Task<IArrowArrayStream> GetColumnsMetadataAsync(string? catalog, string? schemaPattern, string? tablePattern, string? columnNamePattern)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.SetTag("catalog", catalog ?? "(none)");
                activity?.SetTag("schema_pattern", schemaPattern ?? "(none)");
                activity?.SetTag("table_pattern", tablePattern ?? "(none)");
                activity?.SetTag("column_pattern", columnNamePattern ?? "(none)");

                var commandBuilder = new SqlCommandBuilder()
                    .WithCatalog(catalog);

                if (schemaPattern != null)
                {
                    commandBuilder.WithSchemaPattern(schemaPattern);
                }
                if (tablePattern != null)
                {
                    commandBuilder.WithTablePattern(tablePattern);
                }

                commandBuilder.WithColumnPattern(columnNamePattern);

                string sql = commandBuilder.BuildShowColumns(catalog);
                activity?.SetTag("sql_query", sql);

                List<RecordBatch> batches;
                try
                {
                    batches = await ExecuteSqlQueryAsync(sql).ConfigureAwait(false);
                    activity?.SetTag("result_batches", batches.Count);
                    activity?.SetTag("total_rows", batches.Sum(b => b.Length));
                }
                catch (Exception ex)
                {
                    activity?.AddException(ex, [
                        new("error.type", ex.GetType().Name),
                        new("operation", "GetColumnsMetadata"),
                        new("catalog", catalog ?? "(none)"),
                        new("schema_pattern", schemaPattern ?? "(none)"),
                        new("table_pattern", tablePattern ?? "(none)"),
                        new("column_pattern", columnNamePattern ?? "(none)")
                    ]);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    // Return empty result on permission denied or other errors
                    return BuildEmptyColumnsMetadataStream();
                }

                // Build ADBC-formatted result
                return BuildColumnsMetadataStream(batches, activity);
            });
        }

        /// <summary>
        /// Builds an ADBC-formatted Arrow stream from SHOW COLUMNS batches.
        /// </summary>
        private IArrowArrayStream BuildColumnsMetadataStream(List<RecordBatch> batches, System.Diagnostics.Activity? activity)
        {
            var catalogBuilder = new StringArray.Builder();
            var schemaBuilder = new StringArray.Builder();
            var tableBuilder = new StringArray.Builder();
            var columnNameBuilder = new StringArray.Builder();
            var typeNameBuilder = new StringArray.Builder();
            var positionBuilder = new Int32Array.Builder();
            var nullableBuilder = new BooleanArray.Builder();
            var commentBuilder = new StringArray.Builder();
            var xdbcDataTypeBuilder = new Int16Array.Builder();
            var xdbcTypeNameBuilder = new StringArray.Builder();
            var xdbcColumnSizeBuilder = new Int32Array.Builder();
            var xdbcDecimalDigitsBuilder = new Int32Array.Builder();
            var xdbcNumPrecRadixBuilder = new Int16Array.Builder();
            var xdbcBufferLengthBuilder = new Int32Array.Builder();
            var xdbcCharOctetLengthBuilder = new Int32Array.Builder();
            var xdbcSqlDataTypeBuilder = new Int16Array.Builder();
            var xdbcDatetimeSubBuilder = new Int16Array.Builder();

            var tablePositions = new Dictionary<string, int>();

            foreach (var batch in batches)
            {
                var catalogArray = batch.Column("catalogName") as StringArray;
                var schemaArray = batch.Column("namespace") as StringArray;
                var tableNameArray = batch.Column("tableName") as StringArray;
                var colNameArray = batch.Column("col_name") as StringArray;
                var columnTypeArray = batch.Column("columnType") as StringArray;
                var isNullableArray = batch.Column("isNullable") as StringArray;

                StringArray? commentArray = null;
                try
                {
                    commentArray = batch.Column("comment") as StringArray;
                }
                catch { }

                if (colNameArray == null || columnTypeArray == null || tableNameArray == null ||
                    catalogArray == null || schemaArray == null)
                {
                    continue;
                }

                for (int i = 0; i < batch.Length; i++)
                {
                    if (colNameArray.IsNull(i) || columnTypeArray.IsNull(i) || tableNameArray.IsNull(i) ||
                        catalogArray.IsNull(i) || schemaArray.IsNull(i))
                    {
                        continue;
                    }

                    var colName = colNameArray.GetString(i);
                    var dataType = columnTypeArray.GetString(i);
                    var currentTableName = tableNameArray.GetString(i);
                    var currentCatalogName = catalogArray.GetString(i);
                    var currentSchemaName = schemaArray.GetString(i);

                    // Skip empty column names (defensive check)
                    if (string.IsNullOrEmpty(colName))
                    {
                        continue;
                    }

                    var tableKey = $"`{currentCatalogName}`.`{currentSchemaName}`.`{currentTableName}`";
                    if (!tablePositions.ContainsKey(tableKey))
                    {
                        tablePositions[tableKey] = 1;
                    }
                    int position = tablePositions[tableKey]++;

                    var comment = commentArray != null && !commentArray.IsNull(i) ? commentArray.GetString(i) : null;
                    bool isNullable = true; // Default to nullable
                    if (isNullableArray != null && !isNullableArray.IsNull(i))
                    {
                        var nullableStr = isNullableArray.GetString(i);
                        isNullable = string.IsNullOrEmpty(nullableStr) || !nullableStr.Equals("false", StringComparison.OrdinalIgnoreCase);
                    }

                    var xdbcDataType = DatabricksTypeMapper.GetXdbcDataType(dataType);
                    var baseType = DatabricksTypeMapper.StripBaseTypeName(dataType);

                    // Build all columns
                    catalogBuilder.Append(currentCatalogName);
                    schemaBuilder.Append(currentSchemaName);
                    tableBuilder.Append(currentTableName);
                    columnNameBuilder.Append(colName);
                    typeNameBuilder.Append(dataType);
                    positionBuilder.Append(position);
                    nullableBuilder.Append(isNullable);

                    _ = comment != null ? commentBuilder.Append(comment) : commentBuilder.AppendNull();
                    _ = xdbcDataType.HasValue ? xdbcDataTypeBuilder.Append(xdbcDataType.Value) : xdbcDataTypeBuilder.AppendNull();
                    _ = baseType != null ? xdbcTypeNameBuilder.Append(baseType) : xdbcTypeNameBuilder.AppendNull();

                    var columnSize = DatabricksTypeMapper.GetColumnSize(dataType);
                    _ = columnSize.HasValue ? xdbcColumnSizeBuilder.Append(columnSize.Value) : xdbcColumnSizeBuilder.AppendNull();

                    var decimalDigits = DatabricksTypeMapper.GetDecimalDigits(dataType);
                    _ = decimalDigits.HasValue ? xdbcDecimalDigitsBuilder.Append(decimalDigits.Value) : xdbcDecimalDigitsBuilder.AppendNull();

                    var numPrecRadix = DatabricksTypeMapper.GetNumPrecRadix(dataType);
                    _ = numPrecRadix.HasValue ? xdbcNumPrecRadixBuilder.Append(numPrecRadix.Value) : xdbcNumPrecRadixBuilder.AppendNull();

                    var bufferLength = DatabricksTypeMapper.GetBufferLength(dataType);
                    _ = bufferLength.HasValue ? xdbcBufferLengthBuilder.Append(bufferLength.Value) : xdbcBufferLengthBuilder.AppendNull();

                    var charOctetLength = DatabricksTypeMapper.GetCharOctetLength(dataType);
                    _ = charOctetLength.HasValue ? xdbcCharOctetLengthBuilder.Append(charOctetLength.Value) : xdbcCharOctetLengthBuilder.AppendNull();

                    var sqlDataType = DatabricksTypeMapper.GetSqlDataType(dataType);
                    _ = sqlDataType.HasValue ? xdbcSqlDataTypeBuilder.Append(sqlDataType.Value) : xdbcSqlDataTypeBuilder.AppendNull();

                    var datetimeSub = DatabricksTypeMapper.GetSqlDatetimeSub(dataType);
                    _ = datetimeSub.HasValue ? xdbcDatetimeSubBuilder.Append(datetimeSub.Value) : xdbcDatetimeSubBuilder.AppendNull();
                }
            }

            // Build ADBC schema with all metadata fields (17 columns, lowercase names)
            var schema = new Schema(new[]
            {
                new Field("catalog_name", StringType.Default, nullable: false),
                new Field("db_schema_name", StringType.Default, nullable: false),
                new Field("table_name", StringType.Default, nullable: false),
                new Field("column_name", StringType.Default, nullable: false),
                new Field("type_name", StringType.Default, nullable: false),
                new Field("ordinal_position", Int32Type.Default, nullable: false),
                new Field("is_nullable", BooleanType.Default, nullable: false),
                new Field("remarks", StringType.Default, nullable: true),
                new Field("xdbc_data_type", Int16Type.Default, nullable: true),
                new Field("xdbc_type_name", StringType.Default, nullable: true),
                new Field("xdbc_column_size", Int32Type.Default, nullable: true),
                new Field("xdbc_decimal_digits", Int32Type.Default, nullable: true),
                new Field("xdbc_num_prec_radix", Int16Type.Default, nullable: true),
                new Field("xdbc_buffer_length", Int32Type.Default, nullable: true),
                new Field("xdbc_char_octet_length", Int32Type.Default, nullable: true),
                new Field("xdbc_sql_data_type", Int16Type.Default, nullable: true),
                new Field("xdbc_datetime_sub", Int16Type.Default, nullable: true)
            }, null);

            var data = new List<IArrowArray>
            {
                catalogBuilder.Build(),
                schemaBuilder.Build(),
                tableBuilder.Build(),
                columnNameBuilder.Build(),
                typeNameBuilder.Build(),
                positionBuilder.Build(),
                nullableBuilder.Build(),
                commentBuilder.Build(),
                xdbcDataTypeBuilder.Build(),
                xdbcTypeNameBuilder.Build(),
                xdbcColumnSizeBuilder.Build(),
                xdbcDecimalDigitsBuilder.Build(),
                xdbcNumPrecRadixBuilder.Build(),
                xdbcBufferLengthBuilder.Build(),
                xdbcCharOctetLengthBuilder.Build(),
                xdbcSqlDataTypeBuilder.Build(),
                xdbcDatetimeSubBuilder.Build()
            };

            activity?.SetTag("result_count", catalogBuilder.Length);
            activity?.SetTag("unique_tables", tablePositions.Count);
            return new StatementExecInfoArrowStream(schema, data);
        }

        /// <summary>
        /// Builds an empty ADBC-formatted Arrow stream for error cases.
        /// </summary>
        private static IArrowArrayStream BuildEmptyColumnsMetadataStream()
        {
            var schema = new Schema(new[]
            {
                new Field("catalog_name", StringType.Default, nullable: false),
                new Field("db_schema_name", StringType.Default, nullable: false),
                new Field("table_name", StringType.Default, nullable: false),
                new Field("column_name", StringType.Default, nullable: false),
                new Field("type_name", StringType.Default, nullable: false),
                new Field("ordinal_position", Int32Type.Default, nullable: false),
                new Field("is_nullable", BooleanType.Default, nullable: false),
                new Field("remarks", StringType.Default, nullable: true),
                new Field("xdbc_data_type", Int16Type.Default, nullable: true),
                new Field("xdbc_type_name", StringType.Default, nullable: true),
                new Field("xdbc_column_size", Int32Type.Default, nullable: true),
                new Field("xdbc_decimal_digits", Int32Type.Default, nullable: true),
                new Field("xdbc_num_prec_radix", Int16Type.Default, nullable: true),
                new Field("xdbc_buffer_length", Int32Type.Default, nullable: true),
                new Field("xdbc_char_octet_length", Int32Type.Default, nullable: true),
                new Field("xdbc_sql_data_type", Int16Type.Default, nullable: true),
                new Field("xdbc_datetime_sub", Int16Type.Default, nullable: true)
            }, null);

            var emptyData = new List<IArrowArray>
            {
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new Int32Array.Builder().Build(),
                new BooleanArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new Int16Array.Builder().Build(),
                new StringArray.Builder().Build(),
                new Int32Array.Builder().Build(),
                new Int32Array.Builder().Build(),
                new Int16Array.Builder().Build(),
                new Int32Array.Builder().Build(),
                new Int32Array.Builder().Build(),
                new Int16Array.Builder().Build(),
                new Int16Array.Builder().Build()
            };

            return new StatementExecInfoArrowStream(schema, emptyData);
        }

        /// <summary>
        /// Gets columns in flat structure with Thrift HiveServer2-compatible column naming for statement-based queries.
        /// Reuses GetColumnsMetadataAsync() and transforms column names to match Thrift protocol using ColumnMetadataSchemas.
        /// </summary>
        /// <param name="catalog">Catalog name. If null, searches all catalogs.</param>
        /// <param name="dbSchemaPattern">Schema name pattern. If null, searches all schemas.</param>
        /// <param name="tablePattern">Table name pattern. If null, returns all tables.</param>
        /// <param name="columnPattern">Column name pattern. If null, returns all columns.</param>
        /// <returns>Arrow stream with 24-column Thrift/JDBC schema (uppercase column names)</returns>
        internal IArrowArrayStream GetColumnsFlat(string? catalog, string? dbSchemaPattern, string? tablePattern, string? columnPattern)
        {
            // Call core implementation
            var stream = GetColumnsMetadataAsync(catalog, dbSchemaPattern, tablePattern, columnPattern).GetAwaiter().GetResult();

            // Read ADBC stream and transform to Thrift schema
            var schema = ColumnMetadataSchemas.CreateColumnMetadataSchema(); // 24 columns
            var tableCatBuilder = new StringArray.Builder();
            var tableSchemaBuilder = new StringArray.Builder();
            var tableNameBuilder = new StringArray.Builder();
            var columnNameBuilder = new StringArray.Builder();
            var dataTypeBuilder = new Int32Array.Builder();
            var typeNameBuilder = new StringArray.Builder();
            var columnSizeBuilder = new Int32Array.Builder();
            var bufferLengthBuilder = new Int32Array.Builder();
            var decimalDigitsBuilder = new Int32Array.Builder();
            var numPrecRadixBuilder = new Int32Array.Builder();
            var nullableBuilder = new Int32Array.Builder();
            var remarksBuilder = new StringArray.Builder();
            var columnDefBuilder = new StringArray.Builder();
            var sqlDataTypeBuilder = new Int32Array.Builder();
            var sqlDatetimeSubBuilder = new Int32Array.Builder();
            var charOctetLengthBuilder = new Int32Array.Builder();
            var ordinalPositionBuilder = new Int32Array.Builder();
            var isNullableBuilder = new StringArray.Builder();
            var scopeCatalogBuilder = new StringArray.Builder();
            var scopeSchemaBuilder = new StringArray.Builder();
            var scopeTableBuilder = new StringArray.Builder();
            var sourceDataTypeBuilder = new Int16Array.Builder();
            var isAutoIncrementBuilder = new StringArray.Builder();
            var baseTypeNameBuilder = new StringArray.Builder();

            using (var reader = stream)
            {
                RecordBatch? batch;
                while ((batch = reader.ReadNextRecordBatchAsync().GetAwaiter().GetResult()) != null)
                {
                    // Extract columns from ADBC stream (17 columns, lowercase names)
                    var catalogArray = batch.Column("catalog_name") as StringArray;
                    var schemaArray = batch.Column("db_schema_name") as StringArray;
                    var tableArray = batch.Column("table_name") as StringArray;
                    var colNameArray = batch.Column("column_name") as StringArray;
                    var typeNameArray = batch.Column("type_name") as StringArray;
                    var positionArray = batch.Column("ordinal_position") as Int32Array;
                    var nullableArray = batch.Column("is_nullable") as BooleanArray;
                    var remarksArray = batch.Column("remarks") as StringArray;
                    var xdbcDataTypeArray = batch.Column("xdbc_data_type") as Int16Array;
                    var xdbcTypeNameArray = batch.Column("xdbc_type_name") as StringArray;
                    var xdbcColumnSizeArray = batch.Column("xdbc_column_size") as Int32Array;
                    var xdbcDecimalDigitsArray = batch.Column("xdbc_decimal_digits") as Int32Array;
                    var xdbcNumPrecRadixArray = batch.Column("xdbc_num_prec_radix") as Int16Array;
                    var xdbcBufferLengthArray = batch.Column("xdbc_buffer_length") as Int32Array;
                    var xdbcCharOctetLengthArray = batch.Column("xdbc_char_octet_length") as Int32Array;
                    var xdbcSqlDataTypeArray = batch.Column("xdbc_sql_data_type") as Int16Array;
                    var xdbcDatetimeSubArray = batch.Column("xdbc_datetime_sub") as Int16Array;

                    if (catalogArray != null && schemaArray != null && tableArray != null &&
                        colNameArray != null && typeNameArray != null && positionArray != null && nullableArray != null)
                    {
                        for (int i = 0; i < batch.Length; i++)
                        {
                            // TABLE_CAT
                            _ = !catalogArray.IsNull(i) ? tableCatBuilder.Append(catalogArray.GetString(i)) : tableCatBuilder.AppendNull();

                            // TABLE_SCHEM
                            _ = !schemaArray.IsNull(i) ? tableSchemaBuilder.Append(schemaArray.GetString(i)) : tableSchemaBuilder.AppendNull();

                            // TABLE_NAME
                            _ = !tableArray.IsNull(i) ? tableNameBuilder.Append(tableArray.GetString(i)) : tableNameBuilder.AppendNull();

                            // COLUMN_NAME
                            _ = !colNameArray.IsNull(i) ? columnNameBuilder.Append(colNameArray.GetString(i)) : columnNameBuilder.AppendNull();

                            // DATA_TYPE (Int32 XDBC type code)
                            _ = xdbcDataTypeArray != null && !xdbcDataTypeArray.IsNull(i) ? dataTypeBuilder.Append(xdbcDataTypeArray.GetValue(i)) : dataTypeBuilder.AppendNull();

                            // TYPE_NAME
                            _ = !typeNameArray.IsNull(i) ? typeNameBuilder.Append(typeNameArray.GetString(i)) : typeNameBuilder.AppendNull();

                            // COLUMN_SIZE
                            _ = xdbcColumnSizeArray != null && !xdbcColumnSizeArray.IsNull(i) ? columnSizeBuilder.Append(xdbcColumnSizeArray.GetValue(i)) : columnSizeBuilder.AppendNull();

                            // BUFFER_LENGTH (Int8 - always null for Databricks)
                            bufferLengthBuilder.AppendNull();

                            // DECIMAL_DIGITS
                            _ = xdbcDecimalDigitsArray != null && !xdbcDecimalDigitsArray.IsNull(i) ? decimalDigitsBuilder.Append(xdbcDecimalDigitsArray.GetValue(i)) : decimalDigitsBuilder.AppendNull();

                            // NUM_PREC_RADIX
                            _ = xdbcNumPrecRadixArray != null && !xdbcNumPrecRadixArray.IsNull(i) ? numPrecRadixBuilder.Append(xdbcNumPrecRadixArray.GetValue(i)) : numPrecRadixBuilder.AppendNull();

                            // NULLABLE (Int32: 0=no nulls, 1=nullable, 2=unknown)
                            nullableBuilder.Append(nullableArray != null && !nullableArray.IsNull(i) ? (nullableArray.GetValue(i)!.Value ? 1 : 0) : 2);

                            // REMARKS
                            _ = remarksArray != null && !remarksArray.IsNull(i) ? remarksBuilder.Append(remarksArray.GetString(i)) : remarksBuilder.Append("");

                            // COLUMN_DEF (default value - not supported)
                            columnDefBuilder.AppendNull();

                            // SQL_DATA_TYPE
                            _ = xdbcSqlDataTypeArray != null && !xdbcSqlDataTypeArray.IsNull(i) ? sqlDataTypeBuilder.Append(xdbcSqlDataTypeArray.GetValue(i)) : sqlDataTypeBuilder.AppendNull();

                            // SQL_DATETIME_SUB
                            _ = xdbcDatetimeSubArray != null && !xdbcDatetimeSubArray.IsNull(i) ? sqlDatetimeSubBuilder.Append(xdbcDatetimeSubArray.GetValue(i)) : sqlDatetimeSubBuilder.AppendNull();

                            // CHAR_OCTET_LENGTH
                            _ = xdbcCharOctetLengthArray != null && !xdbcCharOctetLengthArray.IsNull(i) ? charOctetLengthBuilder.Append(xdbcCharOctetLengthArray.GetValue(i)) : charOctetLengthBuilder.AppendNull();

                            // ORDINAL_POSITION
                            _ = positionArray != null && !positionArray.IsNull(i) ? ordinalPositionBuilder.Append(positionArray.GetValue(i)) : ordinalPositionBuilder.AppendNull();

                            // IS_NULLABLE (string: "YES", "NO", or "")
                            isNullableBuilder.Append(nullableArray != null && !nullableArray.IsNull(i) ? (nullableArray.GetValue(i)!.Value ? "YES" : "NO") : "");

                            // SCOPE_CATALOG, SCOPE_SCHEMA, SCOPE_TABLE - for REF types (not supported)
                            scopeCatalogBuilder.AppendNull();
                            scopeSchemaBuilder.AppendNull();
                            scopeTableBuilder.AppendNull();

                            // SOURCE_DATA_TYPE - for distinct/UDT types (not supported)
                            sourceDataTypeBuilder.AppendNull();

                            // IS_AUTO_INCREMENT (always "NO" for Databricks)
                            isAutoIncrementBuilder.Append("NO");

                            // BASE_TYPE_NAME
                            _ = xdbcTypeNameArray != null && !xdbcTypeNameArray.IsNull(i) ? baseTypeNameBuilder.Append(xdbcTypeNameArray.GetString(i)) : baseTypeNameBuilder.AppendNull();
                        }
                    }
                }
            }

            var data = new List<IArrowArray>
            {
                tableCatBuilder.Build(),
                tableSchemaBuilder.Build(),
                tableNameBuilder.Build(),
                columnNameBuilder.Build(),
                dataTypeBuilder.Build(),
                typeNameBuilder.Build(),
                columnSizeBuilder.Build(),
                bufferLengthBuilder.Build(),
                decimalDigitsBuilder.Build(),
                numPrecRadixBuilder.Build(),
                nullableBuilder.Build(),
                remarksBuilder.Build(),
                columnDefBuilder.Build(),
                sqlDataTypeBuilder.Build(),
                sqlDatetimeSubBuilder.Build(),
                charOctetLengthBuilder.Build(),
                ordinalPositionBuilder.Build(),
                isNullableBuilder.Build(),
                scopeCatalogBuilder.Build(),
                scopeSchemaBuilder.Build(),
                scopeTableBuilder.Build(),
                sourceDataTypeBuilder.Build(),
                isAutoIncrementBuilder.Build(),
                baseTypeNameBuilder.Build()
            };

            return new StatementExecInfoArrowStream(schema, data);
        }

        /// <summary>
        /// Gets extended column metadata including PK/FK information using DESC TABLE EXTENDED.
        /// This is a performance optimization that retrieves columns, primary keys, and foreign keys
        /// in a single SQL query instead of 3 separate queries.
        /// Matches Thrift protocol GetColumnsExtendedAsync behavior.
        /// </summary>
        /// <param name="catalog">Catalog name. If null, uses session catalog.</param>
        /// <param name="dbSchema">Schema name. If null, uses session schema.</param>
        /// <param name="tableName">Table name (required)</param>
        /// <returns>Arrow stream with extended column metadata including PK/FK fields</returns>
        /// <remarks>
        /// Returns standard GetColumns schema (24 columns) plus additional metadata columns (matching Thrift HiveServer2):
        /// - PK_COLUMN_NAME (string, nullable): Primary key column name if this column is part of a PK
        /// - FK_PKCOLUMN_NAME (string, nullable): Referenced column name (PK column in parent table)
        /// - FK_PKTABLE_CAT (string, nullable): Referenced table catalog (parent table catalog)
        /// - FK_PKTABLE_SCHEM (string, nullable): Referenced table schema (parent table schema)
        /// - FK_PKTABLE_NAME (string, nullable): Referenced table name (parent table name)
        /// - FK_FKCOLUMN_NAME (string, nullable): Foreign key column name (FK column in this table)
        /// - FK_FK_NAME (string, nullable): Foreign key constraint name
        /// - FK_KEQ_SEQ (int32, nullable): Position in multi-column FK (1-based)
        /// </remarks>
        internal IArrowArrayStream GetColumnsExtendedFlat(string? catalog, string? dbSchema, string tableName)
        {
            return GetColumnsExtendedAsync(catalog, dbSchema, tableName).GetAwaiter().GetResult();
        }

        private async Task<IArrowArrayStream> GetColumnsExtendedAsync(string? catalog, string? dbSchema, string tableName)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                // Catalog and schema are required for GetColumnsExtended
                if (string.IsNullOrEmpty(catalog) || string.IsNullOrEmpty(dbSchema))
                {
                    var ex = new ArgumentException("Catalog and schema are required for GetColumnsExtended");
                    activity?.AddException(ex, [
                        new("error.type", ex.GetType().Name),
                        new("operation", "GetColumnsExtended"),
                        new("catalog", catalog ?? "(none)"),
                        new("db_schema", dbSchema ?? "(none)"),
                        new("table_name", tableName)
                    ]);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    throw ex;
                }

                activity?.SetTag("catalog", catalog);
                activity?.SetTag("db_schema", dbSchema);
                activity?.SetTag("table_name", tableName);

                // Build qualified table name using MetadataUtilities
                string? fullTableName = MetadataUtilities.BuildQualifiedTableName(catalog, dbSchema, tableName);
                activity?.SetTag("qualified_table_name", fullTableName ?? "(none)");

                if (string.IsNullOrEmpty(fullTableName))
                {
                    var ex = new ArgumentException("Cannot build qualified table name", nameof(tableName));
                    activity?.AddException(ex, [
                        new("error.type", ex.GetType().Name),
                        new("operation", "GetColumnsExtended"),
                        new("catalog", catalog),
                        new("db_schema", dbSchema),
                        new("table_name", tableName)
                    ]);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    throw ex;
                }

                // Execute DESC TABLE EXTENDED AS JSON query
                string sql = $"DESC TABLE EXTENDED {fullTableName} AS JSON";
                activity?.SetTag("sql_query", sql);

                List<RecordBatch> batches;
                try
                {
                    batches = await ExecuteSqlQueryAsync(sql).ConfigureAwait(false);
                    activity?.SetTag("result_batches", batches.Count);
                    activity?.SetTag("total_rows", batches.Sum(b => b.Length));
                }
                catch (Exception ex)
                {
                    activity?.AddException(ex, [
                        new("error.type", ex.GetType().Name),
                        new("operation", "GetColumnsExtended"),
                        new("catalog", catalog ?? "(none)"),
                        new("db_schema", dbSchema ?? "(none)"),
                        new("table_name", tableName),
                        new("qualified_table_name", fullTableName)
                    ]);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    throw new AdbcException($"Failed to get extended columns for table {fullTableName}: {ex.Message}", ex);
                }

                if (batches == null || batches.Count == 0 || batches[0].Length == 0)
                {
                    activity?.AddEvent("get_columns_extended.empty_result");
                    var emptySchema = ColumnMetadataSchemas.CreateColumnMetadataSchema();
                    var emptyArrays = ColumnMetadataSchemas.CreateColumnMetadataEmptyArray();
                    return new StatementExecInfoArrowStream(emptySchema, emptyArrays);
                }

                // Parse JSON result
                var resultJson = ((StringArray)batches[0].Column(0)).GetString(0);
                activity?.SetTag("result_json_length", resultJson?.Length ?? 0);

                if (string.IsNullOrEmpty(resultJson))
                {
                    throw new FormatException($"Invalid JSON result of {sql}: result is null or empty");
                }

                var result = System.Text.Json.JsonSerializer.Deserialize<DescTableExtendedResult>(resultJson!);
                if (result == null)
                {
                    throw new FormatException($"Invalid JSON result of {sql}. Result={resultJson}");
                }

                activity?.SetTag("column_count", result.Columns?.Count ?? 0);
                activity?.SetTag("pk_count", result.PrimaryKeys?.Count ?? 0);
                activity?.SetTag("fk_count", result.ForeignKeys?.Count ?? 0);

                // Build extended columns result
                return BuildExtendedColumnsResult(result, catalog, dbSchema, tableName);
            });
        }

        /// <summary>
        /// Builds extended columns result with PK/FK metadata columns.
        /// Matches the format from DatabricksStatement.CreateExtendedColumnsResult.
        /// </summary>
        private IArrowArrayStream BuildExtendedColumnsResult(
            DescTableExtendedResult descResult,
            string? catalog,
            string? dbSchema,
            string tableName)
        {
            // Get base column metadata schema (24 columns)
            var baseSchema = ColumnMetadataSchemas.CreateColumnMetadataSchema();

            // Add PK metadata column (matching Thrift HiveServer2Statement.cs line 51-53)
            var pkField = new Field("PK_COLUMN_NAME", StringType.Default, true);

            // Add FK metadata columns (matching Thrift HiveServer2Statement.cs line 52)
            // Field names: PKCOLUMN_NAME, PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, FKCOLUMN_NAME, FK_NAME, KEQ_SEQ
            var fkFields = new List<Field>
            {
                new Field("FK_PKCOLUMN_NAME", StringType.Default, true),
                new Field("FK_PKTABLE_CAT", StringType.Default, true),
                new Field("FK_PKTABLE_SCHEM", StringType.Default, true),
                new Field("FK_PKTABLE_NAME", StringType.Default, true),
                new Field("FK_FKCOLUMN_NAME", StringType.Default, true),
                new Field("FK_FK_NAME", StringType.Default, true),
                new Field("FK_KEQ_SEQ", Int32Type.Default, true)
            };

            // Build combined schema
            var allFields = new List<Field>(baseSchema.FieldsList);
            allFields.Add(pkField);
            allFields.AddRange(fkFields);
            var combinedSchema = new Schema(allFields, baseSchema.Metadata);

            // Build base column arrays (24 columns)
            var tableCatBuilder = new StringArray.Builder();
            var tableSchemaBuilder = new StringArray.Builder();
            var tableNameBuilder = new StringArray.Builder();
            var columnNameBuilder = new StringArray.Builder();
            var dataTypeBuilder = new Int32Array.Builder();
            var typeNameBuilder = new StringArray.Builder();
            var columnSizeBuilder = new Int32Array.Builder();
            var bufferLengthBuilder = new Int32Array.Builder();
            var decimalDigitsBuilder = new Int32Array.Builder();
            var numPrecRadixBuilder = new Int32Array.Builder();
            var nullableBuilder = new Int32Array.Builder();
            var remarksBuilder = new StringArray.Builder();
            var columnDefBuilder = new StringArray.Builder();
            var sqlDataTypeBuilder = new Int32Array.Builder();
            var sqlDatetimeSubBuilder = new Int32Array.Builder();
            var charOctetLengthBuilder = new Int32Array.Builder();
            var ordinalPositionBuilder = new Int32Array.Builder();
            var isNullableBuilder = new StringArray.Builder();
            var scopeCatalogBuilder = new StringArray.Builder();
            var scopeSchemaBuilder = new StringArray.Builder();
            var scopeTableBuilder = new StringArray.Builder();
            var sourceDataTypeBuilder = new Int16Array.Builder();
            var isAutoIncrementBuilder = new StringArray.Builder();
            var baseTypeNameBuilder = new StringArray.Builder();

            // Build PK/FK metadata arrays (matching Thrift field names)
            var pkColumnBuilder = new StringArray.Builder();
            var fkPkColumnBuilder = new StringArray.Builder();     // FK_PKCOLUMN_NAME
            var fkPkTableCatBuilder = new StringArray.Builder();   // FK_PKTABLE_CAT
            var fkPkTableSchemBuilder = new StringArray.Builder(); // FK_PKTABLE_SCHEM
            var fkPkTableNameBuilder = new StringArray.Builder();  // FK_PKTABLE_NAME
            var fkFkColumnBuilder = new StringArray.Builder();     // FK_FKCOLUMN_NAME
            var fkFkNameBuilder = new StringArray.Builder();       // FK_FK_NAME
            var fkKeqSeqBuilder = new Int32Array.Builder();        // FK_KEQ_SEQ

            // Create lookup structures for PK/FK
            var pkColumns = new HashSet<string>(descResult.PrimaryKeys);
            var fkColumns = new Dictionary<string, (int idx, DescTableExtendedResult.ForeignKeyInfo fkInfo)>();

            foreach (var fkInfo in descResult.ForeignKeys)
            {
                for (var i = 0; i < fkInfo.LocalColumns.Count; i++)
                {
                    fkColumns.Add(fkInfo.LocalColumns[i], (i, fkInfo));
                }
            }

            // Process each column
            var position = 0;
            foreach (var column in descResult.Columns)
            {
                var baseTypeName = column.Type.Name.ToUpper();
                // Special case for TIMESTAMP_LTZ/NTZ and INT
                if (baseTypeName == "TIMESTAMP_LTZ" || baseTypeName == "TIMESTAMP_NTZ")
                {
                    baseTypeName = "TIMESTAMP";
                }
                else if (baseTypeName == "INT")
                {
                    baseTypeName = "INTEGER";
                }

                var fullTypeName = column.Type.FullTypeName;
                var colName = column.Name;
                int dataType = (int)column.DataType;

                // Base columns (matching Thrift format)
                tableCatBuilder.Append(descResult.CatalogName);
                tableSchemaBuilder.Append(descResult.SchemaName);
                tableNameBuilder.Append(descResult.TableName);
                columnNameBuilder.Append(colName);
                dataTypeBuilder.Append(dataType);
                typeNameBuilder.Append(fullTypeName);
                columnSizeBuilder.Append(column.ColumnSize);
                bufferLengthBuilder.AppendNull();
                decimalDigitsBuilder.Append(column.DecimalDigits);
                numPrecRadixBuilder.Append(column.IsNumber ? 10 : null);
                nullableBuilder.Append(column.Nullable ? 1 : 0);
                remarksBuilder.Append(column.Comment ?? "");
                columnDefBuilder.AppendNull();
                sqlDataTypeBuilder.AppendNull();
                sqlDatetimeSubBuilder.AppendNull();
                charOctetLengthBuilder.AppendNull();
                ordinalPositionBuilder.Append(position++);
                isNullableBuilder.Append(column.Nullable ? "YES" : "NO");
                scopeCatalogBuilder.AppendNull();
                scopeSchemaBuilder.AppendNull();
                scopeTableBuilder.AppendNull();
                sourceDataTypeBuilder.AppendNull();
                isAutoIncrementBuilder.Append("NO");
                baseTypeNameBuilder.Append(baseTypeName);

                // PK metadata
                pkColumnBuilder.Append(pkColumns.Contains(colName) ? colName : null);

                // FK metadata (matching Thrift HiveServer2 format)
                if (fkColumns.ContainsKey(colName))
                {
                    var (idx, fkInfo) = fkColumns[colName];
                    fkPkColumnBuilder.Append(fkInfo.RefColumns[idx]);       // FK_PKCOLUMN_NAME - referenced column
                    fkPkTableCatBuilder.Append(fkInfo.RefCatalog);          // FK_PKTABLE_CAT - referenced catalog
                    fkPkTableSchemBuilder.Append(fkInfo.RefSchema);         // FK_PKTABLE_SCHEM - referenced schema
                    fkPkTableNameBuilder.Append(fkInfo.RefTable);           // FK_PKTABLE_NAME - referenced table
                    fkFkColumnBuilder.Append(colName);                      // FK_FKCOLUMN_NAME - FK column name
                    fkFkNameBuilder.Append(fkInfo.KeyName);                 // FK_FK_NAME - FK constraint name
                    fkKeqSeqBuilder.Append(1 + idx);                        // FK_KEQ_SEQ - 1-based index
                }
                else
                {
                    fkPkColumnBuilder.AppendNull();
                    fkPkTableCatBuilder.AppendNull();
                    fkPkTableSchemBuilder.AppendNull();
                    fkPkTableNameBuilder.AppendNull();
                    fkFkColumnBuilder.AppendNull();
                    fkFkNameBuilder.AppendNull();
                    fkKeqSeqBuilder.AppendNull();
                }
            }

            // Build all arrays
            var combinedData = new List<IArrowArray>
            {
                // Base 24 columns
                tableCatBuilder.Build(),
                tableSchemaBuilder.Build(),
                tableNameBuilder.Build(),
                columnNameBuilder.Build(),
                dataTypeBuilder.Build(),
                typeNameBuilder.Build(),
                columnSizeBuilder.Build(),
                bufferLengthBuilder.Build(),
                decimalDigitsBuilder.Build(),
                numPrecRadixBuilder.Build(),
                nullableBuilder.Build(),
                remarksBuilder.Build(),
                columnDefBuilder.Build(),
                sqlDataTypeBuilder.Build(),
                sqlDatetimeSubBuilder.Build(),
                charOctetLengthBuilder.Build(),
                ordinalPositionBuilder.Build(),
                isNullableBuilder.Build(),
                scopeCatalogBuilder.Build(),
                scopeSchemaBuilder.Build(),
                scopeTableBuilder.Build(),
                sourceDataTypeBuilder.Build(),
                isAutoIncrementBuilder.Build(),
                baseTypeNameBuilder.Build(),

                // Extended PK/FK metadata (8 columns, matching Thrift order)
                pkColumnBuilder.Build(),         // PK_COLUMN_NAME
                fkPkColumnBuilder.Build(),       // FK_PKCOLUMN_NAME
                fkPkTableCatBuilder.Build(),     // FK_PKTABLE_CAT
                fkPkTableSchemBuilder.Build(),   // FK_PKTABLE_SCHEM
                fkPkTableNameBuilder.Build(),    // FK_PKTABLE_NAME
                fkFkColumnBuilder.Build(),       // FK_FKCOLUMN_NAME
                fkFkNameBuilder.Build(),         // FK_FK_NAME
                fkKeqSeqBuilder.Build()          // FK_KEQ_SEQ
            };

            return new StatementExecInfoArrowStream(combinedSchema, combinedData);
        }

        /// <summary>
        /// Builds a flat structure from ColumnInfo list matching Thrift HiveServer2 GetColumns format.
        /// </summary>
        private IArrowArrayStream BuildGetObjectsResult(
            GetObjectsDepth depth,
            Dictionary<string, Dictionary<string, Dictionary<string, (string tableType, List<ColumnInfo> columns)>>> catalogMap)
        {
            // Build full ADBC-compliant nested structure for ALL depth levels
            // The nested structure (catalog_name, catalog_db_schemas) is consistent with Thrift HiveServer2 implementation
            // and follows the ADBC standard: https://arrow.apache.org/adbc/current/format/specification.html#getobjects
            //
            // The depth parameter controls how deep the nesting goes:
            // - GetObjectsDepth.Catalogs: Only catalog_name populated, catalog_db_schemas contains nulls
            // - GetObjectsDepth.DbSchemas: catalog_name + schema names, tables are null
            // - GetObjectsDepth.Tables: catalog_name + schema names + table names, columns are null
            // - GetObjectsDepth.All: Full hierarchy with all metadata populated

            var catalogNameBuilder = new StringArray.Builder();
            var catalogDbSchemasValues = new List<IArrowArray?>();

            // Iterate over unified catalogMap structure
            foreach (var catalogEntry in catalogMap)
            {
                catalogNameBuilder.Append(catalogEntry.Key);

                if (depth == GetObjectsDepth.Catalogs)
                {
                    // For Catalogs depth: just catalog names, no nested schemas
                    catalogDbSchemasValues.Add(null);
                }
                else
                {
                    // For DbSchemas, Tables, and All depths: build nested schemas structure
                    catalogDbSchemasValues.Add(BuildDbSchemasStruct(depth, catalogEntry.Value));
                }
            }

            // Build and return the standardized GetObjects schema
            var resultSchema = StandardSchemas.GetObjectsSchema;
            var dataArrays = resultSchema.Validate(new List<IArrowArray>
            {
                catalogNameBuilder.Build(),
                catalogDbSchemasValues.BuildListArrayForType(new StructType(StandardSchemas.DbSchemaSchema))
            });

            return new StatementExecInfoArrowStream(resultSchema, dataArrays);
        }

        /// <summary>
        /// Gets catalogs matching the optional pattern using SHOW CATALOGS command.
        /// </summary>
        /// <param name="catalogPattern">Catalog name pattern (SQL LIKE syntax). If null, returns all catalogs.</param>
        /// <returns>Arrow stream with single column 'catalog_name' containing catalog names</returns>
        private async Task<IArrowArrayStream> GetCatalogsMetadataAsync(string? catalogPattern)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.SetTag("catalog_pattern", catalogPattern ?? "(none)");

                // Use SqlCommandBuilder for efficient SQL generation
                var commandBuilder = new SqlCommandBuilder()
                    .WithCatalogPattern(catalogPattern);

                var sql = commandBuilder.BuildShowCatalogs();
                activity?.SetTag("sql_query", sql);

                List<RecordBatch> batches;
                try
                {
                    batches = await ExecuteSqlQueryAsync(sql).ConfigureAwait(false);
                    activity?.SetTag("result_batches", batches.Count);
                    activity?.SetTag("total_rows", batches.Sum(b => b.Length));
                }
                catch (Exception ex)
                {
                    activity?.AddException(ex, [
                        new("error.type", ex.GetType().Name),
                        new("operation", "GetCatalogsMetadata"),
                        new("catalog_pattern", catalogPattern ?? "(none)")
                    ]);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    // Return empty result on permission denied or other errors
                    var emptySchema = new Schema(new[] { new Field("catalog_name", StringType.Default, nullable: false) }, null);
                    var emptyData = new List<IArrowArray> { new StringArray.Builder().Build() };
                    return new StatementExecInfoArrowStream(emptySchema, emptyData);
                }

                // Build result: single column "catalog_name"
                var catalogBuilder = new StringArray.Builder();
                foreach (var batch in batches)
                {
                    var catalogArray = batch.Column("catalog") as StringArray;
                    if (catalogArray == null) continue;

                    for (int i = 0; i < batch.Length; i++)
                    {
                        if (!catalogArray.IsNull(i))
                        {
                            catalogBuilder.Append(catalogArray.GetString(i));
                        }
                    }
                }

                var schema = new Schema(new[] { new Field("catalog_name", StringType.Default, nullable: false) }, null);
                var data = new List<IArrowArray> { catalogBuilder.Build() };
                var result = new StatementExecInfoArrowStream(schema, data);

                activity?.SetTag("result_count", catalogBuilder.Length);
                return result;
            });
        }

        /// <summary>
        /// Gets catalogs in flat structure with Thrift HiveServer2-compatible column naming for statement-based queries.
        /// Reuses GetCatalogsMetadataAsync() and transforms column names to match Thrift protocol.
        /// </summary>
        /// <param name="catalogPattern">Catalog name pattern. If null, returns all catalogs.</param>
        /// <returns>Arrow stream with TABLE_CAT column</returns>
        internal IArrowArrayStream GetCatalogsFlat(string? catalogPattern)
        {
            // Reuse existing method to get data
            var stream = GetCatalogsMetadataAsync(catalogPattern).GetAwaiter().GetResult();

            // Read data from ADBC-style stream
            var catalogBuilder = new StringArray.Builder();

            using (var reader = stream)
            {
                RecordBatch? batch;
                while ((batch = reader.ReadNextRecordBatchAsync().GetAwaiter().GetResult()) != null)
                {
                    // Extract catalog_name column
                    var catalogArray = batch.Column("catalog_name") as StringArray;
                    if (catalogArray != null)
                    {
                        for (int i = 0; i < catalogArray.Length; i++)
                        {
                            if (!catalogArray.IsNull(i))
                                catalogBuilder.Append(catalogArray.GetString(i));
                            else
                                catalogBuilder.AppendNull();
                        }
                    }
                }
            }

            // Create result with Thrift protocol column name
            var resultSchema = ColumnMetadataSchemas.CreateCatalogsSchema();
            var data = new List<IArrowArray> { catalogBuilder.Build() };

            return new StatementExecInfoArrowStream(resultSchema, data);
        }

        private async Task<IArrowArrayStream> GetSchemasMetadataAsync(string? catalog, string? schemaPattern)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.SetTag("catalog", catalog ?? "(none)");
                activity?.SetTag("schema_pattern", schemaPattern ?? "(none)");
                activity?.SetTag("show_all_catalogs", catalog == null);

                // Use SqlCommandBuilder for efficient SQL generation
                var commandBuilder = new SqlCommandBuilder()
                    .WithCatalog(catalog)
                    .WithSchemaPattern(schemaPattern);

                var sql = commandBuilder.BuildShowSchemas();
                activity?.SetTag("sql_query", sql);
                bool showAllCatalogs = catalog == null;

                List<RecordBatch> batches;
                try
                {
                    batches = await ExecuteSqlQueryAsync(sql).ConfigureAwait(false);
                    activity?.SetTag("result_batches", batches.Count);
                    activity?.SetTag("total_rows", batches.Sum(b => b.Length));
                }
                catch (Exception ex)
                {
                    activity?.AddException(ex, [
                        new("error.type", ex.GetType().Name),
                        new("operation", "GetSchemasMetadata"),
                        new("catalog", catalog ?? "(none)"),
                        new("schema_pattern", schemaPattern ?? "(none)")
                    ]);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    // Return empty result on permission denied or other errors
                    var emptySchema = new Schema(new[]
                    {
                        new Field("catalog_name", StringType.Default, nullable: true),
                        new Field("db_schema_name", StringType.Default, nullable: false)
                    }, null);
                    var emptyData = new List<IArrowArray> { new StringArray.Builder().Build(), new StringArray.Builder().Build() };
                    return new StatementExecInfoArrowStream(emptySchema, emptyData);
                }

                // Build result: catalog_name, db_schema_name
                var catalogBuilder = new StringArray.Builder();
                var schemaBuilder = new StringArray.Builder();

                foreach (var batch in batches)
                {
                    // SHOW SCHEMAS IN ALL CATALOGS returns: catalog_name, databaseName
                    // SHOW SCHEMAS IN catalog returns: databaseName
                    StringArray? catalogArray = null;
                    StringArray? schemaArray = null;

                    if (showAllCatalogs)
                    {
                        // When showing all catalogs, first column is catalog name
                        catalogArray = batch.Column(0) as StringArray;
                        schemaArray = batch.Column(1) as StringArray;
                    }
                    else
                    {
                        // When showing schemas in specific catalog, schema is in databaseName
                        schemaArray = batch.Column("databaseName") as StringArray;
                    }

                    if (schemaArray == null) continue;

                    for (int i = 0; i < batch.Length; i++)
                    {
                        if (!schemaArray.IsNull(i))
                        {
                            if (showAllCatalogs && catalogArray != null && !catalogArray.IsNull(i))
                            {
                                catalogBuilder.Append(catalogArray.GetString(i));
                            }
                            else
                            {
                                catalogBuilder.Append(catalog ?? "");
                            }
                            schemaBuilder.Append(schemaArray.GetString(i));
                        }
                    }
                }

            var resultSchema = new Schema(new[]
            {
                new Field("catalog_name", StringType.Default, nullable: true),
                new Field("db_schema_name", StringType.Default, nullable: false)
            }, null);

            var data = new List<IArrowArray> { catalogBuilder.Build(), schemaBuilder.Build() };

            activity?.SetTag("result_count", schemaBuilder.Length);
            return new StatementExecInfoArrowStream(resultSchema, data);
        });
        }

        /// <summary>
        /// Gets schemas in flat structure with Thrift HiveServer2-compatible column naming for statement-based queries.
        /// Reuses GetSchemasMetadataAsync() and transforms column names to match Thrift protocol.
        /// </summary>
        /// <param name="catalog">Catalog name. If null, uses session catalog or returns schemas from all catalogs.</param>
        /// <param name="schemaPattern">Schema name pattern (SQL LIKE syntax). If null, returns all schemas.</param>
        /// <returns>Arrow stream with TABLE_SCHEM and TABLE_CATALOG columns</returns>
        internal IArrowArrayStream GetSchemasFlat(string? catalog, string? schemaPattern)
        {
            // Reuse existing method to get data
            var stream = GetSchemasMetadataAsync(catalog, schemaPattern).GetAwaiter().GetResult();

            // Read data from ADBC-style stream
            var catalogBuilder = new StringArray.Builder();
            var schemaBuilder = new StringArray.Builder();

            using (var reader = stream)
            {
                RecordBatch? batch;
                while ((batch = reader.ReadNextRecordBatchAsync().GetAwaiter().GetResult()) != null)
                {
                    // Extract catalog_name and db_schema_name columns
                    var catalogArray = batch.Column("catalog_name") as StringArray;
                    var schemaArray = batch.Column("db_schema_name") as StringArray;

                    if (schemaArray != null)
                    {
                        for (int i = 0; i < schemaArray.Length; i++)
                        {
                            // TABLE_CATALOG
                            if (catalogArray != null && !catalogArray.IsNull(i))
                                catalogBuilder.Append(catalogArray.GetString(i));
                            else
                                catalogBuilder.AppendNull();

                            // TABLE_SCHEM
                            if (!schemaArray.IsNull(i))
                                schemaBuilder.Append(schemaArray.GetString(i));
                            else
                                schemaBuilder.AppendNull();
                        }
                    }
                }
            }

            // Create result with TABLE_SCHEM and TABLE_CATALOG columns
            var resultSchema = ColumnMetadataSchemas.CreateSchemasSchema();
            var data = new List<IArrowArray> { schemaBuilder.Build(), catalogBuilder.Build() };

            return new StatementExecInfoArrowStream(resultSchema, data);
        }

        /// <summary>
        /// Gets tables matching the optional patterns using SHOW TABLES command.
        /// Core implementation returning ADBC-formatted IArrowArrayStream with extended schema.
        /// </summary>
        /// <param name="catalog">Catalog name. If null, searches all catalogs.</param>
        /// <param name="dbSchemaPattern">Schema name pattern (SQL LIKE syntax). If null, searches all schemas.</param>
        /// <param name="tablePattern">Table name pattern (SQL LIKE syntax). If null, returns all tables.</param>
        /// <param name="tableTypes">List of table types to include (TABLE, VIEW). If null, returns all types.</param>
        /// <returns>Arrow stream with extended ADBC schema: catalog_name, db_schema_name, table_name, table_type, table_remarks (5 columns)</returns>
        /// <remarks>
        /// Extended schema includes table_remarks (5th column) to support both GetObjects (which ignores it) and
        /// statement-based queries (which need it for Thrift/JDBC compatibility). This maintains single source of truth
        /// while allowing different consumers to extract what they need.
        /// </remarks>
        private async Task<IArrowArrayStream> GetTablesMetadataAsync(string? catalog, string? dbSchemaPattern, string? tablePattern, IReadOnlyList<string>? tableTypes)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.SetTag("catalog", catalog ?? "(none)");
                activity?.SetTag("schema_pattern", dbSchemaPattern ?? "(none)");
                activity?.SetTag("table_pattern", tablePattern ?? "(none)");
                activity?.SetTag("table_types", tableTypes != null ? string.Join(",", tableTypes) : "(none)");

                // Use SqlCommandBuilder for efficient SQL generation
                var commandBuilder = new SqlCommandBuilder()
                    .WithCatalog(catalog)
                    .WithSchemaPattern(dbSchemaPattern)
                    .WithTablePattern(tablePattern);

                var sql = commandBuilder.BuildShowTables();
                activity?.SetTag("sql_query", sql);

                List<RecordBatch> batches;
                try
                {
                    batches = await ExecuteSqlQueryAsync(sql).ConfigureAwait(false);
                    activity?.SetTag("result_batches", batches.Count);
                    activity?.SetTag("total_rows", batches.Sum(b => b.Length));
                }
                catch (Exception ex)
                {
                    activity?.AddException(ex, [
                        new("error.type", ex.GetType().Name),
                        new("operation", "GetTablesMetadata"),
                        new("catalog", catalog ?? "(none)"),
                        new("schema_pattern", dbSchemaPattern ?? "(none)"),
                        new("table_pattern", tablePattern ?? "(none)")
                    ]);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    // Return empty result on permission denied or other errors
                    var emptySchema = new Schema(new[]
                    {
                        new Field("catalog_name", StringType.Default, nullable: true),
                        new Field("db_schema_name", StringType.Default, nullable: true),
                        new Field("table_name", StringType.Default, nullable: false),
                        new Field("table_type", StringType.Default, nullable: false),
                        new Field("table_remarks", StringType.Default, nullable: true)
                    }, null);
                    var emptyData = new List<IArrowArray>
                    {
                        new StringArray.Builder().Build(),
                        new StringArray.Builder().Build(),
                        new StringArray.Builder().Build(),
                        new StringArray.Builder().Build(),
                        new StringArray.Builder().Build()
                    };
                    return new StatementExecInfoArrowStream(emptySchema, emptyData);
                }

                // Build result with extended ADBC schema (5 columns including remarks)
                var catalogBuilder = new StringArray.Builder();
                var schemaBuilder = new StringArray.Builder();
                var tableBuilder = new StringArray.Builder();
                var typeBuilder = new StringArray.Builder();
                var remarksBuilder = new StringArray.Builder();

                foreach (var batch in batches)
                {
                    // SHOW TABLES always returns catalogName and namespace columns (unlike SHOW SCHEMAS)
                    var catalogArray = batch.Column("catalogName") as StringArray;
                    var schemaArray = batch.Column("namespace") as StringArray;
                    var tableNameArray = batch.Column("tableName") as StringArray;
                    var tableTypeArray = batch.Column("tableType") as StringArray;
                    var remarksArray = batch.Column("remarks") as StringArray;

                    if (tableNameArray == null || catalogArray == null || schemaArray == null) continue;

                    for (int i = 0; i < batch.Length; i++)
                    {
                        if (tableNameArray.IsNull(i) || catalogArray.IsNull(i) || schemaArray.IsNull(i))
                            continue;

                        var tableName = tableNameArray.GetString(i);
                        var catalogName = catalogArray.GetString(i);
                        var schemaName = schemaArray.GetString(i);

                        // Determine table type: prefer server's tableType column if available
                        string tableType = "TABLE";
                        if (tableTypeArray != null && !tableTypeArray.IsNull(i))
                        {
                            var serverTableType = tableTypeArray.GetString(i);
                            if (!string.IsNullOrEmpty(serverTableType))
                            {
                                tableType = serverTableType;
                            }
                        }

                        // Apply table type filter if specified
                        if (tableTypes != null && tableTypes.Count > 0 && !tableTypes.Contains(tableType))
                            continue;

                        // Get remarks from server response, use empty string if null/empty
                        string remarks = "";
                        if (remarksArray != null && !remarksArray.IsNull(i))
                        {
                            var remarksValue = remarksArray.GetString(i);
                            remarks = remarksValue ?? "";
                        }

                        catalogBuilder.Append(catalogName);
                        schemaBuilder.Append(schemaName);
                        tableBuilder.Append(tableName);
                        typeBuilder.Append(tableType);
                        remarksBuilder.Append(remarks);
                    }
                }

                var resultSchema = new Schema(new[]
                {
                    new Field("catalog_name", StringType.Default, nullable: true),
                    new Field("db_schema_name", StringType.Default, nullable: true),
                    new Field("table_name", StringType.Default, nullable: false),
                    new Field("table_type", StringType.Default, nullable: false),
                    new Field("table_remarks", StringType.Default, nullable: true)
                }, null);

                var data = new List<IArrowArray>
                {
                    catalogBuilder.Build(),
                    schemaBuilder.Build(),
                    tableBuilder.Build(),
                    typeBuilder.Build(),
                    remarksBuilder.Build()
                };

                activity?.SetTag("result_count", catalogBuilder.Length);
                return new StatementExecInfoArrowStream(resultSchema, data);
            });
        }

        /// <summary>
        /// Gets tables in flat structure with Thrift HiveServer2-compatible column naming for statement-based queries.
        /// Reuses GetTablesMetadataAsync() and transforms column names to match Thrift protocol.
        /// </summary>
        /// <param name="catalog">Catalog name. If null, uses session catalog.</param>
        /// <param name="dbSchemaPattern">Schema name pattern. If null, uses session schema.</param>
        /// <param name="tablePattern">Table name pattern (SQL LIKE syntax). If null, returns all tables.</param>
        /// <param name="tableTypes">List of table types to include (TABLE, VIEW). If null, returns all types.</param>
        /// <returns>Arrow stream with TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS, ... columns (Thrift protocol naming, 10 columns total)</returns>
        internal IArrowArrayStream GetTablesFlat(string? catalog, string? dbSchemaPattern, string? tablePattern, List<string>? tableTypes)
        {
            // Call core implementation
            var stream = GetTablesMetadataAsync(catalog, dbSchemaPattern, tablePattern, (IReadOnlyList<string>?)tableTypes).GetAwaiter().GetResult();

            // Read ADBC stream and transform to Thrift schema
            var catalogBuilder = new StringArray.Builder();
            var schemaBuilder = new StringArray.Builder();
            var tableBuilder = new StringArray.Builder();
            var typeBuilder = new StringArray.Builder();
            var remarksBuilder = new StringArray.Builder();
            var typeCatBuilder = new StringArray.Builder();
            var typeSchemaBuilder = new StringArray.Builder();
            var typeNameBuilder = new StringArray.Builder();
            var selfRefColBuilder = new StringArray.Builder();
            var refGenerationBuilder = new StringArray.Builder();

            using (var reader = stream)
            {
                RecordBatch? batch;
                while ((batch = reader.ReadNextRecordBatchAsync().GetAwaiter().GetResult()) != null)
                {
                    // Extract columns from ADBC stream
                    var catalogArray = batch.Column("catalog_name") as StringArray;
                    var schemaArray = batch.Column("db_schema_name") as StringArray;
                    var tableNameArray = batch.Column("table_name") as StringArray;
                    var tableTypeArray = batch.Column("table_type") as StringArray;
                    var tableRemarksArray = batch.Column("table_remarks") as StringArray;

                    if (catalogArray != null && schemaArray != null && tableNameArray != null && tableTypeArray != null)
                    {
                        for (int i = 0; i < batch.Length; i++)
                        {
                            // TABLE_CAT
                            if (!catalogArray.IsNull(i) && !string.IsNullOrEmpty(catalogArray.GetString(i)))
                                catalogBuilder.Append(catalogArray.GetString(i));
                            else
                                catalogBuilder.AppendNull();

                            // TABLE_SCHEM
                            if (!schemaArray.IsNull(i) && !string.IsNullOrEmpty(schemaArray.GetString(i)))
                                schemaBuilder.Append(schemaArray.GetString(i));
                            else
                                schemaBuilder.AppendNull();

                            // TABLE_NAME
                            if (!tableNameArray.IsNull(i))
                                tableBuilder.Append(tableNameArray.GetString(i));
                            else
                                tableBuilder.AppendNull();

                            // TABLE_TYPE
                            if (!tableTypeArray.IsNull(i))
                                typeBuilder.Append(tableTypeArray.GetString(i));
                            else
                                typeBuilder.AppendNull();

                            // REMARKS
                            if (tableRemarksArray != null && !tableRemarksArray.IsNull(i))
                                remarksBuilder.Append(tableRemarksArray.GetString(i));
                            else
                                remarksBuilder.Append("");

                            // TYPE_CAT, TYPE_SCHEM, TYPE_NAME - for structured types
                            typeCatBuilder.AppendNull();
                            typeSchemaBuilder.AppendNull();
                            typeNameBuilder.AppendNull();

                            // SELF_REFERENCING_COL_NAME - for self-referencing tables
                            selfRefColBuilder.AppendNull();

                            // REF_GENERATION - referential generation
                            refGenerationBuilder.AppendNull();
                        }
                    }
                }
            }

            // Create result with Thrift protocol column names (10 columns total)
            var resultSchema = ColumnMetadataSchemas.CreateTablesSchema();

            var data = new List<IArrowArray>
            {
                catalogBuilder.Build(),
                schemaBuilder.Build(),
                tableBuilder.Build(),
                typeBuilder.Build(),
                remarksBuilder.Build(),
                typeCatBuilder.Build(),
                typeSchemaBuilder.Build(),
                typeNameBuilder.Build(),
                selfRefColBuilder.Build(),
                refGenerationBuilder.Build()
            };

            return new StatementExecInfoArrowStream(resultSchema, data);
        }

        /// <remarks>
        /// <para>Returns 3 table types:</para>
        /// <list type="bullet">
        /// <item><description>TABLE - Regular persistent tables</description></item>
        /// <item><description>VIEW - Database views</description></item>
        /// <item><description>SYSTEM TABLE - System/catalog tables</description></item>
        /// </list>
        /// <para>Note: Aligns with JDBC/Thrift implementation table types.</para>
        /// </remarks>
        public override IArrowArrayStream GetTableTypes()
        {
            return this.TraceActivity(activity =>
            {
                var tableTypesBuilder = new StringArray.Builder();
                tableTypesBuilder.Append("TABLE");
                tableTypesBuilder.Append("VIEW");
                tableTypesBuilder.Append("SYSTEM TABLE");

                var schema = new Schema(new[] { new Field("table_type", StringType.Default, nullable: false) }, null);
                var data = new List<IArrowArray> { tableTypesBuilder.Build() };

                activity?.SetTag("result_count", tableTypesBuilder.Length);
                return new StatementExecInfoArrowStream(schema, data);
            });
        }

        /// <summary>
        /// Gets the Arrow schema for a specific table using DESCRIBE TABLE SQL command.
        /// Returns column names, types, and metadata (comments) from the table definition.
        /// </summary>
        /// <param name="catalog">Catalog name. If null, uses session catalog from connection properties.</param>
        /// <param name="dbSchema">Schema name. If null, uses session schema from connection properties.</param>
        /// <param name="tableName">Table name (required)</param>
        /// <returns>Arrow Schema object with field definitions matching the table structure</returns>
        /// <exception cref="AdbcException">Thrown when table does not exist or cannot be described</exception>
        /// <remarks>
        /// <para>Uses SQL command: DESCRIBE TABLE `catalog`.`schema`.`table`</para>
        /// <para>Databricks data types are mapped to Arrow types:</para>
        /// <list type="bullet">
        /// <item><description>INT → Int32Type</description></item>
        /// <item><description>BIGINT → Int64Type</description></item>
        /// <item><description>FLOAT → FloatType</description></item>
        /// <item><description>DOUBLE → DoubleType</description></item>
        /// <item><description>STRING → StringType</description></item>
        /// <item><description>BOOLEAN → BooleanType</description></item>
        /// <item><description>DATE → Date32Type</description></item>
        /// <item><description>TIMESTAMP → TimestampType(Microsecond)</description></item>
        /// <item><description>DECIMAL(p,s) → Decimal128Type</description></item>
        /// </list>
        /// <para>Column comments are preserved in field metadata.</para>
        /// </remarks>
        /// <example>
        /// <code>
        /// using var connection = database.Connect(parameters);
        /// var schema = connection.GetTableSchema("main", "default", "my_table");
        /// foreach (var field in schema.FieldsList)
        /// {
        ///     Console.WriteLine($"{field.Name}: {field.DataType}");
        /// }
        /// </code>
        /// </example>
        public override Schema GetTableSchema(string? catalog, string? dbSchema, string tableName)
        {
            return GetTableSchemaAsync(catalog, dbSchema, tableName).GetAwaiter().GetResult();
        }

        private async Task<Schema> GetTableSchemaAsync(string? catalog, string? dbSchema, string tableName)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.SetTag("catalog", catalog ?? "(none)");
                activity?.SetTag("db_schema", dbSchema ?? "(none)");
                activity?.SetTag("table_name", tableName ?? "(none)");

                var commandBuilder = new SqlCommandBuilder()
                    .WithCatalog(catalog)
                    .WithSchemaPattern(dbSchema)
                    .WithTablePattern(tableName);

                // Use SHOW COLUMNS instead of DESCRIBE TABLE for accurate nullable information
                var sql = commandBuilder.BuildShowColumns();
                activity?.SetTag("sql_query", sql);

                // Build qualified name for error messages
                var qualifiedTableName = MetadataUtilities.BuildQualifiedTableName(catalog, dbSchema, tableName) ?? tableName;
                activity?.SetTag("qualified_table_name", qualifiedTableName);

                List<RecordBatch> batches;
                try
                {
                    batches = await ExecuteSqlQueryAsync(sql).ConfigureAwait(false);
                    activity?.SetTag("result_batches", batches.Count);
                    activity?.SetTag("total_rows", batches.Sum(b => b.Length));
                }
                catch (Exception ex)
                {
                    activity?.AddException(ex, [
                        new("error.type", ex.GetType().Name),
                        new("operation", "GetTableSchema"),
                        new("catalog", catalog ?? "(unspecified)"),
                        new("schema", dbSchema ?? "(unspecified)"),
                        new("table", tableName),
                        new("qualified_table_name", qualifiedTableName)
                    ]);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    throw new AdbcException($"Failed to get columns for table {qualifiedTableName}: {ex.Message}", ex);
                }

                if (batches == null || batches.Count == 0)
                {
                    var ex = new AdbcException($"Table {qualifiedTableName} not found or has no columns");
                    activity?.AddException(ex, [
                        new("error.type", ex.GetType().Name),
                        new("operation", "GetTableSchema"),
                        new("qualified_table_name", qualifiedTableName)
                    ]);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    throw ex;
                }

            var fields = new List<Field>();

            foreach (var batch in batches)
            {
                var colNameArray = batch.Column("col_name") as StringArray;
                var columnTypeArray = batch.Column("columnType") as StringArray;
                var isNullableArray = batch.Column("isNullable") as StringArray;
                var remarksArray = batch.Column("remarks") as StringArray;

                if (colNameArray == null || columnTypeArray == null)
                {
                    continue;
                }

                for (int i = 0; i < batch.Length; i++)
                {
                    if (colNameArray.IsNull(i) || columnTypeArray.IsNull(i))
                        continue;

                    var colName = colNameArray.GetString(i);
                    var columnType = columnTypeArray.GetString(i);

                    // Skip empty column names (defensive check only)
                    if (string.IsNullOrEmpty(colName))
                    {
                        continue;
                    }

                    // Parse nullable from isNullable field (more accurate than hardcoded true)
                    bool nullable = true; // Default to true
                    if (isNullableArray != null && !isNullableArray.IsNull(i))
                    {
                        var nullableStr = isNullableArray.GetString(i);
                        nullable = string.IsNullOrEmpty(nullableStr) ||
                                   !nullableStr.Equals("false", StringComparison.OrdinalIgnoreCase);
                    }

                    var arrowType = ConvertDatabricksTypeToArrow(columnType);

                    // Build field metadata from remarks
                    var metadata = new Dictionary<string, string>();
                    if (remarksArray != null && !remarksArray.IsNull(i))
                    {
                        var remark = remarksArray.GetString(i);
                        if (!string.IsNullOrEmpty(remark))
                        {
                            metadata["comment"] = remark;
                        }
                    }

                    var field = new Field(colName, arrowType, nullable, metadata.Count > 0 ? metadata : null);
                    fields.Add(field);
                }
            }

            if (fields.Count == 0)
            {
                var ex = new AdbcException($"Table {qualifiedTableName} has no columns");
                activity?.AddException(ex, [
                    new("error.type", ex.GetType().Name),
                    new("operation", "GetTableSchema"),
                    new("qualified_table_name", qualifiedTableName)
                ]);
                activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                throw ex;
            }

            activity?.SetTag("result_fields", fields.Count);
            return new Schema(fields, null);
        });
        }

        #region Metadata Implementation Helpers

        private async Task<List<RecordBatch>> ExecuteSqlQueryAsync(string sql)
        {
            // Log SQL command for debugging
            Console.WriteLine($"[SEA SQL]: {sql}");

            using var statement = CreateStatement();
            (statement as StatementExecutionStatement)!.SqlQuery = sql;

            var queryResult = await (statement as StatementExecutionStatement)!.ExecuteQueryAsync().ConfigureAwait(false);
            var batches = new List<RecordBatch>();

            RecordBatch? batch;
            while ((batch = await queryResult.Stream!.ReadNextRecordBatchAsync().ConfigureAwait(false)) != null)
            {
                batches.Add(batch);
            }

            return batches;
        }

        private IArrowType ConvertDatabricksTypeToArrow(string databricksType)
        {
            var baseType = ExtractBaseType(databricksType).ToUpperInvariant();

            return baseType switch
            {
                "BOOLEAN" => BooleanType.Default,
                "TINYINT" => Int8Type.Default,
                "SMALLINT" => Int16Type.Default,
                "INT" or "INTEGER" => Int32Type.Default,
                "BIGINT" => Int64Type.Default,
                "FLOAT" => FloatType.Default,
                "DOUBLE" => DoubleType.Default,
                "DECIMAL" or "NUMERIC" => new Decimal128Type(38, 18),
                "STRING" or "VARCHAR" or "CHAR" => StringType.Default,
                "BINARY" => BinaryType.Default,
                "DATE" => Date32Type.Default,
                "TIMESTAMP" or "TIMESTAMP_NTZ" => new TimestampType(TimeUnit.Microsecond, (string?)null),
                _ => StringType.Default
            };
        }

        private string ExtractBaseType(string typeString)
        {
            if (string.IsNullOrEmpty(typeString))
                return string.Empty;

            var match = System.Text.RegularExpressions.Regex.Match(typeString, @"^([A-Za-z_][A-Za-z0-9_]*)");
            return match.Success ? match.Groups[1].Value : typeString;
        }

        /// <summary>
        /// Builds a StructArray for db_schemas with nested table lists.
        /// Used for GetObjects(All) depth to create the full nested structure.
        /// </summary>
        private static StructArray BuildDbSchemasStruct(
            GetObjectsDepth depth,
            Dictionary<string, Dictionary<string, (string tableType, List<ColumnInfo> columns)>> schemaMap)
        {
            var dbSchemaNameBuilder = new StringArray.Builder();
            var dbSchemaTablesValues = new List<IArrowArray?>();
            var nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;

            foreach (var schemaEntry in schemaMap)
            {
                dbSchemaNameBuilder.Append(schemaEntry.Key);
                length++;
                nullBitmapBuffer.Append(true);

                if (depth == GetObjectsDepth.DbSchemas)
                {
                    dbSchemaTablesValues.Add(null);
                }
                else
                {
                    dbSchemaTablesValues.Add(BuildTablesStruct(depth, schemaEntry.Value));
                }
            }

            var schema = StandardSchemas.DbSchemaSchema;
            var dataArrays = schema.Validate(new List<IArrowArray>
            {
                dbSchemaNameBuilder.Build(),
                dbSchemaTablesValues.BuildListArrayForType(new StructType(StandardSchemas.TableSchema))
            });

            return new StructArray(
                new StructType(schema),
                length,
                dataArrays,
                nullBitmapBuffer.Build());
        }

        /// <summary>
        /// Builds a StructArray for tables with nested column lists.
        /// Used for GetObjects(All) depth to create the full nested structure.
        /// </summary>
        private static StructArray BuildTablesStruct(
            GetObjectsDepth depth,
            Dictionary<string, (string tableType, List<ColumnInfo> columns)> tableMap)
        {
            var tableNameBuilder = new StringArray.Builder();
            var tableTypeBuilder = new StringArray.Builder();
            var tableColumnsValues = new List<IArrowArray?>();
            var tableConstraintsValues = new List<IArrowArray?>();
            var nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;

            foreach (var tableEntry in tableMap)
            {
                tableNameBuilder.Append(tableEntry.Key);
                tableTypeBuilder.Append(tableEntry.Value.tableType);
                nullBitmapBuffer.Append(true);
                length++;

                // Constraints not supported in REST API
                tableConstraintsValues.Add(null);

                if (depth == GetObjectsDepth.Tables)
                {
                    tableColumnsValues.Add(null);
                }
                else
                {
                    tableColumnsValues.Add(BuildColumnsStruct(tableEntry.Value.columns));
                }
            }

            var schema = StandardSchemas.TableSchema;
            var dataArrays = schema.Validate(new List<IArrowArray>
            {
                tableNameBuilder.Build(),
                tableTypeBuilder.Build(),
                tableColumnsValues.BuildListArrayForType(new StructType(StandardSchemas.ColumnSchema)),
                tableConstraintsValues.BuildListArrayForType(new StructType(StandardSchemas.ConstraintSchema))
            });

            return new StructArray(
                new StructType(schema),
                length,
                dataArrays,
                nullBitmapBuffer.Build());
        }

        /// <summary>
        /// Builds a StructArray for columns.
        /// Used for GetObjects(All) depth to create the full nested structure.
        /// </summary>
        private static StructArray BuildColumnsStruct(List<ColumnInfo> columns)
        {
            var columnNameBuilder = new StringArray.Builder();
            var ordinalPositionBuilder = new Int32Array.Builder();
            var remarksBuilder = new StringArray.Builder();
            var xdbcDataTypeBuilder = new Int16Array.Builder();
            var xdbcTypeNameBuilder = new StringArray.Builder();
            var xdbcColumnSizeBuilder = new Int32Array.Builder();
            var xdbcDecimalDigitsBuilder = new Int16Array.Builder();
            var xdbcNumPrecRadixBuilder = new Int16Array.Builder();
            var xdbcNullableBuilder = new Int16Array.Builder();
            var xdbcColumnDefBuilder = new StringArray.Builder();
            var xdbcSqlDataTypeBuilder = new Int16Array.Builder();
            var xdbcDatetimeSubBuilder = new Int16Array.Builder();
            var xdbcCharOctetLengthBuilder = new Int32Array.Builder();
            var xdbcIsNullableBuilder = new StringArray.Builder();
            var xdbcScopeCatalogBuilder = new StringArray.Builder();
            var xdbcScopeSchemaBuilder = new StringArray.Builder();
            var xdbcScopeTableBuilder = new StringArray.Builder();
            var xdbcIsAutoincrementBuilder = new BooleanArray.Builder();
            var xdbcIsGeneratedcolumnBuilder = new BooleanArray.Builder();
            var nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;

            foreach (var column in columns)
            {
                columnNameBuilder.Append(column.Name);
                ordinalPositionBuilder.Append(column.Position);
                // Preserve original casing from server (e.g., "STRUCT<f1: STRING>" not "STRUCT<F1: STRING>")
                remarksBuilder.Append(column.TypeName);

                _ = column.XdbcDataType.HasValue ? xdbcDataTypeBuilder.Append(column.XdbcDataType.Value) : xdbcDataTypeBuilder.AppendNull();
                xdbcTypeNameBuilder.Append(column.XdbcTypeName ?? string.Empty);
                _ = column.XdbcColumnSize.HasValue ? xdbcColumnSizeBuilder.Append(column.XdbcColumnSize.Value) : xdbcColumnSizeBuilder.AppendNull();
                _ = column.XdbcDecimalDigits.HasValue ? xdbcDecimalDigitsBuilder.Append((short)column.XdbcDecimalDigits.Value) : xdbcDecimalDigitsBuilder.AppendNull();
                _ = column.XdbcNumPrecRadix.HasValue ? xdbcNumPrecRadixBuilder.Append(column.XdbcNumPrecRadix.Value) : xdbcNumPrecRadixBuilder.AppendNull();

                xdbcNullableBuilder.Append((short)(column.Nullable ? 1 : 0));
                xdbcColumnDefBuilder.Append("");

                _ = column.XdbcSqlDataType.HasValue ? xdbcSqlDataTypeBuilder.Append(column.XdbcSqlDataType.Value) : xdbcSqlDataTypeBuilder.AppendNull();
                _ = column.XdbcDatetimeSub.HasValue ? xdbcDatetimeSubBuilder.Append(column.XdbcDatetimeSub.Value) : xdbcDatetimeSubBuilder.AppendNull();
                _ = column.XdbcCharOctetLength.HasValue ? xdbcCharOctetLengthBuilder.Append(column.XdbcCharOctetLength.Value) : xdbcCharOctetLengthBuilder.AppendNull();

                xdbcIsNullableBuilder.Append(column.Nullable ? "YES" : "NO");
                xdbcScopeCatalogBuilder.AppendNull();
                xdbcScopeSchemaBuilder.AppendNull();
                xdbcScopeTableBuilder.AppendNull();
                xdbcIsAutoincrementBuilder.Append(false);
                xdbcIsGeneratedcolumnBuilder.Append(true);
                nullBitmapBuffer.Append(true);
                length++;
            }

            var schema = StandardSchemas.ColumnSchema;
            var dataArrays = schema.Validate(new List<IArrowArray>
            {
                columnNameBuilder.Build(),
                ordinalPositionBuilder.Build(),
                remarksBuilder.Build(),
                xdbcDataTypeBuilder.Build(),
                xdbcTypeNameBuilder.Build(),
                xdbcColumnSizeBuilder.Build(),
                xdbcDecimalDigitsBuilder.Build(),
                xdbcNumPrecRadixBuilder.Build(),
                xdbcNullableBuilder.Build(),
                xdbcColumnDefBuilder.Build(),
                xdbcSqlDataTypeBuilder.Build(),
                xdbcDatetimeSubBuilder.Build(),
                xdbcCharOctetLengthBuilder.Build(),
                xdbcIsNullableBuilder.Build(),
                xdbcScopeCatalogBuilder.Build(),
                xdbcScopeSchemaBuilder.Build(),
                xdbcScopeTableBuilder.Build(),
                xdbcIsAutoincrementBuilder.Build(),
                xdbcIsGeneratedcolumnBuilder.Build()
            });

            return new StructArray(
                new StructType(schema),
                length,
                dataArrays,
                nullBitmapBuffer.Build());
        }

        private struct ColumnInfo
        {
            // Context information
            public string Catalog { get; set; }
            public string Schema { get; set; }
            public string Table { get; set; }

            // Column information
            public string Name { get; set; }
            public string TypeName { get; set; }
            public int Position { get; set; }
            public bool Nullable { get; set; }
            public string? Comment { get; set; }

            // Computed XDBC fields (populated using DatabricksTypeMapper)
            public short? XdbcDataType { get; set; }
            public string? XdbcTypeName { get; set; }
            public int? XdbcColumnSize { get; set; }
            public int? XdbcDecimalDigits { get; set; }
            public short? XdbcNumPrecRadix { get; set; }
            public int? XdbcBufferLength { get; set; }
            public int? XdbcCharOctetLength { get; set; }
            public short? XdbcSqlDataType { get; set; }
            public short? XdbcDatetimeSub { get; set; }
        }

        /// <summary>
        /// IArrowArrayStream adapter for Statement Execution API metadata queries.
        /// Wraps pre-materialized (in-memory) data for GetObjects, GetTableTypes, GetInfo, etc.
        ///
        /// This is parallel to HiveInfoArrowStream in the Thrift protocol implementation.
        /// Used when metadata is built entirely in memory before returning to the caller,
        /// as opposed to streaming readers (CloudFetchReader) that fetch data incrementally.
        /// </summary>
        private class StatementExecInfoArrowStream : IArrowArrayStream
        {
            private Schema _schema;
            private RecordBatch? _batch;

            public StatementExecInfoArrowStream(Schema schema, IReadOnlyList<IArrowArray> data)
            {
                _schema = schema ?? throw new ArgumentNullException(nameof(schema));
                if (data == null)
                {
                    throw new ArgumentNullException(nameof(data));
                }

                if (data.Count == 0)
                {
                    _batch = null;
                }
                else
                {
                    _batch = new RecordBatch(schema, data, data[0].Length);
                }
            }

            public StatementExecInfoArrowStream(Schema schema, RecordBatch[] batches)
            {
                _schema = schema;
                _batch = batches.Length > 0 ? batches[0] : null;
            }

            public Schema Schema => _schema;

            public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                RecordBatch? batch = _batch;
                _batch = null;
                return new ValueTask<RecordBatch?>(batch);
            }

            public void Dispose()
            {
                _batch?.Dispose();
                _batch = null;
            }
        }

        #endregion

        /// <summary>
        /// Gets information about the driver and database.
        /// Returns standardized metadata including driver name, version, vendor information, and capabilities.
        /// </summary>
        /// <param name="codes">List of info codes to retrieve. If empty, returns all supported info codes.</param>
        /// <returns>Arrow stream with info_name (uint32) and info_value (union) columns per ADBC spec</returns>
        /// <remarks>
        /// <para>Returns metadata as Arrow stream with DenseUnionType for info_value column.</para>
        /// <para>Supported info codes:</para>
        /// <list type="bullet">
        /// <item><description>VendorName: "Databricks"</description></item>
        /// <item><description>VendorVersion: Databricks server version (if available)</description></item>
        /// <item><description>VendorArrowVersion: Apache Arrow version</description></item>
        /// <item><description>VendorSql: false (uses Spark SQL, not standard SQL)</description></item>
        /// <item><description>DriverName: "ADBC Databricks Driver (Statement Execution API)"</description></item>
        /// <item><description>DriverVersion: Driver assembly version</description></item>
        /// <item><description>DriverArrowVersion: Apache Arrow version used by driver</description></item>
        /// </list>
        /// </remarks>
        public override IArrowArrayStream GetInfo(IReadOnlyList<AdbcInfoCode> codes)
        {
            return this.TraceActivity(activity =>
            {
                activity?.SetTag("requested_codes_count", codes?.Count ?? 0);
                activity?.SetTag("requested_codes", codes != null && codes.Count > 0 ? string.Join(",", codes) : "(none)");

                // Info value type IDs for DenseUnionType
                const int strValTypeID = 0;
                const int boolValTypeId = 1;

                // Supported info codes for Statement Execution API
                var supportedCodes = new[]
                {
                    AdbcInfoCode.VendorName,
                    AdbcInfoCode.VendorVersion,
                    AdbcInfoCode.VendorSql,
                    AdbcInfoCode.DriverName,
                    AdbcInfoCode.DriverVersion,
                    AdbcInfoCode.DriverArrowVersion
                };

                // If no codes specified, return all supported codes
                if (codes == null || codes.Count == 0)
                {
                    codes = supportedCodes;
                    activity?.SetTag("using_all_supported_codes", true);
                }

            // Build info arrays
            var infoNameBuilder = new UInt32Array.Builder();
            var typeBuilder = new ArrowBuffer.Builder<byte>();
            var offsetBuilder = new ArrowBuffer.Builder<int>();
            var stringInfoBuilder = new StringArray.Builder();
            var booleanInfoBuilder = new BooleanArray.Builder();

            int nullCount = 0;
            int arrayLength = codes.Count;
            int offset = 0;

            // Populate info values
            foreach (var code in codes)
            {
                switch (code)
                {
                    case AdbcInfoCode.VendorName:
                        infoNameBuilder.Append((uint)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(offset++);
                        stringInfoBuilder.Append("Databricks");
                        booleanInfoBuilder.AppendNull();
                        break;

                    case AdbcInfoCode.VendorVersion:
                        infoNameBuilder.Append((uint)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(offset++);
                        // Try to get warehouse version from properties, otherwise use "Unknown"
                        string vendorVersion = PropertyHelper.GetStringProperty(_properties, "adbc.databricks.warehouse_version", "Unknown");
                        stringInfoBuilder.Append(vendorVersion);
                        booleanInfoBuilder.AppendNull();
                        break;

                    case AdbcInfoCode.VendorSql:
                        infoNameBuilder.Append((uint)code);
                        typeBuilder.Append(boolValTypeId);
                        offsetBuilder.Append(offset++);
                        stringInfoBuilder.AppendNull();
                        booleanInfoBuilder.Append(true); // Matches Thrift implementation for consistency
                        break;

                    case AdbcInfoCode.DriverName:
                        infoNameBuilder.Append((uint)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(offset++);
                        stringInfoBuilder.Append("ADBC Databricks Driver (Statement Execution API)");
                        booleanInfoBuilder.AppendNull();
                        break;

                    case AdbcInfoCode.DriverVersion:
                        infoNameBuilder.Append((uint)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(offset++);
                        stringInfoBuilder.Append(AssemblyVersion);
                        booleanInfoBuilder.AppendNull();
                        break;

                    case AdbcInfoCode.DriverArrowVersion:
                        infoNameBuilder.Append((uint)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(offset++);
                        stringInfoBuilder.Append("1.0.0"); // ADBC Arrow version
                        booleanInfoBuilder.AppendNull();
                        break;

                    default:
                        // Unsupported code - return null
                        infoNameBuilder.Append((uint)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(offset++);
                        stringInfoBuilder.AppendNull();
                        booleanInfoBuilder.AppendNull();
                        nullCount++;
                        break;
                }
            }

            // Build empty arrays for unused union types
            var int64Array = new Int64Array.Builder().Build();
            var int32Array = new Int32Array.Builder().Build();
            var stringListArray = new ListArray.Builder(StringType.Default).Build();

            // Build empty struct array for int32_to_int32_list_map
            var entryType = new StructType(new[]
            {
                new Field("key", Int32Type.Default, false),
                new Field("value", Int32Type.Default, true)
            });
            var entriesDataArray = new StructArray(
                entryType,
                0,
                new[] { new Int32Array.Builder().Build(), new Int32Array.Builder().Build() },
                new ArrowBuffer.BitmapBuilder().Build());
            var mapArray = new List<IArrowArray?> { entriesDataArray }.BuildListArrayForType(entryType);

            // Build DenseUnionArray for info_value
            var childrenArrays = new IArrowArray[]
            {
                stringInfoBuilder.Build(),
                booleanInfoBuilder.Build(),
                int64Array,
                int32Array,
                stringListArray,
                mapArray
            };

            var infoUnionType = new UnionType(
                new[]
                {
                    new Field("string_value", StringType.Default, true),
                    new Field("bool_value", BooleanType.Default, true),
                    new Field("int64_value", Int64Type.Default, true),
                    new Field("int32_bitmask", Int32Type.Default, true),
                    new Field("string_list", new ListType(new Field("item", StringType.Default, true)), false),
                    new Field("int32_to_int32_list_map", new ListType(new Field("entries", entryType, false)), true)
                },
                new[] { 0, 1, 2, 3, 4, 5 },
                UnionMode.Dense);

            var infoValue = new DenseUnionArray(
                infoUnionType,
                arrayLength,
                childrenArrays,
                typeBuilder.Build(),
                offsetBuilder.Build(),
                nullCount);

            // Build final arrays
            var dataArrays = new IArrowArray[]
            {
                infoNameBuilder.Build(),
                infoValue
            };

            // Validate against standard schema
            StandardSchemas.GetInfoSchema.Validate(dataArrays);

            // Return as Arrow stream
            var schema = StandardSchemas.GetInfoSchema;
            activity?.SetTag("result_count", arrayLength);
            activity?.SetTag("null_count", nullCount);
            return new StatementExecInfoArrowStream(schema, dataArrays);
        });
        }

        private async Task<IArrowArrayStream> GetPrimaryKeysAsync(string? catalog, string? dbSchema, string tableName)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                if (string.IsNullOrEmpty(tableName))
                {
                    var ex = new ArgumentNullException(nameof(tableName), "Table name is required");
                    activity?.AddException(ex, [
                        new("error.type", ex.GetType().Name),
                        new("operation", "GetPrimaryKeys")
                    ]);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    throw ex;
                }

                // Catalog and schema are required for GetPrimaryKeys
                if (string.IsNullOrEmpty(catalog) || string.IsNullOrEmpty(dbSchema))
                {
                    var ex = new ArgumentException("Catalog and schema are required for GetPrimaryKeys");
                    activity?.AddException(ex, [
                        new("error.type", ex.GetType().Name),
                        new("operation", "GetPrimaryKeys"),
                        new("catalog", catalog ?? "(none)"),
                        new("db_schema", dbSchema ?? "(none)"),
                        new("table_name", tableName)
                    ]);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    throw ex;
                }

                activity?.SetTag("catalog", catalog);
                activity?.SetTag("db_schema", dbSchema);
                activity?.SetTag("table_name", tableName);

                // Format: SHOW KEYS IN CATALOG `catalog` IN SCHEMA `schema` IN TABLE `table`
                var commandBuilder = new SqlCommandBuilder()
                    .WithCatalog(catalog)
                    .WithSchema(dbSchema)
                    .WithTable(tableName);

                var sql = commandBuilder.BuildShowPrimaryKeys();
                activity?.SetTag("sql_query", sql);

                List<RecordBatch> batches;
                try
                {
                    batches = await ExecuteSqlQueryAsync(sql).ConfigureAwait(false);
                    activity?.SetTag("result_batches", batches.Count);
                    activity?.SetTag("total_rows", batches.Sum(b => b.Length));
                }
                catch (Exception ex)
                {
                    activity?.AddException(ex, [
                        new("error.type", ex.GetType().Name),
                        new("operation", "GetPrimaryKeys"),
                        new("catalog", catalog ?? "(none)"),
                        new("db_schema", dbSchema ?? "(none)"),
                        new("table_name", tableName),
                        new("note", "SHOW KEYS not supported or permission denied")
                    ]);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    // Return empty result gracefully
                    return BuildPrimaryKeysResult(new List<PrimaryKeyInfo>(), catalog, dbSchema, tableName);
                }

            var primaryKeys = new List<PrimaryKeyInfo>();
            int keySequence = 1;

            foreach (var batch in batches)
            {
                // Expected columns: col_name, constraintName, constraintType
                var colNameArray = batch.Column("col_name") as StringArray;
                var constraintNameArray = batch.Column("constraintName") as StringArray;
                var constraintTypeArray = batch.Column("constraintType") as StringArray;

                if (colNameArray == null || constraintTypeArray == null)
                    continue;

                for (int i = 0; i < batch.Length; i++)
                {
                    if (!colNameArray.IsNull(i) && !constraintTypeArray.IsNull(i))
                    {
                        var constraintType = constraintTypeArray.GetString(i);

                        // Only include PRIMARY constraints (Unity Catalog returns "PRIMARY" not "PRIMARY KEY")
                        if (constraintType.Equals("PRIMARY", StringComparison.OrdinalIgnoreCase))
                        {
                            var constraintName = constraintNameArray != null && !constraintNameArray.IsNull(i)
                                ? constraintNameArray.GetString(i)
                                : null;

                            primaryKeys.Add(new PrimaryKeyInfo
                            {
                                CatalogName = catalog,
                                SchemaName = dbSchema,
                                TableName = tableName,
                                ColumnName = colNameArray.GetString(i),
                                KeySequence = keySequence++,
                                ConstraintName = constraintName
                            });
                        }
                    }
                }
            }

            activity?.SetTag("result_primary_keys", primaryKeys.Count);
            return BuildPrimaryKeysResult(primaryKeys, catalog, dbSchema, tableName);
        });
        }

        private IArrowArrayStream BuildPrimaryKeysResult(List<PrimaryKeyInfo> primaryKeys, string? catalog, string? schema, string table)
        {
            var catalogBuilder = new StringArray.Builder();
            var schemaBuilder = new StringArray.Builder();
            var tableBuilder = new StringArray.Builder();
            var columnBuilder = new StringArray.Builder();
            var sequenceBuilder = new Int32Array.Builder();
            var constraintNameBuilder = new StringArray.Builder();

            foreach (var pk in primaryKeys)
            {
                if (pk.CatalogName != null)
                    catalogBuilder.Append(pk.CatalogName);
                else
                    catalogBuilder.AppendNull();

                if (pk.SchemaName != null)
                    schemaBuilder.Append(pk.SchemaName);
                else
                    schemaBuilder.AppendNull();

                tableBuilder.Append(pk.TableName);
                columnBuilder.Append(pk.ColumnName);
                sequenceBuilder.Append(pk.KeySequence);
                _ = pk.ConstraintName != null ? constraintNameBuilder.Append(pk.ConstraintName) : constraintNameBuilder.AppendNull();
            }

            var resultSchema = new Schema(new[]
            {
                new Field("catalog_name", StringType.Default, nullable: true),
                new Field("db_schema_name", StringType.Default, nullable: true),
                new Field("table_name", StringType.Default, nullable: false),
                new Field("column_name", StringType.Default, nullable: false),
                new Field("key_sequence", Int32Type.Default, nullable: false),
                new Field("constraint_name", StringType.Default, nullable: true)
            }, null);

            var data = new List<IArrowArray>
            {
                catalogBuilder.Build(),
                schemaBuilder.Build(),
                tableBuilder.Build(),
                columnBuilder.Build(),
                sequenceBuilder.Build(),
                constraintNameBuilder.Build()
            };

            return new StatementExecInfoArrowStream(resultSchema, data);
        }

        /// <summary>
        /// Gets primary keys in flat structure with Thrift HiveServer2-compatible column naming for statement-based queries.
        /// Reuses GetPrimaryKeysAsync() and transforms column names to match Thrift protocol.
        /// </summary>
        /// <param name="catalog">Catalog name. If null, uses session catalog.</param>
        /// <param name="dbSchema">Schema name. If null, uses session schema.</param>
        /// <param name="tableName">Table name (required)</param>
        /// <returns>Arrow stream with TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, KEQ_SEQ, PK_NAME columns (Thrift protocol naming)</returns>
        internal IArrowArrayStream GetPrimaryKeysFlat(string? catalog, string? dbSchema, string tableName)
        {
            // Reuse existing method to get data
            var stream = GetPrimaryKeysAsync(catalog, dbSchema, tableName).GetAwaiter().GetResult();

            // Read data from ADBC-style stream
            var catalogBuilder = new StringArray.Builder();
            var schemaBuilder = new StringArray.Builder();
            var tableBuilder = new StringArray.Builder();
            var columnBuilder = new StringArray.Builder();
            var sequenceBuilder = new Int32Array.Builder();
            var pkNameBuilder = new StringArray.Builder();

            using (var reader = stream)
            {
                RecordBatch? batch;
                while ((batch = reader.ReadNextRecordBatchAsync().GetAwaiter().GetResult()) != null)
                {
                    // Extract columns
                    var catalogArray = batch.Column("catalog_name") as StringArray;
                    var schemaArray = batch.Column("db_schema_name") as StringArray;
                    var tableArray = batch.Column("table_name") as StringArray;
                    var columnArray = batch.Column("column_name") as StringArray;
                    var sequenceArray = batch.Column("key_sequence") as Int32Array;
                    var constraintNameArray = batch.Column("constraint_name") as StringArray;

                    if (columnArray != null)
                    {
                        for (int i = 0; i < columnArray.Length; i++)
                        {
                            // TABLE_CAT
                            if (catalogArray != null && !catalogArray.IsNull(i))
                                catalogBuilder.Append(catalogArray.GetString(i));
                            else
                                catalogBuilder.AppendNull();

                            // TABLE_SCHEM
                            if (schemaArray != null && !schemaArray.IsNull(i))
                                schemaBuilder.Append(schemaArray.GetString(i));
                            else
                                schemaBuilder.AppendNull();

                            // TABLE_NAME
                            if (tableArray != null && !tableArray.IsNull(i))
                                tableBuilder.Append(tableArray.GetString(i));
                            else
                                tableBuilder.AppendNull();

                            // COLUMN_NAME
                            if (!columnArray.IsNull(i))
                                columnBuilder.Append(columnArray.GetString(i));
                            else
                                columnBuilder.AppendNull();

                            // KEQ_SEQ
                            if (sequenceArray != null && !sequenceArray.IsNull(i))
                                sequenceBuilder.Append(sequenceArray.GetValue(i)!.Value);
                            else
                                sequenceBuilder.AppendNull();

                            // PK_NAME (from constraint_name in ADBC stream)
                            if (constraintNameArray != null && !constraintNameArray.IsNull(i))
                                pkNameBuilder.Append(constraintNameArray.GetString(i));
                            else
                                pkNameBuilder.AppendNull();
                        }
                    }
                }
            }

            // Create result with Thrift protocol column names
            var resultSchema = ColumnMetadataSchemas.CreatePrimaryKeySchema();

            var data = new List<IArrowArray>
            {
                catalogBuilder.Build(),
                schemaBuilder.Build(),
                tableBuilder.Build(),
                columnBuilder.Build(),
                sequenceBuilder.Build(),
                pkNameBuilder.Build()
            };

            return new StatementExecInfoArrowStream(resultSchema, data);
        }

        /// <summary>
        /// Gets foreign keys (imported keys) for a specific table using SHOW FOREIGN KEYS SQL command.
        /// </summary>
        /// <param name="catalog">Catalog name. If null, uses session catalog.</param>
        /// <param name="dbSchema">Schema name. If null, uses session schema.</param>
        /// <param name="tableName">Table name (required)</param>
        /// <returns>Arrow stream with foreign key information</returns>
        /// <exception cref="ArgumentNullException">Thrown when tableName is null or empty</exception>
        /// <remarks>
        /// <para>Uses SQL command: SHOW FOREIGN KEYS IN catalog.schema.table</para>
        /// <para>Returns columns (ADBC spec):</para>
        /// <list type="bullet">
        /// <item><description>pk_catalog_name (string, nullable): Referenced table's catalog</description></item>
        /// <item><description>pk_db_schema_name (string, nullable): Referenced table's schema</description></item>
        /// <item><description>pk_table_name (string): Referenced table</description></item>
        /// <item><description>pk_column_name (string): Referenced column</description></item>
        /// <item><description>fk_catalog_name (string, nullable): Foreign key table's catalog</description></item>
        /// <item><description>fk_db_schema_name (string, nullable): Foreign key table's schema</description></item>
        /// <item><description>fk_table_name (string): Foreign key table</description></item>
        /// <item><description>fk_column_name (string): Foreign key column</description></item>
        /// <item><description>key_sequence (int32): Position in key (1-based)</description></item>
        /// <item><description>fk_constraint_name (string, nullable): Constraint name</description></item>
        /// <item><description>pk_key_name (string, nullable): Primary key name</description></item>
        /// <item><description>update_rule (uint8, nullable): ON UPDATE rule</description></item>
        /// <item><description>delete_rule (uint8, nullable): ON DELETE rule</description></item>
        /// </list>
        /// <para>Referential action codes: 0=CASCADE, 1=RESTRICT, 2=SET NULL, 3=NO ACTION, 4=SET DEFAULT</para>
        /// </remarks>
        internal IArrowArrayStream GetImportedKeys(string? catalog, string? dbSchema, string tableName)
        {
            return GetImportedKeysAsync(catalog, dbSchema, tableName).GetAwaiter().GetResult();
        }

        private async Task<IArrowArrayStream> GetImportedKeysAsync(string? catalog, string? dbSchema, string tableName)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                if (string.IsNullOrEmpty(tableName))
                {
                    var ex = new ArgumentNullException(nameof(tableName), "Table name is required");
                    activity?.AddException(ex, [
                        new("error.type", ex.GetType().Name),
                        new("operation", "GetImportedKeys")
                    ]);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    throw ex;
                }

                // Catalog and schema are required for GetImportedKeys
                if (string.IsNullOrEmpty(catalog) || string.IsNullOrEmpty(dbSchema))
                {
                    var ex = new ArgumentException("Catalog and schema are required for GetImportedKeys");
                    activity?.AddException(ex, [
                        new("error.type", ex.GetType().Name),
                        new("operation", "GetImportedKeys"),
                        new("catalog", catalog ?? "(none)"),
                        new("db_schema", dbSchema ?? "(none)"),
                        new("table_name", tableName)
                    ]);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    throw ex;
                }

                activity?.SetTag("catalog", catalog);
                activity?.SetTag("db_schema", dbSchema);
                activity?.SetTag("table_name", tableName);

                // Format: SHOW FOREIGN KEYS IN CATALOG `catalog` IN SCHEMA `schema` IN TABLE `table`
                var commandBuilder = new SqlCommandBuilder()
                    .WithCatalog(catalog)
                    .WithSchema(dbSchema)
                    .WithTable(tableName);

                var sql = commandBuilder.BuildShowForeignKeys();
                activity?.SetTag("sql_query", sql);

                List<RecordBatch> batches;
                try
                {
                    batches = await ExecuteSqlQueryAsync(sql).ConfigureAwait(false);
                    activity?.SetTag("result_batches", batches.Count);
                    activity?.SetTag("total_rows", batches.Sum(b => b.Length));
                }
                catch (Exception ex)
                {
                    activity?.AddException(ex, [
                        new("error.type", ex.GetType().Name),
                        new("operation", "GetImportedKeys"),
                        new("catalog", catalog ?? "(none)"),
                        new("db_schema", dbSchema ?? "(none)"),
                        new("table_name", tableName),
                        new("note", "SHOW FOREIGN KEYS not supported or permission denied")
                    ]);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    // SHOW FOREIGN KEYS not supported (Hive metastore) or permission denied
                    // Return empty result gracefully
                    return BuildImportedKeysResult(new List<ForeignKeyInfo>());
                }

            var foreignKeys = new List<ForeignKeyInfo>();
            var keySequences = new Dictionary<string, int>();

            foreach (var batch in batches)
            {
                // SHOW FOREIGN KEYS returns columns with camelCase names
                // Expected columns: constraintName, parentCatalogName, parentNamespace, parentTableName, parentColName, updateRule, deleteRule, deferrability
                var constraintNameArray = batch.Column("constraintName") as StringArray;
                var fkColNameArray = batch.Column("col_name") as StringArray; // The FK column itself
                var pkCatalogArray = batch.Column("parentCatalogName") as StringArray;
                var pkSchemaArray = batch.Column("parentNamespace") as StringArray;
                var pkTableArray = batch.Column("parentTableName") as StringArray;
                var pkColNameArray = batch.Column("parentColName") as StringArray;
                var updateRuleArray = batch.Column("updateRule") as StringArray;
                var deleteRuleArray = batch.Column("deleteRule") as StringArray;
                var deferrabilityArray = batch.Column("deferrability") as Int32Array;

                if (fkColNameArray == null || pkColNameArray == null)
                    continue;

                for (int i = 0; i < batch.Length; i++)
                {
                    if (!fkColNameArray.IsNull(i) && !pkColNameArray.IsNull(i))
                    {
                        var constraintName = constraintNameArray != null && !constraintNameArray.IsNull(i)
                            ? constraintNameArray.GetString(i)
                            : null;

                        // Track key sequence per constraint
                        if (constraintName != null)
                        {
                            if (!keySequences.ContainsKey(constraintName))
                            {
                                keySequences[constraintName] = 1;
                            }
                        }

                        foreignKeys.Add(new ForeignKeyInfo
                        {
                            PkCatalogName = pkCatalogArray != null && !pkCatalogArray.IsNull(i) ? pkCatalogArray.GetString(i) : null,
                            PkSchemaName = pkSchemaArray != null && !pkSchemaArray.IsNull(i) ? pkSchemaArray.GetString(i) : null,
                            PkTableName = pkTableArray != null && !pkTableArray.IsNull(i) ? pkTableArray.GetString(i) : string.Empty,
                            PkColumnName = pkColNameArray.GetString(i),
                            FkCatalogName = catalog,
                            FkSchemaName = dbSchema,
                            FkTableName = tableName,
                            FkColumnName = fkColNameArray.GetString(i),
                            KeySequence = constraintName != null ? keySequences[constraintName]++ : 1,
                            FkConstraintName = constraintName,
                            UpdateRule = ParseReferentialAction(updateRuleArray != null && !updateRuleArray.IsNull(i) ? updateRuleArray.GetString(i) : null),
                            DeleteRule = ParseReferentialAction(deleteRuleArray != null && !deleteRuleArray.IsNull(i) ? deleteRuleArray.GetString(i) : null),
                            Deferrability = deferrabilityArray != null && !deferrabilityArray.IsNull(i) ? (short?)deferrabilityArray.GetValue(i) : null
                        });
                    }
                }
            }

            activity?.SetTag("result_foreign_keys", foreignKeys.Count);
            activity?.SetTag("unique_constraints", keySequences.Count);
            return BuildImportedKeysResult(foreignKeys);
        });
        }

        private byte? ParseReferentialAction(string? action)
        {
            if (string.IsNullOrEmpty(action))
                return null;

            return action!.ToUpperInvariant() switch
            {
                "CASCADE" => 0,
                "RESTRICT" => 1,
                "SET NULL" => 2,
                "NO ACTION" => 3,
                "SET DEFAULT" => 4,
                _ => null
            };
        }

        private IArrowArrayStream BuildImportedKeysResult(List<ForeignKeyInfo> foreignKeys)
        {
            var pkCatalogBuilder = new StringArray.Builder();
            var pkSchemaBuilder = new StringArray.Builder();
            var pkTableBuilder = new StringArray.Builder();
            var pkColumnBuilder = new StringArray.Builder();
            var fkCatalogBuilder = new StringArray.Builder();
            var fkSchemaBuilder = new StringArray.Builder();
            var fkTableBuilder = new StringArray.Builder();
            var fkColumnBuilder = new StringArray.Builder();
            var sequenceBuilder = new Int32Array.Builder();
            var fkNameBuilder = new StringArray.Builder();
            var pkNameBuilder = new StringArray.Builder();
            var updateRuleBuilder = new UInt8Array.Builder();
            var deleteRuleBuilder = new UInt8Array.Builder();
            var deferrabilityBuilder = new Int32Array.Builder();

            foreach (var fk in foreignKeys)
            {
                _ = fk.PkCatalogName != null ? pkCatalogBuilder.Append(fk.PkCatalogName) : pkCatalogBuilder.AppendNull();
                _ = fk.PkSchemaName != null ? pkSchemaBuilder.Append(fk.PkSchemaName) : pkSchemaBuilder.AppendNull();
                pkTableBuilder.Append(fk.PkTableName);
                pkColumnBuilder.Append(fk.PkColumnName);

                _ = fk.FkCatalogName != null ? fkCatalogBuilder.Append(fk.FkCatalogName) : fkCatalogBuilder.AppendNull();
                _ = fk.FkSchemaName != null ? fkSchemaBuilder.Append(fk.FkSchemaName) : fkSchemaBuilder.AppendNull();
                fkTableBuilder.Append(fk.FkTableName);
                fkColumnBuilder.Append(fk.FkColumnName);

                sequenceBuilder.Append(fk.KeySequence);
                _ = fk.FkConstraintName != null ? fkNameBuilder.Append(fk.FkConstraintName) : fkNameBuilder.AppendNull();
                _ = fk.PkKeyName != null ? pkNameBuilder.Append(fk.PkKeyName) : pkNameBuilder.AppendNull();
                _ = fk.UpdateRule.HasValue ? updateRuleBuilder.Append(fk.UpdateRule.Value) : updateRuleBuilder.AppendNull();
                _ = fk.DeleteRule.HasValue ? deleteRuleBuilder.Append(fk.DeleteRule.Value) : deleteRuleBuilder.AppendNull();
                _ = fk.Deferrability.HasValue ? deferrabilityBuilder.Append((int)fk.Deferrability.Value) : deferrabilityBuilder.AppendNull();
            }

            var resultSchema = new Schema(new[]
            {
                new Field("pk_catalog_name", StringType.Default, nullable: true),
                new Field("pk_db_schema_name", StringType.Default, nullable: true),
                new Field("pk_table_name", StringType.Default, nullable: false),
                new Field("pk_column_name", StringType.Default, nullable: false),
                new Field("fk_catalog_name", StringType.Default, nullable: true),
                new Field("fk_db_schema_name", StringType.Default, nullable: true),
                new Field("fk_table_name", StringType.Default, nullable: false),
                new Field("fk_column_name", StringType.Default, nullable: false),
                new Field("key_sequence", Int32Type.Default, nullable: false),
                new Field("fk_constraint_name", StringType.Default, nullable: true),
                new Field("pk_key_name", StringType.Default, nullable: true),
                new Field("update_rule", UInt8Type.Default, nullable: true),
                new Field("delete_rule", UInt8Type.Default, nullable: true),
                new Field("deferrability", Int32Type.Default, nullable: true)
            }, null);

            var data = new List<IArrowArray>
            {
                pkCatalogBuilder.Build(),
                pkSchemaBuilder.Build(),
                pkTableBuilder.Build(),
                pkColumnBuilder.Build(),
                fkCatalogBuilder.Build(),
                fkSchemaBuilder.Build(),
                fkTableBuilder.Build(),
                fkColumnBuilder.Build(),
                sequenceBuilder.Build(),
                fkNameBuilder.Build(),
                pkNameBuilder.Build(),
                updateRuleBuilder.Build(),
                deleteRuleBuilder.Build(),
                deferrabilityBuilder.Build()
            };

            return new StatementExecInfoArrowStream(resultSchema, data);
        }

        /// <summary>
        /// Gets cross-reference (foreign key) information with Thrift/HiveServer2 column naming for statement-based queries.
        /// Returns 14 columns matching JDBC DatabaseMetaData.getCrossReference() including DEFERRABILITY.
        /// </summary>
        /// <param name="pkCatalog">Primary key catalog name</param>
        /// <param name="pkSchema">Primary key schema name</param>
        /// <param name="pkTable">Primary key table name</param>
        /// <param name="fkCatalog">Foreign key catalog name</param>
        /// <param name="fkSchema">Foreign key schema name</param>
        /// <param name="fkTable">Foreign key table name (required)</param>
        /// <returns>Arrow stream with JDBC-style uppercase column names (14 columns)</returns>
        internal IArrowArrayStream GetCrossReferenceFlat(
            string? pkCatalog,
            string? pkSchema,
            string? pkTable,
            string? fkCatalog,
            string? fkSchema,
            string? fkTable)
        {
            // Reuse existing method to get data
            var stream = GetCrossReferenceAsync(pkCatalog, pkSchema, pkTable, fkCatalog, fkSchema, fkTable)
                .GetAwaiter().GetResult();

            // Read data from ADBC-style stream
            var pkCatalogBuilder = new StringArray.Builder();
            var pkSchemaBuilder = new StringArray.Builder();
            var pkTableBuilder = new StringArray.Builder();
            var pkColumnBuilder = new StringArray.Builder();
            var fkCatalogBuilder = new StringArray.Builder();
            var fkSchemaBuilder = new StringArray.Builder();
            var fkTableBuilder = new StringArray.Builder();
            var fkColumnBuilder = new StringArray.Builder();
            var keySeqBuilder = new Int32Array.Builder();
            var updateRuleBuilder = new Int32Array.Builder();
            var deleteRuleBuilder = new Int32Array.Builder();
            var fkNameBuilder = new StringArray.Builder();
            var pkNameBuilder = new StringArray.Builder();
            var deferrabilityBuilder = new Int32Array.Builder();

            using (var reader = stream)
            {
                RecordBatch? batch;
                while ((batch = reader.ReadNextRecordBatchAsync().GetAwaiter().GetResult()) != null)
                {
                    // Extract columns from ADBC schema
                    var pkCatalogArray = batch.Column("pk_catalog_name") as StringArray;
                    var pkSchemaArray = batch.Column("pk_db_schema_name") as StringArray;
                    var pkTableArray = batch.Column("pk_table_name") as StringArray;
                    var pkColumnArray = batch.Column("pk_column_name") as StringArray;
                    var fkCatalogArray = batch.Column("fk_catalog_name") as StringArray;
                    var fkSchemaArray = batch.Column("fk_db_schema_name") as StringArray;
                    var fkTableArray = batch.Column("fk_table_name") as StringArray;
                    var fkColumnArray = batch.Column("fk_column_name") as StringArray;
                    var keySeqArray = batch.Column("key_sequence") as Int32Array;
                    var updateRuleArray = batch.Column("update_rule") as UInt8Array;
                    var deleteRuleArray = batch.Column("delete_rule") as UInt8Array;
                    var fkNameArray = batch.Column("fk_constraint_name") as StringArray;
                    var pkNameArray = batch.Column("pk_key_name") as StringArray;
                    var deferrabilityArray = batch.Column("deferrability") as Int32Array;

                    if (fkColumnArray != null)
                    {
                        for (int i = 0; i < fkColumnArray.Length; i++)
                        {
                            // PKTABLE_CAT
                            _ = pkCatalogArray != null && !pkCatalogArray.IsNull(i) ? pkCatalogBuilder.Append(pkCatalogArray.GetString(i)) : pkCatalogBuilder.AppendNull();

                            // PKTABLE_SCHEM
                            _ = pkSchemaArray != null && !pkSchemaArray.IsNull(i) ? pkSchemaBuilder.Append(pkSchemaArray.GetString(i)) : pkSchemaBuilder.AppendNull();

                            // PKTABLE_NAME
                            _ = pkTableArray != null && !pkTableArray.IsNull(i) ? pkTableBuilder.Append(pkTableArray.GetString(i)) : pkTableBuilder.AppendNull();

                            // PKCOLUMN_NAME
                            _ = pkColumnArray != null && !pkColumnArray.IsNull(i) ? pkColumnBuilder.Append(pkColumnArray.GetString(i)) : pkColumnBuilder.AppendNull();

                            // FKTABLE_CAT
                            _ = fkCatalogArray != null && !fkCatalogArray.IsNull(i) ? fkCatalogBuilder.Append(fkCatalogArray.GetString(i)) : fkCatalogBuilder.AppendNull();

                            // FKTABLE_SCHEM
                            _ = fkSchemaArray != null && !fkSchemaArray.IsNull(i) ? fkSchemaBuilder.Append(fkSchemaArray.GetString(i)) : fkSchemaBuilder.AppendNull();

                            // FKTABLE_NAME
                            _ = fkTableArray != null && !fkTableArray.IsNull(i) ? fkTableBuilder.Append(fkTableArray.GetString(i)) : fkTableBuilder.AppendNull();

                            // FKCOLUMN_NAME
                            _ = !fkColumnArray.IsNull(i) ? fkColumnBuilder.Append(fkColumnArray.GetString(i)) : fkColumnBuilder.AppendNull();

                            // KEQ_SEQ
                            _ = keySeqArray != null && !keySeqArray.IsNull(i) ? keySeqBuilder.Append(keySeqArray.GetValue(i)!.Value) : keySeqBuilder.AppendNull();

                            // UPDATE_RULE (convert from UInt8 to Int32 to match Thrift)
                            _ = updateRuleArray != null && !updateRuleArray.IsNull(i) ? updateRuleBuilder.Append((int)updateRuleArray.GetValue(i)!.Value) : updateRuleBuilder.AppendNull();

                            // DELETE_RULE (convert from UInt8 to Int32 to match Thrift)
                            _ = deleteRuleArray != null && !deleteRuleArray.IsNull(i) ? deleteRuleBuilder.Append((int)deleteRuleArray.GetValue(i)!.Value) : deleteRuleBuilder.AppendNull();

                            // FK_NAME
                            _ = fkNameArray != null && !fkNameArray.IsNull(i) ? fkNameBuilder.Append(fkNameArray.GetString(i)) : fkNameBuilder.AppendNull();

                            // PK_NAME
                            _ = pkNameArray != null && !pkNameArray.IsNull(i) ? pkNameBuilder.Append(pkNameArray.GetString(i)) : pkNameBuilder.AppendNull();

                            // DEFERRABILITY (already Int32 to match Thrift)
                            _ = deferrabilityArray != null && !deferrabilityArray.IsNull(i) ? deferrabilityBuilder.Append(deferrabilityArray.GetValue(i)!.Value) : deferrabilityBuilder.AppendNull();
                        }
                    }
                }
            }

            // Create result with Thrift protocol column names (14 columns matching JDBC spec)
            var resultSchema = ColumnMetadataSchemas.CreateForeignKeySchema();

            var data = new List<IArrowArray>
            {
                pkCatalogBuilder.Build(),
                pkSchemaBuilder.Build(),
                pkTableBuilder.Build(),
                pkColumnBuilder.Build(),
                fkCatalogBuilder.Build(),
                fkSchemaBuilder.Build(),
                fkTableBuilder.Build(),
                fkColumnBuilder.Build(),
                keySeqBuilder.Build(),
                updateRuleBuilder.Build(),
                deleteRuleBuilder.Build(),
                fkNameBuilder.Build(),
                pkNameBuilder.Build(),
                deferrabilityBuilder.Build()
            };

            return new StatementExecInfoArrowStream(resultSchema, data);
        }

        private async Task<IArrowArrayStream> GetCrossReferenceAsync(
            string? pkCatalog,
            string? pkSchema,
            string? pkTable,
            string? fkCatalog,
            string? fkSchema,
            string? fkTable)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                if (string.IsNullOrEmpty(fkTable))
                {
                    var ex = new ArgumentNullException(nameof(fkTable), "Foreign table name is required for GetCrossReference");
                    activity?.AddException(ex, [
                        new("error.type", ex.GetType().Name),
                        new("operation", "GetCrossReference")
                    ]);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    throw ex;
                }

                // Catalog and schema are required for foreign table
                if (string.IsNullOrEmpty(fkCatalog) || string.IsNullOrEmpty(fkSchema))
                {
                    var ex = new ArgumentException("Foreign table catalog and schema are required for GetCrossReference");
                    activity?.AddException(ex, [
                        new("error.type", ex.GetType().Name),
                        new("operation", "GetCrossReference"),
                        new("fk_catalog", fkCatalog ?? "(none)"),
                        new("fk_schema", fkSchema ?? "(none)"),
                        new("fk_table", fkTable)
                    ]);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    throw ex;
                }

                activity?.SetTag("pk_catalog", pkCatalog ?? "(none)");
                activity?.SetTag("pk_schema", pkSchema ?? "(none)");
                activity?.SetTag("pk_table", pkTable ?? "(none)");
                activity?.SetTag("fk_catalog", fkCatalog);
                activity?.SetTag("fk_schema", fkSchema);
                activity?.SetTag("fk_table", fkTable);

                // Execute SHOW FOREIGN KEYS for the CHILD (foreign) table
                // This returns all foreign keys from the child table
                var allForeignKeys = await GetImportedKeysAsync(fkCatalog, fkSchema, fkTable!).ConfigureAwait(false);

                // Filter results by parent table parameters (if specified)
                // If parent parameters are null, return all foreign keys from the child table
                bool needsFiltering = !string.IsNullOrEmpty(pkCatalog) || !string.IsNullOrEmpty(pkSchema) || !string.IsNullOrEmpty(pkTable);
                activity?.SetTag("needs_filtering", needsFiltering);

                if (!needsFiltering)
                {
                    // No parent filter specified - return all foreign keys
                    activity?.SetTag("note", "No parent filter - returning all foreign keys");
                    return allForeignKeys;
                }

            // Need to filter by parent table
            // Read all foreign keys from the stream
            var foreignKeysList = new List<ForeignKeyInfo>();
            var schema = allForeignKeys.Schema;

            RecordBatch? batch;
            while ((batch = await allForeignKeys.ReadNextRecordBatchAsync().ConfigureAwait(false)) != null)
            {
                using (batch)
                {
                    // Read the foreign key data from the batch
                var pkCatalogArray = batch.Column("pk_catalog_name") as StringArray;
                var pkSchemaArray = batch.Column("pk_db_schema_name") as StringArray;
                var pkTableArray = batch.Column("pk_table_name") as StringArray;
                var pkColumnArray = batch.Column("pk_column_name") as StringArray;
                var fkCatalogArray = batch.Column("fk_catalog_name") as StringArray;
                var fkSchemaArray = batch.Column("fk_db_schema_name") as StringArray;
                var fkTableArray = batch.Column("fk_table_name") as StringArray;
                var fkColumnArray = batch.Column("fk_column_name") as StringArray;
                var sequenceArray = batch.Column("key_sequence") as Int32Array;
                var fkNameArray = batch.Column("fk_constraint_name") as StringArray;
                var pkNameArray = batch.Column("pk_key_name") as StringArray;
                var updateRuleArray = batch.Column("update_rule") as UInt8Array;
                var deleteRuleArray = batch.Column("delete_rule") as UInt8Array;
                var deferrabilityArray = batch.Column("deferrability") as Int16Array;

                for (int i = 0; i < batch.Length; i++)
                {
                    // Extract values
                    var rowPkCatalog = pkCatalogArray != null && !pkCatalogArray.IsNull(i) ? pkCatalogArray.GetString(i) : null;
                    var rowPkSchema = pkSchemaArray != null && !pkSchemaArray.IsNull(i) ? pkSchemaArray.GetString(i) : null;
                    var rowPkTable = pkTableArray != null && !pkTableArray.IsNull(i) ? pkTableArray.GetString(i) : string.Empty;

                    // Apply parent table filter
                    bool catalogMatches = string.IsNullOrEmpty(pkCatalog) || string.Equals(rowPkCatalog, pkCatalog, StringComparison.OrdinalIgnoreCase);
                    bool schemaMatches = string.IsNullOrEmpty(pkSchema) || string.Equals(rowPkSchema, pkSchema, StringComparison.OrdinalIgnoreCase);
                    bool tableMatches = string.IsNullOrEmpty(pkTable) || string.Equals(rowPkTable, pkTable, StringComparison.OrdinalIgnoreCase);

                    if (catalogMatches && schemaMatches && tableMatches)
                    {
                        // This row matches the parent table filter - include it
                        foreignKeysList.Add(new ForeignKeyInfo
                        {
                            PkCatalogName = rowPkCatalog,
                            PkSchemaName = rowPkSchema,
                            PkTableName = rowPkTable,
                            PkColumnName = pkColumnArray != null && !pkColumnArray.IsNull(i) ? pkColumnArray.GetString(i) : string.Empty,
                            FkCatalogName = fkCatalogArray != null && !fkCatalogArray.IsNull(i) ? fkCatalogArray.GetString(i) : null,
                            FkSchemaName = fkSchemaArray != null && !fkSchemaArray.IsNull(i) ? fkSchemaArray.GetString(i) : null,
                            FkTableName = fkTableArray != null && !fkTableArray.IsNull(i) ? fkTableArray.GetString(i) : string.Empty,
                            FkColumnName = fkColumnArray != null && !fkColumnArray.IsNull(i) ? fkColumnArray.GetString(i) : string.Empty,
                            KeySequence = sequenceArray != null && !sequenceArray.IsNull(i) ? sequenceArray.GetValue(i).GetValueOrDefault(1) : 1,
                            FkConstraintName = fkNameArray != null && !fkNameArray.IsNull(i) ? fkNameArray.GetString(i) : null,
                            PkKeyName = pkNameArray != null && !pkNameArray.IsNull(i) ? pkNameArray.GetString(i) : null,
                            UpdateRule = updateRuleArray != null && !updateRuleArray.IsNull(i) ? updateRuleArray.GetValue(i) : null,
                            DeleteRule = deleteRuleArray != null && !deleteRuleArray.IsNull(i) ? deleteRuleArray.GetValue(i) : null,
                            Deferrability = deferrabilityArray != null && !deferrabilityArray.IsNull(i) ? deferrabilityArray.GetValue(i) : null
                        });
                    }
                }
                }
            }

            // Build and return filtered result
            activity?.SetTag("filtered_foreign_keys", foreignKeysList.Count);
            activity?.SetTag("note", "Applied parent table filter");
            return BuildImportedKeysResult(foreignKeysList);
        });
        }


        private struct PrimaryKeyInfo
        {
            public string? CatalogName { get; set; }
            public string? SchemaName { get; set; }
            public string TableName { get; set; }
            public string ColumnName { get; set; }
            public int KeySequence { get; set; }
            public string? ConstraintName { get; set; }
        }

        private struct ForeignKeyInfo
        {
            public string? PkCatalogName { get; set; }
            public string? PkSchemaName { get; set; }
            public string PkTableName { get; set; }
            public string PkColumnName { get; set; }
            public string? FkCatalogName { get; set; }
            public string? FkSchemaName { get; set; }
            public string FkTableName { get; set; }
            public string FkColumnName { get; set; }
            public int KeySequence { get; set; }
            public string? FkConstraintName { get; set; }
            public string? PkKeyName { get; set; }
            public byte? UpdateRule { get; set; }
            public byte? DeleteRule { get; set; }
            public short? Deferrability { get; set; }
        }

        /// <summary>
        /// Disposes the connection and deletes the session if it exists.
        /// </summary>
        public override void Dispose()
        {
            this.TraceActivity(activity =>
            {
                activity?.SetTag("session_id", _sessionId);
                activity?.SetTag("warehouse_id", _warehouseId);

                if (_sessionId != null)
                {
                    try
                    {
                        activity?.AddEvent(new System.Diagnostics.ActivityEvent("session.delete.start"));
                        // Delete session synchronously during dispose
                        _client.DeleteSessionAsync(_sessionId, _warehouseId, CancellationToken.None).GetAwaiter().GetResult();
                        activity?.AddEvent(new System.Diagnostics.ActivityEvent("session.delete.success"));
                    }
                    catch (Exception ex)
                    {
                        // Best effort - ignore errors during dispose but trace them
                        activity?.AddEvent(new System.Diagnostics.ActivityEvent("session.delete.error",
                            tags: new System.Diagnostics.ActivityTagsCollection { { "error", ex.Message } }));
                    }
                    finally
                    {
                        _sessionId = null;
                    }
                }

                // Dispose the HTTP client if we own it
                if (_ownsHttpClient)
                {
                    _httpClient.Dispose();
                }

                // Dispose the CloudFetch HTTP client (we always own it)
                _cloudFetchHttpClient.Dispose();

                // Dispose the auth HTTP client if it was created
                _authHttpClient?.Dispose();

                _sessionLock.Dispose();

                // Dispose the trace listener
                _fileActivityListener?.Dispose();
            });

            // Call base class Dispose
            base.Dispose();
        }

        // TracingConnection provides IActivityTracer implementation
        public override string AssemblyVersion => GetType().Assembly.GetName().Version?.ToString() ?? "1.0.0";
        public override string AssemblyName => "AdbcDrivers.Databricks";
    }
}
