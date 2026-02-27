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
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Http;
using AdbcDrivers.HiveServer2.Hive2;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tracing;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using static Apache.Arrow.Adbc.AdbcConnection;

namespace AdbcDrivers.Databricks.StatementExecution
{
    /// <summary>
    /// Connection implementation using the Databricks Statement Execution REST API.
    /// Manages session lifecycle and creates statements for query execution.
    /// Extends TracingConnection for consistent tracing support with Thrift protocol.
    /// </summary>
    internal class StatementExecutionConnection : TracingConnection, IGetObjectsDataProvider
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
        private readonly string? _identityFederationClientId;

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
            // Note: We still need proxy and TLS configuration for corporate network access
            _cloudFetchHttpClient = HttpClientFactory.CreateCloudFetchHttpClient(properties);

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
                BaseHandler = HttpClientFactory.CreateHandler(properties),
                BaseAuthHandler = HttpClientFactory.CreateHandler(properties),
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

            var httpClient = new HttpClient(result)
            {
                Timeout = TimeSpan.FromMinutes(timeoutMinutes)
            };

            // Set user agent
            string userAgent = GetUserAgent(properties);
            httpClient.DefaultRequestHeaders.UserAgent.ParseAdd(userAgent);

            return httpClient;
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

        public override IArrowArrayStream GetObjects(GetObjectsDepth depth, string? catalogPattern, string? schemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
        {
            return GetObjectsResultBuilder.BuildGetObjectsResult(
                this, depth, catalogPattern, schemaPattern,
                tableNamePattern, tableTypes, columnNamePattern);
        }

        public override IArrowArrayStream GetTableTypes()
        {
            var builder = new StringArray.Builder();
            builder.Append("TABLE");
            builder.Append("VIEW");
            var schema = new Schema(new[] { new Field("table_type", StringType.Default, false) }, null);
            return new HiveInfoArrowStream(schema, new IArrowArray[] { builder.Build() });
        }

        public override Schema GetTableSchema(string? catalog, string? dbSchema, string tableName)
        {
            string sql = new ShowColumnsCommand(
                catalog ?? _catalog, dbSchema, tableName).Build();
            var batches = ExecuteMetadataSql(sql);

            var fields = new List<Field>();
            foreach (var batch in batches)
            {
                var colNameArray = TryGetColumn<StringArray>(batch, "col_name");
                var columnTypeArray = TryGetColumn<StringArray>(batch, "columnType");
                var isNullableArray = TryGetColumn<StringArray>(batch, "isNullable");

                if (colNameArray == null || columnTypeArray == null) continue;

                for (int i = 0; i < batch.Length; i++)
                {
                    if (colNameArray.IsNull(i) || columnTypeArray.IsNull(i)) continue;

                    string colName = colNameArray.GetString(i);
                    string colType = columnTypeArray.GetString(i);
                    bool nullable = isNullableArray == null || isNullableArray.IsNull(i) ||
                        !isNullableArray.GetString(i).Equals("false", StringComparison.OrdinalIgnoreCase);

                    IArrowType arrowType = ColumnMetadataHelper.GetArrowType(colType);
                    fields.Add(new Field(colName, arrowType, nullable));
                }
            }
            return new Schema(fields, null);
        }

        // IGetObjectsDataProvider implementation

        IReadOnlyList<string> IGetObjectsDataProvider.GetCatalogs(string? catalogPattern)
        {
            string sql = new ShowCatalogsCommand(catalogPattern).Build();
            var batches = ExecuteMetadataSql(sql);
            var result = new List<string>();
            foreach (var batch in batches)
            {
                var catalogArray = TryGetColumn<StringArray>(batch, "catalog");
                if (catalogArray == null) continue;
                for (int i = 0; i < batch.Length; i++)
                {
                    if (!catalogArray.IsNull(i))
                        result.Add(catalogArray.GetString(i));
                }
            }
            return result;
        }

        IReadOnlyList<(string catalog, string schema)> IGetObjectsDataProvider.GetSchemas(string? catalogPattern, string? schemaPattern)
        {
            string sql = new ShowSchemasCommand(catalogPattern, schemaPattern).Build();
            var batches = ExecuteMetadataSql(sql);
            var result = new List<(string, string)>();
            foreach (var batch in batches)
            {
                var schemaArray = TryGetColumn<StringArray>(batch, "databaseName");
                if (schemaArray == null) continue;

                // SHOW SCHEMAS IN ALL CATALOGS has catalog_name column;
                // SHOW SCHEMAS IN `catalog` does not — use the catalogPattern as the catalog.
                var catalogArray = TryGetColumn<StringArray>(batch, "catalog_name");

                for (int i = 0; i < batch.Length; i++)
                {
                    if (schemaArray.IsNull(i)) continue;
                    string catalog = catalogArray != null && !catalogArray.IsNull(i)
                        ? catalogArray.GetString(i)
                        : catalogPattern ?? "";
                    result.Add((catalog, schemaArray.GetString(i)));
                }
            }
            return result;
        }

        IReadOnlyList<(string catalog, string schema, string table, string tableType)> IGetObjectsDataProvider.GetTables(
            string? catalogPattern, string? schemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes)
        {
            string sql = new ShowTablesCommand(catalogPattern, schemaPattern, tableNamePattern).Build();
            var batches = ExecuteMetadataSql(sql);
            var result = new List<(string, string, string, string)>();
            foreach (var batch in batches)
            {
                var catalogArray = TryGetColumn<StringArray>(batch, "catalogName");
                var schemaArray = TryGetColumn<StringArray>(batch, "namespace");
                var tableArray = TryGetColumn<StringArray>(batch, "tableName");
                var tableTypeArray = TryGetColumn<StringArray>(batch, "tableType");

                if (catalogArray == null || schemaArray == null || tableArray == null) continue;

                for (int i = 0; i < batch.Length; i++)
                {
                    if (catalogArray.IsNull(i) || schemaArray.IsNull(i) || tableArray.IsNull(i)) continue;

                    string tableType = "TABLE";
                    if (tableTypeArray != null && !tableTypeArray.IsNull(i))
                    {
                        string serverType = tableTypeArray.GetString(i);
                        if (!string.IsNullOrEmpty(serverType))
                            tableType = serverType;
                    }

                    if (tableTypes != null && tableTypes.Count > 0 && !tableTypes.Contains(tableType))
                        continue;

                    result.Add((catalogArray.GetString(i), schemaArray.GetString(i),
                        tableArray.GetString(i), tableType));
                }
            }
            return result;
        }

        void IGetObjectsDataProvider.PopulateColumnInfo(string? catalogPattern, string? schemaPattern,
            string? tablePattern, string? columnPattern,
            Dictionary<string, Dictionary<string, Dictionary<string, TableInfo>>> catalogMap)
        {
            string sql = new ShowColumnsCommand(catalogPattern, schemaPattern, tablePattern, columnPattern).Build();
            var batches = ExecuteMetadataSql(sql);

            var tablePositions = new Dictionary<string, int>();

            foreach (var batch in batches)
            {
                var catalogArray = TryGetColumn<StringArray>(batch, "catalogName");
                var schemaArray = TryGetColumn<StringArray>(batch, "namespace");
                var tableNameArray = TryGetColumn<StringArray>(batch, "tableName");
                var colNameArray = TryGetColumn<StringArray>(batch, "col_name");
                var columnTypeArray = TryGetColumn<StringArray>(batch, "columnType");
                var isNullableArray = TryGetColumn<StringArray>(batch, "isNullable");

                if (catalogArray == null || schemaArray == null || tableNameArray == null ||
                    colNameArray == null || columnTypeArray == null) continue;

                for (int i = 0; i < batch.Length; i++)
                {
                    if (catalogArray.IsNull(i) || schemaArray.IsNull(i) || tableNameArray.IsNull(i) ||
                        colNameArray.IsNull(i) || columnTypeArray.IsNull(i)) continue;

                    string cat = catalogArray.GetString(i);
                    string sch = schemaArray.GetString(i);
                    string tbl = tableNameArray.GetString(i);
                    string colName = colNameArray.GetString(i);
                    string colType = columnTypeArray.GetString(i);

                    if (string.IsNullOrEmpty(colName)) continue;

                    string tableKey = $"{cat}.{sch}.{tbl}";
                    if (!tablePositions.ContainsKey(tableKey))
                        tablePositions[tableKey] = 1;
                    int position = tablePositions[tableKey]++;

                    bool nullable = isNullableArray == null || isNullableArray.IsNull(i) ||
                        !isNullableArray.GetString(i).Equals("false", StringComparison.OrdinalIgnoreCase);

                    if (catalogMap.TryGetValue(cat, out var schemaMap)
                        && schemaMap.TryGetValue(sch, out var tableMap)
                        && tableMap.TryGetValue(tbl, out var tableInfo))
                    {
                        ColumnMetadataHelper.PopulateTableInfoFromTypeName(
                            tableInfo, colName, colType, position, nullable);
                    }
                }
            }
        }

        private static T? TryGetColumn<T>(RecordBatch batch, string name) where T : class, IArrowArray
        {
            try
            {
                return batch.Column(name) as T;
            }
            catch (ArgumentOutOfRangeException)
            {
                return null;
            }
        }

        internal List<RecordBatch> ExecuteMetadataSql(string sql)
        {
            var batches = new List<RecordBatch>();
            using var stmt = CreateStatement();
            stmt.SqlQuery = sql;
            var result = stmt.ExecuteQuery();
            using var stream = result.Stream;
            if (stream == null) return batches;
            while (true)
            {
                var batch = stream.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult();
                if (batch == null) break;
                batches.Add(batch);
            }
            return batches;
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

                _sessionLock.Dispose();
            });
        }

        // TracingConnection provides IActivityTracer implementation
        public override string AssemblyVersion => GetType().Assembly.GetName().Version?.ToString() ?? "1.0.0";
        public override string AssemblyName => "AdbcDrivers.Databricks";
    }
}
