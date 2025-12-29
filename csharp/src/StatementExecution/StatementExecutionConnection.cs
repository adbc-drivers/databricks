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
using AdbcDrivers.Databricks.Metadata;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Extensions;
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
        /// Uses SHOW CATALOGS, SHOW SCHEMAS, SHOW TABLES SQL commands for metadata retrieval.
        /// </summary>
        /// <param name="depth">The depth of metadata to retrieve (Catalogs, DbSchemas, Tables, or All)</param>
        /// <param name="catalogPattern">Pattern to filter catalog names (supports SQL LIKE wildcards: %, _). Null means all catalogs.</param>
        /// <param name="schemaPattern">Pattern to filter schema names (supports SQL LIKE wildcards: %, _). Null means all schemas.</param>
        /// <param name="tableNamePattern">Pattern to filter table names (supports SQL LIKE wildcards: %, _). Null means all tables.</param>
        /// <param name="tableTypes">List of table types to include (e.g., "TABLE", "VIEW", "LOCAL TEMPORARY"). Null means all types.</param>
        /// <param name="columnNamePattern">Pattern to filter column names (supports SQL LIKE wildcards: %, _). Null means all columns. Only used with All depth.</param>
        /// <returns>Arrow stream containing metadata records with schema dependent on depth parameter</returns>
        /// <remarks>
        /// <para>Returned schema depends on depth:</para>
        /// <list type="bullet">
        /// <item><description>Catalogs: Returns (catalog_name)</description></item>
        /// <item><description>DbSchemas: Returns (catalog_name, db_schema_name)</description></item>
        /// <item><description>Tables: Returns (catalog_name, db_schema_name, table_name, table_type)</description></item>
        /// <item><description>All: Returns simplified flat structure (full nested structure TODO)</description></item>
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
                // Add tracing tags for input parameters
                activity?.SetTag("depth", depth.ToString());
                activity?.SetTag("catalog_pattern", catalogPattern ?? "(all)");
                activity?.SetTag("schema_pattern", schemaPattern ?? "(all)");
                activity?.SetTag("table_pattern", tableNamePattern ?? "(all)");
                activity?.SetTag("column_pattern", columnNamePattern ?? "(all)");
                activity?.SetTag("table_types", tableTypes != null ? string.Join(",", tableTypes) : "(all)");

                // Per ADBC spec: catalogPattern is a catalog NAME (exact match, not a pattern), or null for all catalogs
                // Only schemaPattern, tableNamePattern, and columnNamePattern support LIKE pattern matching
                // The server-side SHOW commands handle pattern matching - no client-side iteration needed

                // Single unified structure: catalog → schema → table → (tableType, columns)
                // This matches the Thrift HiveServer2 pattern for consistency
                var catalogMap = new Dictionary<string, Dictionary<string, Dictionary<string, (string tableType, List<ColumnInfo> columns)>>>();

                // Fetch catalogs
                if (depth == GetObjectsDepth.All || depth >= GetObjectsDepth.Catalogs)
                {
                    // GetCatalogsAsync will return empty list if catalog doesn't exist
                    var catalogs = await GetCatalogsAsync(catalogPattern).ConfigureAwait(false);
                    activity?.SetTag("fetched_catalogs", catalogs.Count);

                    // Handle servers that don't support 'catalog' in the namespace
                    // (matches Thrift HiveServer2 behavior)
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
        /// </summary>
        private async Task<List<string>> GetCatalogsAsync(string? catalogPattern)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.SetTag("catalog_pattern", catalogPattern ?? "null");

                // Use SqlCommandBuilder for efficient SQL generation
                var commandBuilder = new SqlCommandBuilder(QuoteIdentifier, EscapeSqlPattern)
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
                        new("operation", "GetCatalogs"),
                        new("catalog_pattern", catalogPattern ?? "(all)")
                    ]);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    // Return empty list on permission denied or other errors
                    // This allows BI tools to show "Access Denied" instead of crashing
                    return new List<string>();
                }

                var catalogs = new List<string>();

                foreach (var batch in batches)
                {
                    var catalogArray = batch.Column("catalog") as StringArray;
                    if (catalogArray == null) continue;

                    for (int i = 0; i < batch.Length; i++)
                    {
                        if (!catalogArray.IsNull(i))
                        {
                            var catalog = catalogArray.GetString(i);
                            // Pattern matching is already done by SQL LIKE clause, no need to check again
                            catalogs.Add(catalog);
                        }
                    }
                }

                activity?.SetTag("result_count", catalogs.Count);
                return catalogs;
            });
        }

        /// <summary>
        /// Gets list of schemas matching the optional pattern.
        /// </summary>
        private async Task<List<(string catalog, string schema)>> GetSchemasAsync(string? catalog, string? schemaPattern)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.SetTag("catalog", catalog ?? "(all)");
                activity?.SetTag("schema_pattern", schemaPattern ?? "(all)");

                // Use SqlCommandBuilder for efficient SQL generation
                var commandBuilder = new SqlCommandBuilder(QuoteIdentifier, EscapeSqlPattern)
                    .WithCatalog(catalog)
                    .WithSchemaPattern(schemaPattern);

                var sql = commandBuilder.BuildShowSchemas();
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
                    activity?.SetTag("error", ex.Message);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    // Return empty list on permission denied or other errors
                    // This allows BI tools to show "Access Denied"
                    return new List<(string, string)>();
                }

                var schemas = new List<(string catalog, string schema)>();

                foreach (var batch in batches)
                {
                    // When using "IN ALL CATALOGS", results include catalog column
                    StringArray? catalogArray = null;
                    if (string.IsNullOrEmpty(catalog))
                    {
                        catalogArray = batch.Column("catalog") as StringArray;
                    }

                    // Result columns are typically 'databaseName' or 'namespace' depending on DBR version
                    StringArray? schemaArray = batch.Column("databaseName") as StringArray;
                    if (schemaArray == null)
                    {
                        schemaArray = batch.Column("namespace") as StringArray;
                    }
                    if (schemaArray == null) continue;

                    for (int i = 0; i < batch.Length; i++)
                    {
                        if (!schemaArray.IsNull(i))
                        {
                            var schemaName = schemaArray.GetString(i);
                            // Pattern matching is already done by SQL LIKE clause, no need to check again

                            string catalogName;
                            if (string.IsNullOrEmpty(catalog))
                            {
                                // Get catalog from result when using "IN ALL CATALOGS"
                                if (catalogArray == null || catalogArray.IsNull(i))
                                    continue;
                                catalogName = catalogArray.GetString(i);
                            }
                            else
                            {
                                catalogName = catalog;
                            }

                            schemas.Add((catalogName, schemaName));
                        }
                    }
                }

                activity?.SetTag("result_count", schemas.Count);
                return schemas;
            });
        }

        /// <summary>
        /// Gets list of tables matching the optional patterns.
        /// </summary>
        private async Task<List<(string catalog, string schema, string tableName, string tableType, string remarks)>> GetTablesAsync(
            string? catalog, string? schema, string? tableNamePattern, IReadOnlyList<string>? tableTypes)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.SetTag("catalog", catalog ?? "(all)");
                activity?.SetTag("schema", schema ?? "(all)");
                activity?.SetTag("table_pattern", tableNamePattern ?? "(all)");
                activity?.SetTag("table_types", tableTypes != null ? string.Join(",", tableTypes) : "(all)");

                // Use SqlCommandBuilder for efficient SQL generation
                var commandBuilder = new SqlCommandBuilder(QuoteIdentifier, EscapeSqlPattern)
                    .WithCatalog(catalog)
                    .WithSchema(schema)
                    // Don't set schema pattern when we have an exact schema - this would create invalid SQL
                    .WithTablePattern(tableNamePattern);

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
                    activity?.SetTag("error", ex.Message);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    // Return empty list on permission denied or other errors
                    // This allows BI tools to show "Access Denied"
                    return new List<(string, string, string, string, string)>();
                }

                var tables = new List<(string catalog, string schema, string tableName, string tableType, string remarks)>();

                foreach (var batch in batches)
                {
                    // When using "IN ALL CATALOGS", results include catalog and schema columns
                    StringArray? catalogArray = null;
                    StringArray? schemaArray = null;
                    if (string.IsNullOrEmpty(catalog))
                    {
                        catalogArray = batch.Column("catalog") as StringArray;
                        schemaArray = batch.Column("database") as StringArray;
                        if (schemaArray == null) schemaArray = batch.Column("databaseName") as StringArray;
                    }

                    var tableNameArray = batch.Column("tableName") as StringArray;
                    var isTemporaryArray = batch.Column("isTemporary") as BooleanArray;
                    var remarksArray = batch.Column("remarks") as StringArray;

                    if (tableNameArray == null) continue;

                    for (int i = 0; i < batch.Length; i++)
                    {
                        if (tableNameArray.IsNull(i))
                            continue;

                        var tableName = tableNameArray.GetString(i);

                        // Get catalog and schema from results when using "IN ALL CATALOGS"
                        string catalogName;
                        string schemaName;
                        if (string.IsNullOrEmpty(catalog))
                        {
                            if (catalogArray == null || catalogArray.IsNull(i))
                                continue;
                            catalogName = catalogArray.GetString(i);

                            if (schemaArray == null || schemaArray.IsNull(i))
                                continue;
                            schemaName = schemaArray.GetString(i);
                        }
                        else
                        {
                            catalogName = catalog;
                            schemaName = schema ?? "";
                        }

                        // Determine table type
                        string tableType = "TABLE";
                        if (isTemporaryArray != null && !isTemporaryArray.IsNull(i))
                        {
                            var isTemporary = isTemporaryArray.GetValue(i);
                            if (isTemporary == true)
                            {
                                tableType = "LOCAL TEMPORARY";
                            }
                        }

                        // Pattern matching is already done by SQL LIKE clause, no need to check again
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

                        tables.Add((catalogName, schemaName, tableName, tableType, remarks));
                    }
                }

                activity?.SetTag("result_count", tables.Count);
                return tables;
            });
        }

        private async Task<List<ColumnInfo>> GetColumnsAsync(string? catalog, string? schema, string? tableName, string? columnNamePattern)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.SetTag("catalog", catalog ?? "(all)");
                activity?.SetTag("schema", schema ?? "(all)");
                activity?.SetTag("table", tableName ?? "(all)");
                activity?.SetTag("column_pattern", columnNamePattern ?? "(all)");

                // Now that SHOW COLUMNS IN ALL CATALOGS is supported, we can call directly
                // without iterating over catalogs
                var result = await GetColumnsForCatalogAsync(catalog, schema, tableName, columnNamePattern).ConfigureAwait(false);
                activity?.SetTag("result_count", result.Count);
                if (!string.IsNullOrEmpty(catalog))
                {
                    activity?.SetTag("unique_tables", result.Select(c => $"{c.Catalog}.{c.Schema}.{c.Table}").Distinct().Count());
                }
                return result;
            });
        }

        private async Task<List<ColumnInfo>> GetColumnsForCatalogAsync(string? catalog, string? schema, string? tableName, string? columnNamePattern)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.SetTag("catalog", catalog ?? "(all)");
                activity?.SetTag("schema", schema ?? "(all)");
                activity?.SetTag("table", tableName ?? "(all)");
                activity?.SetTag("column_pattern", columnNamePattern ?? "(all)");

                var commandBuilder = new SqlCommandBuilder(QuoteIdentifier, EscapeSqlPattern)
                    .WithCatalog(catalog);

                if (!string.IsNullOrEmpty(schema))
                {
                    commandBuilder.WithSchema(schema);
                }
                if (!string.IsNullOrEmpty(tableName))
                {
                    commandBuilder.WithTable(tableName);
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
                    activity?.SetTag("error", ex.Message);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    // If SHOW COLUMNS fails (e.g., permission denied), return empty list
                    return new List<ColumnInfo>();
                }

                var columns = new List<ColumnInfo>();
                var tablePositions = new Dictionary<string, int>();

                foreach (var batch in batches)
                {
                    // When using "IN ALL CATALOGS", results include catalog column
                    StringArray? catalogArray = null;
                    if (string.IsNullOrEmpty(catalog))
                    {
                        catalogArray = batch.Column("catalog") as StringArray;
                    }

                    var tableNameArray = batch.Column("tableName") as StringArray;
                    var colNameArray = batch.Column("col_name") as StringArray;
                    var columnTypeArray = batch.Column("columnType") as StringArray;
                    var isNullableArray = batch.Column("isNullable") as StringArray;

                    StringArray? schemaArray = null;
                    try
                    {
                        schemaArray = batch.Column("database") as StringArray ?? batch.Column("databaseName") as StringArray;
                    }
                    catch { }

                    StringArray? commentArray = null;
                    try
                    {
                        commentArray = batch.Column("comment") as StringArray;
                    }
                    catch { }

                    if (colNameArray == null || columnTypeArray == null || tableNameArray == null)
                    {
                        continue;
                    }

                    for (int i = 0; i < batch.Length; i++)
                    {
                        if (colNameArray.IsNull(i) || columnTypeArray.IsNull(i) || tableNameArray.IsNull(i))
                        {
                            continue;
                        }

                        var colName = colNameArray.GetString(i);
                        var dataType = columnTypeArray.GetString(i);
                        var currentTableName = tableNameArray.GetString(i);

                        // Get catalog from result when using "IN ALL CATALOGS"
                        string? currentCatalogName;
                        if (string.IsNullOrEmpty(catalog))
                        {
                            // Using "IN ALL CATALOGS" - get catalog from result
                            if (catalogArray == null || catalogArray.IsNull(i))
                                continue;
                            currentCatalogName = catalogArray.GetString(i);
                        }
                        else
                        {
                            currentCatalogName = catalog;
                        }

                        var currentSchemaName = schemaArray != null && !schemaArray.IsNull(i)
                            ? schemaArray.GetString(i)
                            : schema;

                        if (string.IsNullOrEmpty(colName) || colName.StartsWith("#") ||
                            dataType.Contains("Partition Information") || dataType.Contains("# col_name"))
                        {
                            continue;
                        }

                        var tableKey = $"{currentCatalogName}.{currentSchemaName}.{currentTableName}";
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

                        columns.Add(new ColumnInfo
                        {
                            Catalog = currentCatalogName,
                            Schema = currentSchemaName ?? "",
                            Table = currentTableName,
                            Name = colName,
                            TypeName = dataType,
                            Position = position,
                            Nullable = isNullable,
                            Comment = comment,
                            XdbcDataType = xdbcDataType,
                            XdbcTypeName = baseType,
                            XdbcColumnSize = DatabricksTypeMapper.GetColumnSize(dataType),
                            XdbcDecimalDigits = DatabricksTypeMapper.GetDecimalDigits(dataType),
                            XdbcNumPrecRadix = DatabricksTypeMapper.GetNumPrecRadix(dataType),
                            XdbcBufferLength = DatabricksTypeMapper.GetBufferLength(dataType),
                            XdbcCharOctetLength = DatabricksTypeMapper.GetCharOctetLength(dataType),
                            XdbcSqlDataType = DatabricksTypeMapper.GetSqlDataType(dataType),
                            XdbcDatetimeSub = DatabricksTypeMapper.GetSqlDatetimeSub(dataType)
                        });
                    }
                }

                activity?.SetTag("result_count", columns.Count);
                activity?.SetTag("unique_tables", tablePositions.Count);
                activity?.SetTag("skipped_partition_cols", batches.Sum(b => b.Length) - columns.Count);
                return columns;
            });
        }

        internal IArrowArrayStream GetColumnsFlat(string? catalog, string? dbSchema, string? tableName, string? columnName)
        {
            catalog = catalog ?? _catalog;
            dbSchema = dbSchema ?? _schema;

            if (string.IsNullOrEmpty(catalog) || string.IsNullOrEmpty(dbSchema))
            {
                return BuildFlatColumnsResult(catalog, dbSchema, tableName, new List<ColumnInfo>());
            }

            var columns = GetColumnsAsync(catalog, dbSchema, tableName, columnName).GetAwaiter().GetResult();
            return BuildFlatColumnsResult(catalog, dbSchema, tableName, columns);
        }

        /// <summary>
        /// Builds a flat structure (24 columns) from ColumnInfo list matching Thrift HiveServer2 GetColumns format.
        /// </summary>
        private IArrowArrayStream BuildFlatColumnsResult(string? catalog, string? dbSchema, string? tableName, List<ColumnInfo> columns)
        {
            var schema = ColumnMetadataSchemas.CreateColumnMetadataSchema();
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

            foreach (var column in columns)
            {
                tableCatBuilder.Append(catalog);
                tableSchemaBuilder.Append(dbSchema);
                tableNameBuilder.Append(tableName);
                columnNameBuilder.Append(column.Name);

                if (column.XdbcDataType.HasValue)
                    dataTypeBuilder.Append((int)column.XdbcDataType.Value);
                else
                    dataTypeBuilder.AppendNull();

                // Preserve original casing from server (e.g., "STRUCT<f1: STRING>" not "STRUCT<F1: STRING>")
                typeNameBuilder.Append(column.TypeName);

                if (column.XdbcColumnSize.HasValue)
                    columnSizeBuilder.Append(column.XdbcColumnSize.Value);
                else
                    columnSizeBuilder.AppendNull();

                if (column.XdbcBufferLength.HasValue)
                    bufferLengthBuilder.Append(column.XdbcBufferLength.Value);
                else
                    bufferLengthBuilder.AppendNull();

                if (column.XdbcDecimalDigits.HasValue)
                    decimalDigitsBuilder.Append(column.XdbcDecimalDigits.Value);
                else
                    decimalDigitsBuilder.AppendNull();

                if (column.XdbcNumPrecRadix.HasValue)
                    numPrecRadixBuilder.Append((int)column.XdbcNumPrecRadix.Value);
                else
                    numPrecRadixBuilder.AppendNull();

                nullableBuilder.Append(column.Nullable ? 1 : 0);
                remarksBuilder.Append(column.Comment ?? "");
                columnDefBuilder.AppendNull();

                if (column.XdbcSqlDataType.HasValue)
                    sqlDataTypeBuilder.Append((int)column.XdbcSqlDataType.Value);
                else
                    sqlDataTypeBuilder.AppendNull();

                if (column.XdbcDatetimeSub.HasValue)
                    sqlDatetimeSubBuilder.Append((int)column.XdbcDatetimeSub.Value);
                else
                    sqlDatetimeSubBuilder.AppendNull();

                if (column.XdbcCharOctetLength.HasValue)
                    charOctetLengthBuilder.Append(column.XdbcCharOctetLength.Value);
                else
                    charOctetLengthBuilder.AppendNull();

                ordinalPositionBuilder.Append(column.Position - 1);
                isNullableBuilder.Append(column.Nullable ? "YES" : "NO");
                scopeCatalogBuilder.AppendNull();
                scopeSchemaBuilder.AppendNull();
                scopeTableBuilder.AppendNull();
                sourceDataTypeBuilder.AppendNull();
                isAutoIncrementBuilder.Append("NO");
                baseTypeNameBuilder.Append(column.XdbcTypeName ?? string.Empty);
            }

            // Build the result arrays
            var dataArrays = new List<IArrowArray>
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

            // Create a single record batch with all the data
            var recordBatch = new RecordBatch(schema, dataArrays, columns.Count);

            // Return as a simple array stream
            return new StatementExecInfoArrowStream(schema, new[] { recordBatch });
        }

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

            // Iterate over unified catalogMap structure (matches Thrift pattern)
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
        /// Gets all supported table types in the database.
        /// Statement Execution API returns TABLE, VIEW, and LOCAL TEMPORARY.
        /// </summary>
        /// <returns>Arrow stream with single column 'table_type' containing supported table type names</returns>
        /// <summary>
        /// Gets a list of catalogs (databases) available in the system.
        /// </summary>
        /// <param name="catalogPattern">Catalog name pattern (SQL LIKE syntax). If null, returns all catalogs.</param>
        /// <returns>Arrow stream with catalog_name column</returns>
        internal IArrowArrayStream GetCatalogs(string? catalogPattern)
        {
            return GetCatalogsMetadataAsync(catalogPattern).GetAwaiter().GetResult();
        }

        private async Task<IArrowArrayStream> GetCatalogsMetadataAsync(string? catalogPattern)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.SetTag("catalog_pattern", catalogPattern ?? "(all)");

                // Use SqlCommandBuilder for efficient SQL generation
                var commandBuilder = new SqlCommandBuilder(QuoteIdentifier, EscapeSqlPattern)
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
                    activity?.SetTag("error", ex.Message);
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
        /// <param name="catalogPattern">Catalog name pattern (SQL LIKE syntax). If null, returns all catalogs.</param>
        /// <returns>Arrow stream with TABLE_CAT column (Thrift protocol naming)</returns>
        internal IArrowArrayStream GetCatalogsFlat(string? catalogPattern)
        {
            // Reuse existing method to get data
            var stream = GetCatalogsMetadataAsync(catalogPattern).GetAwaiter().GetResult();

            // Read data from ADBC-style stream
            var catalogBuilder = new StringArray.Builder();

            using (var reader = stream)
            {
                while (true)
                {
                    var batch = reader.ReadNextRecordBatchAsync().GetAwaiter().GetResult();
                    if (batch == null) break;

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
            var resultSchema = new Schema(new[] { new Field("TABLE_CAT", StringType.Default, nullable: true) }, null);
            var data = new List<IArrowArray> { catalogBuilder.Build() };

            return new StatementExecInfoArrowStream(resultSchema, data);
        }

        /// <summary>
        /// Gets a list of schemas in the specified catalog.
        /// </summary>
        /// <param name="catalog">Catalog name. If null, uses session catalog or returns schemas from all catalogs.</param>
        /// <param name="schemaPattern">Schema name pattern (SQL LIKE syntax). If null, returns all schemas.</param>
        /// <returns>Arrow stream with catalog_name and db_schema_name columns</returns>
        internal IArrowArrayStream GetSchemas(string? catalog, string? schemaPattern)
        {
            return GetSchemasMetadataAsync(catalog, schemaPattern).GetAwaiter().GetResult();
        }

        private async Task<IArrowArrayStream> GetSchemasMetadataAsync(string? catalog, string? schemaPattern)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                catalog = catalog ?? _catalog;

                activity?.SetTag("catalog", catalog ?? "(all)");
                activity?.SetTag("schema_pattern", schemaPattern ?? "(all)");
                activity?.SetTag("show_all_catalogs", string.IsNullOrEmpty(catalog));

                // Use SqlCommandBuilder for efficient SQL generation
                var commandBuilder = new SqlCommandBuilder(QuoteIdentifier, EscapeSqlPattern)
                    .WithCatalog(catalog)
                    .WithSchemaPattern(schemaPattern);

                var sql = commandBuilder.BuildShowSchemas();
                activity?.SetTag("sql_query", sql);
                bool showAllCatalogs = string.IsNullOrEmpty(catalog);

                List<RecordBatch> batches;
                try
                {
                    batches = await ExecuteSqlQueryAsync(sql).ConfigureAwait(false);
                    activity?.SetTag("result_batches", batches.Count);
                    activity?.SetTag("total_rows", batches.Sum(b => b.Length));
                }
                catch (Exception ex)
                {
                    activity?.SetTag("error", ex.Message);
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
                    // SHOW SCHEMAS IN ALL CATALOGS returns: catalog_name, databaseName (or namespace)
                    // SHOW SCHEMAS IN catalog returns: databaseName (or namespace)
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
                        // When showing schemas in specific catalog, schema is in databaseName or namespace column
                        schemaArray = batch.Column("databaseName") as StringArray;
                        if (schemaArray == null)
                        {
                            schemaArray = batch.Column("namespace") as StringArray;
                        }
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
        /// <returns>Arrow stream with TABLE_SCHEM and TABLE_CATALOG columns (Thrift protocol naming)</returns>
        internal IArrowArrayStream GetSchemasFlat(string? catalog, string? schemaPattern)
        {
            // Reuse existing method to get data
            var stream = GetSchemasMetadataAsync(catalog, schemaPattern).GetAwaiter().GetResult();

            // Read data from ADBC-style stream
            var catalogBuilder = new StringArray.Builder();
            var schemaBuilder = new StringArray.Builder();

            using (var reader = stream)
            {
                while (true)
                {
                    var batch = reader.ReadNextRecordBatchAsync().GetAwaiter().GetResult();
                    if (batch == null) break;

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

            // Create result with Thrift protocol column names (note: TABLE_SCHEM comes first in HiveServer2 spec)
            var resultSchema = new Schema(new[]
            {
                new Field("TABLE_SCHEM", StringType.Default, nullable: false),
                new Field("TABLE_CATALOG", StringType.Default, nullable: true)
            }, null);

            var data = new List<IArrowArray> { schemaBuilder.Build(), catalogBuilder.Build() };

            return new StatementExecInfoArrowStream(resultSchema, data);
        }

        /// <summary>
        /// Gets a list of tables in the specified catalog and schema.
        /// </summary>
        /// <param name="catalog">Catalog name. If null, uses session catalog.</param>
        /// <param name="dbSchema">Schema name. If null, uses session schema.</param>
        /// <param name="tablePattern">Table name pattern (SQL LIKE syntax). If null, returns all tables.</param>
        /// <param name="tableTypes">List of table types to include (TABLE, VIEW). If null, returns all types.</param>
        /// <returns>Arrow stream with catalog_name, db_schema_name, table_name, table_type columns</returns>
        internal IArrowArrayStream GetTables(string? catalog, string? dbSchema, string? tablePattern, List<string>? tableTypes)
        {
            return GetTablesAsync(catalog, dbSchema, tablePattern, tableTypes).GetAwaiter().GetResult();
        }

        private async Task<IArrowArrayStream> GetTablesAsync(string? catalog, string? dbSchema, string? tablePattern, List<string>? tableTypes)
        {
            catalog = catalog ?? _catalog;
            dbSchema = dbSchema ?? _schema;

            if (string.IsNullOrEmpty(catalog) || string.IsNullOrEmpty(dbSchema))
            {
                throw new ArgumentException("Catalog and schema are required for GetTables");
            }

            var tables = await GetTablesAsync(catalog, dbSchema, tablePattern, (IReadOnlyList<string>?)tableTypes).ConfigureAwait(false);

            var catalogBuilder = new StringArray.Builder();
            var schemaBuilder = new StringArray.Builder();
            var tableBuilder = new StringArray.Builder();
            var typeBuilder = new StringArray.Builder();

            foreach (var (catalogName, schemaName, tableName, tableType, _) in tables)
            {
                catalogBuilder.Append(catalogName);
                schemaBuilder.Append(schemaName);
                tableBuilder.Append(tableName);
                typeBuilder.Append(tableType);
            }

            var resultSchema = new Schema(new[]
            {
                new Field("catalog_name", StringType.Default, nullable: true),
                new Field("db_schema_name", StringType.Default, nullable: true),
                new Field("table_name", StringType.Default, nullable: false),
                new Field("table_type", StringType.Default, nullable: false)
            }, null);

            var data = new List<IArrowArray>
            {
                catalogBuilder.Build(),
                schemaBuilder.Build(),
                tableBuilder.Build(),
                typeBuilder.Build()
            };

            return new StatementExecInfoArrowStream(resultSchema, data);
        }

        /// <summary>
        /// Gets tables in flat structure with Thrift HiveServer2-compatible column naming for statement-based queries.
        /// Reuses GetTablesAsync() and transforms column names to match Thrift protocol.
        /// </summary>
        /// <param name="catalog">Catalog name. If null, uses session catalog.</param>
        /// <param name="dbSchema">Schema name. If null, uses session schema.</param>
        /// <param name="tablePattern">Table name pattern (SQL LIKE syntax). If null, returns all tables.</param>
        /// <param name="tableTypes">List of table types to include (TABLE, VIEW). If null, returns all types.</param>
        /// <returns>Arrow stream with TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE columns (Thrift protocol naming)</returns>
        internal IArrowArrayStream GetTablesFlat(string? catalog, string? dbSchema, string? tablePattern, List<string>? tableTypes)
        {
            catalog = catalog ?? _catalog;
            dbSchema = dbSchema ?? _schema;

            if (string.IsNullOrEmpty(catalog) || string.IsNullOrEmpty(dbSchema))
            {
                throw new ArgumentException("Catalog and schema are required for GetTablesFlat");
            }

            // Call private GetTablesAsync to get remarks data
            var tables = GetTablesAsync(catalog, dbSchema, tablePattern, (IReadOnlyList<string>?)tableTypes).GetAwaiter().GetResult();

            // Build Thrift HiveServer2-compatible result
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

            foreach (var (catalogName, schemaName, tableName, tableType, remarks) in tables)
            {
                // TABLE_CAT
                if (!string.IsNullOrEmpty(catalogName))
                    catalogBuilder.Append(catalogName);
                else
                    catalogBuilder.AppendNull();

                // TABLE_SCHEM
                if (!string.IsNullOrEmpty(schemaName))
                    schemaBuilder.Append(schemaName);
                else
                    schemaBuilder.AppendNull();

                // TABLE_NAME
                tableBuilder.Append(tableName);

                // TABLE_TYPE
                typeBuilder.Append(tableType);

                // REMARKS - use actual server value (already converted to "" if null/empty)
                remarksBuilder.Append(remarks);

                // TYPE_CAT, TYPE_SCHEM, TYPE_NAME - for structured types (not supported by Databricks)
                typeCatBuilder.AppendNull();
                typeSchemaBuilder.AppendNull();
                typeNameBuilder.AppendNull();

                // SELF_REFERENCING_COL_NAME - for self-referencing tables (not supported by Databricks)
                selfRefColBuilder.AppendNull();

                // REF_GENERATION - referential generation (not supported by Databricks)
                refGenerationBuilder.AppendNull();
            }

            // Create result with Thrift protocol column names (10 columns total)
            var resultSchema = new Schema(new[]
            {
                new Field("TABLE_CAT", StringType.Default, nullable: true),
                new Field("TABLE_SCHEM", StringType.Default, nullable: true),
                new Field("TABLE_NAME", StringType.Default, nullable: false),
                new Field("TABLE_TYPE", StringType.Default, nullable: false),
                new Field("REMARKS", StringType.Default, nullable: true),
                new Field("TYPE_CAT", StringType.Default, nullable: true),
                new Field("TYPE_SCHEM", StringType.Default, nullable: true),
                new Field("TYPE_NAME", StringType.Default, nullable: true),
                new Field("SELF_REFERENCING_COL_NAME", StringType.Default, nullable: true),
                new Field("REF_GENERATION", StringType.Default, nullable: true)
            }, null);

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

        /// <summary>
        /// Gets a list of columns in the specified table.
        /// </summary>
        /// <param name="catalog">Catalog name. If null, uses session catalog.</param>
        /// <param name="dbSchema">Schema name. If null, uses session schema.</param>
        /// <param name="tablePattern">Table name pattern (SQL LIKE syntax). If null, returns columns from all tables.</param>
        /// <param name="columnPattern">Column name pattern (SQL LIKE syntax). If null, returns all columns.</param>
        /// <returns>Arrow stream with catalog_name, db_schema_name, table_name, column_name, ordinal_position, remarks columns</returns>
        internal IArrowArrayStream GetColumns(string? catalog, string? dbSchema, string? tablePattern, string? columnPattern)
        {
            return GetColumnsMetadataAsync(catalog, dbSchema, tablePattern, columnPattern).GetAwaiter().GetResult();
        }

        private async Task<IArrowArrayStream> GetColumnsMetadataAsync(string? catalog, string? dbSchema, string? tablePattern, string? columnPattern)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                catalog = catalog ?? _catalog;
                dbSchema = dbSchema ?? _schema;

                activity?.SetTag("catalog", catalog ?? "(all)");
                activity?.SetTag("db_schema", dbSchema ?? "(all)");
                activity?.SetTag("table_pattern", tablePattern ?? "(all)");
                activity?.SetTag("column_pattern", columnPattern ?? "(all)");

                if (string.IsNullOrEmpty(catalog) || string.IsNullOrEmpty(dbSchema))
                {
                    activity?.SetTag("error", "Catalog and schema are required");
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    throw new ArgumentException("Catalog and schema are required for GetColumns");
                }

                // Note: tablePattern can now be null - SHOW COLUMNS supports pattern matching across all tables in a single query
                // This eliminates the N+1 query problem that existed with DESCRIBE TABLE

                // Use SqlCommandBuilder for efficient SQL generation
                var commandBuilder = new SqlCommandBuilder(QuoteIdentifier, EscapeSqlPattern)
                    .WithCatalog(catalog)
                    .WithSchemaPattern(dbSchema)
                    .WithTablePattern(tablePattern)
                    .WithColumnPattern(columnPattern);

                var sql = commandBuilder.BuildShowColumns(catalog);
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
                    activity?.SetTag("error", ex.Message);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    // Return empty result on permission denied or other errors
                    var emptySchema = new Schema(new[]
                    {
                        new Field("catalog_name", StringType.Default, nullable: true),
                        new Field("db_schema_name", StringType.Default, nullable: true),
                        new Field("table_name", StringType.Default, nullable: false),
                        new Field("column_name", StringType.Default, nullable: false),
                        new Field("ordinal_position", Int32Type.Default, nullable: false),
                        new Field("remarks", StringType.Default, nullable: true)
                    }, null);
                    var emptyData = new List<IArrowArray>
                    {
                        new StringArray.Builder().Build(),
                        new StringArray.Builder().Build(),
                        new StringArray.Builder().Build(),
                        new StringArray.Builder().Build(),
                        new Int32Array.Builder().Build(),
                        new StringArray.Builder().Build()
                    };
                    return new StatementExecInfoArrowStream(emptySchema, emptyData);
                }

            // Build result: catalog_name, db_schema_name, table_name, column_name, ordinal_position, remarks
            var catalogBuilder = new StringArray.Builder();
            var schemaBuilder = new StringArray.Builder();
            var tableBuilder = new StringArray.Builder();
            var columnBuilder = new StringArray.Builder();
            var ordinalBuilder = new Int32Array.Builder();
            var remarksBuilder = new StringArray.Builder();

            // Track ordinal position per table (since SHOW COLUMNS can return multiple tables)
            var tablePositions = new Dictionary<string, int>();

            foreach (var batch in batches)
            {
                // SHOW COLUMNS returns: tableName, col_name, dataType (int), columnType (string), nullable, SQLDataType, ordinal_position, isNullable
                // Note: catalog and schema are NOT in the result - we use them from method parameters
                var tableNameArray = batch.Column("tableName") as StringArray;
                var colNameArray = batch.Column("col_name") as StringArray;
                var columnTypeArray = batch.Column("columnType") as StringArray;
                var isNullableArray = batch.Column("isNullable") as StringArray;

                // Try to get comment if it exists (may not be in all versions)
                StringArray? commentArray = null;
                try
                {
                    commentArray = batch.Column("comment") as StringArray;
                }
                catch
                {
                    // comment column doesn't exist - that's ok
                }

                if (colNameArray == null || columnTypeArray == null || tableNameArray == null)
                    continue;

                for (int i = 0; i < batch.Length; i++)
                {
                    if (colNameArray.IsNull(i) || columnTypeArray.IsNull(i) || tableNameArray.IsNull(i))
                        continue;

                    var colName = colNameArray.GetString(i);
                    var dataType = columnTypeArray.GetString(i);
                    var currentTableName = tableNameArray.GetString(i);
                    var currentCatalogName = catalog;
                    var currentSchemaName = dbSchema;

                    // Skip metadata rows (SHOW COLUMNS shouldn't return these, but be defensive)
                    if (string.IsNullOrEmpty(colName) ||
                        colName.StartsWith("#") ||
                        dataType.Contains("Partition Information") ||
                        dataType.Contains("# col_name"))
                    {
                        continue;
                    }

                    // Track ordinal position per table (not global)
                    var tableKey = $"{currentCatalogName}.{currentSchemaName}.{currentTableName}";
                    if (!tablePositions.ContainsKey(tableKey))
                    {
                        tablePositions[tableKey] = 1; // Start at 1 for ordinal position (1-based)
                    }
                    int position = tablePositions[tableKey]++;

                    var comment = commentArray != null && !commentArray.IsNull(i) ? commentArray.GetString(i) : null;

                    catalogBuilder.Append(currentCatalogName);
                    schemaBuilder.Append(currentSchemaName);
                    tableBuilder.Append(currentTableName);
                    columnBuilder.Append(colName);
                    ordinalBuilder.Append(position);
                    MetadataUtilities.AppendNullableString(remarksBuilder, comment);
                }
            }

            var resultSchema = new Schema(new[]
            {
                new Field("catalog_name", StringType.Default, nullable: true),
                new Field("db_schema_name", StringType.Default, nullable: true),
                new Field("table_name", StringType.Default, nullable: false),
                new Field("column_name", StringType.Default, nullable: false),
                new Field("ordinal_position", Int32Type.Default, nullable: false),
                new Field("remarks", StringType.Default, nullable: true)
            }, null);

            var data = new List<IArrowArray>
            {
                catalogBuilder.Build(),
                schemaBuilder.Build(),
                tableBuilder.Build(),
                columnBuilder.Build(),
                ordinalBuilder.Build(),
                remarksBuilder.Build()
            };

            activity?.SetTag("result_columns", columnBuilder.Length);
            activity?.SetTag("unique_tables", tablePositions.Count);
            return new StatementExecInfoArrowStream(resultSchema, data);
        });
        }

        /// <remarks>
        /// <para>Returns 3 table types:</para>
        /// <list type="bullet">
        /// <item><description>TABLE - Regular persistent tables</description></item>
        /// <item><description>VIEW - Database views</description></item>
        /// <item><description>LOCAL TEMPORARY - Temporary tables (detected via isTemporary column in SHOW TABLES)</description></item>
        /// </list>
        /// <para>Note: Thrift protocol returns only TABLE and VIEW (2 types). REST/Statement Execution API includes LOCAL TEMPORARY detection.</para>
        /// </remarks>
        public override IArrowArrayStream GetTableTypes()
        {
            return this.TraceActivity(activity =>
            {
                var tableTypesBuilder = new StringArray.Builder();
                tableTypesBuilder.Append("TABLE");
                tableTypesBuilder.Append("VIEW");
                tableTypesBuilder.Append("LOCAL TEMPORARY");

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
                // Use session catalog/schema if not provided
                catalog = catalog ?? _catalog;
                dbSchema = dbSchema ?? _schema;

                activity?.SetTag("catalog", catalog ?? "(session)");
                activity?.SetTag("db_schema", dbSchema ?? "(session)");
                activity?.SetTag("table_name", tableName);

                var commandBuilder = new SqlCommandBuilder(QuoteIdentifier, EscapeSqlPattern)
                    .WithCatalog(catalog)
                    .WithSchema(dbSchema)
                    .WithTable(tableName);

                var sql = commandBuilder.BuildDescribeTable();
                activity?.SetTag("sql_query", sql);

                // Build qualified name for error messages
                var qualifiedTableName = BuildQualifiedTableName(catalog, dbSchema, tableName);
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
                    activity?.SetTag("error", ex.Message);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    throw new AdbcException($"Failed to describe table {qualifiedTableName}: {ex.Message}", ex);
                }

                if (batches == null || batches.Count == 0)
                {
                    activity?.SetTag("error", "Table not found or has no schema information");
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    throw new AdbcException($"Table {qualifiedTableName} not found or has no schema information");
                }

            var fields = new List<Field>();

            foreach (var batch in batches)
            {
                var colNameArray = batch.Column("col_name") as StringArray;
                var dataTypeArray = batch.Column("data_type") as StringArray;
                var commentArray = batch.Column("comment") as StringArray;

                if (colNameArray == null || dataTypeArray == null)
                {
                    continue;
                }

                for (int i = 0; i < batch.Length; i++)
                {
                    if (colNameArray.IsNull(i) || dataTypeArray.IsNull(i))
                        continue;

                    var colName = colNameArray.GetString(i);
                    var dataType = dataTypeArray.GetString(i);

                    // Skip partition information and other metadata rows
                    if (string.IsNullOrEmpty(colName) ||
                        colName.StartsWith("#") ||
                        dataType.Contains("Partition Information") ||
                        dataType.Contains("# col_name"))
                    {
                        continue;
                    }

                    var arrowType = ConvertDatabricksTypeToArrow(dataType);

                    // Build field metadata if there's a comment
                    var metadata = new Dictionary<string, string>();
                    if (commentArray != null && !commentArray.IsNull(i))
                    {
                        var comment = commentArray.GetString(i);
                        if (!string.IsNullOrEmpty(comment))
                        {
                            metadata["comment"] = comment;
                        }
                    }

                    var field = new Field(colName, arrowType, nullable: true, metadata.Count > 0 ? metadata : null);
                    fields.Add(field);
                }
            }

            if (fields.Count == 0)
            {
                activity?.SetTag("error", "Table has no columns");
                activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                throw new AdbcException($"Table {qualifiedTableName} has no columns");
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

            while (true)
            {
                var batch = await queryResult.Stream.ReadNextRecordBatchAsync().ConfigureAwait(false);
                if (batch == null)
                    break;
                batches.Add(batch);
            }

            return batches;
        }

        private string QuoteIdentifier(string identifier)
        {
            return $"`{identifier.Replace("`", "``")}`";
        }

        /// <summary>
        /// Converts ADBC SQL patterns to Databricks glob patterns and escapes for SQL.
        /// ADBC patterns: % (zero or more), _ (exactly one), \ (escape)
        /// Databricks patterns: * (zero or more), . (exactly one)
        /// </summary>
        private string EscapeSqlPattern(string pattern)
        {
            if (string.IsNullOrEmpty(pattern))
                return pattern;

            var result = new System.Text.StringBuilder(pattern.Length);
            bool escapeNext = false;

            for (int i = 0; i < pattern.Length; i++)
            {
                char c = pattern[i];

                if (c == '\\')
                {
                    // Check if it's an escaped backslash (\\)
                    if (i + 1 < pattern.Length && pattern[i + 1] == '\\')
                    {
                        result.Append("\\\\");  // Preserve both backslashes
                        i++; // Skip the next backslash
                    }
                    else
                    {
                        escapeNext = !escapeNext;  // Toggle escape state for next character
                    }
                }
                else if (escapeNext)
                {
                    // If the current character is escaped, add it directly (literal)
                    result.Append(c);
                    escapeNext = false;
                }
                else
                {
                    // Handle unescaped characters
                    if (c == '%')
                    {
                        // ADBC wildcard % → Databricks wildcard *
                        result.Append('*');
                    }
                    else if (c == '_')
                    {
                        // ADBC single char _ → Databricks single char .
                        result.Append('.');
                    }
                    else if (c == '\'')
                    {
                        // SQL escape: ' → ''
                        result.Append("''");
                    }
                    else
                    {
                        result.Append(c);
                    }
                }
            }

            return result.ToString();
        }

        private string BuildQualifiedTableName(string? catalog, string? schema, string tableName)
        {
            return MetadataUtilities.BuildQualifiedTableName(catalog, schema, tableName) ?? tableName;
        }

        private bool PatternMatches(string value, string pattern)
        {
            var regexPattern = "^" + System.Text.RegularExpressions.Regex.Escape(pattern)
                .Replace("%", ".*")
                .Replace("_", ".") + "$";

            return System.Text.RegularExpressions.Regex.IsMatch(value, regexPattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
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

                if (column.XdbcDataType.HasValue)
                    xdbcDataTypeBuilder.Append(column.XdbcDataType.Value);
                else
                    xdbcDataTypeBuilder.AppendNull();

                xdbcTypeNameBuilder.Append(column.XdbcTypeName ?? string.Empty);

                if (column.XdbcColumnSize.HasValue)
                    xdbcColumnSizeBuilder.Append(column.XdbcColumnSize.Value);
                else
                    xdbcColumnSizeBuilder.AppendNull();

                if (column.XdbcDecimalDigits.HasValue)
                    xdbcDecimalDigitsBuilder.Append((short)column.XdbcDecimalDigits.Value);
                else
                    xdbcDecimalDigitsBuilder.AppendNull();

                if (column.XdbcNumPrecRadix.HasValue)
                    xdbcNumPrecRadixBuilder.Append(column.XdbcNumPrecRadix.Value);
                else
                    xdbcNumPrecRadixBuilder.AppendNull();

                xdbcNullableBuilder.Append((short)(column.Nullable ? 1 : 0));
                xdbcColumnDefBuilder.Append("");

                if (column.XdbcSqlDataType.HasValue)
                    xdbcSqlDataTypeBuilder.Append(column.XdbcSqlDataType.Value);
                else
                    xdbcSqlDataTypeBuilder.AppendNull();

                if (column.XdbcDatetimeSub.HasValue)
                    xdbcDatetimeSubBuilder.Append(column.XdbcDatetimeSub.Value);
                else
                    xdbcDatetimeSubBuilder.AppendNull();

                if (column.XdbcCharOctetLength.HasValue)
                    xdbcCharOctetLengthBuilder.Append(column.XdbcCharOctetLength.Value);
                else
                    xdbcCharOctetLengthBuilder.AppendNull();

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
                _schema = schema;
                _batch = new RecordBatch(schema, data, data[0].Length);
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
        /// <example>
        /// <code>
        /// using var connection = database.Connect();
        /// using var stream = connection.GetInfo(new[] { AdbcInfoCode.DriverName, AdbcInfoCode.DriverVersion });
        ///
        /// while (true)
        /// {
        ///     using var batch = stream.ReadNextRecordBatchAsync().Result;
        ///     if (batch == null) break;
        ///
        ///     var infoNameArray = batch.Column("info_name") as UInt32Array;
        ///     var infoValueArray = batch.Column("info_value") as DenseUnionArray;
        ///
        ///     for (int i = 0; i &lt; batch.Length; i++)
        ///     {
        ///         var code = (AdbcInfoCode)infoNameArray.GetValue(i);
        ///         // Extract value from union based on type
        ///     }
        /// }
        /// </code>
        /// </example>
        public override IArrowArrayStream GetInfo(IReadOnlyList<AdbcInfoCode> codes)
        {
            return this.TraceActivity(activity =>
            {
                activity?.SetTag("requested_codes_count", codes?.Count ?? 0);
                activity?.SetTag("requested_codes", codes != null && codes.Count > 0 ? string.Join(",", codes) : "(all)");

                // Info value type IDs for DenseUnionType
                const int strValTypeID = 0;
                const int boolValTypeId = 1;

                // Supported info codes for Statement Execution API (matches Thrift implementation)
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
                        stringInfoBuilder.Append("1.0.0"); // ADBC Arrow version (matches Thrift implementation)
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

        /// <summary>
        /// Gets primary keys for a specific table using SHOW KEYS SQL command.
        /// Only Unity Catalog tables support primary keys. Hive metastore tables will return empty results.
        /// </summary>
        /// <param name="catalog">Catalog name. If null, uses session catalog.</param>
        /// <param name="dbSchema">Schema name. If null, uses session schema.</param>
        /// <param name="tableName">Table name (required)</param>
        /// <returns>Arrow stream with primary key information (catalog_name, db_schema_name, table_name, column_name, key_sequence)</returns>
        /// <exception cref="ArgumentNullException">Thrown when tableName is null or empty</exception>
        /// <remarks>
        /// <para>Uses SQL command: SHOW KEYS IN catalog.schema.table</para>
        /// <para>Returns columns:</para>
        /// <list type="bullet">
        /// <item><description>catalog_name (string, nullable): Catalog name</description></item>
        /// <item><description>db_schema_name (string, nullable): Schema name</description></item>
        /// <item><description>table_name (string): Table name</description></item>
        /// <item><description>column_name (string): Column name</description></item>
        /// <item><description>key_sequence (int32): Position in key (1-based)</description></item>
        /// </list>
        /// <para>Only Unity Catalog tables support primary keys. Returns empty results for Hive metastore tables or on permission errors.</para>
        /// </remarks>
        /// <example>
        /// <code>
        /// using var connection = database.Connect();
        /// using var stream = connection.GetPrimaryKeys("main", "default", "customers");
        ///
        /// while (true)
        /// {
        ///     using var batch = stream.ReadNextRecordBatchAsync().Result;
        ///     if (batch == null) break;
        ///
        ///     var columnArray = batch.Column("column_name") as StringArray;
        ///     var sequenceArray = batch.Column("key_sequence") as Int32Array;
        ///
        ///     for (int i = 0; i < batch.Length; i++)
        ///     {
        ///         Console.WriteLine($"PK Column: {columnArray.GetString(i)}, Sequence: {sequenceArray.GetValue(i)}");
        ///     }
        /// }
        /// </code>
        /// </example>
        internal IArrowArrayStream GetPrimaryKeys(string? catalog, string? dbSchema, string tableName)
        {
            return GetPrimaryKeysAsync(catalog, dbSchema, tableName).GetAwaiter().GetResult();
        }

        private async Task<IArrowArrayStream> GetPrimaryKeysAsync(string? catalog, string? dbSchema, string tableName)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                if (string.IsNullOrEmpty(tableName))
                {
                    activity?.SetTag("error", "Table name is required");
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    throw new ArgumentNullException(nameof(tableName), "Table name is required");
                }

                // Use session catalog/schema if not provided
                catalog = catalog ?? _catalog;
                dbSchema = dbSchema ?? _schema;

                activity?.SetTag("catalog", catalog ?? "(session)");
                activity?.SetTag("db_schema", dbSchema ?? "(session)");
                activity?.SetTag("table_name", tableName);

                // Build SHOW KEYS query using Unity Catalog syntax
                // Format: SHOW KEYS IN CATALOG `catalog` IN SCHEMA `schema` IN TABLE `table`
                var commandBuilder = new SqlCommandBuilder(QuoteIdentifier, EscapeSqlPattern)
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
                    activity?.SetTag("error", ex.Message);
                    activity?.SetTag("note", "SHOW KEYS not supported or permission denied");
                    // SHOW KEYS not supported (Hive metastore) or permission denied
                    // Return empty result gracefully
                    return BuildPrimaryKeysResult(new List<PrimaryKeyInfo>(), catalog, dbSchema, tableName);
                }

            var primaryKeys = new List<PrimaryKeyInfo>();
            int keySequence = 1;

            foreach (var batch in batches)
            {
                // SHOW KEYS returns columns with camelCase names in Unity Catalog
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
                MetadataUtilities.AppendNullableString(constraintNameBuilder, pk.ConstraintName);
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
                while (true)
                {
                    var batch = reader.ReadNextRecordBatchAsync().GetAwaiter().GetResult();
                    if (batch == null) break;

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
                                sequenceBuilder.Append(sequenceArray.GetValue(i).Value);
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
        /// Only Unity Catalog tables support foreign keys. Hive metastore tables will return empty results.
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
        /// <para>Only Unity Catalog tables support foreign keys. Returns empty results for Hive metastore tables or on permission errors.</para>
        /// </remarks>
        /// <example>
        /// <code>
        /// using var connection = database.Connect();
        /// using var stream = connection.GetImportedKeys("main", "default", "orders");
        ///
        /// while (true)
        /// {
        ///     using var batch = stream.ReadNextRecordBatchAsync().Result;
        ///     if (batch == null) break;
        ///
        ///     var fkColumnArray = batch.Column("fk_column_name") as StringArray;
        ///     var pkTableArray = batch.Column("pk_table_name") as StringArray;
        ///     var pkColumnArray = batch.Column("pk_column_name") as StringArray;
        ///
        ///     for (int i = 0; i < batch.Length; i++)
        ///     {
        ///         Console.WriteLine($"FK: {fkColumnArray.GetString(i)} -> {pkTableArray.GetString(i)}.{pkColumnArray.GetString(i)}");
        ///     }
        /// }
        /// </code>
        /// </example>
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
                    activity?.SetTag("error", "Table name is required");
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    throw new ArgumentNullException(nameof(tableName), "Table name is required");
                }

                // Use session catalog/schema if not provided
                catalog = catalog ?? _catalog;
                dbSchema = dbSchema ?? _schema;

                activity?.SetTag("catalog", catalog ?? "(session)");
                activity?.SetTag("db_schema", dbSchema ?? "(session)");
                activity?.SetTag("table_name", tableName);

                // Build SHOW FOREIGN KEYS query using Unity Catalog syntax
                // Format: SHOW FOREIGN KEYS IN CATALOG `catalog` IN SCHEMA `schema` IN TABLE `table`
                var commandBuilder = new SqlCommandBuilder(QuoteIdentifier, EscapeSqlPattern)
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
                    activity?.SetTag("error", ex.Message);
                    activity?.SetTag("note", "SHOW FOREIGN KEYS not supported or permission denied");
                    // SHOW FOREIGN KEYS not supported (Hive metastore) or permission denied
                    // Return empty result gracefully
                    return BuildImportedKeysResult(new List<ForeignKeyInfo>());
                }

            var foreignKeys = new List<ForeignKeyInfo>();
            var keySequences = new Dictionary<string, int>();

            foreach (var batch in batches)
            {
                // SHOW FOREIGN KEYS returns columns with camelCase names in Unity Catalog
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

            return action.ToUpperInvariant() switch
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
            var deferrabilityBuilder = new Int16Array.Builder();

            foreach (var fk in foreignKeys)
            {
                MetadataUtilities.AppendNullableString(pkCatalogBuilder, fk.PkCatalogName);
                MetadataUtilities.AppendNullableString(pkSchemaBuilder, fk.PkSchemaName);
                pkTableBuilder.Append(fk.PkTableName);
                pkColumnBuilder.Append(fk.PkColumnName);

                MetadataUtilities.AppendNullableString(fkCatalogBuilder, fk.FkCatalogName);
                MetadataUtilities.AppendNullableString(fkSchemaBuilder, fk.FkSchemaName);
                fkTableBuilder.Append(fk.FkTableName);
                fkColumnBuilder.Append(fk.FkColumnName);

                sequenceBuilder.Append(fk.KeySequence);
                MetadataUtilities.AppendNullableString(fkNameBuilder, fk.FkConstraintName);
                MetadataUtilities.AppendNullableString(pkNameBuilder, fk.PkKeyName);

                if (fk.UpdateRule.HasValue)
                    updateRuleBuilder.Append(fk.UpdateRule.Value);
                else
                    updateRuleBuilder.AppendNull();

                if (fk.DeleteRule.HasValue)
                    deleteRuleBuilder.Append(fk.DeleteRule.Value);
                else
                    deleteRuleBuilder.AppendNull();

                if (fk.Deferrability.HasValue)
                    deferrabilityBuilder.Append(fk.Deferrability.Value);
                else
                    deferrabilityBuilder.AppendNull();
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
                new Field("deferrability", Int16Type.Default, nullable: true)
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
        /// Gets foreign keys that reference a specific primary key (cross-reference between two tables).
        /// Implementation approach: execute SHOW FOREIGN KEYS on the foreign (child) table,
        /// then filter results by the parent table parameters.
        /// </summary>
        /// <param name="pkCatalog">Parent (primary key) table catalog. If null, uses session catalog.</param>
        /// <param name="pkSchema">Parent (primary key) table schema. If null, uses session schema.</param>
        /// <param name="pkTable">Parent (primary key) table name. Can be null to include all parent tables.</param>
        /// <param name="fkCatalog">Foreign (child) table catalog. If null, uses session catalog.</param>
        /// <param name="fkSchema">Foreign (child) table schema. If null, uses session schema.</param>
        /// <param name="fkTable">Foreign (child) table name. Required.</param>
        /// <returns>Arrow stream containing the cross-reference foreign key metadata.</returns>
        /// <example>
        /// // Find all foreign keys in "orders" table that reference "customers" table:
        /// using var stream = connection.GetCrossReference(
        ///     pkCatalog: "main",
        ///     pkSchema: "default",
        ///     pkTable: "customers",
        ///     fkCatalog: "main",
        ///     fkSchema: "default",
        ///     fkTable: "orders"
        /// );
        /// </example>
        internal IArrowArrayStream GetCrossReference(
            string? pkCatalog,
            string? pkSchema,
            string? pkTable,
            string? fkCatalog,
            string? fkSchema,
            string? fkTable)
        {
            return GetCrossReferenceAsync(pkCatalog, pkSchema, pkTable, fkCatalog, fkSchema, fkTable)
                .GetAwaiter().GetResult();
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
            var updateRuleBuilder = new Int16Array.Builder();
            var deleteRuleBuilder = new Int16Array.Builder();
            var fkNameBuilder = new StringArray.Builder();
            var pkNameBuilder = new StringArray.Builder();
            var deferrabilityBuilder = new Int16Array.Builder();

            using (var reader = stream)
            {
                while (true)
                {
                    var batch = reader.ReadNextRecordBatchAsync().GetAwaiter().GetResult();
                    if (batch == null) break;

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
                    var deferrabilityArray = batch.Column("deferrability") as Int16Array;

                    if (fkColumnArray != null)
                    {
                        for (int i = 0; i < fkColumnArray.Length; i++)
                        {
                            // PKTABLE_CAT
                            if (pkCatalogArray != null && !pkCatalogArray.IsNull(i))
                                pkCatalogBuilder.Append(pkCatalogArray.GetString(i));
                            else
                                pkCatalogBuilder.AppendNull();

                            // PKTABLE_SCHEM
                            if (pkSchemaArray != null && !pkSchemaArray.IsNull(i))
                                pkSchemaBuilder.Append(pkSchemaArray.GetString(i));
                            else
                                pkSchemaBuilder.AppendNull();

                            // PKTABLE_NAME
                            if (pkTableArray != null && !pkTableArray.IsNull(i))
                                pkTableBuilder.Append(pkTableArray.GetString(i));
                            else
                                pkTableBuilder.AppendNull();

                            // PKCOLUMN_NAME
                            if (pkColumnArray != null && !pkColumnArray.IsNull(i))
                                pkColumnBuilder.Append(pkColumnArray.GetString(i));
                            else
                                pkColumnBuilder.AppendNull();

                            // FKTABLE_CAT
                            if (fkCatalogArray != null && !fkCatalogArray.IsNull(i))
                                fkCatalogBuilder.Append(fkCatalogArray.GetString(i));
                            else
                                fkCatalogBuilder.AppendNull();

                            // FKTABLE_SCHEM
                            if (fkSchemaArray != null && !fkSchemaArray.IsNull(i))
                                fkSchemaBuilder.Append(fkSchemaArray.GetString(i));
                            else
                                fkSchemaBuilder.AppendNull();

                            // FKTABLE_NAME
                            if (fkTableArray != null && !fkTableArray.IsNull(i))
                                fkTableBuilder.Append(fkTableArray.GetString(i));
                            else
                                fkTableBuilder.AppendNull();

                            // FKCOLUMN_NAME
                            if (!fkColumnArray.IsNull(i))
                                fkColumnBuilder.Append(fkColumnArray.GetString(i));
                            else
                                fkColumnBuilder.AppendNull();

                            // KEQ_SEQ
                            if (keySeqArray != null && !keySeqArray.IsNull(i))
                                keySeqBuilder.Append(keySeqArray.GetValue(i).Value);
                            else
                                keySeqBuilder.AppendNull();

                            // UPDATE_RULE (convert from UInt8 to Int16)
                            if (updateRuleArray != null && !updateRuleArray.IsNull(i))
                                updateRuleBuilder.Append((short)updateRuleArray.GetValue(i).Value);
                            else
                                updateRuleBuilder.AppendNull();

                            // DELETE_RULE (convert from UInt8 to Int16)
                            if (deleteRuleArray != null && !deleteRuleArray.IsNull(i))
                                deleteRuleBuilder.Append((short)deleteRuleArray.GetValue(i).Value);
                            else
                                deleteRuleBuilder.AppendNull();

                            // FK_NAME
                            if (fkNameArray != null && !fkNameArray.IsNull(i))
                                fkNameBuilder.Append(fkNameArray.GetString(i));
                            else
                                fkNameBuilder.AppendNull();

                            // PK_NAME
                            if (pkNameArray != null && !pkNameArray.IsNull(i))
                                pkNameBuilder.Append(pkNameArray.GetString(i));
                            else
                                pkNameBuilder.AppendNull();

                            // DEFERRABILITY (read actual value from database)
                            if (deferrabilityArray != null && !deferrabilityArray.IsNull(i))
                                deferrabilityBuilder.Append(deferrabilityArray.GetValue(i).Value);
                            else
                                deferrabilityBuilder.AppendNull();
                        }
                    }
                }
            }

            // Create result with Thrift protocol column names (14 columns matching JDBC spec)
            var resultSchema = new Schema(new[]
            {
                new Field("PKTABLE_CAT", StringType.Default, nullable: true),
                new Field("PKTABLE_SCHEM", StringType.Default, nullable: true),
                new Field("PKTABLE_NAME", StringType.Default, nullable: false),
                new Field("PKCOLUMN_NAME", StringType.Default, nullable: false),
                new Field("FKTABLE_CAT", StringType.Default, nullable: true),
                new Field("FKTABLE_SCHEM", StringType.Default, nullable: true),
                new Field("FKTABLE_NAME", StringType.Default, nullable: false),
                new Field("FKCOLUMN_NAME", StringType.Default, nullable: false),
                new Field("KEQ_SEQ", Int32Type.Default, nullable: false),  // Int32 to match Thrift
                new Field("UPDATE_RULE", Int16Type.Default, nullable: true),  // SEA-specific field, not in Thrift
                new Field("DELETE_RULE", Int16Type.Default, nullable: true),  // SEA-specific field, not in Thrift
                new Field("FK_NAME", StringType.Default, nullable: true),
                new Field("PK_NAME", StringType.Default, nullable: true),
                new Field("DEFERRABILITY", Int16Type.Default, nullable: false)  // SEA-specific field, not in Thrift
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
                    activity?.SetTag("error", "Foreign table name is required");
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    throw new ArgumentNullException(nameof(fkTable), "Foreign table name is required for GetCrossReference");
                }

                // Use session defaults for foreign table if not provided
                fkCatalog = fkCatalog ?? _catalog;
                fkSchema = fkSchema ?? _schema;

                activity?.SetTag("pk_catalog", pkCatalog ?? "(all)");
                activity?.SetTag("pk_schema", pkSchema ?? "(all)");
                activity?.SetTag("pk_table", pkTable ?? "(all)");
                activity?.SetTag("fk_catalog", fkCatalog ?? "(session)");
                activity?.SetTag("fk_schema", fkSchema ?? "(session)");
                activity?.SetTag("fk_table", fkTable);

                // Execute SHOW FOREIGN KEYS for the CHILD (foreign) table
                // This returns all foreign keys from the child table
                var allForeignKeys = await GetImportedKeysAsync(fkCatalog, fkSchema, fkTable).ConfigureAwait(false);

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

            while (true)
            {
                using var batch = await allForeignKeys.ReadNextRecordBatchAsync().ConfigureAwait(false);
                if (batch == null) break;

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
            });
        }

        // TracingConnection provides IActivityTracer implementation
        public override string AssemblyVersion => GetType().Assembly.GetName().Version?.ToString() ?? "1.0.0";
        public override string AssemblyName => "AdbcDrivers.Databricks";
    }
}
