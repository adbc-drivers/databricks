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
            // Fetch data based on depth
            var catalogs = new List<string>();
            var catalogSchemas = new Dictionary<string, List<string>>();
            var catalogSchemaTables = new Dictionary<string, Dictionary<string, List<(string tableName, string tableType)>>>();
            var catalogSchemaTableColumns = new Dictionary<string, Dictionary<string, Dictionary<string, List<ColumnInfo>>>>();

            // Fetch catalogs
            if (depth >= GetObjectsDepth.Catalogs)
            {
                catalogs = await GetCatalogsAsync(catalogPattern).ConfigureAwait(false);

                // Fetch schemas
                if (depth >= GetObjectsDepth.DbSchemas)
                {
                    foreach (var catalog in catalogs)
                    {
                        var schemas = await GetSchemasAsync(catalog, schemaPattern).ConfigureAwait(false);
                        catalogSchemas[catalog] = schemas;

                        // Fetch tables
                        if (depth >= GetObjectsDepth.Tables)
                        {
                            if (!catalogSchemaTables.ContainsKey(catalog))
                            {
                                catalogSchemaTables[catalog] = new Dictionary<string, List<(string, string)>>();
                            }

                            foreach (var schema in schemas)
                            {
                                var tables = await GetTablesAsync(catalog, schema, tableNamePattern, tableTypes).ConfigureAwait(false);
                                catalogSchemaTables[catalog][schema] = tables;

                                // Fetch columns (only for All depth)
                                if (depth == GetObjectsDepth.All)
                                {
                                    if (!catalogSchemaTableColumns.ContainsKey(catalog))
                                    {
                                        catalogSchemaTableColumns[catalog] = new Dictionary<string, Dictionary<string, List<ColumnInfo>>>();
                                    }
                                    if (!catalogSchemaTableColumns[catalog].ContainsKey(schema))
                                    {
                                        catalogSchemaTableColumns[catalog][schema] = new Dictionary<string, List<ColumnInfo>>();
                                    }

                                    foreach (var (tableName, _) in tables)
                                    {
                                        var columns = await GetColumnsAsync(catalog, schema, tableName, columnNamePattern).ConfigureAwait(false);
                                        catalogSchemaTableColumns[catalog][schema][tableName] = columns;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Build and return result
            return BuildGetObjectsResult(depth, catalogs, catalogSchemas, catalogSchemaTables, catalogSchemaTableColumns);
        }

        private async Task<List<string>> GetCatalogsAsync(string? catalogPattern)
        {
            var sql = "SHOW CATALOGS";
            if (!string.IsNullOrEmpty(catalogPattern))
            {
                sql += $" LIKE '{EscapeSqlPattern(catalogPattern)}'";
            }

            var batches = await ExecuteSqlQueryAsync(sql).ConfigureAwait(false);
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
                        if (!string.IsNullOrEmpty(catalogPattern) && !PatternMatches(catalog, catalogPattern))
                            continue;
                        catalogs.Add(catalog);
                    }
                }
            }

            return catalogs;
        }

        private async Task<List<string>> GetSchemasAsync(string catalog, string? schemaPattern)
        {
            var sql = $"SHOW SCHEMAS IN {QuoteIdentifier(catalog)}";
            if (!string.IsNullOrEmpty(schemaPattern))
            {
                sql += $" LIKE '{EscapeSqlPattern(schemaPattern)}'";
            }

            var batches = await ExecuteSqlQueryAsync(sql).ConfigureAwait(false);
            var schemas = new List<string>();

            foreach (var batch in batches)
            {
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
                        var schema = schemaArray.GetString(i);
                        if (!string.IsNullOrEmpty(schemaPattern) && !PatternMatches(schema, schemaPattern))
                            continue;
                        schemas.Add(schema);
                    }
                }
            }

            return schemas;
        }

        private async Task<List<(string tableName, string tableType)>> GetTablesAsync(string catalog, string schema, string? tableNamePattern, IReadOnlyList<string>? tableTypes)
        {
            var sql = $"SHOW TABLES IN {QuoteIdentifier(catalog)}.{QuoteIdentifier(schema)}";
            if (!string.IsNullOrEmpty(tableNamePattern))
            {
                sql += $" LIKE '{EscapeSqlPattern(tableNamePattern)}'";
            }

            var batches = await ExecuteSqlQueryAsync(sql).ConfigureAwait(false);
            var tables = new List<(string, string)>();

            foreach (var batch in batches)
            {
                var tableNameArray = batch.Column("tableName") as StringArray;
                var isTemporaryArray = batch.Column("isTemporary") as BooleanArray;

                if (tableNameArray == null) continue;

                for (int i = 0; i < batch.Length; i++)
                {
                    if (tableNameArray.IsNull(i))
                        continue;

                    var tableName = tableNameArray.GetString(i);

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

                    // Apply table name pattern if specified
                    if (!string.IsNullOrEmpty(tableNamePattern) && !PatternMatches(tableName, tableNamePattern))
                        continue;

                    // Apply table type filter if specified
                    if (tableTypes != null && tableTypes.Count > 0 && !tableTypes.Contains(tableType))
                        continue;

                    tables.Add((tableName, tableType));
                }
            }

            return tables;
        }

        private async Task<List<ColumnInfo>> GetColumnsAsync(string catalog, string schema, string tableName, string? columnNamePattern)
        {
            var qualifiedTableName = BuildQualifiedTableName(catalog, schema, tableName);
            var sql = $"DESCRIBE TABLE {qualifiedTableName}";

            List<RecordBatch> batches;
            try
            {
                batches = await ExecuteSqlQueryAsync(sql).ConfigureAwait(false);
            }
            catch
            {
                // If DESCRIBE fails, return empty list
                return new List<ColumnInfo>();
            }

            var columns = new List<ColumnInfo>();
            int position = 0;

            foreach (var batch in batches)
            {
                var colNameArray = batch.Column("col_name") as StringArray;
                var dataTypeArray = batch.Column("data_type") as StringArray;
                var commentArray = batch.Column("comment") as StringArray;

                if (colNameArray == null || dataTypeArray == null)
                    continue;

                for (int i = 0; i < batch.Length; i++)
                {
                    if (colNameArray.IsNull(i) || dataTypeArray.IsNull(i))
                        continue;

                    var colName = colNameArray.GetString(i);
                    var dataType = dataTypeArray.GetString(i);

                    // Skip metadata rows
                    if (string.IsNullOrEmpty(colName) ||
                        colName.StartsWith("#") ||
                        dataType.Contains("Partition Information") ||
                        dataType.Contains("# col_name"))
                    {
                        continue;
                    }

                    // Apply column name pattern if specified
                    if (!string.IsNullOrEmpty(columnNamePattern) && !PatternMatches(colName, columnNamePattern))
                        continue;

                    var comment = commentArray != null && !commentArray.IsNull(i) ? commentArray.GetString(i) : null;

                    columns.Add(new ColumnInfo
                    {
                        Name = colName,
                        TypeName = dataType,
                        Position = position++,
                        Nullable = true,
                        Comment = comment
                    });
                }
            }

            return columns;
        }

        private IArrowArrayStream BuildGetObjectsResult(
            GetObjectsDepth depth,
            List<string> catalogs,
            Dictionary<string, List<string>> catalogSchemas,
            Dictionary<string, Dictionary<string, List<(string tableName, string tableType)>>> catalogSchemaTables,
            Dictionary<string, Dictionary<string, Dictionary<string, List<ColumnInfo>>>> catalogSchemaTableColumns)
        {
            // For now, return a simplified result structure
            // TODO: Implement full nested ListArray/StructArray structure for All depth
            // This matches the limitation documented in CURRENT_STATUS.md

            if (depth == GetObjectsDepth.Catalogs)
            {
                var catalogBuilder = new StringArray.Builder();
                foreach (var catalog in catalogs)
                {
                    catalogBuilder.Append(catalog);
                }

                var schema = new Schema(new[] { new Field("catalog_name", StringType.Default, nullable: true) }, null);
                var data = new List<IArrowArray> { catalogBuilder.Build() };
                return new SimpleArrowArrayStream(schema, data);
            }
            else if (depth == GetObjectsDepth.DbSchemas)
            {
                var catalogBuilder = new StringArray.Builder();
                var schemaBuilder = new StringArray.Builder();

                foreach (var kvp in catalogSchemas)
                {
                    foreach (var schema in kvp.Value)
                    {
                        catalogBuilder.Append(kvp.Key);
                        schemaBuilder.Append(schema);
                    }
                }

                var resultSchema = new Schema(new[]
                {
                    new Field("catalog_name", StringType.Default, nullable: true),
                    new Field("db_schema_name", StringType.Default, nullable: true)
                }, null);
                var data = new List<IArrowArray> { catalogBuilder.Build(), schemaBuilder.Build() };
                return new SimpleArrowArrayStream(resultSchema, data);
            }
            else if (depth == GetObjectsDepth.Tables)
            {
                var catalogBuilder = new StringArray.Builder();
                var schemaBuilder = new StringArray.Builder();
                var tableBuilder = new StringArray.Builder();
                var typeBuilder = new StringArray.Builder();

                foreach (var catKvp in catalogSchemaTables)
                {
                    foreach (var schemaKvp in catKvp.Value)
                    {
                        foreach (var (tableName, tableType) in schemaKvp.Value)
                        {
                            catalogBuilder.Append(catKvp.Key);
                            schemaBuilder.Append(schemaKvp.Key);
                            tableBuilder.Append(tableName);
                            typeBuilder.Append(tableType);
                        }
                    }
                }

                var resultSchema = new Schema(new[]
                {
                    new Field("catalog_name", StringType.Default, nullable: true),
                    new Field("db_schema_name", StringType.Default, nullable: true),
                    new Field("table_name", StringType.Default, nullable: true),
                    new Field("table_type", StringType.Default, nullable: true)
                }, null);
                var data = new List<IArrowArray> { catalogBuilder.Build(), schemaBuilder.Build(), tableBuilder.Build(), typeBuilder.Build() };
                return new SimpleArrowArrayStream(resultSchema, data);
            }
            else // GetObjectsDepth.All
            {
                // Build full ADBC nested structure with catalog->schema->table->column hierarchy
                var catalogNameBuilder = new StringArray.Builder();
                var catalogDbSchemasValues = new List<IArrowArray?>();

                foreach (var catalogEntry in catalogSchemaTables)
                {
                    catalogNameBuilder.Append(catalogEntry.Key);

                    // Build db_schemas struct for this catalog
                    var schemaMap = catalogEntry.Value;
                    if (!catalogSchemaTableColumns.TryGetValue(catalogEntry.Key, out var schemaTableColumns))
                    {
                        schemaTableColumns = new Dictionary<string, Dictionary<string, List<ColumnInfo>>>();
                    }

                    catalogDbSchemasValues.Add(BuildDbSchemasStruct(depth, schemaMap, schemaTableColumns));
                }

                var resultSchema = StandardSchemas.GetObjectsSchema;
                var dataArrays = resultSchema.Validate(new List<IArrowArray>
                {
                    catalogNameBuilder.Build(),
                    catalogDbSchemasValues.BuildListArrayForType(new StructType(StandardSchemas.DbSchemaSchema))
                });

                return new SimpleArrowArrayStream(resultSchema, dataArrays);
            }
        }

        /// <summary>
        /// Gets all supported table types in the database.
        /// Statement Execution API returns TABLE, VIEW, and LOCAL TEMPORARY.
        /// </summary>
        /// <returns>Arrow stream with single column 'table_type' containing supported table type names</returns>
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
            var tableTypesBuilder = new StringArray.Builder();
            tableTypesBuilder.Append("TABLE");
            tableTypesBuilder.Append("VIEW");
            tableTypesBuilder.Append("LOCAL TEMPORARY");

            var schema = new Schema(new[] { new Field("table_type", StringType.Default, nullable: false) }, null);
            var data = new List<IArrowArray> { tableTypesBuilder.Build() };

            return new SimpleArrowArrayStream(schema, data);
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
            // Use session catalog/schema if not provided
            catalog = catalog ?? _catalog;
            dbSchema = dbSchema ?? _schema;

            var qualifiedTableName = BuildQualifiedTableName(catalog, dbSchema, tableName);
            var sql = $"DESCRIBE TABLE {qualifiedTableName}";

            List<RecordBatch> batches;
            try
            {
                batches = await ExecuteSqlQueryAsync(sql).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new AdbcException($"Failed to describe table {qualifiedTableName}: {ex.Message}", ex);
            }

            if (batches == null || batches.Count == 0)
            {
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
                throw new AdbcException($"Table {qualifiedTableName} has no columns");
            }

            return new Schema(fields, null);
        }

        #region Metadata Implementation Helpers

        private async Task<List<RecordBatch>> ExecuteSqlQueryAsync(string sql)
        {
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

        private string EscapeSqlPattern(string pattern)
        {
            return pattern.Replace("'", "''");
        }

        private string BuildQualifiedTableName(string? catalog, string? schema, string tableName)
        {
            var parts = new List<string>();

            if (!string.IsNullOrEmpty(catalog))
                parts.Add(QuoteIdentifier(catalog));

            if (!string.IsNullOrEmpty(schema))
                parts.Add(QuoteIdentifier(schema));

            parts.Add(QuoteIdentifier(tableName));

            return string.Join(".", parts);
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
            Dictionary<string, List<(string tableName, string tableType)>> schemaMap,
            Dictionary<string, Dictionary<string, List<ColumnInfo>>> schemaTableColumns)
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
                    // Build tables struct for this schema
                    var tableMap = schemaEntry.Value;
                    if (!schemaTableColumns.TryGetValue(schemaEntry.Key, out var tableColumns))
                    {
                        tableColumns = new Dictionary<string, List<ColumnInfo>>();
                    }
                    dbSchemaTablesValues.Add(BuildTablesStruct(depth, tableMap, tableColumns));
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
            List<(string tableName, string tableType)> tableMap,
            Dictionary<string, List<ColumnInfo>> tableColumns)
        {
            var tableNameBuilder = new StringArray.Builder();
            var tableTypeBuilder = new StringArray.Builder();
            var tableColumnsValues = new List<IArrowArray?>();
            var tableConstraintsValues = new List<IArrowArray?>();
            var nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;

            foreach (var (tableName, tableType) in tableMap)
            {
                tableNameBuilder.Append(tableName);
                tableTypeBuilder.Append(tableType);
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
                    if (!tableColumns.TryGetValue(tableName, out var columns))
                    {
                        columns = new List<ColumnInfo>();
                    }
                    tableColumnsValues.Add(BuildColumnsStruct(columns));
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
                remarksBuilder.Append(column.Comment ?? column.TypeName);
                xdbcDataTypeBuilder.AppendNull(); // XDBC type not available
                xdbcTypeNameBuilder.Append(column.TypeName);
                xdbcColumnSizeBuilder.AppendNull(); // Size not available
                xdbcDecimalDigitsBuilder.AppendNull(); // Precision not available
                xdbcNumPrecRadixBuilder.AppendNull();
                xdbcNullableBuilder.Append((short)(column.Nullable ? 1 : 0));
                xdbcColumnDefBuilder.AppendNull();
                xdbcSqlDataTypeBuilder.AppendNull();
                xdbcDatetimeSubBuilder.AppendNull();
                xdbcCharOctetLengthBuilder.AppendNull();
                xdbcIsNullableBuilder.Append(column.Nullable ? "YES" : "NO");
                xdbcScopeCatalogBuilder.AppendNull();
                xdbcScopeSchemaBuilder.AppendNull();
                xdbcScopeTableBuilder.AppendNull();
                xdbcIsAutoincrementBuilder.Append(false); // Auto-increment not available
                xdbcIsGeneratedcolumnBuilder.Append(false); // Generated column not available
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
            public string Name { get; set; }
            public string TypeName { get; set; }
            public int Position { get; set; }
            public bool Nullable { get; set; }
            public string? Comment { get; set; }
        }

        private class SimpleArrowArrayStream : IArrowArrayStream
        {
            private Schema _schema;
            private RecordBatch? _batch;

            public SimpleArrowArrayStream(Schema schema, IReadOnlyList<IArrowArray> data)
            {
                _schema = schema;
                _batch = new RecordBatch(schema, data, data[0].Length);
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
            // Info value type IDs for DenseUnionType
            const int strValTypeID = 0;
            const int boolValTypeId = 1;

            // Supported info codes for Statement Execution API
            var supportedCodes = new[]
            {
                AdbcInfoCode.VendorName,
                AdbcInfoCode.VendorVersion,
                AdbcInfoCode.VendorArrowVersion,
                AdbcInfoCode.VendorSql,
                AdbcInfoCode.DriverName,
                AdbcInfoCode.DriverVersion,
                AdbcInfoCode.DriverArrowVersion
            };

            // If no codes specified, return all supported codes
            if (codes == null || codes.Count == 0)
            {
                codes = supportedCodes;
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

                    case AdbcInfoCode.VendorArrowVersion:
                        infoNameBuilder.Append((uint)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(offset++);
                        stringInfoBuilder.Append("17.0.0"); // Apache Arrow version
                        booleanInfoBuilder.AppendNull();
                        break;

                    case AdbcInfoCode.VendorSql:
                        infoNameBuilder.Append((uint)code);
                        typeBuilder.Append(boolValTypeId);
                        offsetBuilder.Append(offset++);
                        stringInfoBuilder.AppendNull();
                        booleanInfoBuilder.Append(false); // Databricks uses Spark SQL, not standard SQL
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
                        stringInfoBuilder.Append("17.0.0"); // Apache Arrow version used by driver
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
            return new SimpleArrowArrayStream(schema, dataArrays);
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
