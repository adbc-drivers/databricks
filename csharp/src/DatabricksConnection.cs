/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* This file has been modified from its original version, which is
* under the Apache License:
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
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
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Auth;
using AdbcDrivers.Databricks.Http;
using AdbcDrivers.Databricks.Reader;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Proto;
using AdbcDrivers.Databricks.Telemetry.TagDefinitions;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.Hive2;
using AdbcDrivers.HiveServer2.Spark;
using AdbcDrivers.HiveServer2.Thrift;
using Apache.Arrow.Adbc.Tracing;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift.Protocol;

namespace AdbcDrivers.Databricks
{
    internal class DatabricksConnection : SparkHttpConnection
    {
        internal static new readonly string s_assemblyName = ApacheUtility.GetAssemblyName(typeof(DatabricksConnection));
        internal static new readonly string s_assemblyVersion = ApacheUtility.GetAssemblyVersion(typeof(DatabricksConnection));

        /// <summary>
        /// The environment variable name that contains the path to the default Databricks configuration file.
        /// </summary>
        public const string DefaultConfigEnvironmentVariable = "DATABRICKS_CONFIG_FILE";

        public const string DefaultInitialSchema = "default";

        internal static readonly Dictionary<string, string> timestampConfig = new Dictionary<string, string>
        {
            { "spark.thriftserver.arrowBasedRowSet.timestampAsString", "false" },
        };
        private bool _applySSPWithQueries = false;
        private bool _enableDirectResults = true;
        private bool _enableMultipleCatalogSupport = true;
        private bool _enablePKFK = true;
        private bool _runAsyncInThrift = true;

        // DirectQuery configuration
        private const long DefaultDirectResultMaxBytes = 10 * 1024 * 1024; // 10MB for direct query results size limit
        private const long DefaultDirectResultMaxRows = 500 * 1000; // upper limit for 10MB result assume smallest 20 Byte column
        private long _directResultMaxBytes = DefaultDirectResultMaxBytes;
        private long _directResultMaxRows = DefaultDirectResultMaxRows;
        // CloudFetch configuration
        private const long DefaultMaxBytesPerFile = 20 * 1024 * 1024; // 20MB
        private const int DefaultQueryTimeSeconds = 3 * 60 * 60; // 3 hours
        private bool _useCloudFetch = true;
        private bool _canDecompressLz4 = true;
        private long _maxBytesPerFile = DefaultMaxBytesPerFile;
        private const long DefaultMaxBytesPerFetchRequest = 400 * 1024 * 1024; // 400MB
        private long _maxBytesPerFetchRequest = DefaultMaxBytesPerFetchRequest;
        private const bool DefaultRetryOnUnavailable = true;
        private const bool DefaultRateLimitRetry = true;
        private bool _useDescTableExtended = false;

        // Trace propagation configuration
        private bool _tracePropagationEnabled = true;
        private string _traceParentHeaderName = "traceparent";
        private bool _traceStateEnabled = false;

        // Identity federation client ID for token exchange
        private string? _identityFederationClientId;

        // Heartbeat interval configuration
        private int _fetchHeartbeatIntervalSeconds = DatabricksConstants.DefaultOperationStatusPollingIntervalSeconds;

        // Request timeout configuration
        private int _operationStatusRequestTimeoutSeconds = DatabricksConstants.DefaultOperationStatusRequestTimeoutSeconds;

        // Default namespace
        private TNamespace? _defaultNamespace;

        // Telemetry fields
        private ITelemetryClient? _telemetryClient;
        private MetricsAggregator? _metricsAggregator;
        private TelemetryConfiguration? _telemetryConfig;
        private string? _telemetryHost;

        /// <summary>
        /// Optional factory for creating telemetry exporters in tests.
        /// When set, this factory is used instead of creating the real
        /// <see cref="CircuitBreakerTelemetryExporter"/> wrapping <see cref="DatabricksTelemetryExporter"/>.
        /// </summary>
        internal Func<ITelemetryExporter>? TestExporterFactory { get; set; }

        /// <summary>
        /// RecyclableMemoryStreamManager for LZ4 decompression.
        /// If provided by Database, this is shared across all connections for optimal pooling.
        /// If created directly, each connection has its own pool.
        /// </summary>
        internal Microsoft.IO.RecyclableMemoryStreamManager RecyclableMemoryStreamManager { get; }

        /// <summary>
        /// LZ4 buffer pool for decompression.
        /// If provided by Database, this is shared across all connections for optimal pooling.
        /// If created directly, each connection has its own pool.
        /// </summary>
        internal System.Buffers.ArrayPool<byte> Lz4BufferPool { get; }

        public DatabricksConnection(IReadOnlyDictionary<string, string> properties)
            : this(properties, null, null)
        {
        }

        internal DatabricksConnection(
            IReadOnlyDictionary<string, string> properties,
            Microsoft.IO.RecyclableMemoryStreamManager? memoryStreamManager,
            System.Buffers.ArrayPool<byte>? lz4BufferPool)
            : base(properties)
        {
            // Use provided manager (from Database) or create new instance (for direct construction)
            RecyclableMemoryStreamManager = memoryStreamManager ?? new Microsoft.IO.RecyclableMemoryStreamManager();
            // Use provided pool (from Database) or create new instance (for direct construction)
            Lz4BufferPool = lz4BufferPool ?? System.Buffers.ArrayPool<byte>.Create(maxArrayLength: 4 * 1024 * 1024, maxArraysPerBucket: 10);

            ValidateProperties();
        }

        private void LogConnectionProperties(Activity? activity)
        {
            if (activity == null) return;

            foreach (var kvp in Properties)
            {
                string key = kvp.Key;
                string value = kvp.Value;

                // Sanitize sensitive properties - only mask actual credentials/tokens, not configuration
                bool isSensitive = key.IndexOf("password", StringComparison.OrdinalIgnoreCase) >= 0 ||
                                   key.IndexOf("secret", StringComparison.OrdinalIgnoreCase) >= 0 ||
                                   key.IndexOf("token", StringComparison.OrdinalIgnoreCase) >= 0 ||
                                   key.Equals(AdbcOptions.Password, StringComparison.OrdinalIgnoreCase) ||
                                   key.Equals(SparkParameters.Token, StringComparison.OrdinalIgnoreCase) ||
                                   key.Equals(DatabricksParameters.OAuthClientSecret, StringComparison.OrdinalIgnoreCase);

                string logValue = isSensitive ? "***" : value;

                activity.SetTag(key, logValue);
            }
        }

        public override IEnumerable<KeyValuePair<string, object?>>? GetActivitySourceTags(IReadOnlyDictionary<string, string> properties)
        {
            IEnumerable<KeyValuePair<string, object?>>? tags = base.GetActivitySourceTags(properties);
            // TODO: Add any additional tags specific to Databricks connection
            //tags ??= [];
            //tags.Concat([new("key", "value")]);
            return tags;
        }

        protected override TCLIService.IAsync CreateTCLIServiceClient(TProtocol protocol)
        {
            return new ThreadSafeClient(new TCLIService.Client(protocol));
        }

        private void ValidateProperties()
        {
            _enablePKFK = PropertyHelper.GetBooleanPropertyWithValidation(Properties, DatabricksParameters.EnablePKFK, _enablePKFK);
            _enableMultipleCatalogSupport = PropertyHelper.GetBooleanPropertyWithValidation(Properties, DatabricksParameters.EnableMultipleCatalogSupport, _enableMultipleCatalogSupport);
            _applySSPWithQueries = PropertyHelper.GetBooleanPropertyWithValidation(Properties, DatabricksParameters.ApplySSPWithQueries, _applySSPWithQueries);
            _enableDirectResults = PropertyHelper.GetBooleanPropertyWithValidation(Properties, DatabricksParameters.EnableDirectResults, _enableDirectResults);

            // Parse CloudFetch options from connection properties
            _useCloudFetch = PropertyHelper.GetBooleanPropertyWithValidation(Properties, DatabricksParameters.UseCloudFetch, _useCloudFetch);
            _canDecompressLz4 = PropertyHelper.GetBooleanPropertyWithValidation(Properties, DatabricksParameters.CanDecompressLz4, _canDecompressLz4);
            _useDescTableExtended = PropertyHelper.GetBooleanPropertyWithValidation(Properties, DatabricksParameters.UseDescTableExtended, _useDescTableExtended);
            _runAsyncInThrift = PropertyHelper.GetBooleanPropertyWithValidation(Properties, DatabricksParameters.EnableRunAsyncInThriftOp, _runAsyncInThrift);

            if (Properties.ContainsKey(DatabricksParameters.MaxBytesPerFile))
            {
                _maxBytesPerFile = PropertyHelper.GetPositiveLongPropertyWithValidation(Properties, DatabricksParameters.MaxBytesPerFile, _maxBytesPerFile);
            }

            if (Properties.TryGetValue(DatabricksParameters.MaxBytesPerFetchRequest, out string? maxBytesPerFetchRequestStr))
            {
                try
                {
                    long maxBytesPerFetchRequestValue = ParseBytesWithUnits(maxBytesPerFetchRequestStr);
                    if (maxBytesPerFetchRequestValue < 0)
                    {
                        throw new ArgumentOutOfRangeException(
                            nameof(Properties),
                            maxBytesPerFetchRequestValue,
                            $"Parameter '{DatabricksParameters.MaxBytesPerFetchRequest}' value must be a non-negative integer. Use 0 for no limit.");
                    }
                    _maxBytesPerFetchRequest = maxBytesPerFetchRequestValue;
                }
                catch (FormatException)
                {
                    throw new ArgumentException($"Parameter '{DatabricksParameters.MaxBytesPerFetchRequest}' value '{maxBytesPerFetchRequestStr}' could not be parsed. Valid formats: number with optional unit suffix (B, KB, MB, GB). Examples: '400MB', '1024KB', '1073741824'.");
                }
            }

            // Parse default namespace
            string? defaultCatalog = null;
            string? defaultSchema = null;
            // only if enableMultipleCatalogSupport is true, do we supply catalog from connection properties
            if (_enableMultipleCatalogSupport)
            {
                Properties.TryGetValue(AdbcOptions.Connection.CurrentCatalog, out defaultCatalog);
            }
            Properties.TryGetValue(AdbcOptions.Connection.CurrentDbSchema, out defaultSchema);

            // This maintains backward compatibility with older workspaces, where the Hive metastore was accessed via the spark catalog name.
            // In newer DBR versions with Unity Catalog, the default catalog is typically hive_metastore.
            // Passing null here allows the runtime to fall back to the workspace-defined default catalog for the session.
            defaultCatalog = HandleSparkCatalog(defaultCatalog);
            var ns = new TNamespace();

            ns.SchemaName = string.IsNullOrWhiteSpace(defaultSchema) ? DefaultInitialSchema : defaultSchema;

            if (!string.IsNullOrWhiteSpace(defaultCatalog))
                ns.CatalogName = defaultCatalog!;
            _defaultNamespace = ns;

            // Parse trace propagation options
            _tracePropagationEnabled = PropertyHelper.GetBooleanPropertyWithValidation(Properties, DatabricksParameters.TracePropagationEnabled, _tracePropagationEnabled);
            if (Properties.TryGetValue(DatabricksParameters.TraceParentHeaderName, out string? traceParentHeaderName))
            {
                if (!string.IsNullOrWhiteSpace(traceParentHeaderName))
                {
                    _traceParentHeaderName = traceParentHeaderName;
                }
                else
                {
                    throw new ArgumentException($"Parameter '{DatabricksParameters.TraceParentHeaderName}' cannot be empty.");
                }
            }
            _traceStateEnabled = PropertyHelper.GetBooleanPropertyWithValidation(Properties, DatabricksParameters.TraceStateEnabled, _traceStateEnabled);

            if (!Properties.ContainsKey(ApacheParameters.QueryTimeoutSeconds))
            {
                // Default QueryTimeSeconds in Hive2Connection is only 60s, which is too small for lots of long running query
                QueryTimeoutSeconds = DefaultQueryTimeSeconds;
            }

            if (Properties.TryGetValue(DatabricksParameters.IdentityFederationClientId, out string? identityFederationClientId))
            {
                _identityFederationClientId = identityFederationClientId;
            }

            if (Properties.ContainsKey(DatabricksParameters.FetchHeartbeatInterval))
            {
                _fetchHeartbeatIntervalSeconds = PropertyHelper.GetPositiveIntPropertyWithValidation(Properties, DatabricksParameters.FetchHeartbeatInterval, _fetchHeartbeatIntervalSeconds);
            }

            if (Properties.ContainsKey(DatabricksParameters.OperationStatusRequestTimeout))
            {
                _operationStatusRequestTimeoutSeconds = PropertyHelper.GetPositiveIntPropertyWithValidation(Properties, DatabricksParameters.OperationStatusRequestTimeout, _operationStatusRequestTimeoutSeconds);
            }
        }

        /// <summary>
        /// Gets whether server side properties should be applied using queries.
        /// </summary>
        internal bool ApplySSPWithQueries => _applySSPWithQueries;

        /// <summary>
        /// Gets whether direct results are enabled.
        /// </summary>
        internal bool EnableDirectResults => _enableDirectResults;

        /// <inheritdoc/>
        protected internal override bool TrySetGetDirectResults(IRequest request)
        {
            if (EnableDirectResults)
            {
                request.GetDirectResults = new()
                {
                    MaxRows = _directResultMaxRows,
                    MaxBytes = _directResultMaxBytes
                };
                return true;
            }
            return false;
        }

        /// <summary>
        /// Gets the maximum bytes per fetch block for directResult
        /// </summary>
        internal long DirectResultMaxBytes => _directResultMaxBytes;

        /// <summary>
        /// Gets the maximum rows per fetch block for directResult
        /// </summary>
        internal long DirectResultMaxRows => _directResultMaxRows;

        /// <summary>
        /// Gets whether CloudFetch is enabled.
        /// </summary>
        internal bool UseCloudFetch => _useCloudFetch;

        /// <summary>
        /// Gets whether LZ4 decompression is enabled.
        /// </summary>
        internal bool CanDecompressLz4 => _canDecompressLz4;

        /// <summary>
        /// Gets the maximum bytes per file for CloudFetch.
        /// </summary>
        internal long MaxBytesPerFile => _maxBytesPerFile;

        /// <summary>
        /// Gets the maximum bytes per fetch request.
        /// </summary>
        internal long MaxBytesPerFetchRequest => _maxBytesPerFetchRequest;

        /// <summary>
        /// Gets the default namespace to use for SQL queries.
        /// </summary>
        internal TNamespace? DefaultNamespace => _defaultNamespace;

        /// <summary>
        /// Gets the heartbeat interval in seconds for long-running operations.
        /// </summary>
        internal int FetchHeartbeatIntervalSeconds => _fetchHeartbeatIntervalSeconds;

        /// <summary>
        /// Gets the request timeout in seconds for operation status polling requests.
        /// </summary>
        internal int OperationStatusRequestTimeoutSeconds => _operationStatusRequestTimeoutSeconds;

        /// <summary>
        /// Gets whether multiple catalog is supported
        /// </summary>
        internal bool EnableMultipleCatalogSupport => _enableMultipleCatalogSupport;

        /// <summary>
        /// Check if current connection can use `DESC TABLE EXTENDED` query
        /// </summary>
        internal bool CanUseDescTableExtended => _useDescTableExtended && ServerProtocolVersion != null && FeatureVersionNegotiator.SupportsDESCTableExtended(ServerProtocolVersion.Value);

        /// <summary>
        /// Gets whether PK/FK metadata call is enabled
        /// </summary>
        public bool EnablePKFK => _enablePKFK;

        /// <summary>
        /// Enable RunAsync flag in Thrift Operation
        /// </summary>
        public bool RunAsyncInThrift => _runAsyncInThrift;

        /// <summary>
        /// Gets a value indicating whether to retry requests that receive retryable responses (408, 502, 503, 504) .
        /// </summary>
        protected bool TemporarilyUnavailableRetry { get; private set; } = DefaultRetryOnUnavailable;

        /// <summary>
        /// Gets the maximum total time in seconds to retry retryable responses (408, 502, 503, 504) before failing.
        /// </summary>
        protected int TemporarilyUnavailableRetryTimeout { get; private set; } = DatabricksConstants.DefaultTemporarilyUnavailableRetryTimeout;

        /// <summary>
        /// Gets a value indicating whether to retry requests that receive HTTP 429 responses.
        /// </summary>
        protected bool RateLimitRetry { get; private set; } = DefaultRateLimitRetry;

        /// <summary>
        /// Gets the number of seconds to wait before stopping an attempt to retry HTTP 429 responses.
        /// </summary>
        protected int RateLimitRetryTimeout { get; private set; } = DatabricksConstants.DefaultRateLimitRetryTimeout;

        protected override HttpMessageHandler CreateHttpHandler()
        {
            HttpMessageHandler baseHandler = base.CreateHttpHandler();
            HttpMessageHandler baseAuthHandler = HiveServer2TlsImpl.NewHttpClientHandler(TlsOptions, _proxyConfigurator);

            var config = new HttpHandlerFactory.HandlerConfig
            {
                BaseHandler = baseHandler,
                BaseAuthHandler = baseAuthHandler,
                Properties = Properties,
                Host = GetHost(),
                ActivityTracer = this,
                TracePropagationEnabled = _tracePropagationEnabled,
                TraceParentHeaderName = _traceParentHeaderName,
                TraceStateEnabled = _traceStateEnabled,
                IdentityFederationClientId = _identityFederationClientId,
                TemporarilyUnavailableRetry = TemporarilyUnavailableRetry,
                TemporarilyUnavailableRetryTimeout = TemporarilyUnavailableRetryTimeout,
                RateLimitRetry = RateLimitRetry,
                RateLimitRetryTimeout = RateLimitRetryTimeout,
                TimeoutMinutes = 1,
                AddThriftErrorHandler = true
            };

            var result = HttpHandlerFactory.CreateHandlers(config);
            return result;
        }

        protected override bool GetObjectsPatternsRequireLowerCase => true;

        protected override string DriverName => "ADBC Databricks Driver";

        internal override IArrowArrayStream NewReader<T>(T statement, Schema schema, IResponse response, TGetResultSetMetadataResp? metadataResp = null)
        {
            bool isLz4Compressed = false;

            DatabricksStatement? databricksStatement = statement as DatabricksStatement;

            if (databricksStatement == null)
            {
                throw new InvalidOperationException("Cannot obtain a reader for Databricks");
            }

            if (metadataResp != null && metadataResp.__isset.lz4Compressed)
            {
                isLz4Compressed = metadataResp.Lz4Compressed;
            }

            HttpClient httpClient = HttpClientFactory.CreateCloudFetchHttpClient(Properties);

            // Extract telemetry IDs for propagation to reader activities
            string? telemetrySessionId = null;
            if (SessionHandle?.SessionId?.Guid != null && SessionHandle.SessionId.Guid.Length == 16)
            {
                telemetrySessionId = new Guid(SessionHandle.SessionId.Guid).ToString("N");
            }
            string? telemetryStatementId = null;
            if (response.OperationHandle?.OperationId?.Guid != null && response.OperationHandle.OperationId.Guid.Length == 16)
            {
                telemetryStatementId = new Guid(response.OperationHandle.OperationId.Guid).ToString("N");
            }

            return new DatabricksCompositeReader(databricksStatement, schema, response, isLz4Compressed, httpClient,
                telemetrySessionId: telemetrySessionId,
                telemetryStatementId: telemetryStatementId);
        }

        internal override SchemaParser SchemaParser => new DatabricksSchemaParser();

        public override AdbcStatement CreateStatement()
        {
            DatabricksStatement statement = new DatabricksStatement(this);
            return statement;
        }

        protected override TOpenSessionReq CreateSessionRequest()
        {
            return this.TraceActivity(activity =>
            {
                // Log driver information at the beginning of the connection
                activity?.AddEvent("connection.driver.info", [
                    new("driver.name", "Apache Arrow ADBC Databricks Driver"),
                    new("driver.version", s_assemblyVersion),
                    new("driver.assembly", s_assemblyName)
                ]);

                // Log connection properties (sanitize sensitive values)
                LogConnectionProperties(activity);

                var req = new TOpenSessionReq
                {
                    Client_protocol = TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7,
                    Client_protocol_i64 = (long)TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7,
                    CanUseMultipleCatalogs = _enableMultipleCatalogSupport,
                };

                // Log OpenSession request details
                activity?.SetTag("connection.client_protocol", req.Client_protocol.ToString());

                // Set default namespace if available
                if (_defaultNamespace != null)
                {
                    req.InitialNamespace = _defaultNamespace;
                    activity?.SetTag("connection.initial_namespace.catalog", _defaultNamespace.CatalogName ?? "(none)");
                    activity?.SetTag("connection.initial_namespace.schema", _defaultNamespace.SchemaName ?? "(none)");
                }
                req.Configuration = new Dictionary<string, string>();
                // merge timestampConfig with serverSideProperties
                foreach (var kvp in timestampConfig)
                {
                    req.Configuration[kvp.Key] = kvp.Value;
                }
                // If not using queries to set server-side properties, include them in Configuration
                if (!_applySSPWithQueries)
                {
                    var serverSideProperties = GetServerSideProperties(activity);
                    foreach (var property in serverSideProperties)
                    {
                        req.Configuration[property.Key] = property.Value;
                    }
                }

                activity?.SetTag("connection.configuration_count", req.Configuration.Count);

                return req;
            });
        }

        protected override async Task HandleOpenSessionResponse(TOpenSessionResp? session, Activity? activity = default)
        {

            await base.HandleOpenSessionResponse(session, activity);

            if (session == null)
            {
                activity?.SetTag("error.type", "NullSessionResponse");
                return;
            }

            var version = session.ServerProtocolVersion;

            // Log server protocol version
            activity?.SetTag("connection.server_protocol_version", version.ToString());

            // Validate it's a Databricks server
            if (!FeatureVersionNegotiator.IsDatabricksProtocolVersion(version))
            {
                var exception = new DatabricksException("Attempted to use databricks driver with a non-databricks server");
                activity?.AddException(exception, [
                    new("error.type", "InvalidServerProtocol")
                ]);
                throw exception;
            }

            // Log protocol version capabilities (what the server supports)
            bool protocolSupportsPKFK = FeatureVersionNegotiator.SupportsPKFK(version);
            bool protocolSupportsDescTableExtended = FeatureVersionNegotiator.SupportsDESCTableExtended(version);

            activity?.SetTag("connection.protocol.supports_pk_fk", protocolSupportsPKFK);
            activity?.SetTag("connection.protocol.supports_desc_table_extended", protocolSupportsDescTableExtended);

            // Apply protocol constraints to user settings
            bool pkfkBefore = _enablePKFK;
            _enablePKFK = _enablePKFK && protocolSupportsPKFK;

            if (pkfkBefore && !_enablePKFK)
            {
                activity?.SetTag("connection.feature_downgrade.pk_fk", true);
                activity?.SetTag("connection.feature_downgrade.pk_fk.reason", "Protocol version does not support PK/FK");
            }

            // Handle multiple catalog support from server response
            _enableMultipleCatalogSupport = session.__isset.canUseMultipleCatalogs ? session.CanUseMultipleCatalogs : false;

            // Log final feature flags as tags
            activity?.SetTag("connection.feature.enable_pk_fk", _enablePKFK);
            activity?.SetTag("connection.feature.enable_multiple_catalog_support", _enableMultipleCatalogSupport);
            activity?.SetTag("connection.feature.enable_direct_results", _enableDirectResults);
            activity?.SetTag("connection.feature.use_cloud_fetch", _useCloudFetch);
            activity?.SetTag("connection.feature.use_desc_table_extended", _useDescTableExtended);
            activity?.SetTag("connection.feature.enable_run_async_in_thrift_op", _runAsyncInThrift);

            // Handle default namespace
            if (session.__isset.initialNamespace)
            {
                _defaultNamespace = session.InitialNamespace;
                activity?.AddEvent("connection.namespace.set_from_server", [
                    new("catalog", _defaultNamespace.CatalogName ?? "(none)"),
                    new("schema", _defaultNamespace.SchemaName ?? "(none)")
                ]);
            }
            else if (_defaultNamespace != null && !string.IsNullOrEmpty(_defaultNamespace.SchemaName))
            {
                // catalog in namespace is introduced when SET CATALOG is introduced, so we don't need to fallback
                // server version is too old. Explicitly set the schema using queries
                activity?.AddEvent("connection.namespace.fallback_to_use_schema", [
                    new("schema_name", _defaultNamespace.SchemaName),
                    new("reason", "Server does not support initialNamespace in OpenSessionResp")
                ]);
                await SetSchema(_defaultNamespace.SchemaName);
            }

            // Set connection-level telemetry tags for MetricsAggregator extraction
            SetConnectionTelemetryTags(activity);

            // Initialize telemetry pipeline after session is established
            InitializeTelemetry(activity);
        }

        // Since Databricks Namespace was introduced in newer versions, we fallback to USE SCHEMA to set default schema
        // in case the server version is too low.
        private async Task SetSchema(string schemaName)
        {
            using var statement = new DatabricksStatement(this);
            statement.SqlQuery = $"USE {schemaName}";
            await statement.ExecuteUpdateAsync();
        }

        /// <summary>
        /// Gets a dictionary of server-side properties extracted from connection properties.
        /// Only includes properties with valid property names (letters, numbers, dots, and underscores).
        /// Invalid property names are logged to the activity trace and filtered out.
        /// </summary>
        /// <param name="activity">Optional activity for tracing filtered properties.</param>
        /// <returns>Dictionary of server-side properties with prefix removed from keys and invalid names filtered out.</returns>
        private Dictionary<string, string> GetServerSideProperties(Activity? activity = null)
        {
            var result = new Dictionary<string, string>();

            foreach (var property in Properties.Where(p => p.Key.ToLowerInvariant().StartsWith(DatabricksParameters.ServerSidePropertyPrefix)))
            {
                string propertyName = property.Key.Substring(DatabricksParameters.ServerSidePropertyPrefix.Length);

                if (!IsValidPropertyName(propertyName))
                {
                    activity?.AddEvent("connection.server_side_property.filtered", [
                        new("property_name", propertyName),
                        new("reason", "Invalid property name format")
                    ]);
                    continue;
                }

                result[propertyName] = property.Value;
            }

            return result;
        }

        /// <summary>
        /// Applies server-side properties by executing "set key=value" queries.
        /// </summary>
        /// <returns>A task representing the asynchronous operation.</returns>
        public async Task ApplyServerSidePropertiesAsync()
        {
            await this.TraceActivityAsync(async activity =>
            {
                if (!_applySSPWithQueries)
                {
                    return;
                }

                var serverSideProperties = GetServerSideProperties(activity);

                if (serverSideProperties.Count == 0)
                {
                    return;
                }

                activity?.SetTag("connection.server_side_properties.count", serverSideProperties.Count);

                using var statement = new DatabricksStatement(this);

                foreach (var property in serverSideProperties)
                {
                    string escapedValue = EscapeSqlString(property.Value);
                    string query = $"SET {property.Key}={escapedValue}";
                    statement.SqlQuery = query;

                    try
                    {
                        await statement.ExecuteUpdateAsync();
                    }
                    catch (Exception ex)
                    {
                        activity?.AddEvent("connection.server_side_property.set_failed", [
                            new("property_name", property.Key),
                            new("error_message", ex.Message)
                        ]);
                    }
                }
            });
        }

        internal bool IsValidPropertyName(string propertyName)
        {
            // Allow property names with letters, numbers, dots, and underscores
            // Examples: spark.sql.adaptive.enabled, spark.executor.instances, my_property123
            return System.Text.RegularExpressions.Regex.IsMatch(
                propertyName,
                @"^[a-zA-Z0-9_.]+$");
        }

        private string EscapeSqlString(string value)
        {
            return "`" + value.Replace("`", "``") + "`";
        }

        /// <summary>
        /// Parses a byte value that may include unit suffixes (B, KB, MB, GB).
        /// </summary>
        /// <param name="value">The value to parse, e.g., "400MB", "1024KB", "1073741824"</param>
        /// <returns>The value in bytes</returns>
        /// <exception cref="FormatException">Thrown when the value cannot be parsed</exception>
        internal static long ParseBytesWithUnits(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                throw new FormatException("Value cannot be null or empty");
            }

            value = value.Trim().ToUpperInvariant();

            // Check for unit suffixes
            long multiplier = 1;
            string numberPart = value;

            if (value.EndsWith("GB"))
            {
                multiplier = 1024L * 1024L * 1024L;
                numberPart = value.Substring(0, value.Length - 2);
            }
            else if (value.EndsWith("MB"))
            {
                multiplier = 1024L * 1024L;
                numberPart = value.Substring(0, value.Length - 2);
            }
            else if (value.EndsWith("KB"))
            {
                multiplier = 1024L;
                numberPart = value.Substring(0, value.Length - 2);
            }
            else if (value.EndsWith("B"))
            {
                multiplier = 1L;
                numberPart = value.Substring(0, value.Length - 1);
            }

            if (!long.TryParse(numberPart.Trim(), out long number))
            {
                throw new FormatException($"Invalid number format: {numberPart}");
            }

            try
            {
                return checked(number * multiplier);
            }
            catch (OverflowException)
            {
                throw new FormatException($"Value {value} results in overflow when converted to bytes");
            }
        }

        protected override void ValidateOptions()
        {
            base.ValidateOptions();

            if (Properties.TryGetValue(DatabricksParameters.TemporarilyUnavailableRetry, out string? tempUnavailableRetryStr))
            {
                if (!bool.TryParse(tempUnavailableRetryStr, out bool tempUnavailableRetryValue))
                {
                    throw new ArgumentOutOfRangeException(DatabricksParameters.TemporarilyUnavailableRetry, tempUnavailableRetryStr,
                        $"must be a value of false (disabled) or true (enabled). Default is true.");
                }

                TemporarilyUnavailableRetry = tempUnavailableRetryValue;
            }

            if (Properties.TryGetValue(DatabricksParameters.RateLimitRetry, out string? rateLimitRetryStr))
            {
                if (!bool.TryParse(rateLimitRetryStr, out bool rateLimitRetryValue))
                {
                    throw new ArgumentOutOfRangeException(DatabricksParameters.RateLimitRetry, rateLimitRetryStr,
                        $"must be a value of false (disabled) or true (enabled). Default is true.");
                }

                RateLimitRetry = rateLimitRetryValue;
            }

            if (Properties.TryGetValue(DatabricksParameters.TemporarilyUnavailableRetryTimeout, out string? tempUnavailableRetryTimeoutStr))
            {
                if (!int.TryParse(tempUnavailableRetryTimeoutStr, out int tempUnavailableRetryTimeoutValue) ||
                    tempUnavailableRetryTimeoutValue < 0)
                {
                    throw new ArgumentOutOfRangeException(DatabricksParameters.TemporarilyUnavailableRetryTimeout, tempUnavailableRetryTimeoutStr,
                        $"must be a value of 0 (retry indefinitely) or a positive integer representing seconds. Default is 900 seconds (15 minutes).");
                }
                TemporarilyUnavailableRetryTimeout = tempUnavailableRetryTimeoutValue;
            }

            if (Properties.TryGetValue(DatabricksParameters.RateLimitRetryTimeout, out string? rateLimitRetryTimeoutStr))
            {
                if (!int.TryParse(rateLimitRetryTimeoutStr, out int rateLimitRetryTimeoutValue) ||
                    rateLimitRetryTimeoutValue < 0)
                {
                    throw new ArgumentOutOfRangeException(DatabricksParameters.RateLimitRetryTimeout, rateLimitRetryTimeoutStr,
                        $"must be a value of 0 (retry indefinitely) or a positive integer representing seconds. Default is 120 seconds (2 minutes).");
                }
                RateLimitRetryTimeout = rateLimitRetryTimeoutValue;
            }

            // When TemporarilyUnavailableRetry is enabled, we need to make sure connection timeout (which is used to cancel the HttpConnection) is equal
            // or greater than TemporarilyUnavailableRetryTimeout so that it won't timeout before server startup timeout (TemporarilyUnavailableRetryTimeout)
            if (TemporarilyUnavailableRetry && TemporarilyUnavailableRetryTimeout * 1000 > ConnectTimeoutMilliseconds)
            {
                ConnectTimeoutMilliseconds = TemporarilyUnavailableRetryTimeout * 1000;
            }
        }

        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(IResponse response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults!.ResultSetMetadata);

        protected override Task<TRowSet> GetRowSetAsync(IResponse response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults!.ResultSet.Results);

        protected override AuthenticationHeaderValue? GetAuthenticationHeaderValue(SparkAuthType authType)
        {
            // All authentication is handled by delegating handlers in HttpHandlerFactory:
            // - Token authentication -> StaticBearerTokenHandler
            // - OAuth authentication -> OAuthDelegatingHandler / TokenRefreshDelegatingHandler / StaticBearerTokenHandler
            // Return null to let handlers manage authentication rather than setting default headers
            return null;
        }

        protected override void ValidateOAuthParameters()
        {
            Properties.TryGetValue(DatabricksParameters.OAuthGrantType, out string? grantTypeStr);
            DatabricksOAuthGrantType grantType;

            if (!DatabricksOAuthGrantTypeParser.TryParse(grantTypeStr, out grantType))
            {
                throw new ArgumentOutOfRangeException(
                    DatabricksParameters.OAuthGrantType,
                    grantTypeStr,
                    $"Unsupported {DatabricksParameters.OAuthGrantType} value. Refer to the Databricks documentation for valid values."
                );
            }

            // If we have a valid grant type, validate the required parameters
            if (grantType == DatabricksOAuthGrantType.ClientCredentials)
            {
                Properties.TryGetValue(DatabricksParameters.OAuthClientId, out string? clientId);
                Properties.TryGetValue(DatabricksParameters.OAuthClientSecret, out string? clientSecret);

                if (string.IsNullOrEmpty(clientId))
                {
                    throw new ArgumentException(
                        $"Parameter '{DatabricksParameters.OAuthGrantType}' is set to '{DatabricksConstants.OAuthGrantTypes.ClientCredentials}' but parameter '{DatabricksParameters.OAuthClientId}' is not set. Please provide a value for '{DatabricksParameters.OAuthClientId}'.",
                        nameof(Properties));
                }
                if (string.IsNullOrEmpty(clientSecret))
                {
                    throw new ArgumentException(
                        $"Parameter '{DatabricksParameters.OAuthGrantType}' is set to '{DatabricksConstants.OAuthGrantTypes.ClientCredentials}' but parameter '{DatabricksParameters.OAuthClientSecret}' is not set. Please provide a value for '{DatabricksParameters.OAuthClientSecret}'.",
                        nameof(Properties));
                }
            }
            else
            {
                // For other auth flows, use default OAuth validation
                base.ValidateOAuthParameters();
            }
        }

        /// <summary>
        /// Gets the host from the connection properties.
        /// </summary>
        /// <returns>The host, or empty string if not found.</returns>
        private string GetHost()
        {
            if (Properties.TryGetValue(SparkParameters.HostName, out string? host) && !string.IsNullOrEmpty(host))
            {
                return host;
            }

            if (Properties.TryGetValue(AdbcOptions.Uri, out string? uri) && !string.IsNullOrEmpty(uri))
            {
                // Parse the URI to extract the host
                if (Uri.TryCreate(uri, UriKind.Absolute, out Uri? parsedUri))
                {
                    return parsedUri.Host;
                }
            }

            throw new ArgumentException("Host not found in connection properties. Please provide a valid host using either 'HostName' or 'Uri' property.");
        }

        public override string AssemblyName => s_assemblyName;

        public override string AssemblyVersion => s_assemblyVersion;

        internal static string? HandleSparkCatalog(string? CatalogName)
        {
            if (CatalogName != null && CatalogName.Equals("SPARK", StringComparison.OrdinalIgnoreCase))
            {
                return null;
            }
            return CatalogName;
        }

        /// <summary>
        /// Sets connection-level telemetry tags on the activity so that the MetricsAggregator
        /// can extract them. This includes session ID, auth type, driver/runtime/OS system info,
        /// connection parameters, and feature flags.
        /// </summary>
        /// <param name="activity">The activity to set tags on.</param>
        private void SetConnectionTelemetryTags(Activity? activity)
        {
            if (activity == null) return;

            // Session ID
            string? sessionId = GetSessionIdString();
            if (sessionId != null)
            {
                activity.SetTag(ConnectionOpenEvent.SessionId, sessionId);
            }

            // Auth type
            string authType = DetermineAuthType();
            activity.SetTag(ConnectionOpenEvent.AuthType, authType);

            // Driver system tags
            activity.SetTag(ConnectionOpenEvent.DriverName, "Apache Arrow ADBC Databricks Driver");
            activity.SetTag(ConnectionOpenEvent.DriverVersion, s_assemblyVersion);
            activity.SetTag(ConnectionOpenEvent.DriverOS, RuntimeInformation.OSDescription);
            activity.SetTag(ConnectionOpenEvent.DriverRuntime, RuntimeInformation.FrameworkDescription);

            // Runtime info
            activity.SetTag(ConnectionOpenEvent.RuntimeName, RuntimeInformation.FrameworkDescription);
            activity.SetTag(ConnectionOpenEvent.RuntimeVersion, Environment.Version.ToString());
            activity.SetTag(ConnectionOpenEvent.RuntimeVendor, "Microsoft");

            // OS info
            activity.SetTag(ConnectionOpenEvent.OsName, RuntimeInformation.OSDescription);
            activity.SetTag(ConnectionOpenEvent.OsVersion, Environment.OSVersion.Version.ToString());
            activity.SetTag(ConnectionOpenEvent.OsArch, RuntimeInformation.OSArchitecture.ToString());

            // Client/locale/process info
            string? entryAssemblyName = System.Reflection.Assembly.GetEntryAssembly()?.GetName().Name;
            activity.SetTag(ConnectionOpenEvent.ClientAppName, entryAssemblyName ?? "unknown");
            activity.SetTag(ConnectionOpenEvent.LocaleName, CultureInfo.CurrentCulture.Name);
            activity.SetTag(ConnectionOpenEvent.CharSetEncoding, Encoding.Default.WebName);
            activity.SetTag(ConnectionOpenEvent.ProcessName, Process.GetCurrentProcess().ProcessName);

            // Connection parameter tags (fallback to URI if individual properties not set)
            Properties.TryGetValue(SparkParameters.Path, out string? httpPath);
            Properties.TryGetValue(SparkParameters.HostName, out string? host);
            Properties.TryGetValue(SparkParameters.Port, out string? port);

            if (string.IsNullOrEmpty(host) && Properties.TryGetValue(AdbcOptions.Uri, out string? connUri) && !string.IsNullOrEmpty(connUri))
            {
                if (Uri.TryCreate(connUri, UriKind.Absolute, out Uri? parsedUri))
                {
                    host = parsedUri.Host;
                    if (string.IsNullOrEmpty(httpPath)) httpPath = parsedUri.AbsolutePath;
                    if (string.IsNullOrEmpty(port) && parsedUri.Port > 0) port = parsedUri.Port.ToString();
                }
            }

            activity.SetTag(ConnectionOpenEvent.ConnectionHttpPath, httpPath ?? string.Empty);
            activity.SetTag(ConnectionOpenEvent.ConnectionHost, host ?? string.Empty);
            activity.SetTag(ConnectionOpenEvent.ConnectionPort, port ?? "443");

            Properties.TryGetValue(DatabricksParameters.Protocol, out string? protocol);
            activity.SetTag(ConnectionOpenEvent.ConnectionMode, protocol ?? "thrift");

            Properties.TryGetValue(SparkParameters.AuthType, out string? authMech);
            activity.SetTag(ConnectionOpenEvent.ConnectionAuthMech, authMech ?? string.Empty);

            Properties.TryGetValue(DatabricksParameters.OAuthGrantType, out string? authFlow);
            activity.SetTag(ConnectionOpenEvent.ConnectionAuthFlow, authFlow ?? string.Empty);

            // Use Databricks-specific defaults: DatabricksStatement uses 2M rows,
            // and DatabricksConstants.DefaultAsyncExecPollIntervalMs is 100ms.
            // These may be overridden by connection properties.
            Properties.TryGetValue(ApacheParameters.BatchSize, out string? batchSizeStr);
            long batchSizeValue = (batchSizeStr != null && long.TryParse(batchSizeStr, out long parsedBatchSize))
                ? parsedBatchSize
                : DatabricksConstants.DefaultBatchSize;
            activity.SetTag(ConnectionOpenEvent.ConnectionBatchSize, batchSizeValue);

            Properties.TryGetValue(ApacheParameters.PollTimeMilliseconds, out string? pollIntervalStr);
            int pollIntervalValue = (pollIntervalStr != null && int.TryParse(pollIntervalStr, out int parsedPollInterval))
                ? parsedPollInterval
                : DatabricksConstants.DefaultAsyncExecPollIntervalMs;
            activity.SetTag(ConnectionOpenEvent.ConnectionPollIntervalMs, pollIntervalValue);

            Properties.TryGetValue(HttpProxyOptions.UseProxy, out string? useProxy);
            activity.SetTag(ConnectionOpenEvent.ConnectionUseProxy, string.Equals(useProxy, "true", StringComparison.OrdinalIgnoreCase));

            // Feature tags
            activity.SetTag(ConnectionOpenEvent.FeatureArrow, true); // Always Arrow-based
            activity.SetTag(ConnectionOpenEvent.FeatureDirectResults, _enableDirectResults);
            activity.SetTag(ConnectionOpenEvent.FeatureCloudFetch, _useCloudFetch);
            activity.SetTag(ConnectionOpenEvent.FeatureLz4, _canDecompressLz4);

            // Local diagnostics (not exported to Databricks)
            activity.SetTag(ConnectionOpenEvent.ServerAddress, host ?? string.Empty);
        }

        /// <summary>
        /// Builds a <see cref="DriverSystemConfiguration"/> proto from the current connection state.
        /// </summary>
        private DriverSystemConfiguration BuildSystemConfiguration()
        {
            string? entryAssemblyName = System.Reflection.Assembly.GetEntryAssembly()?.GetName().Name;

            return new DriverSystemConfiguration
            {
                DriverName = "Apache Arrow ADBC Databricks Driver",
                DriverVersion = s_assemblyVersion,
                RuntimeName = RuntimeInformation.FrameworkDescription,
                RuntimeVersion = Environment.Version.ToString(),
                RuntimeVendor = "Microsoft",
                OsName = RuntimeInformation.OSDescription,
                OsVersion = Environment.OSVersion.Version.ToString(),
                OsArch = RuntimeInformation.OSArchitecture.ToString(),
                ClientAppName = entryAssemblyName ?? "unknown",
                LocaleName = CultureInfo.CurrentCulture.Name,
                CharSetEncoding = Encoding.Default.WebName,
                ProcessName = Process.GetCurrentProcess().ProcessName
            };
        }

        /// <summary>
        /// Builds a <see cref="DriverConnectionParameters"/> proto from the current connection properties.
        /// </summary>
        private DriverConnectionParameters BuildConnectionParameters()
        {
            Properties.TryGetValue(SparkParameters.Path, out string? httpPath);
            Properties.TryGetValue(SparkParameters.HostName, out string? host);
            Properties.TryGetValue(SparkParameters.Port, out string? port);
            Properties.TryGetValue(DatabricksParameters.Protocol, out string? protocol);
            Properties.TryGetValue(SparkParameters.AuthType, out string? authMech);
            Properties.TryGetValue(DatabricksParameters.OAuthGrantType, out string? authFlow);
            Properties.TryGetValue(HttpProxyOptions.UseProxy, out string? useProxy);
            Properties.TryGetValue(ApacheParameters.BatchSize, out string? batchSizeStr);
            Properties.TryGetValue(ApacheParameters.PollTimeMilliseconds, out string? pollIntervalStr);
            Properties.TryGetValue(DatabricksParameters.OAuthScope, out string? authScope);
            Properties.TryGetValue(SparkParameters.ConnectTimeoutMilliseconds, out string? socketTimeoutStr);
            Properties.TryGetValue(DatabricksParameters.QueryTags, out string? queryTags);

            // Fallback to URI if individual host/path/port not set
            if (string.IsNullOrEmpty(host) && Properties.TryGetValue(AdbcOptions.Uri, out string? uri) && !string.IsNullOrEmpty(uri))
            {
                if (Uri.TryCreate(uri, UriKind.Absolute, out Uri? parsedUri))
                {
                    host = parsedUri.Host;
                    if (string.IsNullOrEmpty(httpPath))
                    {
                        httpPath = parsedUri.AbsolutePath;
                    }
                    if (string.IsNullOrEmpty(port) && parsedUri.Port > 0)
                    {
                        port = parsedUri.Port.ToString();
                    }
                }
            }

            int portValue = (port != null && int.TryParse(port, out int parsedPort)) ? parsedPort : 443;
            long batchSizeValue = (batchSizeStr != null && long.TryParse(batchSizeStr, out long parsedBatchSize))
                ? parsedBatchSize
                : DatabricksConstants.DefaultBatchSize;
            int pollIntervalValue = (pollIntervalStr != null && int.TryParse(pollIntervalStr, out int parsedPollInterval))
                ? parsedPollInterval
                : DatabricksConstants.DefaultAsyncExecPollIntervalMs;
            long socketTimeoutSec = (socketTimeoutStr != null && long.TryParse(socketTimeoutStr, out long parsedTimeout))
                ? parsedTimeout / 1000
                : DatabricksConstants.DefaultTemporarilyUnavailableRetryTimeout;

            // Determine auth mech and flow from token/auth type presence
            string authType = DetermineAuthType();
            DriverAuthMechType authMechType = StatementTelemetryContext.ParseAuthMech(authMech);
            DriverAuthFlowType authFlowType = StatementTelemetryContext.ParseAuthFlow(authFlow);
            if (authMechType == DriverAuthMechType.Unspecified)
            {
                if (authType == "pat") authMechType = DriverAuthMechType.DriverAuthMechPat;
                else if (authType.StartsWith("oauth")) authMechType = DriverAuthMechType.DriverAuthMechOauth;
            }
            if (authFlowType == DriverAuthFlowType.Unspecified)
            {
                if (authType == "pat") authFlowType = DriverAuthFlowType.DriverAuthFlowTokenPassthrough;
                else if (authType == "oauth-m2m") authFlowType = DriverAuthFlowType.DriverAuthFlowClientCredentials;
            }

            // Build host URL with protocol prefix like JDBC does
            string hostUrl = host ?? string.Empty;
            if (!string.IsNullOrEmpty(hostUrl) && !hostUrl.StartsWith("http", StringComparison.OrdinalIgnoreCase))
            {
                hostUrl = $"https://{hostUrl}:{portValue}";
            }

            return new DriverConnectionParameters
            {
                HttpPath = httpPath ?? string.Empty,
                Mode = StatementTelemetryContext.ParseDriverMode(protocol ?? "thrift"),
                HostInfo = new HostDetails
                {
                    HostUrl = hostUrl,
                    Port = 0 // JDBC uses port=0 when port is in host_url
                },
                UseProxy = string.Equals(useProxy, "true", StringComparison.OrdinalIgnoreCase),
                AuthMech = authMechType,
                AuthFlow = authFlowType,
                AuthScope = authScope ?? string.Empty,
                EnableArrow = true,
                EnableDirectResults = _enableDirectResults,
                RowsFetchedPerBlock = batchSizeValue,
                AsyncPollIntervalMillis = pollIntervalValue,
                SocketTimeout = socketTimeoutSec,
                QueryTags = queryTags ?? string.Empty,
                CheckCertificateRevocation = true,
                EnableComplexDatatypeSupport = false,
                StringColumnLength = 255,
            };
        }

        /// <summary>
        /// Determines the authentication type string for telemetry.
        /// </summary>
        /// <returns>A string describing the auth type (e.g., "pat", "oauth-m2m", "oauth", "basic", "token").</returns>
        private string DetermineAuthType()
        {
            Properties.TryGetValue(SparkParameters.AuthType, out string? authType);
            Properties.TryGetValue(DatabricksParameters.OAuthGrantType, out string? grantType);

            if (string.Equals(authType, SparkAuthTypeConstants.OAuth, StringComparison.OrdinalIgnoreCase))
            {
                if (string.Equals(grantType, DatabricksConstants.OAuthGrantTypes.ClientCredentials, StringComparison.OrdinalIgnoreCase))
                {
                    return "oauth-m2m";
                }
                return "oauth";
            }

            if (string.Equals(authType, SparkAuthTypeConstants.Token, StringComparison.OrdinalIgnoreCase))
            {
                return "pat";
            }

            if (string.Equals(authType, SparkAuthTypeConstants.Basic, StringComparison.OrdinalIgnoreCase))
            {
                return "basic";
            }

            // Infer auth type from presence of token/credentials when AuthType not explicitly set
            if (string.IsNullOrEmpty(authType))
            {
                if (Properties.ContainsKey(SparkParameters.Token))
                {
                    return "pat";
                }
                if (Properties.ContainsKey(SparkParameters.AccessToken))
                {
                    return "oauth";
                }
            }

            return authType ?? "unknown";
        }

        /// <summary>
        /// Initializes the telemetry pipeline after the session has been established.
        /// Creates shared ITelemetryClient from TelemetryClientManager, per-connection
        /// MetricsAggregator, and registers with the global DatabricksActivityListener.
        /// </summary>
        /// <remarks>
        /// All telemetry initialization is wrapped in try-catch to ensure that
        /// telemetry failures never prevent a connection from opening.
        /// </remarks>
        private void InitializeTelemetry(Activity? activity)
        {
            try
            {
                // Step 1: Parse telemetry configuration from connection properties
                _telemetryConfig = TelemetryConfiguration.FromProperties(Properties);

                activity?.SetTag("telemetry.enabled", _telemetryConfig.Enabled);

                if (!_telemetryConfig.Enabled)
                {
                    activity?.AddEvent("telemetry.skipped", [
                        new("reason", "disabled_by_config")
                    ]);
                    return;
                }

                // Step 2: Resolve host for telemetry client sharing
                _telemetryHost = FeatureFlagCache.TryGetHost(Properties);
                if (string.IsNullOrEmpty(_telemetryHost))
                {
                    activity?.AddEvent("telemetry.skipped", [
                        new("reason", "no_host_available")
                    ]);
                    return;
                }

                // Step 3: Get shared telemetry client from TelemetryClientManager
                // The exporter factory creates CircuitBreakerTelemetryExporter wrapping DatabricksTelemetryExporter
                _telemetryClient = TelemetryClientManager.GetInstance().GetOrCreateClient(
                    _telemetryHost,
                    CreateTelemetryExporter,
                    _telemetryConfig);

                // Step 4: Create per-connection MetricsAggregator
                _metricsAggregator = new MetricsAggregator(_telemetryClient, _telemetryConfig);

                // Step 5: Set session context on the aggregator
                string? sessionId = GetSessionIdString();
                if (sessionId != null)
                {
                    DriverSystemConfiguration systemConfig = BuildSystemConfiguration();
                    DriverConnectionParameters connectionParams = BuildConnectionParameters();
                    string authType = DetermineAuthType();
                    _metricsAggregator.SetSessionContext(sessionId, 0, systemConfig, connectionParams, authType, _useCloudFetch, _canDecompressLz4);
                }

                // Step 6: Register aggregator with the global DatabricksActivityListener
                string aggregatorKey = sessionId ?? Guid.NewGuid().ToString("N");
                DatabricksActivityListener.Instance.RegisterAggregator(aggregatorKey, _metricsAggregator);

                // Step 7: Start the listener if not already started
                DatabricksActivityListener.Instance.Start();

                activity?.AddEvent("telemetry.initialized", [
                    new("host", _telemetryHost),
                    new("session_id", sessionId ?? "(none)")
                ]);
            }
            catch (Exception ex)
            {
                // Telemetry initialization failure must never prevent connection from opening
                Debug.WriteLine($"[TRACE] Error during telemetry initialization: {ex.Message}");
                activity?.AddEvent("telemetry.initialization_failed", [
                    new("error.type", ex.GetType().Name),
                    new("error.message", ex.Message)
                ]);

                // Clean up any partially initialized telemetry state
                CleanupTelemetryState();
            }
        }

        /// <summary>
        /// Creates a telemetry exporter instance. Uses <see cref="TestExporterFactory"/>
        /// if set (for testing), otherwise creates a <see cref="CircuitBreakerTelemetryExporter"/>
        /// wrapping a <see cref="DatabricksTelemetryExporter"/>.
        /// </summary>
        /// <returns>A new telemetry exporter instance.</returns>
        internal ITelemetryExporter CreateTelemetryExporter()
        {
            if (TestExporterFactory != null)
            {
                return TestExporterFactory();
            }

            string host = _telemetryHost ?? FeatureFlagCache.TryGetHost(Properties) ?? string.Empty;

            // Create an authenticated HttpClient for telemetry export
            HttpClient httpClient = HttpClientFactory.CreateFeatureFlagHttpClient(
                Properties,
                host,
                s_assemblyVersion);

            // Use authenticated endpoint since we have credentials from the connection
            DatabricksTelemetryExporter innerExporter = new DatabricksTelemetryExporter(
                httpClient,
                host,
                true, // isAuthenticated - connection has credentials
                _telemetryConfig ?? TelemetryConfiguration.FromProperties(Properties));

            return new CircuitBreakerTelemetryExporter(
                host,
                innerExporter);
        }

        /// <summary>
        /// Gets the session ID as a string from the current session handle.
        /// </summary>
        /// <returns>The session ID string in "N" format (no dashes), or null if no session is open.</returns>
        private string? GetSessionIdString()
        {
            if (SessionHandle?.SessionId?.Guid != null && SessionHandle.SessionId.Guid.Length == 16)
            {
                return new Guid(SessionHandle.SessionId.Guid).ToString("N");
            }
            return null;
        }

        /// <summary>
        /// Cleans up telemetry state without throwing exceptions.
        /// Used during error recovery in initialization and during disposal.
        /// </summary>
        private void CleanupTelemetryState()
        {
            _metricsAggregator = null;
            _telemetryClient = null;
            _telemetryConfig = null;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                DisposeTelemetry();
            }

            base.Dispose(disposing);
        }

        /// <summary>
        /// Cleans up telemetry resources in the correct order:
        /// 1. Unregister aggregator from listener
        /// 2. Flush aggregator
        /// 3. Release telemetry client from TelemetryClientManager
        /// </summary>
        /// <remarks>
        /// All telemetry cleanup is wrapped in try-catch to ensure that
        /// telemetry failures never prevent connection disposal.
        /// </remarks>
        private void DisposeTelemetry()
        {
            try
            {
                if (_metricsAggregator != null)
                {
                    // Step 1: Unregister aggregator from the global listener (also flushes)
                    string? sessionId = GetSessionIdString();
                    if (sessionId != null)
                    {
                        try
                        {
                            DatabricksActivityListener.Instance.UnregisterAggregatorAsync(sessionId).ConfigureAwait(false).GetAwaiter().GetResult();
                        }
                        catch (Exception ex)
                        {
                            Debug.WriteLine($"[TRACE] Error unregistering telemetry aggregator: {ex.Message}");
                        }
                    }

                    // Step 2: Flush aggregator to send any remaining pending metrics
                    try
                    {
                        _metricsAggregator.FlushAsync().ConfigureAwait(false).GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        Debug.WriteLine($"[TRACE] Error flushing telemetry aggregator: {ex.Message}");
                    }

                    _metricsAggregator = null;
                }

                // Step 3: Release the shared telemetry client (decrements ref count)
                if (_telemetryClient != null && !string.IsNullOrEmpty(_telemetryHost))
                {
                    try
                    {
                        TelemetryClientManager.GetInstance().ReleaseClientAsync(_telemetryHost!).ConfigureAwait(false).GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        Debug.WriteLine($"[TRACE] Error releasing telemetry client: {ex.Message}");
                    }

                    _telemetryClient = null;
                }

                _telemetryConfig = null;
            }
            catch (Exception ex)
            {
                // Telemetry cleanup failure must never prevent connection disposal
                Debug.WriteLine($"[TRACE] Error during telemetry cleanup: {ex.Message}");
            }
        }
    }
}
