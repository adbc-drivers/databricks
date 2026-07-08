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
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.Hive2;
using AdbcDrivers.Databricks.StatementExecution.MetadataCommands;
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
        private readonly string? _orgId;
        private string? _catalog;
        private string? _schema;
        private readonly HttpClient _httpClient;
        private readonly HttpClient _cloudFetchHttpClient; // Separate HttpClient without auth headers for CloudFetch downloads
        private readonly IReadOnlyDictionary<string, string> _properties;
        private readonly bool _ownsHttpClient;

        // Session management
        private string? _sessionId;
        private readonly SemaphoreSlim _sessionLock = new SemaphoreSlim(1, 1);

        // Configuration for statement creation — assigned by ValidateProperties().
        private string _resultDisposition = null!;
        private string _resultFormat = null!;
        private string? _resultCompression;
        // Request wait_timeout: null = unset (direct results on), "0s" = async (direct results off).
        // Never a positive value (see ValidateProperties / ES-2034600).
        private string? _waitTimeout;
        private int _pollingIntervalMs;
        // Query timeout (seconds) shared by regular queries and metadata operations. 0 = no timeout.
        private int _queryTimeoutSeconds;
        private bool _enablePKFK;
        private bool _enableMultipleCatalogSupport;
        private bool _useDescTableExtended;
        private bool _enableFastMetadataQuery;
        private bool _applySSPWithQueries;

        // Connection bring-up timeout (PECO-3059). Mirrors the Thrift path's
        // ConnectTimeoutMilliseconds — used as a CancellationToken bound on
        // CreateSession to honor adbc.spark.connect_timeout_ms on the SEA path.
        // Default matches HiveServer2Connection.ConnectTimeoutMillisecondsDefault (30 s).
        private const int ConnectTimeoutMillisecondsDefault = 30000;
        private int _connectTimeoutMilliseconds;

        // Memory pooling (shared across connection)
        private readonly Microsoft.IO.RecyclableMemoryStreamManager _recyclableMemoryStreamManager;
        private readonly System.Buffers.ArrayPool<byte> _lz4BufferPool;

        // Tracing propagation configuration — assigned by ValidateProperties().
        private bool _tracePropagationEnabled;
        private string _traceParentHeaderName = null!;
        private bool _traceStateEnabled;
        private bool _enableComplexDatatypeSupport;

        // Authentication support — assigned by ValidateProperties().
        private string? _identityFederationClientId;

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

            // Extract org ID from ?o=yyy query parameter in path or URI
            _orgId = PropertyHelper.ParseOrgIdFromProperties(properties);

            // Strip query string from path before warehouse regex matching
            if (!string.IsNullOrEmpty(path))
            {
                int queryIndex = path.IndexOf('?');
                if (queryIndex >= 0)
                    path = path.Substring(0, queryIndex);
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
                        throw new DatabricksException(
                            "Statement Execution API requires a SQL Warehouse, not a general cluster. " +
                            $"The provided path '{path}' appears to be a general cluster endpoint. " +
                            "Please use a SQL Warehouse path like '/sql/1.0/warehouses/{warehouse_id}' or '/sql/1.0/endpoints/{warehouse_id}'.",
                            AdbcStatusCode.InvalidArgument);
                    }
                }
            }

            if (string.IsNullOrEmpty(warehouseId))
            {
                throw new DatabricksException(
                    "Warehouse ID is required for Statement Execution API. " +
                    "Please provide it via 'adbc.databricks.warehouse_id' parameter, include it in the 'path' parameter (e.g., '/sql/1.0/warehouses/your-warehouse-id'), " +
                    "or provide a full URI with the warehouse path.",
                    AdbcStatusCode.InvalidArgument);
            }
            _warehouseId = warehouseId;

            // Get host URL
            if (string.IsNullOrEmpty(hostName))
            {
                throw new DatabricksException(
                    "Host name is required. Please provide it via 'hostName' parameter or via 'uri' parameter.",
                    AdbcStatusCode.InvalidArgument);
            }
            string baseUrl = $"https://{hostName}";

            // Centralized property parsing / validation. Every property-driven field is parsed
            // and validated in one place so the constructor body stays focused on wiring.
            ValidateProperties();

            // Memory pooling
            _recyclableMemoryStreamManager = memoryStreamManager ?? new Microsoft.IO.RecyclableMemoryStreamManager();
            _lz4BufferPool = lz4BufferPool ?? System.Buffers.ArrayPool<byte>.Create(maxArrayLength: 4 * 1024 * 1024, maxArraysPerBucket: 10);

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
            bool transportErrorRetry = PropertyHelper.GetBooleanPropertyWithValidation(properties, DatabricksParameters.TransportErrorRetry, true);
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
                TransportErrorRetry = transportErrorRetry,
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

            if (!string.IsNullOrEmpty(_orgId))
                httpClient.DefaultRequestHeaders.TryAddWithoutValidation(DatabricksConstants.OrgIdHeader, _orgId);

            return httpClient;
        }

        /// <summary>
        /// Parses and validates every property-driven option for this connection in one place,
        /// keeping option-handling logic centralized rather than scattered through the constructor.
        /// </summary>
        private void ValidateProperties()
        {
            var properties = _properties;

            // Connection feature flags — must be parsed before catalog loading (depends on _enableMultipleCatalogSupport).
            _enablePKFK = PropertyHelper.GetBooleanPropertyWithValidation(properties, DatabricksParameters.EnablePKFK, true);
            _enableMultipleCatalogSupport = PropertyHelper.GetBooleanPropertyWithValidation(properties, DatabricksParameters.EnableMultipleCatalogSupport, true);
            _useDescTableExtended = PropertyHelper.GetBooleanPropertyWithValidation(properties, DatabricksParameters.UseDescTableExtended, true);
            _enableFastMetadataQuery = PropertyHelper.GetBooleanPropertyWithValidation(properties, DatabricksParameters.EnableFastMetadataQuery, false);
            // When true, SSPs (adbc.databricks.ssp_*) are applied via post-open SET statements
            // rather than CreateSession.session_confs — mirrors Thrift's behavior so callers
            // who depend on the SET-statement path (e.g., for audit visibility or for SSPs
            // the server validates differently in CreateSession vs SET) get the same semantics
            // on SEA. JDBC has no equivalent flag — this is parity with the Thrift driver only.
            _applySSPWithQueries = PropertyHelper.GetBooleanPropertyWithValidation(properties, DatabricksParameters.ApplySSPWithQueries, false);

            // Session configuration.
            // Only supply catalog from connection properties when EnableMultipleCatalogSupport is true.
            // This matches DatabricksConnection (Thrift) behavior: when flag=false, the session uses
            // the server's default catalog rather than a client-specified one.
            if (_enableMultipleCatalogSupport)
            {
                properties.TryGetValue(AdbcOptions.Connection.CurrentCatalog, out _catalog);
                // Match Thrift behavior: SPARK is a legacy alias — map it to null so the
                // runtime falls back to the workspace default (typically hive_metastore).
                _catalog = DatabricksConnection.HandleSparkCatalog(_catalog);
            }
            properties.TryGetValue(AdbcOptions.Connection.CurrentDbSchema, out _schema);

            // Result configuration.
            _resultDisposition = PropertyHelper.GetStringProperty(properties, DatabricksParameters.ResultDisposition, "INLINE_OR_EXTERNAL_LINKS");
            _resultFormat = PropertyHelper.GetStringProperty(properties, DatabricksParameters.ResultFormat, "ARROW_STREAM");
            // Result compression is derived solely from the client's LZ4 capability flag
            // (adbc.databricks.cloudfetch.lz4.enabled, default true) so a single flag drives LZ4
            // across both the Thrift/CloudFetch and REST paths, and disabling that pre-existing
            // flag keeps disabling LZ4 everywhere. The reader decompresses based on the response
            // manifest, so an uncompressed response is still handled correctly.
            //
            // adbc.databricks.rest.result_compression is deprecated and no longer read: it drove
            // no real client and only added confusion. The LZ4 capability flag is the single knob.
            //
            // LZ4_FRAME is only requested for Arrow-based result formats. The SEA server's
            // tolerance of LZ4_FRAME on non-Arrow formats (JSON_ARRAY, CSV) is not something we
            // verify here, and this driver's reader is Arrow-only so non-Arrow results are not
            // consumable regardless. Gating on format keeps the request conservative: non-Arrow
            // formats send NONE (matching the prior behavior) rather than relying on the server
            // to silently ignore a compression codec it may not accept for that format.
            bool canDecompressLz4 = PropertyHelper.GetBooleanPropertyWithValidation(properties, DatabricksParameters.CanDecompressLz4, true);
            bool isArrowFormat = _resultFormat.IndexOf("ARROW", StringComparison.OrdinalIgnoreCase) >= 0;
            _resultCompression = (canDecompressLz4 && isArrowFormat) ? "LZ4_FRAME" : "NONE";

            // wait_timeout is NOT a customer-tunable knob; it is derived from the direct-results flag.
            //   Direct results ON (default): leave wait_timeout UNSET so the server returns the full
            //     result inline in a single response (state SUCCEEDED or CLOSED) — the SEA equivalent
            //     of Thrift DirectResults, and what databricks-jdbc sends by default.
            //   Direct results OFF: send wait_timeout="0s" (fully async) and poll for the result.
            // We deliberately never send a NON-ZERO wait_timeout: a positive value routes the request
            //   to the old SEA sync-hybrid results path, which truncates multi-chunk results (the
            //   manifest advertises N chunks but only chunk 0 is delivered). Tracked as ES-2034600.
            bool enableDirectResults =
                !(properties.TryGetValue(DatabricksParameters.EnableDirectResults, out var directResults)
                  && directResults.Equals("false", StringComparison.OrdinalIgnoreCase));
            _waitTimeout = enableDirectResults ? null : "0s";
            // PECO-3064: adbc.apache.statement.polltime_ms is the single key for SEA polling cadence
            // (consolidated with the Thrift path). SEA defaults to 1000 ms — HTTP/JSON polls are
            // heavier than Thrift's 100 ms default — but both protocols share the same property.
            _pollingIntervalMs = PropertyHelper.GetPositiveIntPropertyWithValidation(
                properties, ApacheParameters.PollTimeMilliseconds, defaultValue: 1000);

            // Query timeout (adbc.apache.statement.query_timeout_s) shared by regular queries and
            // metadata operations — a metadata call is just another query, and can fan out into many
            // SHOW COLUMNS / SHOW CATALOGS statements. Default matches the Thrift path (3h); 0 = no
            // timeout. Bounds the metadata operation via CreateMetadataTimeoutCts and the statement's
            // own poll loop (PollWithTimeoutAsync) using the same value.
            _queryTimeoutSeconds = PropertyHelper.GetIntPropertyWithValidation(
                properties, ApacheParameters.QueryTimeoutSeconds,
                DatabricksConstants.DefaultQueryTimeoutSeconds);

            // Tracing propagation configuration. Base class (TracingConnection) already handles ActivityTrace init.
            _tracePropagationEnabled = PropertyHelper.GetBooleanPropertyWithValidation(properties, DatabricksParameters.TracePropagationEnabled, true);
            _traceParentHeaderName = PropertyHelper.GetStringProperty(properties, DatabricksParameters.TraceParentHeaderName, "traceparent");
            _traceStateEnabled = PropertyHelper.GetBooleanPropertyWithValidation(properties, DatabricksParameters.TraceStateEnabled, false);
            _enableComplexDatatypeSupport = PropertyHelper.GetBooleanPropertyWithValidation(properties, DatabricksParameters.EnableComplexDatatypeSupport, false);

            // Authentication configuration.
            if (properties.TryGetValue(DatabricksParameters.IdentityFederationClientId, out string? identityFederationClientId))
            {
                _identityFederationClientId = identityFederationClientId;
            }

            // PECO-3059: honor adbc.spark.connect_timeout_ms on the SEA path. The Thrift path uses
            // this value as a CancellationToken bound on OpenSession; mirror that here for CreateSession.
            // 0 means infinite. Negative values are invalid (matches Thrift's SparkHttpConnection validation).
            int connectTimeoutMs = ConnectTimeoutMillisecondsDefault;
            if (properties.TryGetValue(SparkParameters.ConnectTimeoutMilliseconds, out string? connectTimeoutStr))
            {
                if (!int.TryParse(connectTimeoutStr, System.Globalization.NumberStyles.Integer,
                        System.Globalization.CultureInfo.InvariantCulture, out int parsed) ||
                    parsed < 0)
                {
                    throw new ArgumentOutOfRangeException(
                        SparkParameters.ConnectTimeoutMilliseconds,
                        connectTimeoutStr,
                        $"must be a value of 0 (infinite) or between 1 .. {int.MaxValue}. default is {ConnectTimeoutMillisecondsDefault} milliseconds.");
                }
                connectTimeoutMs = parsed;
            }

            // Mirror DatabricksConnection.ValidateOptions parity-bump: when temporarily-unavailable retry is on,
            // the retry budget must fit inside the connect-timeout token — otherwise the connect-timeout
            // would cancel mid-retry-loop before the server warms up.
            bool temporarilyUnavailableRetry = PropertyHelper.GetBooleanPropertyWithValidation(
                properties, DatabricksParameters.TemporarilyUnavailableRetry, true);
            int temporarilyUnavailableRetryTimeout = PropertyHelper.GetIntPropertyWithValidation(
                properties,
                DatabricksParameters.TemporarilyUnavailableRetryTimeout,
                DatabricksConstants.DefaultTemporarilyUnavailableRetryTimeout);
            if (temporarilyUnavailableRetry &&
                temporarilyUnavailableRetryTimeout * 1000L > connectTimeoutMs &&
                connectTimeoutMs != 0) // 0 == infinite — don't shrink it
            {
                long bumped = temporarilyUnavailableRetryTimeout * 1000L;
                connectTimeoutMs = bumped > int.MaxValue ? int.MaxValue : (int)bumped;
            }
            _connectTimeoutMilliseconds = connectTimeoutMs;
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

            throw new DatabricksException("Host not found in connection properties. Please provide a valid host using either 'hostName' or 'uri' property.", AdbcStatusCode.InvalidArgument);
        }

        /// <summary>
        /// Builds the user agent string for HTTP requests.
        /// Format: ADBCDatabricksDriver/{version} REST [user_agent_entry] — delegated to
        /// <see cref="UserAgentHelper"/> so the prefix/entry handling stays shared with the
        /// feature-flag path. "REST" marks the SEA transport (Thrift uses a "Thrift/{v}" token).
        /// </summary>
        private string GetUserAgent(IReadOnlyDictionary<string, string> properties)
            => UserAgentHelper.GetUserAgent(DatabricksConnection.DriverVersion, properties, product: "REST");

        /// <summary>
        /// Opens the connection and creates a session.
        /// Session management is always enabled for REST API connections.
        /// </summary>
        public async Task OpenAsync(CancellationToken cancellationToken = default)
        {
            if (_sessionId == null)
            {
                // PECO-3059: bound the session bring-up with adbc.spark.connect_timeout_ms.
                // The Thrift path applies this same timeout to OpenSession via a
                // CancellationToken; we mirror that here. 0 means infinite — skip the
                // timer entirely so we don't fight an external caller-supplied token.
                using var connectTimeoutCts = _connectTimeoutMilliseconds > 0
                    ? new CancellationTokenSource(TimeSpan.FromMilliseconds(_connectTimeoutMilliseconds))
                    : null;
                using var linkedCts = connectTimeoutCts != null
                    ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, connectTimeoutCts.Token)
                    : null;
                CancellationToken openToken = linkedCts?.Token ?? cancellationToken;

                // Track lock ownership so the single finally only releases when we actually acquired it.
                // This collapses the previous outer/inner try pair into one — see PR #466 review feedback.
                bool lockAcquired = false;
                try
                {
                    await _sessionLock.WaitAsync(openToken).ConfigureAwait(false);
                    lockAcquired = true;

                    // Double-check after acquiring lock
                    if (_sessionId == null)
                    {
                        // When apply_ssp_with_queries=true, SSPs are deferred to post-open SET
                        // statements (see ApplyServerSidePropertiesAsync), so omit them here to
                        // avoid double-setting and to match Thrift parity.
                        var sessionConfigs = _applySSPWithQueries
                            ? new Dictionary<string, string>()
                            : ExtractServerSideProperties(_properties);
                        var request = new CreateSessionRequest
                        {
                            WarehouseId = _warehouseId,
                            Catalog = _catalog,
                            Schema = _schema,
                            SessionConfigs = sessionConfigs.Count > 0 ? sessionConfigs : null
                        };

                        try
                        {
                            var response = await _client.CreateSessionAsync(request, openToken).ConfigureAwait(false);
                            _sessionId = response.SessionId;
                        }
                        // PECO-3059: when the connect-timeout fired before the caller's token,
                        // translate any exception (including DatabricksException-wrapped OAuth
                        // cancellations) into TimeoutException for parity with HiveServer2Connection.
                        // This filter must run BEFORE the unconditional DatabricksException
                        // rethrow because OAuthClientCredentialsProvider wraps cancelled HTTP
                        // calls as DatabricksException("Failed to acquire OAuth access token: …").
                        catch (Exception ex) when (
                            connectTimeoutCts != null &&
                            connectTimeoutCts.IsCancellationRequested &&
                            !cancellationToken.IsCancellationRequested)
                        {
                            throw new TimeoutException(
                                "The operation timed out while attempting to open a session. Please try increasing connect timeout.",
                                ex);
                        }
                        catch (DatabricksException)
                        {
                            throw;
                        }
                        catch (Exception ex)
                        {
                            throw new DatabricksException(
                                $"Failed to connect to Databricks: {ex.GetBaseException().Message}",
                                AdbcStatusCode.IOError,
                                ex);
                        }

                        // If user didn't specify a catalog, discover the server's default.
                        // In Thrift, the server returns this in OpenSessionResp.InitialNamespace.
                        // SEA's CreateSession response doesn't include it, so we query explicitly.
                        if (_catalog == null && _enableMultipleCatalogSupport)
                        {
                            _catalog = GetCurrentCatalog();
                        }
                    }
                }
                catch (OperationCanceledException ex) when (
                    connectTimeoutCts != null &&
                    connectTimeoutCts.IsCancellationRequested &&
                    !cancellationToken.IsCancellationRequested)
                {
                    // The semaphore wait itself was cancelled by the connect-timeout token
                    // (rare but possible under contention).
                    throw new TimeoutException(
                        "The operation timed out while attempting to open a session. Please try increasing connect timeout.",
                        ex);
                }
                finally
                {
                    if (lockAcquired)
                    {
                        _sessionLock.Release();
                    }
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
                _waitTimeout,
                _pollingIntervalMs,
                _properties,
                _recyclableMemoryStreamManager,
                _lz4BufferPool,
                _cloudFetchHttpClient,
                this); // Pass connection as TracingConnection for tracing support
        }

        public override void SetOption(string key, string? value)
        {
            switch (key)
            {
                case AdbcOptions.Telemetry.TraceParent:
                    SetTraceParent(string.IsNullOrWhiteSpace(value) ? null : value);
                    return;
            }

            base.SetOption(key, value);
        }

        public override IArrowArrayStream GetObjects(GetObjectsDepth depth, string? catalogPattern, string? schemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
        {
            return this.TraceActivity(activity =>
            {
                activity?.SetTag("depth", depth.ToString());
                activity?.SetTag("catalog_pattern", catalogPattern ?? "(none)");
                activity?.SetTag("schema_pattern", schemaPattern ?? "(none)");
                activity?.SetTag("table_pattern", tableNamePattern ?? "(none)");
                activity?.SetTag("column_pattern", columnNamePattern ?? "(none)");

                // Databricks identifiers are case-insensitive — lowercase patterns
                // to match server behavior (same as DatabricksConnection/Thrift path).
                catalogPattern = catalogPattern?.ToLower();
                schemaPattern = schemaPattern?.ToLower();
                tableNamePattern = tableNamePattern?.ToLower();
                columnNamePattern = columnNamePattern?.ToLower();

                using var cts = CreateMetadataTimeoutCts();
                try
                {
                    return GetObjectsResultBuilder.BuildGetObjectsResultAsync(
                        this, depth, catalogPattern, schemaPattern,
                        tableNamePattern, tableTypes, columnNamePattern,
                        cts.Token).GetAwaiter().GetResult();
                }
                catch (Exception ex) when (cts.IsCancellationRequested && ex is not TimeoutException)
                {
                    // The aggregate metadata timeout fired: the CTS cancelled the underlying HTTP
                    // request, surfacing a raw TaskCanceledException. Translate it to TimeoutException
                    // for parity with HiveServer2Connection.GetObjects (Thrift base).
                    throw new TimeoutException(
                        "The metadata query execution timed out. Consider increasing the query timeout value.", ex);
                }
            }, nameof(GetObjects));
        }

        public override IArrowArrayStream GetInfo(IReadOnlyList<AdbcInfoCode> codes)
        {
            return this.TraceActivity(activity =>
            {
                var supportedCodes = new AdbcInfoCode[]
                {
                    AdbcInfoCode.DriverName,
                    AdbcInfoCode.DriverVersion,
                    AdbcInfoCode.DriverArrowVersion,
                    AdbcInfoCode.VendorName,
                    AdbcInfoCode.VendorSql,
                    AdbcInfoCode.VendorVersion,
                };

                if (codes == null || codes.Count == 0)
                    codes = supportedCodes;

                activity?.SetTag("requested_codes", string.Join(",", codes));

                var values = new Dictionary<AdbcInfoCode, object>
                {
                    { AdbcInfoCode.DriverName, DatabricksConnection.DatabricksDriverName },
                    { AdbcInfoCode.DriverVersion, AssemblyVersion },
                    { AdbcInfoCode.DriverArrowVersion, "1.0.0" },
                    { AdbcInfoCode.VendorName, "Databricks" },
                    { AdbcInfoCode.VendorVersion, AssemblyVersion },
                    { AdbcInfoCode.VendorSql, true },
                };

                return MetadataSchemaFactory.BuildGetInfoResult(codes, values);
            }, nameof(GetInfo));
        }

        public override IArrowArrayStream GetTableTypes()
        {
            return this.TraceActivity(activity =>
            {
                var builder = new StringArray.Builder();
                builder.Append("TABLE");
                builder.Append("VIEW");
                var schema = new Schema(new[] { new Field("table_type", StringType.Default, false) }, null);
                return new HiveInfoArrowStream(schema, new IArrowArray[] { builder.Build() });
            }, nameof(GetTableTypes));
        }

        public override Schema GetTableSchema(string? catalog, string? dbSchema, string tableName)
        {
            return this.TraceActivity(activity =>
            {
                activity?.SetTag("catalog", catalog ?? "(none)");
                activity?.SetTag("db_schema", dbSchema ?? "(none)");
                activity?.SetTag("table_name", tableName);

                using var cts = CreateMetadataTimeoutCts();
                // Pass catalog through with SPARK→null normalization, matching Thrift
                // which sends catalog as-is to the server. ExecuteShowColumnsAsync
                // handles null by iterating all catalogs.
                string? resolvedCatalog = DatabricksConnection.HandleSparkCatalog(catalog);
                List<RecordBatch> batches;
                try
                {
                    batches = ExecuteShowColumnsAsync(resolvedCatalog, dbSchema, tableName, null, cts.Token)
                        .GetAwaiter().GetResult();
                }
                catch (Exception ex) when (cts.IsCancellationRequested && ex is not TimeoutException)
                {
                    // Aggregate metadata timeout fired; translate the CTS cancellation to
                    // TimeoutException for parity with the Thrift base (see GetObjects).
                    throw new TimeoutException(
                        "The metadata query execution timed out. Consider increasing the query timeout value.", ex);
                }

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

                        short typeCode = ColumnMetadataHelper.GetDataTypeCode(colType);
                        IArrowType arrowType = HiveServer2Connection.GetArrowType(typeCode, colType, false, null, null);
                        fields.Add(new Field(colName, arrowType, nullable));
                    }
                }

                activity?.SetTag("result_fields", fields.Count);
                return new Schema(fields, null);
            }, nameof(GetTableSchema));
        }

        // IGetObjectsDataProvider implementation

        async Task<IReadOnlyList<string>> IGetObjectsDataProvider.GetCatalogsAsync(string? catalogPattern, CancellationToken cancellationToken)
        {
            string sql = new ShowCatalogsCommand(catalogPattern).Build();
            var batches = await ExecuteMetadataSqlAsync(sql, cancellationToken).ConfigureAwait(false);
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

        async Task<IReadOnlyList<(string catalog, string schema)>> IGetObjectsDataProvider.GetSchemasAsync(string? catalogPattern, string? schemaPattern, CancellationToken cancellationToken)
        {
            // Note: catalogPattern comes from GetObjectsResultBuilder which resolves individual
            // catalog names before calling this method. Despite the "pattern" name (from the
            // IGetObjectsDataProvider interface), the value passed to ShowSchemasCommand is used
            // as a literal catalog identifier (backtick-quoted), not a wildcard pattern.
            string sql = new ShowSchemasCommand(catalogPattern, schemaPattern).Build();

            List<RecordBatch> batches;
            try
            {
                batches = await ExecuteMetadataSqlAsync(sql, cancellationToken).ConfigureAwait(false);
            }
            catch (DatabricksException ex) when (ex.IsObjectNotFoundException())
            {
                return System.Array.Empty<(string, string)>();
            }

            // SHOW SCHEMAS IN ALL CATALOGS returns 2 columns: databaseName, catalog
            // SHOW SCHEMAS IN `catalog` returns 1 column: databaseName
            bool showSchemasInAllCatalogs = catalogPattern == null;

            var result = new List<(string, string)>();
            foreach (var batch in batches)
            {
                StringArray? catalogArray = null;
                StringArray? schemaArray = null;

                if (showSchemasInAllCatalogs)
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
                    string catalog = catalogArray != null && !catalogArray.IsNull(i)
                        ? catalogArray.GetString(i)
                        : catalogPattern ?? "";
                    result.Add((catalog, schemaArray.GetString(i)));
                }
            }
            return result;
        }

        async Task<IReadOnlyList<(string catalog, string schema, string table, string tableType)>> IGetObjectsDataProvider.GetTablesAsync(
            string? catalogPattern, string? schemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes, CancellationToken cancellationToken)
        {
            string sql = new ShowTablesCommand(catalogPattern, schemaPattern, tableNamePattern).Build();

            List<RecordBatch> batches;
            try
            {
                batches = await ExecuteMetadataSqlAsync(sql, cancellationToken).ConfigureAwait(false);
            }
            catch (DatabricksException ex) when (ex.IsObjectNotFoundException())
            {
                return System.Array.Empty<(string, string, string, string)>();
            }
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

        async Task IGetObjectsDataProvider.PopulateColumnInfoAsync(string? catalogPattern, string? schemaPattern,
            string? tablePattern, string? columnPattern,
            Dictionary<string, Dictionary<string, Dictionary<string, TableInfo>>> catalogMap,
            CancellationToken cancellationToken)
        {
            List<RecordBatch> batches;
            try
            {
                batches = await ExecuteShowColumnsAsync(catalogPattern, schemaPattern, tablePattern, columnPattern, cancellationToken).ConfigureAwait(false);
            }
            catch (DatabricksException ex) when (ex.IsObjectNotFoundException())
            {
                return;
            }

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

        internal async Task<List<RecordBatch>> ExecuteMetadataSqlAsync(string sql, CancellationToken cancellationToken = default)
        {
            var batches = new List<RecordBatch>();
            using var stmt = (StatementExecutionStatement)CreateStatement();
            stmt.SqlQuery = sql;
            var result = await stmt.ExecuteQueryAsync(cancellationToken, isMetadataExecution: true).ConfigureAwait(false);
            using var stream = result.Stream;
            if (stream == null) return batches;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var batch = await stream.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false);
                if (batch == null) break;
                batches.Add(batch);
            }
            return batches;
        }

        internal List<RecordBatch> ExecuteMetadataSql(string sql, CancellationToken cancellationToken = default)
        {
            return ExecuteMetadataSqlAsync(sql, cancellationToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Executes a SHOW COLUMNS command. When catalog is null, iterates over all catalogs
        /// since SHOW COLUMNS IN ALL CATALOGS is not yet supported by the backend.
        /// </summary>
        internal async Task<List<RecordBatch>> ExecuteShowColumnsAsync(
            string? catalog, string? schemaPattern, string? tablePattern, string? columnPattern,
            CancellationToken cancellationToken)
        {
            if (catalog != null)
            {
                string sql = new ShowColumnsCommand(catalog, schemaPattern, tablePattern, columnPattern).Build();
                return await ExecuteMetadataSqlAsync(sql, cancellationToken).ConfigureAwait(false);
            }

            // SHOW COLUMNS IN ALL CATALOGS is not supported — iterate over each catalog.
            // TODO: Remove this fallback when the backend supports SHOW COLUMNS IN ALL CATALOGS.
            var allBatches = new List<RecordBatch>();
            string catalogsSql = new ShowCatalogsCommand(null).Build();
            var catalogBatches = await ExecuteMetadataSqlAsync(catalogsSql, cancellationToken).ConfigureAwait(false);

            foreach (var batch in catalogBatches)
            {
                var catalogArray = batch.Column(0) as StringArray;
                if (catalogArray == null) continue;
                for (int i = 0; i < catalogArray.Length; i++)
                {
                    if (catalogArray.IsNull(i)) continue;
                    string cat = catalogArray.GetString(i);
                    string sql = new ShowColumnsCommand(cat, schemaPattern, tablePattern, columnPattern).Build();
                    try
                    {
                        var batches = await ExecuteMetadataSqlAsync(sql, cancellationToken).ConfigureAwait(false);
                        allBatches.AddRange(batches);
                    }
                    catch
                    {
                        // Skip catalogs we can't access (permission errors)
                    }
                }
            }
            return allBatches;
        }

        internal bool EnablePKFK => _enablePKFK;

        internal bool EnableMultipleCatalogSupport => _enableMultipleCatalogSupport;

        /// <summary>
        /// Whether to use DESC TABLE EXTENDED AS JSON for GetColumnsExtended.
        /// No server version check is needed for SEA — the flag alone gates the behaviour.
        /// Default: false (falls back to GetColumns + GetPrimaryKeys + GetCrossReference).
        /// </summary>
        internal bool UseDescTableExtended => _useDescTableExtended;

        /// <summary>
        /// Whether to emit <c>DESC TABLE EXTENDED &lt;t&gt; AS JSON STATIC ONLY</c> in place of
        /// the base <c>DESC TABLE EXTENDED &lt;t&gt; AS JSON</c>. SEA always targets a DBSQL
        /// warehouse, so the flag alone is sufficient (no warehouse-path check needed). The
        /// metadata-query header is already sent by <see cref="ExecuteMetadataSqlAsync"/>,
        /// which provides the SEA equivalent of Thrift's RunAsync=false signal.
        /// Default: false.
        /// </summary>
        internal bool EnableFastMetadataQuery => _enableFastMetadataQuery;

        /// <summary>
        /// Returns the session's default catalog. Used by statements when
        /// enableMultipleCatalogSupport=false and no catalog was specified.
        /// </summary>
        internal string? GetSessionDefaultCatalog() => GetCurrentCatalog();

        /// <summary>
        /// Queries the server for the current catalog via SELECT CURRENT_CATALOG().
        /// </summary>
        private string? GetCurrentCatalog()
        {
            var batches = ExecuteMetadataSql("SELECT CURRENT_CATALOG()");
            foreach (var batch in batches)
            {
                if (batch.Length > 0 && batch.Column(0) is StringArray col && !col.IsNull(0))
                {
                    return col.GetString(0);
                }
            }

            return _catalog;
        }

        // Metadata operations (GetObjects/GetTableSchema) are just queries — bound them with the same
        // query timeout as regular queries (adbc.apache.statement.query_timeout_s, default 3h,
        // matching Thrift). A single metadata call can fan out into many SHOW COLUMNS / SHOW CATALOGS
        // statements, so this bounds the whole tree. 0 = no timeout (never-cancelled CTS), matching
        // PollWithTimeoutAsync and the Thrift ApacheUtility.GetCancellationToken semantics.
        internal CancellationTokenSource CreateMetadataTimeoutCts()
        {
            return _queryTimeoutSeconds <= 0
                ? new CancellationTokenSource()
                : new CancellationTokenSource(TimeSpan.FromSeconds(_queryTimeoutSeconds));
        }

        /// <summary>
        /// Extracts server-side properties from connection properties.
        /// Filters properties with the "adbc.databricks.ssp_" prefix, strips the prefix,
        /// and validates names contain only letters, digits, dots, and underscores.
        /// </summary>
        private static Dictionary<string, string> ExtractServerSideProperties(
            IReadOnlyDictionary<string, string> properties)
        {
            var result = new Dictionary<string, string>();
            foreach (var kvp in properties)
            {
                if (kvp.Key.StartsWith(DatabricksParameters.ServerSidePropertyPrefix,
                    StringComparison.OrdinalIgnoreCase))
                {
                    string name = kvp.Key.Substring(DatabricksParameters.ServerSidePropertyPrefix.Length);
                    if (System.Text.RegularExpressions.Regex.IsMatch(name, @"^[a-zA-Z0-9_.]+$"))
                        result[name] = kvp.Value;
                }
            }
            return result;
        }

        /// <summary>
        /// Applies server-side properties (adbc.databricks.ssp_*) by executing
        /// <c>SET key=value</c> statements after the session is open. No-op when
        /// <c>apply_ssp_with_queries=false</c> — in that case the values are
        /// already in <see cref="CreateSessionRequest.SessionConfigs"/> from
        /// <see cref="OpenAsync"/>. Mirrors <c>DatabricksConnection.ApplyServerSidePropertiesAsync</c>
        /// on the Thrift path so both protocols honor the same flag.
        /// </summary>
        public async Task ApplyServerSidePropertiesAsync(CancellationToken cancellationToken = default)
        {
            if (!_applySSPWithQueries)
            {
                return;
            }

            var serverSideProperties = ExtractServerSideProperties(_properties);
            if (serverSideProperties.Count == 0)
            {
                return;
            }

            foreach (var property in serverSideProperties)
            {
                // Backtick-escaped to match the Thrift path's SET escaping (DatabricksConnection.EscapeSqlString)
                // — preserves values containing reserved characters while keeping a SET-compatible literal.
                string escapedValue = "`" + property.Value.Replace("`", "``") + "`";
                string query = $"SET {property.Key}={escapedValue}";

                try
                {
                    using var stmt = (StatementExecutionStatement)CreateStatement();
                    stmt.SqlQuery = query;
                    await stmt.ExecuteUpdateAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Best-effort, matching Thrift behavior — a single bad SSP value should not
                    // tear down the whole session. The error is surfaced in tracing only.
                }
            }
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
        internal bool EnableComplexDatatypeSupport => _enableComplexDatatypeSupport;

        public override string AssemblyVersion => GetType().Assembly.GetName().Version?.ToString() ?? "1.0.0";
        public override string AssemblyName => "AdbcDrivers.Databricks";
    }
}
