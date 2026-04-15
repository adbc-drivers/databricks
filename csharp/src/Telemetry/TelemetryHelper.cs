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
using System.Net.Http;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Auth;
using AdbcDrivers.Databricks.Http;
using AdbcDrivers.Databricks.Reader.CloudFetch;
using AdbcDrivers.Databricks.Telemetry.Models;
using AdbcDrivers.Databricks.Telemetry.Proto;
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;
using ExecutionResultFormat = AdbcDrivers.Databricks.Telemetry.Proto.ExecutionResult.Types.Format;
using OperationType = AdbcDrivers.Databricks.Telemetry.Proto.Operation.Types.Type;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Static helper class for shared telemetry logic.
    /// Provides reusable methods for building telemetry configuration and initializing telemetry
    /// across different connection types (DatabricksConnection and StatementExecutionConnection).
    /// Also provides statement-level telemetry methods shared by both DatabricksStatement and StatementExecutionStatement.
    /// </summary>
    internal static class TelemetryHelper
    {
        /// <summary>
        /// Builds system configuration proto with OS, runtime, and driver information.
        /// </summary>
        /// <param name="driverVersion">The driver version string.</param>
        /// <param name="properties">Connection properties for client app name extraction.</param>
        /// <returns>A populated DriverSystemConfiguration proto.</returns>
        public static DriverSystemConfiguration BuildSystemConfiguration(
            string driverVersion,
            IReadOnlyDictionary<string, string> properties)
        {
            var osVersion = Environment.OSVersion;
            return new DriverSystemConfiguration
            {
                DriverVersion = driverVersion,
                DriverName = "Databricks ADBC Driver",
                OsName = osVersion.Platform.ToString(),
                OsVersion = osVersion.Version.ToString(),
                OsArch = System.Runtime.InteropServices.RuntimeInformation.OSArchitecture.ToString(),
                RuntimeName = System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription,
                RuntimeVersion = Environment.Version.ToString(),
                RuntimeVendor = "Microsoft",
                LocaleName = System.Globalization.CultureInfo.CurrentCulture.Name,
                CharSetEncoding = System.Text.Encoding.Default.WebName,
                ProcessName = Process.GetCurrentProcess().ProcessName,
                ClientAppName = GetClientAppName(properties)
            };
        }

        /// <summary>
        /// Builds driver connection parameters proto with auth, protocol mode, and feature flags.
        /// </summary>
        /// <param name="properties">Connection properties.</param>
        /// <param name="host">The Databricks host.</param>
        /// <param name="mode">The driver mode (THRIFT or SEA).</param>
        /// <param name="enableDirectResults">Whether direct results are enabled.</param>
        /// <param name="useDescTableExtended">Whether DESC TABLE EXTENDED is used.</param>
        /// <returns>A populated DriverConnectionParameters proto.</returns>
        public static DriverConnectionParameters BuildDriverConnectionParams(
            IReadOnlyDictionary<string, string> properties,
            string host,
            DriverMode.Types.Type mode,
            bool enableDirectResults = false,
            bool useDescTableExtended = false)
        {
            properties.TryGetValue("adbc.spark.http_path", out string? httpPath);

            // Determine auth mechanism
            var authMech = DriverAuthMech.Types.Type.Unspecified;
            var authFlow = DriverAuthFlow.Types.Type.Unspecified;

            properties.TryGetValue(SparkParameters.AuthType, out string? authType);
            properties.TryGetValue(DatabricksParameters.OAuthGrantType, out string? grantType);

            bool isAuthenticated = mode == DriverMode.Types.Type.Thrift; // THRIFT requires auth, SEA might not

            if (!string.IsNullOrEmpty(grantType) &&
                grantType == DatabricksConstants.OAuthGrantTypes.ClientCredentials)
            {
                authMech = DriverAuthMech.Types.Type.Oauth;
                authFlow = DriverAuthFlow.Types.Type.ClientCredentials;
            }
            else if (isAuthenticated)
            {
                authMech = DriverAuthMech.Types.Type.Pat;
                authFlow = DriverAuthFlow.Types.Type.TokenPassthrough;
            }

            return new DriverConnectionParameters
            {
                HttpPath = httpPath ?? "",
                Mode = mode,
                HostInfo = new HostDetails
                {
                    HostUrl = $"https://{host}:443",
                    Port = 0
                },
                AuthMech = authMech,
                AuthFlow = authFlow,
                EnableArrow = true, // Always true for ADBC driver
                RowsFetchedPerBlock = GetBatchSize(properties),
                SocketTimeout = GetSocketTimeout(properties),
                EnableDirectResults = enableDirectResults,
                EnableComplexDatatypeSupport = useDescTableExtended,
                AutoCommit = true, // ADBC always uses auto-commit (implicit commits)
            };
        }

        /// <summary>
        /// Initializes telemetry for a connection, creating the TelemetrySessionContext.
        /// All exceptions are swallowed to ensure telemetry failures don't impact connection.
        /// </summary>
        /// <param name="properties">Connection properties.</param>
        /// <param name="host">The Databricks host.</param>
        /// <param name="driverVersion">The driver version string.</param>
        /// <param name="sessionHandle">The Thrift session handle (if applicable).</param>
        /// <param name="openSessionResp">The Thrift OpenSession response (if applicable).</param>
        /// <param name="oauthTokenProvider">Optional OAuth token provider for authenticated telemetry export.</param>
        /// <param name="mode">The driver mode (THRIFT or SEA).</param>
        /// <param name="enableDirectResults">Whether direct results are enabled.</param>
        /// <param name="useDescTableExtended">Whether DESC TABLE EXTENDED is used.</param>
        /// <param name="activity">Optional activity for tracing telemetry initialization.</param>
        /// <returns>The initialized TelemetrySessionContext, or null if telemetry is disabled or initialization fails.</returns>
        public static TelemetrySessionContext? InitializeTelemetry(
            IReadOnlyDictionary<string, string> properties,
            string host,
            string driverVersion,
            TSessionHandle? sessionHandle,
            TOpenSessionResp? openSessionResp,
            OAuthClientCredentialsProvider? oauthTokenProvider,
            DriverMode.Types.Type mode,
            bool enableDirectResults = false,
            bool useDescTableExtended = false,
            Activity? activity = null)
        {
            try
            {
                // Parse telemetry configuration from connection properties
                TelemetryConfiguration telemetryConfig = TelemetryConfiguration.FromProperties(properties);

                // Only initialize telemetry if enabled
                if (!telemetryConfig.Enabled)
                {
                    activity?.AddEvent(new ActivityEvent("telemetry.initialization.skipped",
                        tags: new ActivityTagsCollection { { "reason", "feature_flag_disabled" } }));
                    return null;
                }

                // Validate configuration
                IReadOnlyList<string> validationErrors = telemetryConfig.Validate();
                if (validationErrors.Count > 0)
                {
                    activity?.AddEvent(new ActivityEvent("telemetry.initialization.failed",
                        tags: new ActivityTagsCollection
                        {
                            { "reason", "invalid_configuration" },
                            { "errors", string.Join("; ", validationErrors) }
                        }));
                    return null;
                }

                // Create HTTP client for telemetry export, reusing the connection's OAuth token provider
                HttpClient telemetryHttpClient = HttpClientFactory.CreateTelemetryHttpClient(
                    properties, host, driverVersion, oauthTokenProvider);

                // Get or create telemetry client from manager (per-host singleton)
                ITelemetryClient telemetryClient = TelemetryClientManager.GetInstance().GetOrCreateClient(
                    host,
                    telemetryHttpClient,
                    true, // unauthed failure will be reported separately
                    telemetryConfig);

                // Extract workspace ID from server configuration or connection properties
                long workspaceId = ExtractWorkspaceId(properties, openSessionResp, activity);

                // Create session-level telemetry context for V3 direct-object pipeline
                var telemetrySession = new TelemetrySessionContext
                {
                    SessionId = sessionHandle?.SessionId?.Guid != null
                        ? new Guid(sessionHandle.SessionId.Guid).ToString()
                        : null,
                    WorkspaceId = workspaceId,
                    TelemetryClient = telemetryClient,
                    SystemConfiguration = BuildSystemConfiguration(driverVersion, properties),
                    DriverConnectionParams = BuildDriverConnectionParams(
                        properties, host, mode, enableDirectResults, useDescTableExtended),
                    AuthType = DetermineAuthType(properties)
                };

                activity?.AddEvent(new ActivityEvent("telemetry.initialization.success",
                    tags: new ActivityTagsCollection
                    {
                        { "host", host },
                        { "batch_size", telemetryConfig.BatchSize },
                        { "flush_interval_ms", telemetryConfig.FlushIntervalMs }
                    }));

                return telemetrySession;
            }
            catch (Exception ex)
            {
                // Swallow all telemetry initialization exceptions per design requirement
                // Telemetry failures must not impact connection behavior
                activity?.AddEvent(new ActivityEvent("telemetry.initialization.error",
                    tags: new ActivityTagsCollection
                    {
                        { "error.type", ex.GetType().Name },
                        { "error.message", ex.Message }
                    }));
                return null;
            }
        }

        /// <summary>
        /// Gets the client application name from connection properties or falls back to process name.
        /// </summary>
        private static string GetClientAppName(IReadOnlyDictionary<string, string> properties)
        {
            properties.TryGetValue("adbc.databricks.client_app_name", out string? appName);
            return appName ?? Process.GetCurrentProcess().ProcessName;
        }

        /// <summary>
        /// Gets the batch size from connection properties.
        /// </summary>
        private static int GetBatchSize(IReadOnlyDictionary<string, string> properties)
        {
            const int DefaultBatchSize = 50000; // HiveServer2Connection.BatchSizeDefault
            if (properties.TryGetValue(ApacheParameters.BatchSize, out string? batchSizeStr) &&
                int.TryParse(batchSizeStr, out int batchSize))
            {
                return batchSize;
            }
            return DefaultBatchSize;
        }

        /// <summary>
        /// Gets the socket timeout from connection properties.
        /// </summary>
        private static int GetSocketTimeout(IReadOnlyDictionary<string, string> properties)
        {
            const int DefaultConnectTimeoutMs = 30000; // Default from HiveServer2
            if (properties.TryGetValue(SparkParameters.ConnectTimeoutMilliseconds, out string? timeoutStr) &&
                int.TryParse(timeoutStr, out int timeout))
            {
                return timeout;
            }
            return DefaultConnectTimeoutMs;
        }

        /// <summary>
        /// Determines the auth_type string based on connection properties.
        /// Mapping: PAT -> 'pat', OAuth client_credentials -> 'oauth-m2m', OAuth browser -> 'oauth-u2m', Other -> 'other'
        /// </summary>
        private static string DetermineAuthType(IReadOnlyDictionary<string, string> properties)
        {
            // Check for OAuth grant type first
            properties.TryGetValue(DatabricksParameters.OAuthGrantType, out string? grantType);

            if (!string.IsNullOrEmpty(grantType))
            {
                if (grantType == DatabricksConstants.OAuthGrantTypes.ClientCredentials)
                {
                    // OAuth M2M (machine-to-machine) - client credentials flow
                    return "oauth-m2m";
                }
                else if (grantType == DatabricksConstants.OAuthGrantTypes.AccessToken)
                {
                    // OAuth U2M (user-to-machine) - browser-based flow with access token
                    return "oauth-u2m";
                }
            }

            // Check for PAT (Personal Access Token)
            properties.TryGetValue(SparkParameters.Token, out string? token);
            if (!string.IsNullOrEmpty(token))
            {
                return "pat";
            }

            // Default to 'other' for unknown or unspecified auth types
            return "other";
        }

        /// <summary>
        /// Extracts workspace ID from server configuration or connection properties.
        /// </summary>
        private static long ExtractWorkspaceId(
            IReadOnlyDictionary<string, string> properties,
            TOpenSessionResp? openSessionResp,
            Activity? activity)
        {
            long workspaceId = 0;

            // Strategy 1: Try to extract from server configuration (for clusters)
            if (openSessionResp?.__isset.configuration == true && openSessionResp.Configuration != null)
            {
                if (openSessionResp.Configuration.TryGetValue("spark.databricks.clusterUsageTags.orgId", out string? orgIdStr))
                {
                    if (long.TryParse(orgIdStr, out long parsedOrgId))
                    {
                        workspaceId = parsedOrgId;
                        activity?.AddEvent(new ActivityEvent("telemetry.workspace_id.extracted_from_config",
                            tags: new ActivityTagsCollection { { "workspace_id", workspaceId } }));
                    }
                    else
                    {
                        activity?.AddEvent(new ActivityEvent("telemetry.workspace_id.parse_failed",
                            tags: new ActivityTagsCollection { { "orgId_value", orgIdStr } }));
                    }
                }
            }

            // Strategy 2: Check connection property as fallback
            if (workspaceId == 0 && properties.TryGetValue("adbc.databricks.workspace_id", out string? workspaceIdProp))
            {
                if (long.TryParse(workspaceIdProp, out long propWorkspaceId))
                {
                    workspaceId = propWorkspaceId;
                    activity?.AddEvent(new ActivityEvent("telemetry.workspace_id.from_property",
                        tags: new ActivityTagsCollection { { "workspace_id", workspaceId } }));
                }
            }

            // Log if workspace ID could not be determined
            if (workspaceId == 0)
            {
                activity?.AddEvent(new ActivityEvent("telemetry.workspace_id.unavailable",
                    tags: new ActivityTagsCollection
                    {
                        { "reason", "Not available in server config or connection properties" },
                        { "workaround", "Set adbc.databricks.workspace_id connection property if needed" }
                    }));
            }

            return workspaceId;
        }

        /// <summary>
        /// Creates a statement telemetry context for tracking a single statement execution.
        /// </summary>
        /// <param name="session">The session-level telemetry context.</param>
        /// <param name="statementType">The type of statement (QUERY, UPDATE, METADATA, etc.).</param>
        /// <param name="operationType">The type of operation (EXECUTE_STATEMENT, EXECUTE_STATEMENT_ASYNC, etc.).</param>
        /// <param name="isCompressed">Whether results are compressed.</param>
        /// <param name="isInternalCall">Whether this is an internal call (driver-generated operation).</param>
        /// <returns>A new StatementTelemetryContext, or null if telemetry is disabled.</returns>
        public static StatementTelemetryContext? CreateTelemetryContext(
            TelemetrySessionContext? session,
            Telemetry.Proto.Statement.Types.Type statementType,
            OperationType operationType,
            bool isCompressed,
            bool isInternalCall = false)
        {
            if (session?.TelemetryClient == null)
                return null;

            var ctx = new StatementTelemetryContext(session);
            ctx.StatementType = statementType;
            ctx.OperationType = operationType;
            ctx.IsCompressed = isCompressed;
            ctx.IsInternalCall = isInternalCall;
            return ctx;
        }

        /// <summary>
        /// Records a successful statement execution.
        /// </summary>
        /// <param name="ctx">The statement telemetry context.</param>
        /// <param name="statementId">The statement ID from the server.</param>
        /// <param name="resultFormat">The result format (INLINE_ARROW, EXTERNAL_LINKS, etc.).</param>
        public static void RecordSuccess(
            StatementTelemetryContext ctx,
            string? statementId,
            ExecutionResultFormat resultFormat)
        {
            ctx.RecordFirstBatchReady();
            ctx.ResultFormat = resultFormat;
            ctx.StatementId = statementId;
        }

        /// <summary>
        /// Records an error during statement execution.
        /// </summary>
        /// <param name="ctx">The statement telemetry context.</param>
        /// <param name="ex">The exception that occurred.</param>
        public static void RecordError(StatementTelemetryContext ctx, Exception ex)
        {
            ctx.HasError = true;
            ctx.ErrorName = ex.GetType().Name;
            ctx.ErrorMessage = ex.Message;
        }

        /// <summary>
        /// Emits telemetry for a completed statement execution.
        /// Extracts chunk metrics from CloudFetch reader if applicable and builds the telemetry log.
        /// All exceptions are swallowed to ensure telemetry failures don't impact driver operations.
        /// </summary>
        /// <param name="ctx">The statement telemetry context.</param>
        /// <param name="session">The session-level telemetry context.</param>
        /// <param name="reader">Optional reader to extract chunk metrics from (if CloudFetch).</param>
        public static void EmitTelemetry(
            StatementTelemetryContext ctx,
            TelemetrySessionContext? session,
            IArrowArrayStream? reader = null)
        {
            try
            {
                ctx.RecordResultsConsumed();

                // Extract retry count from Activity if available
                if (Activity.Current != null)
                {
                    var retryCountTag = Activity.Current.GetTagItem("http.retry.total_attempts");
                    if (retryCountTag is int retryCount)
                    {
                        ctx.RetryCount = retryCount;
                    }
                }

                // Extract chunk metrics if this was a CloudFetch query
                if (reader != null)
                {
                    ChunkMetrics? metrics = ExtractChunkMetrics(reader);
                    if (metrics != null)
                    {
                        ctx.SetChunkDetails(
                            metrics.TotalChunksPresent,
                            metrics.TotalChunksIterated,
                            metrics.InitialChunkLatencyMs,
                            metrics.SlowestChunkLatencyMs,
                            metrics.SumChunksDownloadTimeMs);
                    }
                }

                OssSqlDriverTelemetryLog telemetryLog = ctx.BuildTelemetryLog();

                var frontendLog = new TelemetryFrontendLog
                {
                    WorkspaceId = ctx.WorkspaceId,
                    FrontendLogEventId = Guid.NewGuid().ToString(),
                    Context = new FrontendLogContext
                    {
                        TimestampMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    },
                    Entry = new FrontendLogEntry
                    {
                        SqlDriverLog = telemetryLog
                    }
                };

                session?.TelemetryClient?.Enqueue(frontendLog);
            }
            catch
            {
                // Telemetry must never impact driver operations
            }
        }

        /// <summary>
        /// Extracts chunk metrics from a CloudFetch reader.
        /// Handles both CloudFetchReader (direct) and DatabricksCompositeReader (wrapped).
        /// </summary>
        /// <param name="reader">The Arrow array stream reader.</param>
        /// <returns>ChunkMetrics if available, or null.</returns>
        private static ChunkMetrics? ExtractChunkMetrics(IArrowArrayStream reader)
        {
            try
            {
                if (reader is CloudFetchReader cfReader)
                {
                    return cfReader.GetChunkMetrics();
                }
                else if (reader is Reader.DatabricksCompositeReader compositeReader)
                {
                    return compositeReader.GetChunkMetrics();
                }
            }
            catch
            {
                // Ignore errors retrieving chunk metrics - telemetry must not fail driver operations
            }
            return null;
        }
    }
}
