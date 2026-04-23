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
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Auth;
using AdbcDrivers.Databricks.Http;
using AdbcDrivers.Databricks.Telemetry.Models;
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow.Adbc;
using Apache.Hive.Service.Rpc.Thrift;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Real telemetry implementation that instruments connection-level operations.
    /// Use <see cref="Create"/> factory method which returns <see cref="NoOpConnectionTelemetry"/>
    /// when telemetry is disabled or initialization fails.
    /// </summary>
    internal sealed class ConnectionTelemetry : IConnectionTelemetry
    {
        private readonly string _host;
        private ITelemetryClient? _telemetryClient;

        public TelemetrySessionContext? Session { get; }

        private ConnectionTelemetry(
            string host,
            ITelemetryClient telemetryClient,
            TelemetrySessionContext session)
        {
            _host = host;
            _telemetryClient = telemetryClient;
            Session = session;
        }

        /// <summary>
        /// Creates an <see cref="IConnectionTelemetry"/> instance.
        /// Returns <see cref="NoOpConnectionTelemetry"/> if telemetry is disabled, misconfigured, or fails to initialize.
        /// Never throws.
        /// </summary>
        public static IConnectionTelemetry Create(
            IReadOnlyDictionary<string, string> properties,
            string host,
            string assemblyVersion,
            OAuthClientCredentialsProvider? oauthTokenProvider,
            TSessionHandle? sessionHandle,
            bool enableDirectResults,
            bool useDescTableExtended,
            int connectTimeoutMilliseconds,
            Activity? activity)
        {
            try
            {
                TelemetryConfiguration telemetryConfig = TelemetryConfiguration.FromProperties(properties);

                if (!telemetryConfig.Enabled)
                {
                    activity?.AddEvent(new ActivityEvent("telemetry.initialization.skipped",
                        tags: new ActivityTagsCollection { { "reason", "feature_flag_disabled" } }));
                    return NoOpConnectionTelemetry.Instance;
                }

                IReadOnlyList<string> validationErrors = telemetryConfig.Validate();
                if (validationErrors.Count > 0)
                {
                    activity?.AddEvent(new ActivityEvent("telemetry.initialization.failed",
                        tags: new ActivityTagsCollection
                        {
                            { "reason", "invalid_configuration" },
                            { "errors", string.Join("; ", validationErrors) }
                        }));
                    return NoOpConnectionTelemetry.Instance;
                }

                HttpClient telemetryHttpClient = HttpClientFactory.CreateTelemetryHttpClient(
                    properties, host, assemblyVersion, oauthTokenProvider);

                ITelemetryClient telemetryClient = TelemetryClientManager.GetInstance().GetOrCreateClient(
                    host,
                    telemetryHttpClient,
                    true,
                    telemetryConfig);

                var session = new TelemetrySessionContext
                {
                    SessionId = sessionHandle?.SessionId?.Guid != null
                        ? new System.Guid(sessionHandle.SessionId.Guid).ToString()
                        : null,
                    TelemetryClient = telemetryClient,
                    SystemConfiguration = BuildSystemConfiguration(assemblyVersion),
                    DriverConnectionParams = BuildDriverConnectionParams(
                        properties, host, enableDirectResults, useDescTableExtended, connectTimeoutMilliseconds),
                    AuthType = DetermineAuthType(properties)
                };

                activity?.AddEvent(new ActivityEvent("telemetry.initialization.success",
                    tags: new ActivityTagsCollection
                    {
                        { "host", host },
                        { "batch_size", telemetryConfig.BatchSize },
                        { "flush_interval_ms", telemetryConfig.FlushIntervalMs }
                    }));

                return new ConnectionTelemetry(host, telemetryClient, session);
            }
            catch (Exception ex)
            {
                activity?.AddEvent(new ActivityEvent("telemetry.initialization.error",
                    tags: new ActivityTagsCollection
                    {
                        { "error.type", ex.GetType().Name },
                        { "error.message", ex.Message }
                    }));
                return NoOpConnectionTelemetry.Instance;
            }
        }

        public T ExecuteWithMetadataTelemetry<T>(
            Proto.Operation.Types.Type operationType,
            Func<T> operation,
            Activity? activity)
        {
            StatementTelemetryContext? telemetryContext = null;
            try
            {
                if (Session?.TelemetryClient != null)
                {
                    telemetryContext = new StatementTelemetryContext(Session)
                    {
                        StatementType = Proto.Statement.Types.Type.Metadata,
                        OperationType = operationType,
                        ResultFormat = Proto.ExecutionResult.Types.Format.InlineArrow,
                        IsCompressed = false
                    };

                    activity?.SetTag("telemetry.operation_type", operationType.ToString());
                    activity?.SetTag("telemetry.statement_type", "METADATA");
                }
            }
            catch (Exception ex)
            {
                activity?.AddEvent(new ActivityEvent("telemetry.context_creation.error",
                    tags: new ActivityTagsCollection
                    {
                        { "error.type", ex.GetType().Name },
                        { "error.message", ex.Message }
                    }));
            }

            T result;
            try
            {
                result = operation();
            }
            catch (Exception ex)
            {
                if (telemetryContext != null)
                {
                    try
                    {
                        telemetryContext.HasError = true;
                        telemetryContext.ErrorName = ex.GetType().Name;
                        telemetryContext.ErrorMessage = ex.Message;
                    }
                    catch
                    {
                        // Swallow telemetry errors
                    }
                }
                throw;
            }
            finally
            {
                if (telemetryContext != null)
                {
                    try
                    {
                        var telemetryLog = telemetryContext.BuildTelemetryLog();

                        var frontendLog = new TelemetryFrontendLog
                        {
                            WorkspaceId = telemetryContext.WorkspaceId,
                            FrontendLogEventId = System.Guid.NewGuid().ToString(),
                            Context = new FrontendLogContext
                            {
                                TimestampMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                            },
                            Entry = new FrontendLogEntry
                            {
                                SqlDriverLog = telemetryLog
                            }
                        };

                        Session?.TelemetryClient?.Enqueue(frontendLog);
                    }
                    catch (Exception ex)
                    {
                        activity?.AddEvent(new ActivityEvent("telemetry.emit.error",
                            tags: new ActivityTagsCollection
                            {
                                { "error.type", ex.GetType().Name },
                                { "error.message", ex.Message }
                            }));
                    }
                }
            }

            return result;
        }

        public async Task DisposeAsync()
        {
            try
            {
                if (_telemetryClient != null && !string.IsNullOrEmpty(_host))
                {
                    Activity.Current?.AddEvent(new ActivityEvent("telemetry.dispose.started",
                        tags: new ActivityTagsCollection { { "host", _host } }));

                    try
                    {
                        await _telemetryClient.FlushAsync(CancellationToken.None).ConfigureAwait(false);
                        Activity.Current?.AddEvent(new ActivityEvent("telemetry.dispose.flushed"));
                    }
                    catch (Exception ex)
                    {
                        Activity.Current?.AddEvent(new ActivityEvent("telemetry.dispose.flush_error",
                            tags: new ActivityTagsCollection
                            {
                                { "error.type", ex.GetType().Name },
                                { "error.message", ex.Message }
                            }));
                    }

                    try
                    {
                        await TelemetryClientManager.GetInstance()
                            .ReleaseClientAsync(_host)
                            .ConfigureAwait(false);
                        Activity.Current?.AddEvent(new ActivityEvent("telemetry.dispose.client_released"));
                    }
                    catch (Exception ex)
                    {
                        Activity.Current?.AddEvent(new ActivityEvent("telemetry.dispose.release_error",
                            tags: new ActivityTagsCollection
                            {
                                { "error.type", ex.GetType().Name },
                                { "error.message", ex.Message }
                            }));
                    }

                    _telemetryClient = null;

                    Activity.Current?.AddEvent(new ActivityEvent("telemetry.dispose.completed"));
                }
            }
            catch (Exception ex)
            {
                Activity.Current?.AddEvent(new ActivityEvent("telemetry.dispose.error",
                    tags: new ActivityTagsCollection
                    {
                        { "error.type", ex.GetType().Name },
                        { "error.message", ex.Message }
                    }));
            }
        }

        private static Proto.DriverSystemConfiguration BuildSystemConfiguration(string assemblyVersion)
        {
            var osVersion = System.Environment.OSVersion;
            var processName = Process.GetCurrentProcess().ProcessName;
            return new Proto.DriverSystemConfiguration
            {
                DriverVersion = assemblyVersion,
                DriverName = "Databricks ADBC Driver",
                OsName = osVersion.Platform.ToString(),
                OsVersion = osVersion.Version.ToString(),
                OsArch = System.Runtime.InteropServices.RuntimeInformation.OSArchitecture.ToString(),
                RuntimeName = System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription,
                RuntimeVersion = System.Environment.Version.ToString(),
                RuntimeVendor = "Microsoft",
                LocaleName = System.Globalization.CultureInfo.CurrentCulture.Name,
                CharSetEncoding = System.Text.Encoding.Default.WebName,
                ProcessName = processName,
                ClientAppName = processName
            };
        }

        internal static Proto.DriverConnectionParameters BuildDriverConnectionParams(
            IReadOnlyDictionary<string, string> properties,
            string host,
            bool enableDirectResults,
            bool useDescTableExtended,
            int connectTimeoutMilliseconds)
        {
            properties.TryGetValue(SparkParameters.Path, out string? httpPath);
            int port = ResolvePort(properties);

            var authMech = Proto.DriverAuthMech.Types.Type.Unspecified;
            var authFlow = Proto.DriverAuthFlow.Types.Type.Unspecified;

            properties.TryGetValue(SparkParameters.AuthType, out string? authType);
            properties.TryGetValue(DatabricksParameters.OAuthGrantType, out string? grantType);

            bool isOAuth = SparkAuthTypeParser.TryParse(authType, out SparkAuthType authTypeValue)
                && authTypeValue == SparkAuthType.OAuth;

            if (isOAuth)
            {
                authMech = Proto.DriverAuthMech.Types.Type.Oauth;
                authFlow = !string.IsNullOrEmpty(grantType) &&
                           grantType == DatabricksConstants.OAuthGrantTypes.ClientCredentials
                    ? Proto.DriverAuthFlow.Types.Type.ClientCredentials
                    : Proto.DriverAuthFlow.Types.Type.TokenPassthrough;
            }
            else
            {
                authMech = Proto.DriverAuthMech.Types.Type.Pat;
                authFlow = Proto.DriverAuthFlow.Types.Type.TokenPassthrough;
            }

            int batchSize = GetBatchSize(properties);

            return new Proto.DriverConnectionParameters
            {
                HttpPath = httpPath ?? "",
                Mode = Proto.DriverMode.Types.Type.Thrift,
                HostInfo = new Proto.HostDetails
                {
                    // Bare hostname, matching JDBC. Scheme is implicit (always https) and
                    // port is reported in the sibling Port field, so including them here
                    // was redundant and forced analysts to normalize across drivers.
                    HostUrl = host,
                    Port = port
                },
                AuthMech = authMech,
                AuthFlow = authFlow,
                EnableArrow = true,
                RowsFetchedPerBlock = batchSize,
                SocketTimeout = connectTimeoutMilliseconds / 1000,
                EnableDirectResults = enableDirectResults,
                EnableComplexDatatypeSupport = useDescTableExtended,
                AutoCommit = true,
            };
        }

        private static string DetermineAuthType(IReadOnlyDictionary<string, string> properties)
        {
            properties.TryGetValue(DatabricksParameters.OAuthGrantType, out string? grantType);

            if (!string.IsNullOrEmpty(grantType))
            {
                return $"oauth-{grantType}";
            }

            properties.TryGetValue(SparkParameters.Token, out string? token);
            if (!string.IsNullOrEmpty(token))
            {
                return "pat";
            }

            return "other";
        }

        private static int GetBatchSize(IReadOnlyDictionary<string, string> properties)
        {
            if (properties.TryGetValue(ApacheParameters.BatchSize, out string? batchSizeStr) &&
                int.TryParse(batchSizeStr, out int batchSize))
            {
                return batchSize;
            }
            return (int)DatabricksStatement.DatabricksBatchSizeDefault;
        }

        private const int DefaultHttpsPort = 443;

        private static int ResolvePort(IReadOnlyDictionary<string, string> properties)
        {
            if (properties.TryGetValue(SparkParameters.Port, out string? portStr) &&
                int.TryParse(portStr, out int port) && port > 0)
            {
                return port;
            }

            if (properties.TryGetValue(AdbcOptions.Uri, out string? uri) &&
                !string.IsNullOrEmpty(uri) &&
                Uri.TryCreate(uri, UriKind.Absolute, out Uri? parsedUri) &&
                parsedUri.Port > 0)
            {
                return parsedUri.Port;
            }

            return DefaultHttpsPort;
        }
    }
}
