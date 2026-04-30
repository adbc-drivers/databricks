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

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Telemetry implementation for the SEA (Statement Execution API / REST) protocol.
    /// Mirrors ConnectionTelemetry but does not require a Thrift session handle.
    /// Metadata operations (GetObjects, GetTableTypes) are not instrumented at the
    /// connection level for SEA; statement-level telemetry is emitted per-statement
    /// inside StatementExecutionStatement.
    /// </summary>
    internal sealed class SeaConnectionTelemetry : IConnectionTelemetry
    {
        private readonly string _host;
        private ITelemetryClient? _telemetryClient;

        /// <inheritdoc/>
        public TelemetrySessionContext? Session { get; }

        internal SeaConnectionTelemetry(
            string host,
            ITelemetryClient telemetryClient,
            TelemetrySessionContext session)
        {
            _host = host;
            _telemetryClient = telemetryClient;
            Session = session;
        }

        /// <summary>
        /// Creates an <see cref="IConnectionTelemetry"/> instance for the SEA protocol.
        /// Returns <see cref="NoOpConnectionTelemetry"/> if telemetry is disabled, misconfigured, or fails to initialize.
        /// Never throws.
        /// </summary>
        public static IConnectionTelemetry Create(
            IReadOnlyDictionary<string, string> properties,
            string host,
            string assemblyVersion,
            string? sessionId,
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
                    properties, host, assemblyVersion);

                ITelemetryClient telemetryClient = TelemetryClientManager.GetInstance().GetOrCreateClient(
                    host,
                    telemetryHttpClient,
                    true,
                    telemetryConfig);

                var session = new TelemetrySessionContext
                {
                    SessionId = sessionId,
                    TelemetryClient = telemetryClient,
                    SystemConfiguration = ConnectionTelemetry.BuildSystemConfiguration(assemblyVersion),
                    DriverConnectionParams = BuildDriverConnectionParams(properties, host),
                    AuthType = DetermineAuthType(properties)
                };

                activity?.AddEvent(new ActivityEvent("telemetry.initialization.success",
                    tags: new ActivityTagsCollection
                    {
                        { "host", host },
                        { "batch_size", telemetryConfig.BatchSize },
                        { "flush_interval_ms", telemetryConfig.FlushIntervalMs }
                    }));

                return new SeaConnectionTelemetry(host, telemetryClient, session);
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

        /// <inheritdoc/>
        /// <remarks>
        /// For SEA connections, metadata operations are executed directly via SQL;
        /// no connection-level wrapping is needed. Telemetry is emitted per-statement
        /// inside StatementExecutionStatement.
        /// </remarks>
        public T ExecuteWithMetadataTelemetry<T>(
            Proto.Operation.Types.Type operationType,
            Func<T> operation,
            Activity? activity)
        {
            // Delegate directly — metadata telemetry for SEA is emitted at statement level.
            return operation();
        }

        /// <inheritdoc/>
        public async Task DisposeAsync()
        {
            try
            {
                if (_telemetryClient != null && !string.IsNullOrEmpty(_host))
                {
                    try
                    {
                        await _telemetryClient.FlushAsync(CancellationToken.None).ConfigureAwait(false);
                    }
                    catch
                    {
                        // Ignore flush errors during dispose
                    }

                    try
                    {
                        await TelemetryClientManager.GetInstance()
                            .ReleaseClientAsync(_host)
                            .ConfigureAwait(false);
                    }
                    catch
                    {
                        // Ignore release errors during dispose
                    }

                    _telemetryClient = null;
                }
            }
            catch
            {
                // Telemetry must never impact driver operations
            }
        }

        private static Proto.DriverConnectionParameters BuildDriverConnectionParams(
            IReadOnlyDictionary<string, string> properties,
            string host)
        {
            properties.TryGetValue(HiveServer2.Spark.SparkParameters.Path, out string? httpPath);

            int port = 443;
            if (properties.TryGetValue(HiveServer2.Spark.SparkParameters.Port, out string? portStr) &&
                int.TryParse(portStr, out int parsedPort) && parsedPort > 0)
            {
                port = parsedPort;
            }
            else if (properties.TryGetValue(Apache.Arrow.Adbc.AdbcOptions.Uri, out string? uri) &&
                     !string.IsNullOrEmpty(uri) &&
                     Uri.TryCreate(uri, UriKind.Absolute, out Uri? parsedUri) &&
                     parsedUri.Port > 0)
            {
                port = parsedUri.Port;
            }

            var authMech = Proto.DriverAuthMech.Types.Type.Unspecified;
            var authFlow = Proto.DriverAuthFlow.Types.Type.Unspecified;

            properties.TryGetValue(HiveServer2.Spark.SparkParameters.AuthType, out string? authType);
            properties.TryGetValue(DatabricksParameters.OAuthGrantType, out string? grantType);

            bool isOAuth = HiveServer2.Spark.SparkAuthTypeParser.TryParse(authType, out HiveServer2.Spark.SparkAuthType authTypeValue)
                && authTypeValue == HiveServer2.Spark.SparkAuthType.OAuth;

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

            int batchSize = (int)DatabricksStatement.DatabricksBatchSizeDefault;

            return new Proto.DriverConnectionParameters
            {
                HttpPath = httpPath ?? "",
                Mode = Proto.DriverMode.Types.Type.Sea,
                HostInfo = new Proto.HostDetails
                {
                    HostUrl = host,
                    Port = port
                },
                AuthMech = authMech,
                AuthFlow = authFlow,
                EnableArrow = true,
                RowsFetchedPerBlock = batchSize,
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

            properties.TryGetValue(HiveServer2.Spark.SparkParameters.Token, out string? token);
            if (!string.IsNullOrEmpty(token))
            {
                return "pat";
            }

            return "other";
        }
    }
}
