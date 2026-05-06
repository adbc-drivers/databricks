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
using System.IO;
using System.Net.Http;
using System.Reflection;
using System.Runtime.InteropServices;
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

                // PECO-2998 / PECO-2996: build each session field defensively. Previously a single
                // exception inside BuildSystemConfiguration / BuildDriverConnectionParams would
                // either propagate to the outer catch (yielding NoOp telemetry) or — depending
                // on the build order — leave a partially-populated session. Both nested
                // auth_mech/auth_flow and the top-level auth_type must always agree, so we derive
                // them from a single helper that can never drop one without the other.
                Proto.DriverSystemConfiguration systemConfiguration =
                    SafeBuildSystemConfiguration(assemblyVersion, activity);
                Proto.DriverConnectionParameters driverConnectionParams =
                    SafeBuildDriverConnectionParams(
                        properties, host, enableDirectResults, useDescTableExtended,
                        connectTimeoutMilliseconds, activity);
                string authType = SafeDetermineAuthType(properties, activity);

                var session = new TelemetrySessionContext
                {
                    SessionId = sessionHandle?.SessionId?.Guid != null
                        ? new System.Guid(sessionHandle.SessionId.Guid).ToString()
                        : null,
                    TelemetryClient = telemetryClient,
                    SystemConfiguration = systemConfiguration,
                    DriverConnectionParams = driverConnectionParams,
                    AuthType = authType
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

        // DriverSystemConfiguration is effectively process-wide constant: OS, runtime, driver
        // version, process name, locale and encoding do not change during process lifetime.
        // Cache the first-computed value to avoid repeated /etc/os-release reads and reflection
        // lookups on every new connection.
        private static Proto.DriverSystemConfiguration? s_systemConfiguration;

        internal static Proto.DriverSystemConfiguration BuildSystemConfiguration(string assemblyVersion)
        {
            return LazyInitializer.EnsureInitialized(
                ref s_systemConfiguration,
                () => CreateSystemConfiguration(assemblyVersion))!;
        }

        // ── PECO-2998 / PECO-2996: Safe wrappers used by Create() ────────────────────────
        // Each wrapper either returns the real builder's output or a best-effort fallback
        // proto that still populates the always-derivable fields (driver_name,
        // runtime_vendor, the constant connection-param flags, and an "unknown" auth_type
        // string). This keeps the session non-null on partial-init failures so that the
        // first row from the connection still reports identifying data instead of being
        // dropped via the outer NoOp fallback.

        internal static Proto.DriverSystemConfiguration SafeBuildSystemConfiguration(
            string assemblyVersion, Activity? activity)
        {
            try
            {
                return BuildSystemConfiguration(assemblyVersion);
            }
            catch (Exception ex)
            {
                activity?.AddEvent(new ActivityEvent("telemetry.system_configuration.fallback",
                    tags: new ActivityTagsCollection
                    {
                        { "error.type", ex.GetType().Name },
                        { "error.message", ex.Message }
                    }));
                return new Proto.DriverSystemConfiguration
                {
                    DriverVersion = assemblyVersion ?? string.Empty,
                    DriverName = DatabricksConnection.DatabricksDriverName,
                    RuntimeVendor = "Microsoft"
                };
            }
        }

        internal static Proto.DriverConnectionParameters SafeBuildDriverConnectionParams(
            IReadOnlyDictionary<string, string> properties,
            string host,
            bool enableDirectResults,
            bool useDescTableExtended,
            int connectTimeoutMilliseconds,
            Activity? activity)
        {
            try
            {
                return BuildDriverConnectionParams(
                    properties, host, enableDirectResults, useDescTableExtended,
                    connectTimeoutMilliseconds);
            }
            catch (Exception ex)
            {
                activity?.AddEvent(new ActivityEvent("telemetry.driver_connection_params.fallback",
                    tags: new ActivityTagsCollection
                    {
                        { "error.type", ex.GetType().Name },
                        { "error.message", ex.Message }
                    }));

                // Fallback that still surfaces the always-derivable, driver-side constants.
                // Auth descriptors fall back to Unspecified rather than guessing.
                return new Proto.DriverConnectionParameters
                {
                    HttpPath = string.Empty,
                    Mode = Proto.DriverMode.Types.Type.Thrift,
                    HostInfo = new Proto.HostDetails
                    {
                        HostUrl = host ?? string.Empty,
                        Port = DefaultHttpsPort
                    },
                    AuthMech = Proto.DriverAuthMech.Types.Type.Unspecified,
                    AuthFlow = Proto.DriverAuthFlow.Types.Type.Unspecified,
                    EnableArrow = true,
                    EnableDirectResults = enableDirectResults,
                    SocketTimeout = connectTimeoutMilliseconds > 0 ? connectTimeoutMilliseconds / 1000 : 0,
                    EnableComplexDatatypeSupport = useDescTableExtended,
                    AutoCommit = true,
                };
            }
        }

        internal static string SafeDetermineAuthType(
            IReadOnlyDictionary<string, string> properties, Activity? activity)
        {
            try
            {
                return DetermineAuthType(properties);
            }
            catch (Exception ex)
            {
                activity?.AddEvent(new ActivityEvent("telemetry.auth_type.fallback",
                    tags: new ActivityTagsCollection
                    {
                        { "error.type", ex.GetType().Name },
                        { "error.message", ex.Message }
                    }));
                return "other";
            }
        }

        private static Proto.DriverSystemConfiguration CreateSystemConfiguration(string assemblyVersion)
        {
            // PECO-2996: build the proto field-by-field with each value wrapped in a safe-getter.
            // Previously a single throw inside e.g. RuntimeInformation.OSArchitecture would
            // propagate to ConnectionTelemetry.Create's outer catch and discard ALL telemetry
            // for that connection. Now any individual field that fails simply falls back to
            // empty / a sensible default, so always-derivable fields (driver_name,
            // runtime_vendor) are populated even on partially-failing init paths.
            var config = new Proto.DriverSystemConfiguration
            {
                DriverVersion = assemblyVersion ?? string.Empty,
                DriverName = DatabricksConnection.DatabricksDriverName,
                // runtime_vendor is a process-wide constant for .NET — always emit it so the
                // 110-row gap reported in PECO-2996 closes regardless of which other lookup
                // throws.
                RuntimeVendor = "Microsoft",
            };

            TrySet(() => config.OsName = System.Environment.OSVersion.Platform.ToString());
            TrySet(() => config.OsVersion = GetOsVersion());
            TrySet(() => config.OsArch = RuntimeInformation.OSArchitecture.ToString());
            TrySet(() => config.RuntimeName = RuntimeInformation.FrameworkDescription);
            TrySet(() => config.RuntimeVersion = System.Environment.Version.ToString());
            TrySet(() => config.LocaleName = System.Globalization.CultureInfo.CurrentCulture.Name);
            // Normalize to upper-case (e.g. "UTF-8") to match the casing emitted by the
            // OSS JDBC and DatabricksJDBC drivers; .NET Core / Linux returns "utf-8".
            TrySet(() => config.CharSetEncoding = System.Text.Encoding.Default.WebName.ToUpperInvariant());

            // Prefer the entry assembly name so process_name reflects the actual application
            // (e.g. "MyApp") rather than the .NET host executable name ("dotnet"). Fall back
            // to the OS process name when there is no entry assembly (e.g. unmanaged hosts).
            string? processName = null;
            TrySet(() => processName = Assembly.GetEntryAssembly()?.GetName().Name);
            if (string.IsNullOrEmpty(processName))
            {
                TrySet(() => processName = Process.GetCurrentProcess().ProcessName);
            }
            if (!string.IsNullOrEmpty(processName))
            {
                config.ProcessName = processName!;
                config.ClientAppName = processName!;
            }

            return config;
        }

        // Run an action and swallow any exception. Used to keep CreateSystemConfiguration
        // resilient on a per-field basis without polluting it with try/catch boilerplate.
        // Telemetry must never impact driver operations.
        private static void TrySet(Action action)
        {
            try
            {
                action();
            }
            catch
            {
                // Intentionally swallowed.
            }
        }

        // Returns a human-readable OS version string, preferring the distro "PRETTY_NAME"
        // from /etc/os-release on Linux (e.g. "Ubuntu 22.04.3 LTS") over the kernel release
        // (e.g. "5.4.0.1156") that Environment.OSVersion.Version reports there.
        internal static string GetOsVersion()
        {
            try
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                {
                    string? prettyName = TryReadLinuxPrettyName();
                    if (!string.IsNullOrEmpty(prettyName))
                    {
                        return prettyName!;
                    }
                }

                string description = RuntimeInformation.OSDescription;
                if (!string.IsNullOrEmpty(description))
                {
                    return description;
                }
            }
            catch
            {
                // Telemetry must never impact driver operations; fall through to the kernel version.
            }

            return System.Environment.OSVersion.Version.ToString();
        }

        private static string? TryReadLinuxPrettyName()
        {
            try
            {
                const string osReleasePath = "/etc/os-release";
                if (!File.Exists(osReleasePath))
                {
                    return null;
                }

                foreach (string line in File.ReadAllLines(osReleasePath))
                {
                    const string key = "PRETTY_NAME=";
                    if (line.StartsWith(key, StringComparison.Ordinal))
                    {
                        return UnquoteOsReleaseValue(line.Substring(key.Length));
                    }
                }
            }
            catch
            {
                // Intentionally swallowed - telemetry must not fail.
            }

            return null;
        }

        // os-release(5) allows values to be wrapped in double or single quotes.
        internal static string UnquoteOsReleaseValue(string raw)
        {
            if (raw.Length >= 2
                && ((raw[0] == '"' && raw[raw.Length - 1] == '"')
                    || (raw[0] == '\'' && raw[raw.Length - 1] == '\'')))
            {
                return raw.Substring(1, raw.Length - 2);
            }
            return raw;
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

            AuthDescriptors auth = DetermineAuthDescriptors(properties);

            int batchSize = GetBatchSize(properties);
            int asyncPollIntervalMillis = GetAsyncPollIntervalMillis(properties);

            return new Proto.DriverConnectionParameters
            {
                HttpPath = httpPath ?? "",
                Mode = Proto.DriverMode.Types.Type.Thrift,
                HostInfo = new Proto.HostDetails
                {
                    // Bare hostname, matching JDBC. Scheme is implicit (always https) and
                    // port is reported in the sibling Port field, so including them here
                    // was redundant and forced analysts to normalize across drivers.
                    HostUrl = host ?? string.Empty,
                    Port = port
                },
                AuthMech = auth.AuthMech,
                AuthFlow = auth.AuthFlow,
                // PECO-2996: these flags are best-effort constants and must always be populated,
                // even on partial-init paths. They reflect driver-side behavior that doesn't
                // depend on any backend negotiation.
                EnableArrow = true,
                RowsFetchedPerBlock = batchSize,
                SocketTimeout = connectTimeoutMilliseconds > 0 ? connectTimeoutMilliseconds / 1000 : 0,
                EnableDirectResults = enableDirectResults,
                EnableComplexDatatypeSupport = useDescTableExtended,
                AutoCommit = true,
                AsyncPollIntervalMillis = asyncPollIntervalMillis,
            };
        }

        // PECO-2997: report the async-execution poll interval (in milliseconds) used by
        // DatabricksStatement when polling for query completion. The driver overrides
        // the Apache base default (500ms) with DefaultAsyncExecPollIntervalMs (100ms);
        // callers can override via ApacheParameters.PollTimeMilliseconds.
        private static int GetAsyncPollIntervalMillis(IReadOnlyDictionary<string, string> properties)
        {
            if (properties.TryGetValue(ApacheParameters.PollTimeMilliseconds, out string? pollTimeStr) &&
                int.TryParse(pollTimeStr, out int pollTime) && pollTime > 0)
            {
                return pollTime;
            }
            return DatabricksConstants.DefaultAsyncExecPollIntervalMs;
        }

        /// <summary>
        /// Derives the nested <c>auth_mech</c>/<c>auth_flow</c> and the top-level
        /// <c>auth_type</c> string together so they cannot drift.
        /// </summary>
        /// <remarks>
        /// PECO-2998: prior to this consolidation, <see cref="BuildDriverConnectionParams"/>
        /// keyed off <c>SparkParameters.AuthType</c> while <c>DetermineAuthType</c> keyed off
        /// <c>OAuthGrantType</c> alone, so dashboards saw mismatches such as
        /// <c>auth_mech=OAUTH</c> with <c>auth_type=NULL</c> (or worse, <c>auth_type=pat</c>
        /// for OAuth U2M token passthrough). Both fields are now derived from a single
        /// branching decision.
        /// </remarks>
        private static AuthDescriptors DetermineAuthDescriptors(IReadOnlyDictionary<string, string> properties)
        {
            properties.TryGetValue(SparkParameters.AuthType, out string? authType);
            properties.TryGetValue(DatabricksParameters.OAuthGrantType, out string? grantType);

            bool isOAuth = SparkAuthTypeParser.TryParse(authType, out SparkAuthType authTypeValue)
                && authTypeValue == SparkAuthType.OAuth;

            if (isOAuth)
            {
                bool isClientCredentials = !string.IsNullOrEmpty(grantType) &&
                    grantType == DatabricksConstants.OAuthGrantTypes.ClientCredentials;

                return new AuthDescriptors(
                    Proto.DriverAuthMech.Types.Type.Oauth,
                    isClientCredentials
                        ? Proto.DriverAuthFlow.Types.Type.ClientCredentials
                        : Proto.DriverAuthFlow.Types.Type.TokenPassthrough,
                    // Top-level auth_type must reflect the actual OAuth shape, not just the
                    // optional grant_type property. When grant_type is absent (U2M access-token
                    // passthrough) the previous code returned "pat" or "other" even though
                    // auth_mech=OAUTH — that is the 90-row drift fixed by PECO-2998.
                    !string.IsNullOrEmpty(grantType)
                        ? $"oauth-{grantType}"
                        : "oauth-access_token");
            }

            // PAT / token-based or unspecified.
            properties.TryGetValue(SparkParameters.Token, out string? token);
            string topLevelAuthType = !string.IsNullOrEmpty(token) ? "pat" : "other";
            return new AuthDescriptors(
                Proto.DriverAuthMech.Types.Type.Pat,
                Proto.DriverAuthFlow.Types.Type.TokenPassthrough,
                topLevelAuthType);
        }

        /// <summary>
        /// Determines the top-level <c>auth_type</c> string consistently with
        /// <see cref="BuildDriverConnectionParams"/>. Exposed as <c>internal</c> so the
        /// telemetry init path and unit tests can call it directly.
        /// </summary>
        internal static string DetermineAuthType(IReadOnlyDictionary<string, string> properties)
        {
            return DetermineAuthDescriptors(properties).AuthType;
        }

        private readonly struct AuthDescriptors
        {
            public AuthDescriptors(
                Proto.DriverAuthMech.Types.Type authMech,
                Proto.DriverAuthFlow.Types.Type authFlow,
                string authType)
            {
                AuthMech = authMech;
                AuthFlow = authFlow;
                AuthType = authType;
            }

            public Proto.DriverAuthMech.Types.Type AuthMech { get; }
            public Proto.DriverAuthFlow.Types.Type AuthFlow { get; }
            public string AuthType { get; }
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
