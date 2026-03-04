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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry.Models;
using AdbcDrivers.Databricks.Telemetry.Proto;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Per-connection aggregator that collects Activity data by statement_id,
    /// builds proto messages, and enqueues them to the shared <see cref="ITelemetryClient"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Each connection creates one MetricsAggregator instance. Activities are routed
    /// to <see cref="StatementTelemetryContext"/> instances keyed by statement_id.
    /// When a root activity completes (status is Ok or Error), the aggregated proto
    /// message is built, wrapped in a <see cref="TelemetryFrontendLog"/>, and enqueued
    /// to the <see cref="ITelemetryClient"/>.
    /// </para>
    /// <para>
    /// A root activity is defined as one whose parent is null or whose parent's
    /// ActivitySource name is not "Databricks.Adbc.Driver".
    /// </para>
    /// <para>
    /// All exceptions are swallowed to ensure telemetry never impacts driver operations.
    /// </para>
    /// </remarks>
    internal sealed class MetricsAggregator : IDisposable
    {
        /// <summary>
        /// The ActivitySource name used by the Databricks ADBC driver.
        /// Used to determine if an activity is a root activity.
        /// This must match the assembly name used by <see cref="DatabricksConnection"/>
        /// as the ActivitySource name in <c>TracingConnection</c>.
        /// </summary>
        internal static readonly string DatabricksActivitySourceName =
            DatabricksActivityListener.DatabricksActivitySourceName;

        private readonly ITelemetryClient _telemetryClient;
        private readonly TelemetryConfiguration _config;
        private readonly ConcurrentDictionary<string, StatementTelemetryContext> _contexts;
        private bool _disposed;

        // Session-level configuration injected into each new StatementTelemetryContext
        private string? _sessionId;
        private long _workspaceId;
        private DriverSystemConfiguration? _systemConfiguration;
        private DriverConnectionParameters? _connectionParams;

        /// <summary>
        /// Initializes a new instance of the <see cref="MetricsAggregator"/> class.
        /// </summary>
        /// <param name="telemetryClient">The shared per-host telemetry client for enqueuing logs.</param>
        /// <param name="config">The telemetry configuration.</param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when <paramref name="telemetryClient"/> or <paramref name="config"/> is null.
        /// </exception>
        public MetricsAggregator(ITelemetryClient telemetryClient, TelemetryConfiguration config)
        {
            _telemetryClient = telemetryClient ?? throw new ArgumentNullException(nameof(telemetryClient));
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _contexts = new ConcurrentDictionary<string, StatementTelemetryContext>(StringComparer.Ordinal);
        }

        /// <summary>
        /// Gets the number of pending statement contexts that have not yet been emitted.
        /// </summary>
        internal int PendingContextCount => _contexts.Count;

        /// <summary>
        /// Sets session-level context that will be injected into each new
        /// <see cref="StatementTelemetryContext"/> created by this aggregator.
        /// </summary>
        /// <param name="sessionId">The connection session ID.</param>
        /// <param name="workspaceId">The Databricks workspace ID.</param>
        /// <param name="systemConfig">The driver system configuration.</param>
        /// <param name="connectionParams">The driver connection parameters.</param>
        public void SetSessionContext(
            string sessionId,
            long workspaceId,
            DriverSystemConfiguration? systemConfig,
            DriverConnectionParameters? connectionParams)
        {
            _sessionId = sessionId;
            _workspaceId = workspaceId;
            _systemConfiguration = systemConfig;
            _connectionParams = connectionParams;
        }

        /// <summary>
        /// Processes a completed activity by extracting the statement.id tag,
        /// merging data into the corresponding <see cref="StatementTelemetryContext"/>,
        /// and emitting the proto message if the activity is a completed root activity.
        /// </summary>
        /// <param name="activity">The completed activity to process.</param>
        /// <remarks>
        /// Activities without a statement.id tag are silently ignored.
        /// All exceptions are swallowed with Debug.WriteLine at TRACE level.
        /// </remarks>
        public void ProcessActivity(Activity activity)
        {
            try
            {
                if (activity == null || _disposed)
                {
                    return;
                }

                // Extract statement.id tag - ignore activities without it
                string? statementId = activity.GetTagItem("statement.id") as string;
                if (string.IsNullOrEmpty(statementId))
                {
                    return;
                }

                // Get or create the context for this statement
                StatementTelemetryContext context = _contexts.GetOrAdd(statementId!, CreateNewContext);

                // Merge activity data into the context
                context.MergeFrom(activity);

                // Check if this is a completed root activity - if so, emit the proto
                if (IsRootActivity(activity) && IsActivityComplete(activity))
                {
                    EmitAndRemoveContext(statementId!);
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] MetricsAggregator.ProcessActivity error: {ex.Message}");
            }
        }

        /// <summary>
        /// Flushes all remaining pending contexts by building and enqueuing their
        /// proto messages. Called on connection close to ensure no data is lost.
        /// </summary>
        /// <returns>A task that completes when all pending contexts have been emitted.</returns>
        public Task FlushAsync()
        {
            try
            {
                // Snapshot the keys to avoid modification during iteration
                List<string> keys = new List<string>(_contexts.Keys);

                foreach (string statementId in keys)
                {
                    try
                    {
                        EmitAndRemoveContext(statementId);
                    }
                    catch (Exception ex)
                    {
                        Debug.WriteLine($"[TRACE] MetricsAggregator.FlushAsync error for statement {statementId}: {ex.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] MetricsAggregator.FlushAsync error: {ex.Message}");
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// Determines whether the given activity is a root activity.
        /// A root activity has no parent or its parent's ActivitySource name
        /// is not the Databricks driver source.
        /// </summary>
        /// <param name="activity">The activity to check.</param>
        /// <returns>True if the activity is a root activity; otherwise, false.</returns>
        internal static bool IsRootActivity(Activity activity)
        {
            if (activity.Parent == null)
            {
                return true;
            }

            return activity.Parent.Source.Name != DatabricksActivitySourceName;
        }

        /// <summary>
        /// Determines whether the given activity has completed with a terminal status.
        /// An activity is complete if its status is <see cref="ActivityStatusCode.Ok"/>
        /// or <see cref="ActivityStatusCode.Error"/>.
        /// </summary>
        /// <param name="activity">The activity to check.</param>
        /// <returns>True if the activity has a terminal status; otherwise, false.</returns>
        internal static bool IsActivityComplete(Activity activity)
        {
            return activity.Status == ActivityStatusCode.Ok
                || activity.Status == ActivityStatusCode.Error;
        }

        /// <summary>
        /// Disposes the aggregator and clears all pending contexts.
        /// </summary>
        public void Dispose()
        {
            if (!_disposed)
            {
                _disposed = true;
                _contexts.Clear();
            }
        }

        /// <summary>
        /// Creates a new <see cref="StatementTelemetryContext"/> with session-level data.
        /// </summary>
        /// <param name="statementId">The statement ID (used as key, also set on context).</param>
        /// <returns>A new context pre-populated with session-level configuration.</returns>
        private StatementTelemetryContext CreateNewContext(string statementId)
        {
            StatementTelemetryContext context = new StatementTelemetryContext
            {
                StatementId = statementId,
                SessionId = _sessionId,
                SystemConfiguration = _systemConfiguration,
                DriverConnectionParams = _connectionParams
            };

            return context;
        }

        /// <summary>
        /// Builds a <see cref="TelemetryFrontendLog"/> from the context,
        /// enqueues it to the telemetry client, and removes the context from the dictionary.
        /// </summary>
        /// <param name="statementId">The statement ID to emit and remove.</param>
        private void EmitAndRemoveContext(string statementId)
        {
            if (_contexts.TryRemove(statementId, out StatementTelemetryContext? context))
            {
                OssSqlDriverTelemetryLog protoLog = context.BuildTelemetryLog();

                TelemetryFrontendLog frontendLog = new TelemetryFrontendLog
                {
                    WorkspaceId = _workspaceId,
                    FrontendLogEventId = Guid.NewGuid().ToString(),
                    Context = new FrontendLogContext
                    {
                        ClientContext = new TelemetryClientContext
                        {
                            UserAgent = BuildUserAgent()
                        },
                        TimestampMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    },
                    Entry = new FrontendLogEntry
                    {
                        SqlDriverLog = protoLog
                    }
                };

                _telemetryClient.Enqueue(frontendLog);
            }
        }

        /// <summary>
        /// Builds a user agent string from the system configuration.
        /// </summary>
        /// <returns>A user agent string, or null if no system configuration is available.</returns>
        private string? BuildUserAgent()
        {
            if (_systemConfiguration == null)
            {
                return null;
            }

            string driverName = !string.IsNullOrEmpty(_systemConfiguration.DriverName)
                ? _systemConfiguration.DriverName
                : "AdbcDatabricksDriver";

            string driverVersion = !string.IsNullOrEmpty(_systemConfiguration.DriverVersion)
                ? _systemConfiguration.DriverVersion
                : "unknown";

            return $"{driverName}/{driverVersion}";
        }
    }
}
