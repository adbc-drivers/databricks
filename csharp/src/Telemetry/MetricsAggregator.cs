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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry.Models;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Per-connection aggregator that collects data from MULTIPLE activities
    /// and creates a single proto message per statement.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Each <see cref="DatabricksConnection"/> owns its own <see cref="MetricsAggregator"/> instance.
    /// The aggregator receives <see cref="ProcessActivity"/> callbacks for EVERY activity (parent and children)
    /// and merges them into <see cref="StatementTelemetryContext"/> instances keyed by statement_id.
    /// </para>
    /// <para>
    /// When a root activity completes (no parent or parent from a different ActivitySource),
    /// the aggregated proto is emitted via <see cref="ITelemetryClient.Enqueue"/>.
    /// </para>
    /// <para>
    /// Connection.Open activities are emitted immediately without statement_id aggregation.
    /// </para>
    /// <para>
    /// All exceptions are swallowed per the telemetry design principle that telemetry
    /// operations should never impact driver operations.
    /// </para>
    /// </remarks>
    internal sealed class MetricsAggregator : IDisposable
    {
        /// <summary>
        /// The ActivitySource name used by the main Databricks ADBC driver.
        /// Used to determine if an activity is a root activity.
        /// </summary>
        internal const string DatabricksActivitySourceName = "Databricks.Adbc.Driver";

        private readonly ITelemetryClient _telemetryClient;
        private readonly TelemetryConfiguration _config;
        private readonly ConcurrentDictionary<string, StatementTelemetryContext> _statements = new ConcurrentDictionary<string, StatementTelemetryContext>();

        // Connection-level context shared across all statements
        private readonly string? _sessionId;
        private readonly Proto.DriverSystemConfiguration? _systemConfig;
        private readonly Proto.DriverConnectionParameters? _connectionParams;
        private readonly long _workspaceId;

        private volatile bool _disposed;

        /// <summary>
        /// Creates a new <see cref="MetricsAggregator"/>.
        /// </summary>
        /// <param name="telemetryClient">The shared per-host telemetry client for enqueuing events.</param>
        /// <param name="config">The telemetry configuration.</param>
        /// <param name="sessionId">Optional connection session ID to include in all protos.</param>
        /// <param name="systemConfig">Optional pre-built system configuration from connection setup.</param>
        /// <param name="connectionParams">Optional pre-built connection parameters from connection setup.</param>
        /// <param name="workspaceId">The Databricks workspace ID for the connection.</param>
        /// <exception cref="ArgumentNullException">Thrown when telemetryClient or config is null.</exception>
        public MetricsAggregator(
            ITelemetryClient telemetryClient,
            TelemetryConfiguration config,
            string? sessionId = null,
            Proto.DriverSystemConfiguration? systemConfig = null,
            Proto.DriverConnectionParameters? connectionParams = null,
            long workspaceId = 0)
        {
            _telemetryClient = telemetryClient ?? throw new ArgumentNullException(nameof(telemetryClient));
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _sessionId = sessionId;
            _systemConfig = systemConfig;
            _connectionParams = connectionParams;
            _workspaceId = workspaceId;
        }

        /// <summary>
        /// Process an activity. Called for EVERY activity (parent and children).
        /// Extracts tags/events and merges into the statement context.
        /// If this is a root activity completing, emits the proto.
        /// Connection.Open activities are emitted immediately.
        /// </summary>
        /// <param name="activity">The activity to process.</param>
        public void ProcessActivity(Activity activity)
        {
            try
            {
                if (_disposed || activity == null) return;

                // Handle Connection.Open activities immediately (no statement_id aggregation)
                if (IsConnectionOpenActivity(activity))
                {
                    EmitConnectionOpen(activity);
                    return;
                }

                // Extract statement.id for routing
                var statementId = activity.GetTagItem("statement.id")?.ToString();
                if (string.IsNullOrEmpty(statementId)) return;

                // Get or create context for this statement
                var context = _statements.GetOrAdd(statementId,
                    _ => new StatementTelemetryContext(_sessionId, _systemConfig, _connectionParams));

                // Merge this activity's data into the context
                context.MergeFrom(activity);

                // If this is root activity completing, emit proto and clean up
                if (IsRootActivity(activity) && IsActivityComplete(activity))
                {
                    EmitStatementProto(statementId, context);
                    _statements.TryRemove(statementId, out _);
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] Telemetry aggregator error in ProcessActivity: {ex.Message}");
            }
        }

        /// <summary>
        /// Flush all pending statement contexts. Called on connection close
        /// to ensure no telemetry events are lost.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A task representing the asynchronous flush operation.</returns>
        public Task FlushAsync(CancellationToken ct = default)
        {
            try
            {
                if (_disposed) return Task.CompletedTask;

                // Snapshot and emit all pending statement contexts
                var keys = _statements.Keys;
                foreach (var key in keys)
                {
                    if (ct.IsCancellationRequested) break;

                    if (_statements.TryRemove(key, out var context))
                    {
                        try
                        {
                            EmitStatementProto(key, context);
                        }
                        catch (Exception ex)
                        {
                            Debug.WriteLine($"[TRACE] Telemetry aggregator error flushing statement {key}: {ex.Message}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] Telemetry aggregator error in FlushAsync: {ex.Message}");
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// Determines if an activity is a Connection.Open event.
        /// </summary>
        /// <param name="activity">The activity to check.</param>
        /// <returns>True if the activity represents a connection open operation.</returns>
        internal static bool IsConnectionOpenActivity(Activity activity)
        {
            var operationName = activity.OperationName;
            return operationName == "Connection.Open" ||
                   operationName == "Connection.OpenAsync";
        }

        /// <summary>
        /// Determines if an activity is a root activity.
        /// Root activities have no parent, or their parent is from a different ActivitySource.
        /// </summary>
        /// <param name="activity">The activity to check.</param>
        /// <returns>True if the activity is a root activity.</returns>
        internal static bool IsRootActivity(Activity activity)
        {
            return activity.Parent == null ||
                   activity.Parent.Source.Name != DatabricksActivitySourceName;
        }

        /// <summary>
        /// Determines if an activity has completed (success or error).
        /// Activities with Unset status are still in progress.
        /// </summary>
        /// <param name="activity">The activity to check.</param>
        /// <returns>True if the activity has completed.</returns>
        internal static bool IsActivityComplete(Activity activity)
        {
            return activity.Status == ActivityStatusCode.Ok ||
                   activity.Status == ActivityStatusCode.Error;
        }

        /// <summary>
        /// Emits a statement proto via the telemetry client.
        /// Builds the proto from the context and wraps it in a TelemetryFrontendLog.
        /// </summary>
        private void EmitStatementProto(string statementId, StatementTelemetryContext context)
        {
            var proto = context.BuildProto();
            var frontendLog = BuildFrontendLog(proto);
            _telemetryClient.Enqueue(frontendLog);
        }

        /// <summary>
        /// Emits a Connection.Open event immediately.
        /// Creates a minimal proto from the connection open activity tags.
        /// </summary>
        private void EmitConnectionOpen(Activity activity)
        {
            var context = new StatementTelemetryContext(_sessionId, _systemConfig, _connectionParams);
            context.MergeFrom(activity);
            var proto = context.BuildProto();
            var frontendLog = BuildFrontendLog(proto);
            _telemetryClient.Enqueue(frontendLog);
        }

        /// <summary>
        /// Wraps an OssSqlDriverTelemetryLog proto in a TelemetryFrontendLog envelope.
        /// </summary>
        private TelemetryFrontendLog BuildFrontendLog(Proto.OssSqlDriverTelemetryLog proto)
        {
            return new TelemetryFrontendLog
            {
                WorkspaceId = _workspaceId,
                FrontendLogEventId = Guid.NewGuid().ToString(),
                Context = new FrontendLogContext
                {
                    TimestampMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                },
                Entry = new FrontendLogEntry
                {
                    SqlDriverLog = proto
                }
            };
        }

        /// <summary>
        /// Disposes the aggregator and clears pending state.
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _statements.Clear();
        }
    }
}
