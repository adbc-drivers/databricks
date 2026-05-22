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
using System.Diagnostics;
using AdbcDrivers.Databricks.Reader.CloudFetch;
using ExecutionResultFormat = AdbcDrivers.Databricks.Telemetry.Proto.ExecutionResult.Types.Format;
using OperationType = AdbcDrivers.Databricks.Telemetry.Proto.Operation.Types.Type;
using StatementType = AdbcDrivers.Databricks.Telemetry.Proto.Statement.Types.Type;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Belt-and-suspenders decorator over any <see cref="IStatementOperationObserver"/>.
    /// Wraps every interface method in a per-method try/catch so an inner observer that
    /// violates the <see cref="IStatementOperationObserver"/> fail-open contract cannot
    /// surface its exception to the statement callsite.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The first-party observers in this assembly (<see cref="NullObserver"/>,
    /// <see cref="TelemetryObserver"/>) already honor the fail-open contract internally.
    /// This decorator exists for the optional case where a third-party observer is
    /// plugged in — see design §12, "optional SafeObserver decorator is available for
    /// future third-party observer implementations that may not honor the contract."
    /// </para>
    /// <para>
    /// Swallowed exceptions are recorded as an OpenTelemetry activity event
    /// (<c>telemetry.observer.suppressed</c>) on the ambient <see cref="Activity"/>, if
    /// any. This is the codebase's convention for trace-level diagnostics: it remains
    /// visible in distributed traces and verbose tooling without polluting standard
    /// logs or affecting the caller's control flow.
    /// </para>
    /// <para>
    /// <b>Thread-safety:</b> Inherits whatever thread-safety the inner observer provides.
    /// The decorator itself stores only the inner reference and adds no mutable state.
    /// </para>
    /// </remarks>
    internal sealed class SafeObserver : IStatementOperationObserver
    {
        private readonly IStatementOperationObserver _inner;

        /// <summary>
        /// Wraps <paramref name="inner"/> with a per-method try/catch.
        /// </summary>
        /// <param name="inner">The observer to delegate to. Required.</param>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="inner"/> is null.</exception>
        public SafeObserver(IStatementOperationObserver inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        }

        /// <summary>
        /// Exposes the wrapped observer for tests and diagnostic introspection.
        /// </summary>
        internal IStatementOperationObserver Inner => _inner;

        /// <inheritdoc />
        public void OnExecuteStarted(StatementType stmtType, OperationType opType, bool isCompressed) =>
            Safe(() => _inner.OnExecuteStarted(stmtType, opType, isCompressed));

        /// <inheritdoc />
        public void OnExecuteSucceeded(string statementId, ExecutionResultFormat resultFormat) =>
            Safe(() => _inner.OnExecuteSucceeded(statementId, resultFormat));

        /// <inheritdoc />
        public void OnPollCompleted(int count, long latencyMs) =>
            Safe(() => _inner.OnPollCompleted(count, latencyMs));

        /// <inheritdoc />
        public void OnFirstBatchReady(long latencyMs) =>
            Safe(() => _inner.OnFirstBatchReady(latencyMs));

        /// <inheritdoc />
        public void OnConsumed(long latencyMs) =>
            Safe(() => _inner.OnConsumed(latencyMs));

        /// <inheritdoc />
        public void OnChunksDownloaded(ChunkMetrics metrics) =>
            Safe(() => _inner.OnChunksDownloaded(metrics));

        /// <inheritdoc />
        public void OnReaderInspected(ExecutionResultFormat resultFormat, bool isCompressed) =>
            Safe(() => _inner.OnReaderInspected(resultFormat, isCompressed));

        /// <inheritdoc />
        public void OnError(Exception ex) =>
            Safe(() => _inner.OnError(ex));

        /// <inheritdoc />
        public void OnFinalized() =>
            Safe(() => _inner.OnFinalized());

        /// <summary>
        /// Executes <paramref name="action"/> and swallows any exception, surfacing it as
        /// a trace-level <see cref="ActivityEvent"/> on the ambient <see cref="Activity"/>.
        /// </summary>
        private static void Safe(Action action)
        {
            try
            {
                action();
            }
            catch (Exception ex)
            {
                try
                {
                    Activity.Current?.AddEvent(new ActivityEvent("telemetry.observer.suppressed",
                        tags: new ActivityTagsCollection
                        {
                            { "error.type", ex.GetType().Name },
                            { "error.message", SafeMessage(ex) },
                            { "observer.suppressed.source", "SafeObserver" },
                        }));
                }
                catch
                {
                    // Recording the suppression must not itself throw. Intentionally empty.
                }
            }
        }

        // Some exceptions throw from their .Message property (rare but observed in the
        // wild for user-defined types). Wrap it so the trace event can never become a
        // source of observer failure.
        private static string? SafeMessage(Exception ex)
        {
            try
            {
                return ex.Message;
            }
            catch
            {
                return null;
            }
        }
    }
}
