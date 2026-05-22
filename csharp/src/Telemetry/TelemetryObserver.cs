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
using System.Threading;
using AdbcDrivers.Databricks.Reader.CloudFetch;
using AdbcDrivers.Databricks.Telemetry.Models;
using ExecutionResultFormat = AdbcDrivers.Databricks.Telemetry.Proto.ExecutionResult.Types.Format;
using OperationType = AdbcDrivers.Databricks.Telemetry.Proto.Operation.Types.Type;
using StatementType = AdbcDrivers.Databricks.Telemetry.Proto.Statement.Types.Type;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Default <see cref="IStatementOperationObserver"/> implementation that translates
    /// observer method calls into mutations on a <see cref="StatementTelemetryContext"/>
    /// and, on <see cref="OnFinalized"/>, builds a <see cref="Proto.OssSqlDriverTelemetryLog"/>
    /// and enqueues it on the session's telemetry client for background export.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Fail-open:</b> Every public method routes through the private <c>Safe(Action)</c>
    /// helper, which swallows any exception and emits an OpenTelemetry activity event. This
    /// concentrates the fail-open contract in exactly one place rather than scattering
    /// try/catch boilerplate across each method body.
    /// </para>
    /// <para>
    /// <b>Thread-safe:</b> The scalar writes into the per-statement context are inherently
    /// safe for the lifecycle calls (each is called at most a small number of times from
    /// the statement's execution path or the reader's disposal thread). The terminal
    /// <see cref="OnFinalized"/> uses <see cref="Interlocked.CompareExchange(ref int, int, int)"/>
    /// on an <c>_emitted</c> flag to guarantee exactly-once enqueue even if it is invoked
    /// concurrently from multiple threads (e.g. error path + dispose path).
    /// </para>
    /// <para>
    /// <b>Non-blocking:</b> <see cref="OnFinalized"/> only enqueues the log into the
    /// telemetry client's internal queue. The actual HTTP export runs on the client's
    /// background flush timer and never blocks the calling thread.
    /// </para>
    /// </remarks>
    internal sealed class TelemetryObserver : IStatementOperationObserver
    {
        // 0 = not yet emitted, 1 = emitted. Mutated via Interlocked.CompareExchange so the
        // terminal OnFinalized() is exactly-once even under concurrent invocation.
        private int _emitted;

        private readonly TelemetrySessionContext _session;
        private readonly StatementTelemetryContext _ctx;

        /// <summary>
        /// Initializes a new observer that records into a freshly created
        /// <see cref="StatementTelemetryContext"/> seeded from <paramref name="session"/>.
        /// </summary>
        /// <param name="session">Per-connection telemetry session context. Required.</param>
        public TelemetryObserver(TelemetrySessionContext session)
            : this(session, new StatementTelemetryContext(session ?? throw new ArgumentNullException(nameof(session))))
        {
        }

        // Test seam: allows unit tests to inject a pre-populated context. Internal so
        // it does not leak from the assembly.
        internal TelemetryObserver(TelemetrySessionContext session, StatementTelemetryContext context)
        {
            _session = session ?? throw new ArgumentNullException(nameof(session));
            _ctx = context ?? throw new ArgumentNullException(nameof(context));
        }

        /// <summary>
        /// Internal accessor for the underlying context, exposed for unit tests that need
        /// to assert per-field state without building a full telemetry log.
        /// </summary>
        internal StatementTelemetryContext Context => _ctx;

        /// <summary>
        /// Internal accessor for the idempotency flag, exposed for unit tests that need to
        /// confirm exactly-once semantics without reaching into the enqueue path.
        /// </summary>
        internal bool HasEmitted => Volatile.Read(ref _emitted) != 0;

        /// <inheritdoc />
        public void OnExecuteStarted(StatementType stmtType, OperationType opType, bool isCompressed) =>
            Safe(() =>
            {
                _ctx.StatementType = stmtType;
                _ctx.OperationType = opType;
                _ctx.IsCompressed = isCompressed;
            });

        /// <inheritdoc />
        public void OnExecuteSucceeded(string statementId, ExecutionResultFormat resultFormat) =>
            Safe(() =>
            {
                _ctx.StatementId = statementId;
                _ctx.ResultFormat = resultFormat;
                _ctx.RecordExecuteComplete();
            });

        /// <inheritdoc />
        public void OnPollCompleted(int count, long latencyMs) =>
            Safe(() =>
            {
                _ctx.PollCount = count;
                _ctx.PollLatencyMs = latencyMs;
            });

        /// <inheritdoc />
        public void OnFirstBatchReady(long latencyMs) =>
            Safe(() =>
            {
                // Only the first call wins: the underlying setter is null-guarded so
                // repeated calls (e.g. inline reader emits one, cloud-fetch reader emits
                // another) do not overwrite the earliest observed latency.
                if (_ctx.FirstBatchReadyMs == null)
                {
                    _ctx.FirstBatchReadyMs = latencyMs;
                }
            });

        /// <inheritdoc />
        public void OnConsumed(long latencyMs) =>
            Safe(() => _ctx.ResultsConsumedMs = latencyMs);

        /// <inheritdoc />
        public void OnChunksDownloaded(ChunkMetrics metrics) =>
            Safe(() =>
            {
                // Tolerate a null or empty ChunkMetrics — the gap-fix plumbing may not be
                // landed yet, and the proto fields are nullable on the wire.
                if (metrics == null)
                {
                    return;
                }
                _ctx.SetChunkDetails(
                    metrics.TotalChunksPresent,
                    metrics.TotalChunksIterated,
                    metrics.InitialChunkLatencyMs,
                    metrics.SlowestChunkLatencyMs,
                    metrics.SumChunksDownloadTimeMs);
            });

        /// <inheritdoc />
        public void OnReaderInspected(ExecutionResultFormat resultFormat, bool isCompressed) =>
            Safe(() =>
            {
                // Overwrite the placeholders stamped at OnExecuteStarted time with the
                // server-reported truth for the active reader (PECO-2988, PECO-2978).
                // Mutating the same fields directly preserves byte-identical output vs.
                // the legacy EmitTelemetry helper, which set both fields immediately
                // before BuildTelemetryLog.
                _ctx.ResultFormat = resultFormat;
                _ctx.IsCompressed = isCompressed;
            });

        /// <inheritdoc />
        public void OnError(Exception ex) =>
            Safe(() =>
            {
                if (ex == null)
                {
                    return;
                }
                _ctx.HasError = true;
                // GetType().Name is always safe; do not capture ex.Message here unless we
                // have explicit consent: the proto's DriverErrorInfo.error_message field is
                // pending LPP review (see ConnectionTelemetry.EmitOperationTelemetry).
                _ctx.ErrorName = ex.GetType().Name;
                _ctx.ErrorMessage = SafeMessage(ex);
            });

        /// <inheritdoc />
        public void OnFinalized()
        {
            // Idempotency gate: only the first caller proceeds. Doing this outside Safe()
            // ensures even a defective Safe() helper can't bypass the once-only semantics.
            if (Interlocked.CompareExchange(ref _emitted, 1, 0) != 0)
            {
                return;
            }

            Safe(() =>
            {
                ITelemetryClient? client = _session.TelemetryClient;
                if (client == null)
                {
                    // Telemetry is disabled at the session level. The idempotency flag has
                    // already been set so a subsequent OnFinalized() remains a no-op.
                    return;
                }

                Proto.OssSqlDriverTelemetryLog log = _ctx.BuildTelemetryLog();
                TelemetryFrontendLog frontendLog = new TelemetryFrontendLog
                {
                    WorkspaceId = _ctx.WorkspaceId,
                    FrontendLogEventId = Guid.NewGuid().ToString(),
                    Context = new FrontendLogContext
                    {
                        TimestampMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    },
                    Entry = new FrontendLogEntry
                    {
                        SqlDriverLog = log,
                    },
                };

                // Enqueue is non-blocking; the client buffers events and flushes on a
                // background timer. No HTTP I/O happens on the calling thread.
                client.Enqueue(frontendLog);
            });
        }

        /// <summary>
        /// Concentrates the fail-open try/catch in one place. Any exception is suppressed
        /// and surfaced as an OpenTelemetry activity event so it is still observable in
        /// traces without affecting the caller's control flow.
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
                            { "error.message", ex.Message }
                        }));
                }
                catch
                {
                    // Recording the suppression must not itself throw. Intentionally empty.
                }
            }
        }

        // Some exceptions throw from their .Message property (rare but observed in the
        // wild for user-defined types). Wrap it so OnError can never become a source of
        // observer failure.
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
