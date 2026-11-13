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
using AdbcDrivers.Databricks.Reader.CloudFetch;
using ExecutionResultFormat = AdbcDrivers.Databricks.Telemetry.Proto.ExecutionResult.Types.Format;
using OperationType = AdbcDrivers.Databricks.Telemetry.Proto.Operation.Types.Type;
using StatementType = AdbcDrivers.Databricks.Telemetry.Proto.Statement.Types.Type;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Observer of a single statement's operational lifecycle. Sits between the statement
    /// classes (Thrift and SEA) and the telemetry implementation so the statement code
    /// does not depend on telemetry types directly.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Fail-open contract:</b> Implementations <b>MUST NOT throw</b> from any method on
    /// this interface. All exceptions raised inside an implementation must be swallowed
    /// internally — callsites in statement code intentionally contain no try/catch around
    /// observer calls. Telemetry must never affect the caller's control flow.
    /// </para>
    /// <para>
    /// <b>Thread-safety:</b> Methods on this interface may be invoked from any thread.
    /// Implementations <b>MUST be thread-safe</b>. In practice the calls happen from the
    /// statement's executing task and the reader's disposal thread, which may differ.
    /// </para>
    /// <para>
    /// <b>Terminal call:</b> <see cref="OnFinalized"/> is the terminal call. After it has
    /// been invoked once, the observer's record is considered complete and any further
    /// calls — including additional <see cref="OnFinalized"/> calls — <b>MUST be no-ops
    /// (idempotent)</b>. This protects against the common case where both an error path
    /// and a dispose path attempt to finalize the same statement.
    /// </para>
    /// <para>
    /// <b>Non-telemetry uses:</b> The interface is shaped around the statement's lifecycle
    /// rather than the telemetry data model so future observers (tracing, audit) can be
    /// added without changing statement code.
    /// </para>
    /// </remarks>
    internal interface IStatementOperationObserver
    {
        /// <summary>
        /// Called once just before the statement is submitted to the server.
        /// </summary>
        /// <param name="stmtType">The statement type (QUERY, UPDATE, METADATA, ...).</param>
        /// <param name="opType">The operation type (EXECUTE_STATEMENT, EXECUTE_STATEMENT_ASYNC, ...).</param>
        /// <param name="isCompressed">Whether results are expected to be compressed (LZ4).</param>
        void OnExecuteStarted(StatementType stmtType, OperationType opType, bool isCompressed);

        /// <summary>
        /// Called once after the server has accepted the statement and a statement id is known.
        /// </summary>
        /// <param name="statementId">The server-assigned statement id.</param>
        /// <param name="resultFormat">The result format inferred for the execution (INLINE_ARROW, EXTERNAL_LINKS, ...).</param>
        void OnExecuteSucceeded(string statementId, ExecutionResultFormat resultFormat);

        /// <summary>
        /// Called once after the polling loop reaches a terminal state, with the accumulated
        /// poll count and total elapsed poll latency.
        /// </summary>
        /// <param name="count">Total number of status-poll calls issued.</param>
        /// <param name="latencyMs">Sum of wall-clock time spent in poll calls, in milliseconds.</param>
        void OnPollCompleted(int count, long latencyMs);

        /// <summary>
        /// Called when the first batch of results is available to the reader.
        /// Implementations should treat repeated calls as a no-op (only the first wins).
        /// </summary>
        /// <param name="latencyMs">Elapsed time from execute start to first batch ready, in milliseconds.</param>
        void OnFirstBatchReady(long latencyMs);

        /// <summary>
        /// Called when the reader has been fully consumed (or disposed).
        /// </summary>
        /// <param name="latencyMs">Elapsed time from execute start to results fully consumed, in milliseconds.</param>
        void OnConsumed(long latencyMs);

        /// <summary>
        /// Called once when chunk metrics for the CloudFetch download are available.
        /// </summary>
        /// <param name="metrics">Aggregated CloudFetch chunk metrics. Implementations must
        /// tolerate empty / default metrics if the gap-fix plumbing has not landed yet.</param>
        void OnChunksDownloaded(ChunkMetrics metrics);

        /// <summary>
        /// Called once if the statement execution fails. Implementations should record the
        /// error and continue to accept further calls; an explicit <see cref="OnFinalized"/>
        /// call is still required to terminate the observer.
        /// </summary>
        /// <param name="ex">The exception that occurred.</param>
        void OnError(Exception ex);

        /// <summary>
        /// Terminal call. Implementations build and dispatch any pending record and mark
        /// the observer as finalized. <b>Must be idempotent</b>: repeated calls are no-ops.
        /// </summary>
        void OnFinalized();
    }
}
