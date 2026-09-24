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
    /// Singleton no-op implementation of <see cref="IStatementOperationObserver"/>.
    /// Used as the default observer so callsites in statement classes never need to
    /// null-check before calling observer methods.
    /// </summary>
    /// <remarks>
    /// All methods are intentionally empty. They satisfy the fail-open, thread-safe,
    /// and idempotent contract trivially.
    /// </remarks>
    internal sealed class NullObserver : IStatementOperationObserver
    {
        /// <summary>
        /// The singleton instance. Use this directly rather than constructing new instances.
        /// </summary>
        public static readonly NullObserver Instance = new NullObserver();

        private NullObserver()
        {
        }

        /// <inheritdoc />
        public void OnExecuteStarted(StatementType stmtType, OperationType opType, bool isCompressed)
        {
            // No-op.
        }

        /// <inheritdoc />
        public void OnExecuteSucceeded(string statementId, ExecutionResultFormat resultFormat)
        {
            // No-op.
        }

        /// <inheritdoc />
        public void OnPollCompleted(int count, long latencyMs)
        {
            // No-op.
        }

        /// <inheritdoc />
        public void OnFirstBatchReady(long latencyMs)
        {
            // No-op.
        }

        /// <inheritdoc />
        public void OnConsumed(long latencyMs)
        {
            // No-op.
        }

        /// <inheritdoc />
        public void OnChunksDownloaded(ChunkMetrics metrics)
        {
            // No-op.
        }

        /// <inheritdoc />
        public void OnError(Exception ex)
        {
            // No-op.
        }

        /// <inheritdoc />
        public void OnFinalized()
        {
            // No-op. Idempotent by construction.
        }
    }
}
