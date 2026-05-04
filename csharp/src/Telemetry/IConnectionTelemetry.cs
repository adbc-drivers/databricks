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
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Abstracts connection-level telemetry so DatabricksConnection can delegate
    /// to either a real implementation or a no-op when telemetry is disabled.
    /// </summary>
    internal interface IConnectionTelemetry
    {
        /// <summary>
        /// The session context consumed by DatabricksStatement to create per-statement contexts.
        /// Returns null when telemetry is disabled.
        /// </summary>
        TelemetrySessionContext? Session { get; }

        /// <summary>
        /// Wraps a metadata operation (GetObjects, GetTableTypes) with telemetry instrumentation.
        /// </summary>
        T ExecuteWithMetadataTelemetry<T>(
            Proto.Operation.Types.Type operationType,
            Func<T> operation,
            Activity? activity);

        /// <summary>
        /// Emits a single telemetry log for a discrete operation that has no separate
        /// execution body to wrap (e.g. CREATE_SESSION, DELETE_SESSION, CANCEL_STATEMENT,
        /// CLOSE_STATEMENT). Used when the work is already done and we just need to
        /// record that it happened.
        /// </summary>
        /// <param name="operationType">The operation type to record.</param>
        /// <param name="statementType">The statement type. Use TYPE_UNSPECIFIED for
        /// session lifecycle events that aren't tied to a specific statement.</param>
        /// <param name="statementId">Optional server-assigned statement id, when applicable.</param>
        /// <param name="elapsedMs">Elapsed wall-clock time of the operation in milliseconds.</param>
        /// <param name="error">Exception thrown by the operation, or null on success.</param>
        void EmitOperationTelemetry(
            Proto.Operation.Types.Type operationType,
            Proto.Statement.Types.Type statementType,
            string? statementId,
            long elapsedMs,
            Exception? error);

        /// <summary>
        /// Flushes pending events and releases the telemetry client.
        /// Safe to call multiple times.
        /// </summary>
        Task DisposeAsync();
    }
}
