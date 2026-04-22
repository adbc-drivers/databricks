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
        /// Flushes pending events and releases the telemetry client.
        /// Safe to call multiple times.
        /// </summary>
        Task DisposeAsync();
    }
}
