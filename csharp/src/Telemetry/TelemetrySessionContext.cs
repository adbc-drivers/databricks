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

using AdbcDrivers.Databricks.Telemetry.Proto;
using ExecutionResultFormat = AdbcDrivers.Databricks.Telemetry.Proto.ExecutionResult.Types.Format;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Placeholder interface for telemetry client.
    /// Will be fully implemented in a future work item.
    /// </summary>
    internal interface ITelemetryClient
    {
    }

    /// <summary>
    /// Immutable per-connection session-level telemetry context.
    /// Created once at connection open and shared (read-only) with all statements on that connection.
    /// </summary>
    /// <remarks>
    /// This class holds frozen session-level data that does not change for the lifetime of a connection.
    /// All properties have private setters to ensure immutability after construction.
    /// Use the constructor or object initializer to set values.
    /// </remarks>
    internal sealed class TelemetrySessionContext
    {
        /// <summary>
        /// Gets the session ID from the server.
        /// </summary>
        public string? SessionId { get; set; }

        /// <summary>
        /// Gets the authentication type used for the connection (e.g., "PAT", "OAuth-M2M").
        /// </summary>
        public string? AuthType { get; set; }

        /// <summary>
        /// Gets the Databricks workspace ID.
        /// </summary>
        public long WorkspaceId { get; set; }

        /// <summary>
        /// Gets the driver system configuration (OS, runtime, driver version, etc.).
        /// </summary>
        public DriverSystemConfiguration? SystemConfiguration { get; set; }

        /// <summary>
        /// Gets the driver connection parameters (HTTP path, protocol mode, host details, etc.).
        /// </summary>
        public DriverConnectionParameters? DriverConnectionParams { get; set; }

        /// <summary>
        /// Gets the default result format configured for this connection.
        /// </summary>
        public ExecutionResultFormat DefaultResultFormat { get; set; }

        /// <summary>
        /// Gets whether compression is enabled by default for this connection.
        /// </summary>
        public bool DefaultCompressionEnabled { get; set; }

        /// <summary>
        /// Gets the telemetry client for exporting telemetry events.
        /// </summary>
        public ITelemetryClient? TelemetryClient { get; set; }
    }
}
