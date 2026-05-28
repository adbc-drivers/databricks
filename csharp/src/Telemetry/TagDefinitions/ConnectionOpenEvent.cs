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

using System.Collections.Generic;

namespace AdbcDrivers.Databricks.Telemetry.TagDefinitions
{
    /// <summary>
    /// Tag definitions for Connection.Open events.
    /// </summary>
    public static class ConnectionOpenEvent
    {
        /// <summary>
        /// The event name for connection open operations.
        /// </summary>
        public const string EventName = "Connection.Open";

        /// <summary>
        /// Databricks workspace ID.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("workspace.id",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Databricks workspace ID",
            Required = true)]
        public const string WorkspaceId = "workspace.id";

        /// <summary>
        /// Connection session ID.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("session.id",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Connection session ID",
            Required = true)]
        public const string SessionId = "session.id";

        /// <summary>
        /// ADBC driver version.
        /// Exported to all destinations.
        /// </summary>
        [TelemetryTag("driver.version",
            ExportScope = TagExportScope.ExportAll,
            Description = "ADBC driver version")]
        public const string DriverVersion = "driver.version";

        /// <summary>
        /// Operating system information.
        /// Exported to all destinations.
        /// </summary>
        [TelemetryTag("driver.os",
            ExportScope = TagExportScope.ExportAll,
            Description = "Operating system")]
        public const string DriverOS = "driver.os";

        /// <summary>
        /// .NET runtime version.
        /// Exported to all destinations.
        /// </summary>
        [TelemetryTag("driver.runtime",
            ExportScope = TagExportScope.ExportAll,
            Description = ".NET runtime version")]
        public const string DriverRuntime = "driver.runtime";

        /// <summary>
        /// Whether CloudFetch is enabled.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("feature.cloudfetch",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "CloudFetch enabled")]
        public const string FeatureCloudFetch = "feature.cloudfetch";

        /// <summary>
        /// Whether LZ4 compression is enabled.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("feature.lz4",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "LZ4 compression enabled")]
        public const string FeatureLz4 = "feature.lz4";

        /// <summary>
        /// Workspace host address.
        /// Only exported to local diagnostics (contains potentially sensitive data).
        /// </summary>
        [TelemetryTag("server.address",
            ExportScope = TagExportScope.ExportLocal,
            Description = "Workspace host (local diagnostics only)")]
        public const string ServerAddress = "server.address";

        /// <summary>
        /// Thrift client protocol version as an integer (e.g. 7 for
        /// SPARK_CLI_SERVICE_PROTOCOL_V7). Companion to the existing string
        /// tag <c>connection.client_protocol</c>, added in #486 to give
        /// dashboards a uniform numeric representation that matches the
        /// existing int tag <c>connection.server_protocol_version</c>. The
        /// string tag is intentionally kept for backward compatibility.
        /// Exported to local diagnostics only — protocol version is uninteresting
        /// to backend telemetry but useful in local span dumps.
        /// </summary>
        [TelemetryTag("connection.client_protocol_version",
            ExportScope = TagExportScope.ExportLocal,
            Description = "Thrift client protocol version (int) — companion to connection.client_protocol (string); see #486")]
        public const string ClientProtocolVersion = "connection.client_protocol_version";

        /// <summary>
        /// Gets all tags that should be exported to Databricks telemetry service.
        /// </summary>
        /// <returns>A set of tag names for Databricks export.</returns>
        public static HashSet<string> GetDatabricksExportTags()
        {
            return new HashSet<string>
            {
                WorkspaceId,
                SessionId,
                DriverVersion,
                DriverOS,
                DriverRuntime,
                FeatureCloudFetch,
                FeatureLz4
            };
        }
    }
}
