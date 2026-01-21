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
    /// Defines all Activity tags used when a connection is opened.
    /// </summary>
    internal static class ConnectionOpenEvent
    {
        /// <summary>
        /// The event name for connection open activities.
        /// </summary>
        public const string EventName = "Connection.Open";

        // ============================================================================
        // Standard identification tags
        // ============================================================================

        /// <summary>
        /// Databricks workspace ID. Required for Databricks export.
        /// </summary>
        [TelemetryTag("workspace.id",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Databricks workspace ID",
            Required = true)]
        public const string WorkspaceId = "workspace.id";

        /// <summary>
        /// Connection session ID. Required for correlation across events.
        /// </summary>
        [TelemetryTag("session.id",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Connection session ID",
            Required = true)]
        public const string SessionId = "session.id";

        // ============================================================================
        // Driver configuration tags
        // ============================================================================

        /// <summary>
        /// ADBC driver version string.
        /// </summary>
        [TelemetryTag("driver.version",
            ExportScope = TagExportScope.ExportAll,
            Description = "ADBC driver version")]
        public const string DriverVersion = "driver.version";

        /// <summary>
        /// Operating system name.
        /// </summary>
        [TelemetryTag("driver.os",
            ExportScope = TagExportScope.ExportAll,
            Description = "Operating system")]
        public const string DriverOS = "driver.os";

        /// <summary>
        /// .NET runtime version.
        /// </summary>
        [TelemetryTag("driver.runtime",
            ExportScope = TagExportScope.ExportAll,
            Description = ".NET runtime version")]
        public const string DriverRuntime = "driver.runtime";

        /// <summary>
        /// Driver name identifier.
        /// </summary>
        [TelemetryTag("driver.name",
            ExportScope = TagExportScope.ExportAll,
            Description = "Driver name")]
        public const string DriverName = "driver.name";

        /// <summary>
        /// System locale name.
        /// </summary>
        [TelemetryTag("driver.locale",
            ExportScope = TagExportScope.ExportAll,
            Description = "System locale name")]
        public const string DriverLocale = "driver.locale";

        // ============================================================================
        // Feature flag tags
        // ============================================================================

        /// <summary>
        /// Whether CloudFetch is enabled.
        /// </summary>
        [TelemetryTag("feature.cloudfetch",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "CloudFetch enabled")]
        public const string FeatureCloudFetch = "feature.cloudfetch";

        /// <summary>
        /// Whether LZ4 compression is enabled.
        /// </summary>
        [TelemetryTag("feature.lz4",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "LZ4 compression enabled")]
        public const string FeatureLz4 = "feature.lz4";

        /// <summary>
        /// Whether direct results are enabled.
        /// </summary>
        [TelemetryTag("feature.direct_results",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Direct results enabled")]
        public const string FeatureDirectResults = "feature.direct_results";

        // ============================================================================
        // Connection parameters tags
        // ============================================================================

        /// <summary>
        /// HTTP path for the connection.
        /// </summary>
        [TelemetryTag("connection.http_path",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "HTTP path for connection")]
        public const string ConnectionHttpPath = "connection.http_path";

        /// <summary>
        /// Connection mode (thrift or sea).
        /// </summary>
        [TelemetryTag("connection.mode",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Connection mode (thrift/sea)")]
        public const string ConnectionMode = "connection.mode";

        /// <summary>
        /// Authentication mechanism type.
        /// </summary>
        [TelemetryTag("connection.auth_mech",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Authentication mechanism")]
        public const string ConnectionAuthMech = "connection.auth_mech";

        // ============================================================================
        // Sensitive tags - NOT exported to Databricks
        // ============================================================================

        /// <summary>
        /// Workspace host address. Local diagnostics only.
        /// </summary>
        [TelemetryTag("server.address",
            ExportScope = TagExportScope.ExportLocal,
            Description = "Workspace host (local diagnostics only)")]
        public const string ServerAddress = "server.address";

        /// <summary>
        /// User identity. Local diagnostics only.
        /// </summary>
        [TelemetryTag("user.id",
            ExportScope = TagExportScope.ExportLocal,
            Description = "User identity (local diagnostics only)")]
        public const string UserId = "user.id";

        // Cached set of Databricks export tags for performance
        private static readonly HashSet<string> s_databricksExportTags = new HashSet<string>
        {
            WorkspaceId,
            SessionId,
            DriverVersion,
            DriverOS,
            DriverRuntime,
            DriverName,
            DriverLocale,
            FeatureCloudFetch,
            FeatureLz4,
            FeatureDirectResults,
            ConnectionHttpPath,
            ConnectionMode,
            ConnectionAuthMech
        };

        /// <summary>
        /// Get all tags that should be exported to Databricks for this event type.
        /// </summary>
        /// <returns>Set of tag names allowed for Databricks export.</returns>
        public static IReadOnlyCollection<string> GetDatabricksExportTags()
        {
            return s_databricksExportTags;
        }

        /// <summary>
        /// Check if a tag should be exported to Databricks.
        /// </summary>
        /// <param name="tagName">The tag name to check.</param>
        /// <returns>True if the tag should be exported to Databricks.</returns>
        public static bool ShouldExportToDatabricks(string tagName)
        {
            return s_databricksExportTags.Contains(tagName);
        }
    }
}
