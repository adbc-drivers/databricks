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

        // --- System Configuration Tags (DriverSystemConfiguration) ---

        /// <summary>
        /// Driver name.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("driver.name",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Driver name")]
        public const string DriverName = "driver.name";

        /// <summary>
        /// .NET runtime name (e.g., ".NET", ".NET Framework", ".NET Core").
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("runtime.name",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Runtime name")]
        public const string RuntimeName = "runtime.name";

        /// <summary>
        /// .NET runtime version (e.g., "8.0.0").
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("runtime.version",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Runtime version")]
        public const string RuntimeVersion = "runtime.version";

        /// <summary>
        /// Runtime vendor (e.g., "Microsoft").
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("runtime.vendor",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Runtime vendor")]
        public const string RuntimeVendor = "runtime.vendor";

        /// <summary>
        /// Operating system name.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("os.name",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Operating system name")]
        public const string OsName = "os.name";

        /// <summary>
        /// Operating system version.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("os.version",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Operating system version")]
        public const string OsVersion = "os.version";

        /// <summary>
        /// OS architecture (e.g., "X64", "Arm64").
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("os.arch",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "OS architecture")]
        public const string OsArch = "os.arch";

        /// <summary>
        /// Client application name.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("client.app_name",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Client application name")]
        public const string ClientAppName = "client.app_name";

        /// <summary>
        /// Current locale name (e.g., "en-US").
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("locale.name",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Locale name")]
        public const string LocaleName = "locale.name";

        /// <summary>
        /// Character set encoding (e.g., "utf-8").
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("char_set_encoding",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Character set encoding")]
        public const string CharSetEncoding = "char_set_encoding";

        /// <summary>
        /// Current process name.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("process.name",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Process name")]
        public const string ProcessName = "process.name";

        // --- Connection Parameter Tags (DriverConnectionParameters) ---

        /// <summary>
        /// Authentication type (e.g., "pat", "oauth-m2m").
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("auth.type",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Authentication type")]
        public const string AuthType = "auth.type";

        /// <summary>
        /// HTTP path for the connection.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("connection.http_path",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "HTTP path")]
        public const string ConnectionHttpPath = "connection.http_path";

        /// <summary>
        /// Connection host.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("connection.host",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Connection host")]
        public const string ConnectionHost = "connection.host";

        /// <summary>
        /// Connection port.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("connection.port",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Connection port")]
        public const string ConnectionPort = "connection.port";

        /// <summary>
        /// Connection mode (e.g., "thrift", "sea").
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("connection.mode",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Connection mode")]
        public const string ConnectionMode = "connection.mode";

        /// <summary>
        /// Authentication mechanism (e.g., "pat", "oauth").
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("connection.auth_mech",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Authentication mechanism")]
        public const string ConnectionAuthMech = "connection.auth_mech";

        /// <summary>
        /// Authentication flow (e.g., "token_passthrough", "client_credentials").
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("connection.auth_flow",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Authentication flow")]
        public const string ConnectionAuthFlow = "connection.auth_flow";

        /// <summary>
        /// Whether Arrow format is enabled (always true for ADBC).
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("feature.arrow",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Arrow format enabled")]
        public const string FeatureArrow = "feature.arrow";

        /// <summary>
        /// Whether direct results are enabled.
        /// Exported to Databricks telemetry service.
        /// </summary>
        [TelemetryTag("feature.direct_results",
            ExportScope = TagExportScope.ExportDatabricks,
            Description = "Direct results enabled")]
        public const string FeatureDirectResults = "feature.direct_results";

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
                DriverName,
                RuntimeName,
                RuntimeVersion,
                RuntimeVendor,
                OsName,
                OsVersion,
                OsArch,
                ClientAppName,
                LocaleName,
                CharSetEncoding,
                ProcessName,
                AuthType,
                ConnectionHttpPath,
                ConnectionHost,
                ConnectionPort,
                ConnectionMode,
                ConnectionAuthMech,
                ConnectionAuthFlow,
                FeatureArrow,
                FeatureDirectResults,
                FeatureCloudFetch,
                FeatureLz4
            };
        }
    }
}
