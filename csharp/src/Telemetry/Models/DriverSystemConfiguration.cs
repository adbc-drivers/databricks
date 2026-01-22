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

using System.Text.Json.Serialization;

namespace AdbcDrivers.Databricks.Telemetry.Models
{
    /// <summary>
    /// System configuration information for the driver.
    /// This follows the JDBC driver format for compatibility with Databricks telemetry backend.
    /// </summary>
    public class DriverSystemConfiguration
    {
        /// <summary>
        /// Name of the driver.
        /// Example: "Databricks ADBC Driver"
        /// </summary>
        [JsonPropertyName("driver_name")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? DriverName { get; set; }

        /// <summary>
        /// Version of the driver.
        /// Example: "1.0.0"
        /// </summary>
        [JsonPropertyName("driver_version")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? DriverVersion { get; set; }

        /// <summary>
        /// Operating system name.
        /// Example: "Windows", "Linux", "macOS"
        /// </summary>
        [JsonPropertyName("os_name")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? OsName { get; set; }

        /// <summary>
        /// Operating system version.
        /// Example: "10.0.19041"
        /// </summary>
        [JsonPropertyName("os_version")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? OsVersion { get; set; }

        /// <summary>
        /// Operating system architecture.
        /// Example: "x64", "arm64"
        /// </summary>
        [JsonPropertyName("os_arch")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? OsArch { get; set; }

        /// <summary>
        /// Runtime name.
        /// Example: ".NET"
        /// </summary>
        [JsonPropertyName("runtime_name")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? RuntimeName { get; set; }

        /// <summary>
        /// Runtime version.
        /// Example: "8.0.0"
        /// </summary>
        [JsonPropertyName("runtime_version")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? RuntimeVersion { get; set; }

        /// <summary>
        /// Locale of the system.
        /// Example: "en-US"
        /// </summary>
        [JsonPropertyName("locale")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? Locale { get; set; }

        /// <summary>
        /// Timezone of the system.
        /// Example: "America/Los_Angeles"
        /// </summary>
        [JsonPropertyName("timezone")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? Timezone { get; set; }
    }
}
