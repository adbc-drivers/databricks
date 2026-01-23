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
using System.Text.Json.Serialization;

namespace AdbcDrivers.Databricks.Telemetry.Models
{
    /// <summary>
    /// Top-level wrapper for telemetry requests sent to Databricks telemetry service.
    /// This follows the JDBC driver format for compatibility with Databricks telemetry backend.
    /// </summary>
    /// <remarks>
    /// The request contains:
    /// - uploadTime: Unix timestamp in milliseconds when the data was uploaded
    /// - protoLogs: Array of JSON-serialized TelemetryFrontendLog objects
    /// </remarks>
    public class TelemetryRequest
    {
        /// <summary>
        /// Unix timestamp in milliseconds when the telemetry data was uploaded.
        /// </summary>
        [JsonPropertyName("uploadTime")]
        public long UploadTime { get; set; }

        /// <summary>
        /// Array of JSON-serialized TelemetryFrontendLog objects.
        /// Each element is a JSON string representation of a TelemetryFrontendLog.
        /// </summary>
        [JsonPropertyName("protoLogs")]
        public List<string> ProtoLogs { get; set; } = new List<string>();
    }
}
