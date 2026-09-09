/*
* Copyright (c) 2026 ADBC Drivers Contributors
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
using AdbcDrivers.Databricks.Telemetry.Models;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// System.Text.Json source-generated metadata for the telemetry request envelope so it is
    /// serialized without reflection (trim- and NativeAOT-safe). Options mirror the prior runtime
    /// <see cref="TelemetryJsonOptions"/>: camelCase property names and null properties omitted.
    ///
    /// <para>
    /// Only <see cref="TelemetryRequest"/> lives here. <c>TelemetryFrontendLog</c> nests a
    /// protobuf-generated type whose graph the source generator can't process (SYSLIB1031 name
    /// collisions among its nested <c>Type</c> enums), so it is serialized by hand in
    /// <see cref="TelemetryPayloadWriter"/> instead.
    /// </para>
    /// </summary>
    [JsonSourceGenerationOptions(
        PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
    [JsonSerializable(typeof(TelemetryRequest))]
    internal partial class TelemetryJsonContext : JsonSerializerContext
    {
    }
}
