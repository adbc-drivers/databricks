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
using System.Text.Json;
using System.Text.Json.Serialization;
using AdbcDrivers.Databricks.Telemetry.Proto;
using Google.Protobuf;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// JSON converter for protobuf-generated OssSqlDriverTelemetryLog messages.
    /// Uses Google.Protobuf's JsonFormatter.Default which produces camelCase field names
    /// as per proto3 JSON mapping specification.
    /// </summary>
    internal sealed class OssSqlDriverTelemetryLogJsonConverter : JsonConverter<OssSqlDriverTelemetryLog>
    {
        public override OssSqlDriverTelemetryLog? Read(
            ref Utf8JsonReader reader,
            Type typeToConvert,
            JsonSerializerOptions options)
        {
            // Read the raw JSON
            using var doc = JsonDocument.ParseValue(ref reader);
            var json = doc.RootElement.GetRawText();

            if (string.IsNullOrEmpty(json) || json == "null")
            {
                return null;
            }

            return JsonParser.Default.Parse<OssSqlDriverTelemetryLog>(json);
        }

        public override void Write(
            Utf8JsonWriter writer,
            OssSqlDriverTelemetryLog value,
            JsonSerializerOptions options)
        {
            if (value == null)
            {
                writer.WriteNullValue();
                return;
            }

            // Use protobuf's default JSON formatter
            var json = JsonFormatter.Default.Format(value);

            // Write raw JSON value
            using var doc = JsonDocument.Parse(json);
            doc.RootElement.WriteTo(writer);
        }
    }

    /// <summary>
    /// JSON serialization options configured for telemetry with proto support.
    /// </summary>
    internal static class TelemetryJsonOptions
    {
        /// <summary>
        /// JSON serializer options configured for telemetry serialization.
        /// Includes converters for protobuf types.
        /// </summary>
        public static readonly JsonSerializerOptions Default = CreateOptions();

        private static JsonSerializerOptions CreateOptions()
        {
            var options = new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
            };

            options.Converters.Add(new OssSqlDriverTelemetryLogJsonConverter());

            return options;
        }
    }
}
