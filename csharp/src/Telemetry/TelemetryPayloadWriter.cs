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

using System.IO;
using System.Text;
using System.Text.Json;
using AdbcDrivers.Databricks.Telemetry.Models;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// AOT- and trim-safe serializer for <see cref="TelemetryFrontendLog"/>.
    ///
    /// <para>
    /// The frontend log nests a protobuf-generated <c>OssSqlDriverTelemetryLog</c>, whose type
    /// graph contains many nested enums sharing the simple name <c>Type</c>. The System.Text.Json
    /// source generator walks that graph during type discovery and fails with SYSLIB1031 (name
    /// collision) even when the proto member carries a custom <c>[JsonConverter]</c>. So rather
    /// than route this type through a <see cref="JsonSerializerContext"/>, we write its small,
    /// stable envelope by hand with <see cref="Utf8JsonWriter"/> and delegate the proto leaf to
    /// <see cref="OssSqlDriverTelemetryLogJsonConverter.WriteValue"/> — the same formatter the
    /// reflection-based path uses.
    /// </para>
    ///
    /// <para>
    /// Output matches the reflection serializer configured by <see cref="TelemetryJsonOptions"/>:
    /// the default encoder, explicit snake_case property names, and null properties omitted.
    /// </para>
    /// </summary>
    internal static class TelemetryPayloadWriter
    {
        internal static string SerializeFrontendLog(TelemetryFrontendLog log)
        {
            using MemoryStream stream = new MemoryStream();
            using (Utf8JsonWriter writer = new Utf8JsonWriter(stream))
            {
                WriteFrontendLog(writer, log);
            }
            return Encoding.UTF8.GetString(stream.GetBuffer(), 0, (int)stream.Length);
        }

        private static void WriteFrontendLog(Utf8JsonWriter writer, TelemetryFrontendLog log)
        {
            writer.WriteStartObject();

            writer.WriteNumber("workspace_id", log.WorkspaceId);
            if (log.FrontendLogEventId != null)
            {
                writer.WriteString("frontend_log_event_id", log.FrontendLogEventId);
            }

            if (log.Context != null)
            {
                writer.WritePropertyName("context");
                WriteContext(writer, log.Context);
            }

            if (log.Entry != null)
            {
                writer.WritePropertyName("entry");
                WriteEntry(writer, log.Entry);
            }

            writer.WriteEndObject();
        }

        private static void WriteContext(Utf8JsonWriter writer, FrontendLogContext context)
        {
            writer.WriteStartObject();

            if (context.ClientContext != null)
            {
                writer.WritePropertyName("client_context");
                writer.WriteStartObject();
                if (context.ClientContext.UserAgent != null)
                {
                    writer.WriteString("user_agent", context.ClientContext.UserAgent);
                }
                writer.WriteEndObject();
            }

            writer.WriteNumber("timestamp_millis", context.TimestampMillis);

            writer.WriteEndObject();
        }

        private static void WriteEntry(Utf8JsonWriter writer, FrontendLogEntry entry)
        {
            writer.WriteStartObject();

            if (entry.SqlDriverLog != null)
            {
                writer.WritePropertyName("sql_driver_log");
                OssSqlDriverTelemetryLogJsonConverter.WriteValue(writer, entry.SqlDriverLog);
            }

            writer.WriteEndObject();
        }
    }
}
