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

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text.Json;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using AdbcDrivers.Databricks.Telemetry.Proto;
using ExecutionResultFormat = AdbcDrivers.Databricks.Telemetry.Proto.ExecutionResult.Types.Format;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Guards the hand-written <see cref="TelemetryPayloadWriter"/> against drift from the
    /// reflection-based serializer.
    ///
    /// <para>
    /// <see cref="TelemetryPayloadWriter"/> exists because the telemetry frontend log nests a
    /// protobuf type the System.Text.Json source generator can't process. Because it emits the
    /// envelope field-by-field by hand, a new property added to one of the envelope models would
    /// be serialized by the reflection path (<see cref="TelemetryJsonOptions.Default"/>) but
    /// silently dropped by the manual writer — a quiet wire-format regression.
    /// </para>
    ///
    /// <para>
    /// Two checks close that gap: a round-trip test asserts the two paths produce identical JSON
    /// for a fully-populated log, and a reflection-based completeness check asserts the shared
    /// fixture actually populates every envelope property (so the round-trip stays meaningful when
    /// a property is added). Adding an envelope property therefore forces both a fixture update and
    /// a <see cref="TelemetryPayloadWriter"/> update, rather than shipping a missing field.
    /// </para>
    /// </summary>
    public class TelemetryPayloadWriterTests
    {
        /// <summary>
        /// The envelope models that <see cref="TelemetryPayloadWriter"/> serializes by hand. The
        /// proto leaf (<c>OssSqlDriverTelemetryLog</c>) is intentionally excluded — it is delegated
        /// to the shared <see cref="OssSqlDriverTelemetryLogJsonConverter"/> in both paths and is
        /// treated as opaque here.
        /// </summary>
        private static readonly HashSet<Type> EnvelopeTypes = new HashSet<Type>
        {
            typeof(TelemetryFrontendLog),
            typeof(FrontendLogContext),
            typeof(FrontendLogEntry),
            typeof(TelemetryClientContext),
        };

        [Fact]
        public void SerializeFrontendLog_MatchesReflectionSerializer()
        {
            TelemetryFrontendLog log = CreateFullyPopulatedLog();

            string reflectionJson = JsonSerializer.Serialize(log, TelemetryJsonOptions.Default);
            string manualJson = TelemetryPayloadWriter.SerializeFrontendLog(log);

            Assert.Equal(reflectionJson, manualJson);
        }

        [Fact]
        public void FrontendLog_AllEnvelopePropertiesAreSerialized()
        {
            // The round-trip test above only catches a dropped field if the fixture populates that
            // field (an unset property is omitted by both paths and stays equal). This asserts the
            // shared fixture leaves no envelope property at its default, so a newly-added property
            // forces a fixture update and the round-trip then catches a missing writer branch.
            AssertFullyPopulated(CreateFullyPopulatedLog());
        }

        /// <summary>
        /// Builds a <see cref="TelemetryFrontendLog"/> with every envelope property set to a
        /// non-default value. Shared by both tests so they cannot drift apart.
        /// </summary>
        private static TelemetryFrontendLog CreateFullyPopulatedLog()
        {
            return new TelemetryFrontendLog
            {
                WorkspaceId = 12345678901234L,
                FrontendLogEventId = "550e8400-e29b-41d4-a716-446655440000",
                Context = new FrontendLogContext
                {
                    TimestampMillis = 1700000000000L,
                    ClientContext = new TelemetryClientContext
                    {
                        UserAgent = "AdbcDatabricksDriver/1.0.0 (.NET 8.0; Windows 10)"
                    }
                },
                Entry = new FrontendLogEntry
                {
                    SqlDriverLog = new OssSqlDriverTelemetryLog
                    {
                        SessionId = "session-uuid-123",
                        SqlStatementId = "statement-uuid-456",
                        OperationLatencyMs = 150,
                        SystemConfiguration = new DriverSystemConfiguration
                        {
                            DriverName = DatabricksConnection.DatabricksDriverName,
                            DriverVersion = "1.0.0",
                            OsName = "Windows",
                            RuntimeName = ".NET",
                            RuntimeVersion = "8.0.0"
                        },
                        SqlOperation = new SqlExecutionEvent
                        {
                            ExecutionResult = ExecutionResultFormat.ExternalLinks,
                            IsCompressed = true
                        }
                    }
                }
            };
        }

        /// <summary>
        /// Asserts every public readable property of <paramref name="instance"/> is set to a
        /// non-default value, recursing into nested <see cref="EnvelopeTypes"/>. The proto leaf is
        /// only required to be non-null (its fields are serialized by a shared converter, not the
        /// hand-written writer).
        /// </summary>
        private static void AssertFullyPopulated(object instance)
        {
            Type type = instance.GetType();
            foreach (PropertyInfo prop in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (!prop.CanRead || prop.GetIndexParameters().Length > 0)
                    continue;

                object? value = prop.GetValue(instance);
                Assert.True(
                    value != null,
                    $"{type.Name}.{prop.Name} must be populated in the TelemetryPayloadWriter round-trip fixture. " +
                    "A new envelope property needs both a fixture value here and a matching branch in TelemetryPayloadWriter.");

                if (value is string s)
                {
                    Assert.False(
                        string.IsNullOrEmpty(s),
                        $"{type.Name}.{prop.Name} must be a non-empty string in the fixture.");
                }
                else if (prop.PropertyType.IsValueType)
                {
                    object defaultValue = Activator.CreateInstance(prop.PropertyType)!;
                    Assert.False(
                        value!.Equals(defaultValue),
                        $"{type.Name}.{prop.Name} must be set to a non-default value in the fixture.");
                }

                if (EnvelopeTypes.Contains(prop.PropertyType))
                {
                    AssertFullyPopulated(value!);
                }
            }
        }
    }
}
