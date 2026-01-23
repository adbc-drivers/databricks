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
using System.Text.Json;
using AdbcDrivers.Databricks.Telemetry.Models;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry.Models
{
    /// <summary>
    /// Tests for TelemetryRequest serialization and structure.
    /// </summary>
    public class TelemetryRequestTests
    {
        [Fact]
        public void TelemetryRequest_Serialization_ProducesValidJson()
        {
            // Arrange
            var request = new TelemetryRequest
            {
                UploadTime = 1700000000000L,
                ProtoLogs = new List<string>
                {
                    "{\"workspace_id\":123,\"frontend_log_event_id\":\"test-id\"}",
                    "{\"workspace_id\":456,\"frontend_log_event_id\":\"test-id-2\"}"
                }
            };

            // Act
            var json = JsonSerializer.Serialize(request);
            var parsed = JsonDocument.Parse(json);
            var root = parsed.RootElement;

            // Assert
            Assert.True(root.TryGetProperty("uploadTime", out var uploadTime));
            Assert.Equal(1700000000000L, uploadTime.GetInt64());

            Assert.True(root.TryGetProperty("protoLogs", out var protoLogs));
            Assert.Equal(JsonValueKind.Array, protoLogs.ValueKind);
            Assert.Equal(2, protoLogs.GetArrayLength());
        }

        [Fact]
        public void TelemetryRequest_ProtoLogs_ContainsSerializedStrings()
        {
            // Arrange
            var frontendLog = new TelemetryFrontendLog
            {
                WorkspaceId = 12345,
                FrontendLogEventId = "event-123"
            };
            var serializedFrontendLog = JsonSerializer.Serialize(frontendLog);

            var request = new TelemetryRequest
            {
                UploadTime = 1700000000000L,
                ProtoLogs = new List<string> { serializedFrontendLog }
            };

            // Act
            var json = JsonSerializer.Serialize(request);
            var parsed = JsonDocument.Parse(json);
            var protoLogs = parsed.RootElement.GetProperty("protoLogs");

            // Assert - protoLogs should contain strings (JSON-serialized TelemetryFrontendLog)
            Assert.Single(protoLogs.EnumerateArray());
            var firstLog = protoLogs[0].GetString();
            Assert.NotNull(firstLog);
            Assert.Contains("workspace_id", firstLog);
            Assert.Contains("12345", firstLog);
        }

        [Fact]
        public void TelemetryRequest_EmptyProtoLogs_SerializesCorrectly()
        {
            // Arrange
            var request = new TelemetryRequest
            {
                UploadTime = 1700000000000L,
                ProtoLogs = new List<string>()
            };

            // Act
            var json = JsonSerializer.Serialize(request);
            var parsed = JsonDocument.Parse(json);
            var root = parsed.RootElement;

            // Assert
            Assert.True(root.TryGetProperty("protoLogs", out var protoLogs));
            Assert.Equal(JsonValueKind.Array, protoLogs.ValueKind);
            Assert.Equal(0, protoLogs.GetArrayLength());
        }

        [Fact]
        public void TelemetryRequest_PropertyNames_AreCorrect()
        {
            // Arrange
            var request = new TelemetryRequest
            {
                UploadTime = 1700000000000L,
                ProtoLogs = new List<string> { "test" }
            };

            // Act
            var json = JsonSerializer.Serialize(request);

            // Assert - property names should match JDBC format (camelCase for these)
            Assert.Contains("\"uploadTime\":", json);
            Assert.Contains("\"protoLogs\":", json);
            Assert.DoesNotContain("\"UploadTime\":", json);
            Assert.DoesNotContain("\"ProtoLogs\":", json);
        }

        [Fact]
        public void TelemetryRequest_Deserialization_WorksCorrectly()
        {
            // Arrange
            var json = "{\"uploadTime\":1700000000000,\"protoLogs\":[\"log1\",\"log2\"]}";

            // Act
            var request = JsonSerializer.Deserialize<TelemetryRequest>(json);

            // Assert
            Assert.NotNull(request);
            Assert.Equal(1700000000000L, request.UploadTime);
            Assert.Equal(2, request.ProtoLogs.Count);
            Assert.Equal("log1", request.ProtoLogs[0]);
            Assert.Equal("log2", request.ProtoLogs[1]);
        }

        [Fact]
        public void TelemetryRequest_DefaultConstructor_InitializesEmptyProtoLogs()
        {
            // Act
            var request = new TelemetryRequest();

            // Assert
            Assert.NotNull(request.ProtoLogs);
            Assert.Empty(request.ProtoLogs);
            Assert.Equal(0L, request.UploadTime);
        }
    }
}
