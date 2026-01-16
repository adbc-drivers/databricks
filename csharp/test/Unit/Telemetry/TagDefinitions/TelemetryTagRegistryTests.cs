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
using System.Linq;
using AdbcDrivers.Databricks.Telemetry.TagDefinitions;
using Xunit;

namespace AdbcDrivers.Databricks.Tests
{
    public class TelemetryTagRegistryTests
    {
        [Fact]
        public void TelemetryTagRegistry_GetDatabricksExportTags_ConnectionOpen_ReturnsCorrectTags()
        {
            // Act
            var tags = TelemetryTagRegistry.GetDatabricksExportTags(TelemetryEventType.ConnectionOpen);

            // Assert
            Assert.NotNull(tags);
            Assert.Contains("workspace.id", tags);
            Assert.Contains("session.id", tags);
            Assert.Contains("driver.version", tags);
            Assert.Contains("driver.os", tags);
            Assert.Contains("driver.runtime", tags);
            Assert.Contains("feature.cloudfetch", tags);
            Assert.Contains("feature.lz4", tags);

            // Sensitive tags should NOT be included
            Assert.DoesNotContain("server.address", tags);
        }

        [Fact]
        public void TelemetryTagRegistry_GetDatabricksExportTags_StatementExecution_ReturnsCorrectTags()
        {
            // Act
            var tags = TelemetryTagRegistry.GetDatabricksExportTags(TelemetryEventType.StatementExecution);

            // Assert
            Assert.NotNull(tags);
            Assert.Contains("statement.id", tags);
            Assert.Contains("session.id", tags);
            Assert.Contains("result.format", tags);
            Assert.Contains("result.chunk_count", tags);
            Assert.Contains("result.bytes_downloaded", tags);
            Assert.Contains("result.compression_enabled", tags);
            Assert.Contains("poll.count", tags);
            Assert.Contains("poll.latency_ms", tags);

            // Sensitive tags should NOT be included
            Assert.DoesNotContain("db.statement", tags);
        }

        [Fact]
        public void TelemetryTagRegistry_GetDatabricksExportTags_Error_ReturnsCorrectTags()
        {
            // Act
            var tags = TelemetryTagRegistry.GetDatabricksExportTags(TelemetryEventType.Error);

            // Assert
            Assert.NotNull(tags);
            Assert.Contains("error.type", tags);
            Assert.Contains("session.id", tags);
            Assert.Contains("statement.id", tags);
            Assert.Contains("error.code", tags);
            Assert.Contains("error.category", tags);
            Assert.Contains("http.status_code", tags);

            // Sensitive tags should NOT be included
            Assert.DoesNotContain("error.message", tags);
            Assert.DoesNotContain("error.stack_trace", tags);
        }

        [Fact]
        public void TelemetryTagRegistry_ShouldExportToDatabricks_SensitiveTag_ReturnsFalse()
        {
            // Test sensitive tags from different event types
            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.StatementExecution, "db.statement"));

            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.ConnectionOpen, "server.address"));

            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.Error, "error.message"));

            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.Error, "error.stack_trace"));
        }

        [Fact]
        public void TelemetryTagRegistry_ShouldExportToDatabricks_SafeTag_ReturnsTrue()
        {
            // Test safe tags from different event types
            Assert.True(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.StatementExecution, "statement.id"));

            Assert.True(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.StatementExecution, "result.format"));

            Assert.True(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.ConnectionOpen, "workspace.id"));

            Assert.True(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.Error, "error.type"));
        }

        [Fact]
        public void TelemetryTagRegistry_ShouldExportToDatabricks_UnknownTag_ReturnsFalse()
        {
            // Unknown tags should not be exported
            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.StatementExecution, "unknown.tag"));

            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.ConnectionOpen, "not.defined"));
        }

        [Fact]
        public void ConnectionOpenEvent_GetDatabricksExportTags_ExcludesServerAddress()
        {
            // Act
            var tags = ConnectionOpenEvent.GetDatabricksExportTags();

            // Assert
            Assert.NotNull(tags);
            Assert.DoesNotContain("server.address", tags);
            Assert.Contains("workspace.id", tags);
            Assert.Contains("session.id", tags);
        }

        [Fact]
        public void StatementExecutionEvent_GetDatabricksExportTags_ExcludesDbStatement()
        {
            // Act
            var tags = StatementExecutionEvent.GetDatabricksExportTags();

            // Assert
            Assert.NotNull(tags);
            Assert.DoesNotContain("db.statement", tags);
            Assert.Contains("statement.id", tags);
            Assert.Contains("result.format", tags);
        }

        [Fact]
        public void ErrorEvent_GetDatabricksExportTags_ExcludesSensitiveErrorInfo()
        {
            // Act
            var tags = ErrorEvent.GetDatabricksExportTags();

            // Assert
            Assert.NotNull(tags);
            Assert.DoesNotContain("error.message", tags);
            Assert.DoesNotContain("error.stack_trace", tags);
            Assert.Contains("error.type", tags);
            Assert.Contains("error.code", tags);
        }

        [Fact]
        public void ConnectionOpenEvent_GetDatabricksExportTags_ContainsAllRequiredTags()
        {
            // Act
            var tags = ConnectionOpenEvent.GetDatabricksExportTags();

            // Assert - verify all expected tags are present
            var expectedTags = new[]
            {
                "workspace.id",
                "session.id",
                "driver.version",
                "driver.os",
                "driver.runtime",
                "feature.cloudfetch",
                "feature.lz4"
            };

            foreach (var expectedTag in expectedTags)
            {
                Assert.Contains(expectedTag, tags);
            }

            Assert.Equal(expectedTags.Length, tags.Count);
        }

        [Fact]
        public void StatementExecutionEvent_GetDatabricksExportTags_ContainsAllRequiredTags()
        {
            // Act
            var tags = StatementExecutionEvent.GetDatabricksExportTags();

            // Assert - verify all expected tags are present
            var expectedTags = new[]
            {
                "statement.id",
                "session.id",
                "result.format",
                "result.chunk_count",
                "result.bytes_downloaded",
                "result.compression_enabled",
                "poll.count",
                "poll.latency_ms"
            };

            foreach (var expectedTag in expectedTags)
            {
                Assert.Contains(expectedTag, tags);
            }

            Assert.Equal(expectedTags.Length, tags.Count);
        }

        [Fact]
        public void ErrorEvent_GetDatabricksExportTags_ContainsAllRequiredTags()
        {
            // Act
            var tags = ErrorEvent.GetDatabricksExportTags();

            // Assert - verify all expected tags are present
            var expectedTags = new[]
            {
                "error.type",
                "session.id",
                "statement.id",
                "error.code",
                "error.category",
                "http.status_code"
            };

            foreach (var expectedTag in expectedTags)
            {
                Assert.Contains(expectedTag, tags);
            }

            Assert.Equal(expectedTags.Length, tags.Count);
        }

        [Fact]
        public void TelemetryTagRegistry_GetDatabricksExportTags_ReturnsReadOnlyCollection()
        {
            // Act
            var tags = TelemetryTagRegistry.GetDatabricksExportTags(TelemetryEventType.ConnectionOpen);

            // Assert - verify the returned collection is read-only
            Assert.IsAssignableFrom<IReadOnlyCollection<string>>(tags);
        }

        [Fact]
        public void TelemetryTagRegistry_ShouldExportToDatabricks_CaseSensitive()
        {
            // Tags should be case-sensitive
            Assert.True(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.StatementExecution, "statement.id"));

            // Different case should not match
            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.StatementExecution, "Statement.Id"));

            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.StatementExecution, "STATEMENT.ID"));
        }
    }
}
