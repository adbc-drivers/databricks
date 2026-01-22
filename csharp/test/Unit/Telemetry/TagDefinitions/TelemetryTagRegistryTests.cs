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

using System.Linq;
using AdbcDrivers.Databricks.Telemetry.TagDefinitions;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry.TagDefinitions
{
    /// <summary>
    /// Tests for TelemetryTagRegistry and related tag definition classes.
    /// </summary>
    public class TelemetryTagRegistryTests
    {
        #region TelemetryTagRegistry Tests

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
        }

        [Fact]
        public void TelemetryTagRegistry_GetDatabricksExportTags_Error_ReturnsCorrectTags()
        {
            // Act
            var tags = TelemetryTagRegistry.GetDatabricksExportTags(TelemetryEventType.Error);

            // Assert
            Assert.NotNull(tags);
            Assert.Contains("error.type", tags);
            Assert.Contains("error.message", tags);
            Assert.Contains("error.http_status", tags);
            Assert.Contains("statement.id", tags);
            Assert.Contains("session.id", tags);
        }

        [Fact]
        public void TelemetryTagRegistry_ShouldExportToDatabricks_SensitiveTag_ReturnsFalse()
        {
            // Assert - sensitive tags should not be exported to Databricks
            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.StatementExecution, "db.statement"));
            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.ConnectionOpen, "server.address"));
            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.Error, "error.stack_trace"));
        }

        [Fact]
        public void TelemetryTagRegistry_ShouldExportToDatabricks_SafeTag_ReturnsTrue()
        {
            // Assert - safe tags should be exported to Databricks
            Assert.True(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.StatementExecution, "statement.id"));
            Assert.True(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.ConnectionOpen, "workspace.id"));
            Assert.True(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.Error, "error.type"));
        }

        [Fact]
        public void TelemetryTagRegistry_ShouldExportToDatabricks_UnknownTag_ReturnsFalse()
        {
            // Assert - unknown tags should not be exported
            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.ConnectionOpen, "unknown.tag"));
            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.StatementExecution, "some.other.tag"));
        }

        [Fact]
        public void TelemetryTagRegistry_ShouldExportToDatabricks_NullOrEmptyTag_ReturnsFalse()
        {
            // Assert
            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.ConnectionOpen, null!));
            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.ConnectionOpen, ""));
        }

        [Fact]
        public void TelemetryTagRegistry_GetAllEventTypes_ReturnsAllTypes()
        {
            // Act
            var eventTypes = TelemetryTagRegistry.GetAllEventTypes().ToList();

            // Assert
            Assert.Equal(3, eventTypes.Count);
            Assert.Contains(TelemetryEventType.ConnectionOpen, eventTypes);
            Assert.Contains(TelemetryEventType.StatementExecution, eventTypes);
            Assert.Contains(TelemetryEventType.Error, eventTypes);
        }

        #endregion

        #region ConnectionOpenEvent Tests

        [Fact]
        public void ConnectionOpenEvent_GetDatabricksExportTags_ExcludesServerAddress()
        {
            // Act
            var tags = ConnectionOpenEvent.GetDatabricksExportTags();

            // Assert
            Assert.DoesNotContain("server.address", tags);
        }

        [Fact]
        public void ConnectionOpenEvent_EventName_IsCorrect()
        {
            // Assert
            Assert.Equal("Connection.Open", ConnectionOpenEvent.EventName);
        }

        [Fact]
        public void ConnectionOpenEvent_TagConstants_HaveCorrectValues()
        {
            // Assert
            Assert.Equal("workspace.id", ConnectionOpenEvent.WorkspaceId);
            Assert.Equal("session.id", ConnectionOpenEvent.SessionId);
            Assert.Equal("driver.version", ConnectionOpenEvent.DriverVersion);
            Assert.Equal("driver.os", ConnectionOpenEvent.DriverOS);
            Assert.Equal("driver.runtime", ConnectionOpenEvent.DriverRuntime);
            Assert.Equal("feature.cloudfetch", ConnectionOpenEvent.FeatureCloudFetch);
            Assert.Equal("feature.lz4", ConnectionOpenEvent.FeatureLz4);
            Assert.Equal("server.address", ConnectionOpenEvent.ServerAddress);
        }

        [Fact]
        public void ConnectionOpenEvent_GetDatabricksExportTags_ReturnsExpectedCount()
        {
            // Act
            var tags = ConnectionOpenEvent.GetDatabricksExportTags();

            // Assert - 7 tags should be exported (excludes server.address)
            Assert.Equal(7, tags.Count);
        }

        #endregion

        #region StatementExecutionEvent Tests

        [Fact]
        public void StatementExecutionEvent_GetDatabricksExportTags_ExcludesDbStatement()
        {
            // Act
            var tags = StatementExecutionEvent.GetDatabricksExportTags();

            // Assert
            Assert.DoesNotContain("db.statement", tags);
        }

        [Fact]
        public void StatementExecutionEvent_EventName_IsCorrect()
        {
            // Assert
            Assert.Equal("Statement.Execute", StatementExecutionEvent.EventName);
        }

        [Fact]
        public void StatementExecutionEvent_TagConstants_HaveCorrectValues()
        {
            // Assert
            Assert.Equal("statement.id", StatementExecutionEvent.StatementId);
            Assert.Equal("session.id", StatementExecutionEvent.SessionId);
            Assert.Equal("result.format", StatementExecutionEvent.ResultFormat);
            Assert.Equal("result.chunk_count", StatementExecutionEvent.ResultChunkCount);
            Assert.Equal("result.bytes_downloaded", StatementExecutionEvent.ResultBytesDownloaded);
            Assert.Equal("result.compression_enabled", StatementExecutionEvent.ResultCompressionEnabled);
            Assert.Equal("poll.count", StatementExecutionEvent.PollCount);
            Assert.Equal("poll.latency_ms", StatementExecutionEvent.PollLatencyMs);
            Assert.Equal("db.statement", StatementExecutionEvent.DbStatement);
        }

        [Fact]
        public void StatementExecutionEvent_GetDatabricksExportTags_ReturnsExpectedCount()
        {
            // Act
            var tags = StatementExecutionEvent.GetDatabricksExportTags();

            // Assert - 8 tags should be exported (excludes db.statement)
            Assert.Equal(8, tags.Count);
        }

        #endregion

        #region ErrorEvent Tests

        [Fact]
        public void ErrorEvent_GetDatabricksExportTags_ExcludesStackTrace()
        {
            // Act
            var tags = ErrorEvent.GetDatabricksExportTags();

            // Assert
            Assert.DoesNotContain("error.stack_trace", tags);
        }

        [Fact]
        public void ErrorEvent_EventName_IsCorrect()
        {
            // Assert
            Assert.Equal("Error", ErrorEvent.EventName);
        }

        [Fact]
        public void ErrorEvent_TagConstants_HaveCorrectValues()
        {
            // Assert
            Assert.Equal("error.type", ErrorEvent.ErrorType);
            Assert.Equal("error.message", ErrorEvent.ErrorMessage);
            Assert.Equal("error.http_status", ErrorEvent.ErrorHttpStatus);
            Assert.Equal("statement.id", ErrorEvent.StatementId);
            Assert.Equal("session.id", ErrorEvent.SessionId);
            Assert.Equal("error.stack_trace", ErrorEvent.ErrorStackTrace);
        }

        [Fact]
        public void ErrorEvent_GetDatabricksExportTags_ReturnsExpectedCount()
        {
            // Act
            var tags = ErrorEvent.GetDatabricksExportTags();

            // Assert - 5 tags should be exported (excludes error.stack_trace)
            Assert.Equal(5, tags.Count);
        }

        #endregion

        #region TelemetryTagAttribute Tests

        [Fact]
        public void TelemetryTagAttribute_Constructor_SetsTagName()
        {
            // Arrange & Act
            var attribute = new TelemetryTagAttribute("test.tag");

            // Assert
            Assert.Equal("test.tag", attribute.TagName);
        }

        [Fact]
        public void TelemetryTagAttribute_DefaultExportScope_IsExportAll()
        {
            // Arrange & Act
            var attribute = new TelemetryTagAttribute("test.tag");

            // Assert
            Assert.Equal(TagExportScope.ExportAll, attribute.ExportScope);
        }

        [Fact]
        public void TelemetryTagAttribute_DefaultRequired_IsFalse()
        {
            // Arrange & Act
            var attribute = new TelemetryTagAttribute("test.tag");

            // Assert
            Assert.False(attribute.Required);
        }

        [Fact]
        public void TelemetryTagAttribute_DefaultDescription_IsNull()
        {
            // Arrange & Act
            var attribute = new TelemetryTagAttribute("test.tag");

            // Assert
            Assert.Null(attribute.Description);
        }

        [Fact]
        public void TelemetryTagAttribute_Properties_CanBeSet()
        {
            // Arrange & Act
            var attribute = new TelemetryTagAttribute("test.tag")
            {
                ExportScope = TagExportScope.ExportLocal,
                Description = "Test description",
                Required = true
            };

            // Assert
            Assert.Equal("test.tag", attribute.TagName);
            Assert.Equal(TagExportScope.ExportLocal, attribute.ExportScope);
            Assert.Equal("Test description", attribute.Description);
            Assert.True(attribute.Required);
        }

        #endregion

        #region TagExportScope Tests

        [Fact]
        public void TagExportScope_None_HasZeroValue()
        {
            // Assert
            Assert.Equal(0, (int)TagExportScope.None);
        }

        [Fact]
        public void TagExportScope_ExportLocal_HasValue1()
        {
            // Assert
            Assert.Equal(1, (int)TagExportScope.ExportLocal);
        }

        [Fact]
        public void TagExportScope_ExportDatabricks_HasValue2()
        {
            // Assert
            Assert.Equal(2, (int)TagExportScope.ExportDatabricks);
        }

        [Fact]
        public void TagExportScope_ExportAll_IsCombinationOfLocalAndDatabricks()
        {
            // Assert
            Assert.Equal(
                TagExportScope.ExportLocal | TagExportScope.ExportDatabricks,
                TagExportScope.ExportAll);
            Assert.Equal(3, (int)TagExportScope.ExportAll);
        }

        [Fact]
        public void TagExportScope_FlagsWork_Correctly()
        {
            // Assert
            Assert.True((TagExportScope.ExportAll & TagExportScope.ExportLocal) == TagExportScope.ExportLocal);
            Assert.True((TagExportScope.ExportAll & TagExportScope.ExportDatabricks) == TagExportScope.ExportDatabricks);
            Assert.True((TagExportScope.ExportLocal & TagExportScope.ExportDatabricks) == TagExportScope.None);
        }

        #endregion

        #region TelemetryEventType Tests

        [Fact]
        public void TelemetryEventType_HasAllExpectedValues()
        {
            // Assert
            Assert.Equal(0, (int)TelemetryEventType.ConnectionOpen);
            Assert.Equal(1, (int)TelemetryEventType.StatementExecution);
            Assert.Equal(2, (int)TelemetryEventType.Error);
        }

        #endregion
    }
}
