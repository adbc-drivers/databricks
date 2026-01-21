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

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry.TagDefinitions
{
    /// <summary>
    /// Tests for TelemetryTagRegistry class.
    /// </summary>
    public class TelemetryTagRegistryTests
    {
        // ============================================================================
        // GetDatabricksExportTags Tests - ConnectionOpen
        // ============================================================================

        [Fact]
        public void TelemetryTagRegistry_GetDatabricksExportTags_ConnectionOpen_ReturnsCorrectTags()
        {
            // Act
            var tags = TelemetryTagRegistry.GetDatabricksExportTags(TelemetryEventType.ConnectionOpen);

            // Assert
            Assert.NotNull(tags);
            Assert.NotEmpty(tags);

            // Should contain standard identification tags
            Assert.Contains(ConnectionOpenEvent.WorkspaceId, tags);
            Assert.Contains(ConnectionOpenEvent.SessionId, tags);

            // Should contain driver configuration tags
            Assert.Contains(ConnectionOpenEvent.DriverVersion, tags);
            Assert.Contains(ConnectionOpenEvent.DriverOS, tags);
            Assert.Contains(ConnectionOpenEvent.DriverRuntime, tags);

            // Should contain feature flag tags
            Assert.Contains(ConnectionOpenEvent.FeatureCloudFetch, tags);
            Assert.Contains(ConnectionOpenEvent.FeatureLz4, tags);
        }

        [Fact]
        public void ConnectionOpenEvent_GetDatabricksExportTags_ExcludesServerAddress()
        {
            // Act
            var tags = ConnectionOpenEvent.GetDatabricksExportTags();

            // Assert - server.address should NOT be exported to Databricks (sensitive)
            Assert.DoesNotContain(ConnectionOpenEvent.ServerAddress, tags);
        }

        [Fact]
        public void ConnectionOpenEvent_GetDatabricksExportTags_ExcludesUserId()
        {
            // Act
            var tags = ConnectionOpenEvent.GetDatabricksExportTags();

            // Assert - user.id should NOT be exported to Databricks (sensitive)
            Assert.DoesNotContain(ConnectionOpenEvent.UserId, tags);
        }

        // ============================================================================
        // GetDatabricksExportTags Tests - StatementExecution
        // ============================================================================

        [Fact]
        public void TelemetryTagRegistry_GetDatabricksExportTags_StatementExecution_ReturnsCorrectTags()
        {
            // Act
            var tags = TelemetryTagRegistry.GetDatabricksExportTags(TelemetryEventType.StatementExecution);

            // Assert
            Assert.NotNull(tags);
            Assert.NotEmpty(tags);

            // Should contain statement identification tags
            Assert.Contains(StatementExecutionEvent.StatementId, tags);
            Assert.Contains(StatementExecutionEvent.SessionId, tags);

            // Should contain result format tags
            Assert.Contains(StatementExecutionEvent.ResultFormat, tags);
            Assert.Contains(StatementExecutionEvent.ResultChunkCount, tags);
            Assert.Contains(StatementExecutionEvent.ResultBytesDownloaded, tags);

            // Should contain polling metrics tags
            Assert.Contains(StatementExecutionEvent.PollCount, tags);
            Assert.Contains(StatementExecutionEvent.PollLatencyMs, tags);
        }

        [Fact]
        public void StatementExecutionEvent_GetDatabricksExportTags_ExcludesDbStatement()
        {
            // Act
            var tags = StatementExecutionEvent.GetDatabricksExportTags();

            // Assert - db.statement should NOT be exported to Databricks (SQL query text is sensitive)
            Assert.DoesNotContain(StatementExecutionEvent.DbStatement, tags);
        }

        [Fact]
        public void StatementExecutionEvent_GetDatabricksExportTags_ExcludesDbParameters()
        {
            // Act
            var tags = StatementExecutionEvent.GetDatabricksExportTags();

            // Assert - db.parameters should NOT be exported to Databricks (sensitive)
            Assert.DoesNotContain(StatementExecutionEvent.DbParameters, tags);
        }

        // ============================================================================
        // GetDatabricksExportTags Tests - Error
        // ============================================================================

        [Fact]
        public void TelemetryTagRegistry_GetDatabricksExportTags_Error_ReturnsCorrectTags()
        {
            // Act
            var tags = TelemetryTagRegistry.GetDatabricksExportTags(TelemetryEventType.Error);

            // Assert
            Assert.NotNull(tags);
            Assert.NotEmpty(tags);

            // Should contain error identification tags
            Assert.Contains(ErrorEvent.ErrorType, tags);
            Assert.Contains(ErrorEvent.ErrorName, tags);
            Assert.Contains(ErrorEvent.ErrorCode, tags);

            // Should contain correlation tags
            Assert.Contains(ErrorEvent.SessionId, tags);
            Assert.Contains(ErrorEvent.StatementId, tags);
        }

        [Fact]
        public void ErrorEvent_GetDatabricksExportTags_ExcludesErrorMessage()
        {
            // Act
            var tags = ErrorEvent.GetDatabricksExportTags();

            // Assert - error.message should NOT be exported to Databricks (may contain sensitive data)
            Assert.DoesNotContain(ErrorEvent.ErrorMessage, tags);
        }

        [Fact]
        public void ErrorEvent_GetDatabricksExportTags_ExcludesStackTrace()
        {
            // Act
            var tags = ErrorEvent.GetDatabricksExportTags();

            // Assert - error.stack_trace should NOT be exported to Databricks (may contain sensitive paths)
            Assert.DoesNotContain(ErrorEvent.ErrorStackTrace, tags);
        }

        // ============================================================================
        // ShouldExportToDatabricks Tests
        // ============================================================================

        [Fact]
        public void TelemetryTagRegistry_ShouldExportToDatabricks_SensitiveTag_ReturnsFalse()
        {
            // Act & Assert - Statement execution sensitive tags
            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.StatementExecution, StatementExecutionEvent.DbStatement));
            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.StatementExecution, StatementExecutionEvent.DbParameters));

            // Connection open sensitive tags
            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.ConnectionOpen, ConnectionOpenEvent.ServerAddress));
            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.ConnectionOpen, ConnectionOpenEvent.UserId));

            // Error sensitive tags
            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.Error, ErrorEvent.ErrorMessage));
            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.Error, ErrorEvent.ErrorStackTrace));
        }

        [Fact]
        public void TelemetryTagRegistry_ShouldExportToDatabricks_SafeTag_ReturnsTrue()
        {
            // Act & Assert - Statement execution safe tags
            Assert.True(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.StatementExecution, StatementExecutionEvent.StatementId));
            Assert.True(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.StatementExecution, StatementExecutionEvent.ResultFormat));
            Assert.True(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.StatementExecution, StatementExecutionEvent.ResultChunkCount));

            // Connection open safe tags
            Assert.True(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.ConnectionOpen, ConnectionOpenEvent.WorkspaceId));
            Assert.True(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.ConnectionOpen, ConnectionOpenEvent.DriverVersion));

            // Error safe tags
            Assert.True(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.Error, ErrorEvent.ErrorType));
            Assert.True(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.Error, ErrorEvent.ErrorCode));
        }

        [Fact]
        public void TelemetryTagRegistry_ShouldExportToDatabricks_UnknownTag_ReturnsFalse()
        {
            // Act & Assert - Unknown tags should not be exported
            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.StatementExecution, "unknown.tag"));
            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.ConnectionOpen, "random.tag.name"));
        }

        [Fact]
        public void TelemetryTagRegistry_ShouldExportToDatabricks_NullTag_ReturnsFalse()
        {
            // Act & Assert
            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.StatementExecution, null!));
        }

        [Fact]
        public void TelemetryTagRegistry_ShouldExportToDatabricks_EmptyTag_ReturnsFalse()
        {
            // Act & Assert
            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.StatementExecution, ""));
            Assert.False(TelemetryTagRegistry.ShouldExportToDatabricks(
                TelemetryEventType.StatementExecution, "   "));
        }

        // ============================================================================
        // ShouldExportToLocal Tests
        // ============================================================================

        [Fact]
        public void TelemetryTagRegistry_ShouldExportToLocal_AllTags_ReturnsTrue()
        {
            // Act & Assert - All tags should be exportable to local diagnostics
            Assert.True(TelemetryTagRegistry.ShouldExportToLocal(
                TelemetryEventType.StatementExecution, StatementExecutionEvent.DbStatement));
            Assert.True(TelemetryTagRegistry.ShouldExportToLocal(
                TelemetryEventType.ConnectionOpen, ConnectionOpenEvent.ServerAddress));
            Assert.True(TelemetryTagRegistry.ShouldExportToLocal(
                TelemetryEventType.Error, ErrorEvent.ErrorMessage));
            Assert.True(TelemetryTagRegistry.ShouldExportToLocal(
                TelemetryEventType.Error, ErrorEvent.ErrorStackTrace));
        }

        [Fact]
        public void TelemetryTagRegistry_ShouldExportToLocal_NullTag_ReturnsFalse()
        {
            // Act & Assert
            Assert.False(TelemetryTagRegistry.ShouldExportToLocal(
                TelemetryEventType.StatementExecution, null!));
        }

        [Fact]
        public void TelemetryTagRegistry_ShouldExportToLocal_EmptyTag_ReturnsFalse()
        {
            // Act & Assert
            Assert.False(TelemetryTagRegistry.ShouldExportToLocal(
                TelemetryEventType.StatementExecution, ""));
        }

        // ============================================================================
        // FilterForDatabricksExport Tests
        // ============================================================================

        [Fact]
        public void TelemetryTagRegistry_FilterForDatabricksExport_RemovesSensitiveTags()
        {
            // Arrange
            var tags = new Dictionary<string, string?>
            {
                { StatementExecutionEvent.StatementId, "stmt-123" },
                { StatementExecutionEvent.SessionId, "session-456" },
                { StatementExecutionEvent.DbStatement, "SELECT * FROM users WHERE id = 1" }, // sensitive
                { StatementExecutionEvent.ResultFormat, "cloudfetch" }
            };

            // Act
            var filtered = TelemetryTagRegistry.FilterForDatabricksExport(
                TelemetryEventType.StatementExecution, tags);

            // Assert
            Assert.Equal(3, filtered.Count);
            Assert.Contains(StatementExecutionEvent.StatementId, filtered.Keys);
            Assert.Contains(StatementExecutionEvent.SessionId, filtered.Keys);
            Assert.Contains(StatementExecutionEvent.ResultFormat, filtered.Keys);
            Assert.DoesNotContain(StatementExecutionEvent.DbStatement, filtered.Keys);
        }

        [Fact]
        public void TelemetryTagRegistry_FilterForDatabricksExport_PreservesValues()
        {
            // Arrange
            var tags = new Dictionary<string, string?>
            {
                { StatementExecutionEvent.StatementId, "stmt-123" },
                { StatementExecutionEvent.ResultChunkCount, "5" }
            };

            // Act
            var filtered = TelemetryTagRegistry.FilterForDatabricksExport(
                TelemetryEventType.StatementExecution, tags);

            // Assert
            Assert.Equal("stmt-123", filtered[StatementExecutionEvent.StatementId]);
            Assert.Equal("5", filtered[StatementExecutionEvent.ResultChunkCount]);
        }

        [Fact]
        public void TelemetryTagRegistry_FilterForDatabricksExport_NullTags_ReturnsEmptyDictionary()
        {
            // Act
            var filtered = TelemetryTagRegistry.FilterForDatabricksExport(
                TelemetryEventType.StatementExecution, null!);

            // Assert
            Assert.NotNull(filtered);
            Assert.Empty(filtered);
        }

        [Fact]
        public void TelemetryTagRegistry_FilterForDatabricksExport_EmptyTags_ReturnsEmptyDictionary()
        {
            // Arrange
            var tags = new Dictionary<string, string?>();

            // Act
            var filtered = TelemetryTagRegistry.FilterForDatabricksExport(
                TelemetryEventType.StatementExecution, tags);

            // Assert
            Assert.NotNull(filtered);
            Assert.Empty(filtered);
        }

        // ============================================================================
        // GetEventName Tests
        // ============================================================================

        [Fact]
        public void TelemetryTagRegistry_GetEventName_ConnectionOpen_ReturnsCorrectName()
        {
            // Act
            var eventName = TelemetryTagRegistry.GetEventName(TelemetryEventType.ConnectionOpen);

            // Assert
            Assert.Equal(ConnectionOpenEvent.EventName, eventName);
            Assert.Equal("Connection.Open", eventName);
        }

        [Fact]
        public void TelemetryTagRegistry_GetEventName_StatementExecution_ReturnsCorrectName()
        {
            // Act
            var eventName = TelemetryTagRegistry.GetEventName(TelemetryEventType.StatementExecution);

            // Assert
            Assert.Equal(StatementExecutionEvent.EventName, eventName);
            Assert.Equal("Statement.Execute", eventName);
        }

        [Fact]
        public void TelemetryTagRegistry_GetEventName_Error_ReturnsCorrectName()
        {
            // Act
            var eventName = TelemetryTagRegistry.GetEventName(TelemetryEventType.Error);

            // Assert
            Assert.Equal(ErrorEvent.EventName, eventName);
            Assert.Equal("Error", eventName);
        }

        // ============================================================================
        // DetermineEventType Tests
        // ============================================================================

        [Fact]
        public void TelemetryTagRegistry_DetermineEventType_ConnectionOpen_ReturnsConnectionOpen()
        {
            // Act & Assert
            Assert.Equal(TelemetryEventType.ConnectionOpen,
                TelemetryTagRegistry.DetermineEventType("Connection.Open", false));
            Assert.Equal(TelemetryEventType.ConnectionOpen,
                TelemetryTagRegistry.DetermineEventType("Connection.OpenAsync", false));
            Assert.Equal(TelemetryEventType.ConnectionOpen,
                TelemetryTagRegistry.DetermineEventType("Connection.Close", false));
        }

        [Fact]
        public void TelemetryTagRegistry_DetermineEventType_StatementExecution_ReturnsStatementExecution()
        {
            // Act & Assert
            Assert.Equal(TelemetryEventType.StatementExecution,
                TelemetryTagRegistry.DetermineEventType("Statement.Execute", false));
            Assert.Equal(TelemetryEventType.StatementExecution,
                TelemetryTagRegistry.DetermineEventType("Statement.ExecuteQuery", false));
            Assert.Equal(TelemetryEventType.StatementExecution,
                TelemetryTagRegistry.DetermineEventType("Statement.ExecuteUpdate", false));
        }

        [Fact]
        public void TelemetryTagRegistry_DetermineEventType_WithErrorTag_ReturnsError()
        {
            // Act & Assert - Error tag takes precedence over operation name
            Assert.Equal(TelemetryEventType.Error,
                TelemetryTagRegistry.DetermineEventType("Statement.Execute", true));
            Assert.Equal(TelemetryEventType.Error,
                TelemetryTagRegistry.DetermineEventType("Connection.Open", true));
        }

        [Fact]
        public void TelemetryTagRegistry_DetermineEventType_UnknownOperation_ReturnsStatementExecution()
        {
            // Act & Assert - Default to StatementExecution for unknown operations
            Assert.Equal(TelemetryEventType.StatementExecution,
                TelemetryTagRegistry.DetermineEventType("Unknown.Operation", false));
            Assert.Equal(TelemetryEventType.StatementExecution,
                TelemetryTagRegistry.DetermineEventType("CloudFetch.Download", false));
        }

        [Fact]
        public void TelemetryTagRegistry_DetermineEventType_NullOrEmptyOperation_ReturnsStatementExecution()
        {
            // Act & Assert
            Assert.Equal(TelemetryEventType.StatementExecution,
                TelemetryTagRegistry.DetermineEventType(null!, false));
            Assert.Equal(TelemetryEventType.StatementExecution,
                TelemetryTagRegistry.DetermineEventType("", false));
        }

        [Fact]
        public void TelemetryTagRegistry_DetermineEventType_CaseInsensitive()
        {
            // Act & Assert - Should be case insensitive
            Assert.Equal(TelemetryEventType.ConnectionOpen,
                TelemetryTagRegistry.DetermineEventType("connection.open", false));
            Assert.Equal(TelemetryEventType.StatementExecution,
                TelemetryTagRegistry.DetermineEventType("STATEMENT.EXECUTE", false));
        }

        // ============================================================================
        // Tag Constant Value Tests
        // ============================================================================

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
        public void StatementExecutionEvent_TagConstants_HaveCorrectValues()
        {
            // Assert
            Assert.Equal("statement.id", StatementExecutionEvent.StatementId);
            Assert.Equal("session.id", StatementExecutionEvent.SessionId);
            Assert.Equal("result.format", StatementExecutionEvent.ResultFormat);
            Assert.Equal("result.chunk_count", StatementExecutionEvent.ResultChunkCount);
            Assert.Equal("result.bytes_downloaded", StatementExecutionEvent.ResultBytesDownloaded);
            Assert.Equal("poll.count", StatementExecutionEvent.PollCount);
            Assert.Equal("poll.latency_ms", StatementExecutionEvent.PollLatencyMs);
            Assert.Equal("db.statement", StatementExecutionEvent.DbStatement);
        }

        [Fact]
        public void ErrorEvent_TagConstants_HaveCorrectValues()
        {
            // Assert
            Assert.Equal("error.type", ErrorEvent.ErrorType);
            Assert.Equal("error.name", ErrorEvent.ErrorName);
            Assert.Equal("error.code", ErrorEvent.ErrorCode);
            Assert.Equal("error.message", ErrorEvent.ErrorMessage);
            Assert.Equal("error.stack_trace", ErrorEvent.ErrorStackTrace);
            Assert.Equal("session.id", ErrorEvent.SessionId);
            Assert.Equal("statement.id", ErrorEvent.StatementId);
        }

        // ============================================================================
        // TagExportScope Enum Tests
        // ============================================================================

        [Fact]
        public void TagExportScope_Values_AreCorrect()
        {
            // Assert
            Assert.Equal(0, (int)TagExportScope.None);
            Assert.Equal(1, (int)TagExportScope.ExportLocal);
            Assert.Equal(2, (int)TagExportScope.ExportDatabricks);
            Assert.Equal(3, (int)TagExportScope.ExportAll);
        }

        [Fact]
        public void TagExportScope_ExportAll_IsCombinationOfLocalAndDatabricks()
        {
            // Assert
            Assert.Equal(TagExportScope.ExportLocal | TagExportScope.ExportDatabricks, TagExportScope.ExportAll);
        }

        // ============================================================================
        // TelemetryEventType Enum Tests
        // ============================================================================

        [Fact]
        public void TelemetryEventType_Values_AreCorrect()
        {
            // Assert
            Assert.Equal(0, (int)TelemetryEventType.ConnectionOpen);
            Assert.Equal(1, (int)TelemetryEventType.StatementExecution);
            Assert.Equal(2, (int)TelemetryEventType.Error);
        }

        // ============================================================================
        // TelemetryTagAttribute Tests
        // ============================================================================

        [Fact]
        public void TelemetryTagAttribute_Constructor_SetsTagName()
        {
            // Act
            var attr = new TelemetryTagAttribute("test.tag");

            // Assert
            Assert.Equal("test.tag", attr.TagName);
        }

        [Fact]
        public void TelemetryTagAttribute_DefaultExportScope_IsExportAll()
        {
            // Act
            var attr = new TelemetryTagAttribute("test.tag");

            // Assert
            Assert.Equal(TagExportScope.ExportAll, attr.ExportScope);
        }

        [Fact]
        public void TelemetryTagAttribute_DefaultRequired_IsFalse()
        {
            // Act
            var attr = new TelemetryTagAttribute("test.tag");

            // Assert
            Assert.False(attr.Required);
        }

        [Fact]
        public void TelemetryTagAttribute_SetProperties_Works()
        {
            // Act
            var attr = new TelemetryTagAttribute("test.tag")
            {
                ExportScope = TagExportScope.ExportLocal,
                Description = "Test description",
                Required = true
            };

            // Assert
            Assert.Equal("test.tag", attr.TagName);
            Assert.Equal(TagExportScope.ExportLocal, attr.ExportScope);
            Assert.Equal("Test description", attr.Description);
            Assert.True(attr.Required);
        }
    }
}
