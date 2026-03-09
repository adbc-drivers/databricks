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
using System.Threading;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Proto;
using DriverModeType = AdbcDrivers.Databricks.Telemetry.Proto.DriverMode.Types.Type;
using ExecutionResultFormat = AdbcDrivers.Databricks.Telemetry.Proto.ExecutionResult.Types.Format;
using OperationType = AdbcDrivers.Databricks.Telemetry.Proto.Operation.Types.Type;
using StatementType = AdbcDrivers.Databricks.Telemetry.Proto.Statement.Types.Type;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for StatementTelemetryContext lifecycle and proto building.
    /// </summary>
    public class StatementTelemetryContextTests
    {
        private TelemetrySessionContext CreateTestSessionContext()
        {
            return new TelemetrySessionContext
            {
                SessionId = "test-session-123",
                AuthType = "PAT",
                WorkspaceId = 12345678901234L,
                SystemConfiguration = new DriverSystemConfiguration
                {
                    DriverVersion = "1.0.0",
                    DriverName = "Apache Arrow ADBC Databricks Driver",
                    RuntimeName = ".NET 8.0",
                    OsName = "Linux"
                },
                DriverConnectionParams = new DriverConnectionParameters
                {
                    HttpPath = "/sql/1.0/warehouses/abc123",
                    Mode = DriverModeType.Thrift
                },
                DefaultResultFormat = ExecutionResultFormat.ExternalLinks,
                DefaultCompressionEnabled = true
            };
        }

        [Fact]
        public void StatementTelemetryContext_Construction_RequiresSessionContext()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new StatementTelemetryContext(null!));
        }

        [Fact]
        public void StatementTelemetryContext_Construction_StartsStopwatch()
        {
            // Arrange
            TelemetrySessionContext sessionContext = CreateTestSessionContext();

            // Act
            StatementTelemetryContext context = new StatementTelemetryContext(sessionContext);
            Thread.Sleep(10); // Small delay to ensure stopwatch is running

            // Assert
            Assert.NotNull(context.ExecuteStopwatch);
            Assert.True(context.ExecuteStopwatch.IsRunning);
            Assert.True(context.ExecuteStopwatch.ElapsedMilliseconds > 0);
        }

        [Fact]
        public void StatementTelemetryContext_SessionProperties_InheritedFromSessionContext()
        {
            // Arrange
            TelemetrySessionContext sessionContext = CreateTestSessionContext();

            // Act
            StatementTelemetryContext context = new StatementTelemetryContext(sessionContext);

            // Assert
            Assert.Equal("test-session-123", context.SessionId);
            Assert.Equal("PAT", context.AuthType);
            Assert.Equal(12345678901234L, context.WorkspaceId);
            Assert.Same(sessionContext.SystemConfiguration, context.SystemConfiguration);
            Assert.Same(sessionContext.DriverConnectionParams, context.DriverConnectionParams);
        }

        [Fact]
        public void StatementTelemetryContext_StatementProperties_CanBeSet()
        {
            // Arrange
            TelemetrySessionContext sessionContext = CreateTestSessionContext();
            StatementTelemetryContext context = new StatementTelemetryContext(sessionContext);

            // Act
            context.StatementId = "statement-uuid-789";
            context.StatementType = StatementType.Query;
            context.OperationType = OperationType.ExecuteStatement;
            context.ResultFormat = ExecutionResultFormat.ExternalLinks;
            context.IsCompressed = true;

            // Assert
            Assert.Equal("statement-uuid-789", context.StatementId);
            Assert.Equal(StatementType.Query, context.StatementType);
            Assert.Equal(OperationType.ExecuteStatement, context.OperationType);
            Assert.Equal(ExecutionResultFormat.ExternalLinks, context.ResultFormat);
            Assert.True(context.IsCompressed);
        }

        [Fact]
        public void StatementTelemetryContext_RecordFirstBatchReady_SetsTimingOnce()
        {
            // Arrange
            TelemetrySessionContext sessionContext = CreateTestSessionContext();
            StatementTelemetryContext context = new StatementTelemetryContext(sessionContext);
            Thread.Sleep(5);

            // Act
            context.RecordFirstBatchReady();
            long firstTimestamp = context.FirstBatchReadyMs!.Value;
            Thread.Sleep(5);
            context.RecordFirstBatchReady(); // Second call should not update

            // Assert
            Assert.NotNull(context.FirstBatchReadyMs);
            Assert.True(context.FirstBatchReadyMs > 0);
            Assert.Equal(firstTimestamp, context.FirstBatchReadyMs); // Should not change
        }

        [Fact]
        public void StatementTelemetryContext_RecordResultsConsumed_SetsElapsedTime()
        {
            // Arrange
            TelemetrySessionContext sessionContext = CreateTestSessionContext();
            StatementTelemetryContext context = new StatementTelemetryContext(sessionContext);
            Thread.Sleep(5);

            // Act
            context.RecordResultsConsumed();

            // Assert
            Assert.NotNull(context.ResultsConsumedMs);
            Assert.True(context.ResultsConsumedMs > 0);
        }

        [Fact]
        public void StatementTelemetryContext_SetChunkDetails_PopulatesAllFields()
        {
            // Arrange
            TelemetrySessionContext sessionContext = CreateTestSessionContext();
            StatementTelemetryContext context = new StatementTelemetryContext(sessionContext);

            // Act
            context.SetChunkDetails(
                totalPresent: 10,
                totalIterated: 8,
                initialLatencyMs: 50,
                slowestLatencyMs: 150,
                sumDownloadTimeMs: 800);

            // Assert
            Assert.Equal(10, context.TotalChunksPresent);
            Assert.Equal(8, context.TotalChunksIterated);
            Assert.Equal(50, context.InitialChunkLatencyMs);
            Assert.Equal(150, context.SlowestChunkLatencyMs);
            Assert.Equal(800, context.SumChunksDownloadTimeMs);
        }

        [Fact]
        public void StatementTelemetryContext_ErrorProperties_CanBeSet()
        {
            // Arrange
            TelemetrySessionContext sessionContext = CreateTestSessionContext();
            StatementTelemetryContext context = new StatementTelemetryContext(sessionContext);

            // Act
            context.HasError = true;
            context.ErrorName = "DatabricksException";
            context.ErrorMessage = "Connection timeout";

            // Assert
            Assert.True(context.HasError);
            Assert.Equal("DatabricksException", context.ErrorName);
            Assert.Equal("Connection timeout", context.ErrorMessage);
        }

        [Fact]
        public void StatementTelemetryContext_BuildTelemetryLog_ConstructsValidProto()
        {
            // Arrange
            TelemetrySessionContext sessionContext = CreateTestSessionContext();
            StatementTelemetryContext context = new StatementTelemetryContext(sessionContext);
            context.StatementId = "stmt-123";
            context.StatementType = StatementType.Query;
            context.OperationType = OperationType.ExecuteStatement;
            context.ResultFormat = ExecutionResultFormat.ExternalLinks;
            context.IsCompressed = true;
            Thread.Sleep(10);

            // Act
            OssSqlDriverTelemetryLog log = context.BuildTelemetryLog();

            // Assert - verify session-level fields
            Assert.Equal("test-session-123", log.SessionId);
            Assert.Equal("stmt-123", log.SqlStatementId);
            Assert.Equal("PAT", log.AuthType);
            Assert.Same(sessionContext.SystemConfiguration, log.SystemConfiguration);
            Assert.Same(sessionContext.DriverConnectionParams, log.DriverConnectionParams);

            // Assert - verify operation latency
            Assert.True(log.OperationLatencyMs > 0);

            // Assert - verify SQL execution event
            Assert.NotNull(log.SqlOperation);
            Assert.Equal(StatementType.Query, log.SqlOperation.StatementType);
            Assert.True(log.SqlOperation.IsCompressed);
            Assert.Equal(ExecutionResultFormat.ExternalLinks, log.SqlOperation.ExecutionResult);

            // Assert - verify operation detail
            Assert.NotNull(log.SqlOperation.OperationDetail);
            Assert.Equal(OperationType.ExecuteStatement, log.SqlOperation.OperationDetail.OperationType);
            Assert.False(log.SqlOperation.OperationDetail.IsInternalCall);
        }

        [Fact]
        public void StatementTelemetryLog_WithChunkDetails_IncludesChunkInfo()
        {
            // Arrange
            TelemetrySessionContext sessionContext = CreateTestSessionContext();
            StatementTelemetryContext context = new StatementTelemetryContext(sessionContext);
            context.SetChunkDetails(
                totalPresent: 5,
                totalIterated: 5,
                initialLatencyMs: 100,
                slowestLatencyMs: 200,
                sumDownloadTimeMs: 750);

            // Act
            OssSqlDriverTelemetryLog log = context.BuildTelemetryLog();

            // Assert
            Assert.NotNull(log.SqlOperation);
            Assert.NotNull(log.SqlOperation.ChunkDetails);
            Assert.Equal(5, log.SqlOperation.ChunkDetails.TotalChunksPresent);
            Assert.Equal(5, log.SqlOperation.ChunkDetails.TotalChunksIterated);
            Assert.Equal(100, log.SqlOperation.ChunkDetails.InitialChunkLatencyMillis);
            Assert.Equal(200, log.SqlOperation.ChunkDetails.SlowestChunkLatencyMillis);
            Assert.Equal(750, log.SqlOperation.ChunkDetails.SumChunksDownloadTimeMillis);
        }

        [Fact]
        public void StatementTelemetryLog_WithResultLatency_IncludesTimingInfo()
        {
            // Arrange
            TelemetrySessionContext sessionContext = CreateTestSessionContext();
            StatementTelemetryContext context = new StatementTelemetryContext(sessionContext);
            Thread.Sleep(5);
            context.RecordFirstBatchReady();
            Thread.Sleep(5);
            context.RecordResultsConsumed();

            // Act
            OssSqlDriverTelemetryLog log = context.BuildTelemetryLog();

            // Assert
            Assert.NotNull(log.SqlOperation);
            Assert.NotNull(log.SqlOperation.ResultLatency);
            Assert.True(log.SqlOperation.ResultLatency.ResultSetReadyLatencyMillis > 0);
            Assert.True(log.SqlOperation.ResultLatency.ResultSetConsumptionLatencyMillis > 0);
            Assert.True(log.SqlOperation.ResultLatency.ResultSetConsumptionLatencyMillis >=
                       log.SqlOperation.ResultLatency.ResultSetReadyLatencyMillis);
        }

        [Fact]
        public void StatementTelemetryLog_WithPolling_IncludesOperationDetail()
        {
            // Arrange
            TelemetrySessionContext sessionContext = CreateTestSessionContext();
            StatementTelemetryContext context = new StatementTelemetryContext(sessionContext);
            context.OperationType = OperationType.ExecuteStatement;
            context.PollCount = 3;
            context.PollLatencyMs = 450;

            // Act
            OssSqlDriverTelemetryLog log = context.BuildTelemetryLog();

            // Assert
            Assert.NotNull(log.SqlOperation);
            Assert.NotNull(log.SqlOperation.OperationDetail);
            Assert.Equal(3, log.SqlOperation.OperationDetail.NOperationStatusCalls);
            Assert.Equal(450, log.SqlOperation.OperationDetail.OperationStatusLatencyMillis);
            Assert.Equal(OperationType.ExecuteStatement, log.SqlOperation.OperationDetail.OperationType);
        }

        [Fact]
        public void StatementTelemetryLog_WithError_IncludesErrorInfo()
        {
            // Arrange
            TelemetrySessionContext sessionContext = CreateTestSessionContext();
            StatementTelemetryContext context = new StatementTelemetryContext(sessionContext);
            context.HasError = true;
            context.ErrorName = "TimeoutException";
            context.ErrorMessage = "Query execution timed out";

            // Act
            OssSqlDriverTelemetryLog log = context.BuildTelemetryLog();

            // Assert
            Assert.NotNull(log.ErrorInfo);
            Assert.Equal("TimeoutException", log.ErrorInfo.ErrorName);
            Assert.Equal(string.Empty, log.ErrorInfo.StackTrace); // Should be empty per privacy requirements
        }

        [Fact]
        public void StatementTelemetryLog_WithoutError_NoErrorInfo()
        {
            // Arrange
            TelemetrySessionContext sessionContext = CreateTestSessionContext();
            StatementTelemetryContext context = new StatementTelemetryContext(sessionContext);
            context.HasError = false;

            // Act
            OssSqlDriverTelemetryLog log = context.BuildTelemetryLog();

            // Assert
            Assert.Null(log.ErrorInfo);
        }

        [Fact]
        public void StatementTelemetryLog_WithoutChunks_NoChunkDetails()
        {
            // Arrange
            TelemetrySessionContext sessionContext = CreateTestSessionContext();
            StatementTelemetryContext context = new StatementTelemetryContext(sessionContext);
            // Don't set any chunk details

            // Act
            OssSqlDriverTelemetryLog log = context.BuildTelemetryLog();

            // Assert
            Assert.NotNull(log.SqlOperation);
            Assert.Null(log.SqlOperation.ChunkDetails);
        }

        [Fact]
        public void StatementTelemetryLog_ProtocolAgnostic_WorksForThriftAndSea()
        {
            // Test Thrift
            TelemetrySessionContext thriftSession = new TelemetrySessionContext
            {
                SessionId = "thrift-session",
                WorkspaceId = 123L,
                DriverConnectionParams = new DriverConnectionParameters
                {
                    Mode = DriverModeType.Thrift
                }
            };
            StatementTelemetryContext thriftContext = new StatementTelemetryContext(thriftSession);
            thriftContext.StatementId = "thrift-stmt";
            OssSqlDriverTelemetryLog thriftLog = thriftContext.BuildTelemetryLog();

            // Test SEA
            TelemetrySessionContext seaSession = new TelemetrySessionContext
            {
                SessionId = "sea-session",
                WorkspaceId = 456L,
                DriverConnectionParams = new DriverConnectionParameters
                {
                    Mode = DriverModeType.Sea
                }
            };
            StatementTelemetryContext seaContext = new StatementTelemetryContext(seaSession);
            seaContext.StatementId = "sea-stmt";
            OssSqlDriverTelemetryLog seaLog = seaContext.BuildTelemetryLog();

            // Assert - both work the same way
            Assert.Equal("thrift-session", thriftLog.SessionId);
            Assert.Equal("thrift-stmt", thriftLog.SqlStatementId);
            Assert.Equal(DriverModeType.Thrift, thriftLog.DriverConnectionParams.Mode);

            Assert.Equal("sea-session", seaLog.SessionId);
            Assert.Equal("sea-stmt", seaLog.SqlStatementId);
            Assert.Equal(DriverModeType.Sea, seaLog.DriverConnectionParams.Mode);
        }

        [Fact]
        public void StatementTelemetryContext_StopwatchTiming_IsConsistent()
        {
            // Arrange
            TelemetrySessionContext sessionContext = CreateTestSessionContext();
            StatementTelemetryContext context = new StatementTelemetryContext(sessionContext);

            // Act
            Thread.Sleep(10);
            context.RecordFirstBatchReady();
            Thread.Sleep(10);
            context.RecordResultsConsumed();
            OssSqlDriverTelemetryLog log = context.BuildTelemetryLog();

            // Assert - all timings should be from same stopwatch
            long operationLatency = log.OperationLatencyMs;
            long firstBatchReady = context.FirstBatchReadyMs!.Value;
            long resultsConsumed = context.ResultsConsumedMs!.Value;

            Assert.True(operationLatency >= resultsConsumed);
            Assert.True(resultsConsumed >= firstBatchReady);
            Assert.True(firstBatchReady > 0);
        }
    }
}
