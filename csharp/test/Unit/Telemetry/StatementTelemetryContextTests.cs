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
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Proto;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for <see cref="StatementTelemetryContext"/>.
    /// Uses real Activity objects created from ActivitySource as required.
    /// </summary>
    public class StatementTelemetryContextTests : IDisposable
    {
        private static readonly ActivitySource TestSource = new("Test.StatementTelemetryContext");
        private readonly ActivityListener _listener;

        public StatementTelemetryContextTests()
        {
            _listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
            };
            ActivitySource.AddActivityListener(_listener);
        }

        public void Dispose()
        {
            _listener.Dispose();
        }

        #region Constructor Tests

        [Fact]
        public void Constructor_DefaultParameters_InitializesWithDefaults()
        {
            // Act
            var context = new StatementTelemetryContext();

            // Assert
            Assert.Equal(string.Empty, context.StatementId);
            Assert.Null(context.SessionId);
            Assert.Null(context.AuthType);
            Assert.Null(context.SystemConfiguration);
            Assert.Null(context.ConnectionParameters);
            Assert.Null(context.StatementType);
            Assert.Equal(0, context.TotalLatencyMs);
            Assert.False(context.HasError);
        }

        [Fact]
        public void Constructor_WithParameters_StoresConnectionLevelData()
        {
            // Arrange
            var sessionId = "sess-123";
            var systemConfig = new DriverSystemConfiguration
            {
                DriverVersion = "1.0.0",
                OsName = "Linux"
            };
            var connectionParams = new DriverConnectionParameters
            {
                HttpPath = "/sql/1.0/warehouses/abc"
            };

            // Act
            var context = new StatementTelemetryContext(sessionId, systemConfig, connectionParams);

            // Assert
            Assert.Equal(sessionId, context.SessionId);
            Assert.Same(systemConfig, context.SystemConfiguration);
            Assert.Same(connectionParams, context.ConnectionParameters);
        }

        #endregion

        #region MergeFrom - ExecuteQuery Tests

        [Fact]
        public void MergeFrom_ExecuteQuery_CapturesLatencyAndType()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("ExecuteQuery", a =>
            {
                a.SetTag("session.id", "sess-123");
                a.SetTag("statement.id", "stmt-456");
                a.SetTag("auth.type", "pat");
                a.SetTag("statement.type", "query");
                a.SetTag("result.format", "cloudfetch");
                a.SetTag("result.compression_enabled", true);
                a.SetTag("result.ready_latency_ms", 150L);
                a.SetTag("result.consumption_latency_ms", 500L);
                a.SetTag("retry.count", 2);
                a.SetTag("operation.type", "execute_statement");
                a.SetTag("operation.is_internal", false);
                // Add a small delay to ensure non-zero duration
                Thread.Sleep(5);
            });

            // Act
            context.MergeFrom(activity);

            // Assert - Identifiers
            Assert.Equal("sess-123", context.SessionId);
            Assert.Equal("stmt-456", context.StatementId);
            Assert.Equal("pat", context.AuthType);

            // Assert - Statement metrics
            Assert.True(context.TotalLatencyMs >= 0);
            Assert.Equal(StatementType.StatementQuery, context.StatementType);
            Assert.Equal("cloudfetch", context.ResultFormat);
            Assert.True(context.CompressionEnabled);
            Assert.Equal(150L, context.ResultReadyLatencyMs);
            Assert.Equal(500L, context.ResultConsumptionLatencyMs);
            Assert.Equal(2, context.RetryCount);
            Assert.Equal(OperationType.OperationExecuteStatement, context.OperationType);
            Assert.False(context.IsInternalCall);
        }

        [Fact]
        public void MergeFrom_ExecuteUpdate_CapturesUpdateType()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("ExecuteUpdate", a =>
            {
                a.SetTag("statement.id", "stmt-789");
                a.SetTag("statement.type", "update");
            });

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal("stmt-789", context.StatementId);
            Assert.Equal(StatementType.StatementUpdate, context.StatementType);
        }

        [Fact]
        public void MergeFrom_GetCatalogs_CapturesMetadataType()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("GetCatalogs", a =>
            {
                a.SetTag("statement.id", "stmt-meta-1");
                a.SetTag("statement.type", "metadata");
                a.SetTag("operation.type", "list_catalogs");
                a.SetTag("operation.is_internal", true);
            });

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal(StatementType.StatementMetadata, context.StatementType);
            Assert.Equal(OperationType.OperationListCatalogs, context.OperationType);
            Assert.True(context.IsInternalCall);
        }

        #endregion

        #region MergeFrom - DownloadFiles Tests

        [Fact]
        public void MergeFrom_DownloadFiles_CapturesChunkDetails()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("DownloadFiles", a =>
            {
                a.SetTag("statement.id", "stmt-dl-1");

                // Add cloudfetch.download_summary event with tags
                var eventTags = new ActivityTagsCollection();
                eventTags["total_files"] = 10;
                eventTags["successful_downloads"] = 8;
                eventTags["total_time_ms"] = 5000L;
                eventTags["initial_chunk_latency_ms"] = 100L;
                eventTags["slowest_chunk_latency_ms"] = 500L;
                a.AddEvent(new ActivityEvent("cloudfetch.download_summary", default, eventTags));
            });

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal(10, context.TotalChunksPresent);
            Assert.Equal(8, context.TotalChunksIterated);
            Assert.Equal(5000L, context.SumChunksDownloadTimeMs);
            Assert.Equal(100L, context.InitialChunkLatencyMs);
            Assert.Equal(500L, context.SlowestChunkLatencyMs);
        }

        [Fact]
        public void MergeFrom_DownloadFiles_NoSummaryEvent_NoChunkData()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("DownloadFiles", a =>
            {
                a.SetTag("statement.id", "stmt-dl-2");
                // No download_summary event added
            });

            // Act
            context.MergeFrom(activity);

            // Assert - No chunk data captured
            Assert.Null(context.TotalChunksPresent);
            Assert.Null(context.TotalChunksIterated);
            Assert.Null(context.SumChunksDownloadTimeMs);
            Assert.Null(context.InitialChunkLatencyMs);
            Assert.Null(context.SlowestChunkLatencyMs);
        }

        [Fact]
        public void MergeFrom_DownloadFiles_IgnoresNonSummaryEvents()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("DownloadFiles", a =>
            {
                a.SetTag("statement.id", "stmt-dl-3");

                // Add a non-summary event
                var otherTags = new ActivityTagsCollection();
                otherTags["chunk_id"] = 1;
                a.AddEvent(new ActivityEvent("cloudfetch.chunk_downloaded", default, otherTags));
            });

            // Act
            context.MergeFrom(activity);

            // Assert - Should not have captured any chunk details
            Assert.Null(context.TotalChunksPresent);
        }

        #endregion

        #region MergeFrom - PollOperationStatus Tests

        [Fact]
        public void MergeFrom_PollStatus_CapturesPollingMetrics()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("PollOperationStatus", a =>
            {
                a.SetTag("statement.id", "stmt-poll-1");
                a.SetTag("poll.count", 5);
                a.SetTag("poll.latency_ms", 2500L);
            });

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal(5, context.PollCount);
            Assert.Equal(2500L, context.PollLatencyMs);
        }

        [Fact]
        public void MergeFrom_PollStatus_StringValues_ParsesCorrectly()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("PollOperationStatus", a =>
            {
                a.SetTag("statement.id", "stmt-poll-2");
                a.SetTag("poll.count", "3");
                a.SetTag("poll.latency_ms", "1500");
            });

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal(3, context.PollCount);
            Assert.Equal(1500L, context.PollLatencyMs);
        }

        #endregion

        #region MergeFrom - Multiple Activities Tests

        [Fact]
        public void MergeFrom_MultipleActivities_AggregatesCorrectly()
        {
            // Arrange - Simulates the typical activity hierarchy:
            // ExecuteQuery (root) → DownloadFiles (child) → PollOperationStatus (child)
            var context = new StatementTelemetryContext();

            // Child activity 1: PollOperationStatus
            var pollActivity = CreateAndStopActivity("PollOperationStatus", a =>
            {
                a.SetTag("statement.id", "stmt-multi-1");
                a.SetTag("session.id", "sess-multi-1");
                a.SetTag("poll.count", 3);
                a.SetTag("poll.latency_ms", 1000L);
            });

            // Child activity 2: DownloadFiles
            var downloadActivity = CreateAndStopActivity("DownloadFiles", a =>
            {
                a.SetTag("statement.id", "stmt-multi-1");
                var eventTags = new ActivityTagsCollection();
                eventTags["total_files"] = 5;
                eventTags["successful_downloads"] = 5;
                eventTags["total_time_ms"] = 3000L;
                eventTags["initial_chunk_latency_ms"] = 50L;
                eventTags["slowest_chunk_latency_ms"] = 200L;
                a.AddEvent(new ActivityEvent("cloudfetch.download_summary", default, eventTags));
            });

            // Root activity: ExecuteQuery
            var executeActivity = CreateAndStopActivity("ExecuteQuery", a =>
            {
                a.SetTag("statement.id", "stmt-multi-1");
                a.SetTag("session.id", "sess-multi-1");
                a.SetTag("auth.type", "oauth-m2m");
                a.SetTag("statement.type", "query");
                a.SetTag("result.format", "cloudfetch");
                a.SetTag("result.compression_enabled", true);
                a.SetTag("result.ready_latency_ms", 200L);
                Thread.Sleep(5);
            });

            // Act - Merge in child-first order (as Activity hierarchy dictates)
            context.MergeFrom(pollActivity);
            context.MergeFrom(downloadActivity);
            context.MergeFrom(executeActivity);

            // Assert - All data from all 3 activities is aggregated
            Assert.Equal("stmt-multi-1", context.StatementId);
            Assert.Equal("sess-multi-1", context.SessionId);
            Assert.Equal("oauth-m2m", context.AuthType);

            // From ExecuteQuery
            Assert.Equal(StatementType.StatementQuery, context.StatementType);
            Assert.Equal("cloudfetch", context.ResultFormat);
            Assert.True(context.CompressionEnabled);
            Assert.True(context.TotalLatencyMs >= 0);
            Assert.Equal(200L, context.ResultReadyLatencyMs);

            // From PollOperationStatus
            Assert.Equal(3, context.PollCount);
            Assert.Equal(1000L, context.PollLatencyMs);

            // From DownloadFiles
            Assert.Equal(5, context.TotalChunksPresent);
            Assert.Equal(5, context.TotalChunksIterated);
            Assert.Equal(3000L, context.SumChunksDownloadTimeMs);
            Assert.Equal(50L, context.InitialChunkLatencyMs);
            Assert.Equal(200L, context.SlowestChunkLatencyMs);

            // No errors
            Assert.False(context.HasError);
        }

        [Fact]
        public void MergeFrom_IdentifiersUseFirstNonNull()
        {
            // Arrange
            var context = new StatementTelemetryContext();

            var activity1 = CreateAndStopActivity("PollOperationStatus", a =>
            {
                a.SetTag("session.id", "first-session");
                a.SetTag("statement.id", "first-stmt");
                a.SetTag("auth.type", "pat");
                a.SetTag("poll.count", 1);
            });

            var activity2 = CreateAndStopActivity("ExecuteQuery", a =>
            {
                a.SetTag("session.id", "second-session");
                a.SetTag("statement.id", "second-stmt");
                a.SetTag("auth.type", "oauth");
                a.SetTag("statement.type", "query");
            });

            // Act
            context.MergeFrom(activity1);
            context.MergeFrom(activity2);

            // Assert - First non-null values win for identifiers
            Assert.Equal("first-session", context.SessionId);
            Assert.Equal("first-stmt", context.StatementId);
            Assert.Equal("pat", context.AuthType);
        }

        #endregion

        #region MergeFrom - Error Tests

        [Fact]
        public void MergeFrom_ErrorActivity_CapturesErrorInfo()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("ExecuteQuery", a =>
            {
                a.SetTag("statement.id", "stmt-err-1");
                a.SetTag("statement.type", "query");
                a.SetTag("error.type", "TimeoutException");
                a.SetStatus(ActivityStatusCode.Error, "Connection timed out after 30s");
            });

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.True(context.HasError);
            Assert.Equal("TimeoutException", context.ErrorName);
            Assert.Equal("Connection timed out after 30s", context.ErrorMessage);
        }

        [Fact]
        public void MergeFrom_NonErrorActivity_NoErrorInfo()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("ExecuteQuery", a =>
            {
                a.SetTag("statement.id", "stmt-ok-1");
                a.SetTag("statement.type", "query");
                a.SetStatus(ActivityStatusCode.Ok);
            });

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.False(context.HasError);
            Assert.Null(context.ErrorName);
            Assert.Null(context.ErrorMessage);
        }

        [Fact]
        public void MergeFrom_ErrorOnChildActivity_CapturesErrorInfo()
        {
            // Arrange - Error occurs on child activity, not root
            var context = new StatementTelemetryContext();

            var childActivity = CreateAndStopActivity("DownloadFiles", a =>
            {
                a.SetTag("statement.id", "stmt-child-err");
                a.SetTag("error.type", "HttpRequestException");
                a.SetStatus(ActivityStatusCode.Error, "HTTP 503 Service Unavailable");
            });

            var rootActivity = CreateAndStopActivity("ExecuteQuery", a =>
            {
                a.SetTag("statement.id", "stmt-child-err");
                a.SetTag("statement.type", "query");
                a.SetStatus(ActivityStatusCode.Ok);
            });

            // Act
            context.MergeFrom(childActivity);
            context.MergeFrom(rootActivity);

            // Assert
            Assert.True(context.HasError);
            Assert.Equal("HttpRequestException", context.ErrorName);
            Assert.Equal("HTTP 503 Service Unavailable", context.ErrorMessage);
        }

        #endregion

        #region MergeFrom - System Configuration Tests

        [Fact]
        public void MergeFrom_ConnectionActivity_CapturesSystemConfig()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("ExecuteQuery", a =>
            {
                a.SetTag("statement.id", "stmt-config-1");
                a.SetTag("statement.type", "query");

                // System configuration tags
                a.SetTag("driver.version", "0.23.0");
                a.SetTag("driver.name", "oss-adbc-dotnet");
                a.SetTag("runtime.name", ".NET");
                a.SetTag("runtime.version", "8.0.0");
                a.SetTag("runtime.vendor", "Microsoft");
                a.SetTag("os.name", "Linux");
                a.SetTag("os.version", "5.15.0");
                a.SetTag("os.arch", "x86_64");
                a.SetTag("client.app_name", "TestApp");
                a.SetTag("locale.name", "en_US");
                a.SetTag("char_set_encoding", "UTF-8");
                a.SetTag("process.name", "dotnet");
            });

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.NotNull(context.SystemConfiguration);
            Assert.Equal("0.23.0", context.SystemConfiguration.DriverVersion);
            Assert.Equal("oss-adbc-dotnet", context.SystemConfiguration.DriverName);
            Assert.Equal(".NET", context.SystemConfiguration.RuntimeName);
            Assert.Equal("8.0.0", context.SystemConfiguration.RuntimeVersion);
            Assert.Equal("Microsoft", context.SystemConfiguration.RuntimeVendor);
            Assert.Equal("Linux", context.SystemConfiguration.OsName);
            Assert.Equal("5.15.0", context.SystemConfiguration.OsVersion);
            Assert.Equal("x86_64", context.SystemConfiguration.OsArch);
            Assert.Equal("TestApp", context.SystemConfiguration.ClientAppName);
            Assert.Equal("en_US", context.SystemConfiguration.LocaleName);
            Assert.Equal("UTF-8", context.SystemConfiguration.CharSetEncoding);
            Assert.Equal("dotnet", context.SystemConfiguration.ProcessName);
        }

        [Fact]
        public void MergeFrom_SystemConfigAlreadySet_DoesNotOverwrite()
        {
            // Arrange - Constructor provides system config
            var existingConfig = new DriverSystemConfiguration
            {
                DriverVersion = "1.0.0",
                OsName = "Windows"
            };
            var context = new StatementTelemetryContext(
                systemConfig: existingConfig);

            var activity = CreateAndStopActivity("ExecuteQuery", a =>
            {
                a.SetTag("statement.id", "stmt-config-2");
                a.SetTag("statement.type", "query");
                a.SetTag("driver.version", "2.0.0"); // Should NOT overwrite
                a.SetTag("os.name", "Linux"); // Should NOT overwrite
            });

            // Act
            context.MergeFrom(activity);

            // Assert - Original config preserved
            Assert.Same(existingConfig, context.SystemConfiguration);
            Assert.Equal("1.0.0", context.SystemConfiguration.DriverVersion);
            Assert.Equal("Windows", context.SystemConfiguration.OsName);
        }

        [Fact]
        public void MergeFrom_NoSystemConfigTags_SystemConfigStaysNull()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("ExecuteQuery", a =>
            {
                a.SetTag("statement.id", "stmt-no-config");
                a.SetTag("statement.type", "query");
                // No driver.version or system config tags
            });

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Null(context.SystemConfiguration);
        }

        #endregion

        #region MergeFrom - Unknown Operation Name Tests

        [Fact]
        public void MergeFrom_UnknownOperationName_StillCapturesIdentifiers()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("SomeUnknownOperation", a =>
            {
                a.SetTag("session.id", "sess-unknown");
                a.SetTag("statement.id", "stmt-unknown");
                a.SetTag("auth.type", "pat");
            });

            // Act
            context.MergeFrom(activity);

            // Assert - Identifiers are always captured
            Assert.Equal("sess-unknown", context.SessionId);
            Assert.Equal("stmt-unknown", context.StatementId);
            Assert.Equal("pat", context.AuthType);
            // No statement-specific data
            Assert.Null(context.StatementType);
            Assert.Equal(0, context.TotalLatencyMs);
        }

        #endregion

        #region BuildProto Tests

        [Fact]
        public void BuildProto_ReturnsCompleteMessage()
        {
            // Arrange - Populate all fields via MergeFrom
            var systemConfig = new DriverSystemConfiguration
            {
                DriverVersion = "0.23.0",
                DriverName = "oss-adbc-dotnet",
                RuntimeName = ".NET",
                RuntimeVersion = "8.0.0",
                RuntimeVendor = "Microsoft",
                OsName = "Linux",
                OsVersion = "5.15.0",
                OsArch = "x86_64"
            };
            var connectionParams = new DriverConnectionParameters
            {
                HttpPath = "/sql/1.0/warehouses/abc",
                Mode = DriverModeType.DriverModeThrift,
                EnableArrow = true,
                EnableDirectResults = true
            };
            var context = new StatementTelemetryContext("sess-proto-1", systemConfig, connectionParams);

            // Merge child activities first (as real execution order)
            var pollActivity = CreateAndStopActivity("PollOperationStatus", a =>
            {
                a.SetTag("statement.id", "stmt-proto-1");
                a.SetTag("poll.count", 4);
                a.SetTag("poll.latency_ms", 2000L);
            });
            context.MergeFrom(pollActivity);

            var downloadActivity = CreateAndStopActivity("DownloadFiles", a =>
            {
                a.SetTag("statement.id", "stmt-proto-1");
                var eventTags = new ActivityTagsCollection();
                eventTags["total_files"] = 20;
                eventTags["successful_downloads"] = 18;
                eventTags["total_time_ms"] = 8000L;
                eventTags["initial_chunk_latency_ms"] = 75L;
                eventTags["slowest_chunk_latency_ms"] = 350L;
                a.AddEvent(new ActivityEvent("cloudfetch.download_summary", default, eventTags));
            });
            context.MergeFrom(downloadActivity);

            // Merge root activity last
            var executeActivity = CreateAndStopActivity("ExecuteQuery", a =>
            {
                a.SetTag("statement.id", "stmt-proto-1");
                a.SetTag("auth.type", "pat");
                a.SetTag("statement.type", "query");
                a.SetTag("result.format", "cloudfetch");
                a.SetTag("result.compression_enabled", true);
                a.SetTag("retry.count", 1);
                a.SetTag("result.ready_latency_ms", 250L);
                a.SetTag("result.consumption_latency_ms", 1000L);
                a.SetTag("operation.type", "execute_statement");
                a.SetTag("operation.is_internal", false);
                Thread.Sleep(5);
            });
            context.MergeFrom(executeActivity);

            // Act
            var proto = context.BuildProto();

            // Assert - Root-level fields
            Assert.NotNull(proto);
            Assert.Equal("sess-proto-1", proto.SessionId);
            Assert.Equal("stmt-proto-1", proto.SqlStatementId);
            Assert.Equal("pat", proto.AuthType);
            Assert.True(proto.OperationLatencyMs >= 0);

            // System configuration
            Assert.NotNull(proto.SystemConfiguration);
            Assert.Equal("0.23.0", proto.SystemConfiguration.DriverVersion);
            Assert.Equal("oss-adbc-dotnet", proto.SystemConfiguration.DriverName);
            Assert.Equal("Linux", proto.SystemConfiguration.OsName);

            // Connection parameters
            Assert.NotNull(proto.DriverConnectionParams);
            Assert.Equal("/sql/1.0/warehouses/abc", proto.DriverConnectionParams.HttpPath);
            Assert.Equal(DriverModeType.DriverModeThrift, proto.DriverConnectionParams.Mode);

            // SQL operation
            Assert.NotNull(proto.SqlOperation);
            Assert.Equal(StatementType.StatementQuery, proto.SqlOperation.StatementType);
            Assert.True(proto.SqlOperation.IsCompressed);
            Assert.Equal(ExecutionResultFormat.ExecutionResultExternalLinks, proto.SqlOperation.ExecutionResult);
            Assert.Equal(1, proto.SqlOperation.RetryCount);

            // Chunk details
            Assert.NotNull(proto.SqlOperation.ChunkDetails);
            Assert.Equal(20, proto.SqlOperation.ChunkDetails.TotalChunksPresent);
            Assert.Equal(18, proto.SqlOperation.ChunkDetails.TotalChunksIterated);
            Assert.Equal(8000L, proto.SqlOperation.ChunkDetails.SumChunksDownloadTimeMillis);
            Assert.Equal(75L, proto.SqlOperation.ChunkDetails.InitialChunkLatencyMillis);
            Assert.Equal(350L, proto.SqlOperation.ChunkDetails.SlowestChunkLatencyMillis);

            // Result latency
            Assert.NotNull(proto.SqlOperation.ResultLatency);
            Assert.Equal(250L, proto.SqlOperation.ResultLatency.ResultSetReadyLatencyMillis);
            Assert.Equal(1000L, proto.SqlOperation.ResultLatency.ResultSetConsumptionLatencyMillis);

            // Operation detail
            Assert.NotNull(proto.SqlOperation.OperationDetail);
            Assert.Equal(4, proto.SqlOperation.OperationDetail.NOperationStatusCalls);
            Assert.Equal(2000L, proto.SqlOperation.OperationDetail.OperationStatusLatencyMillis);
            Assert.Equal(OperationType.OperationExecuteStatement, proto.SqlOperation.OperationDetail.OperationType);
            Assert.False(proto.SqlOperation.OperationDetail.IsInternalCall);

            // No error
            Assert.Null(proto.ErrorInfo);
        }

        [Fact]
        public void BuildProto_MissingFields_DefaultToEmptyOrZero()
        {
            // Arrange - Minimal context with no merged activities
            var context = new StatementTelemetryContext();

            // Act
            var proto = context.BuildProto();

            // Assert - All string fields default to empty string
            Assert.Equal(string.Empty, proto.SessionId);
            Assert.Equal(string.Empty, proto.SqlStatementId);
            Assert.Equal(string.Empty, proto.AuthType);

            // Numeric fields default to 0
            Assert.Equal(0L, proto.OperationLatencyMs);

            // Nested configs are null (not set on proto)
            Assert.Null(proto.SystemConfiguration);
            Assert.Null(proto.DriverConnectionParams);

            // SQL operation is present with defaults
            Assert.NotNull(proto.SqlOperation);
            Assert.Equal(StatementType.Unspecified, proto.SqlOperation.StatementType);
            Assert.False(proto.SqlOperation.IsCompressed);
            Assert.Equal(ExecutionResultFormat.Unspecified, proto.SqlOperation.ExecutionResult);
            Assert.Equal(0, proto.SqlOperation.RetryCount);

            // Chunk details all zeros
            Assert.NotNull(proto.SqlOperation.ChunkDetails);
            Assert.Equal(0, proto.SqlOperation.ChunkDetails.TotalChunksPresent);
            Assert.Equal(0, proto.SqlOperation.ChunkDetails.TotalChunksIterated);
            Assert.Equal(0L, proto.SqlOperation.ChunkDetails.SumChunksDownloadTimeMillis);
            Assert.Equal(0L, proto.SqlOperation.ChunkDetails.InitialChunkLatencyMillis);
            Assert.Equal(0L, proto.SqlOperation.ChunkDetails.SlowestChunkLatencyMillis);

            // Result latency all zeros
            Assert.NotNull(proto.SqlOperation.ResultLatency);
            Assert.Equal(0L, proto.SqlOperation.ResultLatency.ResultSetReadyLatencyMillis);
            Assert.Equal(0L, proto.SqlOperation.ResultLatency.ResultSetConsumptionLatencyMillis);

            // Operation detail all defaults
            Assert.NotNull(proto.SqlOperation.OperationDetail);
            Assert.Equal(0, proto.SqlOperation.OperationDetail.NOperationStatusCalls);
            Assert.Equal(0L, proto.SqlOperation.OperationDetail.OperationStatusLatencyMillis);
            Assert.Equal(OperationType.Unspecified, proto.SqlOperation.OperationDetail.OperationType);
            Assert.False(proto.SqlOperation.OperationDetail.IsInternalCall);

            // No error info
            Assert.Null(proto.ErrorInfo);
        }

        [Fact]
        public void BuildProto_WithError_IncludesErrorInfo()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("ExecuteQuery", a =>
            {
                a.SetTag("statement.id", "stmt-err-proto");
                a.SetTag("statement.type", "query");
                a.SetTag("error.type", "DatabaseException");
                a.SetStatus(ActivityStatusCode.Error, "Table not found");
            });
            context.MergeFrom(activity);

            // Act
            var proto = context.BuildProto();

            // Assert
            Assert.NotNull(proto.ErrorInfo);
            Assert.Equal("DatabaseException", proto.ErrorInfo.ErrorName);
            Assert.Equal("Table not found", proto.ErrorInfo.StackTrace);
        }

        [Fact]
        public void BuildProto_WithLongErrorMessage_Truncates()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var longMessage = new string('x', 300); // Exceeds 200 char limit
            var activity = CreateAndStopActivity("ExecuteQuery", a =>
            {
                a.SetTag("statement.id", "stmt-err-long");
                a.SetTag("error.type", "LongException");
                a.SetStatus(ActivityStatusCode.Error, longMessage);
            });
            context.MergeFrom(activity);

            // Act
            var proto = context.BuildProto();

            // Assert
            Assert.NotNull(proto.ErrorInfo);
            Assert.Equal(StatementTelemetryContext.MaxErrorMessageLength, proto.ErrorInfo.StackTrace.Length);
            Assert.Equal(200, proto.ErrorInfo.StackTrace.Length);
        }

        #endregion

        #region Parse Helper Tests

        [Fact]
        public void ParseStatementType_ValidValues_ParsesCorrectly()
        {
            Assert.Equal(StatementType.StatementQuery, StatementTelemetryContext.ParseStatementType("query"));
            Assert.Equal(StatementType.StatementSql, StatementTelemetryContext.ParseStatementType("sql"));
            Assert.Equal(StatementType.StatementUpdate, StatementTelemetryContext.ParseStatementType("update"));
            Assert.Equal(StatementType.StatementMetadata, StatementTelemetryContext.ParseStatementType("metadata"));
            Assert.Equal(StatementType.StatementVolume, StatementTelemetryContext.ParseStatementType("volume"));
        }

        [Fact]
        public void ParseStatementType_CaseInsensitive()
        {
            Assert.Equal(StatementType.StatementQuery, StatementTelemetryContext.ParseStatementType("QUERY"));
            Assert.Equal(StatementType.StatementUpdate, StatementTelemetryContext.ParseStatementType("Update"));
        }

        [Fact]
        public void ParseStatementType_Unknown_ReturnsUnspecified()
        {
            Assert.Equal(StatementType.Unspecified, StatementTelemetryContext.ParseStatementType("unknown"));
            Assert.Equal(StatementType.Unspecified, StatementTelemetryContext.ParseStatementType(null));
            Assert.Equal(StatementType.Unspecified, StatementTelemetryContext.ParseStatementType(""));
        }

        [Fact]
        public void ParseOperationType_ValidValues_ParsesCorrectly()
        {
            Assert.Equal(OperationType.OperationExecuteStatement,
                StatementTelemetryContext.ParseOperationType("execute_statement"));
            Assert.Equal(OperationType.OperationListCatalogs,
                StatementTelemetryContext.ParseOperationType("list_catalogs"));
            Assert.Equal(OperationType.OperationListTables,
                StatementTelemetryContext.ParseOperationType("list_tables"));
        }

        [Fact]
        public void ParseOperationType_Unknown_ReturnsUnspecified()
        {
            Assert.Equal(OperationType.Unspecified,
                StatementTelemetryContext.ParseOperationType("unknown"));
            Assert.Equal(OperationType.Unspecified,
                StatementTelemetryContext.ParseOperationType(null));
        }

        [Fact]
        public void ParseExecutionResult_ValidValues_ParsesCorrectly()
        {
            Assert.Equal(ExecutionResultFormat.ExecutionResultExternalLinks,
                StatementTelemetryContext.ParseExecutionResult("cloudfetch"));
            Assert.Equal(ExecutionResultFormat.ExecutionResultExternalLinks,
                StatementTelemetryContext.ParseExecutionResult("external_links"));
            Assert.Equal(ExecutionResultFormat.ExecutionResultInlineArrow,
                StatementTelemetryContext.ParseExecutionResult("arrow"));
            Assert.Equal(ExecutionResultFormat.ExecutionResultInlineArrow,
                StatementTelemetryContext.ParseExecutionResult("inline_arrow"));
            Assert.Equal(ExecutionResultFormat.ExecutionResultInlineJson,
                StatementTelemetryContext.ParseExecutionResult("json"));
            Assert.Equal(ExecutionResultFormat.ExecutionResultInlineJson,
                StatementTelemetryContext.ParseExecutionResult("inline_json"));
            Assert.Equal(ExecutionResultFormat.ExecutionResultColumnarInline,
                StatementTelemetryContext.ParseExecutionResult("columnar"));
            Assert.Equal(ExecutionResultFormat.ExecutionResultColumnarInline,
                StatementTelemetryContext.ParseExecutionResult("columnar_inline"));
        }

        [Fact]
        public void ParseExecutionResult_Unknown_ReturnsUnspecified()
        {
            Assert.Equal(ExecutionResultFormat.Unspecified,
                StatementTelemetryContext.ParseExecutionResult("unknown"));
            Assert.Equal(ExecutionResultFormat.Unspecified,
                StatementTelemetryContext.ParseExecutionResult(null));
        }

        [Fact]
        public void TruncateMessage_ShortMessage_ReturnsAsIs()
        {
            Assert.Equal("short", StatementTelemetryContext.TruncateMessage("short", 200));
        }

        [Fact]
        public void TruncateMessage_ExactLength_ReturnsAsIs()
        {
            var message = new string('a', 200);
            Assert.Equal(message, StatementTelemetryContext.TruncateMessage(message, 200));
        }

        [Fact]
        public void TruncateMessage_LongMessage_Truncates()
        {
            var message = new string('b', 300);
            var result = StatementTelemetryContext.TruncateMessage(message, 200);
            Assert.Equal(200, result.Length);
        }

        [Fact]
        public void TruncateMessage_NullOrEmpty_ReturnsEmpty()
        {
            Assert.Equal(string.Empty, StatementTelemetryContext.TruncateMessage(null, 200));
            Assert.Equal(string.Empty, StatementTelemetryContext.TruncateMessage("", 200));
        }

        #endregion

        #region Helper Methods

        /// <summary>
        /// Creates a real Activity with the given operation name, configures it,
        /// and stops it (populating Duration).
        /// </summary>
        private Activity CreateAndStopActivity(string operationName, Action<Activity>? configure = null)
        {
            var activity = TestSource.StartActivity(operationName);
            if (activity == null)
            {
                throw new InvalidOperationException(
                    $"Failed to create activity '{operationName}'. Ensure ActivityListener is registered.");
            }

            configure?.Invoke(activity);
            activity.Stop();
            return activity;
        }

        #endregion
    }
}
