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
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Proto;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Unit tests for <see cref="StatementTelemetryContext"/>.
    /// Tests MergeFrom(Activity) and BuildTelemetryLog() methods, as well
    /// as static helper parse methods and TruncateMessage.
    /// </summary>
    public class StatementTelemetryContextTests : IDisposable
    {
        private readonly ActivitySource _activitySource;
        private readonly ActivityListener _listener;

        public StatementTelemetryContextTests()
        {
            _activitySource = new ActivitySource("Test.StatementTelemetryContext");

            // Set up a listener to ensure activities are recorded
            _listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == "Test.StatementTelemetryContext",
                Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStarted = activity => { },
                ActivityStopped = activity => { }
            };
            ActivitySource.AddActivityListener(_listener);
        }

        public void Dispose()
        {
            _listener.Dispose();
            _activitySource.Dispose();
        }

        #region MergeFrom - ExecuteQuery Tests

        [Fact]
        public void MergeFrom_ExecuteQuery_CapturesLatencyAndStatementType()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext();
            using Activity? activity = _activitySource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(activity);
            activity!.SetTag("statement.type", "query");

            // Simulate some duration by stopping the activity
            System.Threading.Thread.Sleep(10);
            activity.Stop();

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.NotNull(context.TotalLatencyMs);
            Assert.True(context.TotalLatencyMs > 0, "TotalLatencyMs should be positive from activity.Duration");
            Assert.Equal(StatementType.StatementQuery, context.StatementTypeValue);
        }

        [Fact]
        public void MergeFrom_ExecuteQuery_CapturesSessionAndStatementId()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext();
            using Activity? activity = _activitySource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(activity);
            activity!.SetTag("session.id", "test-session-123");
            activity.SetTag("statement.id", "test-statement-456");
            activity.Stop();

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal("test-session-123", context.SessionId);
            Assert.Equal("test-statement-456", context.StatementId);
        }

        [Fact]
        public void MergeFrom_ExecuteQuery_DefaultsToQueryType()
        {
            // Arrange - activity has ExecuteQuery in name but no explicit statement.type tag
            StatementTelemetryContext context = new StatementTelemetryContext();
            using Activity? activity = _activitySource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(activity);
            activity!.Stop();

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal(StatementType.StatementQuery, context.StatementTypeValue);
        }

        [Fact]
        public void MergeFrom_ExecuteUpdate_DefaultsToUpdateType()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext();
            using Activity? activity = _activitySource.StartActivity("Statement.ExecuteUpdate");
            Assert.NotNull(activity);
            activity!.Stop();

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal(StatementType.StatementUpdate, context.StatementTypeValue);
        }

        [Fact]
        public void MergeFrom_ExecuteQuery_CapturesResultFormat()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext();
            using Activity? activity = _activitySource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(activity);
            activity!.SetTag("result.format", "cloudfetch");
            activity.Stop();

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal(ExecutionResultFormat.ExecutionResultExternalLinks, context.ResultFormat);
        }

        [Fact]
        public void MergeFrom_ExecuteQuery_CapturesCompression()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext();
            using Activity? activity = _activitySource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(activity);
            activity!.SetTag("result.compression_enabled", true);
            activity.Stop();

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.True(context.CompressionEnabled);
        }

        [Fact]
        public void MergeFrom_ExecuteQuery_CapturesRetryCount()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext();
            using Activity? activity = _activitySource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(activity);
            activity!.SetTag("retry.count", 3L);
            activity.Stop();

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal(3L, context.RetryCount);
        }

        [Fact]
        public void MergeFrom_ExecuteQuery_CapturesResultLatency()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext();
            using Activity? activity = _activitySource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(activity);
            activity!.SetTag("result.ready_latency_ms", 100L);
            activity.SetTag("result.consumption_latency_ms", 1400L);
            activity.Stop();

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal(100L, context.ResultReadyLatencyMs);
            Assert.Equal(1400L, context.ResultConsumptionLatencyMs);
        }

        [Fact]
        public void MergeFrom_ExecuteQuery_CapturesAuthType()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext();
            using Activity? activity = _activitySource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(activity);
            activity!.SetTag("auth.type", "oauth-m2m");
            activity.Stop();

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal("oauth-m2m", context.AuthType);
        }

        #endregion

        #region MergeFrom - DownloadFiles Tests

        [Fact]
        public void MergeFrom_DownloadFiles_CapturesChunkDetailsFromTags()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext();
            using Activity? activity = _activitySource.StartActivity("CloudFetch.DownloadFiles");
            Assert.NotNull(activity);
            activity!.SetTag("cloudfetch.total_chunks", 10);
            activity.SetTag("cloudfetch.chunks_iterated", 8);
            activity.SetTag("cloudfetch.initial_chunk_latency_ms", 50L);
            activity.SetTag("cloudfetch.slowest_chunk_latency_ms", 200L);
            activity.SetTag("cloudfetch.sum_download_time_ms", 1000L);
            activity.Stop();

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal(10, context.TotalChunksPresent);
            Assert.Equal(8, context.TotalChunksIterated);
            Assert.Equal(50L, context.InitialChunkLatencyMs);
            Assert.Equal(200L, context.SlowestChunkLatencyMs);
            Assert.Equal(1000L, context.SumChunksDownloadTimeMs);
        }

        [Fact]
        public void MergeFrom_DownloadFiles_CapturesChunkDetailsFromEvents()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext();
            using Activity? activity = _activitySource.StartActivity("CloudFetch.DownloadFiles");
            Assert.NotNull(activity);

            // Add a cloudfetch.download_summary event (matching what CloudFetchDownloader emits)
            activity!.AddEvent(new ActivityEvent("cloudfetch.download_summary",
                tags: new ActivityTagsCollection
                {
                    { "total_files", 15 },
                    { "successful_downloads", 12 },
                    { "total_time_ms", 2500L }
                }));
            activity.Stop();

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal(15, context.TotalChunksPresent);
            Assert.Equal(12, context.TotalChunksIterated);
            Assert.Equal(2500L, context.SumChunksDownloadTimeMs);
        }

        [Fact]
        public void MergeFrom_DownloadFiles_TagsTakePrecedenceOverEvents()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext();
            using Activity? activity = _activitySource.StartActivity("CloudFetch.DownloadFiles");
            Assert.NotNull(activity);

            // Set tags first
            activity!.SetTag("cloudfetch.total_chunks", 20);

            // Also add an event
            activity.AddEvent(new ActivityEvent("cloudfetch.download_summary",
                tags: new ActivityTagsCollection
                {
                    { "total_files", 15 }
                }));
            activity.Stop();

            // Act
            context.MergeFrom(activity);

            // Assert - tags should take precedence (event only fills null values)
            Assert.Equal(20, context.TotalChunksPresent);
        }

        [Fact]
        public void MergeFrom_DownloadFiles_CapturesIdentifiersAlso()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext();
            using Activity? activity = _activitySource.StartActivity("CloudFetch.DownloadFiles");
            Assert.NotNull(activity);
            activity!.SetTag("session.id", "session-from-download");
            activity.SetTag("statement.id", "stmt-from-download");
            activity.Stop();

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal("session-from-download", context.SessionId);
            Assert.Equal("stmt-from-download", context.StatementId);
        }

        #endregion

        #region MergeFrom - PollOperationStatus Tests

        [Fact]
        public void MergeFrom_PollOperationStatus_CapturesPollMetrics()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext();
            using Activity? activity = _activitySource.StartActivity("Statement.PollOperationStatus");
            Assert.NotNull(activity);
            activity!.SetTag("poll.count", 5);
            activity.SetTag("poll.latency_ms", 250L);
            activity.Stop();

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal(5, context.PollCount);
            Assert.Equal(250L, context.PollLatencyMs);
        }

        [Fact]
        public void MergeFrom_GetOperationStatus_CapturesPollMetrics()
        {
            // Arrange - alternate operation name
            StatementTelemetryContext context = new StatementTelemetryContext();
            using Activity? activity = _activitySource.StartActivity("Statement.GetOperationStatus");
            Assert.NotNull(activity);
            activity!.SetTag("poll.count", 3);
            activity.SetTag("poll.latency_ms", 150L);
            activity.Stop();

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal(3, context.PollCount);
            Assert.Equal(150L, context.PollLatencyMs);
        }

        #endregion

        #region MergeFrom - Error Tests

        [Fact]
        public void MergeFrom_ErrorActivity_CapturesErrorInfo()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext();
            using Activity? activity = _activitySource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(activity);
            activity!.SetStatus(ActivityStatusCode.Error, "Connection timed out");
            activity.SetTag("error.type", "TimeoutException");
            activity.SetTag("error.message", "The operation has timed out");
            activity.Stop();

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.True(context.HasError);
            Assert.Equal("TimeoutException", context.ErrorName);
            Assert.Equal("The operation has timed out", context.ErrorMessage);
        }

        [Fact]
        public void MergeFrom_ErrorActivity_UsesStatusDescriptionWhenNoErrorMessageTag()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext();
            using Activity? activity = _activitySource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(activity);
            activity!.SetStatus(ActivityStatusCode.Error, "Something went wrong");
            activity.SetTag("error.type", "GenericError");
            activity.Stop();

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.True(context.HasError);
            Assert.Equal("GenericError", context.ErrorName);
            Assert.Equal("Something went wrong", context.ErrorMessage);
        }

        [Fact]
        public void MergeFrom_OkActivity_DoesNotSetError()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext();
            using Activity? activity = _activitySource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(activity);
            activity!.SetStatus(ActivityStatusCode.Ok);
            activity.Stop();

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.False(context.HasError);
            Assert.Null(context.ErrorName);
            Assert.Null(context.ErrorMessage);
        }

        #endregion

        #region MergeFrom - Multiple Activities Tests

        [Fact]
        public void MergeFrom_MultipleActivities_AggregatesCorrectly()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext();

            // Root activity (ExecuteQuery)
            using Activity? rootActivity = _activitySource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(rootActivity);
            rootActivity!.SetTag("session.id", "session-123");
            rootActivity.SetTag("statement.id", "stmt-456");
            rootActivity.SetTag("auth.type", "pat");
            rootActivity.SetTag("result.format", "cloudfetch");
            rootActivity.SetTag("result.compression_enabled", true);
            System.Threading.Thread.Sleep(10);
            rootActivity.Stop();

            // Child activity (PollOperationStatus)
            using Activity? pollActivity = _activitySource.StartActivity("Statement.PollOperationStatus");
            Assert.NotNull(pollActivity);
            pollActivity!.SetTag("session.id", "session-123");
            pollActivity.SetTag("statement.id", "stmt-456");
            pollActivity.SetTag("poll.count", 5);
            pollActivity.SetTag("poll.latency_ms", 250L);
            pollActivity.Stop();

            // Child activity (DownloadFiles)
            using Activity? downloadActivity = _activitySource.StartActivity("CloudFetch.DownloadFiles");
            Assert.NotNull(downloadActivity);
            downloadActivity!.SetTag("session.id", "session-123");
            downloadActivity.SetTag("statement.id", "stmt-456");
            downloadActivity.SetTag("cloudfetch.total_chunks", 10);
            downloadActivity.SetTag("cloudfetch.chunks_iterated", 10);
            downloadActivity.SetTag("cloudfetch.initial_chunk_latency_ms", 50L);
            downloadActivity.SetTag("cloudfetch.slowest_chunk_latency_ms", 200L);
            downloadActivity.SetTag("cloudfetch.sum_download_time_ms", 1000L);
            downloadActivity.Stop();

            // Act - merge all activities
            context.MergeFrom(rootActivity);
            context.MergeFrom(pollActivity);
            context.MergeFrom(downloadActivity);

            // Assert - all fields populated
            Assert.Equal("session-123", context.SessionId);
            Assert.Equal("stmt-456", context.StatementId);
            Assert.Equal("pat", context.AuthType);

            // Statement execution
            Assert.NotNull(context.TotalLatencyMs);
            Assert.True(context.TotalLatencyMs > 0);
            Assert.Equal(ExecutionResultFormat.ExecutionResultExternalLinks, context.ResultFormat);
            Assert.True(context.CompressionEnabled);

            // Polling
            Assert.Equal(5, context.PollCount);
            Assert.Equal(250L, context.PollLatencyMs);

            // Chunk details
            Assert.Equal(10, context.TotalChunksPresent);
            Assert.Equal(10, context.TotalChunksIterated);
            Assert.Equal(50L, context.InitialChunkLatencyMs);
            Assert.Equal(200L, context.SlowestChunkLatencyMs);
            Assert.Equal(1000L, context.SumChunksDownloadTimeMs);
        }

        [Fact]
        public void MergeFrom_NullActivity_DoesNotThrow()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext();

            // Act & Assert - should not throw
            context.MergeFrom(null!);
        }

        [Fact]
        public void MergeFrom_UnknownOperationName_CapturesIdentifiersOnly()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext();
            using Activity? activity = _activitySource.StartActivity("SomeUnknown.Operation");
            Assert.NotNull(activity);
            activity!.SetTag("session.id", "session-from-unknown");
            activity.SetTag("statement.id", "stmt-from-unknown");
            activity.Stop();

            // Act
            context.MergeFrom(activity);

            // Assert - identifiers captured, but no execution-specific data
            Assert.Equal("session-from-unknown", context.SessionId);
            Assert.Equal("stmt-from-unknown", context.StatementId);
            Assert.Null(context.TotalLatencyMs);
            Assert.Null(context.PollCount);
            Assert.Null(context.TotalChunksPresent);
        }

        #endregion

        #region BuildTelemetryLog Tests

        [Fact]
        public void BuildTelemetryLog_CreatesCompleteProto()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext
            {
                SessionId = "session-123",
                StatementId = "stmt-456",
                AuthType = "oauth-m2m",
                TotalLatencyMs = 1500,
                StatementTypeValue = StatementType.StatementQuery,
                OperationTypeValue = OperationType.OperationExecuteStatementAsync,
                ResultFormat = ExecutionResultFormat.ExecutionResultExternalLinks,
                CompressionEnabled = true,
                RetryCount = 0,
                ResultReadyLatencyMs = 100,
                ResultConsumptionLatencyMs = 1400,
                TotalChunksPresent = 10,
                TotalChunksIterated = 10,
                InitialChunkLatencyMs = 50,
                SlowestChunkLatencyMs = 200,
                SumChunksDownloadTimeMs = 1000,
                PollCount = 5,
                PollLatencyMs = 250,
                SystemConfiguration = new DriverSystemConfiguration
                {
                    DriverName = "adbc-databricks",
                    DriverVersion = "1.0.0",
                    OsName = "Linux",
                    RuntimeName = ".NET",
                    RuntimeVersion = "8.0"
                },
                DriverConnectionParams = new DriverConnectionParameters
                {
                    Mode = DriverModeType.DriverModeThrift,
                    AuthMech = DriverAuthMechType.DriverAuthMechOauth
                }
            };

            // Act
            OssSqlDriverTelemetryLog log = context.BuildTelemetryLog();

            // Assert - top-level fields
            Assert.Equal("session-123", log.SessionId);
            Assert.Equal("stmt-456", log.SqlStatementId);
            Assert.Equal("oauth-m2m", log.AuthType);
            Assert.Equal(1500L, log.OperationLatencyMs);

            // System configuration
            Assert.NotNull(log.SystemConfiguration);
            Assert.Equal("adbc-databricks", log.SystemConfiguration.DriverName);
            Assert.Equal("1.0.0", log.SystemConfiguration.DriverVersion);

            // Driver connection parameters
            Assert.NotNull(log.DriverConnectionParams);
            Assert.Equal(DriverModeType.DriverModeThrift, log.DriverConnectionParams.Mode);
            Assert.Equal(DriverAuthMechType.DriverAuthMechOauth, log.DriverConnectionParams.AuthMech);

            // SQL operation
            Assert.NotNull(log.SqlOperation);
            Assert.Equal(StatementType.StatementQuery, log.SqlOperation.StatementType);
            Assert.True(log.SqlOperation.IsCompressed);
            Assert.Equal(ExecutionResultFormat.ExecutionResultExternalLinks, log.SqlOperation.ExecutionResult);
            Assert.Equal(0L, log.SqlOperation.RetryCount);

            // Chunk details
            Assert.NotNull(log.SqlOperation.ChunkDetails);
            Assert.Equal(10, log.SqlOperation.ChunkDetails.TotalChunksPresent);
            Assert.Equal(10, log.SqlOperation.ChunkDetails.TotalChunksIterated);
            Assert.Equal(50L, log.SqlOperation.ChunkDetails.InitialChunkLatencyMillis);
            Assert.Equal(200L, log.SqlOperation.ChunkDetails.SlowestChunkLatencyMillis);
            Assert.Equal(1000L, log.SqlOperation.ChunkDetails.SumChunksDownloadTimeMillis);

            // Result latency
            Assert.NotNull(log.SqlOperation.ResultLatency);
            Assert.Equal(100L, log.SqlOperation.ResultLatency.ResultSetReadyLatencyMillis);
            Assert.Equal(1400L, log.SqlOperation.ResultLatency.ResultSetConsumptionLatencyMillis);

            // Operation detail
            Assert.NotNull(log.SqlOperation.OperationDetail);
            Assert.Equal(5, log.SqlOperation.OperationDetail.NOperationStatusCalls);
            Assert.Equal(250L, log.SqlOperation.OperationDetail.OperationStatusLatencyMillis);
            Assert.Equal(OperationType.OperationExecuteStatementAsync, log.SqlOperation.OperationDetail.OperationType);
        }

        [Fact]
        public void BuildTelemetryLog_WithError_IncludesDriverErrorInfo()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext
            {
                SessionId = "session-123",
                StatementId = "stmt-456",
                HasError = true,
                ErrorName = "TimeoutException",
                ErrorMessage = "The operation has timed out"
            };

            // Act
            OssSqlDriverTelemetryLog log = context.BuildTelemetryLog();

            // Assert
            Assert.NotNull(log.ErrorInfo);
            Assert.Equal("TimeoutException", log.ErrorInfo.ErrorName);
            Assert.Equal("The operation has timed out", log.ErrorInfo.StackTrace);
        }

        [Fact]
        public void BuildTelemetryLog_WithChunkDetails_IncludesCloudFetchMetrics()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext
            {
                SessionId = "session-123",
                StatementId = "stmt-456",
                TotalChunksPresent = 15,
                TotalChunksIterated = 12,
                SumChunksDownloadTimeMs = 2500,
                InitialChunkLatencyMs = 30,
                SlowestChunkLatencyMs = 500
            };

            // Act
            OssSqlDriverTelemetryLog log = context.BuildTelemetryLog();

            // Assert
            Assert.NotNull(log.SqlOperation);
            Assert.NotNull(log.SqlOperation.ChunkDetails);
            Assert.Equal(15, log.SqlOperation.ChunkDetails.TotalChunksPresent);
            Assert.Equal(12, log.SqlOperation.ChunkDetails.TotalChunksIterated);
            Assert.Equal(2500L, log.SqlOperation.ChunkDetails.SumChunksDownloadTimeMillis);
            Assert.Equal(30L, log.SqlOperation.ChunkDetails.InitialChunkLatencyMillis);
            Assert.Equal(500L, log.SqlOperation.ChunkDetails.SlowestChunkLatencyMillis);
        }

        [Fact]
        public void BuildTelemetryLog_EmptyContext_ReturnsMinimalProto()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext();

            // Act
            OssSqlDriverTelemetryLog log = context.BuildTelemetryLog();

            // Assert - minimal fields set with defaults
            Assert.Equal(string.Empty, log.SessionId);
            Assert.Equal(string.Empty, log.SqlStatementId);
            Assert.Equal(string.Empty, log.AuthType);
            Assert.Equal(0L, log.OperationLatencyMs);
            Assert.Null(log.SystemConfiguration);
            Assert.Null(log.DriverConnectionParams);
            Assert.Null(log.SqlOperation);
            Assert.Null(log.ErrorInfo);
        }

        [Fact]
        public void BuildTelemetryLog_NoError_DoesNotIncludeErrorInfo()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext
            {
                SessionId = "session-123",
                StatementId = "stmt-456",
                HasError = false
            };

            // Act
            OssSqlDriverTelemetryLog log = context.BuildTelemetryLog();

            // Assert
            Assert.Null(log.ErrorInfo);
        }

        [Fact]
        public void BuildTelemetryLog_OnlyPollingMetrics_CreatesOperationDetail()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext
            {
                PollCount = 3,
                PollLatencyMs = 150
            };

            // Act
            OssSqlDriverTelemetryLog log = context.BuildTelemetryLog();

            // Assert
            Assert.NotNull(log.SqlOperation);
            Assert.NotNull(log.SqlOperation.OperationDetail);
            Assert.Equal(3, log.SqlOperation.OperationDetail.NOperationStatusCalls);
            Assert.Equal(150L, log.SqlOperation.OperationDetail.OperationStatusLatencyMillis);
        }

        #endregion

        #region ParseExecutionResult Tests

        [Theory]
        [InlineData("cloudfetch", ExecutionResultFormat.ExecutionResultExternalLinks)]
        [InlineData("cloud_fetch", ExecutionResultFormat.ExecutionResultExternalLinks)]
        [InlineData("external_links", ExecutionResultFormat.ExecutionResultExternalLinks)]
        [InlineData("inline_arrow", ExecutionResultFormat.ExecutionResultInlineArrow)]
        [InlineData("inline-arrow", ExecutionResultFormat.ExecutionResultInlineArrow)]
        [InlineData("inlinearrow", ExecutionResultFormat.ExecutionResultInlineArrow)]
        [InlineData("inline_json", ExecutionResultFormat.ExecutionResultInlineJson)]
        [InlineData("columnar_inline", ExecutionResultFormat.ExecutionResultColumnarInline)]
        [InlineData("", ExecutionResultFormat.Unspecified)]
        [InlineData(null, ExecutionResultFormat.Unspecified)]
        [InlineData("unknown", ExecutionResultFormat.Unspecified)]
        public void ParseExecutionResult_MapsCorrectly(string? input, ExecutionResultFormat expected)
        {
            ExecutionResultFormat result = StatementTelemetryContext.ParseExecutionResult(input);
            Assert.Equal(expected, result);
        }

        [Fact]
        public void ParseExecutionResult_CaseInsensitive()
        {
            Assert.Equal(ExecutionResultFormat.ExecutionResultExternalLinks,
                StatementTelemetryContext.ParseExecutionResult("CloudFetch"));
            Assert.Equal(ExecutionResultFormat.ExecutionResultInlineArrow,
                StatementTelemetryContext.ParseExecutionResult("INLINE_ARROW"));
        }

        #endregion

        #region ParseDriverMode Tests

        [Theory]
        [InlineData("thrift", DriverModeType.DriverModeThrift)]
        [InlineData("sea", DriverModeType.DriverModeSea)]
        [InlineData("", DriverModeType.Unspecified)]
        [InlineData(null, DriverModeType.Unspecified)]
        [InlineData("unknown", DriverModeType.Unspecified)]
        public void ParseDriverMode_MapsCorrectly(string? input, DriverModeType expected)
        {
            DriverModeType result = StatementTelemetryContext.ParseDriverMode(input);
            Assert.Equal(expected, result);
        }

        #endregion

        #region ParseAuthMech Tests

        [Theory]
        [InlineData("pat", DriverAuthMechType.DriverAuthMechPat)]
        [InlineData("token", DriverAuthMechType.DriverAuthMechPat)]
        [InlineData("oauth", DriverAuthMechType.DriverAuthMechOauth)]
        [InlineData("oauth2", DriverAuthMechType.DriverAuthMechOauth)]
        [InlineData("other", DriverAuthMechType.DriverAuthMechOther)]
        [InlineData("", DriverAuthMechType.Unspecified)]
        [InlineData(null, DriverAuthMechType.Unspecified)]
        [InlineData("unknown", DriverAuthMechType.Unspecified)]
        public void ParseAuthMech_MapsCorrectly(string? input, DriverAuthMechType expected)
        {
            DriverAuthMechType result = StatementTelemetryContext.ParseAuthMech(input);
            Assert.Equal(expected, result);
        }

        #endregion

        #region ParseAuthFlow Tests

        [Theory]
        [InlineData("client_credentials", DriverAuthFlowType.DriverAuthFlowClientCredentials)]
        [InlineData("m2m", DriverAuthFlowType.DriverAuthFlowClientCredentials)]
        [InlineData("token_passthrough", DriverAuthFlowType.DriverAuthFlowTokenPassthrough)]
        [InlineData("token", DriverAuthFlowType.DriverAuthFlowTokenPassthrough)]
        [InlineData("browser", DriverAuthFlowType.DriverAuthFlowBrowserBasedAuthentication)]
        [InlineData("browser_based", DriverAuthFlowType.DriverAuthFlowBrowserBasedAuthentication)]
        [InlineData("", DriverAuthFlowType.Unspecified)]
        [InlineData(null, DriverAuthFlowType.Unspecified)]
        [InlineData("unknown", DriverAuthFlowType.Unspecified)]
        public void ParseAuthFlow_MapsCorrectly(string? input, DriverAuthFlowType expected)
        {
            DriverAuthFlowType result = StatementTelemetryContext.ParseAuthFlow(input);
            Assert.Equal(expected, result);
        }

        #endregion

        #region ParseStatementType Tests

        [Theory]
        [InlineData("query", StatementType.StatementQuery)]
        [InlineData("sql", StatementType.StatementSql)]
        [InlineData("update", StatementType.StatementUpdate)]
        [InlineData("metadata", StatementType.StatementMetadata)]
        [InlineData("volume", StatementType.StatementVolume)]
        [InlineData("", StatementType.Unspecified)]
        [InlineData(null, StatementType.Unspecified)]
        [InlineData("unknown", StatementType.Unspecified)]
        public void ParseStatementType_MapsCorrectly(string? input, StatementType expected)
        {
            StatementType result = StatementTelemetryContext.ParseStatementType(input);
            Assert.Equal(expected, result);
        }

        #endregion

        #region ParseOperationType Tests

        [Theory]
        [InlineData("execute_statement", OperationType.OperationExecuteStatement)]
        [InlineData("execute_statement_async", OperationType.OperationExecuteStatementAsync)]
        [InlineData("list_catalogs", OperationType.OperationListCatalogs)]
        [InlineData("list_schemas", OperationType.OperationListSchemas)]
        [InlineData("list_tables", OperationType.OperationListTables)]
        [InlineData("create_session", OperationType.OperationCreateSession)]
        [InlineData("", OperationType.Unspecified)]
        [InlineData(null, OperationType.Unspecified)]
        [InlineData("unknown", OperationType.Unspecified)]
        public void ParseOperationType_MapsCorrectly(string? input, OperationType expected)
        {
            OperationType result = StatementTelemetryContext.ParseOperationType(input);
            Assert.Equal(expected, result);
        }

        #endregion

        #region TruncateMessage Tests

        [Fact]
        public void TruncateMessage_LongMessage_Truncated()
        {
            // Arrange
            string longMessage = new string('x', 300);

            // Act
            string result = StatementTelemetryContext.TruncateMessage(longMessage);

            // Assert
            Assert.Equal(200, result.Length);
        }

        [Fact]
        public void TruncateMessage_ShortMessage_NotTruncated()
        {
            // Arrange
            string shortMessage = "Short error message";

            // Act
            string result = StatementTelemetryContext.TruncateMessage(shortMessage);

            // Assert
            Assert.Equal(shortMessage, result);
        }

        [Fact]
        public void TruncateMessage_ExactlyMaxLength_NotTruncated()
        {
            // Arrange
            string exactMessage = new string('x', StatementTelemetryContext.MaxErrorMessageLength);

            // Act
            string result = StatementTelemetryContext.TruncateMessage(exactMessage);

            // Assert
            Assert.Equal(StatementTelemetryContext.MaxErrorMessageLength, result.Length);
        }

        [Fact]
        public void TruncateMessage_NullMessage_ReturnsEmpty()
        {
            string result = StatementTelemetryContext.TruncateMessage(null);
            Assert.Equal(string.Empty, result);
        }

        [Fact]
        public void TruncateMessage_EmptyMessage_ReturnsEmpty()
        {
            string result = StatementTelemetryContext.TruncateMessage(string.Empty);
            Assert.Equal(string.Empty, result);
        }

        #endregion

        #region String Parsing from Tags Tests

        [Fact]
        public void MergeFrom_StringTagValues_ParsedCorrectly()
        {
            // Arrange - tags set as strings instead of native types
            StatementTelemetryContext context = new StatementTelemetryContext();
            using Activity? activity = _activitySource.StartActivity("Statement.ExecuteQuery");
            Assert.NotNull(activity);
            activity!.SetTag("retry.count", "5");
            activity.SetTag("result.compression_enabled", "true");
            activity.SetTag("result.ready_latency_ms", "100");
            activity.SetTag("result.consumption_latency_ms", "1400");
            activity.Stop();

            // Act
            context.MergeFrom(activity);

            // Assert - string values should be parsed correctly
            Assert.Equal(5L, context.RetryCount);
            Assert.True(context.CompressionEnabled);
            Assert.Equal(100L, context.ResultReadyLatencyMs);
            Assert.Equal(1400L, context.ResultConsumptionLatencyMs);
        }

        [Fact]
        public void MergeFrom_DownloadFiles_StringTagValues_ParsedCorrectly()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext();
            using Activity? activity = _activitySource.StartActivity("CloudFetch.DownloadFiles");
            Assert.NotNull(activity);
            activity!.SetTag("cloudfetch.total_chunks", "10");
            activity.SetTag("cloudfetch.chunks_iterated", "8");
            activity.SetTag("cloudfetch.initial_chunk_latency_ms", "50");
            activity.SetTag("cloudfetch.slowest_chunk_latency_ms", "200");
            activity.SetTag("cloudfetch.sum_download_time_ms", "1000");
            activity.Stop();

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal(10, context.TotalChunksPresent);
            Assert.Equal(8, context.TotalChunksIterated);
            Assert.Equal(50L, context.InitialChunkLatencyMs);
            Assert.Equal(200L, context.SlowestChunkLatencyMs);
            Assert.Equal(1000L, context.SumChunksDownloadTimeMs);
        }

        [Fact]
        public void MergeFrom_PollOperationStatus_StringTagValues_ParsedCorrectly()
        {
            // Arrange
            StatementTelemetryContext context = new StatementTelemetryContext();
            using Activity? activity = _activitySource.StartActivity("Statement.PollOperationStatus");
            Assert.NotNull(activity);
            activity!.SetTag("poll.count", "3");
            activity.SetTag("poll.latency_ms", "150");
            activity.Stop();

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal(3, context.PollCount);
            Assert.Equal(150L, context.PollLatencyMs);
        }

        #endregion
    }
}
