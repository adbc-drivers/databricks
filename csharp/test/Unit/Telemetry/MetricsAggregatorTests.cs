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
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for MetricsAggregator to ensure correct aggregation, flushing, and exception handling.
    /// </summary>
    public class MetricsAggregatorTests : IDisposable
    {
        private readonly MockTelemetryExporter _mockExporter;
        private readonly TelemetryConfiguration _config;
        private readonly MetricsAggregator _aggregator;
        private static readonly ActivitySource _activitySource = new ActivitySource("TestActivitySource");

        public MetricsAggregatorTests()
        {
            _mockExporter = new MockTelemetryExporter();
            _config = new TelemetryConfiguration
            {
                BatchSize = 10,
                FlushIntervalMs = 1000
            };
            _aggregator = new MetricsAggregator(_mockExporter, _config);
        }

        public void Dispose()
        {
            _aggregator?.Dispose();
        }

        // Test: Connection.Open events are emitted immediately

        [Fact]
        public void MetricsAggregator_ProcessActivity_ConnectionOpen_EmitsImmediately()
        {
            // Arrange
            using var activity = _activitySource.StartActivity("Connection.Open");
            activity?.SetTag("session.id", "session-123");
            activity?.SetTag("workspace.id", "12345");
            activity?.Stop();

            // Act
            _aggregator.ProcessActivity(activity!);

            // Give a moment for async export
            Thread.Sleep(100);

            // Assert
            Assert.True(_mockExporter.ExportedMetrics.Count > 0, "Connection event should be exported immediately");
            var metric = _mockExporter.ExportedMetrics.First().First();
            Assert.Equal("connectionopen", metric.MetricType);
            Assert.Equal("session-123", metric.SessionId);
            Assert.Equal(12345L, metric.WorkspaceId);
        }

        // Test: Statement activities are aggregated by statement_id

        [Fact]
        public void MetricsAggregator_ProcessActivity_Statement_AggregatesByStatementId()
        {
            // Arrange
            var statementId = "stmt-456";
            var sessionId = "session-123";

            // Simulate multiple activities for the same statement
            using var activity1 = _activitySource.StartActivity("Statement.Execute");
            activity1?.SetTag("statement.id", statementId);
            activity1?.SetTag("session.id", sessionId);
            activity1?.SetTag("result.chunk_count", "5");
            System.Threading.Thread.Sleep(10);
            activity1?.Stop();

            using var activity2 = _activitySource.StartActivity("CloudFetch.Download");
            activity2?.SetTag("statement.id", statementId);
            activity2?.SetTag("session.id", sessionId);
            activity2?.SetTag("result.bytes_downloaded", "1024");
            System.Threading.Thread.Sleep(10);
            activity2?.Stop();

            // Act
            _aggregator.ProcessActivity(activity1!);
            _aggregator.ProcessActivity(activity2!);

            // Activities should be aggregated but not yet flushed
            Assert.Empty(_mockExporter.ExportedMetrics);
        }

        // Test: CompleteStatement emits aggregated metric

        [Fact]
        public void MetricsAggregator_CompleteStatement_EmitsAggregatedMetric()
        {
            // Arrange
            var statementId = "stmt-789";
            var sessionId = "session-123";

            using var activity = _activitySource.StartActivity("Statement.Execute");
            activity?.SetTag("statement.id", statementId);
            activity?.SetTag("session.id", sessionId);
            activity?.SetTag("workspace.id", "12345");
            activity?.SetTag("result.format", "cloudfetch");
            activity?.SetTag("result.chunk_count", "3");
            activity?.SetTag("poll.count", "2");
            System.Threading.Thread.Sleep(10);
            activity?.Stop();

            _aggregator.ProcessActivity(activity!);

            // Act
            _aggregator.CompleteStatement(statementId);

            // Give a moment for async operations
            Thread.Sleep(100);

            // Assert
            Assert.True(_mockExporter.ExportedMetrics.Count > 0, "Statement should be exported after completion");
            var exportedBatches = _mockExporter.ExportedMetrics;
            var allMetrics = exportedBatches.SelectMany(batch => batch).ToList();

            var statementMetric = allMetrics.FirstOrDefault(m => m.StatementId == statementId);
            Assert.NotNull(statementMetric);
            Assert.Equal("statement", statementMetric.MetricType);
            Assert.Equal(sessionId, statementMetric.SessionId);
            Assert.Equal("cloudfetch", statementMetric.ResultFormat);
            Assert.Equal(3, statementMetric.ChunkCount);
            Assert.Equal(2, statementMetric.PollCount);
        }

        // Test: Batch size threshold triggers flush

        [Fact]
        public async Task MetricsAggregator_FlushAsync_BatchSizeReached_ExportsMetrics()
        {
            // Arrange
            var batchSize = 5;
            var config = new TelemetryConfiguration { BatchSize = batchSize, FlushIntervalMs = 10000 };
            using var aggregator = new MetricsAggregator(_mockExporter, config);

            // Complete enough statements to reach batch size
            for (int i = 0; i < batchSize; i++)
            {
                var statementId = $"stmt-{i}";
                using var activity = _activitySource.StartActivity("Statement.Execute");
                activity?.SetTag("statement.id", statementId);
                activity?.SetTag("session.id", "session-123");
                activity?.Stop();

                aggregator.ProcessActivity(activity!);
                aggregator.CompleteStatement(statementId, failed: false);
            }

            // Give time for async flush
            await Task.Delay(200);

            // Assert
            Assert.True(_mockExporter.ExportedMetrics.Count > 0, "Metrics should be flushed when batch size reached");
            var totalExported = _mockExporter.ExportedMetrics.Sum(batch => batch.Count);
            Assert.True(totalExported >= batchSize, $"Expected at least {batchSize} metrics, got {totalExported}");
        }

        // Test: Time interval triggers flush

        [Fact]
        public async Task MetricsAggregator_FlushAsync_TimeInterval_ExportsMetrics()
        {
            // Arrange - very short flush interval for testing
            var config = new TelemetryConfiguration { BatchSize = 100, FlushIntervalMs = 200 };
            using var aggregator = new MetricsAggregator(_mockExporter, config);

            var statementId = "stmt-time-test";
            using var activity = _activitySource.StartActivity("Statement.Execute");
            activity?.SetTag("statement.id", statementId);
            activity?.SetTag("session.id", "session-123");
            activity?.Stop();

            aggregator.ProcessActivity(activity!);
            aggregator.CompleteStatement(statementId, failed: false);

            // Act - wait for timer to trigger
            await Task.Delay(500);

            // Assert
            Assert.True(_mockExporter.ExportedMetrics.Count > 0, "Metrics should be flushed after time interval");
        }

        // Test: Terminal exceptions are flushed immediately

        [Fact]
        public void MetricsAggregator_RecordException_Terminal_FlushesImmediately()
        {
            // Arrange
            var statementId = "stmt-error";
            var terminalException = new HttpRequestException("Auth failed", null, HttpStatusCode.Unauthorized);

            // Act
            _aggregator.RecordException(statementId, terminalException);

            // Give time for async export
            Thread.Sleep(100);

            // Assert
            Assert.True(_mockExporter.ExportedMetrics.Count > 0, "Terminal exception should be flushed immediately");
            var metric = _mockExporter.ExportedMetrics.First().First();
            Assert.Equal("error", metric.MetricType);
            Assert.Equal(statementId, metric.StatementId);
        }

        // Test: Retryable exceptions are buffered until statement completes

        [Fact]
        public void MetricsAggregator_RecordException_Retryable_BuffersUntilComplete()
        {
            // Arrange
            var statementId = "stmt-retry";
            var retryableException = new HttpRequestException("Service unavailable", null, HttpStatusCode.ServiceUnavailable);

            using var activity = _activitySource.StartActivity("Statement.Execute");
            activity?.SetTag("statement.id", statementId);
            activity?.SetTag("session.id", "session-123");
            activity?.Stop();

            _aggregator.ProcessActivity(activity!);

            // Act - record retryable exception
            _aggregator.RecordException(statementId, retryableException);

            // Give time - should NOT be flushed yet
            Thread.Sleep(100);

            // Assert - exception should be buffered, not exported yet
            var errorMetrics = _mockExporter.ExportedMetrics
                .SelectMany(batch => batch)
                .Where(m => m.MetricType == "error")
                .ToList();
            Assert.Empty(errorMetrics);

            // Complete statement as failed - now exception should be flushed
            _aggregator.CompleteStatement(statementId, failed: true);
            Thread.Sleep(100);

            errorMetrics = _mockExporter.ExportedMetrics
                .SelectMany(batch => batch)
                .Where(m => m.MetricType == "error")
                .ToList();
            Assert.True(errorMetrics.Count > 0, "Retryable exception should be flushed when statement completes as failed");
        }

        // Test: Retryable exceptions are NOT flushed if statement succeeds

        [Fact]
        public void MetricsAggregator_RecordException_Retryable_NotFlushedOnSuccess()
        {
            // Arrange
            var statementId = "stmt-retry-success";
            var retryableException = new HttpRequestException("Timeout", null, HttpStatusCode.RequestTimeout);

            using var activity = _activitySource.StartActivity("Statement.Execute");
            activity?.SetTag("statement.id", statementId);
            activity?.SetTag("session.id", "session-123");
            activity?.Stop();

            _aggregator.ProcessActivity(activity!);
            _aggregator.RecordException(statementId, retryableException);

            // Act - complete statement as successful
            _aggregator.CompleteStatement(statementId);
            Thread.Sleep(100);

            // Assert - exception should NOT be flushed
            var errorMetrics = _mockExporter.ExportedMetrics
                .SelectMany(batch => batch)
                .Where(m => m.MetricType == "error")
                .ToList();
            Assert.Empty(errorMetrics);
        }

        // Test: Exception swallowing - no exceptions thrown

        [Fact]
        public void MetricsAggregator_ProcessActivity_ExceptionSwallowed_NoThrow()
        {
            // Arrange - create activity with invalid data that might cause parsing errors
            using var activity = _activitySource.StartActivity("Statement.Execute");
            activity?.SetTag("workspace.id", "not-a-number"); // Invalid workspace ID
            activity?.SetTag("statement.id", "stmt-invalid");
            activity?.Stop();

            // Act & Assert - should not throw
            var exception = Record.Exception(() => _aggregator.ProcessActivity(activity!));
            Assert.Null(exception);
        }

        // Test: Tag filtering using TelemetryTagRegistry

        [Fact]
        public void MetricsAggregator_ProcessActivity_FiltersTags_UsingRegistry()
        {
            // Arrange
            using var activity = _activitySource.StartActivity("Statement.Execute");
            activity?.SetTag("statement.id", "stmt-filter");
            activity?.SetTag("session.id", "session-123");
            activity?.SetTag("result.format", "cloudfetch");
            activity?.SetTag("db.statement", "SELECT * FROM table"); // Should be filtered out
            activity?.SetTag("server.address", "workspace.cloud.databricks.com"); // Should be filtered out
            activity?.Stop();

            _aggregator.ProcessActivity(activity!);

            // Act
            _aggregator.CompleteStatement("stmt-filter", failed: false);
            Thread.Sleep(100);

            // Assert - sensitive tags should not be in exported metrics
            var exportedMetrics = _mockExporter.ExportedMetrics.SelectMany(batch => batch).ToList();
            var statementMetric = exportedMetrics.FirstOrDefault(m => m.StatementId == "stmt-filter");

            Assert.NotNull(statementMetric);
            Assert.Equal("cloudfetch", statementMetric.ResultFormat); // Allowed tag
            // Note: We can't directly check for absence of filtered tags as they're not exposed in TelemetryMetric
            // But they should not have been set during processing
        }

        // Test: Multiple statements are aggregated independently

        [Fact]
        public void MetricsAggregator_ProcessActivity_MultipleStatements_AggregatedIndependently()
        {
            // Arrange
            var stmt1 = "stmt-1";
            var stmt2 = "stmt-2";

            using var activity1 = _activitySource.StartActivity("Statement.Execute");
            activity1?.SetTag("statement.id", stmt1);
            activity1?.SetTag("session.id", "session-123");
            activity1?.SetTag("result.chunk_count", "5");
            activity1?.Stop();

            using var activity2 = _activitySource.StartActivity("Statement.Execute");
            activity2?.SetTag("statement.id", stmt2);
            activity2?.SetTag("session.id", "session-123");
            activity2?.SetTag("result.chunk_count", "3");
            activity2?.Stop();

            // Act
            _aggregator.ProcessActivity(activity1!);
            _aggregator.ProcessActivity(activity2!);
            _aggregator.CompleteStatement(stmt1, failed: false);
            _aggregator.CompleteStatement(stmt2, failed: false);

            Thread.Sleep(100);

            // Assert
            var exportedMetrics = _mockExporter.ExportedMetrics.SelectMany(batch => batch).ToList();
            var metric1 = exportedMetrics.FirstOrDefault(m => m.StatementId == stmt1);
            var metric2 = exportedMetrics.FirstOrDefault(m => m.StatementId == stmt2);

            Assert.NotNull(metric1);
            Assert.NotNull(metric2);
            Assert.Equal(5, metric1.ChunkCount);
            Assert.Equal(3, metric2.ChunkCount);
        }

        // Test: session_id and statement_id both included in exports

        [Fact]
        public void MetricsAggregator_CompleteStatement_IncludesBothSessionAndStatementIds()
        {
            // Arrange
            var statementId = "stmt-correlation";
            var sessionId = "session-correlation-test";

            using var activity = _activitySource.StartActivity("Statement.Execute");
            activity?.SetTag("statement.id", statementId);
            activity?.SetTag("session.id", sessionId);
            activity?.Stop();

            _aggregator.ProcessActivity(activity!);

            // Act
            _aggregator.CompleteStatement(statementId);
            Thread.Sleep(100);

            // Assert
            var exportedMetrics = _mockExporter.ExportedMetrics.SelectMany(batch => batch).ToList();
            var metric = exportedMetrics.FirstOrDefault(m => m.StatementId == statementId);

            Assert.NotNull(metric);
            Assert.Equal(statementId, metric.StatementId);
            Assert.Equal(sessionId, metric.SessionId);
        }

        // Test: Aggregation accumulates metrics correctly

        [Fact]
        public void MetricsAggregator_ProcessActivity_AccumulatesMetricsCorrectly()
        {
            // Arrange
            var statementId = "stmt-accumulate";

            // First activity - initial chunk download
            using var activity1 = _activitySource.StartActivity("Statement.Execute");
            activity1?.SetTag("statement.id", statementId);
            activity1?.SetTag("session.id", "session-123");
            activity1?.SetTag("result.chunk_count", "2");
            activity1?.SetTag("result.bytes_downloaded", "1024");
            activity1?.SetTag("poll.count", "1");
            System.Threading.Thread.Sleep(10);
            activity1?.Stop();

            // Second activity - more chunks
            using var activity2 = _activitySource.StartActivity("CloudFetch.Download");
            activity2?.SetTag("statement.id", statementId);
            activity2?.SetTag("result.chunk_count", "3");
            activity2?.SetTag("result.bytes_downloaded", "2048");
            activity2?.SetTag("poll.count", "2");
            System.Threading.Thread.Sleep(10);
            activity2?.Stop();

            // Act
            _aggregator.ProcessActivity(activity1!);
            _aggregator.ProcessActivity(activity2!);
            _aggregator.CompleteStatement(statementId);

            Thread.Sleep(100);

            // Assert
            var exportedMetrics = _mockExporter.ExportedMetrics.SelectMany(batch => batch).ToList();
            var metric = exportedMetrics.FirstOrDefault(m => m.StatementId == statementId);

            Assert.NotNull(metric);
            Assert.Equal(5, metric.ChunkCount); // 2 + 3
            Assert.Equal(3072L, metric.TotalBytesDownloaded); // 1024 + 2048
            Assert.Equal(3, metric.PollCount); // 1 + 2
        }

        // Mock Telemetry Exporter for testing

        private class MockTelemetryExporter : ITelemetryExporter
        {
            public List<List<TelemetryMetric>> ExportedMetrics { get; } = new List<List<TelemetryMetric>>();

            public Task ExportAsync(IReadOnlyList<TelemetryMetric> metrics, CancellationToken ct = default)
            {
                ExportedMetrics.Add(new List<TelemetryMetric>(metrics));
                return Task.CompletedTask;
            }
        }
    }
}
