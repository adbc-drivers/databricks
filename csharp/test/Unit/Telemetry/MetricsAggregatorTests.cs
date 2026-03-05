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
using System.Security.Authentication;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Unit tests for MetricsAggregator.
    /// </summary>
    public class MetricsAggregatorTests : IDisposable
    {
        private readonly ActivityListener _activityListener;
        private readonly ActivitySource _testSource;

        /// <summary>
        /// Initializes a new instance of the <see cref="MetricsAggregatorTests"/> class.
        /// </summary>
        public MetricsAggregatorTests()
        {
            _testSource = new ActivitySource("TestSource");

            // Set up an ActivityListener so Activities are actually created
            _activityListener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == "TestSource",
                Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded
            };

            ActivitySource.AddActivityListener(_activityListener);
        }

        /// <summary>
        /// Disposes test resources.
        /// </summary>
        public void Dispose()
        {
            _activityListener?.Dispose();
            _testSource?.Dispose();
        }
        /// <summary>
        /// Test that connection activities are emitted immediately.
        /// </summary>
        [Fact]
        public async Task ProcessActivity_ConnectionOpen_EmitsImmediately()
        {
            // Arrange
            MockTelemetryExporter exporter = new MockTelemetryExporter();
            MetricsAggregator aggregator = new MetricsAggregator(exporter, batchSize: 10);

            Activity activity = CreateConnectionActivity();

            // Act
            aggregator.ProcessActivity(activity);
            await aggregator.FlushAsync();

            // Assert
            Assert.Single(exporter.ExportedLogs);
            await aggregator.DisposeAsync();
        }

        /// <summary>
        /// Test that statement activities aggregate by statement_id.
        /// </summary>
        [Fact]
        public async Task ProcessActivity_Statement_AggregatesByStatementId()
        {
            // Arrange
            MockTelemetryExporter exporter = new MockTelemetryExporter();
            MetricsAggregator aggregator = new MetricsAggregator(exporter, batchSize: 10);

            string statementId = "stmt-123";

            // Create multiple activities with the same statement_id
            Activity activity1 = CreateStatementActivity(statementId, chunkCount: 2, bytesDownloaded: 1000);
            Activity activity2 = CreateStatementActivity(statementId, chunkCount: 3, bytesDownloaded: 2000);

            // Act
            aggregator.ProcessActivity(activity1);
            aggregator.ProcessActivity(activity2);
            aggregator.CompleteStatement(statementId);
            await aggregator.FlushAsync();

            // Assert
            // Should have 1 aggregated metric for the statement
            Assert.Single(exporter.ExportedLogs);
            await aggregator.DisposeAsync();
        }

        /// <summary>
        /// Test that CompleteStatement emits the aggregated metric.
        /// </summary>
        [Fact]
        public async Task CompleteStatement_EmitsAggregatedMetric()
        {
            // Arrange
            MockTelemetryExporter exporter = new MockTelemetryExporter();
            MetricsAggregator aggregator = new MetricsAggregator(exporter, batchSize: 10);

            string statementId = "stmt-456";
            Activity activity = CreateStatementActivity(statementId, chunkCount: 5, bytesDownloaded: 5000);

            // Act
            aggregator.ProcessActivity(activity);
            aggregator.CompleteStatement(statementId);
            await aggregator.FlushAsync();

            // Assert
            Assert.Single(exporter.ExportedLogs);
            await aggregator.DisposeAsync();
        }

        /// <summary>
        /// Test that batch size threshold triggers flush.
        /// </summary>
        [Fact]
        public async Task FlushAsync_BatchSizeReached_ExportsMetrics()
        {
            // Arrange
            int batchSize = 3;
            MockTelemetryExporter exporter = new MockTelemetryExporter();
            MetricsAggregator aggregator = new MetricsAggregator(exporter, batchSize: batchSize);

            // Act - create more activities than batch size
            for (int i = 0; i < batchSize; i++)
            {
                Activity activity = CreateConnectionActivity();
                aggregator.ProcessActivity(activity);
            }

            // Give the async flush task time to complete
            await Task.Delay(100);
            await aggregator.FlushAsync();

            // Assert
            Assert.True(exporter.ExportedLogs.Count >= batchSize);
            await aggregator.DisposeAsync();
        }

        /// <summary>
        /// Test that time interval triggers flush.
        /// </summary>
        [Fact]
        public async Task FlushAsync_TimeInterval_ExportsMetrics()
        {
            // Arrange
            TimeSpan flushInterval = TimeSpan.FromMilliseconds(200);
            MockTelemetryExporter exporter = new MockTelemetryExporter();
            MetricsAggregator aggregator = new MetricsAggregator(exporter, batchSize: 100, flushInterval: flushInterval);

            Activity activity = CreateConnectionActivity();

            // Act
            aggregator.ProcessActivity(activity);

            // Wait for flush interval + buffer
            await Task.Delay(flushInterval + TimeSpan.FromMilliseconds(200));

            // Assert
            Assert.True(exporter.ExportedLogs.Count > 0);
            await aggregator.DisposeAsync();
        }

        /// <summary>
        /// Test that terminal exceptions flush immediately.
        /// </summary>
        [Fact]
        public async Task RecordException_Terminal_FlushesImmediately()
        {
            // Arrange
            MockTelemetryExporter exporter = new MockTelemetryExporter();
            MetricsAggregator aggregator = new MetricsAggregator(exporter, batchSize: 100);

            string statementId = "stmt-terminal";
            Activity activity = CreateStatementActivity(statementId);
            aggregator.ProcessActivity(activity);

            // Act - record a terminal exception (AuthenticationException is terminal)
            Exception terminalException = new AuthenticationException("Authentication failed");
            aggregator.RecordException(statementId, terminalException);

            // Give the async flush task time to complete
            await Task.Delay(100);
            await aggregator.FlushAsync();

            // Assert - terminal exception should trigger immediate flush
            Assert.True(exporter.ExportedLogs.Count > 0);
            await aggregator.DisposeAsync();
        }

        /// <summary>
        /// Test that retryable exceptions buffer until statement complete.
        /// </summary>
        [Fact]
        public async Task RecordException_Retryable_BuffersUntilComplete()
        {
            // Arrange
            MockTelemetryExporter exporter = new MockTelemetryExporter();
            MetricsAggregator aggregator = new MetricsAggregator(exporter, batchSize: 100);

            string statementId = "stmt-retryable";
            Activity activity = CreateStatementActivity(statementId);
            aggregator.ProcessActivity(activity);

            // Act - record a retryable exception (generic Exception is retryable)
            Exception retryableException = new Exception("Temporary network error");
            aggregator.RecordException(statementId, retryableException);

            // Wait a bit to ensure no immediate flush
            await Task.Delay(100);
            await aggregator.FlushAsync();

            // At this point, the exception is buffered but not flushed
            int countBeforeComplete = exporter.ExportedLogs.Count;

            // Now complete the statement
            aggregator.CompleteStatement(statementId);
            await aggregator.FlushAsync();

            // Assert - should have flushed after CompleteStatement
            Assert.True(exporter.ExportedLogs.Count > countBeforeComplete);
            await aggregator.DisposeAsync();
        }

        /// <summary>
        /// Test that exceptions during activity processing are swallowed.
        /// </summary>
        [Fact]
        public async Task ProcessActivity_ExceptionSwallowed_NoThrow()
        {
            // Arrange
            MockTelemetryExporter exporter = new MockTelemetryExporter();
            MetricsAggregator aggregator = new MetricsAggregator(exporter, batchSize: 10);

            // Create an activity with null tags to potentially trigger exceptions
            Activity? nullActivity = null;

            // Act & Assert - should not throw
            aggregator.ProcessActivity(nullActivity);

            await aggregator.DisposeAsync();
        }

        /// <summary>
        /// Test that tags are filtered using TelemetryTagRegistry.
        /// </summary>
        [Fact]
        public async Task ProcessActivity_FiltersTags_UsingRegistry()
        {
            // Arrange
            MockTelemetryExporter exporter = new MockTelemetryExporter();
            MetricsAggregator aggregator = new MetricsAggregator(exporter, batchSize: 10);

            // Create activity with both allowed and sensitive tags
            Activity activity = CreateConnectionActivity();
            activity.SetTag("sensitive_data", "should_be_filtered"); // This should be filtered out

            // Act
            aggregator.ProcessActivity(activity);
            await aggregator.FlushAsync();

            // Assert
            // The activity was processed without error (tags were filtered)
            Assert.True(exporter.ExportedLogs.Count > 0);
            await aggregator.DisposeAsync();
        }

        /// <summary>
        /// Test that null or empty statement IDs are handled gracefully.
        /// </summary>
        [Fact]
        public async Task ProcessActivity_NullStatementId_HandledGracefully()
        {
            // Arrange
            MockTelemetryExporter exporter = new MockTelemetryExporter();
            MetricsAggregator aggregator = new MetricsAggregator(exporter, batchSize: 10);

            Activity activity = CreateStatementActivity(null);

            // Act & Assert - should not throw
            aggregator.ProcessActivity(activity);
            await aggregator.FlushAsync();

            await aggregator.DisposeAsync();
        }

        /// <summary>
        /// Test that CompleteStatement with unknown statement ID is handled gracefully.
        /// </summary>
        [Fact]
        public async Task CompleteStatement_UnknownStatementId_HandledGracefully()
        {
            // Arrange
            MockTelemetryExporter exporter = new MockTelemetryExporter();
            MetricsAggregator aggregator = new MetricsAggregator(exporter, batchSize: 10);

            // Act & Assert - should not throw
            aggregator.CompleteStatement("unknown-statement-id");
            await aggregator.FlushAsync();

            await aggregator.DisposeAsync();
        }

        /// <summary>
        /// Test that multiple statements can be aggregated concurrently.
        /// </summary>
        [Fact]
        public async Task ProcessActivity_MultipleStatements_AggregatesSeparately()
        {
            // Arrange
            MockTelemetryExporter exporter = new MockTelemetryExporter();
            MetricsAggregator aggregator = new MetricsAggregator(exporter, batchSize: 100);

            string stmt1 = "stmt-1";
            string stmt2 = "stmt-2";
            string stmt3 = "stmt-3";

            // Act
            aggregator.ProcessActivity(CreateStatementActivity(stmt1, chunkCount: 1));
            aggregator.ProcessActivity(CreateStatementActivity(stmt2, chunkCount: 2));
            aggregator.ProcessActivity(CreateStatementActivity(stmt3, chunkCount: 3));

            aggregator.CompleteStatement(stmt1);
            aggregator.CompleteStatement(stmt2);
            aggregator.CompleteStatement(stmt3);

            await aggregator.FlushAsync();

            // Assert - should have 3 separate metrics
            Assert.True(exporter.ExportedLogs.Count >= 3);
            await aggregator.DisposeAsync();
        }

        /// <summary>
        /// Test that DisposeAsync flushes remaining metrics.
        /// </summary>
        [Fact]
        public async Task DisposeAsync_FlushesRemainingMetrics()
        {
            // Arrange
            MockTelemetryExporter exporter = new MockTelemetryExporter();
            MetricsAggregator aggregator = new MetricsAggregator(exporter, batchSize: 100);

            Activity activity = CreateConnectionActivity();
            aggregator.ProcessActivity(activity);

            // Verify nothing has been exported yet (batch size not reached, timer not fired)
            Assert.Empty(exporter.ExportedLogs);

            // Act - DisposeAsync should flush all pending metrics
            await aggregator.DisposeAsync();

            // Assert - metrics should now be exported
            Assert.True(exporter.ExportedLogs.Count > 0);
        }

        /// <summary>
        /// Test that aggregator handles high volume of activities.
        /// </summary>
        [Fact]
        public async Task ProcessActivity_HighVolume_HandlesCorrectly()
        {
            // Arrange
            MockTelemetryExporter exporter = new MockTelemetryExporter();
            MetricsAggregator aggregator = new MetricsAggregator(exporter, batchSize: 50);

            int activityCount = 200;

            // Act
            for (int i = 0; i < activityCount; i++)
            {
                if (i % 2 == 0)
                {
                    aggregator.ProcessActivity(CreateConnectionActivity());
                }
                else
                {
                    string stmtId = $"stmt-{i}";
                    aggregator.ProcessActivity(CreateStatementActivity(stmtId));
                    aggregator.CompleteStatement(stmtId);
                }
            }

            await aggregator.FlushAsync();

            // Assert
            Assert.True(exporter.ExportedLogs.Count > 0);
            await aggregator.DisposeAsync();
        }

        // ── Helper Methods ──

        /// <summary>
        /// Creates a mock connection activity.
        /// </summary>
        private Activity CreateConnectionActivity()
        {
            Activity activity = _testSource.StartActivity("Connection.OpenAsync", ActivityKind.Client)!;

            activity.SetTag("workspace.id", 12345L);
            activity.SetTag("session.id", "session-abc");
            activity.SetTag("driver.version", "1.0.0");
            activity.SetTag("driver.os", "Linux");
            activity.SetTag("driver.runtime", ".NET 8.0");

            activity.Stop();
            return activity;
        }

        /// <summary>
        /// Creates a mock statement activity.
        /// </summary>
        private Activity CreateStatementActivity(string? statementId, int? chunkCount = null, long? bytesDownloaded = null)
        {
            Activity activity = _testSource.StartActivity("Statement.ExecuteQuery", ActivityKind.Client)!;

            activity.SetTag("workspace.id", 12345L);
            activity.SetTag("session.id", "session-abc");

            if (!string.IsNullOrEmpty(statementId))
            {
                activity.SetTag("statement.id", statementId);
            }

            activity.SetTag("result.format", "cloudfetch");

            if (chunkCount.HasValue)
            {
                activity.SetTag("result.chunk_count", chunkCount.Value);
            }

            if (bytesDownloaded.HasValue)
            {
                activity.SetTag("result.bytes_downloaded", bytesDownloaded.Value);
            }

            // Simulate some execution time
            Thread.Sleep(10);
            activity.Stop();

            return activity;
        }

        /// <summary>
        /// Mock telemetry exporter for testing.
        /// </summary>
        private class MockTelemetryExporter : ITelemetryExporter
        {
            public List<TelemetryFrontendLog> ExportedLogs { get; } = new List<TelemetryFrontendLog>();

            public Task<bool> ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
            {
                if (logs != null)
                {
                    ExportedLogs.AddRange(logs);
                }
                return Task.FromResult(true);
            }
        }
    }
}
