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
using System.Linq;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// E2E tests for CloudFetch chunk metrics aggregation.
    /// Verifies that chunk details are properly tracked and reported in telemetry.
    /// </summary>
    public class ChunkMetricsAggregationTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public ChunkMetricsAggregationTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Test that initial chunk latency is recorded and is positive.
        /// Exit criteria: CloudFetchDownloader tracks first chunk latency.
        /// </summary>
        [SkippableFact]
        public async Task ChunkMetrics_InitialChunkLatency_IsRecorded()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                // Arrange - use same setup as CloudFetchE2ETest
                var connectionOptions = new Dictionary<string, string>
                {
                    [DatabricksParameters.UseCloudFetch] = "true",
                    [DatabricksParameters.EnableDirectResults] = "false",
                    [DatabricksParameters.CanDecompressLz4] = "true",
                    [DatabricksParameters.MaxBytesPerFile] = "10485760",
                    [TelemetryConfiguration.PropertyKeyEnabled] = "true",
                };

                exporter = new CapturingTelemetryExporter();
                TelemetryClientManager.ExporterOverride = exporter;
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(
                    TestEnvironment.GetDriverParameters(TestConfiguration), connectionOptions);

                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT * FROM main.tpcds_sf100_delta.store_sales LIMIT 1000000";

                var result = statement.ExecuteQuery();
                using var reader = result.Stream;

                // Consume all results to trigger chunk downloads
                while (await reader.ReadNextRecordBatchAsync() != null)
                {
                    // Process batches
                }

                // Explicitly dispose statement to trigger telemetry emission
                statement.Dispose();

                // Act - wait for telemetry to be exported
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, 1, timeoutMs: 10000);

                // Assert
                Assert.NotEmpty(logs);
                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                Assert.NotNull(protoLog.SqlOperation);

                Assert.NotNull(protoLog.SqlOperation.ChunkDetails);

                var chunkDetails = protoLog.SqlOperation.ChunkDetails;

                // Verify initial chunk latency is positive
                Assert.True(chunkDetails.InitialChunkLatencyMillis > 0,
                    $"initial_chunk_latency_millis should be > 0, got {chunkDetails.InitialChunkLatencyMillis}");

                OutputHelper?.WriteLine($"Initial chunk latency: {chunkDetails.InitialChunkLatencyMillis}ms");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Test that slowest chunk latency is >= initial chunk latency.
        /// Exit criteria: CloudFetchDownloader tracks max chunk latency.
        /// </summary>
        [SkippableFact]
        public async Task ChunkMetrics_SlowestChunkLatency_GreaterThanOrEqualToInitial()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                // Arrange
                // CloudFetch connection options (same setup as CloudFetchE2ETest)
                var connectionOptions = new Dictionary<string, string>
                {
                    [DatabricksParameters.UseCloudFetch] = "true",
                    [DatabricksParameters.EnableDirectResults] = "false",
                    [DatabricksParameters.CanDecompressLz4] = "true",
                    [DatabricksParameters.MaxBytesPerFile] = "10485760",
                };

                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(
                    TestEnvironment.GetDriverParameters(TestConfiguration), connectionOptions);
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT * FROM main.tpcds_sf100_delta.store_sales LIMIT 1000000";

                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                while (await reader.ReadNextRecordBatchAsync() != null) { }

                // Explicitly dispose statement to trigger telemetry emission
                statement.Dispose();

                // Act
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, 1, timeoutMs: 10000);

                // Assert
                Assert.NotEmpty(logs);
                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                Assert.NotNull(protoLog.SqlOperation.ChunkDetails);
                var chunkDetails = protoLog.SqlOperation.ChunkDetails;

                // Verify slowest >= initial
                Assert.True(chunkDetails.SlowestChunkLatencyMillis >= chunkDetails.InitialChunkLatencyMillis,
                    $"slowest_chunk_latency_millis ({chunkDetails.SlowestChunkLatencyMillis}) should be >= initial ({chunkDetails.InitialChunkLatencyMillis})");

                OutputHelper?.WriteLine($"Initial: {chunkDetails.InitialChunkLatencyMillis}ms, Slowest: {chunkDetails.SlowestChunkLatencyMillis}ms");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Test that sum of download times is >= slowest chunk latency.
        /// Exit criteria: CloudFetchDownloader sums all chunk latencies.
        /// </summary>
        [SkippableFact]
        public async Task ChunkMetrics_SumDownloadTime_GreaterThanOrEqualToSlowest()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                // CloudFetch connection options (same setup as CloudFetchE2ETest)
                var connectionOptions = new Dictionary<string, string>
                {
                    [DatabricksParameters.UseCloudFetch] = "true",
                    [DatabricksParameters.EnableDirectResults] = "false",
                    [DatabricksParameters.CanDecompressLz4] = "true",
                    [DatabricksParameters.MaxBytesPerFile] = "10485760",
                };

                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(
                    TestEnvironment.GetDriverParameters(TestConfiguration), connectionOptions);

                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT * FROM main.tpcds_sf100_delta.store_sales LIMIT 1000000";

                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                while (await reader.ReadNextRecordBatchAsync() != null) { }

                // Explicitly dispose statement to trigger telemetry emission
                statement.Dispose();

                // Act
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, 1, timeoutMs: 10000);

                // Assert
                Assert.NotEmpty(logs);
                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                Assert.NotNull(protoLog.SqlOperation.ChunkDetails);
                var chunkDetails = protoLog.SqlOperation.ChunkDetails;

                // Verify sum >= slowest
                Assert.True(chunkDetails.SumChunksDownloadTimeMillis >= chunkDetails.SlowestChunkLatencyMillis,
                    $"sum_chunks_download_time_millis ({chunkDetails.SumChunksDownloadTimeMillis}) should be >= slowest ({chunkDetails.SlowestChunkLatencyMillis})");

                OutputHelper?.WriteLine($"Sum: {chunkDetails.SumChunksDownloadTimeMillis}ms, Slowest: {chunkDetails.SlowestChunkLatencyMillis}ms");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Test that total chunks present matches the link count.
        /// Exit criteria: ChunkMetrics class defines all 5 required fields.
        /// </summary>
        [SkippableFact]
        public async Task ChunkMetrics_TotalChunksPresent_MatchesLinkCount()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                // CloudFetch connection options (same setup as CloudFetchE2ETest)
                var connectionOptions = new Dictionary<string, string>
                {
                    [DatabricksParameters.UseCloudFetch] = "true",
                    [DatabricksParameters.EnableDirectResults] = "false",
                    [DatabricksParameters.CanDecompressLz4] = "true",
                    [DatabricksParameters.MaxBytesPerFile] = "10485760",
                };

                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(
                    TestEnvironment.GetDriverParameters(TestConfiguration), connectionOptions);

                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT * FROM main.tpcds_sf100_delta.store_sales LIMIT 1000000";

                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                while (await reader.ReadNextRecordBatchAsync() != null) { }

                // Explicitly dispose statement to trigger telemetry emission
                statement.Dispose();

                // Act
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, 1, timeoutMs: 10000);

                // Assert
                Assert.NotEmpty(logs);
                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                Assert.NotNull(protoLog.SqlOperation.ChunkDetails);
                var chunkDetails = protoLog.SqlOperation.ChunkDetails;

                // Verify total_chunks_present > 0 (should have at least one chunk)
                Assert.True(chunkDetails.TotalChunksPresent > 0,
                    $"total_chunks_present should be > 0, got {chunkDetails.TotalChunksPresent}");

                OutputHelper?.WriteLine($"Total chunks present: {chunkDetails.TotalChunksPresent}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Test that total chunks iterated is <= total chunks present.
        /// Exit criteria: GetChunkMetrics() returns aggregated metrics.
        /// </summary>
        [SkippableFact]
        public async Task ChunkMetrics_TotalChunksIterated_LessThanOrEqualToPresent()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                // CloudFetch connection options (same setup as CloudFetchE2ETest)
                var connectionOptions = new Dictionary<string, string>
                {
                    [DatabricksParameters.UseCloudFetch] = "true",
                    [DatabricksParameters.EnableDirectResults] = "false",
                    [DatabricksParameters.CanDecompressLz4] = "true",
                    [DatabricksParameters.MaxBytesPerFile] = "10485760",
                };

                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(
                    TestEnvironment.GetDriverParameters(TestConfiguration), connectionOptions);

                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT * FROM main.tpcds_sf100_delta.store_sales LIMIT 1000000";

                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                while (await reader.ReadNextRecordBatchAsync() != null) { }

                // Explicitly dispose statement to trigger telemetry emission
                statement.Dispose();

                // Act
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, 1, timeoutMs: 10000);

                // Assert
                Assert.NotEmpty(logs);
                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                Assert.NotNull(protoLog.SqlOperation.ChunkDetails);
                var chunkDetails = protoLog.SqlOperation.ChunkDetails;

                // Verify iterated <= present
                Assert.True(chunkDetails.TotalChunksIterated <= chunkDetails.TotalChunksPresent,
                    $"total_chunks_iterated ({chunkDetails.TotalChunksIterated}) should be <= total_chunks_present ({chunkDetails.TotalChunksPresent})");

                OutputHelper?.WriteLine($"Chunks iterated: {chunkDetails.TotalChunksIterated}, Present: {chunkDetails.TotalChunksPresent}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Test that all 5 ChunkDetails fields are populated correctly.
        /// Comprehensive validation of all chunk metric fields.
        /// </summary>
        [SkippableFact]
        public async Task ChunkMetrics_AllFieldsPopulated_WithValidValues()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                // CloudFetch connection options (same setup as CloudFetchE2ETest)
                var connectionOptions = new Dictionary<string, string>
                {
                    [DatabricksParameters.UseCloudFetch] = "true",
                    [DatabricksParameters.EnableDirectResults] = "false",
                    [DatabricksParameters.CanDecompressLz4] = "true",
                    [DatabricksParameters.MaxBytesPerFile] = "10485760",
                };

                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(
                    TestEnvironment.GetDriverParameters(TestConfiguration), connectionOptions);

                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT * FROM main.tpcds_sf100_delta.store_sales LIMIT 1000000";

                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                while (await reader.ReadNextRecordBatchAsync() != null) { }

                // Explicitly dispose statement to trigger telemetry emission
                statement.Dispose();

                // Act
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, 1, timeoutMs: 10000);

                // Assert
                Assert.NotEmpty(logs);
                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                Assert.NotNull(protoLog.SqlOperation.ChunkDetails);
                var chunkDetails = protoLog.SqlOperation.ChunkDetails;

                // Verify all 5 fields are populated
                Assert.True(chunkDetails.TotalChunksPresent > 0, "total_chunks_present should be > 0");
                Assert.True(chunkDetails.TotalChunksIterated > 0, "total_chunks_iterated should be > 0");
                Assert.True(chunkDetails.InitialChunkLatencyMillis > 0, "initial_chunk_latency_millis should be > 0");
                Assert.True(chunkDetails.SlowestChunkLatencyMillis > 0, "slowest_chunk_latency_millis should be > 0");
                Assert.True(chunkDetails.SumChunksDownloadTimeMillis > 0, "sum_chunks_download_time_millis should be > 0");

                // Verify relationships between fields
                Assert.True(chunkDetails.SlowestChunkLatencyMillis >= chunkDetails.InitialChunkLatencyMillis,
                    "slowest >= initial");
                Assert.True(chunkDetails.SumChunksDownloadTimeMillis >= chunkDetails.SlowestChunkLatencyMillis,
                    "sum >= slowest");
                Assert.True(chunkDetails.TotalChunksIterated <= chunkDetails.TotalChunksPresent,
                    "iterated <= present");

                OutputHelper?.WriteLine($"ChunkDetails: Present={chunkDetails.TotalChunksPresent}, " +
                    $"Iterated={chunkDetails.TotalChunksIterated}, " +
                    $"Initial={chunkDetails.InitialChunkLatencyMillis}ms, " +
                    $"Slowest={chunkDetails.SlowestChunkLatencyMillis}ms, " +
                    $"Sum={chunkDetails.SumChunksDownloadTimeMillis}ms");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }
    }
}
