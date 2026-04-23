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
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry.Proto;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// E2E tests validating SetChunkDetails() call in DatabricksStatement.EmitTelemetry().
    /// Tests all 5 ChunkDetails proto fields and validates CloudFetch vs inline result scenarios.
    ///
    /// Exit Criteria:
    /// 1. SetChunkDetails() is called for CloudFetch results
    /// 2. All 5 ChunkDetails proto fields are populated in telemetry log
    /// 3. Inline results do not have chunk_details (null)
    /// 4. E2E tests pass for CloudFetch and inline scenarios
    /// </summary>
    public class ChunkDetailsTelemetryTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public ChunkDetailsTelemetryTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Test that all 5 ChunkDetails fields are populated and non-zero for CloudFetch.
        /// Exit criteria: All 5 ChunkDetails proto fields are populated in telemetry log.
        /// </summary>
        [SkippableFact]
        public async Task CloudFetch_AllChunkDetailsFields_ArePopulated()
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

                // Execute a query that will trigger CloudFetch
                // Use a large result set to ensure CloudFetch is used
                statement.SqlQuery = "SELECT * FROM main.tpcds_sf100_delta.store_sales LIMIT 1000000";

                var result = statement.ExecuteQuery();
                using var reader = result.Stream;

                // Consume all results to ensure telemetry is emitted
                while (await reader.ReadNextRecordBatchAsync() is { } batch)
                {
                    batch.Dispose();
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

                // Validate all 5 ChunkDetails fields are non-zero
                Assert.True(chunkDetails.TotalChunksPresent > 0,
                    $"total_chunks_present should be > 0, got {chunkDetails.TotalChunksPresent}");
                Assert.True(chunkDetails.TotalChunksIterated > 0,
                    $"total_chunks_iterated should be > 0, got {chunkDetails.TotalChunksIterated}");
                Assert.True(chunkDetails.InitialChunkLatencyMillis > 0,
                    $"initial_chunk_latency_millis should be > 0, got {chunkDetails.InitialChunkLatencyMillis}");
                Assert.True(chunkDetails.SlowestChunkLatencyMillis > 0,
                    $"slowest_chunk_latency_millis should be > 0, got {chunkDetails.SlowestChunkLatencyMillis}");
                Assert.True(chunkDetails.SumChunksDownloadTimeMillis > 0,
                    $"sum_chunks_download_time_millis should be > 0, got {chunkDetails.SumChunksDownloadTimeMillis}");

                OutputHelper?.WriteLine($"All 5 ChunkDetails fields populated:");
                OutputHelper?.WriteLine($"  total_chunks_present: {chunkDetails.TotalChunksPresent}");
                OutputHelper?.WriteLine($"  total_chunks_iterated: {chunkDetails.TotalChunksIterated}");
                OutputHelper?.WriteLine($"  initial_chunk_latency_millis: {chunkDetails.InitialChunkLatencyMillis}");
                OutputHelper?.WriteLine($"  slowest_chunk_latency_millis: {chunkDetails.SlowestChunkLatencyMillis}");
                OutputHelper?.WriteLine($"  sum_chunks_download_time_millis: {chunkDetails.SumChunksDownloadTimeMillis}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Test that initial_chunk_latency_millis is positive and represents first chunk download time.
        /// Exit criteria: initial_chunk_latency_millis > 0.
        /// </summary>
        [SkippableFact]
        public async Task CloudFetch_InitialChunkLatency_IsPositive()
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

                // Consume all results
                while (await reader.ReadNextRecordBatchAsync() is { } batch)
                {
                    batch.Dispose();
                }

                // Explicitly dispose statement to trigger telemetry emission
                statement.Dispose();

                // Act
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, 1, timeoutMs: 10000);

                // Assert
                Assert.NotEmpty(logs);
                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                Assert.NotNull(protoLog.SqlOperation.ChunkDetails);
                var chunkDetails = protoLog.SqlOperation.ChunkDetails;

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
        /// Test that slowest_chunk_latency_millis >= initial_chunk_latency_millis.
        /// Exit criteria: slowest_chunk_latency_millis >= initial.
        /// </summary>
        [SkippableFact]
        public async Task CloudFetch_SlowestChunkLatency_IsGreaterOrEqualToInitial()
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

                // Consume all results
                while (await reader.ReadNextRecordBatchAsync() is { } batch)
                {
                    batch.Dispose();
                }

                // Explicitly dispose statement to trigger telemetry emission
                statement.Dispose();

                // Act
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, 1, timeoutMs: 10000);

                // Assert
                Assert.NotEmpty(logs);
                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                Assert.NotNull(protoLog.SqlOperation.ChunkDetails);
                var chunkDetails = protoLog.SqlOperation.ChunkDetails;

                Assert.True(chunkDetails.SlowestChunkLatencyMillis >= chunkDetails.InitialChunkLatencyMillis,
                    $"slowest_chunk_latency_millis ({chunkDetails.SlowestChunkLatencyMillis}) " +
                    $"should be >= initial_chunk_latency_millis ({chunkDetails.InitialChunkLatencyMillis})");

                OutputHelper?.WriteLine($"Initial chunk latency: {chunkDetails.InitialChunkLatencyMillis}ms");
                OutputHelper?.WriteLine($"Slowest chunk latency: {chunkDetails.SlowestChunkLatencyMillis}ms");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Test that sum_chunks_download_time_millis >= slowest_chunk_latency_millis.
        /// Exit criteria: sum_chunks_download_time_millis >= slowest.
        /// </summary>
        [SkippableFact]
        public async Task CloudFetch_SumChunksDownloadTime_IsGreaterOrEqualToSlowest()
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

                // Consume all results
                while (await reader.ReadNextRecordBatchAsync() is { } batch)
                {
                    batch.Dispose();
                }

                // Explicitly dispose statement to trigger telemetry emission
                statement.Dispose();

                // Act
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, 1, timeoutMs: 10000);

                // Assert
                Assert.NotEmpty(logs);
                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                Assert.NotNull(protoLog.SqlOperation.ChunkDetails);
                var chunkDetails = protoLog.SqlOperation.ChunkDetails;

                Assert.True(chunkDetails.SumChunksDownloadTimeMillis >= chunkDetails.SlowestChunkLatencyMillis,
                    $"sum_chunks_download_time_millis ({chunkDetails.SumChunksDownloadTimeMillis}) " +
                    $"should be >= slowest_chunk_latency_millis ({chunkDetails.SlowestChunkLatencyMillis})");

                OutputHelper?.WriteLine($"Slowest chunk latency: {chunkDetails.SlowestChunkLatencyMillis}ms");
                OutputHelper?.WriteLine($"Sum chunks download time: {chunkDetails.SumChunksDownloadTimeMillis}ms");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Test that total_chunks_iterated <= total_chunks_present.
        /// Exit criteria: total_chunks_iterated <= total_chunks_present.
        /// </summary>
        [SkippableFact]
        public async Task CloudFetch_TotalChunksIterated_IsLessThanOrEqualToPresent()
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

                // Consume all results
                while (await reader.ReadNextRecordBatchAsync() is { } batch)
                {
                    batch.Dispose();
                }

                // Explicitly dispose statement to trigger telemetry emission
                statement.Dispose();

                // Act
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, 1, timeoutMs: 10000);

                // Assert
                Assert.NotEmpty(logs);
                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                Assert.NotNull(protoLog.SqlOperation.ChunkDetails);
                var chunkDetails = protoLog.SqlOperation.ChunkDetails;

                Assert.True(chunkDetails.TotalChunksIterated <= chunkDetails.TotalChunksPresent,
                    $"total_chunks_iterated ({chunkDetails.TotalChunksIterated}) " +
                    $"should be <= total_chunks_present ({chunkDetails.TotalChunksPresent})");

                OutputHelper?.WriteLine($"Total chunks present: {chunkDetails.TotalChunksPresent}");
                OutputHelper?.WriteLine($"Total chunks iterated: {chunkDetails.TotalChunksIterated}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Test that inline results have null chunk_details.
        /// Exit criteria: Inline results do not have chunk_details (null).
        /// </summary>
        [SkippableFact]
        public async Task InlineResults_ChunkDetails_IsNull()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                // Arrange
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                using var statement = connection.CreateStatement();

                // Execute a query with small result set to ensure inline results
                // Use a very small result set that will fit in direct results
                statement.SqlQuery = "SELECT 1 AS value";

                var result = statement.ExecuteQuery();
                using var reader = result.Stream;

                // Consume all results
                while (await reader.ReadNextRecordBatchAsync() is { } batch)
                {
                    batch.Dispose();
                }

                // Explicitly dispose statement to trigger telemetry emission
                statement.Dispose();

                // Act
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, 1, timeoutMs: 10000);

                // Assert
                Assert.NotEmpty(logs);
                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                Assert.NotNull(protoLog.SqlOperation);

                // Verify this is indeed an inline result
                if (protoLog.SqlOperation.ExecutionResult == ExecutionResult.Types.Format.ExternalLinks)
                {
                    // If CloudFetch was used despite small result, skip this test
                    Skip.If(true, "Test skipped: CloudFetch was used instead of inline results");
                }

                // For inline results, chunk_details should be null
                Assert.Null(protoLog.SqlOperation.ChunkDetails);

                OutputHelper?.WriteLine($"Inline result confirmed: chunk_details is null");
                OutputHelper?.WriteLine($"Execution result format: {protoLog.SqlOperation.ExecutionResult}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Test that execution_result is EXTERNAL_LINKS for CloudFetch queries.
        /// Exit criteria: execution_result is EXTERNAL_LINKS for CloudFetch.
        /// </summary>
        [SkippableFact]
        public async Task CloudFetch_ExecutionResult_IsExternalLinks()
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

                // Consume all results
                while (await reader.ReadNextRecordBatchAsync() is { } batch)
                {
                    batch.Dispose();
                }

                // Explicitly dispose statement to trigger telemetry emission
                statement.Dispose();

                // Act
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, 1, timeoutMs: 10000);

                // Assert
                Assert.NotEmpty(logs);
                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                Assert.NotNull(protoLog.SqlOperation);

                // If CloudFetch was used, verify EXTERNAL_LINKS format
                if (protoLog.SqlOperation.ChunkDetails != null)
                {
                    Assert.Equal(ExecutionResult.Types.Format.ExternalLinks, protoLog.SqlOperation.ExecutionResult);
                    OutputHelper?.WriteLine($"CloudFetch confirmed: execution_result is EXTERNAL_LINKS");
                }
                else
                {
                    // Inline results were used
                    Skip.If(true, "Test skipped: CloudFetch not used for this query");
                }
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Test that ChunkDetails fields maintain expected relationships in a multi-chunk scenario.
        /// This comprehensive test validates all relationships between the 5 fields.
        /// </summary>
        [SkippableFact]
        public async Task CloudFetch_ChunkDetailsRelationships_AreValid()
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

                // Use a large result set to ensure multiple chunks
                statement.SqlQuery = "SELECT * FROM main.tpcds_sf100_delta.store_sales LIMIT 1000000";

                var result = statement.ExecuteQuery();
                using var reader = result.Stream;

                // Consume all results
                int batchCount = 0;
                while (await reader.ReadNextRecordBatchAsync() is { } batch)
                {
                    batchCount++;
                    batch.Dispose();
                }

                // Explicitly dispose statement to trigger telemetry emission
                statement.Dispose();

                // Act
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, 1, timeoutMs: 10000);

                // Assert
                Assert.NotEmpty(logs);
                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                Assert.NotNull(protoLog.SqlOperation.ChunkDetails);
                var cd = protoLog.SqlOperation.ChunkDetails;

                // Validate all relationships
                Assert.True(cd.TotalChunksPresent > 0, "total_chunks_present should be > 0");
                Assert.True(cd.TotalChunksIterated > 0, "total_chunks_iterated should be > 0");
                Assert.True(cd.TotalChunksIterated <= cd.TotalChunksPresent,
                    "total_chunks_iterated should be <= total_chunks_present");

                Assert.True(cd.InitialChunkLatencyMillis > 0, "initial_chunk_latency_millis should be > 0");
                Assert.True(cd.SlowestChunkLatencyMillis > 0, "slowest_chunk_latency_millis should be > 0");
                Assert.True(cd.SlowestChunkLatencyMillis >= cd.InitialChunkLatencyMillis,
                    "slowest_chunk_latency_millis should be >= initial_chunk_latency_millis");

                Assert.True(cd.SumChunksDownloadTimeMillis > 0, "sum_chunks_download_time_millis should be > 0");
                Assert.True(cd.SumChunksDownloadTimeMillis >= cd.SlowestChunkLatencyMillis,
                    "sum_chunks_download_time_millis should be >= slowest_chunk_latency_millis");

                OutputHelper?.WriteLine($"All ChunkDetails relationships validated:");
                OutputHelper?.WriteLine($"  Batches consumed: {batchCount}");
                OutputHelper?.WriteLine($"  total_chunks_present: {cd.TotalChunksPresent}");
                OutputHelper?.WriteLine($"  total_chunks_iterated: {cd.TotalChunksIterated}");
                OutputHelper?.WriteLine($"  initial_chunk_latency_millis: {cd.InitialChunkLatencyMillis}");
                OutputHelper?.WriteLine($"  slowest_chunk_latency_millis: {cd.SlowestChunkLatencyMillis}");
                OutputHelper?.WriteLine($"  sum_chunks_download_time_millis: {cd.SumChunksDownloadTimeMillis}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// PECO-2988: is_compressed must reflect the actual per-result compression state,
        /// not the connection-level LZ4 capability flag.
        ///
        /// When CloudFetch is disabled, results stream inline over Thrift and are never
        /// LZ4-compressed, even if the connection advertises LZ4 capability.
        /// Prior to the fix, is_compressed echoed the capability flag and reported true here.
        /// </summary>
        [SkippableFact]
        public async Task InlineResults_IsCompressed_IsFalse_EvenWhenLz4CapabilityEnabled()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                // Force the inline Arrow path while advertising LZ4 capability on the connection.
                // This is the exact scenario that produced is_compressed=true bug reports.
                var connectionOptions = new Dictionary<string, string>
                {
                    [DatabricksParameters.UseCloudFetch] = "false",
                    [DatabricksParameters.CanDecompressLz4] = "true",
                };

                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(
                    TestEnvironment.GetDriverParameters(TestConfiguration), connectionOptions);

                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 1 AS value";

                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                while (await reader.ReadNextRecordBatchAsync() is { } batch)
                {
                    batch.Dispose();
                }
                statement.Dispose();

                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, 1, timeoutMs: 10000);

                Assert.NotEmpty(logs);
                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);
                Assert.NotNull(protoLog.SqlOperation);

                Assert.False(protoLog.SqlOperation.IsCompressed,
                    "is_compressed must be false for inline results regardless of the connection-level LZ4 capability flag (PECO-2988)");

                OutputHelper?.WriteLine($"Inline result: is_compressed={protoLog.SqlOperation.IsCompressed}, execution_result={protoLog.SqlOperation.ExecutionResult}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// PECO-2988: For CloudFetch results, is_compressed should reflect the actual LZ4
        /// compression state of the downloaded chunks (from <c>metadataResp.Lz4Compressed</c>),
        /// which maps to the LZ4 capability flag on the connection. When LZ4 is enabled and the
        /// server returns compressed chunks, is_compressed should be true.
        /// </summary>
        [SkippableFact]
        public async Task CloudFetch_IsCompressed_IsTrueWhenLz4Enabled()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
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
                while (await reader.ReadNextRecordBatchAsync() is { } batch)
                {
                    batch.Dispose();
                }
                statement.Dispose();

                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, 1, timeoutMs: 10000);

                Assert.NotEmpty(logs);
                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);
                Assert.NotNull(protoLog.SqlOperation);

                // Only assert the IsCompressed mapping when the server actually returned CloudFetch chunks.
                if (protoLog.SqlOperation.ExecutionResult != ExecutionResult.Types.Format.ExternalLinks)
                {
                    Skip.If(true, $"Test skipped: server returned {protoLog.SqlOperation.ExecutionResult}, not CloudFetch");
                }

                Assert.True(protoLog.SqlOperation.IsCompressed,
                    "is_compressed must be true for CloudFetch results when LZ4 is enabled and chunks were compressed by the server (PECO-2988)");

                OutputHelper?.WriteLine($"CloudFetch result: is_compressed={protoLog.SqlOperation.IsCompressed}, execution_result={protoLog.SqlOperation.ExecutionResult}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// PECO-2988: For CloudFetch results with LZ4 disabled at the connection, chunks are
        /// not LZ4-compressed and is_compressed must be false.
        /// </summary>
        [SkippableFact]
        public async Task CloudFetch_IsCompressed_IsFalseWhenLz4Disabled()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var connectionOptions = new Dictionary<string, string>
                {
                    [DatabricksParameters.UseCloudFetch] = "true",
                    [DatabricksParameters.EnableDirectResults] = "false",
                    [DatabricksParameters.CanDecompressLz4] = "false",
                    [DatabricksParameters.MaxBytesPerFile] = "10485760",
                };

                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(
                    TestEnvironment.GetDriverParameters(TestConfiguration), connectionOptions);

                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT * FROM main.tpcds_sf100_delta.store_sales LIMIT 1000000";

                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                while (await reader.ReadNextRecordBatchAsync() is { } batch)
                {
                    batch.Dispose();
                }
                statement.Dispose();

                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, 1, timeoutMs: 10000);

                Assert.NotEmpty(logs);
                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);
                Assert.NotNull(protoLog.SqlOperation);

                if (protoLog.SqlOperation.ExecutionResult != ExecutionResult.Types.Format.ExternalLinks)
                {
                    Skip.If(true, $"Test skipped: server returned {protoLog.SqlOperation.ExecutionResult}, not CloudFetch");
                }

                Assert.False(protoLog.SqlOperation.IsCompressed,
                    "is_compressed must be false for CloudFetch results when LZ4 is disabled on the connection (PECO-2988)");

                OutputHelper?.WriteLine($"CloudFetch (LZ4 disabled) result: is_compressed={protoLog.SqlOperation.IsCompressed}, execution_result={protoLog.SqlOperation.ExecutionResult}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }
    }
}
