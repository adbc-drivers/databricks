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
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Proto;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;
using ExecutionResultFormat = AdbcDrivers.Databricks.Telemetry.Proto.ExecutionResult.Types.Format;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// E2E tests validating SetChunkDetails() call in StatementExecutionStatement for SEA CloudFetch.
    /// Tests all 5 ChunkDetails proto fields and validates CloudFetch vs inline result scenarios for SEA.
    ///
    /// Exit Criteria:
    /// 1. SEA CloudFetch results populate all 5 ChunkDetails fields
    /// 2. Chunk metrics match actual download behavior
    /// 3. E2E tests validate SEA chunk details telemetry
    /// </summary>
    public class SEAChunkDetailsTelemetryTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public SEAChunkDetailsTelemetryTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Helper method to create a REST (SEA) connection with capturing telemetry.
        /// </summary>
        private (AdbcConnection Connection, CapturingTelemetryExporter Exporter) CreateRestConnectionWithCapturingTelemetry()
        {
            var properties = new Dictionary<string, string>
            {
                [DatabricksParameters.Protocol] = "rest",
            };

            // Use URI if available (connection will parse host and warehouse ID from it)
            if (!string.IsNullOrEmpty(TestConfiguration.Uri))
            {
                properties[AdbcOptions.Uri] = TestConfiguration.Uri;
            }
            else
            {
                // Fall back to individual properties
                if (!string.IsNullOrEmpty(TestConfiguration.HostName))
                {
                    properties[SparkParameters.HostName] = TestConfiguration.HostName;
                }
                if (!string.IsNullOrEmpty(TestConfiguration.Path))
                {
                    properties[SparkParameters.Path] = TestConfiguration.Path;
                }
            }

            // Token-based authentication (PAT or OAuth access token)
            if (!string.IsNullOrEmpty(TestConfiguration.Token))
            {
                properties[SparkParameters.Token] = TestConfiguration.Token;
            }
            if (!string.IsNullOrEmpty(TestConfiguration.AccessToken))
            {
                properties[SparkParameters.AccessToken] = TestConfiguration.AccessToken;
            }

            // OAuth M2M authentication (client_credentials)
            if (!string.IsNullOrEmpty(TestConfiguration.AuthType))
            {
                properties[SparkParameters.AuthType] = TestConfiguration.AuthType;
            }
            if (!string.IsNullOrEmpty(TestConfiguration.OAuthGrantType))
            {
                properties[DatabricksParameters.OAuthGrantType] = TestConfiguration.OAuthGrantType;
            }
            if (!string.IsNullOrEmpty(TestConfiguration.OAuthClientId))
            {
                properties[DatabricksParameters.OAuthClientId] = TestConfiguration.OAuthClientId;
            }
            if (!string.IsNullOrEmpty(TestConfiguration.OAuthClientSecret))
            {
                properties[DatabricksParameters.OAuthClientSecret] = TestConfiguration.OAuthClientSecret;
            }
            if (!string.IsNullOrEmpty(TestConfiguration.OAuthScope))
            {
                properties[DatabricksParameters.OAuthScope] = TestConfiguration.OAuthScope;
            }

            // Enable telemetry
            properties[TelemetryConfiguration.PropertyKeyEnabled] = "true";

            // Create and set the capturing exporter
            var exporter = new CapturingTelemetryExporter();
            TelemetryClientManager.ExporterOverride = exporter;

            // Create driver and database
            AdbcDriver driver = new DatabricksDriver();
            AdbcDatabase database = driver.Open(properties);

            // Create and open connection
            AdbcConnection connection = database.Connect(properties);

            return (connection, exporter);
        }

        /// <summary>
        /// Test that SEA CloudFetch populates all 5 ChunkDetails fields.
        /// Exit criterion: SEA CloudFetch results populate all 5 ChunkDetails fields.
        /// </summary>
        [SkippableFact]
        public async Task SEACloudFetch_AllChunkDetailsFields_ArePopulated()
        {
            AdbcConnection? connection = null;

            try
            {
                (connection, var exporter) = CreateRestConnectionWithCapturingTelemetry();

                // Execute a query that will trigger CloudFetch
                // Use a very large result set to ensure CloudFetch is used (not inline)
                // SEA may use inline results for smaller datasets, so we use 1M+ rows
                using (var statement = connection.CreateStatement())
                {
                    statement.SqlQuery = "SELECT * FROM range(5000000)";
                    var result = statement.ExecuteQuery();
                    using var reader = result.Stream;

                    // Consume all results to ensure telemetry is emitted
                    while (await reader.ReadNextRecordBatchAsync() is { } batch)
                    {
                        batch.Dispose();
                    }

                    statement.Dispose();
                }

                // Wait for telemetry to be emitted
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1, timeoutMs: 10000);

                // Assert
                Assert.NotEmpty(logs);
                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                Assert.NotNull(protoLog.SqlOperation);

                // Skip test if CloudFetch was not used (inline results)
                if (protoLog.SqlOperation.ExecutionResult != ExecutionResultFormat.ExternalLinks)
                {
                    Skip.If(true, "Test skipped: CloudFetch not used for this query (inline results used instead)");
                }

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

                OutputHelper?.WriteLine($"✓ All 5 ChunkDetails fields populated for SEA CloudFetch:");
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
        /// Test that initial_chunk_latency_millis is positive for SEA CloudFetch.
        /// Exit criterion: Test initial <= slowest <= sum relationships.
        /// </summary>
        [SkippableFact]
        public async Task SEACloudFetch_InitialChunkLatency_IsPositive()
        {
            AdbcConnection? connection = null;

            try
            {
                (connection, var exporter) = CreateRestConnectionWithCapturingTelemetry();

                using (var statement = connection.CreateStatement())
                {
                    statement.SqlQuery = "SELECT * FROM range(5000000)";
                    var result = statement.ExecuteQuery();
                    using var reader = result.Stream;

                    // Consume all results
                    while (await reader.ReadNextRecordBatchAsync() is { } batch)
                    {
                        batch.Dispose();
                    }

                    statement.Dispose();
                }

                // Wait for telemetry
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1, timeoutMs: 10000);

                Assert.NotEmpty(logs);
                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Skip if not CloudFetch
                if (protoLog.SqlOperation?.ExecutionResult != ExecutionResultFormat.ExternalLinks)
                {
                    Skip.If(true, "CloudFetch not used");
                }

                var chunkDetails = protoLog.SqlOperation.ChunkDetails;
                Assert.NotNull(chunkDetails);

                Assert.True(chunkDetails.InitialChunkLatencyMillis > 0,
                    $"initial_chunk_latency_millis should be positive, got {chunkDetails.InitialChunkLatencyMillis}");

                OutputHelper?.WriteLine($"✓ initial_chunk_latency_millis = {chunkDetails.InitialChunkLatencyMillis}ms (SEA CloudFetch)");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Test that slowest_chunk_latency_millis >= initial_chunk_latency_millis for SEA CloudFetch.
        /// Exit criterion: Test initial <= slowest <= sum relationships.
        /// </summary>
        [SkippableFact]
        public async Task SEACloudFetch_SlowestChunkLatency_GteInitial()
        {
            AdbcConnection? connection = null;

            try
            {
                (connection, var exporter) = CreateRestConnectionWithCapturingTelemetry();

                using (var statement = connection.CreateStatement())
                {
                    statement.SqlQuery = "SELECT * FROM range(5000000)";
                    var result = statement.ExecuteQuery();
                    using var reader = result.Stream;

                    while (await reader.ReadNextRecordBatchAsync() is { } batch)
                    {
                        batch.Dispose();
                    }

                    statement.Dispose();
                }

                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1, timeoutMs: 10000);

                Assert.NotEmpty(logs);
                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Skip if not CloudFetch
                if (protoLog.SqlOperation?.ExecutionResult != ExecutionResultFormat.ExternalLinks)
                {
                    Skip.If(true, "CloudFetch not used");
                }

                var chunkDetails = protoLog.SqlOperation.ChunkDetails;
                Assert.NotNull(chunkDetails);

                Assert.True(chunkDetails.SlowestChunkLatencyMillis >= chunkDetails.InitialChunkLatencyMillis,
                    $"slowest ({chunkDetails.SlowestChunkLatencyMillis}) should be >= initial ({chunkDetails.InitialChunkLatencyMillis})");

                OutputHelper?.WriteLine($"✓ slowest_chunk_latency_millis ({chunkDetails.SlowestChunkLatencyMillis}ms) >= initial ({chunkDetails.InitialChunkLatencyMillis}ms) (SEA CloudFetch)");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Test that sum_chunks_download_time_millis >= slowest_chunk_latency_millis for SEA CloudFetch.
        /// Exit criterion: Test initial <= slowest <= sum relationships.
        /// </summary>
        [SkippableFact]
        public async Task SEACloudFetch_SumDownloadTime_GteSlowest()
        {
            AdbcConnection? connection = null;

            try
            {
                (connection, var exporter) = CreateRestConnectionWithCapturingTelemetry();

                using (var statement = connection.CreateStatement())
                {
                    statement.SqlQuery = "SELECT * FROM range(5000000)";
                    var result = statement.ExecuteQuery();
                    using var reader = result.Stream;

                    while (await reader.ReadNextRecordBatchAsync() is { } batch)
                    {
                        batch.Dispose();
                    }

                    statement.Dispose();
                }

                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1, timeoutMs: 10000);

                Assert.NotEmpty(logs);
                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Skip if not CloudFetch
                if (protoLog.SqlOperation?.ExecutionResult != ExecutionResultFormat.ExternalLinks)
                {
                    Skip.If(true, "CloudFetch not used");
                }

                var chunkDetails = protoLog.SqlOperation.ChunkDetails;
                Assert.NotNull(chunkDetails);

                Assert.True(chunkDetails.SumChunksDownloadTimeMillis >= chunkDetails.SlowestChunkLatencyMillis,
                    $"sum ({chunkDetails.SumChunksDownloadTimeMillis}) should be >= slowest ({chunkDetails.SlowestChunkLatencyMillis})");

                OutputHelper?.WriteLine($"✓ sum_chunks_download_time_millis ({chunkDetails.SumChunksDownloadTimeMillis}ms) >= slowest ({chunkDetails.SlowestChunkLatencyMillis}ms) (SEA CloudFetch)");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Test that total_chunks_iterated <= total_chunks_present for SEA CloudFetch.
        /// Exit criterion: Test iterated <= present relationship.
        /// </summary>
        [SkippableFact]
        public async Task SEACloudFetch_TotalChunksIterated_LtePresent()
        {
            AdbcConnection? connection = null;

            try
            {
                (connection, var exporter) = CreateRestConnectionWithCapturingTelemetry();

                using (var statement = connection.CreateStatement())
                {
                    statement.SqlQuery = "SELECT * FROM range(5000000)";
                    var result = statement.ExecuteQuery();
                    using var reader = result.Stream;

                    while (await reader.ReadNextRecordBatchAsync() is { } batch)
                    {
                        batch.Dispose();
                    }

                    statement.Dispose();
                }

                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1, timeoutMs: 10000);

                Assert.NotEmpty(logs);
                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Skip if not CloudFetch
                if (protoLog.SqlOperation?.ExecutionResult != ExecutionResultFormat.ExternalLinks)
                {
                    Skip.If(true, "CloudFetch not used");
                }

                var chunkDetails = protoLog.SqlOperation.ChunkDetails;
                Assert.NotNull(chunkDetails);

                Assert.True(chunkDetails.TotalChunksIterated <= chunkDetails.TotalChunksPresent,
                    $"iterated ({chunkDetails.TotalChunksIterated}) should be <= present ({chunkDetails.TotalChunksPresent})");

                OutputHelper?.WriteLine($"✓ total_chunks_iterated ({chunkDetails.TotalChunksIterated}) <= total_chunks_present ({chunkDetails.TotalChunksPresent}) (SEA CloudFetch)");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Comprehensive test validating all SEA chunk details telemetry exit criteria.
        /// This test ensures:
        /// 1. SEA CloudFetch results populate all 5 ChunkDetails fields
        /// 2. Chunk metrics match actual download behavior (relationships are valid)
        /// 3. E2E tests validate SEA chunk details telemetry
        /// </summary>
        [SkippableFact]
        public async Task SEACloudFetch_ChunkDetailsRelationships_AreValid()
        {
            AdbcConnection? connection = null;

            try
            {
                (connection, var exporter) = CreateRestConnectionWithCapturingTelemetry();

                // Execute a query that will trigger CloudFetch
                using (var statement = connection.CreateStatement())
                {
                    statement.SqlQuery = "SELECT * FROM range(5000000)";
                    var result = statement.ExecuteQuery();
                    using var reader = result.Stream;

                    // Consume all results
                    while (await reader.ReadNextRecordBatchAsync() is { } batch)
                    {
                        batch.Dispose();
                    }

                    statement.Dispose();
                }

                // Wait for telemetry
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1, timeoutMs: 10000);

                Assert.NotEmpty(logs);
                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Skip if not CloudFetch
                if (protoLog.SqlOperation?.ExecutionResult != ExecutionResultFormat.ExternalLinks)
                {
                    Skip.If(true, "CloudFetch not used - cannot validate chunk details");
                }

                var chunkDetails = protoLog.SqlOperation.ChunkDetails;
                Assert.NotNull(chunkDetails);

                // Exit criterion 1: All 5 fields populated
                Assert.True(chunkDetails.TotalChunksPresent > 0);
                Assert.True(chunkDetails.TotalChunksIterated > 0);
                Assert.True(chunkDetails.InitialChunkLatencyMillis > 0);
                Assert.True(chunkDetails.SlowestChunkLatencyMillis > 0);
                Assert.True(chunkDetails.SumChunksDownloadTimeMillis > 0);
                OutputHelper?.WriteLine("✓ Exit criterion 1: All 5 ChunkDetails fields are non-zero");

                // Exit criterion 2: Chunk metrics match actual download behavior
                Assert.True(chunkDetails.SlowestChunkLatencyMillis >= chunkDetails.InitialChunkLatencyMillis,
                    "slowest >= initial");
                Assert.True(chunkDetails.SumChunksDownloadTimeMillis >= chunkDetails.SlowestChunkLatencyMillis,
                    "sum >= slowest");
                Assert.True(chunkDetails.TotalChunksIterated <= chunkDetails.TotalChunksPresent,
                    "iterated <= present");
                OutputHelper?.WriteLine("✓ Exit criterion 2: Chunk metrics relationships are valid:");
                OutputHelper?.WriteLine($"  - initial ({chunkDetails.InitialChunkLatencyMillis}ms) <= slowest ({chunkDetails.SlowestChunkLatencyMillis}ms) <= sum ({chunkDetails.SumChunksDownloadTimeMillis}ms)");
                OutputHelper?.WriteLine($"  - iterated ({chunkDetails.TotalChunksIterated}) <= present ({chunkDetails.TotalChunksPresent})");

                // Exit criterion 3: E2E tests validate SEA chunk details telemetry
                OutputHelper?.WriteLine("✓ Exit criterion 3: E2E test validated SEA chunk details telemetry");

                OutputHelper?.WriteLine("\n✅ All SEA chunk details telemetry exit criteria met successfully!");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }
    }
}
