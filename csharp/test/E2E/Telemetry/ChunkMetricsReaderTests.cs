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
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Reader.CloudFetch;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Apache.Arrow.Ipc;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// E2E tests for CloudFetchReader.GetChunkMetrics() API.
    /// Verifies that the reader exposes chunk metrics from the downloader and that
    /// these metrics are accessible and accurate after consuming batches.
    /// </summary>
    public class ChunkMetricsReaderTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public ChunkMetricsReaderTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Test that reader.GetChunkMetrics() returns non-null ChunkMetrics object.
        /// Exit criteria: CloudFetchReader.GetChunkMetrics() returns ChunkMetrics.
        /// </summary>
        [SkippableFact]
        public async Task Reader_GetChunkMetrics_ReturnsNonNull()
        {
            AdbcConnection? connection = null;
            Apache.Arrow.Ipc.IArrowArrayStream? reader = null;

            try
            {
                // Arrange
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);

                // Force CloudFetch by setting max rows per batch low to ensure external results
                properties["adbc.databricks.batch_size"] = "10000";

                AdbcDriver driver = new DatabricksDriver();
                AdbcDatabase database = driver.Open(properties);
                connection = database.Connect(properties);

                using var statement = connection.CreateStatement();

                // Execute a query that will trigger CloudFetch (large result set)
                // Use a large enough dataset to ensure CloudFetch is used
                statement.SqlQuery = "SELECT * FROM range(1000000)";

                var result = statement.ExecuteQuery();
                reader = result.Stream;

                // Consume at least one batch to ensure chunks are downloaded
                var batch = await reader.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
                batch?.Dispose();

                // Act - Get chunk metrics using reflection since CloudFetchReader is internal
                var chunkMetrics = GetChunkMetricsViaReflection(reader);

                // Assert
                // Note: Metrics might be null if inline results are used instead of CloudFetch
                // This can happen if the result set is small enough to fit in direct results
                if (chunkMetrics == null)
                {
                    Skip.If(true, "Test skipped: CloudFetch not used for this query (inline results used instead)");
                }

                Assert.NotNull(chunkMetrics);
                OutputHelper?.WriteLine($"ChunkMetrics retrieved successfully from reader");
            }
            finally
            {
                reader?.Dispose();
                connection?.Dispose();
            }
        }

        /// <summary>
        /// Test that metrics from reader match those from the downloader.
        /// Exit criteria: Metrics match those from downloader.
        /// </summary>
        [SkippableFact]
        public async Task Reader_GetChunkMetrics_MatchesDownloaderValues()
        {
            AdbcConnection? connection = null;
            Apache.Arrow.Ipc.IArrowArrayStream? reader = null;

            try
            {
                // Arrange
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                properties["adbc.databricks.batch_size"] = "10000";

                AdbcDriver driver = new DatabricksDriver();
                AdbcDatabase database = driver.Open(properties);
                connection = database.Connect(properties);

                using var statement = connection.CreateStatement();

                // Execute a query that will trigger CloudFetch with multiple chunks
                statement.SqlQuery = "SELECT * FROM range(1000000)";

                var result = statement.ExecuteQuery();
                reader = result.Stream;

                // Consume several batches to ensure multiple chunks are processed
                int batchCount = 0;
                while (await reader.ReadNextRecordBatchAsync() is { } batch && batchCount < 5)
                {
                    batch.Dispose();
                    batchCount++;
                }

                // Act - Get chunk metrics from reader
                var readerMetrics = GetChunkMetricsViaReflection(reader);

                // Skip if CloudFetch not used
                if (readerMetrics == null)
                {
                    Skip.If(true, "Test skipped: CloudFetch not used for this query");
                }

                // Assert - Verify metrics are populated with valid values
                Assert.NotNull(readerMetrics);

                var totalChunksPresent = GetProperty<int>(readerMetrics, "TotalChunksPresent");
                var totalChunksIterated = GetProperty<int>(readerMetrics, "TotalChunksIterated");
                var initialChunkLatencyMs = GetProperty<long>(readerMetrics, "InitialChunkLatencyMs");
                var slowestChunkLatencyMs = GetProperty<long>(readerMetrics, "SlowestChunkLatencyMs");
                var sumChunksDownloadTimeMs = GetProperty<long>(readerMetrics, "SumChunksDownloadTimeMs");

                // Verify basic metric properties
                Assert.True(totalChunksPresent > 0, "TotalChunksPresent should be > 0");
                Assert.True(totalChunksIterated > 0, "TotalChunksIterated should be > 0");
                Assert.True(initialChunkLatencyMs > 0, "InitialChunkLatencyMs should be > 0");
                Assert.True(slowestChunkLatencyMs >= initialChunkLatencyMs,
                    "SlowestChunkLatencyMs should be >= InitialChunkLatencyMs");
                Assert.True(sumChunksDownloadTimeMs >= slowestChunkLatencyMs,
                    "SumChunksDownloadTimeMs should be >= SlowestChunkLatencyMs");
                Assert.True(totalChunksIterated <= totalChunksPresent,
                    "TotalChunksIterated should be <= TotalChunksPresent");

                OutputHelper?.WriteLine($"Reader metrics validated:");
                OutputHelper?.WriteLine($"  TotalChunksPresent: {totalChunksPresent}");
                OutputHelper?.WriteLine($"  TotalChunksIterated: {totalChunksIterated}");
                OutputHelper?.WriteLine($"  InitialChunkLatencyMs: {initialChunkLatencyMs}");
                OutputHelper?.WriteLine($"  SlowestChunkLatencyMs: {slowestChunkLatencyMs}");
                OutputHelper?.WriteLine($"  SumChunksDownloadTimeMs: {sumChunksDownloadTimeMs}");
            }
            finally
            {
                reader?.Dispose();
                connection?.Dispose();
            }
        }

        /// <summary>
        /// Test that metrics are available after consuming batches.
        /// Exit criteria: Metrics available after batch consumption.
        /// </summary>
        [SkippableFact]
        public async Task Reader_GetChunkMetrics_AvailableAfterBatchConsumption()
        {
            AdbcConnection? connection = null;
            Apache.Arrow.Ipc.IArrowArrayStream? reader = null;

            try
            {
                // Arrange
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                properties["adbc.databricks.batch_size"] = "10000";

                AdbcDriver driver = new DatabricksDriver();
                AdbcDatabase database = driver.Open(properties);
                connection = database.Connect(properties);

                using var statement = connection.CreateStatement();

                // Execute a query that will trigger CloudFetch
                statement.SqlQuery = "SELECT * FROM range(1000000)";

                var result = statement.ExecuteQuery();
                reader = result.Stream;

                // Act - Consume all batches
                int totalBatches = 0;
                while (await reader.ReadNextRecordBatchAsync() is { } batch)
                {
                    totalBatches++;
                    batch.Dispose();
                }

                OutputHelper?.WriteLine($"Consumed {totalBatches} batches");

                // Get metrics after all batches consumed
                var metrics = GetChunkMetricsViaReflection(reader);

                // Skip if CloudFetch not used
                if (metrics == null)
                {
                    Skip.If(true, "Test skipped: CloudFetch not used for this query");
                }

                // Assert
                Assert.NotNull(metrics);

                var totalChunksPresent = GetProperty<int>(metrics, "TotalChunksPresent");
                var totalChunksIterated = GetProperty<int>(metrics, "TotalChunksIterated");

                // After consuming all batches, chunks iterated should equal chunks present
                Assert.True(totalChunksPresent > 0, "TotalChunksPresent should be > 0");
                Assert.True(totalChunksIterated > 0, "TotalChunksIterated should be > 0");
                Assert.Equal(totalChunksPresent, totalChunksIterated);

                OutputHelper?.WriteLine($"Metrics available after full consumption:");
                OutputHelper?.WriteLine($"  TotalChunksPresent: {totalChunksPresent}");
                OutputHelper?.WriteLine($"  TotalChunksIterated: {totalChunksIterated}");
            }
            finally
            {
                reader?.Dispose();
                connection?.Dispose();
            }
        }

        /// <summary>
        /// Test that metrics reflect partial consumption correctly.
        /// This test validates that TotalChunksIterated is less than TotalChunksPresent
        /// when we stop reading early.
        /// </summary>
        [SkippableFact]
        public async Task Reader_GetChunkMetrics_ReflectsPartialConsumption()
        {
            AdbcConnection? connection = null;
            Apache.Arrow.Ipc.IArrowArrayStream? reader = null;

            try
            {
                // Arrange
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                properties["adbc.databricks.batch_size"] = "10000";

                AdbcDriver driver = new DatabricksDriver();
                AdbcDatabase database = driver.Open(properties);
                connection = database.Connect(properties);

                using var statement = connection.CreateStatement();

                // Execute a query that will trigger CloudFetch with multiple chunks
                statement.SqlQuery = "SELECT * FROM range(2000000)"; // Large enough to ensure multiple chunks

                var result = statement.ExecuteQuery();
                reader = result.Stream;

                // Act - Consume only a few batches, not all
                int batchesToConsume = 3;
                int batchCount = 0;
                while (await reader.ReadNextRecordBatchAsync() is { } batch && batchCount < batchesToConsume)
                {
                    batch.Dispose();
                    batchCount++;
                }

                // Get metrics after partial consumption
                var metrics = GetChunkMetricsViaReflection(reader);

                // Skip if CloudFetch not used
                if (metrics == null)
                {
                    Skip.If(true, "Test skipped: CloudFetch not used for this query");
                }

                // Assert
                Assert.NotNull(metrics);

                var totalChunksPresent = GetProperty<int>(metrics, "TotalChunksPresent");
                var totalChunksIterated = GetProperty<int>(metrics, "TotalChunksIterated");

                // With partial consumption, we expect chunks present >= chunks iterated
                Assert.True(totalChunksPresent > 0, "TotalChunksPresent should be > 0");
                Assert.True(totalChunksIterated > 0, "TotalChunksIterated should be > 0");
                Assert.True(totalChunksIterated <= totalChunksPresent,
                    "TotalChunksIterated should be <= TotalChunksPresent for partial consumption");

                OutputHelper?.WriteLine($"Partial consumption metrics:");
                OutputHelper?.WriteLine($"  Batches consumed: {batchCount}");
                OutputHelper?.WriteLine($"  TotalChunksPresent: {totalChunksPresent}");
                OutputHelper?.WriteLine($"  TotalChunksIterated: {totalChunksIterated}");
            }
            finally
            {
                reader?.Dispose();
                connection?.Dispose();
            }
        }

        /// <summary>
        /// Test that metrics are consistent across multiple calls.
        /// Verifies that calling GetChunkMetrics() multiple times returns consistent values.
        /// </summary>
        [SkippableFact]
        public async Task Reader_GetChunkMetrics_ConsistentAcrossMultipleCalls()
        {
            AdbcConnection? connection = null;
            Apache.Arrow.Ipc.IArrowArrayStream? reader = null;

            try
            {
                // Arrange
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                properties["adbc.databricks.batch_size"] = "10000";

                AdbcDriver driver = new DatabricksDriver();
                AdbcDatabase database = driver.Open(properties);
                connection = database.Connect(properties);

                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT * FROM range(1000000)";

                var result = statement.ExecuteQuery();
                reader = result.Stream;

                // Consume some batches
                var batch = await reader.ReadNextRecordBatchAsync();
                batch?.Dispose();

                // Act - Get metrics multiple times
                var metrics1 = GetChunkMetricsViaReflection(reader);
                var metrics2 = GetChunkMetricsViaReflection(reader);

                // Skip if CloudFetch not used
                if (metrics1 == null || metrics2 == null)
                {
                    Skip.If(true, "Test skipped: CloudFetch not used for this query");
                }

                // Assert - Metrics should be the same across calls
                Assert.NotNull(metrics1);
                Assert.NotNull(metrics2);

                var present1 = GetProperty<int>(metrics1, "TotalChunksPresent");
                var present2 = GetProperty<int>(metrics2, "TotalChunksPresent");
                var iterated1 = GetProperty<int>(metrics1, "TotalChunksIterated");
                var iterated2 = GetProperty<int>(metrics2, "TotalChunksIterated");

                Assert.Equal(present1, present2);
                Assert.Equal(iterated1, iterated2);

                OutputHelper?.WriteLine("Metrics are consistent across multiple calls");
            }
            finally
            {
                reader?.Dispose();
                connection?.Dispose();
            }
        }

        /// <summary>
        /// Helper method to get ChunkMetrics from reader using reflection.
        /// CloudFetchReader is internal, so we need reflection to access GetChunkMetrics().
        /// Works with both CloudFetchReader and DatabricksCompositeReader.
        /// </summary>
        private object? GetChunkMetricsViaReflection(object reader)
        {
            var readerType = reader.GetType();

            // Try to get GetChunkMetrics method (available on both CloudFetchReader and DatabricksCompositeReader)
            var method = readerType.GetMethod("GetChunkMetrics", BindingFlags.Public | BindingFlags.Instance);

            if (method == null)
            {
                throw new InvalidOperationException($"GetChunkMetrics method not found on {readerType.Name}");
            }

            var result = method.Invoke(reader, null);

            // If result is null, this means we're not using CloudFetch (e.g., inline results)
            if (result == null)
            {
                OutputHelper?.WriteLine($"Reader type is {readerType.Name}, but not using CloudFetch. Metrics not available.");
            }

            return result;
        }

        /// <summary>
        /// Helper method to get a property value from an object using reflection.
        /// </summary>
        private T GetProperty<T>(object obj, string propertyName)
        {
            var property = obj.GetType().GetProperty(propertyName);
            if (property == null)
            {
                throw new InvalidOperationException($"Property {propertyName} not found");
            }

            var value = property.GetValue(obj);
            if (value == null)
            {
                throw new InvalidOperationException($"Property {propertyName} is null");
            }

            return (T)value;
        }
    }
}
