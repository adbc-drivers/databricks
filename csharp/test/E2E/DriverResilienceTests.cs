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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using AdbcDrivers.HiveServer2;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests
{
    /// <summary>
    /// Tests for driver resilience patterns that real applications exercise:
    /// partial result consumption, statement reuse, error recovery, and
    /// interleaved workloads. These test the cleanup and state-reset code
    /// paths that are rarely covered by functional tests.
    /// </summary>
    public class DriverResilienceTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public DriverResilienceTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        // ------------------------------------------------------------------
        // Partial result consumption
        // ------------------------------------------------------------------

        /// <summary>
        /// Read only 1 batch from a large result, then dispose the statement.
        /// Tests CloudFetch pipeline cancellation: the background fetcher, download
        /// manager, and downloader tasks must all stop cleanly.
        /// </summary>
        [SkippableFact]
        public async Task PartialRead_DisposeStatement_ShouldNotHangOrLeak()
        {
            using var connection = NewConnection();
            long memBefore, memAfter;

            GC.Collect(2, GCCollectionMode.Forced, true);
            memBefore = GC.GetTotalMemory(true);

            // Do this 5 times to amplify any leak
            for (int i = 0; i < 5; i++)
            {
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT * FROM RANGE(1000000)";
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;

                // Read only the first batch, then let disposal clean up the rest
                var batch = await reader.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
                OutputHelper?.WriteLine($"Iteration {i + 1}: read first batch ({batch!.Length} rows), disposing remainder");
                // reader and statement dispose here — CloudFetch pipeline must cancel
            }

            // Allow background tasks to settle
            await Task.Delay(1000);
            GC.Collect(2, GCCollectionMode.Forced, true);
            GC.WaitForPendingFinalizers();
            GC.Collect(2, GCCollectionMode.Forced, true);
            memAfter = GC.GetTotalMemory(true);

            double growthMB = (memAfter - memBefore) / 1024.0 / 1024.0;
            OutputHelper?.WriteLine($"Memory before: {memBefore / 1024.0 / 1024.0:F2} MB");
            OutputHelper?.WriteLine($"Memory after:  {memAfter / 1024.0 / 1024.0:F2} MB");
            OutputHelper?.WriteLine($"Growth: {growthMB:F2} MB");

            // Each abandoned result set is ~8MB of Arrow data (1M int64 values).
            // If the pipeline leaks downloaded buffers, we'd see 40+ MB growth.
            Assert.True(growthMB < 20.0,
                $"Possible CloudFetch pipeline leak on partial consumption: {growthMB:F2} MB growth after 5 abandoned result sets.");
        }

        /// <summary>
        /// Read only 1 batch from a large CloudFetch result using the real tpcds table,
        /// then dispose. This exercises the multi-file CloudFetch pipeline cancellation
        /// more thoroughly — there are actual presigned URLs being downloaded in background.
        /// </summary>
        [SkippableFact]
        public async Task PartialRead_LargeCloudFetch_DisposeCleanly()
        {
            var parameters = new Dictionary<string, string>
            {
                [DatabricksParameters.UseCloudFetch] = "true",
                [DatabricksParameters.EnableDirectResults] = "true",
                [DatabricksParameters.CanDecompressLz4] = "true",
                [DatabricksParameters.MaxBytesPerFile] = "10485760",
            };

            using var connection = NewConnection(TestConfiguration, parameters);

            for (int i = 0; i < 3; i++)
            {
                var sw = Stopwatch.StartNew();
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT * FROM main.tpcds_sf100_delta.store_sales LIMIT 1000000";
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;

                // Read just enough to confirm CloudFetch is active, then abandon
                int rowsRead = 0;
                while (rowsRead < 10_000)
                {
                    var batch = await reader.ReadNextRecordBatchAsync();
                    if (batch == null) break;
                    rowsRead += batch.Length;
                }
                sw.Stop();

                OutputHelper?.WriteLine($"Iteration {i + 1}: read {rowsRead:N0} rows in {sw.ElapsedMilliseconds}ms, disposing remainder");
                // Statement + reader dispose — pipeline must shut down cleanly
            }

            // Verify connection is still usable after abandoning results
            using var verifyStatement = connection.CreateStatement();
            verifyStatement.SqlQuery = "SELECT 42 AS answer";
            var verifyResult = verifyStatement.ExecuteQuery();
            using var verifyReader = verifyResult.Stream;
            var verifyBatch = await verifyReader.ReadNextRecordBatchAsync();
            Assert.NotNull(verifyBatch);
            Assert.Equal(1, verifyBatch!.Length);
            OutputHelper?.WriteLine("Connection still usable after partial reads — passed");
        }

        // ------------------------------------------------------------------
        // Statement reuse
        // ------------------------------------------------------------------

        /// <summary>
        /// Execute a query, fully consume results, then execute a different query
        /// on the same statement object. Tests that statement state (operation handle,
        /// result reader, CloudFetch pipeline) is properly reset between executions.
        /// </summary>
        [SkippableFact]
        public async Task StatementReuse_SequentialQueries_ReturnsCorrectResults()
        {
            using var connection = NewConnection();
            using var statement = connection.CreateStatement();

            // Query 1: returns 10 rows
            statement.SqlQuery = "SELECT id FROM RANGE(10)";
            var result1 = statement.ExecuteQuery();
            using (var reader1 = result1.Stream)
            {
                int rows = 0;
                while (await reader1.ReadNextRecordBatchAsync() != null) rows++;
                // rows is batch count, not row count — but we just need it to complete
            }
            OutputHelper?.WriteLine("Query 1 completed (RANGE 10)");

            // Query 2: returns 5 rows with different schema
            statement.SqlQuery = "SELECT 'hello' AS greeting, 42 AS number FROM RANGE(5)";
            var result2 = statement.ExecuteQuery();
            using (var reader2 = result2.Stream)
            {
                int totalRows = 0;
                RecordBatch? batch;
                while ((batch = await reader2.ReadNextRecordBatchAsync()) != null)
                {
                    totalRows += batch.Length;

                    // Verify schema changed — should have 2 columns, not 1
                    Assert.Equal(2, batch.Schema.FieldsList.Count);
                    Assert.Equal("greeting", batch.Schema.FieldsList[0].Name);
                    Assert.Equal("number", batch.Schema.FieldsList[1].Name);
                }
                Assert.Equal(5, totalRows);
            }
            OutputHelper?.WriteLine("Query 2 completed (different schema, 5 rows)");

            // Query 3: returns 1 row — verify statement still works
            statement.SqlQuery = "SELECT 1 AS val";
            var result3 = statement.ExecuteQuery();
            using (var reader3 = result3.Stream)
            {
                var batch = await reader3.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
                Assert.Equal(1, batch!.Length);
            }
            OutputHelper?.WriteLine("Query 3 completed — statement reuse works correctly");
        }

        /// <summary>
        /// Reuse a statement WITHOUT fully consuming the previous result.
        /// This is a common user mistake — they execute a new query before
        /// reading all results from the previous one. The driver should handle
        /// this gracefully (cancel/close the previous operation).
        /// </summary>
        [SkippableFact]
        public async Task StatementReuse_WithoutConsumingPreviousResult_ShouldWork()
        {
            using var connection = NewConnection();
            using var statement = connection.CreateStatement();

            // Query 1: start but don't fully consume
            statement.SqlQuery = "SELECT * FROM RANGE(100000)";
            var result1 = statement.ExecuteQuery();
            using (var reader1 = result1.Stream)
            {
                // Read just one batch
                var batch = await reader1.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
                OutputHelper?.WriteLine($"Query 1: read first batch ({batch!.Length} rows), not consuming rest");
                // DON'T read remaining batches — let it be abandoned
            }

            // Query 2: execute on the same statement — previous result should be cleaned up
            statement.SqlQuery = "SELECT 'reused' AS status";
            var result2 = statement.ExecuteQuery();
            using (var reader2 = result2.Stream)
            {
                var batch = await reader2.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
                Assert.Equal(1, batch!.Length);

                var col = (StringArray)batch.Column(0);
                Assert.Equal("reused", col.GetString(0));
            }
            OutputHelper?.WriteLine("Query 2 completed after abandoning query 1 — statement reuse works");
        }

        // ------------------------------------------------------------------
        // Error recovery
        // ------------------------------------------------------------------

        /// <summary>
        /// Execute invalid SQL, catch the error, then execute valid SQL on
        /// the same connection. Tests that error handling doesn't leave the
        /// connection in a broken state (stale operation handle, locked semaphore, etc).
        /// </summary>
        [SkippableFact]
        public async Task ErrorRecovery_BadSqlThenGoodSql_ConnectionRecovers()
        {
            using var connection = NewConnection();

            // Execute invalid SQL — should throw
            using (var badStatement = connection.CreateStatement())
            {
                badStatement.SqlQuery = "SELECT * FROM this_table_definitely_does_not_exist_xyz_123";
                var ex = Assert.ThrowsAny<Exception>(() => badStatement.ExecuteQuery());
                OutputHelper?.WriteLine($"Bad SQL threw: {ex.GetType().Name}: {ex.Message.Substring(0, Math.Min(200, ex.Message.Length))}");
            }

            // Execute valid SQL — connection should still work
            using (var goodStatement = connection.CreateStatement())
            {
                goodStatement.SqlQuery = "SELECT 1 AS recovered";
                var result = goodStatement.ExecuteQuery();
                using var reader = result.Stream;
                var batch = await reader.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
                Assert.Equal(1, batch!.Length);
            }
            OutputHelper?.WriteLine("Connection recovered after bad SQL — passed");
        }

        /// <summary>
        /// Execute multiple bad queries in a row, then verify recovery.
        /// Tests that error state doesn't accumulate across failed operations.
        /// </summary>
        [SkippableFact]
        public async Task ErrorRecovery_MultipleBadQueriesThenGood_ConnectionRecovers()
        {
            using var connection = NewConnection();

            string[] badQueries = new[]
            {
                "TOTALLY INVALID SQL",
                "SELECT * FROM nonexistent_catalog.nonexistent_schema.nonexistent_table",
                "SELECT 1 / 0",  // Division by zero — may succeed or fail depending on SQL mode
            };

            int errorCount = 0;
            foreach (var badSql in badQueries)
            {
                try
                {
                    using var statement = connection.CreateStatement();
                    statement.SqlQuery = badSql;
                    var result = statement.ExecuteQuery();
                    using var reader = result.Stream;
                    while (await reader.ReadNextRecordBatchAsync() != null) { }
                    // Some "bad" queries might succeed (e.g., division by zero returns NULL in some modes)
                    OutputHelper?.WriteLine($"Query unexpectedly succeeded: {badSql}");
                }
                catch (Exception ex)
                {
                    errorCount++;
                    OutputHelper?.WriteLine($"Error {errorCount}: {ex.GetType().Name} for: {badSql}");
                }
            }

            OutputHelper?.WriteLine($"Total errors: {errorCount} out of {badQueries.Length} bad queries");

            // Now verify recovery — run 5 good queries
            for (int i = 0; i < 5; i++)
            {
                using var statement = connection.CreateStatement();
                statement.SqlQuery = $"SELECT {i} AS val";
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                var batch = await reader.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
                Assert.Equal(1, batch!.Length);
            }
            OutputHelper?.WriteLine("5 good queries succeeded after error recovery — passed");
        }

        // ------------------------------------------------------------------
        // Interleaved workloads
        // ------------------------------------------------------------------

        /// <summary>
        /// Alternate between metadata operations and data queries on the same
        /// connection. These use different Thrift RPCs (GetTables/GetColumns vs
        /// ExecuteStatement/FetchResults) and different result reader code paths.
        /// Tests that switching between workload types doesn't corrupt state.
        /// </summary>
        [SkippableFact]
        public async Task InterleavedMetadataAndDataQueries_AllSucceed()
        {
            using var connection = NewConnection();

            for (int i = 0; i < 10; i++)
            {
                switch (i % 4)
                {
                    case 0:
                        // Data query
                        using (var statement = connection.CreateStatement())
                        {
                            statement.SqlQuery = $"SELECT {i} AS iteration, 'data' AS type FROM RANGE(5)";
                            var result = statement.ExecuteQuery();
                            using var reader = result.Stream;
                            int rows = 0;
                            while (true)
                            {
                                var batch = await reader.ReadNextRecordBatchAsync();
                                if (batch == null) break;
                                rows += batch.Length;
                            }
                            Assert.Equal(5, rows);
                            OutputHelper?.WriteLine($"Iteration {i,2}: data query — {rows} rows");
                        }
                        break;

                    case 1:
                        // GetTableTypes metadata
                        using (var tableTypes = connection.GetTableTypes())
                        {
                            int rows = 0;
                            while (await tableTypes.ReadNextRecordBatchAsync() != null) rows++;
                            OutputHelper?.WriteLine($"Iteration {i,2}: GetTableTypes — {rows} batches");
                        }
                        break;

                    case 2:
                        // Another data query with different schema
                        using (var statement = connection.CreateStatement())
                        {
                            statement.SqlQuery = "SELECT CURRENT_TIMESTAMP() AS ts, 'metadata_test' AS source";
                            var result = statement.ExecuteQuery();
                            using var reader = result.Stream;
                            var batch = await reader.ReadNextRecordBatchAsync();
                            Assert.NotNull(batch);
                            Assert.Equal(1, batch!.Length);
                            OutputHelper?.WriteLine($"Iteration {i,2}: timestamp query — 1 row");
                        }
                        break;

                    case 3:
                        // GetInfo metadata
                        using (var info = connection.GetInfo(new List<AdbcInfoCode> { AdbcInfoCode.VendorName }))
                        {
                            int rows = 0;
                            while (await info.ReadNextRecordBatchAsync() != null) rows++;
                            OutputHelper?.WriteLine($"Iteration {i,2}: GetInfo — {rows} batches");
                        }
                        break;
                }
            }

            OutputHelper?.WriteLine("All 10 interleaved operations completed successfully");
        }
    }
}
