/*
* Quick validation script for CloudFetch parity changes.
* Runs catalog_returns SF10 query and reports row count + timing.
*/

using System;
using System.Collections.Generic;
using System.Diagnostics;
using AdbcDrivers.Databricks;
using AdbcDrivers.HiveServer2.Spark;

namespace Apache.Arrow.Adbc.Benchmarks
{
    public class QuickValidation
    {
        public static void RunValidation()
        {
            string uri = Environment.GetEnvironmentVariable("DATABRICKS_URI")
                ?? throw new InvalidOperationException("Set DATABRICKS_URI env var (e.g. https://host/sql/1.0/warehouses/id)");
            string token = Environment.GetEnvironmentVariable("DATABRICKS_TOKEN")
                ?? throw new InvalidOperationException("Set DATABRICKS_TOKEN env var");
            string query = Environment.GetEnvironmentVariable("DATABRICKS_QUERY")
                ?? "SELECT * FROM main.tpcds_sf10_delta.catalog_sales";
            long expectedRows = long.Parse(
                Environment.GetEnvironmentVariable("DATABRICKS_EXPECTED_ROWS") ?? "14400425");

            Console.WriteLine("=== ADBC CloudFetch Validation ===");
            Console.WriteLine($"Query: {query}");
            Console.WriteLine($"Expected rows: {expectedRows}");

            var parameters = new Dictionary<string, string>
            {
                [AdbcOptions.Uri] = uri,
                [SparkParameters.Token] = token,
                [DatabricksParameters.UseCloudFetch] = "true",
                [DatabricksParameters.EnableDirectResults] = "true",
                [DatabricksParameters.CanDecompressLz4] = "true",
            };

            var driver = new DatabricksDriver();
            using var database = driver.Open(parameters);
            using var connection = database.Connect(parameters);
            using var statement = connection.CreateStatement();

            statement.SqlQuery = query;

            var execSw = Stopwatch.StartNew();
            var queryResult = statement.ExecuteQuery();
            long execMs = execSw.ElapsedMilliseconds;
            Console.WriteLine($"QUERY_EXEC_MS={execMs}");

            var sw = Stopwatch.StartNew();
            long totalRows = 0;
            int totalBatches = 0;
            long ttfrMs = 0;
            long readerWaitMs = 0;
            long batchProcessMs = 0;

            using (var reader = queryResult.Stream)
            {
                while (true)
                {
                    var waitSw = Stopwatch.StartNew();
                    var batch = reader.ReadNextRecordBatchAsync().Result;
                    waitSw.Stop();

                    if (batch == null) break;

                    readerWaitMs += waitSw.ElapsedMilliseconds;

                    var procSw = Stopwatch.StartNew();
                    totalBatches++;
                    totalRows += batch.Length;

                    if (totalBatches == 1)
                    {
                        ttfrMs = sw.ElapsedMilliseconds;
                        Console.WriteLine($"TTFR_MS={ttfrMs}");
                    }

                    batch.Dispose();
                    procSw.Stop();
                    batchProcessMs += procSw.ElapsedMilliseconds;
                }
            }

            sw.Stop();
            Console.WriteLine();
            Console.WriteLine("=== Reader-Side Timing ===");
            Console.WriteLine($"  Query execution:      {execMs:N0} ms");
            Console.WriteLine($"  Reader total:         {sw.ElapsedMilliseconds:N0} ms");
            Console.WriteLine($"  Reader wait (chunks): {readerWaitMs:N0} ms");
            Console.WriteLine($"  Batch process+dispose:{batchProcessMs:N0} ms");
            Console.WriteLine($"  Overhead (framework): {sw.ElapsedMilliseconds - readerWaitMs - batchProcessMs:N0} ms");
            Console.WriteLine();

            Console.WriteLine($"TOTAL_ROWS={totalRows}");
            Console.WriteLine($"TOTAL_BATCHES={totalBatches}");
            Console.WriteLine($"TOTAL_TIME_MS={sw.ElapsedMilliseconds}");
            Console.WriteLine($"ROWS_PER_SEC={totalRows / (sw.ElapsedMilliseconds / 1000.0):F0}");

            if (totalRows != expectedRows)
            {
                Console.Error.WriteLine($"ROW COUNT MISMATCH! Expected {expectedRows}, got {totalRows}");
                Environment.Exit(1);
            }

            Console.WriteLine("Row count verified OK.");
        }
    }
}
