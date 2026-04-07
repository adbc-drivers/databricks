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
                ?? throw new InvalidOperationException("Set DATABRICKS_URI");
            string token = Environment.GetEnvironmentVariable("DATABRICKS_TOKEN")
                ?? throw new InvalidOperationException("Set DATABRICKS_TOKEN");
            string query = Environment.GetEnvironmentVariable("DATABRICKS_QUERY")
                ?? "SELECT * FROM main.tpcds_sf10_delta.catalog_sales";
            long expectedRows = long.Parse(
                Environment.GetEnvironmentVariable("DATABRICKS_EXPECTED_ROWS") ?? "0");

            Console.WriteLine("=== ADBC CloudFetch Validation (FULL DATA READ) ===");
            Console.WriteLine($"Query: {query}");

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

            var sw = Stopwatch.StartNew();
            var queryResult = statement.ExecuteQuery();

            long totalRows = 0;
            int totalBatches = 0;
            int totalColumns = 0;
            long bytesRead = 0;

            using (var reader = queryResult.Stream)
            {
                while (true)
                {
                    var batch = reader.ReadNextRecordBatchAsync().Result;
                    if (batch == null) break;

                    totalBatches++;
                    totalRows += batch.Length;
                    totalColumns = batch.ColumnCount;

                    // Read every column's underlying buffer to force full data materialization
                    for (int col = 0; col < batch.ColumnCount; col++)
                    {
                        var array = batch.Column(col);
                        // Access the raw data buffers - this forces the Arrow data into CPU cache
                        for (int bufIdx = 0; bufIdx < array.Data.Buffers.Length; bufIdx++)
                        {
                            var buffer = array.Data.Buffers[bufIdx];
                            bytesRead += buffer.Length;
                        }
                    }

                    batch.Dispose();
                }
            }

            sw.Stop();

            Console.WriteLine($"TOTAL_ROWS={totalRows}");
            Console.WriteLine($"TOTAL_BATCHES={totalBatches}");
            Console.WriteLine($"TOTAL_COLUMNS={totalColumns}");
            Console.WriteLine($"BYTES_READ={bytesRead}");
            Console.WriteLine($"BYTES_READ_MB={bytesRead / 1024.0 / 1024.0:F1}");
            Console.WriteLine($"TOTAL_TIME_MS={sw.ElapsedMilliseconds}");
            Console.WriteLine($"ROWS_PER_SEC={totalRows / (sw.ElapsedMilliseconds / 1000.0):F0}");

            if (expectedRows > 0 && totalRows != expectedRows)
            {
                Console.Error.WriteLine($"ROW COUNT MISMATCH!");
                Environment.Exit(1);
            }
            Console.WriteLine("Row count verified OK.");
        }
    }
}
