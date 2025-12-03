/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
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
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Arrow.Ipc;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
#if NET472
using System.Net;
#endif

namespace Apache.Arrow.Adbc.Benchmarks
{
    /// <summary>
    /// Custom column to display peak memory usage in the benchmark results table.
    /// </summary>
    public class PeakMemoryColumn : IColumn
    {
        public string Id => nameof(PeakMemoryColumn);
        public string ColumnName => "Peak Memory (MB)";
        public string Legend => "Peak private memory usage during benchmark execution";
        public UnitType UnitType => UnitType.Size;
        public bool AlwaysShow => true;
        public ColumnCategory Category => ColumnCategory.Custom;
        public int PriorityInCategory => 0;
        public bool IsNumeric => true;
        public bool IsAvailable(Summary summary) => true;
        public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => false;

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase)
        {
            if (benchmarkCase.Descriptor.Type == typeof(CloudFetchRealE2EBenchmark))
            {
                try
                {
                    var readDelayParam = benchmarkCase.Parameters["ReadDelayMs"];
                    var queryParam = benchmarkCase.Parameters["benchmarkQuery"] as BenchmarkQuery;
                    string queryName = queryParam?.name ?? "default";
                    string key = $"ExecuteLargeQuery_{queryName}_{readDelayParam}";

                    string metricsFilePath = Path.Combine(Path.GetTempPath(), "cloudfetch_benchmark_metrics.json");
                    if (File.Exists(metricsFilePath))
                    {
                        string json = File.ReadAllText(metricsFilePath);
                        var allMetrics = JsonSerializer.Deserialize<Dictionary<string, BenchmarkMetrics>>(json);
                        if (allMetrics != null && allMetrics.TryGetValue(key, out var metrics))
                        {
                            return $"{metrics.PeakMemoryMB:F2}";
                        }
                    }
                }
                catch (Exception ex)
                {
                    return $"Error: {ex.Message}";
                }
            }

            return "See previous console output";
        }

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style)
        {
            return GetValue(summary, benchmarkCase);
        }

        public override string ToString() => ColumnName;
    }

    /// <summary>
    /// Custom column to display total rows processed in the benchmark results table.
    /// </summary>
    public class TotalRowsColumn : IColumn
    {
        public string Id => nameof(TotalRowsColumn);
        public string ColumnName => "Total Rows";
        public string Legend => "Total number of rows processed during benchmark";
        public UnitType UnitType => UnitType.Dimensionless;
        public bool AlwaysShow => true;
        public ColumnCategory Category => ColumnCategory.Custom;
        public int PriorityInCategory => 1;
        public bool IsNumeric => true;
        public bool IsAvailable(Summary summary) => true;
        public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => false;

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase)
        {
            if (benchmarkCase.Descriptor.Type == typeof(CloudFetchRealE2EBenchmark))
            {
                try
                {
                    var readDelayParam = benchmarkCase.Parameters["ReadDelayMs"];
                    var queryParam = benchmarkCase.Parameters["benchmarkQuery"] as BenchmarkQuery;
                    string queryName = queryParam?.name ?? "default";
                    string key = $"ExecuteLargeQuery_{queryName}_{readDelayParam}";

                    string metricsFilePath = Path.Combine(Path.GetTempPath(), "cloudfetch_benchmark_metrics.json");
                    if (File.Exists(metricsFilePath))
                    {
                        string json = File.ReadAllText(metricsFilePath);
                        var allMetrics = JsonSerializer.Deserialize<Dictionary<string, BenchmarkMetrics>>(json);
                        if (allMetrics != null && allMetrics.TryGetValue(key, out var metrics))
                        {
                            return metrics.TotalRows.ToString("N0");
                        }
                    }
                }
                catch (Exception ex)
                {
                    return $"Error: {ex.Message}";
                }
            }

            return "N/A";
        }

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style)
        {
            return GetValue(summary, benchmarkCase);
        }

        public override string ToString() => ColumnName;
    }

    /// <summary>
    /// Custom column to display total batches processed in the benchmark results table.
    /// </summary>
    public class TotalBatchesColumn : IColumn
    {
        public string Id => nameof(TotalBatchesColumn);
        public string ColumnName => "Total Batches";
        public string Legend => "Total number of Arrow batches processed during benchmark";
        public UnitType UnitType => UnitType.Dimensionless;
        public bool AlwaysShow => true;
        public ColumnCategory Category => ColumnCategory.Custom;
        public int PriorityInCategory => 2;
        public bool IsNumeric => true;
        public bool IsAvailable(Summary summary) => true;
        public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => false;

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase)
        {
            if (benchmarkCase.Descriptor.Type == typeof(CloudFetchRealE2EBenchmark))
            {
                try
                {
                    var readDelayParam = benchmarkCase.Parameters["ReadDelayMs"];
                    var queryParam = benchmarkCase.Parameters["benchmarkQuery"] as BenchmarkQuery;
                    string queryName = queryParam?.name ?? "default";
                    string key = $"ExecuteLargeQuery_{queryName}_{readDelayParam}";

                    string metricsFilePath = Path.Combine(Path.GetTempPath(), "cloudfetch_benchmark_metrics.json");
                    if (File.Exists(metricsFilePath))
                    {
                        string json = File.ReadAllText(metricsFilePath);
                        var allMetrics = JsonSerializer.Deserialize<Dictionary<string, BenchmarkMetrics>>(json);
                        if (allMetrics != null && allMetrics.TryGetValue(key, out var metrics))
                        {
                            return metrics.TotalBatches.ToString("N0");
                        }
                    }
                }
                catch (Exception ex)
                {
                    return $"Error: {ex.Message}";
                }
            }

            return "N/A";
        }

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style)
        {
            return GetValue(summary, benchmarkCase);
        }

        public override string ToString() => ColumnName;
    }


    /// <summary>
    /// Custom column to display estimated GC time percentage in the benchmark results table.
    /// </summary>
    public class GCTimePercentageColumn : IColumn
    {
        public string Id => nameof(GCTimePercentageColumn);
        public string ColumnName => "GC Time %";
        public string Legend => "Percentage of time spent in garbage collection (precise on .NET 6+, estimated on older versions)";
        public UnitType UnitType => UnitType.Dimensionless;
        public bool AlwaysShow => true;
        public ColumnCategory Category => ColumnCategory.Custom;
        public int PriorityInCategory => 3;
        public bool IsNumeric => true;
        public bool IsAvailable(Summary summary) => true;
        public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => false;

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase)
        {
            if (benchmarkCase.Descriptor.Type == typeof(CloudFetchRealE2EBenchmark))
            {
                try
                {
                    var readDelayParam = benchmarkCase.Parameters["ReadDelayMs"];
                    var queryParam = benchmarkCase.Parameters["benchmarkQuery"] as BenchmarkQuery;
                    string queryName = queryParam?.name ?? "default";
                    string key = $"ExecuteLargeQuery_{queryName}_{readDelayParam}";

                    string metricsFilePath = Path.Combine(Path.GetTempPath(), "cloudfetch_benchmark_metrics.json");
                    if (File.Exists(metricsFilePath))
                    {
                        string json = File.ReadAllText(metricsFilePath);
                        var allMetrics = JsonSerializer.Deserialize<Dictionary<string, BenchmarkMetrics>>(json);
                        if (allMetrics != null && allMetrics.TryGetValue(key, out var metrics))
                        {
                            return $"{metrics.GCTimePercentage:F2}";
                        }
                    }
                }
                catch (Exception ex)
                {
                    return $"Error: {ex.Message}";
                }
            }

            return "N/A";
        }

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style)
        {
            return GetValue(summary, benchmarkCase);
        }

        public override string ToString() => ColumnName;
    }

    /// <summary>
    /// Metrics collected during benchmark execution.
    /// </summary>
    internal class BenchmarkMetrics
    {
        public double PeakMemoryMB { get; set; }
        public long TotalRows { get; set; }
        public long TotalBatches { get; set; }
        public double GCTimePercentage { get; set; }
    }








    /// <summary>
    /// Configuration model for Databricks test configuration JSON file.
    /// </summary>
    internal class DatabricksTestConfig
    {
        public string? uri { get; set; }
        public string? token { get; set; }
        public string? auth_type { get; set; }
        public string? grant_type { get; set; }
        public string? client_id { get; set; }
        public string? client_secret { get; set; }
        public string? query { get; set; }
        public string? type { get; set; }
        public string? catalog { get; set; }
        public string? schema { get; set; }
    }

    /// <summary>
    /// Query definition for benchmark suite.
    /// </summary>
    internal class BenchmarkQuery
    {
        public string name { get; set; } = string.Empty;
        public string description { get; set; } = string.Empty;
        public string query { get; set; } = string.Empty;
        public int expected_rows { get; set; }
        public int columns { get; set; }
        public string category { get; set; } = string.Empty;
    }

    /// <summary>
    /// Real E2E performance benchmark for Databricks CloudFetch with actual cluster.
    ///
    /// Prerequisites:
    /// - Set DATABRICKS_TEST_CONFIG_FILE environment variable
    /// - Config file should contain cluster connection details
    ///
    /// Run with: dotnet run -c Release --project Benchmarks/Benchmarks.csproj --framework net8.0 -- --filter "*CloudFetchRealE2E*" --job dry
    ///
    /// Measures:
    /// - Peak memory usage
    /// - Total allocations
    /// - GC collections
    /// - Query execution time
    /// - Row processing throughput
    ///
    /// Parameters:
    /// - ReadDelayMs: Fixed at 5 milliseconds per 10K rows to simulate Power BI consumption
    /// </summary>
    [MemoryDiagnoser]
    [GcServer(true)]
    [SimpleJob(warmupCount: 1, iterationCount: 3)]
    [MinColumn, MaxColumn, MeanColumn, MedianColumn]
    public class CloudFetchRealE2EBenchmark
    {
        private AdbcConnection? _connection;
        private Process _currentProcess = null!;
        private long _peakMemoryBytes;
        private long _totalRows;
        private long _totalBatches;
        private TimeSpan _initialProcessorTime;
        private long _initialAllocatedBytes;
        private int _initialGen0Collections;
        private int _initialGen1Collections;
        private int _initialGen2Collections;
#if NET6_0_OR_GREATER
        private TimeSpan _initialGCPauseDuration;
#endif
        private DatabricksTestConfig _testConfig = null!;
        private string _hostname = null!;
        private string _httpPath = null!;
        private static List<BenchmarkQuery>? _benchmarkQueries;
        private string _currentQueryName = "default";

        [Params(5)] // Read delay in milliseconds per 10K rows (5 = simulate Power BI)
        public int ReadDelayMs { get; set; }

        // Load queries from benchmark-queries.json file
        public static IEnumerable<BenchmarkQuery> GetQueries()
        {
            if (_benchmarkQueries != null)
            {
                return _benchmarkQueries;
            }

            // Check for queries file path from environment variable
            string? queriesFile = Environment.GetEnvironmentVariable("DATABRICKS_BENCHMARK_QUERIES_FILE");
            if (string.IsNullOrEmpty(queriesFile))
            {
                // Fall back to single query mode from config file
                string? configFile = Environment.GetEnvironmentVariable("DATABRICKS_TEST_CONFIG_FILE");
                if (string.IsNullOrEmpty(configFile))
                {
                    throw new InvalidOperationException("Either DATABRICKS_BENCHMARK_QUERIES_FILE or DATABRICKS_TEST_CONFIG_FILE must be set");
                }

                string configJson = File.ReadAllText(configFile);
                var config = JsonSerializer.Deserialize<DatabricksTestConfig>(configJson);
                if (config == null || string.IsNullOrEmpty(config.query))
                {
                    throw new InvalidOperationException("Config file must contain 'query' field");
                }

                _benchmarkQueries = new List<BenchmarkQuery>
                {
                    new BenchmarkQuery
                    {
                        name = "default",
                        description = "Query from config file",
                        query = config.query,
                        category = "custom"
                    }
                };
                return _benchmarkQueries;
            }

            // Load queries from JSON file
            string queriesJson = File.ReadAllText(queriesFile);
            _benchmarkQueries = JsonSerializer.Deserialize<List<BenchmarkQuery>>(queriesJson)
                ?? throw new InvalidOperationException("Failed to parse queries file");

            return _benchmarkQueries;
        }

        [GlobalSetup]
        public void GlobalSetup()
        {
#if NET472
            // Enable TLS 1.2/1.3 for .NET Framework 4.7.2 (required for modern HTTPS endpoints)
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12 | SecurityProtocolType.Tls11 | (SecurityProtocolType)3072; // 3072 = Tls13
#endif
            // Check if Databricks config is available
            string? configFile = Environment.GetEnvironmentVariable("DATABRICKS_TEST_CONFIG_FILE");
            if (string.IsNullOrEmpty(configFile))
            {
                throw new InvalidOperationException(
                    "DATABRICKS_TEST_CONFIG_FILE environment variable must be set. " +
                    "Set it to the path of your Databricks test configuration JSON file.");
            }

            // Read and parse config file
            string configJson = File.ReadAllText(configFile);
            _testConfig = JsonSerializer.Deserialize<DatabricksTestConfig>(configJson)
                ?? throw new InvalidOperationException("Failed to parse config file");

            if (string.IsNullOrEmpty(_testConfig.uri))
            {
                throw new InvalidOperationException("Config file must contain 'uri' field");
            }

            // Validate authentication: either token or OAuth client credentials
            bool hasToken = !string.IsNullOrEmpty(_testConfig.token);
            bool hasOAuth = !string.IsNullOrEmpty(_testConfig.client_id) && !string.IsNullOrEmpty(_testConfig.client_secret);

            if (!hasToken && !hasOAuth)
            {
                throw new InvalidOperationException(
                    "Config file must contain either 'token' field or both 'client_id' and 'client_secret' fields for OAuth authentication");
            }

            if (string.IsNullOrEmpty(_testConfig.query))
            {
                throw new InvalidOperationException("Config file must contain 'query' field");
            }

            // Parse URI to extract hostname and http_path
            // Format: https://hostname/sql/1.0/warehouses/xxx
            var uri = new Uri(_testConfig.uri);
            _hostname = uri.Host;
            _httpPath = uri.PathAndQuery;

            _currentProcess = Process.GetCurrentProcess();
            Console.WriteLine($"Loaded config from: {configFile}");
            Console.WriteLine($"Hostname: {_hostname}");
            Console.WriteLine($"HTTP Path: {_httpPath}");
            Console.WriteLine($"Benchmark will test CloudFetch with {ReadDelayMs}ms per 10K rows read delay");

            // Load queries to validate they exist
            var queries = GetQueries().ToList();
            Console.WriteLine($"Loaded {queries.Count} benchmark queries");
        }

        [IterationSetup]
        public void IterationSetup()
        {
            // Create connection for this iteration using config values
            var parameters = new Dictionary<string, string>
            {
                [AdbcOptions.Uri] = _testConfig.uri!,
                [DatabricksParameters.UseCloudFetch] = "true",
                [DatabricksParameters.EnableDirectResults] = "true",
                [DatabricksParameters.CanDecompressLz4] = "true",
                [DatabricksParameters.MaxBytesPerFile] = "10485760", // 10MB per file
            };

            // Add authentication parameters based on config
            if (!string.IsNullOrEmpty(_testConfig.token))
            {
                // Token-based authentication
                parameters[SparkParameters.Token] = _testConfig.token!;
            }
            else if (!string.IsNullOrEmpty(_testConfig.client_id) && !string.IsNullOrEmpty(_testConfig.client_secret))
            {
                // OAuth client credentials authentication
                parameters[SparkParameters.AuthType] = "oauth";
                parameters[DatabricksParameters.OAuthGrantType] = "client_credentials";
                parameters[DatabricksParameters.OAuthClientId] = _testConfig.client_id!;
                parameters[DatabricksParameters.OAuthClientSecret] = _testConfig.client_secret!;
            }

            var driver = new DatabricksDriver();
            var database = driver.Open(parameters);
            _connection = database.Connect(parameters);

            // Reset memory state for clean benchmark
            GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: false);
            GC.WaitForPendingFinalizers();
            GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: false);
            _currentProcess.Refresh();
            _peakMemoryBytes = _currentProcess.PrivateMemorySize64;
            _totalRows = 0;
            _totalBatches = 0;

            // Capture initial GC and process metrics
            _initialProcessorTime = _currentProcess.TotalProcessorTime;
            _initialAllocatedBytes = GC.GetTotalMemory(forceFullCollection: false);
            _initialGen0Collections = GC.CollectionCount(0);
            _initialGen1Collections = GC.CollectionCount(1);
            _initialGen2Collections = GC.CollectionCount(2);
#if NET6_0_OR_GREATER
            _initialGCPauseDuration = GC.GetTotalPauseDuration();
#endif
        }

        [IterationCleanup]
        public void IterationCleanup()
        {
            _connection?.Dispose();
            _connection = null;

            // Calculate final metrics
            double peakMemoryMB = _peakMemoryBytes / 1024.0 / 1024.0;
            _currentProcess.Refresh();
            var finalProcessorTime = _currentProcess.TotalProcessorTime;
            var finalAllocatedBytes = GC.GetTotalMemory(forceFullCollection: false);
            var finalGen0Collections = GC.CollectionCount(0);
            var finalGen1Collections = GC.CollectionCount(1);
            var finalGen2Collections = GC.CollectionCount(2);

            // Calculate deltas
            double processorTimeMs = (finalProcessorTime - _initialProcessorTime).TotalMilliseconds;
            long totalAllocatedBytes = finalAllocatedBytes - _initialAllocatedBytes;
            int gen0Collections = finalGen0Collections - _initialGen0Collections;
            int gen1Collections = finalGen1Collections - _initialGen1Collections;
            int gen2Collections = finalGen2Collections - _initialGen2Collections;

            // Calculate GC time percentage
#if NET6_0_OR_GREATER
            // Use precise GC pause duration on .NET 6+
            var finalGCPauseDuration = GC.GetTotalPauseDuration();
            var gcPauseTime = finalGCPauseDuration - _initialGCPauseDuration;
            double gcTimePercentage = processorTimeMs > 0 ?
                (gcPauseTime.TotalMilliseconds / processorTimeMs) * 100.0 : 0.0;
#else
            // Estimate GC time percentage (rough approximation based on collection counts)
            int totalCollections = gen0Collections + gen1Collections + gen2Collections;
            double gcTimePercentage = totalCollections > 0 ?
                Math.Min((totalCollections * 0.1), 5.0) : 0.0; // Cap at 5% as rough estimate
#endif

            // Print metrics for this iteration
            Console.WriteLine($"CloudFetch E2E [{_currentQueryName}, Delay={ReadDelayMs}ms/10K rows] - Peak memory: {peakMemoryMB:F2} MB, Total rows: {_totalRows:N0}, Total batches: {_totalBatches:N0}");
#if NET6_0_OR_GREATER
            Console.WriteLine($"  Process time: {processorTimeMs:F2} ms, Total allocated: {(totalAllocatedBytes/1024.0/1024.0):F2} MB, GC time: {gcTimePercentage:F2}% (precise)");
#else
            Console.WriteLine($"  Process time: {processorTimeMs:F2} ms, Total allocated: {(totalAllocatedBytes/1024.0/1024.0):F2} MB, GC time: {gcTimePercentage:F2}% (estimated)");
#endif

            // Save metrics to temp file for the custom columns
            // Key includes both query name and read delay to uniquely identify each benchmark combination
            string key = $"ExecuteLargeQuery_{_currentQueryName}_{ReadDelayMs}";
            var metrics = new BenchmarkMetrics
            {
                PeakMemoryMB = peakMemoryMB,
                TotalRows = _totalRows,
                TotalBatches = _totalBatches,
                GCTimePercentage = gcTimePercentage
            };

            // Load existing metrics or create new dictionary
            string metricsFilePath = Path.Combine(Path.GetTempPath(), "cloudfetch_benchmark_metrics.json");
            Dictionary<string, BenchmarkMetrics> allMetrics;

            if (File.Exists(metricsFilePath))
            {
                try
                {
                    string existingJson = File.ReadAllText(metricsFilePath);
                    allMetrics = JsonSerializer.Deserialize<Dictionary<string, BenchmarkMetrics>>(existingJson)
                        ?? new Dictionary<string, BenchmarkMetrics>();
                }
                catch
                {
                    allMetrics = new Dictionary<string, BenchmarkMetrics>();
                }
            }
            else
            {
                allMetrics = new Dictionary<string, BenchmarkMetrics>();
            }

            // Update with current metrics and save
            allMetrics[key] = metrics;
            string json = JsonSerializer.Serialize(allMetrics, new JsonSerializerOptions { WriteIndented = true });
            File.WriteAllText(metricsFilePath, json);
        }

        /// <summary>
        /// Execute a large query against Databricks and consume all result batches.
        /// Simulates client behavior like Power BI reading data.
        /// Uses the query from the config file or benchmark queries file.
        /// </summary>
        [Benchmark]
        [ArgumentsSource(nameof(GetQueries))]
        public async Task<long> ExecuteLargeQuery(BenchmarkQuery benchmarkQuery)
        {
            if (_connection == null)
            {
                throw new InvalidOperationException("Connection not initialized");
            }

            // Store current query name for metrics
            _currentQueryName = benchmarkQuery.name;

            Console.WriteLine($"\n=== Running query: {benchmarkQuery.name} ===");
            Console.WriteLine($"Description: {benchmarkQuery.description}");

            // Execute query
            var statement = _connection.CreateStatement();
            statement.SqlQuery = benchmarkQuery.query;

            var result = await statement.ExecuteQueryAsync();
            if (result.Stream == null)
            {
                throw new InvalidOperationException("Result stream is null");
            }

            // Read all batches and track peak memory
            RecordBatch? batch;

            while ((batch = await result.Stream.ReadNextRecordBatchAsync()) != null)
            {
                _totalRows += batch.Length;
                _totalBatches++;

                // Track peak memory periodically
                if (_totalBatches % 10 == 0)
                {
                    TrackPeakMemory();
                }

                // Simulate Power BI processing delay if configured
                // Delay is proportional to batch size: ReadDelayMs per 10K rows
                if (ReadDelayMs > 0)
                {
                    int delayForBatch = (int)((batch.Length / 10000.0) * ReadDelayMs);
                    if (delayForBatch > 0)
                    {
                        Thread.Sleep(delayForBatch);
                    }
                }

                batch.Dispose();
            }

            // Final peak memory check
            TrackPeakMemory();

            statement.Dispose();
            return _totalRows;
        }


        private void TrackPeakMemory()
        {
            _currentProcess.Refresh();
            long currentMemory = _currentProcess.PrivateMemorySize64;
            if (currentMemory > _peakMemoryBytes)
            {
                _peakMemoryBytes = currentMemory;
            }
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
            GC.WaitForPendingFinalizers();
        }
    }
}
