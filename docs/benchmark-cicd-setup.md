<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Benchmark CI/CD Setup and Usage Guide

This guide explains how the automated benchmark CI/CD pipeline works and how to set it up for your repository.

## Overview

The benchmark automation system runs CloudFetch E2E performance benchmarks automatically on every commit to the `main` branch. It:

- Executes benchmarks on both .NET 8.0 (Ubuntu) and .NET Framework 4.7.2 (Windows)
- Tracks memory consumption trends: Peak Memory, Allocated Memory, and Gen2 Collections
- Stores detailed results as artifacts
- Alerts maintainers if memory usage increases by 150% or more (2.5x baseline)

## Architecture

### Workflow: `.github/workflows/benchmarks.yml`

The workflow consists of two parallel jobs:

1. **benchmark-net8**: Runs on `ubuntu-latest` with .NET 8.0
2. **benchmark-net472**: Runs on `windows-2022` with .NET Framework 4.7.2

Each job:
1. Checks out the repository (with submodules)
2. Sets up .NET SDK
3. Creates a Databricks configuration file from secrets
4. Builds the driver
5. Runs the benchmark using `ci/scripts/csharp_benchmark.sh`
6. Uploads results as artifacts
7. Extracts performance metrics
8. Stores results for trend tracking

### CI Script: `ci/scripts/csharp_benchmark.sh`

The benchmark script:
- Takes two parameters: workspace directory and target framework
- Validates the framework parameter (net8.0 or net472)
- Checks for the required `DATABRICKS_TEST_CONFIG_FILE` environment variable
- Runs BenchmarkDotNet with JSON, HTML, and CSV exporters
- Saves results to `csharp/Benchmarks/BenchmarkDotNet.Artifacts/results/`

## Repository Setup

### Required GitHub Secrets

The benchmark workflow requires the following secrets to be configured in the repository (under **Settings → Secrets and variables → Actions → Environment secrets** for the `azure-prod` environment):

- `DATABRICKS_HOST`: Databricks workspace hostname (e.g., `adb-xxx.azuredatabricks.net`)
- `TEST_PECO_WAREHOUSE_HTTP_PATH`: SQL warehouse HTTP path (e.g., `/sql/1.0/warehouses/xxx`)
- `DATABRICKS_TOKEN`: Databricks personal access token with SQL execution permissions

### Enabling Trend Tracking (GitHub Pages)

The workflow uses [`benchmark-action/github-action-benchmark`](https://github.com/benchmark-action/github-action-benchmark) to track performance trends over time. To enable this feature:

1. **Enable GitHub Pages** in repository settings:
   - Go to **Settings → Pages**
   - Under "Source", select **Deploy from a branch**
   - Select branch: **gh-pages** and folder: **/ (root)**
   - Click **Save**

2. **Configure GitHub Actions permissions**:
   - Go to **Settings → Actions → General**
   - Under "Workflow permissions", select **Read and write permissions**
   - Check **Allow GitHub Actions to create and approve pull requests**
   - Click **Save**

3. **First run will create gh-pages branch**:
   - The first benchmark run will automatically create the `gh-pages` branch
   - Subsequent runs will push benchmark data to this branch
   - GitHub Pages will be available at: `https://<organization>.github.io/<repository>/bench/net8/` (for net8.0 results)

### Viewing Benchmark Trends

Once GitHub Pages is enabled, you can view interactive performance charts at:

- **.NET 8.0 trends**: `https://<organization>.github.io/<repository>/bench/net8/`
- **.NET Framework 4.7.2 trends**: `https://<organization>.github.io/<repository>/bench/net472/`

These pages display:
- Performance trend charts over time
- Mean, min, and max values
- Historical comparison
- Regression detection

## Running Benchmarks

### Automatic Triggers

Benchmarks run automatically on:
- Every push to the `main` branch (when changes affect benchmark code or driver source)

### Manual Triggers

You can manually trigger a benchmark run via GitHub Actions UI:

1. Go to **Actions → Performance Benchmarks**
2. Click **Run workflow**
3. Select the `main` branch
4. Optionally specify a custom SQL query
5. Click **Run workflow**

### Local Execution

To run benchmarks locally (for development):

```bash
# Set configuration
export DATABRICKS_TEST_CONFIG_FILE=/path/to/config.json

# Run via the CI script
./ci/scripts/csharp_benchmark.sh $(pwd) net8.0

# Or run directly
cd csharp/Benchmarks
dotnet run -c Release --project DatabricksBenchmarks.csproj --framework net8.0 -- --filter "*CloudFetchRealE2E*"
```

## Configuration

### Benchmark Query

The default benchmark query is:
```sql
select * from main.tpcds_sf1_delta.catalog_sales
```

This query processes approximately 1.4M rows and is designed to test CloudFetch with realistic data volumes.

To use a custom query:
1. Trigger the workflow manually (see above)
2. Enter your custom query in the "Custom SQL query" field
3. The query will be added to the config file automatically

### Performance Alert Threshold

The workflow is configured to alert if **memory metrics increase by 150%** or more compared to the previous run (i.e., memory usage reaches 2.5x the baseline). Since lower memory is better, the workflow uses `customSmallerIsBetter` mode.

This threshold can be adjusted in `.github/workflows/benchmarks.yml`:

```yaml
- name: Store benchmark results for trend tracking
  uses: benchmark-action/github-action-benchmark@v1
  with:
    tool: 'customSmallerIsBetter'  # Lower memory is better
    alert-threshold: '150%'        # Alert if memory increases to 2.5x baseline
```

### Timeout

Each benchmark job has a 30-minute timeout. This can be adjusted if needed:

```yaml
jobs:
  benchmark-net8:
    timeout-minutes: 30  # Adjust this value
```

## Interpreting Results

### Artifacts

After each run, detailed results are uploaded as GitHub Actions artifacts (retained for 90 days):

- `benchmark-results-net8`: Results from .NET 8.0 benchmark
- `benchmark-results-net472`: Results from .NET Framework 4.7.2 benchmark

Each artifact contains:
- HTML report (`*-report.html`) - Human-readable summary
- JSON report (`*-report-full-compressed.json`) - Detailed metrics
- CSV export (`*-report.csv`) - Spreadsheet-compatible data

### Key Metrics

The benchmark tracks and monitors trends for the following metrics:

**Tracked in GitHub Pages (trend analysis):**

1. **Peak Memory (MB)**: Maximum working set memory (private bytes) during execution
   - Lower is better
   - Alert threshold: 150% (triggers if memory increases to 2.5x baseline)
   - Source: Custom metrics from `Process.PrivateMemorySize64`

2. **Allocated Memory (MB)**: Total managed memory allocated during execution
   - Lower is better
   - Alert threshold: 150%
   - Source: BenchmarkDotNet's `MemoryDiagnoser`

3. **Gen2 Collections**: Number of full garbage collections
   - Lower is better (indicates less memory pressure)
   - Alert threshold: 150%
   - Source: BenchmarkDotNet's `MemoryDiagnoser`

**Additional metrics in BenchmarkDotNet reports:**

4. **Mean/Median/Min/Max Time**: End-to-end execution time including:
   - Query execution
   - CloudFetch downloads
   - LZ4 decompression
   - Batch consumption with simulated delays

5. **Total Rows/Batches**: Data volume processed

6. **GC Time %**: Percentage of time spent in garbage collection

7. **Gen0/Gen1 Collections**: Minor and partial garbage collections

### Trend Analysis

The GitHub Pages dashboard shows:
- **Performance over time**: Line chart of execution time across commits
- **Regression detection**: Automatic alerts when performance degrades
- **Comparison view**: Compare performance between different commits

## Maintenance

### Updating the Benchmark

When modifying the benchmark code (`csharp/Benchmarks/CloudFetchRealE2EBenchmark.cs`):

1. Test locally first
2. Push changes to a feature branch
3. The workflow will run on merge to `main`
4. Verify results in the GitHub Actions UI

### Troubleshooting

**Issue**: Workflow fails with "Config file not found"
- **Solution**: Verify that GitHub secrets are properly configured in the `azure-prod` environment

**Issue**: Benchmark times out
- **Solution**: Increase the `timeout-minutes` value or use a smaller test query

**Issue**: Trend tracking not working
- **Solution**: Ensure GitHub Pages is enabled and workflow has write permissions

**Issue**: Results not showing in GitHub Pages
- **Solution**: Wait a few minutes after the first run for Pages to deploy, then check the gh-pages branch

## Best Practices

1. **Run benchmarks on stable infrastructure**: The workflow uses GitHub-hosted runners which may have variable performance
2. **Use consistent test data**: The default TPC-DS query provides consistent results
3. **Monitor trends, not absolute values**: Focus on relative changes over time
4. **Review alerts promptly**: Performance regressions should be investigated quickly
5. **Keep benchmarks fast**: Long-running benchmarks slow down the CI pipeline

## Security Considerations

- Databricks credentials are stored as GitHub secrets and never exposed in logs
- The config file is created at runtime and contains credentials only in memory
- Benchmark results do not contain sensitive data (only performance metrics)

## References

- [BenchmarkDotNet Documentation](https://benchmarkdotnet.org/)
- [github-action-benchmark](https://github.com/benchmark-action/github-action-benchmark)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Benchmark README](../csharp/Benchmarks/README.md)
