<!--
  Copyright (c) 2025 ADBC Drivers Contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# ADBC Drivers for Databricks

This repository contains [ADBC drivers](https://arrow.apache.org/adbc/) for
Databricks, implemented in different languages.

## Installation

At this time pre-packaged drivers are not yet available.

## Usage

At this time user documentation is not yet available.

## Benchmarking

The C# driver includes comprehensive benchmarks for performance testing:

```bash
# Run CloudFetch E2E benchmarks with .NET 8.0
cd csharp/Benchmarks
dotnet run -c Release -f net8.0 -- --filter "*CloudFetchRealE2E*"

# Run with .NET Framework 4.7.2 (Windows only)
dotnet run -c Release -f net472 -- --filter "*CloudFetchRealE2E*"
```

**Prerequisites:**
- Set `DATABRICKS_TEST_CONFIG_FILE` environment variable pointing to a JSON config file
- Config file should contain: `{"uri": "...", "token": "...", "query": "..."}`

**Benchmark Metrics:**
- Peak Memory (MB) - Peak private memory usage
- Total Rows/Batches - Actual data processed
- GC Time % - Precise GC overhead (.NET 6+) or estimated (older versions)
- Standard BenchmarkDotNet metrics (Gen0/1/2 collections, allocated memory, execution time)

## Building

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
