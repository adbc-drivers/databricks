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

The C# driver includes a comprehensive benchmark suite for CloudFetch performance testing using 7 TPC-DS queries that cover different data characteristics (size, width, data types).

**View Results:**
- **GitHub Pages Dashboard**: https://adbc-drivers.github.io/databricks/bench/
- Interactive charts tracking Mean Execution Time, Peak Memory, Allocated Memory, and Gen2 Collections
- Historical trends across commits for .NET 8.0 and .NET Framework 4.7.2

**Running Benchmarks:**

Locally:
```bash
cd csharp/Benchmarks
dotnet run -c Release -f net8.0
```

On Pull Requests:
- Add the `benchmark` label to your PR to run the full suite
- Results automatically posted as comparison comments

For detailed documentation, see [csharp/Benchmarks/README.md](csharp/Benchmarks/README.md) and [csharp/Benchmarks/benchmark-queries.md](csharp/Benchmarks/benchmark-queries.md).

## Building

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
