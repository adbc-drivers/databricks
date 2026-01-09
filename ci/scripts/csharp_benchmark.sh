#!/usr/bin/env bash
#
# Copyright (c) 2025 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -ex

# Run Databricks CloudFetch benchmarks
# Usage: csharp_benchmark.sh <workspace_dir> <framework>
# Example: csharp_benchmark.sh /path/to/workspace net8.0

workspace_dir=${1}
framework=${2:-net8.0}  # Default to net8.0 if not specified

benchmark_dir=${workspace_dir}/csharp/Benchmarks

# Validate framework parameter
if [[ "$framework" != "net8.0" && "$framework" != "net472" ]]; then
  echo "Error: Invalid framework '$framework'. Must be 'net8.0' or 'net472'"
  exit 1
fi

# Ensure DATABRICKS_TEST_CONFIG_FILE is set
if [ -z "$DATABRICKS_TEST_CONFIG_FILE" ]; then
  echo "Error: DATABRICKS_TEST_CONFIG_FILE environment variable is not set"
  exit 1
fi

if [ ! -f "$DATABRICKS_TEST_CONFIG_FILE" ]; then
  echo "Error: Config file not found at $DATABRICKS_TEST_CONFIG_FILE"
  exit 1
fi

echo "=================================================="
echo "Running Databricks CloudFetch Benchmarks"
echo "Framework: $framework"
echo "Config: $DATABRICKS_TEST_CONFIG_FILE"
echo "=================================================="

pushd ${benchmark_dir}

# Run the CloudFetch E2E benchmark
dotnet run -c Release --project DatabricksBenchmarks.csproj --framework $framework -- \
  --filter "*CloudFetchRealE2E*" \
  --exporters json

echo ""
echo "=================================================="
echo "Benchmark completed successfully!"
echo "Results saved to: ${benchmark_dir}/BenchmarkDotNet.Artifacts/results/"
echo "=================================================="

popd
