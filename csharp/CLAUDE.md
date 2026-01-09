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

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This repository contains ADBC (Apache Arrow Database Connectivity) drivers for Databricks, implemented in C#. The driver is built on top of the Apache Spark ADBC driver and provides Databricks-specific functionality including OAuth authentication, CloudFetch for high-performance result retrieval, and comprehensive Databricks SQL support.

## Building and Testing

### C# Driver

The C# implementation is located in the `csharp/` directory. Build and test using standard .NET commands:

```shell
# Build the driver
cd csharp/src
dotnet build Apache.Arrow.Adbc.Drivers.Databricks.csproj

# Run tests
cd csharp/test
dotnet test Apache.Arrow.Adbc.Tests.Drivers.Databricks.csproj

# Run specific test
dotnet test --filter "FullyQualifiedName~TestName"
```

The driver targets multiple frameworks: `netstandard2.0`, `net472`, and `net8.0`.

### Pre-commit Checks

Before opening a pull request, run static checks using pre-commit:

```shell
pre-commit run --all-files
```

Note: Ensure all changes are staged/committed as unstaged changes will be ignored.

## Architecture

### Driver Hierarchy

The codebase follows a layered architecture:

```
DatabricksDriver (entry point)
  ↓
DatabricksDatabase
  ↓
DatabricksConnection (extends SparkHttpConnection)
  ↓
DatabricksStatement (extends SparkStatement)
```

Key classes:
- `DatabricksDriver`: Entry point implementing `AdbcDriver`
- `DatabricksConnection`: Manages connection lifecycle, authentication, and HTTP communication (csharp/src/DatabricksConnection.cs)
- `DatabricksStatement`: Handles query execution and result fetching (csharp/src/DatabricksStatement.cs)
- `DatabricksConfiguration`: Manages configuration properties with support for environment variable-based config files (csharp/src/DatabricksConfiguration.cs)

### Authentication System

Located in `csharp/src/Auth/`:

- **Token-based auth**: Personal access tokens via `access_token` grant type
- **OAuth client credentials**: M2M authentication via `client_credentials` grant type
- **Token refresh**: Automatic token renewal handled by `TokenRefreshDelegatingHandler`
- **Workload identity federation**: Supported via mandatory token exchange using `MandatoryTokenExchangeDelegatingHandler`

The authentication pipeline uses HTTP delegating handlers to inject and refresh tokens transparently.

### CloudFetch Pipeline

CloudFetch is Databricks' high-performance result retrieval system that downloads results directly from cloud storage. Located in `csharp/src/Reader/CloudFetch/`:

**Pipeline Design** (see cloudfetch-pipeline-design.md):
1. `CloudFetchResultFetcher`: Background worker that fetches result metadata from Thrift server
2. `CloudFetchDownloadManager`: Orchestrates parallel downloads while respecting memory limits
3. `CloudFetchMemoryBufferManager`: Enforces memory limits for buffered files
4. `CloudFetchDownloader`: Manages download queue and provides files to the reader
5. `CloudFetchReader`: Consumes downloaded Arrow files

Key features:
- Parallel downloads (configurable, default: 3)
- Prefetching (configurable, default: 2 files)
- Memory buffering (default: 200MB)
- LZ4 decompression support

The pipeline uses concurrent queues with `DownloadResult` objects to coordinate async file downloads without blocking the reader.

### Result Readers

Located in `csharp/src/Reader/`:

- `DatabricksCompositeReader`: Orchestrates multiple result sources (CloudFetch + direct results)
- `DatabricksReader`: Handles standard Thrift-based results
- `BaseDatabricksReader`: Base class for common reader functionality
- `DatabricksOperationStatusPoller`: Polls query execution status with configurable intervals (default: 100ms)

### Configuration

The driver supports multiple configuration methods:

1. **Direct properties**: Pass properties when creating connection
2. **Environment variables**: Load JSON config via `DATABRICKS_CONFIG_FILE` environment variable
3. **Hybrid**: Merge both sources with configurable precedence via `adbc.databricks.driver_config_take_precedence`

All configuration values in JSON files must be strings (e.g., `"true"` not `true`, `"4443"` not `4443`).

## Important Implementation Details

### Batch Size
The default batch size is 2,000,000 rows (see `DatabricksBatchSizeDefault` in DatabricksStatement.cs:48), significantly higher than standard Arrow batches, optimized for CloudFetch's 1024MB limit vs standard 10MB limit.

### Polling Interval
Query status polling defaults to 100ms (vs Apache Spark's 500ms default) for faster query feedback.

### Apache Arrow Submodule
The `csharp/arrow-adbc` directory contains a git submodule pointing to `apache/arrow-adbc`. The driver extends classes from this base implementation.

## Pull Request Guidelines

Ensure PR titles follow [Conventional Commits](https://www.conventionalcommits.org/) format:
- Component should be `csharp` for changes affecting the C# driver
- Examples: `feat(csharp): add CloudFetch prefetching`, `fix(csharp): handle token expiration`
- Use `!` for breaking changes: `fix!(csharp): change default batch size`

End PR descriptions with `Closes #NNN` or `Fixes #NNN` to link issues.

## Testing Notes

Test resources are located in `csharp/test/Resources/` including:
- SQL scripts for test data setup
- JSON files for validation
- Configuration files copied to output during build

Tests use xunit framework with `Xunit.SkippableFact` for conditional test execution.
