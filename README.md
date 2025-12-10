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

At this time pre-packaged drivers are not yet available. See [Building](#building) for instructions on building from source.

## Usage

### Overview

The Databricks ADBC driver is a C# implementation built on the Apache Spark ADBC driver foundation. It adds Databricks-specific functionality including:
- **OAuth Authentication**: Support for personal access tokens and client credentials flow
- **CloudFetch**: High-performance result retrieval directly from cloud storage
- **Enhanced Performance**: Optimized batch sizes and polling intervals for Databricks SQL
- **Comprehensive Metadata**: Primary key, foreign key, and catalog support

### Quick Start

```csharp
using Apache.Arrow.Adbc.Drivers.Databricks;

// Create driver and database
var driver = new DatabricksDriver();
var database = driver.Open(new Dictionary<string, string>
{
    ["uri"] = "https://your-workspace.cloud.databricks.com/sql/1.0/warehouses/your-warehouse-id",
    ["adbc.spark.auth_type"] = "oauth",
    ["adbc.databricks.oauth.grant_type"] = "access_token",
    ["adbc.spark.oauth.access_token"] = "your-personal-access-token"
});

// Execute query
using var connection = database.Connect();
using var statement = connection.CreateStatement();
statement.SqlQuery = "SELECT * FROM main.default.my_table LIMIT 10";
using var reader = statement.ExecuteQuery();

// Process results
while (reader.ReadNextRecordBatch())
{
    // Process Arrow RecordBatch
}
```

### Configuration Methods

The driver supports three flexible configuration approaches:

#### 1. Direct Property Configuration
Pass properties directly when creating connections:

```csharp
var config = new Dictionary<string, string>
{
    ["uri"] = "https://workspace.databricks.com/sql/1.0/warehouses/...",
    ["adbc.spark.auth_type"] = "oauth",
    ["adbc.databricks.oauth.grant_type"] = "client_credentials",
    ["adbc.databricks.oauth.client_id"] = "your-client-id",
    ["adbc.databricks.oauth.client_secret"] = "your-client-secret"
};
```

#### 2. Environment Variable Configuration
Create a JSON configuration file (all values must be strings):

```json
{
  "uri": "https://workspace.databricks.com/sql/1.0/warehouses/...",
  "adbc.spark.auth_type": "oauth",
  "adbc.databricks.oauth.grant_type": "client_credentials",
  "adbc.databricks.oauth.client_id": "your-client-id",
  "adbc.databricks.oauth.client_secret": "your-client-secret",
  "adbc.connection.catalog": "main",
  "adbc.connection.db_schema": "default"
}
```

Set the environment variable:
```bash
export DATABRICKS_CONFIG_FILE="/path/to/config.json"
```

#### 3. Hybrid Configuration
Combine both methods with the `adbc.databricks.driver_config_take_precedence` property:
- `true`: Environment config overrides constructor properties
- `false` (default): Constructor properties override environment config

### Authentication

#### Token-based Authentication (Personal Access Token)
```csharp
var config = new Dictionary<string, string>
{
    ["uri"] = "https://workspace.databricks.com/sql/1.0/warehouses/...",
    ["adbc.spark.auth_type"] = "oauth",
    ["adbc.databricks.oauth.grant_type"] = "access_token",
    ["adbc.spark.oauth.access_token"] = "your-personal-access-token"
};
```

#### OAuth Client Credentials (Machine-to-Machine)
```csharp
var config = new Dictionary<string, string>
{
    ["uri"] = "https://workspace.databricks.com/sql/1.0/warehouses/...",
    ["adbc.spark.auth_type"] = "oauth",
    ["adbc.databricks.oauth.grant_type"] = "client_credentials",
    ["adbc.databricks.oauth.client_id"] = "your-client-id",
    ["adbc.databricks.oauth.client_secret"] = "your-client-secret",
    ["adbc.databricks.oauth.scope"] = "sql"  // Optional, defaults to "sql"
};
```

**Authentication Properties:**

| Property | Description | Default |
|----------|-------------|---------|
| `adbc.spark.auth_type` | Authentication type: `none`, `username_only`, `basic`, `token`, `oauth` | (required) |
| `adbc.databricks.oauth.grant_type` | OAuth grant type: `access_token` or `client_credentials` | `access_token` |
| `adbc.databricks.oauth.client_id` | OAuth client ID for credentials flow | |
| `adbc.databricks.oauth.client_secret` | OAuth client secret for credentials flow | |
| `adbc.databricks.oauth.scope` | OAuth scope for credentials flow | `sql` |
| `adbc.spark.oauth.access_token` | Personal access token (for `access_token` grant type) | |
| `adbc.databricks.token_renew_limit` | Minutes before expiration to renew token (0 disables) | `0` |
| `adbc.databricks.identity_federation_client_id` | Service principal client ID for workload identity | |

**Note:** Basic username/password authentication is not supported.

### Connection Properties

#### Core Spark Properties (Inherited)

| Property | Description | Default |
|----------|-------------|---------|
| `uri` | Full connection URI (alternative to host/port/path) | (required) |
| `adbc.spark.type` | Server type: `http` | `http` |
| `adbc.spark.host` | Hostname without scheme/port | |
| `adbc.spark.port` | Connection port | `443` |
| `adbc.spark.path` | URI path on server | |
| `adbc.spark.connect_timeout_ms` | Session establishment timeout | `30000` |
| `adbc.apache.statement.query_timeout_s` | Query execution timeout | `60` |
| `adbc.apache.connection.polltime_ms` | Query status polling interval | `500` (Databricks: `100`) |
| `adbc.apache.statement.batch_size` | Max rows per batch request | `50000` (Databricks: `2000000`) |
| `adbc.spark.data_type_conv` | Data type conversion: `none` or `scalar` | `scalar` |

#### Databricks-Specific Properties

| Property | Description | Default |
|----------|-------------|---------|
| `adbc.connection.catalog` | Default catalog for session | |
| `adbc.connection.db_schema` | Default schema for session | |
| `adbc.databricks.enable_direct_results` | Use direct results when executing | `true` |
| `adbc.databricks.apply_ssp_with_queries` | Apply server-side properties with queries | `false` |
| `adbc.databricks.enable_multiple_catalog_support` | Support multiple catalogs | `true` |
| `adbc.databricks.enable_pk_fk` | Enable primary/foreign key metadata | `true` |
| `adbc.databricks.use_desc_table_extended` | Use DESC TABLE EXTENDED when supported | `true` |
| `adbc.databricks.enable_run_async_thrift` | Enable RunAsync flag | `true` |

**Performance Notes:**
- Databricks default `batch_size` is `2000000` (vs Spark's `50000`) - optimized for CloudFetch's 1024MB limit
- Databricks default `polltime_ms` is `100` (vs Spark's `500`) - faster query status feedback

### CloudFetch Configuration

CloudFetch is Databricks' high-performance result retrieval system that downloads results directly from cloud storage. It's **enabled by default** and provides significant performance improvements for large result sets.

**CloudFetch Properties:**

| Property | Description | Default |
|----------|-------------|---------|
| `adbc.databricks.cloudfetch.enabled` | Enable/disable CloudFetch | `true` |
| `adbc.databricks.cloudfetch.lz4.enabled` | Enable LZ4 decompression | `true` |
| `adbc.databricks.cloudfetch.max_bytes_per_file` | Max bytes per file (supports KB, MB, GB) | `20MB` |
| `adbc.databricks.cloudfetch.parallel_downloads` | Max parallel downloads | `3` |
| `adbc.databricks.cloudfetch.prefetch_count` | Files to prefetch ahead | `2` |
| `adbc.databricks.cloudfetch.memory_buffer_size_mb` | Max memory buffer in MB | `200` |
| `adbc.databricks.cloudfetch.prefetch_enabled` | Enable prefetch | `true` |
| `adbc.databricks.cloudfetch.max_retries` | Max retry attempts | `3` |
| `adbc.databricks.cloudfetch.retry_delay_ms` | Delay between retries | `500` |
| `adbc.databricks.cloudfetch.timeout_minutes` | HTTP operation timeout | `5` |
| `adbc.databricks.cloudfetch.url_expiration_buffer_seconds` | URL refresh buffer | `60` |
| `adbc.databricks.cloudfetch.max_url_refresh_attempts` | Max URL refresh attempts | `3` |

**Example Configuration:**
```csharp
var config = new Dictionary<string, string>
{
    // ... auth config ...
    ["adbc.databricks.cloudfetch.parallel_downloads"] = "5",
    ["adbc.databricks.cloudfetch.memory_buffer_size_mb"] = "500",
    ["adbc.databricks.cloudfetch.prefetch_count"] = "4"
};
```

### TLS/SSL Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `adbc.http_options.tls.enabled` | Enable TLS | `true` |
| `adbc.http_options.tls.disable_server_certificate_validation` | Skip certificate validation | `false` |
| `adbc.http_options.tls.allow_self_signed` | Accept self-signed certificates | `false` |
| `adbc.http_options.tls.allow_hostname_mismatch` | Allow hostname mismatches | `false` |
| `adbc.http_options.tls.trusted_certificate_path` | Custom CA certificate path | |

### Data Type Support

The driver automatically converts Databricks/Spark types to Apache Arrow types:

| Databricks Type | Arrow Type | C# Type | Notes |
|----------------|------------|---------|-------|
| BIGINT | Int64 | long | |
| BOOLEAN | Boolean | bool | |
| DATE | Date32 | DateTime | With `scalar` conversion |
| DECIMAL | Decimal128/Decimal256 | decimal | With `scalar` conversion |
| DOUBLE | Double | double | |
| FLOAT | Float | float | Converted to Float instead of Double |
| INT | Int32 | int | |
| SMALLINT | Int16 | short | |
| STRING | String | string | |
| TIMESTAMP | Timestamp | DateTimeOffset | With `scalar` conversion |
| TINYINT | Int8 | sbyte | |
| BINARY | Binary | byte[] | |
| ARRAY | List | string | Complex types serialize as strings |
| MAP | Map | string | Complex types serialize as strings |
| STRUCT | Struct | string | Complex types serialize as strings |

**Data Type Conversion Mode:**
- `adbc.spark.data_type_conv` = `scalar` (default): Converts DATE, DECIMAL, TIMESTAMP to native types
- `adbc.spark.data_type_conv` = `none`: No conversion, returns raw string representations

### Server-Side Properties

Properties prefixed with `adbc.databricks.ssp_` are passed to the Databricks server via SET queries.

**Example:**
```csharp
var config = new Dictionary<string, string>
{
    ["adbc.databricks.ssp_use_cached_result"] = "true"
};
// Results in: SET use_cached_result=true
```

### Tracing and Observability

The driver includes built-in tracing support via OpenTelemetry.

**Tracing Properties:**

| Property | Description | Default |
|----------|-------------|---------|
| `adbc.databricks.trace_propagation.enabled` | Propagate trace parent headers | `true` |
| `adbc.databricks.trace_propagation.header_name` | HTTP header name for trace | `traceparent` |
| `adbc.databricks.trace_propagation.state_enabled` | Include trace state header | `false` |
| `adbc.traces.exporter` | Trace exporter: `none`, `otlp`, `console`, `adbcfile` | |
| `adbc.telemetry.trace_parent` | W3C trace context identifier | |

**Enable File-Based Tracing:**

Via environment variable:
```bash
export OTEL_TRACES_EXPORTER=adbcfile
```

Or via configuration:
```csharp
var config = new Dictionary<string, string>
{
    ["adbc.traces.exporter"] = "adbcfile"
};
```

**Trace File Locations:**
- **Windows:** `%LOCALAPPDATA%\Apache.Arrow.Adbc\Traces`
- **macOS:** `~/Library/Application Support/Apache.Arrow.Adbc/Traces`
- **Linux:** `~/.local/share/Apache.Arrow.Adbc/Traces`

Files rotate automatically with a pattern: `apache.arrow.adbc.drivers.databricks-<YYYY-MM-DD-HH-mm-ss-fff>-<process-id>.log`

Default: 999 files maximum, 1024 KB each.

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
