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

# ADBC C# Drivers for Databricks

This folder contains an [ADBC driver](https://arrow.apache.org/adbc/) for
Databricks written in C#.

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
Pass properties directly when creating connections.

You can use either a full URI or separate host/path parameters:

```csharp
// Option 1: Using full URI
var config = new Dictionary<string, string>
{
    ["uri"] = "https://workspace.databricks.com/sql/1.0/warehouses/...",
    ["adbc.spark.auth_type"] = "oauth",
    ["adbc.databricks.oauth.grant_type"] = "client_credentials",
    ["adbc.databricks.oauth.client_id"] = "your-client-id",
    ["adbc.databricks.oauth.client_secret"] = "your-client-secret"
};

// Option 2: Using separate host and path (from Spark driver)
var config = new Dictionary<string, string>
{
    ["adbc.spark.type"] = "http",
    ["adbc.spark.host"] = "workspace.databricks.com",
    ["adbc.spark.port"] = "443",
    ["adbc.spark.path"] = "/sql/1.0/warehouses/...",
    ["adbc.spark.auth_type"] = "oauth",
    ["adbc.databricks.oauth.grant_type"] = "client_credentials",
    ["adbc.databricks.oauth.client_id"] = "your-client-id",
    ["adbc.databricks.oauth.client_secret"] = "your-client-secret"
};
```

#### 2. Environment Variable Configuration
Create a JSON configuration file (all values must be strings).

You can use either a full URI or separate host/path parameters:

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

Or using separate host and path (from Spark driver):

```json
{
  "adbc.spark.type": "http",
  "adbc.spark.host": "workspace.databricks.com",
  "adbc.spark.port": "443",
  "adbc.spark.path": "/sql/1.0/warehouses/...",
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

**Note:** OAuth U2M (User-to-Machine) authentication support is planned for a future release.

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
| `uri` | Full connection URI (alternative to host/port/path) | |
| `adbc.spark.type` | Server type: `http` | `http` |
| `adbc.spark.host` | Hostname without scheme/port | |
| `adbc.spark.port` | Connection port | `443` |
| `adbc.spark.path` | URI path on server | |
| `adbc.spark.token` | Token for token-based authentication | |
| `username` | Username for basic authentication | |
| `password` | Password for basic authentication | |
| `adbc.spark.connect_timeout_ms` | Session establishment timeout | `30000` |
| `adbc.apache.statement.query_timeout_s` | Query execution timeout | `60` |
| `adbc.apache.connection.polltime_ms` | Query status polling interval | `500` (Databricks: `100`) |
| `adbc.apache.statement.batch_size` | Max rows per batch request | `50000` (Databricks: `2000000`) |
| `adbc.spark.data_type_conv` | Data type conversion: `none` or `scalar` | `scalar` |

**Note:** Either `uri` or the combination of `adbc.spark.host` + `adbc.spark.path` is required.

#### Databricks-Specific Properties

| Property | Description | Default |
|----------|-------------|---------|
| `adbc.connection.catalog` | Default catalog for session | |
| `adbc.connection.db_schema` | Default schema for session | |
| `adbc.databricks.enable_direct_results` | Use direct results when executing | `true` |
| `adbc.databricks.max_bytes_per_fetch_request` | Max bytes per fetch request (supports B, KB, MB, GB) | `400MB` |
| `adbc.databricks.apply_ssp_with_queries` | Apply server-side properties with queries | `false` |
| `adbc.databricks.enable_multiple_catalog_support` | Support multiple catalogs | `true` |
| `adbc.databricks.enable_pk_fk` | Enable primary/foreign key metadata | `true` |
| `adbc.databricks.use_desc_table_extended` | Use DESC TABLE EXTENDED when supported | `true` |
| `adbc.databricks.enable_run_async_thrift` | Enable RunAsync flag | `true` |
| `adbc.databricks.fetch_heartbeat_interval` | Heartbeat interval for long operations (seconds) | `60` |
| `adbc.databricks.operation_status_request_timeout` | Timeout for status polling requests (seconds) | `30` |
| `adbc.spark.temporarily_unavailable_retry` | Retry on 408/502/503/504 responses | `true` |
| `adbc.spark.temporarily_unavailable_retry_timeout` | Max retry time for 4xx/5xx errors (seconds) | `900` |
| `adbc.databricks.rate_limit_retry` | Retry on HTTP 429 (rate limit) responses | `true` |
| `adbc.databricks.rate_limit_retry_timeout` | Max retry time for rate limits (seconds) | `120` |

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

### Statement Execution API (REST Protocol)

The driver supports both Thrift/HiveServer2 protocol (default) and Statement Execution REST API for query execution.

**Protocol Properties:**

| Property | Description | Default |
|----------|-------------|---------|
| `adbc.databricks.protocol` | Protocol: `thrift` or `rest` | `thrift` |
| `adbc.databricks.warehouse_id` | Warehouse ID for query execution | (required for REST) |

**REST API Configuration:**

| Property | Description | Default |
|----------|-------------|---------|
| `adbc.databricks.rest.result_disposition` | Result mode: `inline`, `external_links`, or `inline_or_external_links` | `inline_or_external_links` |
| `adbc.databricks.rest.result_format` | Format: `arrow_stream`, `json_array`, or `csv` | `arrow_stream` |
| `adbc.databricks.rest.result_compression` | Compression: `lz4`, `gzip`, or `none` | (auto) |
| `adbc.databricks.rest.wait_timeout` | Wait timeout in seconds (0=async, 5-50=sync) | `10` |
| `adbc.databricks.rest.polling_interval_ms` | Polling interval for async execution (ms) | `1000` |
| `adbc.databricks.rest.enable_session_management` | Enable session management | `true` |

**Example - Using REST Protocol:**
```csharp
var config = new Dictionary<string, string>
{
    ["uri"] = "https://workspace.databricks.com/sql/1.0/warehouses/...",
    ["adbc.spark.auth_type"] = "oauth",
    ["adbc.databricks.oauth.grant_type"] = "access_token",
    ["adbc.spark.oauth.access_token"] = "your-token",
    ["adbc.databricks.protocol"] = "rest",
    ["adbc.databricks.warehouse_id"] = "your-warehouse-id",
    ["adbc.databricks.rest.result_disposition"] = "inline_or_external_links"
};
```

### Metadata Operations

The driver provides comprehensive database metadata operations following the ADBC specification. Both Thrift and REST protocols support metadata retrieval, with some differences in capabilities.

#### Supported Metadata Operations

- **GetInfo()**: Get driver and database metadata (version, name, capabilities)
- **GetTableTypes()**: Get all supported table types (TABLE, VIEW, LOCAL TEMPORARY)
- **GetObjects()**: Retrieve catalogs, schemas, tables, and columns with filtering
- **GetTableSchema()**: Get the Arrow schema for a specific table

#### Get Info

Retrieve driver and database metadata information:

```csharp
using var connection = database.Connect();

// Get all supported info codes
using var stream = connection.GetInfo(Array.Empty<AdbcInfoCode>());
var schema = stream.Schema;  // Columns: info_name (uint32), info_value (union)

while (true)
{
    using var batch = stream.ReadNextRecordBatchAsync().Result;
    if (batch == null) break;

    var infoNameArray = batch.Column("info_name") as UInt32Array;
    var infoValueUnion = batch.Column("info_value") as DenseUnionArray;

    for (int i = 0; i < batch.Length; i++)
    {
        var code = (AdbcInfoCode)infoNameArray.GetValue(i);

        // Extract value based on type
        switch (code)
        {
            case AdbcInfoCode.VendorName:
            case AdbcInfoCode.DriverName:
            case AdbcInfoCode.DriverVersion:
                var stringArray = infoValueUnion.Fields[0] as StringArray;
                Console.WriteLine($"{code}: {stringArray.GetString(i)}");
                break;
            case AdbcInfoCode.VendorSql:
                var boolArray = infoValueUnion.Fields[1] as BooleanArray;
                Console.WriteLine($"{code}: {boolArray.GetValue(i)}");
                break;
        }
    }
}

// Get specific info codes
var requestedCodes = new[] { AdbcInfoCode.VendorName, AdbcInfoCode.DriverVersion };
using var stream2 = connection.GetInfo(requestedCodes);
// Processes only the requested codes...
```

**Supported Info Codes:**
- `VendorName`: "Databricks"
- `VendorVersion`: Databricks server version (or "Unknown")
- `VendorArrowVersion`: Apache Arrow version (e.g., "17.0.0")
- `VendorSql`: false (Databricks uses Spark SQL, not standard SQL)
- `DriverName`: "ADBC Databricks Driver (Statement Execution API)" or "ADBC Databricks Driver (Thrift Protocol)"
- `DriverVersion`: Driver assembly version (e.g., "1.0.0")
- `DriverArrowVersion`: Apache Arrow version used by driver (e.g., "17.0.0")

#### Get Table Types

Retrieve all table types supported by Databricks:

```csharp
using var connection = database.Connect();
using var stream = connection.GetTableTypes();

var schema = stream.Schema;  // Single column: table_type
while (true)
{
    using var batch = stream.ReadNextRecordBatchAsync().Result;
    if (batch == null) break;

    var tableTypeArray = batch.Column("table_type") as StringArray;
    for (int i = 0; i < batch.Length; i++)
    {
        if (!tableTypeArray.IsNull(i))
        {
            var tableType = tableTypeArray.GetString(i);
            Console.WriteLine($"Table Type: {tableType}");
        }
    }
}
// Output: TABLE, VIEW, LOCAL TEMPORARY
```

#### Get Objects - Catalogs

Retrieve all available catalogs:

```csharp
using var connection = database.Connect();
using var stream = connection.GetObjects(
    AdbcConnection.GetObjectsDepth.Catalogs,
    catalogPattern: null,
    schemaPattern: null,
    tableNamePattern: null,
    tableTypes: null,
    columnNamePattern: null);

// Schema: (catalog_name)
while (true)
{
    using var batch = stream.ReadNextRecordBatchAsync().Result;
    if (batch == null) break;

    var catalogArray = batch.Column("catalog_name") as StringArray;
    for (int i = 0; i < batch.Length; i++)
    {
        Console.WriteLine($"Catalog: {catalogArray.GetString(i)}");
    }
}
```

#### Get Objects - Schemas

Retrieve schemas in a specific catalog with pattern matching:

```csharp
using var connection = database.Connect();
using var stream = connection.GetObjects(
    AdbcConnection.GetObjectsDepth.DbSchemas,
    catalogPattern: "main",          // Specific catalog
    schemaPattern: "def%",           // Pattern: schemas starting with "def"
    tableNamePattern: null,
    tableTypes: null,
    columnNamePattern: null);

// Schema: (catalog_name, db_schema_name)
while (true)
{
    using var batch = stream.ReadNextRecordBatchAsync().Result;
    if (batch == null) break;

    var catalogArray = batch.Column("catalog_name") as StringArray;
    var schemaArray = batch.Column("db_schema_name") as StringArray;

    for (int i = 0; i < batch.Length; i++)
    {
        Console.WriteLine($"Schema: {catalogArray.GetString(i)}.{schemaArray.GetString(i)}");
    }
}
```

#### Get Objects - Tables

Retrieve tables with optional table type filtering:

```csharp
using var connection = database.Connect();
using var stream = connection.GetObjects(
    AdbcConnection.GetObjectsDepth.Tables,
    catalogPattern: "main",
    schemaPattern: "default",
    tableNamePattern: "sales_%",     // Pattern: tables starting with "sales_"
    tableTypes: new[] { "TABLE", "VIEW" },  // Filter to tables and views only
    columnNamePattern: null);

// Schema: (catalog_name, db_schema_name, table_name, table_type)
while (true)
{
    using var batch = stream.ReadNextRecordBatchAsync().Result;
    if (batch == null) break;

    var tableArray = batch.Column("table_name") as StringArray;
    var typeArray = batch.Column("table_type") as StringArray;

    for (int i = 0; i < batch.Length; i++)
    {
        Console.WriteLine($"Table: {tableArray.GetString(i)} ({typeArray.GetString(i)})");
    }
}
```

#### Get Table Schema

Retrieve the Arrow schema for a specific table:

```csharp
using var connection = database.Connect();
var schema = connection.GetTableSchema(
    catalog: "main",
    dbSchema: "default",
    tableName: "customers");

Console.WriteLine($"Table has {schema.FieldsList.Count} columns:");
foreach (var field in schema.FieldsList)
{
    Console.WriteLine($"  {field.Name}: {field.DataType}");

    // Access column comment if available
    if (field.Metadata != null && field.Metadata.ContainsKey("comment"))
    {
        Console.WriteLine($"    Comment: {field.Metadata["comment"]}");
    }
}
```

#### Pattern Matching

All metadata operations support SQL LIKE pattern matching:

- **`%`** - Matches any sequence of characters (0 or more)
- **`_`** - Matches any single character

**Examples:**

```csharp
// Match catalogs starting with "main"
catalogPattern: "main%"

// Match schemas containing "analytics"
schemaPattern: "%analytics%"

// Match tables ending with "_temp"
tableNamePattern: "%_temp"

// Match columns named "id", "Id", "ID" (case-insensitive)
columnNamePattern: "id"
```

**Pattern matching is case-insensitive** and applies to catalog, schema, table, and column names.

#### REST vs Thrift Protocol Differences

| Feature | REST Protocol | Thrift Protocol |
|---------|--------------|-----------------|
| GetTableTypes() | Returns 3 types: TABLE, VIEW, LOCAL TEMPORARY | Returns 2 types: TABLE, VIEW |
| GetObjects(Catalogs) | ✅ Supported | ✅ Supported |
| GetObjects(DbSchemas) | ✅ Supported | ✅ Supported |
| GetObjects(Tables) | ✅ Supported | ✅ Supported |
| GetObjects(All) | ✅ Full nested structure | ✅ Full nested structure |
| GetTableSchema() | ✅ Supported | ✅ Supported |
| Implementation | SQL-based (SHOW commands) | Native Thrift calls |
| Performance | Good for most use cases | Optimized for metadata |

**Implementation Details:**
- REST protocol now returns full ADBC-compliant nested structure for `GetObjectsDepth.All`
- Hierarchical catalog→schema→table→column structure using ListArray and StructArray
- Follows ADBC StandardSchemas for full spec compliance
- All metadata depths are fully supported and tested

#### Implementation Details

The REST protocol implements metadata operations using SQL commands:
- **SHOW CATALOGS** - Retrieve available catalogs
- **SHOW SCHEMAS IN catalog** - Retrieve schemas in a catalog
- **SHOW TABLES IN catalog.schema** - Retrieve tables in a schema
- **DESCRIBE TABLE catalog.schema.table** - Retrieve table schema and column details

This SQL-based approach provides:
- ✅ Consistent behavior across Databricks deployments
- ✅ Pattern matching via SQL LIKE syntax
- ✅ Easy debugging (SQL queries can be tested directly)
- ✅ Type detection including temporary tables (via isTemporary column)

#### Metadata Caching

The REST protocol supports optional metadata caching with configurable TTL (Time-To-Live) to reduce repeated database queries. Caching is **disabled by default** to avoid stale data issues.

**Caching Properties:**

| Property | Description | Default |
|----------|-------------|---------|
| `adbc.databricks.metadata_cache.enabled` | Enable metadata caching | `false` |
| `adbc.databricks.metadata_cache.catalog_ttl_seconds` | TTL for cached catalog lists | `300` (5 minutes) |
| `adbc.databricks.metadata_cache.schema_ttl_seconds` | TTL for cached schema lists | `120` (2 minutes) |
| `adbc.databricks.metadata_cache.table_ttl_seconds` | TTL for cached table lists | `60` (1 minute) |
| `adbc.databricks.metadata_cache.column_ttl_seconds` | TTL for cached column lists | `30` (30 seconds) |

**Example Configuration:**

```csharp
var config = new Dictionary<string, string>
{
    ["uri"] = "https://workspace.databricks.com/sql/1.0/warehouses/...",
    ["adbc.spark.auth_type"] = "oauth",
    ["adbc.databricks.oauth.grant_type"] = "access_token",
    ["adbc.spark.oauth.access_token"] = "your-token",
    ["adbc.databricks.protocol"] = "rest",

    // Enable metadata caching
    ["adbc.databricks.metadata_cache.enabled"] = "true",
    ["adbc.databricks.metadata_cache.catalog_ttl_seconds"] = "600",  // 10 minutes
    ["adbc.databricks.metadata_cache.schema_ttl_seconds"] = "300",   // 5 minutes
    ["adbc.databricks.metadata_cache.table_ttl_seconds"] = "120",    // 2 minutes
    ["adbc.databricks.metadata_cache.column_ttl_seconds"] = "60"     // 1 minute
};

using var database = driver.Open(config);
using var connection = database.Connect();

// First call - fetches from database (~150ms)
using var stream1 = connection.GetObjects(
    AdbcConnection.GetObjectsDepth.Catalogs,
    null, null, null, null, null);

// Second call within TTL - returns cached results (<10ms)
using var stream2 = connection.GetObjects(
    AdbcConnection.GetObjectsDepth.Catalogs,
    null, null, null, null, null);
```

**Performance Benefits:**
- Reduces latency by 90%+ for repeated metadata queries
- Lowers database load for BI tools that refresh navigator trees frequently
- Thread-safe implementation using `ConcurrentDictionary`
- Automatic TTL-based expiration

**Important Notes:**
- Caching is **per-connection** - each connection has its own cache
- Pattern-specific caching: Different patterns are cached separately (e.g., `"main%"` vs `"m%"`)
- Cache expiration: Entries are checked on read and removed if expired
- **Disable in production** if you need real-time metadata changes (e.g., tables created by other processes)
- Recommended TTL hierarchy: catalogs (longer TTL) → schemas → tables → columns (shorter TTL)

### TLS/SSL Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `adbc.http_options.tls.enabled` | Enable TLS | `true` |
| `adbc.http_options.tls.disable_server_certificate_validation` | Skip certificate validation | `false` |
| `adbc.http_options.tls.allow_self_signed` | Accept self-signed certificates | `false` |
| `adbc.http_options.tls.allow_hostname_mismatch` | Allow hostname mismatches | `false` |
| `adbc.http_options.tls.trusted_certificate_path` | Custom CA certificate path | |

#### Using mitmproxy for Debugging

For debugging HTTP/HTTPS traffic, you can use [mitmproxy](https://mitmproxy.org/) with custom certificate configuration:

```csharp
var config = new Dictionary<string, string>
{
    ["uri"] = "https://workspace.databricks.com/sql/1.0/warehouses/...",
    ["adbc.spark.auth_type"] = "oauth",
    ["adbc.databricks.oauth.grant_type"] = "access_token",
    ["adbc.spark.oauth.access_token"] = "your-token",

    // Configure to trust mitmproxy's certificate
    ["adbc.http_options.tls.trusted_certificate_path"] = "/path/to/mitmproxy-ca-cert.pem",
    // Or disable certificate validation (not recommended for production)
    ["adbc.http_options.tls.disable_server_certificate_validation"] = "true"
};
```

**Steps:**
1. Install and start mitmproxy: `mitmproxy -p 8080`
2. Export mitmproxy's CA certificate from `~/.mitmproxy/mitmproxy-ca-cert.pem`
3. Configure your system to route traffic through the proxy (e.g., set HTTP_PROXY environment variable)
4. Use `trusted_certificate_path` to trust mitmproxy's certificate, or disable validation for testing

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

Properties prefixed with `adbc.databricks.ssp_` are passed to the Databricks server as session configuration.

The behavior is controlled by `adbc.databricks.apply_ssp_with_queries`:

- **`false` (default)**: Server-side properties are applied in the Thrift configuration when the session is opened
- **`true`**: Server-side properties are applied using SET queries during execution

**Example:**
```csharp
var config = new Dictionary<string, string>
{
    ["adbc.databricks.ssp_use_cached_result"] = "true",
    ["adbc.databricks.apply_ssp_with_queries"] = "false"  // Apply during session open (default)
};

// With apply_ssp_with_queries = false:
// Properties are included in the Thrift session configuration

// With apply_ssp_with_queries = true:
// Results in: SET use_cached_result=true (executed as a query)
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

## Testing

The driver includes a comprehensive test suite using xUnit framework.

### Running Tests Locally

#### Prerequisites

Before running tests, you need to create a test configuration file.

#### Step 1: Create Test Configuration File

Create a JSON file with your Databricks connection details. All values must be strings.

**Option A: Using Personal Access Token (Recommended for local testing)**

Create `databricks-test-config.json`:
```json
{
  "uri": "https://your-workspace.cloud.databricks.com/sql/1.0/warehouses/your-warehouse-id",
  "adbc.spark.auth_type": "oauth",
  "adbc.databricks.oauth.grant_type": "access_token",
  "adbc.spark.oauth.access_token": "dapi1234567890abcdef",
  "adbc.connection.catalog": "main",
  "adbc.connection.db_schema": "default"
}
```

**Option B: Using OAuth Client Credentials (M2M)**

Create `databricks-test-config.json`:
```json
{
  "uri": "https://your-workspace.cloud.databricks.com/sql/1.0/warehouses/your-warehouse-id",
  "adbc.spark.auth_type": "oauth",
  "adbc.databricks.oauth.grant_type": "client_credentials",
  "adbc.databricks.oauth.client_id": "your-client-id",
  "adbc.databricks.oauth.client_secret": "your-client-secret",
  "adbc.databricks.oauth.scope": "sql",
  "adbc.connection.catalog": "main",
  "adbc.connection.db_schema": "default"
}
```

**Option C: Using Separate Host and Path (Alternative)**

Create `databricks-test-config.json`:
```json
{
  "adbc.spark.type": "http",
  "adbc.spark.host": "your-workspace.cloud.databricks.com",
  "adbc.spark.port": "443",
  "adbc.spark.path": "/sql/1.0/warehouses/your-warehouse-id",
  "adbc.spark.auth_type": "oauth",
  "adbc.databricks.oauth.grant_type": "access_token",
  "adbc.spark.oauth.access_token": "dapi1234567890abcdef",
  "adbc.connection.catalog": "main",
  "adbc.connection.db_schema": "default"
}
```

**Configuration Notes:**
- Save the file in a secure location (e.g., `~/.databricks/test-config.json`)
- **Never commit this file to version control** - add it to `.gitignore`
- All property values must be strings (quoted), including numbers
- The `catalog` and `db_schema` specify which database the tests will use
- Ensure your user/service principal has access to the specified warehouse and catalog

#### Step 2: Set Environment Variable

Point the `DATABRICKS_TEST_CONFIG_FILE` environment variable to your config file:

**On Windows (PowerShell):**
```powershell
$env:DATABRICKS_TEST_CONFIG_FILE="C:\Users\YourName\.databricks\test-config.json"
```

**On Windows (Command Prompt):**
```cmd
set DATABRICKS_TEST_CONFIG_FILE=C:\Users\YourName\.databricks\test-config.json
```

**On macOS/Linux:**
```bash
export DATABRICKS_TEST_CONFIG_FILE="$HOME/.databricks/test-config.json"
```

**Make it Permanent (macOS/Linux):**

Add to your shell profile (`~/.bashrc`, `~/.zshrc`, or `~/.bash_profile`):
```bash
export DATABRICKS_TEST_CONFIG_FILE="$HOME/.databricks/test-config.json"
```

Then reload: `source ~/.bashrc` (or `~/.zshrc`)

**Verify Configuration:**
```bash
# Check the variable is set
echo $DATABRICKS_TEST_CONFIG_FILE  # macOS/Linux
echo %DATABRICKS_TEST_CONFIG_FILE%  # Windows CMD
$env:DATABRICKS_TEST_CONFIG_FILE   # Windows PowerShell

# Verify the file exists and is readable
cat $DATABRICKS_TEST_CONFIG_FILE  # macOS/Linux
type %DATABRICKS_TEST_CONFIG_FILE%  # Windows CMD
Get-Content $env:DATABRICKS_TEST_CONFIG_FILE  # Windows PowerShell
```

**Note:** The test environment variable is `DATABRICKS_TEST_CONFIG_FILE`, which is different from the driver's runtime environment variable `DATABRICKS_CONFIG_FILE`. This separation allows you to use different configurations for testing vs. production use.

#### Run All Tests

From the test project directory:
```bash
cd csharp/test
dotnet test
```

Run tests for a specific framework:
```bash
# Test on .NET 8.0
dotnet test -f net8.0

# Test on .NET Framework 4.7.2 (Windows only)
dotnet test -f net472
```

#### Run Specific Tests

Run a specific test class:
```bash
dotnet test --filter "FullyQualifiedName~TracingDelegatingHandlerTest"
```

Run a specific test method:
```bash
dotnet test --filter "FullyQualifiedName~TracingDelegatingHandlerTest.TestTracePropagation"
```

Run tests matching a pattern:
```bash
# Run all tests with "CloudFetch" in the name
dotnet test --filter "FullyQualifiedName~CloudFetch"
```

#### Verbose Test Output

Get detailed test output:
```bash
dotnet test --logger "console;verbosity=detailed"
```

### Debugging Tests in Visual Studio

#### Setup

1. **Open Solution**
   - Open `csharp/Apache.Arrow.Adbc.Drivers.Databricks.sln` in Visual Studio

2. **Set Environment Variable**
   - Right-click on the test project → Properties
   - Navigate to Debug → General → Open debug launch profiles UI
   - Add environment variable:
     - Name: `DATABRICKS_TEST_CONFIG_FILE`
     - Value: `C:\path\to\your\config.json`

#### Running Tests

1. **Test Explorer**
   - Open Test Explorer: Test → Test Explorer (or Ctrl+E, T)
   - Build the solution to discover all tests
   - Tests will appear grouped by namespace and class

2. **Run/Debug Tests**
   - **Run a test**: Right-click test → Run
   - **Debug a test**: Right-click test → Debug
   - **Run all tests**: Click "Run All" in Test Explorer
   - **Run tests in a class**: Right-click class → Run

3. **Set Breakpoints**
   - Open the test file or source code
   - Click in the left margin to set breakpoints
   - Debug the test - execution will pause at breakpoints
   - Use F10 (Step Over), F11 (Step Into), F5 (Continue)

#### Tips

- **Filter tests**: Use the search box in Test Explorer to filter by name
- **Group by**: Use the grouping dropdown to organize by Class, Namespace, or Project
- **Run Failed Tests**: After a test run, right-click failed tests → Run
- **Live Unit Testing**: Test → Live Unit Testing → Start (Enterprise edition)

### Continuous Integration

Tests run automatically in CI:
- On every push to `main`
- On every pull request
- Multiple frameworks: .NET 8.0 (Linux) and .NET Framework 4.7.2 (Windows)

## Benchmarking

The C# driver includes a comprehensive benchmark suite for CloudFetch performance testing. For complete documentation including:
- How to run benchmarks locally and in CI
- Benchmark query details and characteristics
- Viewing results on GitHub Pages
- Understanding benchmark metrics and comparisons

See the [Benchmark Documentation](csharp/Benchmarks/README.md).

## Building

See [CONTRIBUTING.md](../CONTRIBUTING.md).

## Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md).
