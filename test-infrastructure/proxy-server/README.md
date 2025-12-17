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

# Thrift Protocol Test Proxy Server

A standalone Go application that intercepts Databricks Thrift protocol traffic and injects controlled failures for testing driver resilience.

## Overview

This proxy server sits between the ADBC driver and Databricks workspace, allowing deterministic failure injection for testing:

```
Driver -> Proxy (localhost:8080) -> Databricks Workspace
          Control API (localhost:8081)
```

## Quick Start

### Prerequisites

- Go 1.21 or later
- Access to a Databricks workspace

### Installation

```bash
cd test-infrastructure/proxy-server
go mod download
```

### Running the Proxy

```bash
# Start with default config
go run main.go --config proxy-config.yaml

# Or build and run
go build -o proxy-server
./proxy-server --config proxy-config.yaml
```

The proxy will start two servers:
- **Port 8080**: Proxy server (intercepts Thrift/HTTP traffic)
- **Port 8081**: Control API (enable/disable failure scenarios)

## Configuration

Edit `proxy-config.yaml` to configure the proxy:

```yaml
proxy:
  listen_port: 8080
  target_server: "https://your-workspace.databricks.com"
  api_port: 8081
  log_requests: true
  log_level: "info"

failure_scenarios:
  - name: "cloudfetch_expired_link"
    description: "CloudFetch link expires"
    operation: "CloudFetchDownload"
    action: "expire_cloud_link"
```

See [proxy-config-schema.md](../../docs/designs/thrift-protocol-tests/proxy-config-schema.md) for full schema documentation.

## Control API

The Control API is defined using OpenAPI 3.0 specification in `openapi.yaml`. This enables automatic client generation for 50+ languages including C#, Java, C++, Python, Go, and Rust.

### Generate Client Libraries

See [CLIENTS.md](CLIENTS.md) for detailed instructions on generating clients for your language.

**Quick start:**

```bash
# Install OpenAPI Generator
brew install openapi-generator

# Generate C# client
make generate-csharp

# Generate Java client
make generate-java

# Or generate all supported clients
make generate-clients
```

### Using curl (Simple Testing)

```bash
# List all scenarios
curl http://localhost:8081/scenarios

# Enable a scenario
curl -X POST http://localhost:8081/scenarios/cloudfetch_expired_link/enable

# Disable a scenario
curl -X POST http://localhost:8081/scenarios/cloudfetch_expired_link/disable
```

### Using Generated Clients

**C#:**
```csharp
using ProxyControlApi.Api;
using ProxyControlApi.Client;

var config = new Configuration { BasePath = "http://localhost:18081" };
var api = new ScenariosApi(config);

// Enable scenario
var status = api.EnableScenario("cloudfetch_timeout");
```

**Java:**
```java
import com.databricks.proxy.api.*;

ApiClient client = new ApiClient();
client.setBasePath("http://localhost:18081");
ScenariosApi api = new ScenariosApi(client);

ScenarioStatus status = api.enableScenario("cloudfetch_timeout");
```

**Python:**
```python
from proxy_control_client import ApiClient, Configuration, ScenariosApi

config = Configuration(host="http://localhost:18081")
api = ScenariosApi(ApiClient(config))

status = api.enable_scenario("cloudfetch_timeout")
```

See [CLIENTS.md](CLIENTS.md) for more examples.

## Usage in Tests

```csharp
// Configure driver to use proxy
var connectionString = "Host=localhost:8080;...";

// Enable failure scenario
var httpClient = new HttpClient();
await httpClient.PostAsync(
    "http://localhost:8081/scenarios/cloudfetch_expired_link/enable",
    null);

try
{
    // Execute query - CloudFetch failure will be injected on next download
    var result = await driver.ExecuteQuery("SELECT * FROM large_table");

    // Verify driver handled the failure correctly
    Assert.NotNull(result);
}
finally
{
    // Scenario auto-disables after injection, but you can explicitly disable too
    await httpClient.PostAsync(
        "http://localhost:8081/scenarios/cloudfetch_expired_link/disable",
        null);
}
```

**How it works:**

1. Test enables a scenario via Control API
2. Driver executes query that triggers CloudFetch
3. Proxy detects CloudFetch download (HTTP GET to cloud storage)
4. Proxy injects the failure based on scenario action
5. Scenario auto-disables after injection (one-shot)
6. Test verifies driver recovery behavior

## Features

**v0.2 (PECO-2861)** - CloudFetch Failure Injection:

- ✅ YAML configuration loading
- ✅ Control API for enabling/disabling scenarios
- ✅ HTTP reverse proxy with request interception
- ✅ CloudFetch download detection (Azure Blob, S3, GCS)
- ✅ CloudFetch failure injection (3 scenarios):
  - `cloudfetch_expired_link`: Returns 403 with expired signature error
  - `cloudfetch_azure_403`: Returns 403 Forbidden with custom message
  - `cloudfetch_timeout`: Injects 65s delay before download
- ✅ One-shot injection (scenarios auto-disable after triggering)

**Coming next:**

- ❌ Thrift protocol parsing and interception
- ❌ Thrift operation-specific failures (session, statement execution)
- ❌ Connection reset and SSL error injection

See [design.md](../../docs/designs/thrift-protocol-tests/design.md) for the full implementation roadmap.

## Architecture

```
┌─────────────────────┐
│   Driver Tests      │
│   (C#, Java, etc)   │
└──────────┬──────────┘
           │
           ↓ HTTP/Thrift
┌─────────────────────┐
│   Proxy Server      │
│  ┌───────────────┐  │
│  │ Control API   │  │ ← Enable/disable scenarios
│  │  (Port 8081)  │  │
│  └───────────────┘  │
│  ┌───────────────┐  │
│  │ Reverse Proxy │  │ ← Intercept & modify traffic
│  │  (Port 8080)  │  │
│  └───────────────┘  │
└──────────┬──────────┘
           │
           ↓ HTTP/Thrift
┌─────────────────────┐
│ Databricks Workspace│
└─────────────────────┘
```

## Development

### Project Structure

```
proxy-server/
├── main.go              # HTTP server & routing
├── config.go            # YAML configuration loading
├── proxy-config.yaml    # Example configuration
├── go.mod               # Go module dependencies
└── README.md            # This file
```

### Testing Locally

1. Start the proxy:
   ```bash
   go run main.go --config proxy-config.yaml
   ```

2. Configure your driver to connect to `localhost:8080`

3. Use the control API to enable scenarios:
   ```bash
   curl -X POST http://localhost:8081/scenarios/cloudfetch_timeout/enable
   ```

4. Run your driver tests and verify failures are injected correctly

## Next Steps (Future PRs)

- **PECO-2861**: Implement Thrift protocol interception and 5 priority failure scenarios
- **PECO-2862**: C# test infrastructure integration
- **PECO-2863-2865**: Comprehensive test suites for session, statement, and CloudFetch operations

## Related Documentation

- [Design Document](../../docs/designs/thrift-protocol-tests/design.md)
- [YAML Configuration Schema](../../docs/designs/thrift-protocol-tests/proxy-config-schema.md)
- [Test Specifications](../../docs/designs/thrift-protocol-tests/README.md)
