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

### List All Scenarios

```bash
curl http://localhost:8081/scenarios
```

Response:
```json
{
  "scenarios": [
    {
      "name": "cloudfetch_expired_link",
      "description": "CloudFetch link expires",
      "enabled": false
    }
  ]
}
```

### Enable a Scenario

```bash
curl -X POST http://localhost:8081/scenarios/cloudfetch_expired_link/enable
```

Response:
```json
{
  "scenario": "cloudfetch_expired_link",
  "enabled": true
}
```

### Disable a Scenario

```bash
curl -X POST http://localhost:8081/scenarios/cloudfetch_expired_link/disable
```

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
    // Execute query - failure will be injected on next matching request
    var result = await driver.ExecuteQuery("SELECT * FROM large_table");

    // Verify driver handled the failure correctly
    Assert.NotNull(result);
}
finally
{
    // Clean up
    await httpClient.PostAsync(
        "http://localhost:8081/scenarios/cloudfetch_expired_link/disable",
        null);
}
```

## Current Limitations (v0.1)

This is the initial version (PECO-2860) with basic structure:

- ✅ YAML configuration loading
- ✅ Control API for enabling/disabling scenarios
- ✅ Basic HTTP reverse proxy
- ❌ Thrift protocol interception (coming in PECO-2861)
- ❌ Failure injection logic (coming in PECO-2861)
- ❌ CloudFetch URL modification (coming in PECO-2861)

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
