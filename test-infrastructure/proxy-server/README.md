# Thrift Proxy Server

A standalone proxy server for testing ADBC driver behavior under various failure scenarios.

## Overview

This proxy sits between ADBC drivers and the real Databricks Thrift server, allowing controlled injection of failures for integration testing.

```
Driver → Proxy Server → Real Thrift Server
           ↑
    Inject failures here
```

## Features

- **Request Forwarding**: Transparent proxy for normal operations
- **Failure Injection**: Simulate various error scenarios
- **Request Logging**: Capture all Thrift traffic for debugging
- **Configuration-Driven**: Define failure scenarios via YAML

## Supported Failure Scenarios

- `cloudfetch_link_expired`: Return expired presigned URLs
- `cloudfetch_link_not_found`: Return 404 for cloud storage
- `network_timeout`: Delay responses to trigger timeouts
- `connection_reset`: Abruptly close connections
- `invalid_arrow_batch`: Corrupt Arrow IPC batches
- `session_expired`: Return session expired errors
- `operation_timeout`: Simulate long-running operations

## Usage

### Basic Usage

```bash
go run main.go --target "https://workspace.databricks.com:443"
```

### With Configuration

```bash
go run main.go --config proxy-config.yaml
```

Example `proxy-config.yaml`:
```yaml
proxy:
  listen_port: 8080
  target_server: "https://workspace.databricks.com:443"
  log_requests: true

failure_scenarios:
  - name: "cloudfetch_link_expired"
    trigger: "after_requests"
    count: 1
    action: "expire_cloud_link"
```

### From Tests

Set environment variable to point to proxy:

**C#:**
```csharp
var config = new Dictionary<string, string>
{
    ["uri"] = "http://localhost:8080",
    // ... other config
};
```

**Java:**
```java
props.setProperty("host", "localhost");
props.setProperty("port", "8080");
```

## Configuration

### Trigger Types

- `always`: Always inject failure
- `after_requests`: After N successful requests
- `probability`: Probabilistic (0.0-1.0)
- `operation_type`: For specific Thrift operations

### Actions

- `expire_cloud_link`: Mark CloudFetch URLs as expired
- `delay`: Add latency to responses
- `drop_connection`: Close connection abruptly
- `corrupt_response`: Modify response data
- `return_error`: Return specific error code

## Implementation Status

- [ ] Basic HTTP proxy
- [ ] Thrift request/response parsing
- [ ] Failure injection framework
- [ ] Configuration loading
- [ ] Request logging
- [ ] CloudFetch URL manipulation
- [ ] Arrow batch corruption
- [ ] Documentation

## Development

### Prerequisites

- Go 1.21+
- Access to Databricks workspace for testing

### Build

```bash
go build -o proxy-server
```

### Run Tests

```bash
go test ./...
```

## Architecture

```go
// Core components (to be implemented)

// Proxy handles incoming requests
type Proxy struct {
    Target string
    Config *ProxyConfig
    Interceptors []Interceptor
}

// Interceptor modifies requests/responses
type Interceptor interface {
    InterceptRequest(req *Request) error
    InterceptResponse(resp *Response) error
}

// FailureInjector implements specific failure scenarios
type FailureInjector struct {
    Scenario FailureScenario
    Config FailureConfig
}
```

## Security Considerations

- Proxy should only be used in test environments
- Do not log sensitive data (tokens, credentials)
- Use TLS for proxy-to-server communication
- Validate all configuration inputs

## Future Enhancements

- WebUI for real-time traffic inspection
- Record/replay functionality
- Performance metrics collection
- Docker container for easy deployment
- Kubernetes sidecar pattern support
