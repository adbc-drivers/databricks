# Proxy Server YAML Configuration Schema

## Design Philosophy

**YAML defines WHAT to inject, API controls WHEN**

- YAML: Declare available failure scenarios (version controlled, reviewed)
- API: Enable/disable scenarios at test runtime (deterministic, reproducible)
- When enabled: Scenario activates on next matching request

---

## Top-Level Structure

```yaml
proxy:
  # Proxy server settings

failure_scenarios:
  # Array of available failure scenarios
```

---

## 1. Proxy Configuration

```yaml
proxy:
  listen_port: 8080                                    # Required: Port for Thrift/HTTP traffic
  target_server: "https://workspace.databricks.com"   # Required: Upstream Databricks server
  api_port: 8081                                       # Optional: Control API port (default: 8081)
  log_requests: true                                   # Optional: Log all requests (default: false)
  log_level: "info"                                    # Optional: debug|info|warn|error (default: info)
```

### Required Fields
- `listen_port`: Port for proxy (Thrift + HTTP CloudFetch)
- `target_server`: Upstream Databricks server URL

### Optional Fields
- `api_port`: Control API port (default: 8081)
- `log_requests`: Enable request/response logging
- `log_level`: Logging verbosity

---

## 2. Failure Scenario Structure

Each scenario defines a failure that can be injected:

```yaml
- name: "scenario_name"           # Required: Unique identifier
  description: "What this tests"  # Required: Human-readable description
  operation: "FetchResults"       # Optional: Which Thrift operation to match
  action: "return_error"          # Required: What to inject
  # ... action-specific parameters
```

### Core Fields

| Field | Required | Type | Description |
|-------|----------|------|-------------|
| `name` | ✅ | string | Unique scenario identifier |
| `description` | ✅ | string | What this scenario tests |
| `operation` | ❌ | string | Which Thrift operation to match (if omitted, matches any) |
| `action` | ✅ | string | What failure to inject |
| `jira` | ❌ | string | JIRA ticket for traceability |
| `priority` | ❌ | string | P0/P1/P2 priority |

### `operation` Field (Optional)

If specified, scenario only activates for this Thrift operation:

**Valid operations:**
- `OpenSession` - Session creation
- `CloseSession` - Session termination
- `ExecuteStatement` - Query execution
- `GetOperationStatus` - Status polling
- `FetchResults` - Result fetching
- `GetResultSetMetadata` - Schema fetching
- `CancelOperation` - Query cancellation
- `CloseOperation` - Operation cleanup
- `GetCatalogs`, `GetSchemas`, `GetTables`, `GetColumns`, `GetFunctions` - Metadata operations
- `CloudFetchDownload` - HTTP download from cloud storage (special)

**If omitted:** Scenario applies to any matching request (e.g., any CloudFetch download)

---

## 3. Action Types

Actions define **WHAT** failure to inject when scenario is enabled.

### 3.1 `return_error` - Return error response

Return a Thrift error or HTTP error to the driver.

```yaml
action: "return_error"
error_code: 403                     # Optional: HTTP error code (for CloudFetch)
error_message: "Error description"  # Required: Error message
retryable: true                     # Optional: Whether error is retryable (default: false)
```

**Examples:**

```yaml
# CloudFetch: Azure 403 error
- name: "cloudfetch_azure_403"
  description: "Azure returns 403 Forbidden"
  operation: "CloudFetchDownload"
  action: "return_error"
  error_code: 403
  error_message: "[FILES_API_AZURE_FORBIDDEN]"

# Thrift: Invalid session handle
- name: "invalid_session_handle"
  description: "Session handle is invalid"
  operation: "GetOperationStatus"
  action: "return_error"
  error_message: "Invalid SessionHandle"
  retryable: true
```

---

### 3.2 `delay` - Inject latency

Add artificial delay to simulate timeouts or slow operations.

```yaml
action: "delay"
duration: "65s"        # Required: Delay duration (e.g., "5s", "2m", "1h")
```

**Examples:**

```yaml
# CloudFetch timeout
- name: "cloudfetch_timeout"
  description: "CloudFetch download times out"
  operation: "CloudFetchDownload"
  action: "delay"
  duration: "65s"  # Exceeds typical 60s timeout

# Query execution delay
- name: "query_execution_slow"
  description: "Query takes too long"
  operation: "ExecuteStatement"
  action: "delay"
  duration: "35s"
```

---

### 3.3 `close_connection` - Drop TCP connection

Abruptly close the connection to simulate network failures.

```yaml
action: "close_connection"
at_byte: 5000          # Optional: Close after N bytes transferred
```

**Examples:**

```yaml
# Connection reset during result fetch
- name: "connection_reset_during_fetch"
  description: "Connection reset while fetching results"
  operation: "FetchResults"
  action: "close_connection"

# Connection reset during CloudFetch download
- name: "cloudfetch_connection_reset"
  description: "Connection reset during download"
  operation: "CloudFetchDownload"
  action: "close_connection"
  at_byte: 1048576  # After 1MB
```

---

### 3.4 `expire_cloud_link` - Expire CloudFetch URL

Modify CloudFetch presigned URL to simulate expired signature.

```yaml
action: "expire_cloud_link"
```

**Example:**

```yaml
# Expired CloudFetch link
- name: "cloudfetch_expired_link"
  description: "CloudFetch link has expired"
  operation: "CloudFetchDownload"
  action: "expire_cloud_link"
```

---

### 3.5 `ssl_error` - Return SSL/TLS error

Simulate SSL certificate or handshake errors.

```yaml
action: "ssl_error"
error_type: "certificate_validation_failed"  # Required: Type of SSL error
```

**Valid error types:**
- `certificate_validation_failed` - Invalid/untrusted certificate
- `handshake_timeout` - TLS handshake timeout
- `connection_reset` - SSL connection reset

**Examples:**

```yaml
# SSL certificate error
- name: "cloudfetch_ssl_cert_error"
  description: "SSL certificate validation fails"
  operation: "CloudFetchDownload"
  action: "ssl_error"
  error_type: "certificate_validation_failed"

# TLS handshake timeout
- name: "tls_handshake_timeout"
  description: "TLS handshake exceeds timeout"
  action: "ssl_error"
  error_type: "handshake_timeout"
```

---

### 3.6 `modify_response` - Modify response data

Modify the Thrift response (advanced, for testing edge cases).

```yaml
action: "modify_response"
field: "directResults"              # Required: Which field to modify
modification: "exceed_maxrows"      # Required: How to modify
```

**Examples:**

```yaml
# DirectResults exceeds MaxRows
- name: "direct_results_exceed_maxrows"
  description: "DirectResults returns more rows than MaxRows"
  operation: "OpenSession"
  action: "modify_response"
  field: "directResults"
  modification: "exceed_maxrows"
```

---

### 3.7 `invalidate_session` - Invalidate session handle

Make the session handle invalid on the server side.

```yaml
action: "invalidate_session"
```

**Example:**

```yaml
# Session invalidated unexpectedly
- name: "session_invalidated"
  description: "Session becomes invalid"
  action: "invalidate_session"
```

---

## 4. Complete Example

```yaml
# proxy-config.yaml
proxy:
  listen_port: 8080
  target_server: "https://workspace.databricks.com"
  api_port: 8081
  log_requests: true
  log_level: "debug"

failure_scenarios:
  # === CloudFetch Failures ===

  - name: "cloudfetch_expired_link"
    description: "CloudFetch link expires, driver should retry"
    operation: "CloudFetchDownload"
    action: "expire_cloud_link"

  - name: "cloudfetch_azure_403"
    description: "Azure returns 403 Forbidden"
    operation: "CloudFetchDownload"
    action: "return_error"
    error_code: 403
    error_message: "[FILES_API_AZURE_FORBIDDEN]"

  - name: "cloudfetch_timeout"
    description: "CloudFetch download times out"
    operation: "CloudFetchDownload"
    action: "delay"
    duration: "65s"

  - name: "cloudfetch_connection_reset"
    description: "Connection reset during CloudFetch download"
    operation: "CloudFetchDownload"
    action: "close_connection"

  # === Connection Failures ===

  - name: "connection_reset_during_fetch"
    description: "Connection reset when fetching large results"
    operation: "FetchResults"
    action: "close_connection"

  - name: "tls_handshake_timeout"
    description: "TLS handshake times out"
    action: "ssl_error"
    error_type: "handshake_timeout"

  # === Session Failures ===

  - name: "invalid_session_handle"
    description: "Session invalidated but client unaware"
    operation: "GetOperationStatus"
    action: "return_error"
    error_message: "Invalid SessionHandle"

  - name: "session_terminated_active_query"
    description: "Session terminated with active queries"
    operation: "GetOperationStatus"
    action: "return_error"
    error_message: "Session terminated with active queries"

  # === Query Execution Failures ===

  - name: "query_execution_timeout"
    description: "Query execution times out"
    operation: "ExecuteStatement"
    action: "delay"
    duration: "35s"
```

---

## 5. How Tests Use This

### Start proxy once:
```bash
go run main.go --config proxy-config.yaml
# Proxy starts with all scenarios defined but NONE active
```

### Tests enable scenarios:
```csharp
[Fact]
public async Task TestCloudFetchExpiredLink()
{
    var proxy = new ProxyControlClient("http://localhost:8081");

    // Enable scenario - NEXT CloudFetch will return expired link
    await proxy.EnableScenario("cloudfetch_expired_link");

    try
    {
        // This triggers the failure (deterministic!)
        var result = await driver.ExecuteQuery("SELECT * FROM large_table");

        // Verify driver retried successfully
        Assert.NotNull(result);
    }
    finally
    {
        // Clean up
        await proxy.DisableScenario("cloudfetch_expired_link");
    }
}

[Fact]
public async Task TestAzure403Error()
{
    var proxy = new ProxyControlClient("http://localhost:8081");

    // Different test, different scenario, same proxy!
    await proxy.EnableScenario("cloudfetch_azure_403");

    try
    {
        var result = await driver.ExecuteQuery("SELECT * FROM large_table");
        Assert.NotNull(result);
    }
    finally
    {
        await proxy.DisableScenario("cloudfetch_azure_403");
    }
}
```

---

## 6. Field Reference Summary

### Scenario Core Fields
| Field | Required | Type | Description |
|-------|----------|------|-------------|
| `name` | ✅ | string | Unique identifier |
| `description` | ✅ | string | What this tests |
| `operation` | ❌ | string | Which Thrift operation (if omitted, any) |
| `action` | ✅ | string | What to inject |
| `jira` | ❌ | string | JIRA ticket |
| `priority` | ❌ | string | P0/P1/P2 |

### Action-Specific Fields

| Action | Required Parameters | Optional Parameters |
|--------|-------------------|---------------------|
| `return_error` | `error_message` | `error_code`, `retryable` |
| `delay` | `duration` | - |
| `close_connection` | - | `at_byte` |
| `expire_cloud_link` | - | - |
| `ssl_error` | `error_type` | - |
| `modify_response` | `field`, `modification` | - |
| `invalidate_session` | - | - |

---

## 7. Validation Rules

1. **Unique Names**: Each `name` must be unique
2. **Valid Operations**: `operation` must be a valid Thrift operation or omitted
3. **Valid Actions**: `action` must be one of the defined types
4. **Required Parameters**: Action-specific required fields must be present
5. **Duration Format**: `duration` must match `\d+(s|m|h)` (e.g., "5s", "2m")
6. **Error Types**: `error_type` for `ssl_error` must be valid
7. **Port Range**: Ports must be 1-65535
