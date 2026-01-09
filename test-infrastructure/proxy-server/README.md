# Thrift Proxy Server

A mitmproxy-based proxy server for testing ADBC driver behavior under various failure scenarios.

## Overview

This proxy sits between ADBC drivers and the real Databricks Thrift server, intercepting both HTTP (Thrift) and HTTPS (CloudFetch) traffic to enable controlled failure injection for integration testing.

```
Driver → mitmproxy (port 18080) → Databricks Server
           ↓
    Flask Control API (port 18081)
           ↓
    Enable/disable failure scenarios
```

## Features

- **HTTPS Interception**: Full man-in-the-middle for CloudFetch downloads from cloud storage (S3, Azure Blob, GCS)
- **Thrift Protocol Decoding**: Tracks and verifies all Thrift RPC calls for test assertions
- **Flask Control API**: REST API for programmatic scenario management
- **Production-Validated Scenarios**: 10+ failure scenarios based on real customer issues (JIRA-referenced)
- **One-Shot Behavior**: Scenarios automatically disable after injection for deterministic testing
- **No Restart Required**: Tests enable/disable scenarios via API without restarting proxy

## Design Philosophy: Hybrid Approach

**"Code Defines WHAT, API Controls WHEN"**

The proxy follows a hybrid design pattern inspired by mocking frameworks:

1. **Scenarios are defined once** in Python code (`mitmproxy_addon.py`)
   - Version controlled and reviewable
   - All possible failure scenarios documented in code
   - Maps to JIRA tickets for production issues

2. **Tests control activation** via Flask API (port 18081)
   - Enable scenario: `POST /scenarios/{name}/enable`
   - Disable scenario: `POST /scenarios/{name}/disable`
   - No proxy restart needed between tests

3. **Fast test execution**
   - Start proxy once: `mitmdump -s mitmproxy_addon.py`
   - Run hundreds of tests enabling different scenarios
   - Each test gets clean state (scenarios auto-disable after injection)

**Benefits:**
- ✅ Fast: No restart overhead between tests
- ✅ Simple: Clear separation between definition (code) and control (API)
- ✅ Deterministic: One-shot behavior ensures predictable test results
- ✅ Traceable: JIRA references link scenarios to production issues

## Installation

### Prerequisites

- Python 3.11+
- pip

### Install Dependencies

```bash
pip install -r requirements.txt
```

This installs:
- `mitmproxy` - HTTPS proxy with certificate generation
- `flask` - Control API server
- `thrift` - Thrift protocol parsing

## Usage

### Starting the Proxy

```bash
# Basic usage
mitmdump -s mitmproxy_addon.py --listen-port 18080

# With custom API port
mitmdump -s mitmproxy_addon.py --listen-port 18080 --set api_port=18081
```

The proxy will:
1. Start listening on port 18080 for HTTP/HTTPS traffic
2. Start Flask API server on port 18081
3. Generate TLS certificates in `~/.mitmproxy/` (first run only)

### Control API Endpoints

#### List Available Scenarios

```bash
curl http://localhost:18081/scenarios
```

Returns:
```json
{
  "scenarios": [
    {
      "name": "cloudfetch_expired_link",
      "description": "CloudFetch link expires, driver should retry via FetchResults",
      "enabled": false
    },
    ...
  ]
}
```

#### Enable a Scenario

```bash
curl -X POST http://localhost:18081/scenarios/cloudfetch_expired_link/enable
```

**With runtime configuration** (for configurable scenarios like delays):
```bash
curl -X POST http://localhost:18081/scenarios/cloudfetch_timeout/enable \
  -H "Content-Type: application/json" \
  -d '{"duration_seconds": 30}'
```

#### Disable a Scenario

```bash
curl -X POST http://localhost:18081/scenarios/cloudfetch_expired_link/disable
```

#### Get Scenario Status

```bash
curl http://localhost:18081/scenarios/cloudfetch_expired_link/status
```

#### Thrift Call Verification

Get all Thrift calls (for test verification):
```bash
curl http://localhost:18081/thrift/calls
```

Count specific Thrift method calls:
```bash
curl http://localhost:18081/thrift/calls/count?method=FetchResults
```

Reset call history (automatically resets when scenario enabled):
```bash
curl -X POST http://localhost:18081/thrift/calls/reset
```

## Available Failure Scenarios

### CloudFetch Failures

| Scenario | HTTP Code | Description | JIRA Reference |
|----------|-----------|-------------|----------------|
| `cloudfetch_expired_link` | 403 | Expired CloudFetch link, driver should refresh via FetchResults | PECOBLR-1131 |
| `cloudfetch_400` | 400 | Bad Request (malformed request or missing parameters) | - |
| `cloudfetch_403` | 403 | Forbidden (expired link or insufficient permissions) | ES-1624602 |
| `cloudfetch_404` | 404 | Not Found (object does not exist) | - |
| `cloudfetch_405` | 405 | Method Not Allowed (incorrect HTTP method) | - |
| `cloudfetch_412` | 412 | Precondition Failed (condition not met) | - |
| `cloudfetch_500` | 500 | Internal Server Error (server-side error) | - |
| `cloudfetch_503` | 503 | Service Unavailable (rate limiting or temporary failure) | - |
| `cloudfetch_timeout` | - | Download exceeds 60s timeout (configurable delay) | BL-13239 |
| `cloudfetch_connection_reset` | - | Connection abruptly closed during download | BL-13580 |

## Using from Tests

### C# Example

```csharp
public class CloudFetchTests : ProxyTestBase
{
    [Fact]
    public async Task TestCloudFetchExpiredLink()
    {
        // Proxy automatically started by ProxyTestBase.InitializeAsync()

        // Enable failure scenario
        await ControlClient.EnableScenarioAsync("cloudfetch_expired_link");

        // Run driver code
        using var connection = CreateProxiedConnection();
        using var statement = connection.CreateStatement();
        statement.SqlQuery = "SELECT * FROM large_table";
        var result = statement.ExecuteQuery();

        // Verify driver behavior
        Assert.NotNull(result);

        // Verify driver called FetchResults to refresh the link
        int fetchResultsCalls = await ControlClient.CountThriftMethodCallsAsync("FetchResults");
        Assert.True(fetchResultsCalls > 0);

        // Scenario automatically disabled after one-shot injection
    }
}
```

The `ProxyTestBase` class:
- Starts/stops mitmproxy automatically per test
- Sets `HTTP_PROXY` and `HTTPS_PROXY` environment variables
- Configures driver with TLS certificate trust settings
- Resets scenario state between tests

### Configuration Required

Drivers must be configured to:

1. **Route through proxy**:
   ```csharp
   parameters["adbc.proxy_options.use_proxy"] = "true";
   parameters["adbc.proxy_options.proxy_host"] = "localhost";
   parameters["adbc.proxy_options.proxy_port"] = "18080";
   ```

2. **Trust mitmproxy's self-signed certificate**:
   ```csharp
   parameters["adbc.http_options.tls.allow_self_signed"] = "true";
   parameters["adbc.http_options.tls.disable_server_certificate_validation"] = "true";
   parameters["adbc.http_options.tls.allow_hostname_mismatch"] = "true";
   parameters["adbc.http_options.tls.trusted_certificate_path"] = "~/.mitmproxy/mitmproxy-ca-cert.pem";
   ```

3. **Set environment variables** (for CloudFetch HTTP client):
   ```csharp
   Environment.SetEnvironmentVariable("HTTP_PROXY", "http://localhost:18080");
   Environment.SetEnvironmentVariable("HTTPS_PROXY", "http://localhost:18080");
   ```

## Architecture

### Components

1. **mitmproxy_addon.py** - Main proxy implementation
   - Intercepts HTTP/HTTPS requests
   - Implements failure injection logic
   - Runs Flask API server in background thread
   - Decodes Thrift protocol for call tracking

2. **thrift_decoder.py** - Thrift protocol decoder
   - Parses Thrift binary protocol
   - Extracts method names and message types
   - Enables test assertions on Thrift calls

3. **requirements.txt** - Python dependencies
   - mitmproxy
   - flask
   - thrift

### Failure Injection Flow

```
1. Test enables scenario via API: POST /scenarios/{name}/enable
2. Proxy stores scenario config in memory
3. Driver makes request → mitmproxy intercepts
4. Proxy checks if scenario enabled AND matches request
5. If match: inject failure, disable scenario (one-shot)
6. If no match: forward request normally
7. Test verifies behavior via Thrift call tracking
```

### One-Shot Behavior

Scenarios automatically disable after first injection to ensure:
- **Deterministic testing**: Each scenario triggers exactly once
- **Test isolation**: Scenarios don't affect subsequent operations
- **Clear causality**: Easy to correlate failure with driver behavior

## Adding New Scenarios

To add a new failure scenario:

1. **Define scenario** in `mitmproxy_addon.py`:
   ```python
   SCENARIOS = {
       "my_new_scenario": {
           "description": "What this tests",
           "operation": "CloudFetchDownload",  # or omit for any operation
           "action": "return_error",
           "error_code": 500,
           "error_message": "Custom error"
       }
   }
   ```

2. **Implement injection logic** in `response()` or `request()` hooks

3. **Add test** in C#:
   ```csharp
   [Fact]
   public async Task TestMyNewScenario()
   {
       await ControlClient.EnableScenarioAsync("my_new_scenario");
       // ... test code ...
   }
   ```

4. **Document** with JIRA reference if based on production issue

## Debugging

### View All Traffic

```bash
# Start with verbose logging
mitmdump -s mitmproxy_addon.py --listen-port 18080 -v
```

### Inspect Certificates

```bash
# Check certificate generated
ls -la ~/.mitmproxy/

# View certificate details
openssl x509 -in ~/.mitmproxy/mitmproxy-ca-cert.pem -text -noout
```

### Test Scenarios Manually

```bash
# Start proxy
mitmdump -s mitmproxy_addon.py --listen-port 18080 &

# Enable scenario
curl -X POST http://localhost:18081/scenarios/cloudfetch_403/enable

# Make request through proxy (will trigger failure)
curl -x http://localhost:18080 https://example.com

# Check status
curl http://localhost:18081/scenarios/cloudfetch_403/status
```

## Security Considerations

- **Test environments only**: Never use in production
- **No sensitive logging**: Proxy does not log tokens or credentials
- **Certificate trust**: mitmproxy CA certificate must be trusted by system/driver
- **Localhost only**: API server binds to localhost by default

## References

- [mitmproxy Documentation](https://docs.mitmproxy.org/) - Official mitmproxy docs
- [Test Infrastructure README](../README.md) - Main test infrastructure overview
