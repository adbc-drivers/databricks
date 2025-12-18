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

# Thrift Protocol Test Infrastructure

A test infrastructure for injecting controlled failures into Databricks Thrift protocol operations and CloudFetch downloads using **mitmproxy** for HTTPS traffic interception.

## Overview

This infrastructure uses mitmproxy as a forward proxy to intercept both:
- **Thrift operations** - Driver ↔ Databricks communication
- **CloudFetch downloads** - Direct downloads from cloud storage (Azure Blob, S3, GCS)

```
Driver (HTTP_PROXY set) → mitmproxy:18080 → Databricks/Cloud Storage
                           ↓
                      Control API:18081
```

## Key Features

✅ **HTTPS Interception** - Inspects and modifies encrypted CloudFetch downloads
✅ **Automatic Certificates** - Generates TLS certificates on-the-fly
✅ **Battle-Tested** - mitmproxy is used by security researchers worldwide
✅ **Multi-Language Clients** - OpenAPI-generated clients for C#, Java, Python, C++, Go
✅ **Test Integration** - Automatically managed by C# test infrastructure

## Quick Start

### Prerequisites

```bash
# Install mitmproxy and Flask
pip install -r requirements.txt

# Trust mitmproxy certificate (macOS, first time only)
sudo security add-trusted-cert -d -r trustRoot \
  -k /Library/Keychains/System.keychain \
  ~/.mitmproxy/mitmproxy-ca-cert.pem
```

For other platforms, see: https://docs.mitmproxy.org/stable/concepts-certificates/

### Run Tests

```bash
# C# tests (proxy starts automatically)
cd test-infrastructure/tests/csharp
export DATABRICKS_TEST_CONFIG_FILE=/path/to/databricks-test-config.json
dotnet test --filter "FullyQualifiedName~CloudFetchTests"

# Manual proxy startup (for development/debugging)
cd test-infrastructure/proxy-server
make start-proxy
```

## Available Failure Scenarios

All scenarios are controlled via the REST API on port 18081:

| Scenario | Description | Effect |
|----------|-------------|--------|
| `cloudfetch_expired_link` | Expired Azure SAS token | Returns 403 with AuthorizationQueryParametersError |
| `cloudfetch_azure_403` | Azure Blob Forbidden | Returns 403 with AuthenticationFailed |
| `cloudfetch_timeout` | 65-second delay | Triggers driver timeout (60s default) |
| `cloudfetch_connection_reset` | Abrupt connection close | Simulates network failure |

### Scenario API Examples

```bash
# Enable a scenario
curl -X POST http://localhost:18081/scenarios/cloudfetch_expired_link/enable

# Check scenario status
curl http://localhost:18081/scenarios/cloudfetch_expired_link

# List all scenarios
curl http://localhost:18081/scenarios

# Disable all scenarios
curl -X POST http://localhost:18081/scenarios/disable-all
```

## OpenAPI Client Generation

The Control API is documented with OpenAPI 3.0, enabling auto-generated clients:

```bash
# Generate C# client
make generate-csharp

# Generate clients for all languages
make generate-clients
```

See [CLIENTS.md](CLIENTS.md) for usage examples in each language.
See [OPENAPI-IMPLEMENTATION.md](OPENAPI-IMPLEMENTATION.md) for implementation details.

## Architecture Details

### How HTTPS Interception Works

1. **Certificate Trust**: mitmproxy generates a root CA certificate on first run (`~/.mitmproxy/mitmproxy-ca-cert.pem`)
2. **Environment Variables**: Driver sets `HTTP_PROXY` and `HTTPS_PROXY` to `http://localhost:18080`
3. **TLS Man-in-the-Middle**: mitmproxy presents its own certificate for HTTPS connections
4. **Request Inspection**: Addon code (`mitmproxy_addon.py`) inspects URLs and injects failures
5. **Transparent Proxying**: Non-failing requests pass through unchanged

### Test Infrastructure Integration

The C# test base class (`ProxyTestBase`) automatically:
- Starts mitmproxy before each test
- Sets proxy environment variables
- Configures driver to trust mitmproxy certificate
- Stops proxy and cleans up after test

See `test-infrastructure/tests/csharp/ProxyTestBase.cs` for implementation.

## Files

| File | Purpose |
|------|---------|
| `mitmproxy_addon.py` | mitmproxy addon with Flask control API |
| `requirements.txt` | Python dependencies |
| `openapi.yaml` | OpenAPI spec for Control API |
| `Makefile` | Build automation (client generation, proxy management) |
| `CLIENTS.md` | Multi-language client usage examples |
| `OPENAPI-IMPLEMENTATION.md` | OpenAPI design and implementation guide |

## Development

```bash
# Validate OpenAPI spec
make validate-api

# Start proxy manually
make start-proxy

# Stop proxy
make stop-proxy

# Clean generated files
make clean
```

## Troubleshooting

**Issue**: Tests hang or connection refused
**Solution**: Ensure mitmproxy is installed and certificate is trusted

**Issue**: HTTPS connections fail with certificate errors
**Solution**: Trust mitmproxy CA certificate (see Prerequisites)

**Issue**: CloudFetch still succeeds despite enabled scenario
**Solution**: Scenarios are one-shot (auto-disable after first use). Re-enable for next test.

**Issue**: Proxy doesn't intercept CloudFetch URLs
**Solution**: Verify `HTTP_PROXY` and `HTTPS_PROXY` environment variables are set correctly
