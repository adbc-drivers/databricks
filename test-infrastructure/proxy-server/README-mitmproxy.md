# Thrift Protocol Test Infrastructure - mitmproxy Implementation

This implementation uses **mitmproxy** for HTTPS traffic interception and failure injection.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run tests (proxy starts automatically)
cd test-infrastructure/tests/csharp
dotnet test --filter "FullyQualifiedName~CloudFetchTests"
```

## Key Benefits of mitmproxy

✅ **HTTPS Interception** - Can inspect CloudFetch downloads to cloud storage  
✅ **Automatic Certificates** - Generates TLS certificates on-the-fly  
✅ **Battle-Tested** - Used by security researchers worldwide  
✅ **Compatible API** - Works with existing C# test infrastructure  

## Architecture

```
Driver (HTTP_PROXY set) → mitmproxy:18080 → Databricks/Cloud Storage
                           ↓
                      Control API:18081
```

## Setup

Install mitmproxy and trust its certificate (first time only):

```bash
# Install
pip install -r requirements.txt

# Trust certificate (macOS)
sudo security add-trusted-cert -d -r trustRoot \
  -k /Library/Keychains/System.keychain \
  ~/.mitmproxy/mitmproxy-ca-cert.pem
```

For other platforms, see official docs: https://docs.mitmproxy.org/stable/concepts-certificates/

## Available Scenarios

All scenarios work via Control API (port 18081):

- `cloudfetch_expired_link` - Expired Azure SAS token (403)
- `cloudfetch_azure_403` - Azure Blob Forbidden error
- `cloudfetch_timeout` - 65s delay (exceeds 60s timeout)
- `cloudfetch_connection_reset` - Abrupt connection close

## Files

- `mitmproxy_addon.py` - Main addon with Flask control API
- `requirements.txt` - Python dependencies
- `README-mitmproxy.md` - This file

