# Proxy Control API Clients

This document explains how to generate Control API clients for different languages from the OpenAPI specification.

## Overview

The Proxy Control API is defined in `openapi.yaml` using the OpenAPI 3.0 specification. Client libraries can be automatically generated for 50+ languages, eliminating the need to manually implement HTTP clients for each language.

## Prerequisites

Install the OpenAPI Generator CLI:

```bash
# macOS
brew install openapi-generator

# Or use npm
npm install -g @openapitools/openapi-generator-cli

# Or use Docker
docker pull openapitools/openapi-generator-cli
```

## Generate Clients

### C#

```bash
# Generate C# client for .NET
openapi-generator-cli generate \
  -i openapi.yaml \
  -g csharp-netcore \
  -o generated/csharp \
  --additional-properties=packageName=ProxyControlApi,targetFramework=net8.0

# Output: generated/csharp/src/ProxyControlApi/
```

**Usage in tests:**
```csharp
using ProxyControlApi.Api;
using ProxyControlApi.Client;

var config = new Configuration { BasePath = "http://localhost:18081" };
var api = new ScenariosApi(config);

// List scenarios
var scenarios = api.ListScenarios();

// Enable a scenario
var status = api.EnableScenario("cloudfetch_timeout");
```

### Java

```bash
openapi-generator-cli generate \
  -i openapi.yaml \
  -g java \
  -o generated/java \
  --additional-properties=library=okhttp-gson,groupId=com.databricks,artifactId=proxy-control-client
```

**Usage:**
```java
import com.databricks.proxy.*;
import com.databricks.proxy.api.*;

ApiClient client = new ApiClient();
client.setBasePath("http://localhost:18081");
ScenariosApi api = new ScenariosApi(client);

// Enable scenario
ScenarioStatus status = api.enableScenario("cloudfetch_timeout");
```

### C++

```bash
openapi-generator-cli generate \
  -i openapi.yaml \
  -g cpp-restsdk \
  -o generated/cpp
```

**Usage:**
```cpp
#include "ProxyControlApi.h"

auto api = std::make_shared<org::openapitools::client::api::ScenariosApi>();
api->setBasePath(U("http://localhost:18081"));

// Enable scenario
auto status = api->enableScenario(U("cloudfetch_timeout")).get();
```

### Python

```bash
openapi-generator-cli generate \
  -i openapi.yaml \
  -g python \
  -o generated/python \
  --additional-properties=packageName=proxy_control_client
```

**Usage:**
```python
from proxy_control_client import ApiClient, Configuration, ScenariosApi

config = Configuration(host="http://localhost:18081")
api = ScenariosApi(ApiClient(config))

# Enable scenario
status = api.enable_scenario("cloudfetch_timeout")
```

### Go

```bash
openapi-generator-cli generate \
  -i openapi.yaml \
  -g go \
  -o generated/go \
  --additional-properties=packageName=proxycontrol
```

**Usage:**
```go
import "generated/go/proxycontrol"

config := proxycontrol.NewConfiguration()
config.Servers[0].URL = "http://localhost:18081"
client := proxycontrol.NewAPIClient(config)

// Enable scenario
status, _, _ := client.ScenariosApi.EnableScenario(context.Background(), "cloudfetch_timeout").Execute()
```

### Rust

```bash
openapi-generator-cli generate \
  -i openapi.yaml \
  -g rust \
  -o generated/rust
```

## Supported Languages

The OpenAPI Generator supports 50+ languages including:

- **JVM**: Java, Kotlin, Scala, Clojure
- **C/C++**: C++, C (libcurl)
- **.NET**: C#, F#, VB.NET
- **Web**: TypeScript, JavaScript, PHP, Ruby
- **Systems**: Go, Rust, Swift, Objective-C
- **Scripting**: Python, Perl, R, Lua
- **Functional**: Haskell, Elm, Erlang

See full list: https://openapi-generator.tech/docs/generators

## Testing Without Generated Clients

For simple testing or languages not yet generated, use curl:

```bash
# List scenarios
curl http://localhost:18081/scenarios

# Enable scenario
curl -X POST http://localhost:18081/scenarios/cloudfetch_timeout/enable

# Disable scenario
curl -X POST http://localhost:18081/scenarios/cloudfetch_timeout/disable
```

## Integration with Build Systems

### CMake (C++)
```cmake
# Add to CMakeLists.txt
add_custom_command(
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/generated/cpp
  COMMAND openapi-generator-cli generate -i ${CMAKE_SOURCE_DIR}/openapi.yaml -g cpp-restsdk -o ${CMAKE_CURRENT_BINARY_DIR}/generated/cpp
  DEPENDS ${CMAKE_SOURCE_DIR}/openapi.yaml
)
```

### Maven (Java)
```xml
<!-- Add to pom.xml -->
<plugin>
  <groupId>org.openapitools</groupId>
  <artifactId>openapi-generator-maven-plugin</artifactId>
  <version>7.0.0</version>
  <executions>
    <execution>
      <goals>
        <goal>generate</goal>
      </goals>
      <configuration>
        <inputSpec>${project.basedir}/openapi.yaml</inputSpec>
        <generatorName>java</generatorName>
        <output>${project.build.directory}/generated-sources</output>
      </configuration>
    </execution>
  </executions>
</plugin>
```

### .NET (.csproj)
```xml
<!-- Add to .csproj -->
<Target Name="GenerateOpenApiClient" BeforeTargets="CoreCompile">
  <Exec Command="openapi-generator-cli generate -i openapi.yaml -g csharp-netcore -o generated/csharp" />
</Target>
```

## Benefits

**Before (manual implementation per language):**
- C#: 160 lines (ProxyControlClient.cs)
- Java: ~150 lines (estimated)
- C++: ~180 lines (estimated)
- Python: ~120 lines (estimated)
- **Total: ~610 lines × N languages**

**After (generated clients):**
- OpenAPI spec: 1 file (openapi.yaml)
- Generation command: 1 line per language
- **Total: ~0 lines of client code to maintain**

## Validation

Validate the OpenAPI spec:

```bash
openapi-generator-cli validate -i openapi.yaml
```

## Next Steps

1. Generate client for your language using commands above
2. Integrate generation into your build system
3. Replace manual HTTP client code with generated client
4. Contribute language-specific examples to this document
