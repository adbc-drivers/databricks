# Copyright (c) 2025 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.PHONY: all start-proxy stop-proxy clean generate-clients validate-api

# Default target
all: generate-csharp

# Start mitmproxy with the addon
start-proxy:
	@echo "Starting mitmproxy with failure injection addon..."
	@echo "Control API will be available at http://localhost:18081"
	@echo "Proxy will listen on port 18080"
	@mitmdump -s mitmproxy_addon.py --set block_global=false --listen-port 18080

# Stop any running mitmproxy instances
stop-proxy:
	@echo "Stopping mitmproxy..."
	@pkill -f "mitmdump.*mitmproxy_addon.py" || echo "No mitmproxy process found"

# Clean generated artifacts
clean:
	rm -rf generated/

# Validate OpenAPI specification
validate-api:
	@echo "Validating OpenAPI spec..."
	@if command -v openapi-generator >/dev/null 2>&1; then \
		openapi-generator validate -i openapi.yaml; \
	elif command -v openapi-generator-cli >/dev/null 2>&1; then \
		openapi-generator-cli validate -i openapi.yaml; \
	else \
		echo "⚠️  openapi-generator not found. Install with:"; \
		echo "   brew install openapi-generator"; \
		echo "   OR npm install -g @openapitools/openapi-generator-cli"; \
	fi

# Generate all language clients
generate-clients: generate-csharp generate-java generate-cpp generate-python generate-go

# Generate C# client
generate-csharp:
	@echo "Generating C# client..."
	@mkdir -p generated/csharp
	@if command -v openapi-generator >/dev/null 2>&1; then \
		openapi-generator generate \
			-i openapi.yaml \
			-g csharp \
			-o generated/csharp \
			--additional-properties=packageName=ProxyControlApi,targetFramework=net8.0; \
	else \
		openapi-generator-cli generate \
			-i openapi.yaml \
			-g csharp \
			-o generated/csharp \
			--additional-properties=packageName=ProxyControlApi,targetFramework=net8.0; \
	fi

# Generate Java client
generate-java:
	@echo "Generating Java client..."
	@mkdir -p generated/java
	@if command -v openapi-generator >/dev/null 2>&1; then \
		openapi-generator generate \
			-i openapi.yaml \
			-g java \
			-o generated/java \
			--additional-properties=library=okhttp-gson,groupId=com.databricks,artifactId=proxy-control-client,invokerPackage=com.databricks.proxy,apiPackage=com.databricks.proxy.api,modelPackage=com.databricks.proxy.model; \
	else \
		openapi-generator-cli generate \
			-i openapi.yaml \
			-g java \
			-o generated/java \
			--additional-properties=library=okhttp-gson,groupId=com.databricks,artifactId=proxy-control-client,invokerPackage=com.databricks.proxy,apiPackage=com.databricks.proxy.api,modelPackage=com.databricks.proxy.model; \
	fi

# Generate C++ client
generate-cpp:
	@echo "Generating C++ client..."
	@mkdir -p generated/cpp
	@if command -v openapi-generator >/dev/null 2>&1; then \
		openapi-generator generate \
			-i openapi.yaml \
			-g cpp-restsdk \
			-o generated/cpp; \
	else \
		openapi-generator-cli generate \
			-i openapi.yaml \
			-g cpp-restsdk \
			-o generated/cpp; \
	fi

# Generate Python client
generate-python:
	@echo "Generating Python client..."
	@mkdir -p generated/python
	@if command -v openapi-generator >/dev/null 2>&1; then \
		openapi-generator generate \
			-i openapi.yaml \
			-g python \
			-o generated/python \
			--additional-properties=packageName=proxy_control_client,projectName=proxy-control-client; \
	else \
		openapi-generator-cli generate \
			-i openapi.yaml \
			-g python \
			-o generated/python \
			--additional-properties=packageName=proxy_control_client,projectName=proxy-control-client; \
	fi

# Generate Go client
generate-go:
	@echo "Generating Go client..."
	@mkdir -p generated/go
	@if command -v openapi-generator >/dev/null 2>&1; then \
		openapi-generator generate \
			-i openapi.yaml \
			-g go \
			-o generated/go \
			--additional-properties=packageName=proxycontrol,isGoSubmodule=true; \
	else \
		openapi-generator-cli generate \
			-i openapi.yaml \
			-g go \
			-o generated/go \
			--additional-properties=packageName=proxycontrol,isGoSubmodule=true; \
	fi

# Help target
help:
	@echo "Available targets:"
	@echo "  make start-proxy        - Start mitmproxy with failure injection addon"
	@echo "  make stop-proxy         - Stop running mitmproxy instances"
	@echo "  make clean              - Remove generated artifacts"
	@echo "  make validate-api       - Validate OpenAPI spec"
	@echo "  make generate-clients   - Generate all language clients"
	@echo "  make generate-csharp    - Generate C# client only"
	@echo "  make generate-java      - Generate Java client only"
	@echo "  make generate-cpp       - Generate C++ client only"
	@echo "  make generate-python    - Generate Python client only"
	@echo "  make generate-go        - Generate Go client only"
	@echo ""
	@echo "See README-mitmproxy.md for mitmproxy setup and usage"
	@echo "See CLIENTS.md for client usage examples in each language"
