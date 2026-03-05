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

# C# Coding Guidelines - ADBC Databricks Driver

This document provides coding standards and best practices for contributing to the C# ADBC Databricks driver.

## Table of Contents

1. [Formatting & Style](#1-formatting--style)
2. [Naming Conventions](#2-naming-conventions)
3. [Language Preferences](#3-language-preferences)
4. [Project Configuration](#4-project-configuration)
5. [Documentation](#5-documentation)
6. [Testing](#6-testing)
7. [Code Patterns](#7-code-patterns)
8. [Pull Request Guidelines](#8-pull-request-guidelines)
9. [Key Principles](#9-key-principles)
10. [Tracing & Logging](#10-tracing--logging)

---

## 1. Formatting & Style

| Setting | Value |
|---------|-------|
| Indentation | 4 spaces |
| XML files (`.csproj`, `.props`, `.targets`) | 2 spaces |
| Line endings | Newline at end of file |
| Trailing whitespace | Trimmed |
| Braces | Allman style (new line before open brace) |
| Max line length | No hard limit, but keep readable |

### Brace Style

```csharp
// Good - Allman style
if (condition)
{
    DoSomething();
}
else
{
    DoSomethingElse();
}

// Avoid - K&R style
if (condition) {
    DoSomething();
}
```

### Spacing

```csharp
// Good
if (condition)
for (int i = 0; i < count; i++)
method(arg1, arg2)
array[index]

// Avoid
if(condition)
for(int i=0;i<count;i++)
method( arg1 , arg2 )
array[ index ]
```

---

## 2. Naming Conventions

| Element | Convention | Example |
|---------|------------|---------|
| **Private/internal fields** | `_camelCase` | `_enableDirectResults` |
| **Private static fields** | `s_camelCase` | `s_assemblyName` |
| **Constants** | `PascalCase` | `DefaultConfigEnvironmentVariable` |
| **Public properties** | `PascalCase` | `UseCloudFetch` |
| **Methods** | `PascalCase` | `ValidateProperties()` |
| **Parameters** | `camelCase` | `propertyName` |
| **Local variables** | `camelCase` | `maxRetries` |
| **Interfaces** | `IPascalCase` | `IActivityTracer` |
| **Type parameters** | `TPascalCase` | `TResponse` |

### Examples

```csharp
public class DatabricksConnection
{
    // Private static field - s_ prefix
    private static readonly string s_assemblyName = "...";

    // Private instance field - _ prefix
    private bool _enableDirectResults = true;
    private readonly long _maxBytesPerFile;

    // Constants - PascalCase
    private const long DefaultMaxBytesPerFile = 20 * 1024 * 1024;
    public const string DefaultConfigEnvironmentVariable = "DATABRICKS_CONFIG_FILE";

    // Public property - PascalCase
    public bool UseCloudFetch => _useCloudFetch;

    // Method - PascalCase with camelCase parameters
    public void ValidateProperties(string propertyName, int maxRetries)
    {
        // Local variable - camelCase
        bool isValid = CheckValidity(propertyName);
    }
}
```

---

## 3. Language Preferences

### Explicit Types Over `var`

```csharp
// Good - explicit types
Dictionary<string, string> result = new Dictionary<string, string>();
long maxBytesPerFetchRequestValue = ParseBytesWithUnits(maxBytesPerFetchRequestStr);
HttpMessageHandler baseHandler = base.CreateHttpHandler();

// Avoid - var usage
var result = new Dictionary<string, string>();
var value = ParseBytesWithUnits(str);
```

### Avoid `this.` Qualification

```csharp
// Good
_enableDirectResults = true;
ValidateProperties();

// Avoid
this._enableDirectResults = true;
this.ValidateProperties();
```

### Use Language Keywords

```csharp
// Good
int count = 0;
string name = "test";
bool isValid = true;

// Avoid
Int32 count = 0;
String name = "test";
Boolean isValid = true;
```

### Modifier Order

Always use this order:
```
public, private, protected, internal, static, extern, new, virtual, abstract, sealed, override, readonly, unsafe, volatile, async
```

Example:
```csharp
public static readonly string Name = "...";
private static async Task DoWorkAsync() { }
protected internal virtual void Process() { }
```

---

## 4. Project Configuration

The project uses these settings in `Directory.Build.props`:

```xml
<LangVersion>latest</LangVersion>
<TreatWarningsAsErrors>true</TreatWarningsAsErrors>
<Nullable>enable</Nullable>
<ImplicitUsings>disable</ImplicitUsings>
```

### Target Frameworks

- `netstandard2.0` - For broad compatibility
- `net472` - Windows .NET Framework
- `net8.0` - Modern .NET

### Nullable Reference Types

Nullable is enabled. Use nullable annotations appropriately:

```csharp
// Nullable field
private string? _identityFederationClientId;

// Non-nullable with default
private string _traceParentHeaderName = "traceparent";

// Nullable parameter
public void Configure(string? optionalValue = null) { }

// Non-null return with nullable input
public string GetOrDefault(string? input) => input ?? "default";
```

---

## 5. Documentation

### License Header

Every file must include the Apache License header:

```csharp
/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
```

### XML Documentation

Use XML comments for public and internal APIs:

```csharp
/// <summary>
/// Gets whether CloudFetch is enabled for result retrieval.
/// </summary>
/// <remarks>
/// CloudFetch downloads results directly from cloud storage for better performance.
/// </remarks>
internal bool UseCloudFetch => _useCloudFetch;

/// <summary>
/// Parses a byte value that may include unit suffixes (B, KB, MB, GB).
/// </summary>
/// <param name="value">The value to parse, e.g., "400MB", "1024KB"</param>
/// <returns>The value in bytes</returns>
/// <exception cref="FormatException">Thrown when the value cannot be parsed</exception>
internal static long ParseBytesWithUnits(string value)
```

---

## 6. Testing

### Framework

- Use **xUnit** for unit tests
- Use `Xunit.SkippableFact` for conditional test execution

### Test File Naming

- Test files: `<ClassName>Test.cs`
- Test class: `<ClassName>Test`
- Test methods: Descriptive names explaining the scenario

### Test Structure

```csharp
public class DatabricksConnectionTest
{
    [Fact]
    public void ParseBytesWithUnits_ValidMegabytes_ReturnsCorrectBytes()
    {
        // Arrange
        string input = "400MB";
        long expected = 400 * 1024 * 1024;

        // Act
        long result = DatabricksConnection.ParseBytesWithUnits(input);

        // Assert
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("100KB", 100 * 1024)]
    [InlineData("1GB", 1024 * 1024 * 1024L)]
    public void ParseBytesWithUnits_VariousUnits_ReturnsCorrectBytes(string input, long expected)
    {
        long result = DatabricksConnection.ParseBytesWithUnits(input);
        Assert.Equal(expected, result);
    }

    [SkippableFact]
    public void IntegrationTest_RequiresConnection()
    {
        Skip.If(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DATABRICKS_TEST_CONFIG_FILE")));
        // Test logic...
    }
}
```

### Running Tests

```bash
# Run all tests
cd csharp/test
dotnet test

# Run specific test class
dotnet test --filter "FullyQualifiedName~DatabricksConnectionTest"

# Run specific test method
dotnet test --filter "FullyQualifiedName~ParseBytesWithUnits_ValidMegabytes"

# Run with verbose output
dotnet test --logger "console;verbosity=detailed"
```

---

## 7. Code Patterns

### Property Validation Pattern

```csharp
private void ValidateProperties()
{
    // Boolean property with default
    _enablePKFK = PropertyHelper.GetBooleanPropertyWithValidation(
        Properties,
        DatabricksParameters.EnablePKFK,
        _enablePKFK);

    // Integer property with validation
    _fetchHeartbeatIntervalSeconds = PropertyHelper.GetPositiveIntPropertyWithValidation(
        Properties,
        DatabricksParameters.FetchHeartbeatInterval,
        _fetchHeartbeatIntervalSeconds);

    // String property with null check
    if (Properties.TryGetValue(DatabricksParameters.TraceParentHeaderName, out string? headerName))
    {
        if (!string.IsNullOrWhiteSpace(headerName))
        {
            _traceParentHeaderName = headerName;
        }
        else
        {
            throw new ArgumentException($"Parameter '{DatabricksParameters.TraceParentHeaderName}' cannot be empty.");
        }
    }
}
```

### Async/Await Pattern

```csharp
// Good - async method naming
public async Task<TResult> ExecuteQueryAsync(CancellationToken cancellationToken = default)
{
    // Use ConfigureAwait(false) for library code
    var response = await SendRequestAsync(request, cancellationToken).ConfigureAwait(false);
    return await ProcessResponseAsync(response, cancellationToken).ConfigureAwait(false);
}

// Good - cancellation token propagation
private async Task SendRequestAsync(TRequest request, CancellationToken ct)
{
    ct.ThrowIfCancellationRequested();
    await _httpClient.PostAsync(url, content, ct).ConfigureAwait(false);
}
```

### Disposal Pattern

```csharp
public class MyResource : IDisposable
{
    private bool _disposed;
    private readonly HttpClient _httpClient;

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            if (disposing)
            {
                // Dispose managed resources
                _httpClient?.Dispose();
            }
            _disposed = true;
        }
    }

    public void Dispose()
    {
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }
}
```

---

## 8. Pull Request Guidelines

### PR Title Format

Use [Conventional Commits](https://www.conventionalcommits.org/) format:

```
<type>(<scope>): <description>
```

**Types:**
- `feat` - New feature
- `fix` - Bug fix
- `docs` - Documentation changes
- `refactor` - Code refactoring
- `test` - Adding/updating tests
- `chore` - Maintenance tasks

**Scope:** Use `csharp` for C# driver changes

**Examples:**
```
feat(csharp): add CloudFetch prefetching support
fix(csharp): handle token expiration during long queries
docs(csharp): update authentication configuration guide
refactor(csharp): extract HTTP handler factory
test(csharp): add unit tests for byte parsing
```

**Breaking changes:** Use `!` suffix
```
fix!(csharp): change default batch size to 2M rows
```

### Pre-commit Checks

Before submitting:
```bash
pre-commit run --all-files
```

### PR Description

Include:
- Summary of changes
- Link to related issue (`Closes #123` or `Fixes #123`)
- Testing notes if applicable

---

## 9. Key Principles

### 9.1 Explicit Over Implicit

Prefer explicit types and clear naming over `var` and abbreviated names.

```csharp
// Good - explicit types
Dictionary<string, string> result = new Dictionary<string, string>();
long maxBytesPerFetchRequestValue = ParseBytesWithUnits(maxBytesPerFetchRequestStr);
HttpMessageHandler baseHandler = base.CreateHttpHandler();

// Avoid - var usage
var result = new Dictionary<string, string>();
```

### 9.2 Immutability

Use `readonly` fields where possible, especially for static fields and injected dependencies.

```csharp
// Good - readonly for static fields
internal static readonly string s_assemblyName = ApacheUtility.GetAssemblyName(typeof(DatabricksConnection));
internal static readonly string s_assemblyVersion = ApacheUtility.GetAssemblyVersion(typeof(DatabricksConnection));

internal static readonly Dictionary<string, string> timestampConfig = new Dictionary<string, string>
{
    { "spark.thriftserver.arrowBasedRowSet.timestampAsString", "false" },
};

// Good - readonly for injected dependencies
internal Microsoft.IO.RecyclableMemoryStreamManager RecyclableMemoryStreamManager { get; }
internal System.Buffers.ArrayPool<byte> Lz4BufferPool { get; }
```

### 9.3 Null Safety

Use nullable annotations and explicit null checks with meaningful error messages.

```csharp
// Good - nullable type annotations
private string? _identityFederationClientId;
private TNamespace? _defaultNamespace;

// Good - null checks with clear handling
if (session == null)
{
    activity?.SetTag("error.type", "NullSessionResponse");
    return;
}

// Good - null-conditional operator for optional operations
activity?.SetTag("connection.server_protocol_version", version.ToString());
activity?.AddEvent("connection.namespace.set_from_server", [
    new("catalog", _defaultNamespace.CatalogName ?? "(none)"),
    new("schema", _defaultNamespace.SchemaName ?? "(none)")
]);

// Good - null coalescing for defaults
RecyclableMemoryStreamManager = memoryStreamManager ?? new Microsoft.IO.RecyclableMemoryStreamManager();
Lz4BufferPool = lz4BufferPool ?? System.Buffers.ArrayPool<byte>.Create(maxArrayLength: 4 * 1024 * 1024, maxArraysPerBucket: 10);
```

### 9.4 Tracing

Add OpenTelemetry activity tags for observability in key operations.

```csharp
// Good - wrap operations with tracing
protected override TOpenSessionReq CreateSessionRequest()
{
    return this.TraceActivity(activity =>
    {
        // Log driver information
        activity?.AddEvent("connection.driver.info", [
            new("driver.name", "Apache Arrow ADBC Databricks Driver"),
            new("driver.version", s_assemblyVersion),
            new("driver.assembly", s_assemblyName)
        ]);

        // Set tags for key configuration values
        activity?.SetTag("connection.client_protocol", req.Client_protocol.ToString());
        activity?.SetTag("connection.configuration_count", req.Configuration.Count);

        return req;
    });
}

// Good - async tracing pattern
public async Task ApplyServerSidePropertiesAsync()
{
    await this.TraceActivityAsync(async activity =>
    {
        activity?.SetTag("connection.server_side_properties.count", serverSideProperties.Count);

        try
        {
            // ... operation
        }
        catch (Exception ex)
        {
            // Track failures as events
            activity?.AddEvent("connection.server_side_property.set_failed", [
                new("property_name", property.Key),
                new("error_message", ex.Message)
            ]);
        }
    });
}
```

### 9.5 Error Handling

Throw meaningful exceptions with parameter names and context.

```csharp
// Good - ArgumentOutOfRangeException with parameter name and value
if (maxBytesPerFetchRequestValue < 0)
{
    throw new ArgumentOutOfRangeException(
        nameof(Properties),
        maxBytesPerFetchRequestValue,
        $"Parameter '{DatabricksParameters.MaxBytesPerFetchRequest}' value must be a non-negative integer. Use 0 for no limit.");
}

// Good - ArgumentException with detailed message
if (string.IsNullOrEmpty(clientId))
{
    throw new ArgumentException(
        $"Parameter '{DatabricksParameters.OAuthGrantType}' is set to '{DatabricksConstants.OAuthGrantTypes.ClientCredentials}' " +
        $"but parameter '{DatabricksParameters.OAuthClientId}' is not set. " +
        $"Please provide a value for '{DatabricksParameters.OAuthClientId}'.",
        nameof(Properties));
}

// Good - FormatException with examples of valid input
catch (FormatException)
{
    throw new ArgumentException(
        $"Parameter '{DatabricksParameters.MaxBytesPerFetchRequest}' value '{maxBytesPerFetchRequestStr}' could not be parsed. " +
        "Valid formats: number with optional unit suffix (B, KB, MB, GB). Examples: '400MB', '1024KB', '1073741824'.");
}

// Good - custom domain exception
if (!FeatureVersionNegotiator.IsDatabricksProtocolVersion(version))
{
    var exception = new DatabricksException("Attempted to use databricks driver with a non-databricks server");
    activity?.AddException(exception, [
        new("error.type", "InvalidServerProtocol")
    ]);
    throw exception;
}
```

---

## 10. Tracing & Logging

This section covers how to add trace lines that can be written to local log files.

### 10.1 Enable File-Based Tracing

**Via environment variable:**
```bash
export OTEL_TRACES_EXPORTER=adbcfile
```

**Via connection properties:**
```csharp
var config = new Dictionary<string, string>
{
    ["adbc.traces.exporter"] = "adbcfile",
    // ... other config
};
```

**Trace file locations:**
- **Linux:** `~/.local/share/Apache.Arrow.Adbc/Traces/`
- **macOS:** `~/Library/Application Support/Apache.Arrow.Adbc/Traces/`
- **Windows:** `%LOCALAPPDATA%\Apache.Arrow.Adbc\Traces\`

### 10.2 Adding Trace Lines (Within IActivityTracer Classes)

For classes that implement `IActivityTracer` (like `DatabricksConnection`, `DatabricksStatement`):

```csharp
using System.Diagnostics;
using Apache.Arrow.Adbc.Tracing;

public void MyMethod()
{
    this.TraceActivity(activity =>
    {
        // Single trace line as a tag (key-value metadata)
        activity?.SetTag("my.custom.key", "my value");

        // Single trace line as an event (log-style message)
        activity?.AddEvent("my.custom.event", [
            new("message", "This is my trace message"),
            new("timestamp", DateTimeOffset.UtcNow.ToString("O")),
            new("value", 42)
        ]);

        // ... your logic
    });
}

// Async version
public async Task MyAsyncMethodAsync()
{
    await this.TraceActivityAsync(async activity =>
    {
        activity?.AddEvent("operation.started", [
            new("operation_id", Guid.NewGuid().ToString())
        ]);

        // ... async logic

        activity?.AddEvent("operation.completed");
    });
}
```

### 10.3 Adding Trace Lines (Standalone ActivitySource)

For classes that don't implement `IActivityTracer`:

```csharp
using System.Diagnostics;

internal class MyComponent
{
    // Create a dedicated ActivitySource for your component
    private static readonly ActivitySource s_activitySource =
        new ActivitySource("AdbcDrivers.Databricks.MyComponent");

    public void MyMethod()
    {
        // Add trace event to current activity
        Activity.Current?.AddEvent(new ActivityEvent("my.debug.message",
            tags: new ActivityTagsCollection
            {
                { "message", "This is my debug trace line" },
                { "value", 42 },
                { "timestamp", DateTimeOffset.UtcNow }
            }));
    }
}
```

### 10.4 Quick Debug Logging

For quick debugging during development:

```csharp
using System.Diagnostics;

// Simple console/debug trace
Trace.TraceInformation($"[DEBUG] My trace message: value={someValue}");

// Debug-only output (removed in Release builds)
Debug.WriteLine($"[DEBUG] My trace message: value={someValue}");
```

### 10.5 Common Trace Patterns

```csharp
// Pattern 1: Log a structured event with tags
activity?.AddEvent("cloudfetch.download.started", [
    new("file_index", fileIndex),
    new("url", url),
    new("expected_size_bytes", expectedSize)
]);

// Pattern 2: Set key-value tags on activity span
activity?.SetTag("connection.protocol.supports_pk_fk", true);
activity?.SetTag("connection.feature.use_cloud_fetch", _useCloudFetch);

// Pattern 3: Log an error event
activity?.AddEvent("connection.server_side_property.set_failed", [
    new("property_name", property.Key),
    new("error_message", ex.Message)
]);

// Pattern 4: Log with exception details
activity?.AddException(exception, [
    new("error.type", "InvalidServerProtocol")
]);

// Pattern 5: Conditional logging
activity?.AddConditionalTag("connection.catalog", catalogName, !string.IsNullOrEmpty(catalogName));
```

### 10.6 Trace Method Quick Reference

| Method | Use Case |
|--------|----------|
| `activity?.SetTag(key, value)` | Key-value metadata attached to span |
| `activity?.AddEvent(name, tags)` | Log-style message within a span |
| `activity?.AddException(ex, tags)` | Record exception with context |
| `this.TraceActivity(activity => ...)` | Wrap sync operation with tracing |
| `this.TraceActivityAsync(async activity => ...)` | Wrap async operation with tracing |
| `Trace.TraceInformation(msg)` | Simple console/debug output |
| `Debug.WriteLine(msg)` | Debug-only output |

---

## Quick Reference Card

| Category | Guideline |
|----------|-----------|
| **Indentation** | 4 spaces (2 for XML) |
| **Private fields** | `_camelCase` |
| **Static fields** | `s_camelCase` |
| **Constants** | `PascalCase` |
| **Types** | Explicit (avoid `var`) |
| **Nullable** | Enabled, use annotations |
| **Async methods** | Suffix with `Async`, use `ConfigureAwait(false)` |
| **Tests** | xUnit, `<ClassName>Test.cs` |
| **PR titles** | Conventional Commits: `type(csharp): description` |
| **Pre-commit** | Run `pre-commit run --all-files` |
