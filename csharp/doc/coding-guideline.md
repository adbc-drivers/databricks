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

# C# ADBC Driver Coding Guidelines

## 1. Framework Compatibility

### Target Frameworks
All code **MUST** be compatible with:
- **.NET Framework 4.7.2** (`net472`)
- **.NET Standard 2.0** (`netstandard2.0`)
- **.NET 8.0** (`net8.0`)

### Compatibility Rules

| Feature | Allowed | Alternative |
|---------|---------|-------------|
| `async/await` | ✅ Yes | - |
| `ValueTask` | ❌ No | Use `Task` |
| `IAsyncEnumerable` | ❌ No | Use callbacks or `Task<IEnumerable>` |
| `Span<T>`, `Memory<T>` | ⚠️ Limited | Available via System.Memory package |
| `Record` types | ❌ No | Use `class` with properties |
| `init` accessors | ❌ No | Use `set` or constructor |
| `required` modifier | ❌ No | Validate in constructor |
| Nullable reference types | ✅ Yes | Use `#nullable enable` |
| `System.Text.Json` | ⚠️ Limited | Use package, check API availability |
| `HttpClient.GetAsync` | ✅ Yes | - |

### Testing Compatibility
```bash
# Always build and test against all target frameworks
dotnet build -f net472
dotnet build -f netstandard2.0
dotnet build -f net8.0
```

---

## 2. Code Style and Conventions

### File Structure

```csharp
/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* ...
*/

using System;                          // System namespaces first
using System.Collections.Generic;
using AdbcDrivers.Databricks;          // Project namespaces
using Apache.Arrow.Adbc;               // External dependencies

namespace AdbcDrivers.Databricks.Feature
{
    /// <summary>
    /// XML documentation for public types.
    /// </summary>
    public class MyClass
    {
        // Order: constants, static fields, instance fields, constructors, properties, methods
    }
}
```

### Naming Conventions

| Element | Convention | Example |
|---------|------------|---------|
| Namespace | PascalCase | `AdbcDrivers.Databricks.Telemetry` |
| Class/Interface | PascalCase | `TelemetryConfiguration`, `ITelemetryClient` |
| Method | PascalCase | `ExportAsync`, `GetOrCreateClient` |
| Property | PascalCase | `BatchSize`, `Enabled` |
| Private field | `_camelCase` | `_httpClient`, `_configuration` |
| Constant | PascalCase | `DefaultBatchSize`, `PropertyKeyEnabled` |
| Parameter | camelCase | `connectionProperties`, `cancellationToken` |
| Local variable | camelCase | `result`, `httpResponse` |

### Property Keys and Environment Variables

```csharp
// Property keys: lowercase with dots
public const string PropertyKeyEnabled = "telemetry.enabled";
public const string PropertyKeyBatchSize = "telemetry.batch_size";

// Environment variables: SCREAMING_SNAKE_CASE
public const string EnvKeyEnabled = "DATABRICKS_TELEMETRY_ENABLED";
```

### Access Modifiers
- Prefer `internal` over `public` for implementation classes
- Use `sealed` for classes not designed for inheritance
- Use `readonly` for fields that shouldn't change after construction

```csharp
internal sealed class TelemetryExporter : ITelemetryExporter
{
    private readonly HttpClient _httpClient;
    private readonly TelemetryConfiguration _config;
}
```

---

## 3. Tracing and Diagnostics

### Activity-Based Tracing

Follow the existing `System.Diagnostics.Activity` pattern for tracing:

```csharp
using System.Diagnostics;

internal class MyComponent
{
    private static readonly ActivitySource s_activitySource =
        new ActivitySource("Databricks.Adbc.Driver");

    public async Task DoWorkAsync()
    {
        using var activity = s_activitySource.StartActivity("MyComponent.DoWork");
        activity?.SetTag("operation.type", "export");

        try
        {
            // ... work
            activity?.SetStatus(ActivityStatusCode.Ok);
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }
}
```

### Logging Levels

| Level | Use Case | Example |
|-------|----------|---------|
| `Trace.WriteLine` with `[TRACE]` | Detailed debugging | `[TRACE] Exporting 5 events to endpoint` |
| `Debug.WriteLine` with `[DEBUG]` | Development info | `[DEBUG] Circuit breaker state: Open` |
| **Never** | User-visible logs | Telemetry must be silent to users |

```csharp
// Correct: Use Debug/Trace for internal diagnostics only
Debug.WriteLine($"[DEBUG] Circuit breaker opened after {failureCount} failures");

// WRONG: Never use Console.WriteLine or throw for telemetry failures
// Console.WriteLine("Telemetry export failed");  // DON'T DO THIS
```

### Telemetry-Specific Rules

1. **Never throw exceptions** from telemetry code paths
2. **Never log at INFO or higher** - telemetry is invisible to users
3. **Always wrap in try-catch** at entry points

```csharp
public async Task ExportAsync(IReadOnlyList<TelemetryEvent> events)
{
    try
    {
        await ExportInternalAsync(events);
    }
    catch (Exception ex)
    {
        // Swallow - telemetry failures must not affect driver operations
        Debug.WriteLine($"[TRACE] Telemetry export failed: {ex.Message}");
    }
}
```

---

## 4. Error Handling

### Graceful Degradation

For configuration parsing, use defaults instead of throwing:

```csharp
// Correct: Graceful degradation
public static int ParseBatchSize(string? value, int defaultValue)
{
    if (int.TryParse(value, out int result) && result > 0)
    {
        return result;
    }
    return defaultValue;  // Invalid input uses default
}

// WRONG: Throwing on invalid config
public static int ParseBatchSize(string value)
{
    return int.Parse(value);  // Throws on invalid input - DON'T DO THIS
}
```

### Exception Classification

For telemetry, classify exceptions as terminal vs retryable:

```csharp
// Terminal exceptions (flush immediately):
// - HTTP 400, 401, 403, 404
// - AuthenticationException

// Retryable exceptions (buffer and retry):
// - HTTP 429, 500, 503
// - TimeoutException
// - Network errors
```

---

## 5. Testing Requirements

### Test Organization

```
csharp/test/
├── Unit/                    # Fast, isolated unit tests
│   └── Telemetry/
│       ├── TelemetryConfigurationTests.cs
│       └── CircuitBreakerTests.cs
└── E2E/                     # End-to-end tests against real services
    └── Telemetry/
        └── TelemetryExporterE2ETests.cs
```

### Unit Test Pattern

Use Arrange-Act-Assert with clear naming:

```csharp
[Fact]
public void ClassName_MethodName_ExpectedBehavior()
{
    // Arrange
    var config = new TelemetryConfiguration { BatchSize = 50 };

    // Act
    var errors = config.Validate();

    // Assert
    Assert.Empty(errors);
}
```

### E2E Test Requirements

**Never skip E2E tests.** Use tracing to verify expected behavior:

```csharp
[Fact]
public async Task TelemetryExporter_ExportAsync_RealEndpoint_Succeeds()
{
    // Arrange
    var events = CreateTestEvents();
    var exporter = CreateExporter();

    // Act
    await exporter.ExportAsync(events);

    // Assert - Use ActivityListener to verify calls were made
    Assert.True(_activityListener.RecordedActivities
        .Any(a => a.OperationName == "TelemetryExporter.Export"));
}
```

### Test Naming Convention

```
{ClassName}_{MethodName}_{Scenario}_{ExpectedResult}
```

Examples:
- `TelemetryConfiguration_FromProperties_ValidInput_ParsesCorrectly`
- `CircuitBreaker_Execute_ThresholdReached_TransitionsToOpen`
- `ExceptionClassifier_IsTerminal_Http401_ReturnsTrue`

---

## 6. Async Patterns

### Async Method Guidelines

```csharp
// Always use Async suffix
public async Task ExportAsync(CancellationToken ct = default)

// Always accept CancellationToken
public async Task<bool> CheckFlagAsync(string host, CancellationToken ct)

// Use ConfigureAwait(false) in library code
var response = await _httpClient.PostAsync(url, content, ct).ConfigureAwait(false);

// Never use .Result or .Wait() - causes deadlocks in .NET Framework
// WRONG: var result = task.Result;
// CORRECT: var result = await task;
```

### Fire-and-Forget Pattern

For telemetry that shouldn't block:

```csharp
// Correct: Explicit fire-and-forget with discard
_ = ExportAsync(events);  // Discards the task intentionally

// For tracking failures, use a helper:
private void FireAndForget(Task task)
{
    task.ContinueWith(t =>
    {
        if (t.IsFaulted)
            Debug.WriteLine($"[TRACE] Background task failed: {t.Exception?.Message}");
    }, TaskContinuationOptions.OnlyOnFaulted);
}
```

---

## 7. Documentation

### XML Documentation Required For:
- All `public` types and members
- All `protected` members
- Complex `internal` classes

```csharp
/// <summary>
/// Configuration class for telemetry settings.
/// </summary>
/// <remarks>
/// Configuration values are read from connection properties with fallback to
/// environment variables, then defaults.
/// Priority: Connection Properties > Environment Variables > Defaults.
/// </remarks>
public sealed class TelemetryConfiguration
{
    /// <summary>
    /// Gets or sets whether telemetry is enabled. Default is true.
    /// </summary>
    public bool Enabled { get; set; } = true;
}
```

### Code Comments

```csharp
// Use comments to explain WHY, not WHAT
// Good: Explains reasoning
// Buffer retryable exceptions until statement completes to avoid
// sending incomplete error context
private readonly Dictionary<string, List<Exception>> _bufferedExceptions;

// Bad: States the obvious
// Create a new dictionary
private readonly Dictionary<string, List<Exception>> _bufferedExceptions;
```

---

## 8. Pull Request Guidelines

### PR Title Format
Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(csharp): add telemetry exporter with JDBC-compatible format
fix(csharp): handle null workspace ID in telemetry events
refactor(csharp): extract circuit breaker to separate class
test(csharp): add E2E tests for telemetry export
docs(csharp): update coding guidelines
```

### PR Checklist

- [ ] Code builds on all target frameworks (`net472`, `netstandard2.0`, `net8.0`)
- [ ] All unit tests pass
- [ ] E2E tests pass (not skipped)
- [ ] XML documentation added for public APIs
- [ ] No `Console.WriteLine` or user-visible logging for telemetry
- [ ] Exceptions are properly handled (no throws from telemetry paths)
- [ ] Pre-commit hooks pass: `pre-commit run --all-files`

---

## Quick Reference

### Do's
- ✅ Target `netstandard2.0` compatibility
- ✅ Use `async/await` with `ConfigureAwait(false)`
- ✅ Use `Activity` for tracing
- ✅ Swallow exceptions in telemetry code
- ✅ Use graceful degradation for config parsing
- ✅ Write comprehensive E2E tests
- ✅ Use XML documentation

### Don'ts
- ❌ Use C# 9+ features (records, init, required)
- ❌ Use `Task.Result` or `Task.Wait()`
- ❌ Throw exceptions from telemetry code paths
- ❌ Log at INFO level or higher for telemetry
- ❌ Skip E2E tests
- ❌ Use `Console.WriteLine`
