# Test Generation Guide

This guide explains how to generate test code from YAML specifications with high accuracy.

## For AI/Claude Code Generators

When using AI assistants like Claude Code to generate tests from specs, follow this process:

### 1. Load Required Files
```
1. Read the test spec (e.g., specs/session-lifecycle.yaml)
2. Read .claude/.claude/test-patterns.yaml for language-specific code patterns
3. Read implementation_notes from the spec for additional guidance
```

### 2. Parse Test Structure
```yaml
# From spec file:
test_config:
  simple_query: "SELECT 1 AS test_value"  # → Variable name: SimpleQuery

tests:
  - id: SESSION-001  # → Method name: Session_BasicOpenSessionSuccess
    steps:
      - action: reset_call_history  # → Look up in .claude/test-patterns.yaml
      - action: execute_query
        query: test_config.simple_query  # → Resolve to SimpleQuery
        read_batches: 1
    assertions:
      - type: query_succeeds
```

### 3. Pattern Matching Algorithm
```
For each step in test:
  1. Get action name (e.g., "execute_query")
  2. Look up action in .claude/test-patterns.yaml under languages.{language}.actions
  3. Check for parameters (e.g., read_batches, expect_error)
  4. Select appropriate code variant:
     - execute_query → actions.execute_query.code
     - execute_query with read_batches → actions.execute_query.with_read_batches
     - execute_query with expect_error → actions.execute_query.with_expect_error
  5. Replace placeholders ({query_variable}, {read_batches}, etc.)
  6. Add step comment using config.step_comment_format

For each assertion:
  1. Get assertion type (e.g., "error_thrown")
  2. Look up in .claude/test-patterns.yaml under languages.{language}.assertions
  3. Replace placeholders ({error_pattern_checks}, {method}, etc.)
  4. For error_pattern: split on | and use pattern_check_template for each
```

### 4. Variable Resolution
```yaml
# Query variables (from .claude/test-patterns.yaml config.query_variable_mapping)
test_config.simple_query → SimpleQuery
test_config.query → TestQuery

# Driver config resolution
driver_config:
  session_idle_timeout_minutes: 1
# → Generate:
var parameters = new Dictionary<string, string>
{
    ["spark.sql.session.timeZone"] = "UTC"
};
```

### 5. Error Pattern Handling
```yaml
# From spec:
error_pattern: session|expired|timeout

# Generate (using pattern_check_template from .claude/test-patterns.yaml):
fullMessage.Contains("session", StringComparison.OrdinalIgnoreCase) ||
fullMessage.Contains("expired", StringComparison.OrdinalIgnoreCase) ||
fullMessage.Contains("timeout", StringComparison.OrdinalIgnoreCase)
```

### 6. Expected Accuracy
- **CloudFetch tests**: 99.5% accuracy
- **Session Lifecycle tests**: 92-95% accuracy
- **Target for new suites**: 95%+ accuracy

Common differences (acceptable 5-8% gap):
- Variable naming (e.g., `conn` vs `connection`)
- Minor code organization
- Comment phrasing
- Null check placement

### 7. Prompt Template for Claude

```
Please generate test code from the following YAML spec.

Files to read:
- {spec_file}.yaml (contains test definitions)
- .claude/test-patterns.yaml (contains language-specific patterns)

Target language: {language} (e.g., csharp, java, python)
Test ID: {test_id} (e.g., SESSION-004)

Steps:
1. Parse the test from the spec file
2. For each step, look up the action in .claude/test-patterns.yaml under languages.{language}.actions
3. For each assertion, look up in .claude/test-patterns.yaml under languages.{language}.assertions
4. Resolve query variables using config.query_variable_mapping
5. Handle error patterns by splitting on | and applying pattern_check_template
6. Use implementation_notes from spec for additional guidance
7. Generate clean, commented code following best practices

Expected output format:
[Fact]
public async Task {MethodName}()
{
    // Step 1: {description}
    {code}

    // Step 2: {description}
    {code}

    // Assertion: {description}
    {assertion_code}
}
```

## Overview

Our test specifications are designed for **~95% spec-to-code accuracy**. When generating tests:
1. Read the YAML spec
2. Read .claude/test-patterns.yaml for language-specific patterns
3. Map actions and assertions using .claude/test-patterns.yaml
4. Use implementation_notes for additional language-specific guidance
5. Generate clean, concise test code

## Action Mapping (Language-Agnostic)

### Common Actions

| Action | Purpose | Parameters |
|--------|---------|------------|
| `reset_call_history` | Clear proxy call history before test | None |
| `enable_failure_scenario` | Enable proxy failure scenario | `scenario`, optional `config` |
| `execute_query` | Execute SQL query | `query`, `read_batches`, optional `expect_error` |
| `open_connection` | Create new connection | optional `driver_config` |
| `close_connection` | Dispose connection | optional `allow_error` |

### Action to Code Patterns (C#)

#### `reset_call_history`
```csharp
// Step N: Reset call history
await ControlClient.ResetCallHistoryAsync();
```

#### `enable_failure_scenario`
```csharp
// Step N: Enable failure scenario
await ControlClient.EnableScenarioAsync("scenario_name");
```

#### `execute_query`
```csharp
// Step N: Execute query
using var statement = connection.CreateStatement();
statement.SqlQuery = SimpleQuery;
var result = statement.ExecuteQuery();
Assert.NotNull(result);

using var reader = result.Stream;
var batch = reader.ReadNextRecordBatchAsync().Result;
Assert.NotNull(batch);
Assert.True(batch.Length > 0);
```

With `read_batches: 2`:
```csharp
// Step N: Execute query and read 2 batches
using var statement = connection.CreateStatement();
statement.SqlQuery = TestQuery;
var result = statement.ExecuteQuery();

using var reader = result.Stream;
for (int i = 0; i < 2; i++)
{
    var batch = await reader.ReadNextRecordBatchAsync();
    if (batch == null || batch.Length == 0)
        break;
}
```

With `expect_error: true`:
```csharp
// Step N: Attempt query (expect error)
var exception = Assert.ThrowsAny<Exception>(() =>
{
    using var statement = connection.CreateStatement();
    statement.SqlQuery = SimpleQuery;
    var result = statement.ExecuteQuery();
    using var reader = result.Stream;
    _ = reader.ReadNextRecordBatchAsync().Result;
});
```

#### `open_connection`
```csharp
// Step N: Open connection
using var connection = CreateProxiedConnection();
```

With `driver_config`:
```csharp
// Step N: Open connection with config
var parameters = new Dictionary<string, string>
{
    ["spark.sql.session.timeZone"] = "UTC"
};
using var connection = CreateProxiedConnectionWithParameters(parameters);
```

#### `close_connection`
```csharp
// Step N: Close connection
connection.Dispose();
await Task.Delay(100);  // Allow async cleanup
```

## Assertion Mapping

### Common Assertions

| Assertion Type | Purpose | Implementation |
|----------------|---------|----------------|
| `query_succeeds` | Query completes without error | No exception thrown (implicit) |
| `error_thrown` | Operation throws exception | `Assert.ThrowsAny<Exception>()` + pattern check |
| `thrift_call_count` | Verify call count | `await ControlClient.CountThriftMethodCallsAsync("Method")` |
| `thrift_call_exists` | Find duplicate calls | LINQ GroupBy + Where(count >= 2) |
| `duplicate_cloud_fetch_urls` | Find URL retries | GroupBy URL + Where(count > 1) |
| `retry_backoff_delay` | Verify timing | `DateTime.UtcNow` timing check |

### Assertion Examples (C#)

#### `query_succeeds`
```csharp
// Assertion: Query succeeds
// (implicit - no exception thrown)
```

#### `error_thrown` with `error_pattern`
```csharp
// Assertion: Should get session error
var fullMessage = exception.ToString();
Assert.True(
    fullMessage.Contains("session", StringComparison.OrdinalIgnoreCase) ||
    fullMessage.Contains("expired", StringComparison.OrdinalIgnoreCase),
    $"Expected session error, but got: {fullMessage}");
```

#### `thrift_call_count`
```csharp
// Assertion: OpenSession called exactly once
var openSessionCalls = await ControlClient.CountThriftMethodCallsAsync("OpenSession");
Assert.Equal(1, openSessionCalls);
```

With `expected: ">= 2"`:
```csharp
// Assertion: OpenSession called at least twice
var openSessionCalls = await ControlClient.CountThriftMethodCallsAsync("OpenSession");
Assert.True(openSessionCalls >= 2,
    $"Expected at least 2 OpenSession calls, but got {openSessionCalls}");
```

#### `thrift_call_exists` (duplicate detection)
```csharp
// Assertion: Driver called FetchResults multiple times with same offset
var fetchResultsCalls = await ControlClient.GetThriftMethodCallsAsync("FetchResults");

var offsetCounts = fetchResultsCalls
    .Select(call => ThriftFieldExtractor.GetLongValue(call, "startRowOffset"))
    .Where(offset => offset.HasValue && offset.Value > 0)
    .GroupBy(offset => offset.Value)
    .Where(group => group.Count() >= 2)
    .ToList();

Assert.NotEmpty(offsetCounts);
```

#### `duplicate_cloud_fetch_urls`
```csharp
// Assertion: Driver retried the same CloudFetch URL
var allCalls = await ControlClient.GetThriftCallsAsync();
var cloudDownloads = allCalls.Calls?
    .Where(c => c.Type == "cloud_download")
    .ToList() ?? new List<ThriftCall>();

var duplicateUrls = cloudDownloads
    .GroupBy(c => c.Url)
    .Where(g => g.Count() > 1)
    .ToList();

Assert.NotEmpty(duplicateUrls);
```

#### `retry_backoff_delay`
```csharp
// Assertion: Should have taken at least 0.8s for retry backoff
var startTime = DateTime.UtcNow;
// ... operation ...
var elapsed = DateTime.UtcNow - startTime;

Assert.True(elapsed.TotalSeconds >= 0.8,
    $"Expected at least 0.8s for retry backoff, but took {elapsed.TotalSeconds:F3}s");
```

## Using Implementation Notes

Each spec file includes an `implementation_notes` section with language-specific guidance.

### Example: session-lifecycle.yaml

```yaml
implementation_notes:
  query_execution: |
    Use Arrow's ReadNextRecordBatchAsync() to read result batches.
    Always wrap statements in using blocks for proper disposal.

  timing_measurement: |
    Use DateTime.UtcNow for timing measurements (not Stopwatch).

  retry_count_tracking: |
    For retry tests, get initial call count AFTER enabling scenario but BEFORE the operation.
    Then compare final count to initial count to see how many retries occurred.

  error_checking: |
    Use exception.ToString() (not exception.Message) to check full error context.
```

### Applying Implementation Notes

#### Before (without notes)
```csharp
var result = statement.ExecuteQuery();
result.Read();  // ❌ Wrong pattern
```

#### After (with notes)
```csharp
var result = statement.ExecuteQuery();
Assert.NotNull(result);

using var reader = result.Stream;
var batch = reader.ReadNextRecordBatchAsync().Result;  // ✓ Correct Arrow pattern
```

## Complete Example: Generating SESSION-004

### Spec (YAML)
```yaml
- id: SESSION-004
  name: Session Timeout Due to Inactivity

  steps:
    - action: reset_call_history
    - action: open_connection
      driver_config: driver_config
    - action: execute_query
      query: test_config.simple_query
      read_batches: 1
    - action: enable_failure_scenario
      scenario: session_timeout_premature
    - action: execute_query
      query: test_config.simple_query
      expect_error: true

  assertions:
    - type: error_thrown
      error_pattern: session|expired|timeout
```

### Generated Code (C#)
```csharp
[Fact]
public async Task SessionTimeout_HandlesInactivityExpiration()
{
    // Step 1: Reset call history
    await ControlClient.ResetCallHistoryAsync();

    // Step 2: Open connection with session timeout config
    var parameters = new Dictionary<string, string>
    {
        ["spark.sql.session.timeZone"] = "UTC"
    };
    using var connection = CreateProxiedConnectionWithParameters(parameters);

    // Step 3: Execute query to establish working session
    using (var statement = connection.CreateStatement())
    {
        statement.SqlQuery = SimpleQuery;
        var result = statement.ExecuteQuery();
        using var reader = result.Stream;
        _ = reader.ReadNextRecordBatchAsync().Result;
    }

    // Step 4: Enable session timeout scenario
    await ControlClient.EnableScenarioAsync("session_timeout_premature");

    // Step 5: Attempt query on expired session
    var exception = Assert.ThrowsAny<Exception>(() =>
    {
        using var statement = connection.CreateStatement();
        statement.SqlQuery = SimpleQuery;
        var result = statement.ExecuteQuery();
        using var reader = result.Stream;
        _ = reader.ReadNextRecordBatchAsync().Result;
    });

    // Assertion: Should get session expiration error
    Assert.NotNull(exception);
    var fullMessage = exception.ToString();
    Assert.True(
        fullMessage.Contains("session", StringComparison.OrdinalIgnoreCase) ||
        fullMessage.Contains("expired", StringComparison.OrdinalIgnoreCase) ||
        fullMessage.Contains("timeout", StringComparison.OrdinalIgnoreCase),
        $"Expected session timeout/expiration error, but got: {fullMessage}");
}
```

**Accuracy**: ~95% (only minor variable name differences)

## Best Practices

### 1. Always Use `using` for Disposal
```csharp
// ✓ Good
using var statement = connection.CreateStatement();

// ✗ Bad
var statement = connection.CreateStatement();
```

### 2. Add Null Checks
```csharp
// ✓ Good
var result = statement.ExecuteQuery();
Assert.NotNull(result);

// ✗ Bad (missing check)
var result = statement.ExecuteQuery();
```

### 3. Use Arrow Reading Pattern
```csharp
// ✓ Good
using var reader = result.Stream;
var batch = reader.ReadNextRecordBatchAsync().Result;

// ✗ Bad
result.Read();  // Generic pattern, not Arrow-specific
```

### 4. Consistent Step Comments
```csharp
// ✓ Good
// Step 1: Reset call history
// Step 2: Open connection
// Step 3: Execute query

// ✗ Bad (inconsistent)
// Reset call history
// Step 2: Open connection
// Execute the query
```

### 5. Use `StringComparison.OrdinalIgnoreCase`
```csharp
// ✓ Good
fullMessage.Contains("session", StringComparison.OrdinalIgnoreCase)

// ✗ Bad (case-sensitive)
fullMessage.ToLower().Contains("session")
```

## Troubleshooting

### Generated test fails with "Object reference not set"
**Cause**: Missing null checks
**Fix**: Add `Assert.NotNull()` after getting result

### Generated test fails with "Method not found"
**Cause**: Using wrong API pattern (e.g., `result.Read()` instead of Arrow)
**Fix**: Check `implementation_notes.query_execution` in spec

### Generated test has wrong timing
**Cause**: Using wrong timing mechanism
**Fix**: Check `implementation_notes.timing_measurement` in spec

### Generated test counts are wrong
**Cause**: Getting initial count at wrong time (should be AFTER enabling scenario)
**Fix**: Check `implementation_notes.retry_count_tracking` in spec

## Cross-Language Notes

### Java Example
Actions map similarly but with Java syntax:
```java
// reset_call_history
proxyClient.resetCallHistory();

// execute_query
try (Statement stmt = conn.createStatement();
     ResultSet rs = stmt.executeQuery(simpleQuery)) {
    rs.next();
}

// error_thrown
assertThrows(SQLException.class, () -> {
    // operation
});
```

### Python Example
```python
# reset_call_history
await proxy_client.reset_call_history()

# execute_query
async with connection.cursor() as cursor:
    await cursor.execute(simple_query)
    result = await cursor.fetchone()

# error_thrown
with pytest.raises(Exception) as exc_info:
    # operation
assert "session" in str(exc_info.value).lower()
```

## Summary

With proper action mapping and implementation notes:
- **CloudFetch spec**: 99.5% accuracy
- **Session Lifecycle spec**: 92-95% accuracy
- **Future specs**: Target 95%+ accuracy

The key is balancing language-agnostic actions with language-specific implementation guidance.
