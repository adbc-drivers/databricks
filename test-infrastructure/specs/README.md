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

# Test Specifications

Language-agnostic test specifications in YAML format that define test cases for Thrift protocol compliance.

## Purpose

These YAML specifications serve as:
- **Single source of truth** for test behavior across all driver implementations (C#, Java, C++, Go)
- **Machine-readable format** that maps directly to proxy scenarios
- **Implementation guide** for developers writing tests in any language
- **Documentation** of expected driver behavior

## Structure

Each YAML file defines a test suite with:

```yaml
test_suite: SuiteName
priority: Critical | High | Medium | Low

tests:
  - id: SUITE-NNN
    name: Test Name
    priority: Critical | High | Medium | Low
    description: What this test validates

    jira: JIRA-1234  # Optional: Production issue this addresses
    proxy_scenario: scenario_name  # Maps to mitmproxy_addon.py scenario

    driver_config:  # Optional: Driver-specific configuration
      key: value

    steps:
      - action: reset_call_history
        description: Clear proxy call history before test

      - action: enable_failure_scenario
        scenario: scenario_name
        config:  # Optional: Runtime scenario config
          key: value

      - action: execute_query
        query: test_config.query
        read_batches: test_config.read_batches
        driver_config: driver_config  # Optional

    assertions:
      - type: query_succeeds
        description: Query completes without error

      - type: thrift_call_exists
        method: MethodName
        where:
          - field: fieldName
            operator: greater_than
            value: 0
        count: at_least_duplicates
        description: Driver called method multiple times (proves retry/refresh)

    notes: Additional implementation notes
```

## Available Test Suites

### Implemented

| File | Suite | Tests | Priority | Status |
|------|-------|-------|----------|--------|
| `cloudfetch.yaml` | CloudFetch | 4 implemented, 6 planned | Critical | ✅ In Progress |

### Planned (~300 tests across 16 categories)

These test suites should be created as additional YAML files following the same structure as `cloudfetch.yaml`:

| File (Future) | Suite | Tests | Priority | Description |
|---------------|-------|-------|----------|-------------|
| `session-lifecycle.yaml` | Session Lifecycle | 15 | Critical | OpenSession, CloseSession, session expiration, timeouts |
| `statement-execution.yaml` | Statement Execution | 25 | Critical | Sync/async execution, cancellation, long-running queries |
| `metadata-operations.yaml` | Metadata Operations | 40 | High | GetCatalogs, GetSchemas, GetTables, GetColumns, GetFunctions, GetTypeInfo, GetPrimaryKeys, GetCrossReference |
| `arrow-format.yaml` | Arrow Format | 20 | High | Arrow IPC format, compression (LZ4, ZSTD), type handling, schema validation |
| `direct-results.yaml` | Direct Results | 15 | High | TSparkDirectResults optimization, inline results in OpenSession response |
| `parameterized-queries.yaml` | Parameterized Queries | 20 | High | Named parameters, positional parameters, type binding |
| `result-fetching.yaml` | Result Fetching | 15 | High | Pagination, cursor management, batch sizes, offset handling |
| `error-handling.yaml` | Error Handling | 30 | Critical | Error codes, error messages, retry logic, recovery strategies |
| `timeout-cleanup.yaml` | Timeout & Cleanup | 12 | Medium | Session timeouts, operation timeouts, resource cleanup |
| `concurrency.yaml` | Concurrency | 15 | Medium | Thread safety, parallel operations, connection pooling |
| `protocol-versions.yaml` | Protocol Versions | 12 | Medium | Version negotiation, backward compatibility, feature detection |
| `security.yaml` | Security | 15 | High | Authentication (OAuth, PAT), authorization, SSL/TLS, token refresh |
| `performance.yaml` | Performance | 10 | Low | Large result sets, batch size limits, memory limits |
| `edge-cases.yaml` | Edge Cases | 36 | Medium | NULL handling, empty results, special characters, boundary conditions |

**Total: ~300 test cases**

Each YAML file should:
- Follow the structure defined above
- Map tests to proxy scenarios where applicable
- Include JIRA references for tests based on production issues
- Define clear steps, assertions, and expected behavior
- Support implementation across all languages (C#, Java, C++, Go)

## Test Actions

### `reset_call_history`
Clear proxy call history before test to ensure clean state for verification.

### `enable_failure_scenario`
Enable a proxy failure scenario (maps to `mitmproxy_addon.py` SCENARIOS). Optionally provide runtime config.

### `execute_query`
Execute the test query with optional driver configuration and batch reading settings.

## Assertions

Assertions validate expected behavior by checking for specific patterns in proxy call history.

**Common assertion patterns:**

```yaml
assertions:
  # Query completes successfully
  - type: query_succeeds
    description: Query completes without error

  # Result is not null
  - type: result_not_null
    description: Query returns valid result

  # Check for duplicate Thrift calls (proves retry/refresh)
  - type: thrift_call_exists
    method: FetchResults
    where:
      - field: startRowOffset
        operator: greater_than
        value: 0
    count: at_least_duplicates
    description: Driver called FetchResults multiple times with same offset

  # Check for duplicate cloud fetch URLs (proves retry)
  - type: duplicate_cloud_fetch_urls
    count: at_least_one
    description: Driver retried the same CloudFetch URL

  # Schema validation against golden results
  - type: schema_matches
    golden_file: "golden/get_columns_result.json"
```

**Examples for specific test scenarios:**

GetColumns metadata test:
```yaml
assertions:
  - type: query_succeeds
  - type: schema_matches
    golden_file: "golden/get_columns_schema.json"
```

CloudFetch expired link recovery:
```yaml
assertions:
  - type: query_succeeds
  - type: thrift_call_exists
    method: FetchResults
    where:
      - field: startRowOffset
        operator: greater_than
        value: 0
    count: at_least_duplicates
```

## Using Specs in Your Language

### C# Example

The existing C# tests in `test-infrastructure/tests/csharp/CloudFetchTests.cs` implement these specs:

```csharp
// CLOUDFETCH-001: Expired Link Recovery
[Fact]
public async Task CloudFetchExpiredLink_RefreshesLinkViaFetchResults()
{
    // Step 1: reset_call_history
    await ControlClient.ResetCallHistoryAsync();

    // Step 2: enable_failure_scenario
    await ControlClient.EnableScenarioAsync("cloudfetch_expired_link");

    // Step 3: execute_query - read multiple batches
    using var connection = CreateProxiedConnection();
    using var statement = connection.CreateStatement();
    statement.SqlQuery = "SELECT * FROM main.tpcds_sf1_delta.catalog_returns";

    var result = statement.ExecuteQuery();
    using var reader = result.Stream;

    for (int i = 0; i < 2; i++)
    {
        var batch = await reader.ReadNextRecordBatchAsync();
        if (batch == null || batch.Length == 0)
            break;
    }

    // Assertion: thrift_call_exists - Driver called FetchResults multiple times with same offset
    var fetchResultsCalls = await ControlClient.GetThriftMethodCallsAsync("FetchResults");
    var offsetCounts = fetchResultsCalls
        .Select(call => ThriftFieldExtractor.GetLongValue(call, "startRowOffset"))
        .Where(offset => offset.HasValue && offset.Value > 0)
        .GroupBy(offset => offset.Value)
        .Where(group => group.Count() >= 2)
        .ToList();

    Assert.NotEmpty(offsetCounts);
}
```

### Java Example (Future)

```java
// CLOUDFETCH-001: Expired Link Recovery
@Test
public void testCloudFetchExpiredLinkRecovery() throws Exception {
    // Step 1: reset_call_history
    proxyClient.resetCallHistory();

    // Step 2: enable_failure_scenario
    proxyClient.enableScenario("cloudfetch_expired_link");

    // Step 3: execute_query
    try (Connection conn = createProxiedConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT * FROM main.tpcds_sf1_delta.catalog_returns")) {

        // Read multiple batches
        for (int i = 0; i < 2 && rs.next(); i++) {
            // Continue reading
        }

        // Assertion: Check for duplicate FetchResults calls with same offset
        List<ThriftCall> fetchResultsCalls = proxyClient.getThriftMethodCalls("FetchResults");
        Map<Long, Long> offsetCounts = fetchResultsCalls.stream()
            .map(call -> ThriftFieldExtractor.getLongValue(call, "startRowOffset"))
            .filter(offset -> offset != null && offset > 0)
            .collect(Collectors.groupingBy(offset -> offset, Collectors.counting()));

        assertTrue(offsetCounts.values().stream().anyMatch(count -> count >= 2));
    }
}
```

## Adding New Specs

1. Create new YAML file in this directory (e.g., `session-lifecycle.yaml`)
2. Follow the structure above
3. Map to proxy scenarios in `proxy-server/mitmproxy_addon.py`
4. Implement tests in your language following the steps and assertions
5. Update this README to list the new suite

## Validation

You can validate YAML syntax:
```bash
python -c "import yaml; yaml.safe_load(open('cloudfetch.yaml'))"
```

Future: Schema validation tool to ensure YAML follows expected structure.

## References

- [Proxy Server README](../proxy-server/README.md) - Available proxy scenarios
- [Test Infrastructure README](../README.md) - Overall architecture
