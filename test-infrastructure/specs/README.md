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
      - action: establish_baseline
        execute_query: "SELECT ..."
        measure:
          - thrift_method: MethodName
            save_as: variable_name

      - action: enable_failure_scenario
        scenario: scenario_name
        config:  # Optional: Runtime scenario config
          key: value

      - action: execute_test
        execute_query: "SELECT ..."

    assertions:
      - type: query_succeeds
      - type: thrift_call_count
        method: MethodName
        expected: variable_name + 1

    notes: Additional implementation notes
```

## Available Test Suites

### Implemented

| File | Suite | Tests | Priority | Status |
|------|-------|-------|----------|--------|
| `cloudfetch.yaml` | CloudFetch | 3 implemented, 7 planned | Critical | ✅ In Progress |

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

### `establish_baseline`
Execute query without failures to measure normal behavior. Use `measure` to save metrics for later comparison.

### `enable_failure_scenario`
Enable a proxy failure scenario (maps to `mitmproxy_addon.py` SCENARIOS). Optionally provide runtime config.

### `execute_test`
Execute the test query with failure scenario active.

## Assertions

Assertions validate expected behavior using equations/formulas rather than predefined types. This allows flexible validation for any test scenario.

**Common assertion patterns:**

```yaml
assertions:
  # No error thrown
  - type: no_error

  # Thrift method call count verification
  - type: thrift_call_count
    method: FetchResults  # or GetCatalogs, GetColumns, OpenSession, etc.
    expected: baseline_count + 1

  # Schema validation against golden results
  - type: schema_matches
    golden_file: "golden/get_columns_result.json"

  # Result not null
  - type: result_not_null

  # CloudFetch download count
  - type: cloud_download_count
    expected: baseline_downloads + 1
```

**Examples for specific test scenarios:**

GetColumns metadata test:
```yaml
assertions:
  - type: no_error
  - type: schema_matches
    golden_file: "golden/get_columns_schema.json"
```

CloudFetch expired link recovery:
```yaml
assertions:
  - type: no_error
  - type: thrift_call_count
    method: FetchResults
    expected: baseline_fetch_results + 1
```

## Measurement Types

Measurements capture baseline metrics during the test for later comparison in assertions.

| Type | Description | Parameters | Save As |
|------|-------------|------------|---------|
| `thrift_method` | Count calls to specific Thrift method | `method` (e.g., FetchResults, GetCatalogs, GetColumns, OpenSession) | Variable name |
| `cloud_downloads` | Count CloudFetch downloads | - | Variable name |

## Using Specs in Your Language

### C# Example

The existing C# tests in `test-infrastructure/tests/csharp/CloudFetchTests.cs` implement these specs:

```csharp
// CLOUDFETCH-001: Expired Link Recovery
[Fact]
public async Task CloudFetchExpiredLink_RefreshesLinkViaFetchResults()
{
    // Step 1: establish_baseline
    int baselineFetchResults;
    using (var connection = CreateProxiedConnection())
    using (var statement = connection.CreateStatement())
    {
        statement.SqlQuery = "SELECT * FROM main.tpcds_sf1_delta.catalog_returns";
        var result = statement.ExecuteQuery();
        using var reader = result.Stream;
        _ = reader.ReadNextRecordBatchAsync().Result;
        baselineFetchResults = await ControlClient.CountThriftMethodCallsAsync("FetchResults");
    }

    // Step 2: enable_failure_scenario
    await ControlClient.EnableScenarioAsync("cloudfetch_expired_link");

    // Step 3: execute_test
    using var connection2 = CreateProxiedConnection();
    using var statement2 = connection2.CreateStatement();
    statement2.SqlQuery = "SELECT * FROM main.tpcds_sf1_delta.catalog_returns";
    var result2 = statement2.ExecuteQuery();

    // Assertions
    using var reader2 = result2.Stream;
    var batch = reader2.ReadNextRecordBatchAsync().Result;
    Assert.NotNull(batch);  // query_succeeds, result_not_null, batch_not_empty

    var actualFetchResults = await ControlClient.CountThriftMethodCallsAsync("FetchResults");
    Assert.Equal(baselineFetchResults + 1, actualFetchResults);  // thrift_call_count
}
```

### Java Example (Future)

```java
// CLOUDFETCH-001: Expired Link Recovery
@Test
public void testCloudFetchExpiredLinkRecovery() throws Exception {
    // Step 1: establish_baseline
    int baselineFetchResults;
    try (Connection conn = createProxiedConnection();
         Statement stmt = conn.createStatement()) {
        stmt.execute("SELECT * FROM main.tpcds_sf1_delta.catalog_returns");
        baselineFetchResults = proxyClient.countThriftMethodCalls("FetchResults");
    }

    // Step 2: enable_failure_scenario
    proxyClient.enableScenario("cloudfetch_expired_link");

    // Step 3: execute_test
    try (Connection conn = createProxiedConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT * FROM main.tpcds_sf1_delta.catalog_returns")) {

        // Assertions
        assertTrue(rs.next());  // query_succeeds, result_not_null, batch_not_empty

        int actualFetchResults = proxyClient.countThriftMethodCalls("FetchResults");
        assertEquals(baselineFetchResults + 1, actualFetchResults);  // thrift_call_count
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
