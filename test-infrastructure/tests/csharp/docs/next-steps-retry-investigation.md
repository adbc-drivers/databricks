# Next Steps for Retry Test Investigation

## Summary of Investigation So Far

After thorough code analysis, I've confirmed:

1. **✅ Handler Chain is Correct**: RetryHttpHandler IS properly configured in the HTTP handler chain
   - Chain: OAuth → MandatoryTokenExchange → ThriftErrorMessageHandler → RetryHttpHandler → Tracing → Base
   - RetryHttpHandler sees HTTP responses BEFORE ThriftErrorMessageHandler
   - THttpTransport uses the HttpClient with the complete handler chain

2. **✅ ThriftErrorMessageHandler Doesn't Block Retries**: It only throws exceptions for responses with the `x-thriftserver-error-message` header
   - Proxy-injected HTTP errors (503, 408, etc.) don't have this header
   - So they pass through to RetryHttpHandler first

3. **❓ Most Likely Issue**: Proxy scenarios aren't actually injecting HTTP errors when expected
   - Tests complete in ~0.4s instead of 0.8s+ (no retry backoff delay observed)
   - This suggests either:
     - Scenario isn't returning HTTP 503 at all (connection succeeds immediately)
     - OR retry is happening without backoff delay (unlikely given code review)

## Diagnostic Changes Made

### 1. Added Logging to SESSION-011 Test

File: `SessionLifecycleTests.cs:566-624`

Added diagnostic output to track:
- Initial and final OpenSession call counts
- Scenario enable confirmation
- Connection timing (start/end timestamps)
- Number of OpenSession calls made during test

### 2. Added Logging to RetryHttpHandler

File: `RetryHttpHandler.cs:112-123, 193-195`

Added console output for:
- Every HTTP response received (status code + timestamp)
- Whether status code is retryable
- Retry attempt number
- Wait time before retry
- Confirmation when wait completes

## How to Run Diagnostic Test

```bash
# Navigate to test directory
cd /Users/e.wang/Documents/dev/databricks/test-infrastructure/tests/csharp

# Make sure DATABRICKS_TEST_CONFIG_FILE is set (ask user for the path)
# export DATABRICKS_TEST_CONFIG_FILE=/path/to/your/databricks-test-config.json

# Run the single test with verbose output
dotnet test --filter "FullyQualifiedName~ServiceUnavailable503_RetriesAndSucceeds" --logger "console;verbosity=detailed"
```

## What to Look For in Output

The diagnostic output will answer three key questions:

### Question 1: Is the proxy scenario actually returning HTTP 503?

Look for:
```
[RetryHttpHandler] Response received: HTTP 503 ServiceUnavailable at HH:mm:ss.fff
```

- **If YES**: Proxy is working, proceed to Question 2
- **If NO** (you see HTTP 200 OK): Proxy scenario isn't triggering - this is the root cause

### Question 2: Is RetryHttpHandler detecting it as retryable?

Look for:
```
[RetryHttpHandler] Status code ServiceUnavailable is retryable, attempt #1
```

- **If YES**: Retry logic is triggered, proceed to Question 3
- **If NO**: There's a bug in `IsRetryableStatusCode()` - unlikely since unit tests pass

### Question 3: Is the backoff delay actually happening?

Look for:
```
[RetryHttpHandler] Waiting 1s before retry at HH:mm:ss.fff
[RetryHttpHandler] Wait completed, retrying now at HH:mm:ss.fff (should be ~1 second later)
```

- **If YES**: Retry logic is working correctly, but test timing assertion is wrong
- **If NO**: Task.Delay isn't waiting as expected - very unusual

### Question 4: How many OpenSession calls were made?

Look for:
```
[DIAG] OpenSession calls made: X
```

- **If X = 1**: No retry happened (scenario didn't trigger or succeeded immediately)
- **If X >= 2**: Retry happened (should have taken >= 0.8s)

## Expected Outcomes & Next Actions

### Scenario A: Proxy Isn't Returning HTTP 503

**Symptoms**:
- No `[RetryHttpHandler]` logs showing HTTP 503
- OpenSession calls made: 1
- Elapsed time: ~0.4s

**Root Cause**: Proxy scenario isn't working as expected

**Next Actions**:
1. Check proxy implementation - how does auto-disable work?
2. Add delay between scenario enable and connection attempt
3. Verify scenario matching logic recognizes OpenSession requests
4. Consider using counter-based disable instead of auto-disable

### Scenario B: Retry Is Working, Test Expectations Wrong

**Symptoms**:
- `[RetryHttpHandler]` logs show HTTP 503, then HTTP 200
- Backoff delay logs show proper timing
- OpenSession calls made: 2+
- BUT elapsed time is still < 0.8s

**Root Cause**: Test timing measurement is incorrect or jitter calculation has a bug

**Next Actions**:
1. Review timing measurement in test
2. Check if connection establishment happens before or after OpenSession
3. Consider adjusting test expectations

### Scenario C: Everything Works (Test Should Pass)

**Symptoms**:
- HTTP 503 returned and detected
- Backoff delay executed (~1 second)
- Retry succeeded with HTTP 200
- OpenSession calls made: 2
- Elapsed time: >= 0.8s

**Root Cause**: Tests should be passing!

**Next Actions**:
1. Verify with user that tests are actually failing
2. Check if there's environment-specific behavior

## Cleanup After Investigation

Once the root cause is identified and fixed, remember to:

1. **Remove diagnostic logging from RetryHttpHandler**:
   - Delete System.Console.WriteLine statements added in lines 113, 118, 123, 193, 195
   - Restore original code

2. **Keep or remove test diagnostics**:
   - Test diagnostics (Console.WriteLine in SessionLifecycleTests.cs) can stay if helpful
   - Or remove them to reduce test output noise

3. **Update test-failures-analysis.md** with findings and resolution

## Questions for User

Before running the diagnostic test, please confirm:

1. **Do you have the DATABRICKS_TEST_CONFIG_FILE environment variable set?**
   - If not, what's the path to your databricks-test-config.json?

2. **Are the tests currently failing when you run them?**
   - Want to confirm we're solving a real problem

3. **Should I run the diagnostic test now, or would you prefer to run it yourself?**
   - I can run it if the config is available
   - Or you can run it and share the output
