# Session Lifecycle Test Design Notes

## SESSION-006: CloseSession with Active Operations

### Design Decision: Server-Side Cleanup

The C# ADBC driver for Databricks relies on **server-side cleanup** when closing a session with active operations, rather than explicitly calling `CancelOperation` before `CloseSession`.

### Rationale

According to HiveServer2 specifications and implementation details:

1. **Automatic Cleanup**: When `CloseSession` is called, HiveServer2 automatically closes and removes all operations associated with that session. The server's `HiveSession.close()` method handles cleanup of operation handles internally.

2. **Simplified Client Logic**: The driver doesn't need to maintain a registry of active operations solely for disposal purposes. This reduces complexity and potential race conditions.

3. **Existing Cancellation Support**: The driver already provides explicit cancellation via `CancellationToken` (see `HiveServer2Statement.CancelOperationAsync` in the upstream Apache Arrow ADBC codebase). This is available for user-initiated cancellations when needed.

4. **Industry Practice**: Review of JDBC implementations shows that many drivers rely on server-side cleanup during session closure rather than explicitly canceling each operation.

### Important Caveat

⚠️ **Historical Compatibility Issue**

Historical HiveServer2 JDBC implementations had a known limitation: calling `.close()` or `.cancel()` on statements would clean up client-side handles but **did not always terminate running jobs on the cluster**.

**References:**
- Discussion: [How to terminate a running Hive Query (JDBC, Hive Server 2)](https://user.hive.apache.narkive.com/zC6TuNlr/how-to-terminate-a-running-hive-query-executed-with-jdbc-hive-server-2)
- Tabnine code examples: [HiveSession.close implementation](https://www.tabnine.com/code/java/methods/org.apache.hive.service.cli.session.HiveSession/close)
- JIRA: [HIVE-5799 - session/operation timeout for hiveserver2](https://issues.apache.org/jira/browse/HIVE-5799)

While modern implementations (including Databricks) should handle this correctly, **applications requiring guaranteed immediate job termination should explicitly cancel operations before closing connections**.

### Recommended Practice for Applications

If your application needs to ensure running queries are immediately stopped:

```csharp
// Option 1: Use CancellationToken for query execution
var cts = new CancellationTokenSource();
var statement = connection.CreateStatement();
statement.SqlQuery = "SELECT * FROM long_running_query";

// Later, cancel the query explicitly
cts.Cancel();  // This will call CancelOperation on the server

// Option 2: Dispose statement before connection
using (var statement = connection.CreateStatement())
{
    // Execute query
}
// Statement disposal may trigger cleanup

connection.Dispose();  // Now close connection
```

### Test Validation

SESSION-006 validates:
- ✅ Connection closes successfully without hanging
- ✅ `CloseSession` is called to the server
- ✅ Background query task completes (either naturally or via interruption)
- ❌ Does NOT validate that `CancelOperation` is explicitly called (relies on server)

### Related Tests

- **SESSION-005**: Tests explicit auto-reconnect after communication errors
- **SESSION-007**: Tests concurrent session close scenarios
- **SESSION-010**: Tests network failure during CloseSession

---

## General Session Lifecycle Principles

### Session Management Philosophy

1. **Trust the Server**: The server (HiveServer2/Databricks) is responsible for resource cleanup
2. **Explicit When Needed**: Use `CancellationToken` for user-initiated cancellations
3. **Graceful Degradation**: Handle network errors during cleanup without failing
4. **No Resource Leaks**: Ensure client-side resources (connections, readers) are disposed

### Key Driver Behaviors

| Scenario | Driver Behavior | Server Behavior |
|----------|----------------|-----------------|
| Normal connection close | Calls `CloseSession` | Cleans up session and operations |
| Connection close with active ops | Calls `CloseSession` | Cleans up session and operations |
| User cancels query | Calls `CancelOperation` | Cancels operation, keeps session |
| Network failure during close | Disposes transport, logs error | Session eventually times out |
| Concurrent close attempts | Thread-safe disposal | Single `CloseSession` call |

### Testing Strategy

Session lifecycle tests use a proxy server (mitmproxy) to:
- Inject network failures and error responses
- Track Thrift method calls (OpenSession, CloseSession, CancelOperation, etc.)
- Simulate timeout and retry scenarios
- Validate driver behavior under adverse conditions

This allows testing edge cases that would be difficult or impossible to reproduce with a real Databricks cluster.
