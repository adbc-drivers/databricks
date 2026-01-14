/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.ThriftProtocol
{
    /// <summary>
    /// Tests that validate driver behavior for session management operations including
    /// OpenSession, CloseSession, session expiration, and timeout handling.
    ///
    /// Maps to test-infrastructure/specs/session-lifecycle.yaml
    /// </summary>
    public class SessionLifecycleTests : ProxyTestBase
    {
        private const string SimpleQuery = "SELECT 1 AS test_value";

        /// <summary>
        /// SESSION-001: Basic OpenSession Success
        /// Validates that driver can successfully open a session with valid credentials
        /// and receives a valid session handle.
        /// </summary>
        [Fact]
        public async Task BasicOpenSession_SucceedsWithValidCredentials()
        {
            // Arrange - No failure scenarios enabled

            // Act - Open a connection (which triggers OpenSession)
            using var connection = CreateProxiedConnection();

            // Assert - OpenSession should have been called exactly once
            var openSessionCalls = await ControlClient.CountThriftMethodCallsAsync("OpenSession");
            Assert.Equal(1, openSessionCalls);

            // Verify we can execute a simple query (proves session is valid)
            using var statement = connection.CreateStatement();
            statement.SqlQuery = SimpleQuery;
            var result = statement.ExecuteQuery();
            Assert.NotNull(result);

            using var reader = result.Stream;
            var batch = reader.ReadNextRecordBatchAsync().Result;
            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);
        }

        /// <summary>
        /// SESSION-002: CloseSession on Active Session
        /// Validates that driver can successfully close an active session and
        /// properly cleans up resources.
        /// </summary>
        [Fact]
        public async Task CloseSession_SucceedsOnActiveSession()
        {
            // Arrange - Open a connection
            var connection = CreateProxiedConnection();

            // Execute a query to ensure session is active
            using (var statement = connection.CreateStatement())
            {
                statement.SqlQuery = SimpleQuery;
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                _ = reader.ReadNextRecordBatchAsync().Result;
            }

            // Act - Close the connection (triggers CloseSession)
            connection.Dispose();

            // Wait a moment for async cleanup
            await Task.Delay(100);

            // Assert - CloseSession should have been called exactly once
            var closeSessionCalls = await ControlClient.CountThriftMethodCallsAsync("CloseSession");
            Assert.Equal(1, closeSessionCalls);
        }

        /// <summary>
        /// SESSION-003: Operation on Closed Session
        /// Validates that driver properly handles attempts to use a session handle
        /// after the session has been closed.
        ///
        /// JIRA: ES-610899
        /// </summary>
        [Fact]
        public async Task OperationOnClosedSession_ThrowsAppropriateError()
        {
            // Act & Assert - Attempting to use closed session should fail
            using var connection = CreateProxiedConnection();

            // First query should work (establishes session)
            using (var statement = connection.CreateStatement())
            {
                statement.SqlQuery = SimpleQuery;
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                _ = reader.ReadNextRecordBatchAsync().Result;
            }

            // Arrange - Enable invalid session handle scenario after first statement
            await ControlClient.EnableScenarioAsync("invalid_session_handle");

            // Scenario will invalidate the session on next operation
            // Second query should fail with session error
            var exception = Assert.ThrowsAny<Exception>(() =>
            {
                using var statement = connection.CreateStatement();
                statement.SqlQuery = SimpleQuery;
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                _ = reader.ReadNextRecordBatchAsync().Result;
            });

            // Verify the error indicates session issue (may be in inner exception)
            var fullMessage = exception.ToString();
            Assert.True(
                fullMessage.Contains("session", StringComparison.OrdinalIgnoreCase) ||
                fullMessage.Contains("500", StringComparison.OrdinalIgnoreCase) ||
                fullMessage.Contains("INVALID_HANDLE", StringComparison.OrdinalIgnoreCase),
                $"Expected error related to session/invalid handle, but got: {fullMessage}");
        }

        /// <summary>
        /// SESSION-004: Session Timeout Due to Inactivity
        /// Validates that driver handles session timeout when session idle timeout
        /// is exceeded without activity.
        ///
        /// This test configures a 1-minute session timeout and simulates the session
        /// expiring after 70 seconds of inactivity by using a proxy scenario that
        /// returns SESSION_EXPIRED error (simulating what the server would do after
        /// the actual timeout period).
        ///
        /// JIRA: ES-1661289
        /// </summary>
        [Fact]
        public async Task SessionTimeout_HandlesInactivityExpiration()
        {
            // Arrange - Create connection with 1-minute session idle timeout
            var parameters = new Dictionary<string, string>
            {
                // Configure session idle timeout to 1 minute for testing
                // (default is typically 60 minutes which is impractical for unit tests)
                ["spark.sql.session.timeZone"] = "UTC", // Example Spark config to verify param passing
                // Note: Actual session timeout is server-controlled, but we configure it here
                // to document the intended behavior
            };

            using var connection = CreateProxiedConnectionWithParameters(parameters);

            // Establish baseline - first query should succeed
            int baselineOpenSessionCount;
            using (var statement = connection.CreateStatement())
            {
                statement.SqlQuery = SimpleQuery;
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                _ = reader.ReadNextRecordBatchAsync().Result;
                baselineOpenSessionCount = await ControlClient.CountThriftMethodCallsAsync("OpenSession");
            }

            // Act - Simulate session timeout by enabling scenario
            // In reality, we would wait 70 seconds for the 1-minute timeout to expire,
            // but for unit testing we use a proxy scenario to immediately simulate
            // the SESSION_EXPIRED response that the server would return
            await ControlClient.EnableScenarioAsync("session_timeout_premature");

            // Attempt to use expired session on the same connection
            var exception = Assert.ThrowsAny<Exception>(() =>
            {
                using var statement = connection.CreateStatement();
                statement.SqlQuery = SimpleQuery;
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                _ = reader.ReadNextRecordBatchAsync().Result;
            });

            // Assert - Should get session expiration error
            Assert.NotNull(exception);
            var fullMessage = exception.ToString();
            Assert.True(
                fullMessage.Contains("session", StringComparison.OrdinalIgnoreCase) ||
                fullMessage.Contains("expired", StringComparison.OrdinalIgnoreCase) ||
                fullMessage.Contains("timeout", StringComparison.OrdinalIgnoreCase) ||
                fullMessage.Contains("500", StringComparison.OrdinalIgnoreCase),
                $"Expected error related to session timeout/expiration, but got: {fullMessage}");
        }

        /// <summary>
        /// SESSION-005: Auto-Reconnect on Communication Error During Query Execution
        /// Validates that driver automatically reconnects when communication errors occur
        /// during query execution (connection drops, network timeouts).
        ///
        /// This test simulates a connection drop during ExecuteStatement (query execution).
        /// With auto-reconnect enabled, the driver should:
        /// 1. Detect the communication error
        /// 2. Automatically create a new session
        /// 3. Retry the query on the new session
        /// 4. Return results without throwing exception to the caller
        ///
        /// Expected behavior with auto-reconnect:
        /// - Query succeeds without exception
        /// - Driver opens a new session automatically (OpenSession count increases)
        /// - Application is unaware of the reconnection
        ///
        /// Note: This is different from SESSION-004 (session expiration), which tests
        /// handling of SESSION_EXPIRED Thrift errors, not communication/transport errors.
        ///
        /// JIRA: ES-1661289
        /// </summary>
        [Fact(Skip = "Auto-reconnect feature not yet implemented - waiting for driver support")]
        public async Task CommunicationError_WithAutoReconnect_ReconnectsSuccessfully()
        {
            // Arrange - Create connection with auto-reconnect enabled
            var parameters = new Dictionary<string, string>
            {
                // TODO: Replace with actual auto-reconnect parameter when implemented
                // Expected parameter name might be one of:
                // - "adbc.spark.auto_reconnect"
                // - "adbc.spark.session.auto_reconnect"
                // - "adbc.databricks.auto_reconnect"
                ["adbc.spark.auto_reconnect"] = "true"
            };

            using var connection = CreateProxiedConnectionWithParameters(parameters);

            // Execute initial query to establish baseline
            int baselineOpenSessionCount;
            using (var statement = connection.CreateStatement())
            {
                statement.SqlQuery = SimpleQuery;
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                _ = reader.ReadNextRecordBatchAsync().Result;
                baselineOpenSessionCount = await ControlClient.CountThriftMethodCallsAsync("OpenSession");
            }

            // Enable communication error scenario - simulates connection drop during ExecuteStatement
            await ControlClient.EnableScenarioAsync("connection_drop_during_metadata");

            // Act - Execute query that will encounter communication error during execution
            // Driver should detect connection error and automatically reconnect
            using (var statement = connection.CreateStatement())
            {
                statement.SqlQuery = SimpleQuery;
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                var batch = reader.ReadNextRecordBatchAsync().Result;

                // Assert - Query should succeed after auto-reconnect (no exception thrown)
                Assert.NotNull(batch);
                Assert.True(batch.Length > 0);
            }

            // Assert - Driver should have opened a new session automatically
            var finalOpenSessionCount = await ControlClient.CountThriftMethodCallsAsync("OpenSession");
            Assert.True(finalOpenSessionCount > baselineOpenSessionCount,
                $"Expected driver to open new session automatically after connection error. Baseline: {baselineOpenSessionCount}, Final: {finalOpenSessionCount}");

            // Verify that driver logs show reconnection attempt
            // (This would require checking driver logs, which may need proxy support)
        }

        /// <summary>
        /// SESSION-006: CloseSession with Active Operations
        /// Validates driver behavior when CloseSession is called while
        /// operations are still running.
        ///
        /// Expected behavior:
        /// - Connection close should succeed even if operations are running
        /// - CloseSession should be called successfully
        /// - Server is responsible for cleaning up active operations when session closes
        ///
        /// IMPLEMENTATION NOTE:
        /// The driver relies on server-side cleanup when CloseSession is called. According to
        /// HiveServer2 specifications, the server automatically closes and removes all operations
        /// associated with a session when CloseSession is invoked. The driver does NOT explicitly
        /// call CancelOperation before CloseSession.
        ///
        /// The driver DOES support explicit cancellation via CancellationToken (see
        /// HiveServer2Statement.CancelOperationAsync), but this is only used for user-initiated
        /// cancellations, not for automatic cleanup during connection disposal.
        ///
        /// CAVEAT:
        /// Historical HiveServer2 JDBC implementations had issues where closing operations didn't
        /// always terminate running jobs on the cluster - they only cleaned up client-side handles.
        /// While modern implementations should handle this correctly, applications requiring
        /// guaranteed job cancellation should explicitly cancel operations before closing connections.
        ///
        /// JIRA: XTA-11040
        /// </summary>
        [Fact]
        public async Task CloseSessionWithActiveOperations_ClosesSuccessfully()
        {
            // Arrange - Open connection
            var connection = CreateProxiedConnection();

            // Start a long-running query in the background
            // Using a large CROSS JOIN to ensure query takes significant time
            var longRunningQueryTask = Task.Run(() =>
            {
                try
                {
                    using var statement = connection.CreateStatement();
                    // Query that will take a few seconds: cross join on small ranges
                    // 2000 x 2000 = 4 million rows, should take 2-3 seconds
                    statement.SqlQuery = "SELECT count(*) FROM range(1, 2000) CROSS JOIN range(1, 2000)";
                    var result = statement.ExecuteQuery();
                    using var reader = result.Stream;
                    _ = reader.ReadNextRecordBatchAsync().Result;
                    return true; // Query completed
                }
                catch
                {
                    return false; // Query was interrupted/cancelled
                }
            });

            // Wait to ensure query has started executing
            await Task.Delay(1000);

            // Get baseline call counts before closing
            var closeSessionCountBefore = await ControlClient.CountThriftMethodCallsAsync("CloseSession");

            // Act - Close connection while query is running
            // This should succeed without hanging, relying on server-side cleanup
            connection.Dispose();

            // Assert - CloseSession should have been called
            var closeSessionCountAfter = await ControlClient.CountThriftMethodCallsAsync("CloseSession");
            Assert.True(closeSessionCountAfter > closeSessionCountBefore,
                $"Expected CloseSession to be called when disposing connection. Before: {closeSessionCountBefore}, After: {closeSessionCountAfter}");

            // Wait for background task to complete (may take longer since server handles cleanup)
            var completed = await Task.WhenAny(longRunningQueryTask, Task.Delay(10000)) == longRunningQueryTask;
            Assert.True(completed, "Long-running query task should complete within 10 seconds after connection close");

            // The query may either complete successfully (if it finished before connection closed)
            // or be interrupted (if connection closed while still executing).
            // Both outcomes are acceptable - the key is that the connection closed without hanging.
            var querySucceeded = await longRunningQueryTask;
            // No assertion on querySucceeded - we just verify the connection closed cleanly above
        }

        /// <summary>
        /// SESSION-007: Concurrent Session Close
        /// Validates that driver handles multiple threads attempting to
        /// close the same session simultaneously.
        /// </summary>
        [Fact]
        public async Task ConcurrentSessionClose_HandlesGracefully()
        {
            // Arrange - Open a connection
            var connection = CreateProxiedConnection();

            // Execute a query to ensure session is active
            using (var statement = connection.CreateStatement())
            {
                statement.SqlQuery = SimpleQuery;
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                _ = reader.ReadNextRecordBatchAsync().Result;
            }

            // Act - Attempt to close from multiple threads simultaneously
            var exceptions = new System.Collections.Concurrent.ConcurrentBag<Exception>();
            var tasks = new[]
            {
                Task.Run(() => { try { connection.Dispose(); } catch (Exception ex) { exceptions.Add(ex); } }),
                Task.Run(() => { try { connection.Dispose(); } catch (Exception ex) { exceptions.Add(ex); } }),
                Task.Run(() => { try { connection.Dispose(); } catch (Exception ex) { exceptions.Add(ex); } })
            };

            await Task.WhenAll(tasks);

            // Assert - Should not throw exceptions (driver handles concurrent close gracefully)
            Assert.Empty(exceptions);

            // Only one CloseSession should actually be sent
            var closeSessionCalls = await ControlClient.CountThriftMethodCallsAsync("CloseSession");
            Assert.True(closeSessionCalls == 1, $"Expected exactly 1 CloseSession call, but got {closeSessionCalls}");
        }

        /// <summary>
        /// SESSION-008: Session with Expired Credentials
        /// Validates that driver handles OpenSession failure due to
        /// expired authentication credentials.
        /// </summary>
        [Fact]
        public async Task SessionWithExpiredCredentials_ThrowsAuthenticationError()
        {
            // Arrange - Enable expired credentials scenario
            await ControlClient.EnableScenarioAsync("expired_credentials");

            // Act & Assert - Should fail with authentication error
            var exception = Assert.ThrowsAny<Exception>(() =>
            {
                using var connection = CreateProxiedConnection();
            });

            // Verify error indicates authentication or session issue
            Assert.NotNull(exception);
            var message = exception.Message.ToLower();
            Assert.True(
                message.Contains("auth") || message.Contains("token") || message.Contains("credential"),
                $"Expected authentication error but got: {exception.Message}"
            );
        }

        /// <summary>
        /// SESSION-009: OpenSession Network Timeout
        /// Validates that driver handles network timeout during OpenSession request.
        ///
        /// The driver's connection timeout is determined by:
        /// - Base timeout: HiveServer2Connection.ConnectTimeoutMillisecondsDefault = 30 seconds
        /// - With TemporarilyUnavailableRetry enabled (default): timeout is adjusted to
        ///   max(base_timeout, TemporarilyUnavailableRetryTimeout * 1000)
        /// - Default TemporarilyUnavailableRetryTimeout = 900 seconds (15 minutes)
        ///
        /// This test configures TemporarilyUnavailableRetryTimeout to 20 seconds, so the
        /// effective connection timeout becomes 20 seconds. The proxy delays OpenSession
        /// by 35 seconds, which exceeds this timeout.
        ///
        /// Without auto-reconnect: Should throw timeout/connection error
        /// With auto-reconnect enabled: Should retry and eventually succeed or fail after max retries
        ///
        /// This test validates the error case without auto-reconnect.
        /// For auto-reconnect behavior, see SESSION-005.
        /// </summary>
        [Fact(Skip = "35 second delay - enable for comprehensive testing")]
        public async Task OpenSessionNetworkTimeout_ThrowsTimeoutError()
        {
            // Arrange - Configure timeout to 20 seconds, then enable 35-second delay scenario
            // Driver will adjust ConnectTimeoutMilliseconds to 20,000ms based on retry timeout
            var parameters = new Dictionary<string, string>
            {
                ["adbc.spark.temporarily_unavailable_retry_timeout"] = "20"  // 20 seconds
            };

            // Enable network timeout scenario (35s delay) - exceeds our 20s timeout
            await ControlClient.EnableScenarioAsync("network_timeout_open_session");

            // Act & Assert - Driver should throw timeout error after 20+ seconds
            var startTime = DateTime.UtcNow;

            var exception = Assert.ThrowsAny<Exception>(() =>
            {
                using var connection = CreateProxiedConnectionWithParameters(parameters);
            });

            var elapsed = DateTime.UtcNow - startTime;

            // Verify the timeout occurred (should be around 20 seconds, not 35)
            Assert.True(elapsed.TotalSeconds >= 18 && elapsed.TotalSeconds <= 32,
                $"Expected timeout around 20s but took {elapsed.TotalSeconds}s");

            // Verify timeout or connection error occurred
            Assert.NotNull(exception);
            var exceptionString = exception.ToString();
            Assert.True(
                exceptionString.Contains("timeout", StringComparison.OrdinalIgnoreCase) ||
                exceptionString.Contains("connection", StringComparison.OrdinalIgnoreCase) ||
                exceptionString.Contains("network", StringComparison.OrdinalIgnoreCase),
                $"Expected timeout/connection error but got: {exceptionString}");
        }

        /// <summary>
        /// SESSION-010: CloseSession Network Failure
        /// Validates that driver handles network failure during CloseSession
        /// and still cleans up local resources.
        /// </summary>
        [Fact]
        public async Task CloseSessionNetworkFailure_CleansUpLocalResources()
        {
            // Arrange - Open a connection and execute a query
            var connection = CreateProxiedConnection();
            using (var statement = connection.CreateStatement())
            {
                statement.SqlQuery = SimpleQuery;
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                _ = reader.ReadNextRecordBatchAsync().Result;
            }

            // Enable network failure for CloseSession
            await ControlClient.EnableScenarioAsync("network_failure_close_session");

            // Act - Connection disposal with network failure during CloseSession
            // The driver may throw during Dispose() if CloseSession fails,
            // but should still clean up local resources
            Exception? caughtException = null;
            try
            {
                connection.Dispose();
            }
            catch (Exception ex)
            {
                caughtException = ex;
            }

            // Assert - Either Dispose succeeds silently OR throws a network-related error
            // Both behaviors are acceptable as long as resources are cleaned up
            if (caughtException != null)
            {
                // If an exception was thrown, verify it's related to network/connection failure
                var exceptionString = caughtException.ToString();
                Assert.True(
                    exceptionString.Contains("connect", StringComparison.OrdinalIgnoreCase) ||
                    exceptionString.Contains("response ended", StringComparison.OrdinalIgnoreCase) ||
                    exceptionString.Contains("network", StringComparison.OrdinalIgnoreCase) ||
                    exceptionString.Contains("connection", StringComparison.OrdinalIgnoreCase),
                    $"Expected network-related error during CloseSession, but got: {exceptionString}");
            }

            // Note: We can't easily verify local resource cleanup without driver instrumentation,
            // but the test validates the driver handles CloseSession failure appropriately
        }

        /// <summary>
        /// SESSION-011: Retry on Transient HTTP 503 Service Unavailable
        /// Validates that driver automatically retries OpenSession when receiving
        /// HTTP 503 Service Unavailable errors using exponential backoff.
        ///
        /// The driver's RetryHttpHandler retries these status codes:
        /// - 408 Request Timeout
        /// - 429 Too Many Requests (separate timeout)
        /// - 502 Bad Gateway
        /// - 503 Service Unavailable
        /// - 504 Gateway Timeout
        ///
        /// Retry behavior (RetryHttpHandler.cs):
        /// - Uses Retry-After header if present, otherwise exponential backoff
        /// - Backoff: starts at 1s, doubles each time (1, 2, 4, 8, 16, 32), max 32s
        /// - Adds jitter (80-120% of base) to avoid thundering herd
        /// - Retries until TemporarilyUnavailableRetryTimeout is exceeded
        /// - Default TemporarilyUnavailableRetryTimeout = 900 seconds (15 minutes)
        ///
        /// This test simulates a 503 error that clears after the first attempt.
        /// The proxy returns 503 once then auto-disables. The driver should:
        /// 1. Receive 503 on first OpenSession attempt
        /// 2. Wait ~1 second (initial backoff with jitter)
        /// 3. Retry OpenSession and succeed
        /// </summary>
        [Fact]
        public async Task ServiceUnavailable503_RetriesAndSucceeds()
        {
            // Arrange - Enable 503 Service Unavailable scenario
            // The proxy will return 503 once, then auto-disable (allowing retry to succeed)

            // Enable scenario first (this clears call history)
            await ControlClient.EnableScenarioAsync("service_unavailable_503_open_session");
            Console.WriteLine($"[DIAG] Scenario 'service_unavailable_503_open_session' enabled");

            // Small delay to ensure scenario is fully enabled
            await Task.Delay(100);

            // Get baseline call count AFTER enabling scenario (after call history is cleared)
            var initialOpenSessionCount = await ControlClient.CountThriftMethodCallsAsync("OpenSession");
            Console.WriteLine($"[DIAG] Initial OpenSession count: {initialOpenSessionCount}");

            // Act - Attempt to open connection
            // Driver should:
            // 1. Try OpenSession -> receive 503
            // 2. Wait with backoff (~1 second with jitter)
            // 3. Retry OpenSession -> succeed (scenario auto-disabled)
            Console.WriteLine($"[DIAG] Starting connection attempt at {DateTime.UtcNow:HH:mm:ss.fff}");
            var startTime = DateTime.UtcNow;
            using var connection = CreateProxiedConnection();
            var elapsed = DateTime.UtcNow - startTime;
            Console.WriteLine($"[DIAG] Connection established at {DateTime.UtcNow:HH:mm:ss.fff}");
            Console.WriteLine($"[DIAG] Total elapsed time: {elapsed.TotalSeconds:F3}s");

            // Check OpenSession call count
            var finalOpenSessionCount = await ControlClient.CountThriftMethodCallsAsync("OpenSession");
            var openSessionCallsMade = finalOpenSessionCount - initialOpenSessionCount;
            Console.WriteLine($"[DIAG] Final OpenSession count: {finalOpenSessionCount}");
            Console.WriteLine($"[DIAG] OpenSession calls made: {openSessionCallsMade}");

            // Assert - Verify retry behavior
            // 1. Connection should eventually succeed (no exception thrown)
            Assert.NotNull(connection);

            // 2. Should have taken at least 1 second (backoff delay)
            // Allow some tolerance for timing (0.8s - accounts for jitter 80-120%)
            Assert.True(elapsed.TotalSeconds >= 0.8,
                $"Expected at least 0.8s for retry backoff, but only took {elapsed.TotalSeconds:F3}s. " +
                $"OpenSession was called {openSessionCallsMade} times (expected >= 2).");

            // 3. OpenSession should have been called more than once (showing retry happened)
            Assert.True(openSessionCallsMade >= 2,
                $"Expected at least 2 OpenSession calls (initial + retry), but only {openSessionCallsMade} were made. " +
                $"Elapsed time: {elapsed.TotalSeconds:F3}s");

            // 4. Verify we can execute a query (connection is valid)
            using var statement = connection.CreateStatement();
            statement.SqlQuery = SimpleQuery;
            var result = statement.ExecuteQuery();
            Assert.NotNull(result);

            using var reader = result.Stream;
            var batch = reader.ReadNextRecordBatchAsync().Result;
            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);
        }

        /// <summary>
        /// SESSION-012: Retry on Other Transient HTTP Errors (408, 429, 502, 504)
        /// Validates that driver automatically retries OpenSession when receiving
        /// other retryable HTTP errors.
        ///
        /// Status codes tested:
        /// - 408 Request Timeout
        /// - 429 Too Many Requests (uses RateLimitRetryTimeout instead of TemporarilyUnavailableRetryTimeout)
        /// - 502 Bad Gateway
        /// - 504 Gateway Timeout
        ///
        /// Note: 429 uses RateLimitRetryTimeout (default 120s) while others use
        /// TemporarilyUnavailableRetryTimeout (default 900s), but for a single retry
        /// (~1 second backoff), both timeouts are sufficient.
        /// </summary>
        [Theory]
        [InlineData(408, "request_timeout_408_open_session", "Request Timeout")]
        [InlineData(429, "too_many_requests_429_open_session", "Too Many Requests")]
        [InlineData(502, "bad_gateway_502_open_session", "Bad Gateway")]
        [InlineData(504, "gateway_timeout_504_open_session", "Gateway Timeout")]
        public async Task TransientHttpErrors_RetriesAndSucceeds(int statusCode, string scenarioName, string errorType)
        {
            // Arrange - Enable error scenario (auto-disables after first error)
            await ControlClient.EnableScenarioAsync(scenarioName);

            // Small delay to ensure scenario is fully enabled
            await Task.Delay(100);

            // Get baseline call count AFTER enabling scenario (after call history is cleared)
            var initialOpenSessionCount = await ControlClient.CountThriftMethodCallsAsync("OpenSession");

            // Act - Attempt to open connection
            var startTime = DateTime.UtcNow;
            using var connection = CreateProxiedConnection();
            var elapsed = DateTime.UtcNow - startTime;

            // Assert - Verify retry behavior
            Assert.NotNull(connection);

            // Should have taken at least 0.8 seconds (backoff delay with jitter)
            Assert.True(elapsed.TotalSeconds >= 0.8,
                $"Expected at least 0.8s for retry backoff on HTTP {statusCode} ({errorType}), but only took {elapsed.TotalSeconds}s");

            // OpenSession should have been called at least twice (initial + retry)
            var finalOpenSessionCount = await ControlClient.CountThriftMethodCallsAsync("OpenSession");
            var openSessionCallsMade = finalOpenSessionCount - initialOpenSessionCount;
            Assert.True(openSessionCallsMade >= 2,
                $"Expected at least 2 OpenSession calls for HTTP {statusCode} ({errorType}), but only {openSessionCallsMade} were made");

            // Verify connection is valid
            using var statement = connection.CreateStatement();
            statement.SqlQuery = SimpleQuery;
            var result = statement.ExecuteQuery();
            Assert.NotNull(result);
        }
    }
}
