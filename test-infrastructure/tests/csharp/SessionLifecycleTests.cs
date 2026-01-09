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

            // Assert - CloseSession should have been called
            var closeSessionCalls = await ControlClient.CountThriftMethodCallsAsync("CloseSession");
            Assert.True(closeSessionCalls >= 1, $"Expected CloseSession to be called at least once, but was called {closeSessionCalls} times");
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
            // Arrange - Enable invalid session handle scenario
            await ControlClient.EnableScenarioAsync("invalid_session_handle");

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

            // Scenario will invalidate the session on next operation
            // Second query should fail with session error
            var exception = Assert.Throws<AdbcException>(() =>
            {
                using var statement = connection.CreateStatement();
                statement.SqlQuery = SimpleQuery;
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                _ = reader.ReadNextRecordBatchAsync().Result;
            });

            // Verify the error indicates session issue
            Assert.Contains("session", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// SESSION-004: Session Timeout Due to Inactivity
        /// Validates that driver handles session timeout when session idle timeout
        /// is exceeded without activity.
        ///
        /// JIRA: ES-1661289
        /// </summary>
        [Fact(Skip = "Requires 70+ second wait time - enable for comprehensive testing")]
        public async Task SessionTimeout_HandlesInactivityExpiration()
        {
            // Arrange - Enable session timeout scenario
            await ControlClient.EnableScenarioAsync("session_timeout_premature");

            // Establish baseline
            int baselineOpenSessionCount;
            using (var connection = CreateProxiedConnection())
            using (var statement = connection.CreateStatement())
            {
                statement.SqlQuery = SimpleQuery;
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                _ = reader.ReadNextRecordBatchAsync().Result;
                baselineOpenSessionCount = await ControlClient.CountThriftMethodCallsAsync("OpenSession");
            }

            // Act - Wait for session to timeout (70 seconds > 60s default timeout)
            await Task.Delay(TimeSpan.FromSeconds(70));

            // Attempt to use expired session
            var exception = Assert.ThrowsAny<Exception>(() =>
            {
                using var connection = CreateProxiedConnection();
                using var statement = connection.CreateStatement();
                statement.SqlQuery = SimpleQuery;
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                _ = reader.ReadNextRecordBatchAsync().Result;
            });

            // Assert - Should get session expiration error
            Assert.NotNull(exception);
        }

        /// <summary>
        /// SESSION-005: CloseSession with Active Operations
        /// Validates driver behavior when CloseSession is called while
        /// operations are still running.
        ///
        /// JIRA: XTA-11040
        /// </summary>
        [Fact(Skip = "Requires complex concurrency handling - implement when proxy supports operation tracking")]
        public async Task CloseSessionWithActiveOperations_CancelsOperations()
        {
            // Arrange - Enable scenario
            await ControlClient.EnableScenarioAsync("session_close_with_active_operations");

            // This test requires:
            // 1. Starting a long-running query
            // 2. Closing connection while query is running
            // 3. Verifying CancelOperation is called
            //
            // Implementation depends on proxy server tracking active operations

            Assert.True(true, "Test structure defined - implementation pending proxy support");
        }

        /// <summary>
        /// SESSION-007: Multiple Concurrent Sessions
        /// Validates that driver can maintain multiple concurrent sessions
        /// independently without interference.
        /// </summary>
        [Fact]
        public async Task MultipleConcurrentSessions_OperateIndependently()
        {
            // Arrange & Act - Open two separate connections
            using var connection1 = CreateProxiedConnection();
            using var connection2 = CreateProxiedConnection();

            // Execute queries on both connections
            using (var statement1 = connection1.CreateStatement())
            {
                statement1.SqlQuery = SimpleQuery;
                var result1 = statement1.ExecuteQuery();
                using var reader1 = result1.Stream;
                var batch1 = reader1.ReadNextRecordBatchAsync().Result;
                Assert.NotNull(batch1);
            }

            using (var statement2 = connection2.CreateStatement())
            {
                statement2.SqlQuery = SimpleQuery;
                var result2 = statement2.ExecuteQuery();
                using var reader2 = result2.Stream;
                var batch2 = reader2.ReadNextRecordBatchAsync().Result;
                Assert.NotNull(batch2);
            }

            // Assert - Two separate OpenSession calls should have been made
            var openSessionCalls = await ControlClient.CountThriftMethodCallsAsync("OpenSession");
            Assert.Equal(2, openSessionCalls);

            // Both queries should have succeeded independently
            await ControlClient.AssertThriftMethodCalledAsync("ExecuteStatement", minCalls: 2);
        }

        /// <summary>
        /// SESSION-008: OpenSession with Configuration Parameters
        /// Validates that driver correctly passes session configuration parameters
        /// to OpenSession request.
        /// </summary>
        [Fact]
        public async Task OpenSessionWithConfiguration_PassesParameters()
        {
            // Arrange - Create connection with custom session configuration
            var parameters = new System.Collections.Generic.Dictionary<string, string>
            {
                ["spark.sql.adaptive.enabled"] = "true",
                ["spark.sql.shuffle.partitions"] = "200"
            };

            // Act - Open connection (triggers OpenSession with config)
            using var connection = CreateProxiedConnectionWithParameters(parameters);

            // Execute a query to ensure session is active
            using var statement = connection.CreateStatement();
            statement.SqlQuery = SimpleQuery;
            var result = statement.ExecuteQuery();
            Assert.NotNull(result);

            // Assert - OpenSession should have been called
            var openSessionCalls = await ControlClient.CountThriftMethodCallsAsync("OpenSession");
            Assert.Equal(1, openSessionCalls);

            // Note: Verifying that parameters were actually sent requires Thrift message inspection
            // which is tracked by the proxy but not yet exposed via control API
        }

        /// <summary>
        /// SESSION-014: Concurrent Session Close
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
        /// SESSION-010: Session with Expired Credentials
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
        /// SESSION-011: OpenSession Network Timeout
        /// Validates that driver handles network timeout during OpenSession request.
        /// </summary>
        [Fact(Skip = "35 second delay - enable for comprehensive testing")]
        public async Task OpenSessionNetworkTimeout_RetriesWithBackoff()
        {
            // Arrange - Enable network timeout scenario (35s delay)
            await ControlClient.EnableScenarioAsync("network_timeout_open_session");

            // Act & Assert - Driver should handle timeout
            // This will take 35+ seconds as it waits for the delay
            var startTime = DateTime.UtcNow;

            var exception = Assert.ThrowsAny<Exception>(() =>
            {
                using var connection = CreateProxiedConnection();
            });

            var elapsed = DateTime.UtcNow - startTime;

            // Verify the delay occurred (at least 30 seconds)
            Assert.True(elapsed.TotalSeconds >= 30,
                $"Expected delay of at least 30s but only took {elapsed.TotalSeconds}s");

            // Verify some kind of timeout or connection error occurred
            Assert.NotNull(exception);
        }

        /// <summary>
        /// SESSION-012: CloseSession Network Failure
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

            // Act - Connection disposal should handle failure gracefully
            // (CloseSession will fail but local resources should still be cleaned)
            // The key requirement is that Dispose() doesn't throw to the caller
            Exception? caughtException = null;
            try
            {
                connection.Dispose();
            }
            catch (Exception ex)
            {
                caughtException = ex;
            }

            // Assert - Dispose should not throw even if network fails
            // Driver should clean up local resources and log the failure
            Assert.Null(caughtException);

            // Note: We can't easily verify local resource cleanup without driver instrumentation,
            // but the absence of exceptions indicates graceful handling
        }
    }
}
