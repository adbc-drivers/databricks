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

using System.Threading.Tasks;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.ThriftProtocol
{
    /// <summary>
    /// Tests that validate driver behavior for Thrift session lifecycle operations.
    /// Verifies OpenSession, CloseSession, and session management with call tracking.
    /// </summary>
    public class SessionLifecycleTests : ProxyTestBase
    {
        [Fact]
        public async Task BasicSession_OpensAndCloses()
        {
            // Arrange - Reset call history explicitly for this test
            await ControlClient.ResetThriftCallsAsync();

            // Act - Create and dispose connection (triggers OpenSession and CloseSession)
            using (var connection = CreateProxiedConnection())
            {
                // Connection is open, session should be established
                Assert.NotNull(connection);
            }
            // Dispose triggers CloseSession

            // Assert - Verify OpenSession and CloseSession were called
            var result = await ControlClient.VerifyThriftCallsAsync(
                type: "contains_sequence",
                methods: new System.Collections.Generic.List<string> { "OpenSession", "CloseSession" });

            Assert.NotNull(result);
            Assert.True(result.Verified,
                $"Expected OpenSession → CloseSession sequence. Actual: [{string.Join(", ", result.Actual ?? new System.Collections.Generic.List<string>())}]");
        }

        [Fact]
        public async Task Session_ExecutesQuery_WithProperSequence()
        {
            // Arrange
            await ControlClient.ResetThriftCallsAsync();

            // Act - Execute a simple query
            using (var connection = CreateProxiedConnection())
            {
                // Disable cached results to force separate CloseOperation call (slow path)
                using (var disableCache = connection.CreateStatement())
                {
                    disableCache.SqlQuery = "SET use_cached_result = false";
                    disableCache.ExecuteUpdate();
                }

                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 1 as test_col";

                var result = statement.ExecuteQuery();
                Assert.NotNull(result);

                using var reader = result.Stream;
                Assert.NotNull(reader);

                // Read at least one batch to ensure operation completes
                var batch = reader.ReadNextRecordBatchAsync().Result;
                Assert.NotNull(batch);

                // Re-enable cached results
                using (var enableCache = connection.CreateStatement())
                {
                    enableCache.SqlQuery = "SET use_cached_result = true";
                    enableCache.ExecuteUpdate();
                }
            } // Connection disposed here

            // Give time for async disposal to complete
            await Task.Delay(500);

            // Assert - Verify expected sequence
            var verification = await ControlClient.VerifyThriftCallsAsync(
                type: "contains_sequence",
                methods: new System.Collections.Generic.List<string>
                {
                    "OpenSession",
                    "ExecuteStatement",
                    "CloseSession"
                });

            Assert.NotNull(verification);
            Assert.True(verification.Verified,
                $"Expected OpenSession → ExecuteStatement → CloseSession. " +
                $"Actual: [{string.Join(", ", verification.Actual ?? new System.Collections.Generic.List<string>())}]");

            // If GetOperationStatus was called (slow path), CloseOperation must also be called
            var getOpStatus = await ControlClient.VerifyThriftCallsAsync(
                type: "method_exists",
                method: "GetOperationStatus");

            if (getOpStatus?.Verified == true)
            {
                var closeOp = await ControlClient.VerifyThriftCallsAsync(
                    type: "method_exists",
                    method: "CloseOperation");

                Assert.True(closeOp?.Verified == true,
                    "CloseOperation must be called when GetOperationStatus is called (slow path)");
            }
        }

        [Fact]
        public async Task Session_WithMultipleStatements_TracksAllOperations()
        {
            // Arrange
            await ControlClient.ResetThriftCallsAsync();

            // Act - Execute multiple statements in same session
            using var connection = CreateProxiedConnection();

            // First statement
            using (var statement1 = connection.CreateStatement())
            {
                statement1.SqlQuery = "SELECT 1 as col1";
                var result1 = statement1.ExecuteQuery();
                using var reader1 = result1.Stream;
                reader1.ReadNextRecordBatchAsync().AsTask().Wait();
            }

            // Second statement
            using (var statement2 = connection.CreateStatement())
            {
                statement2.SqlQuery = "SELECT 2 as col2";
                var result2 = statement2.ExecuteQuery();
                using var reader2 = result2.Stream;
                reader2.ReadNextRecordBatchAsync().AsTask().Wait();
            }

            // Assert - Verify OpenSession called once, ExecuteStatement called twice
            var openSessionCount = await ControlClient.VerifyThriftCallsAsync(
                type: "method_count",
                method: "OpenSession",
                count: 1);
            Assert.True(openSessionCount?.Verified,
                $"Expected OpenSession called once. Actual count: {openSessionCount?.ActualCount}");

            var executeCount = await ControlClient.VerifyThriftCallsAsync(
                type: "method_count",
                method: "ExecuteStatement",
                count: 2);
            Assert.True(executeCount?.Verified,
                $"Expected ExecuteStatement called twice. Actual count: {executeCount?.ActualCount}");
        }

        [Fact]
        public async Task Session_CloseOperationCalled_AfterEachStatement()
        {
            // Arrange
            await ControlClient.ResetThriftCallsAsync();

            // Act - Execute two statements
            using var connection = CreateProxiedConnection();

            // Disable cached results to force separate CloseOperation calls (slow path)
            using (var disableCache = connection.CreateStatement())
            {
                disableCache.SqlQuery = "SET use_cached_result = false";
                disableCache.ExecuteUpdate();
            }

            using (var statement1 = connection.CreateStatement())
            {
                statement1.SqlQuery = "SELECT 1";
                var result1 = statement1.ExecuteQuery();
                using var reader1 = result1.Stream;
                reader1.ReadNextRecordBatchAsync().AsTask().Wait();
            }

            using (var statement2 = connection.CreateStatement())
            {
                statement2.SqlQuery = "SELECT 2";
                var result2 = statement2.ExecuteQuery();
                using var reader2 = result2.Stream;
                reader2.ReadNextRecordBatchAsync().AsTask().Wait();
            }

            // Re-enable cached results
            using (var enableCache = connection.CreateStatement())
            {
                enableCache.SqlQuery = "SET use_cached_result = true";
                enableCache.ExecuteUpdate();
            }

            // Give time for async disposal to complete
            await Task.Delay(500);

            // Assert - Verify ExecuteStatement count
            var executeCount = await ControlClient.VerifyThriftCallsAsync(
                type: "method_count",
                method: "ExecuteStatement",
                count: 4); // 2 SETs + 2 queries

            Assert.True(executeCount?.Verified == true,
                $"Expected 4 ExecuteStatement calls (2 SETs + 2 queries). " +
                $"Actual: ExecuteStatement={executeCount?.ActualCount}");

            // If GetOperationStatus was called (slow path), verify CloseOperation count matches
            var callHistory = await ControlClient.GetThriftCallsAsync();
            int getOpCount = 0;
            int closeOpCount = 0;

            if (callHistory?.Calls != null)
            {
                foreach (var call in callHistory.Calls)
                {
                    if (call.Method == "GetOperationStatus") getOpCount++;
                    if (call.Method == "CloseOperation") closeOpCount++;
                }
            }

            if (getOpCount > 0)
            {
                Assert.Equal(getOpCount, closeOpCount);
            }
        }
    }
}
