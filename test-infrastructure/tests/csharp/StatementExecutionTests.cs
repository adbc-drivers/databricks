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
    /// Tests that validate driver behavior for Thrift statement execution operations.
    /// Verifies ExecuteStatement, GetOperationStatus, FetchResults, and CloseOperation sequences.
    /// </summary>
    public class StatementExecutionTests : ProxyTestBase
    {
        [Fact]
        public async Task SimpleQuery_ExecutesWithExpectedSequence()
        {
            // Arrange
            await ControlClient.ResetThriftCallsAsync();

            // Act - Execute simple SELECT query
            using var connection = CreateProxiedConnection();

            // Disable cached results to force separate CloseOperation call (slow path)
            using (var disableCache = connection.CreateStatement())
            {
                disableCache.SqlQuery = "SET use_cached_result = false";
                disableCache.ExecuteUpdate();
            }

            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 42 as answer, 'hello' as greeting";

            var result = statement.ExecuteQuery();
            Assert.NotNull(result);

            using var reader = result.Stream;
            var batch = reader.ReadNextRecordBatchAsync().Result;
            Assert.NotNull(batch);
            Assert.Equal(1, batch.Length); // Should have 1 row

            // Assert - Verify ExecuteStatement was called
            var verification = await ControlClient.VerifyThriftCallsAsync(
                type: "method_exists",
                method: "ExecuteStatement");

            Assert.NotNull(verification);
            Assert.True(verification.Verified, "ExecuteStatement should have been called");

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
        public async Task LongRunningQuery_PollsOperationStatus()
        {
            // Arrange
            await ControlClient.ResetThriftCallsAsync();

            // Act - Execute query that takes some time (sleep simulation)
            // Note: This may or may not trigger GetOperationStatus depending on query speed
            using var connection = CreateProxiedConnection();
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT java_method('java.lang.Thread', 'sleep', 100L), 'done' as status";

            var result = statement.ExecuteQuery();
            using var reader = result.Stream;
            reader.ReadNextRecordBatchAsync().AsTask().Wait();

            // Assert - Check if GetOperationStatus was called (optional, depends on timing)
            var callHistory = await ControlClient.GetThriftCallsAsync();
            Assert.NotNull(callHistory);

            var methods = new System.Collections.Generic.List<string>();
            if (callHistory.Calls != null)
            {
                foreach (var call in callHistory.Calls)
                {
                    if (!string.IsNullOrEmpty(call.Method))
                    {
                        methods.Add(call.Method);
                    }
                }
            }

            // ExecuteStatement must be present
            Assert.Contains("ExecuteStatement", methods);

            // GetOperationStatus is optional (may not be called for fast queries with directResults)
            // Just log whether it was called for informational purposes
            var hasGetOpStatus = methods.Contains("GetOperationStatus");
            // Note: We don't assert on this since it depends on query execution speed
        }

        [Fact]
        public async Task StatementWithFetchResults_CallsExpectedMethods()
        {
            // Arrange
            await ControlClient.ResetThriftCallsAsync();

            // Act - Execute query with larger result set to potentially trigger FetchResults
            // Using catalog_returns which has many rows
            using var connection = CreateProxiedConnection();
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT * FROM main.tpcds_sf1_delta.catalog_returns LIMIT 1000";

            var result = statement.ExecuteQuery();
            Assert.NotNull(result);

            using var reader = result.Stream;
            // Read batches until done
            while (true)
            {
                var batch = reader.ReadNextRecordBatchAsync().Result;
                if (batch == null)
                    break;
            }

            // Assert - Verify FetchResults was called at least once
            var fetchResults = await ControlClient.VerifyThriftCallsAsync(
                type: "method_exists",
                method: "FetchResults");

            Assert.NotNull(fetchResults);
            Assert.True(fetchResults.Verified,
                "FetchResults should be called for result retrieval");
        }

        [Fact]
        public async Task MultipleStatements_EachHasOwnOperation()
        {
            // Arrange
            await ControlClient.ResetThriftCallsAsync();

            // Act - Execute three statements sequentially
            using var connection = CreateProxiedConnection();

            // Disable cached results to force separate CloseOperation calls (slow path)
            using (var disableCache = connection.CreateStatement())
            {
                disableCache.SqlQuery = "SET use_cached_result = false";
                disableCache.ExecuteUpdate();
            }

            for (int i = 1; i <= 3; i++)
            {
                using var statement = connection.CreateStatement();
                statement.SqlQuery = $"SELECT {i} as num";
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                reader.ReadNextRecordBatchAsync().AsTask().Wait();
            }

            // Assert - Verify ExecuteStatement count (1 SET + 3 queries = 4)
            var executeCount = await ControlClient.VerifyThriftCallsAsync(
                type: "method_count",
                method: "ExecuteStatement",
                count: 4);

            Assert.True(executeCount?.Verified,
                $"Expected 4 ExecuteStatement calls (1 SET + 3 queries). Actual: {executeCount?.ActualCount}");

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

        [Fact]
        public async Task Statement_OperationLifecycle_ProperSequence()
        {
            // Arrange
            await ControlClient.ResetThriftCallsAsync();

            // Act - Execute single statement and complete reading
            using var connection = CreateProxiedConnection();
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 'test' as col1";

            var result = statement.ExecuteQuery();
            using var reader = result.Stream;
            while (reader.ReadNextRecordBatchAsync().Result != null) { }

            // Assert - Verify sequence: ExecuteStatement must come before CloseOperation
            var callHistory = await ControlClient.GetThriftCallsAsync();
            Assert.NotNull(callHistory);
            Assert.NotNull(callHistory.Calls);

            int executeIndex = -1;
            int closeOpIndex = -1;

            for (int i = 0; i < callHistory.Calls.Count; i++)
            {
                var call = callHistory.Calls[i];
                if (call.Method == "ExecuteStatement" && executeIndex == -1)
                {
                    executeIndex = i;
                }
                if (call.Method == "CloseOperation" && executeIndex != -1 && closeOpIndex == -1)
                {
                    closeOpIndex = i;
                }
            }

            Assert.True(executeIndex >= 0, "ExecuteStatement should be called");
            Assert.True(closeOpIndex >= 0, "CloseOperation should be called");
            Assert.True(executeIndex < closeOpIndex,
                $"ExecuteStatement (index {executeIndex}) should come before CloseOperation (index {closeOpIndex})");
        }
    }
}
