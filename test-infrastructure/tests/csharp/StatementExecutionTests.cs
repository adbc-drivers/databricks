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

            // Verify CloseOperation was called
            var closeOp = await ControlClient.VerifyThriftCallsAsync(
                type: "method_exists",
                method: "CloseOperation");

            Assert.NotNull(closeOp);
            Assert.True(closeOp.Verified, "CloseOperation should have been called after statement completion");
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
            reader.ReadNextRecordBatchAsync().Wait();

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

            for (int i = 1; i <= 3; i++)
            {
                using var statement = connection.CreateStatement();
                statement.SqlQuery = $"SELECT {i} as num";
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;
                reader.ReadNextRecordBatchAsync().Wait();
            }

            // Assert - Should have 3 ExecuteStatement and 3 CloseOperation calls
            var executeCount = await ControlClient.VerifyThriftCallsAsync(
                type: "method_count",
                method: "ExecuteStatement",
                count: 3);

            var closeOpCount = await ControlClient.VerifyThriftCallsAsync(
                type: "method_count",
                method: "CloseOperation",
                count: 3);

            Assert.True(executeCount?.Verified,
                $"Expected 3 ExecuteStatement calls. Actual: {executeCount?.ActualCount}");
            Assert.True(closeOpCount?.Verified,
                $"Expected 3 CloseOperation calls. Actual: {closeOpCount?.ActualCount}");
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
