/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Generic;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Arrow.Adbc.Drivers.Databricks.StatementExecution;
using Apache.Arrow.Adbc.Tests.Drivers.Apache.Common;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.E2E.StatementExecution
{
    /// <summary>
    /// E2E tests for StatementExecutionConnection using the full driver stack.
    /// These tests require a real Databricks endpoint.
    /// Set DATABRICKS_TEST_CONFIG_FILE and USE_REAL_STATEMENT_EXECUTION_ENDPOINT=true to run.
    /// </summary>
    public class StatementExecutionConnectionE2ETests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public StatementExecutionConnectionE2ETests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
        }

        private Dictionary<string, string> GetConnectionProperties(bool enableSessionManagement = true)
        {
            return DatabricksTestHelpers.GetPropertiesWithStatementExecutionEnabled(
                TestEnvironment, TestConfiguration);
        }

        [SkippableFact]
        public void ConnectionLifecycle_OpenAndClose_CreatesAndDeletesSession()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            Skip.IfNot(Environment.GetEnvironmentVariable("USE_REAL_STATEMENT_EXECUTION_ENDPOINT") == "true");

            var properties = GetConnectionProperties(enableSessionManagement: true);
            using var driver = NewDriver;
            using var database = driver.Open(properties);
            using var connection = database.Connect(properties) as StatementExecutionConnection;

            Assert.NotNull(connection);
            Assert.NotNull(connection.SessionId);
            Assert.False(string.IsNullOrEmpty(connection.SessionId));
        }

        [SkippableFact]
        public void ConnectionLifecycle_WithCatalogAndSchema_PassesToSession()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            Skip.IfNot(Environment.GetEnvironmentVariable("USE_REAL_STATEMENT_EXECUTION_ENDPOINT") == "true");

            var properties = GetConnectionProperties(enableSessionManagement: true);
            properties[AdbcOptions.Connection.CurrentCatalog] = "main";
            properties[AdbcOptions.Connection.CurrentDbSchema] = "default";

            using var driver = NewDriver;
            using var database = driver.Open(properties);
            using var connection = database.Connect(properties) as StatementExecutionConnection;

            Assert.NotNull(connection);
            Assert.NotNull(connection.SessionId);
        }

        [SkippableFact]
        public void ConnectionLifecycle_SessionManagementDisabled_DoesNotCreateSession()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            Skip.IfNot(Environment.GetEnvironmentVariable("USE_REAL_STATEMENT_EXECUTION_ENDPOINT") == "true");

            var properties = GetConnectionProperties(enableSessionManagement: false);
            properties[DatabricksParameters.EnableSessionManagement] = "false";

            using var driver = NewDriver;
            using var database = driver.Open(properties);
            using var connection = database.Connect(properties) as StatementExecutionConnection;

            Assert.NotNull(connection);
            Assert.Null(connection.SessionId);
        }

        [SkippableFact]
        public void CreateStatement_ReturnsStatementExecutionStatement()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            Skip.IfNot(Environment.GetEnvironmentVariable("USE_REAL_STATEMENT_EXECUTION_ENDPOINT") == "true");

            var properties = GetConnectionProperties(enableSessionManagement: true);
            using var driver = NewDriver;
            using var database = driver.Open(properties);
            using var connection = database.Connect(properties);

            var statement = connection.CreateStatement();

            Assert.NotNull(statement);
            Assert.IsType<StatementExecutionStatement>(statement);
        }

        [SkippableFact]
        public void ExecuteQuery_SimpleQuery_ReturnsResults()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            Skip.IfNot(Environment.GetEnvironmentVariable("USE_REAL_STATEMENT_EXECUTION_ENDPOINT") == "true");

            var properties = GetConnectionProperties(enableSessionManagement: true);

            using var driver = NewDriver;
            using var database = driver.Open(properties);
            using var connection = database.Connect(properties);
            using var statement = connection.CreateStatement();

            statement.SqlQuery = "SELECT 1 as col1, 'test' as col2";
            var result = statement.ExecuteQuery();

            Assert.NotNull(result.Stream);
            var batch = result.Stream.ReadNextRecordBatchAsync().Result;
            Assert.NotNull(batch);
            Assert.Equal(2, batch.ColumnCount);
            Assert.Equal(1, batch.Length);
        }

        [SkippableFact]
        public void ExecuteUpdate_DDLStatement_Succeeds()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            Skip.IfNot(Environment.GetEnvironmentVariable("USE_REAL_STATEMENT_EXECUTION_ENDPOINT") == "true");

            var properties = GetConnectionProperties(enableSessionManagement: true);
            using var driver = NewDriver;
            using var database = driver.Open(properties);
            using var connection = database.Connect(properties);

            // Create table
            using (var statement = connection.CreateStatement())
            {
                statement.SqlQuery = @"
                    CREATE OR REPLACE TABLE test_e2e_connection_table (
                        id INT,
                        name STRING
                    )";
                var result = statement.ExecuteUpdate();
                Assert.True(result.AffectedRows >= 0);
            }

            // Drop table
            using (var statement = connection.CreateStatement())
            {
                statement.SqlQuery = "DROP TABLE test_e2e_connection_table";
                var result = statement.ExecuteUpdate();
                Assert.True(result.AffectedRows >= 0);
            }
        }
    }
}
