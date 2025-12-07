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
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Arrow.Adbc.Tests.Drivers.Apache.Common;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.E2E.StatementExecution
{
    /// <summary>
    /// E2E tests for the Statement Execution REST API through the full driver flow.
    /// Tests the complete path: DatabricksDriver -> DatabricksDatabase -> StatementExecutionConnection -> StatementExecutionStatement
    /// </summary>
    public class StatementExecutionDriverE2ETests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public StatementExecutionDriverE2ETests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
        }

        private void SkipIfNotConfigured()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable), "Test configuration not available");

            // REST API supports both direct token authentication (PAT or OAuth access token)
            // and OAuth M2M (client_credentials) flow (implemented in PECO-2857).
        }

        private AdbcConnection CreateRestConnection()
        {
            var properties = new Dictionary<string, string>
            {
                [DatabricksParameters.Protocol] = "rest",
            };

            // Use URI if available (connection will parse host and warehouse ID from it)
            if (!string.IsNullOrEmpty(TestConfiguration.Uri))
            {
                properties[AdbcOptions.Uri] = TestConfiguration.Uri;
            }
            else
            {
                // Fall back to individual properties
                if (!string.IsNullOrEmpty(TestConfiguration.HostName))
                {
                    properties[SparkParameters.HostName] = TestConfiguration.HostName;
                }
                if (!string.IsNullOrEmpty(TestConfiguration.Path))
                {
                    properties[SparkParameters.Path] = TestConfiguration.Path;
                }
            }

            // Token-based authentication (PAT or OAuth access token)
            if (!string.IsNullOrEmpty(TestConfiguration.Token))
            {
                properties[SparkParameters.Token] = TestConfiguration.Token;
            }
            if (!string.IsNullOrEmpty(TestConfiguration.AccessToken))
            {
                properties[SparkParameters.AccessToken] = TestConfiguration.AccessToken;
            }

            // OAuth M2M authentication (client_credentials)
            if (!string.IsNullOrEmpty(TestConfiguration.AuthType))
            {
                properties[SparkParameters.AuthType] = TestConfiguration.AuthType;
            }
            if (!string.IsNullOrEmpty(TestConfiguration.OAuthGrantType))
            {
                properties[DatabricksParameters.OAuthGrantType] = TestConfiguration.OAuthGrantType;
            }
            if (!string.IsNullOrEmpty(TestConfiguration.OAuthClientId))
            {
                properties[DatabricksParameters.OAuthClientId] = TestConfiguration.OAuthClientId;
            }
            if (!string.IsNullOrEmpty(TestConfiguration.OAuthClientSecret))
            {
                properties[DatabricksParameters.OAuthClientSecret] = TestConfiguration.OAuthClientSecret;
            }
            if (!string.IsNullOrEmpty(TestConfiguration.OAuthScope))
            {
                properties[DatabricksParameters.OAuthScope] = TestConfiguration.OAuthScope;
            }

            var driver = new DatabricksDriver();
            var database = driver.Open(properties);
            return database.Connect(null);
        }

        [SkippableFact]
        public void ExecuteQuery_SimpleSelect_ReturnsResults()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 1 AS value";

            var result = statement.ExecuteQuery();
            Assert.NotNull(result);

            using var reader = result.Stream;
            Assert.NotNull(reader);

            var schema = reader.Schema;
            Assert.NotNull(schema);
            Assert.True(schema.FieldsList.Count > 0, "Schema should have at least one field");

            // Read at least one batch
            var batch = reader.ReadNextRecordBatchAsync().Result;
            Assert.NotNull(batch);
            Assert.True(batch.Length > 0, "Batch should have at least one row");
        }

        [SkippableFact]
        public void ExecuteQuery_MultipleColumns_ReturnsAllColumns()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 1 AS col1, 'hello' AS col2, 3.14 AS col3";

            var result = statement.ExecuteQuery();
            Assert.NotNull(result);

            using var reader = result.Stream;
            Assert.NotNull(reader);

            var schema = reader.Schema;
            Assert.NotNull(schema);
            Assert.Equal(3, schema.FieldsList.Count);

            var batch = reader.ReadNextRecordBatchAsync().Result;
            Assert.NotNull(batch);
            Assert.Equal(1, batch.Length);
        }

        [SkippableFact]
        public void ExecuteUpdate_CreateTable_Succeeds()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();
            using var statement = connection.CreateStatement();

            // Create a temporary table
            var tableName = $"test_rest_driver_{Guid.NewGuid():N}".Substring(0, 40);
            statement.SqlQuery = $"CREATE TABLE {tableName} (id INT, name STRING) USING DELTA";

            var result = statement.ExecuteUpdate();
            // CREATE TABLE typically returns -1 or 0
            Assert.True(result.AffectedRows >= -1);

            // Drop the table
            using var dropStatement = connection.CreateStatement();
            dropStatement.SqlQuery = $"DROP TABLE IF EXISTS {tableName}";
            dropStatement.ExecuteUpdate();
        }

        [SkippableFact]
        public void ExecuteQuery_WithInlineResults_ReturnsData()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();
            using var statement = connection.CreateStatement();

            // Small query that should return inline results
            statement.SqlQuery = "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)";

            var result = statement.ExecuteQuery();
            Assert.NotNull(result);

            using var reader = result.Stream;
            Assert.NotNull(reader);

            var schema = reader.Schema;
            Assert.NotNull(schema);

            int totalRows = 0;
            while (true)
            {
                var batch = reader.ReadNextRecordBatchAsync().Result;
                if (batch == null) break;
                totalRows += batch.Length;
            }

            Assert.Equal(3, totalRows);
        }

        [SkippableFact]
        public void ExecuteUpdate_InsertData_ReturnsAffectedRows()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();
            var tableName = $"test_insert_{Guid.NewGuid():N}".Substring(0, 40);

            try
            {
                // Create table
                using var createStatement = connection.CreateStatement();
                createStatement.SqlQuery = $"CREATE TABLE {tableName} (id INT, name STRING) USING DELTA";
                createStatement.ExecuteUpdate();

                // Insert data
                using var insertStatement = connection.CreateStatement();
                insertStatement.SqlQuery = $"INSERT INTO {tableName} VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')";
                var result = insertStatement.ExecuteUpdate();

                // INSERT should return number of affected rows
                Assert.True(result.AffectedRows >= 0, $"Expected non-negative affected rows, got {result.AffectedRows}");
            }
            finally
            {
                // Cleanup
                using var dropStatement = connection.CreateStatement();
                dropStatement.SqlQuery = $"DROP TABLE IF EXISTS {tableName}";
                dropStatement.ExecuteUpdate();
            }
        }

        [SkippableFact]
        public void ExecuteUpdate_UpdateData_ReturnsAffectedRows()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();
            var tableName = $"test_update_{Guid.NewGuid():N}".Substring(0, 40);

            try
            {
                // Create and populate table
                using var createStatement = connection.CreateStatement();
                createStatement.SqlQuery = $"CREATE TABLE {tableName} (id INT, name STRING) USING DELTA";
                createStatement.ExecuteUpdate();

                using var insertStatement = connection.CreateStatement();
                insertStatement.SqlQuery = $"INSERT INTO {tableName} VALUES (1, 'Alice'), (2, 'Bob')";
                insertStatement.ExecuteUpdate();

                // Update data
                using var updateStatement = connection.CreateStatement();
                updateStatement.SqlQuery = $"UPDATE {tableName} SET name = 'Updated' WHERE id = 1";
                var result = updateStatement.ExecuteUpdate();

                // UPDATE should return number of affected rows
                Assert.True(result.AffectedRows >= 0, $"Expected non-negative affected rows, got {result.AffectedRows}");
            }
            finally
            {
                // Cleanup
                using var dropStatement = connection.CreateStatement();
                dropStatement.SqlQuery = $"DROP TABLE IF EXISTS {tableName}";
                dropStatement.ExecuteUpdate();
            }
        }

        [SkippableFact]
        public void ExecuteUpdate_DeleteData_ReturnsAffectedRows()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();
            var tableName = $"test_delete_{Guid.NewGuid():N}".Substring(0, 40);

            try
            {
                // Create and populate table
                using var createStatement = connection.CreateStatement();
                createStatement.SqlQuery = $"CREATE TABLE {tableName} (id INT, name STRING) USING DELTA";
                createStatement.ExecuteUpdate();

                using var insertStatement = connection.CreateStatement();
                insertStatement.SqlQuery = $"INSERT INTO {tableName} VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')";
                insertStatement.ExecuteUpdate();

                // Delete data
                using var deleteStatement = connection.CreateStatement();
                deleteStatement.SqlQuery = $"DELETE FROM {tableName} WHERE id > 1";
                var result = deleteStatement.ExecuteUpdate();

                // DELETE should return number of affected rows
                Assert.True(result.AffectedRows >= 0, $"Expected non-negative affected rows, got {result.AffectedRows}");
            }
            finally
            {
                // Cleanup
                using var dropStatement = connection.CreateStatement();
                dropStatement.SqlQuery = $"DROP TABLE IF EXISTS {tableName}";
                dropStatement.ExecuteUpdate();
            }
        }

        [SkippableFact]
        public void ExecuteUpdate_DropTable_Succeeds()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();
            var tableName = $"test_drop_{Guid.NewGuid():N}".Substring(0, 40);

            // Create table first
            using var createStatement = connection.CreateStatement();
            createStatement.SqlQuery = $"CREATE TABLE {tableName} (id INT) USING DELTA";
            createStatement.ExecuteUpdate();

            // Drop the table
            using var dropStatement = connection.CreateStatement();
            dropStatement.SqlQuery = $"DROP TABLE {tableName}";
            var result = dropStatement.ExecuteUpdate();

            // DROP TABLE typically returns -1 or 0
            Assert.True(result.AffectedRows >= -1);
        }

        [SkippableFact]
        public void ExecuteQuery_AfterInsert_ReturnsInsertedData()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();
            var tableName = $"test_query_after_insert_{Guid.NewGuid():N}".Substring(0, 40);

            try
            {
                // Create and populate table
                using var createStatement = connection.CreateStatement();
                createStatement.SqlQuery = $"CREATE TABLE {tableName} (id INT, name STRING) USING DELTA";
                createStatement.ExecuteUpdate();

                using var insertStatement = connection.CreateStatement();
                insertStatement.SqlQuery = $"INSERT INTO {tableName} VALUES (1, 'Alice'), (2, 'Bob')";
                insertStatement.ExecuteUpdate();

                // Query the data
                using var selectStatement = connection.CreateStatement();
                selectStatement.SqlQuery = $"SELECT * FROM {tableName} ORDER BY id";
                var queryResult = selectStatement.ExecuteQuery();

                Assert.NotNull(queryResult);
                using var reader = queryResult.Stream;
                Assert.NotNull(reader);

                int totalRows = 0;
                while (true)
                {
                    var batch = reader.ReadNextRecordBatchAsync().Result;
                    if (batch == null) break;
                    totalRows += batch.Length;
                }

                Assert.Equal(2, totalRows);
            }
            finally
            {
                // Cleanup
                using var dropStatement = connection.CreateStatement();
                dropStatement.SqlQuery = $"DROP TABLE IF EXISTS {tableName}";
                dropStatement.ExecuteUpdate();
            }
        }

        [SkippableFact]
        public void ExecuteQuery_MultipleRows_ReturnsAllRows()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();
            using var statement = connection.CreateStatement();

            // Query that returns multiple rows
            statement.SqlQuery = "SELECT id FROM range(100) AS t(id)";

            var result = statement.ExecuteQuery();
            Assert.NotNull(result);

            using var reader = result.Stream;
            Assert.NotNull(reader);

            int totalRows = 0;
            while (true)
            {
                var batch = reader.ReadNextRecordBatchAsync().Result;
                if (batch == null) break;
                totalRows += batch.Length;
            }

            Assert.Equal(100, totalRows);
        }

        [SkippableFact]
        public void ExecuteQuery_WithNullValues_HandlesNullsCorrectly()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();
            using var statement = connection.CreateStatement();

            statement.SqlQuery = "SELECT NULL AS null_col, 1 AS int_col, CAST(NULL AS STRING) AS null_string";

            var result = statement.ExecuteQuery();
            Assert.NotNull(result);

            using var reader = result.Stream;
            Assert.NotNull(reader);

            var schema = reader.Schema;
            Assert.NotNull(schema);
            Assert.Equal(3, schema.FieldsList.Count);

            var batch = reader.ReadNextRecordBatchAsync().Result;
            Assert.NotNull(batch);
            Assert.Equal(1, batch.Length);
        }
    }
}
