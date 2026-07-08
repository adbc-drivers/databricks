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
using System.Diagnostics;
using System.Threading.Tasks;
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.StatementExecution
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

        private AdbcConnection CreateRestConnection(IReadOnlyDictionary<string, string>? extraProperties = null)
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

            // Test-specific overrides take precedence over TestConfiguration defaults.
            if (extraProperties != null)
            {
                foreach (var kv in extraProperties)
                {
                    properties[kv.Key] = kv.Value;
                }
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

        // PECO-3064: adbc.apache.statement.polltime_ms is the single key driving SEA's polling
        // cadence (consolidated with the Thrift path). The polling cadence dominates wall-clock
        // for async queries (wait_timeout=0): the first GetStatement happens only after
        // Task.Delay(_pollingIntervalMs), so an unusually large interval is directly observable.
        //
        //  - wait_timeout=0 forces async-then-poll (no server-side block).
        //  - enable_direct_results=false ensures wait_timeout isn't overridden by the connection.
        //  - polltime_ms=2000 → total wall-clock >= 1800ms.
        //  - If the SEA path were to ignore polltime_ms and fall back to the 1000ms default,
        //    wall-clock would be ~1100ms and this test fails.
        [SkippableFact]
        public void ExecuteQuery_PollTimeMs_DrivesSeaPollingCadence()
        {
            SkipIfNotConfigured();

            const int slowPollMs = 2000;
            var extra = new Dictionary<string, string>
            {
                [ApacheParameters.PollTimeMilliseconds] = slowPollMs.ToString(),
                [DatabricksParameters.WaitTimeout] = "0",
                [DatabricksParameters.EnableDirectResults] = "false",
            };

            using var connection = CreateRestConnection(extra);
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 1 AS value";

            var sw = Stopwatch.StartNew();
            var result = statement.ExecuteQuery();
            using var reader = result.Stream;
            while (reader.ReadNextRecordBatchAsync().Result != null) { }
            sw.Stop();

            Assert.True(
                sw.ElapsedMilliseconds >= 1800,
                $"Expected polltime_ms={slowPollMs} to drive SEA polling cadence, but query " +
                $"completed in {sw.ElapsedMilliseconds}ms (expected >= 1800ms). " +
                $"This indicates polltime_ms is not wired into SEA's polling interval.");
        }

        // Issue #525: the `%` SQL-LIKE wildcard must mean "match all catalogs" on the SEA
        // path, exactly as Thrift treats it. Previously the SEA metadata path passed the
        // catalog argument through as a literal backtick-quoted identifier, so
        // GetSchemas(catalog="%") generated `SHOW SCHEMAS IN `%`` which matches no catalog
        // and returns an empty result, while Thrift enumerates every catalog. This asserts
        // the SEA path now expands `%` to all catalogs and echoes the server's matched
        // catalog name (e.g. "main"), not the literal "%" argument.
        [SkippableFact]
        public async Task GetSchemas_CatalogPercentWildcard_ExpandsToAllCatalogs()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();
            using var statement = connection.CreateStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SetOption(ApacheParameters.CatalogName, "%");
            statement.SqlQuery = "GetSchemas";

            var result = statement.ExecuteQuery();
            using var reader = result.Stream;
            Assert.NotNull(reader);

            int catalogColIndex = -1;
            for (int j = 0; j < reader!.Schema.FieldsList.Count; j++)
            {
                var name = reader.Schema.GetFieldByIndex(j).Name;
                if (name.Equals("TABLE_CATALOG", StringComparison.OrdinalIgnoreCase) ||
                    name.Equals("TABLE_CAT", StringComparison.OrdinalIgnoreCase))
                {
                    catalogColIndex = j;
                    break;
                }
            }
            Assert.True(catalogColIndex >= 0, "GetSchemas result must contain a catalog column");

            var catalogValues = new HashSet<string>(StringComparer.Ordinal);
            while (true)
            {
                using var batch = await reader.ReadNextRecordBatchAsync();
                if (batch == null) break;
                var catalogArray = (Apache.Arrow.StringArray)batch.Column(catalogColIndex);
                for (int i = 0; i < batch.Length; i++)
                {
                    if (!catalogArray.IsNull(i))
                        catalogValues.Add(catalogArray.GetString(i));
                }
            }

            // `%` must expand to all catalogs, so we must get schemas from at least the
            // "main" catalog, and the echoed catalog name must be the server's matched
            // name ("main"), never the literal wildcard "%".
            Assert.Contains("main", catalogValues);
            Assert.DoesNotContain("%", catalogValues);
        }
    }
}
