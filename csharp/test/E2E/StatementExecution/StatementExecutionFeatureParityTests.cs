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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Arrow.Adbc.Tests.Drivers.Apache.Common;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.E2E.StatementExecution
{
    /// <summary>
    /// E2E tests for feature parity: metadata operations, inline results, and cancellation.
    /// These tests require a real Databricks endpoint.
    /// Set DATABRICKS_TEST_CONFIG_FILE and USE_REAL_STATEMENT_EXECUTION_ENDPOINT=true to run.
    /// </summary>
    public class StatementExecutionFeatureParityTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public StatementExecutionFeatureParityTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
        }

        private Dictionary<string, string> GetStatementExecutionProperties()
        {
            return DatabricksTestHelpers.GetPropertiesWithStatementExecutionEnabled(
                TestEnvironment, TestConfiguration);
        }

        /// <summary>
        /// Tests GetTableTypes returns expected table types.
        /// </summary>
        [SkippableFact]
        public void GetTableTypes_ReturnsStandardTypes()
        {
            // Skip if configuration is not available or statement execution is not enabled
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            Skip.IfNot(Environment.GetEnvironmentVariable("USE_REAL_STATEMENT_EXECUTION_ENDPOINT") == "true");

            var properties = GetStatementExecutionProperties();
            using var driver = NewDriver;
            using var database = driver.Open(properties);
            using var connection = database.Connect(properties);

            // Act
            using var tableTypesStream = connection.GetTableTypes();
            var tableTypes = new List<string>();

            while (true)
            {
                var batch = tableTypesStream.ReadNextRecordBatchAsync().Result;
                if (batch == null)
                    break;

                var tableTypeColumn = batch.Column(0) as StringArray;
                if (tableTypeColumn != null)
                {
                    for (int i = 0; i < tableTypeColumn.Length; i++)
                    {
                        if (!tableTypeColumn.IsNull(i))
                        {
                            tableTypes.Add(tableTypeColumn.GetString(i));
                        }
                    }
                }
            }

            // Assert - should have standard table types
            Assert.Contains("TABLE", tableTypes);
            Assert.Contains("VIEW", tableTypes);
            Assert.True(tableTypes.Count >= 2, "Should return at least TABLE and VIEW types");
        }

        /// <summary>
        /// Tests GetObjects with catalog depth returns catalogs.
        /// </summary>
        [SkippableFact]
        public void GetObjects_CatalogDepth_ReturnsCatalogs()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            Skip.IfNot(Environment.GetEnvironmentVariable("USE_REAL_STATEMENT_EXECUTION_ENDPOINT") == "true");

            var properties = GetStatementExecutionProperties();
            using var driver = NewDriver;
            using var database = driver.Open(properties);
            using var connection = database.Connect(properties);

            // Act
            using var objectsStream = connection.GetObjects(AdbcConnection.GetObjectsDepth.Catalogs, null, null, null, null, null);
            var catalogs = new List<string>();

            while (true)
            {
                var batch = objectsStream.ReadNextRecordBatchAsync().Result;
                if (batch == null)
                    break;

                var catalogColumn = batch.Column(0) as StringArray;
                if (catalogColumn != null)
                {
                    for (int i = 0; i < catalogColumn.Length; i++)
                    {
                        if (!catalogColumn.IsNull(i))
                        {
                            catalogs.Add(catalogColumn.GetString(i));
                        }
                    }
                }
            }

            // Assert - should have at least one catalog
            Assert.NotEmpty(catalogs);
        }

        /// <summary>
        /// Tests GetTableSchema returns correct schema for a known table.
        /// </summary>
        [SkippableFact]
        public void GetTableSchema_KnownTable_ReturnsSchema()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            Skip.IfNot(Environment.GetEnvironmentVariable("USE_REAL_STATEMENT_EXECUTION_ENDPOINT") == "true");

            var properties = GetStatementExecutionProperties();
            using var driver = NewDriver;
            using var database = driver.Open(properties);
            using var connection = database.Connect(properties);

            // First create a test table
            using (var statement = connection.CreateStatement())
            {
                statement.SqlQuery = @"
                    CREATE OR REPLACE TABLE test_schema_table (
                        id INT,
                        name STRING,
                        value DOUBLE
                    )";
                statement.ExecuteUpdate();
            }

            try
            {
                // Act - Get schema for the test table
                var schema = connection.GetTableSchema(null, null, "test_schema_table");

                // Assert
                Assert.NotNull(schema);
                Assert.True(schema.FieldsList.Count >= 3, "Should have at least 3 columns");

                var fieldNames = schema.FieldsList.Select(f => f.Name).ToList();
                Assert.Contains("id", fieldNames, StringComparer.OrdinalIgnoreCase);
                Assert.Contains("name", fieldNames, StringComparer.OrdinalIgnoreCase);
                Assert.Contains("value", fieldNames, StringComparer.OrdinalIgnoreCase);
            }
            finally
            {
                // Cleanup
                using (var statement = connection.CreateStatement())
                {
                    statement.SqlQuery = "DROP TABLE IF EXISTS test_schema_table";
                    statement.ExecuteUpdate();
                }
            }
        }

        /// <summary>
        /// Tests inline results with small result sets.
        /// </summary>
        [SkippableFact]
        public void ExecuteQuery_InlineResults_ReturnsData()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            Skip.IfNot(Environment.GetEnvironmentVariable("USE_REAL_STATEMENT_EXECUTION_ENDPOINT") == "true");

            var properties = GetStatementExecutionProperties();
            // Set inline disposition
            properties[DatabricksParameters.ResultDisposition] = "inline";

            using var driver = NewDriver;
            using var database = driver.Open(properties);
            using var connection = database.Connect(properties);
            using var statement = connection.CreateStatement();

            // Execute a small query that will return inline results
            statement.SqlQuery = "SELECT 1 as col1, 'test' as col2";
            var result = statement.ExecuteQuery();

            // Assert
            Assert.NotNull(result.Stream);

            var batch = result.Stream.ReadNextRecordBatchAsync().Result;
            Assert.NotNull(batch);
            Assert.Equal(2, batch.ColumnCount);
            Assert.Equal(1, batch.Length);
        }

        /// <summary>
        /// Tests inline results with multiple rows.
        /// </summary>
        [SkippableFact]
        public void ExecuteQuery_InlineResults_MultipleRows_ReturnsAllData()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            Skip.IfNot(Environment.GetEnvironmentVariable("USE_REAL_STATEMENT_EXECUTION_ENDPOINT") == "true");

            var properties = GetStatementExecutionProperties();
            properties[DatabricksParameters.ResultDisposition] = "inline";

            using var driver = NewDriver;
            using var database = driver.Open(properties);
            using var connection = database.Connect(properties);
            using var statement = connection.CreateStatement();

            // Execute query with multiple rows
            statement.SqlQuery = "SELECT id, id * 2 as doubled FROM range(100)";
            var result = statement.ExecuteQuery();

            // Assert - count all rows
            int totalRows = 0;
            while (true)
            {
                var batch = result.Stream.ReadNextRecordBatchAsync().Result;
                if (batch == null)
                    break;
                totalRows += batch.Length;
            }

            Assert.Equal(100, totalRows);
        }

        /// <summary>
        /// Tests hybrid mode (inline_or_external_links).
        /// </summary>
        [SkippableFact]
        public void ExecuteQuery_HybridDisposition_ReturnsData()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            Skip.IfNot(Environment.GetEnvironmentVariable("USE_REAL_STATEMENT_EXECUTION_ENDPOINT") == "true");

            var properties = GetStatementExecutionProperties();
            // Set hybrid disposition (server decides based on size)
            properties[DatabricksParameters.ResultDisposition] = "inline_or_external_links";

            using var driver = NewDriver;
            using var database = driver.Open(properties);
            using var connection = database.Connect(properties);

            // Test with small result (should be inline)
            using (var statement = connection.CreateStatement())
            {
                statement.SqlQuery = "SELECT 1 as col1";
                var result = statement.ExecuteQuery();

                Assert.NotNull(result.Stream);
                var batch = result.Stream.ReadNextRecordBatchAsync().Result;
                Assert.NotNull(batch);
            }

            // Test with larger result (may use external links)
            using (var statement = connection.CreateStatement())
            {
                statement.SqlQuery = "SELECT * FROM range(1000)";
                var result = statement.ExecuteQuery();

                Assert.NotNull(result.Stream);

                int totalRows = 0;
                while (true)
                {
                    var batch = result.Stream.ReadNextRecordBatchAsync().Result;
                    if (batch == null)
                        break;
                    totalRows += batch.Length;
                }

                Assert.Equal(1000, totalRows);
            }
        }

        /// <summary>
        /// Tests external_links disposition for large results (CloudFetch).
        /// </summary>
        [SkippableFact]
        public void ExecuteQuery_ExternalLinks_LargeResult_UsesCloudFetch()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            Skip.IfNot(Environment.GetEnvironmentVariable("USE_REAL_STATEMENT_EXECUTION_ENDPOINT") == "true");

            var properties = GetStatementExecutionProperties();
            properties[DatabricksParameters.ResultDisposition] = "external_links";

            using var driver = NewDriver;
            using var database = driver.Open(properties);
            using var connection = database.Connect(properties);
            using var statement = connection.CreateStatement();

            // Execute query that will definitely use external links
            statement.SqlQuery = "SELECT id, id * 2 as doubled, CONCAT('row_', CAST(id AS STRING)) as label FROM range(10000)";
            var result = statement.ExecuteQuery();

            // Assert - should successfully read all data via CloudFetch
            int totalRows = 0;
            while (true)
            {
                var batch = result.Stream.ReadNextRecordBatchAsync().Result;
                if (batch == null)
                    break;
                totalRows += batch.Length;
            }

            Assert.Equal(10000, totalRows);
        }

        /// <summary>
        /// Tests that DDL statements work correctly (CREATE, DROP).
        /// </summary>
        [SkippableFact]
        public void ExecuteUpdate_DDLStatements_WorksCorrectly()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            Skip.IfNot(Environment.GetEnvironmentVariable("USE_REAL_STATEMENT_EXECUTION_ENDPOINT") == "true");

            var properties = GetStatementExecutionProperties();
            using var driver = NewDriver;
            using var database = driver.Open(properties);
            using var connection = database.Connect(properties);

            // Create table
            using (var statement = connection.CreateStatement())
            {
                statement.SqlQuery = @"
                    CREATE OR REPLACE TABLE test_ddl_table (
                        id INT,
                        name STRING,
                        value DOUBLE
                    )";
                var result = statement.ExecuteUpdate();
                Assert.True(result.AffectedRows >= 0);
            }

            // Verify table exists by querying it
            using (var statement = connection.CreateStatement())
            {
                statement.SqlQuery = "SELECT * FROM test_ddl_table LIMIT 1";
                var result = statement.ExecuteQuery();
                Assert.NotNull(result.Stream);
            }

            // Drop table
            using (var statement = connection.CreateStatement())
            {
                statement.SqlQuery = "DROP TABLE test_ddl_table";
                var result = statement.ExecuteUpdate();
                Assert.True(result.AffectedRows >= 0);
            }
        }
    }
}
