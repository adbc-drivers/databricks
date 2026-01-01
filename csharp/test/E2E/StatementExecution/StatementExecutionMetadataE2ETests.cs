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
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;
using AdbcDrivers.Databricks.StatementExecution;

namespace AdbcDrivers.Databricks.Tests.E2E.StatementExecution
{
    /// <summary>
    /// Minimal E2E tests for Statement Execution API metadata operations.
    /// Tests basic metadata functionality. More comprehensive tests will be added later.
    /// </summary>
    public class StatementExecutionMetadataE2ETests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        private const string TestCatalog = "main";
        private const string TestSchema = "adbc_testing";

        public StatementExecutionMetadataE2ETests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
        }

        private void SkipIfNotConfigured()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable), "Test configuration not available");
        }

        private StatementExecutionConnection CreateRestConnection()
        {
            var properties = new Dictionary<string, string>
            {
                [DatabricksParameters.Protocol] = "rest",
            };

            if (!string.IsNullOrEmpty(TestConfiguration.Uri))
            {
                properties[AdbcOptions.Uri] = TestConfiguration.Uri;
            }
            else
            {
                if (!string.IsNullOrEmpty(TestConfiguration.HostName))
                {
                    properties[SparkParameters.HostName] = TestConfiguration.HostName;
                }
                if (!string.IsNullOrEmpty(TestConfiguration.Path))
                {
                    properties[SparkParameters.Path] = TestConfiguration.Path;
                }
            }

            if (!string.IsNullOrEmpty(TestConfiguration.Token))
            {
                properties[SparkParameters.Token] = TestConfiguration.Token;
            }
            if (!string.IsNullOrEmpty(TestConfiguration.AccessToken))
            {
                properties[SparkParameters.AccessToken] = TestConfiguration.AccessToken;
            }

            if (!string.IsNullOrEmpty(TestConfiguration.AuthType))
            {
                properties[SparkParameters.AuthType] = TestConfiguration.AuthType;
            }

            var database = NewDriver.Open(properties);
            return (StatementExecutionConnection)database.Connect(properties);
        }

        #region GetObjects Tests

        [SkippableFact]
        public void CanGetCatalogs()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();
            using var stream = connection.GetObjects(
                AdbcConnection.GetObjectsDepth.Catalogs,
                null, null, null, null, null);

            var schema = stream.Schema;
            Assert.NotNull(schema);

            var catalogs = new List<string>();
            while (true)
            {
                using var batch = stream.ReadNextRecordBatchAsync().Result;
                if (batch == null) break;

                var catalogArray = batch.Column("catalog_name") as Apache.Arrow.StringArray;
                if (catalogArray != null)
                {
                    for (int i = 0; i < batch.Length; i++)
                    {
                        if (!catalogArray.IsNull(i))
                        {
                            catalogs.Add(catalogArray.GetString(i));
                        }
                    }
                }
            }

            Assert.NotEmpty(catalogs);
            Assert.Contains(TestCatalog, catalogs);
            OutputHelper?.WriteLine($"✓ GetObjects(Catalogs) returned {catalogs.Count} catalogs including '{TestCatalog}'");
        }

        [SkippableFact]
        public void CanGetSchemas()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();
            using var stream = connection.GetObjects(
                AdbcConnection.GetObjectsDepth.DbSchemas,
                TestCatalog, null, null, null, null);

            var schemas = new List<string>();
            while (true)
            {
                using var batch = stream.ReadNextRecordBatchAsync().Result;
                if (batch == null) break;

                // Navigate nested structure to get schemas
                var catalogSchemas = batch.Column("catalog_db_schemas") as Apache.Arrow.ListArray;
                if (catalogSchemas != null)
                {
                    for (int catIdx = 0; catIdx < catalogSchemas.Length; catIdx++)
                    {
                        if (catalogSchemas.IsNull(catIdx)) continue;

                        var dbSchemasStruct = catalogSchemas.GetSlicedValues(catIdx) as Apache.Arrow.StructArray;
                        if (dbSchemasStruct == null) continue;

                        var schemaNameField = dbSchemasStruct.Fields.FirstOrDefault(f => f is Apache.Arrow.StringArray);
                        if (schemaNameField is Apache.Arrow.StringArray schemaNameArray)
                        {
                            for (int i = 0; i < schemaNameArray.Length; i++)
                            {
                                if (!schemaNameArray.IsNull(i))
                                {
                                    schemas.Add(schemaNameArray.GetString(i));
                                }
                            }
                        }
                    }
                }
            }

            Assert.NotEmpty(schemas);
            OutputHelper?.WriteLine($"✓ GetObjects(DbSchemas) returned {schemas.Count} schemas");
        }

        [SkippableFact]
        public async Task CanGetTables()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();

            // Create a simple test table
            var testTable = $"metadata_test_{Guid.NewGuid():N}";
            try
            {
                using (var stmt = connection.CreateStatement())
                {
                    stmt.SqlQuery = $"CREATE TABLE IF NOT EXISTS {TestCatalog}.{TestSchema}.{testTable} (id INT, name STRING)";
                    await stmt.ExecuteUpdateAsync();
                }

                using var stream = connection.GetObjects(
                    AdbcConnection.GetObjectsDepth.Tables,
                    TestCatalog, TestSchema, null, null, null);

                var tables = new List<string>();
                while (true)
                {
                    using var batch = stream.ReadNextRecordBatchAsync().Result;
                    if (batch == null) break;

                    // Navigate nested structure to get table names
                    var catalogSchemas = batch.Column("catalog_db_schemas") as Apache.Arrow.ListArray;
                    if (catalogSchemas != null)
                    {
                        for (int catIdx = 0; catIdx < catalogSchemas.Length; catIdx++)
                        {
                            if (catalogSchemas.IsNull(catIdx)) continue;

                            var dbSchemasStruct = catalogSchemas.GetSlicedValues(catIdx) as Apache.Arrow.StructArray;
                            if (dbSchemasStruct == null) continue;

                            var dbSchemaTablesField = dbSchemasStruct.Fields.FirstOrDefault(f => f.Data.DataType is Apache.Arrow.Types.ListType);
                            if (dbSchemaTablesField is Apache.Arrow.ListArray dbSchemaTablesList)
                            {
                                for (int schemaIdx = 0; schemaIdx < dbSchemaTablesList.Length; schemaIdx++)
                                {
                                    if (dbSchemaTablesList.IsNull(schemaIdx)) continue;

                                    var tablesStruct = dbSchemaTablesList.GetSlicedValues(schemaIdx) as Apache.Arrow.StructArray;
                                    if (tablesStruct == null) continue;

                                    var tableNameField = tablesStruct.Fields.FirstOrDefault(f => f is Apache.Arrow.StringArray);
                                    if (tableNameField is Apache.Arrow.StringArray tableNameArray)
                                    {
                                        for (int i = 0; i < tableNameArray.Length; i++)
                                        {
                                            if (!tableNameArray.IsNull(i))
                                            {
                                                tables.Add(tableNameArray.GetString(i));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                Assert.Contains(testTable, tables);
                OutputHelper?.WriteLine($"✓ GetObjects(Tables) found test table: {testTable}");
            }
            finally
            {
                try
                {
                    using var stmt = connection.CreateStatement();
                    stmt.SqlQuery = $"DROP TABLE IF EXISTS {TestCatalog}.{TestSchema}.{testTable}";
                    await stmt.ExecuteUpdateAsync();
                }
                catch (Exception ex)
                {
                    OutputHelper?.WriteLine($"⚠ Failed to drop {testTable}: {ex.Message}");
                }
            }
        }

        #endregion

        #region GetTableSchema Tests

        [SkippableFact]
        public async Task CanGetTableSchema()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();

            // Create a simple test table
            var testTable = $"schema_test_{Guid.NewGuid():N}";
            try
            {
                using (var stmt = connection.CreateStatement())
                {
                    stmt.SqlQuery = $@"
                        CREATE TABLE IF NOT EXISTS {TestCatalog}.{TestSchema}.{testTable} (
                            col_int INT,
                            col_string STRING,
                            col_double DOUBLE
                        )";
                    await stmt.ExecuteUpdateAsync();
                }

                var schema = connection.GetTableSchema(TestCatalog, TestSchema, testTable);

                Assert.NotNull(schema);
                Assert.Equal(3, schema.FieldsList.Count);

                var columnNames = schema.FieldsList.Select(f => f.Name).ToList();
                Assert.Contains("col_int", columnNames);
                Assert.Contains("col_string", columnNames);
                Assert.Contains("col_double", columnNames);

                OutputHelper?.WriteLine($"✓ GetTableSchema returned {schema.FieldsList.Count} columns");
            }
            finally
            {
                try
                {
                    using var stmt = connection.CreateStatement();
                    stmt.SqlQuery = $"DROP TABLE IF EXISTS {TestCatalog}.{TestSchema}.{testTable}";
                    await stmt.ExecuteUpdateAsync();
                }
                catch (Exception ex)
                {
                    OutputHelper?.WriteLine($"⚠ Failed to drop {testTable}: {ex.Message}");
                }
            }
        }

        #endregion

        #region GetTableTypes Tests

        [SkippableFact]
        public void CanGetTableTypes()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();
            using var stream = connection.GetTableTypes();
            var schema = stream.Schema;

            Assert.NotNull(schema);
            Assert.Single(schema.FieldsList);
            Assert.Equal("table_type", schema.FieldsList[0].Name);

            var tableTypes = new List<string>();
            while (true)
            {
                using var batch = stream.ReadNextRecordBatchAsync().Result;
                if (batch == null) break;

                var typeArray = batch.Column("table_type") as Apache.Arrow.StringArray;
                if (typeArray != null)
                {
                    for (int i = 0; i < batch.Length; i++)
                    {
                        if (!typeArray.IsNull(i))
                        {
                            tableTypes.Add(typeArray.GetString(i));
                        }
                    }
                }
            }

            Assert.NotEmpty(tableTypes);
            OutputHelper?.WriteLine($"✓ GetTableTypes returned {tableTypes.Count} types: {string.Join(", ", tableTypes)}");
        }

        #endregion

        #region GetInfo Tests

        [SkippableFact]
        public void CanGetInfo()
        {
            SkipIfNotConfigured();
            using var connection = CreateRestConnection();

            // Test GetInfo with no codes (returns all supported codes)
            using (var stream = connection.GetInfo(new List<Apache.Arrow.Adbc.AdbcInfoCode>()))
            {
                var schema = stream.Schema;
                Assert.NotNull(schema);
                Assert.Equal(2, schema.FieldsList.Count);
                Assert.Equal("info_name", schema.FieldsList[0].Name);
                Assert.Equal("info_value", schema.FieldsList[1].Name);

                var batch = stream.ReadNextRecordBatchAsync().Result;
                Assert.NotNull(batch);
                Assert.True(batch.Length > 0);

                OutputHelper?.WriteLine($"✓ GetInfo() returned {batch.Length} info codes");
            }
        }

        #endregion
    }
}
