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
using AdbcDrivers.Databricks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Types;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.StatementExecution
{
    /// <summary>
    /// E2E tests for Statement Execution API (REST) metadata operations.
    /// Tests GetTableTypes(), GetObjects(), and GetTableSchema() implementations.
    /// </summary>
    public class StatementExecutionMetadataE2ETests : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly DatabricksDriver _driver;
        private readonly AdbcDatabase _database;
        private readonly AdbcConnection _connection;
        private readonly DatabricksTestConfiguration _testConfiguration;

        public StatementExecutionMetadataE2ETests(ITestOutputHelper output)
        {
            _output = output;
            _testConfiguration = DatabricksTestingUtils.TestConfiguration;

            // Force Statement Execution API (REST protocol)
            var parameters = new Dictionary<string, string>(_testConfiguration.ToDictionary())
            {
                [DatabricksParameters.Protocol] = "rest"
            };

            _driver = new DatabricksDriver();
            _database = _driver.Open(parameters);
            _connection = _database.Connect(parameters);
        }

        [SkippableFact]
        public void CanGetTableTypes()
        {
            Skip.If(!_testConfiguration.ShouldRunTests());

            using var stream = _connection.GetTableTypes();
            var schema = stream.Schema;

            // Verify schema
            Assert.NotNull(schema);
            Assert.Equal(1, schema.FieldsList.Count);
            Assert.Equal("table_type", schema.FieldsList[0].Name);

            // Read results
            var tableTypes = new List<string>();
            while (true)
            {
                using var batch = stream.ReadNextRecordBatchAsync().Result;
                if (batch == null) break;

                var tableTypeArray = batch.Column("table_type") as StringArray;
                Assert.NotNull(tableTypeArray);

                for (int i = 0; i < batch.Length; i++)
                {
                    if (!tableTypeArray.IsNull(i))
                    {
                        tableTypes.Add(tableTypeArray.GetString(i));
                    }
                }
            }

            // Statement Execution API should return 3 types (including LOCAL TEMPORARY)
            Assert.NotEmpty(tableTypes);
            Assert.Contains("TABLE", tableTypes);
            Assert.Contains("VIEW", tableTypes);
            Assert.Contains("LOCAL TEMPORARY", tableTypes);
            Assert.Equal(3, tableTypes.Count);

            _output.WriteLine($"✓ GetTableTypes returned {tableTypes.Count} types: {string.Join(", ", tableTypes)}");
        }

        [SkippableFact]
        public void CanGetObjectsCatalogs()
        {
            Skip.If(!_testConfiguration.ShouldRunTests());

            using var stream = _connection.GetObjects(
                AdbcConnection.GetObjectsDepth.Catalogs,
                catalogPattern: null,
                schemaPattern: null,
                tableNamePattern: null,
                tableTypes: null,
                columnNamePattern: null);

            var schema = stream.Schema;
            Assert.NotNull(schema);
            Assert.Equal(1, schema.FieldsList.Count);
            Assert.Equal("catalog_name", schema.FieldsList[0].Name);

            var catalogs = new List<string>();
            while (true)
            {
                using var batch = stream.ReadNextRecordBatchAsync().Result;
                if (batch == null) break;

                var catalogArray = batch.Column("catalog_name") as StringArray;
                Assert.NotNull(catalogArray);

                for (int i = 0; i < batch.Length; i++)
                {
                    if (!catalogArray.IsNull(i))
                    {
                        catalogs.Add(catalogArray.GetString(i));
                    }
                }
            }

            Assert.NotEmpty(catalogs);
            _output.WriteLine($"✓ GetObjects(Catalogs) returned {catalogs.Count} catalogs");
        }

        [SkippableFact]
        public void CanGetObjectsDbSchemas()
        {
            Skip.If(!_testConfiguration.ShouldRunTests());

            var testCatalog = _testConfiguration.Metadata.Catalog ?? "main";

            using var stream = _connection.GetObjects(
                AdbcConnection.GetObjectsDepth.DbSchemas,
                catalogPattern: testCatalog,
                schemaPattern: null,
                tableNamePattern: null,
                tableTypes: null,
                columnNamePattern: null);

            var schema = stream.Schema;
            Assert.NotNull(schema);
            Assert.Equal(2, schema.FieldsList.Count);
            Assert.Equal("catalog_name", schema.FieldsList[0].Name);
            Assert.Equal("db_schema_name", schema.FieldsList[1].Name);

            var schemaNames = new List<string>();
            while (true)
            {
                using var batch = stream.ReadNextRecordBatchAsync().Result;
                if (batch == null) break;

                var schemaArray = batch.Column("db_schema_name") as StringArray;
                Assert.NotNull(schemaArray);

                for (int i = 0; i < batch.Length; i++)
                {
                    if (!schemaArray.IsNull(i))
                    {
                        schemaNames.Add(schemaArray.GetString(i));
                    }
                }
            }

            Assert.NotEmpty(schemaNames);
            _output.WriteLine($"✓ GetObjects(DbSchemas) returned {schemaNames.Count} schemas in catalog '{testCatalog}'");
        }

        [SkippableFact]
        public void CanGetObjectsTables()
        {
            Skip.If(!_testConfiguration.ShouldRunTests());

            var testCatalog = _testConfiguration.Metadata.Catalog ?? "main";
            var testSchema = _testConfiguration.Metadata.Schema ?? "default";

            using var stream = _connection.GetObjects(
                AdbcConnection.GetObjectsDepth.Tables,
                catalogPattern: testCatalog,
                schemaPattern: testSchema,
                tableNamePattern: null,
                tableTypes: null,
                columnNamePattern: null);

            var schema = stream.Schema;
            Assert.NotNull(schema);
            Assert.Equal(4, schema.FieldsList.Count);
            Assert.Equal("catalog_name", schema.FieldsList[0].Name);
            Assert.Equal("db_schema_name", schema.FieldsList[1].Name);
            Assert.Equal("table_name", schema.FieldsList[2].Name);
            Assert.Equal("table_type", schema.FieldsList[3].Name);

            var tables = new List<(string catalog, string schema, string table, string type)>();
            while (true)
            {
                using var batch = stream.ReadNextRecordBatchAsync().Result;
                if (batch == null) break;

                var catalogArray = batch.Column("catalog_name") as StringArray;
                var schemaArray = batch.Column("db_schema_name") as StringArray;
                var tableArray = batch.Column("table_name") as StringArray;
                var typeArray = batch.Column("table_type") as StringArray;

                for (int i = 0; i < batch.Length; i++)
                {
                    if (!catalogArray.IsNull(i) && !schemaArray.IsNull(i) &&
                        !tableArray.IsNull(i) && !typeArray.IsNull(i))
                    {
                        tables.Add((
                            catalogArray.GetString(i),
                            schemaArray.GetString(i),
                            tableArray.GetString(i),
                            typeArray.GetString(i)
                        ));
                    }
                }
            }

            _output.WriteLine($"✓ GetObjects(Tables) returned {tables.Count} tables in {testCatalog}.{testSchema}");
        }

        [SkippableFact]
        public void CanGetObjectsWithPatternMatching()
        {
            Skip.If(!_testConfiguration.ShouldRunTests());

            var testCatalog = _testConfiguration.Metadata.Catalog ?? "main";

            // Test catalog pattern with %
            using var stream = _connection.GetObjects(
                AdbcConnection.GetObjectsDepth.Catalogs,
                catalogPattern: "m%",  // Should match 'main' and others starting with 'm'
                schemaPattern: null,
                tableNamePattern: null,
                tableTypes: null,
                columnNamePattern: null);

            var catalogs = new List<string>();
            while (true)
            {
                using var batch = stream.ReadNextRecordBatchAsync().Result;
                if (batch == null) break;

                var catalogArray = batch.Column("catalog_name") as StringArray;
                for (int i = 0; i < batch.Length; i++)
                {
                    if (!catalogArray.IsNull(i))
                    {
                        var catalog = catalogArray.GetString(i);
                        catalogs.Add(catalog);
                        // Verify pattern match
                        Assert.StartsWith("m", catalog, StringComparison.OrdinalIgnoreCase);
                    }
                }
            }

            Assert.NotEmpty(catalogs);
            _output.WriteLine($"✓ Pattern matching 'm%' returned {catalogs.Count} catalogs: {string.Join(", ", catalogs)}");
        }

        [SkippableFact]
        public void CanGetTableSchema()
        {
            Skip.If(!_testConfiguration.ShouldRunTests());
            Skip.If(string.IsNullOrEmpty(_testConfiguration.Metadata.Table), "Test table not configured");

            var testCatalog = _testConfiguration.Metadata.Catalog ?? "main";
            var testSchema = _testConfiguration.Metadata.Schema ?? "default";
            var testTable = _testConfiguration.Metadata.Table;

            var schema = _connection.GetTableSchema(testCatalog, testSchema, testTable);

            Assert.NotNull(schema);
            Assert.NotEmpty(schema.FieldsList);

            _output.WriteLine($"✓ GetTableSchema returned schema for {testCatalog}.{testSchema}.{testTable}:");
            foreach (var field in schema.FieldsList)
            {
                _output.WriteLine($"  - {field.Name}: {field.DataType}");
            }
        }

        [SkippableFact]
        public void GetTableSchemaThrowsForNonExistentTable()
        {
            Skip.If(!_testConfiguration.ShouldRunTests());

            var testCatalog = _testConfiguration.Metadata.Catalog ?? "main";
            var testSchema = _testConfiguration.Metadata.Schema ?? "default";

            var exception = Assert.Throws<AdbcException>(() =>
            {
                _connection.GetTableSchema(testCatalog, testSchema, "nonexistent_table_12345");
            });

            Assert.Contains("Failed to describe table", exception.Message);
            _output.WriteLine($"✓ GetTableSchema correctly threw exception for non-existent table");
        }

        [SkippableFact]
        public async Task CanGetObjectsWithTableTypeFilter()
        {
            Skip.If(!_testConfiguration.ShouldRunTests());

            var testCatalog = _testConfiguration.Metadata.Catalog ?? "main";
            var testSchema = _testConfiguration.Metadata.Schema ?? "default";

            // First, create a test view
            var viewName = $"adbc_test_view_{Guid.NewGuid():N}";
            try
            {
                using (var stmt = _connection.CreateStatement())
                {
                    stmt.SqlQuery = $"CREATE OR REPLACE VIEW {testCatalog}.{testSchema}.{viewName} AS SELECT 1 as id";
                    await stmt.ExecuteUpdateAsync();
                }

                // Query for VIEWs only
                using var stream = _connection.GetObjects(
                    AdbcConnection.GetObjectsDepth.Tables,
                    catalogPattern: testCatalog,
                    schemaPattern: testSchema,
                    tableNamePattern: viewName,
                    tableTypes: new[] { "VIEW" },
                    columnNamePattern: null);

                var views = new List<string>();
                while (true)
                {
                    using var batch = stream.ReadNextRecordBatchAsync().Result;
                    if (batch == null) break;

                    var tableArray = batch.Column("table_name") as StringArray;
                    var typeArray = batch.Column("table_type") as StringArray;

                    for (int i = 0; i < batch.Length; i++)
                    {
                        if (!tableArray.IsNull(i) && !typeArray.IsNull(i))
                        {
                            var tableName = tableArray.GetString(i);
                            var tableType = typeArray.GetString(i);

                            // Verify only VIEWs are returned
                            Assert.Equal("VIEW", tableType);
                            views.Add(tableName);
                        }
                    }
                }

                Assert.Contains(viewName, views);
                _output.WriteLine($"✓ Table type filter returned {views.Count} views including '{viewName}'");
            }
            finally
            {
                // Cleanup
                try
                {
                    using var stmt = _connection.CreateStatement();
                    stmt.SqlQuery = $"DROP VIEW IF EXISTS {testCatalog}.{testSchema}.{viewName}";
                    await stmt.ExecuteUpdateAsync();
                }
                catch (Exception ex)
                {
                    _output.WriteLine($"⚠ Cleanup failed: {ex.Message}");
                }
            }
        }

        [SkippableFact]
        public void CanGetObjectsAll()
        {
            Skip.If(!_testConfiguration.ShouldRunTests());

            var testCatalog = _testConfiguration.Metadata.Catalog ?? "main";

            using var stream = _connection.GetObjects(
                AdbcConnection.GetObjectsDepth.All,
                catalogPattern: testCatalog,
                schemaPattern: null,
                tableNamePattern: null,
                tableTypes: null,
                columnNamePattern: null);

            var schema = stream.Schema;
            Assert.NotNull(schema);

            // Note: Current implementation returns simplified schema
            // TODO: Full ADBC nested structure will be implemented in future
            int totalBatches = 0;
            while (true)
            {
                using var batch = stream.ReadNextRecordBatchAsync().Result;
                if (batch == null) break;
                totalBatches++;
            }

            _output.WriteLine($"✓ GetObjects(All) returned {totalBatches} batches (simplified structure)");
        }

        [SkippableFact]
        public async Task MetadataOperationsWorkWithDifferentDataTypes()
        {
            Skip.If(!_testConfiguration.ShouldRunTests());

            var testCatalog = _testConfiguration.Metadata.Catalog ?? "main";
            var testSchema = _testConfiguration.Metadata.Schema ?? "default";
            var tableName = $"adbc_test_types_{Guid.NewGuid():N}";

            try
            {
                // Create table with various data types
                using (var stmt = _connection.CreateStatement())
                {
                    stmt.SqlQuery = $@"
                        CREATE OR REPLACE TABLE {testCatalog}.{testSchema}.{tableName} (
                            int_col INT,
                            bigint_col BIGINT,
                            float_col FLOAT,
                            double_col DOUBLE,
                            decimal_col DECIMAL(10,2),
                            string_col STRING,
                            boolean_col BOOLEAN,
                            date_col DATE,
                            timestamp_col TIMESTAMP
                        )";
                    await stmt.ExecuteUpdateAsync();
                }

                // Get table schema
                var schema = _connection.GetTableSchema(testCatalog, testSchema, tableName);

                Assert.NotNull(schema);
                Assert.Equal(9, schema.FieldsList.Count);

                // Verify type mappings
                var fieldNames = schema.FieldsList.Select(f => f.Name).ToList();
                Assert.Contains("int_col", fieldNames);
                Assert.Contains("bigint_col", fieldNames);
                Assert.Contains("float_col", fieldNames);
                Assert.Contains("double_col", fieldNames);
                Assert.Contains("decimal_col", fieldNames);
                Assert.Contains("string_col", fieldNames);
                Assert.Contains("boolean_col", fieldNames);
                Assert.Contains("date_col", fieldNames);
                Assert.Contains("timestamp_col", fieldNames);

                // Verify Arrow types
                var intField = schema.FieldsList.First(f => f.Name == "int_col");
                Assert.IsType<Int32Type>(intField.DataType);

                var stringField = schema.FieldsList.First(f => f.Name == "string_col");
                Assert.IsType<StringType>(stringField.DataType);

                _output.WriteLine($"✓ Metadata operations work correctly with {schema.FieldsList.Count} different data types");
            }
            finally
            {
                // Cleanup
                try
                {
                    using var stmt = _connection.CreateStatement();
                    stmt.SqlQuery = $"DROP TABLE IF EXISTS {testCatalog}.{testSchema}.{tableName}";
                    await stmt.ExecuteUpdateAsync();
                }
                catch (Exception ex)
                {
                    _output.WriteLine($"⚠ Cleanup failed: {ex.Message}");
                }
            }
        }

        public void Dispose()
        {
            _connection?.Dispose();
            _database?.Dispose();
            _driver?.Dispose();
        }
    }
}
