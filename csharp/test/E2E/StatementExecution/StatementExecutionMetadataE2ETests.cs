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
using Apache.Arrow.Types;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.StatementExecution
{
    /// <summary>
    /// E2E tests for Statement Execution API (REST) metadata operations.
    /// Tests GetTableTypes(), GetObjects(), and GetTableSchema() implementations.
    /// </summary>
    public class StatementExecutionMetadataE2ETests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public StatementExecutionMetadataE2ETests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
        }

        private void SkipIfNotConfigured()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable), "Test configuration not available");
        }

        private AdbcConnection CreateRestConnection()
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
            return database.Connect(properties);
        }

        [SkippableFact]
        public void CanGetTableTypes()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();
            using var stream = connection.GetTableTypes();
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

            OutputHelper?.WriteLine($"✓ GetTableTypes returned {tableTypes.Count} types: {string.Join(", ", tableTypes)}");
        }

        [SkippableFact]
        public void CanGetObjectsCatalogs()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();
            using var stream = connection.GetObjects(
                AdbcConnection.GetObjectsDepth.Catalogs,
                null, null, null, null, null);

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
            OutputHelper?.WriteLine($"✓ GetObjects(Catalogs) returned {catalogs.Count} catalogs");
        }

        [SkippableFact]
        public void CanGetObjectsDbSchemas()
        {
            SkipIfNotConfigured();

            var testCatalog = TestConfiguration.Metadata?.Catalog ?? "main";

            using var connection = CreateRestConnection();
            using var stream = connection.GetObjects(
                AdbcConnection.GetObjectsDepth.DbSchemas,
                testCatalog, null, null, null, null);

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
            OutputHelper?.WriteLine($"✓ GetObjects(DbSchemas) returned {schemaNames.Count} schemas in catalog '{testCatalog}'");
        }

        [SkippableFact]
        public void CanGetObjectsTables()
        {
            SkipIfNotConfigured();

            var testCatalog = TestConfiguration.Metadata?.Catalog ?? "main";
            var testSchema = TestConfiguration.Metadata?.Schema ?? "default";

            using var connection = CreateRestConnection();
            using var stream = connection.GetObjects(
                AdbcConnection.GetObjectsDepth.Tables,
                testCatalog, testSchema, null, null, null);

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

            OutputHelper?.WriteLine($"✓ GetObjects(Tables) returned {tables.Count} tables in {testCatalog}.{testSchema}");
        }

        [SkippableFact]
        public void CanGetObjectsWithPatternMatching()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();
            // Test catalog pattern with %
            using var stream = connection.GetObjects(
                AdbcConnection.GetObjectsDepth.Catalogs,
                "m%", null, null, null, null);  // Should match 'main' and others starting with 'm'

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
            OutputHelper?.WriteLine($"✓ Pattern matching 'm%' returned {catalogs.Count} catalogs: {string.Join(", ", catalogs)}");
        }

        [SkippableFact]
        public void CanGetTableSchema()
        {
            SkipIfNotConfigured();
            Skip.If(string.IsNullOrEmpty(TestConfiguration.Metadata?.Table), "Test table not configured");

            var testCatalog = TestConfiguration.Metadata?.Catalog ?? "main";
            var testSchema = TestConfiguration.Metadata?.Schema ?? "default";
            var testTable = TestConfiguration.Metadata!.Table;

            using var connection = CreateRestConnection();
            var schema = connection.GetTableSchema(testCatalog, testSchema, testTable);

            Assert.NotNull(schema);
            Assert.NotEmpty(schema.FieldsList);

            OutputHelper?.WriteLine($"✓ GetTableSchema returned schema for {testCatalog}.{testSchema}.{testTable}:");
            foreach (var field in schema.FieldsList)
            {
                OutputHelper?.WriteLine($"  - {field.Name}: {field.DataType}");
            }
        }

        [SkippableFact]
        public void GetTableSchemaThrowsForNonExistentTable()
        {
            SkipIfNotConfigured();

            var testCatalog = TestConfiguration.Metadata?.Catalog ?? "main";
            var testSchema = TestConfiguration.Metadata?.Schema ?? "default";

            using var connection = CreateRestConnection();
            var exception = Assert.Throws<AdbcException>(() =>
            {
                connection.GetTableSchema(testCatalog, testSchema, "nonexistent_table_12345");
            });

            Assert.Contains("Failed to describe table", exception.Message);
            OutputHelper?.WriteLine($"✓ GetTableSchema correctly threw exception for non-existent table");
        }

        [SkippableFact]
        public async Task CanGetObjectsWithTableTypeFilter()
        {
            SkipIfNotConfigured();

            var testCatalog = TestConfiguration.Metadata?.Catalog ?? "main";
            var testSchema = TestConfiguration.Metadata?.Schema ?? "default";

            // First, create a test view
            var viewName = $"adbc_test_view_{Guid.NewGuid():N}";
            using var connection = CreateRestConnection();

            try
            {
                using (var stmt = connection.CreateStatement())
                {
                    stmt.SqlQuery = $"CREATE OR REPLACE VIEW {testCatalog}.{testSchema}.{viewName} AS SELECT 1 as id";
                    await stmt.ExecuteUpdateAsync();
                }

                // Query for VIEWs only
                using var stream = connection.GetObjects(
                    AdbcConnection.GetObjectsDepth.Tables,
                    testCatalog, testSchema, viewName,
                    new[] { "VIEW" }, null);

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
                OutputHelper?.WriteLine($"✓ Table type filter returned {views.Count} views including '{viewName}'");
            }
            finally
            {
                // Cleanup
                try
                {
                    using var stmt = connection.CreateStatement();
                    stmt.SqlQuery = $"DROP VIEW IF EXISTS {testCatalog}.{testSchema}.{viewName}";
                    await stmt.ExecuteUpdateAsync();
                }
                catch (Exception ex)
                {
                    OutputHelper?.WriteLine($"⚠ Cleanup failed: {ex.Message}");
                }
            }
        }

        [SkippableFact]
        public void CanGetObjectsAll()
        {
            SkipIfNotConfigured();

            var testCatalog = TestConfiguration.Metadata?.Catalog ?? "main";

            using var connection = CreateRestConnection();
            using var stream = connection.GetObjects(
                AdbcConnection.GetObjectsDepth.All,
                testCatalog, null, null, null, null);

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

            OutputHelper?.WriteLine($"✓ GetObjects(All) returned {totalBatches} batches (simplified structure)");
        }

        [SkippableFact]
        public async Task MetadataOperationsWorkWithDifferentDataTypes()
        {
            SkipIfNotConfigured();

            var testCatalog = TestConfiguration.Metadata?.Catalog ?? "main";
            var testSchema = TestConfiguration.Metadata?.Schema ?? "default";
            var tableName = $"adbc_test_types_{Guid.NewGuid():N}";

            using var connection = CreateRestConnection();

            try
            {
                // Create table with various data types
                using (var stmt = connection.CreateStatement())
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
                var schema = connection.GetTableSchema(testCatalog, testSchema, tableName);

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

                OutputHelper?.WriteLine($"✓ Metadata operations work correctly with {schema.FieldsList.Count} different data types");
            }
            finally
            {
                // Cleanup
                try
                {
                    using var stmt = connection.CreateStatement();
                    stmt.SqlQuery = $"DROP TABLE IF EXISTS {testCatalog}.{testSchema}.{tableName}";
                    await stmt.ExecuteUpdateAsync();
                }
                catch (Exception ex)
                {
                    OutputHelper?.WriteLine($"⚠ Cleanup failed: {ex.Message}");
                }
            }
        }

        [SkippableFact]
        public void CanGetInfo()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();

            // Test with all codes (null/empty array)
            using var streamAll = connection.GetInfo(System.Array.Empty<AdbcInfoCode>());
            Assert.NotNull(streamAll);
            Assert.NotNull(streamAll.Schema);

            // Verify schema has correct structure (info_name: uint32, info_value: union)
            Assert.Equal(2, streamAll.Schema.FieldsList.Count);
            Assert.Equal("info_name", streamAll.Schema.FieldsList[0].Name);
            Assert.Equal("info_value", streamAll.Schema.FieldsList[1].Name);
            Assert.IsType<UInt32Type>(streamAll.Schema.FieldsList[0].DataType);
            Assert.IsType<UnionType>(streamAll.Schema.FieldsList[1].DataType);

            // Read the batch
            var batchAll = streamAll.ReadNextRecordBatchAsync().Result;
            Assert.NotNull(batchAll);
            Assert.True(batchAll.Length >= 7); // Should have at least 7 supported codes

            OutputHelper?.WriteLine($"✓ GetInfo() returned {batchAll.Length} info codes");

            // Test specific info codes
            var requestedCodes = new[]
            {
                AdbcInfoCode.VendorName,
                AdbcInfoCode.DriverName,
                AdbcInfoCode.DriverVersion,
                AdbcInfoCode.VendorSql
            };

            using var stream = connection.GetInfo(requestedCodes);
            using var batch = stream.ReadNextRecordBatchAsync().Result;

            Assert.NotNull(batch);
            Assert.Equal(4, batch.Length);

            var infoNameArray = batch.Column("info_name") as UInt32Array;
            var infoValueUnion = batch.Column("info_value") as DenseUnionArray;
            Assert.NotNull(infoNameArray);
            Assert.NotNull(infoValueUnion);

            // Verify VendorName = "Databricks"
            var vendorNameIndex = FindInfoCodeIndex(infoNameArray, AdbcInfoCode.VendorName);
            Assert.True(vendorNameIndex >= 0, "VendorName not found");
            var vendorNameValue = GetStringValueFromUnion(infoValueUnion, vendorNameIndex);
            Assert.Equal("Databricks", vendorNameValue);
            OutputHelper?.WriteLine($"✓ VendorName = {vendorNameValue}");

            // Verify DriverName contains "ADBC Databricks Driver"
            var driverNameIndex = FindInfoCodeIndex(infoNameArray, AdbcInfoCode.DriverName);
            Assert.True(driverNameIndex >= 0, "DriverName not found");
            var driverNameValue = GetStringValueFromUnion(infoValueUnion, driverNameIndex);
            Assert.Contains("ADBC Databricks Driver", driverNameValue);
            OutputHelper?.WriteLine($"✓ DriverName = {driverNameValue}");

            // Verify DriverVersion is a valid version string
            var driverVersionIndex = FindInfoCodeIndex(infoNameArray, AdbcInfoCode.DriverVersion);
            Assert.True(driverVersionIndex >= 0, "DriverVersion not found");
            var driverVersionValue = GetStringValueFromUnion(infoValueUnion, driverVersionIndex);
            Assert.NotEmpty(driverVersionValue);
            Assert.Matches(@"^\d+\.\d+\.\d+", driverVersionValue);
            OutputHelper?.WriteLine($"✓ DriverVersion = {driverVersionValue}");

            // Verify VendorSql = false (Databricks uses Spark SQL, not standard SQL)
            var vendorSqlIndex = FindInfoCodeIndex(infoNameArray, AdbcInfoCode.VendorSql);
            Assert.True(vendorSqlIndex >= 0, "VendorSql not found");
            var vendorSqlValue = GetBoolValueFromUnion(infoValueUnion, vendorSqlIndex);
            Assert.False(vendorSqlValue);
            OutputHelper?.WriteLine($"✓ VendorSql = {vendorSqlValue} (uses Spark SQL)");

            OutputHelper?.WriteLine("✓ GetInfo() E2E test passed");
        }

        private int FindInfoCodeIndex(UInt32Array infoNameArray, AdbcInfoCode code)
        {
            for (int i = 0; i < infoNameArray.Length; i++)
            {
                if ((AdbcInfoCode)infoNameArray.GetValue(i) == code)
                {
                    return i;
                }
            }
            return -1;
        }

        private string GetStringValueFromUnion(DenseUnionArray unionArray, int index)
        {
            var stringArray = unionArray.Fields[0] as StringArray;
            return stringArray!.GetString(index);
        }

        private bool GetBoolValueFromUnion(DenseUnionArray unionArray, int index)
        {
            var boolArray = unionArray.Fields[1] as BooleanArray;
            return boolArray!.GetValue(index) ?? false; // Handle nullable bool
        }
    }
}
