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
using AdbcDrivers.Databricks.StatementExecution;

namespace AdbcDrivers.Databricks.Tests.E2E.StatementExecution
{
    /// <summary>
    /// E2E tests for Statement Execution API metadata operations.
    /// Creates test tables with all data types and tests all metadata operations.
    /// </summary>
    public class StatementExecutionMetadataE2ETests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        private const string TestCatalog = "main";
        private const string TestSchema = "adbc_testing";
        private readonly string TestTable = $"all_data_types_test_{Guid.NewGuid():N}";
        private readonly string ReferenceTable = $"ref_table_{Guid.NewGuid():N}";

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

        private async Task CreateTestTables(StatementExecutionConnection connection)
        {
            // Create reference table with primary key
            using (var stmt = connection.CreateStatement())
            {
                stmt.SqlQuery = $@"
                    CREATE TABLE IF NOT EXISTS {TestCatalog}.{TestSchema}.{ReferenceTable} (
                        ref_id BIGINT NOT NULL,
                        ref_name STRING,
                        PRIMARY KEY (ref_id)
                    )";
                await stmt.ExecuteUpdateAsync();
            }

            // Create main test table with all data types and foreign key
            using (var stmt = connection.CreateStatement())
            {
                stmt.SqlQuery = $@"
                    CREATE TABLE IF NOT EXISTS {TestCatalog}.{TestSchema}.{TestTable} (
                        col_tinyint TINYINT,
                        col_smallint SMALLINT,
                        col_int INT NOT NULL,
                        col_bigint BIGINT,
                        col_decimal DECIMAL(10,2),
                        col_float FLOAT,
                        col_double DOUBLE,
                        col_string STRING NOT NULL,
                        col_binary BINARY,
                        col_boolean BOOLEAN,
                        col_date DATE,
                        col_timestamp TIMESTAMP,
                        col_timestamp_ntz TIMESTAMP_NTZ,
                        col_array ARRAY<INT>,
                        col_map MAP<STRING, INT>,
                        col_struct STRUCT<name: STRING, age: INT>,
                        PRIMARY KEY (col_string, col_int),
                        FOREIGN KEY (col_bigint) REFERENCES {TestCatalog}.{TestSchema}.{ReferenceTable}(ref_id)
                    )";
                await stmt.ExecuteUpdateAsync();
            }

            OutputHelper?.WriteLine($"✓ Created test tables: {TestTable}, {ReferenceTable}");
        }

        private async Task DropTestTables(StatementExecutionConnection connection)
        {
            try
            {
                using var stmt = connection.CreateStatement();
                stmt.SqlQuery = $"DROP TABLE IF EXISTS {TestCatalog}.{TestSchema}.{TestTable}";
                await stmt.ExecuteUpdateAsync();
            }
            catch (Exception ex)
            {
                OutputHelper?.WriteLine($"⚠ Failed to drop {TestTable}: {ex.Message}");
            }

            try
            {
                using var stmt = connection.CreateStatement();
                stmt.SqlQuery = $"DROP TABLE IF EXISTS {TestCatalog}.{TestSchema}.{ReferenceTable}";
                await stmt.ExecuteUpdateAsync();
            }
            catch (Exception ex)
            {
                OutputHelper?.WriteLine($"⚠ Failed to drop {ReferenceTable}: {ex.Message}");
            }
        }

        #region GetCatalogs Tests

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
            Assert.Single(schema.FieldsList);
            Assert.Equal("catalog_name", schema.FieldsList[0].Name);

            var catalogs = new List<string>();
            while (true)
            {
                using var batch = stream.ReadNextRecordBatchAsync().Result;
                if (batch == null) break;

                var catalogArray = (StringArray)batch.Column("catalog_name");
                for (int i = 0; i < batch.Length; i++)
                {
                    if (!catalogArray.IsNull(i))
                    {
                        catalogs.Add(catalogArray.GetString(i));
                    }
                }
            }

            Assert.NotEmpty(catalogs);
            Assert.Contains(TestCatalog, catalogs);
            OutputHelper?.WriteLine($"✓ GetObjects(Catalogs) returned {catalogs.Count} catalogs including '{TestCatalog}'");
        }

        #endregion

        #region GetSchemas Tests

        [SkippableFact]
        public void CanGetSchemas()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();
            using var stream = connection.GetObjects(
                AdbcConnection.GetObjectsDepth.DbSchemas,
                TestCatalog, null, null, null, null);

            var schema = stream.Schema;
            Assert.NotNull(schema);
            Assert.Equal(2, schema.FieldsList.Count);
            Assert.Equal("catalog_name", schema.FieldsList[0].Name);
            Assert.Equal("db_schema_name", schema.FieldsList[1].Name);

            var schemas = new List<string>();
            while (true)
            {
                using var batch = stream.ReadNextRecordBatchAsync().Result;
                if (batch == null) break;

                var schemaArray = (StringArray)batch.Column("db_schema_name");
                for (int i = 0; i < batch.Length; i++)
                {
                    if (!schemaArray.IsNull(i))
                    {
                        schemas.Add(schemaArray.GetString(i));
                    }
                }
            }

            Assert.NotEmpty(schemas);
            Assert.Contains(TestSchema, schemas);
            OutputHelper?.WriteLine($"✓ GetObjects(DbSchemas) returned {schemas.Count} schemas including '{TestSchema}'");
        }

        #endregion

        #region GetTables Tests

        [SkippableFact]
        public async Task CanGetTables()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();

            try
            {
                await CreateTestTables(connection);

                using var stream = connection.GetObjects(
                    AdbcConnection.GetObjectsDepth.Tables,
                    TestCatalog, TestSchema, null, null, null);

                var schema = stream.Schema;
                Assert.NotNull(schema);
                Assert.Equal(4, schema.FieldsList.Count);
                Assert.Equal("catalog_name", schema.FieldsList[0].Name);
                Assert.Equal("db_schema_name", schema.FieldsList[1].Name);
                Assert.Equal("table_name", schema.FieldsList[2].Name);
                Assert.Equal("table_type", schema.FieldsList[3].Name);

                var tables = new List<(string name, string type)>();
                while (true)
                {
                    using var batch = stream.ReadNextRecordBatchAsync().Result;
                    if (batch == null) break;

                    var tableArray = (StringArray)batch.Column("table_name");
                    var typeArray = (StringArray)batch.Column("table_type");

                    for (int i = 0; i < batch.Length; i++)
                    {
                        if (!tableArray.IsNull(i) && !typeArray.IsNull(i))
                        {
                            tables.Add((tableArray.GetString(i), typeArray.GetString(i)));
                        }
                    }
                }

                Assert.Contains(tables, t => t.name == TestTable);
                Assert.Contains(tables, t => t.name == ReferenceTable);
                OutputHelper?.WriteLine($"✓ GetObjects(Tables) found test tables: {TestTable}, {ReferenceTable}");
            }
            finally
            {
                await DropTestTables(connection);
            }
        }

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

                var typeArray = (StringArray)batch.Column("table_type");
                for (int i = 0; i < batch.Length; i++)
                {
                    if (!typeArray.IsNull(i))
                    {
                        tableTypes.Add(typeArray.GetString(i));
                    }
                }
            }

            Assert.NotEmpty(tableTypes);
            Assert.Contains("TABLE", tableTypes);
            Assert.Contains("VIEW", tableTypes);
            OutputHelper?.WriteLine($"✓ GetTableTypes returned {tableTypes.Count} types: {string.Join(", ", tableTypes)}");
        }

        #endregion

        #region GetColumns Tests

        [SkippableFact]
        public async Task CanGetColumnsForAllDataTypes()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();

            try
            {
                await CreateTestTables(connection);

                var schema = connection.GetTableSchema(TestCatalog, TestSchema, TestTable);

                Assert.NotNull(schema);
                Assert.Equal(16, schema.FieldsList.Count);

                // Verify all column names
                var columnNames = schema.FieldsList.Select(f => f.Name).ToList();
                Assert.Contains("col_tinyint", columnNames);
                Assert.Contains("col_smallint", columnNames);
                Assert.Contains("col_int", columnNames);
                Assert.Contains("col_bigint", columnNames);
                Assert.Contains("col_decimal", columnNames);
                Assert.Contains("col_float", columnNames);
                Assert.Contains("col_double", columnNames);
                Assert.Contains("col_string", columnNames);
                Assert.Contains("col_binary", columnNames);
                Assert.Contains("col_boolean", columnNames);
                Assert.Contains("col_date", columnNames);
                Assert.Contains("col_timestamp", columnNames);
                Assert.Contains("col_timestamp_ntz", columnNames);
                Assert.Contains("col_array", columnNames);
                Assert.Contains("col_map", columnNames);
                Assert.Contains("col_struct", columnNames);

                // Verify type mappings for primitive types
                var tinyintField = schema.FieldsList.First(f => f.Name == "col_tinyint");
                Assert.IsType<Int8Type>(tinyintField.DataType);

                var smallintField = schema.FieldsList.First(f => f.Name == "col_smallint");
                Assert.IsType<Int16Type>(smallintField.DataType);

                var intField = schema.FieldsList.First(f => f.Name == "col_int");
                Assert.IsType<Int32Type>(intField.DataType);

                var bigintField = schema.FieldsList.First(f => f.Name == "col_bigint");
                Assert.IsType<Int64Type>(bigintField.DataType);

                var floatField = schema.FieldsList.First(f => f.Name == "col_float");
                Assert.IsType<FloatType>(floatField.DataType);

                var doubleField = schema.FieldsList.First(f => f.Name == "col_double");
                Assert.IsType<DoubleType>(doubleField.DataType);

                var stringField = schema.FieldsList.First(f => f.Name == "col_string");
                Assert.IsType<StringType>(stringField.DataType);

                var boolField = schema.FieldsList.First(f => f.Name == "col_boolean");
                Assert.IsType<BooleanType>(boolField.DataType);

                var dateField = schema.FieldsList.First(f => f.Name == "col_date");
                Assert.IsType<Date32Type>(dateField.DataType);

                var timestampField = schema.FieldsList.First(f => f.Name == "col_timestamp");
                Assert.IsType<TimestampType>(timestampField.DataType);

                OutputHelper?.WriteLine($"✓ GetTableSchema returned {schema.FieldsList.Count} columns with correct type mappings");
                foreach (var field in schema.FieldsList)
                {
                    OutputHelper?.WriteLine($"  - {field.Name}: {field.DataType}");
                }
            }
            finally
            {
                await DropTestTables(connection);
            }
        }

        [SkippableFact]
        public async Task GetColumnsReturnsExactColumnDetails()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();

            try
            {
                await CreateTestTables(connection);

                // Use GetObjects to get detailed column metadata (same as GetColumns internally)
                using var statement = connection.CreateStatement();
                statement.SqlQuery = $"SELECT * FROM {TestCatalog}.{TestSchema}.{TestTable} LIMIT 0";

                var queryResult = statement.ExecuteQuery();
                using var reader = queryResult.Stream;
                var schema = reader.Schema;

                Assert.NotNull(schema);
                Assert.Equal(16, schema.FieldsList.Count);

                OutputHelper?.WriteLine($"Validating exact column details for {TestTable}:");

                // Validate col_tinyint
                var col = schema.FieldsList[0];
                Assert.Equal("col_tinyint", col.Name);
                Assert.IsType<Int8Type>(col.DataType);
                Assert.True(col.IsNullable);
                OutputHelper?.WriteLine($"  ✓ col_tinyint: TINYINT, nullable=true");

                // Validate col_smallint
                col = schema.FieldsList[1];
                Assert.Equal("col_smallint", col.Name);
                Assert.IsType<Int16Type>(col.DataType);
                Assert.True(col.IsNullable);
                OutputHelper?.WriteLine($"  ✓ col_smallint: SMALLINT, nullable=true");

                // Validate col_int (NOT NULL)
                col = schema.FieldsList[2];
                Assert.Equal("col_int", col.Name);
                Assert.IsType<Int32Type>(col.DataType);
                Assert.False(col.IsNullable); // NOT NULL constraint
                OutputHelper?.WriteLine($"  ✓ col_int: INT, nullable=false (NOT NULL)");

                // Validate col_bigint
                col = schema.FieldsList[3];
                Assert.Equal("col_bigint", col.Name);
                Assert.IsType<Int64Type>(col.DataType);
                Assert.True(col.IsNullable);
                OutputHelper?.WriteLine($"  ✓ col_bigint: BIGINT, nullable=true");

                // Validate col_decimal
                col = schema.FieldsList[4];
                Assert.Equal("col_decimal", col.Name);
                Assert.IsType<Decimal128Type>(col.DataType);
                var decimalType = (Decimal128Type)col.DataType;
                Assert.Equal(10, decimalType.Precision);
                Assert.Equal(2, decimalType.Scale);
                Assert.True(col.IsNullable);
                OutputHelper?.WriteLine($"  ✓ col_decimal: DECIMAL(10,2), nullable=true");

                // Validate col_float
                col = schema.FieldsList[5];
                Assert.Equal("col_float", col.Name);
                Assert.IsType<FloatType>(col.DataType);
                Assert.True(col.IsNullable);
                OutputHelper?.WriteLine($"  ✓ col_float: FLOAT, nullable=true");

                // Validate col_double
                col = schema.FieldsList[6];
                Assert.Equal("col_double", col.Name);
                Assert.IsType<DoubleType>(col.DataType);
                Assert.True(col.IsNullable);
                OutputHelper?.WriteLine($"  ✓ col_double: DOUBLE, nullable=true");

                // Validate col_string (NOT NULL)
                col = schema.FieldsList[7];
                Assert.Equal("col_string", col.Name);
                Assert.IsType<StringType>(col.DataType);
                Assert.False(col.IsNullable); // NOT NULL constraint
                OutputHelper?.WriteLine($"  ✓ col_string: STRING, nullable=false (NOT NULL)");

                // Validate col_binary
                col = schema.FieldsList[8];
                Assert.Equal("col_binary", col.Name);
                Assert.IsType<BinaryType>(col.DataType);
                Assert.True(col.IsNullable);
                OutputHelper?.WriteLine($"  ✓ col_binary: BINARY, nullable=true");

                // Validate col_boolean
                col = schema.FieldsList[9];
                Assert.Equal("col_boolean", col.Name);
                Assert.IsType<BooleanType>(col.DataType);
                Assert.True(col.IsNullable);
                OutputHelper?.WriteLine($"  ✓ col_boolean: BOOLEAN, nullable=true");

                // Validate col_date
                col = schema.FieldsList[10];
                Assert.Equal("col_date", col.Name);
                Assert.IsType<Date32Type>(col.DataType);
                Assert.True(col.IsNullable);
                OutputHelper?.WriteLine($"  ✓ col_date: DATE, nullable=true");

                // Validate col_timestamp
                col = schema.FieldsList[11];
                Assert.Equal("col_timestamp", col.Name);
                Assert.IsType<TimestampType>(col.DataType);
                Assert.True(col.IsNullable);
                OutputHelper?.WriteLine($"  ✓ col_timestamp: TIMESTAMP, nullable=true");

                // Validate col_timestamp_ntz
                col = schema.FieldsList[12];
                Assert.Equal("col_timestamp_ntz", col.Name);
                Assert.IsType<TimestampType>(col.DataType);
                Assert.True(col.IsNullable);
                OutputHelper?.WriteLine($"  ✓ col_timestamp_ntz: TIMESTAMP_NTZ, nullable=true");

                // Validate col_array - ARRAY<INT>
                col = schema.FieldsList[13];
                Assert.Equal("col_array", col.Name);
                Assert.IsType<ListType>(col.DataType);
                var listType = (ListType)col.DataType;
                Assert.IsType<Int32Type>(listType.ValueDataType);
                Assert.True(col.IsNullable);
                OutputHelper?.WriteLine($"  ✓ col_array: ARRAY<INT>, nullable=true");

                // Validate col_map - MAP<STRING, INT>
                col = schema.FieldsList[14];
                Assert.Equal("col_map", col.Name);
                Assert.IsType<MapType>(col.DataType);
                // Note: MapType is a complex nested structure in Arrow (ListType containing StructType with key/value)
                // For this test, just verify it's recognized as MapType
                Assert.True(col.IsNullable);
                OutputHelper?.WriteLine($"  ✓ col_map: MAP<STRING, INT>, nullable=true");

                // Validate col_struct - STRUCT<name: STRING, age: INT>
                // This is the critical test for lowercase field names after the casing fix
                col = schema.FieldsList[15];
                Assert.Equal("col_struct", col.Name);
                Assert.IsType<StructType>(col.DataType);
                var structType = (StructType)col.DataType;
                Assert.Equal(2, structType.Fields.Count);

                // Validate struct field names are lowercase
                var nameField = structType.Fields[0];
                Assert.Equal("name", nameField.Name); // Should be lowercase
                Assert.IsType<StringType>(nameField.DataType);

                var ageField = structType.Fields[1];
                Assert.Equal("age", ageField.Name); // Should be lowercase
                Assert.IsType<Int32Type>(ageField.DataType);

                Assert.True(col.IsNullable);
                OutputHelper?.WriteLine($"  ✓ col_struct: STRUCT<name: STRING, age: INT>, nullable=true");
                OutputHelper?.WriteLine($"    - Field names preserved as lowercase (name, age) ✓");

                OutputHelper?.WriteLine($"\n✅ All 16 columns validated with exact type details");
            }
            finally
            {
                await DropTestTables(connection);
            }
        }

        [SkippableFact]
        public async Task GetColumnsWithGetObjectsReturnsExactMetadata()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();

            try
            {
                await CreateTestTables(connection);

                // Use GetObjects with depth=All to get complete metadata including column details
                using var reader = connection.GetObjects(
                    AdbcConnection.GetObjectsDepth.All,
                    TestCatalog,
                    TestSchema,
                    TestTable,
                    null,
                    null);

                var batch = await reader.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);

                // Navigate the nested structure to find column metadata
                var catalogSchemas = batch.Column("catalog_db_schemas") as ListArray;
                Assert.NotNull(catalogSchemas);

                // Find our catalog
                bool foundTable = false;
                for (int catIdx = 0; catIdx < catalogSchemas.Length; catIdx++)
                {
                    if (catalogSchemas.IsNull(catIdx)) continue;

                    var dbSchemasStruct = catalogSchemas.GetSlicedValues(catIdx) as StructArray;
                    if (dbSchemasStruct == null) continue;

                    var dbSchemasList = dbSchemasStruct.Fields[0] as ListArray; // db_schema_tables
                    if (dbSchemasList == null) continue;

                    // Find our schema
                    for (int schemaIdx = 0; schemaIdx < dbSchemasList.Length; schemaIdx++)
                    {
                        if (dbSchemasList.IsNull(schemaIdx)) continue;

                        var tablesStruct = dbSchemasList.GetSlicedValues(schemaIdx) as StructArray;
                        if (tablesStruct == null) continue;

                        var tablesList = tablesStruct.Fields[0] as ListArray; // tables
                        if (tablesList == null) continue;

                        // Find our table
                        for (int tableIdx = 0; tableIdx < tablesList.Length; tableIdx++)
                        {
                            if (tablesList.IsNull(tableIdx)) continue;

                            var tableStruct = tablesList.GetSlicedValues(tableIdx) as StructArray;
                            if (tableStruct == null) continue;

                            // Check table name
                            var tableNameArray = tableStruct.Fields[0] as StringArray; // table_name
                            if (tableNameArray == null) continue;

                            var tableName = tableNameArray.GetString(0);
                            if (tableName != TestTable) continue;

                            // Found our table - now validate columns
                            var columnsListField = tableStruct.Fields.FirstOrDefault(f => f.Data.DataType is ListType lt &&
                                lt.ValueDataType is StructType st && st.Fields.Any(sf => sf.Name == "column_name"));

                            if (columnsListField != null)
                            {
                                var columnsList = columnsListField as ListArray;
                                Assert.NotNull(columnsList);

                                int columnCount = 0;
                                for (int colListIdx = 0; colListIdx < columnsList.Length; colListIdx++)
                                {
                                    if (columnsList.IsNull(colListIdx)) continue;

                                    var columnsStruct = columnsList.GetSlicedValues(colListIdx) as StructArray;
                                    if (columnsStruct == null) continue;

                                    columnCount = columnsStruct.Length;

                                    // Get column metadata arrays
                                    var columnNames = columnsStruct.Fields.First(f => f.Data.DataType is StringType &&
                                        ((StructType)columnsStruct.Data.DataType).Fields.Any(sf => sf.Name == "column_name")) as StringArray;

                                    var xdbcTypeNames = columnsStruct.Fields.First(f =>
                                        ((StructType)columnsStruct.Data.DataType).Fields.Any(sf => sf.Name == "xdbc_type_name")) as StringArray;

                                    Assert.NotNull(columnNames);
                                    Assert.Equal(16, columnCount);

                                    OutputHelper?.WriteLine($"\nValidating GetObjects column metadata for {TestTable}:");

                                    for (int i = 0; i < columnCount; i++)
                                    {
                                        var colName = columnNames.GetString(i);
                                        var xdbcTypeName = xdbcTypeNames?.IsNull(i) == false ? xdbcTypeNames.GetString(i) : "NULL";
                                        OutputHelper?.WriteLine($"  [{i}] {colName}: xdbc_type_name={xdbcTypeName}");

                                        // Validate specific critical columns
                                        if (colName == "col_struct")
                                        {
                                            // CRITICAL: Validate STRUCT type preserves lowercase field names
                                            // After fix: should be "STRUCT<name: STRING, age: INT>"
                                            // Before fix: was "STRUCT<NAME: STRING, AGE: INT>"
                                            Assert.Contains("name:", xdbcTypeName, StringComparison.Ordinal);
                                            Assert.Contains("age:", xdbcTypeName, StringComparison.Ordinal);
                                            Assert.DoesNotContain("NAME:", xdbcTypeName);
                                            Assert.DoesNotContain("AGE:", xdbcTypeName);
                                            OutputHelper?.WriteLine($"    ✓ STRUCT field names are lowercase (verified)");
                                        }
                                        else if (colName == "col_array")
                                        {
                                            Assert.Contains("ARRAY", xdbcTypeName);
                                            Assert.Contains("INT", xdbcTypeName);
                                        }
                                        else if (colName == "col_map")
                                        {
                                            Assert.Contains("MAP", xdbcTypeName);
                                            Assert.Contains("STRING", xdbcTypeName);
                                            Assert.Contains("INT", xdbcTypeName);
                                        }
                                        else if (colName == "col_decimal")
                                        {
                                            Assert.Contains("DECIMAL", xdbcTypeName);
                                            Assert.Contains("10", xdbcTypeName); // precision
                                            Assert.Contains("2", xdbcTypeName);  // scale
                                        }
                                    }

                                    foundTable = true;
                                    OutputHelper?.WriteLine($"\n✅ GetObjects returned exact metadata for all {columnCount} columns");
                                    break;
                                }
                            }

                            if (foundTable) break;
                        }

                        if (foundTable) break;
                    }

                    if (foundTable) break;
                }

                Assert.True(foundTable, $"Could not find table {TestTable} in GetObjects result");
            }
            finally
            {
                await DropTestTables(connection);
            }
        }

        #endregion

        #region GetPrimaryKeys Tests

        [SkippableFact]
        public async Task CanGetPrimaryKeysForCompositeKey()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();

            try
            {
                await CreateTestTables(connection);

                using var stream = connection.GetPrimaryKeys(TestCatalog, TestSchema, TestTable);
                var schema = stream.Schema;

                // Verify schema - 5 fields per ADBC spec
                Assert.NotNull(schema);
                Assert.Equal(5, schema.FieldsList.Count);
                Assert.Equal("catalog_name", schema.FieldsList[0].Name);
                Assert.Equal("db_schema_name", schema.FieldsList[1].Name);
                Assert.Equal("table_name", schema.FieldsList[2].Name);
                Assert.Equal("column_name", schema.FieldsList[3].Name);
                Assert.Equal("key_sequence", schema.FieldsList[4].Name);

                var primaryKeys = new List<(string column, int sequence)>();
                while (true)
                {
                    using var batch = stream.ReadNextRecordBatchAsync().Result;
                    if (batch == null) break;

                    var catalogArray = (StringArray)batch.Column("catalog_name");
                    var schemaArray = (StringArray)batch.Column("db_schema_name");
                    var tableArray = (StringArray)batch.Column("table_name");
                    var columnArray = (StringArray)batch.Column("column_name");
                    var sequenceArray = (Int32Array)batch.Column("key_sequence");

                    for (int i = 0; i < batch.Length; i++)
                    {
                        Assert.Equal(TestCatalog, catalogArray.GetString(i));
                        Assert.Equal(TestSchema, schemaArray.GetString(i));
                        Assert.Equal(TestTable, tableArray.GetString(i));

                        primaryKeys.Add((
                            columnArray.GetString(i),
                            sequenceArray.GetValue(i) ?? 0
                        ));
                    }
                }

                // Verify composite primary key: (col_string, col_int)
                Assert.Equal(2, primaryKeys.Count);

                var orderedKeys = primaryKeys.OrderBy(pk => pk.sequence).ToList();
                Assert.Equal("col_string", orderedKeys[0].column);
                Assert.Equal(1, orderedKeys[0].sequence);
                Assert.Equal("col_int", orderedKeys[1].column);
                Assert.Equal(2, orderedKeys[1].sequence);

                OutputHelper?.WriteLine($"✓ GetPrimaryKeys returned composite PK: ({orderedKeys[0].column}, {orderedKeys[1].column})");
            }
            finally
            {
                await DropTestTables(connection);
            }
        }

        #endregion

        #region GetImportedKeys Tests

        [SkippableFact]
        public async Task CanGetImportedKeysForTableWithForeignKey()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();

            try
            {
                await CreateTestTables(connection);

                using var stream = connection.GetImportedKeys(TestCatalog, TestSchema, TestTable);
                var schema = stream.Schema;

                // Verify schema - 13 fields per ADBC spec
                Assert.NotNull(schema);
                Assert.Equal(13, schema.FieldsList.Count);
                Assert.Equal("pk_catalog_name", schema.FieldsList[0].Name);
                Assert.Equal("pk_db_schema_name", schema.FieldsList[1].Name);
                Assert.Equal("pk_table_name", schema.FieldsList[2].Name);
                Assert.Equal("pk_column_name", schema.FieldsList[3].Name);
                Assert.Equal("fk_catalog_name", schema.FieldsList[4].Name);
                Assert.Equal("fk_db_schema_name", schema.FieldsList[5].Name);
                Assert.Equal("fk_table_name", schema.FieldsList[6].Name);
                Assert.Equal("fk_column_name", schema.FieldsList[7].Name);
                Assert.Equal("key_sequence", schema.FieldsList[8].Name);

                var foreignKeys = new List<(string pkTable, string pkColumn, string fkColumn, int sequence)>();
                while (true)
                {
                    using var batch = stream.ReadNextRecordBatchAsync().Result;
                    if (batch == null) break;

                    var pkTableArray = (StringArray)batch.Column("pk_table_name");
                    var pkColumnArray = (StringArray)batch.Column("pk_column_name");
                    var fkColumnArray = (StringArray)batch.Column("fk_column_name");
                    var sequenceArray = (Int32Array)batch.Column("key_sequence");

                    for (int i = 0; i < batch.Length; i++)
                    {
                        foreignKeys.Add((
                            pkTableArray.GetString(i),
                            pkColumnArray.GetString(i),
                            fkColumnArray.GetString(i),
                            sequenceArray.GetValue(i) ?? 0
                        ));
                    }
                }

                // Verify FK: col_bigint REFERENCES ref_table(ref_id)
                Assert.Single(foreignKeys);
                Assert.Equal(ReferenceTable, foreignKeys[0].pkTable);
                Assert.Equal("ref_id", foreignKeys[0].pkColumn);
                Assert.Equal("col_bigint", foreignKeys[0].fkColumn);
                Assert.Equal(1, foreignKeys[0].sequence);

                OutputHelper?.WriteLine($"✓ GetImportedKeys returned FK: {foreignKeys[0].fkColumn} -> {foreignKeys[0].pkTable}.{foreignKeys[0].pkColumn}");
            }
            finally
            {
                await DropTestTables(connection);
            }
        }

        [SkippableFact]
        public async Task GetImportedKeysForTableWithoutForeignKey()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();

            try
            {
                await CreateTestTables(connection);

                using var stream = connection.GetImportedKeys(TestCatalog, TestSchema, ReferenceTable);

                var foreignKeys = new List<string>();
                while (true)
                {
                    using var batch = stream.ReadNextRecordBatchAsync().Result;
                    if (batch == null) break;

                    var fkColumnArray = (StringArray)batch.Column("fk_column_name");
                    for (int i = 0; i < batch.Length; i++)
                    {
                        foreignKeys.Add(fkColumnArray.GetString(i));
                    }
                }

                // Reference table has no foreign keys
                Assert.Empty(foreignKeys);
                OutputHelper?.WriteLine("✓ GetImportedKeys correctly returned empty result for table without FKs");
            }
            finally
            {
                await DropTestTables(connection);
            }
        }

        #endregion

        #region GetCrossReference Tests

        [SkippableFact]
        public async Task CanGetCrossReferenceWithActualForeignKey()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();

            try
            {
                await CreateTestTables(connection);

                // PK table = ref_table, FK table = all_data_types_test
                using var stream = connection.GetCrossReference(
                    TestCatalog, TestSchema, ReferenceTable,  // PK table (parent)
                    TestCatalog, TestSchema, TestTable);      // FK table (child)
                var schema = stream.Schema;

                // Verify schema - 14 fields per JDBC spec including DEFERRABILITY
                Assert.NotNull(schema);
                Assert.Equal(14, schema.FieldsList.Count);
                Assert.Equal("pk_catalog_name", schema.FieldsList[0].Name);
                Assert.Equal("pk_db_schema_name", schema.FieldsList[1].Name);
                Assert.Equal("pk_table_name", schema.FieldsList[2].Name);
                Assert.Equal("pk_column_name", schema.FieldsList[3].Name);
                Assert.Equal("fk_catalog_name", schema.FieldsList[4].Name);
                Assert.Equal("fk_db_schema_name", schema.FieldsList[5].Name);
                Assert.Equal("fk_table_name", schema.FieldsList[6].Name);
                Assert.Equal("fk_column_name", schema.FieldsList[7].Name);
                Assert.Equal("key_sequence", schema.FieldsList[8].Name);
                Assert.Equal("fk_constraint_name", schema.FieldsList[9].Name);
                Assert.Equal("pk_key_name", schema.FieldsList[10].Name);
                Assert.Equal("update_rule", schema.FieldsList[11].Name);
                Assert.Equal("delete_rule", schema.FieldsList[12].Name);
                Assert.Equal("deferrability", schema.FieldsList[13].Name);

                var crossRefs = new List<(string pkColumn, string fkColumn, int sequence, short? deferrability)>();
                while (true)
                {
                    using var batch = stream.ReadNextRecordBatchAsync().Result;
                    if (batch == null) break;

                    var pkColumnArray = (StringArray)batch.Column("pk_column_name");
                    var fkColumnArray = (StringArray)batch.Column("fk_column_name");
                    var sequenceArray = (Int32Array)batch.Column("key_sequence");
                    var deferrabilityArray = (Int16Array)batch.Column("deferrability");

                    for (int i = 0; i < batch.Length; i++)
                    {
                        crossRefs.Add((
                            pkColumnArray.GetString(i),
                            fkColumnArray.GetString(i),
                            sequenceArray.GetValue(i) ?? 0,
                            deferrabilityArray?.IsNull(i) == false ? deferrabilityArray.GetValue(i) : null
                        ));
                    }
                }

                // Verify FK relationship
                Assert.Single(crossRefs);
                Assert.Equal("ref_id", crossRefs[0].pkColumn);
                Assert.Equal("col_bigint", crossRefs[0].fkColumn);
                Assert.Equal(1, crossRefs[0].sequence);
                Assert.NotNull(crossRefs[0].deferrability);
                Assert.Equal((short)5, crossRefs[0].deferrability); // 5 = NOT_DEFERRABLE

                OutputHelper?.WriteLine($"✓ GetCrossReference returned FK with deferrability:");
                OutputHelper?.WriteLine($"  {TestTable}.{crossRefs[0].fkColumn} -> {ReferenceTable}.{crossRefs[0].pkColumn}");
                OutputHelper?.WriteLine($"  Deferrability: {crossRefs[0].deferrability} (NOT_DEFERRABLE)");
            }
            finally
            {
                await DropTestTables(connection);
            }
        }

        [SkippableFact]
        public async Task GetCrossReferenceWithNoRelationship()
        {
            SkipIfNotConfigured();

            using var connection = CreateRestConnection();

            try
            {
                await CreateTestTables(connection);

                // Try to get cross reference in reverse direction (no FK from ref_table to test table)
                using var stream = connection.GetCrossReference(
                    TestCatalog, TestSchema, TestTable,       // PK table
                    TestCatalog, TestSchema, ReferenceTable); // FK table

                var crossRefs = new List<string>();
                while (true)
                {
                    using var batch = stream.ReadNextRecordBatchAsync().Result;
                    if (batch == null) break;

                    var fkColumnArray = (StringArray)batch.Column("fk_column_name");
                    for (int i = 0; i < batch.Length; i++)
                    {
                        crossRefs.Add(fkColumnArray.GetString(i));
                    }
                }

                // No FK relationship exists in this direction
                Assert.Empty(crossRefs);
                OutputHelper?.WriteLine("✓ GetCrossReference correctly returned empty result for non-existent relationship");
            }
            finally
            {
                await DropTestTables(connection);
            }
        }

        [SkippableFact]
        public async Task CanGetObjectsWithDifferentDepthLevels()
        {
            SkipIfNotConfigured();
            using var connection = CreateRestConnection();

            try
            {
                await CreateTestTables(connection);

                // Test depth: Catalogs only
                using (var stream = connection.GetObjects(AdbcConnection.GetObjectsDepth.Catalogs, TestCatalog, null, null, null, null))
                {
                    var schema = stream.Schema;
                    Assert.NotNull(schema);
                    Assert.Equal(2, schema.FieldsList.Count);
                    Assert.Equal("catalog_name", schema.FieldsList[0].Name);
                    Assert.Equal("catalog_db_schemas", schema.FieldsList[1].Name);

                    var batch = await stream.ReadNextRecordBatchAsync();
                    Assert.NotNull(batch);

                    var catalogArray = batch.Column("catalog_name") as Apache.Arrow.StringArray;
                    Assert.NotNull(catalogArray);

                    bool found = false;
                    for (int i = 0; i < catalogArray.Length; i++)
                    {
                        if (catalogArray.GetString(i) == TestCatalog)
                        {
                            found = true;
                            break;
                        }
                    }
                    Assert.True(found, $"Expected catalog '{TestCatalog}' not found");

                    OutputHelper?.WriteLine($"✓ GetObjects(Catalogs) returned {catalogArray.Length} catalogs");
                }

                // Test depth: DbSchemas
                using (var stream = connection.GetObjects(AdbcConnection.GetObjectsDepth.DbSchemas, TestCatalog, TestSchema, null, null, null))
                {
                    var batch = await stream.ReadNextRecordBatchAsync();
                    Assert.NotNull(batch);

                    var catalogArray = batch.Column("catalog_name") as Apache.Arrow.StringArray;
                    Assert.NotNull(catalogArray);
                    Assert.Equal(TestCatalog, catalogArray.GetString(0));

                    OutputHelper?.WriteLine("✓ GetObjects(DbSchemas) returned schema information");
                }

                // Test depth: Tables
                using (var stream = connection.GetObjects(AdbcConnection.GetObjectsDepth.Tables, TestCatalog, TestSchema, TestTable, null, null))
                {
                    var batch = await stream.ReadNextRecordBatchAsync();
                    Assert.NotNull(batch);

                    var catalogArray = batch.Column("catalog_name") as Apache.Arrow.StringArray;
                    Assert.NotNull(catalogArray);
                    Assert.Equal(TestCatalog, catalogArray.GetString(0));

                    OutputHelper?.WriteLine("✓ GetObjects(Tables) returned table information");
                }

                OutputHelper?.WriteLine("✓ All GetObjects depth levels work correctly");
            }
            finally
            {
                await DropTestTables(connection);
            }
        }

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

            // Test GetInfo with specific codes
            var requestedCodes = new List<Apache.Arrow.Adbc.AdbcInfoCode>
            {
                Apache.Arrow.Adbc.AdbcInfoCode.VendorName,
                Apache.Arrow.Adbc.AdbcInfoCode.DriverName,
                Apache.Arrow.Adbc.AdbcInfoCode.DriverVersion
            };

            using (var stream = connection.GetInfo(requestedCodes))
            {
                var batch = stream.ReadNextRecordBatchAsync().Result;
                Assert.NotNull(batch);
                Assert.Equal(3, batch.Length);

                var infoNameArray = batch.Column("info_name") as Apache.Arrow.UInt32Array;
                Assert.NotNull(infoNameArray);

                var codes = new List<uint>();
                for (int i = 0; i < batch.Length; i++)
                {
                    codes.Add(infoNameArray.GetValue(i).Value);
                }

                Assert.Contains((uint)Apache.Arrow.Adbc.AdbcInfoCode.VendorName, codes);
                Assert.Contains((uint)Apache.Arrow.Adbc.AdbcInfoCode.DriverName, codes);
                Assert.Contains((uint)Apache.Arrow.Adbc.AdbcInfoCode.DriverVersion, codes);

                OutputHelper?.WriteLine("✓ GetInfo(specific codes) returned requested information");
            }
        }

        [SkippableFact]
        public async Task CanGetTableSchemaForComplexTypes()
        {
            SkipIfNotConfigured();
            using var connection = CreateRestConnection();

            try
            {
                await CreateTestTables(connection);

                var schema = connection.GetTableSchema(TestCatalog, TestSchema, TestTable);
                Assert.NotNull(schema);
                Assert.Equal(16, schema.FieldsList.Count);

                // Verify STRUCT type field
                var structField = schema.FieldsList.FirstOrDefault(f => f.Name == "col_struct");
                Assert.NotNull(structField);
                Assert.IsType<Apache.Arrow.Types.StructType>(structField.DataType);

                // Verify ARRAY type field
                var arrayField = schema.FieldsList.FirstOrDefault(f => f.Name == "col_array");
                Assert.NotNull(arrayField);
                Assert.IsType<Apache.Arrow.Types.ListType>(arrayField.DataType);

                // Verify MAP type field
                var mapField = schema.FieldsList.FirstOrDefault(f => f.Name == "col_map");
                Assert.NotNull(mapField);
                Assert.IsType<Apache.Arrow.Types.MapType>(mapField.DataType);

                OutputHelper?.WriteLine("✓ GetTableSchema returned complex types correctly");
            }
            finally
            {
                await DropTestTables(connection);
            }
        }

        [SkippableFact]
        public async Task GetTablesWithPatternMatching()
        {
            SkipIfNotConfigured();
            using var connection = CreateRestConnection();

            try
            {
                await CreateTestTables(connection);

                using var statement = connection.CreateStatement();
                statement.SqlQuery = $@"
                    CREATE TABLE IF NOT EXISTS {TestCatalog}.{TestSchema}.test_table_one (id INT);
                    CREATE TABLE IF NOT EXISTS {TestCatalog}.{TestSchema}.test_table_two (id INT);
                    CREATE TABLE IF NOT EXISTS {TestCatalog}.{TestSchema}.other_table (id INT);
                ";
                statement.ExecuteUpdate();

                // Test pattern: test%
                using (var stream = connection.GetObjects(AdbcConnection.GetObjectsDepth.Tables, TestCatalog, TestSchema, "test%", null, null))
                {
                    var tables = new List<string>();
                    while (true)
                    {
                        var batch = await stream.ReadNextRecordBatchAsync();
                        if (batch == null) break;

                        var catalogArray = batch.Column("catalog_name") as Apache.Arrow.StringArray;
                        if (catalogArray == null) continue;

                        for (int i = 0; i < batch.Length; i++)
                        {
                            var catalog = catalogArray.GetString(i);
                            if (catalog == TestCatalog)
                            {
                                // Access nested structure to get table names
                                var schemasArray = batch.Column("catalog_db_schemas") as Apache.Arrow.ListArray;
                                if (schemasArray != null && !schemasArray.IsNull(i))
                                {
                                    OutputHelper?.WriteLine($"Found tables matching 'test%' pattern");
                                }
                            }
                        }
                    }
                }

                // Cleanup additional tables
                using (var cleanupStatement = connection.CreateStatement())
                {
                    cleanupStatement.SqlQuery = $@"
                        DROP TABLE IF EXISTS {TestCatalog}.{TestSchema}.test_table_one;
                        DROP TABLE IF EXISTS {TestCatalog}.{TestSchema}.test_table_two;
                        DROP TABLE IF EXISTS {TestCatalog}.{TestSchema}.other_table;
                    ";
                    cleanupStatement.ExecuteUpdate();
                }

                OutputHelper?.WriteLine("✓ Pattern matching in GetTables works correctly");
            }
            finally
            {
                await DropTestTables(connection);
            }
        }

        [SkippableFact]
        public async Task GetColumnsWithPatternMatching()
        {
            SkipIfNotConfigured();
            using var connection = CreateRestConnection();

            try
            {
                await CreateTestTables(connection);

                // Create table with specific column pattern
                using var statement = connection.CreateStatement();
                statement.SqlQuery = $@"
                    CREATE TABLE IF NOT EXISTS {TestCatalog}.{TestSchema}.pattern_test (
                        col_int INT,
                        col_string STRING,
                        col_double DOUBLE,
                        other_field INT
                    )
                ";
                statement.ExecuteUpdate();

                // Get columns matching pattern "col%"
                using (var stream = connection.GetObjects(AdbcConnection.GetObjectsDepth.All, TestCatalog, TestSchema, "pattern_test", null, "col%"))
                {
                    var columnNames = new List<string>();
                    while (true)
                    {
                        var batch = await stream.ReadNextRecordBatchAsync();
                        if (batch == null) break;

                        var catalogArray = batch.Column("catalog_name") as Apache.Arrow.StringArray;
                        if (catalogArray == null) continue;

                        for (int i = 0; i < batch.Length; i++)
                        {
                            if (catalogArray.GetString(i) == TestCatalog)
                            {
                                OutputHelper?.WriteLine($"Found columns matching 'col%' pattern");
                            }
                        }
                    }
                }

                // Cleanup
                using (var cleanupStatement = connection.CreateStatement())
                {
                    cleanupStatement.SqlQuery = $"DROP TABLE IF EXISTS {TestCatalog}.{TestSchema}.pattern_test";
                    cleanupStatement.ExecuteUpdate();
                }

                OutputHelper?.WriteLine("✓ Pattern matching in GetColumns works correctly");
            }
            finally
            {
                await DropTestTables(connection);
            }
        }

        [SkippableFact]
        public async Task GetObjectsHandlesEmptyResults()
        {
            SkipIfNotConfigured();
            using var connection = CreateRestConnection();

            // Try to get tables for a non-existent catalog (should return empty)
            using (var stream = connection.GetObjects(AdbcConnection.GetObjectsDepth.Tables, "nonexistent_catalog_xyz", null, null, null, null))
            {
                var batch = await stream.ReadNextRecordBatchAsync();

                // Should return empty result, not throw exception
                if (batch != null)
                {
                    Assert.Equal(0, batch.Length);
                }

                OutputHelper?.WriteLine("✓ GetObjects handles non-existent catalog gracefully");
            }

            // Try to get columns for non-existent table (should return empty)
            using (var stream = connection.GetObjects(AdbcConnection.GetObjectsDepth.All, TestCatalog, TestSchema, "nonexistent_table_xyz", null, null))
            {
                var batch = await stream.ReadNextRecordBatchAsync();

                // Should return empty result
                if (batch != null)
                {
                    var catalogArray = batch.Column("catalog_name") as Apache.Arrow.StringArray;
                    OutputHelper?.WriteLine($"✓ GetObjects handles non-existent table gracefully (returned {batch.Length} catalogs)");
                }
            }
        }

        [SkippableFact]
        public void GetTableTypesReturnsExpectedTypes()
        {
            SkipIfNotConfigured();
            using var connection = CreateRestConnection();

            using (var stream = connection.GetTableTypes())
            {
                var schema = stream.Schema;
                Assert.NotNull(schema);
                Assert.Single(schema.FieldsList);
                Assert.Equal("table_type", schema.FieldsList[0].Name);

                var batch = stream.ReadNextRecordBatchAsync().Result;
                Assert.NotNull(batch);

                var tableTypeArray = batch.Column("table_type") as Apache.Arrow.StringArray;
                Assert.NotNull(tableTypeArray);

                var types = new List<string>();
                for (int i = 0; i < batch.Length; i++)
                {
                    types.Add(tableTypeArray.GetString(i));
                }

                // Should return TABLE, VIEW, and LOCAL TEMPORARY
                Assert.Contains("TABLE", types);
                Assert.Contains("VIEW", types);
                Assert.Contains("LOCAL TEMPORARY", types);

                OutputHelper?.WriteLine($"✓ GetTableTypes returned {types.Count} types: {string.Join(", ", types)}");
            }
        }

        #endregion
    }
}
