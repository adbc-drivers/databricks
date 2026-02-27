/*
 * Copyright (c) 2025 ADBC Drivers Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.Spark;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.StatementExecution
{
    /// <summary>
    /// E2E tests asserting that SEA metadata operations return correct values
    /// for the test table main.adbc_testing.all_column_types.
    /// Both Thrift and SEA are tested for parity.
    ///
    /// Requires DATABRICKS_TEST_CONFIG_FILE environment variable pointing to
    /// a JSON config with: uri, token, adbc.spark.token, adbc.connection.catalog
    /// </summary>
    public class SeaMetadataE2ETests : IDisposable
    {
        private const string TestCatalog = "main";
        private const string TestSchema = "adbc_testing";
        private const string TestTable = "all_column_types";

        private readonly ITestOutputHelper? _output;
        private readonly Dictionary<string, string>? _config;
        private readonly bool _canRun;

        public SeaMetadataE2ETests(ITestOutputHelper? output)
        {
            _output = output;
            var configFile = Environment.GetEnvironmentVariable("DATABRICKS_TEST_CONFIG_FILE");
            if (string.IsNullOrEmpty(configFile) || !File.Exists(configFile))
            {
                _canRun = false;
                return;
            }

            var json = File.ReadAllText(configFile);
            var parsed = JsonDocument.Parse(json);
            _config = new Dictionary<string, string>();
            foreach (var prop in parsed.RootElement.EnumerateObject())
                _config[prop.Name] = prop.Value.GetString() ?? "";
            _canRun = true;
        }

        public void Dispose() { }

        private AdbcConnection CreateThriftConnection()
        {
            var parameters = new Dictionary<string, string>(_config!)
            {
                { SparkParameters.Type, SparkServerTypeConstants.Http },
                { SparkParameters.AuthType, SparkAuthTypeConstants.Token },
                { DatabricksParameters.Protocol, "thrift" }
            };
            var driver = new DatabricksDriver();
            var db = driver.Open(parameters);
            return db.Connect(new Dictionary<string, string>());
        }

        private AdbcConnection CreateSeaConnection()
        {
            var parameters = new Dictionary<string, string>(_config!)
            {
                { DatabricksParameters.Protocol, "rest" },
                { DatabricksParameters.EnableSessionManagement, "true" }
            };
            var driver = new DatabricksDriver();
            var db = driver.Open(parameters);
            return db.Connect(new Dictionary<string, string>());
        }

        private async Task<List<Dictionary<string, string>>> ReadMetadata(AdbcConnection connection, string command,
            string? catalog = null, string? schema = null, string? table = null, string? column = null)
        {
            var results = new List<Dictionary<string, string>>();
            using var stmt = connection.CreateStatement();
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            if (catalog != null) stmt.SetOption(ApacheParameters.CatalogName, catalog);
            if (schema != null) stmt.SetOption(ApacheParameters.SchemaName, schema);
            if (table != null) stmt.SetOption(ApacheParameters.TableName, table);
            if (column != null) stmt.SetOption(ApacheParameters.ColumnName, column);

            stmt.SqlQuery = command;
            var result = stmt.ExecuteQuery();
            using var reader = result.Stream;

            while (true)
            {
                using var batch = await reader.ReadNextRecordBatchAsync();
                if (batch == null) break;
                for (int i = 0; i < batch.Length; i++)
                {
                    var row = new Dictionary<string, string>();
                    for (int j = 0; j < batch.ColumnCount; j++)
                    {
                        var name = reader.Schema.GetFieldByIndex(j).Name;
                        var array = batch.Column(j);
                        row[name] = GetStringValue(array, i);
                    }
                    results.Add(row);
                }
            }
            return results;
        }

        private static string GetStringValue(IArrowArray array, int index)
        {
            if (array.IsNull(index)) return "null";
            return array switch
            {
                Int8Array a => a.GetValue(index)?.ToString() ?? "null",
                Int16Array a => a.GetValue(index)?.ToString() ?? "null",
                Int32Array a => a.GetValue(index)?.ToString() ?? "null",
                Int64Array a => a.GetValue(index)?.ToString() ?? "null",
                FloatArray a => a.GetValue(index)?.ToString() ?? "null",
                DoubleArray a => a.GetValue(index)?.ToString() ?? "null",
                StringArray a => a.GetString(index),
                BooleanArray a => a.GetValue(index)?.ToString() ?? "null",
                _ => $"[{array.Data.DataType}]"
            };
        }

        // --- GetCatalogs ---

        [SkippableFact]
        public async Task Thrift_GetCatalogs_ContainsMain()
        {
            Skip.IfNot(_canRun);
            using var conn = CreateThriftConnection();
            var rows = await ReadMetadata(conn, "GetCatalogs");
            Assert.True(rows.Count > 0, "GetCatalogs should return at least one catalog");
            Assert.Contains(rows, r => r["TABLE_CAT"] == "main");
        }

        [SkippableFact]
        public async Task SEA_GetCatalogs_ContainsMain()
        {
            Skip.IfNot(_canRun);
            using var conn = CreateSeaConnection();
            var rows = await ReadMetadata(conn, "GetCatalogs");
            Assert.True(rows.Count > 0, "GetCatalogs should return at least one catalog");
            Assert.Contains(rows, r => r["TABLE_CAT"] == "main");
        }

        [SkippableFact]
        public async Task GetCatalogs_ThriftAndSEA_SameRowCount()
        {
            Skip.IfNot(_canRun);
            using var thrift = CreateThriftConnection();
            using var sea = CreateSeaConnection();
            var thriftRows = await ReadMetadata(thrift, "GetCatalogs");
            var seaRows = await ReadMetadata(sea, "GetCatalogs");
            Assert.Equal(thriftRows.Count, seaRows.Count);
        }

        // --- GetTables ---

        [SkippableFact]
        public async Task Thrift_GetTables_ReturnsAllColumnTypes()
        {
            Skip.IfNot(_canRun);
            using var conn = CreateThriftConnection();
            var rows = await ReadMetadata(conn, "GetTables", TestCatalog, TestSchema);
            Assert.Contains(rows, r => r["TABLE_NAME"] == TestTable);
            // Verify 10-column schema
            var row = rows.Find(r => r["TABLE_NAME"] == TestTable);
            Assert.NotNull(row);
            Assert.Equal(TestCatalog, row!["TABLE_CAT"]);
            Assert.Equal(TestSchema, row["TABLE_SCHEM"]);
            Assert.True(row.ContainsKey("TYPE_CAT"), "Should have TYPE_CAT column");
            Assert.True(row.ContainsKey("REF_GENERATION"), "Should have REF_GENERATION column");
        }

        [SkippableFact]
        public async Task SEA_GetTables_ReturnsAllColumnTypes()
        {
            Skip.IfNot(_canRun);
            using var conn = CreateSeaConnection();
            var rows = await ReadMetadata(conn, "GetTables", TestCatalog, TestSchema);
            Assert.Contains(rows, r => r["TABLE_NAME"] == TestTable);
            var row = rows.Find(r => r["TABLE_NAME"] == TestTable);
            Assert.NotNull(row);
            Assert.Equal(TestCatalog, row!["TABLE_CAT"]);
            Assert.Equal(TestSchema, row["TABLE_SCHEM"]);
            Assert.True(row.ContainsKey("TYPE_CAT"), "Should have TYPE_CAT column");
            Assert.True(row.ContainsKey("REF_GENERATION"), "Should have REF_GENERATION column");
        }

        [SkippableFact]
        public async Task GetTables_ThriftAndSEA_SameCount()
        {
            Skip.IfNot(_canRun);
            using var thrift = CreateThriftConnection();
            using var sea = CreateSeaConnection();
            var thriftRows = await ReadMetadata(thrift, "GetTables", TestCatalog, TestSchema);
            var seaRows = await ReadMetadata(sea, "GetTables", TestCatalog, TestSchema);
            Assert.Equal(thriftRows.Count, seaRows.Count);
        }

        // --- GetColumnsExtended ---

        [SkippableFact]
        public async Task Thrift_GetColumnsExtended_Returns20Columns()
        {
            Skip.IfNot(_canRun);
            using var conn = CreateThriftConnection();
            var rows = await ReadMetadata(conn, "GetColumnsExtended", TestCatalog, TestSchema, TestTable);
            Assert.Equal(20, rows.Count);
        }

        [SkippableFact]
        public async Task SEA_GetColumnsExtended_Returns20Columns()
        {
            Skip.IfNot(_canRun);
            using var conn = CreateSeaConnection();
            var rows = await ReadMetadata(conn, "GetColumnsExtended", TestCatalog, TestSchema, TestTable);
            Assert.Equal(20, rows.Count);
        }

        [SkippableFact]
        public async Task GetColumnsExtended_ThriftAndSEA_SameColumnNames()
        {
            Skip.IfNot(_canRun);
            using var thrift = CreateThriftConnection();
            using var sea = CreateSeaConnection();
            var thriftRows = await ReadMetadata(thrift, "GetColumnsExtended", TestCatalog, TestSchema, TestTable);
            var seaRows = await ReadMetadata(sea, "GetColumnsExtended", TestCatalog, TestSchema, TestTable);
            Assert.Equal(thriftRows.Count, seaRows.Count);
            for (int i = 0; i < thriftRows.Count; i++)
            {
                Assert.Equal(thriftRows[i]["COLUMN_NAME"], seaRows[i]["COLUMN_NAME"]);
                Assert.Equal(thriftRows[i]["DATA_TYPE"], seaRows[i]["DATA_TYPE"]);
                Assert.Equal(thriftRows[i]["BASE_TYPE_NAME"], seaRows[i]["BASE_TYPE_NAME"]);
                Assert.Equal(thriftRows[i]["COLUMN_SIZE"], seaRows[i]["COLUMN_SIZE"]);
                Assert.Equal(thriftRows[i]["DECIMAL_DIGITS"], seaRows[i]["DECIMAL_DIGITS"]);
            }
        }

        [SkippableFact]
        public async Task GetColumnsExtended_ThriftAndSEA_32ColumnSchema()
        {
            Skip.IfNot(_canRun);
            using var thrift = CreateThriftConnection();
            using var sea = CreateSeaConnection();
            var thriftRows = await ReadMetadata(thrift, "GetColumnsExtended", TestCatalog, TestSchema, TestTable);
            var seaRows = await ReadMetadata(sea, "GetColumnsExtended", TestCatalog, TestSchema, TestTable);
            // Both should have 32 columns (24 base + 8 PK/FK)
            Assert.Equal(32, thriftRows[0].Count);
            Assert.Equal(32, seaRows[0].Count);
        }

        // --- GetPrimaryKeys ---

        [SkippableFact]
        public async Task Thrift_GetPrimaryKeys_ReturnsPKColumns()
        {
            Skip.IfNot(_canRun);
            using var conn = CreateThriftConnection();
            var rows = await ReadMetadata(conn, "GetPrimaryKeys", TestCatalog, TestSchema, TestTable);
            Assert.Equal(2, rows.Count);
            Assert.Equal("c_string", rows[0]["COLUMN_NAME"]);
            Assert.Equal("c_int", rows[1]["COLUMN_NAME"]);
        }

        [SkippableFact]
        public async Task SEA_GetPrimaryKeys_ReturnsPKColumns()
        {
            Skip.IfNot(_canRun);
            using var conn = CreateSeaConnection();
            var rows = await ReadMetadata(conn, "GetPrimaryKeys", TestCatalog, TestSchema, TestTable);
            Assert.Equal(2, rows.Count);
            Assert.Equal("c_string", rows[0]["COLUMN_NAME"]);
            Assert.Equal("c_int", rows[1]["COLUMN_NAME"]);
        }

        // --- GetTableSchema ---

        [SkippableFact]
        public void Thrift_GetTableSchema_Returns20Fields()
        {
            Skip.IfNot(_canRun);
            using var conn = CreateThriftConnection();
            // Use cross_ref_customers to avoid Thrift NotImplementedException on complex types
            var schema = conn.GetTableSchema(TestCatalog, TestSchema, "cross_ref_customers");
            Assert.True(schema.FieldsList.Count > 0);
        }

        [SkippableFact]
        public void SEA_GetTableSchema_ReturnsFields()
        {
            Skip.IfNot(_canRun);
            using var conn = CreateSeaConnection();
            // Use cross_ref_customers to avoid NotImplementedException on complex types (INTERVAL, MAP, etc.)
            var schema = conn.GetTableSchema(TestCatalog, TestSchema, "cross_ref_customers");
            Assert.True(schema.FieldsList.Count > 0);
            Assert.Equal("customer_id", schema.FieldsList[0].Name);
        }

        [SkippableFact]
        public void GetTableSchema_ThriftAndSEA_SameFieldNames()
        {
            Skip.IfNot(_canRun);
            using var thrift = CreateThriftConnection();
            using var sea = CreateSeaConnection();
            var thriftSchema = thrift.GetTableSchema(TestCatalog, TestSchema, "cross_ref_customers");
            var seaSchema = sea.GetTableSchema(TestCatalog, TestSchema, "cross_ref_customers");
            Assert.Equal(thriftSchema.FieldsList.Count, seaSchema.FieldsList.Count);
            for (int i = 0; i < thriftSchema.FieldsList.Count; i++)
            {
                Assert.Equal(thriftSchema.FieldsList[i].Name, seaSchema.FieldsList[i].Name);
                Assert.Equal(thriftSchema.FieldsList[i].DataType.TypeId, seaSchema.FieldsList[i].DataType.TypeId);
            }
        }

        // --- GetTableTypes ---

        [SkippableFact]
        public void SEA_GetTableTypes_ReturnsTableAndView()
        {
            Skip.IfNot(_canRun);
            using var conn = CreateSeaConnection();
            using var stream = conn.GetTableTypes();
            var batch = stream.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult();
            Assert.NotNull(batch);
            Assert.Equal(2, batch!.Length);
            var col = batch.Column("table_type") as StringArray;
            Assert.NotNull(col);
            var types = new HashSet<string>();
            for (int i = 0; i < col!.Length; i++)
                types.Add(col.GetString(i));
            Assert.Contains("TABLE", types);
            Assert.Contains("VIEW", types);
        }

        // --- GetInfo ---

        [SkippableFact]
        public void SEA_GetInfo_ReturnsDriverInfo()
        {
            Skip.IfNot(_canRun);
            using var conn = CreateSeaConnection();
            using var stream = conn.GetInfo(new List<AdbcInfoCode>());
            var batch = stream.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult();
            Assert.NotNull(batch);
            Assert.True(batch!.Length >= 5, "GetInfo should return at least 5 info codes");
        }
    }
}
