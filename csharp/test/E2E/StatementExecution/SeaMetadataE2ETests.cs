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
using Apache.Arrow.Adbc.Tests;
using AdbcDrivers.HiveServer2;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.StatementExecution
{
    /// <summary>
    /// E2E tests asserting that SEA metadata operations return correct values
    /// for the test table main.adbc_testing.all_column_types.
    /// Both Thrift and SEA are tested for parity.
    /// </summary>
    public class SeaMetadataE2ETests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        private const string TestCatalog = "main";
        // Read-only fixture schema. CREATE/DROP/INSERT must use
        // TestConfiguration.Metadata.Schema instead.
        private const string TestSchema = DatabricksTestEnvironment.FixtureSchema;
        private const string TestTable = "all_column_types";

        // Golden GetColumnsExtended result for main.adbc_testing.all_column_types
        // (shared with StatementTests.CanGetColumnsExtended).
        private const string ExpectedColumnsResource = "Resources/result_get_column_extended_all_types.json";

        // Per-column metadata that must be identical on any protocol. BUFFER_LENGTH is
        // deliberately excluded: it differs between Thrift (Int8) and SEA (Int32) — see
        // PECO-3008 — so it is not protocol-invariant.
        private static readonly string[] ProtocolInvariantColumns =
        {
            "COLUMN_NAME", "DATA_TYPE", "BASE_TYPE_NAME", "COLUMN_SIZE", "DECIMAL_DIGITS",
        };

        public SeaMetadataE2ETests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
        }

        private void SkipIfNotConfigured()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable), "Test configuration not available");
        }

        // Connection on whatever protocol the test suite was configured with (driver
        // parameters / config file). Tests never pick the protocol themselves — the run
        // argument decides it, so the SEA/Reyden nightly runs them over REST and the
        // Thrift CI over Thrift. Each test asserts a fixed expected result, which must
        // hold for any protocol; running across the CI matrix gives cross-protocol parity
        // without comparing protocols inside a test.
        private AdbcConnection CreateConnection(Dictionary<string, string>? extraParams = null)
        {
            var parameters = GetDriverParameters(TestConfiguration);
            if (extraParams != null)
            {
                foreach (var kvp in extraParams)
                    parameters[kvp.Key] = kvp.Value;
            }
            var driver = new DatabricksDriver();
            var db = driver.Open(parameters);
            return db.Connect(new Dictionary<string, string>());
        }

        private async Task<List<Dictionary<string, string>>> ReadMetadata(AdbcConnection connection, string command,
            string? catalog = null, string? schema = null, string? table = null, string? column = null,
            string? tableTypes = null)
        {
            var results = new List<Dictionary<string, string>>();
            using var stmt = connection.CreateStatement();
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            if (catalog != null) stmt.SetOption(ApacheParameters.CatalogName, catalog);
            if (schema != null) stmt.SetOption(ApacheParameters.SchemaName, schema);
            if (table != null) stmt.SetOption(ApacheParameters.TableName, table);
            if (column != null) stmt.SetOption(ApacheParameters.ColumnName, column);
            if (tableTypes != null) stmt.SetOption(ApacheParameters.TableTypes, tableTypes);

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
        public async Task GetCatalogs_ContainsMain()
        {
            SkipIfNotConfigured();
            using var conn = CreateConnection();
            var rows = await ReadMetadata(conn, "GetCatalogs");
            Assert.True(rows.Count > 0, "GetCatalogs should return at least one catalog");
            Assert.Contains(rows, r => r["TABLE_CAT"] == "main");
        }

        // --- GetTables ---

        [SkippableFact]
        public async Task GetTables_ReturnsAllColumnTypes()
        {
            SkipIfNotConfigured();
            using var conn = CreateConnection();
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

        // Issue #526: the GetTables `types` filter must be case-SENSITIVE (exact match
        // against the server's uppercase type names, matching JDBC's
        // Arrays.asList(tableTypes).contains(row.get(3)) and the Thrift path). A lowercase
        // "table" filter must match NOTHING, while an uppercase "TABLE" filter still matches.
        // Both Thrift and SEA paths are case-sensitive, so the behavior is identical on
        // whatever protocol the run is configured with.
        [SkippableFact]
        public async Task GetTables_TypesFilter_IsCaseSensitive()
        {
            SkipIfNotConfigured();
            using var conn = CreateConnection();

            // Uppercase "TABLE" exactly matches the server type -> the table is returned.
            var upperRows = await ReadMetadata(conn, "GetTables", TestCatalog, TestSchema, TestTable, tableTypes: "TABLE");
            Assert.Contains(upperRows, r => r["TABLE_NAME"] == TestTable);

            // Lowercase "table" does NOT match the uppercase server type -> no rows.
            var lowerRows = await ReadMetadata(conn, "GetTables", TestCatalog, TestSchema, TestTable, tableTypes: "table");
            Assert.DoesNotContain(lowerRows, r => r["TABLE_NAME"] == TestTable);
        }

        // --- GetColumnsExtended ---

        [SkippableFact]
        public async Task GetColumnsExtended_ReturnsExpectedColumns()
        {
            SkipIfNotConfigured();
            using var conn = CreateConnection();
            var rows = await ReadMetadata(conn, "GetColumnsExtended", TestCatalog, TestSchema, TestTable);

            var expected = JsonSerializer.Deserialize<List<Dictionary<string, JsonElement>>>(
                File.ReadAllText(ExpectedColumnsResource))!;

            // One row per table column, in ordinal order, with the 32-column metadata
            // schema (24 base + 8 PK/FK fields).
            Assert.Equal(expected.Count, rows.Count);
            Assert.Equal(32, rows[0].Count);

            // Assert the protocol-invariant column metadata against the golden result, so
            // any protocol (Thrift or SEA) is checked against the same expected values.
            for (int i = 0; i < expected.Count; i++)
            {
                foreach (var col in ProtocolInvariantColumns)
                {
                    Assert.Equal(expected[i][col].ToString(), rows[i][col]);
                }
            }
        }

        [SkippableFact]
        public async Task GetColumnsExtended_FallbackAndDescTable_SameResults()
        {
            SkipIfNotConfigured();
            // adbc.databricks.use_desc_table_extended is honored on both protocols
            // (DatabricksConnection.CanUseDescTableExtended for Thrift, StatementExecution
            // for SEA), so the two paths must agree on whatever protocol the run uses.
            using var fallbackConn = CreateConnection(new Dictionary<string, string>
            {
                { DatabricksParameters.UseDescTableExtended, "false" }
            });
            using var descTableConn = CreateConnection(new Dictionary<string, string>
            {
                { DatabricksParameters.UseDescTableExtended, "true" }
            });
            var fallbackRows = await ReadMetadata(fallbackConn, "GetColumnsExtended", TestCatalog, TestSchema, TestTable);
            var descTableRows = await ReadMetadata(descTableConn, "GetColumnsExtended", TestCatalog, TestSchema, TestTable);

            Assert.Equal(fallbackRows.Count, descTableRows.Count);
            Assert.True(fallbackRows.Count > 0, "Should return at least one row");

            // Both paths should have the same schema width (32 columns: 24 base + 8 PK/FK)
            Assert.Equal(fallbackRows[0].Count, descTableRows[0].Count);

            // All columns consumed by the Power BI connector must match between the two paths
            var columnsToCompare = new[]
            {
                // Core column metadata (GetTableType, GetPowerQueryType)
                "COLUMN_NAME",
                "TYPE_NAME",
                "BASE_TYPE_NAME",
                "COLUMN_SIZE",
                "DECIMAL_DIGITS",
                "NULLABLE",
                // Primary key (GetTableType, GetRelationships)
                "PK_COLUMN_NAME",
                // Foreign key relationship fields (GetRelationships)
                "FK_FKCOLUMN_NAME",
                "FK_PKTABLE_CAT",
                "FK_PKTABLE_SCHEM",
                "FK_PKTABLE_NAME",
                "FK_PKCOLUMN_NAME",
                "FK_FK_NAME",
                "FK_KEQ_SEQ",
            };

            for (int i = 0; i < fallbackRows.Count; i++)
            {
                foreach (var col in columnsToCompare)
                {
                    Assert.True(fallbackRows[i].ContainsKey(col), $"Fallback row {i} missing column {col}");
                    Assert.True(descTableRows[i].ContainsKey(col), $"DescTable row {i} missing column {col}");
                    Assert.Equal(descTableRows[i][col], fallbackRows[i][col]);
                }
            }
        }

        // --- GetPrimaryKeys ---

        [SkippableFact]
        public async Task GetPrimaryKeys_ReturnsPKColumns()
        {
            SkipIfNotConfigured();
            using var conn = CreateConnection();
            var rows = await ReadMetadata(conn, "GetPrimaryKeys", TestCatalog, TestSchema, TestTable);
            Assert.Equal(2, rows.Count);
            Assert.Equal("c_string", rows[0]["COLUMN_NAME"]);
            Assert.Equal("c_int", rows[1]["COLUMN_NAME"]);
        }

        // --- GetTableSchema ---

        [SkippableFact]
        public void GetTableSchema_ReturnsFields()
        {
            SkipIfNotConfigured();
            using var conn = CreateConnection();
            // Use cross_ref_customers to avoid NotImplementedException on complex types (INTERVAL, MAP, etc.)
            var schema = conn.GetTableSchema(TestCatalog, TestSchema, "cross_ref_customers");
            Assert.True(schema.FieldsList.Count > 0);
            Assert.Equal("customer_id", schema.FieldsList[0].Name);
        }

        // --- GetTableTypes ---

        [SkippableFact]
        public void GetTableTypes_ReturnsTableAndView()
        {
            SkipIfNotConfigured();
            using var conn = CreateConnection();
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
        public void GetInfo_ReturnsDriverInfo()
        {
            SkipIfNotConfigured();
            using var conn = CreateConnection();
            using var stream = conn.GetInfo(new List<AdbcInfoCode>());
            var batch = stream.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult();
            Assert.NotNull(batch);
            Assert.True(batch!.Length >= 5, "GetInfo should return at least 5 info codes");
        }
    }

    /// <summary>
    /// Issue #524: an empty-string identifier argument (catalog="", schema="",
    /// table="", foreign_schema="", foreign_table="") passed to a metadata op must
    /// NOT throw HiveServer2Exception on the Thrift path. It must return an empty
    /// result set, matching the SEA/REST path (Thrift vs SEA outcome parity).
    ///
    /// These tests force the Thrift protocol (adbc.databricks.protocol=thrift) so the
    /// regression reproduces regardless of the CI run's default protocol; the Thrift
    /// path is where the divergence lives.
    /// </summary>
    public class EmptyStringMetadataArgE2ETest : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public EmptyStringMetadataArgE2ETest(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
        }

        private void SkipIfNotConfigured()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable), "Test configuration not available");
        }

        // Connection pinned to the Thrift protocol regardless of the configured default.
        private AdbcConnection CreateThriftConnection()
        {
            var parameters = GetDriverParameters(TestConfiguration);
            parameters[DatabricksParameters.Protocol] = "thrift";
            var driver = new DatabricksDriver();
            var db = driver.Open(parameters);
            return db.Connect(new Dictionary<string, string>());
        }

        /// <summary>
        /// Executes a metadata command and returns the total row count. Must not throw.
        /// </summary>
        private static async Task<int> ExecuteMetadataRowCount(AdbcConnection connection, string command,
            string? catalog = null, string? schema = null, string? table = null,
            string? foreignCatalog = null, string? foreignSchema = null, string? foreignTable = null)
        {
            using var stmt = connection.CreateStatement();
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            if (catalog != null) stmt.SetOption(ApacheParameters.CatalogName, catalog);
            if (schema != null) stmt.SetOption(ApacheParameters.SchemaName, schema);
            if (table != null) stmt.SetOption(ApacheParameters.TableName, table);
            if (foreignCatalog != null) stmt.SetOption(ApacheParameters.ForeignCatalogName, foreignCatalog);
            if (foreignSchema != null) stmt.SetOption(ApacheParameters.ForeignSchemaName, foreignSchema);
            if (foreignTable != null) stmt.SetOption(ApacheParameters.ForeignTableName, foreignTable);

            stmt.SqlQuery = command;
            QueryResult result = await stmt.ExecuteQueryAsync();

            int rows = 0;
            using var reader = result.Stream!;
            while (true)
            {
                using var batch = await reader.ReadNextRecordBatchAsync();
                if (batch == null) break;
                rows += batch.Length;
            }
            return rows;
        }

        // GetColumns

        [SkippableFact]
        public async Task GetColumns_EmptyCatalog_ReturnsEmptyWithoutThrowing()
        {
            SkipIfNotConfigured();
            using var conn = CreateThriftConnection();
            int rows = await ExecuteMetadataRowCount(conn, "GetColumns",
                catalog: "", schema: TestConfiguration.Metadata.Schema, table: TestConfiguration.Metadata.Table);
            Assert.Equal(0, rows);
        }

        [SkippableFact]
        public async Task GetColumns_EmptySchema_ReturnsEmptyWithoutThrowing()
        {
            SkipIfNotConfigured();
            using var conn = CreateThriftConnection();
            int rows = await ExecuteMetadataRowCount(conn, "GetColumns",
                catalog: TestConfiguration.Metadata.Catalog, schema: "", table: TestConfiguration.Metadata.Table);
            Assert.Equal(0, rows);
        }

        // GetTables

        [SkippableFact]
        public async Task GetTables_EmptyCatalog_ReturnsEmptyWithoutThrowing()
        {
            SkipIfNotConfigured();
            using var conn = CreateThriftConnection();
            int rows = await ExecuteMetadataRowCount(conn, "GetTables",
                catalog: "", schema: TestConfiguration.Metadata.Schema);
            Assert.Equal(0, rows);
        }

        [SkippableFact]
        public async Task GetTables_EmptySchema_ReturnsEmptyWithoutThrowing()
        {
            SkipIfNotConfigured();
            using var conn = CreateThriftConnection();
            int rows = await ExecuteMetadataRowCount(conn, "GetTables",
                catalog: TestConfiguration.Metadata.Catalog, schema: "");
            Assert.Equal(0, rows);
        }

        // GetPrimaryKeys

        [SkippableFact]
        public async Task GetPrimaryKeys_EmptySchema_ReturnsEmptyWithoutThrowing()
        {
            SkipIfNotConfigured();
            using var conn = CreateThriftConnection();
            int rows = await ExecuteMetadataRowCount(conn, "GetPrimaryKeys",
                catalog: TestConfiguration.Metadata.Catalog, schema: "", table: TestConfiguration.Metadata.Table);
            Assert.Equal(0, rows);
        }

        [SkippableFact]
        public async Task GetPrimaryKeys_EmptyTable_ReturnsEmptyWithoutThrowing()
        {
            SkipIfNotConfigured();
            using var conn = CreateThriftConnection();
            int rows = await ExecuteMetadataRowCount(conn, "GetPrimaryKeys",
                catalog: TestConfiguration.Metadata.Catalog, schema: TestConfiguration.Metadata.Schema, table: "");
            Assert.Equal(0, rows);
        }

        // GetCrossReference

        [SkippableFact]
        public async Task GetCrossReference_EmptyForeignSchema_ReturnsEmptyWithoutThrowing()
        {
            SkipIfNotConfigured();
            using var conn = CreateThriftConnection();
            int rows = await ExecuteMetadataRowCount(conn, "GetCrossReference",
                catalog: TestConfiguration.Metadata.Catalog,
                schema: TestConfiguration.Metadata.Schema,
                table: TestConfiguration.Metadata.Table,
                foreignCatalog: TestConfiguration.Metadata.Catalog,
                foreignSchema: "",
                foreignTable: TestConfiguration.Metadata.Table);
            Assert.Equal(0, rows);
        }

        [SkippableFact]
        public async Task GetCrossReference_EmptyForeignTable_ReturnsEmptyWithoutThrowing()
        {
            SkipIfNotConfigured();
            using var conn = CreateThriftConnection();
            int rows = await ExecuteMetadataRowCount(conn, "GetCrossReference",
                catalog: TestConfiguration.Metadata.Catalog,
                schema: TestConfiguration.Metadata.Schema,
                table: TestConfiguration.Metadata.Table,
                foreignCatalog: TestConfiguration.Metadata.Catalog,
                foreignSchema: TestConfiguration.Metadata.Schema,
                foreignTable: "");
            Assert.Equal(0, rows);
        }
    }
}
