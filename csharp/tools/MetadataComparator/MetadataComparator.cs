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
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using AdbcDrivers.Databricks;

namespace AdbcDrivers.Databricks.Examples
{
    class CompareMetadata
    {
        private static StringBuilder _output = new StringBuilder();

        static async Task Main(string[] args)
        {
            // Add console listener for Trace output (Debug.WriteLine also writes to Trace listeners)
            System.Diagnostics.Trace.Listeners.Add(new System.Diagnostics.ConsoleTraceListener());

            var configFile = Environment.GetEnvironmentVariable("DATABRICKS_CONFIG_FILE");
            if (string.IsNullOrEmpty(configFile) || !System.IO.File.Exists(configFile))
            {
                Console.WriteLine("Error: DATABRICKS_CONFIG_FILE not set");
                return;
            }

            var json = System.IO.File.ReadAllText(configFile);
            var parsed = System.Text.Json.JsonDocument.Parse(json);
            var config = new Dictionary<string, string>();
            foreach (var prop in parsed.RootElement.EnumerateObject())
            {
                config[prop.Name] = prop.Value.GetString() ?? "";
            }

            WriteLine("╔════════════════════════════════════════════════════════════════════════════════╗");
            WriteLine("║              METADATA COMPARISON: THRIFT vs SEA                                ║");
            WriteLine("╚════════════════════════════════════════════════════════════════════════════════╝");
            WriteLine();

            await CompareGetCatalogs(config);
            await CompareGetSchemas(config);
            await CompareGetTables(config);
            await CompareGetColumns(config);
            await CompareGetColumnsExtended(config);
            await CompareGetPrimaryKeys(config);
            await CompareGetCrossReference(config);
            await CompareGetTableSchema(config);
            await CompareGetObjects(config, AdbcConnection.GetObjectsDepth.Catalogs, "Catalogs", "ma%", null, null);
            await CompareGetObjects(config, AdbcConnection.GetObjectsDepth.DbSchemas, "DbSchemas", "main", "default", null);
            await CompareGetObjects(config, AdbcConnection.GetObjectsDepth.Tables, "Tables", "main", "adbc_testing", null);
            await CompareGetObjects(config, AdbcConnection.GetObjectsDepth.All, "All", "main", "adbc_testing", "all_column_types");

            var outputPath = Path.Combine(Directory.GetCurrentDirectory(), "metadata_comparison_output.txt");
            File.WriteAllText(outputPath, _output.ToString());
            WriteLine();
            WriteLine("════════════════════════════════════════════════════════════════════════════════");
            WriteLine($"Output saved to: {outputPath}");
            WriteLine("════════════════════════════════════════════════════════════════════════════════");
        }

        static Task CompareGetInfo(Dictionary<string, string> config)
        {
            PrintTestHeader("GetInfo");
            using var driver = new DatabricksDriver();

            using var thriftDb = driver.Open(CreateThriftParams(config));
            using var thriftConn = thriftDb.Connect(new Dictionary<string, string>());
            using var seaDb = driver.Open(CreateSeaParams(config));
            using var seaConn = seaDb.Connect(new Dictionary<string, string>());

            var thriftInfo = GetInfoData(thriftConn);
            var seaInfo = GetInfoData(seaConn);

            CompareResults(thriftInfo, seaInfo);
            return Task.CompletedTask;
        }

        static async Task CompareGetCatalogs(Dictionary<string, string> config)
        {
            PrintTestHeader("GetCatalogs");
            using var driver = new DatabricksDriver();

            using var thriftDb = driver.Open(CreateThriftParams(config));
            using var thriftConn = thriftDb.Connect(new Dictionary<string, string>());
            var thriftData = await GetMetadataData(thriftConn, "GetCatalogs", null, null, null);

            using var seaDb = driver.Open(CreateSeaParams(config));
            using var seaConn = seaDb.Connect(new Dictionary<string, string>());
            var seaData = await GetMetadataData(seaConn, "GetCatalogs", null, null, null);

            CompareResults(thriftData, seaData);
        }

        static async Task CompareGetSchemas(Dictionary<string, string> config)
        {
            PrintTestHeader("GetSchemas", "catalog=main");
            using var driver = new DatabricksDriver();

            using var thriftDb = driver.Open(CreateThriftParams(config));
            using var thriftConn = thriftDb.Connect(new Dictionary<string, string>());
            var thriftData = await GetMetadataData(thriftConn, "GetSchemas", null, null, null);

            using var seaDb = driver.Open(CreateSeaParams(config));
            using var seaConn = seaDb.Connect(new Dictionary<string, string>());
            var seaData = await GetMetadataData(seaConn, "GetSchemas", null, null, null);

            CompareResults(thriftData, seaData);
        }

        static async Task CompareGetTables(Dictionary<string, string> config)
        {
            PrintTestHeader("GetTables", "catalog=main, schema=adbc_testing");
            using var driver = new DatabricksDriver();

            using var thriftDb = driver.Open(CreateThriftParams(config));
            using var thriftConn = thriftDb.Connect(new Dictionary<string, string>());
            var thriftData = await GetMetadataData(thriftConn, "GetTables", "main", "adbc_testing", null);

            using var seaDb = driver.Open(CreateSeaParams(config));
            using var seaConn = seaDb.Connect(new Dictionary<string, string>());
            var seaData = await GetMetadataData(seaConn, "GetTables", "main", "adbc_testing", null);

            CompareResults(thriftData, seaData);
        }

        static async Task CompareGetColumns(Dictionary<string, string> config)
        {
            PrintTestHeader("GetColumns", "table=main.adbc_testing.all_column_types");
            using var driver = new DatabricksDriver();

            using var thriftDb = driver.Open(CreateThriftParams(config));
            using var thriftConn = thriftDb.Connect(new Dictionary<string, string>());
            var thriftData = await GetMetadataData(thriftConn, "GetColumns", "main", "adbc_testing", "all_column_types");

            using var seaDb = driver.Open(CreateSeaParams(config));
            using var seaConn = seaDb.Connect(new Dictionary<string, string>());
            var seaData = await GetMetadataData(seaConn, "GetColumns", "main", "adbc_testing", "all_column_types");

            CompareResults(thriftData, seaData);
        }

        static async Task CompareGetColumnsExtended(Dictionary<string, string> config)
        {
            PrintTestHeader("GetColumnsExtended", "table=main.adbc_testing.all_column_types");
            using var driver = new DatabricksDriver();

            using var thriftDb = driver.Open(CreateThriftParams(config));
            using var thriftConn = thriftDb.Connect(new Dictionary<string, string>());
            var thriftData = await GetMetadataData(thriftConn, "GetColumnsExtended", "main", "adbc_testing", "all_column_types");

            using var seaDb = driver.Open(CreateSeaParams(config));
            using var seaConn = seaDb.Connect(new Dictionary<string, string>());
            var seaData = await GetMetadataData(seaConn, "GetColumnsExtended", "main", "adbc_testing", "all_column_types");

            CompareResults(thriftData, seaData);
        }

        static async Task CompareGetPrimaryKeys(Dictionary<string, string> config)
        {
            PrintTestHeader("GetPrimaryKeys", "table=main.adbc_testing.cross_ref_customers");
            using var driver = new DatabricksDriver();

            using var thriftDb = driver.Open(CreateThriftParams(config));
            using var thriftConn = thriftDb.Connect(new Dictionary<string, string>());
            var thriftData = await GetPrimaryKeysData(thriftConn, "main", "adbc_testing", "cross_ref_customers");

            using var seaDb = driver.Open(CreateSeaParams(config));
            using var seaConn = seaDb.Connect(new Dictionary<string, string>());
            var seaData = await GetPrimaryKeysData(seaConn, "main", "adbc_testing", "cross_ref_customers");

            CompareResults(thriftData, seaData);
        }

        static async Task CompareGetCrossReference(Dictionary<string, string> config)
        {
            PrintTestHeader("GetCrossReference", "pk_table=cross_ref_customers, fk_table=cross_ref_orders");
            using var driver = new DatabricksDriver();

            using var thriftDb = driver.Open(CreateThriftParams(config));
            using var thriftConn = thriftDb.Connect(new Dictionary<string, string>());
            var thriftData = await GetCrossReferenceData(thriftConn, "main", "adbc_testing", "cross_ref_customers", "main", "adbc_testing", "cross_ref_orders");

            using var seaDb = driver.Open(CreateSeaParams(config));
            using var seaConn = seaDb.Connect(new Dictionary<string, string>());
            var seaData = await GetCrossReferenceData(seaConn, "main", "adbc_testing", "cross_ref_customers", "main", "adbc_testing", "cross_ref_orders");

            CompareResults(thriftData, seaData);
        }

        static Task CompareGetTableSchema(Dictionary<string, string> config)
        {
            PrintTestHeader("GetTableSchema", "table=main.adbc_testing.cross_ref_customers");
            using var driver = new DatabricksDriver();

            using var thriftDb = driver.Open(CreateThriftParams(config));
            using var thriftConn = thriftDb.Connect(new Dictionary<string, string>());
            var thriftSchema = thriftConn.GetTableSchema("main", "adbc_testing", "cross_ref_customers");

            using var seaDb = driver.Open(CreateSeaParams(config));
            using var seaConn = seaDb.Connect(new Dictionary<string, string>());
            var seaSchema = seaConn.GetTableSchema("main", "adbc_testing", "cross_ref_customers");

            // Compare field by field
            var thriftData = new List<Dictionary<string, string>>();
            var seaData = new List<Dictionary<string, string>>();
            int maxFields = Math.Max(thriftSchema.FieldsList.Count, seaSchema.FieldsList.Count);
            for (int i = 0; i < maxFields; i++)
            {
                var thriftRow = new Dictionary<string, string>();
                var seaRow = new Dictionary<string, string>();
                if (i < thriftSchema.FieldsList.Count)
                {
                    var f = thriftSchema.FieldsList[i];
                    thriftRow["field_name"] = f.Name;
                    thriftRow["field_type"] = f.DataType.ToString();
                    thriftRow["nullable"] = f.IsNullable.ToString();
                }
                if (i < seaSchema.FieldsList.Count)
                {
                    var f = seaSchema.FieldsList[i];
                    seaRow["field_name"] = f.Name;
                    seaRow["field_type"] = f.DataType.ToString();
                    seaRow["nullable"] = f.IsNullable.ToString();
                }
                thriftData.Add(thriftRow);
                seaData.Add(seaRow);
            }

            CompareResults(thriftData, seaData);
            return Task.CompletedTask;
        }

        static async Task CompareGetObjects(Dictionary<string, string> config, AdbcConnection.GetObjectsDepth depth, string depthName, string? catalog = null, string? schema = null, string? table = null)
        {
            var paramsStr = $"catalog={catalog ?? "null"}, schema={schema ?? "null"}, table={table ?? "null"}";
            PrintTestHeader($"GetObjects(depth={depthName})", paramsStr);
            using var driver = new DatabricksDriver();

            using var thriftDb = driver.Open(CreateThriftParams(config));
            using var thriftConn = thriftDb.Connect(new Dictionary<string, string>());
            var thriftData = await GetObjectsData(thriftConn, depth, catalog, schema, table);

            using var seaDb = driver.Open(CreateSeaParams(config));
            using var seaConn = seaDb.Connect(new Dictionary<string, string>());
            var seaData = await GetObjectsData(seaConn, depth, catalog, schema, table);

            CompareResults(thriftData, seaData);
        }

        static void PrintTestHeader(string testName, string? parameters = null)
        {
            WriteLine();
            WriteLine("────────────────────────────────────────────────────────────────────────────────");
            if (parameters != null)
                WriteLine($"TEST: {testName} ({parameters})");
            else
                WriteLine($"TEST: {testName}");
            WriteLine("────────────────────────────────────────────────────────────────────────────────");
        }

        static Dictionary<string, string> CreateThriftParams(Dictionary<string, string> config)
        {
            return new Dictionary<string, string>(config)
            {
                { SparkParameters.Type, SparkServerTypeConstants.Http },
                { SparkParameters.AuthType, SparkAuthTypeConstants.Token },
                { "adbc.databricks.protocol", "thrift" }
            };
        }

        static Dictionary<string, string> CreateSeaParams(Dictionary<string, string> config)
        {
            return new Dictionary<string, string>(config)
            {
                { "adbc.databricks.protocol", "rest" },
                { "adbc.databricks.enable_session_management", "true" }
            };
        }

        static List<Dictionary<string, string>> GetInfoData(AdbcConnection connection)
        {
            var results = new List<Dictionary<string, string>>();
            var infoCodes = new List<AdbcInfoCode>
            {
                AdbcInfoCode.DriverName,
                AdbcInfoCode.DriverVersion,
                AdbcInfoCode.VendorName,
                AdbcInfoCode.VendorVersion,
                AdbcInfoCode.DriverArrowVersion
            };

            using var stream = connection.GetInfo(infoCodes);

            while (true)
            {
                var batch = stream.ReadNextRecordBatchAsync().Result;
                if (batch == null) break;

                for (int i = 0; i < batch.Length; i++)
                {
                    var row = new Dictionary<string, string>();
                    for (int j = 0; j < batch.ColumnCount; j++)
                    {
                        var name = batch.Schema.GetFieldByIndex(j).Name;
                        var value = GetValue(batch.Column(j), i)?.ToString() ?? "null";
                        row[name] = value;
                    }
                    results.Add(row);
                }
            }
            return results;
        }

        static async Task<List<Dictionary<string, string>>> GetMetadataData(AdbcConnection connection, string command, string? catalog, string? schema, string? table)
        {
            var results = new List<Dictionary<string, string>>();

            using var statement = connection.CreateStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            if (catalog != null) statement.SetOption(ApacheParameters.CatalogName, catalog);
            if (schema != null) statement.SetOption(ApacheParameters.SchemaName, schema);
            if (table != null) statement.SetOption(ApacheParameters.TableName, table);
            if (command == "GetColumns" || command == "GetColumnsExtended")
                statement.SetOption(ApacheParameters.ColumnName, "%");

            statement.SqlQuery = command;
            var queryResult = statement.ExecuteQuery();
            using var reader = queryResult.Stream;

            while (true)
            {
                using var batch = await reader.ReadNextRecordBatchAsync();
                if (batch == null) break;

                for (int i = 0; i < batch.Length; i++)
                {
                    var row = new Dictionary<string, string>();
                    for (int j = 0; j < batch.ColumnCount; j++)
                    {
                        var name = reader.Schema?.GetFieldByIndex(j).Name ?? $"col_{j}";
                        var value = GetValue(batch.Column(j), i)?.ToString() ?? "null";
                        row[name] = value;
                    }
                    results.Add(row);
                }
            }

            return results;
        }

        static async Task<List<Dictionary<string, string>>> GetPrimaryKeysData(AdbcConnection connection, string catalog, string schema, string table)
        {
            var results = new List<Dictionary<string, string>>();

            using var statement = connection.CreateStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SetOption(ApacheParameters.CatalogName, catalog);
            statement.SetOption(ApacheParameters.SchemaName, schema);
            statement.SetOption(ApacheParameters.TableName, table);

            statement.SqlQuery = "GetPrimaryKeys";
            var queryResult = statement.ExecuteQuery();
            using var reader = queryResult.Stream;

            while (true)
            {
                using var batch = await reader.ReadNextRecordBatchAsync();
                if (batch == null) break;

                for (int i = 0; i < batch.Length; i++)
                {
                    var row = new Dictionary<string, string>();
                    for (int j = 0; j < batch.ColumnCount; j++)
                    {
                        var name = reader.Schema?.GetFieldByIndex(j).Name ?? $"col_{j}";
                        var value = GetValue(batch.Column(j), i)?.ToString() ?? "null";
                        row[name] = value;
                    }
                    results.Add(row);
                }
            }

            return results;
        }

        static async Task<List<Dictionary<string, string>>> GetCrossReferenceData(AdbcConnection connection, string pkCatalog, string pkSchema, string pkTable, string fkCatalog, string fkSchema, string fkTable)
        {
            var results = new List<Dictionary<string, string>>();

            using var statement = connection.CreateStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SetOption(ApacheParameters.CatalogName, pkCatalog);
            statement.SetOption(ApacheParameters.SchemaName, pkSchema);
            statement.SetOption(ApacheParameters.TableName, pkTable);
            statement.SetOption(ApacheParameters.ForeignCatalogName, fkCatalog);
            statement.SetOption(ApacheParameters.ForeignSchemaName, fkSchema);
            statement.SetOption(ApacheParameters.ForeignTableName, fkTable);

            statement.SqlQuery = "GetCrossReference";
            var queryResult = statement.ExecuteQuery();
            using var reader = queryResult.Stream;

            while (true)
            {
                using var batch = await reader.ReadNextRecordBatchAsync();
                if (batch == null) break;

                for (int i = 0; i < batch.Length; i++)
                {
                    var row = new Dictionary<string, string>();
                    for (int j = 0; j < batch.ColumnCount; j++)
                    {
                        var name = reader.Schema?.GetFieldByIndex(j).Name ?? $"col_{j}";
                        var value = GetValue(batch.Column(j), i)?.ToString() ?? "null";
                        row[name] = value;
                    }
                    results.Add(row);
                }
            }

            return results;
        }

        static List<Dictionary<string, string>> GetTableSchemaInfo(AdbcConnection connection, string? catalog, string? schema, string? table)
        {
            var results = new List<Dictionary<string, string>>();

            try
            {
                var schemaa = connection.GetTableSchema(catalog, schema, table);

                foreach (var field in schemaa.FieldsList)
                {
                    var row = new Dictionary<string, string>
                    {
                        ["field_name"] = field.Name,
                        ["field_type"] = field.DataType.ToString(),
                        ["nullable"] = field.IsNullable.ToString(),
                        ["comment"] = field.Metadata?.ContainsKey("comment") == true ? field.Metadata["comment"] : "null"
                    };
                    results.Add(row);
                }
            }
            catch (Exception ex)
            {
                WriteLine($"ERROR: {ex.Message}");
            }

            return results;
        }

        static async Task<List<Dictionary<string, string>>> GetObjectsData(AdbcConnection connection, AdbcConnection.GetObjectsDepth depth, string? catalog, string? schema, string? table)
        {
            var results = new List<Dictionary<string, string>>();

            using var stream = connection.GetObjects(
                depth,
                catalogPattern: catalog,
                dbSchemaPattern: schema,
                tableNamePattern: table,
                tableTypes: null,
                columnNamePattern: null);

            while (true)
            {
                using var batch = await stream.ReadNextRecordBatchAsync();
                if (batch == null) break;

                var catalogArray = batch.Column("catalog_name") as StringArray;
                var dbSchemasArray = batch.Column("catalog_db_schemas") as ListArray;

                for (int catIdx = 0; catIdx < batch.Length; catIdx++)
                {
                    if (catalogArray?.IsNull(catIdx) != false)
                    {
                        results.Add(new Dictionary<string, string> { ["catalog_name"] = "null" });
                        continue;
                    }

                    if (depth == AdbcConnection.GetObjectsDepth.Catalogs)
                    {
                        results.Add(new Dictionary<string, string> { ["catalog_name"] = catalogArray.GetString(catIdx) });
                        continue;
                    }

                    if (dbSchemasArray?.IsNull(catIdx) != false) continue;
                    var schemaStruct = dbSchemasArray.GetSlicedValues(catIdx) as StructArray;
                    if (schemaStruct == null) continue;

                    StringArray? schemaNameArray = null;
                    ListArray? tablesListArray = null;
                    try
                    {
                        var fields = schemaStruct.Fields;
                        if (fields == null || fields.Count < 2) continue;
                        schemaNameArray = fields[0] as StringArray;
                        tablesListArray = fields[1] as ListArray;
                    }
                    catch (NullReferenceException)
                    {
                        continue;
                    }

                    for (int schemaIdx = 0; schemaIdx < schemaStruct.Length; schemaIdx++)
                    {
                        if (schemaNameArray?.IsNull(schemaIdx) != false) continue;

                        if (depth == AdbcConnection.GetObjectsDepth.DbSchemas)
                        {
                            results.Add(new Dictionary<string, string>
                            {
                                ["catalog_name"] = catalogArray.GetString(catIdx),
                                ["db_schema_name"] = schemaNameArray.GetString(schemaIdx)
                            });
                            continue;
                        }

                        if (tablesListArray?.IsNull(schemaIdx) != false) continue;
                        var tableStruct = tablesListArray.GetSlicedValues(schemaIdx) as StructArray;
                        if (tableStruct == null) continue;

                        StringArray? tableNameArray = null;
                        StringArray? tableTypeArray = null;
                        ListArray? columnsListArray = null;
                        try
                        {
                            var tableFields = tableStruct.Fields;
                            if (tableFields == null || tableFields.Count < 3) continue;
                            tableNameArray = tableFields[0] as StringArray;
                            tableTypeArray = tableFields[1] as StringArray;
                            columnsListArray = tableFields[2] as ListArray;
                        }
                        catch (NullReferenceException)
                        {
                            continue;
                        }

                        for (int tableIdx = 0; tableIdx < tableStruct.Length; tableIdx++)
                        {
                            if (tableNameArray?.IsNull(tableIdx) != false) continue;

                            if (depth == AdbcConnection.GetObjectsDepth.Tables)
                            {
                                results.Add(new Dictionary<string, string>
                                {
                                    ["catalog_name"] = catalogArray.GetString(catIdx),
                                    ["db_schema_name"] = schemaNameArray.GetString(schemaIdx),
                                    ["table_name"] = tableNameArray.GetString(tableIdx),
                                    ["table_type"] = tableTypeArray?.IsNull(tableIdx) == false ? tableTypeArray.GetString(tableIdx) : "null"
                                });
                                continue;
                            }

                            if (columnsListArray == null || columnsListArray.IsNull(tableIdx)) continue;
                            var columnStruct = columnsListArray.GetSlicedValues(tableIdx) as StructArray;
                            if (columnStruct == null) continue;

                            for (int colIdx = 0; colIdx < columnStruct.Length; colIdx++)
                            {
                                var row = new Dictionary<string, string>
                                {
                                    ["catalog_name"] = catalogArray.GetString(catIdx),
                                    ["db_schema_name"] = schemaNameArray.GetString(schemaIdx),
                                    ["table_name"] = tableNameArray.GetString(tableIdx)
                                };

                                try
                                {
                                    var columnFields = columnStruct.Fields;
                                    if (columnFields == null) continue;

                                    var columnSchema = ((IArrowArray)columnStruct).Data.DataType as StructType;
                                    for (int fieldIdx = 0; fieldIdx < columnFields.Count; fieldIdx++)
                                    {
                                        var fieldName = columnSchema?.Fields[fieldIdx].Name ?? $"field_{fieldIdx}";
                                        var fieldArray = columnFields[fieldIdx];
                                        var value = GetValue(fieldArray, colIdx)?.ToString() ?? "null";
                                        row[fieldName] = value;
                                    }
                                }
                                catch (NullReferenceException)
                                {
                                    // Skip this column if we can't access its fields
                                }

                                results.Add(row);
                            }
                        }
                    }
                }
            }

            return results;
        }

        static void CompareResults(List<Dictionary<string, string>> thriftData, List<Dictionary<string, string>> seaData)
        {
            WriteLine($"Row Count: Thrift={thriftData.Count}, SEA={seaData.Count}");
            WriteLine();

            if (thriftData.Count == 0 && seaData.Count == 0)
            {
                WriteLine("RESULT: ✅ BOTH EMPTY (MATCH)");
                return;
            }

            // Get all column names
            var allColumns = new HashSet<string>();
            foreach (var row in thriftData.Concat(seaData))
                foreach (var key in row.Keys)
                    allColumns.Add(key);

            var thriftColumns = thriftData.FirstOrDefault()?.Keys.ToHashSet() ?? new HashSet<string>();
            var seaColumns = seaData.FirstOrDefault()?.Keys.ToHashSet() ?? new HashSet<string>();
            var commonColumns = thriftColumns.Intersect(seaColumns).ToList();
            var missingInSea = thriftColumns.Except(seaColumns).ToList();
            var missingInThrift = seaColumns.Except(thriftColumns).ToList();

            // Schema comparison
            WriteLine("SCHEMA COMPARISON:");
            WriteLine($"  Common columns: {commonColumns.Count}");
            if (missingInSea.Any())
                WriteLine($"  Missing in SEA: {string.Join(", ", missingInSea)}");
            if (missingInThrift.Any())
                WriteLine($"  Missing in Thrift: {string.Join(", ", missingInThrift)}");
            WriteLine();

            int totalMatches = 0;
            int totalMismatches = 0;
            var mismatchSummary = new Dictionary<string, int>();

            // Calculate column widths based on actual content
            int colNameWidth = Math.Max(30, allColumns.Max(c => c.Length) + 2);
            int valueWidth = 40;

            for (int rowIdx = 0; rowIdx < Math.Max(thriftData.Count, seaData.Count); rowIdx++)
            {
                var thriftRow = rowIdx < thriftData.Count ? thriftData[rowIdx] : new Dictionary<string, string>();
                var seaRow = rowIdx < seaData.Count ? seaData[rowIdx] : new Dictionary<string, string>();

                WriteLine($"Row {rowIdx}:");
                var topBorder = "┌" + new string('─', colNameWidth) + "┬" + new string('─', valueWidth) + "┬" + new string('─', valueWidth) + "┬───────┐";
                WriteLine(topBorder);
                var headerCol = "Column".PadRight(colNameWidth - 2);
                var headerThrift = "Thrift".PadRight(valueWidth - 2);
                var headerSea = "SEA".PadRight(valueWidth - 2);
                WriteLine($"│ {headerCol} │ {headerThrift} │ {headerSea} │ Match │");
                var midBorder = "├" + new string('─', colNameWidth) + "┼" + new string('─', valueWidth) + "┼" + new string('─', valueWidth) + "┼───────┤";
                WriteLine(midBorder);

                foreach (var col in allColumns.OrderBy(c => c))
                {
                    var thriftVal = thriftRow.ContainsKey(col) ? thriftRow[col] : "N/A";
                    var seaVal = seaRow.ContainsKey(col) ? seaRow[col] : "N/A";

                    bool match = (thriftVal == seaVal) && (thriftVal != "N/A");
                    if (match)
                        totalMatches++;
                    else
                    {
                        totalMismatches++;
                        if (!mismatchSummary.ContainsKey(col))
                            mismatchSummary[col] = 0;
                        mismatchSummary[col]++;
                    }

                    string matchIcon = match ? "  ✅  " : "  ❌  ";
                    var colPadded = col.PadRight(colNameWidth - 2);
                    var thriftPadded = thriftVal.PadRight(valueWidth - 2);
                    var seaPadded = seaVal.PadRight(valueWidth - 2);
                    WriteLine($"│ {colPadded} │ {thriftPadded} │ {seaPadded} │ {matchIcon} │");
                }

                var bottomBorder = "└" + new string('─', colNameWidth) + "┴" + new string('─', valueWidth) + "┴" + new string('─', valueWidth) + "┴───────┘";
                WriteLine(bottomBorder);
                WriteLine();
            }

            // Mismatch summary
            if (mismatchSummary.Any())
            {
                WriteLine("MISMATCHED COLUMNS SUMMARY:");
                foreach (var kvp in mismatchSummary.OrderByDescending(x => x.Value).Take(10))
                {
                    WriteLine($"  {kvp.Key}: {kvp.Value} mismatches");
                }
                if (mismatchSummary.Count > 10)
                    WriteLine($"  ... and {mismatchSummary.Count - 10} more columns with mismatches");
                WriteLine();
            }

            // Final result
            int total = totalMatches + totalMismatches;
            double percentage = total > 0 ? (totalMatches * 100.0 / total) : 0;

            WriteLine("════════════════════════════════════════════════════════════════════════════════");
            WriteLine($"RESULT: {totalMatches}/{total} cells match ({percentage:F1}%)");
            if (percentage == 100.0)
                WriteLine("STATUS: ✅ PASS - Perfect match!");
            else if (percentage >= 90.0)
                WriteLine("STATUS: ⚠️  MOSTLY MATCHING - Minor differences");
            else if (percentage >= 50.0)
                WriteLine("STATUS: ⚠️  PARTIALLY MATCHING - Significant differences");
            else
                WriteLine("STATUS: ❌ FAIL - Major differences");
            WriteLine("════════════════════════════════════════════════════════════════════════════════");
        }

        static object? GetValue(IArrowArray array, int index)
        {
            if (array.IsNull(index)) return null;

            return array switch
            {
                Int8Array a => a.GetValue(index),
                Int16Array a => a.GetValue(index),
                Int32Array a => a.GetValue(index),
                Int64Array a => a.GetValue(index),
                UInt8Array a => a.GetValue(index),
                UInt16Array a => a.GetValue(index),
                UInt32Array a => a.GetValue(index),
                UInt64Array a => a.GetValue(index),
                FloatArray a => a.GetValue(index),
                DoubleArray a => a.GetValue(index),
                StringArray a => a.GetString(index),
                BooleanArray a => a.GetValue(index),
                BinaryArray a => BitConverter.ToString(a.GetBytes(index).ToArray()),
                _ => $"[{array.Data.DataType}]"
            };
        }

        static void WriteLine(string line = "")
        {
            Console.WriteLine(line);
            _output.AppendLine(line);
        }
    }
}
