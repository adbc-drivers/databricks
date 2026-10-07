/*
 * Copyright (c) 2026 ADBC Drivers Contributors
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
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.StatementExecution;
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.Hive2;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests.Metadata;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Moq;
using Moq.Protected;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    public class StatementExecutionGetObjectsCatalogPatternTests
    {
        private static readonly string[] s_catalogs = { "main", "marketing", "other", "foo_bar", "fooxbar" };

        public static IEnumerable<object?[]> CatalogPatterns()
        {
            (string? Pattern, string[] Matches)[] patterns =
            {
                ("main", new[] { "main" }),
                ("ma%", new[] { "main", "marketing" }),
                ("ma_n", new[] { "main" }),
                (null, s_catalogs),
                ("%", s_catalogs),
                ("%%", s_catalogs),
                ("", System.Array.Empty<string>()),
                ("missing%", System.Array.Empty<string>()),
                ("MA%", new[] { "main", "marketing" }),
                (@"foo\_bar", new[] { "foo_bar" }),
                ("foo_bar", new[] { "foo_bar", "fooxbar" }),
            };
            foreach (int mode in new[] { 0, 1, 2, 3 })
            {
                foreach (AdbcConnection.GetObjectsDepth depth in new[]
                    { AdbcConnection.GetObjectsDepth.Tables, AdbcConnection.GetObjectsDepth.All })
                {
                    foreach (var (pattern, matches) in patterns)
                        yield return new object?[] { mode, depth, pattern, matches };
                }
            }
        }

        [Theory]
        [MemberData(nameof(CatalogPatterns))]
        public async Task GetObjects_ResolvesCatalogPatterns(
            int mode, AdbcConnection.GetObjectsDepth depth, string? pattern, string[] expectedCatalogs)
        {
            List<string> statements = new List<string>();
            using HttpClient http = CreateHttpClient(mode, statements);
            using StatementExecutionConnection connection = CreateConnection(http);
            using IArrowArrayStream stream = connection.GetObjects(depth, pattern, "default", "t1", null, null);
            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();

            Assert.NotNull(batch);
            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(batch, null);
            Assert.Equal(expectedCatalogs.OrderBy(name => name), catalogs.Select(catalog => catalog.Name).OrderBy(name => name));
            foreach (AdbcCatalog catalog in catalogs)
            {
                AdbcDbSchema schema = Assert.Single(catalog.DbSchemas!);
                Assert.Equal("default", schema.Name);
                AdbcTable table = Assert.Single(schema.Tables!);
                Assert.Equal("t1", table.Name);
                if (depth == AdbcConnection.GetObjectsDepth.All)
                {
                    AdbcColumn column = Assert.Single(table.Columns!);
                    Assert.Equal("a", column.Name);
                    Assert.Equal(1, column.OrdinalPosition);
                }
            }

            if (pattern == "")
                Assert.Empty(statements);
            if (pattern == "main")
                Assert.Single(statements, sql => sql.StartsWith("SHOW CATALOGS", StringComparison.Ordinal));
        }

        [Theory]
        [InlineData("GetCatalogs", false)]
        [InlineData("GetCatalogs", true)]
        [InlineData("GetSchemas", false)]
        [InlineData("GetSchemas", true)]
        [InlineData("GetTables", false)]
        [InlineData("GetTables", true)]
        [InlineData("GetColumns", false)]
        [InlineData("GetColumns", true)]
        public void MetadataCommand_RecordsResponseFormat(string command, bool native)
        {
            using HttpClient http = CreateHttpClient(native ? 1 : 0, new List<string>());
            using StatementExecutionConnection connection = CreateConnection(http);
            using StatementExecutionStatement statement = (StatementExecutionStatement)connection.CreateStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SetOption(ApacheParameters.CatalogName, "main");
            statement.SetOption(ApacheParameters.SchemaName, "default");
            statement.SetOption(ApacheParameters.TableName, "t1");
            statement.SqlQuery = command;
            QueryResult result = statement.ExecuteQuery();
            using IArrowArrayStream? stream = result.Stream;

            Assert.Equal(native, statement.IsNativeMetadataResult);
        }

        [Fact]
        public void MetadataCommand_ResetsNativeFlagForSyntheticResults()
        {
            using HttpClient http = CreateHttpClient(1, new List<string>());
            using StatementExecutionConnection connection = CreateConnection(http);
            using StatementExecutionStatement statement = (StatementExecutionStatement)connection.CreateStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SetOption(ApacheParameters.CatalogName, "main");
            statement.SqlQuery = "GetTables";
            using IArrowArrayStream? native = statement.ExecuteQuery().Stream;
            Assert.True(statement.IsNativeMetadataResult);

            statement.SetOption(ApacheParameters.TableTypes, "");
            using IArrowArrayStream? empty = statement.ExecuteQuery().Stream;
            Assert.False(statement.IsNativeMetadataResult);
        }

        [Fact]
        public async Task GetColumns_MixedResponses_PreserveNativeAttributesAndFlatOrdinals()
        {
            using HttpClient http = CreateHttpClient(3, new List<string>());
            using StatementExecutionConnection connection = CreateConnection(http);
            using StatementExecutionStatement statement = (StatementExecutionStatement)connection.CreateStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SqlQuery = "GetColumns";
            using IArrowArrayStream stream = statement.ExecuteQuery().Stream!;
            using RecordBatch batch = (await stream.ReadNextRecordBatchAsync())!;

            Assert.False(statement.IsNativeMetadataResult);
            Assert.Equal(s_catalogs.Length, batch.Length);
            var catalogs = (StringArray)batch.Column("TABLE_CAT");
            var ordinals = (Int32Array)batch.Column("ORDINAL_POSITION");
            var defaults = (StringArray)batch.Column("COLUMN_DEF");
            var autoIncrement = (StringArray)batch.Column("IS_AUTO_INCREMENT");
            for (int row = 0; row < batch.Length; row++)
            {
                Assert.Equal(0, ordinals.GetValue(row));
                bool native = catalogs.GetString(row) == "main";
                Assert.Equal(native ? "7" : null, defaults.GetString(row));
                Assert.Equal(native ? "YES" : "NO", autoIncrement.GetString(row));
            }
        }

        private static StatementExecutionConnection CreateConnection(HttpClient http)
        {
            Dictionary<string, string> properties = new Dictionary<string, string>
            {
                [SparkParameters.HostName] = "test.databricks.com",
                [DatabricksParameters.WarehouseId] = "wh-1",
                [SparkParameters.AccessToken] = "token",
            };
            return new StatementExecutionConnection(properties, http);
        }

        private static HttpClient CreateHttpClient(int mode, List<string> statements)
        {
            Mock<HttpMessageHandler> handler = new Mock<HttpMessageHandler>();
            handler.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync((HttpRequestMessage request, CancellationToken _) =>
                {
                    if (request.RequestUri?.AbsolutePath.EndsWith("/sql/sessions") == true)
                        return Response("{\"session_id\":\"s1\"}");
                    if (request.Method != HttpMethod.Post)
                        return new HttpResponseMessage(HttpStatusCode.OK);

                    using JsonDocument json = JsonDocument.Parse(
                        request.Content!.ReadAsStringAsync().GetAwaiter().GetResult());
                    string sql = json.RootElement.GetProperty("statement").GetString()!;
                    statements.Add(sql);
                    string operation = request.Headers.GetValues("x-databricks-metadata-operation-type").Single();
                    string? requestedCatalog = sql.Contains("IN ALL CATALOGS") || operation == "GetCatalogs"
                        ? null : s_catalogs.Single(catalog => sql.Contains($"`{catalog}`"));
                    bool native = mode == 1 || (mode == 2 && operation is "GetCatalogs" or "GetTables") ||
                        (mode == 3 && (operation != "GetColumns" || requestedCatalog == "main"));
                    string[] catalogs = requestedCatalog == null ? s_catalogs : new[] { requestedCatalog };
                    if (native && requestedCatalog != null && operation is "GetTables" or "GetColumns")
                        catalogs = catalogs.Concat(new[] { requestedCatalog == "other" ? "main" : "other" }).ToArray();

                    using RecordBatch batch = CreateBatch(operation, native, catalogs, requestedCatalog);
                    using MemoryStream raw = new MemoryStream();
                    using (ArrowStreamWriter writer = new ArrowStreamWriter(raw, batch.Schema))
                    {
                        writer.WriteRecordBatch(batch);
                        writer.WriteEnd();
                    }
                    return Response(JsonSerializer.Serialize(new
                    {
                        statement_id = "stmt-1",
                        status = new { state = "SUCCEEDED" },
                        manifest = new
                        {
                            is_native_metadata_result = native,
                            total_row_count = batch.Length,
                            schema = new
                            {
                                column_count = batch.ColumnCount,
                                columns = batch.Schema.FieldsList.Select((field, position) => new
                                {
                                    name = field.Name,
                                    position,
                                    type_name = field.DataType.TypeId == ArrowTypeId.String ? "STRING"
                                        : field.DataType.TypeId == ArrowTypeId.Int16 ? "SMALLINT" : "INT",
                                }).ToArray(),
                            },
                        },
                        result = new { attachment = raw.ToArray() },
                    }));
                });
            return new HttpClient(handler.Object);
        }

        private static HttpResponseMessage Response(string body)
            => new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body) };

        private static RecordBatch CreateBatch(string operation, bool native, string[] catalogs, string? requestedCatalog)
        {
            Schema schema;
            if (native)
            {
                schema = operation switch
                {
                    "GetCatalogs" => MetadataSchemaFactory.CreateCatalogsSchema(),
                    "GetSchemas" => MetadataSchemaFactory.CreateSchemasSchema(),
                    "GetTables" => MetadataSchemaFactory.CreateTablesSchema(),
                    _ => new Schema(MetadataSchemaFactory.CreateColumnMetadataSchema().FieldsList.Take(23), null),
                };
            }
            else
            {
                string[] names = operation switch
                {
                    "GetCatalogs" => new[] { "catalog" },
                    "GetSchemas" => requestedCatalog == null ? new[] { "databaseName", "catalog" } : new[] { "databaseName" },
                    "GetTables" => new[] { "catalogName", "namespace", "tableName", "tableType" },
                    _ => new[] { "catalogName", "namespace", "tableName", "col_name", "columnType", "isNullable" },
                };
                schema = new Schema(names.Select(name => new Field(name, StringType.Default, true)), null);
            }

            IArrowArray[] arrays = schema.FieldsList.Select(field =>
            {
                if (field.DataType.TypeId == ArrowTypeId.String)
                {
                    StringArray.Builder strings = new StringArray.Builder();
                    foreach (string catalog in catalogs)
                    {
                        string? value = field.Name switch
                        {
                            "TABLE_CAT" or "catalog" or "catalogName" => catalog,
                            "TABLE_CATALOG" => requestedCatalog == null ? catalog : null,
                            "TABLE_SCHEM" or "databaseName" or "namespace" => "default",
                            "TABLE_NAME" or "tableName" => "t1",
                            "TABLE_TYPE" or "tableType" => "TABLE",
                            "COLUMN_NAME" or "col_name" => "a",
                            "TYPE_NAME" or "columnType" => "INT",
                            "COLUMN_DEF" => "7",
                            "IS_AUTO_INCREMENT" => "YES",
                            "isNullable" => "true",
                            _ => null,
                        };
                        if (value == null) strings.AppendNull(); else strings.Append(value);
                    }
                    return (IArrowArray)strings.Build();
                }
                int number = field.Name == "NULLABLE" ? 1 : field.Name == "DATA_TYPE" ? 4 : 0;
                return field.DataType.TypeId switch
                {
                    ArrowTypeId.Int16 => (IArrowArray)new Int16Array.Builder().AppendRange(
                        Enumerable.Repeat((short)number, catalogs.Length)).Build(),
                    ArrowTypeId.Int32 => new Int32Array.Builder().AppendRange(
                        Enumerable.Repeat(number, catalogs.Length)).Build(),
                    _ => throw new InvalidOperationException($"Unexpected metadata type: {field.DataType}"),
                };
            }).ToArray();
            return new RecordBatch(schema, arrays, catalogs.Length);
        }
    }
}
