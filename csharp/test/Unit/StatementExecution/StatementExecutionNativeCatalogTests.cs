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
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Moq;
using Moq.Protected;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    public class StatementExecutionNativeCatalogTests
    {
        private static byte[] ArrowStrings(string name, params string[] values)
        {
            var schema = new Schema(new[] { new Field(name, StringType.Default, true) }, null);
            var builder = new StringArray.Builder();
            foreach (string value in values) builder.Append(value);
            using var raw = new MemoryStream();
            using (var writer = new ArrowStreamWriter(raw, schema))
            {
                writer.WriteRecordBatch(new RecordBatch(schema, new IArrowArray[] { builder.Build() }, values.Length));
                writer.WriteEnd();
            }
            return raw.ToArray();
        }

        private static byte[] ArrowNativeColumns(Field[] fields, string nativeCatalog)
        {
            var schema = new Schema(fields, null);
            var arrays = fields.Select(field =>
            {
                if (field.Name == "TABLE_CAT")
                    return (IArrowArray)new StringArray.Builder().Append(nativeCatalog).Append("fooxbar").Build();
                string value = field.Name switch
                {
                    "TABLE_SCHEM" => "default",
                    "TABLE_NAME" => "t",
                    "COLUMN_NAME" => "a",
                    "TYPE_NAME" => "INT",
                    "IS_AUTO_INCREMENT" => "YES",
                    _ => field.Name,
                };
                return field.DataType.TypeId switch
                {
                    ArrowTypeId.String => (IArrowArray)new StringArray.Builder()
                        .Append(value).Append(value).Build(),
                    ArrowTypeId.Int8 => new Int8Array.Builder().Append(0).Append(0).Build(),
                    ArrowTypeId.Int16 => new Int16Array.Builder().Append(0).Append(0).Build(),
                    ArrowTypeId.Int32 => new Int32Array.Builder().Append(0).Append(0).Build(),
                    _ => (IArrowArray)new Int64Array.Builder().Append(0).Append(0).Build(),
                };
            }).ToArray();
            using var raw = new MemoryStream();
            using (var writer = new ArrowStreamWriter(raw, schema))
            {
                writer.WriteRecordBatch(new RecordBatch(schema, arrays, 2));
                writer.WriteEnd();
            }
            return raw.ToArray();
        }

        private static HttpClient CreateHttpClient(
            bool overlappingNativeColumns = false, Field[]? columnFields = null, string nativeCatalog = "foo_bar")
        {
            byte[] catalogs = overlappingNativeColumns
                ? ArrowStrings("TABLE_CAT", nativeCatalog, "fooxbar")
                : ArrowStrings("TABLE_CAT", "main", "other");
            Field[] nativeColumnFields = columnFields ??
                MetadataSchemaFactory.CreateColumnMetadataSchema().FieldsList.Take(23).ToArray();
            byte[] columns = overlappingNativeColumns
                ? ArrowNativeColumns(nativeColumnFields, nativeCatalog)
                : ArrowStrings("col_name", "a");
            var handler = new Mock<HttpMessageHandler>();
            handler.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync((HttpRequestMessage request, CancellationToken _) =>
                {
                    if (request.RequestUri?.AbsolutePath.EndsWith("/sql/sessions") == true)
                        return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("{\"session_id\":\"s1\"}") };

                    string sql = "";
                    if (request.Method == HttpMethod.Post)
                    {
                        using var json = JsonDocument.Parse(request.Content!.ReadAsStringAsync().GetAwaiter().GetResult());
                        sql = json.RootElement.GetProperty("statement").GetString() ?? "";
                    }
                    bool isCatalogs = sql.StartsWith("SHOW CATALOGS");
                    Field[] fields = isCatalogs
                        ? new[] { new Field("TABLE_CAT", StringType.Default, true) }
                        : overlappingNativeColumns
                            ? nativeColumnFields
                            : new[] { new Field("col_name", StringType.Default, true) };
                    var manifestColumns = fields.Select((field, position) =>
                    {
                        string type = field.DataType.TypeId switch
                        {
                            ArrowTypeId.Int8 => "TINYINT",
                            ArrowTypeId.Int16 => "SMALLINT",
                            ArrowTypeId.Int32 => "INT",
                            ArrowTypeId.Int64 => "BIGINT",
                            _ => "STRING",
                        };
                        return new { name = field.Name, position, type_name = type, type_text = type };
                    }).ToArray();
                    var body = JsonSerializer.Serialize(new
                    {
                        statement_id = "stmt-1",
                        status = new { state = "SUCCEEDED" },
                        manifest = new
                        {
                            is_native_metadata_result = isCatalogs || overlappingNativeColumns,
                            total_row_count = isCatalogs || overlappingNativeColumns ? 2 : 1,
                            schema = new
                            {
                                column_count = fields.Length,
                                columns = manifestColumns,
                            },
                        },
                        result = new { attachment = isCatalogs ? catalogs : columns },
                    });
                    return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body) };
                });
            return new HttpClient(handler.Object);
        }

        private static StatementExecutionConnection CreateConnection(HttpClient http)
        {
            var properties = new Dictionary<string, string>
            {
                [SparkParameters.HostName] = "test.databricks.com",
                [DatabricksParameters.WarehouseId] = "wh-1",
                [SparkParameters.AccessToken] = "token",
            };
            return new StatementExecutionConnection(properties, http);
        }

        [Fact]
        public async Task NativeCatalogs_AreFilteredForGetObjects()
        {
            using var http = CreateHttpClient();
            using var connection = CreateConnection(http);

            var catalogs = await ((IGetObjectsDataProvider)connection).GetCatalogsAsync("main", CancellationToken.None);

            Assert.Equal(new[] { "main" }, catalogs);
        }

        [Fact]
        public async Task GetObjects_CatalogDepth_FiltersNativeCatalogs()
        {
            using var http = CreateHttpClient();
            using var connection = CreateConnection(http);
            using var stream = connection.GetObjects(
                AdbcConnection.GetObjectsDepth.Catalogs, "main", null, null, null, null);
            using var batch = await stream.ReadNextRecordBatchAsync();

            Assert.NotNull(batch);
            Assert.Equal(1, batch.Length);
            Assert.Equal("main", ((StringArray)batch.Column(0)).GetString(0));
        }

        [Fact]
        public async Task ColumnFanout_KeepsTheQueriedCatalog()
        {
            using var http = CreateHttpClient();
            using var connection = CreateConnection(http);

            var result = await connection.ReadColumnsAsync(null, null, null, null, CancellationToken.None);

            Assert.Equal(2, result.Batches.Count);
            Assert.Equal(new[] { "main", "other" }, result.SourceCatalogs);
        }

        [Theory]
        [InlineData("count", "expected 23 columns, found 22")]
        [InlineData("name", "missing TABLE_CAT")]
        [InlineData("type", "unexpected type or duplicate TABLE_SCHEM")]
        public async Task ColumnFanout_MalformedNativeColumns_PropagatesValidationError(
            string invalidSchema, string expectedError)
        {
            Field[] fields = MetadataSchemaFactory.CreateColumnMetadataSchema().FieldsList.Take(23).ToArray();
            switch (invalidSchema)
            {
                case "count":
                    fields = fields.Skip(1).ToArray();
                    break;
                case "name":
                    fields[0] = new Field("UNKNOWN", fields[0].DataType, true);
                    break;
                case "type":
                    fields[1] = new Field(fields[1].Name, Int32Type.Default, true);
                    break;
            }
            using var http = CreateHttpClient(overlappingNativeColumns: true, columnFields: fields);
            using var connection = CreateConnection(http);

            var exception = await Assert.ThrowsAsync<DatabricksException>(() =>
                connection.ReadColumnsAsync(null, null, null, null, CancellationToken.None));

            Assert.StartsWith("Invalid native GetColumns result:", exception.Message);
            Assert.Contains(expectedError, exception.Message);
        }

        [Theory]
        [InlineData("foo_bar", "foo_bar", 1)]
        [InlineData("FOO_BAR", "foo_bar", 1)]
        [InlineData("foo%bar", "foo%bar", 1)]
        [InlineData("FOO%BAR", "foo%bar", 1)]
        [InlineData("fooxbar", "foo_bar", 1)]
        [InlineData("missing", "foo_bar", 0)]
        [InlineData(null, "foo_bar", 2)]
        [InlineData("SPARK", "foo_bar", 2)]
        public void GetTableSchema_UsesExactNativeCatalogScope(
            string? requestedCatalog, string nativeCatalog, int expectedFields)
        {
            using var http = CreateHttpClient(overlappingNativeColumns: true, nativeCatalog: nativeCatalog);
            using var connection = CreateConnection(http);

            var schema = connection.GetTableSchema(requestedCatalog, "default", "t");

            Assert.Equal(expectedFields, schema.FieldsList.Count);
            Assert.All(schema.FieldsList, field => Assert.Equal("a", field.Name));
        }

        [Fact]
        public async Task ColumnFanout_FiltersOverlappingNativeCatalogNames()
        {
            using var http = CreateHttpClient(overlappingNativeColumns: true);
            using var connection = CreateConnection(http);

            var columns = await connection.ReadColumnsAsync(null, null, null, null, CancellationToken.None);

            Assert.True(columns.IsNative);
            Assert.Equal(2, columns.Batches.Count);
            Assert.All(columns.Batches, batch => Assert.Equal(2, batch.Length));
            Assert.Equal(new[] { "foo_bar", "fooxbar" }, columns.Rows.Select(row => row.Catalog));

            var result = NativeMetadataResultBuilder.Build(
                columns.Batches,
                MetadataSchemaFactory.CreateColumnMetadataSchema(), MetadataOperation.GetColumns,
                sourceCatalogs: columns.SourceCatalogs, requireExactSourceCatalog: true);
            using var stream = result.Stream!;
            Assert.Equal(2, result.RowCount);

            var catalogMap = new Dictionary<string, Dictionary<string, Dictionary<string, TableInfo>>>();
            foreach (string catalog in new[] { "foo_bar", "fooxbar" })
            {
                catalogMap[catalog] = new Dictionary<string, Dictionary<string, TableInfo>>
                {
                    ["default"] = new Dictionary<string, TableInfo> { ["t"] = new TableInfo("TABLE") },
                };
            }
            await ((IGetObjectsDataProvider)connection).PopulateColumnInfoAsync(
                null, null, null, null, catalogMap, CancellationToken.None);
            Assert.Single(catalogMap["foo_bar"]["default"]["t"].ColumnName);
            Assert.Single(catalogMap["fooxbar"]["default"]["t"].ColumnName);
            Assert.True(catalogMap["foo_bar"]["default"]["t"].IsAutoIncrement.Single());
        }
    }
}
