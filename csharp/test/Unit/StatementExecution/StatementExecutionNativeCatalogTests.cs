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
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.StatementExecution;
using AdbcDrivers.HiveServer2.Hive2;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow;
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

        private static HttpClient CreateHttpClient(string? otherFailureState = null)
        {
            byte[] catalogs = ArrowStrings("TABLE_CAT", "main", "other");
            byte[] columns = ArrowStrings("col_name", "a");
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
                    if (otherFailureState != null && sql.Contains("`other`"))
                    {
                        string failedBody = JsonSerializer.Serialize(new
                        {
                            statement_id = "failed",
                            status = new
                            {
                                state = "FAILED",
                                sql_state = otherFailureState,
                                error = new { error_code = "QUERY_ERROR", message = "catalog query failed" },
                            },
                        });
                        return new HttpResponseMessage(HttpStatusCode.OK)
                        {
                            Content = new StringContent(failedBody),
                        };
                    }

                    bool isCatalogs = sql.StartsWith("SHOW CATALOGS");
                    string name = isCatalogs ? "TABLE_CAT" : "col_name";
                    var body = JsonSerializer.Serialize(new
                    {
                        statement_id = "stmt-1",
                        status = new { state = "SUCCEEDED" },
                        manifest = new
                        {
                            is_native_metadata_result = isCatalogs,
                            total_row_count = isCatalogs ? 2 : 1,
                            schema = new
                            {
                                column_count = 1,
                                columns = new[] { new { name, position = 0, type_name = "STRING", type_text = "STRING" } },
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
        public async Task ColumnFanout_KeepsTheQueriedCatalog()
        {
            using var http = CreateHttpClient();
            using var connection = CreateConnection(http);

            var batches = await connection.ExecuteNativeShowColumnsAsync(null, null, null, null, CancellationToken.None);

            Assert.Equal(2, batches.Count);
            Assert.Equal("main", batches[0].Catalog);
            Assert.Equal("other", batches[1].Catalog);
        }

        [Fact]
        public async Task ColumnFanout_PropagatesUnrelatedCatalogFailure()
        {
            using var http = CreateHttpClient(otherFailureState: "XX000");
            using var connection = CreateConnection(http);

            await Assert.ThrowsAsync<DatabricksException>(() =>
                connection.ExecuteNativeShowColumnsAsync(null, null, null, null, CancellationToken.None));
        }

        [Fact]
        public async Task ColumnFanout_SkipsSqlPermissionDeniedCatalog()
        {
            using var http = CreateHttpClient(otherFailureState: "42501");
            using var connection = CreateConnection(http);

            var batches = await connection.ExecuteNativeShowColumnsAsync(null, null, null, null, CancellationToken.None);

            Assert.Single(batches);
            Assert.Equal("main", batches[0].Catalog);
        }
    }
}
