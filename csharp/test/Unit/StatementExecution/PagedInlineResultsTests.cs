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
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.StatementExecution;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using K4os.Compression.LZ4.Streams;
using Moq;
using Moq.Protected;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    public class PagedInlineResultsTests
    {
        private static byte[] Attachment(long value, bool compressed)
        {
            var schema = new Schema.Builder().Field(f => f.Name("n").DataType(Int64Type.Default)).Build();
            using var batch = new RecordBatch(schema, new IArrowArray[] { new Int64Array.Builder().Append(value).Build() }, 1);
            using var output = new MemoryStream();
            using (var writer = new ArrowStreamWriter(output, schema, leaveOpen: true))
            {
                writer.WriteRecordBatchAsync(batch).GetAwaiter().GetResult();
                writer.WriteEndAsync().GetAwaiter().GetResult();
            }
            if (!compressed) return output.ToArray();
            using var framed = new MemoryStream();
            using (var encoder = LZ4Stream.Encode(framed, leaveOpen: true))
            {
                byte[] raw = output.ToArray();
                encoder.Write(raw, 0, raw.Length);
            }
            return framed.ToArray();
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task ReadsPagesLazilyUsingOffsetsWithoutManifestTotals(bool compressed)
        {
            var paths = new List<string>();
            var handler = new Mock<HttpMessageHandler>();
            handler.Protected().Setup<Task<HttpResponseMessage>>("SendAsync",
                ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>((request, _) =>
                {
                    string path = request.RequestUri!.PathAndQuery;
                    paths.Add(path);
                    object response;
                    if (path.EndsWith("/sessions")) response = new { session_id = "session-1" };
                    else if (path.EndsWith("/statements")) response = new
                    {
                        statement_id = "statement-1", status = new { state = "SUCCEEDED" },
                        manifest = new
                        {
                            result_compression = compressed ? "LZ4_FRAME" : "NONE",
                            schema = new { column_count = 1, columns = new[] {
                                new { name = "n", position = 0, type_name = "LONG", type_text = "BIGINT" } } }
                        },
                        result = new ResultData { ChunkIndex = 0, RowOffset = 0, RowCount = 1,
                            NextChunkIndex = 1, Attachment = Attachment(7, compressed) }
                    };
                    else if (path.EndsWith("/chunks/1?row_offset=1")) response = new ResultData
                    {
                        ChunkIndex = 1, RowOffset = 1, RowCount = 1, NextChunkIndex = 1,
                        Attachment = Attachment(8, compressed)
                    };
                    else if (path.EndsWith("/chunks/1?row_offset=2")) response = new ResultData
                    {
                        ChunkIndex = 1, RowOffset = 2, RowCount = 1, Attachment = Attachment(9, compressed)
                    };
                    else response = new { };
                    return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
                    { Content = new StringContent(JsonSerializer.Serialize(response)) });
                });
            using var http = new HttpClient(handler.Object);
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.HostName, "test.databricks.com" },
                { DatabricksParameters.WarehouseId, "warehouse-1" },
                { SparkParameters.AccessToken, "test-token" },
                { "adbc.connection.catalog", "main" }
            };
            using var connection = new StatementExecutionConnection(properties, http);
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT n FROM values_table";
            var result = await statement.ExecuteQueryAsync();
            using var stream = result.Stream;
            Assert.DoesNotContain(paths, p => p.Contains("/chunks/"));
            foreach (long expected in new long[] { 7, 8, 9 })
            {
                using var batch = await stream.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
                Assert.Equal(expected, ((Int64Array)batch!.Column(0)).GetValue(0));
            }
            Assert.Null(await stream.ReadNextRecordBatchAsync());
            Assert.Equal(2, paths.FindAll(p => p.Contains("/chunks/")).Count);
        }
    }
}
