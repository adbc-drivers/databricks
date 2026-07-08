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
    /// <summary>
    /// Verifies the SEA (Statement Execution / REST) path derives its result_compression
    /// default from the client's LZ4 capability flag (adbc.databricks.cloudfetch.lz4.enabled,
    /// default true) — LZ4_FRAME when enabled, NONE when disabled — so a single flag drives LZ4
    /// across both the Thrift/CloudFetch and REST paths. An explicit result_compression property
    /// always overrides the derived default.
    ///
    /// Mocks at the HttpMessageHandler level and captures the ExecuteStatement request body:
    /// CreateSession returns a session id, and the ExecuteStatement POST is captured before a
    /// FAILED response short-circuits the rest of execution (we only assert on the request).
    /// </summary>
    public class StatementExecutionResultCompressionTests
    {
        private static (HttpClient Http, Func<string?> GetExecuteBody) HttpClientCapturingExecuteBody()
        {
            string? capturedBody = null;
            var sessionBody = JsonSerializer.Serialize(new { session_id = "session-1" });
            var failedBody = JsonSerializer.Serialize(new
            {
                statement_id = "stmt-1",
                status = new
                {
                    state = "FAILED",
                    error = new { error_code = "STOP_AFTER_CAPTURE", message = "stop after capture" },
                },
            });

            var handler = new Mock<HttpMessageHandler>();
            handler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync((HttpRequestMessage req, CancellationToken _) =>
                {
                    var path = req.RequestUri?.AbsolutePath ?? string.Empty;
                    if (path.EndsWith("/api/2.0/sql/sessions"))
                    {
                        return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(sessionBody) };
                    }

                    if (path.EndsWith("/api/2.0/sql/statements") && req.Method == HttpMethod.Post)
                    {
                        capturedBody = req.Content?.ReadAsStringAsync().GetAwaiter().GetResult();
                    }

                    return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(failedBody) };
                });

            return (new HttpClient(handler.Object), () => capturedBody);
        }

        private static Dictionary<string, string> BaseProperties() => new Dictionary<string, string>
        {
            { SparkParameters.HostName, "test.databricks.com" },
            { DatabricksParameters.WarehouseId, "wh-1" },
            { SparkParameters.AccessToken, "token" },
        };

        private static async Task<string?> CaptureExecuteStatementBodyAsync(Dictionary<string, string> properties)
        {
            var (http, getBody) = HttpClientCapturingExecuteBody();
            using (http)
            {
                var connection = new StatementExecutionConnection(properties, http);
                using var stmt = (StatementExecutionStatement)connection.CreateStatement();
                stmt.SqlQuery = "SELECT 1";
                // The FAILED execute response throws; we only care about the captured request body.
                await Assert.ThrowsAnyAsync<Exception>(() => stmt.ExecuteQueryAsync(CancellationToken.None));
            }

            return getBody();
        }

        [Fact]
        public async Task ExecuteStatement_DefaultsToLz4FrameCompression()
        {
            var body = await CaptureExecuteStatementBodyAsync(BaseProperties());

            Assert.NotNull(body);
            using var doc = JsonDocument.Parse(body!);
            Assert.Equal("LZ4_FRAME", doc.RootElement.GetProperty("result_compression").GetString());
        }

        [Fact]
        public async Task ExecuteStatement_RespectsExplicitCompressionOverride()
        {
            var properties = BaseProperties();
            properties[DatabricksParameters.ResultCompression] = "NONE";

            var body = await CaptureExecuteStatementBodyAsync(properties);

            Assert.NotNull(body);
            using var doc = JsonDocument.Parse(body!);
            Assert.Equal("NONE", doc.RootElement.GetProperty("result_compression").GetString());
        }

        [Fact]
        public async Task ExecuteStatement_DerivesNoneWhenLz4Disabled()
        {
            var properties = BaseProperties();
            // Pre-existing capability flag disabled, no REST-specific override → derive NONE.
            properties[DatabricksParameters.CanDecompressLz4] = "false";

            var body = await CaptureExecuteStatementBodyAsync(properties);

            Assert.NotNull(body);
            using var doc = JsonDocument.Parse(body!);
            Assert.Equal("NONE", doc.RootElement.GetProperty("result_compression").GetString());
        }

        [Fact]
        public async Task ExecuteStatement_ExplicitCompressionOverridesLz4DisabledFlag()
        {
            var properties = BaseProperties();
            // Explicit rest.result_compression wins even when the capability flag is disabled.
            properties[DatabricksParameters.CanDecompressLz4] = "false";
            properties[DatabricksParameters.ResultCompression] = "LZ4_FRAME";

            var body = await CaptureExecuteStatementBodyAsync(properties);

            Assert.NotNull(body);
            using var doc = JsonDocument.Parse(body!);
            Assert.Equal("LZ4_FRAME", doc.RootElement.GetProperty("result_compression").GetString());
        }

        [Theory]
        [InlineData("JSON_ARRAY")]
        [InlineData("CSV")]
        public async Task ExecuteStatement_DerivesNoneForNonArrowFormat(string resultFormat)
        {
            var properties = BaseProperties();
            // Compression is only meaningful for ARROW_STREAM; for JSON_ARRAY/CSV the LZ4 default
            // must NOT be derived even when the capability flag is enabled (its default).
            properties[DatabricksParameters.ResultFormat] = resultFormat;

            var body = await CaptureExecuteStatementBodyAsync(properties);

            Assert.NotNull(body);
            using var doc = JsonDocument.Parse(body!);
            Assert.Equal("NONE", doc.RootElement.GetProperty("result_compression").GetString());
        }

        [Fact]
        public async Task ExecuteStatement_ExplicitCompressionHonoredForNonArrowFormat()
        {
            var properties = BaseProperties();
            // An explicit override still takes precedence regardless of format.
            properties[DatabricksParameters.ResultFormat] = "JSON_ARRAY";
            properties[DatabricksParameters.ResultCompression] = "LZ4_FRAME";

            var body = await CaptureExecuteStatementBodyAsync(properties);

            Assert.NotNull(body);
            using var doc = JsonDocument.Parse(body!);
            Assert.Equal("LZ4_FRAME", doc.RootElement.GetProperty("result_compression").GetString());
        }

        [Fact]
        public async Task ExecuteUpdate_ReadsLz4FramedNumAffectedRows()
        {
            // Regression: with LZ4 requested by default, the server returns the inline
            // num_affected_rows result LZ4-framed. ExecuteUpdate must decode it (via the shared
            // CreateReader route) and return the real count rather than silently falling back
            // to -1 when the frame can't be parsed as raw Arrow IPC.
            byte[] lz4Attachment = BuildLz4FramedNumAffectedRows(42);
            var executeBody = JsonSerializer.Serialize(new
            {
                statement_id = "stmt-1",
                status = new { state = "SUCCEEDED" },
                manifest = new
                {
                    result_compression = "LZ4_FRAME",
                    total_row_count = 1,
                    schema = new
                    {
                        column_count = 1,
                        columns = new[]
                        {
                            new { name = "num_affected_rows", position = 0, type_name = "LONG", type_text = "BIGINT" },
                        },
                    },
                },
                result = new { attachment = lz4Attachment },
            });

            using var http = HttpClientReturningExecuteResponse(executeBody);
            var connection = new StatementExecutionConnection(BaseProperties(), http);
            using var stmt = (StatementExecutionStatement)connection.CreateStatement();
            stmt.SqlQuery = "DELETE FROM t WHERE 1 = 1";

            var result = await stmt.ExecuteUpdateAsync(CancellationToken.None);

            Assert.Equal(42, result.AffectedRows);
        }

        [Fact]
        public async Task MetadataExecution_UsesLz4Compression_LikeRegularQueries()
        {
            // Metadata executions no longer special-case compression. The ES-2034600 truncation
            // (large inline multi-chunk metadata results dropping chunks under LZ4) was rooted in
            // sending a non-zero wait_timeout, which routed the request to the broken SEA
            // sync-hybrid results path. Since wait_timeout is now only ever unset (direct results)
            // or "0s" (async) and never positive, multi-chunk results are delivered correctly, so
            // metadata requests the same LZ4 default as regular queries.
            var (http, getBody) = HttpClientCapturingExecuteBody();
            using (http)
            {
                var connection = new StatementExecutionConnection(BaseProperties(), http);
                // ExecuteMetadataSqlAsync runs with isMetadataExecution=true. The mocked FAILED
                // response throws after the request is captured; we only assert on the request body.
                await Assert.ThrowsAnyAsync<Exception>(
                    () => connection.ExecuteMetadataSqlAsync("SHOW COLUMNS IN CATALOG main", CancellationToken.None));
            }

            var body = getBody();
            Assert.NotNull(body);
            using var doc = JsonDocument.Parse(body!);
            Assert.Equal("LZ4_FRAME", doc.RootElement.GetProperty("result_compression").GetString());
        }

        /// <summary>
        /// Builds an LZ4-framed Arrow IPC stream containing a single-row <c>num_affected_rows</c>
        /// Int64 column — the exact shape the SEA server returns for a DML statement when LZ4
        /// result compression was requested.
        /// </summary>
        private static byte[] BuildLz4FramedNumAffectedRows(long value)
        {
            var schema = new Schema(new[] { new Field("num_affected_rows", Int64Type.Default, nullable: false) }, null);
            var column = new Int64Array.Builder().Append(value).Build();
            var batch = new RecordBatch(schema, new IArrowArray[] { column }, 1);

            byte[] rawArrow;
            using (var raw = new MemoryStream())
            {
                using (var writer = new ArrowStreamWriter(raw, schema))
                {
                    writer.WriteRecordBatch(batch);
                    writer.WriteEnd();
                }
                rawArrow = raw.ToArray();
            }

            using var framed = new MemoryStream();
            using (var lz4 = LZ4Stream.Encode(framed, leaveOpen: true))
            {
                lz4.Write(rawArrow, 0, rawArrow.Length);
            }
            return framed.ToArray();
        }

        private static HttpClient HttpClientReturningExecuteResponse(string executeBodyJson)
        {
            var sessionBody = JsonSerializer.Serialize(new { session_id = "session-1" });

            var handler = new Mock<HttpMessageHandler>();
            handler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync((HttpRequestMessage req, CancellationToken _) =>
                {
                    var path = req.RequestUri?.AbsolutePath ?? string.Empty;
                    if (path.EndsWith("/api/2.0/sql/sessions"))
                    {
                        return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(sessionBody) };
                    }

                    return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(executeBodyJson) };
                });

            return new HttpClient(handler.Object);
        }
    }
}
