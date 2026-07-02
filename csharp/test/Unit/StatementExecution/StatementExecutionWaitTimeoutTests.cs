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
using Moq;
using Moq.Protected;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    /// <summary>
    /// Verifies the SEA (Statement Execution / REST) path derives wait_timeout from the direct-results
    /// flag rather than exposing it as a customer knob: direct results ON (default) → wait_timeout is
    /// omitted (server returns a direct result in one round-trip); direct results OFF → wait_timeout="0s"
    /// (async + poll). A non-zero wait_timeout would route to the old sync-hybrid path that truncates
    /// multi-chunk results (ES-2034600). Also verifies state=CLOSED is treated as success (the direct
    /// result is read from that response) rather than throwing.
    /// </summary>
    public class StatementExecutionWaitTimeoutTests
    {
        private static Dictionary<string, string> BaseProps() => new Dictionary<string, string>
        {
            { SparkParameters.HostName, "test.databricks.com" },
            { DatabricksParameters.WarehouseId, "wh-1" },
            { SparkParameters.AccessToken, "token" },
        };

        private static (HttpClient Http, Func<string?> GetBody) CapturingHttp(string statementsResponseJson)
        {
            string? captured = null;
            var sessionBody = JsonSerializer.Serialize(new { session_id = "s1" });
            var handler = new Mock<HttpMessageHandler>();
            handler.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync((HttpRequestMessage req, CancellationToken _) =>
                {
                    var path = req.RequestUri?.AbsolutePath ?? string.Empty;
                    if (path.EndsWith("/api/2.0/sql/sessions"))
                        return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(sessionBody) };
                    if (path.EndsWith("/api/2.0/sql/statements") && req.Method == HttpMethod.Post)
                        captured = req.Content?.ReadAsStringAsync().GetAwaiter().GetResult();
                    return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(statementsResponseJson) };
                });
            return (new HttpClient(handler.Object), () => captured);
        }

        private static readonly string FailedBody = JsonSerializer.Serialize(new
        {
            statement_id = "x",
            status = new { state = "FAILED", error = new { error_code = "STOP", message = "stop after capture" } },
        });

        private static async Task<string?> CaptureExecuteBodyAsync(Dictionary<string, string> props)
        {
            var (http, getBody) = CapturingHttp(FailedBody);
            using (http)
            {
                var connection = new StatementExecutionConnection(props, http);
                using var stmt = (StatementExecutionStatement)connection.CreateStatement();
                stmt.SqlQuery = "SELECT 1";
                await Assert.ThrowsAnyAsync<Exception>(() => stmt.ExecuteQueryAsync(CancellationToken.None));
            }
            return getBody();
        }

        [Fact]
        public async Task DirectResultsEnabled_OmitsWaitTimeout()
        {
            var body = await CaptureExecuteBodyAsync(BaseProps());
            Assert.NotNull(body);
            using var doc = JsonDocument.Parse(body!);
            Assert.False(doc.RootElement.TryGetProperty("wait_timeout", out _),
                "wait_timeout must be omitted when direct results are enabled (default)");
        }

        [Fact]
        public async Task DirectResultsDisabled_SendsZeroWaitTimeout()
        {
            var props = BaseProps();
            props[DatabricksParameters.EnableDirectResults] = "false";
            var body = await CaptureExecuteBodyAsync(props);
            Assert.NotNull(body);
            using var doc = JsonDocument.Parse(body!);
            Assert.Equal("0s", doc.RootElement.GetProperty("wait_timeout").GetString());
        }

        [Fact]
        public async Task WaitTimeoutProperty_IsIgnored_NotCustomerTunable()
        {
            var props = BaseProps();
            props[DatabricksParameters.WaitTimeout] = "30"; // must be ignored (derived from direct results)
            var body = await CaptureExecuteBodyAsync(props);
            Assert.NotNull(body);
            using var doc = JsonDocument.Parse(body!);
            Assert.False(doc.RootElement.TryGetProperty("wait_timeout", out _),
                "adbc.databricks.rest.wait_timeout must be ignored; direct results (default) → omitted");
        }

        [Fact]
        public async Task ClosedState_WithDirectResult_ReturnsRows_NotException()
        {
            byte[] arrow = BuildSingleRowIntArrow("id", 42);
            var closedBody = JsonSerializer.Serialize(new
            {
                statement_id = "x",
                status = new { state = "CLOSED" },
                manifest = new
                {
                    total_chunk_count = 1,
                    total_row_count = 1,
                    schema = new
                    {
                        column_count = 1,
                        columns = new[] { new { name = "id", position = 0, type_name = "INT", type_text = "INT" } },
                    },
                },
                result = new { chunk_index = 0, row_count = 1, attachment = arrow },
            });

            var (http, _) = CapturingHttp(closedBody);
            using (http)
            {
                var connection = new StatementExecutionConnection(BaseProps(), http);
                using var stmt = (StatementExecutionStatement)connection.CreateStatement();
                stmt.SqlQuery = "SELECT 42 AS id";

                // Must NOT throw on CLOSED — the direct result is present in the response.
                var result = await stmt.ExecuteQueryAsync(CancellationToken.None);
                using var reader = result.Stream;
                var batch = await reader.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
                Assert.Equal(1, batch!.Length);
            }
        }

        private static byte[] BuildSingleRowIntArrow(string column, int value)
        {
            var schema = new Schema(new[] { new Field(column, Int32Type.Default, nullable: false) }, null);
            var arr = new Int32Array.Builder().Append(value).Build();
            var batch = new RecordBatch(schema, new IArrowArray[] { arr }, 1);
            using var ms = new MemoryStream();
            using (var writer = new ArrowStreamWriter(ms, schema))
            {
                writer.WriteRecordBatch(batch);
                writer.WriteEnd();
            }
            return ms.ToArray();
        }
    }
}
