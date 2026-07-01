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
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.StatementExecution;
using AdbcDrivers.HiveServer2.Spark;
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
    }
}
