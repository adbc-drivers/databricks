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
    /// Tests that SSP parameters (adbc.databricks.ssp_*) are forwarded
    /// as session_confs in the CreateSession request (PECO-2936).
    /// </summary>
    public class StatementExecutionConnectionSspTests
    {
        private static Dictionary<string, string> BaseProperties() => new()
        {
            { SparkParameters.HostName, "test.databricks.com" },
            { DatabricksParameters.WarehouseId, "test-warehouse" },
            { SparkParameters.AccessToken, "test-token" }
        };

        private static (HttpClient, Mock<HttpMessageHandler>, List<string>) CreateCapturingHttpClient()
        {
            var capturedBodies = new List<string>();
            var mockHandler = new Mock<HttpMessageHandler>();
            var sessionResponse = JsonSerializer.Serialize(new { session_id = "session-123" });

            mockHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>(async (req, _) =>
                {
                    if (req.Content != null)
                        capturedBodies.Add(await req.Content.ReadAsStringAsync());
                    return new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new StringContent(sessionResponse)
                    };
                });

            return (new HttpClient(mockHandler.Object), mockHandler, capturedBodies);
        }

        [Fact]
        public async Task OpenAsync_WithSspProperties_IncludesSessionConfs()
        {
            var (httpClient, _, bodies) = CreateCapturingHttpClient();
            var properties = BaseProperties();
            properties["adbc.databricks.ssp_ansi_mode"] = "true";
            properties["adbc.databricks.ssp_timezone"] = "UTC";

            using var conn = new StatementExecutionConnection(properties, httpClient);
            await conn.OpenAsync(CancellationToken.None);

            Assert.Single(bodies);
            var body = bodies[0];
            Assert.Contains("\"session_confs\"", body);
            Assert.Contains("ansi_mode", body);
            Assert.Contains("timezone", body);
            Assert.Contains("\"true\"", body);
            Assert.Contains("\"UTC\"", body);
        }

        [Fact]
        public async Task OpenAsync_WithNoSspProperties_OmitsSessionConfs()
        {
            var (httpClient, _, bodies) = CreateCapturingHttpClient();

            using var conn = new StatementExecutionConnection(BaseProperties(), httpClient);
            await conn.OpenAsync(CancellationToken.None);

            Assert.Single(bodies);
            Assert.DoesNotContain("session_confs", bodies[0]);
        }

        [Fact]
        public async Task OpenAsync_SspPropertyNameHasPrefixStripped()
        {
            var (httpClient, _, bodies) = CreateCapturingHttpClient();
            var properties = BaseProperties();
            properties["adbc.databricks.ssp_use_cached_result"] = "false";

            using var conn = new StatementExecutionConnection(properties, httpClient);
            await conn.OpenAsync(CancellationToken.None);

            var body = bodies[0];
            // Prefix stripped: "use_cached_result", not "adbc.databricks.ssp_use_cached_result"
            Assert.Contains("use_cached_result", body);
            Assert.DoesNotContain("adbc.databricks.ssp_", body);
        }

        [Fact]
        public async Task OpenAsync_NonSspPropertiesExcludedFromSessionConfs()
        {
            var (httpClient, _, bodies) = CreateCapturingHttpClient();
            var properties = BaseProperties();
            properties["adbc.databricks.ssp_ansi_mode"] = "true";
            properties["adbc.databricks.some_other_param"] = "value";
            properties["custom.param"] = "value";

            using var conn = new StatementExecutionConnection(properties, httpClient);
            await conn.OpenAsync(CancellationToken.None);

            var body = bodies[0];
            Assert.Contains("ansi_mode", body);
            Assert.DoesNotContain("some_other_param", body);
            Assert.DoesNotContain("custom.param", body);
        }

        [Fact]
        public async Task OpenAsync_InvalidSspPropertyName_IsFiltered()
        {
            var (httpClient, _, bodies) = CreateCapturingHttpClient();
            var properties = BaseProperties();
            properties["adbc.databricks.ssp_ansi_mode"] = "true";          // valid
            properties["adbc.databricks.ssp_bad name!"] = "oops";           // invalid: space + !
            properties["adbc.databricks.ssp_also;bad"] = "oops";            // invalid: semicolon

            using var conn = new StatementExecutionConnection(properties, httpClient);
            await conn.OpenAsync(CancellationToken.None);

            var body = bodies[0];
            Assert.Contains("ansi_mode", body);
            Assert.DoesNotContain("bad name", body);
            Assert.DoesNotContain("also;bad", body);
        }

        [Fact]
        public async Task OpenAsync_SspPropertyWithDots_IsIncluded()
        {
            var (httpClient, _, bodies) = CreateCapturingHttpClient();
            var properties = BaseProperties();
            properties["adbc.databricks.ssp_spark.sql.ansi.enabled"] = "true";

            using var conn = new StatementExecutionConnection(properties, httpClient);
            await conn.OpenAsync(CancellationToken.None);

            Assert.Contains("spark.sql.ansi.enabled", bodies[0]);
        }

        [Fact]
        public async Task OpenAsync_SessionConfsNotSentTwice_WhenCalledTwice()
        {
            var (httpClient, _, bodies) = CreateCapturingHttpClient();
            var properties = BaseProperties();
            properties["adbc.databricks.ssp_ansi_mode"] = "true";

            using var conn = new StatementExecutionConnection(properties, httpClient);
            await conn.OpenAsync(CancellationToken.None);
            await conn.OpenAsync(CancellationToken.None); // second call is no-op

            // CreateSession should only be called once
            Assert.Single(bodies);
        }
    }
}
