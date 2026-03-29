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
    /// Tests that the UsePreviewEndpoint property routes requests to
    /// /2.0/preview/sql/... instead of /api/2.0/sql/... paths.
    /// </summary>
    public class StatementExecutionConnectionPreviewEndpointTests
    {
        private static Dictionary<string, string> BaseProperties() => new()
        {
            { SparkParameters.HostName, "test.databricks.com" },
            { DatabricksParameters.WarehouseId, "test-warehouse" },
            { SparkParameters.AccessToken, "test-token" },
            { "adbc.connection.catalog", "main" }
        };

        private static (HttpClient, List<Uri?>) CreateCapturingHttpClient()
        {
            var capturedUris = new List<Uri?>();
            var mockHandler = new Mock<HttpMessageHandler>();
            var sessionResponse = JsonSerializer.Serialize(new { session_id = "session-123" });

            mockHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>((req, _) =>
                {
                    capturedUris.Add(req.RequestUri);
                    return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new StringContent(sessionResponse)
                    });
                });

            return (new HttpClient(mockHandler.Object), capturedUris);
        }

        [Fact]
        public async Task OpenAsync_WithPreviewEndpointTrue_UsesPreviewSessionPath()
        {
            var (httpClient, uris) = CreateCapturingHttpClient();
            var properties = BaseProperties();
            properties[DatabricksParameters.UsePreviewEndpoint] = "true";

            using var conn = new StatementExecutionConnection(properties, httpClient);
            await conn.OpenAsync(CancellationToken.None);

            Assert.Single(uris);
            Assert.Contains("/2.0/preview/sql/sessions", uris[0]?.ToString());
        }

        [Fact]
        public async Task OpenAsync_WithPreviewEndpointFalse_UsesStandardSessionPath()
        {
            var (httpClient, uris) = CreateCapturingHttpClient();
            var properties = BaseProperties();
            properties[DatabricksParameters.UsePreviewEndpoint] = "false";

            using var conn = new StatementExecutionConnection(properties, httpClient);
            await conn.OpenAsync(CancellationToken.None);

            Assert.Single(uris);
            Assert.Contains("/api/2.0/sql/sessions", uris[0]?.ToString());
        }

        [Fact]
        public async Task OpenAsync_WithoutPreviewEndpointProperty_UsesStandardSessionPath()
        {
            var (httpClient, uris) = CreateCapturingHttpClient();
            var properties = BaseProperties();
            // UsePreviewEndpoint not set — should default to standard path

            using var conn = new StatementExecutionConnection(properties, httpClient);
            await conn.OpenAsync(CancellationToken.None);

            Assert.Single(uris);
            Assert.Contains("/api/2.0/sql/sessions", uris[0]?.ToString());
        }
    }
}
