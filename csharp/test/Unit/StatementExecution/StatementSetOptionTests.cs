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
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.Spark;
using Microsoft.IO;
using Moq;
using Moq.Protected;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    /// <summary>
    /// Unit tests verifying that unrecognized options are silently dropped by SetOption
    /// instead of throwing exceptions (PECO-2952).
    /// </summary>
    public class StatementSetOptionTests
    {
        private static StatementExecutionStatement CreateStatement()
        {
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.HostName, "test.databricks.com" },
                { DatabricksParameters.WarehouseId, "wh-1" },
                { SparkParameters.AccessToken, "token" },
            };

            var handlerMock = new Mock<HttpMessageHandler>();
            handlerMock.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(
                        JsonSerializer.Serialize(new { session_id = "s1" }))
                });
            var httpClient = new HttpClient(handlerMock.Object);

            var connection = new StatementExecutionConnection(properties, httpClient);
            return new StatementExecutionStatement(
                client: Mock.Of<IStatementExecutionClient>(),
                sessionId: "session-1",
                warehouseId: "wh-1",
                catalog: null,
                schema: null,
                resultDisposition: "INLINE_OR_EXTERNAL_LINKS",
                resultFormat: "ARROW_STREAM",
                resultCompression: null,
                waitTimeoutSeconds: 0,
                pollingIntervalMs: 50,
                properties: properties,
                recyclableMemoryStreamManager: new RecyclableMemoryStreamManager(),
                lz4BufferPool: System.Buffers.ArrayPool<byte>.Shared,
                httpClient: httpClient,
                connection: connection);
        }

        [Fact]
        public void SetOption_UnrecognizedKey_DoesNotThrow()
        {
            var statement = CreateStatement();
            // Should not throw for a completely unknown option
            statement.SetOption("adbc.databricks.unknown_future_option", "some_value");
        }

        [Fact]
        public void SetOption_QueryTagsKey_DoesNotThrow()
        {
            var statement = CreateStatement();
            // query_tags is a known Databricks option — should still be silently accepted
            statement.SetOption(DatabricksParameters.QueryTags, "tag1=value1");
        }

        [Fact]
        public void SetOption_ArbitraryPrefix_DoesNotThrow()
        {
            var statement = CreateStatement();
            // Options with unrecognized prefixes should be silently dropped
            statement.SetOption("adbc.somevendor.some_option", "value");
        }
    }
}
