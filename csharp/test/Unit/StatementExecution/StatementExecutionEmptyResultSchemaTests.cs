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
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.IO;
using Moq;
using Moq.Protected;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    /// <summary>
    /// Tests that schema metadata is correctly preserved for empty result sets in SEA mode.
    ///
    /// In SEA (Statement Execution API / REST) mode, the schema lives in ResultManifest.schema
    /// and must be returned by IArrowArrayStream.Schema even when the result contains zero rows
    /// (e.g. SELECT * FROM empty_table). This mirrors the JDBC driver behaviour where
    /// DatabricksResultSetMetaData is always constructed from ResultManifest independently
    /// of data presence.
    /// </summary>
    public class StatementExecutionEmptyResultSchemaTests
    {
        private const string StatementId = "stmt-empty-schema-test";

        private static StatementExecutionStatement CreateStatement(
            IStatementExecutionClient client)
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
                client,
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

        private static ResultManifest BuildManifest(params (string name, string typeName)[] columns)
        {
            var columnInfos = new List<ColumnInfo>();
            foreach (var (name, typeName) in columns)
            {
                columnInfos.Add(new ColumnInfo { Name = name, TypeName = typeName });
            }

            return new ResultManifest
            {
                Format = "ARROW_STREAM",
                Schema = new ResultSchema { Columns = columnInfos },
                TotalRowCount = 0,
                Chunks = new List<ResultChunk>(),
            };
        }

        [Fact]
        public async Task ExecuteQuery_EmptyTable_SchemaContainsCorrectColumns()
        {
            // Arrange: server returns SUCCEEDED with schema but no data
            var manifest = BuildManifest(("id", "INT"), ("name", "STRING"), ("score", "DOUBLE"));

            var mockClient = new Mock<IStatementExecutionClient>();
            mockClient
                .Setup(c => c.ExecuteStatementAsync(
                    It.IsAny<ExecuteStatementRequest>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ExecuteStatementResponse
                {
                    StatementId = StatementId,
                    Status = new StatementStatus { State = "SUCCEEDED" },
                    Manifest = manifest,
                    Result = new ResultData { Attachment = null },
                });

            using var stmt = CreateStatement(mockClient.Object);
            stmt.SqlQuery = "SELECT id, name, score FROM empty_table";

            // Act
            var queryResult = await stmt.ExecuteQueryAsync(CancellationToken.None);

            // Assert: schema must reflect the table columns, not an empty schema
            var stream = queryResult.Stream;
            Assert.NotNull(stream);
            Assert.Equal(3, stream.Schema.FieldsList.Count);
            Assert.Equal("id", stream.Schema.FieldsList[0].Name);
            Assert.Equal("name", stream.Schema.FieldsList[1].Name);
            Assert.Equal("score", stream.Schema.FieldsList[2].Name);

            // No record batches (empty table)
            var batch = await stream.ReadNextRecordBatchAsync(CancellationToken.None);
            Assert.Null(batch);
        }

        [Fact]
        public async Task ExecuteQuery_EmptyTable_ArrowTypesAreMappedCorrectly()
        {
            // Arrange: various Databricks SQL types
            var manifest = BuildManifest(
                ("a", "INT"),
                ("b", "BIGINT"),
                ("c", "STRING"),
                ("d", "BOOLEAN"),
                ("e", "DOUBLE"),
                ("f", "DATE"),
                ("g", "TIMESTAMP"));

            var mockClient = new Mock<IStatementExecutionClient>();
            mockClient
                .Setup(c => c.ExecuteStatementAsync(
                    It.IsAny<ExecuteStatementRequest>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ExecuteStatementResponse
                {
                    StatementId = StatementId,
                    Status = new StatementStatus { State = "SUCCEEDED" },
                    Manifest = manifest,
                    Result = new ResultData { Attachment = null },
                });

            using var stmt = CreateStatement(mockClient.Object);
            stmt.SqlQuery = "SELECT * FROM typed_empty_table";

            var queryResult = await stmt.ExecuteQueryAsync(CancellationToken.None);
            var fields = queryResult.Stream!.Schema.FieldsList;

            Assert.Equal(7, fields.Count);
            Assert.IsType<Int32Type>(fields[0].DataType);
            Assert.IsType<Int64Type>(fields[1].DataType);
            Assert.IsType<StringType>(fields[2].DataType);
            Assert.IsType<BooleanType>(fields[3].DataType);
            Assert.IsType<DoubleType>(fields[4].DataType);
            Assert.IsType<Date32Type>(fields[5].DataType);
            Assert.IsType<TimestampType>(fields[6].DataType);
        }

        [Fact]
        public async Task ExecuteQuery_NullManifest_ReturnsEmptySchema()
        {
            // Arrange: server returns null manifest (no results at all, e.g. DDL)
            var mockClient = new Mock<IStatementExecutionClient>();
            mockClient
                .Setup(c => c.ExecuteStatementAsync(
                    It.IsAny<ExecuteStatementRequest>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ExecuteStatementResponse
                {
                    StatementId = StatementId,
                    Status = new StatementStatus { State = "SUCCEEDED" },
                    Manifest = null,
                    Result = null,
                });

            using var stmt = CreateStatement(mockClient.Object);
            stmt.SqlQuery = "CREATE TABLE foo (id INT)";

            var queryResult = await stmt.ExecuteQueryAsync(CancellationToken.None);

            // No schema and no rows — just no exception
            Assert.NotNull(queryResult.Stream);
            Assert.Empty(queryResult.Stream!.Schema.FieldsList);
        }
    }
}
