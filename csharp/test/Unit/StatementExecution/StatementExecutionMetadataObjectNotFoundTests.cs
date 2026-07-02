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
using Apache.Arrow.Adbc;
using Microsoft.IO;
using Moq;
using Moq.Protected;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    /// <summary>
    /// Tests that SEA metadata methods (GetSchemas / GetTables / GetColumns /
    /// GetPrimaryKeys / GetCrossReference) return empty result sets when the
    /// underlying SHOW command throws a DatabricksException whose message
    /// indicates a non-existent catalog, schema, or table.
    ///
    /// Mock at the HttpMessageHandler level: the connection builds its own internal
    /// IStatementExecutionClient over the supplied HttpClient, so injecting at the
    /// IStatementExecutionClient seam doesn't reach the metadata path. Returning a
    /// 200 OK with status.state=FAILED and a NOT_FOUND error code makes the real
    /// StatementExecutionClient throw a DatabricksException — exercising the catch.
    /// </summary>
    public class StatementExecutionMetadataObjectNotFoundTests
    {
        // Routes CreateSession (/api/2.0/sql/sessions) to a 200 OK with a session id,
        // and ExecuteStatement (/api/2.0/sql/statements) to a 200 OK with the supplied
        // FAILED-state body. This is what makes the real SEA client throw on the
        // metadata call.
        private static HttpClient HttpClientWithFailedExecuteStatement(string errorCode, string errorMessage)
        {
            var failedBody = JsonSerializer.Serialize(new
            {
                statement_id = "stmt-failed",
                status = new
                {
                    state = "FAILED",
                    error = new { error_code = errorCode, message = errorMessage },
                },
            });
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
                    var body = path.EndsWith("/api/2.0/sql/sessions")
                        ? sessionBody
                        : failedBody;
                    return new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new StringContent(body),
                    };
                });

            return new HttpClient(handler.Object);
        }

        // Returns a 400 Bad Request with the supplied error body — this makes
        // EnsureSuccessStatusCodeAsync throw a DatabricksException with SqlState
        // unset but the message containing the body content.
        private static HttpClient HttpClientWithHttpError(HttpStatusCode statusCode, string errorBody)
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
                        return new HttpResponseMessage(HttpStatusCode.OK)
                        {
                            Content = new StringContent(sessionBody),
                        };
                    }
                    return new HttpResponseMessage(statusCode)
                    {
                        Content = new StringContent(errorBody),
                    };
                });

            return new HttpClient(handler.Object);
        }

        private static StatementExecutionStatement CreateMetadataStatement(HttpClient httpClient)
        {
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.HostName, "test.databricks.com" },
                { DatabricksParameters.WarehouseId, "wh-1" },
                { SparkParameters.AccessToken, "token" },
            };

            var connection = new StatementExecutionConnection(properties, httpClient);
            // The outer statement's IStatementExecutionClient is unused on the metadata
            // path (the connection rebuilds its own from the HttpClient), so a stub mock
            // is fine here.
            var stubClient = new Mock<IStatementExecutionClient>().Object;
            return new StatementExecutionStatement(
                stubClient,
                sessionId: "session-1",
                warehouseId: "wh-1",
                catalog: "main",
                schema: null,
                resultDisposition: "INLINE_OR_EXTERNAL_LINKS",
                resultFormat: "ARROW_STREAM",
                resultCompression: null,
                waitTimeout: "0s",
                pollingIntervalMs: 50,
                properties: properties,
                recyclableMemoryStreamManager: new RecyclableMemoryStreamManager(),
                lz4BufferPool: System.Buffers.ArrayPool<byte>.Shared,
                httpClient: httpClient,
                connection: connection);
        }

        // Empty results from the catch path return RowCount=0 with the schema preserved.
        // The first ReadNextRecordBatchAsync may return a 0-row batch or null depending on
        // which factory built the result; both indicate "no rows".
        private static async Task AssertEmptyStreamAsync(QueryResult queryResult)
        {
            Assert.Equal(0, queryResult.RowCount);
            Assert.NotNull(queryResult.Stream);
            var batch = await queryResult.Stream!.ReadNextRecordBatchAsync(CancellationToken.None);
            if (batch != null)
            {
                Assert.Equal(0, batch.Length);
                var next = await queryResult.Stream.ReadNextRecordBatchAsync(CancellationToken.None);
                Assert.Null(next);
            }
        }

        private static void AssertSchemaFieldNames(QueryResult queryResult, params string[] expectedFieldNames)
        {
            Assert.NotNull(queryResult.Stream);
            var fields = queryResult.Stream!.Schema.FieldsList;
            Assert.Equal(expectedFieldNames.Length, fields.Count);
            for (int i = 0; i < expectedFieldNames.Length; i++)
            {
                Assert.Equal(expectedFieldNames[i], fields[i].Name);
            }
        }

        [Fact]
        public async Task GetSchemas_NoSuchCatalog_ReturnsEmptyResult()
        {
            using var http = HttpClientWithFailedExecuteStatement(
                "NO_SUCH_CATALOG_EXCEPTION", "Catalog 'doesnotexist' not found");
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.CatalogName, "doesnotexist");
            stmt.SqlQuery = "getschemas";

            var queryResult = await stmt.ExecuteQueryAsync(CancellationToken.None);
            await AssertEmptyStreamAsync(queryResult);
        }

        [Fact]
        public async Task GetTables_TableOrViewNotFound_ReturnsEmptyResult()
        {
            using var http = HttpClientWithFailedExecuteStatement(
                "TABLE_OR_VIEW_NOT_FOUND", "Table not found");
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.CatalogName, "main");
            stmt.SetOption(ApacheParameters.SchemaName, "nonexistent");
            stmt.SqlQuery = "gettables";

            var queryResult = await stmt.ExecuteQueryAsync(CancellationToken.None);
            await AssertEmptyStreamAsync(queryResult);
        }

        [Fact]
        public async Task GetColumns_SchemaNotFound_ReturnsEmptyResult()
        {
            using var http = HttpClientWithFailedExecuteStatement(
                "SCHEMA_NOT_FOUND", "Schema not found");
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.CatalogName, "main");
            stmt.SetOption(ApacheParameters.SchemaName, "nonexistent");
            stmt.SetOption(ApacheParameters.TableName, "any_table");
            stmt.SqlQuery = "getcolumns";

            var queryResult = await stmt.ExecuteQueryAsync(CancellationToken.None);
            await AssertEmptyStreamAsync(queryResult);
        }

        [Fact]
        public async Task GetPrimaryKeys_TableNotFound_ReturnsEmptyResult()
        {
            using var http = HttpClientWithFailedExecuteStatement(
                "TABLE_OR_VIEW_NOT_FOUND", "Table not found");
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.CatalogName, "main");
            stmt.SetOption(ApacheParameters.SchemaName, "schema1");
            stmt.SetOption(ApacheParameters.TableName, "missing_table");
            stmt.SqlQuery = "getprimarykeys";

            var queryResult = await stmt.ExecuteQueryAsync(CancellationToken.None);
            await AssertEmptyStreamAsync(queryResult);
        }

        [Fact]
        public async Task GetCrossReference_TableNotFound_ReturnsEmptyResult()
        {
            using var http = HttpClientWithFailedExecuteStatement(
                "TABLE_OR_VIEW_NOT_FOUND", "Table not found");
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            // Cross-reference uses the foreign-table fields as the SHOW FOREIGN KEYS target
            stmt.SetOption(ApacheParameters.ForeignCatalogName, "main");
            stmt.SetOption(ApacheParameters.ForeignSchemaName, "schema1");
            stmt.SetOption(ApacheParameters.ForeignTableName, "missing_table");
            stmt.SqlQuery = "getcrossreference";

            var queryResult = await stmt.ExecuteQueryAsync(CancellationToken.None);
            await AssertEmptyStreamAsync(queryResult);
        }

        [Fact]
        public async Task GetSchemas_UnrelatedDatabricksException_PropagatesToCaller()
        {
            // ACCESS_DENIED isn't an object-not-found error, so the catch must NOT swallow
            // it — caller must see the failure.
            using var http = HttpClientWithFailedExecuteStatement(
                "ACCESS_DENIED", "Insufficient privileges to perform SHOW SCHEMAS");
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.CatalogName, "main");
            stmt.SqlQuery = "getschemas";

            await Assert.ThrowsAsync<DatabricksException>(
                () => stmt.ExecuteQueryAsync(CancellationToken.None));
        }

        [Fact]
        public async Task GetTables_HttpErrorContainingNotFoundKeyword_ReturnsEmptyResult()
        {
            // EnsureSuccessStatusCodeAsync also throws DatabricksException; verify the
            // message-substring check still fires when the error comes through that path.
            var errorBody = JsonSerializer.Serialize(new
            {
                error_code = "BAD_REQUEST",
                message = "[TABLE_OR_VIEW_NOT_FOUND] The table or view `main`.`x`.`y` cannot be found.",
            });
            using var http = HttpClientWithHttpError(HttpStatusCode.BadRequest, errorBody);
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.CatalogName, "main");
            stmt.SetOption(ApacheParameters.SchemaName, "x");
            stmt.SqlQuery = "gettables";

            var queryResult = await stmt.ExecuteQueryAsync(CancellationToken.None);
            await AssertEmptyStreamAsync(queryResult);
        }

        // ─── Schema-shape verification ──────────────────────────────────────────────
        // The empty result returned by the catch path must carry the same schema (field
        // names and order) that consumers expect from the non-empty result, otherwise
        // ADBC clients will see a different shape for "table doesn't exist" vs "table
        // is empty" — breaking column lookups by name.

        [Fact]
        public async Task GetSchemas_EmptyResult_PreservesJdbcSchema()
        {
            using var http = HttpClientWithFailedExecuteStatement(
                "NO_SUCH_CATALOG_EXCEPTION", "Catalog not found");
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.CatalogName, "x");
            stmt.SqlQuery = "getschemas";

            var queryResult = await stmt.ExecuteQueryAsync(CancellationToken.None);
            AssertSchemaFieldNames(queryResult,
                "TABLE_SCHEM", "TABLE_CATALOG");
        }

        [Fact]
        public async Task GetTables_EmptyResult_PreservesJdbcSchema()
        {
            using var http = HttpClientWithFailedExecuteStatement(
                "SCHEMA_NOT_FOUND", "Schema not found");
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.CatalogName, "main");
            stmt.SetOption(ApacheParameters.SchemaName, "missing");
            stmt.SqlQuery = "gettables";

            var queryResult = await stmt.ExecuteQueryAsync(CancellationToken.None);
            AssertSchemaFieldNames(queryResult,
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS",
                "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
                "SELF_REFERENCING_COL_NAME", "REF_GENERATION");
        }

        [Fact]
        public async Task GetColumns_EmptyResult_PreservesJdbcSchema()
        {
            using var http = HttpClientWithFailedExecuteStatement(
                "TABLE_OR_VIEW_NOT_FOUND", "Table not found");
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.CatalogName, "main");
            stmt.SetOption(ApacheParameters.SchemaName, "schema1");
            stmt.SetOption(ApacheParameters.TableName, "missing");
            stmt.SqlQuery = "getcolumns";

            var queryResult = await stmt.ExecuteQueryAsync(CancellationToken.None);
            // 24 columns including the Databricks-specific BASE_TYPE_NAME at the end.
            AssertSchemaFieldNames(queryResult,
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
                "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH",
                "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS",
                "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA",
                "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTO_INCREMENT", "BASE_TYPE_NAME");
        }

        [Fact]
        public async Task GetPrimaryKeys_EmptyResult_PreservesJdbcSchema()
        {
            using var http = HttpClientWithFailedExecuteStatement(
                "TABLE_OR_VIEW_NOT_FOUND", "Table not found");
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.CatalogName, "main");
            stmt.SetOption(ApacheParameters.SchemaName, "schema1");
            stmt.SetOption(ApacheParameters.TableName, "missing");
            stmt.SqlQuery = "getprimarykeys";

            var queryResult = await stmt.ExecuteQueryAsync(CancellationToken.None);
            // KEQ_SEQ is a pre-existing typo in MetadataSchemaDefinitions.CreatePrimaryKeysSchema
            // (should be KEY_SEQ per JDBC spec); the empty path must match the success path.
            AssertSchemaFieldNames(queryResult,
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEQ_SEQ", "PK_NAME");
        }

        [Fact]
        public async Task GetCrossReference_EmptyResult_PreservesJdbcSchema()
        {
            using var http = HttpClientWithFailedExecuteStatement(
                "TABLE_OR_VIEW_NOT_FOUND", "Table not found");
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.ForeignCatalogName, "main");
            stmt.SetOption(ApacheParameters.ForeignSchemaName, "schema1");
            stmt.SetOption(ApacheParameters.ForeignTableName, "missing");
            stmt.SqlQuery = "getcrossreference";

            var queryResult = await stmt.ExecuteQueryAsync(CancellationToken.None);
            // Same KEQ_SEQ typo as above.
            AssertSchemaFieldNames(queryResult,
                "PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
                "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME",
                "KEQ_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY");
        }
    }
}
