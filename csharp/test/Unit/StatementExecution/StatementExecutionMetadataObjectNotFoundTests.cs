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
using Apache.Arrow.Adbc;
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
    /// Tests for SEA metadata methods at the HttpMessageHandler seam.
    /// The connection builds its own internal IStatementExecutionClient over the
    /// supplied HttpClient, so injection at the IStatementExecutionClient seam
    /// does not reach the metadata path.
    /// </summary>
    public class StatementExecutionMetadataObjectNotFoundTests
    {
        // Records the `statement` body of every ExecuteStatement call into
        // body of every ExecuteStatement call into <paramref name="captured"/> before
        // returning a SUCCEEDED empty result. Returns an empty result so that the metadata
        // path does not throw — the SQL the path emitted is captured deterministically
        // without needing a live warehouse.
        private static HttpClient HttpClientCapturingStatements(List<string> captured)
        {
            // A SUCCEEDED response with no result attachment and no manifest schema:
            // StatementExecutionStatement.CreateReader falls through to EmptyArrowArrayStream.
            var succeededBody = JsonSerializer.Serialize(new
            {
                statement_id = "stmt-ok",
                status = new { state = "SUCCEEDED" },
                manifest = new { format = "ARROW_STREAM", total_chunk_count = 0, total_row_count = 0 },
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
                    if (path.EndsWith("/api/2.0/sql/sessions"))
                    {
                        return new HttpResponseMessage(HttpStatusCode.OK)
                        {
                            Content = new StringContent(sessionBody),
                        };
                    }

                    var requestBody = req.Content?.ReadAsStringAsync().GetAwaiter().GetResult();
                    if (requestBody != null)
                    {
                        using var doc = JsonDocument.Parse(requestBody);
                        if (doc.RootElement.TryGetProperty("statement", out var stmt))
                            captured.Add(stmt.GetString() ?? string.Empty);
                    }

                    return new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new StringContent(succeededBody),
                    };
                });

            return new HttpClient(handler.Object);
        }

        // Returns an HttpClient whose ExecuteStatement always responds with an immediately
        // FAILED state carrying an object-not-found error (e.g. SCHEMA_NOT_FOUND). The SEA
        // client turns this into a DatabricksException, which the metadata methods now
        // PROPAGATE (matching Thrift's behavior) instead of swallowing to an empty result.
        private static HttpClient HttpClientFailingWith(string errorCode, string message, string sqlState)
        {
            var failedBody = JsonSerializer.Serialize(new
            {
                statement_id = "stmt-fail",
                status = new
                {
                    state = "FAILED",
                    error = new { error_code = errorCode, message, sql_state = sqlState },
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
                    var body = path.EndsWith("/api/2.0/sql/sessions") ? sessionBody : failedBody;
                    return new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new StringContent(body),
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

        // ─── Issue #525: `%` match-all catalog wildcard ──────────────────────────────
        // The `%` SQL-LIKE wildcard must mean "all catalogs" on the SEA path, exactly as
        // Thrift treats it, rather than being passed through as a literal backtick-quoted
        // identifier. These tests capture the emitted SHOW SQL at the HttpMessageHandler
        // seam and assert that CatalogName="%" produces "IN ALL CATALOGS" (never a literal
        // `%` identifier), guarding IsMatchAllCatalogPattern in CI where the E2E test is
        // skipped for lack of warehouse credentials.

        [Fact]
        public async Task GetSchemas_CatalogPercentWildcard_EmitsInAllCatalogs()
        {
            var captured = new List<string>();
            using var http = HttpClientCapturingStatements(captured);
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.CatalogName, "%");
            stmt.SqlQuery = "getschemas";

            await stmt.ExecuteQueryAsync(CancellationToken.None);

            Assert.Contains("SHOW SCHEMAS IN ALL CATALOGS", captured);
            Assert.DoesNotContain(captured, sql => sql.Contains("`%`"));
        }

        [Fact]
        public async Task GetTables_CatalogPercentWildcard_EmitsInAllCatalogs()
        {
            var captured = new List<string>();
            using var http = HttpClientCapturingStatements(captured);
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.CatalogName, "%");
            stmt.SqlQuery = "gettables";

            await stmt.ExecuteQueryAsync(CancellationToken.None);

            Assert.Contains("SHOW TABLES IN ALL CATALOGS", captured);
            Assert.DoesNotContain(captured, sql => sql.Contains("`%`"));
        }

        [Fact]
        public async Task GetColumns_CatalogPercentWildcard_DoesNotEmitLiteralWildcardIdentifier()
        {
            // SHOW COLUMNS IN ALL CATALOGS is not yet supported by the backend, so the
            // null/match-all catalog fans out to SHOW CATALOGS + per-catalog SHOW COLUMNS.
            // The key regression guard is that the literal `%` identifier is never emitted
            // (which is what the pre-fix code did via SHOW COLUMNS IN CATALOG `%`).
            var captured = new List<string>();
            using var http = HttpClientCapturingStatements(captured);
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.CatalogName, "%");
            stmt.SetOption(ApacheParameters.SchemaName, "s");
            stmt.SetOption(ApacheParameters.TableName, "t");
            stmt.SqlQuery = "getcolumns";

            await stmt.ExecuteQueryAsync(CancellationToken.None);

            // `%` expands to "all catalogs", which begins by enumerating catalogs.
            Assert.Contains("SHOW CATALOGS", captured);
            Assert.DoesNotContain(captured, sql => sql.Contains("`%`"));
        }

        // ─── Issue: double-escape of pre-escaped patterns with escape_pattern_wildcards ──
        //
        // When escape_pattern_wildcards=true the driver must escape raw _ and % to \_ and \%
        // so the server treats them as literal characters instead of LIKE wildcards.
        // But a caller may already have pre-escaped its pattern (e.g. the comparator passes
        // "test\_result\_set\_types" verbatim). The naive Replace("_", "\\_") re-escapes the
        // _ inside \_ → \\_ which ConvertPattern then turns into an escaped-backslash + wildcard
        // dot, producing "test\\.result\\.set\\.types" — an invalid SHOW-command glob that the
        // server rejects with a DatabricksException, while the Thrift path returns rows.
        //
        // The fix: EscapePatternWildcardsInName must recognise already-escaped sequences (\_, \%,
        // \\) and pass them through unchanged, escaping only bare (unescaped) _ and %.
        //
        // These tests pin the SQL emitted at the HttpMessageHandler seam, guarding the fix in CI
        // without a live warehouse.

        [Fact]
        public async Task GetTables_RawUnderscore_EscapeTrue_EmitsLiteralUnderscore()
        {
            // raw "foo_bar" + escape=true → \_ → ConvertPattern strips escape → literal _ in glob
            var captured = new List<string>();
            using var http = HttpClientCapturingStatements(captured);
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.EscapePatternWildcards, "true");
            stmt.SetOption(ApacheParameters.TableName, "foo_bar");
            stmt.SqlQuery = "gettables";

            await stmt.ExecuteQueryAsync(CancellationToken.None);

            // \_ produced by escaping; ConvertPattern converts \_ to literal _ (not glob .)
            Assert.Contains(captured, sql => sql.Contains("LIKE 'foo_bar'"));
            // Sanity: the raw wildcard dot must NOT appear
            Assert.DoesNotContain(captured, sql => sql.Contains("LIKE 'foo.bar'"));
        }

        [Fact]
        public async Task GetTables_PreEscapedUnderscore_EscapeTrue_DoesNotDoubleEscape()
        {
            // escape=true means the input is a LITERAL name, so "foo\_bar" is the 8-char
            // literal name foo \ _ bar. The escape step escapes every metacharacter
            // (\ -> \\, _ -> \_) and LikePattern doubles backslashes for the SQL string
            // literal, yielding LIKE 'foo\\_bar' (which the server reads as the literal
            // name foo\_bar). No "already-escaped" idempotency — that guess was unsound.
            var captured = new List<string>();
            using var http = HttpClientCapturingStatements(captured);
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.EscapePatternWildcards, "true");
            stmt.SetOption(ApacheParameters.TableName, @"foo\_bar");
            stmt.SqlQuery = "gettables";

            await stmt.ExecuteQueryAsync(CancellationToken.None);

            // Literal name foo\_bar → LIKE 'foo\\_bar' (two backslashes in the SQL literal).
            Assert.Contains(captured, sql => sql.Contains(@"LIKE 'foo\\\\_bar'"));
        }

        [Fact]
        public async Task GetTables_PreEscapedPercent_EscapeTrue_DoesNotDoubleEscape()
        {
            // escape=true → "foo\%bar" is the 8-char literal name foo \ % bar. Escaping
            // every metacharacter (\ -> \\, % -> \%) then doubling for the SQL string
            // literal yields LIKE 'foo\\%bar' (server reads literal name foo\%bar).
            var captured = new List<string>();
            using var http = HttpClientCapturingStatements(captured);
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.EscapePatternWildcards, "true");
            stmt.SetOption(ApacheParameters.TableName, @"foo\%bar");
            stmt.SqlQuery = "gettables";

            await stmt.ExecuteQueryAsync(CancellationToken.None);

            // Literal name foo\%bar → LIKE 'foo\\%bar' (two backslashes in the SQL literal).
            Assert.Contains(captured, sql => sql.Contains(@"LIKE 'foo\\\\%bar'"));
        }

        [Fact]
        public async Task GetColumns_PreEscapedTableName_EscapeTrue_DoesNotDoubleEscape()
        {
            // escape=true → the input is the literal name test\_result\_set\_types
            // (containing literal backslashes). Each backslash is escaped (\ -> \\) and
            // each underscore (_ -> \_), then LikePattern doubles backslashes for the SQL
            // string literal, yielding LIKE 'test\\_result\\_set\\_types' — the server
            // reads it as the literal name test\_result\_set\_types.
            var captured = new List<string>();
            using var http = HttpClientCapturingStatements(captured);
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.EscapePatternWildcards, "true");
            stmt.SetOption(ApacheParameters.SchemaName, "default");
            stmt.SetOption(ApacheParameters.TableName, @"test\_result\_set\_types");
            stmt.SqlQuery = "getcolumns";

            await stmt.ExecuteQueryAsync(CancellationToken.None);

            Assert.Contains(captured, sql => sql.Contains(@"LIKE 'test\\\\_result\\\\_set\\\\_types'"));
        }

        [Fact]
        public async Task GetTables_BackslashPattern_EscapeFalse_DoublesBackslashForSqlLiteral()
        {
            // escape=false: the input is a JDBC LIKE pattern, so "foo\\bar" is an escaped
            // backslash that ConvertPattern preserves as a literal backslash in the glob
            // (foo\\bar). LikePattern then doubles backslashes for the SQL string literal,
            // yielding LIKE 'foo\\\\bar' so a schema/table literally named foo\bar matches
            // through both the SQL-literal parser and the SHOW ... LIKE regex.
            //
            // This is the Layer-2 doubling on the escape=false path (prior behavior emitted
            // LIKE 'foo\\bar', which collapsed to a single backslash and never matched).
            var captured = new List<string>();
            using var http = HttpClientCapturingStatements(captured);
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            // escape=false (default): pattern is interpreted as a JDBC LIKE pattern.
            stmt.SetOption(ApacheParameters.TableName, @"foo\\bar");
            stmt.SqlQuery = "gettables";

            await stmt.ExecuteQueryAsync(CancellationToken.None);

            // Literal backslash → LIKE 'foo\\\\bar' (four backslashes in the SQL literal).
            Assert.Contains(captured, sql => sql.Contains(@"LIKE 'foo\\\\bar'"));
        }

        // ─── Issue #593: catalog="%" + escape_pattern_wildcards=true → empty result ────
        //
        // Thrift escapes "%" → "\%" (a literal catalog matching nothing → 0 rows, no throw).
        // SEA's EffectiveCatalog leaves "%" as a literal backtick-quoted identifier, so it
        // issues SHOW ... IN `%`, the server returns SCHEMA_NOT_FOUND, and the object-not-found
        // catch (DatabricksException.IsObjectNotFoundException) swallows it to an empty result —
        // matching Thrift's 0 rows. (Earlier this branch had a dedicated pre-SHOW short-circuit
        // for this case; it was removed as redundant once the general object-not-found catch was
        // restored — verified live that catalog="%"+escape still returns 0 rows on both protocols.)
        // These tests assert the empty (0-row) result and its JDBC-shaped schema.

        [Fact]
        public async Task GetSchemas_PercentCatalog_EscapeTrue_ReturnsEmpty()
        {
            using var http = HttpClientCapturingStatements(new List<string>());
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.EscapePatternWildcards, "true");
            stmt.SetOption(ApacheParameters.CatalogName, "%");
            stmt.SqlQuery = "getschemas";

            var result = await stmt.ExecuteQueryAsync(CancellationToken.None);

            Assert.Equal(0, result.RowCount);
            // The empty result must still carry the JDBC-shaped GetSchemas schema (field names
            // and order) that ADBC consumers rely on for column-by-name lookups.
            var fields = result.Stream!.Schema.FieldsList;
            Assert.Equal(2, fields.Count);
            Assert.Equal("TABLE_SCHEM", fields[0].Name);
            Assert.Equal("TABLE_CATALOG", fields[1].Name);
        }

        [Fact]
        public async Task GetTables_PercentCatalog_EscapeTrue_ReturnsEmpty()
        {
            using var http = HttpClientCapturingStatements(new List<string>());
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.EscapePatternWildcards, "true");
            stmt.SetOption(ApacheParameters.CatalogName, "%");
            stmt.SqlQuery = "gettables";

            var result = await stmt.ExecuteQueryAsync(CancellationToken.None);
            Assert.Equal(0, result.RowCount);
        }

        [Fact]
        public async Task GetColumns_PercentCatalog_EscapeTrue_ReturnsEmpty()
        {
            using var http = HttpClientCapturingStatements(new List<string>());
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.EscapePatternWildcards, "true");
            stmt.SetOption(ApacheParameters.CatalogName, "%");
            stmt.SetOption(ApacheParameters.SchemaName, "s");
            stmt.SetOption(ApacheParameters.TableName, "t");
            stmt.SqlQuery = "getcolumns";

            var result = await stmt.ExecuteQueryAsync(CancellationToken.None);
            Assert.Equal(0, result.RowCount);
        }

        [Fact]
        public async Task GetColumnsExtended_PercentCatalog_EscapeTrue_ReturnsEmpty()
        {
            // On a real server, DESC TABLE EXTENDED `%`.`s`.`t` fails with
            // TABLE_OR_VIEW_NOT_FOUND (verified live), which the object-not-found catch
            // swallows to empty — so use a FAILED mock carrying that error (not the
            // SUCCEEDED-empty capturing mock, which would surface as a FormatException the
            // real server never produces here).
            using var http = HttpClientFailingWith(
                "TABLE_OR_VIEW_NOT_FOUND", "The table or view `%`.`s`.`t` cannot be found", "42P01");
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.EscapePatternWildcards, "true");
            stmt.SetOption(ApacheParameters.CatalogName, "%");
            stmt.SetOption(ApacheParameters.SchemaName, "s");
            stmt.SetOption(ApacheParameters.TableName, "t");
            stmt.SqlQuery = "getcolumnsextended";

            var result = await stmt.ExecuteQueryAsync(CancellationToken.None);
            Assert.Equal(0, result.RowCount);
        }

        [Fact]
        public async Task GetSchemas_StarCatalog_EscapeTrue_ReturnsEmpty()
        {
            // "*" is the Databricks alias for "%" in the match-all catalog wildcard
            using var http = HttpClientCapturingStatements(new List<string>());
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.EscapePatternWildcards, "true");
            stmt.SetOption(ApacheParameters.CatalogName, "*");
            stmt.SqlQuery = "getschemas";

            var result = await stmt.ExecuteQueryAsync(CancellationToken.None);
            Assert.Equal(0, result.RowCount);
        }

        [Fact]
        public async Task GetSchemas_PercentCatalog_EscapeFalse_StillEmitsShowInAllCatalogs()
        {
            // When escape=false, "%" is the match-all wildcard → should expand to "IN ALL CATALOGS",
            // not short-circuit. Verifies the short-circuit does not fire for escape=false.
            var captured = new List<string>();
            using var http = HttpClientCapturingStatements(captured);
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            // escape=false (default): "%" → null → "IN ALL CATALOGS"
            stmt.SetOption(ApacheParameters.CatalogName, "%");
            stmt.SqlQuery = "getschemas";

            await stmt.ExecuteQueryAsync(CancellationToken.None);

            // escape=false: the existing #525 path still fires — SHOW IN ALL CATALOGS.
            Assert.Contains("SHOW SCHEMAS IN ALL CATALOGS", captured);
        }

        // ─── Exact-match ops throw on missing table, matching the Thrift path ─────────
        // GetPrimaryKeys / GetCrossReference are exact-match (table required). Thrift's
        // TGetPrimaryKeysReq / TGetCrossReferenceReq are rejected server-side with
        // AdbcStatusCode.InternalError + SqlState 42000 when the table is null; SEA must
        // throw an equivalent error instead of returning empty. SEA throws its own
        // DatabricksException (the natural SEA type); the comparator treats any
        // AdbcException subclass as equivalent and compares Status + SqlState, so those
        // two are the load-bearing assertions here (not the concrete type).

        [Fact]
        public async Task GetPrimaryKeys_NullTable_ThrowsWithThriftStatusAndSqlState()
        {
            using var http = HttpClientCapturingStatements(new List<string>());
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.CatalogName, "main");
            stmt.SetOption(ApacheParameters.SchemaName, "some_schema");
            // TableName deliberately not set (null).
            stmt.SqlQuery = "getprimarykeys";

            var ex = await Assert.ThrowsAsync<DatabricksException>(
                () => stmt.ExecuteQueryAsync(CancellationToken.None));
            Assert.Equal(AdbcStatusCode.InternalError, ex.Status);
            Assert.Equal("42000", ex.SqlState);
        }

        [Fact]
        public async Task GetCrossReference_NullTables_ThrowsWithThriftStatusAndSqlState()
        {
            using var http = HttpClientCapturingStatements(new List<string>());
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.ForeignCatalogName, "main");
            stmt.SetOption(ApacheParameters.ForeignSchemaName, "some_schema");
            // Neither foreign table nor parent table set (both null).
            stmt.SqlQuery = "getcrossreference";

            var ex = await Assert.ThrowsAsync<DatabricksException>(
                () => stmt.ExecuteQueryAsync(CancellationToken.None));
            Assert.Equal(AdbcStatusCode.InternalError, ex.Status);
            Assert.Equal("42000", ex.SqlState);
        }

        // Exact-match ops also require schema when catalog is specified (mirroring the JDBC
        // reference driver's resolveKeyBasedParams). Validating client-side gives a clean
        // error and avoids the Thrift server's internal "GET_FUNCTIONS assertion failed" bug
        // on a null schema.
        [Fact]
        public async Task GetPrimaryKeys_CatalogSetSchemaNull_Throws()
        {
            using var http = HttpClientCapturingStatements(new List<string>());
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.CatalogName, "main");
            stmt.SetOption(ApacheParameters.TableName, "t");
            // SchemaName deliberately not set (null) while catalog IS set.
            stmt.SqlQuery = "getprimarykeys";

            var ex = await Assert.ThrowsAsync<DatabricksException>(
                () => stmt.ExecuteQueryAsync(CancellationToken.None));
            Assert.Equal(AdbcStatusCode.InternalError, ex.Status);
            Assert.Equal("42000", ex.SqlState);
        }

        [Fact]
        public async Task GetCrossReference_ForeignCatalogSetSchemaNull_Throws()
        {
            using var http = HttpClientCapturingStatements(new List<string>());
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.ForeignCatalogName, "main");
            stmt.SetOption(ApacheParameters.ForeignTableName, "fk_child");
            // ForeignSchemaName deliberately not set (null) while foreign catalog IS set.
            stmt.SqlQuery = "getcrossreference";

            var ex = await Assert.ThrowsAsync<DatabricksException>(
                () => stmt.ExecuteQueryAsync(CancellationToken.None));
            Assert.Equal(AdbcStatusCode.InternalError, ex.Status);
            Assert.Equal("42000", ex.SqlState);
        }

        // ─── Object-not-found errors are SWALLOWED to an empty result ────────────────
        //
        // SEA metadata methods catch NO_SUCH_CATALOG / SCHEMA_NOT_FOUND /
        // TABLE_OR_VIEW_NOT_FOUND / INVALID_PARAMETER_VALUE (DatabricksException.
        // IsObjectNotFoundException) and return an EMPTY result set rather than throwing —
        // matching BOTH the Thrift path (which returns 0 rows on object-not-found, verified
        // live) and the JDBC reference driver (isObjectNotFoundException). These tests mock a
        // FAILED execute response carrying such an error and assert the metadata method
        // returns an empty result (guarding against a regression that lets the error escape).
        [Fact]
        public async Task GetSchemas_SchemaNotFound_ReturnsEmpty()
        {
            using var http = HttpClientFailingWith("SCHEMA_NOT_FOUND", "Schema 'missing' not found", "42000");
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.CatalogName, "main");
            stmt.SetOption(ApacheParameters.SchemaName, "missing");
            stmt.SqlQuery = "getschemas";

            var result = await stmt.ExecuteQueryAsync(CancellationToken.None);
            Assert.Equal(0, result.RowCount);
        }

        [Fact]
        public async Task GetTables_TableOrViewNotFound_ReturnsEmpty()
        {
            using var http = HttpClientFailingWith("TABLE_OR_VIEW_NOT_FOUND", "Table not found", "42P01");
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.CatalogName, "main");
            stmt.SetOption(ApacheParameters.SchemaName, "missing");
            stmt.SqlQuery = "gettables";

            var result = await stmt.ExecuteQueryAsync(CancellationToken.None);
            Assert.Equal(0, result.RowCount);
        }

        [Fact]
        public async Task GetColumns_NoSuchCatalog_ReturnsEmpty()
        {
            // Real server error shape (captured live): NO_SUCH_CATALOG_EXCEPTION + SQLSTATE 42704.
            using var http = HttpClientFailingWith("NO_SUCH_CATALOG_EXCEPTION", "Catalog 'main' was not found", "42704");
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.CatalogName, "main");
            stmt.SetOption(ApacheParameters.SchemaName, "s");
            stmt.SetOption(ApacheParameters.TableName, "t");
            stmt.SqlQuery = "getcolumns";

            var result = await stmt.ExecuteQueryAsync(CancellationToken.None);
            Assert.Equal(0, result.RowCount);
        }
    }
}
