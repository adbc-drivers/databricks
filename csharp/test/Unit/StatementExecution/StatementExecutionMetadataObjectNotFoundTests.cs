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
            // pre-escaped "foo\_bar" + escape=true → must NOT become "foo\\_bar"
            // which ConvertPattern would turn into "foo\\.bar" (invalid glob, server throws).
            // The fix leaves "foo\_bar" unchanged → ConvertPattern → "foo_bar" (literal match).
            var captured = new List<string>();
            using var http = HttpClientCapturingStatements(captured);
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.EscapePatternWildcards, "true");
            stmt.SetOption(ApacheParameters.TableName, @"foo\_bar");
            stmt.SqlQuery = "gettables";

            await stmt.ExecuteQueryAsync(CancellationToken.None);

            // Pre-escaped \_ passes through unchanged → ConvertPattern strips the \ → literal _
            Assert.Contains(captured, sql => sql.Contains("LIKE 'foo_bar'"));
            // The double-escape bug would produce \\.bar (glob wildcard dot) — must not appear
            Assert.DoesNotContain(captured, sql => sql.Contains("LIKE 'foo\\\\.bar'"));
            Assert.DoesNotContain(captured, sql => sql.Contains("LIKE 'foo\\.bar'"));
        }

        [Fact]
        public async Task GetTables_PreEscapedPercent_EscapeTrue_DoesNotDoubleEscape()
        {
            // pre-escaped "foo\%bar" + escape=true → must stay "foo\%bar", not "foo\\%bar"
            var captured = new List<string>();
            using var http = HttpClientCapturingStatements(captured);
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.EscapePatternWildcards, "true");
            stmt.SetOption(ApacheParameters.TableName, @"foo\%bar");
            stmt.SqlQuery = "gettables";

            await stmt.ExecuteQueryAsync(CancellationToken.None);

            // \% passes through unchanged → ConvertPattern strips the \ → literal % in glob
            Assert.Contains(captured, sql => sql.Contains("LIKE 'foo%bar'"));
            // Double-escape bug would produce \\%bar which ConvertPattern turns into \*bar
            Assert.DoesNotContain(captured, sql => sql.Contains("LIKE 'foo\\\\%bar'"));
        }

        [Fact]
        public async Task GetColumns_PreEscapedTableName_EscapeTrue_DoesNotDoubleEscape()
        {
            // Regression guard for the specific comparator fixture pattern:
            // "test\_result\_set\_types" (pre-escaped) + escape=true
            // Before fix: double-escaped → invalid SQL → DatabricksException
            // After fix: passed through → LIKE 'test_result_set_types' (exact literal match)
            var captured = new List<string>();
            using var http = HttpClientCapturingStatements(captured);
            using var stmt = CreateMetadataStatement(http);
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.EscapePatternWildcards, "true");
            stmt.SetOption(ApacheParameters.SchemaName, "default");
            stmt.SetOption(ApacheParameters.TableName, @"test\_result\_set\_types");
            stmt.SqlQuery = "getcolumns";

            await stmt.ExecuteQueryAsync(CancellationToken.None);

            // All three \_ sequences should resolve to literal _ in the glob pattern
            Assert.Contains(captured, sql => sql.Contains("LIKE 'test_result_set_types'"));
            // The pre-fix double-escape would produce test\\.result\\.set\\.types — a wildcard
            Assert.DoesNotContain(captured, sql => sql.Contains("test\\\\.") || sql.Contains("test\\."));
        }
    }
}
