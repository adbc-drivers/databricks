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
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.StatementExecution;
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.Hive2;
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
    /// METADATA-035: an EMPTY (non-null) tableTypes filter must match NO table types
    /// (zero rows) on the SEA path, matching databricks-jdbc; a null filter still
    /// matches ALL types. The SEA GetTables provider filters SHOW TABLES rows
    /// client-side, so these tests drive that filter directly at the
    /// IGetObjectsDataProvider seam with a mocked SHOW TABLES Arrow result carrying
    /// one TABLE and one VIEW row.
    /// </summary>
    public class StatementExecutionEmptyTableTypesTests
    {
        // A SHOW TABLES result with two rows: a TABLE and a VIEW in main.default.
        private static byte[] BuildShowTablesArrow()
        {
            var schema = new Schema(new[]
            {
                new Field("catalogName", StringType.Default, true),
                new Field("namespace", StringType.Default, true),
                new Field("tableName", StringType.Default, true),
                new Field("tableType", StringType.Default, true),
            }, null);

            var catalog = new StringArray.Builder().Append("main").Append("main").Build();
            var ns = new StringArray.Builder().Append("default").Append("default").Build();
            var name = new StringArray.Builder().Append("my_table").Append("my_view").Build();
            var type = new StringArray.Builder().Append("TABLE").Append("VIEW").Build();
            var batch = new RecordBatch(schema, new IArrowArray[] { catalog, ns, name, type }, 2);

            using var raw = new MemoryStream();
            using (var writer = new ArrowStreamWriter(raw, schema))
            {
                writer.WriteRecordBatch(batch);
                writer.WriteEnd();
            }
            return raw.ToArray();
        }

        private static HttpClient HttpClientReturningShowTables()
        {
            byte[] attachment = BuildShowTablesArrow();
            var executeBody = JsonSerializer.Serialize(new
            {
                statement_id = "stmt-tables",
                status = new { state = "SUCCEEDED" },
                manifest = new
                {
                    total_row_count = 2,
                    schema = new
                    {
                        column_count = 4,
                        columns = new[]
                        {
                            new { name = "catalogName", position = 0, type_name = "STRING", type_text = "STRING" },
                            new { name = "namespace", position = 1, type_name = "STRING", type_text = "STRING" },
                            new { name = "tableName", position = 2, type_name = "STRING", type_text = "STRING" },
                            new { name = "tableType", position = 3, type_name = "STRING", type_text = "STRING" },
                        },
                    },
                },
                result = new { attachment },
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
                    var body = path.EndsWith("/api/2.0/sql/sessions") ? sessionBody : executeBody;
                    return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body) };
                });

            return new HttpClient(handler.Object);
        }

        private static StatementExecutionConnection CreateConnection(HttpClient http)
        {
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.HostName, "test.databricks.com" },
                { DatabricksParameters.WarehouseId, "wh-1" },
                { SparkParameters.AccessToken, "token" },
            };
            return new StatementExecutionConnection(properties, http);
        }

        private static Task<IReadOnlyList<(string catalog, string schema, string table, string tableType)>> ListTablesAsync(
            StatementExecutionConnection connection, IReadOnlyList<string>? tableTypes)
        {
            return ((IGetObjectsDataProvider)connection).GetTablesAsync(
                catalogPattern: "main",
                schemaPattern: "default",
                tableNamePattern: null,
                tableTypes: tableTypes,
                cancellationToken: CancellationToken.None);
        }

        [Fact]
        public async Task GetTables_NullTableTypes_ReturnsAllTypes()
        {
            using var http = HttpClientReturningShowTables();
            var connection = CreateConnection(http);

            var rows = await ListTablesAsync(connection, tableTypes: null);

            Assert.Equal(2, rows.Count);
            Assert.Contains(rows, r => r.tableType == "TABLE");
            Assert.Contains(rows, r => r.tableType == "VIEW");
        }

        [Fact]
        public async Task GetTables_EmptyTableTypes_ReturnsNoTypes()
        {
            // METADATA-035: empty (non-null) filter matches nothing — neither TABLE nor VIEW.
            using var http = HttpClientReturningShowTables();
            var connection = CreateConnection(http);

            var rows = await ListTablesAsync(connection, tableTypes: new string[] { });

            Assert.Empty(rows);
        }

        [Fact]
        public async Task GetTables_SpecificTableType_ReturnsOnlyThatType()
        {
            // Regression: a non-empty filter still filters normally.
            using var http = HttpClientReturningShowTables();
            var connection = CreateConnection(http);

            var rows = await ListTablesAsync(connection, tableTypes: new[] { "TABLE" });

            Assert.Single(rows);
            Assert.Equal("TABLE", rows[0].tableType);
        }

        // ─── is_metadata_command path (StatementExecutionStatement.GetTablesAsync) ──────
        // The metadata-command shim receives tableTypes as a pre-joined string option
        // (adbc.get_metadata.target_table_types). An empty string means "match none"
        // (METADATA-035); a never-set (null) option means "all types".

        private static async Task<List<string>> MetadataCommandTableTypes(HttpClient http, string? tableTypesOption)
        {
            var connection = CreateConnection(http);
            using var stmt = (StatementExecutionStatement)connection.CreateStatement();
            stmt.SetOption(ApacheParameters.IsMetadataCommand, "true");
            stmt.SetOption(ApacheParameters.CatalogName, "main");
            stmt.SetOption(ApacheParameters.SchemaName, "default");
            if (tableTypesOption != null)
                stmt.SetOption(ApacheParameters.TableTypes, tableTypesOption);
            stmt.SqlQuery = "gettables";

            var result = await stmt.ExecuteQueryAsync(CancellationToken.None);
            using var reader = result.Stream!;
            int typeIdx = reader.Schema.FieldsList.ToList().FindIndex(f => f.Name == "TABLE_TYPE");
            var types = new List<string>();
            while (true)
            {
                using var batch = await reader.ReadNextRecordBatchAsync();
                if (batch == null) break;
                var typeArr = (StringArray)batch.Column(typeIdx);
                for (int i = 0; i < batch.Length; i++)
                    types.Add(typeArr.GetString(i));
            }
            return types;
        }

        [Fact]
        public async Task MetadataCommand_UnsetTableTypes_ReturnsAllTypes()
        {
            using var http = HttpClientReturningShowTables();
            var types = await MetadataCommandTableTypes(http, tableTypesOption: null);
            Assert.Equal(2, types.Count);
            Assert.Contains("TABLE", types);
            Assert.Contains("VIEW", types);
        }

        [Fact]
        public async Task MetadataCommand_EmptyStringTableTypes_ReturnsNoTypes()
        {
            // METADATA-035: empty (non-null) filter matches nothing.
            using var http = HttpClientReturningShowTables();
            var types = await MetadataCommandTableTypes(http, tableTypesOption: "");
            Assert.Empty(types);
        }

        [Fact]
        public async Task MetadataCommand_SpecificTableType_ReturnsOnlyThatType()
        {
            using var http = HttpClientReturningShowTables();
            var types = await MetadataCommandTableTypes(http, tableTypesOption: "TABLE");
            Assert.Single(types);
            Assert.Equal("TABLE", types[0]);
        }
    }
}
