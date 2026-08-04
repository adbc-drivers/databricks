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
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Apache.Arrow.Adbc.Tests.Metadata;
using Apache.Arrow.Ipc;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.StatementExecution
{
    /// <summary>
    /// METADATA-035 live E2E: an EMPTY (non-null) tableTypes filter to GetObjects must
    /// match NO table types (zero tables), matching databricks-jdbc; a null filter must
    /// match ALL types. The test is parameterized over both protocols — it forces
    /// <c>adbc.databricks.protocol=thrift</c> and <c>=rest</c> on independent connections
    /// (rather than relying on the run's default protocol) so a single invocation
    /// exercises BOTH the Thrift server-side sentinel and the SEA client-side filter.
    /// Creates a unique table + view so the assertions are deterministic regardless of
    /// what else lives in the fixture schema.
    /// </summary>
    public class EmptyTableTypesE2ETests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public EmptyTableTypesE2ETests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
        }

        private static int CountTables(AdbcConnection connection, string catalog, string schema,
            string tablePattern, IReadOnlyList<string>? tableTypes, out List<AdbcTable> tables)
        {
            using var stream = connection.GetObjects(
                AdbcConnection.GetObjectsDepth.Tables, catalog, schema, tablePattern, tableTypes, null);
            tables = new List<AdbcTable>();
            var batch = stream.ReadNextRecordBatchAsync().GetAwaiter().GetResult();
            while (batch != null)
            {
                foreach (var cat in GetObjectsParser.ParseCatalog(batch, schema))
                {
                    foreach (var db in cat.DbSchemas ?? Enumerable.Empty<AdbcDbSchema>())
                    {
                        foreach (var t in db.Tables ?? Enumerable.Empty<AdbcTable>())
                            tables.Add(t);
                    }
                }
                batch = stream.ReadNextRecordBatchAsync().GetAwaiter().GetResult();
            }
            return tables.Count;
        }

        // Connection on an explicitly-forced protocol, independent of the run's default.
        // Per the repo's learning log, a SEA/StatementExecution path is only exercised when
        // the connection sets adbc.databricks.protocol=rest — so we force each protocol
        // rather than trusting the run default (Thrift in most environments).
        private AdbcConnection NewConnectionForProtocol(string protocol)
        {
            var parameters = new Dictionary<string, string>(TestEnvironment.GetDriverParameters(TestConfiguration));
            parameters[DatabricksParameters.Protocol] = protocol;
            return TestEnvironment.CreateNewDriver().Open(parameters).Connect(new Dictionary<string, string>());
        }

        [SkippableTheory]
        [InlineData("thrift")]
        [InlineData("rest")]
        public async Task GetObjects_EmptyTableTypes_MatchesNone_NullMatchesAll(string protocol)
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable), "Test configuration not available");

            string catalog = TestConfiguration.Metadata.Catalog;
            string schema = TestConfiguration.Metadata.Schema; // writable fixture schema
            string suffix = Guid.NewGuid().ToString("N").Substring(0, 12);
            string tableName = $"ett_tbl_{suffix}";
            string viewName = $"ett_view_{suffix}";
            string fqTable = $"{DelimitIdentifier(catalog)}.{DelimitIdentifier(schema)}.{DelimitIdentifier(tableName)}";
            string fqView = $"{DelimitIdentifier(catalog)}.{DelimitIdentifier(schema)}.{DelimitIdentifier(viewName)}";

            using AdbcConnection connection = NewConnectionForProtocol(protocol);
            using AdbcStatement setup = connection.CreateStatement();

            try
            {
                setup.SqlQuery = $"CREATE TABLE IF NOT EXISTS {fqTable} (id INT)";
                setup.ExecuteUpdate();
                setup.SqlQuery = $"CREATE VIEW {fqView} AS SELECT 1 AS id";
                setup.ExecuteUpdate();

                // Pattern matches both the table and the view (shared prefix).
                string pattern = $"ett_%_{suffix}";

                // null -> all types: both the table and the view are returned.
                int allCount = CountTables(connection, catalog, schema, pattern, tableTypes: null, out var allTables);
                Assert.True(allTables.Any(t => t.Name == tableName), "null filter should return the TABLE");
                Assert.True(allTables.Any(t => t.Name == viewName), "null filter should return the VIEW");

                // empty (non-null) -> match NONE: neither is returned (METADATA-035).
                int emptyCount = CountTables(connection, catalog, schema, pattern, tableTypes: new string[] { }, out _);
                Assert.Equal(0, emptyCount);

                // ["TABLE"] -> only the TABLE.
                CountTables(connection, catalog, schema, pattern, tableTypes: new[] { "TABLE" }, out var tableOnly);
                Assert.True(tableOnly.Any(t => t.Name == tableName), "TABLE filter should return the table");
                Assert.False(tableOnly.Any(t => t.Name == viewName), "TABLE filter should exclude the view");
            }
            finally
            {
                setup.SqlQuery = $"DROP VIEW IF EXISTS {fqView}";
                try { setup.ExecuteUpdate(); } catch { /* best-effort cleanup */ }
                setup.SqlQuery = $"DROP TABLE IF EXISTS {fqTable}";
                try { setup.ExecuteUpdate(); } catch { /* best-effort cleanup */ }
            }

            await Task.CompletedTask;
        }
    }
}
