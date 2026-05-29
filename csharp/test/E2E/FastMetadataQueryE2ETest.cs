/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*/

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using AdbcDrivers.HiveServer2;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E
{
    /// <summary>
    /// One-off live verification for PECO-3021. Runs GetColumnsExtended with
    /// adbc.databricks.enable_fast_metadata_query=true and prints a time window
    /// the user can use to grep system.query.history for "STATIC ONLY".
    /// </summary>
    public class FastMetadataQueryE2ETest : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public FastMetadataQueryE2ETest(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        [SkippableFact]
        public async Task FastMetadataQuery_LivelyEmitsStaticOnly()
        {
            var extra = new Dictionary<string, string>
            {
                [DatabricksParameters.UseDescTableExtended] = "true",
                [DatabricksParameters.EnableFastMetadataQuery] = "true",
            };

            using AdbcConnection connection = NewConnection(TestConfiguration, extra);
            using var statement = connection.CreateStatement();

            string catalog = TestConfiguration.Metadata.Catalog;
            string schema = TestConfiguration.Metadata.Schema;
            string table = TestConfiguration.Metadata.Table;

            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SetOption(ApacheParameters.CatalogName, catalog);
            statement.SetOption(ApacheParameters.SchemaName, schema);
            statement.SetOption(ApacheParameters.TableName, table);
            statement.SqlQuery = "GetColumnsExtended";

            DateTime startUtc = DateTime.UtcNow;
            OutputHelper?.WriteLine($"PECO-3021 fast-metadata window START (UTC): {startUtc:O}");
            OutputHelper?.WriteLine($"Target table: {catalog}.{schema}.{table}");

            QueryResult result = await statement.ExecuteQueryAsync();
            Assert.NotNull(result.Stream);

            // Drain to ensure server actually executes
            int rowCount = 0;
            using (var stream = result.Stream)
            {
                while (true)
                {
                    var batch = await stream.ReadNextRecordBatchAsync();
                    if (batch == null) break;
                    rowCount += batch.Length;
                    batch.Dispose();
                }
            }

            DateTime endUtc = DateTime.UtcNow;
            OutputHelper?.WriteLine($"PECO-3021 fast-metadata window END (UTC):   {endUtc:O}");
            OutputHelper?.WriteLine($"Rows returned: {rowCount}");
            Assert.True(rowCount > 0, "GetColumnsExtended should return at least one column row");
        }

        /// <summary>
        /// Negative-control: when adbc.databricks.enable_fast_metadata_query is NOT set,
        /// the driver must emit the base form (no STATIC ONLY modifier). Verifies in the
        /// query history that the SQL sent is exactly <c>DESC TABLE EXTENDED ... AS JSON</c>.
        /// </summary>
        [SkippableFact]
        public async Task FastMetadataQuery_Disabled_EmitsBaseAsJson()
        {
            var extra = new Dictionary<string, string>
            {
                [DatabricksParameters.UseDescTableExtended] = "true",
                // EnableFastMetadataQuery NOT set — verify default behavior is base AS JSON
            };

            using AdbcConnection connection = NewConnection(TestConfiguration, extra);
            using var statement = connection.CreateStatement();

            string catalog = TestConfiguration.Metadata.Catalog;
            string schema = TestConfiguration.Metadata.Schema;
            string table = TestConfiguration.Metadata.Table;

            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SetOption(ApacheParameters.CatalogName, catalog);
            statement.SetOption(ApacheParameters.SchemaName, schema);
            statement.SetOption(ApacheParameters.TableName, table);
            statement.SqlQuery = "GetColumnsExtended";

            DateTime startUtc = DateTime.UtcNow;
            OutputHelper?.WriteLine($"PECO-3021 flag-disabled window START (UTC): {startUtc:O}");

            QueryResult result = await statement.ExecuteQueryAsync();
            Assert.NotNull(result.Stream);

            int rowCount = 0;
            using (var stream = result.Stream)
            {
                while (true)
                {
                    var batch = await stream.ReadNextRecordBatchAsync();
                    if (batch == null) break;
                    rowCount += batch.Length;
                    batch.Dispose();
                }
            }

            DateTime endUtc = DateTime.UtcNow;
            OutputHelper?.WriteLine($"PECO-3021 flag-disabled window END (UTC):   {endUtc:O}");
            OutputHelper?.WriteLine($"Rows returned: {rowCount}");
            Assert.True(rowCount > 0);
        }

        /// <summary>
        /// Verifies the *runtime* on the target endpoint accepts the new STATIC ONLY
        /// modifier. Sends DESC TABLE EXTENDED ... AS JSON STATIC ONLY directly as a SQL
        /// statement (not via GetColumnsExtended), so the driver's warehouse-path gating
        /// is bypassed and the runtime's response is what we see.
        ///
        /// Pass: runtime has PR #198486 (returns a row of JSON).
        /// Fail with SQLSTATE=42601 PARSE_SYNTAX_ERROR at 'ONLY': runtime is pre-rollout.
        /// </summary>
        [SkippableFact]
        public async Task StaticOnly_DirectSql_RuntimeAcceptsKeyword()
        {
            using AdbcConnection connection = NewConnection();
            using var statement = connection.CreateStatement();

            string catalog = TestConfiguration.Metadata.Catalog;
            string schema = TestConfiguration.Metadata.Schema;
            string table = TestConfiguration.Metadata.Table;
            string sql = $"DESC TABLE EXTENDED `{catalog}`.`{schema}`.`{table}` AS JSON STATIC ONLY";

            statement.SqlQuery = sql;
            OutputHelper?.WriteLine($"Probing runtime with: {sql}");
            DateTime startUtc = DateTime.UtcNow;

            QueryResult result = await statement.ExecuteQueryAsync();
            Assert.NotNull(result.Stream);

            int rowCount = 0;
            string? firstCell = null;
            using (var stream = result.Stream)
            {
                while (true)
                {
                    var batch = await stream.ReadNextRecordBatchAsync();
                    if (batch == null) break;
                    if (rowCount == 0 && batch.Length > 0 && batch.ColumnCount > 0
                        && batch.Column(0) is StringArray sa)
                    {
                        firstCell = sa.GetString(0);
                    }
                    rowCount += batch.Length;
                    batch.Dispose();
                }
            }
            DateTime endUtc = DateTime.UtcNow;
            OutputHelper?.WriteLine($"Runtime accepted STATIC ONLY — rows={rowCount}, took {(endUtc-startUtc).TotalMilliseconds:0}ms");
            if (firstCell != null)
            {
                OutputHelper?.WriteLine($"Result preview (first {Math.Min(200, firstCell.Length)} chars): {firstCell.Substring(0, Math.Min(200, firstCell.Length))}");
            }
            Assert.True(rowCount > 0, "Runtime should return JSON metadata when STATIC ONLY is supported");
        }
    }
}
