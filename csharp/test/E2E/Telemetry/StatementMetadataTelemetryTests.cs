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
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.HiveServer2;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;
using OperationType = AdbcDrivers.Databricks.Telemetry.Proto.Operation.Types.Type;
using StatementType = AdbcDrivers.Databricks.Telemetry.Proto.Statement.Types.Type;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// E2E tests for statement-level metadata command telemetry.
    /// Validates that metadata commands executed via DatabricksStatement.ExecuteQuery
    /// (e.g., SqlQuery = "getcatalogs") emit telemetry with correct StatementType.Metadata
    /// and the appropriate OperationType, rather than StatementType.Query/OperationType.ExecuteStatement.
    /// </summary>
    public class StatementMetadataTelemetryTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        // Filters to scope metadata queries and avoid MaxMessageSize errors
        private const string TestCatalog = "main";
        private const string TestSchema = "adbc_testing";
        private const string TestTable = "all_column_types";

        public StatementMetadataTelemetryTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        [SkippableFact]
        public async Task Telemetry_StatementGetCatalogs_EmitsMetadataWithListCatalogs()
        {
            await AssertStatementMetadataTelemetry(
                command: "getcatalogs",
                expectedOperationType: OperationType.ListCatalogs);
        }

        [SkippableFact]
        public async Task Telemetry_StatementGetSchemas_EmitsMetadataWithListSchemas()
        {
            await AssertStatementMetadataTelemetry(
                command: "getschemas",
                expectedOperationType: OperationType.ListSchemas,
                options: new Dictionary<string, string>
                {
                    [ApacheParameters.CatalogName] = TestCatalog,
                });
        }

        [SkippableFact]
        public async Task Telemetry_StatementGetTables_EmitsMetadataWithListTables()
        {
            await AssertStatementMetadataTelemetry(
                command: "gettables",
                expectedOperationType: OperationType.ListTables,
                options: new Dictionary<string, string>
                {
                    [ApacheParameters.CatalogName] = TestCatalog,
                    [ApacheParameters.SchemaName] = TestSchema,
                });
        }

        [SkippableFact]
        public async Task Telemetry_StatementGetColumns_EmitsMetadataWithListColumns()
        {
            await AssertStatementMetadataTelemetry(
                command: "getcolumns",
                expectedOperationType: OperationType.ListColumns,
                options: new Dictionary<string, string>
                {
                    [ApacheParameters.CatalogName] = TestCatalog,
                    [ApacheParameters.SchemaName] = TestSchema,
                    [ApacheParameters.TableName] = TestTable,
                });
        }

        [SkippableFact]
        public async Task Telemetry_StatementMetadata_AllCommands_EmitCorrectOperationType()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                var commandMappings = new (string Command, OperationType ExpectedOp, Dictionary<string, string>? Options)[]
                {
                    ("getcatalogs", OperationType.ListCatalogs, null),
                    ("getschemas", OperationType.ListSchemas, new Dictionary<string, string>
                    {
                        [ApacheParameters.CatalogName] = TestCatalog,
                    }),
                    ("gettables", OperationType.ListTables, new Dictionary<string, string>
                    {
                        [ApacheParameters.CatalogName] = TestCatalog,
                        [ApacheParameters.SchemaName] = TestSchema,
                    }),
                    ("getcolumns", OperationType.ListColumns, new Dictionary<string, string>
                    {
                        [ApacheParameters.CatalogName] = TestCatalog,
                        [ApacheParameters.SchemaName] = TestSchema,
                        [ApacheParameters.TableName] = TestTable,
                    }),
                };

                foreach (var mapping in commandMappings)
                {
                    exporter.Reset();

                    // Explicit using block so statement is disposed (and telemetry emitted) before we check
                    using (var statement = connection.CreateStatement())
                    {
                        statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
                        statement.SqlQuery = mapping.Command;

                        if (mapping.Options != null)
                        {
                            foreach (var opt in mapping.Options)
                            {
                                statement.SetOption(opt.Key, opt.Value);
                            }
                        }

                        var result = statement.ExecuteQuery();
                        result.Stream?.Dispose();
                    }

                    // Flush telemetry after statement disposal
                    if (connection is DatabricksConnection dbConn && dbConn.TelemetrySession?.TelemetryClient != null)
                    {
                        await dbConn.TelemetrySession.TelemetryClient.FlushAsync(default);
                    }

                    var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1, timeoutMs: 5000);
                    Assert.NotEmpty(logs);

                    var log = TelemetryTestHelpers.FindLog(logs, proto =>
                        proto.SqlOperation?.OperationDetail?.OperationType == mapping.ExpectedOp);

                    Assert.NotNull(log);

                    var protoLog = TelemetryTestHelpers.GetProtoLog(log);
                    Assert.Equal(StatementType.Metadata, protoLog.SqlOperation.StatementType);
                    Assert.Equal(mapping.ExpectedOp, protoLog.SqlOperation.OperationDetail.OperationType);
                }
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Helper method to test a single statement-level metadata command emits the correct telemetry.
        /// </summary>
        private async Task AssertStatementMetadataTelemetry(
            string command,
            OperationType expectedOperationType,
            Dictionary<string, string>? options = null)
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Execute metadata command via statement path
                // Explicit using block so statement is disposed (and telemetry emitted) before we check
                using (var statement = connection.CreateStatement())
                {
                    statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
                    statement.SqlQuery = command;

                    if (options != null)
                    {
                        foreach (var opt in options)
                        {
                            statement.SetOption(opt.Key, opt.Value);
                        }
                    }

                    var result = statement.ExecuteQuery();
                    result.Stream?.Dispose();
                }

                // Flush telemetry after statement disposal
                if (connection is DatabricksConnection dbConn && dbConn.TelemetrySession?.TelemetryClient != null)
                {
                    await dbConn.TelemetrySession.TelemetryClient.FlushAsync(default);
                }

                // Wait for telemetry events
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1, timeoutMs: 5000);

                Assert.NotEmpty(logs);

                // Find the metadata telemetry log with correct operation type
                var log = TelemetryTestHelpers.FindLog(logs, proto =>
                    proto.SqlOperation?.OperationDetail?.OperationType == expectedOperationType);

                Assert.NotNull(log);

                var protoLog = TelemetryTestHelpers.GetProtoLog(log);

                // Verify statement type is METADATA (not QUERY)
                Assert.Equal(StatementType.Metadata, protoLog.SqlOperation.StatementType);

                // Verify operation type matches the metadata command
                Assert.Equal(expectedOperationType, protoLog.SqlOperation.OperationDetail.OperationType);

                // Verify basic session-level telemetry fields are populated
                TelemetryTestHelpers.AssertSessionFieldsPopulated(protoLog);
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }
    }
}
