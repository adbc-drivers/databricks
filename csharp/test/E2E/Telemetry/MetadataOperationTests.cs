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
using System.Linq;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;
using OperationType = AdbcDrivers.Databricks.Telemetry.Proto.Operation.Types.Type;
using StatementType = AdbcDrivers.Databricks.Telemetry.Proto.Statement.Types.Type;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// E2E tests for metadata operation telemetry.
    /// Validates that GetObjects and GetTableTypes emit telemetry with correct operation types.
    /// </summary>
    public class MetadataOperationTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public MetadataOperationTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        [SkippableFact]
        public async Task Telemetry_GetObjects_Catalogs_EmitsListCatalogs()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Execute GetObjects with depth=Catalogs
                using var stream = connection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.Catalogs,
                    catalogPattern: null,
                    dbSchemaPattern: null,
                    tableNamePattern: null,
                    tableTypes: null,
                    columnNamePattern: null);

                // Consume the stream
                while (await stream.ReadNextRecordBatchAsync() != null) { }

                // Wait for telemetry events
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1, timeoutMs: 5000);

                // Assert we captured at least one telemetry event
                Assert.NotEmpty(logs);

                // Find the GetObjects telemetry log
                var log = TelemetryTestHelpers.FindLog(logs, proto =>
                    proto.SqlOperation?.OperationDetail?.OperationType == OperationType.ListCatalogs);

                Assert.NotNull(log);

                var protoLog = TelemetryTestHelpers.GetProtoLog(log);

                // Verify statement type is METADATA
                Assert.Equal(StatementType.Metadata, protoLog.SqlOperation.StatementType);

                // Verify operation type is LIST_CATALOGS
                Assert.Equal(OperationType.ListCatalogs, protoLog.SqlOperation.OperationDetail.OperationType);

                // Verify basic telemetry fields are populated
                TelemetryTestHelpers.AssertSessionFieldsPopulated(protoLog);
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        [SkippableFact]
        public async Task Telemetry_GetObjects_Schemas_EmitsListSchemas()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Execute GetObjects with depth=DbSchemas
                using var stream = connection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.DbSchemas,
                    catalogPattern: null,
                    dbSchemaPattern: null,
                    tableNamePattern: null,
                    tableTypes: null,
                    columnNamePattern: null);

                // Consume the stream
                while (await stream.ReadNextRecordBatchAsync() != null) { }

                // Wait for telemetry events
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1, timeoutMs: 5000);

                // Assert we captured at least one telemetry event
                Assert.NotEmpty(logs);

                // Find the GetObjects telemetry log
                var log = TelemetryTestHelpers.FindLog(logs, proto =>
                    proto.SqlOperation?.OperationDetail?.OperationType == OperationType.ListSchemas);

                Assert.NotNull(log);

                var protoLog = TelemetryTestHelpers.GetProtoLog(log);

                // Verify statement type is METADATA
                Assert.Equal(StatementType.Metadata, protoLog.SqlOperation.StatementType);

                // Verify operation type is LIST_SCHEMAS
                Assert.Equal(OperationType.ListSchemas, protoLog.SqlOperation.OperationDetail.OperationType);

                // Verify basic telemetry fields are populated
                TelemetryTestHelpers.AssertSessionFieldsPopulated(protoLog);
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        [SkippableFact]
        public async Task Telemetry_GetObjects_Tables_EmitsListTables()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Execute GetObjects with depth=Tables
                using var stream = connection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.Tables,
                    catalogPattern: null,
                    dbSchemaPattern: null,
                    tableNamePattern: null,
                    tableTypes: null,
                    columnNamePattern: null);

                // Consume the stream
                while (await stream.ReadNextRecordBatchAsync() != null) { }

                // Wait for telemetry events
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1, timeoutMs: 5000);

                // Assert we captured at least one telemetry event
                Assert.NotEmpty(logs);

                // Find the GetObjects telemetry log
                var log = TelemetryTestHelpers.FindLog(logs, proto =>
                    proto.SqlOperation?.OperationDetail?.OperationType == OperationType.ListTables);

                Assert.NotNull(log);

                var protoLog = TelemetryTestHelpers.GetProtoLog(log);

                // Verify statement type is METADATA
                Assert.Equal(StatementType.Metadata, protoLog.SqlOperation.StatementType);

                // Verify operation type is LIST_TABLES
                Assert.Equal(OperationType.ListTables, protoLog.SqlOperation.OperationDetail.OperationType);

                // Verify basic telemetry fields are populated
                TelemetryTestHelpers.AssertSessionFieldsPopulated(protoLog);
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        [SkippableFact]
        public async Task Telemetry_GetObjects_Columns_EmitsListColumns()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Execute GetObjects with depth=All (includes columns)
                // Scope to information_schema to avoid tables with unsupported types (e.g., GEOMETRY)
                using var stream = connection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.All,
                    catalogPattern: "main",
                    dbSchemaPattern: "information_schema",
                    tableNamePattern: "columns",
                    tableTypes: null,
                    columnNamePattern: null);

                // Consume the stream
                while (await stream.ReadNextRecordBatchAsync() != null) { }

                // Wait for telemetry events
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1, timeoutMs: 5000);

                // Assert we captured at least one telemetry event
                Assert.NotEmpty(logs);

                // Find the GetObjects telemetry log
                var log = TelemetryTestHelpers.FindLog(logs, proto =>
                    proto.SqlOperation?.OperationDetail?.OperationType == OperationType.ListColumns);

                Assert.NotNull(log);

                var protoLog = TelemetryTestHelpers.GetProtoLog(log);

                // Verify statement type is METADATA
                Assert.Equal(StatementType.Metadata, protoLog.SqlOperation.StatementType);

                // Verify operation type is LIST_COLUMNS
                Assert.Equal(OperationType.ListColumns, protoLog.SqlOperation.OperationDetail.OperationType);

                // Verify basic telemetry fields are populated
                TelemetryTestHelpers.AssertSessionFieldsPopulated(protoLog);
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        [SkippableFact]
        public async Task Telemetry_GetTableTypes_EmitsListTableTypes()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Execute GetTableTypes
                using var stream = connection.GetTableTypes();

                // Consume the stream
                while (await stream.ReadNextRecordBatchAsync() != null) { }

                // Wait for telemetry events
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1, timeoutMs: 5000);

                // Assert we captured at least one telemetry event
                Assert.NotEmpty(logs);

                // Find the GetTableTypes telemetry log
                var log = TelemetryTestHelpers.FindLog(logs, proto =>
                    proto.SqlOperation?.OperationDetail?.OperationType == OperationType.ListTableTypes);

                Assert.NotNull(log);

                var protoLog = TelemetryTestHelpers.GetProtoLog(log);

                // Verify statement type is METADATA
                Assert.Equal(StatementType.Metadata, protoLog.SqlOperation.StatementType);

                // Verify operation type is LIST_TABLE_TYPES
                Assert.Equal(OperationType.ListTableTypes, protoLog.SqlOperation.OperationDetail.OperationType);

                // Verify basic telemetry fields are populated
                TelemetryTestHelpers.AssertSessionFieldsPopulated(protoLog);
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        [SkippableFact]
        public async Task Telemetry_GetObjects_AllDepths_EmitCorrectOperationType()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Test all depth levels
                var depthMappings = new[]
                {
                    (Depth: AdbcConnection.GetObjectsDepth.Catalogs, ExpectedOp: OperationType.ListCatalogs),
                    (Depth: AdbcConnection.GetObjectsDepth.DbSchemas, ExpectedOp: OperationType.ListSchemas),
                    (Depth: AdbcConnection.GetObjectsDepth.Tables, ExpectedOp: OperationType.ListTables),
                    (Depth: AdbcConnection.GetObjectsDepth.All, ExpectedOp: OperationType.ListColumns)
                };

                foreach (var mapping in depthMappings)
                {
                    exporter.Reset(); // Clear previous logs

                    // Scope to information_schema for depth=All to avoid tables with unsupported types (e.g., GEOMETRY)
                    string? catalogPattern = mapping.Depth == AdbcConnection.GetObjectsDepth.All ? "main" : null;
                    string? schemaPattern = mapping.Depth == AdbcConnection.GetObjectsDepth.All ? "information_schema" : null;
                    string? tablePattern = mapping.Depth == AdbcConnection.GetObjectsDepth.All ? "columns" : null;

                    using var stream = connection.GetObjects(
                        depth: mapping.Depth,
                        catalogPattern: catalogPattern,
                        dbSchemaPattern: schemaPattern,
                        tableNamePattern: tablePattern,
                        tableTypes: null,
                        columnNamePattern: null);

                    // Consume the stream
                    while (await stream.ReadNextRecordBatchAsync() != null) { }

                    // Flush telemetry
                    if (connection is DatabricksConnection dbConn && dbConn.TelemetrySession?.TelemetryClient != null)
                    {
                        await dbConn.TelemetrySession.TelemetryClient.FlushAsync(default);
                    }

                    // Wait for telemetry events
                    var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1, timeoutMs: 5000);

                    // Assert we captured the telemetry event
                    Assert.NotEmpty(logs);

                    var log = logs.First();
                    var protoLog = TelemetryTestHelpers.GetProtoLog(log);

                    // Verify operation type matches depth
                    Assert.Equal(mapping.ExpectedOp, protoLog.SqlOperation.OperationDetail.OperationType);

                    // Verify statement type is METADATA for all
                    Assert.Equal(StatementType.Metadata, protoLog.SqlOperation.StatementType);
                }
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }
    }
}
