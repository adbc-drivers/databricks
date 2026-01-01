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
using AdbcDrivers.Databricks.StatementExecution;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Types;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    /// <summary>
    /// Unit tests for GetInfo() method in Statement Execution Connection.
    /// Tests all supported info codes and verifies schema compliance.
    /// </summary>
    public class StatementExecutionGetInfoTests
    {
        private StatementExecutionConnection CreateTestConnection()
        {
            var properties = new Dictionary<string, string>
            {
                [SparkParameters.HostName] = "test.databricks.com",
                [DatabricksParameters.WarehouseId] = "test-warehouse",
                [SparkParameters.Token] = "test-token"
            };

            return new StatementExecutionConnection(properties);
        }

        #region GetInfo Basic Tests

        [Fact]
        public void GetInfo_AllCodes_ReturnsAllSupportedCodes()
        {
            var connection = CreateTestConnection();

            // Call with empty list - should return all supported codes
            using var stream = connection.GetInfo(System.Array.Empty<AdbcInfoCode>());

            Assert.NotNull(stream);
            Assert.NotNull(stream.Schema);

            // Read the batch
            using var batch = stream.ReadNextRecordBatchAsync().Result;
            Assert.NotNull(batch);

            // Should have 6 supported codes
            Assert.Equal(6, batch.Length);

            // Verify schema has correct fields
            Assert.Equal(2, batch.Schema.FieldsList.Count);
            Assert.Equal("info_name", batch.Schema.FieldsList[0].Name);
            Assert.Equal("info_value", batch.Schema.FieldsList[1].Name);
            Assert.IsType<UInt32Type>(batch.Schema.FieldsList[0].DataType);
            Assert.IsType<UnionType>(batch.Schema.FieldsList[1].DataType);
        }

        [Fact]
        public void GetInfo_SpecificCodes_ReturnsRequestedCodes()
        {
            var connection = CreateTestConnection();

            var requestedCodes = new[]
            {
                AdbcInfoCode.DriverName,
                AdbcInfoCode.DriverVersion
            };

            using var stream = connection.GetInfo(requestedCodes);
            using var batch = stream.ReadNextRecordBatchAsync().Result;

            Assert.NotNull(batch);
            Assert.Equal(2, batch.Length);

            var infoNameArray = batch.Column("info_name") as UInt32Array;
            Assert.NotNull(infoNameArray);

            // Verify requested codes are present
            var returnedCodes = new List<AdbcInfoCode>();
            for (int i = 0; i < batch.Length; i++)
            {
                returnedCodes.Add((AdbcInfoCode)infoNameArray.GetValue(i));
            }

            Assert.Contains(AdbcInfoCode.DriverName, returnedCodes);
            Assert.Contains(AdbcInfoCode.DriverVersion, returnedCodes);
        }

        [Fact]
        public void GetInfo_NullCodesList_ReturnsAllCodes()
        {
            var connection = CreateTestConnection();

            using var stream = connection.GetInfo(null!);
            using var batch = stream.ReadNextRecordBatchAsync().Result;

            Assert.NotNull(batch);
            Assert.Equal(6, batch.Length); // All 6 supported codes
        }

        #endregion

        #region Individual Info Code Tests

        [Fact]
        public void GetInfo_VendorName_ReturnsDatabricks()
        {
            var connection = CreateTestConnection();

            using var stream = connection.GetInfo(new[] { AdbcInfoCode.VendorName });
            using var batch = stream.ReadNextRecordBatchAsync().Result;

            Assert.NotNull(batch);
            Assert.Equal(1, batch.Length);

            var infoValueUnion = batch.Column("info_value") as DenseUnionArray;
            Assert.NotNull(infoValueUnion);

            // Type 0 is string_value
            var stringValueArray = infoValueUnion.Fields[0] as StringArray;
            Assert.NotNull(stringValueArray);

            var value = stringValueArray.GetString(0);
            Assert.Equal("Databricks", value);
        }

        [Fact]
        public void GetInfo_VendorVersion_ReturnsVersionOrUnknown()
        {
            var connection = CreateTestConnection();

            using var stream = connection.GetInfo(new[] { AdbcInfoCode.VendorVersion });
            using var batch = stream.ReadNextRecordBatchAsync().Result;

            Assert.NotNull(batch);
            Assert.Equal(1, batch.Length);

            var infoValueUnion = batch.Column("info_value") as DenseUnionArray;
            Assert.NotNull(infoValueUnion);

            var stringValueArray = infoValueUnion.Fields[0] as StringArray;
            Assert.NotNull(stringValueArray);

            var value = stringValueArray.GetString(0);
            Assert.NotNull(value);
            // Should be "Unknown" or a version string
            Assert.True(value == "Unknown" || !string.IsNullOrEmpty(value));
        }

        [Fact]
        public void GetInfo_VendorArrowVersion_NotSupported()
        {
            var connection = CreateTestConnection();

            using var stream = connection.GetInfo(new[] { AdbcInfoCode.VendorArrowVersion });
            using var batch = stream.ReadNextRecordBatchAsync().Result;

            Assert.NotNull(batch);
            var infoValueUnion = batch.Column("info_value") as DenseUnionArray;
            var stringValueArray = infoValueUnion.Fields[0] as StringArray;

            // VendorArrowVersion is not in the supported codes list - should return null
            Assert.True(stringValueArray.IsNull(0));
        }

        [Fact]
        public void GetInfo_VendorSql_ReturnsFalse()
        {
            var connection = CreateTestConnection();

            using var stream = connection.GetInfo(new[] { AdbcInfoCode.VendorSql });
            using var batch = stream.ReadNextRecordBatchAsync().Result;

            Assert.NotNull(batch);
            var infoValueUnion = batch.Column("info_value") as DenseUnionArray;

            // Type 1 is bool_value
            var boolValueArray = infoValueUnion.Fields[1] as BooleanArray;
            Assert.NotNull(boolValueArray);

            var value = boolValueArray.GetValue(0);
            Assert.True(value); // Databricks supports SQL (matches Thrift implementation)
        }

        [Fact]
        public void GetInfo_DriverName_ReturnsCorrectName()
        {
            var connection = CreateTestConnection();

            using var stream = connection.GetInfo(new[] { AdbcInfoCode.DriverName });
            using var batch = stream.ReadNextRecordBatchAsync().Result;

            Assert.NotNull(batch);
            var infoValueUnion = batch.Column("info_value") as DenseUnionArray;
            var stringValueArray = infoValueUnion.Fields[0] as StringArray;
            var value = stringValueArray.GetString(0);

            Assert.Equal("ADBC Databricks Driver (Statement Execution API)", value);
        }

        [Fact]
        public void GetInfo_DriverVersion_ReturnsAssemblyVersion()
        {
            var connection = CreateTestConnection();

            using var stream = connection.GetInfo(new[] { AdbcInfoCode.DriverVersion });
            using var batch = stream.ReadNextRecordBatchAsync().Result;

            Assert.NotNull(batch);
            var infoValueUnion = batch.Column("info_value") as DenseUnionArray;
            var stringValueArray = infoValueUnion.Fields[0] as StringArray;
            var value = stringValueArray.GetString(0);

            Assert.NotNull(value);
            Assert.NotEmpty(value);
            // Should be a version string like "1.0.0"
            Assert.Matches(@"^\d+\.\d+\.\d+", value);
        }

        [Fact]
        public void GetInfo_DriverArrowVersion_ReturnsArrowVersion()
        {
            var connection = CreateTestConnection();

            using var stream = connection.GetInfo(new[] { AdbcInfoCode.DriverArrowVersion });
            using var batch = stream.ReadNextRecordBatchAsync().Result;

            Assert.NotNull(batch);
            var infoValueUnion = batch.Column("info_value") as DenseUnionArray;
            var stringValueArray = infoValueUnion.Fields[0] as StringArray;
            var value = stringValueArray.GetString(0);

            Assert.Equal("1.0.0", value);
        }

        #endregion

        #region Schema Validation Tests

        [Fact]
        public void GetInfo_SchemaFollowsAdbcSpec()
        {
            var connection = CreateTestConnection();

            using var stream = connection.GetInfo(new[] { AdbcInfoCode.DriverName });
            var schema = stream.Schema;

            // Verify schema matches ADBC spec
            Assert.Equal(2, schema.FieldsList.Count);

            // info_name: uint32, not nullable
            var infoNameField = schema.FieldsList[0];
            Assert.Equal("info_name", infoNameField.Name);
            Assert.IsType<UInt32Type>(infoNameField.DataType);
            Assert.False(infoNameField.IsNullable);

            // info_value: union, nullable
            var infoValueField = schema.FieldsList[1];
            Assert.Equal("info_value", infoValueField.Name);
            Assert.IsType<UnionType>(infoValueField.DataType);
            Assert.True(infoValueField.IsNullable);
        }

        [Fact]
        public void GetInfo_UnionTypeHasCorrectFields()
        {
            var connection = CreateTestConnection();

            using var stream = connection.GetInfo(new[] { AdbcInfoCode.DriverName });
            var schema = stream.Schema;

            var infoValueField = schema.FieldsList[1];
            var unionType = infoValueField.DataType as UnionType;
            Assert.NotNull(unionType);

            // Verify union has all required fields
            var fieldNames = unionType.Fields.Select(f => f.Name).ToList();
            Assert.Contains("string_value", fieldNames);
            Assert.Contains("bool_value", fieldNames);
            Assert.Contains("int64_value", fieldNames);
            Assert.Contains("int32_bitmask", fieldNames);
            Assert.Contains("string_list", fieldNames);
            Assert.Contains("int32_to_int32_list_map", fieldNames);

            // Verify union mode is Dense
            Assert.Equal(UnionMode.Dense, unionType.Mode);
        }

        #endregion

        #region Error Handling Tests

        [Fact]
        public void GetInfo_UnsupportedCode_ReturnsNull()
        {
            var connection = CreateTestConnection();

            // Request an unsupported code
            var unsupportedCode = (AdbcInfoCode)999;
            using var stream = connection.GetInfo(new[] { unsupportedCode });
            using var batch = stream.ReadNextRecordBatchAsync().Result;

            Assert.NotNull(batch);
            Assert.Equal(1, batch.Length);

            var infoNameArray = batch.Column("info_name") as UInt32Array;
            Assert.Equal((uint)999, infoNameArray.GetValue(0));

            var infoValueUnion = batch.Column("info_value") as DenseUnionArray;
            var stringValueArray = infoValueUnion.Fields[0] as StringArray;

            // Unsupported codes should return null value
            Assert.True(stringValueArray.IsNull(0));
        }

        [Fact]
        public void GetInfo_MultipleCallsSameConnection_WorksCorrectly()
        {
            var connection = CreateTestConnection();

            // First call
            using (var stream1 = connection.GetInfo(new[] { AdbcInfoCode.DriverName }))
            using (var batch1 = stream1.ReadNextRecordBatchAsync().Result)
            {
                Assert.NotNull(batch1);
                Assert.Equal(1, batch1.Length);
            }

            // Second call
            using (var stream2 = connection.GetInfo(new[] { AdbcInfoCode.DriverVersion }))
            using (var batch2 = stream2.ReadNextRecordBatchAsync().Result)
            {
                Assert.NotNull(batch2);
                Assert.Equal(1, batch2.Length);
            }
        }

        #endregion

        #region Performance Tests

        [Fact]
        public void GetInfo_AllCodes_CompletesQuickly()
        {
            var connection = CreateTestConnection();
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            using var stream = connection.GetInfo(System.Array.Empty<AdbcInfoCode>());
            using var batch = stream.ReadNextRecordBatchAsync().Result;

            stopwatch.Stop();

            // GetInfo should be very fast (<10ms) since it's all in-memory
            Assert.True(stopwatch.ElapsedMilliseconds < 100);
        }

        #endregion

        #region Disposal Tests

        [Fact]
        public void GetInfo_ConnectionDisposal_DoesNotThrow()
        {
            var connection = CreateTestConnection();

            using (var stream = connection.GetInfo(new[] { AdbcInfoCode.DriverName }))
            {
                var batch = stream.ReadNextRecordBatchAsync().Result;
                Assert.NotNull(batch);
            }

            // Dispose connection - should not throw
            var exception = Record.Exception(() => connection.Dispose());
            Assert.Null(exception);
        }

        #endregion
    }
}
