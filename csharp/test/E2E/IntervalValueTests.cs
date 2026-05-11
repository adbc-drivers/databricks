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
using System.Globalization;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using AdbcDrivers.Tests.HiveServer2.Common;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests
{
    /// <summary>
    /// Validates that INTERVAL types (YEAR TO MONTH and DAY TO SECOND) are returned as
    /// UTF-8 strings (StringType) for both Thrift and SEA (Statement Execution API) protocols.
    ///
    /// For SEA, the fix in IntervalSerializingStream converts YearMonthIntervalArray and
    /// DurationArray to StringArray before returning results to the caller.
    ///
    /// String formats:
    ///   YEAR-MONTH: "Y-M" with no zero-padding (e.g., 2 years 6 months => "2-6")
    ///   DAY-TIME:   "D HH:MM:SS.nnnnnnnnn" with 9-digit nanosecond precision
    ///               (e.g., 3 days 12h 30m 15s => "3 12:30:15.000000000")
    /// </summary>
    public class IntervalValueTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public IntervalValueTests(ITestOutputHelper output)
            : base(output, new DatabricksTestEnvironment.Factory())
        {
        }

        /// <summary>
        /// Executes a SELECT returning a single INTERVAL column and validates that the schema
        /// reports StringType and the value matches the expected string representation.
        /// </summary>
        private async Task ValidateIntervalColumnAsync(string sql, string expectedValue)
        {
            Statement.SqlQuery = sql;
            QueryResult result = await Statement.ExecuteQueryAsync();

            using IArrowArrayStream stream = result.Stream ?? throw new InvalidOperationException("stream is null");
            Field field = stream.Schema.GetFieldByIndex(0);

            Assert.IsType<StringType>(field.DataType);

            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.Equal(1, batch.Length);

            StringArray arr = (StringArray)batch.Column(0);
            Assert.Equal(expectedValue, arr.GetString(0));
        }

        /// <summary>
        /// Executes a SELECT returning a single NULL INTERVAL column and validates that the
        /// schema reports StringType and the value is null.
        /// </summary>
        private async Task ValidateNullIntervalColumnAsync(string sql)
        {
            Statement.SqlQuery = sql;
            QueryResult result = await Statement.ExecuteQueryAsync();

            using IArrowArrayStream stream = result.Stream ?? throw new InvalidOperationException("stream is null");
            Field field = stream.Schema.GetFieldByIndex(0);

            Assert.IsType<StringType>(field.DataType);

            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.Equal(1, batch.Length);
            Assert.True(batch.Column(0).IsNull(0), "Expected null value");
        }

        // INTERVAL-001: YEAR TO MONTH interval - 2 years 6 months
        [SkippableFact]
        public async Task INTERVAL001_YearMonthInterval()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            await ValidateIntervalColumnAsync(
                "SELECT INTERVAL '2-6' YEAR TO MONTH",
                "2-6");
        }

        // INTERVAL-002: YEAR TO MONTH interval - 0 years 1 month
        [SkippableFact]
        public async Task INTERVAL002_YearMonthIntervalZeroYears()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            await ValidateIntervalColumnAsync(
                "SELECT INTERVAL '0-1' YEAR TO MONTH",
                "0-1");
        }

        // INTERVAL-003: DAY TO SECOND interval - 3 days 12h 30m 15s
        [SkippableFact]
        public async Task INTERVAL003_DayTimeInterval()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            await ValidateIntervalColumnAsync(
                "SELECT INTERVAL '3 12:30:15' DAY TO SECOND",
                "3 12:30:15.000000000");
        }

        // INTERVAL-004: DAY TO SECOND interval - zero duration
        [SkippableFact]
        public async Task INTERVAL004_DayTimeIntervalZero()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            await ValidateIntervalColumnAsync(
                "SELECT INTERVAL '0 00:00:00' DAY TO SECOND",
                "0 00:00:00.000000000");
        }

        // INTERVAL-005: DAY TO SECOND interval - sub-second precision
        [SkippableFact]
        public async Task INTERVAL005_DayTimeIntervalSubSecond()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            await ValidateIntervalColumnAsync(
                "SELECT INTERVAL '1 00:00:00.123456' DAY TO SECOND",
                "1 00:00:00.123456000");
        }

        // INTERVAL-006: NULL YEAR TO MONTH interval
        [SkippableFact]
        public async Task INTERVAL006_NullYearMonthInterval()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            await ValidateNullIntervalColumnAsync(
                "SELECT CAST(NULL AS INTERVAL YEAR TO MONTH)");
        }

        // INTERVAL-007: NULL DAY TO SECOND interval
        [SkippableFact]
        public async Task INTERVAL007_NullDayTimeInterval()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            await ValidateNullIntervalColumnAsync(
                "SELECT CAST(NULL AS INTERVAL DAY TO SECOND)");
        }

        // INTERVAL-008: CloudFetch path - YEAR TO MONTH interval over large result set
        // Forces the CloudFetch path through ComplexTypeSerializingStream by generating 20,000 rows.
        [SkippableFact]
        public async Task INTERVAL008_YearMonthIntervalCloudFetch()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));

            Statement.SqlQuery = "SELECT INTERVAL '2-6' YEAR TO MONTH FROM (SELECT explode(sequence(1, 20000)))";
            QueryResult result = await Statement.ExecuteQueryAsync();

            using IArrowArrayStream stream = result.Stream ?? throw new InvalidOperationException("stream is null");
            Field field = stream.Schema.GetFieldByIndex(0);

            Assert.IsType<StringType>(field.DataType);

            long totalRows = 0;
            RecordBatch? batch;
            while ((batch = await stream.ReadNextRecordBatchAsync()) != null)
            {
                StringArray arr = (StringArray)batch.Column(0);
                for (int i = 0; i < batch.Length; i++)
                {
                    Assert.Equal("2-6", arr.GetString(i));
                }
                totalRows += batch.Length;
            }

            Assert.Equal(20000L, totalRows);
        }

        // INTERVAL-009: Cross-protocol — Thrift and SEA return identical YEAR TO MONTH strings.
        [SkippableFact]
        public async Task INTERVAL009_YearMonthInterval_ThriftAndSEAReturnSameData()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            await AssertProtocolsAgreeAsync(
                "SELECT" +
                "  INTERVAL '2-6' YEAR TO MONTH               AS c_ym," +
                "  INTERVAL '0-1' YEAR TO MONTH               AS c_ym_zero_years," +
                "  CAST(NULL AS INTERVAL YEAR TO MONTH)       AS c_ym_null," +
                "  INTERVAL '-2-6' YEAR TO MONTH              AS c_ym_neg," +
                "  INTERVAL '-0-10' YEAR TO MONTH             AS c_ym_neg_zero_years");
        }

        // INTERVAL-010: Cross-protocol — Thrift and SEA return identical DAY TO SECOND strings.
        [SkippableFact]
        public async Task INTERVAL010_DayToSecondInterval_ThriftAndSEAReturnSameData()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            await AssertProtocolsAgreeAsync(
                "SELECT" +
                "  INTERVAL '3 12:30:15' DAY TO SECOND        AS c_ds," +
                "  INTERVAL '0 00:00:00' DAY TO SECOND        AS c_ds_zero," +
                "  INTERVAL '1 00:00:00.123456' DAY TO SECOND AS c_ds_subsecond," +
                "  CAST(NULL AS INTERVAL DAY TO SECOND)       AS c_ds_null," +
                "  INTERVAL '-3 12:30:15' DAY TO SECOND       AS c_ds_neg," +
                "  INTERVAL '-0 00:00:00.000001' DAY TO SECOND AS c_ds_neg_subsecond");
        }

        // -----------------------------------------------------------------
        // Cross-protocol helpers
        // -----------------------------------------------------------------

        /// <summary>
        /// Runs <paramref name="sql"/> on independent Thrift and SEA connections (regardless
        /// of what protocol the test config specifies) and asserts every cell is identical.
        /// </summary>
        private async Task AssertProtocolsAgreeAsync(string sql)
        {
            using AdbcConnection thriftConn = NewConnectionForProtocol(protocol: null);
            using AdbcConnection seaConn = NewConnectionForProtocol(protocol: "rest");

            List<string?[]> thriftRows = await ReadAllRowsAsStringsAsync(thriftConn, sql);
            List<string?[]> seaRows = await ReadAllRowsAsStringsAsync(seaConn, sql);

            Assert.Equal(thriftRows.Count, seaRows.Count);
            for (int row = 0; row < thriftRows.Count; row++)
            {
                string?[] thrift = thriftRows[row];
                string?[] sea = seaRows[row];
                Assert.Equal(thrift.Length, sea.Length);
                for (int col = 0; col < thrift.Length; col++)
                {
                    Assert.True(thrift[col] == sea[col],
                        $"Row {row}, col {col}: Thrift={thrift[col]}, SEA={sea[col]}");
                }
            }
        }

        private AdbcConnection NewConnectionForProtocol(string? protocol)
        {
            var parameters = new Dictionary<string, string>(TestEnvironment.GetDriverParameters(TestConfiguration));
            parameters.Remove(DatabricksParameters.Protocol);
            if (protocol != null)
                parameters[DatabricksParameters.Protocol] = protocol;
            return TestEnvironment.CreateNewDriver().Open(parameters).Connect(new Dictionary<string, string>());
        }

        private static async Task<List<string?[]>> ReadAllRowsAsStringsAsync(AdbcConnection conn, string sql)
        {
            using AdbcStatement stmt = conn.CreateStatement();
            stmt.SqlQuery = sql;
            QueryResult result = await stmt.ExecuteQueryAsync();
            using IArrowArrayStream stream = result.Stream ?? throw new InvalidOperationException("stream is null");

            int colCount = stream.Schema.FieldsList.Count;
            var rows = new List<string?[]>();
            RecordBatch? batch;
            while ((batch = await stream.ReadNextRecordBatchAsync()) != null)
            {
                for (int row = 0; row < batch.Length; row++)
                {
                    var rowData = new string?[colCount];
                    for (int col = 0; col < colCount; col++)
                        rowData[col] = ExtractValueAsString(batch.Column(col), row);
                    rows.Add(rowData);
                }
            }
            return rows;
        }

        private static string? ExtractValueAsString(IArrowArray array, int index)
        {
            if (array.IsNull(index))
                return null;
            return array switch
            {
                StringArray sa => sa.GetString(index),
                Int8Array int8 => int8.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture),
                Int16Array int16 => int16.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture),
                Int32Array int32 => int32.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture),
                Int64Array int64 => int64.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture),
                FloatArray floatArr => floatArr.GetValue(index)!.Value.ToString("R", CultureInfo.InvariantCulture),
                DoubleArray doubleArr => doubleArr.GetValue(index)!.Value.ToString("R", CultureInfo.InvariantCulture),
                BooleanArray boolArr => boolArr.GetValue(index)!.Value.ToString(),
                Date32Array date32 => date32.GetDateTime(index)!.Value.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                TimestampArray ts => ts.GetTimestamp(index)!.Value.ToString("O", CultureInfo.InvariantCulture),
                Decimal128Array dec => dec.GetString(index),
                BinaryArray bin => BitConverter.ToString(bin.GetBytes(index, out _).ToArray()).Replace("-", ""),
                _ => throw new InvalidOperationException($"Unhandled Arrow array type: {array.GetType().Name}")
            };
        }
    }
}
