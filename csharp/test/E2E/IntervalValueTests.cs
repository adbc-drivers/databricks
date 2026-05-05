/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* This file has been modified from its original version, which is
* under the Apache License:
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
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
    /// For SEA, the fix in ComplexTypeSerializingStream converts YearMonthIntervalArray and
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
    }
}
