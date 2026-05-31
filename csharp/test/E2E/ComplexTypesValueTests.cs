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
using System.Collections.Generic;
using System.Text.Json;
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
    /// Validates that complex types (ARRAY, MAP, STRUCT) are returned consistently
    /// for both Thrift and SEA (Statement Execution API) protocols.
    ///
    /// Default behavior (EnableComplexDatatypeSupport=false):
    ///   Both Thrift and SEA return complex types as JSON strings (StringType).
    ///   Thrift: ComplexTypesAsArrow=true (client-side serialization via ComplexTypeSerializingStream).
    ///   SEA:    Native Arrow types are serialized to JSON strings by ComplexTypeSerializingStream.
    ///
    /// When EnableComplexDatatypeSupport=true, SEA returns native Arrow types (ListType/MapType/StructType).
    /// </summary>
    public class ComplexTypesValueTests : ComplexTypesValueTests<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public ComplexTypesValueTests(ITestOutputHelper output)
            : base(output, new DatabricksTestEnvironment.Factory())
        {
        }

        /// <summary>
        /// Executes a SELECT returning a single complex-type column and validates it returns
        /// a JSON string (the default behavior for both Thrift and SEA protocols).
        /// </summary>
        private async Task ValidateComplexColumnAsync(string sql, string expectedJson)
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
            Assert.Equal(expectedJson, arr.GetString(0));
        }

        /// <summary>
        /// Validates a query returning a single NULL complex-type column.
        /// Both protocols: asserts StringType column with a null value.
        /// </summary>
        private async Task ValidateNullComplexColumnAsync(string sql)
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

        // Databricks serializes complex-type elements client-side via ComplexTypeSerializingStream
        // (System.Text.Json, valid JSON). For the projections below that differs from the upstream
        // baseline, which encodes the old server-emitted format — unquoted dates/timestamps/intervals
        // and bare-integer map keys, neither of which is valid JSON. We can't fix this at the call
        // site (the [InlineData] lives in the shared Common base, which the Spark tests also inherit
        // and which still use the server format), so map each affected projection to its corrected,
        // valid-JSON expectation here. The exact strings are pinned by ComplexTypeSerializingStreamTests.
        // INT/LONG/NUMERIC/STRING and bare-double already match the baseline and are intentionally absent.
        // Keys must match the base [InlineData] projection verbatim; a drifted key falls back to the
        // inherited value and the test fails loudly rather than silently passing.
        private static readonly Dictionary<string, string> CorrectedArrayExpectations = new Dictionary<string, string>
        {
            ["ARRAY(CAST(1 AS DOUBLE), 2, 3)"] = "[1,2,3]",
            ["ARRAY(CAST('2024-01-01T00:00:00Z' AS DATE), CAST('2024-02-02T02:02:02Z' AS DATE), CAST('2024-03-03T03:03:03Z' AS DATE))"] =
                "[\"2024-01-01\",\"2024-02-02\",\"2024-03-03\"]",
            ["ARRAY(CAST('2024-01-01T00:00:00-07:00' AS TIMESTAMP), CAST('2024-02-02T02:02:02+01:30' AS TIMESTAMP), CAST('2024-03-03T03:03:03Z' AS TIMESTAMP))"] =
                "[\"2024-01-01T07:00:00+00:00\",\"2024-02-02T00:32:02+00:00\",\"2024-03-03T03:03:03+00:00\"]",
            ["ARRAY(INTERVAL 123 YEARS 11 MONTHS, INTERVAL 5 YEARS, INTERVAL 6 MONTHS)"] =
                "[\"123-11\",\"5-0\",\"0-6\"]",
        };

        private static readonly Dictionary<string, string> CorrectedMapExpectations = new Dictionary<string, string>
        {
            // Integer keys: baseline {1:"foo"} is invalid JSON; we emit quoted, key-sorted JSON.
            ["MAP(1, 'John Doe', 2, 'Jane Doe', 3, 'Jack Doe')"] =
                "{\"1\":\"John Doe\",\"2\":\"Jane Doe\",\"3\":\"Jack Doe\"}",
            // The string-key case already matches the upstream (sorted) expectation.
        };

        protected override async System.Threading.Tasks.Task ValidateTestArrayData(string projection, string value)
        {
            if (!CorrectedArrayExpectations.TryGetValue(projection, out string? expected))
                expected = value;
            await base.ValidateTestArrayData(projection, expected);
        }

        protected override async System.Threading.Tasks.Task ValidateTestMapData(string projection, string value)
        {
            if (!CorrectedMapExpectations.TryGetValue(projection, out string? expected))
                expected = value;
            await base.ValidateTestMapData(projection, expected);
        }

        // COMPLEX-001: Simple ARRAY of integers
        [SkippableFact]
        public async Task COMPLEX001_QueryReturningArray()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            await ValidateComplexColumnAsync(
                "SELECT ARRAY(CAST(1 AS INT), 2, 3)",
                "[1,2,3]");
        }

        // COMPLEX-002: Simple MAP with string keys and integer values
        [SkippableFact]
        public async Task COMPLEX002_QueryReturningMap()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            await ValidateComplexColumnAsync(
                "SELECT MAP(CAST('a' AS STRING), CAST(1 AS INT), CAST('b' AS STRING), CAST(2 AS INT))",
                """{"a":1,"b":2}""");
        }

        // PECO-3032 (D3): a MAP value containing double-quote characters must serialize to valid
        // JSON. The pre-fix Thrift server path emitted {"key":"val "quote""} (inner quotes
        // unescaped — invalid JSON); client-side System.Text.Json serialization escapes them.
        // Asserted by parse + round-trip rather than an exact string, since the escape form
        // (\" vs ") is an encoder detail. Runs on whichever protocol the matrix selects.
        [SkippableFact]
        public async Task MapValueContainingDoubleQuote_ReturnsValidJson()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));

            // Literal MAP value is: val "quote" (the value string contains two double quotes).
            Statement.SqlQuery = "SELECT MAP('key', 'val \"quote\"')";
            QueryResult result = await Statement.ExecuteQueryAsync();

            using IArrowArrayStream stream = result.Stream ?? throw new InvalidOperationException("stream is null");
            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.Equal(1, batch!.Length);

            string raw = ((StringArray)batch.Column(0)).GetString(0);
            OutputHelper?.WriteLine($"raw MAP column = {raw}");

            using JsonDocument doc = JsonDocument.Parse(raw);
            Assert.Equal(JsonValueKind.Object, doc.RootElement.ValueKind);
            Assert.Equal("val \"quote\"", doc.RootElement.GetProperty("key").GetString());
        }

        // COMPLEX-003: Simple STRUCT with two scalar fields
        [SkippableFact]
        public async Task COMPLEX003_QueryReturningStruct()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            await ValidateComplexColumnAsync(
                "SELECT STRUCT(CAST(1 AS INT) AS id, CAST('Alice' AS STRING) AS name)",
                """{"id":1,"name":"Alice"}""");
        }

        // COMPLEX-004: STRUCT containing another STRUCT (nested struct)
        [SkippableFact]
        public async Task COMPLEX004_NestedStruct()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            await ValidateComplexColumnAsync(
                "SELECT STRUCT(CAST('outer' AS STRING) AS label, STRUCT(CAST(42 AS INT) AS value) AS inner)",
                """{"label":"outer","inner":{"value":42}}""");
        }

        // COMPLEX-005: ARRAY of STRUCTs
        [SkippableFact]
        public async Task COMPLEX005_ArrayOfStruct()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            await ValidateComplexColumnAsync(
                "SELECT ARRAY(STRUCT(CAST(1 AS INT) AS id, CAST('a' AS STRING) AS val), STRUCT(CAST(2 AS INT) AS id, CAST('b' AS STRING) AS val))",
                """[{"id":1,"val":"a"},{"id":2,"val":"b"}]""");
        }

        // COMPLEX-006: STRUCT containing an ARRAY field
        [SkippableFact]
        public async Task COMPLEX006_StructWithArray()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            await ValidateComplexColumnAsync(
                "SELECT STRUCT(CAST('items' AS STRING) AS label, ARRAY(CAST(10 AS INT), 20, 30) AS nums)",
                """{"label":"items","nums":[10,20,30]}""");
        }

        // COMPLEX-007: ARRAY of ARRAYs (nested array)
        [SkippableFact]
        public async Task COMPLEX007_NestedArray()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            await ValidateComplexColumnAsync(
                "SELECT ARRAY(ARRAY(CAST(1 AS INT), 2), ARRAY(CAST(3 AS INT), 4))",
                "[[1,2],[3,4]]");
        }

        // COMPLEX-008: Empty ARRAY
        [SkippableFact]
        public async Task COMPLEX008_EmptyArray()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            await ValidateComplexColumnAsync(
                "SELECT CAST(ARRAY() AS ARRAY<INT>)",
                "[]");
        }

        // COMPLEX-009: ARRAY containing NULL elements
        [SkippableFact]
        public async Task COMPLEX009_ArrayWithNullElements()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            await ValidateComplexColumnAsync(
                "SELECT ARRAY(CAST(NULL AS INT), CAST(1 AS INT), CAST(NULL AS INT))",
                "[null,1,null]");
        }

        // COMPLEX-010: STRUCT with one NULL field
        [SkippableFact]
        public async Task COMPLEX010_NullInStruct()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            await ValidateComplexColumnAsync(
                "SELECT STRUCT(CAST(NULL AS STRING) AS name, CAST(1 AS INT) AS age)",
                """{"name":null,"age":1}""");
        }

        // COMPLEX-011: STRUCT where all fields are NULL
        [SkippableFact]
        public async Task COMPLEX011_FullyNullStruct()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            await ValidateComplexColumnAsync(
                "SELECT STRUCT(CAST(NULL AS STRING) AS name, CAST(NULL AS INT) AS age)",
                """{"name":null,"age":null}""");
        }

        // COMPLEX-012: MAP with a NULL value
        [SkippableFact]
        public async Task COMPLEX012_MapWithNullValue()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            await ValidateComplexColumnAsync(
                "SELECT MAP(CAST('key' AS STRING), CAST(NULL AS INT))",
                """{"key":null}""");
        }

        // COMPLEX-013: STRUCT containing a MAP field
        [SkippableFact]
        public async Task COMPLEX013_StructWithMap()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            await ValidateComplexColumnAsync(
                "SELECT STRUCT(CAST('meta' AS STRING) AS label, MAP(CAST('x' AS STRING), CAST(99 AS INT)) AS attrs)",
                """{"label":"meta","attrs":{"x":99}}""");
        }

        // COMPLEX-014: NULL complex column (entire ARRAY value is NULL)
        [SkippableFact]
        public async Task COMPLEX014_NullComplexColumn()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            await ValidateNullComplexColumnAsync(
                "SELECT CAST(NULL AS ARRAY<INT>)");
        }
    }
}
