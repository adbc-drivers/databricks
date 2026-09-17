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

using System.Collections.Generic;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Types;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests
{
    /// <summary>
    /// End-to-end tests for Databricks named query parameters (Issue #648).
    /// Databricks supports named parameters using ":name" placeholders. The driver
    /// must forward ADBC-bound parameters (via <see cref="AdbcStatement.Bind"/>) to
    /// the server as TSparkParameter entries so the placeholders resolve.
    /// </summary>
    public class NamedParametersE2ETest : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public NamedParametersE2ETest(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Binds a single named string parameter and asserts the server resolves the
        /// ":p1" placeholder to the bound value. Fails today because the driver never
        /// populates TExecuteStatementReq.Parameters, so the server sees an unbound
        /// parameter reference.
        /// </summary>
        [SkippableFact]
        public async Task NamedStringParameter_RoundTrips()
        {
            using AdbcConnection connection = NewConnection();
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT :p1 AS v";

            var schema = new Schema(
                new List<Field> { new Field("p1", StringType.Default, true) },
                null);
            var valueArray = new StringArray.Builder().Append("hello world").Build();
            var batch = new RecordBatch(schema, new IArrowArray[] { valueArray }, 1);

            statement.Bind(batch, schema);

            var result = await statement.ExecuteQueryAsync();
            Assert.NotNull(result.Stream);

            var resultBatch = await result.Stream!.ReadNextRecordBatchAsync();
            Assert.NotNull(resultBatch);
            var column = Assert.IsType<StringArray>(resultBatch!.Column(0));
            Assert.Equal("hello world", column.GetString(0));
        }

        /// <summary>
        /// Binds non-string named parameters (BIGINT and DOUBLE) used in a typed
        /// arithmetic context, verifying the Arrow-to-parameter type mapping rather
        /// than a bare string passthrough.
        /// </summary>
        [SkippableFact]
        public async Task TypedNamedParameters_InferCorrectSqlType()
        {
            using AdbcConnection connection = NewConnection();
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT :n + 1 AS r, :d * 2 AS d2, :b AS flag";

            var schema = new Schema(
                new List<Field>
                {
                    new Field("n", Int64Type.Default, true),
                    new Field("d", DoubleType.Default, true),
                    new Field("b", BooleanType.Default, true),
                },
                null);
            var nArray = new Int64Array.Builder().Append(41).Build();
            var dArray = new DoubleArray.Builder().Append(1.5).Build();
            var bArray = new BooleanArray.Builder().Append(true).Build();
            var batch = new RecordBatch(schema, new IArrowArray[] { nArray, dArray, bArray }, 1);

            statement.Bind(batch, schema);

            var result = await statement.ExecuteQueryAsync();
            Assert.NotNull(result.Stream);

            var resultBatch = await result.Stream!.ReadNextRecordBatchAsync();
            Assert.NotNull(resultBatch);

            var rColumn = Assert.IsType<Int64Array>(resultBatch!.Column(0));
            Assert.Equal(42, rColumn.GetValue(0));

            var d2Column = Assert.IsType<DoubleArray>(resultBatch.Column(1));
            Assert.Equal(3.0, d2Column.GetValue(0));

            var flagColumn = Assert.IsType<BooleanArray>(resultBatch.Column(2));
            Assert.True(flagColumn.GetValue(0));
        }
    }
}
