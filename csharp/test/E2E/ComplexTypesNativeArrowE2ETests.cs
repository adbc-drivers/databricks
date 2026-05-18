/*
 * Copyright (c) 2025 ADBC Drivers Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E
{
    /// <summary>
    /// E2E coverage for PECO-3047. Each complex-type test runs twice:
    ///   flag=true  → asserts native Arrow ListType / MapType / StructType (with accurate
    ///                element types parsed from the manifest's type_text).
    ///   flag=false → asserts the legacy StringType + JSON-string contract is preserved
    ///                (no regression).
    /// </summary>
    public class ComplexTypesNativeArrowE2ETests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public ComplexTypesNativeArrowE2ETests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Opens a connection with <paramref name="enableComplexDatatypeSupport"/> and runs
        /// <paramref name="sql"/>, returning the connection and reader so the caller can
        /// inspect the schema and data.
        /// </summary>
        private async Task<(AdbcConnection conn, IArrowArrayStream stream)> ExecuteAsync(string sql, bool enableComplexDatatypeSupport)
        {
            var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
            // Some configs have both 'uri' and 'hostName'/'path' — the driver rejects this
            // combination. hostName/path take precedence in test runs, so drop the uri.
            properties.Remove(Apache.Arrow.Adbc.AdbcOptions.Uri);
            properties[DatabricksParameters.EnableComplexDatatypeSupport] = enableComplexDatatypeSupport ? "true" : "false";

            AdbcDriver driver = new DatabricksDriver();
            AdbcDatabase database = driver.Open(properties);
            AdbcConnection connection = database.Connect(properties);

            var statement = connection.CreateStatement();
            statement.SqlQuery = sql;
            QueryResult result = await statement.ExecuteQueryAsync();
            return (connection, result.Stream!);
        }

        [SkippableTheory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Array_OfInt(bool enableComplexDatatypeSupport)
        {
            var (conn, stream) = await ExecuteAsync(
                "SELECT ARRAY(CAST(1 AS INT), 2, 3)",
                enableComplexDatatypeSupport);
            using (conn)
            using (stream)
            {
                Field field = stream.Schema.GetFieldByIndex(0);
                RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
                Assert.Equal(1, batch!.Length);

                if (enableComplexDatatypeSupport)
                {
                    var list = Assert.IsType<ListType>(field.DataType);
                    Assert.IsType<Int32Type>(list.ValueDataType);
                    Assert.IsType<ListArray>(batch.Column(0));
                }
                else
                {
                    Assert.IsType<StringType>(field.DataType);
                    Assert.Equal("[1,2,3]", ((StringArray)batch.Column(0)).GetString(0));
                }
            }
        }

        [SkippableTheory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Map_StringToInt(bool enableComplexDatatypeSupport)
        {
            var (conn, stream) = await ExecuteAsync(
                "SELECT MAP(CAST('a' AS STRING), CAST(1 AS INT), CAST('b' AS STRING), CAST(2 AS INT))",
                enableComplexDatatypeSupport);
            using (conn)
            using (stream)
            {
                Field field = stream.Schema.GetFieldByIndex(0);
                RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
                Assert.Equal(1, batch!.Length);

                if (enableComplexDatatypeSupport)
                {
                    var map = Assert.IsType<MapType>(field.DataType);
                    Assert.IsType<StringType>(map.KeyField.DataType);
                    Assert.IsType<Int32Type>(map.ValueField.DataType);
                }
                else
                {
                    Assert.IsType<StringType>(field.DataType);
                    Assert.Equal("{\"a\":1,\"b\":2}", ((StringArray)batch.Column(0)).GetString(0));
                }
            }
        }

        [SkippableTheory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Struct_TwoFields(bool enableComplexDatatypeSupport)
        {
            var (conn, stream) = await ExecuteAsync(
                "SELECT STRUCT(CAST(1 AS INT) AS id, CAST('alice' AS STRING) AS name)",
                enableComplexDatatypeSupport);
            using (conn)
            using (stream)
            {
                Field field = stream.Schema.GetFieldByIndex(0);
                RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
                Assert.Equal(1, batch!.Length);

                if (enableComplexDatatypeSupport)
                {
                    var structType = Assert.IsType<StructType>(field.DataType);
                    Assert.Equal(2, structType.Fields.Count);
                    Assert.Equal("id", structType.Fields[0].Name);
                    Assert.IsType<Int32Type>(structType.Fields[0].DataType);
                    Assert.Equal("name", structType.Fields[1].Name);
                    Assert.IsType<StringType>(structType.Fields[1].DataType);
                }
                else
                {
                    Assert.IsType<StringType>(field.DataType);
                    Assert.Equal("{\"id\":1,\"name\":\"alice\"}", ((StringArray)batch.Column(0)).GetString(0));
                }
            }
        }

        [SkippableTheory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Nested_ArrayOfStruct(bool enableComplexDatatypeSupport)
        {
            // ARRAY of STRUCT<id:INT, tags:ARRAY<STRING>>
            var (conn, stream) = await ExecuteAsync(
                "SELECT ARRAY(STRUCT(CAST(1 AS INT) AS id, ARRAY(CAST('a' AS STRING), CAST('b' AS STRING)) AS tags))",
                enableComplexDatatypeSupport);
            using (conn)
            using (stream)
            {
                Field field = stream.Schema.GetFieldByIndex(0);
                RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
                Assert.Equal(1, batch!.Length);

                if (enableComplexDatatypeSupport)
                {
                    var outer = Assert.IsType<ListType>(field.DataType);
                    var st = Assert.IsType<StructType>(outer.ValueDataType);
                    Assert.Equal("id", st.Fields[0].Name);
                    Assert.IsType<Int32Type>(st.Fields[0].DataType);
                    Assert.Equal("tags", st.Fields[1].Name);
                    var innerList = Assert.IsType<ListType>(st.Fields[1].DataType);
                    Assert.IsType<StringType>(innerList.ValueDataType);
                }
                else
                {
                    Assert.IsType<StringType>(field.DataType);
                    Assert.Equal("[{\"id\":1,\"tags\":[\"a\",\"b\"]}]", ((StringArray)batch.Column(0)).GetString(0));
                }
            }
        }
    }
}
