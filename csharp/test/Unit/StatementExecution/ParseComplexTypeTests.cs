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

using AdbcDrivers.Databricks.StatementExecution;
using Apache.Arrow.Types;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    /// <summary>
    /// Unit tests for <see cref="StatementExecutionStatement.ParseComplexType"/>.
    /// Used by the SEA manifest-schema mapper when EnableComplexDatatypeSupport=true
    /// to convert Databricks SQL type strings to native Arrow nested types.
    /// </summary>
    public class ParseComplexTypeTests
    {
        [Fact]
        public void Array_OfPrimitive_ReturnsListType()
        {
            var t = StatementExecutionStatement.ParseComplexType("ARRAY<INT>");
            Assert.IsType<Int32Type>(Assert.IsType<ListType>(t).ValueDataType);
        }

        [Fact]
        public void Map_ReturnsMapType_WithCorrectKeyAndValue()
        {
            var t = StatementExecutionStatement.ParseComplexType("MAP<STRING,DOUBLE>");
            var m = Assert.IsType<MapType>(t);
            Assert.IsType<StringType>(m.KeyField.DataType);
            Assert.IsType<DoubleType>(m.ValueField.DataType);
        }

        [Fact]
        public void Struct_ReturnsStructType_WithFieldsInOrder()
        {
            var t = StatementExecutionStatement.ParseComplexType("STRUCT<a:INT,b:STRING>");
            var s = Assert.IsType<StructType>(t);
            Assert.Equal(2, s.Fields.Count);
            Assert.Equal("a", s.Fields[0].Name);
            Assert.IsType<Int32Type>(s.Fields[0].DataType);
            Assert.Equal("b", s.Fields[1].Name);
            Assert.IsType<StringType>(s.Fields[1].DataType);
        }

        [Fact]
        public void Nested_DeeplyNested_Resolves()
        {
            // ARRAY<STRUCT<id:INT, attrs:MAP<STRING,DOUBLE>>>
            var t = StatementExecutionStatement.ParseComplexType("ARRAY<STRUCT<id:INT,attrs:MAP<STRING,DOUBLE>>>");
            var list = Assert.IsType<ListType>(t);
            var st = Assert.IsType<StructType>(list.ValueDataType);
            Assert.IsType<Int32Type>(st.Fields[0].DataType);
            Assert.IsType<MapType>(st.Fields[1].DataType);
        }

        [Fact]
        public void Whitespace_IsTolerated()
        {
            var t = StatementExecutionStatement.ParseComplexType("MAP< STRING , BIGINT >");
            var m = Assert.IsType<MapType>(t);
            Assert.IsType<StringType>(m.KeyField.DataType);
            Assert.IsType<Int64Type>(m.ValueField.DataType);
        }

        [Theory]
        [InlineData("")]
        [InlineData("ARRAY<INT")]   // unclosed
        [InlineData("ARRAY<INT>>")]  // trailing junk
        [InlineData("garbage")]
        public void ParseFailure_FallsBackToString(string input)
        {
            Assert.IsType<StringType>(StatementExecutionStatement.ParseComplexType(input));
        }
    }
}
