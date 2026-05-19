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

using AdbcDrivers.Databricks;
using Apache.Arrow.Types;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    /// <summary>
    /// Unit tests for <see cref="ArrowTypeParser.ParseComplexType"/>.
    /// Used by the SEA manifest-schema mapper when EnableComplexDatatypeSupport=true
    /// to convert Databricks SQL type strings to native Arrow nested types.
    /// </summary>
    public class ParseComplexTypeTests
    {
        [Fact]
        public void Array_OfPrimitive_ReturnsListType()
        {
            var t = ArrowTypeParser.ParseComplexType("ARRAY<INT>");
            Assert.IsType<Int32Type>(Assert.IsType<ListType>(t).ValueDataType);
        }

        [Fact]
        public void Map_ReturnsMapType_WithCorrectKeyAndValue()
        {
            var t = ArrowTypeParser.ParseComplexType("MAP<STRING,DOUBLE>");
            var m = Assert.IsType<MapType>(t);
            Assert.IsType<StringType>(m.KeyField.DataType);
            Assert.IsType<DoubleType>(m.ValueField.DataType);
        }

        [Fact]
        public void Struct_ReturnsStructType_WithFieldsInOrder()
        {
            var t = ArrowTypeParser.ParseComplexType("STRUCT<a:INT,b:STRING>");
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
            var t = ArrowTypeParser.ParseComplexType("ARRAY<STRUCT<id:INT,attrs:MAP<STRING,DOUBLE>>>");
            var list = Assert.IsType<ListType>(t);
            var st = Assert.IsType<StructType>(list.ValueDataType);
            Assert.IsType<Int32Type>(st.Fields[0].DataType);
            Assert.IsType<MapType>(st.Fields[1].DataType);
        }

        [Fact]
        public void Whitespace_IsTolerated()
        {
            var t = ArrowTypeParser.ParseComplexType("MAP< STRING , BIGINT >");
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
            Assert.IsType<StringType>(ArrowTypeParser.ParseComplexType(input));
        }

        [Fact]
        public void Struct_FieldsWithNotNullModifier_ParsedCorrectly()
        {
            // Some warehouses emit field nullability info in the type_text.
            var t = ArrowTypeParser.ParseComplexType("STRUCT<id:INT NOT NULL,name:STRING NOT NULL>");
            var s = Assert.IsType<StructType>(t);
            Assert.Equal(2, s.Fields.Count);
            Assert.Equal("id", s.Fields[0].Name);
            Assert.IsType<Int32Type>(s.Fields[0].DataType);
            Assert.Equal("name", s.Fields[1].Name);
            Assert.IsType<StringType>(s.Fields[1].DataType);
        }

        [Fact]
        public void Struct_FieldWithCommentModifier_ParsedCorrectly()
        {
            var t = ArrowTypeParser.ParseComplexType("STRUCT<id:INT COMMENT 'pk',name:STRING>");
            var s = Assert.IsType<StructType>(t);
            Assert.IsType<Int32Type>(s.Fields[0].DataType);
            Assert.IsType<StringType>(s.Fields[1].DataType);
        }

        [Fact]
        public void Nested_StructInArray_WithNotNullModifier()
        {
            // Reproduces the CI failure for Nested_ArrayOfStruct(enableComplexDatatypeSupport: True).
            var t = ArrowTypeParser.ParseComplexType(
                "ARRAY<STRUCT<id:INT NOT NULL,tags:ARRAY<STRING> NOT NULL>>");
            var list = Assert.IsType<ListType>(t);
            var st = Assert.IsType<StructType>(list.ValueDataType);
            Assert.IsType<Int32Type>(st.Fields[0].DataType);
            var inner = Assert.IsType<ListType>(st.Fields[1].DataType);
            Assert.IsType<StringType>(inner.ValueDataType);
        }

        [Fact]
        public void Struct_NestedDecimalWithModifier_PreservesParens()
        {
            // DECIMAL(p,s) inside a struct field, followed by NOT NULL — modifier stripping
            // must not eat the parens.
            var t = ArrowTypeParser.ParseComplexType("STRUCT<amt:DECIMAL(10,2) NOT NULL>");
            var s = Assert.IsType<StructType>(t);
            Assert.IsType<Decimal128Type>(s.Fields[0].DataType);
        }

        [Fact]
        public void Struct_FieldWithCollateModifier_ParsedCorrectly()
        {
            // Per Databricks STRUCT grammar: fieldType [NOT NULL] [COLLATE collationName] [COMMENT str]
            var t = ArrowTypeParser.ParseComplexType("STRUCT<name:STRING COLLATE utf8_binary>");
            var s = Assert.IsType<StructType>(t);
            Assert.Equal("name", s.Fields[0].Name);
            Assert.IsType<StringType>(s.Fields[0].DataType);
        }

        [Fact]
        public void Struct_FieldWithAllModifiers_ParsedCorrectly()
        {
            // NOT NULL, COLLATE, and COMMENT in order — all three must strip.
            var t = ArrowTypeParser.ParseComplexType(
                "STRUCT<name:STRING NOT NULL COLLATE utf8_binary COMMENT 'user name'>");
            var s = Assert.IsType<StructType>(t);
            Assert.IsType<StringType>(s.Fields[0].DataType);
        }
    }
}
