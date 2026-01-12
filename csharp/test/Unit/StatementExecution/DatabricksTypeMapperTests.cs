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
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    /// <summary>
    /// Unit tests for DatabricksTypeMapper used by Statement Execution API.
    /// Tests type mapping, column size calculation, and metadata field generation.
    /// </summary>
    public class DatabricksTypeMapperTests
    {
        #region DatabricksTypeMapper Tests - XDBC Type Codes

        [Theory]
        [InlineData("BOOLEAN", (short)16)]
        [InlineData("TINYINT", (short)-6)]
        [InlineData("SMALLINT", (short)5)]
        [InlineData("INT", (short)4)]
        [InlineData("INTEGER", (short)4)]
        [InlineData("BIGINT", (short)-5)]
        [InlineData("FLOAT", (short)6)]
        [InlineData("DOUBLE", (short)8)]
        [InlineData("STRING", (short)12)]
        [InlineData("VARCHAR", (short)12)]
        [InlineData("DATE", (short)91)]
        [InlineData("TIMESTAMP", (short)93)]
        public void DatabricksTypeMapper_GetXdbcDataType_MapsCorrectly(string databricksType, short expectedXdbcType)
        {
            var xdbcType = DatabricksTypeMapper.GetXdbcDataType(databricksType);
            Assert.Equal(expectedXdbcType, xdbcType);
        }

        [Fact]
        public void DatabricksTypeMapper_GetXdbcDataType_WithParameterizedType_StripsParameters()
        {
            var xdbcType = DatabricksTypeMapper.GetXdbcDataType("VARCHAR(100)");
            Assert.Equal((short)12, xdbcType);
        }

        [Fact]
        public void DatabricksTypeMapper_GetXdbcDataType_WithComplexType_ReturnsCorrectCode()
        {
            var xdbcType = DatabricksTypeMapper.GetXdbcDataType("ARRAY<STRING>");
            Assert.Equal((short)2003, xdbcType);
        }

        [Fact]
        public void DatabricksTypeMapper_GetXdbcDataType_WithUnknownType_ReturnsNull()
        {
            var xdbcType = DatabricksTypeMapper.GetXdbcDataType("UNKNOWN_TYPE");
            Assert.Null(xdbcType);
        }

        #endregion

        #region DatabricksTypeMapper Tests - Base Type Name Extraction

        [Theory]
        [InlineData("VARCHAR(100)", "VARCHAR")]
        [InlineData("DECIMAL(10,2)", "DECIMAL")]
        [InlineData("ARRAY<STRING>", "ARRAY")]
        [InlineData("MAP<STRING,INT>", "MAP")]
        [InlineData("STRUCT<a:STRING>", "STRUCT")]
        [InlineData("INT", "INTEGER")]
        [InlineData("TIMESTAMP_NTZ", "TIMESTAMP")]
        public void DatabricksTypeMapper_StripBaseTypeName_ExtractsBaseType(string fullType, string expectedBaseType)
        {
            var baseType = DatabricksTypeMapper.StripBaseTypeName(fullType);
            Assert.Equal(expectedBaseType, baseType);
        }

        [Fact]
        public void DatabricksTypeMapper_StripBaseTypeName_WithEmptyString_ReturnsEmpty()
        {
            var baseType = DatabricksTypeMapper.StripBaseTypeName("");
            Assert.Equal(string.Empty, baseType);
        }

        [Fact]
        public void DatabricksTypeMapper_StripBaseTypeName_WithNull_ReturnsEmpty()
        {
            var baseType = DatabricksTypeMapper.StripBaseTypeName(null);
            Assert.Equal(string.Empty, baseType);
        }

        #endregion

        #region DatabricksTypeMapper Tests - Column Size

        [Theory]
        [InlineData("VARCHAR(100)", 100)]
        [InlineData("CHAR(50)", 50)]
        [InlineData("DECIMAL(10,2)", 10)]
        [InlineData("STRING", 2147483647)]
        [InlineData("TINYINT", 1)]
        [InlineData("INT", 4)]
        [InlineData("BIGINT", 8)]
        public void DatabricksTypeMapper_GetColumnSize_ReturnsCorrectSize(string databricksType, int expectedSize)
        {
            var columnSize = DatabricksTypeMapper.GetColumnSize(databricksType);
            Assert.Equal(expectedSize, columnSize);
        }

        [Fact]
        public void DatabricksTypeMapper_GetColumnSize_WithUnknownType_ReturnsMaxInt()
        {
            var columnSize = DatabricksTypeMapper.GetColumnSize("UNKNOWN_TYPE");
            Assert.Equal(2147483647, columnSize);
        }

        #endregion

        #region DatabricksTypeMapper Tests - Decimal Digits

        [Theory]
        [InlineData("DECIMAL(10,2)", 2)]
        [InlineData("DECIMAL(18,4)", 4)]
        [InlineData("DECIMAL(10)", 0)]
        [InlineData("FLOAT", 7)]
        [InlineData("DOUBLE", 15)]
        [InlineData("INT", 0)]
        [InlineData("TIMESTAMP", 6)]
        public void DatabricksTypeMapper_GetDecimalDigits_ReturnsCorrectDigits(string databricksType, int expectedDigits)
        {
            var decimalDigits = DatabricksTypeMapper.GetDecimalDigits(databricksType);
            Assert.Equal(expectedDigits, decimalDigits);
        }

        [Fact]
        public void DatabricksTypeMapper_GetDecimalDigits_WithStringType_ReturnsZero()
        {
            var decimalDigits = DatabricksTypeMapper.GetDecimalDigits("STRING");
            Assert.Equal(0, decimalDigits);
        }

        #endregion

        #region DatabricksTypeMapper Tests - Numeric Precision Radix

        [Theory]
        [InlineData("INT", (short)10)]
        [InlineData("BIGINT", (short)10)]
        [InlineData("DECIMAL(10,2)", (short)10)]
        [InlineData("FLOAT", (short)10)]
        public void DatabricksTypeMapper_GetNumPrecRadix_ReturnsCorrectRadix(string databricksType, short expectedRadix)
        {
            var numPrecRadix = DatabricksTypeMapper.GetNumPrecRadix(databricksType);
            Assert.Equal(expectedRadix, numPrecRadix);
        }

        [Theory]
        [InlineData("STRING")]
        [InlineData("DATE")]
        [InlineData("BOOLEAN")]
        public void DatabricksTypeMapper_GetNumPrecRadix_WithNonNumericType_ReturnsNull(string databricksType)
        {
            var numPrecRadix = DatabricksTypeMapper.GetNumPrecRadix(databricksType);
            Assert.Null(numPrecRadix);
        }

        #endregion

        #region DatabricksTypeMapper Tests - Buffer Length

        [Theory]
        [InlineData("TINYINT", 1)]
        [InlineData("SMALLINT", 2)]
        [InlineData("INT", 4)]
        [InlineData("BIGINT", 8)]
        [InlineData("FLOAT", 4)]
        [InlineData("DOUBLE", 8)]
        public void DatabricksTypeMapper_GetBufferLength_ReturnsCorrectLength(string databricksType, int expectedLength)
        {
            var bufferLength = DatabricksTypeMapper.GetBufferLength(databricksType);
            Assert.Equal(expectedLength, bufferLength);
        }

        [Theory]
        [InlineData("STRING")]
        [InlineData("DATE")]
        [InlineData("ARRAY<INT>")]
        public void DatabricksTypeMapper_GetBufferLength_WithNonApplicableType_ReturnsNull(string databricksType)
        {
            var bufferLength = DatabricksTypeMapper.GetBufferLength(databricksType);
            Assert.Null(bufferLength);
        }

        [Fact]
        public void DatabricksTypeMapper_GetBufferLength_WithDecimal_CalculatesBasedOnPrecision()
        {
            var bufferLength = DatabricksTypeMapper.GetBufferLength("DECIMAL(38,10)");
            Assert.NotNull(bufferLength);
            Assert.True(bufferLength > 0);
        }

        #endregion

        #region DatabricksTypeMapper Tests - Char Octet Length

        [Theory]
        [InlineData("VARCHAR(100)", 100)]
        [InlineData("CHAR(50)", 50)]
        [InlineData("STRING", 2147483647)]
        public void DatabricksTypeMapper_GetCharOctetLength_ReturnsCorrectLength(string databricksType, int expectedLength)
        {
            var octetLength = DatabricksTypeMapper.GetCharOctetLength(databricksType);
            Assert.Equal(expectedLength, octetLength);
        }

        [Theory]
        [InlineData("INT")]
        [InlineData("DATE")]
        [InlineData("BOOLEAN")]
        public void DatabricksTypeMapper_GetCharOctetLength_WithNonCharType_ReturnsNull(string databricksType)
        {
            var octetLength = DatabricksTypeMapper.GetCharOctetLength(databricksType);
            Assert.Null(octetLength);
        }

        #endregion

        #region DatabricksTypeMapper Tests - SQL Data Type

        [Fact]
        public void DatabricksTypeMapper_GetSqlDataType_ReturnsSameAsXdbcDataType()
        {
            var xdbcType = DatabricksTypeMapper.GetXdbcDataType("INT");
            var sqlDataType = DatabricksTypeMapper.GetSqlDataType("INT");
            Assert.Equal(xdbcType, sqlDataType);
        }

        #endregion

        #region DatabricksTypeMapper Tests - SQL Datetime Sub

        [Theory]
        [InlineData("DATE")]
        [InlineData("TIMESTAMP")]
        [InlineData("TIMESTAMP_NTZ")]
        public void DatabricksTypeMapper_GetSqlDatetimeSub_ReturnsNull(string databricksType)
        {
            var datetimeSub = DatabricksTypeMapper.GetSqlDatetimeSub(databricksType);
            Assert.Null(datetimeSub);
        }

        #endregion
    }
}
