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
using Xunit;
using static AdbcDrivers.HiveServer2.Hive2.HiveServer2Connection;

namespace AdbcDrivers.Databricks.Tests
{
    public class ColumnMetadataHelperTests
    {
        [Theory]
        [InlineData("BOOLEAN", (short)ColumnTypeId.BOOLEAN)]
        [InlineData("TINYINT", (short)ColumnTypeId.TINYINT)]
        [InlineData("BYTE", (short)ColumnTypeId.TINYINT)]
        [InlineData("SMALLINT", (short)ColumnTypeId.SMALLINT)]
        [InlineData("SHORT", (short)ColumnTypeId.SMALLINT)]
        [InlineData("INT", (short)ColumnTypeId.INTEGER)]
        [InlineData("INTEGER", (short)ColumnTypeId.INTEGER)]
        [InlineData("BIGINT", (short)ColumnTypeId.BIGINT)]
        [InlineData("LONG", (short)ColumnTypeId.BIGINT)]
        [InlineData("FLOAT", (short)ColumnTypeId.FLOAT)]
        [InlineData("DOUBLE", (short)ColumnTypeId.DOUBLE)]
        [InlineData("DECIMAL", (short)ColumnTypeId.DECIMAL)]
        [InlineData("DECIMAL(10,2)", (short)ColumnTypeId.DECIMAL)]
        [InlineData("DEC", (short)ColumnTypeId.DECIMAL)]
        [InlineData("NUMERIC", (short)ColumnTypeId.DECIMAL)] // SqlTypeNameParser normalizes NUMERIC → DECIMAL
        [InlineData("STRING", (short)ColumnTypeId.VARCHAR)]
        [InlineData("VARCHAR", (short)ColumnTypeId.VARCHAR)]
        [InlineData("VARCHAR(255)", (short)ColumnTypeId.VARCHAR)]
        [InlineData("CHAR", (short)ColumnTypeId.CHAR)]
        [InlineData("BINARY", (short)ColumnTypeId.BINARY)]
        [InlineData("DATE", (short)ColumnTypeId.DATE)]
        [InlineData("TIMESTAMP", (short)ColumnTypeId.TIMESTAMP)]
        [InlineData("TIMESTAMP_NTZ", (short)ColumnTypeId.TIMESTAMP)]
        [InlineData("TIMESTAMP_LTZ", (short)ColumnTypeId.TIMESTAMP)]
        [InlineData("ARRAY<INT>", (short)ColumnTypeId.ARRAY)]
        [InlineData("MAP<STRING,INT>", (short)ColumnTypeId.JAVA_OBJECT)]
        [InlineData("STRUCT<F1:INT>", (short)ColumnTypeId.STRUCT)]
        [InlineData("VOID", (short)ColumnTypeId.NULL)]
        [InlineData("INTERVAL YEAR TO MONTH", (short)ColumnTypeId.OTHER)]
        [InlineData("UNKNOWN_TYPE", (short)ColumnTypeId.OTHER)]
        public void GetDataTypeCode_ReturnsCorrectCode(string typeName, short expectedCode)
        {
            Assert.Equal(expectedCode, ColumnMetadataHelper.GetDataTypeCode(typeName));
        }

        [Theory]
        [InlineData("DECIMAL(10,2)", "DECIMAL")]
        [InlineData("VARCHAR(255)", "VARCHAR")]
        [InlineData("ARRAY<INT>", "ARRAY")]
        [InlineData("MAP<STRING,INT>", "MAP")]
        [InlineData("STRUCT<F1:INT>", "STRUCT")]
        [InlineData("INT", "INTEGER")]
        [InlineData("INTEGER", "INTEGER")]
        [InlineData("DEC", "DECIMAL")]
        [InlineData("TIMESTAMP_NTZ", "TIMESTAMP")]
        [InlineData("TIMESTAMP_LTZ", "TIMESTAMP")]
        [InlineData("BYTE", "TINYINT")]
        [InlineData("SHORT", "SMALLINT")]
        [InlineData("LONG", "BIGINT")]
        [InlineData("STRING", "STRING")]
        [InlineData("BOOLEAN", "BOOLEAN")]
        [InlineData("DOUBLE", "DOUBLE")]
        public void GetBaseTypeName_ReturnsCorrectName(string typeName, string expectedBase)
        {
            Assert.Equal(expectedBase, ColumnMetadataHelper.GetBaseTypeName(typeName));
        }

        [Theory]
        [InlineData("BOOLEAN", 1)]
        [InlineData("TINYINT", 1)]
        [InlineData("SMALLINT", 2)]
        [InlineData("INT", 4)]
        [InlineData("INTEGER", 4)]
        [InlineData("BIGINT", 8)]
        [InlineData("FLOAT", 4)]
        [InlineData("DOUBLE", 8)]
        [InlineData("TIMESTAMP", 8)]
        [InlineData("DATE", 4)]
        [InlineData("DECIMAL(10,2)", 10)]
        [InlineData("DECIMAL", 10)]
        [InlineData("VARCHAR(255)", 255)]
        [InlineData("STRING", int.MaxValue)]
        [InlineData("CHAR(50)", 50)]
        public void GetColumnSizeDefault_ReturnsCorrectSize(string typeName, int expectedSize)
        {
            Assert.Equal(expectedSize, ColumnMetadataHelper.GetColumnSizeDefault(typeName));
        }

        [Theory]
        [InlineData("DECIMAL(10,2)", 2)]
        [InlineData("DECIMAL", 0)]
        [InlineData("FLOAT", 7)]
        [InlineData("DOUBLE", 15)]
        [InlineData("TIMESTAMP", 6)]
        [InlineData("INT", 0)]
        [InlineData("STRING", 0)]
        [InlineData("BOOLEAN", 0)]
        public void GetDecimalDigitsDefault_ReturnsCorrectDigits(string typeName, int expectedDigits)
        {
            Assert.Equal(expectedDigits, ColumnMetadataHelper.GetDecimalDigitsDefault(typeName));
        }

        [Theory]
        [InlineData("BOOLEAN", 1)]
        [InlineData("TINYINT", 1)]
        [InlineData("SMALLINT", 2)]
        [InlineData("INT", 4)]
        [InlineData("BIGINT", 8)]
        [InlineData("FLOAT", 4)]
        [InlineData("DOUBLE", 8)]
        [InlineData("DECIMAL(10,2)", 11)]
        public void GetBufferLength_ReturnsCorrectLength(string typeName, int expectedLength)
        {
            Assert.Equal(expectedLength, ColumnMetadataHelper.GetBufferLength(typeName));
        }

        [Fact]
        public void GetBufferLength_ReturnsNullForNonNumeric()
        {
            Assert.Null(ColumnMetadataHelper.GetBufferLength("STRING"));
            Assert.Null(ColumnMetadataHelper.GetBufferLength("ARRAY<INT>"));
        }

        [Theory]
        [InlineData("INT", (short)10)]
        [InlineData("DECIMAL", (short)10)]
        [InlineData("FLOAT", (short)10)]
        [InlineData("DOUBLE", (short)10)]
        [InlineData("BIGINT", (short)10)]
        public void GetNumPrecRadix_Returns10ForNumeric(string typeName, short expected)
        {
            Assert.Equal(expected, ColumnMetadataHelper.GetNumPrecRadix(typeName));
        }

        [Theory]
        [InlineData("STRING")]
        [InlineData("BOOLEAN")]
        [InlineData("DATE")]
        [InlineData("TIMESTAMP")]
        [InlineData("ARRAY<INT>")]
        public void GetNumPrecRadix_ReturnsNullForNonNumeric(string typeName)
        {
            Assert.Null(ColumnMetadataHelper.GetNumPrecRadix(typeName));
        }

        [Theory]
        [InlineData("STRING", int.MaxValue)]
        [InlineData("VARCHAR(255)", 255)]
        [InlineData("CHAR(50)", 50)]
        public void GetCharOctetLength_ReturnsForCharTypes(string typeName, int expectedLength)
        {
            Assert.Equal(expectedLength, ColumnMetadataHelper.GetCharOctetLength(typeName));
        }

        [Theory]
        [InlineData("INT")]
        [InlineData("DECIMAL(10,2)")]
        [InlineData("BOOLEAN")]
        public void GetCharOctetLength_ReturnsNullForNonCharTypes(string typeName)
        {
            Assert.Null(ColumnMetadataHelper.GetCharOctetLength(typeName));
        }

        [Theory]
        [InlineData("INTERVAL YEAR TO MONTH", 4)]
        [InlineData("INTERVAL DAY TO SECOND", 8)]
        [InlineData("INTERVAL HOUR TO SECOND", 8)]
        public void GetColumnSizeDefault_HandlesIntervalTypes(string typeName, int expectedSize)
        {
            Assert.Equal(expectedSize, ColumnMetadataHelper.GetColumnSizeDefault(typeName));
        }

        [Theory]
        [InlineData("  DECIMAL(10,2)  ", (short)ColumnTypeId.DECIMAL)]
        [InlineData("decimal", (short)ColumnTypeId.DECIMAL)]
        [InlineData("  int  ", (short)ColumnTypeId.INTEGER)]
        public void GetDataTypeCode_HandlesTrimAndCase(string typeName, short expectedCode)
        {
            Assert.Equal(expectedCode, ColumnMetadataHelper.GetDataTypeCode(typeName));
        }
    }
}
