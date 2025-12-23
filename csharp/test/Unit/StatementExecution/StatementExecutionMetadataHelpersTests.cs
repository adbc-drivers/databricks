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
using System.Reflection;
using AdbcDrivers.Databricks.StatementExecution;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Types;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    /// <summary>
    /// Unit tests for Statement Execution API metadata helper methods.
    /// Uses reflection to test private helper methods.
    /// </summary>
    public class StatementExecutionMetadataHelpersTests
    {
        private readonly Type _connectionType = typeof(StatementExecutionConnection);

        #region Helper Method Accessors

        private object InvokePrivateMethod(object instance, string methodName, params object[] parameters)
        {
            var method = _connectionType.GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(method);
            return method!.Invoke(instance, parameters);
        }

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

        #endregion

        #region QuoteIdentifier Tests

        [Fact]
        public void QuoteIdentifier_SimpleIdentifier_ReturnsQuoted()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "QuoteIdentifier", "my_table");

            Assert.Equal("`my_table`", result);
        }

        [Fact]
        public void QuoteIdentifier_WithBacktick_EscapesBacktick()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "QuoteIdentifier", "my`table");

            Assert.Equal("`my``table`", result);
        }

        [Fact]
        public void QuoteIdentifier_WithMultipleBackticks_EscapesAll()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "QuoteIdentifier", "my`complex`table");

            Assert.Equal("`my``complex``table`", result);
        }

        [Fact]
        public void QuoteIdentifier_WithSpecialCharacters_PreservesCharacters()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "QuoteIdentifier", "my-table.name");

            Assert.Equal("`my-table.name`", result);
        }

        [Fact]
        public void QuoteIdentifier_EmptyString_ReturnsEmptyQuoted()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "QuoteIdentifier", "");

            Assert.Equal("``", result);
        }

        #endregion

        #region EscapeSqlPattern Tests

        [Fact]
        public void EscapeSqlPattern_SimplePattern_ReturnsUnchanged()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "EscapeSqlPattern", "main%");

            Assert.Equal("main%", result);
        }

        [Fact]
        public void EscapeSqlPattern_WithSingleQuote_EscapesQuote()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "EscapeSqlPattern", "o'reilly");

            Assert.Equal("o''reilly", result);
        }

        [Fact]
        public void EscapeSqlPattern_WithMultipleQuotes_EscapesAll()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "EscapeSqlPattern", "it's john's");

            Assert.Equal("it''s john''s", result);
        }

        [Fact]
        public void EscapeSqlPattern_WithWildcards_PreservesWildcards()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "EscapeSqlPattern", "main_%");

            Assert.Equal("main_%", result);
        }

        #endregion

        #region BuildQualifiedTableName Tests

        [Fact]
        public void BuildQualifiedTableName_FullyQualified_ReturnsThreePart()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "BuildQualifiedTableName", "main", "default", "table1");

            Assert.Equal("`main`.`default`.`table1`", result);
        }

        [Fact]
        public void BuildQualifiedTableName_NoCatalog_ReturnsTwoPart()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "BuildQualifiedTableName", null, "default", "table1");

            Assert.Equal("`default`.`table1`", result);
        }

        [Fact]
        public void BuildQualifiedTableName_NoSchema_ReturnsTwoPart()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "BuildQualifiedTableName", "main", null, "table1");

            Assert.Equal("`main`.`table1`", result);
        }

        [Fact]
        public void BuildQualifiedTableName_OnlyTable_ReturnsOnePart()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "BuildQualifiedTableName", null, null, "table1");

            Assert.Equal("`table1`", result);
        }

        [Fact]
        public void BuildQualifiedTableName_WithBackticks_EscapesBackticks()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "BuildQualifiedTableName", "my`catalog", "my`schema", "my`table");

            Assert.Equal("`my``catalog`.`my``schema`.`my``table`", result);
        }

        #endregion

        #region PatternMatches Tests

        [Fact]
        public void PatternMatches_ExactMatch_ReturnsTrue()
        {
            var connection = CreateTestConnection();
            var result = (bool)InvokePrivateMethod(connection, "PatternMatches", "main", "main");

            Assert.True(result);
        }

        [Fact]
        public void PatternMatches_PercentWildcard_MatchesPrefix()
        {
            var connection = CreateTestConnection();
            var result = (bool)InvokePrivateMethod(connection, "PatternMatches", "main_catalog", "main%");

            Assert.True(result);
        }

        [Fact]
        public void PatternMatches_PercentWildcard_MatchesSuffix()
        {
            var connection = CreateTestConnection();
            var result = (bool)InvokePrivateMethod(connection, "PatternMatches", "test_main", "%main");

            Assert.True(result);
        }

        [Fact]
        public void PatternMatches_PercentWildcard_MatchesMiddle()
        {
            var connection = CreateTestConnection();
            var result = (bool)InvokePrivateMethod(connection, "PatternMatches", "my_test_catalog", "my%catalog");

            Assert.True(result);
        }

        [Fact]
        public void PatternMatches_UnderscoreWildcard_MatchesSingleChar()
        {
            var connection = CreateTestConnection();
            var result = (bool)InvokePrivateMethod(connection, "PatternMatches", "main1", "main_");

            Assert.True(result);
        }

        [Fact]
        public void PatternMatches_UnderscoreWildcard_DoesNotMatchMultipleChars()
        {
            var connection = CreateTestConnection();
            var result = (bool)InvokePrivateMethod(connection, "PatternMatches", "main12", "main_");

            Assert.False(result);
        }

        [Fact]
        public void PatternMatches_CombinedWildcards_MatchesPattern()
        {
            var connection = CreateTestConnection();
            var result = (bool)InvokePrivateMethod(connection, "PatternMatches", "test_main_catalog", "test_main%");

            Assert.True(result);
        }

        [Fact]
        public void PatternMatches_NoMatch_ReturnsFalse()
        {
            var connection = CreateTestConnection();
            var result = (bool)InvokePrivateMethod(connection, "PatternMatches", "other", "main%");

            Assert.False(result);
        }

        [Fact]
        public void PatternMatches_CaseInsensitive_ReturnsTrue()
        {
            var connection = CreateTestConnection();
            var result = (bool)InvokePrivateMethod(connection, "PatternMatches", "MAIN", "main");

            Assert.True(result);
        }

        #endregion

        #region ConvertDatabricksTypeToArrow Tests

        [Fact]
        public void ConvertDatabricksTypeToArrow_Boolean_ReturnsBooleanType()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "BOOLEAN");

            Assert.IsType<BooleanType>(result);
        }

        [Fact]
        public void ConvertDatabricksTypeToArrow_TinyInt_ReturnsInt8Type()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "TINYINT");

            Assert.IsType<Int8Type>(result);
        }

        [Fact]
        public void ConvertDatabricksTypeToArrow_SmallInt_ReturnsInt16Type()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "SMALLINT");

            Assert.IsType<Int16Type>(result);
        }

        [Fact]
        public void ConvertDatabricksTypeToArrow_Int_ReturnsInt32Type()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "INT");

            Assert.IsType<Int32Type>(result);
        }

        [Fact]
        public void ConvertDatabricksTypeToArrow_Integer_ReturnsInt32Type()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "INTEGER");

            Assert.IsType<Int32Type>(result);
        }

        [Fact]
        public void ConvertDatabricksTypeToArrow_BigInt_ReturnsInt64Type()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "BIGINT");

            Assert.IsType<Int64Type>(result);
        }

        [Fact]
        public void ConvertDatabricksTypeToArrow_Float_ReturnsFloatType()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "FLOAT");

            Assert.IsType<FloatType>(result);
        }

        [Fact]
        public void ConvertDatabricksTypeToArrow_Double_ReturnsDoubleType()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "DOUBLE");

            Assert.IsType<DoubleType>(result);
        }

        [Fact]
        public void ConvertDatabricksTypeToArrow_Decimal_ReturnsDecimal128Type()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "DECIMAL");

            Assert.IsType<Decimal128Type>(result);
        }

        [Fact]
        public void ConvertDatabricksTypeToArrow_String_ReturnsStringType()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "STRING");

            Assert.IsType<StringType>(result);
        }

        [Fact]
        public void ConvertDatabricksTypeToArrow_VarChar_ReturnsStringType()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "VARCHAR");

            Assert.IsType<StringType>(result);
        }

        [Fact]
        public void ConvertDatabricksTypeToArrow_Binary_ReturnsBinaryType()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "BINARY");

            Assert.IsType<BinaryType>(result);
        }

        [Fact]
        public void ConvertDatabricksTypeToArrow_Date_ReturnsDate32Type()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "DATE");

            Assert.IsType<Date32Type>(result);
        }

        [Fact]
        public void ConvertDatabricksTypeToArrow_Timestamp_ReturnsTimestampType()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "TIMESTAMP");

            Assert.IsType<TimestampType>(result);
        }

        [Fact]
        public void ConvertDatabricksTypeToArrow_ComplexType_ReturnsStringType()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "DECIMAL(10,2)");

            // ExtractBaseType should extract "DECIMAL" and return Decimal128Type
            Assert.IsType<Decimal128Type>(result);
        }

        [Fact]
        public void ConvertDatabricksTypeToArrow_UnknownType_ReturnsStringType()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "UNKNOWN_TYPE");

            Assert.IsType<StringType>(result);
        }

        [Fact]
        public void ConvertDatabricksTypeToArrow_CaseInsensitive_Works()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "string");

            Assert.IsType<StringType>(result);
        }

        #endregion

        #region ExtractBaseType Tests

        [Fact]
        public void ExtractBaseType_SimpleType_ReturnsType()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "ExtractBaseType", "INT");

            Assert.Equal("INT", result);
        }

        [Fact]
        public void ExtractBaseType_DecimalWithPrecision_ReturnsDecimal()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "ExtractBaseType", "DECIMAL(10,2)");

            Assert.Equal("DECIMAL", result);
        }

        [Fact]
        public void ExtractBaseType_VarCharWithLength_ReturnsVarChar()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "ExtractBaseType", "VARCHAR(255)");

            Assert.Equal("VARCHAR", result);
        }

        [Fact]
        public void ExtractBaseType_ArrayType_ReturnsArray()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "ExtractBaseType", "ARRAY<INT>");

            Assert.Equal("ARRAY", result);
        }

        [Fact]
        public void ExtractBaseType_MapType_ReturnsMap()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "ExtractBaseType", "MAP<STRING,INT>");

            Assert.Equal("MAP", result);
        }

        [Fact]
        public void ExtractBaseType_StructType_ReturnsStruct()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "ExtractBaseType", "STRUCT<name:STRING,age:INT>");

            Assert.Equal("STRUCT", result);
        }

        [Fact]
        public void ExtractBaseType_EmptyString_ReturnsEmpty()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "ExtractBaseType", "");

            Assert.Equal("", result);
        }

        [Fact]
        public void ExtractBaseType_TypeWithUnderscore_ReturnsFullType()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "ExtractBaseType", "TIMESTAMP_NTZ");

            Assert.Equal("TIMESTAMP_NTZ", result);
        }

        #endregion

        #region Disposal

        // Test cleanup - ensure connection is properly disposed
        [Fact]
        public void Connection_Dispose_DoesNotThrow()
        {
            var connection = CreateTestConnection();
            var exception = Record.Exception(() => connection.Dispose());

            Assert.Null(exception);
        }

        #endregion
    }
}
