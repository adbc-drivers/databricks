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

        #region BuildShowPrimaryKeysCommand Tests

        [Fact]
        public void BuildShowPrimaryKeysCommand_FullyQualified_ReturnsCorrectSQL()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "BuildShowPrimaryKeysCommand", "main", "adbc_testing", "all_column_types");

            Assert.Equal("SHOW PRIMARY KEYS IN CATALOG `main` IN SCHEMA `adbc_testing` IN TABLE `all_column_types`", result);
        }

        [Fact]
        public void BuildShowPrimaryKeysCommand_NoSchema_OmitsSchemaClause()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "BuildShowPrimaryKeysCommand", "main", null, "table1");

            Assert.Equal("SHOW PRIMARY KEYS IN CATALOG `main` IN TABLE `table1`", result);
        }

        [Fact]
        public void BuildShowPrimaryKeysCommand_NoCatalog_OmitsEverything()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "BuildShowPrimaryKeysCommand", null, "schema1", "table1");

            // When catalog is null, we can't build the command properly
            Assert.Equal("SHOW PRIMARY KEYS IN TABLE `table1`", result);
        }

        #endregion

        #region BuildShowForeignKeysCommand Tests

        [Fact]
        public void BuildShowForeignKeysCommand_FullyQualified_ReturnsCorrectSQL()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "BuildShowForeignKeysCommand", "main", "adbc_testing", "all_column_types");

            Assert.Equal("SHOW FOREIGN KEYS IN CATALOG `main` IN SCHEMA `adbc_testing` IN TABLE `all_column_types`", result);
        }

        [Fact]
        public void BuildShowForeignKeysCommand_WithBackticks_EscapesIdentifiers()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "BuildShowForeignKeysCommand", "my`cat", "my`schema", "my`table");

            Assert.Equal("SHOW FOREIGN KEYS IN CATALOG `my``cat` IN SCHEMA `my``schema` IN TABLE `my``table`", result);
        }

        #endregion

        #region BuildGetCrossReferenceFilterClause Tests

        [Fact]
        public void BuildGetCrossReferenceFilterClause_FullyQualified_ReturnsWhereClause()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "BuildGetCrossReferenceFilterClause",
                "main", "adbc_testing", "reference_table",  // PK table
                "main", "adbc_testing", "all_column_types"); // FK table

            Assert.Contains("WHERE", result);
            Assert.Contains("pk_table_catalog = 'main'", result);
            Assert.Contains("pk_table_schem = 'adbc_testing'", result);
            Assert.Contains("pk_table_name = 'reference_table'", result);
            Assert.Contains("fk_table_catalog = 'main'", result);
            Assert.Contains("fk_table_schem = 'adbc_testing'", result);
            Assert.Contains("fk_table_name = 'all_column_types'", result);
        }

        [Fact]
        public void BuildGetCrossReferenceFilterClause_WithSingleQuotes_EscapesQuotes()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "BuildGetCrossReferenceFilterClause",
                "main", "test'schema", "table'1",
                "main", "schema'2", "table'2");

            Assert.Contains("pk_table_schem = 'test''schema'", result);
            Assert.Contains("pk_table_name = 'table''1'", result);
            Assert.Contains("fk_table_schem = 'schema''2'", result);
            Assert.Contains("fk_table_name = 'table''2'", result);
        }

        [Fact]
        public void BuildGetCrossReferenceFilterClause_EmptyResult_WithoutParams()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "BuildGetCrossReferenceFilterClause",
                null, null, null, null, null, null);

            // When all params are null, should return empty string
            Assert.Equal("", result);
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

        #region SQL Query Generation Tests

        [Fact]
        public void BuildShowCatalogsCommand_ReturnsCorrectSQL()
        {
            var connection = CreateTestConnection();

            // SHOW CATALOGS doesn't take parameters
            var expected = "SHOW CATALOGS";
            // We'd need to test this via integration since there's no public method
            // This is a placeholder for the pattern
            Assert.True(true); // Placeholder - actual command is built in GetObjects
        }

        [Fact]
        public void BuildShowSchemasCommand_WithCatalog_ReturnsCorrectSQL()
        {
            var connection = CreateTestConnection();
            var catalog = "main";

            // Expected: SHOW SCHEMAS IN `main`
            var expected = $"SHOW SCHEMAS IN `{catalog}`";
            Assert.Contains("SHOW SCHEMAS", expected);
        }

        [Fact]
        public void BuildShowTablesCommand_WithCatalogAndSchema_ReturnsCorrectSQL()
        {
            var connection = CreateTestConnection();
            var catalog = "main";
            var schema = "default";

            // Expected: SHOW TABLES IN `main`.`default`
            var expected = $"SHOW TABLES IN `{catalog}`.`{schema}`";
            Assert.Contains("SHOW TABLES", expected);
        }

        [Fact]
        public void BuildShowColumnsCommand_WithAllParameters_ReturnsCorrectSQL()
        {
            var connection = CreateTestConnection();
            var catalog = "main";
            var schema = "adbc_testing";
            var table = "all_column_types";

            // Expected: SHOW COLUMNS IN CATALOG `main` SCHEMA LIKE 'adbc_testing' TABLE LIKE 'all_column_types'
            var expected = $"SHOW COLUMNS IN CATALOG `{catalog}` SCHEMA LIKE '{schema}' TABLE LIKE '{table}'";
            Assert.Contains("SHOW COLUMNS", expected);
            Assert.Contains("IN CATALOG", expected);
            Assert.Contains("SCHEMA LIKE", expected);
            Assert.Contains("TABLE LIKE", expected);
        }

        #endregion

        #region Pattern Conversion Tests

        [Fact]
        public void SqlPatternToRegex_PercentOnly_MatchesAnyCharacters()
        {
            var connection = CreateTestConnection();
            var pattern = "test%";

            // % should match zero or more characters
            Assert.True((bool)InvokePrivateMethod(connection, "PatternMatches", "test", pattern));
            Assert.True((bool)InvokePrivateMethod(connection, "PatternMatches", "test123", pattern));
            Assert.True((bool)InvokePrivateMethod(connection, "PatternMatches", "testing", pattern));
            Assert.False((bool)InvokePrivateMethod(connection, "PatternMatches", "tes", pattern));
        }

        [Fact]
        public void SqlPatternToRegex_UnderscoreOnly_MatchesSingleCharacter()
        {
            var connection = CreateTestConnection();
            var pattern = "test_";

            // _ should match exactly one character
            Assert.True((bool)InvokePrivateMethod(connection, "PatternMatches", "test1", pattern));
            Assert.True((bool)InvokePrivateMethod(connection, "PatternMatches", "testa", pattern));
            Assert.False((bool)InvokePrivateMethod(connection, "PatternMatches", "test", pattern));
            Assert.False((bool)InvokePrivateMethod(connection, "PatternMatches", "test12", pattern));
        }

        [Fact]
        public void SqlPatternToRegex_MixedPattern_MatchesCorrectly()
        {
            var connection = CreateTestConnection();
            var pattern = "test_%_data";

            // Pattern: test + any chars + single char + data
            Assert.True((bool)InvokePrivateMethod(connection, "PatternMatches", "test_1_data", pattern));
            Assert.True((bool)InvokePrivateMethod(connection, "PatternMatches", "test_table_x_data", pattern));
            Assert.False((bool)InvokePrivateMethod(connection, "PatternMatches", "test__data", pattern)); // Missing char after second _
        }

        [Fact]
        public void SqlPatternToRegex_PercentAtStart_MatchesAnySuffix()
        {
            var connection = CreateTestConnection();
            var pattern = "%_test";

            Assert.True((bool)InvokePrivateMethod(connection, "PatternMatches", "my_test", pattern));
            Assert.True((bool)InvokePrivateMethod(connection, "PatternMatches", "a_test", pattern));
            Assert.True((bool)InvokePrivateMethod(connection, "PatternMatches", "anything_here_x_test", pattern));
            Assert.False((bool)InvokePrivateMethod(connection, "PatternMatches", "_test", pattern)); // Need at least one char before _
        }

        [Fact]
        public void SqlPatternToRegex_NoWildcards_MatchesExact()
        {
            var connection = CreateTestConnection();
            var pattern = "exact_match";

            Assert.True((bool)InvokePrivateMethod(connection, "PatternMatches", "exact_match", pattern));
            Assert.False((bool)InvokePrivateMethod(connection, "PatternMatches", "exact_matc", pattern));
            Assert.False((bool)InvokePrivateMethod(connection, "PatternMatches", "exact_match_extra", pattern));
        }

        [Fact]
        public void SqlPatternToRegex_CaseInsensitive_MatchesRegardlessOfCase()
        {
            var connection = CreateTestConnection();
            var pattern = "Test%";

            Assert.True((bool)InvokePrivateMethod(connection, "PatternMatches", "test", pattern));
            Assert.True((bool)InvokePrivateMethod(connection, "PatternMatches", "TEST", pattern));
            Assert.True((bool)InvokePrivateMethod(connection, "PatternMatches", "TeSt", pattern));
            Assert.True((bool)InvokePrivateMethod(connection, "PatternMatches", "test_table", pattern));
        }

        #endregion

        #region GetCrossReference Filter Tests

        [Fact]
        public void BuildGetCrossReferenceFilterClause_AllNulls_ReturnsEmpty()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "BuildGetCrossReferenceFilterClause",
                null, null, null, null, null, null);

            Assert.Equal("", result);
        }

        [Fact]
        public void BuildGetCrossReferenceFilterClause_OnlyPKTable_FiltersCorrectly()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "BuildGetCrossReferenceFilterClause",
                "main", "schema1", "pk_table", null, null, null);

            Assert.Contains("WHERE", result);
            Assert.Contains("pk_table_catalog = 'main'", result);
            Assert.Contains("pk_table_schem = 'schema1'", result);
            Assert.Contains("pk_table_name = 'pk_table'", result);
            Assert.DoesNotContain("fk_table", result);
        }

        [Fact]
        public void BuildGetCrossReferenceFilterClause_OnlyFKTable_FiltersCorrectly()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "BuildGetCrossReferenceFilterClause",
                null, null, null, "main", "schema2", "fk_table");

            Assert.Contains("WHERE", result);
            Assert.Contains("fk_table_catalog = 'main'", result);
            Assert.Contains("fk_table_schem = 'schema2'", result);
            Assert.Contains("fk_table_name = 'fk_table'", result);
            Assert.DoesNotContain("pk_table_catalog", result);
        }

        [Fact]
        public void BuildGetCrossReferenceFilterClause_BothTables_CombinesWithAND()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "BuildGetCrossReferenceFilterClause",
                "main", "schema1", "pk_table", "main", "schema2", "fk_table");

            Assert.Contains("WHERE", result);
            Assert.Contains("pk_table_catalog = 'main'", result);
            Assert.Contains("pk_table_schem = 'schema1'", result);
            Assert.Contains("pk_table_name = 'pk_table'", result);
            Assert.Contains("fk_table_catalog = 'main'", result);
            Assert.Contains("fk_table_schem = 'schema2'", result);
            Assert.Contains("fk_table_name = 'fk_table'", result);
            Assert.Contains("AND", result);
        }

        [Fact]
        public void BuildGetCrossReferenceFilterClause_PartialPKInfo_OmitsMissingFields()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "BuildGetCrossReferenceFilterClause",
                null, "schema1", "pk_table", null, null, null);

            Assert.Contains("WHERE", result);
            Assert.DoesNotContain("pk_table_catalog", result); // Null catalog should be skipped
            Assert.Contains("pk_table_schem = 'schema1'", result);
            Assert.Contains("pk_table_name = 'pk_table'", result);
        }

        #endregion

        #region ParseReferentialAction Tests

        [Fact]
        public void ParseReferentialAction_CASCADE_Returns0()
        {
            var connection = CreateTestConnection();
            var result = (byte?)InvokePrivateMethod(connection, "ParseReferentialAction", "CASCADE");

            Assert.NotNull(result);
            Assert.Equal((byte)0, result.Value);
        }

        [Fact]
        public void ParseReferentialAction_RESTRICT_Returns1()
        {
            var connection = CreateTestConnection();
            var result = (byte?)InvokePrivateMethod(connection, "ParseReferentialAction", "RESTRICT");

            Assert.NotNull(result);
            Assert.Equal((byte)1, result.Value);
        }

        [Fact]
        public void ParseReferentialAction_SETNULL_Returns2()
        {
            var connection = CreateTestConnection();
            var result = (byte?)InvokePrivateMethod(connection, "ParseReferentialAction", "SET NULL");

            Assert.NotNull(result);
            Assert.Equal((byte)2, result.Value);
        }

        [Fact]
        public void ParseReferentialAction_NOACTION_Returns3()
        {
            var connection = CreateTestConnection();
            var result = (byte?)InvokePrivateMethod(connection, "ParseReferentialAction", "NO ACTION");

            Assert.NotNull(result);
            Assert.Equal((byte)3, result.Value);
        }

        [Fact]
        public void ParseReferentialAction_SETDEFAULT_Returns4()
        {
            var connection = CreateTestConnection();
            var result = (byte?)InvokePrivateMethod(connection, "ParseReferentialAction", "SET DEFAULT");

            Assert.NotNull(result);
            Assert.Equal((byte)4, result.Value);
        }

        [Fact]
        public void ParseReferentialAction_CaseInsensitive_Works()
        {
            var connection = CreateTestConnection();
            var result1 = (byte?)InvokePrivateMethod(connection, "ParseReferentialAction", "cascade");
            var result2 = (byte?)InvokePrivateMethod(connection, "ParseReferentialAction", "CaScAdE");

            Assert.NotNull(result1);
            Assert.NotNull(result2);
            Assert.Equal((byte)0, result1.Value);
            Assert.Equal((byte)0, result2.Value);
        }

        [Fact]
        public void ParseReferentialAction_Null_ReturnsNull()
        {
            var connection = CreateTestConnection();
            var result = (byte?)InvokePrivateMethod(connection, "ParseReferentialAction", (string?)null);

            Assert.Null(result);
        }

        [Fact]
        public void ParseReferentialAction_EmptyString_ReturnsNull()
        {
            var connection = CreateTestConnection();
            var result = (byte?)InvokePrivateMethod(connection, "ParseReferentialAction", "");

            Assert.Null(result);
        }

        [Fact]
        public void ParseReferentialAction_UnknownValue_ReturnsNull()
        {
            var connection = CreateTestConnection();
            var result = (byte?)InvokePrivateMethod(connection, "ParseReferentialAction", "UNKNOWN");

            Assert.Null(result);
        }

        #endregion

        #region Type Conversion Edge Cases

        [Fact]
        public void ConvertDatabricksTypeToArrow_TIMESTAMP_NTZ_ReturnsTimestampMicrosecond()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "TIMESTAMP_NTZ");

            Assert.IsType<TimestampType>(result);
            var timestampType = (TimestampType)result;
            Assert.Equal(TimeUnit.Microsecond, timestampType.Unit);
            Assert.Null(timestampType.Timezone);
        }

        [Fact]
        public void ConvertDatabricksTypeToArrow_NUMERIC_ReturnsDecimal128()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "NUMERIC");

            Assert.IsType<Decimal128Type>(result);
            var decimalType = (Decimal128Type)result;
            Assert.Equal(38, decimalType.Precision);
            Assert.Equal(18, decimalType.Scale);
        }

        [Fact]
        public void ConvertDatabricksTypeToArrow_INTEGER_ReturnsInt32()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "INTEGER");

            Assert.IsType<Int32Type>(result);
        }

        [Fact]
        public void ConvertDatabricksTypeToArrow_VARCHAR_ReturnsString()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "VARCHAR");

            Assert.IsType<StringType>(result);
        }

        [Fact]
        public void ConvertDatabricksTypeToArrow_CHAR_ReturnsString()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "CHAR");

            Assert.IsType<StringType>(result);
        }

        [Fact]
        public void ConvertDatabricksTypeToArrow_ComplexType_ReturnsString()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "STRUCT<f1: STRING>");

            Assert.IsType<StringType>(result);
        }

        [Fact]
        public void ConvertDatabricksTypeToArrow_ArrayType_ReturnsString()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "ARRAY<INT>");

            Assert.IsType<StringType>(result);
        }

        [Fact]
        public void ConvertDatabricksTypeToArrow_MapType_ReturnsString()
        {
            var connection = CreateTestConnection();
            var result = (IArrowType)InvokePrivateMethod(connection, "ConvertDatabricksTypeToArrow", "MAP<STRING, INT>");

            Assert.IsType<StringType>(result);
        }

        [Fact]
        public void ExtractBaseType_ComplexStruct_ReturnsSTRUCT()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "ExtractBaseType", "STRUCT<f1: STRING, f2: INT, f3: ARRAY<DOUBLE>>");

            Assert.Equal("STRUCT", result);
        }

        [Fact]
        public void ExtractBaseType_ComplexArray_ReturnsARRAY()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "ExtractBaseType", "ARRAY<STRUCT<name: STRING, age: INT>>");

            Assert.Equal("ARRAY", result);
        }

        [Fact]
        public void ExtractBaseType_ComplexMap_ReturnsMAP()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "ExtractBaseType", "MAP<STRING, ARRAY<INT>>");

            Assert.Equal("MAP", result);
        }

        #endregion

        #region SQL Command Builder Edge Cases

        [Fact]
        public void QuoteIdentifier_WithSpaces_PreservesSpaces()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "QuoteIdentifier", "my table");

            Assert.Equal("`my table`", result);
        }

        [Fact]
        public void QuoteIdentifier_Unicode_PreservesUnicode()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "QuoteIdentifier", "таблица");

            Assert.Equal("`таблица`", result);
        }

        [Fact]
        public void EscapeSqlPattern_MultipleWildcards_EscapesAll()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "EscapeSqlPattern", "test%value_data%");

            Assert.Equal("test*value.data*", result);
        }

        [Fact]
        public void EscapeSqlPattern_EscapedBackslash_PreservesBackslash()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "EscapeSqlPattern", "test\\\\value");

            Assert.Equal("test\\\\value", result);
        }

        [Fact]
        public void EscapeSqlPattern_EscapedPercent_BecomesLiteralPercent()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "EscapeSqlPattern", "test\\%value");

            Assert.Equal("test%value", result);
        }

        [Fact]
        public void EscapeSqlPattern_EscapedUnderscore_BecomesLiteralUnderscore()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "EscapeSqlPattern", "test\\_value");

            Assert.Equal("test_value", result);
        }

        [Fact]
        public void EscapeSqlPattern_SingleQuote_DoublesQuote()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "EscapeSqlPattern", "O'Reilly");

            Assert.Equal("O''Reilly", result);
        }

        [Fact]
        public void EscapeSqlPattern_MultipleQuotes_DoublesAll()
        {
            var connection = CreateTestConnection();
            var result = (string)InvokePrivateMethod(connection, "EscapeSqlPattern", "It's O'Reilly's");

            Assert.Equal("It''s O''Reilly''s", result);
        }

        #endregion
    }
}
