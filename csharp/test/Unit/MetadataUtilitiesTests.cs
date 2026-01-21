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
using AdbcDrivers.Databricks;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    /// <summary>
    /// Unit tests for MetadataUtilities helper methods.
    /// Tests public static methods for catalog normalization, PK/FK validation,
    /// identifier quoting, and pattern conversion.
    /// </summary>
    public class MetadataUtilitiesTests
    {
        #region NormalizeSparkCatalog Tests

        [Theory]
        [InlineData("SPARK")]
        [InlineData("spark")]
        [InlineData("Spark")]
        [InlineData("SpArK")]
        public void NormalizeSparkCatalog_WithSparkCatalog_ReturnsNull(string input)
        {
            var result = MetadataUtilities.NormalizeSparkCatalog(input);
            Assert.Null(result);
        }

        [Theory]
        [InlineData("main", "main")]
        [InlineData("unity_catalog", "unity_catalog")]
        [InlineData("", "")]
        public void NormalizeSparkCatalog_WithNonSparkCatalog_ReturnsUnchanged(string input, string expected)
        {
            var result = MetadataUtilities.NormalizeSparkCatalog(input);
            Assert.Equal(expected, result);
        }

        [Fact]
        public void NormalizeSparkCatalog_WithNull_ReturnsNull()
        {
            var result = MetadataUtilities.NormalizeSparkCatalog(null);
            Assert.Null(result);
        }

        #endregion

        #region IsInvalidPKFKCatalog Tests

        [Theory]
        [InlineData("")]
        [InlineData("SPARK")]
        [InlineData("spark")]
        [InlineData("Spark")]
        [InlineData("hive_metastore")]
        [InlineData("HIVE_METASTORE")]
        [InlineData("Hive_Metastore")]
        public void IsInvalidPKFKCatalog_WithInvalidCatalog_ReturnsTrue(string catalog)
        {
            Assert.True(MetadataUtilities.IsInvalidPKFKCatalog(catalog));
        }

        [Theory]
        [InlineData("main")]
        [InlineData("unity_catalog")]
        [InlineData("my_catalog")]
        public void IsInvalidPKFKCatalog_WithValidCatalog_ReturnsFalse(string catalog)
        {
            Assert.False(MetadataUtilities.IsInvalidPKFKCatalog(catalog));
        }

        [Fact]
        public void IsInvalidPKFKCatalog_WithNull_ReturnsTrue()
        {
            Assert.True(MetadataUtilities.IsInvalidPKFKCatalog(null));
        }

        #endregion

        #region ShouldReturnEmptyPKFKResult Tests

        [Theory]
        [InlineData("main", "main", false)]  // Feature disabled
        [InlineData("SPARK", "hive_metastore", true)]  // Both invalid
        public void ShouldReturnEmptyPKFKResult_ReturnsTrue(string pkCatalog, string fkCatalog, bool enablePKFK)
        {
            Assert.True(MetadataUtilities.ShouldReturnEmptyPKFKResult(pkCatalog, fkCatalog, enablePKFK));
        }

        [Fact]
        public void ShouldReturnEmptyPKFKResult_WithBothNull_ReturnsTrue()
        {
            Assert.True(MetadataUtilities.ShouldReturnEmptyPKFKResult(null, null, true));
        }

        [Theory]
        [InlineData("main", "SPARK", true)]  // One valid
        [InlineData("SPARK", "main", true)]  // One valid (reversed)
        [InlineData("main", "unity_catalog", true)]  // Both valid
        public void ShouldReturnEmptyPKFKResult_ReturnsFalse(string pkCatalog, string fkCatalog, bool enablePKFK)
        {
            Assert.False(MetadataUtilities.ShouldReturnEmptyPKFKResult(pkCatalog, fkCatalog, enablePKFK));
        }

        #endregion

        #region BuildQualifiedTableName Tests

        [Theory]
        [InlineData("main", "default", "table1", "`main`.`default`.`table1`")]  // Fully qualified
        [InlineData("my`catalog", "my`schema", "my`table", "`my``catalog`.`my``schema`.`my``table`")]  // Backtick escaping
        [InlineData("SPARK", "default", "table1", "`default`.`table1`")]  // Spark catalog omitted
        [InlineData("spark", "default", "table1", "`default`.`table1`")]  // Spark catalog omitted (lowercase)
        [InlineData("main", "default", "", "")]  // Empty table
        public void BuildQualifiedTableName_ReturnsExpectedValue(string catalog, string schema, string table, string expected)
        {
            var result = MetadataUtilities.BuildQualifiedTableName(catalog, schema, table);
            Assert.Equal(expected, result);
        }

        [Fact]
        public void BuildQualifiedTableName_WithNullCatalog_ReturnsTwoPart()
        {
            var result = MetadataUtilities.BuildQualifiedTableName(null, "default", "table1");
            Assert.Equal("`default`.`table1`", result);
        }

        [Fact]
        public void BuildQualifiedTableName_WithNullSchema_ReturnsOnePart()
        {
            var result = MetadataUtilities.BuildQualifiedTableName("main", null, "table1");
            Assert.Equal("`table1`", result);
        }

        [Fact]
        public void BuildQualifiedTableName_WithNullCatalogAndSchema_ReturnsOnePart()
        {
            var result = MetadataUtilities.BuildQualifiedTableName(null, null, "table1");
            Assert.Equal("`table1`", result);
        }

        [Fact]
        public void BuildQualifiedTableName_WithNullTable_ReturnsNull()
        {
            var result = MetadataUtilities.BuildQualifiedTableName("main", "default", null);
            Assert.Null(result);
        }

        #endregion

        #region QuoteIdentifier Tests

        [Theory]
        [InlineData("my_table", "`my_table`")]  // Simple identifier
        [InlineData("my`table", "`my``table`")]  // Single backtick
        [InlineData("my`complex`table", "`my``complex``table`")]  // Multiple backticks
        [InlineData("my-table.name", "`my-table.name`")]  // Special characters
        [InlineData("my table", "`my table`")]  // Spaces
        [InlineData("таблица", "`таблица`")]  // Unicode
        [InlineData("", "")]  // Empty string
        public void QuoteIdentifier_ReturnsExpectedValue(string identifier, string expected)
        {
            var result = MetadataUtilities.QuoteIdentifier(identifier);
            Assert.Equal(expected, result);
        }

        [Fact]
        public void QuoteIdentifier_WithNull_ReturnsNull()
        {
            var result = MetadataUtilities.QuoteIdentifier(null);
            Assert.Null(result);
        }

        #endregion

        #region ConvertAdbcPatternToDatabricksGlob Tests

        [Theory]
        [InlineData("my_schema", "my.schema")]  // Underscore wildcard
        [InlineData("dev_%", "dev.*")]  // Percent wildcard
        [InlineData("test_", "test.")]  // Trailing underscore
        [InlineData("my\\_schema", "my_schema")]  // Escaped underscore (literal)
        [InlineData("o'reilly", "o''reilly")]  // Single quote escaping
        [InlineData("it's john's", "it''s john''s")]  // Multiple quotes
        [InlineData("dev_%_schema", "dev.*.schema")]  // Mixed wildcards
        [InlineData("test%value_data%", "test*value.data*")]  // Multiple wildcards
        [InlineData("test\\\\value", "test\\\\value")]  // Escaped backslash
        [InlineData("test\\%value", "test%value")]  // Escaped percent (literal)
        [InlineData("test\\_value", "test_value")]  // Escaped underscore (literal)
        [InlineData("dev_%_O'Reilly\\%", "dev.*.O''Reilly%")]  // Complex pattern
        [InlineData("", "")]  // Empty
        public void ConvertAdbcPatternToDatabricksGlob_ReturnsExpectedValue(string pattern, string expected)
        {
            var result = MetadataUtilities.ConvertAdbcPatternToDatabricksGlob(pattern);
            Assert.Equal(expected, result);
        }

        [Fact]
        public void ConvertAdbcPatternToDatabricksGlob_WithNull_ReturnsNull()
        {
            var result = MetadataUtilities.ConvertAdbcPatternToDatabricksGlob(null);
            Assert.Null(result);
        }

        #endregion
    }
}
