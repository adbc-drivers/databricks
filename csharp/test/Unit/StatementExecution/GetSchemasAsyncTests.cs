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

using System.Text.RegularExpressions;
using AdbcDrivers.Databricks.StatementExecution;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    /// <summary>
    /// Unit tests for the helper methods introduced to fix PECO-3017:
    /// <see cref="StatementExecutionConnection.ContainsUnescapedWildcard"/>,
    /// <see cref="StatementExecutionConnection.CatalogPatternToRegex"/>, and
    /// <see cref="StatementExecutionConnection.DecodeLiteralPattern"/>.
    ///
    /// These helpers enable GetSchemasAsync to correctly handle wildcard catalog
    /// patterns (%, _), escaped literals (\_), empty strings, and nonexistent
    /// catalog names without throwing server-side exceptions.
    /// </summary>
    public class GetSchemasAsyncTests
    {
        // ----------------------------------------------------------------
        // ContainsUnescapedWildcard
        // ----------------------------------------------------------------

        [Theory]
        [InlineData("%", true)]           // bare % is a wildcard
        [InlineData("compar%", true)]     // trailing %
        [InlineData("_", true)]           // bare _ is a wildcard
        [InlineData("comparator_tests", true)]  // unescaped _ is a wildcard
        [InlineData(@"comparator\_tests", false)] // \_ is escaped, not a wildcard
        [InlineData(@"\%", false)]        // \% is escaped, not a wildcard
        [InlineData("", false)]           // empty — no wildcard
        [InlineData("nonexistent", false)] // plain literal — no wildcard
        [InlineData("main", false)]       // plain literal — no wildcard
        [InlineData(@"\\", false)]        // escaped backslash — no wildcard
        public void ContainsUnescapedWildcard_ReturnsExpected(string pattern, bool expected)
        {
            Assert.Equal(expected, StatementExecutionConnection.ContainsUnescapedWildcard(pattern));
        }

        // ----------------------------------------------------------------
        // CatalogPatternToRegex — wildcard expansion
        // ----------------------------------------------------------------

        [Fact]
        public void CatalogPatternToRegex_PercentMatchesAllCatalogs()
        {
            Regex regex = StatementExecutionConnection.CatalogPatternToRegex("%");
            Assert.Matches(regex, "main");
            Assert.Matches(regex, "comparator_tests");
            Assert.Matches(regex, "hive_metastore");
            Assert.Matches(regex, "");
        }

        [Fact]
        public void CatalogPatternToRegex_PercentPrefix_MatchesCatalogsStartingWithPrefix()
        {
            Regex regex = StatementExecutionConnection.CatalogPatternToRegex("compar%");
            Assert.Matches(regex, "comparator_tests");
            Assert.Matches(regex, "comparator-tests");
            Assert.Matches(regex, "compar");
            Assert.DoesNotMatch(regex, "main");
            Assert.DoesNotMatch(regex, "xcompar");
        }

        [Fact]
        public void CatalogPatternToRegex_UnderscoreMatchesSingleChar()
        {
            // "comparator_tests" pattern: _ matches any single char,
            // so it should match "comparator_tests" and "comparator-tests".
            Regex regex = StatementExecutionConnection.CatalogPatternToRegex("comparator_tests");
            Assert.Matches(regex, "comparator_tests");
            Assert.Matches(regex, "comparator-tests");
            Assert.Matches(regex, "comparatorXtests");
            Assert.DoesNotMatch(regex, "comparatortests");   // _ requires exactly one char
            Assert.DoesNotMatch(regex, "comparator__tests"); // one char too many
        }

        [Fact]
        public void CatalogPatternToRegex_EscapedUnderscore_MatchesLiteralUnderscore()
        {
            // "comparator\_tests": \_ is a literal underscore, not a single-char wildcard.
            Regex regex = StatementExecutionConnection.CatalogPatternToRegex(@"comparator\_tests");
            Assert.Matches(regex, "comparator_tests");
            Assert.DoesNotMatch(regex, "comparator-tests");
            Assert.DoesNotMatch(regex, "comparatorXtests");
        }

        [Fact]
        public void CatalogPatternToRegex_EscapedPercent_MatchesLiteralPercent()
        {
            Regex regex = StatementExecutionConnection.CatalogPatternToRegex(@"cat\%log");
            Assert.Matches(regex, "cat%log");
            Assert.DoesNotMatch(regex, "catalog");
            Assert.DoesNotMatch(regex, "catXlog");
        }

        [Fact]
        public void CatalogPatternToRegex_IsCaseInsensitive()
        {
            Regex regex = StatementExecutionConnection.CatalogPatternToRegex("MAIN");
            Assert.Matches(regex, "main");
            Assert.Matches(regex, "Main");
            Assert.Matches(regex, "MAIN");
        }

        [Fact]
        public void CatalogPatternToRegex_RegexSpecialCharsInPattern_AreEscaped()
        {
            // Dots in a catalog name must be treated as literals, not regex wildcards.
            Regex regex = StatementExecutionConnection.CatalogPatternToRegex("my.catalog");
            Assert.Matches(regex, "my.catalog");
            Assert.DoesNotMatch(regex, "myXcatalog"); // dot is literal, not regex wildcard
        }

        // ----------------------------------------------------------------
        // DecodeLiteralPattern
        // ----------------------------------------------------------------

        [Fact]
        public void DecodeLiteralPattern_NoEscapes_ReturnsSameString()
        {
            Assert.Equal("comparator_tests", StatementExecutionConnection.DecodeLiteralPattern("comparator_tests"));
        }

        [Fact]
        public void DecodeLiteralPattern_EscapedUnderscore_ReturnsLiteralUnderscore()
        {
            Assert.Equal("comparator_tests", StatementExecutionConnection.DecodeLiteralPattern(@"comparator\_tests"));
        }

        [Fact]
        public void DecodeLiteralPattern_EscapedPercent_ReturnsLiteralPercent()
        {
            Assert.Equal("cat%log", StatementExecutionConnection.DecodeLiteralPattern(@"cat\%log"));
        }

        [Fact]
        public void DecodeLiteralPattern_EscapedBackslash_ReturnsLiteralBackslash()
        {
            Assert.Equal(@"back\slash", StatementExecutionConnection.DecodeLiteralPattern(@"back\\slash"));
        }

        [Fact]
        public void DecodeLiteralPattern_EmptyString_ReturnsEmptyString()
        {
            Assert.Equal("", StatementExecutionConnection.DecodeLiteralPattern(""));
        }

        [Fact]
        public void DecodeLiteralPattern_LoneTrailingBackslash_PreservedAsBackslash()
        {
            // A lone trailing backslash with nothing after it is preserved as-is.
            Assert.Equal(@"test\", StatementExecutionConnection.DecodeLiteralPattern(@"test\"));
        }

        // ----------------------------------------------------------------
        // Round-trip: ContainsUnescapedWildcard + CatalogPatternToRegex
        // covering the six bug-report cases from PECO-3017
        // ----------------------------------------------------------------

        [Fact]
        public void BugReport_PercentPattern_DetectedAsWildcard()
        {
            // "%" should be recognized as a wildcard so GetSchemasAsync fetches all and filters.
            Assert.True(StatementExecutionConnection.ContainsUnescapedWildcard("%"));
            Regex regex = StatementExecutionConnection.CatalogPatternToRegex("%");
            Assert.Matches(regex, "any_catalog");
        }

        [Fact]
        public void BugReport_ComparPercent_DetectedAsWildcard()
        {
            // "compar%" should be a wildcard prefix match.
            Assert.True(StatementExecutionConnection.ContainsUnescapedWildcard("compar%"));
            Regex regex = StatementExecutionConnection.CatalogPatternToRegex("compar%");
            Assert.Matches(regex, "comparator_tests");
            Assert.DoesNotMatch(regex, "main");
        }

        [Fact]
        public void BugReport_EscapedUnderscore_NotAWildcard_DecodesCorrectly()
        {
            // "comparator\_tests": no wildcard → use literal catalog name.
            Assert.False(StatementExecutionConnection.ContainsUnescapedWildcard(@"comparator\_tests"));
            string literal = StatementExecutionConnection.DecodeLiteralPattern(@"comparator\_tests");
            Assert.Equal("comparator_tests", literal);
        }

        [Fact]
        public void BugReport_UnescapedUnderscore_IsSingleCharWildcard()
        {
            // "comparator_tests" (no escape): _ is a wildcard matching any single char.
            Assert.True(StatementExecutionConnection.ContainsUnescapedWildcard("comparator_tests"));
            Regex regex = StatementExecutionConnection.CatalogPatternToRegex("comparator_tests");
            // Matches both "comparator_tests" and "comparator-tests"
            Assert.Matches(regex, "comparator_tests");
            Assert.Matches(regex, "comparator-tests");
        }

        [Fact]
        public void BugReport_EmptyString_NotAWildcard()
        {
            // Empty string: no wildcard; GetSchemasAsync returns empty rowset immediately.
            Assert.False(StatementExecutionConnection.ContainsUnescapedWildcard(""));
        }

        [Fact]
        public void BugReport_NonexistentCatalog_NotAWildcard()
        {
            // "nonexistent": no wildcard; GetSchemasAsync attempts direct query and
            // catches the server error, returning an empty rowset.
            Assert.False(StatementExecutionConnection.ContainsUnescapedWildcard("nonexistent"));
        }
    }
}
