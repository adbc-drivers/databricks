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
    /// Unit tests for the helper methods introduced to fix PECO-3018 (D8):
    /// <see cref="StatementExecutionConnection.ContainsUnescapedWildcard"/> and
    /// <see cref="StatementExecutionConnection.CatalogPatternToRegex"/>.
    ///
    /// These helpers enable GetCatalogsAsync to correctly handle wildcard catalog
    /// patterns (%, _), escaped literals (\_), and empty strings without sending
    /// broken LIKE clauses to the server.
    ///
    /// Root cause: the previous implementation passed the ADBC catalogPattern directly
    /// to ShowCatalogsCommand which converted it via ConvertPattern() and appended it as
    /// a SQL LIKE clause.  This was wrong because:
    ///   - "comparator\_tests" → SHOW CATALOGS LIKE 'comparator_tests'
    ///     (SQL _ is a single-char wildcard, so the literal underscore was not preserved)
    ///   - "%" → SHOW CATALOGS LIKE '*'
    ///     (the glob '*' may not be valid in all SQL LIKE contexts)
    ///
    /// Fix: always issue SHOW CATALOGS (no LIKE) and filter client-side using
    /// CatalogPatternToRegex, mirroring the Thrift path's PatternToRegEx().
    /// </summary>
    public class GetCatalogsAsyncTests
    {
        // ----------------------------------------------------------------
        // ContainsUnescapedWildcard
        // ----------------------------------------------------------------

        [Theory]
        [InlineData("%", true)]                      // bare % is a wildcard
        [InlineData("compar%", true)]                // trailing %
        [InlineData("_", true)]                      // bare _ is a wildcard
        [InlineData("comparator_tests", true)]       // unescaped _ is a wildcard
        [InlineData(@"comparator\_tests", false)]    // \_ is escaped → not a wildcard
        [InlineData(@"\%", false)]                   // \% is escaped → not a wildcard
        [InlineData("", false)]                      // empty — no wildcard
        [InlineData("nonexistent", false)]           // plain literal — no wildcard
        [InlineData("main", false)]                  // plain literal — no wildcard
        [InlineData(@"\\", false)]                   // escaped backslash — no wildcard
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
            // "comparator_tests" pattern: _ matches any single char
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
            // D8 bug case: "comparator\_tests" — \_ is a literal underscore, not a wildcard.
            Regex regex = StatementExecutionConnection.CatalogPatternToRegex(@"comparator\_tests");
            Assert.Matches(regex, "comparator_tests");
            Assert.DoesNotMatch(regex, "comparator-tests");
            Assert.DoesNotMatch(regex, "comparatorXtests");
            Assert.DoesNotMatch(regex, "comparator_Xtests");
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
            Assert.DoesNotMatch(regex, "myXcatalog"); // dot in pattern is literal
        }

        [Fact]
        public void CatalogPatternToRegex_EscapedBackslash()
        {
            // \\ in pattern → literal backslash in output
            Regex regex = StatementExecutionConnection.CatalogPatternToRegex(@"back\\slash");
            Assert.Matches(regex, @"back\slash");
            Assert.DoesNotMatch(regex, "backslash");
        }

        // ----------------------------------------------------------------
        // D8 bug-report scenarios (from PECO-3018 bug report)
        // ----------------------------------------------------------------

        [Fact]
        public void BugReport_D8_EscapedUnderscore_NotAWildcard()
        {
            // D8: "comparator\_tests" should match the literal catalog "comparator_tests"
            // only, not "comparatorXtests" or other single-char variations.
            Assert.False(StatementExecutionConnection.ContainsUnescapedWildcard(@"comparator\_tests"));
            Regex regex = StatementExecutionConnection.CatalogPatternToRegex(@"comparator\_tests");
            Assert.Matches(regex, "comparator_tests");
            Assert.DoesNotMatch(regex, "comparator-tests");
            Assert.DoesNotMatch(regex, "comparatorXtests");
        }

        [Fact]
        public void BugReport_UnescapedUnderscore_IsSingleCharWildcard()
        {
            // "comparator_tests" (no escape): _ is a wildcard matching any single char.
            Assert.True(StatementExecutionConnection.ContainsUnescapedWildcard("comparator_tests"));
            Regex regex = StatementExecutionConnection.CatalogPatternToRegex("comparator_tests");
            Assert.Matches(regex, "comparator_tests");
            Assert.Matches(regex, "comparator-tests");
        }

        [Fact]
        public void BugReport_PercentPattern_DetectedAsWildcard()
        {
            // "%" should be a wildcard matching everything.
            Assert.True(StatementExecutionConnection.ContainsUnescapedWildcard("%"));
            Regex regex = StatementExecutionConnection.CatalogPatternToRegex("%");
            Assert.Matches(regex, "any_catalog");
        }

        [Fact]
        public void BugReport_ComparPercentPrefix_DetectedAsWildcard()
        {
            // "compar%" should be a wildcard prefix match.
            Assert.True(StatementExecutionConnection.ContainsUnescapedWildcard("compar%"));
            Regex regex = StatementExecutionConnection.CatalogPatternToRegex("compar%");
            Assert.Matches(regex, "comparator_tests");
            Assert.DoesNotMatch(regex, "main");
        }

        [Fact]
        public void BugReport_EmptyString_NotAWildcard()
        {
            // Empty string: GetCatalogsAsync returns empty list immediately (no server call).
            Assert.False(StatementExecutionConnection.ContainsUnescapedWildcard(""));
        }

        [Fact]
        public void BugReport_NonexistentCatalog_NotAWildcard_NoMatch()
        {
            // "nonexistent": no wildcard. CatalogPatternToRegex produces a regex that
            // matches only "nonexistent", so real catalogs won't appear in the result.
            Assert.False(StatementExecutionConnection.ContainsUnescapedWildcard("nonexistent"));
            Regex regex = StatementExecutionConnection.CatalogPatternToRegex("nonexistent");
            Assert.DoesNotMatch(regex, "main");
            Assert.DoesNotMatch(regex, "comparator_tests");
            Assert.Matches(regex, "nonexistent");
        }
    }
}
