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

using System;
using AdbcDrivers.Databricks.StatementExecution;
using AdbcDrivers.Databricks.StatementExecution.MetadataCommands;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    public class ShowCommandTests
    {
        // ShowCatalogsCommand

        [Fact]
        public void ShowCatalogs_NoPattern()
        {
            Assert.Equal("SHOW CATALOGS", new ShowCatalogsCommand().Build());
        }

        [Fact]
        public void ShowCatalogs_WithPattern()
        {
            Assert.Equal("SHOW CATALOGS LIKE 'ma*'", new ShowCatalogsCommand("ma%").Build());
        }

        [Fact]
        public void ShowCatalogs_WithUnderscorePattern()
        {
            Assert.Equal("SHOW CATALOGS LIKE 'm.in'", new ShowCatalogsCommand("m_in").Build());
        }

        // ShowSchemasCommand

        [Fact]
        public void ShowSchemas_NullCatalog_UsesAllCatalogs()
        {
            Assert.Equal("SHOW SCHEMAS IN ALL CATALOGS", new ShowSchemasCommand(null).Build());
        }

        [Fact]
        public void ShowSchemas_WithCatalog()
        {
            Assert.Equal("SHOW SCHEMAS IN `main`", new ShowSchemasCommand("main").Build());
        }

        [Fact]
        public void ShowSchemas_WithCatalogAndPattern()
        {
            Assert.Equal("SHOW SCHEMAS IN `main` LIKE 'def*'", new ShowSchemasCommand("main", "def%").Build());
        }

        [Fact]
        public void ShowSchemas_CatalogWithBacktick()
        {
            Assert.Equal("SHOW SCHEMAS IN `my``catalog`", new ShowSchemasCommand("my`catalog").Build());
        }

        // ShowTablesCommand

        [Fact]
        public void ShowTables_NullCatalog_UsesAllCatalogs()
        {
            Assert.Equal("SHOW TABLES IN ALL CATALOGS", new ShowTablesCommand(null).Build());
        }

        [Fact]
        public void ShowTables_WithCatalog()
        {
            Assert.Equal("SHOW TABLES IN CATALOG `main`", new ShowTablesCommand("main").Build());
        }

        [Fact]
        public void ShowTables_WithCatalogAndSchema()
        {
            Assert.Equal(
                "SHOW TABLES IN CATALOG `main` SCHEMA LIKE 'default'",
                new ShowTablesCommand("main", "default").Build());
        }

        [Fact]
        public void ShowTables_WithAllPatterns()
        {
            Assert.Equal(
                "SHOW TABLES IN CATALOG `main` SCHEMA LIKE 'def*' LIKE 'test*'",
                new ShowTablesCommand("main", "def%", "test%").Build());
        }

        // ShowColumnsCommand

        [Fact]
        public void ShowColumns_NullCatalog_UsesAllCatalogs()
        {
            Assert.Equal("SHOW COLUMNS IN ALL CATALOGS", new ShowColumnsCommand(null).Build());
        }

        [Fact]
        public void ShowColumns_WithCatalog()
        {
            Assert.Equal("SHOW COLUMNS IN CATALOG `main`", new ShowColumnsCommand("main").Build());
        }

        [Fact]
        public void ShowColumns_WithAllPatterns()
        {
            Assert.Equal(
                "SHOW COLUMNS IN CATALOG `main` SCHEMA LIKE 'default' TABLE LIKE 'users' LIKE 'col*'",
                new ShowColumnsCommand("main", "default", "users", "col%").Build());
        }

        [Fact]
        public void ShowColumns_WildcardColumnPattern_Omitted()
        {
            Assert.Equal(
                "SHOW COLUMNS IN CATALOG `main`",
                new ShowColumnsCommand("main", null, null, "%").Build());
        }

        // ShowKeysCommand

        [Fact]
        public void ShowKeys_BuildsCorrectly()
        {
            Assert.Equal(
                "SHOW KEYS IN CATALOG `main` IN SCHEMA `default` IN TABLE `users`",
                new ShowKeysCommand("main", "default", "users").Build());
        }

        [Fact]
        public void ShowKeys_NullCatalog_Throws()
        {
            Assert.Throws<ArgumentNullException>(() => new ShowKeysCommand(null!, "default", "users"));
        }

        [Fact]
        public void ShowKeys_NullSchema_Throws()
        {
            Assert.Throws<ArgumentNullException>(() => new ShowKeysCommand("main", null!, "users"));
        }

        [Fact]
        public void ShowKeys_NullTable_Throws()
        {
            Assert.Throws<ArgumentNullException>(() => new ShowKeysCommand("main", "default", null!));
        }

        // ShowForeignKeysCommand

        [Fact]
        public void ShowForeignKeys_BuildsCorrectly()
        {
            Assert.Equal(
                "SHOW FOREIGN KEYS IN CATALOG `main` IN SCHEMA `default` IN TABLE `orders`",
                new ShowForeignKeysCommand("main", "default", "orders").Build());
        }

        [Fact]
        public void ShowForeignKeys_SpecialCharsInIdentifiers()
        {
            Assert.Equal(
                "SHOW FOREIGN KEYS IN CATALOG `my``cat` IN SCHEMA `my``schema` IN TABLE `my``table`",
                new ShowForeignKeysCommand("my`cat", "my`schema", "my`table").Build());
        }

        // Pattern conversion edge cases

        [Fact]
        public void ShowCatalogs_SingleQuoteEscaped()
        {
            Assert.Equal("SHOW CATALOGS LIKE 'it''s'", new ShowCatalogsCommand("it's").Build());
        }

        [Fact]
        public void ShowCatalogs_EmptyPattern()
        {
            Assert.Equal("SHOW CATALOGS LIKE ''", new ShowCatalogsCommand("").Build());
        }

        // MetadataCommandBase wildcard helpers (PECO-3035). These back the client-side
        // catalog-wildcard expansion in StatementExecutionConnection.ListSchemasAsync,
        // so the backslash-escape semantics must be locked in.

        [Theory]
        [InlineData(null, false)]
        [InlineData("", false)]
        [InlineData("abc", false)]
        [InlineData("prod", false)]
        [InlineData("*", false)]            // not a JDBC LIKE wildcard
        [InlineData("%", true)]
        [InlineData("_", true)]
        [InlineData("abc%", true)]
        [InlineData("_abc", true)]
        [InlineData("a%b", true)]
        [InlineData("a_b", true)]
        [InlineData("\\%", false)]          // escaped %
        [InlineData("\\_", false)]          // escaped _
        [InlineData("\\\\%", true)]         // literal backslash + unescaped %
        [InlineData("\\\\_", true)]         // literal backslash + unescaped _
        [InlineData("\\\\\\%", false)]      // literal backslash + escaped %
        [InlineData("\\\\\\_", false)]      // literal backslash + escaped _
        [InlineData("\\", false)]           // lone trailing backslash
        [InlineData("foo\\", false)]        // trailing backslash, no wildcard
        [InlineData("foo\\%bar", false)]    // escaped % in the middle
        [InlineData("foo\\%bar%baz", true)] // escaped % then unescaped %
        public void ContainsUnescapedWildcard_HandlesEscapeSemantics(string? input, bool expected)
        {
            Assert.Equal(expected, MetadataCommandBase.ContainsUnescapedWildcard(input));
        }

        [Theory]
        [InlineData(null, false)]
        [InlineData("", false)]
        [InlineData("%", true)]
        [InlineData("*", true)]             // Spark/Hive convention, also fast-pathed
        [InlineData("%%", false)]
        [InlineData("_", false)]
        [InlineData("prod", false)]
        [InlineData("\\%", false)]
        public void IsMatchAnything_TreatsOnlyBareWildcardsAsMatchAnything(string? input, bool expected)
        {
            Assert.Equal(expected, MetadataCommandBase.IsMatchAnything(input));
        }

        [Theory]
        // Literals (the helper is anchored, so "prod" only matches "prod" exactly).
        [InlineData("prod", "prod", true)]
        [InlineData("prod", "prod_2", false)]
        [InlineData("prod", "production", false)]
        // % wildcard — any sequence including empty.
        [InlineData("%", "anything", true)]
        [InlineData("%", "", true)]
        [InlineData("comp%", "compute", true)]
        [InlineData("comp%", "comp", true)]
        [InlineData("comp%", "system", false)]
        [InlineData("%comp", "mycomp", true)]
        [InlineData("%comp", "myComp", false)]   // case-sensitive: comp ≠ Comp
        [InlineData("%comp", "compsomething", false)]
        // _ wildcard — exactly one char.
        [InlineData("a_c", "abc", true)]
        [InlineData("a_c", "ac", false)]
        [InlineData("a_c", "abbc", false)]
        // Escapes — \% / \_ must match the literal character.
        [InlineData("comp\\%", "comp%", true)]
        [InlineData("comp\\%", "compute", false)]
        [InlineData("a\\_b", "a_b", true)]
        [InlineData("a\\_b", "axb", false)]
        // \\ → literal backslash.
        [InlineData("a\\\\b", "a\\b", true)]
        // Regex metacharacters in the literal portion must be escaped.
        [InlineData("a.b", "a.b", true)]
        [InlineData("a.b", "axb", false)]
        [InlineData("a+b", "a+b", true)]
        [InlineData("a+b", "ab", false)]
        public void JdbcLikeToRegex_MatchesPatternSemantics(string pattern, string input, bool expectedMatch)
        {
            var regex = MetadataCommandBase.JdbcLikeToRegex(pattern);
            Assert.Equal(expectedMatch, regex.IsMatch(input));
        }
    }
}
