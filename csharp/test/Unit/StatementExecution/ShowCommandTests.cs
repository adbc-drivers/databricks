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
            Assert.Equal("SHOW CATALOGS LIKE '*'", new ShowCatalogsCommand("").Build());
        }
    }
}
