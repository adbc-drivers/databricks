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
using AdbcDrivers.Databricks.StatementExecution;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    /// <summary>
    /// Unit tests for SqlCommandBuilder.
    /// Tests SQL query generation for metadata operations.
    /// </summary>
    public class SqlCommandBuilderTests
    {
        #region BuildShowCatalogs Tests

        [Fact]
        public void BuildShowCatalogs_NoPattern_ReturnsBasicCommand()
        {
            var builder = new SqlCommandBuilder();
            var sql = builder.BuildShowCatalogs();

            Assert.Equal("SHOW CATALOGS", sql);
        }

        [Fact]
        public void BuildShowCatalogs_WithPattern_ReturnsCommandWithLike()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalogPattern("main%");
            var sql = builder.BuildShowCatalogs();

            Assert.Equal("SHOW CATALOGS LIKE 'main*'", sql);
        }

        [Fact]
        public void BuildShowCatalogs_WithSingleQuoteInPattern_EscapesQuote()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalogPattern("o'reilly");
            var sql = builder.BuildShowCatalogs();

            Assert.Equal("SHOW CATALOGS LIKE 'o''reilly'", sql);
        }

        #endregion

        #region BuildShowSchemas Tests

        [Fact]
        public void BuildShowSchemas_NoCatalog_ReturnsAllCatalogs()
        {
            var builder = new SqlCommandBuilder();
            var sql = builder.BuildShowSchemas();

            Assert.Equal("SHOW SCHEMAS IN ALL CATALOGS", sql);
        }

        [Fact]
        public void BuildShowSchemas_WithCatalog_ReturnsInCatalog()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalog("main");
            var sql = builder.BuildShowSchemas();

            Assert.Equal("SHOW SCHEMAS IN `main`", sql);
        }

        [Fact]
        public void BuildShowSchemas_WithCatalogAndSchemaPattern_ReturnsWithLike()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalog("main")
                .WithSchemaPattern("dev_%");
            var sql = builder.BuildShowSchemas();

            Assert.Equal("SHOW SCHEMAS IN `main` LIKE 'dev.*'", sql);
        }

        [Fact]
        public void BuildShowSchemas_WithBacktickInCatalog_EscapesBacktick()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalog("my`catalog");
            var sql = builder.BuildShowSchemas();

            Assert.Equal("SHOW SCHEMAS IN `my``catalog`", sql);
        }

        [Fact]
        public void BuildShowSchemas_WithExactSchema_UsesSchemaAsFilter()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalog("main")
                .WithSchemaPattern("default");
            var sql = builder.BuildShowSchemas();

            Assert.Equal("SHOW SCHEMAS IN `main` LIKE 'default'", sql);
        }

        #endregion

        #region BuildShowTables Tests

        [Fact]
        public void BuildShowTables_NoCatalog_ReturnsAllCatalogs()
        {
            var builder = new SqlCommandBuilder();
            var sql = builder.BuildShowTables();

            Assert.Equal("SHOW TABLES IN ALL CATALOGS", sql);
        }

        [Fact]
        public void BuildShowTables_WithCatalog_ReturnsInCatalog()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalog("main");
            var sql = builder.BuildShowTables();

            Assert.Equal("SHOW TABLES IN CATALOG `main`", sql);
        }

        [Fact]
        public void BuildShowTables_WithCatalogAndSchemaPattern_ReturnsWithSchemaLike()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalog("main")
                .WithSchemaPattern("default");
            var sql = builder.BuildShowTables();

            Assert.Equal("SHOW TABLES IN CATALOG `main` SCHEMA LIKE 'default'", sql);
        }

        [Fact]
        public void BuildShowTables_WithTablePattern_ReturnsWithTableLike()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalog("main")
                .WithSchemaPattern("default")
                .WithTablePattern("test_%");
            var sql = builder.BuildShowTables();

            Assert.Equal("SHOW TABLES IN CATALOG `main` SCHEMA LIKE 'default' LIKE 'test.*'", sql);
        }

        [Fact]
        public void BuildShowTables_WithExactSchema_UsesSchemaAsPattern()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalog("main")
                .WithSchemaPattern("default")
                .WithTablePattern("my_table");
            var sql = builder.BuildShowTables();

            Assert.Equal("SHOW TABLES IN CATALOG `main` SCHEMA LIKE 'default' LIKE 'my.table'", sql);
        }

        #endregion

        #region BuildShowColumns Tests

        [Fact]
        public void BuildShowColumns_NoCatalog_ReturnsAllCatalogs()
        {
            var builder = new SqlCommandBuilder();
            var sql = builder.BuildShowColumns();

            Assert.Equal("SHOW COLUMNS IN ALL CATALOGS", sql);
        }

        [Fact]
        public void BuildShowColumns_WithCatalog_ReturnsInCatalog()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalog("main");
            var sql = builder.BuildShowColumns();

            Assert.Equal("SHOW COLUMNS IN CATALOG `main`", sql);
        }

        [Fact]
        public void BuildShowColumns_WithAllFilters_ReturnsCompleteCommand()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalog("main")
                .WithSchemaPattern("default")
                .WithTablePattern("my_table")
                .WithColumnPattern("col_%");
            var sql = builder.BuildShowColumns();

            Assert.Equal("SHOW COLUMNS IN CATALOG `main` SCHEMA LIKE 'default' TABLE LIKE 'my.table' LIKE 'col.*'", sql);
        }

        [Fact]
        public void BuildShowColumns_WithWildcardColumnPattern_OmitsColumnFilter()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalog("main")
                .WithSchemaPattern("default")
                .WithTablePattern("my_table")
                .WithColumnPattern("%");
            var sql = builder.BuildShowColumns();

            Assert.Equal("SHOW COLUMNS IN CATALOG `main` SCHEMA LIKE 'default' TABLE LIKE 'my.table'", sql);
        }

        [Fact]
        public void BuildShowColumns_WithCatalogOverride_UsesOverride()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalog("main");
            var sql = builder.BuildShowColumns("other_catalog");

            Assert.Equal("SHOW COLUMNS IN CATALOG `other_catalog`", sql);
        }

        [Fact]
        public void BuildShowColumns_WithExactSchemaAndTable_UsesAsPatterns()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalog("main")
                .WithSchemaPattern("default")
                .WithTablePattern("my_table");
            var sql = builder.BuildShowColumns();

            Assert.Equal("SHOW COLUMNS IN CATALOG `main` SCHEMA LIKE 'default' TABLE LIKE 'my.table'", sql);
        }

        #endregion

        #region BuildShowPrimaryKeys Tests

        [Fact]
        public void BuildShowPrimaryKeys_WithAllParameters_ReturnsCorrectSQL()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalog("main")
                .WithSchema("default")
                .WithTable("my_table");
            var sql = builder.BuildShowPrimaryKeys();

            Assert.Equal("SHOW KEYS IN CATALOG `main` IN SCHEMA `default` IN TABLE `my_table`", sql);
        }

        [Fact]
        public void BuildShowPrimaryKeys_WithBackticks_EscapesIdentifiers()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalog("my`catalog")
                .WithSchema("my`schema")
                .WithTable("my`table");
            var sql = builder.BuildShowPrimaryKeys();

            Assert.Equal("SHOW KEYS IN CATALOG `my``catalog` IN SCHEMA `my``schema` IN TABLE `my``table`", sql);
        }

        [Fact]
        public void BuildShowPrimaryKeys_MissingCatalog_ThrowsException()
        {
            var builder = new SqlCommandBuilder()
                .WithSchema("default")
                .WithTable("my_table");

            var exception = Assert.Throws<ArgumentException>(() => builder.BuildShowPrimaryKeys());
            Assert.Contains("requires exact catalog, schema, and table", exception.Message);
        }

        [Fact]
        public void BuildShowPrimaryKeys_MissingSchema_ThrowsException()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalog("main")
                .WithTable("my_table");

            var exception = Assert.Throws<ArgumentException>(() => builder.BuildShowPrimaryKeys());
            Assert.Contains("requires exact catalog, schema, and table", exception.Message);
        }

        [Fact]
        public void BuildShowPrimaryKeys_MissingTable_ThrowsException()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalog("main")
                .WithSchema("default");

            var exception = Assert.Throws<ArgumentException>(() => builder.BuildShowPrimaryKeys());
            Assert.Contains("requires exact catalog, schema, and table", exception.Message);
        }

        #endregion

        #region BuildShowForeignKeys Tests

        [Fact]
        public void BuildShowForeignKeys_WithAllParameters_ReturnsCorrectSQL()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalog("main")
                .WithSchema("default")
                .WithTable("my_table");
            var sql = builder.BuildShowForeignKeys();

            Assert.Equal("SHOW FOREIGN KEYS IN CATALOG `main` IN SCHEMA `default` IN TABLE `my_table`", sql);
        }

        [Fact]
        public void BuildShowForeignKeys_WithBackticks_EscapesIdentifiers()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalog("my`catalog")
                .WithSchema("my`schema")
                .WithTable("my`table");
            var sql = builder.BuildShowForeignKeys();

            Assert.Equal("SHOW FOREIGN KEYS IN CATALOG `my``catalog` IN SCHEMA `my``schema` IN TABLE `my``table`", sql);
        }

        [Fact]
        public void BuildShowForeignKeys_MissingParameters_ThrowsException()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalog("main");

            var exception = Assert.Throws<ArgumentException>(() => builder.BuildShowForeignKeys());
            Assert.Contains("requires exact catalog, schema, and table", exception.Message);
        }

        #endregion

        #region Fluent API Tests

        [Fact]
        public void FluentAPI_ChainedCalls_ReturnsBuilder()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalog("main")
                .WithSchema("default")
                .WithTable("my_table")
                .WithCatalogPattern("main%")
                .WithSchemaPattern("dev_%")
                .WithTablePattern("test_%")
                .WithColumnPattern("col_%");

            Assert.NotNull(builder);
        }

        [Fact]
        public void FluentAPI_MultipleBuilds_IndependentResults()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalog("main")
                .WithSchema("default");

            var sql1 = builder.BuildShowSchemas();
            var sql2 = builder.WithSchemaPattern("dev_%").BuildShowSchemas();

            // Both builds should work, using the latest state
            Assert.Contains("SHOW SCHEMAS", sql1);
            Assert.Contains("SHOW SCHEMAS", sql2);
        }

        #endregion

        #region Pattern Conversion Integration Tests

        [Fact]
        public void PatternConversion_PercentWildcard_ConvertsToAsterisk()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalogPattern("dev_%");
            var sql = builder.BuildShowCatalogs();

            Assert.Contains("'dev.*'", sql);
        }

        [Fact]
        public void PatternConversion_UnderscoreWildcard_ConvertsToDot()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalogPattern("test_");
            var sql = builder.BuildShowCatalogs();

            Assert.Contains("'test.'", sql);
        }

        [Fact]
        public void PatternConversion_MixedWildcards_ConvertsAll()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalogPattern("dev_%_prod");
            var sql = builder.BuildShowCatalogs();

            Assert.Contains("'dev.*.prod'", sql);
        }

        [Fact]
        public void PatternConversion_SingleQuote_DoublesQuote()
        {
            var builder = new SqlCommandBuilder()
                .WithCatalogPattern("O'Reilly");
            var sql = builder.BuildShowCatalogs();

            Assert.Contains("'O''Reilly'", sql);
        }

        #endregion
    }
}
