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
using System.Text;
using Apache.Arrow.Adbc;

namespace AdbcDrivers.Databricks.StatementExecution
{
    /// <summary>
    /// SQL command builder for metadata queries.
    /// Handles two types of queries:
    /// 1. Pattern-based queries (SHOW SCHEMAS/TABLES/COLUMNS) - use LIKE with glob patterns
    /// 2. Exact-identifier queries (SHOW KEYS/FOREIGN KEYS) - use IN SCHEMA/IN TABLE syntax
    /// </summary>
    public class SqlCommandBuilder
    {
        // SQL command constants
        private const string ShowCatalogs = "SHOW CATALOGS";
        private const string ShowSchemas = "SHOW SCHEMAS";
        private const string ShowTables = "SHOW TABLES";
        private const string ShowColumns = "SHOW COLUMNS";
        private const string ShowKeys = "SHOW KEYS";
        private const string ShowForeignKeys = "SHOW FOREIGN KEYS";
        private const string ShowTableTypes = "SHOW TABLE_TYPES";
        private const string DescribeTable = "DESCRIBE TABLE";

        private const string InCatalogFormat = " IN CATALOG {0}";
        private const string InSchemaFormat = " IN SCHEMA {0}";
        private const string InTableFormat = " IN TABLE {0}";
        private const string InAllCatalogs = " IN ALL CATALOGS";

        // Pattern matching clauses
        private const string LikeFormat = " LIKE '{0}'";
        private const string SchemaLikeFormat = " SCHEMA" + LikeFormat;
        private const string TableLikeFormat = " TABLE" + LikeFormat;

        // Exact identifiers (for keys-related queries using IN syntax)
        private string? _catalog;
        private string? _schema;
        private string? _table;

        // Patterns (for SHOW queries using LIKE syntax)
        private string? _catalogPattern;
        private string? _schemaPattern;
        private string? _tablePattern;
        private string? _columnPattern;

        /// <summary>
        /// Initializes a new instance of the SqlCommandBuilder class.
        /// Uses MetadataUtilities for identifier quoting and pattern conversion.
        /// </summary>
        public SqlCommandBuilder()
        {
        }

        /// <summary>
        /// Sets the catalog for the SQL command.
        /// </summary>
        public SqlCommandBuilder WithCatalog(string? catalog)
        {
            _catalog = catalog;
            return this;
        }

        /// <summary>
        /// Sets the exact schema identifier for the SQL command.
        /// Use this for exact-match queries like SHOW KEYS, SHOW FOREIGN KEYS.
        /// For pattern-based queries, use WithSchemaPattern instead.
        /// </summary>
        public SqlCommandBuilder WithSchema(string? schema)
        {
            _schema = schema;
            return this;
        }

        /// <summary>
        /// Sets the exact table identifier for the SQL command.
        /// Use this for exact-match queries like SHOW KEYS, SHOW FOREIGN KEYS.
        /// For pattern-based queries, use WithTablePattern instead.
        /// </summary>
        public SqlCommandBuilder WithTable(string? table)
        {
            _table = table;
            return this;
        }

        /// <summary>
        /// Sets the catalog pattern for LIKE matching.
        /// </summary>
        public SqlCommandBuilder WithCatalogPattern(string? pattern)
        {
            _catalogPattern = pattern;
            return this;
        }

        /// <summary>
        /// Sets the schema pattern for LIKE matching.
        /// </summary>
        public SqlCommandBuilder WithSchemaPattern(string? pattern)
        {
            _schemaPattern = pattern;
            return this;
        }

        /// <summary>
        /// Sets the table pattern for LIKE matching.
        /// </summary>
        public SqlCommandBuilder WithTablePattern(string? pattern)
        {
            _tablePattern = pattern;
            return this;
        }

        /// <summary>
        /// Sets the column pattern for LIKE matching.
        /// </summary>
        public SqlCommandBuilder WithColumnPattern(string? pattern)
        {
            _columnPattern = pattern;
            return this;
        }

        /// <summary>
        /// Builds SHOW CATALOGS command with optional pattern.
        /// Uses LIKE syntax for pattern matching.
        /// </summary>
        /// <returns>SQL command string</returns>
        public string BuildShowCatalogs()
        {
            var sql = new StringBuilder(ShowCatalogs);

            if (_catalogPattern != null)
            {
                var escapedPattern = MetadataUtilities.ConvertAdbcPatternToDatabricksGlob(_catalogPattern);
                sql.Append(string.Format(LikeFormat, escapedPattern));
            }

            return sql.ToString();
        }

        /// <summary>
        /// Builds SHOW SCHEMAS command.
        /// Uses patterns with LIKE syntax, supports IN ALL CATALOGS.
        /// </summary>
        /// <returns>SQL command string</returns>
        public string BuildShowSchemas()
        {
            var sql = new StringBuilder(ShowSchemas);

            // Handle catalog scope
            if (_catalog == null)
            {
                sql.Append(InAllCatalogs);
            }
            else
            {
                sql.Append($" IN {MetadataUtilities.QuoteIdentifier(_catalog)}");
            }

            // Apply schema pattern filter
            if (_schemaPattern != null)
            {
                var escapedPattern = MetadataUtilities.ConvertAdbcPatternToDatabricksGlob(_schemaPattern);
                sql.Append(string.Format(LikeFormat, escapedPattern));
            }

            return sql.ToString();
        }

        /// <summary>
        /// Builds SHOW TABLES command.
        /// Uses patterns with LIKE syntax for schema and table filtering.
        /// </summary>
        /// <returns>SQL command string</returns>
        public string BuildShowTables()
        {
            var sql = new StringBuilder(ShowTables);

            // Handle catalog scope
            if (_catalog == null)
            {
                sql.Append(InAllCatalogs);
            }
            else
            {
                sql.Append(string.Format(InCatalogFormat, MetadataUtilities.QuoteIdentifier(_catalog)));
            }

            // Apply schema pattern
            if (_schemaPattern != null)
            {
                var escapedPattern = MetadataUtilities.ConvertAdbcPatternToDatabricksGlob(_schemaPattern);
                sql.Append(string.Format(SchemaLikeFormat, escapedPattern));
            }

            // Apply table pattern
            if (_tablePattern != null)
            {
                var escapedPattern = MetadataUtilities.ConvertAdbcPatternToDatabricksGlob(_tablePattern);
                sql.Append(string.Format(LikeFormat, escapedPattern));
            }

            return sql.ToString();
        }

        /// <summary>
        /// Builds SHOW COLUMNS command.
        /// Uses patterns with LIKE syntax for schema, table, and column filtering.
        /// </summary>
        /// <param name="catalogOverride">Optional catalog override</param>
        /// <returns>SQL command string</returns>
        public string BuildShowColumns(string? catalogOverride = null)
        {
            var effectiveCatalog = catalogOverride ?? _catalog;
            var sql = new StringBuilder(ShowColumns);

            // Handle catalog scope
            if (effectiveCatalog == null)
            {
                sql.Append(InAllCatalogs);
            }
            else
            {
                sql.Append(string.Format(InCatalogFormat, MetadataUtilities.QuoteIdentifier(effectiveCatalog)));
            }

            // Apply schema pattern
            if (_schemaPattern != null)
            {
                var escapedPattern = MetadataUtilities.ConvertAdbcPatternToDatabricksGlob(_schemaPattern);
                sql.Append(string.Format(SchemaLikeFormat, escapedPattern));
            }

            // Apply table pattern
            if (_tablePattern != null)
            {
                var escapedPattern = MetadataUtilities.ConvertAdbcPatternToDatabricksGlob(_tablePattern);
                sql.Append(string.Format(TableLikeFormat, escapedPattern));
            }

            // Apply column pattern (skip wildcard-only pattern)
            if (_columnPattern != null && _columnPattern != "%")
            {
                var escapedPattern = MetadataUtilities.ConvertAdbcPatternToDatabricksGlob(_columnPattern);
                sql.Append(string.Format(LikeFormat, escapedPattern));
            }

            return sql.ToString();
        }

        /// <summary>
        /// Builds SHOW KEYS command for primary keys.
        /// Uses exact identifiers with IN CATALOG/IN SCHEMA/IN TABLE syntax.
        /// DOES NOT support patterns - keys queries require exact identifiers.
        /// </summary>
        /// <returns>SQL command string</returns>
        /// <exception cref="ArgumentException">Thrown when catalog, schema, or table is not set</exception>
        public string BuildShowPrimaryKeys()
        {
            if (string.IsNullOrEmpty(_catalog) || string.IsNullOrEmpty(_schema) || string.IsNullOrEmpty(_table))
            {
                throw new ArgumentException("SHOW KEYS requires exact catalog, schema, and table (no patterns supported)");
            }

            var sql = new StringBuilder(ShowKeys);
            sql.Append(string.Format(InCatalogFormat, MetadataUtilities.QuoteIdentifier(_catalog!)));
            sql.Append(string.Format(InSchemaFormat, MetadataUtilities.QuoteIdentifier(_schema!)));
            sql.Append(string.Format(InTableFormat, MetadataUtilities.QuoteIdentifier(_table!)));

            return sql.ToString();
        }

        /// <summary>
        /// Builds SHOW FOREIGN KEYS command.
        /// Uses exact identifiers with IN CATALOG/IN SCHEMA/IN TABLE syntax.
        /// DOES NOT support patterns - keys queries require exact identifiers.
        /// </summary>
        /// <returns>SQL command string</returns>
        /// <exception cref="ArgumentException">Thrown when catalog, schema, or table is not set</exception>
        public string BuildShowForeignKeys()
        {
            if (string.IsNullOrEmpty(_catalog) || string.IsNullOrEmpty(_schema) || string.IsNullOrEmpty(_table))
            {
                throw new ArgumentException("SHOW FOREIGN KEYS requires exact catalog, schema, and table (no patterns supported)");
            }

            var sql = new StringBuilder(ShowForeignKeys);
            sql.Append(string.Format(InCatalogFormat, MetadataUtilities.QuoteIdentifier(_catalog!)));
            sql.Append(string.Format(InSchemaFormat, MetadataUtilities.QuoteIdentifier(_schema!)));
            sql.Append(string.Format(InTableFormat, MetadataUtilities.QuoteIdentifier(_table!)));

            return sql.ToString();
        }
    }
}
