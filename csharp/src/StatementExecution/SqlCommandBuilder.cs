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
    /// SQL command builder for efficient metadata queries using Unity Catalog syntax.
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

        // Building blocks for Unity Catalog syntax
        private const string InCatalogFormat = " IN CATALOG {0}";
        private const string InSchemaFormat = " IN SCHEMA {0}";
        private const string InTableFormat = " IN TABLE {0}";
        private const string InAllCatalogs = " IN ALL CATALOGS";

        // Pattern matching clauses
        private const string LikeFormat = " LIKE '{0}'";
        private const string SchemaLikeFormat = " SCHEMA" + LikeFormat;
        private const string TableLikeFormat = " TABLE" + LikeFormat;

        private string? _catalog;
        private string? _schema;
        private string? _table;
        private string? _catalogPattern;
        private string? _schemaPattern;
        private string? _tablePattern;
        private string? _columnPattern;
        private readonly Func<string, string> _quoter;
        private readonly Func<string, string> _patternEscaper;

        /// <summary>
        /// Initializes a new instance of the SqlCommandBuilder class.
        /// </summary>
        /// <param name="quoter">Function to quote identifiers (default: backtick quoting)</param>
        /// <param name="patternEscaper">Function to escape SQL patterns (default: no escaping)</param>
        public SqlCommandBuilder(
            Func<string, string>? quoter = null,
            Func<string, string>? patternEscaper = null)
        {
            _quoter = quoter ?? (s => $"`{s}`");
            _patternEscaper = patternEscaper ?? (s => s);
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
        /// Sets the schema for the SQL command.
        /// </summary>
        public SqlCommandBuilder WithSchema(string? schema)
        {
            _schema = schema;
            return this;
        }

        /// <summary>
        /// Sets the table for the SQL command.
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
        /// Builds SHOW CATALOGS command.
        /// </summary>
        /// <returns>SQL command string</returns>
        public string BuildShowCatalogs()
        {
            var sql = new StringBuilder(ShowCatalogs);

            if (!string.IsNullOrEmpty(_catalogPattern))
            {
                sql.Append(string.Format(LikeFormat, _patternEscaper(_catalogPattern)));
            }

            return sql.ToString();
        }

        public string BuildShowSchemas()
        {
            var sql = new StringBuilder(ShowSchemas);

            if (string.IsNullOrEmpty(_catalog))
            {
                sql.Append(InAllCatalogs);
            }
            else
            {
                sql.Append($" IN {_quoter(_catalog)}");
            }

            var schemaFilter = _schema ?? _schemaPattern;
            if (!string.IsNullOrEmpty(schemaFilter))
            {
                sql.Append(string.Format(LikeFormat, _patternEscaper(schemaFilter)));
            }

            return sql.ToString();
        }

        public string BuildShowTables()
        {
            var sql = new StringBuilder(ShowTables);

            if (string.IsNullOrEmpty(_catalog))
            {
                sql.Append(InAllCatalogs);
            }
            else
            {
                sql.Append(string.Format(InCatalogFormat, _quoter(_catalog)));
            }

            var schemaFilter = _schema ?? _schemaPattern;
            if (!string.IsNullOrEmpty(schemaFilter))
            {
                sql.Append(string.Format(SchemaLikeFormat, _patternEscaper(schemaFilter)));
            }

            if (!string.IsNullOrEmpty(_tablePattern))
            {
                sql.Append(string.Format(LikeFormat, _patternEscaper(_tablePattern)));
            }

            return sql.ToString();
        }

        public string BuildShowColumns(string? catalogOverride = null)
        {
            var effectiveCatalog = catalogOverride ?? _catalog;

            if (string.IsNullOrEmpty(effectiveCatalog))
            {
                throw new NotSupportedException("SHOW COLUMNS requires a catalog");
            }

            var sql = new StringBuilder(ShowColumns);
            sql.Append(string.Format(InCatalogFormat, _quoter(effectiveCatalog)));

            var schemaFilter = _schema ?? _schemaPattern;
            if (!string.IsNullOrEmpty(schemaFilter))
            {
                sql.Append(string.Format(SchemaLikeFormat, _patternEscaper(schemaFilter)));
            }

            var tableFilter = _table ?? _tablePattern;
            if (!string.IsNullOrEmpty(tableFilter))
            {
                sql.Append(string.Format(TableLikeFormat, _patternEscaper(tableFilter)));
            }

            if (!string.IsNullOrEmpty(_columnPattern) && _columnPattern != "%")
            {
                sql.Append(string.Format(LikeFormat, _patternEscaper(_columnPattern)));
            }

            return sql.ToString();
        }

        /// <summary>
        /// Builds SHOW KEYS command for primary keys.
        /// </summary>
        /// <returns>SQL command string</returns>
        public string BuildShowPrimaryKeys()
        {
            if (string.IsNullOrEmpty(_catalog) || string.IsNullOrEmpty(_schema) || string.IsNullOrEmpty(_table))
            {
                throw new ArgumentException("SHOW KEYS requires catalog, schema, and table");
            }

            var sql = new StringBuilder(ShowKeys);
            sql.Append(string.Format(InCatalogFormat, _quoter(_catalog)));
            sql.Append(string.Format(InSchemaFormat, _quoter(_schema)));
            sql.Append(string.Format(InTableFormat, _quoter(_table)));

            return sql.ToString();
        }

        /// <summary>
        /// Builds SHOW FOREIGN KEYS command.
        /// </summary>
        /// <returns>SQL command string</returns>
        public string BuildShowForeignKeys()
        {
            if (string.IsNullOrEmpty(_catalog) || string.IsNullOrEmpty(_schema) || string.IsNullOrEmpty(_table))
            {
                throw new ArgumentException("SHOW FOREIGN KEYS requires catalog, schema, and table");
            }

            var sql = new StringBuilder(ShowForeignKeys);
            sql.Append(string.Format(InCatalogFormat, _quoter(_catalog)));
            sql.Append(string.Format(InSchemaFormat, _quoter(_schema)));
            sql.Append(string.Format(InTableFormat, _quoter(_table)));

            return sql.ToString();
        }

        /// <summary>
        /// Builds DESCRIBE TABLE command.
        /// </summary>
        /// <returns>SQL command string</returns>
        public string BuildDescribeTable()
        {
            if (string.IsNullOrEmpty(_table))
            {
                throw new ArgumentException("DESCRIBE TABLE requires a table name");
            }

            var parts = new System.Collections.Generic.List<string>();

            if (!string.IsNullOrEmpty(_catalog))
                parts.Add(_quoter(_catalog));

            if (!string.IsNullOrEmpty(_schema))
                parts.Add(_quoter(_schema));

            parts.Add(_quoter(_table));

            return $"{DescribeTable} {string.Join(".", parts)}";
        }

    }
}