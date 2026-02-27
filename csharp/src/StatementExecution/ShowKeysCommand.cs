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
using System.Text;

namespace AdbcDrivers.Databricks.StatementExecution
{
    internal class ShowKeysCommand : MetadataCommandBase
    {
        private readonly string _catalog;
        private readonly string _schema;
        private readonly string _table;

        public ShowKeysCommand(string catalog, string schema, string table)
        {
            _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _table = table ?? throw new ArgumentNullException(nameof(table));
        }

        public override string Build()
        {
            var sql = new StringBuilder("SHOW KEYS");
            sql.Append(string.Format(InCatalogFormat, QuoteIdentifier(_catalog)));
            sql.Append(string.Format(InSchemaFormat, QuoteIdentifier(_schema)));
            sql.Append(string.Format(InTableFormat, QuoteIdentifier(_table)));
            return sql.ToString();
        }
    }

    internal class ShowForeignKeysCommand : MetadataCommandBase
    {
        private readonly string _catalog;
        private readonly string _schema;
        private readonly string _table;

        public ShowForeignKeysCommand(string catalog, string schema, string table)
        {
            _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _table = table ?? throw new ArgumentNullException(nameof(table));
        }

        public override string Build()
        {
            var sql = new StringBuilder("SHOW FOREIGN KEYS");
            sql.Append(string.Format(InCatalogFormat, QuoteIdentifier(_catalog)));
            sql.Append(string.Format(InSchemaFormat, QuoteIdentifier(_schema)));
            sql.Append(string.Format(InTableFormat, QuoteIdentifier(_table)));
            return sql.ToString();
        }
    }
}
