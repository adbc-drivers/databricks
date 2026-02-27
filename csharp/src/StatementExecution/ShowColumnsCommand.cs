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

using System.Text;

namespace AdbcDrivers.Databricks.StatementExecution
{
    internal class ShowColumnsCommand : MetadataCommandBase
    {
        private readonly string? _catalog;
        private readonly string? _schemaPattern;
        private readonly string? _tablePattern;
        private readonly string? _columnPattern;

        public ShowColumnsCommand(string? catalog, string? schemaPattern = null,
            string? tablePattern = null, string? columnPattern = null)
        {
            _catalog = catalog;
            _schemaPattern = schemaPattern;
            _tablePattern = tablePattern;
            _columnPattern = columnPattern;
        }

        public override string Build()
        {
            var sql = new StringBuilder("SHOW COLUMNS");
            AppendCatalogScope(sql, _catalog);
            if (_schemaPattern != null)
                sql.Append(string.Format(SchemaLikeFormat, ConvertPattern(_schemaPattern)));
            if (_tablePattern != null)
                sql.Append(string.Format(TableLikeFormat, ConvertPattern(_tablePattern)));
            if (_columnPattern != null && _columnPattern != "%")
                sql.Append(string.Format(LikeFormat, ConvertPattern(_columnPattern)));
            return sql.ToString();
        }
    }
}
