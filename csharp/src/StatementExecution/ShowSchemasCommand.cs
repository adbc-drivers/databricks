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
    internal class ShowSchemasCommand : MetadataCommandBase
    {
        private readonly string? _catalog;
        private readonly string? _schemaPattern;

        public ShowSchemasCommand(string? catalog, string? schemaPattern = null)
        {
            _catalog = catalog;
            _schemaPattern = schemaPattern;
        }

        public override string Build()
        {
            var sql = new StringBuilder("SHOW SCHEMAS");
            if (_catalog == null)
                sql.Append(InAllCatalogs);
            else
                sql.Append($" IN {QuoteIdentifier(_catalog)}");
            if (_schemaPattern != null)
                sql.Append(string.Format(LikeFormat, ConvertPattern(_schemaPattern)));
            return sql.ToString();
        }
    }
}
