/*
 * Copyright (c) 2026 ADBC Drivers Contributors
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
using System.Collections.Generic;
using Apache.Arrow;

namespace AdbcDrivers.Databricks.StatementExecution.Metadata.Legacy
{
    internal sealed partial class MetadataDecoder
    {
        public IEnumerable<ColumnRow> Columns(MetadataBatches result)
        {
            foreach (var batch in result.Batches)
            {
                var catalogs = TryGetColumn<StringArray>(batch, "catalogName");
                var schemas = TryGetColumn<StringArray>(batch, "namespace");
                var tables = TryGetColumn<StringArray>(batch, "tableName");
                var names = TryGetColumn<StringArray>(batch, "col_name");
                var types = TryGetColumn<StringArray>(batch, "columnType");
                var nullable = TryGetColumn<StringArray>(batch, "isNullable");
                if (names == null || types == null) continue;
                for (int row = 0; row < batch.Length; row++)
                {
                    string? name = String(names, row);
                    string? typeName = String(types, row);
                    if (name == null || typeName == null) continue;
                    // SHOW COLUMNS has no ordinal, default, or auto-increment column.
                    yield return new ColumnRow(String(catalogs, row), String(schemas, row), String(tables, row),
                        name, typeName,
                        !string.Equals(String(nullable, row), "false", StringComparison.OrdinalIgnoreCase),
                        ordinal: null, columnDefault: null, isAutoIncrement: false);
                }
            }
        }
    }
}
