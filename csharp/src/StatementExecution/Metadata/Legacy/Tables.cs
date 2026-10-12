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

using System.Collections.Generic;
using Apache.Arrow;

namespace AdbcDrivers.Databricks.StatementExecution.Metadata.Legacy
{
    internal sealed partial class MetadataDecoder
    {
        public IEnumerable<TableRow> Tables(MetadataBatches result) => ShowTables(result, normalizeEmptyTableType: true);

        // GetObjects reports an empty SHOW table type as TABLE; flat GetTables keeps it.
        internal static IEnumerable<TableRow> ShowTables(MetadataBatches result, bool normalizeEmptyTableType)
        {
            foreach (var batch in result.Batches)
            {
                var catalogs = TryGetColumn<StringArray>(batch, "catalogName");
                var schemas = TryGetColumn<StringArray>(batch, "namespace");
                var tables = TryGetColumn<StringArray>(batch, "tableName");
                var types = TryGetColumn<StringArray>(batch, "tableType");
                var remarks = TryGetColumn<StringArray>(batch, "remarks");
                if (catalogs == null || schemas == null || tables == null) continue;

                for (int row = 0; row < batch.Length; row++)
                {
                    string? catalog = String(catalogs, row);
                    string? schema = String(schemas, row);
                    string? table = String(tables, row);
                    if (catalog == null || schema == null || table == null) continue;
                    string? serverType = String(types, row);
                    string type = normalizeEmptyTableType
                        ? MetadataRowReader.DefaultTableType(serverType)
                        : serverType ?? "TABLE";
                    yield return new TableRow(catalog, schema, table, type, String(remarks, row) ?? "");
                }
            }
        }
    }
}
