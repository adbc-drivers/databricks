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
using AdbcDrivers.HiveServer2.Hive2;

namespace AdbcDrivers.Databricks.StatementExecution.Metadata.Native
{
    internal sealed partial class MetadataDecoder
    {
        public IEnumerable<TableRow> Tables(MetadataBatches result, IReadOnlyCollection<string>? tableTypes)
        {
            foreach (var row in TableRows(result, tableTypes))
            {
                string? schema = row.String("TABLE_SCHEM");
                string? table = row.String("TABLE_NAME");
                if (schema == null || table == null) continue;
                yield return new TableRow(row.String("TABLE_CAT") ?? row.SourceCatalog ?? "", schema, table,
                    MetadataRowReader.DefaultTableType(row.String("TABLE_TYPE")), row.String("REMARKS") ?? "");
            }
        }

        internal static List<NativeMetadataRow> TableRows(
            MetadataBatches result, IReadOnlyCollection<string>? tableTypes)
        {
            var rows = NativeMetadataRow.Read(result,
                MetadataSchemaFactory.CreateTablesSchema(), MetadataOperation.GetTables);
            if (tableTypes != null)
                rows.RemoveAll(row => !MetadataRowReader.MatchesTableType(
                    tableTypes, MetadataRowReader.DefaultTableType(row.String("TABLE_TYPE"))));
            rows.Sort(CompareTables);
            return rows;
        }

        // Thrift orders tables by type, catalog, schema, then name.
        private static int CompareTables(NativeMetadataRow left, NativeMetadataRow right)
        {
            int order = StringComparer.Ordinal.Compare(
                MetadataRowReader.DefaultTableType(left.String("TABLE_TYPE")),
                MetadataRowReader.DefaultTableType(right.String("TABLE_TYPE")));
            if (order != 0) return order;
            order = StringComparer.Ordinal.Compare(
                left.String("TABLE_CAT") ?? left.SourceCatalog, right.String("TABLE_CAT") ?? right.SourceCatalog);
            if (order != 0) return order;
            order = StringComparer.Ordinal.Compare(left.String("TABLE_SCHEM"), right.String("TABLE_SCHEM"));
            return order != 0 ? order : StringComparer.Ordinal.Compare(left.String("TABLE_NAME"), right.String("TABLE_NAME"));
        }
    }
}
