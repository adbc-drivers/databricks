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
        public IEnumerable<ColumnRow> Columns(MetadataBatches result)
        {
            foreach (var row in NativeMetadataRow.Read(result,
                MetadataSchemaFactory.CreateColumnMetadataSchema(), MetadataOperation.GetColumns))
            {
                string? name = row.String("COLUMN_NAME");
                string? typeName = row.String("TYPE_NAME");
                if (name == null || typeName == null) continue;
                // Native ORDINAL_POSITION is zero-based, like the SHOW numbering.
                yield return new ColumnRow(row.String("TABLE_CAT") ?? row.SourceCatalog, row.String("TABLE_SCHEM"),
                    row.String("TABLE_NAME"), name, typeName, row.Integer("NULLABLE") == 1,
                    checked((int)(row.Integer("ORDINAL_POSITION") ?? 0)),
                    row.String("COLUMN_DEF"),
                    string.Equals(row.String("IS_AUTO_INCREMENT"), "YES", StringComparison.OrdinalIgnoreCase));
            }
        }
    }
}
