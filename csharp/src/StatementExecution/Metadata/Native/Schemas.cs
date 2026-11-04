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
using AdbcDrivers.HiveServer2.Hive2;

namespace AdbcDrivers.Databricks.StatementExecution.Metadata.Native
{
    internal sealed partial class MetadataDecoder
    {
        public IEnumerable<SchemaRow> Schemas(MetadataBatches result)
        {
            foreach (var row in NativeMetadataRow.Read(result,
                MetadataSchemaFactory.CreateSchemasSchema(), MetadataOperation.GetSchemas))
                if (row.String("TABLE_SCHEM") is string schema)
                    yield return new SchemaRow(row.String("TABLE_CATALOG") ?? row.SourceCatalog ?? "", schema);
        }
    }
}
