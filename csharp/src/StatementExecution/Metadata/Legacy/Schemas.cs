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
        public IEnumerable<SchemaRow> Schemas(MetadataBatches result)
        {
            foreach (var batch in result.Batches)
            {
                // Scoped SHOW SCHEMAS omits the catalog column.
                var schemas = batch.Column(0) as StringArray;
                var catalogs = result.SourceCatalog == null ? batch.Column(1) as StringArray : null;
                if (schemas == null) continue;
                for (int row = 0; row < batch.Length; row++)
                {
                    string? schema = String(schemas, row);
                    if (schema != null)
                        yield return new SchemaRow(String(catalogs, row) ?? result.SourceCatalog ?? "", schema);
                }
            }
        }
    }
}
