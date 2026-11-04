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
        // SHOW-only: native primary keys go straight to the native result builder.
        internal IEnumerable<PrimaryKeyRow> PrimaryKeys(
            MetadataBatches result, string catalog, string schema, string table)
        {
            int sequence = 0;
            foreach (var batch in result.Batches)
            {
                var columns = TryGetColumn<StringArray>(batch, "col_name");
                var names = TryGetColumn<StringArray>(batch, "constraintName");
                var sequences = TryGetColumn<Int32Array>(batch, "keySeq");
                var catalogs = TryGetColumn<StringArray>(batch, "catalogName");
                var schemas = TryGetColumn<StringArray>(batch, "namespace");
                var tables = TryGetColumn<StringArray>(batch, "tableName");
                if (columns == null) continue;
                for (int row = 0; row < batch.Length; row++)
                    if (String(columns, row) is string column)
                        yield return new PrimaryKeyRow(String(catalogs, row) ?? catalog,
                            String(schemas, row) ?? schema, String(tables, row) ?? table,
                            column, Integer(sequences, row) ?? ++sequence, String(names, row) ?? "");
            }
        }
    }
}
