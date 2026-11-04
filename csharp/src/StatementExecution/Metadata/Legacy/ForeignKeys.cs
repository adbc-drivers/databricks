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
        // SHOW-only: native foreign keys go straight to the native result builder.
        internal IEnumerable<ForeignKeyRow> ForeignKeys(
            MetadataBatches result, string? parentCatalog, string? parentSchema, string? parentTable,
            string catalog, string schema, string table)
        {
            int sequence = 0;
            foreach (var batch in result.Batches)
            {
                var parentCatalogs = TryGetColumn<StringArray>(batch, "parentCatalogName");
                var parentSchemas = TryGetColumn<StringArray>(batch, "parentNamespace");
                var parentTables = TryGetColumn<StringArray>(batch, "parentTableName");
                var parentColumns = TryGetColumn<StringArray>(batch, "parentColName");
                var catalogs = TryGetColumn<StringArray>(batch, "catalogName");
                var schemas = TryGetColumn<StringArray>(batch, "namespace");
                var tables = TryGetColumn<StringArray>(batch, "tableName");
                var columns = TryGetColumn<StringArray>(batch, "col_name");
                var names = TryGetColumn<StringArray>(batch, "constraintName");
                var sequences = TryGetColumn<Int32Array>(batch, "keySeq");
                var updateRules = TryGetColumn<Int32Array>(batch, "updateRule");
                var deleteRules = TryGetColumn<Int32Array>(batch, "deleteRule");
                var deferrabilities = TryGetColumn<Int32Array>(batch, "deferrability");
                if (columns == null) continue;
                for (int row = 0; row < batch.Length; row++)
                {
                    string? column = String(columns, row);
                    if (column == null) continue;
                    string? rowParentCatalog = String(parentCatalogs, row);
                    string? rowParentSchema = String(parentSchemas, row);
                    string? rowParentTable = String(parentTables, row);
                    // Match raw server identifiers before applying output fallbacks.
                    if (!MetadataRowReader.MatchesParent(parentCatalog, parentSchema, parentTable,
                        rowParentCatalog, rowParentSchema, rowParentTable)) continue;
                    yield return new ForeignKeyRow(rowParentCatalog ?? parentCatalog ?? "",
                        rowParentSchema ?? parentSchema ?? "", rowParentTable ?? parentTable ?? "",
                        String(parentColumns, row) ?? "", String(catalogs, row) ?? catalog,
                        String(schemas, row) ?? schema, String(tables, row) ?? table, column,
                        Integer(sequences, row) ?? ++sequence, Integer(updateRules, row) ?? 0,
                        Integer(deleteRules, row) ?? 0, String(names, row) ?? "", null,
                        Integer(deferrabilities, row) ?? 5);
                }
            }
        }
    }
}
