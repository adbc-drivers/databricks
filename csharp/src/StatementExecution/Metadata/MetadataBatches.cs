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
using System.Linq;
using Apache.Arrow;

namespace AdbcDrivers.Databricks.StatementExecution.Metadata
{
    internal sealed class MetadataBatches
    {
        internal MetadataBatches(List<RecordBatch> batches, bool isNative,
            string? sourceCatalog = null, CatalogFilter catalogFilter = default)
        {
            Batches = batches;
            IsNative = isNative;
            SourceCatalog = sourceCatalog;
            CatalogFilter = catalogFilter;
        }

        internal List<RecordBatch> Batches { get; }
        internal bool IsNative { get; }
        internal string? SourceCatalog { get; }
        internal CatalogFilter CatalogFilter { get; }
    }

    internal sealed class ColumnMetadataResult
    {
        internal ColumnMetadataResult(IReadOnlyList<MetadataBatches> results, bool requireTableIdentifiers = false)
        {
            Results = results;
            IsNative = results.Any(result => result.Batches.Count > 0) &&
                results.Where(result => result.Batches.Count > 0).All(result => result.IsNative);
            RowsByResult = MetadataRowReader.ColumnGroups(results, requireTableIdentifiers).ToList();
            Rows = RowsByResult.SelectMany(rows => rows).ToList();
        }

        internal IReadOnlyList<MetadataBatches> Results { get; }
        internal bool IsNative { get; }
        internal IReadOnlyList<IReadOnlyList<ColumnRow>> RowsByResult { get; }
        internal IReadOnlyList<ColumnRow> Rows { get; }
    }
}
