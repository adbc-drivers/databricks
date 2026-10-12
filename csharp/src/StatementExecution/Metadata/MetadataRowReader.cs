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
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.Hive2;
using Apache.Arrow;
using Apache.Arrow.Adbc;

namespace AdbcDrivers.Databricks.StatementExecution.Metadata
{
    // The only place that branches on the result format, plus the rules both formats share.
    internal static class MetadataRowReader
    {
        // GetObjects ordinals are one-based on both formats, like Thrift's PositionRequiredOffset.
        internal const int GetObjectsOrdinalOffset = 1;

        internal static IMetadataDecoder Decoder(MetadataBatches result)
            => result.IsNative ? Native.MetadataDecoder.Instance : Legacy.MetadataDecoder.Instance;

        internal static IMetadataResultBuilder ResultBuilder(MetadataBatches result) => ResultBuilder(result.IsNative);

        private static IMetadataResultBuilder ResultBuilder(bool isNative)
            => isNative ? Native.MetadataResultBuilder.Instance : Legacy.MetadataResultBuilder.Instance;

        internal static IEnumerable<string> Catalogs(MetadataBatches result) => Decoder(result).Catalogs(result);

        internal static IEnumerable<SchemaRow> Schemas(MetadataBatches result) => Decoder(result).Schemas(result);

        internal static List<TableRow> Tables(MetadataBatches result, IReadOnlyCollection<string>? tableTypes = null)
            => Decoder(result).Tables(result).Where(row => MatchesTableType(tableTypes, row.TableType)).ToList();

        internal static IEnumerable<ColumnRow> Columns(IEnumerable<MetadataBatches> results,
            bool requireTableIdentifiers = false)
            => ColumnGroups(results, requireTableIdentifiers).SelectMany(rows => rows);

        internal static IEnumerable<IReadOnlyList<ColumnRow>> ColumnGroups(IEnumerable<MetadataBatches> results,
            bool requireTableIdentifiers = false)
        {
            var positions = new Dictionary<string, int>();
            foreach (var result in results)
            {
                var group = new List<ColumnRow>();
                foreach (var row in Decoder(result).Columns(result))
                {
                    // GetObjects skips incomplete rows before assigning ordinals.
                    if (requireTableIdentifiers && (row.Catalog == null || row.Schema == null ||
                        row.Table == null || string.IsNullOrEmpty(row.Name))) continue;
                    string key = $"{row.Catalog}.{row.Schema}.{row.Table}";
                    positions.TryGetValue(key, out int position);
                    positions[key] = position + 1;
                    group.Add(row.HasOrdinal ? row : row.WithOrdinal(position));
                }
                yield return group;
            }
        }

        internal static async Task<QueryResult> ColumnsResultAsync(
            ColumnMetadataResult columns, CancellationToken cancellationToken)
        {
            if (columns.IsNative || !columns.Results.Any(result => result.IsNative && result.Batches.Count > 0))
                return ResultBuilder(columns.IsNative).Columns(columns.Results, columns.Rows);

            // Build each fan-out response in its own format so a SHOW fallback cannot discard native fields.
            var schema = MetadataSchemaFactory.CreateColumnMetadataSchema();
            var batches = new List<RecordBatch>();
            try
            {
                for (int index = 0; index < columns.Results.Count; index++)
                {
                    var result = columns.Results[index];
                    var flat = ResultBuilder(result).Columns(new[] { result }, columns.RowsByResult[index]);
                    using var stream = flat.Stream!;
                    var batch = await stream.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false);
                    if (batch != null) batches.Add(batch);
                }

                var arrays = Enumerable.Range(0, schema.FieldsList.Count)
                    .Select(column => ArrowArrayFactory.BuildArray(ArrayDataConcatenator.Concatenate(
                        batches.Select(batch => batch.Column(column).Data).ToList())!))
                    .ToArray();
                return new QueryResult(batches.Sum(batch => batch.Length), new HiveInfoArrowStream(schema, arrays));
            }
            finally
            {
                foreach (var batch in batches) batch.Dispose();
            }
        }

        internal static string DefaultTableType(string? value) => string.IsNullOrEmpty(value) ? "TABLE" : value!;

        internal static bool MatchesTableType(IReadOnlyCollection<string>? tableTypes, string tableType)
            => tableTypes == null || tableTypes.Contains(tableType);

        internal static bool MatchesParent(string? catalog, string? schema, string? table,
            string? actualCatalog, string? actualSchema, string? actualTable)
            => CatalogFilter.Exact(catalog).Matches(actualCatalog) &&
                CatalogFilter.Exact(schema).Matches(actualSchema) &&
                CatalogFilter.Exact(table).Matches(actualTable);
    }
}
