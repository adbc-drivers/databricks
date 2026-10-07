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
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.Hive2;
using Apache.Arrow;

namespace AdbcDrivers.Databricks.StatementExecution
{
    internal sealed class MetadataBatches
    {
        internal MetadataBatches(List<RecordBatch> batches, bool isNative,
            string? sourceCatalog = null, bool requireExactCatalog = false)
        {
            Batches = batches;
            IsNative = isNative;
            SourceCatalog = sourceCatalog;
            RequireExactCatalog = requireExactCatalog;
        }

        internal List<RecordBatch> Batches { get; }
        internal bool IsNative { get; }
        internal string? SourceCatalog { get; }
        internal bool RequireExactCatalog { get; }
    }

    internal sealed class ColumnMetadataResult
    {
        internal ColumnMetadataResult(IReadOnlyList<MetadataBatches> results)
        {
            Batches = results.SelectMany(result => result.Batches).ToList();
            SourceCatalogs = results.SelectMany(result =>
                Enumerable.Repeat(result.SourceCatalog, result.Batches.Count)).ToList();
            IsNative = Batches.Count > 0 &&
                results.Where(result => result.Batches.Count > 0).All(result => result.IsNative);
            Rows = MetadataRowReader.Columns(results).ToList();
        }

        internal IReadOnlyList<RecordBatch> Batches { get; }
        internal IReadOnlyList<string?> SourceCatalogs { get; }
        internal bool IsNative { get; }
        internal IReadOnlyList<ColumnRow> Rows { get; }
    }

    internal readonly struct SchemaRow
    {
        internal SchemaRow(string catalog, string schema)
        {
            Catalog = catalog;
            Schema = schema;
        }

        internal string Catalog { get; }
        internal string Schema { get; }
    }

    internal readonly struct TableRow
    {
        internal TableRow(string catalog, string schema, string table, string tableType, string remarks)
        {
            Catalog = catalog;
            Schema = schema;
            Table = table;
            TableType = tableType;
            Remarks = remarks;
        }

        internal string Catalog { get; }
        internal string Schema { get; }
        internal string Table { get; }
        internal string TableType { get; }
        internal string Remarks { get; }
    }

    internal readonly struct ColumnRow
    {
        internal ColumnRow(string? catalog, string? schema, string? table, string name,
            string typeName, bool nullable, int ordinal, string? columnDefault, bool isAutoIncrement)
        {
            Catalog = catalog;
            Schema = schema;
            Table = table;
            Name = name;
            TypeName = typeName;
            Nullable = nullable;
            Ordinal = ordinal;
            Default = columnDefault;
            IsAutoIncrement = isAutoIncrement;
        }

        internal string? Catalog { get; }
        internal string? Schema { get; }
        internal string? Table { get; }
        internal string Name { get; }
        internal string TypeName { get; }
        internal bool Nullable { get; }
        internal int Ordinal { get; }
        internal string? Default { get; }
        internal bool IsAutoIncrement { get; }
    }

    internal static class MetadataRowReader
    {
        internal static IEnumerable<string> Catalogs(MetadataBatches result, string? pattern = null)
        {
            foreach (var batch in result.Batches)
            {
                var native = result.IsNative
                    ? new NativeMetadataColumns(batch, MetadataSchemaFactory.CreateCatalogsSchema(), MetadataOperation.GetCatalogs)
                    : null;
                var show = result.IsNative ? null : TryGetColumn<StringArray>(batch, "catalog");
                if (!result.IsNative && show == null) continue;
                for (int row = 0; row < batch.Length; row++)
                {
                    string? catalog = result.IsNative ? native!.String("TABLE_CAT", row) : String(show, row);
                    if (catalog != null && MatchesCatalogPattern(pattern, catalog))
                        yield return catalog;
                }
            }
        }

        internal static IEnumerable<SchemaRow> Schemas(MetadataBatches result, string? catalog)
        {
            foreach (var batch in result.Batches)
            {
                if (result.IsNative)
                {
                    var native = new NativeMetadataColumns(
                        batch, MetadataSchemaFactory.CreateSchemasSchema(), MetadataOperation.GetSchemas);
                    for (int row = 0; row < batch.Length; row++)
                    {
                        string? schema = native.String("TABLE_SCHEM", row);
                        string? rowCatalog = native.String("TABLE_CATALOG", row) ?? catalog;
                        if (schema != null && MatchesCatalog(catalog, rowCatalog))
                            yield return new SchemaRow(rowCatalog ?? "", schema);
                    }
                    continue;
                }

                // Scoped SHOW SCHEMAS omits the catalog column.
                var schemas = batch.Column(0) as StringArray;
                var catalogs = catalog == null ? batch.Column(1) as StringArray : null;
                if (schemas == null) continue;
                for (int row = 0; row < batch.Length; row++)
                {
                    string? schema = String(schemas, row);
                    if (schema != null)
                        yield return new SchemaRow(String(catalogs, row) ?? catalog ?? "", schema);
                }
            }
        }

        internal static List<TableRow> Tables(
            MetadataBatches result, string? catalog, IReadOnlyCollection<string>? tableTypes = null)
        {
            var rows = new List<TableRow>();
            foreach (var batch in result.Batches)
            {
                var native = result.IsNative
                    ? new NativeMetadataColumns(batch, MetadataSchemaFactory.CreateTablesSchema(), MetadataOperation.GetTables)
                    : null;
                var catalogs = result.IsNative ? null : TryGetColumn<StringArray>(batch, "catalogName");
                var schemas = result.IsNative ? null : TryGetColumn<StringArray>(batch, "namespace");
                var tables = result.IsNative ? null : TryGetColumn<StringArray>(batch, "tableName");
                var types = result.IsNative ? null : TryGetColumn<StringArray>(batch, "tableType");
                var remarks = result.IsNative ? null : TryGetColumn<StringArray>(batch, "remarks");
                if (!result.IsNative && (catalogs == null || schemas == null || tables == null)) continue;

                for (int row = 0; row < batch.Length; row++)
                {
                    string? rowCatalog = result.IsNative ? native!.String("TABLE_CAT", row) ?? catalog : String(catalogs, row);
                    string? schema = result.IsNative ? native!.String("TABLE_SCHEM", row) : String(schemas, row);
                    string? table = result.IsNative ? native!.String("TABLE_NAME", row) : String(tables, row);
                    if ((!result.IsNative && rowCatalog == null) || schema == null || table == null) continue;
                    if (result.IsNative && !MatchesCatalog(catalog, rowCatalog)) continue;
                    string type = NativeMetadataResultBuilder.DefaultTableType(
                        result.IsNative ? native!.String("TABLE_TYPE", row) : String(types, row));
                    if (tableTypes != null && !tableTypes.Contains(type)) continue;
                    rows.Add(new TableRow(rowCatalog ?? "", schema, table, type,
                        (result.IsNative ? native!.String("REMARKS", row) : String(remarks, row)) ?? ""));
                }
            }
            // Thrift orders tables by type, catalog, schema, then name.
            if (result.IsNative) rows.Sort(CompareTables);
            return rows;
        }

        internal static IEnumerable<ColumnRow> Columns(IEnumerable<MetadataBatches> results)
        {
            var positions = new Dictionary<string, int>();
            foreach (var result in results)
            {
                foreach (var batch in result.Batches)
                {
                    var rows = result.IsNative ? NativeColumns(batch, result) : ShowColumns(batch, positions);
                    foreach (var row in rows)
                    {
                        string key = $"{row.Catalog}.{row.Schema}.{row.Table}";
                        positions.TryGetValue(key, out int position);
                        positions[key] = position + 1;
                        yield return row;
                    }
                }
            }
        }

        private static IEnumerable<ColumnRow> NativeColumns(RecordBatch batch, MetadataBatches result)
        {
            var columns = new NativeMetadataColumns(
                batch, MetadataSchemaFactory.CreateColumnMetadataSchema(), MetadataOperation.GetColumns);
            for (int row = 0; row < batch.Length; row++)
            {
                string? catalog = columns.String("TABLE_CAT", row) ?? result.SourceCatalog;
                // A quoted catalog can still expand as a native LIKE pattern.
                if (result.RequireExactCatalog && !MatchesCatalog(result.SourceCatalog, catalog)) continue;
                string? name = columns.String("COLUMN_NAME", row);
                string? typeName = columns.String("TYPE_NAME", row);
                if (name == null || typeName == null) continue;
                // Both decoders expose zero-based ordinals; GetObjects adds its required offset.
                yield return new ColumnRow(catalog, columns.String("TABLE_SCHEM", row),
                    columns.String("TABLE_NAME", row), name, typeName, columns.Integer("NULLABLE", row) == 1,
                    checked((int)(columns.Integer("ORDINAL_POSITION", row) ?? 0)),
                    columns.String("COLUMN_DEF", row),
                    string.Equals(columns.String("IS_AUTO_INCREMENT", row), "YES", StringComparison.OrdinalIgnoreCase));
            }
        }

        private static IEnumerable<ColumnRow> ShowColumns(RecordBatch batch, Dictionary<string, int> positions)
        {
            var catalogs = TryGetColumn<StringArray>(batch, "catalogName");
            var schemas = TryGetColumn<StringArray>(batch, "namespace");
            var tables = TryGetColumn<StringArray>(batch, "tableName");
            var names = TryGetColumn<StringArray>(batch, "col_name");
            var types = TryGetColumn<StringArray>(batch, "columnType");
            var nullable = TryGetColumn<StringArray>(batch, "isNullable");
            if (names == null || types == null) yield break;
            for (int row = 0; row < batch.Length; row++)
            {
                string? name = String(names, row);
                string? typeName = String(types, row);
                if (name == null || typeName == null) continue;
                string? catalog = String(catalogs, row);
                string? schema = String(schemas, row);
                string? table = String(tables, row);
                positions.TryGetValue($"{catalog}.{schema}.{table}", out int ordinal);
                yield return new ColumnRow(catalog, schema, table, name, typeName,
                    !string.Equals(String(nullable, row), "false", StringComparison.OrdinalIgnoreCase),
                    ordinal, null, false);
            }
        }

        internal static int CompareTables(TableRow left, TableRow right)
            => CompareTables(
                (left.Catalog, left.Schema, left.Table, left.TableType),
                (right.Catalog, right.Schema, right.Table, right.TableType));

        internal static int CompareTables(
            (string? Catalog, string? Schema, string? Table, string? TableType) left,
            (string? Catalog, string? Schema, string? Table, string? TableType) right)
        {
            int order = StringComparer.Ordinal.Compare(
                NativeMetadataResultBuilder.DefaultTableType(left.TableType),
                NativeMetadataResultBuilder.DefaultTableType(right.TableType));
            if (order != 0) return order;
            order = StringComparer.Ordinal.Compare(left.Catalog, right.Catalog);
            if (order != 0) return order;
            order = StringComparer.Ordinal.Compare(left.Schema, right.Schema);
            return order != 0 ? order : StringComparer.Ordinal.Compare(left.Table, right.Table);
        }

        internal static bool MatchesCatalog(string? requested, string? actual)
            => requested == null || string.Equals(requested, actual, StringComparison.OrdinalIgnoreCase);

        private static string? String(StringArray? values, int row)
            => values == null || values.IsNull(row) ? null : values.GetString(row);

        private static T? TryGetColumn<T>(RecordBatch batch, string name) where T : class, IArrowArray
        {
            try
            {
                return batch.Column(name) as T;
            }
            catch (ArgumentOutOfRangeException)
            {
                return null;
            }
        }

        private static bool MatchesCatalogPattern(string? pattern, string catalog)
        {
            if (pattern == null) return true;
            var regex = new StringBuilder("^");
            bool escaped = false;
            foreach (char character in pattern)
            {
                if (!escaped && character == '\\')
                {
                    escaped = true;
                    continue;
                }
                regex.Append(!escaped && character == '%' ? ".*"
                    : !escaped && character == '_' ? "."
                    : Regex.Escape(character.ToString()));
                escaped = false;
            }
            if (escaped) regex.Append(Regex.Escape("\\"));
            regex.Append('$');
            return Regex.IsMatch(catalog, regex.ToString(), RegexOptions.CultureInvariant | RegexOptions.IgnoreCase,
                TimeSpan.FromSeconds(1));
        }
    }
}
