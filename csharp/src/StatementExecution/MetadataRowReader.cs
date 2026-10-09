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
    internal readonly struct CatalogFilter
    {
        private CatalogFilter(string? value, bool isPattern)
        {
            Value = value;
            IsPattern = isPattern;
        }

        internal string? Value { get; }
        internal bool IsPattern { get; }

        internal static CatalogFilter Exact(string? catalog) => new(catalog, false);
        internal static CatalogFilter Pattern(string? pattern) => new(pattern, true);
    }

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
            Rows = MetadataRowReader.Columns(results, requireTableIdentifiers).ToList();
        }

        internal IReadOnlyList<MetadataBatches> Results { get; }
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

    internal readonly struct PrimaryKeyRow
    {
        internal PrimaryKeyRow(string catalog, string schema, string table, string column, int sequence, string name)
        {
            Catalog = catalog;
            Schema = schema;
            Table = table;
            Column = column;
            Sequence = sequence;
            Name = name;
        }

        internal string Catalog { get; }
        internal string Schema { get; }
        internal string Table { get; }
        internal string Column { get; }
        internal int Sequence { get; }
        internal string Name { get; }
    }

    internal readonly struct ForeignKeyRow
    {
        internal ForeignKeyRow(string parentCatalog, string parentSchema, string parentTable, string parentColumn,
            string catalog, string schema, string table, string column, int sequence,
            int updateRule, int deleteRule, string name, string? parentName, int deferrability)
        {
            ParentCatalog = parentCatalog;
            ParentSchema = parentSchema;
            ParentTable = parentTable;
            ParentColumn = parentColumn;
            Catalog = catalog;
            Schema = schema;
            Table = table;
            Column = column;
            Sequence = sequence;
            UpdateRule = updateRule;
            DeleteRule = deleteRule;
            Name = name;
            ParentName = parentName;
            Deferrability = deferrability;
        }

        internal string ParentCatalog { get; }
        internal string ParentSchema { get; }
        internal string ParentTable { get; }
        internal string ParentColumn { get; }
        internal string Catalog { get; }
        internal string Schema { get; }
        internal string Table { get; }
        internal string Column { get; }
        internal int Sequence { get; }
        internal int UpdateRule { get; }
        internal int DeleteRule { get; }
        internal string Name { get; }
        internal string? ParentName { get; }
        internal int Deferrability { get; }
    }

    internal readonly struct NativeMetadataRow
    {
        internal NativeMetadataRow(NativeMetadataColumns columns, int index, string? sourceCatalog)
        {
            Columns = columns;
            Index = index;
            SourceCatalog = sourceCatalog;
        }

        private NativeMetadataColumns Columns { get; }
        private int Index { get; }
        internal string? SourceCatalog { get; }
        internal string? String(string name) => Columns.String(name, Index);
        internal long? Integer(string name) => Columns.Integer(name, Index);
    }

    internal static class MetadataRowReader
    {
        internal static List<NativeMetadataRow> NativeRows(
            IEnumerable<MetadataBatches> results, Schema schema, MetadataOperation operation,
            IReadOnlyCollection<string>? tableTypes = null,
            string? parentCatalog = null, string? parentSchema = null, string? parentTable = null)
        {
            string? catalogField = operation switch
            {
                MetadataOperation.GetCatalogs => "TABLE_CAT",
                MetadataOperation.GetSchemas => "TABLE_CATALOG",
                MetadataOperation.GetTables or MetadataOperation.GetColumns => "TABLE_CAT",
                _ => null,
            };
            var rows = new List<NativeMetadataRow>();
            foreach (var result in results)
            {
                // A wildcard scope cannot supply a missing native catalog identifier.
                string? sourceCatalog = result.CatalogFilter.IsPattern
                    ? LiteralCatalog(result.SourceCatalog) : result.SourceCatalog;
                foreach (var batch in result.Batches)
                {
                    var columns = new NativeMetadataColumns(batch, schema, operation);
                    for (int index = 0; index < batch.Length; index++)
                    {
                        var row = new NativeMetadataRow(columns, index, sourceCatalog);
                        if (catalogField != null &&
                            !MatchesCatalog(result.CatalogFilter, row.String(catalogField) ?? row.SourceCatalog))
                            continue;
                        if (operation == MetadataOperation.GetTables && tableTypes != null &&
                            !tableTypes.Contains(DefaultTableType(row.String("TABLE_TYPE"))))
                            continue;
                        if (operation == MetadataOperation.GetCrossReference &&
                            !MatchesParent(parentCatalog, parentSchema, parentTable,
                                row.String("PKTABLE_CAT"), row.String("PKTABLE_SCHEM"), row.String("PKTABLE_NAME")))
                            continue;
                        rows.Add(row);
                    }
                }
            }

            // Thrift orders tables by type, catalog, schema, then name.
            if (operation == MetadataOperation.GetTables)
                rows.Sort((left, right) => CompareTables(
                    (left.String("TABLE_CAT") ?? left.SourceCatalog, left.String("TABLE_SCHEM"),
                        left.String("TABLE_NAME"), left.String("TABLE_TYPE")),
                    (right.String("TABLE_CAT") ?? right.SourceCatalog, right.String("TABLE_SCHEM"),
                        right.String("TABLE_NAME"), right.String("TABLE_TYPE"))));
            return rows;
        }

        internal static IEnumerable<string> Catalogs(MetadataBatches result)
        {
            if (result.IsNative)
            {
                foreach (var row in NativeRows(new[] { result },
                    MetadataSchemaFactory.CreateCatalogsSchema(), MetadataOperation.GetCatalogs))
                    if (row.String("TABLE_CAT") is string catalog) yield return catalog;
                yield break;
            }

            foreach (var batch in result.Batches)
            {
                var catalogs = TryGetColumn<StringArray>(batch, "catalog");
                if (catalogs == null) continue;
                for (int row = 0; row < batch.Length; row++)
                    if (String(catalogs, row) is string catalog) yield return catalog;
            }
        }

        internal static IEnumerable<SchemaRow> Schemas(MetadataBatches result)
        {
            if (result.IsNative)
            {
                foreach (var row in NativeRows(new[] { result },
                    MetadataSchemaFactory.CreateSchemasSchema(), MetadataOperation.GetSchemas))
                    if (row.String("TABLE_SCHEM") is string schema)
                        yield return new SchemaRow(row.String("TABLE_CATALOG") ?? row.SourceCatalog ?? "", schema);
                yield break;
            }

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

        internal static List<TableRow> Tables(
            MetadataBatches result, IReadOnlyCollection<string>? tableTypes = null,
            bool normalizeEmptyTableType = true)
        {
            var rows = new List<TableRow>();
            if (result.IsNative)
            {
                foreach (var row in NativeRows(new[] { result },
                    MetadataSchemaFactory.CreateTablesSchema(), MetadataOperation.GetTables, tableTypes))
                {
                    string? schema = row.String("TABLE_SCHEM");
                    string? table = row.String("TABLE_NAME");
                    if (schema == null || table == null) continue;
                    rows.Add(new TableRow(row.String("TABLE_CAT") ?? row.SourceCatalog ?? "", schema, table,
                        DefaultTableType(row.String("TABLE_TYPE")), row.String("REMARKS") ?? ""));
                }
                return rows;
            }
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
                    string? rowCatalog = String(catalogs, row);
                    string? schema = String(schemas, row);
                    string? table = String(tables, row);
                    if (rowCatalog == null || schema == null || table == null) continue;
                    string? serverType = String(types, row);
                    string type = normalizeEmptyTableType
                        ? DefaultTableType(serverType)
                        : serverType ?? "TABLE";
                    if (tableTypes != null && !tableTypes.Contains(type)) continue;
                    rows.Add(new TableRow(rowCatalog ?? "", schema, table, type,
                        String(remarks, row) ?? ""));
                }
            }
            return rows;
        }

        internal static IEnumerable<ColumnRow> Columns(IEnumerable<MetadataBatches> results,
            bool requireTableIdentifiers = false)
        {
            var positions = new Dictionary<string, int>();
            foreach (var result in results)
            {
                var rows = result.IsNative ? NativeColumns(result)
                    : result.Batches.SelectMany(batch => ShowColumns(batch, positions));
                foreach (var row in rows)
                {
                    // GetObjects skips incomplete SHOW rows before assigning ordinals.
                    if (requireTableIdentifiers && (row.Catalog == null || row.Schema == null ||
                        row.Table == null || string.IsNullOrEmpty(row.Name))) continue;
                    string key = $"{row.Catalog}.{row.Schema}.{row.Table}";
                    positions.TryGetValue(key, out int position);
                    positions[key] = position + 1;
                    yield return row;
                }
            }
        }

        private static IEnumerable<ColumnRow> NativeColumns(MetadataBatches result)
        {
            foreach (var row in NativeRows(new[] { result },
                MetadataSchemaFactory.CreateColumnMetadataSchema(), MetadataOperation.GetColumns))
            {
                string? name = row.String("COLUMN_NAME");
                string? typeName = row.String("TYPE_NAME");
                if (name == null || typeName == null) continue;
                // Both decoders expose zero-based ordinals; GetObjects adds its required offset.
                yield return new ColumnRow(row.String("TABLE_CAT") ?? row.SourceCatalog, row.String("TABLE_SCHEM"),
                    row.String("TABLE_NAME"), name, typeName, row.Integer("NULLABLE") == 1,
                    checked((int)(row.Integer("ORDINAL_POSITION") ?? 0)),
                    row.String("COLUMN_DEF"),
                    string.Equals(row.String("IS_AUTO_INCREMENT"), "YES", StringComparison.OrdinalIgnoreCase));
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

        internal static IEnumerable<PrimaryKeyRow> PrimaryKeys(
            MetadataBatches result, string catalog, string schema, string table)
        {
            if (result.IsNative)
            {
                foreach (var row in NativeRows(new[] { result },
                    MetadataSchemaFactory.CreatePrimaryKeysSchema(), MetadataOperation.GetPrimaryKeys))
                    if (row.String("COLUMN_NAME") is string column)
                        yield return new PrimaryKeyRow(row.String("TABLE_CAT") ?? catalog,
                            row.String("TABLE_SCHEM") ?? schema, row.String("TABLE_NAME") ?? table,
                            column, checked((int)(row.Integer("KEQ_SEQ") ?? 0)), row.String("PK_NAME") ?? "");
                yield break;
            }

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

        internal static IEnumerable<ForeignKeyRow> ForeignKeys(
            MetadataBatches result, string? parentCatalog, string? parentSchema, string? parentTable,
            string catalog, string schema, string table)
        {
            if (result.IsNative)
            {
                foreach (var row in NativeRows(new[] { result },
                    MetadataSchemaFactory.CreateCrossReferenceSchema(), MetadataOperation.GetCrossReference,
                    parentCatalog: parentCatalog, parentSchema: parentSchema, parentTable: parentTable))
                    if (row.String("FKCOLUMN_NAME") is string column)
                        yield return new ForeignKeyRow(row.String("PKTABLE_CAT") ?? parentCatalog ?? "",
                            row.String("PKTABLE_SCHEM") ?? parentSchema ?? "", row.String("PKTABLE_NAME") ?? parentTable ?? "",
                            row.String("PKCOLUMN_NAME") ?? "", row.String("FKTABLE_CAT") ?? catalog,
                            row.String("FKTABLE_SCHEM") ?? schema, row.String("FKTABLE_NAME") ?? table, column,
                            checked((int)(row.Integer("KEQ_SEQ") ?? 0)),
                            checked((int)(row.Integer("UPDATE_RULE") ?? 0)),
                            checked((int)(row.Integer("DELETE_RULE") ?? 0)), row.String("FK_NAME") ?? "",
                            row.String("PK_NAME"), checked((int)(row.Integer("DEFERRABILITY") ?? 5)));
                yield break;
            }

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
                    if (!MatchesParent(parentCatalog, parentSchema, parentTable,
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

        internal static int CompareTables(
            (string? Catalog, string? Schema, string? Table, string? TableType) left,
            (string? Catalog, string? Schema, string? Table, string? TableType) right)
        {
            int order = StringComparer.Ordinal.Compare(
                DefaultTableType(left.TableType),
                DefaultTableType(right.TableType));
            if (order != 0) return order;
            order = StringComparer.Ordinal.Compare(left.Catalog, right.Catalog);
            if (order != 0) return order;
            order = StringComparer.Ordinal.Compare(left.Schema, right.Schema);
            return order != 0 ? order : StringComparer.Ordinal.Compare(left.Table, right.Table);
        }

        internal static bool MatchesCatalog(CatalogFilter filter, string? actual)
            => filter.Value == null || (actual != null && (filter.IsPattern
                ? MatchesCatalogPattern(filter.Value, actual)
                : string.Equals(filter.Value, actual, StringComparison.OrdinalIgnoreCase)));

        private static bool MatchesParent(string? catalog, string? schema, string? table,
            string? actualCatalog, string? actualSchema, string? actualTable)
            => MatchesCatalog(CatalogFilter.Exact(catalog), actualCatalog) &&
                MatchesCatalog(CatalogFilter.Exact(schema), actualSchema) &&
                MatchesCatalog(CatalogFilter.Exact(table), actualTable);

        internal static string DefaultTableType(string? value) => string.IsNullOrEmpty(value) ? "TABLE" : value!;

        private static string? String(StringArray? values, int row)
            => values == null || values.IsNull(row) ? null : values.GetString(row);

        private static int? Integer(Int32Array? values, int row)
            => values == null || values.IsNull(row) ? null : values.GetValue(row);

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

        private static string? LiteralCatalog(string? pattern)
        {
            if (pattern == null) return null;
            var literal = new StringBuilder();
            bool escaped = false;
            foreach (char character in pattern)
            {
                if (!escaped)
                {
                    if (character is '%' or '_') return null;
                    if (character == '\\')
                    {
                        escaped = true;
                        continue;
                    }
                }
                literal.Append(character);
                escaped = false;
            }
            if (escaped) literal.Append('\\');
            return literal.ToString();
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
