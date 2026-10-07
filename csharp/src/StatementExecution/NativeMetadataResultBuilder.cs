/*
 * Copyright (c) 2025 ADBC Drivers Contributors
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
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.Hive2;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Types;

namespace AdbcDrivers.Databricks.StatementExecution
{
    internal static class NativeMetadataResultBuilder
    {
        internal static QueryResult Build(
            IReadOnlyList<RecordBatch> batches, Schema schema, MetadataOperation operation,
            string? requestedCatalog = null, IReadOnlyCollection<string>? tableTypes = null,
            string? parentCatalog = null, string? parentSchema = null, string? parentTable = null,
            IReadOnlyList<string?>? sourceCatalogs = null, bool requireExactCatalog = false,
            bool requireExactSourceCatalog = false)
        {
            // C# exposes BASE_TYPE_NAME after the 23 Thrift GetColumns fields.
            int sourceColumns = schema.FieldsList.Count - (operation == MetadataOperation.GetColumns ? 1 : 0);
            if (sourceCatalogs != null && sourceCatalogs.Count != batches.Count)
                throw new ArgumentException("Each native batch must have a source catalog", nameof(sourceCatalogs));
            string? catalogField = operation switch
            {
                MetadataOperation.GetSchemas => "TABLE_CATALOG",
                MetadataOperation.GetTables or MetadataOperation.GetColumns => "TABLE_CAT",
                _ => null,
            };
            var rows = new List<(NativeMetadataColumns Columns, int Index, string? Catalog)>();
            for (int batchIndex = 0; batchIndex < batches.Count; batchIndex++)
            {
                var batch = batches[batchIndex];
                var columns = new NativeMetadataColumns(batch, schema, operation);
                string? sourceCatalog = sourceCatalogs?[batchIndex] ?? requestedCatalog;

                for (int row = 0; row < batch.Length; row++)
                {
                    if (operation == MetadataOperation.GetColumns && requireExactSourceCatalog &&
                        !MetadataRowReader.MatchesCatalog(sourceCatalog, columns.String("TABLE_CAT", row) ?? sourceCatalog))
                        continue;
                    // Native metadata can expand wildcard characters in a quoted catalog.
                    if (catalogField != null && requestedCatalog != null &&
                        (operation == MetadataOperation.GetTables || requireExactCatalog) &&
                        !string.Equals(requestedCatalog, columns.String(catalogField, row) ?? sourceCatalog, StringComparison.OrdinalIgnoreCase))
                        continue;
                    if (operation == MetadataOperation.GetTables && tableTypes != null &&
                        !tableTypes.Contains(DefaultTableType(columns.String("TABLE_TYPE", row))))
                        continue;

                    if (operation == MetadataOperation.GetCrossReference &&
                        (!Matches(parentCatalog, columns.String(schema.FieldsList[0].Name, row)) ||
                         !Matches(parentSchema, columns.String(schema.FieldsList[1].Name, row)) ||
                         !Matches(parentTable, columns.String(schema.FieldsList[2].Name, row))))
                        continue;

                    rows.Add((columns, row, sourceCatalog));
                }
            }

            if (operation == MetadataOperation.GetTables)
            {
                int[] sortColumns = { 3, 0, 1, 2 };
                rows.Sort((left, right) =>
                {
                    foreach (int column in sortColumns)
                    {
                        string? leftValue = left.Columns.String(schema.FieldsList[column].Name, left.Index);
                        string? rightValue = right.Columns.String(schema.FieldsList[column].Name, right.Index);
                        if (column == 3)
                        {
                            leftValue = DefaultTableType(leftValue);
                            rightValue = DefaultTableType(rightValue);
                        }
                        if (column == 0)
                        {
                            leftValue ??= left.Catalog;
                            rightValue ??= right.Catalog;
                        }
                        int result = StringComparer.Ordinal.Compare(
                            leftValue, rightValue);
                        if (result != 0) return result;
                    }
                    return 0;
                });
            }

            var arrays = new List<IArrowArray>(schema.FieldsList.Count);
            for (int column = 0; column < schema.FieldsList.Count; column++)
            {
                switch (schema.FieldsList[column].DataType.TypeId)
                {
                    case ArrowTypeId.String:
                        var strings = new StringArray.Builder();
                        foreach (var (columns, row, sourceCatalog) in rows)
                        {
                            string? typeName = column == sourceColumns && operation == MetadataOperation.GetColumns
                                ? columns.String("TYPE_NAME", row)
                                : null;
                            string? value = column == sourceColumns && operation == MetadataOperation.GetColumns
                                ? typeName == null ? null : ColumnMetadataHelper.GetBaseTypeName(typeName)
                                : columns.String(schema.FieldsList[column].Name, row);
                            if (column == 1 && operation == MetadataOperation.GetSchemas)
                                value ??= sourceCatalog ?? "";
                            if (column == 0 && (operation == MetadataOperation.GetTables || operation == MetadataOperation.GetColumns))
                                value ??= sourceCatalog ?? "";
                            if (column == 3 && operation == MetadataOperation.GetTables)
                                value = DefaultTableType(value);
                            if (value == null) strings.AppendNull(); else strings.Append(value);
                        }
                        arrays.Add(strings.Build());
                        break;

                    case ArrowTypeId.Int8:
                        var int8 = new Int8Array.Builder();
                        foreach (var (columns, row, _) in rows)
                        {
                            long? value = columns.Integer(schema.FieldsList[column].Name, row);
                            if (value.HasValue) int8.Append(checked((sbyte)value.Value)); else int8.AppendNull();
                        }
                        arrays.Add(int8.Build());
                        break;

                    case ArrowTypeId.Int16:
                        var int16 = new Int16Array.Builder();
                        foreach (var (columns, row, _) in rows)
                        {
                            long? value = columns.Integer(schema.FieldsList[column].Name, row);
                            if (value.HasValue) int16.Append(checked((short)value.Value)); else int16.AppendNull();
                        }
                        arrays.Add(int16.Build());
                        break;

                    case ArrowTypeId.Int32:
                        var int32 = new Int32Array.Builder();
                        foreach (var (columns, row, _) in rows)
                        {
                            string name = schema.FieldsList[column].Name;
                            long? value = columns.Integer(name, row);
                            if (operation == MetadataOperation.GetColumns && (name is "COLUMN_SIZE" or "DECIMAL_DIGITS"))
                            {
                                string? typeName = columns.String("TYPE_NAME", row);
                                if (typeName != null)
                                    value = name == "COLUMN_SIZE"
                                        ? ColumnMetadataHelper.GetColumnSizeDefault(typeName)
                                        : ColumnMetadataHelper.GetDecimalDigitsDefault(typeName);
                            }
                            if (value.HasValue) int32.Append(checked((int)value.Value)); else int32.AppendNull();
                        }
                        arrays.Add(int32.Build());
                        break;

                    case ArrowTypeId.Int64:
                        var int64 = new Int64Array.Builder();
                        foreach (var (columns, row, _) in rows)
                        {
                            long? value = columns.Integer(schema.FieldsList[column].Name, row);
                            if (value.HasValue) int64.Append(value.Value); else int64.AppendNull();
                        }
                        arrays.Add(int64.Build());
                        break;

                    default:
                        throw new DatabricksException($"Unsupported native metadata field type: {schema.FieldsList[column].DataType}");
                }
            }

            return new QueryResult(rows.Count, new HiveInfoArrowStream(schema, arrays.ToArray()));
        }

        internal static string? ReadString(IArrowArray array, int row)
        {
            if (array.IsNull(row)) return null;
            return array is StringArray strings
                ? strings.GetString(row)
                : throw new DatabricksException($"Expected a native metadata string, found {array.GetType().Name}");
        }

        internal static long? ReadInteger(IArrowArray array, int row)
        {
            if (array.IsNull(row)) return null;
            return array switch
            {
                Int8Array values => values.GetValue(row),
                Int16Array values => values.GetValue(row),
                Int32Array values => values.GetValue(row),
                Int64Array values => values.GetValue(row),
                _ => throw new DatabricksException($"Expected a native metadata integer, found {array.GetType().Name}")
            };
        }

        private static bool Matches(string? requested, string? actual)
            => requested == null || (actual != null && string.Equals(requested, actual, StringComparison.OrdinalIgnoreCase));

        internal static string DefaultTableType(string? value) => string.IsNullOrEmpty(value) ? "TABLE" : value!;
    }
}
