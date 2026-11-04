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

namespace AdbcDrivers.Databricks.StatementExecution.Metadata.Native
{
    // Passes the server's Thrift-shaped columns through, filling the few values Thrift derives.
    internal sealed class MetadataResultBuilder : IMetadataResultBuilder
    {
        internal static readonly MetadataResultBuilder Instance = new();

        private MetadataResultBuilder() { }

        public QueryResult Catalogs(MetadataBatches result)
            => Build(result, MetadataSchemaFactory.CreateCatalogsSchema(), MetadataOperation.GetCatalogs);

        public QueryResult Schemas(MetadataBatches result)
            => Build(result, MetadataSchemaFactory.CreateSchemasSchema(), MetadataOperation.GetSchemas);

        public QueryResult Tables(MetadataBatches result, IReadOnlyCollection<string>? tableTypes)
            => Build(MetadataDecoder.TableRows(result, tableTypes),
                MetadataSchemaFactory.CreateTablesSchema(), MetadataOperation.GetTables);

        public QueryResult Columns(IReadOnlyList<MetadataBatches> results, IReadOnlyList<ColumnRow> rows)
        {
            var schema = MetadataSchemaFactory.CreateColumnMetadataSchema();
            return Build(NativeMetadataRow.Read(results, schema, MetadataOperation.GetColumns),
                schema, MetadataOperation.GetColumns);
        }

        public QueryResult PrimaryKeys(MetadataBatches result, string catalog, string schema, string table)
            => Build(result, MetadataSchemaFactory.CreatePrimaryKeysSchema(), MetadataOperation.GetPrimaryKeys);

        public QueryResult ForeignKeys(MetadataBatches result,
            string? parentCatalog, string? parentSchema, string? parentTable,
            string catalog, string schema, string table)
        {
            var resultSchema = MetadataSchemaFactory.CreateCrossReferenceSchema();
            var rows = NativeMetadataRow.Read(result, resultSchema, MetadataOperation.GetCrossReference)
                .Where(row => MetadataRowReader.MatchesParent(parentCatalog, parentSchema, parentTable,
                    row.String("PKTABLE_CAT"), row.String("PKTABLE_SCHEM"), row.String("PKTABLE_NAME")))
                .ToList();
            return Build(rows, resultSchema, MetadataOperation.GetCrossReference);
        }

        private static QueryResult Build(MetadataBatches result, Schema schema, MetadataOperation operation)
            => Build(NativeMetadataRow.Read(result, schema, operation), schema, operation);

        private static QueryResult Build(
            IReadOnlyList<NativeMetadataRow> rows, Schema schema, MetadataOperation operation)
        {
            var arrays = new List<IArrowArray>(schema.FieldsList.Count);
            foreach (var field in schema.FieldsList)
            {
                switch (field.DataType.TypeId)
                {
                    case ArrowTypeId.String:
                        var strings = new StringArray.Builder();
                        foreach (var row in rows)
                        {
                            string? value;
                            if (operation == MetadataOperation.GetColumns && field.Name == "BASE_TYPE_NAME")
                            {
                                string? typeName = row.String("TYPE_NAME");
                                value = typeName == null ? null : ColumnMetadataHelper.GetBaseTypeName(typeName);
                            }
                            else
                                value = row.String(field.Name);
                            if ((field.Name == "TABLE_CATALOG" && operation == MetadataOperation.GetSchemas) ||
                                (field.Name == "TABLE_CAT" && operation is MetadataOperation.GetTables or MetadataOperation.GetColumns))
                                value ??= row.SourceCatalog ?? "";
                            if (field.Name == "TABLE_TYPE" && operation == MetadataOperation.GetTables)
                                value = MetadataRowReader.DefaultTableType(value);
                            if (value == null) strings.AppendNull(); else strings.Append(value);
                        }
                        arrays.Add(strings.Build());
                        break;

                    case ArrowTypeId.Int8:
                        var int8 = new Int8Array.Builder();
                        arrays.Add(BuildIntegers(rows, field.Name, operation,
                            value => int8.Append(value.HasValue ? checked((sbyte)value.Value) : (sbyte?)null),
                            () => int8.Build()));
                        break;

                    case ArrowTypeId.Int16:
                        var int16 = new Int16Array.Builder();
                        arrays.Add(BuildIntegers(rows, field.Name, operation,
                            value => int16.Append(value.HasValue ? checked((short)value.Value) : (short?)null),
                            () => int16.Build()));
                        break;

                    case ArrowTypeId.Int32:
                        var int32 = new Int32Array.Builder();
                        arrays.Add(BuildIntegers(rows, field.Name, operation,
                            value => int32.Append(value.HasValue ? checked((int)value.Value) : (int?)null),
                            () => int32.Build()));
                        break;

                    case ArrowTypeId.Int64:
                        var int64 = new Int64Array.Builder();
                        arrays.Add(BuildIntegers(rows, field.Name, operation,
                            value => int64.Append(value), () => int64.Build()));
                        break;

                    default:
                        throw new DatabricksException($"Unsupported native metadata field type: {field.DataType}");
                }
            }

            return new QueryResult(rows.Count, new HiveInfoArrowStream(schema, arrays.ToArray()));
        }

        private static IArrowArray BuildIntegers(
            IReadOnlyList<NativeMetadataRow> rows, string field, MetadataOperation operation,
            Action<long?> append, Func<IArrowArray> build)
        {
            foreach (var row in rows)
            {
                long? value = row.Integer(field);
                if (operation == MetadataOperation.GetColumns && (field is "COLUMN_SIZE" or "DECIMAL_DIGITS"))
                {
                    var precisionAndScale = ColumnMetadataHelper.NormalizePrecisionAndScale(
                        row.String("TYPE_NAME"), row.Integer("COLUMN_SIZE"), row.Integer("DECIMAL_DIGITS"));
                    value = field == "COLUMN_SIZE" ? precisionAndScale.ColumnSize : precisionAndScale.DecimalDigits;
                }
                append(value);
            }
            return build();
        }
    }
}
