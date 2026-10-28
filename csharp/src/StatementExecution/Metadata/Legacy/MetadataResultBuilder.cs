/*
 * Copyright (c) 2025 ADBC Drivers Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.Collections.Generic;
using System.Linq;
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.Hive2;
using Apache.Arrow;
using Apache.Arrow.Adbc;

namespace AdbcDrivers.Databricks.StatementExecution.Metadata.Legacy
{
    // Builds Thrift-shaped results from decoded SHOW rows, computing the columns SHOW lacks.
    internal sealed class MetadataResultBuilder : IMetadataResultBuilder
    {
        internal static readonly MetadataResultBuilder Instance = new();

        private MetadataResultBuilder() { }

        public QueryResult Catalogs(MetadataBatches result)
        {
            var tableCatBuilder = new StringArray.Builder();
            int count = 0;
            foreach (string catalog in MetadataDecoder.Instance.Catalogs(result))
            {
                tableCatBuilder.Append(catalog);
                count++;
            }

            var schema = MetadataSchemaFactory.CreateCatalogsSchema();
            return new QueryResult(count, new HiveInfoArrowStream(schema, new IArrowArray[] { tableCatBuilder.Build() }));
        }

        public QueryResult Schemas(MetadataBatches result)
        {
            var tableSchemaBuilder = new StringArray.Builder();
            var tableCatalogBuilder = new StringArray.Builder();
            int count = 0;
            foreach (var row in MetadataDecoder.Instance.Schemas(result))
            {
                tableSchemaBuilder.Append(row.Schema);
                tableCatalogBuilder.Append(row.Catalog);
                count++;
            }

            var schema = MetadataSchemaFactory.CreateSchemasSchema();
            return new QueryResult(count, new HiveInfoArrowStream(schema, new IArrowArray[]
            {
                tableSchemaBuilder.Build(), tableCatalogBuilder.Build()
            }));
        }

        public QueryResult Tables(MetadataBatches result, IReadOnlyCollection<string>? tableTypes)
        {
            var tableCatBuilder = new StringArray.Builder();
            var tableSchemaBuilder = new StringArray.Builder();
            var tableNameBuilder = new StringArray.Builder();
            var tableTypeBuilder = new StringArray.Builder();
            var remarksBuilder = new StringArray.Builder();
            var typeCatBuilder = new StringArray.Builder();
            var typeSchemaBuilder = new StringArray.Builder();
            var typeNameBuilder = new StringArray.Builder();
            var selfRefColBuilder = new StringArray.Builder();
            var refGenBuilder = new StringArray.Builder();
            int count = 0;
            foreach (var row in MetadataDecoder.ShowTables(result, normalizeEmptyTableType: false))
            {
                if (!MetadataRowReader.MatchesTableType(tableTypes, row.TableType)) continue;
                tableCatBuilder.Append(row.Catalog);
                tableSchemaBuilder.Append(row.Schema);
                tableNameBuilder.Append(row.Table);
                tableTypeBuilder.Append(row.TableType);
                remarksBuilder.Append(row.Remarks);
                typeCatBuilder.AppendNull();
                typeSchemaBuilder.AppendNull();
                typeNameBuilder.AppendNull();
                selfRefColBuilder.AppendNull();
                refGenBuilder.AppendNull();
                count++;
            }

            var schema = MetadataSchemaFactory.CreateTablesSchema();
            return new QueryResult(count, new HiveInfoArrowStream(schema, new IArrowArray[]
            {
                tableCatBuilder.Build(), tableSchemaBuilder.Build(), tableNameBuilder.Build(),
                tableTypeBuilder.Build(), remarksBuilder.Build(), typeCatBuilder.Build(),
                typeSchemaBuilder.Build(), typeNameBuilder.Build(), selfRefColBuilder.Build(),
                refGenBuilder.Build()
            }));
        }

        public QueryResult Columns(IReadOnlyList<MetadataBatches> results, IReadOnlyList<ColumnRow> rows)
        {
            var tableInfos = new Dictionary<string, (string catalog, string schema, string table, TableInfo info)>();
            foreach (var column in rows)
            {
                if (column.Catalog == null || column.Schema == null || column.Table == null) continue;
                string key = $"{column.Catalog}.{column.Schema}.{column.Table}";
                if (!tableInfos.ContainsKey(key))
                    tableInfos[key] = (column.Catalog, column.Schema, column.Table, new TableInfo("TABLE"));
                ColumnMetadataHelper.PopulateTableInfoFromTypeName(
                    tableInfos[key].info, column.Name, column.TypeName, column.Ordinal, column.Nullable,
                    columnDefault: column.Default, isAutoIncrement: column.IsAutoIncrement);
            }
            return BuildColumns(tableInfos.Values);
        }

        public QueryResult PrimaryKeys(MetadataBatches result, string catalog, string schema, string table)
            => MetadataSchemaFactory.BuildPrimaryKeysResult(
                MetadataDecoder.Instance.PrimaryKeys(result, catalog, schema, table)
                    .Select(row => (row.Catalog, row.Schema, row.Table, row.Column, row.Sequence, row.Name)));

        public QueryResult ForeignKeys(MetadataBatches result,
            string? parentCatalog, string? parentSchema, string? parentTable,
            string catalog, string schema, string table)
            => MetadataSchemaFactory.BuildCrossReferenceResult(
                MetadataDecoder.Instance.ForeignKeys(result, parentCatalog, parentSchema, parentTable,
                    catalog, schema, table)
                    .Select(row => (row.ParentCatalog, row.ParentSchema, row.ParentTable, row.ParentColumn,
                        row.Catalog, row.Schema, row.Table, row.Column, row.Sequence, row.UpdateRule,
                        row.DeleteRule, row.Name, row.ParentName, row.Deferrability)));

        private static QueryResult BuildColumns(
            IEnumerable<(string catalog, string schema, string table, TableInfo info)> tables)
        {
            var tableCatBuilder = new StringArray.Builder();
            var tableSchemaBuilder = new StringArray.Builder();
            var tableNameBuilder = new StringArray.Builder();
            var columnNameBuilder = new StringArray.Builder();
            var dataTypeBuilder = new Int32Array.Builder();
            var typeNameBuilder = new StringArray.Builder();
            var columnSizeBuilder = new Int32Array.Builder();
            var bufferLengthBuilder = new Int32Array.Builder();
            var decimalDigitsBuilder = new Int32Array.Builder();
            var numPrecRadixBuilder = new Int32Array.Builder();
            var nullableBuilder = new Int32Array.Builder();
            var remarksBuilder = new StringArray.Builder();
            var columnDefBuilder = new StringArray.Builder();
            var sqlDataTypeBuilder = new Int32Array.Builder();
            var sqlDatetimeSubBuilder = new Int32Array.Builder();
            var charOctetLengthBuilder = new Int32Array.Builder();
            var ordinalPositionBuilder = new Int32Array.Builder();
            var isNullableBuilder = new StringArray.Builder();
            var scopeCatalogBuilder = new StringArray.Builder();
            var scopeSchemaBuilder = new StringArray.Builder();
            var scopeTableBuilder = new StringArray.Builder();
            var sourceDataTypeBuilder = new Int16Array.Builder();
            var isAutoIncrementBuilder = new StringArray.Builder();
            var baseTypeNameBuilder = new StringArray.Builder();
            int totalRows = 0;

            foreach (var (catalog, schema, table, info) in tables)
            {
                for (int i = 0; i < info.ColumnName.Count; i++)
                {
                    tableCatBuilder.Append(catalog);
                    tableSchemaBuilder.Append(schema);
                    tableNameBuilder.Append(table);
                    columnNameBuilder.Append(info.ColumnName[i]);
                    dataTypeBuilder.Append(info.ColType[i]);
                    typeNameBuilder.Append(info.TypeName[i]);

                    if (info.Precision[i].HasValue) columnSizeBuilder.Append(info.Precision[i]!.Value); else columnSizeBuilder.AppendNull();

                    int? bufLen = ColumnMetadataHelper.GetBufferLength(info.TypeName[i]);
                    if (bufLen.HasValue) bufferLengthBuilder.Append(bufLen.Value); else bufferLengthBuilder.AppendNull();

                    if (info.Scale[i].HasValue) decimalDigitsBuilder.Append(info.Scale[i]!.Value); else decimalDigitsBuilder.AppendNull();

                    short? radix = ColumnMetadataHelper.GetNumPrecRadix(info.TypeName[i]);
                    if (radix.HasValue) numPrecRadixBuilder.Append(radix.Value); else numPrecRadixBuilder.AppendNull();

                    nullableBuilder.Append(info.Nullable[i]);
                    remarksBuilder.Append("");
                    if (string.IsNullOrEmpty(info.ColumnDefault[i])) columnDefBuilder.AppendNull();
                    else columnDefBuilder.Append(info.ColumnDefault[i]);

                    sqlDataTypeBuilder.Append(info.ColType[i]);

                    short? dtSub = ColumnMetadataHelper.GetSqlDatetimeSub(info.TypeName[i]);
                    if (dtSub.HasValue) sqlDatetimeSubBuilder.Append(dtSub.Value); else sqlDatetimeSubBuilder.AppendNull();

                    int? charOctet = ColumnMetadataHelper.GetCharOctetLength(info.TypeName[i]);
                    if (charOctet.HasValue) charOctetLengthBuilder.Append(charOctet.Value); else charOctetLengthBuilder.AppendNull();

                    ordinalPositionBuilder.Append(info.OrdinalPosition[i]);
                    isNullableBuilder.Append(info.IsNullable[i]);
                    scopeCatalogBuilder.AppendNull();
                    scopeSchemaBuilder.AppendNull();
                    scopeTableBuilder.AppendNull();
                    sourceDataTypeBuilder.AppendNull();
                    isAutoIncrementBuilder.Append(info.IsAutoIncrement[i] ? "YES" : "NO");
                    baseTypeNameBuilder.Append(info.BaseTypeName[i]);
                    totalRows++;
                }
            }

            var resultSchema = MetadataSchemaFactory.CreateColumnMetadataSchema();

            var dataArrays = new IArrowArray[]
            {
                tableCatBuilder.Build(),
                tableSchemaBuilder.Build(),
                tableNameBuilder.Build(),
                columnNameBuilder.Build(),
                dataTypeBuilder.Build(),
                typeNameBuilder.Build(),
                columnSizeBuilder.Build(),
                bufferLengthBuilder.Build(),
                decimalDigitsBuilder.Build(),
                numPrecRadixBuilder.Build(),
                nullableBuilder.Build(),
                remarksBuilder.Build(),
                columnDefBuilder.Build(),
                sqlDataTypeBuilder.Build(),
                sqlDatetimeSubBuilder.Build(),
                charOctetLengthBuilder.Build(),
                ordinalPositionBuilder.Build(),
                isNullableBuilder.Build(),
                scopeCatalogBuilder.Build(),
                scopeSchemaBuilder.Build(),
                scopeTableBuilder.Build(),
                sourceDataTypeBuilder.Build(),
                isAutoIncrementBuilder.Build(),
                baseTypeNameBuilder.Build()
            };

            return new QueryResult(totalRows, new HiveInfoArrowStream(resultSchema, dataArrays));
        }
    }
}
