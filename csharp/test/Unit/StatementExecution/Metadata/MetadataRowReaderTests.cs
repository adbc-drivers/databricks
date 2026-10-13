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
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.StatementExecution.Metadata;
using AdbcDrivers.HiveServer2.Hive2;
using Apache.Arrow;
using Xunit;
using static AdbcDrivers.Databricks.Tests.Unit.StatementExecution.Metadata.MetadataFixtures;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution.Metadata
{
    // Both formats must decode the same logical metadata into the same rows.
    public class MetadataRowReaderTests
    {
        [Fact]
        public void Catalogs_DecodeIdenticallyAcrossFormats()
        {
            string[] catalogs = { "main", "other" };
            using var native = NativeBatch(MetadataSchemaFactory.CreateCatalogsSchema(), catalogs.Length,
                (_, row) => catalogs[row]);
            using var show = Strings(("catalog", catalogs));

            var (nativeRows, showRows) = Decode(native, show, null, result => MetadataRowReader.Catalogs(result));

            Assert.Equal(catalogs, nativeRows);
            Assert.Equal(nativeRows, showRows);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("main")]
        public void Schemas_DecodeIdenticallyAcrossFormats(string? scope)
        {
            (string Catalog, string Schema)[] expected = scope == null
                ? new[] { ("main", "default"), ("other", "sales") }
                : new[] { ("main", "default"), ("main", "sales") };
            // A scoped native response may omit the catalog, like scoped SHOW SCHEMAS.
            using var native = NativeBatch(MetadataSchemaFactory.CreateSchemasSchema(), expected.Length,
                (field, row) => field == "TABLE_SCHEM" ? expected[row].Schema
                    : scope == null ? expected[row].Catalog : null);
            using var show = scope == null
                ? Strings(("databaseName", expected.Select(row => (string?)row.Schema).ToArray()),
                    ("catalog", expected.Select(row => (string?)row.Catalog).ToArray()))
                : Strings(("databaseName", expected.Select(row => (string?)row.Schema).ToArray()));

            var (nativeRows, showRows) = Decode(native, show, scope,
                result => MetadataRowReader.Schemas(result).Select(row => (row.Catalog, row.Schema)));

            Assert.Equal(expected, nativeRows);
            Assert.Equal(nativeRows, showRows);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void Tables_DecodeIdenticallyAcrossFormats(bool filterTypes)
        {
            // Already in Thrift order, which the native decoder enforces and SHOW returns as is.
            (string Table, string? Type, string? Remarks)[] tables =
            {
                ("a", "TABLE", "remark"), ("b", "TABLE", null), ("c", "", null), ("v", "VIEW", null),
            };
            using var native = NativeBatch(MetadataSchemaFactory.CreateTablesSchema(), tables.Length,
                (field, row) => field switch
                {
                    "TABLE_CAT" => "main",
                    "TABLE_SCHEM" => "default",
                    "TABLE_NAME" => tables[row].Table,
                    "TABLE_TYPE" => tables[row].Type,
                    "REMARKS" => tables[row].Remarks,
                    _ => null,
                });
            using var show = Strings(
                ("catalogName", tables.Select(_ => (string?)"main").ToArray()),
                ("namespace", tables.Select(_ => (string?)"default").ToArray()),
                ("tableName", tables.Select(row => (string?)row.Table).ToArray()),
                ("tableType", tables.Select(row => row.Type).ToArray()),
                ("remarks", tables.Select(row => row.Remarks).ToArray()));
            string[]? tableTypes = filterTypes ? new[] { "TABLE" } : null;

            var (nativeRows, showRows) = Decode(native, show, "main", result => MetadataRowReader.Tables(result, tableTypes)
                .Select(row => (row.Catalog, row.Schema, row.Table, row.TableType, row.Remarks)));

            var expected = new[]
            {
                ("main", "default", "a", "TABLE", "remark"), ("main", "default", "b", "TABLE", ""),
                ("main", "default", "c", "TABLE", ""), ("main", "default", "v", "VIEW", ""),
            }.Take(filterTypes ? 3 : 4);
            Assert.Equal(expected, nativeRows);
            Assert.Equal(nativeRows, showRows);
        }

        [Fact]
        public void Columns_DecodeIdenticallyAcrossFormats()
        {
            (string Table, string Name, string Type, bool Nullable, int Ordinal)[] columns =
            {
                ("t", "a", "INT", true, 0), ("u", "c", "INT", true, 0), ("t", "b", "STRING", false, 1),
            };
            using var native = NativeBatch(MetadataSchemaFactory.CreateColumnMetadataSchema(), columns.Length,
                (field, row) => field switch
                {
                    "TABLE_CAT" => "main",
                    "TABLE_SCHEM" => "default",
                    "TABLE_NAME" => columns[row].Table,
                    "COLUMN_NAME" => columns[row].Name,
                    "TYPE_NAME" => columns[row].Type,
                    "NULLABLE" => columns[row].Nullable ? 1 : 0,
                    "ORDINAL_POSITION" => columns[row].Ordinal,
                    "IS_AUTO_INCREMENT" => "NO",
                    _ => null,
                });
            // SHOW numbers each table's columns across batches.
            using var first = ShowColumns(columns.Take(2).ToArray());
            using var second = ShowColumns(columns.Skip(2).ToArray());
            var showResult = new MetadataBatches(new List<RecordBatch> { first, second }, false, "main",
                CatalogFilter.Exact("main"));

            var nativeRows = Describe(new ColumnMetadataResult(new[]
            {
                new MetadataBatches(new List<RecordBatch> { native }, true, "main", CatalogFilter.Exact("main")),
            }).Rows);
            var showRows = Describe(new ColumnMetadataResult(new[] { showResult }).Rows);

            Assert.Equal(columns.Select(column => ((string?)"main", (string?)"default", (string?)column.Table,
                column.Name, column.Type, column.Nullable, column.Ordinal, (string?)null, false)), nativeRows);
            Assert.Equal(nativeRows, showRows);

            RecordBatch ShowColumns(params (string Table, string Name, string Type, bool Nullable, int Ordinal)[] rows)
                => Strings(
                    ("catalogName", rows.Select(_ => (string?)"main").ToArray()),
                    ("namespace", rows.Select(_ => (string?)"default").ToArray()),
                    ("tableName", rows.Select(row => (string?)row.Table).ToArray()),
                    ("col_name", rows.Select(row => (string?)row.Name).ToArray()),
                    ("columnType", rows.Select(row => (string?)row.Type).ToArray()),
                    ("isNullable", rows.Select(row => (string?)(row.Nullable ? "true" : "false")).ToArray()));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void Catalogs_OnlyFilterNativeResponses(bool native)
        {
            using var batch = Strings((native ? "TABLE_CAT" : "catalog",
                new string?[] { "ma_catalog", "maxcatalog", "other" }));
            var result = new MetadataBatches(new List<RecordBatch> { batch }, native,
                catalogFilter: CatalogFilter.Pattern(@"ma\_%"));

            // Native expands the catalog as LIKE; SHOW CATALOGS LIKE was already filtered by the server.
            string[] expected = native
                ? new[] { "ma_catalog" }
                : new[] { "ma_catalog", "maxcatalog", "other" };
            Assert.Equal(expected, MetadataRowReader.Catalogs(result));
        }

        [Theory]
        [InlineData(false, new[] { 0, 1, 2 })]
        [InlineData(true, new[] { 0, 1 })] // Numbering before the skip would give { 0, 2 }.
        public void Columns_GetObjectsSkipsIncompleteRowsBeforeNumbering(bool requireTableIdentifiers, int[] expected)
        {
            using var batch = ShowColumns(("t", "a"), ("t", ""), ("t", "c"));

            var result = new ColumnMetadataResult(new[]
            {
                new MetadataBatches(new List<RecordBatch> { batch }, false),
            }, requireTableIdentifiers);

            Assert.Equal(expected, result.Rows.Select(row => row.Ordinal));
        }

        [Fact]
        public async Task MixedColumns_PreserveServerOrdinalsAndCountShowRows()
        {
            using var native = NativeColumns("main");
            using var show = ShowColumns(("t", "b"));
            var result = new ColumnMetadataResult(new[]
            {
                new MetadataBatches(new List<RecordBatch> { native }, true, "main"),
                new MetadataBatches(new List<RecordBatch> { show }, false, "main"),
            });

            Assert.False(result.IsNative);
            Assert.Equal(new[] { 7, 1 }, result.Rows.Select(row => row.Ordinal));
            Assert.Equal(new[] { "a", "b" }, result.Rows.Select(row => row.Name));

            var flat = await MetadataRowReader.ColumnsResultAsync(result, CancellationToken.None);
            using var stream = flat.Stream!;
            using var output = (await stream.ReadNextRecordBatchAsync())!;
            var positions = (Int32Array)output.Column("ORDINAL_POSITION");
            Assert.Equal(new[] { 7, 1 }, Enumerable.Range(0, output.Length).Select(row => positions.GetValue(row)!.Value));
            // The native row keeps its server default; the SHOW row has none.
            var defaults = (StringArray)output.Column("COLUMN_DEF");
            Assert.Equal("7", defaults.GetString(0));
            Assert.True(defaults.IsNull(1));
        }

        private static (List<T> Native, List<T> Show) Decode<T>(
            RecordBatch native, RecordBatch show, string? scope, Func<MetadataBatches, IEnumerable<T>> decode)
        {
            // Mirrors GetObjects, which scopes both formats with the catalog pattern.
            MetadataBatches Response(RecordBatch batch, bool isNative)
                => new(new List<RecordBatch> { batch }, isNative, scope, CatalogFilter.Pattern(scope));
            return (decode(Response(native, true)).ToList(), decode(Response(show, false)).ToList());
        }

        private static List<(string?, string?, string?, string, string, bool, int, string?, bool)> Describe(
            IEnumerable<ColumnRow> rows)
            => rows.Select(row => (row.Catalog, row.Schema, row.Table, row.Name, row.TypeName, row.Nullable,
                row.Ordinal, row.Default, row.IsAutoIncrement)).ToList();
    }
}
