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
using System.Threading.Tasks;
using AdbcDrivers.Databricks.StatementExecution;
using AdbcDrivers.HiveServer2.Hive2;
using Apache.Arrow;
using Apache.Arrow.Types;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    public class MetadataRowReaderTests
    {
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void Catalogs_OnlyFilterNativeResponses(bool native)
        {
            using var batch = Strings((native ? "TABLE_CAT" : "catalog",
                new string?[] { "ma_catalog", "maxcatalog", "other" }));
            var result = new MetadataBatches(new List<RecordBatch> { batch }, native);

            string[] expected = native
                ? new[] { "ma_catalog" }
                : new[] { "ma_catalog", "maxcatalog", "other" };
            Assert.Equal(expected, MetadataRowReader.Catalogs(result, @"ma\_%"));
        }

        [Theory]
        [InlineData(false, "")]
        [InlineData(true, "TABLE")]
        public void ShowTables_EmptyTypeKeepsExistingCallSiteBehavior(bool normalizeEmptyTableType, string expectedType)
        {
            using var batch = Strings(
                ("catalogName", new string?[] { "main" }),
                ("namespace", new string?[] { "default" }),
                ("tableName", new string?[] { "t" }),
                ("tableType", new string?[] { "" }));
            var result = new MetadataBatches(new List<RecordBatch> { batch }, false);

            var row = Assert.Single(MetadataRowReader.Tables(
                result, "main", normalizeEmptyTableType: normalizeEmptyTableType));
            Assert.Equal(expectedType, row.TableType);
            var filtered = MetadataRowReader.Tables(
                result, "main", new[] { "TABLE" }, normalizeEmptyTableType: normalizeEmptyTableType);
            Assert.Equal(normalizeEmptyTableType ? 1 : 0, filtered.Count);
        }

        [Fact]
        public void ShowColumns_OrdinalsContinueAcrossBatchesPerTable()
        {
            using var first = ShowColumns(("t", "a"), ("other", "b"), ("t", "c"));
            using var second = ShowColumns(("t", "d"));
            var result = new ColumnMetadataResult(new[]
            {
                new MetadataBatches(new List<RecordBatch> { first, second }, false),
            });

            Assert.Equal(new[] { 0, 0, 1, 2 }, result.Rows.Select(row => row.Ordinal));
            Assert.All(result.Rows, row => Assert.True(row.Nullable));
            Assert.False(result.IsNative);
        }

        [Fact]
        public void ShowColumns_PartialIdentifiersStillProvideTableSchemaFields()
        {
            using var batch = Strings(
                ("col_name", new string?[] { "a" }),
                ("columnType", new string?[] { "INT" }),
                ("isNullable", new string?[] { "false" }));
            var result = new ColumnMetadataResult(new[]
            {
                new MetadataBatches(new List<RecordBatch> { batch }, false),
            });

            var row = Assert.Single(result.Rows);
            Assert.Null(row.Catalog);
            Assert.Null(row.Schema);
            Assert.Null(row.Table);
            Assert.Equal("a", row.Name);
            Assert.Equal("INT", row.TypeName);
            Assert.False(row.Nullable);
        }

        [Fact]
        public async Task NativeColumns_FilterExactCatalogAndFillMissingCatalog()
        {
            using var batch = NativeColumns("foo_bar", "fooxbar", null);
            var result = new ColumnMetadataResult(new[]
            {
                new MetadataBatches(new List<RecordBatch> { batch }, true, "foo_bar", requireExactCatalog: true),
            });

            Assert.True(result.IsNative);
            Assert.Equal(3, Assert.Single(result.Batches).Length);
            Assert.Equal(2, result.Rows.Count);
            Assert.All(result.Rows, row =>
            {
                Assert.Equal("foo_bar", row.Catalog);
                Assert.Equal(7, row.Ordinal);
                Assert.Equal("7", row.Default);
                Assert.True(row.Nullable);
                Assert.True(row.IsAutoIncrement);
            });

            var flat = NativeMetadataResultBuilder.Build(result.Batches,
                MetadataSchemaFactory.CreateColumnMetadataSchema(), MetadataOperation.GetColumns,
                sourceCatalogs: result.SourceCatalogs, requireExactSourceCatalog: true);
            using var stream = flat.Stream!;
            using var output = (await stream.ReadNextRecordBatchAsync())!;
            Assert.Equal(result.Rows.Count, output.Length);
            var catalogs = (StringArray)output.Column("TABLE_CAT");
            Assert.Equal(result.Rows.Select(row => row.Catalog),
                Enumerable.Range(0, output.Length).Select(row => catalogs.GetString(row)));
        }

        [Fact]
        public void MixedColumns_PreserveServerOrdinalsAndCountShowRows()
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
        }

        private static RecordBatch ShowColumns(params (string Table, string Name)[] rows)
            => Strings(
                ("catalogName", rows.Select(_ => (string?)"main").ToArray()),
                ("namespace", rows.Select(_ => (string?)"default").ToArray()),
                ("tableName", rows.Select(row => (string?)row.Table).ToArray()),
                ("col_name", rows.Select(row => (string?)row.Name).ToArray()),
                ("columnType", rows.Select(_ => (string?)"INT").ToArray()));

        private static RecordBatch Strings(params (string Name, string?[] Values)[] columns)
        {
            var schema = new Schema(columns.Select(column =>
                new Field(column.Name, StringType.Default, true)), null);
            var arrays = columns.Select(column =>
            {
                var builder = new StringArray.Builder();
                foreach (string? value in column.Values)
                {
                    if (value == null) builder.AppendNull(); else builder.Append(value);
                }
                return (IArrowArray)builder.Build();
            }).ToArray();
            return new RecordBatch(schema, arrays, columns[0].Values.Length);
        }

        private static RecordBatch NativeColumns(params string?[] catalogs)
        {
            var schema = new Schema(MetadataSchemaFactory.CreateColumnMetadataSchema().FieldsList.Take(23), null);
            var arrays = schema.FieldsList.Select(field =>
            {
                if (field.DataType.TypeId == ArrowTypeId.String)
                {
                    var builder = new StringArray.Builder();
                    foreach (string? catalog in catalogs)
                    {
                        string? value = field.Name switch
                        {
                            "TABLE_CAT" => catalog,
                            "TABLE_SCHEM" => "default",
                            "TABLE_NAME" => "t",
                            "COLUMN_NAME" => "a",
                            "TYPE_NAME" => "INT",
                            "COLUMN_DEF" => "7",
                            "IS_AUTO_INCREMENT" => "YES",
                            _ => null,
                        };
                        if (value == null) builder.AppendNull(); else builder.Append(value);
                    }
                    return (IArrowArray)builder.Build();
                }
                int number = field.Name == "ORDINAL_POSITION" ? 7 : field.Name == "NULLABLE" ? 1 : 0;
                return field.DataType.TypeId == ArrowTypeId.Int16
                    ? (IArrowArray)new Int16Array.Builder().AppendRange(
                        Enumerable.Repeat((short)number, catalogs.Length)).Build()
                    : new Int32Array.Builder().AppendRange(Enumerable.Repeat(number, catalogs.Length)).Build();
            }).ToArray();
            return new RecordBatch(schema, arrays, catalogs.Length);
        }
    }
}
