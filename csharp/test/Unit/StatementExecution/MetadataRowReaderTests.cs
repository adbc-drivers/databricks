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
            var result = new MetadataBatches(new List<RecordBatch> { batch }, native,
                catalogFilter: CatalogFilter.Pattern(@"ma\_%"));

            string[] expected = native
                ? new[] { "ma_catalog" }
                : new[] { "ma_catalog", "maxcatalog", "other" };
            Assert.Equal(expected, MetadataRowReader.Catalogs(result));
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
                result, normalizeEmptyTableType: normalizeEmptyTableType));
            Assert.Equal(expectedType, row.TableType);
            var filtered = MetadataRowReader.Tables(
                result, new[] { "TABLE" }, normalizeEmptyTableType: normalizeEmptyTableType);
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

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task NativeColumns_ShareCatalogFilterAndConcreteFallback(bool pattern)
        {
            using var batch = NativeColumns("foo_bar", "fooxbar", null);
            var result = new ColumnMetadataResult(new[]
            {
                new MetadataBatches(new List<RecordBatch> { batch }, true, "foo_bar",
                    pattern ? CatalogFilter.Pattern("foo_bar") : CatalogFilter.Exact("foo_bar")),
            });

            Assert.True(result.IsNative);
            Assert.Equal(3, Assert.Single(Assert.Single(result.Results).Batches).Length);
            Assert.Equal(pattern ? new[] { "foo_bar", "fooxbar" } : new[] { "foo_bar", "foo_bar" },
                result.Rows.Select(row => row.Catalog));
            Assert.All(result.Rows, row =>
            {
                Assert.Equal(7, row.Ordinal);
                Assert.Equal("7", row.Default);
                Assert.True(row.Nullable);
                Assert.True(row.IsAutoIncrement);
            });

            var flat = NativeMetadataResultBuilder.Build(result.Results,
                MetadataSchemaFactory.CreateColumnMetadataSchema(), MetadataOperation.GetColumns);
            using var stream = flat.Stream!;
            using var output = (await stream.ReadNextRecordBatchAsync())!;
            Assert.Equal(result.Rows.Count, output.Length);
            var catalogs = (StringArray)output.Column("TABLE_CAT");
            Assert.Equal(result.Rows.Select(row => row.Catalog),
                Enumerable.Range(0, output.Length).Select(row => catalogs.GetString(row)));
        }

        public static IEnumerable<object?[]> NativeCatalogFallbacks()
        {
            foreach (var operation in new[]
            {
                MetadataOperation.GetSchemas, MetadataOperation.GetTables, MetadataOperation.GetColumns,
            })
                foreach (var (pattern, scope, expected) in new[]
                {
                    (false, "foo_bar", "foo_bar"),
                    (true, "main", "main"),
                    (true, @"foo\_bar", "foo_bar"),
                    (true, @"foo\%bar", "foo%bar"),
                    (true, "ma%", (string?)null),
                    (true, "foo_bar", (string?)null),
                })
                    yield return new object?[] { operation.ToString(), pattern, scope, expected };
        }

        [Theory]
        [MemberData(nameof(NativeCatalogFallbacks))]
        public async Task MissingNativeCatalog_OnlyUsesConcreteScope(
            string operationName, bool pattern, string scope, string? expectedCatalog)
        {
            var operation = (MetadataOperation)System.Enum.Parse(typeof(MetadataOperation), operationName);
            using var batch = operation switch
            {
                MetadataOperation.GetSchemas => Strings(
                    ("TABLE_SCHEM", new string?[] { "default" }),
                    ("TABLE_CATALOG", new string?[] { null })),
                MetadataOperation.GetTables => Strings(MetadataSchemaFactory.CreateTablesSchema().FieldsList
                    .Select(field => (field.Name, new string?[]
                    {
                        field.Name switch
                        {
                            "TABLE_SCHEM" => "default",
                            "TABLE_NAME" => "t",
                            "TABLE_TYPE" => "TABLE",
                            _ => null,
                        },
                    })).ToArray()),
                _ => NativeColumns(new string?[] { null }),
            };
            var result = new MetadataBatches(new List<RecordBatch> { batch }, true, scope,
                pattern ? CatalogFilter.Pattern(scope) : CatalogFilter.Exact(scope));
            var catalogs = operation switch
            {
                MetadataOperation.GetSchemas => MetadataRowReader.Schemas(result).Select(row => row.Catalog),
                MetadataOperation.GetTables => MetadataRowReader.Tables(result).Select(row => row.Catalog),
                _ => MetadataRowReader.Columns(new[] { result }).Select(row => row.Catalog),
            };
            var expected = expectedCatalog == null ? System.Array.Empty<string>() : new[] { expectedCatalog };
            Assert.Equal(expected, catalogs);

            var flat = NativeMetadataResultBuilder.Build(result, batch.Schema, operation);
            using var stream = flat.Stream!;
            using var output = (await stream.ReadNextRecordBatchAsync())!;
            Assert.Equal(expected.Length, output.Length);
            if (expectedCatalog != null)
                Assert.Equal(expectedCatalog, ((StringArray)output.Column(
                    operation == MetadataOperation.GetSchemas ? "TABLE_CATALOG" : "TABLE_CAT")).GetString(0));
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

        [Fact]
        public void ShowPrimaryKeys_PreserveCanonicalNamesAndSequenceFallbackAcrossBatches()
        {
            using var firstStrings = Strings(
                ("col_name", new string?[] { "a", "b", null }),
                ("catalogName", new string?[] { "Main", null, null }),
                ("namespace", new string?[] { "Default", null, null }),
                ("tableName", new string?[] { "Table", null, null }),
                ("constraintName", new string?[] { "pk", null, null }));
            using var sequences = new Int32Array.Builder().Append(7).AppendNull().AppendNull().Build();
            using var first = new RecordBatch(
                new Schema(firstStrings.Schema.FieldsList.Concat(new[] { new Field("keySeq", Int32Type.Default, true) }), null),
                Enumerable.Range(0, firstStrings.ColumnCount).Select(firstStrings.Column).Concat(new[] { sequences }).ToArray(), 3);
            using var second = Strings(("col_name", new string?[] { "c" }));
            var result = new MetadataBatches(new List<RecordBatch> { first, second }, false);

            var rows = MetadataRowReader.PrimaryKeys(result, "main", "default", "table").ToList();

            Assert.Equal(new[] { 7, 1, 2 }, rows.Select(row => row.Sequence));
            Assert.Equal(new[] { "Main", "main", "main" }, rows.Select(row => row.Catalog));
            Assert.Equal(new[] { "Default", "default", "default" }, rows.Select(row => row.Schema));
            Assert.Equal(new[] { "Table", "table", "table" }, rows.Select(row => row.Table));
            Assert.Equal(new[] { "pk", "", "" }, rows.Select(row => row.Name));
        }

        [Fact]
        public void ShowForeignKeys_FilterRawParentBeforeDefaults()
        {
            using var batch = Strings(
                ("parentCatalogName", new string?[] { "Main", null }),
                ("parentNamespace", new string?[] { "Sales", null }),
                ("parentTableName", new string?[] { "Customers", null }),
                ("col_name", new string?[] { "customer_id", "unknown" }));
            var result = new MetadataBatches(new List<RecordBatch> { batch }, false);

            var row = Assert.Single(MetadataRowReader.ForeignKeys(
                result, "main", "sales", "customers", "foreign", "default", "orders"));

            Assert.Equal("Main", row.ParentCatalog);
            Assert.Equal("Sales", row.ParentSchema);
            Assert.Equal("Customers", row.ParentTable);
            Assert.Equal("foreign", row.Catalog);
            Assert.Equal("default", row.Schema);
            Assert.Equal("orders", row.Table);
            Assert.Equal(1, row.Sequence);
            Assert.Equal(0, row.UpdateRule);
            Assert.Equal(0, row.DeleteRule);
            Assert.Equal(5, row.Deferrability);
            Assert.Equal("", row.Name);
            Assert.Null(row.ParentName);
            Assert.Equal(2, MetadataRowReader.ForeignKeys(
                result, null, null, null, "foreign", "default", "orders").Count());
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task NativeKeys_DecodedAndFlatRowsPreserveServerFields(bool foreign)
        {
            var operation = foreign ? MetadataOperation.GetCrossReference : MetadataOperation.GetPrimaryKeys;
            var schema = foreign ? MetadataSchemaFactory.CreateCrossReferenceSchema() : MetadataSchemaFactory.CreatePrimaryKeysSchema();
            schema = new Schema(schema.FieldsList.Reverse(), null);
            var arrays = schema.FieldsList.Select(field => field.DataType.TypeId == ArrowTypeId.String
                ? (IArrowArray)new StringArray.Builder().Append(field.Name switch
                {
                    "TABLE_CAT" or "PKTABLE_CAT" => "Main",
                    "TABLE_SCHEM" or "PKTABLE_SCHEM" => "Sales",
                    "TABLE_NAME" or "PKTABLE_NAME" => "Customers",
                    _ => field.Name,
                }).Build()
                : new Int32Array.Builder().Append(7).Build()).ToArray();
            using var batch = new RecordBatch(schema, arrays, 1);
            var response = new MetadataBatches(new List<RecordBatch> { batch }, true);

            var result = NativeMetadataResultBuilder.Build(response, schema, operation,
                parentCatalog: "main", parentSchema: "sales", parentTable: "customers");
            using var stream = result.Stream!;
            using var flat = (await stream.ReadNextRecordBatchAsync())!;
            Assert.Equal(7, ((Int32Array)flat.Column("KEQ_SEQ")).GetValue(0));
            Assert.Equal("PK_NAME", ((StringArray)flat.Column("PK_NAME")).GetString(0));

            if (foreign)
            {
                var row = Assert.Single(MetadataRowReader.ForeignKeys(
                    response, "main", "sales", "customers", "foreign", "default", "orders"));
                Assert.Equal("Main", row.ParentCatalog);
                Assert.Equal("PK_NAME", row.ParentName);
                Assert.Equal("FKTABLE_CAT", row.Catalog);
                Assert.Equal(7, row.Sequence);
                Assert.Empty(MetadataRowReader.ForeignKeys(
                    response, "other", "sales", "customers", "foreign", "default", "orders"));
                var filtered = NativeMetadataResultBuilder.Build(response, schema, operation, parentCatalog: "other");
                using var filteredStream = filtered.Stream!;
                Assert.Equal(0, filtered.RowCount);
            }
            else
            {
                var row = Assert.Single(MetadataRowReader.PrimaryKeys(response, "fallback", "fallback", "fallback"));
                Assert.Equal("Main", row.Catalog);
                Assert.Equal("Sales", row.Schema);
                Assert.Equal("Customers", row.Table);
                Assert.Equal("PK_NAME", row.Name);
                Assert.Equal(7, row.Sequence);
            }
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
