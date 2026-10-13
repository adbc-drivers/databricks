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

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.StatementExecution;
using AdbcDrivers.Databricks.StatementExecution.Metadata;
using AdbcDrivers.HiveServer2.Hive2;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Types;
using Xunit;
using static AdbcDrivers.Databricks.Tests.Unit.StatementExecution.Metadata.MetadataFixtures;
using Native = AdbcDrivers.Databricks.StatementExecution.Metadata.Native;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution.Metadata
{
    // Native-only compensation: catalog re-filtering, Thrift ordering and defaults, server passthrough.
    public class NativeMetadataTests
    {
        private static IMetadataResultBuilder Builder => Native.MetadataResultBuilder.Instance;

        [Theory]
        [InlineData("DECIMAL(10,2)", 0, 0, 10, 2)]
        [InlineData("INT", 10, 0, 10, 0)]
        [InlineData("FLOAT", 7, 7, 7, 7)]
        [InlineData("BINARY", 99, 3, 99, 3)]
        [InlineData("VARCHAR(42)", 99, 3, 42, 3)]
        [InlineData("CHAR(4)", 99, 3, 4, 3)]
        [InlineData("DOUBLE", null, null, 0, 0)]
        [InlineData("DOUBLE", 15, null, 15, 0)]
        [InlineData("DOUBLE", null, 15, 0, 15)]
        [InlineData("DECIMAL(10,2)", null, null, 10, 2)]
        [InlineData("VARCHAR(42)", null, null, 42, 0)]
        public async Task GetColumns_OnlyNormalizesThriftPrecisionAndScale(
            string typeName, int? columnSize, int? decimalDigits, int expectedSize, int expectedScale)
        {
            var target = new Schema(MetadataSchemaFactory.CreateColumnMetadataSchema().FieldsList.Reverse(), null);
            Assert.Equal(24, target.FieldsList.Count);
            var fields = target.FieldsList.Where(field => field.Name != "BASE_TYPE_NAME").Select(field =>
                field.Name == "ORDINAL_POSITION"
                    ? new Field(field.Name, Int64Type.Default, true)
                    : field).ToArray();
            var source = new Schema(fields, null);
            var arrays = new List<IArrowArray>();
            foreach (var field in fields)
            {
                if (field.Name == "ORDINAL_POSITION")
                    arrays.Add(new Int64Array.Builder().Append(0).Build());
                else if (field.Name == "TABLE_CAT")
                    arrays.Add(new StringArray.Builder().AppendNull().Build());
                else if (field.DataType.TypeId == ArrowTypeId.String)
                    arrays.Add(new StringArray.Builder().Append(field.Name == "TYPE_NAME" ? typeName : "value").Build());
                else if (field.DataType.TypeId == ArrowTypeId.Int8)
                    arrays.Add(new Int8Array.Builder().Append(0).Build());
                else if (field.DataType.TypeId == ArrowTypeId.Int16)
                    arrays.Add(new Int16Array.Builder().Append(0).Build());
                else if (field.Name == "BUFFER_LENGTH")
                    arrays.Add(new Int32Array.Builder().AppendNull().Build());
                else
                    arrays.Add(new Int32Array.Builder().Append(field.Name == "COLUMN_SIZE" ? columnSize
                        : field.Name == "DECIMAL_DIGITS" ? decimalDigits : 0).Build());
            }

            using var nativeBatch = new RecordBatch(source, arrays.ToArray(), 1);
            var result = Columns(new MetadataBatches(new List<RecordBatch> { nativeBatch }, true, "main"));
            using var reader = result.Stream!;
            using var batch = await reader.ReadNextRecordBatchAsync();

            Assert.NotNull(batch);
            Assert.Equal(expectedSize, ((Int32Array)batch.Column("COLUMN_SIZE")).GetValue(0));
            Assert.True(batch.Column("BUFFER_LENGTH").IsNull(0));
            Assert.Equal(expectedScale, ((Int32Array)batch.Column("DECIMAL_DIGITS")).GetValue(0));
            Assert.Equal(0, ((Int32Array)batch.Column("ORDINAL_POSITION")).GetValue(0));
            Assert.Equal("main", ((StringArray)batch.Column("TABLE_CAT")).GetString(0));
            Assert.Equal(ColumnMetadataHelper.GetBaseTypeName(typeName), ((StringArray)batch.Column("BASE_TYPE_NAME")).GetString(0));
        }

        [Fact]
        public async Task GetTables_DefaultsEmptyTypeBeforeFilteringAndFillsRequestedCatalog()
        {
            var schema = new Schema(MetadataSchemaFactory.CreateTablesSchema().FieldsList.Reverse(), null);
            var arrays = schema.FieldsList.Select(field => (IArrowArray)(field.Name switch
            {
                "TABLE_CAT" => new StringArray.Builder().AppendNull().Build(),
                "TABLE_TYPE" => new StringArray.Builder().Append("").Build(),
                _ => new StringArray.Builder().Append(field.Name).Build(),
            })).ToArray();
            using var nativeBatch = new RecordBatch(schema, arrays, 1);

            var result = Builder.Tables(
                new MetadataBatches(new List<RecordBatch> { nativeBatch }, true, "main", CatalogFilter.Exact("main")),
                new[] { "TABLE" });
            using var reader = result.Stream!;
            using var batch = await reader.ReadNextRecordBatchAsync();

            Assert.Equal(1, result.RowCount);
            Assert.Equal("main", ((StringArray)batch!.Column("TABLE_CAT")).GetString(0));
            Assert.Equal("TABLE", ((StringArray)batch.Column("TABLE_TYPE")).GetString(0));
        }

        [Fact]
        public async Task GetSchemas_FillsMissingCatalogByFieldName()
        {
            var schema = new Schema(MetadataSchemaFactory.CreateSchemasSchema().FieldsList.Reverse(), null);
            using var nativeBatch = new RecordBatch(schema, new IArrowArray[]
            {
                new StringArray.Builder().AppendNull().Build(),
                new StringArray.Builder().Append("default").Build(),
            }, 1);
            var response = new MetadataBatches(new List<RecordBatch> { nativeBatch }, true,
                "main", CatalogFilter.Exact("main"));

            var result = Builder.Schemas(response);
            using var reader = result.Stream!;
            using var batch = (await reader.ReadNextRecordBatchAsync())!;

            Assert.Equal("main", ((StringArray)batch.Column("TABLE_CATALOG")).GetString(0));
            Assert.Equal("default", ((StringArray)batch.Column("TABLE_SCHEM")).GetString(0));
            var decoded = Assert.Single(MetadataRowReader.Schemas(response));
            Assert.Equal("main", decoded.Catalog);
            Assert.Equal("default", decoded.Schema);
        }

        [Theory]
        [InlineData(null, false)]
        [InlineData(null, true)]
        [InlineData("main", false)]
        [InlineData("main", true)]
        public async Task GetTables_FlatAndDecodedResultsShareCatalogFilteringAndThriftOrder(
            string? catalog, bool filterTypes)
        {
            var schema = MetadataSchemaFactory.CreateTablesSchema();
            (string? Catalog, string Schema, string Table, string? Type)[] tables =
            {
                ("other", "a", "other_table", "TABLE"),
                (null, "b", "b_a", ""),
                ("main", "a", "a_view", "VIEW"),
                ("main", "a", "a_z", "TABLE"),
                ("main", "a", "a_a", null),
                ("main", "b", "b_z", "TABLE"),
                ("main", "a", "a_b", "TABLE"),
            };
            using var nativeBatch = NativeBatch(schema, tables.Length, (field, row) => field switch
            {
                "TABLE_CAT" => tables[row].Catalog,
                "TABLE_SCHEM" => tables[row].Schema,
                "TABLE_NAME" => tables[row].Table,
                "TABLE_TYPE" => tables[row].Type,
                _ => "server_value",
            });
            string[]? tableTypes = filterTypes ? new[] { "TABLE" } : null;
            var response = new MetadataBatches(new List<RecordBatch> { nativeBatch }, true,
                catalog, CatalogFilter.Exact(catalog));
            var decoded = MetadataRowReader.Tables(response, tableTypes);
            var result = Builder.Tables(response, tableTypes);
            using var reader = result.Stream!;
            using var batch = (await reader.ReadNextRecordBatchAsync())!;
            var names = (StringArray)batch.Column("TABLE_NAME");
            string[] expected = catalog == null
                ? new[] { "b_a", "a_a", "a_b", "a_z", "b_z", "other_table" }
                : new[] { "a_a", "a_b", "a_z", "b_a", "b_z" };
            if (!filterTypes) expected = expected.Concat(new[] { "a_view" }).ToArray();

            Assert.Equal(expected, decoded.Select(row => row.Table));
            Assert.Equal(expected, Enumerable.Range(0, batch.Length).Select(row => names.GetString(row)));
            Assert.All(Enumerable.Range(0, batch.Length), row =>
                Assert.Equal("server_value", ((StringArray)batch.Column("TYPE_CAT")).GetString(row)));
        }

        [Theory]
        [InlineData((int)MetadataOperation.GetSchemas, "%", true, 0)]
        [InlineData((int)MetadataOperation.GetColumns, "compar%", true, 0)]
        [InlineData((int)MetadataOperation.GetColumns, @"comparator\_tests", true, 0)]
        [InlineData((int)MetadataOperation.GetSchemas, "comparator_tests", true, 1)]
        [InlineData((int)MetadataOperation.GetColumns, "COMPARATOR_TESTS", true, 1)]
        [InlineData((int)MetadataOperation.GetSchemas, "compar%", false, 2)]
        public void NativeSchemasAndColumns_FilterLiteralCatalogsOnlyWhenRequested(
            int operationCode, string requestedCatalog, bool requireExactCatalog, int expectedRows)
        {
            var operation = (MetadataOperation)operationCode;
            var target = operation == MetadataOperation.GetSchemas
                ? MetadataSchemaFactory.CreateSchemasSchema()
                : MetadataSchemaFactory.CreateColumnMetadataSchema();
            string catalogField = operation == MetadataOperation.GetSchemas ? "TABLE_CATALOG" : "TABLE_CAT";
            string[] catalogs = { "comparator_tests", "comparator-tests" };
            using var nativeBatch = NativeBatch(target, 2, (field, row) =>
                field == catalogField ? catalogs[row] : field == "TYPE_NAME" ? "INT" : "value");
            var response = new MetadataBatches(new List<RecordBatch> { nativeBatch }, true, requestedCatalog,
                CatalogFilter.Exact(requireExactCatalog ? requestedCatalog : null));

            var result = operation == MetadataOperation.GetSchemas ? Builder.Schemas(response) : Columns(response);

            Assert.Equal(expectedRows, result.RowCount);
        }

        [Theory]
        [InlineData((int)MetadataOperation.GetCatalogs)]
        [InlineData((int)MetadataOperation.GetSchemas)]
        [InlineData((int)MetadataOperation.GetPrimaryKeys)]
        [InlineData((int)MetadataOperation.GetCrossReference)]
        public async Task OtherNativeOperations_PreserveTheirSchemas(int operationCode)
        {
            var operation = (MetadataOperation)operationCode;
            var schema = operation switch
            {
                MetadataOperation.GetCatalogs => MetadataSchemaFactory.CreateCatalogsSchema(),
                MetadataOperation.GetSchemas => MetadataSchemaFactory.CreateSchemasSchema(),
                MetadataOperation.GetPrimaryKeys => MetadataSchemaFactory.CreatePrimaryKeysSchema(),
                _ => MetadataSchemaFactory.CreateCrossReferenceSchema(),
            };
            var arrays = schema.FieldsList.Select(field => field.DataType.TypeId switch
            {
                ArrowTypeId.String => (IArrowArray)new StringArray.Builder().Append("value").Build(),
                ArrowTypeId.Int8 => new Int8Array.Builder().Append(1).Build(),
                ArrowTypeId.Int16 => new Int16Array.Builder().Append(1).Build(),
                ArrowTypeId.Int32 => new Int32Array.Builder().Append(1).Build(),
                _ => (IArrowArray)new Int64Array.Builder().Append(1).Build(),
            }).ToArray();
            using var nativeBatch = new RecordBatch(schema, arrays, 1);
            var response = new MetadataBatches(new List<RecordBatch> { nativeBatch }, true);

            var result = operation switch
            {
                MetadataOperation.GetCatalogs => Builder.Catalogs(response),
                MetadataOperation.GetSchemas => Builder.Schemas(response),
                MetadataOperation.GetPrimaryKeys => Builder.PrimaryKeys(response, "main", "default", "t"),
                _ => Builder.ForeignKeys(response, null, null, null, "main", "default", "t"),
            };
            using var reader = result.Stream!;
            using var batch = await reader.ReadNextRecordBatchAsync();

            Assert.Equal(1, result.RowCount);
            Assert.Equal(schema.FieldsList.Count, batch!.ColumnCount);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void GetColumns_RejectsWrongColumnNameOrType(bool wrongType)
        {
            var target = MetadataSchemaFactory.CreateColumnMetadataSchema();
            var fields = target.FieldsList.Take(23).Select(field =>
                field.Name == "COLUMN_NAME"
                    ? new Field(wrongType ? field.Name : "wrong_name", wrongType ? (IArrowType)Int32Type.Default : field.DataType, true)
                    : field).ToArray();
            var schema = new Schema(fields, null);
            var arrays = fields.Select(field => field.DataType.TypeId switch
            {
                ArrowTypeId.String => (IArrowArray)new StringArray.Builder().Append("value").Build(),
                ArrowTypeId.Int8 => new Int8Array.Builder().Append(0).Build(),
                ArrowTypeId.Int16 => new Int16Array.Builder().Append(0).Build(),
                ArrowTypeId.Int32 => new Int32Array.Builder().Append(0).Build(),
                _ => (IArrowArray)new Int64Array.Builder().Append(0).Build(),
            }).ToArray();
            using var nativeBatch = new RecordBatch(schema, arrays, 1);

            Assert.Throws<DatabricksException>(() =>
                Columns(new MetadataBatches(new List<RecordBatch> { nativeBatch }, true)));
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

            var flat = Builder.Columns(result.Results, result.Rows);
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
                MetadataOperation.GetSchemas => NativeBatch(MetadataSchemaFactory.CreateSchemasSchema(), 1,
                    (field, _) => field == "TABLE_SCHEM" ? "default" : null),
                MetadataOperation.GetTables => NativeBatch(MetadataSchemaFactory.CreateTablesSchema(), 1,
                    (field, _) => field switch
                    {
                        "TABLE_SCHEM" => "default",
                        "TABLE_NAME" => "t",
                        "TABLE_TYPE" => "TABLE",
                        _ => null,
                    }),
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

            var flat = operation switch
            {
                MetadataOperation.GetSchemas => Builder.Schemas(result),
                MetadataOperation.GetTables => Builder.Tables(result, null),
                _ => Columns(result),
            };
            using var stream = flat.Stream!;
            using var output = (await stream.ReadNextRecordBatchAsync())!;
            Assert.Equal(expected.Length, output.Length);
            if (expectedCatalog != null)
                Assert.Equal(expectedCatalog, ((StringArray)output.Column(
                    operation == MetadataOperation.GetSchemas ? "TABLE_CATALOG" : "TABLE_CAT")).GetString(0));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task NativeKeys_PassServerFieldsThroughAndFilterParents(bool foreign)
        {
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

            // Native keys ignore the requested-table fallbacks; parents match case-insensitively.
            var result = foreign
                ? Builder.ForeignKeys(response, "main", "sales", "customers", "foreign", "default", "orders")
                : Builder.PrimaryKeys(response, "fallback", "fallback", "fallback");
            using var stream = result.Stream!;
            using var flat = (await stream.ReadNextRecordBatchAsync())!;
            Assert.Equal(1, flat.Length);
            Assert.Equal(7, ((Int32Array)flat.Column("KEQ_SEQ")).GetValue(0));
            Assert.Equal("PK_NAME", ((StringArray)flat.Column("PK_NAME")).GetString(0));
            Assert.Equal("Main", ((StringArray)flat.Column(foreign ? "PKTABLE_CAT" : "TABLE_CAT")).GetString(0));

            if (foreign)
            {
                Assert.Equal("FKTABLE_CAT", ((StringArray)flat.Column("FKTABLE_CAT")).GetString(0));
                var filtered = Builder.ForeignKeys(response, "other", null, null, "foreign", "default", "orders");
                using var filteredStream = filtered.Stream!;
                Assert.Equal(0, filtered.RowCount);
            }
        }

        private static QueryResult Columns(MetadataBatches response)
            => Builder.Columns(new[] { response }, System.Array.Empty<ColumnRow>());
    }
}
