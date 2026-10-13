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
using AdbcDrivers.Databricks.StatementExecution.Metadata;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Types;
using Xunit;
using static AdbcDrivers.Databricks.Tests.Unit.StatementExecution.Metadata.MetadataFixtures;
using Legacy = AdbcDrivers.Databricks.StatementExecution.Metadata.Legacy;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution.Metadata
{
    // SHOW-only parsing that must keep its existing behavior.
    public class LegacyMetadataTests
    {
        [Fact]
        public async Task ShowTables_OnlyGetObjectsDefaultsEmptyType()
        {
            using var batch = Strings(
                ("catalogName", new string?[] { "main" }),
                ("namespace", new string?[] { "default" }),
                ("tableName", new string?[] { "t" }),
                ("tableType", new string?[] { "" }));
            var result = new MetadataBatches(new List<RecordBatch> { batch }, false);

            Assert.Equal("TABLE", Assert.Single(MetadataRowReader.Tables(result)).TableType);
            Assert.Single(MetadataRowReader.Tables(result, new[] { "TABLE" }));

            var flat = Legacy.MetadataResultBuilder.Instance.Tables(result, null);
            using var stream = flat.Stream!;
            using var output = (await stream.ReadNextRecordBatchAsync())!;
            Assert.Equal("", ((StringArray)output.Column("TABLE_TYPE")).GetString(0));
            var filtered = Legacy.MetadataResultBuilder.Instance.Tables(result, new[] { "TABLE" });
            using var filteredStream = filtered.Stream!;
            Assert.Equal(0, filtered.RowCount);
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

            var rows = Legacy.MetadataDecoder.Instance.PrimaryKeys(result, "main", "default", "table").ToList();

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

            var row = Assert.Single(Legacy.MetadataDecoder.Instance.ForeignKeys(
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
            Assert.Equal(2, Legacy.MetadataDecoder.Instance.ForeignKeys(
                result, null, null, null, "foreign", "default", "orders").Count());
        }

        [Fact]
        public async Task Catalogs_BuildThriftColumns()
        {
            using var batch = Strings(("catalog", new string?[] { "main", "other" }));

            var rows = await Rows(Builder.Catalogs(new MetadataBatches(new List<RecordBatch> { batch }, false)));

            Assert.Equal(new[] { new[] { ("TABLE_CAT", (object?)"main") }, new[] { ("TABLE_CAT", (object?)"other") } }, rows);
        }

        [Fact]
        public async Task Schemas_BuildThriftColumns()
        {
            using var batch = Strings(
                ("databaseName", new string?[] { "default", "sales" }),
                ("catalog", new string?[] { "main", "other" }));

            var rows = await Rows(Builder.Schemas(new MetadataBatches(new List<RecordBatch> { batch }, false)));

            Assert.Equal(new[]
            {
                new[] { ("TABLE_SCHEM", (object?)"default"), ("TABLE_CATALOG", "main") },
                new[] { ("TABLE_SCHEM", (object?)"sales"), ("TABLE_CATALOG", "other") },
            }, rows);
        }

        [Fact]
        public async Task PrimaryKeys_BuildThriftColumnsFromServerValues()
        {
            using var batch = Row(("col_name", "id"), ("constraintName", "pk_orders"), ("keySeq", 2),
                ("catalogName", "main"), ("namespace", "sales"), ("tableName", "orders"));

            var rows = await Rows(Builder.PrimaryKeys(
                new MetadataBatches(new List<RecordBatch> { batch }, false), "x", "y", "z"));

            Assert.Equal(new (string, object?)[]
            {
                ("TABLE_CAT", "main"), ("TABLE_SCHEM", "sales"), ("TABLE_NAME", "orders"),
                ("COLUMN_NAME", "id"), ("KEQ_SEQ", 2), ("PK_NAME", "pk_orders"),
            }, Assert.Single(rows));
        }

        [Fact]
        public async Task ForeignKeys_BuildThriftColumnsFromServerValues()
        {
            using var batch = Row(("parentCatalogName", "p_cat"), ("parentNamespace", "p_schema"),
                ("parentTableName", "p_table"), ("parentColName", "p_col"), ("catalogName", "f_cat"),
                ("namespace", "f_schema"), ("tableName", "f_table"), ("col_name", "f_col"),
                ("constraintName", "fk_name"), ("keySeq", 2), ("updateRule", 3), ("deleteRule", 4),
                ("deferrability", 6));

            var rows = await Rows(Builder.ForeignKeys(new MetadataBatches(new List<RecordBatch> { batch }, false),
                "p_cat", "p_schema", "p_table", "x", "y", "z"));

            Assert.Equal(new (string, object?)[]
            {
                ("PKTABLE_CAT", "p_cat"), ("PKTABLE_SCHEM", "p_schema"), ("PKTABLE_NAME", "p_table"),
                ("PKCOLUMN_NAME", "p_col"), ("FKTABLE_CAT", "f_cat"), ("FKTABLE_SCHEM", "f_schema"),
                ("FKTABLE_NAME", "f_table"), ("FKCOLUMN_NAME", "f_col"), ("KEQ_SEQ", 2), ("UPDATE_RULE", 3),
                ("DELETE_RULE", 4), ("FK_NAME", "fk_name"), ("PK_NAME", null), ("DEFERRABILITY", 6),
            }, Assert.Single(rows));
        }

        private static IMetadataResultBuilder Builder => Legacy.MetadataResultBuilder.Instance;

        // A one-row SHOW result; int values become Int32 columns, everything else strings.
        private static RecordBatch Row(params (string Name, object Value)[] columns)
        {
            var schema = new Schema(columns.Select(column => new Field(column.Name,
                column.Value is int ? Int32Type.Default : StringType.Default, true)), null);
            var arrays = columns.Select(column => column.Value is int number
                ? (IArrowArray)new Int32Array.Builder().Append(number).Build()
                : new StringArray.Builder().Append((string)column.Value).Build()).ToArray();
            return new RecordBatch(schema, arrays, 1);
        }

        // Each row as (column, value) pairs in schema order.
        private static async Task<List<(string, object?)[]>> Rows(QueryResult result)
        {
            using var stream = result.Stream!;
            using var batch = (await stream.ReadNextRecordBatchAsync())!;
            return Enumerable.Range(0, batch.Length).Select(row => batch.Schema.FieldsList.Select((field, index) =>
            {
                var array = batch.Column(index);
                object? value = array.IsNull(row) ? null : array switch
                {
                    StringArray strings => strings.GetString(row),
                    Int32Array integers => integers.GetValue(row),
                    _ => throw new System.InvalidOperationException(field.Name),
                };
                return (field.Name, value);
            }).ToArray()).ToList();
        }
    }
}
