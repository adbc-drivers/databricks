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
    }
}
