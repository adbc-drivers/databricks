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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    /// <summary>
    /// Unit tests for <see cref="TableTypeDefaultingStream"/> — the CI-runnable regression gate for
    /// the JDBC-parity empty/null <c>TABLE_TYPE</c> → <c>"TABLE"</c> substitution that the C# ADBC
    /// comparator exercises against live <c>hive_metastore</c> tables.
    ///
    /// Covers:
    ///   - Empty-string and null TABLE_TYPE entries are replaced with "TABLE".
    ///   - A non-empty value (e.g. "VIEW") is preserved (fill only touches empty/null).
    ///   - A sibling column is passed through untouched.
    ///   - A result with no TABLE_TYPE column is returned unchanged.
    /// </summary>
    public class TableTypeDefaultingStreamTests
    {
        [Fact]
        public async Task EmptyAndNullTableType_DefaultedToTable_OthersPreserved()
        {
            // getTables-shaped result: TABLE_NAME sibling + the TABLE_TYPE column under test.
            Schema schema = new Schema.Builder()
                .Field(new Field("TABLE_NAME", StringType.Default, nullable: true))
                .Field(new Field("TABLE_TYPE", StringType.Default, nullable: true))
                .Build();

            StringArray.Builder names = new StringArray.Builder();
            names.Append("trim_repro");       // empty type  → TABLE
            names.Append("trim_repro_v2");    // null type   → TABLE
            names.Append("trim_repro_view");  // "VIEW"       → preserved
            names.Append("real_table");       // "TABLE"      → preserved
            StringArray nameColumn = names.Build();

            StringArray.Builder types = new StringArray.Builder();
            types.Append("");        // empty → TABLE
            types.AppendNull();      // null  → TABLE
            types.Append("VIEW");    // preserved
            types.Append("TABLE");   // preserved
            StringArray typeColumn = types.Build();

            RecordBatch batch = new RecordBatch(schema, new IArrowArray[] { nameColumn, typeColumn }, 4);

            using IArrowArrayStream inner = new StubArrowArrayStream(schema, new[] { batch });
            using TableTypeDefaultingStream stream = new TableTypeDefaultingStream(inner);

            RecordBatch? result = await stream.ReadNextRecordBatchAsync(CancellationToken.None);
            Assert.NotNull(result);
            Assert.Equal(4, result!.Length);

            StringArray outTypes = Assert.IsType<StringArray>(result.Column(1));
            Assert.Equal("TABLE", outTypes.GetString(0)); // was empty
            Assert.Equal("TABLE", outTypes.GetString(1)); // was null
            Assert.False(outTypes.IsNull(1));
            Assert.Equal("VIEW", outTypes.GetString(2));  // preserved
            Assert.Equal("TABLE", outTypes.GetString(3)); // preserved

            // Sibling column passes through untouched.
            StringArray outNames = Assert.IsType<StringArray>(result.Column(0));
            Assert.Same(nameColumn, outNames);
        }

        [Fact]
        public async Task NoTableTypeColumn_BatchReturnedUnchanged()
        {
            Schema schema = new Schema.Builder()
                .Field(new Field("TABLE_NAME", StringType.Default, nullable: true))
                .Build();

            StringArray.Builder names = new StringArray.Builder();
            names.Append("x");
            RecordBatch batch = new RecordBatch(schema, new IArrowArray[] { names.Build() }, 1);

            using IArrowArrayStream inner = new StubArrowArrayStream(schema, new[] { batch });
            using TableTypeDefaultingStream stream = new TableTypeDefaultingStream(inner);

            RecordBatch? result = await stream.ReadNextRecordBatchAsync(CancellationToken.None);
            Assert.NotNull(result);
            // No TABLE_TYPE column: the original batch instance is returned as-is.
            Assert.Same(batch, result);
        }

        private sealed class StubArrowArrayStream : IArrowArrayStream
        {
            private readonly Queue<RecordBatch> _batches;

            public StubArrowArrayStream(Schema schema, IEnumerable<RecordBatch> batches)
            {
                Schema = schema;
                _batches = new Queue<RecordBatch>(batches);
            }

            public Schema Schema { get; }

            public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default) =>
                new ValueTask<RecordBatch?>(_batches.Count > 0 ? _batches.Dequeue() : null);

            public void Dispose() { }
        }
    }
}
