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
    /// Unit tests for <see cref="NullColumnSerializingStream"/>. These run without a warehouse
    /// and are the CI-runnable regression gate for the untyped-NULL (SQL VOID) conversion that
    /// the E2E <c>UntypedNullColumnReportsStringType_*</c> tests exercise against live data.
    ///
    /// The stream is constructed with an inner reader that exposes the manifest schema
    /// (StringType for the VOID column, carrying <c>Spark:DataType:SqlName=VOID</c> metadata for
    /// detection). The record batches contain the native Arrow <c>NullArray</c> the IPC bytes
    /// would deliver. Mirrors the sibling <see cref="IntervalSerializingStream"/> /
    /// <see cref="ComplexTypeSerializingStream"/> unit tests.
    ///
    /// Covers:
    ///   - A VOID column is detected and converted to an all-null <c>StringArray</c> of equal length.
    ///   - A non-VOID sibling column in the same batch is passed through untouched.
    ///   - The declared (manifest) schema stays StringType.
    ///   - A batch with no VOID column is returned unchanged.
    /// </summary>
    public class NullColumnSerializingStreamTests
    {
        [Fact]
        public async Task VoidColumn_ConvertedToAllNullStringArray_SiblingUntouched()
        {
            const int length = 3;

            // Manifest schema: both fields declared StringType. Only column 0 is tagged VOID.
            Schema manifestSchema = new Schema.Builder()
                .Field(new Field("untyped_null", StringType.Default, nullable: true,
                    new Dictionary<string, string> { ["Spark:DataType:SqlName"] = "VOID" }))
                .Field(new Field("typed_null_string", StringType.Default, nullable: true,
                    new Dictionary<string, string> { ["Spark:DataType:SqlName"] = "STRING" }))
                .Build();

            // Native batch: the VOID column arrives as a NullArray; the sibling as a StringArray.
            var nullColumn = new NullArray(length);
            StringArray.Builder sb = new StringArray.Builder();
            sb.Append("a");
            sb.AppendNull();
            sb.Append("c");
            StringArray siblingColumn = sb.Build();

            Schema nativeSchema = new Schema.Builder()
                .Field(new Field("untyped_null", nullColumn.Data.DataType, nullable: true))
                .Field(new Field("typed_null_string", StringType.Default, nullable: true))
                .Build();
            RecordBatch nativeBatch = new RecordBatch(nativeSchema,
                new IArrowArray[] { nullColumn, siblingColumn }, length);

            using IArrowArrayStream inner = new StubArrowArrayStream(manifestSchema, new[] { nativeBatch });
            using NullColumnSerializingStream stream = new NullColumnSerializingStream(inner);

            // Declared schema must stay StringType for both columns.
            Assert.Equal(ArrowTypeId.String, stream.Schema.GetFieldByIndex(0).DataType.TypeId);
            Assert.Equal(ArrowTypeId.String, stream.Schema.GetFieldByIndex(1).DataType.TypeId);

            RecordBatch? result = await stream.ReadNextRecordBatchAsync(CancellationToken.None);
            Assert.NotNull(result);
            Assert.Equal(length, result!.Length);

            // Column 0 (VOID) is converted to an all-null StringArray of the same length.
            StringArray converted = Assert.IsType<StringArray>(result.Column(0));
            Assert.Equal(length, converted.Length);
            for (int i = 0; i < length; i++)
            {
                Assert.True(converted.IsNull(i), "converted VOID column entry should be null");
            }

            // Column 1 (STRING sibling) is passed through untouched.
            StringArray passthrough = Assert.IsType<StringArray>(result.Column(1));
            Assert.Same(siblingColumn, passthrough);
            Assert.Equal("a", passthrough.GetString(0));
            Assert.True(passthrough.IsNull(1));
            Assert.Equal("c", passthrough.GetString(2));
        }

        [Fact]
        public async Task NoVoidColumn_BatchReturnedUnchanged()
        {
            Schema manifestSchema = new Schema.Builder()
                .Field(new Field("s", StringType.Default, nullable: true,
                    new Dictionary<string, string> { ["Spark:DataType:SqlName"] = "STRING" }))
                .Build();

            StringArray.Builder sb = new StringArray.Builder();
            sb.Append("x");
            StringArray column = sb.Build();
            RecordBatch nativeBatch = new RecordBatch(manifestSchema, new IArrowArray[] { column }, 1);

            using IArrowArrayStream inner = new StubArrowArrayStream(manifestSchema, new[] { nativeBatch });
            using NullColumnSerializingStream stream = new NullColumnSerializingStream(inner);

            RecordBatch? result = await stream.ReadNextRecordBatchAsync(CancellationToken.None);
            Assert.NotNull(result);
            // No VOID column detected: the original batch instance is returned as-is.
            Assert.Same(nativeBatch, result);
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
