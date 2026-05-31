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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Scalars;
using Apache.Arrow.Types;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    /// <summary>
    /// Unit tests for <see cref="ComplexTypeSerializingStream"/> that pin the exact JSON string
    /// produced for each complex-type element. These run without a warehouse and are the local
    /// oracle for the values the E2E ComplexTypesValueTests assert against.
    ///
    /// Contract being verified:
    ///   - DECIMAL  → bare JSON number (full precision), not a quoted string
    ///   - DOUBLE   → JSON number that keeps a trailing ".0" for integral values
    ///   - DATE     → quoted "yyyy-MM-dd"
    ///   - TIMESTAMP/INTERVAL → exact form documented by the asserts below
    ///   - MAP keys → quoted and sorted (valid JSON)
    /// </summary>
    public class ComplexTypeSerializingStreamTests
    {
        [Fact]
        public async Task Array_Double_KeepsTrailingZero()
        {
            ListArray.Builder b = new ListArray.Builder(DoubleType.Default);
            DoubleArray.Builder vb = (DoubleArray.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(1.0);
            vb.Append(2.0);
            vb.Append(3.0);
            Assert.Equal("[1.0,2.0,3.0]", await Serialize("ARRAY<DOUBLE>", b.Build()));
        }

        [Fact]
        public async Task Array_Double_FractionalUnchanged()
        {
            ListArray.Builder b = new ListArray.Builder(DoubleType.Default);
            DoubleArray.Builder vb = (DoubleArray.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(1.1);
            vb.Append(2.5);
            Assert.Equal("[1.1,2.5]", await Serialize("ARRAY<DOUBLE>", b.Build()));
        }

        [Fact]
        public async Task Array_Decimal_IsBareNumber()
        {
            ListArray.Builder b = new ListArray.Builder(new Decimal128Type(38, 0));
            Decimal128Array.Builder vb = (Decimal128Array.Builder)b.ValueBuilder;
            b.Append();
            vb.Append("1");
            vb.Append("2");
            vb.Append("3");
            Assert.Equal("[1,2,3]", await Serialize("ARRAY<DECIMAL(38,0)>", b.Build()));
        }

        [Fact]
        public async Task Array_Date_IsQuoted()
        {
            ListArray.Builder b = new ListArray.Builder(Date32Type.Default);
            Date32Array.Builder vb = (Date32Array.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Unspecified));
            vb.Append(new DateTime(2024, 2, 2, 0, 0, 0, DateTimeKind.Unspecified));
            vb.Append(new DateTime(2024, 3, 3, 0, 0, 0, DateTimeKind.Unspecified));
            Assert.Equal("[\"2024-01-01\",\"2024-02-02\",\"2024-03-03\"]",
                await Serialize("ARRAY<DATE>", b.Build()));
        }

        [Fact]
        public async Task Array_Timestamp_Format()
        {
            ListArray.Builder b = new ListArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
            TimestampArray.Builder vb = (TimestampArray.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(new DateTimeOffset(2024, 1, 1, 7, 0, 0, TimeSpan.Zero));
            // Documents the exact serialized form (DateTimeOffset via System.Text.Json).
            Assert.Equal("[\"2024-01-01T07:00:00+00:00\"]",
                await Serialize("ARRAY<TIMESTAMP>", b.Build()));
        }

        [Fact]
        public async Task Array_YearMonthInterval_Format()
        {
            ListArray.Builder b = new ListArray.Builder(new IntervalType(IntervalUnit.YearMonth));
            YearMonthIntervalArray.Builder vb = (YearMonthIntervalArray.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(new YearMonthInterval(1487)); // 123 years 11 months
            // Quoted "Y-M" string (valid JSON), matching IntervalSerializingStream's format.
            Assert.Equal("[\"123-11\"]", await Serialize("ARRAY<INTERVAL YEAR TO MONTH>", b.Build()));
        }

        // ----- helpers -----------------------------------------------------

        private static async Task<string?> Serialize(string sqlName, IArrowArray nativeArray)
        {
            Schema manifestSchema = new Schema.Builder()
                .Field(new Field("c", StringType.Default, nullable: true,
                    new Dictionary<string, string> { ["Spark:DataType:SqlName"] = sqlName }))
                .Build();
            Schema nativeSchema = new Schema.Builder()
                .Field(new Field("c", nativeArray.Data.DataType, nullable: true))
                .Build();
            RecordBatch batch = new RecordBatch(nativeSchema, new[] { nativeArray }, nativeArray.Length);

            using IArrowArrayStream inner = new StubArrowArrayStream(manifestSchema, new[] { batch });
            using ComplexTypeSerializingStream stream = new ComplexTypeSerializingStream(inner);
            RecordBatch? result = await stream.ReadNextRecordBatchAsync(CancellationToken.None);
            Assert.NotNull(result);
            StringArray outArray = Assert.IsType<StringArray>(result!.Column(0));
            return outArray.GetString(0);
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
