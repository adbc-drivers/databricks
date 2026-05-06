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
    /// Unit tests for <see cref="IntervalSerializingStream"/>.
    ///
    /// The stream is constructed with an inner reader that exposes the manifest schema
    /// (StringType for interval columns, with Spark:DataType:SqlName metadata for detection).
    /// The record batches contain native Arrow interval arrays (as the IPC bytes would).
    ///
    /// Covers:
    ///   - YearMonthIntervalArray → "Y-M" string
    ///   - DurationArray → "D HH:MM:SS.nnnnnnnnn" string
    ///   - Null handling for both types
    ///   - Schema pass-through (manifest schema is already StringType)
    ///   - Non-interval columns pass through unchanged
    /// </summary>
    public class IntervalSerializingStreamTests
    {
        // Helper: build a single-field manifest schema with Spark:DataType:SqlName metadata
        private static Schema ManifestSchema(string fieldName, string sqlName, IArrowType arrowType, bool nullable = true)
        {
            var metadata = new Dictionary<string, string> { ["Spark:DataType:SqlName"] = sqlName };
            return new Schema.Builder()
                .Field(new Field(fieldName, arrowType, nullable, metadata))
                .Build();
        }

        // -----------------------------------------------------------------------
        // FormatYearMonth static helper
        // -----------------------------------------------------------------------

        [Theory]
        [InlineData(0, "0-0")]          // zero
        [InlineData(12, "1-0")]         // exactly 1 year
        [InlineData(30, "2-6")]         // 2 years 6 months
        [InlineData(1, "0-1")]          // 1 month only
        [InlineData(25, "2-1")]         // 2 years 1 month
        [InlineData(-30, "-2-6")]       // negative: matches JDBC and server
        [InlineData(-22, "-1-10")]      // negative: 1 year 10 months
        [InlineData(-10, "-0-10")]      // negative: 0 years 10 months
        [InlineData(-1, "-0-1")]        // negative: 1 month
        public void FormatYearMonth_ReturnsExpectedString(int totalMonths, string expected)
        {
            string actual = IntervalSerializingStream.FormatYearMonth(totalMonths);
            Assert.Equal(expected, actual);
        }

        // -----------------------------------------------------------------------
        // FormatDuration static helper — microsecond unit (Databricks default)
        // -----------------------------------------------------------------------

        [Theory]
        [InlineData(0L, "0 00:00:00.000000000")]                                   // zero
        [InlineData(1_000_000L, "0 00:00:01.000000000")]                            // 1 second
        [InlineData(60_000_000L, "0 00:01:00.000000000")]                           // 1 minute
        [InlineData(3_600_000_000L, "0 01:00:00.000000000")]                        // 1 hour
        [InlineData(86_400_000_000L, "1 00:00:00.000000000")]                       // 1 day
        [InlineData(304_215_000_000L, "3 12:30:15.000000000")]                      // 3 d 12 h 30 m 15 s
        [InlineData(304_215_000_500L, "3 12:30:15.000500000")]                      // plus 500 us
        [InlineData(-304_215_000_000L, "-3 12:30:15.000000000")]                    // negative: matches JDBC and server
        // Databricks boundary: ±106,751,991 days (Long.MAX_VALUE microseconds).
        // These would silently overflow if converted to nanoseconds first (* 1_000).
        [InlineData(9_223_372_022_400_000_000L, "106751991 00:00:00.000000000")]    // max_positive
        [InlineData(-9_223_372_022_400_000_000L, "-106751991 00:00:00.000000000")]  // max_negative
        [InlineData(9_223_372_022_399_999_999L, "106751990 23:59:59.999999000")]    // near_max
        public void FormatDuration_Microseconds_ReturnsExpectedString(long rawUs, string expected)
        {
            string actual = IntervalSerializingStream.FormatDuration(rawUs, TimeUnit.Microsecond);
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void FormatDuration_Nanoseconds_ReturnsExpectedString()
        {
            // 3 days 12 h 30 m 15 s in nanoseconds
            long rawNs = 304_215L * 1_000_000_000L;
            string actual = IntervalSerializingStream.FormatDuration(rawNs, TimeUnit.Nanosecond);
            Assert.Equal("3 12:30:15.000000000", actual);
        }

        [Fact]
        public void FormatDuration_Seconds_ReturnsExpectedString()
        {
            // 3 days 12 h 30 m 15 s — and a large value that would overflow if multiplied to ns.
            long rawS = 304_215L;
            string actual = IntervalSerializingStream.FormatDuration(rawS, TimeUnit.Second);
            Assert.Equal("3 12:30:15.000000000", actual);

            // 106,751 days in seconds (just under the nanosecond overflow boundary)
            long largeSec = 106_751L * 86_400L;
            string largeActual = IntervalSerializingStream.FormatDuration(largeSec, TimeUnit.Second);
            Assert.Equal("106751 00:00:00.000000000", largeActual);
        }

        [Fact]
        public void FormatDuration_Milliseconds_ReturnsExpectedString()
        {
            // 3 days 12 h 30 m 15 s — and a large value that would overflow if multiplied to ns.
            long rawMs = 304_215_000L;
            string actual = IntervalSerializingStream.FormatDuration(rawMs, TimeUnit.Millisecond);
            Assert.Equal("3 12:30:15.000000000", actual);

            // 106,751 days in milliseconds (just under the nanosecond overflow boundary)
            long largeMs = 106_751L * 86_400_000L;
            string largeActual = IntervalSerializingStream.FormatDuration(largeMs, TimeUnit.Millisecond);
            Assert.Equal("106751 00:00:00.000000000", largeActual);
        }

        // -----------------------------------------------------------------------
        // Schema — the wrapper passes the manifest schema through as-is
        // -----------------------------------------------------------------------

        [Fact]
        public void Schema_YearMonthColumn_ReportsManifestStringType()
        {
            // Manifest schema: StringType for interval column (already correct output type)
            Schema schema = new Schema.Builder()
                .Field(f => f.Name("id").DataType(Int32Type.Default).Nullable(false))
                .Field(new Field("tenure", StringType.Default, nullable: true,
                    new Dictionary<string, string> { ["Spark:DataType:SqlName"] = "INTERVAL YEAR TO MONTH" }))
                .Build();

            using IArrowArrayStream inner = new StubArrowArrayStream(schema, System.Array.Empty<RecordBatch>());
            using IntervalSerializingStream stream = new IntervalSerializingStream(inner);

            Schema reported = stream.Schema;
            Assert.Equal(2, reported.FieldsList.Count);
            Assert.IsType<Int32Type>(reported.FieldsList[0].DataType);
            Assert.IsType<StringType>(reported.FieldsList[1].DataType);
            Assert.Equal("tenure", reported.FieldsList[1].Name);
        }

        [Fact]
        public void Schema_DurationColumn_ReportsManifestStringType()
        {
            Schema schema = new Schema.Builder()
                .Field(new Field("elapsed", StringType.Default, nullable: true,
                    new Dictionary<string, string> { ["Spark:DataType:SqlName"] = "INTERVAL DAY TO SECOND" }))
                .Field(f => f.Name("label").DataType(StringType.Default).Nullable(true))
                .Build();

            using IArrowArrayStream inner = new StubArrowArrayStream(schema, System.Array.Empty<RecordBatch>());
            using IntervalSerializingStream stream = new IntervalSerializingStream(inner);

            Schema reported = stream.Schema;
            Assert.Equal(2, reported.FieldsList.Count);
            Assert.IsType<StringType>(reported.FieldsList[0].DataType);
            Assert.Equal("elapsed", reported.FieldsList[0].Name);
            Assert.IsType<StringType>(reported.FieldsList[1].DataType);
        }

        [Fact]
        public void Schema_NonIntervalColumns_AreUnchanged()
        {
            Schema schema = new Schema.Builder()
                .Field(f => f.Name("a").DataType(Int64Type.Default).Nullable(false))
                .Field(f => f.Name("b").DataType(StringType.Default).Nullable(true))
                .Field(f => f.Name("c").DataType(DoubleType.Default).Nullable(true))
                .Build();

            using IArrowArrayStream inner = new StubArrowArrayStream(schema, System.Array.Empty<RecordBatch>());
            using IntervalSerializingStream stream = new IntervalSerializingStream(inner);

            Schema reported = stream.Schema;
            Assert.IsType<Int64Type>(reported.FieldsList[0].DataType);
            Assert.IsType<StringType>(reported.FieldsList[1].DataType);
            Assert.IsType<DoubleType>(reported.FieldsList[2].DataType);
        }

        // -----------------------------------------------------------------------
        // Data conversion — YearMonthIntervalArray → StringArray
        // -----------------------------------------------------------------------

        [Fact]
        public async Task ReadNextBatch_YearMonthColumn_ConvertsValues()
        {
            var ymBuilder = new YearMonthIntervalArray.Builder();
            ymBuilder.Append(new YearMonthInterval(30)); // "2-6"
            ymBuilder.AppendNull();
            ymBuilder.Append(new YearMonthInterval(12)); // "1-0"
            YearMonthIntervalArray ymArray = ymBuilder.Build();

            // Manifest schema: StringType + SqlName metadata (what all result paths expose)
            Schema manifestSchema = new Schema.Builder()
                .Field(new Field("tenure", StringType.Default, nullable: true,
                    new Dictionary<string, string> { ["Spark:DataType:SqlName"] = "INTERVAL YEAR TO MONTH" }))
                .Build();

            // Batch carries native arrays (as the IPC bytes would produce)
            Schema nativeSchema = new Schema.Builder()
                .Field(new Field("tenure", new IntervalType(IntervalUnit.YearMonth), nullable: true))
                .Build();
            RecordBatch batch = new RecordBatch(nativeSchema, new IArrowArray[] { ymArray }, ymArray.Length);

            using IArrowArrayStream inner = new StubArrowArrayStream(manifestSchema, new[] { batch });
            using IntervalSerializingStream stream = new IntervalSerializingStream(inner);

            RecordBatch? result = await stream.ReadNextRecordBatchAsync(CancellationToken.None);
            Assert.NotNull(result);
            Assert.Equal(3, result!.Length);

            StringArray strings = (StringArray)result.Column(0);
            Assert.Equal("2-6", strings.GetString(0));
            Assert.True(strings.IsNull(1));
            Assert.Equal("1-0", strings.GetString(2));
        }

        // -----------------------------------------------------------------------
        // Data conversion — DurationArray → StringArray
        // -----------------------------------------------------------------------

        [Fact]
        public async Task ReadNextBatch_DurationColumn_ConvertsValues()
        {
            // 3 d 12 h 30 m 15 s in microseconds = 304_215_000_000
            long us = 304_215_000_000L;

            var dBuilder = new DurationArray.Builder(DurationType.Microsecond);
            dBuilder.Append(us);
            dBuilder.AppendNull();
            DurationArray dArray = dBuilder.Build();

            Schema manifestSchema = new Schema.Builder()
                .Field(new Field("elapsed", StringType.Default, nullable: true,
                    new Dictionary<string, string> { ["Spark:DataType:SqlName"] = "INTERVAL DAY TO SECOND" }))
                .Build();

            Schema nativeSchema = new Schema.Builder()
                .Field(new Field("elapsed", DurationType.Microsecond, nullable: true))
                .Build();
            RecordBatch batch = new RecordBatch(nativeSchema, new IArrowArray[] { dArray }, dArray.Length);

            using IArrowArrayStream inner = new StubArrowArrayStream(manifestSchema, new[] { batch });
            using IntervalSerializingStream stream = new IntervalSerializingStream(inner);

            RecordBatch? result = await stream.ReadNextRecordBatchAsync(CancellationToken.None);
            Assert.NotNull(result);
            Assert.Equal(2, result!.Length);

            StringArray strings = (StringArray)result.Column(0);
            Assert.Equal("3 12:30:15.000000000", strings.GetString(0));
            Assert.True(strings.IsNull(1));
        }

        // -----------------------------------------------------------------------
        // Mixed schema: interval + non-interval columns
        // -----------------------------------------------------------------------

        [Fact]
        public async Task ReadNextBatch_MixedColumns_OnlyIntervalColumnsConverted()
        {
            var idBuilder = new Int32Array.Builder();
            idBuilder.Append(1);

            var ymBuilder = new YearMonthIntervalArray.Builder();
            ymBuilder.Append(new YearMonthInterval(30)); // "2-6"

            var nameBuilder = new StringArray.Builder();
            nameBuilder.Append("Alice");

            Schema manifestSchema = new Schema.Builder()
                .Field(f => f.Name("id").DataType(Int32Type.Default).Nullable(false))
                .Field(new Field("tenure", StringType.Default, nullable: true,
                    new Dictionary<string, string> { ["Spark:DataType:SqlName"] = "INTERVAL YEAR TO MONTH" }))
                .Field(f => f.Name("name").DataType(StringType.Default).Nullable(true))
                .Build();

            Schema nativeSchema = new Schema.Builder()
                .Field(f => f.Name("id").DataType(Int32Type.Default).Nullable(false))
                .Field(new Field("tenure", new IntervalType(IntervalUnit.YearMonth), nullable: true))
                .Field(f => f.Name("name").DataType(StringType.Default).Nullable(true))
                .Build();

            Int32Array idArray = idBuilder.Build();
            YearMonthIntervalArray ymArray = ymBuilder.Build();
            StringArray nameArray = nameBuilder.Build();

            RecordBatch batch = new RecordBatch(nativeSchema,
                new IArrowArray[] { idArray, ymArray, nameArray }, 1);

            using IArrowArrayStream inner = new StubArrowArrayStream(manifestSchema, new[] { batch });
            using IntervalSerializingStream stream = new IntervalSerializingStream(inner);

            RecordBatch? result = await stream.ReadNextRecordBatchAsync(CancellationToken.None);
            Assert.NotNull(result);

            Assert.IsType<Int32Array>(result!.Column(0));
            Assert.IsType<StringArray>(result.Column(1));
            Assert.IsType<StringArray>(result.Column(2));

            Assert.Equal("2-6", ((StringArray)result.Column(1)).GetString(0));
            Assert.Equal("Alice", ((StringArray)result.Column(2)).GetString(0));
        }

        // -----------------------------------------------------------------------
        // Helper: trivial IArrowArrayStream backed by a fixed list of batches
        // -----------------------------------------------------------------------

        private sealed class StubArrowArrayStream : IArrowArrayStream
        {
            private readonly Queue<RecordBatch> _batches;

            public StubArrowArrayStream(Schema schema, IEnumerable<RecordBatch> batches)
            {
                Schema = schema;
                _batches = new Queue<RecordBatch>(batches);
            }

            public Schema Schema { get; }

            public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                RecordBatch? batch = _batches.Count > 0 ? _batches.Dequeue() : null;
                return new ValueTask<RecordBatch?>(batch);
            }

            public void Dispose() { }
        }
    }
}
