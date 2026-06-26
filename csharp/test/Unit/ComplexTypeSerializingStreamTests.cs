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
        public async Task Array_Double_IntegralIsBareNumber()
        {
            // System.Text.Json renders integral doubles without a fractional part (1.0 -> 1).
            // We accept that: it's valid JSON and round-trips to the same value.
            ListArray.Builder b = new ListArray.Builder(DoubleType.Default);
            DoubleArray.Builder vb = (DoubleArray.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(1.0);
            vb.Append(2.0);
            vb.Append(3.0);
            Assert.Equal("[1,2,3]", await Serialize("ARRAY<DOUBLE>", b.Build()));
        }

        [Fact]
        public async Task Array_Double_FractionalUnchanged()
        {
            ListArray.Builder b = new ListArray.Builder(DoubleType.Default);
            DoubleArray.Builder vb = (DoubleArray.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(1.1);
            vb.Append(2.5);
            string? json = await Serialize("ARRAY<DOUBLE>", b.Build());
            // The exact text of a fractional double is framework-dependent: .NET Core emits the
            // shortest round-trippable form ("1.1"), while .NET Framework's System.Text.Json emits
            // the full "1.1000000000000001". Both are valid JSON that round-trip to the same value.
#if NETFRAMEWORK
            Assert.Equal("[1.1000000000000001,2.5]", json);
#else
            Assert.Equal("[1.1,2.5]", json);
#endif
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

        [Fact]
        public async Task Array_DayTimeDuration_Format()
        {
            // INTERVAL DAY TO SECOND arrives as DurationArray; serialized via the same
            // "D HH:MM:SS.nnnnnnnnn" format IntervalSerializingStream uses for top-level columns.
            ListArray.Builder b = new ListArray.Builder(DurationType.Microsecond);
            DurationArray.Builder vb = (DurationArray.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(304_215_000_000L); // 3 d 12 h 30 m 15 s
            Assert.Equal("[\"3 12:30:15.000000000\"]",
                await Serialize("ARRAY<INTERVAL DAY TO SECOND>", b.Build()));
        }

        [Fact]
        public async Task Map_StringValueWithQuotes_IsEscapedJson()
        {
            // A MAP value containing double quotes must be escaped as \" (relaxed encoder),
            // not the HTML-safe " — and must round-trip. This is the PECO-3032 core case.
            MapArray.Builder mb = new MapArray.Builder(new MapType(StringType.Default, StringType.Default));
            StringArray.Builder kb = (StringArray.Builder)mb.KeyBuilder;
            StringArray.Builder vb = (StringArray.Builder)mb.ValueBuilder;
            mb.Append();
            kb.Append("key1");
            vb.Append("val \"quote\"");
            Assert.Equal("{\"key1\":\"val \\\"quote\\\"\"}", await Serialize("MAP<STRING,STRING>", mb.Build()));
        }

        [Fact]
        public async Task Array_Bool_IsBareBoolean()
        {
            ListArray.Builder b = new ListArray.Builder(BooleanType.Default);
            BooleanArray.Builder vb = (BooleanArray.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(true);
            vb.Append(false);
            Assert.Equal("[true,false]", await Serialize("ARRAY<BOOLEAN>", b.Build()));
        }

        [Fact]
        public async Task Array_Long_IsBareNumber()
        {
            ListArray.Builder b = new ListArray.Builder(Int64Type.Default);
            Int64Array.Builder vb = (Int64Array.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(10L);
            vb.Append(-20L);
            Assert.Equal("[10,-20]", await Serialize("ARRAY<BIGINT>", b.Build()));
        }

        [Fact]
        public async Task Array_NullElement_EmitsJsonNull()
        {
            // A null inside the list must serialize as a bare JSON null, not be dropped.
            ListArray.Builder b = new ListArray.Builder(Int32Type.Default);
            Int32Array.Builder vb = (Int32Array.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(1);
            vb.AppendNull();
            vb.Append(3);
            Assert.Equal("[1,null,3]", await Serialize("ARRAY<INT>", b.Build()));
        }

        [Fact]
        public async Task Array_String_UsesRelaxedEncoder()
        {
            // The relaxed encoder escapes a double quote as \" but emits <, >, &, and non-ASCII
            // characters verbatim (not \uXXXX). This is the property the WriterOptions encoder must
            // preserve for the output to remain compact, valid JSON that round-trips.
            ListArray.Builder b = new ListArray.Builder(StringType.Default);
            StringArray.Builder vb = (StringArray.Builder)b.ValueBuilder;
            b.Append();
            vb.Append("a\"<b>&é→");
            Assert.Equal("[\"a\\\"<b>&é→\"]", await Serialize("ARRAY<STRING>", b.Build()));
        }

        [Fact]
        public async Task Struct_SerializesAsObject_PreservingFieldOrder()
        {
            // STRUCT becomes a JSON object whose keys follow Arrow field order (not sorted).
            Int32Array id = new Int32Array.Builder().Append(7).Build();
            StringArray name = new StringArray.Builder().Append("bob").Build();
            BooleanArray active = new BooleanArray.Builder().Append(true).Build();
            List<Field> fields = new List<Field>
            {
                new Field("id", Int32Type.Default, nullable: true),
                new Field("name", StringType.Default, nullable: true),
                new Field("active", BooleanType.Default, nullable: true),
            };
            StructArray s = new StructArray(new StructType(fields), 1,
                new IArrowArray[] { id, name, active }, AllValid(1));
            Assert.Equal("{\"id\":7,\"name\":\"bob\",\"active\":true}",
                await Serialize("STRUCT<id:INT,name:STRING,active:BOOLEAN>", s));
        }

        [Fact]
        public async Task Struct_NullField_EmitsJsonNull()
        {
            // A null struct field serializes as a JSON null rather than being omitted.
            Int32Array a = new Int32Array.Builder().AppendNull().Build();
            StringArray b = new StringArray.Builder().Append("x").Build();
            List<Field> fields = new List<Field>
            {
                new Field("a", Int32Type.Default, nullable: true),
                new Field("b", StringType.Default, nullable: true),
            };
            StructArray s = new StructArray(new StructType(fields), 1,
                new IArrowArray[] { a, b }, AllValid(1));
            Assert.Equal("{\"a\":null,\"b\":\"x\"}", await Serialize("STRUCT<a:INT,b:STRING>", s));
        }

        [Fact]
        public async Task Struct_NestedArrayAndScalar_RecursesCorrectly()
        {
            // STRUCT<tags: ARRAY<STRING>, score: INT> — exercises recursion through a nested list.
            ListArray.Builder tagsBuilder = new ListArray.Builder(StringType.Default);
            StringArray.Builder tagValues = (StringArray.Builder)tagsBuilder.ValueBuilder;
            tagsBuilder.Append();
            tagValues.Append("x");
            tagValues.Append("y");
            ListArray tags = tagsBuilder.Build();
            Int32Array score = new Int32Array.Builder().Append(42).Build();
            List<Field> fields = new List<Field>
            {
                new Field("tags", new ListType(StringType.Default), nullable: true),
                new Field("score", Int32Type.Default, nullable: true),
            };
            StructArray s = new StructArray(new StructType(fields), 1,
                new IArrowArray[] { tags, score }, AllValid(1));
            Assert.Equal("{\"tags\":[\"x\",\"y\"],\"score\":42}",
                await Serialize("STRUCT<tags:ARRAY<STRING>,score:INT>", s));
        }

        [Fact]
        public async Task Map_MultipleKeys_PreserveSourceOrderAndQuoted()
        {
            // MAP serializes to a JSON object whose keys follow the Arrow entry order the server
            // sent (which is already deterministic), not a re-sorted order.
            MapArray.Builder mb = new MapArray.Builder(new MapType(StringType.Default, Int32Type.Default));
            StringArray.Builder kb = (StringArray.Builder)mb.KeyBuilder;
            Int32Array.Builder vb = (Int32Array.Builder)mb.ValueBuilder;
            mb.Append();
            kb.Append("banana");
            vb.Append(2);
            kb.Append("apple");
            vb.Append(1);
            kb.Append("cherry");
            vb.Append(3);
            Assert.Equal("{\"banana\":2,\"apple\":1,\"cherry\":3}",
                await Serialize("MAP<STRING,INT>", mb.Build()));
        }

        [Fact]
        public async Task Map_NonStringKeys_AreStringified()
        {
            // Non-string MAP keys are rendered as their string form, in source order.
            MapArray.Builder mb = new MapArray.Builder(new MapType(Int32Type.Default, StringType.Default));
            Int32Array.Builder kb = (Int32Array.Builder)mb.KeyBuilder;
            StringArray.Builder vb = (StringArray.Builder)mb.ValueBuilder;
            mb.Append();
            kb.Append(3);
            vb.Append("three");
            kb.Append(1);
            vb.Append("one");
            kb.Append(2);
            vb.Append("two");
            Assert.Equal("{\"3\":\"three\",\"1\":\"one\",\"2\":\"two\"}",
                await Serialize("MAP<INT,STRING>", mb.Build()));
        }

        [Fact]
        public async Task Map_Empty_IsEmptyObject()
        {
            MapArray.Builder mb = new MapArray.Builder(new MapType(StringType.Default, Int32Type.Default));
            mb.Append(); // a row holding an empty map
            Assert.Equal("{}", await Serialize("MAP<STRING,INT>", mb.Build()));
        }

        [Fact]
        public async Task NullComplexValue_ProducesNullStringEntry()
        {
            // A null complex value (the whole list/map/struct is null) becomes a null in the
            // output StringArray, not the literal text "null".
            ListArray.Builder b = new ListArray.Builder(Int32Type.Default);
            b.AppendNull();
            string?[] rows = await SerializeRows("ARRAY<INT>", b.Build());
            Assert.Single(rows);
            Assert.Null(rows[0]);
        }

        [Fact]
        public async Task MultipleRows_SerializeIndependently()
        {
            // The writer reuses a single stream/Utf8JsonWriter across rows; this guards that the
            // buffer is reset between rows so values don't bleed together. Includes an empty list.
            ListArray.Builder b = new ListArray.Builder(Int32Type.Default);
            Int32Array.Builder vb = (Int32Array.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(1);
            vb.Append(2);
            b.Append();
            vb.Append(3);
            b.Append(); // empty list row
            string?[] rows = await SerializeRows("ARRAY<INT>", b.Build());
            Assert.Equal(new[] { "[1,2]", "[3]", "[]" }, rows);
        }

        [Fact]
        public async Task NonComplexColumn_PassesThrough_AndComplexColumnSchemaIsFlattened()
        {
            // A column without a complex SqlName must pass through untouched (same array instance,
            // same Arrow type), while the complex column's schema is flattened to StringType.
            ListArray.Builder listBuilder = new ListArray.Builder(Int32Type.Default);
            Int32Array.Builder listValues = (Int32Array.Builder)listBuilder.ValueBuilder;
            listBuilder.Append();
            listValues.Append(1);
            listValues.Append(2);
            listBuilder.Append();
            listValues.Append(3);
            ListArray complexColumn = listBuilder.Build();
            Int32Array plainColumn = new Int32Array.Builder().Append(100).Append(200).Build();

            Schema manifestSchema = new Schema.Builder()
                .Field(new Field("c0", StringType.Default, nullable: true,
                    new Dictionary<string, string> { ["Spark:DataType:SqlName"] = "ARRAY<INT>" }))
                .Field(new Field("c1", Int32Type.Default, nullable: true,
                    new Dictionary<string, string> { ["Spark:DataType:SqlName"] = "INT" }))
                .Build();
            Schema nativeSchema = new Schema.Builder()
                .Field(new Field("c0", complexColumn.Data.DataType, nullable: true))
                .Field(new Field("c1", Int32Type.Default, nullable: true))
                .Build();
            RecordBatch batch = new RecordBatch(nativeSchema,
                new IArrowArray[] { complexColumn, plainColumn }, 2);

            using IArrowArrayStream inner = new StubArrowArrayStream(manifestSchema, new[] { batch });
            using ComplexTypeSerializingStream stream = new ComplexTypeSerializingStream(inner);
            RecordBatch? result = await stream.ReadNextRecordBatchAsync(CancellationToken.None);

            Assert.NotNull(result);
            Assert.IsType<StringType>(result!.Schema.FieldsList[0].DataType);
            Assert.IsType<Int32Type>(result.Schema.FieldsList[1].DataType);

            StringArray converted = Assert.IsType<StringArray>(result.Column(0));
            Assert.Equal("[1,2]", converted.GetString(0));
            Assert.Equal("[3]", converted.GetString(1));

            // The non-complex column is passed through as the very same instance.
            Assert.Same(plainColumn, result.Column(1));
        }

        // --- Time32 / Time64 (regression: these formerly used WriteRawValue, producing invalid JSON) ---

        [Fact]
        public async Task Array_Time32_Seconds_IsQuotedString()
        {
            // 12:30:45 = 45045 seconds since midnight.
            // Must serialize as a quoted JSON string, not a raw value.
            ListArray.Builder b = new ListArray.Builder(new Time32Type(TimeUnit.Second));
            Time32Array.Builder vb = (Time32Array.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(45045); // 12*3600 + 30*60 + 45
#if NETFRAMEWORK
            Assert.Equal("[\"12:30:45\"]", await Serialize("ARRAY<TIME>", b.Build()));
#else
            Assert.Equal("[\"12:30:45.000000\"]", await Serialize("ARRAY<TIME>", b.Build()));
#endif
        }

        [Fact]
        public async Task Array_Time32_Milliseconds_IsQuotedString()
        {
            // 12:30:45.123 = 45045123 milliseconds since midnight.
            ListArray.Builder b = new ListArray.Builder(new Time32Type(TimeUnit.Millisecond));
            Time32Array.Builder vb = (Time32Array.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(45045123);
#if NETFRAMEWORK
            Assert.Equal("[\"12:30:45.1230000\"]", await Serialize("ARRAY<TIME>", b.Build()));
#else
            Assert.Equal("[\"12:30:45.123000\"]", await Serialize("ARRAY<TIME>", b.Build()));
#endif
        }

        [Fact]
        public async Task Array_Time64_Microseconds_IsQuotedString()
        {
            // 12:30:45.123456 = 45045123456 microseconds since midnight.
            ListArray.Builder b = new ListArray.Builder(new Time64Type(TimeUnit.Microsecond));
            Time64Array.Builder vb = (Time64Array.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(45045123456L);
#if NETFRAMEWORK
            Assert.Equal("[\"12:30:45.1234560\"]", await Serialize("ARRAY<TIME>", b.Build()));
#else
            Assert.Equal("[\"12:30:45.123456\"]", await Serialize("ARRAY<TIME>", b.Build()));
#endif
        }

        [Fact]
        public async Task Array_Time64_Nanoseconds_IsQuotedString()
        {
            // 12:30:45.123456789 = 45045123456789 nanoseconds since midnight.
            // Nanosecond precision → ticks = 45045123456789/100 = 450451234567 (truncated).
            ListArray.Builder b = new ListArray.Builder(new Time64Type(TimeUnit.Nanosecond));
            Time64Array.Builder vb = (Time64Array.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(45045123456700L); // exact tick boundary to avoid rounding differences
#if NETFRAMEWORK
            Assert.Equal("[\"12:30:45.1234567\"]", await Serialize("ARRAY<TIME>", b.Build()));
#else
            Assert.Equal("[\"12:30:45.123456\"]", await Serialize("ARRAY<TIME>", b.Build()));
#endif
        }

        // --- Other scalar types not yet covered ---

        [Fact]
        public async Task Array_Float_IsBareNumber()
        {
            ListArray.Builder b = new ListArray.Builder(FloatType.Default);
            FloatArray.Builder vb = (FloatArray.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(1.5f);
            vb.Append(2.0f);
            Assert.Equal("[1.5,2]", await Serialize("ARRAY<FLOAT>", b.Build()));
        }

        [Fact]
        public async Task Array_Int8_IsBareNumber()
        {
            ListArray.Builder b = new ListArray.Builder(Int8Type.Default);
            Int8Array.Builder vb = (Int8Array.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(42);
            vb.Append(-1);
            Assert.Equal("[42,-1]", await Serialize("ARRAY<TINYINT>", b.Build()));
        }

        [Fact]
        public async Task Array_Int16_IsBareNumber()
        {
            ListArray.Builder b = new ListArray.Builder(Int16Type.Default);
            Int16Array.Builder vb = (Int16Array.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(1000);
            vb.Append(-500);
            Assert.Equal("[1000,-500]", await Serialize("ARRAY<SMALLINT>", b.Build()));
        }

        [Fact]
        public async Task Array_UInt8_IsBareNumber()
        {
            ListArray.Builder b = new ListArray.Builder(UInt8Type.Default);
            UInt8Array.Builder vb = (UInt8Array.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(255);
            Assert.Equal("[255]", await Serialize("ARRAY<TINYINT_UNSIGNED>", b.Build()));
        }

        [Fact]
        public async Task Array_UInt16_IsBareNumber()
        {
            ListArray.Builder b = new ListArray.Builder(UInt16Type.Default);
            UInt16Array.Builder vb = (UInt16Array.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(65535);
            Assert.Equal("[65535]", await Serialize("ARRAY<SMALLINT_UNSIGNED>", b.Build()));
        }

        [Fact]
        public async Task Array_UInt32_IsBareNumber()
        {
            ListArray.Builder b = new ListArray.Builder(UInt32Type.Default);
            UInt32Array.Builder vb = (UInt32Array.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(4294967295U);
            Assert.Equal("[4294967295]", await Serialize("ARRAY<INT_UNSIGNED>", b.Build()));
        }

        [Fact]
        public async Task Array_UInt64_IsBareNumber()
        {
            ListArray.Builder b = new ListArray.Builder(UInt64Type.Default);
            UInt64Array.Builder vb = (UInt64Array.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(18446744073709551615UL);
            Assert.Equal("[18446744073709551615]", await Serialize("ARRAY<BIGINT_UNSIGNED>", b.Build()));
        }

        [Fact]
        public async Task Array_Date64_IsQuoted()
        {
            ListArray.Builder b = new ListArray.Builder(Date64Type.Default);
            Date64Array.Builder vb = (Date64Array.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(new DateTime(2024, 6, 15, 0, 0, 0, DateTimeKind.Unspecified));
            Assert.Equal("[\"2024-06-15\"]", await Serialize("ARRAY<DATE>", b.Build()));
        }

        [Fact]
        public async Task Array_Binary_IsBase64()
        {
            ListArray.Builder b = new ListArray.Builder(BinaryType.Default);
            BinaryArray.Builder vb = (BinaryArray.Builder)b.ValueBuilder;
            b.Append();
            vb.Append(new byte[] { 0x48, 0x65, 0x6C, 0x6C, 0x6F }); // "Hello"
            Assert.Equal("[\"SGVsbG8=\"]", await Serialize("ARRAY<BINARY>", b.Build()));
        }

        [Fact]
        public async Task Array_DayTimeInterval_Format()
        {
            ListArray.Builder b = new ListArray.Builder(new IntervalType(IntervalUnit.DayTime));
            DayTimeIntervalArray.Builder vb = (DayTimeIntervalArray.Builder)b.ValueBuilder;
            b.Append();
            // 3 days, 45045123 ms = 3 days 12:30:45.123
            vb.Append(new DayTimeInterval(3, 45045123));
            string? json = await Serialize("ARRAY<INTERVAL DAY TO SECOND>", b.Build());
            Assert.Equal("[\"3.12:30:45.1230000\"]", json);
        }

        [Fact]
        public async Task Array_MonthDayNanosecondInterval_Format()
        {
            ListArray.Builder b = new ListArray.Builder(new IntervalType(IntervalUnit.MonthDayNanosecond));
            MonthDayNanosecondIntervalArray.Builder vb = (MonthDayNanosecondIntervalArray.Builder)b.ValueBuilder;
            b.Append();
            // 14 months (1 year 2 months), 3 days, 45045123456789 ns
            vb.Append(new MonthDayNanosecondInterval(14, 3, 45045123456789L));
            string? json = await Serialize("ARRAY<INTERVAL>", b.Build());
            Assert.Contains("1-2", json); // year-month portion
            Assert.Contains("3.", json);  // day portion
        }

        // ----- helpers -----------------------------------------------------

        private static async Task<string?> Serialize(string sqlName, IArrowArray nativeArray) =>
            (await SerializeRows(sqlName, nativeArray))[0];

        private static async Task<string?[]> SerializeRows(string sqlName, IArrowArray nativeArray)
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
            string?[] rows = new string?[outArray.Length];
            for (int i = 0; i < outArray.Length; i++)
            {
                rows[i] = outArray.IsNull(i) ? null : outArray.GetString(i);
            }
            return rows;
        }

        // A validity bitmap marking all <paramref name="count"/> rows non-null.
        private static ArrowBuffer AllValid(int count)
        {
            ArrowBuffer.BitmapBuilder bitmap = new ArrowBuffer.BitmapBuilder();
            for (int i = 0; i < count; i++)
            {
                bitmap.Append(true);
            }
            return bitmap.Build();
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
