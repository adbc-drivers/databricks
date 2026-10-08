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

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    public class GeospatialTransformingStreamTests
    {
        private static readonly byte[] s_pointOneTwo = FromHex(
            "0101000000000000000000f03f0000000000000040");
        private static readonly byte[] s_line = FromHex(
            "01020000000200000000000000000000000000000000000000000000000000f03f000000000000f03f");

        [Theory]
        [InlineData(
            "01e9030000000000000000f03f00000000000000400000000000000840",
            "POINT Z (1 2 3)")]
        [InlineData(
            "01d1070000000000000000f03f00000000000000400000000000001040",
            "POINT M (1 2 4)")]
        [InlineData(
            "01b90b0000000000000000f03f000000000000004000000000000008400000000000001040",
            "POINT ZM (1 2 3 4)")]
        public void Wkt_DimensionalMarkerUsesCanonicalSpacing(
            string wkb,
            string expected)
        {
            Assert.Equal(expected, GeospatialWkb.ToWkt(FromHex(wkb)));
        }

        [Theory]
        [InlineData(
            "SRID=4326;POINT Z (1 2 3)",
            0,
            4326,
            "01e9030000000000000000f03f00000000000000400000000000000840")]
        [InlineData(
            "POINT ZM (1 2 3 4)",
            3857,
            3857,
            "01b90b0000000000000000f03f000000000000004000000000000008400000000000001040")]
        public void Ewkt_EncodesStrictLittleEndianWkbWithoutEmbeddedSrid(
            string ewkt,
            int defaultSrid,
            int expectedSrid,
            string expectedWkb)
        {
            byte[] actual = GeospatialWkb.ToWkb(ewkt, defaultSrid, out int srid);

            Assert.Equal(expectedSrid, srid);
            Assert.Equal(FromHex(expectedWkb), actual);
        }

        [Fact]
        public async Task NativeMode_RestoresGeometryTagsAndPreservesValues()
        {
            StructArray physical = NativeGeoArray(
                new[] { 4326, 3857 },
                new[] { s_pointOneTwo, s_line },
                new[] { true, false });
            const string sqlType = "GEOMETRY(ANY)";
            IArrowType outputType = ArrowTypeParser.MapToArrowType(sqlType, true);

            using GeospatialTransformingStream stream = CreateStream(
                sqlType,
                outputType,
                physical,
                enableGeospatialSupport: true);

            StructType schemaType = Assert.IsType<StructType>(
                stream.Schema.GetFieldByIndex(0).DataType);
            Assert.Equal("true", schemaType.Fields[1].Metadata["geometry"]);
            Assert.Equal("-1", schemaType.Fields[1].Metadata["srid"]);

            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            StructArray values = Assert.IsType<StructArray>(batch.Column(0));
            StructType batchType = Assert.IsType<StructType>(values.Data.DataType);
            Assert.Equal("true", batchType.Fields[1].Metadata["geometry"]);
            Assert.Equal(4326, Assert.IsType<Int32Array>(values.Fields[0]).GetValue(0)!.Value);
            Assert.Equal(s_pointOneTwo, Assert.IsType<BinaryArray>(values.Fields[1]).GetBytes(0).ToArray());
            Assert.True(values.IsNull(1));
        }

        [Fact]
        public async Task StringMode_RendersEwktPlainWktAndOuterNull()
        {
            StructArray physical = NativeGeoArray(
                new[] { 4326, 0, 3857 },
                new[] { s_pointOneTwo, s_line, s_pointOneTwo },
                new[] { true, true, false });

            using GeospatialTransformingStream stream = CreateStream(
                "GEOMETRY(ANY)",
                StringType.Default,
                physical,
                enableGeospatialSupport: false);

            Assert.IsType<StringType>(stream.Schema.GetFieldByIndex(0).DataType);
            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            StringArray values = Assert.IsType<StringArray>(batch.Column(0));
            Assert.Equal("SRID=4326;POINT(1 2)", values.GetString(0));
            Assert.Equal("LINESTRING(0 0,1 1)", values.GetString(1));
            Assert.True(values.IsNull(2));
        }

        [Fact]
        public async Task StringMode_RespectsSlicedStructOffset()
        {
            StructArray physical = NativeGeoArray(
                new[] { 4326, 0, 3857 },
                new[] { s_pointOneTwo, s_line, s_pointOneTwo },
                new[] { true, true, true });
            using RecordBatch fullBatch = PhysicalBatch(physical);
            using RecordBatch slicedBatch = fullBatch.Slice(1, 2);
            Schema schema = SchemaFor("GEOMETRY(ANY)", StringType.Default);
            using IArrowArrayStream source = new StubArrowArrayStream(
                schema,
                new[] { slicedBatch });
            using var stream = new GeospatialTransformingStream(
                source,
                enableGeospatialSupport: false);

            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            StringArray values = Assert.IsType<StringArray>(batch.Column(0));
            Assert.Equal("LINESTRING(0 0,1 1)", values.GetString(0));
            Assert.Equal("SRID=3857;POINT(1 2)", values.GetString(1));
        }

        [Fact]
        public async Task Geography_UsesGeographyTag()
        {
            StructArray physical = NativeGeoArray(
                new[] { 4326 },
                new[] { s_pointOneTwo },
                new[] { true });
            const string sqlType = "GEOGRAPHY(4326)";

            using GeospatialTransformingStream stream = CreateStream(
                sqlType,
                ArrowTypeParser.MapToArrowType(sqlType, true),
                physical,
                enableGeospatialSupport: true);

            StructType type = Assert.IsType<StructType>(
                stream.Schema.GetFieldByIndex(0).DataType);
            Assert.Equal("true", type.Fields[1].Metadata["geography"]);
            Assert.Equal("4326", type.Fields[1].Metadata["srid"]);
            Assert.False(type.Fields[1].Metadata.ContainsKey("geometry"));
            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.IsType<StructArray>(batch.Column(0));
        }

        [Fact]
        public async Task LegacyText_IsPreservedInStringMode()
        {
            StringArray text = new StringArray.Builder()
                .Append("SRID=4326;POINT(1 2)")
                .Build();

            using GeospatialTransformingStream stream = CreateStream(
                "GEOMETRY(4326)",
                StringType.Default,
                text,
                enableGeospatialSupport: false);

            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.Equal(
                "SRID=4326;POINT(1 2)",
                Assert.IsType<StringArray>(batch.Column(0)).GetString(0));
        }

        [Fact]
        public async Task LegacyText_IsConvertedInNativeModeWhenSchemaReportsText()
        {
            StringArray text = new StringArray.Builder()
                .Append("SRID=4326;POINT(1 2)")
                .Append("LINESTRING(0 0,1 1)")
                .AppendNull()
                .Build();

            using GeospatialTransformingStream stream = CreateStream(
                "GEOMETRY(ANY)",
                StringType.Default,
                text,
                enableGeospatialSupport: true);

            StructType type = Assert.IsType<StructType>(
                stream.Schema.GetFieldByIndex(0).DataType);
            Assert.Equal("true", type.Fields[1].Metadata["geometry"]);
            Assert.Equal("-1", type.Fields[1].Metadata["srid"]);
            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            StructArray values = Assert.IsType<StructArray>(batch.Column(0));
            Int32Array srids = Assert.IsType<Int32Array>(values.Fields[0]);
            BinaryArray wkbs = Assert.IsType<BinaryArray>(values.Fields[1]);
            Assert.Equal(4326, srids.GetValue(0));
            Assert.Equal(s_pointOneTwo, wkbs.GetBytes(0).ToArray());
            Assert.Equal(0, srids.GetValue(1));
            Assert.Equal(s_line, wkbs.GetBytes(1).ToArray());
            Assert.True(values.IsNull(2));
        }

        [Fact]
        public async Task LegacyGeographyTextWithoutSrid_UsesGeographyDefault()
        {
            StringArray text = new StringArray.Builder()
                .Append("POINT(1 2)")
                .Build();

            using GeospatialTransformingStream stream = CreateStream(
                "GEOGRAPHY(ANY)",
                StringType.Default,
                text,
                enableGeospatialSupport: true);

            StructType type = Assert.IsType<StructType>(
                stream.Schema.GetFieldByIndex(0).DataType);
            Assert.Equal("true", type.Fields[1].Metadata["geography"]);
            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            StructArray values = Assert.IsType<StructArray>(batch.Column(0));
            Assert.Equal(4326, Assert.IsType<Int32Array>(values.Fields[0]).GetValue(0));
            Assert.Equal(
                s_pointOneTwo,
                Assert.IsType<BinaryArray>(values.Fields[1]).GetBytes(0).ToArray());
        }

        [Fact]
        public async Task LegacyLargeText_IsNormalizedToStringInStringMode()
        {
            LargeStringArray text = new LargeStringArray.Builder()
                .Append("SRID=4326;POINT(1 2)")
                .AppendNull()
                .Build();

            using GeospatialTransformingStream stream = CreateStream(
                "GEOGRAPHY(ANY)",
                StringType.Default,
                text,
                enableGeospatialSupport: false);

            Assert.IsType<StringType>(stream.Schema.GetFieldByIndex(0).DataType);
            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            StringArray values = Assert.IsType<StringArray>(batch.Column(0));
            Assert.Equal("SRID=4326;POINT(1 2)", values.GetString(0));
            Assert.True(values.IsNull(1));
        }

        [Fact]
        public async Task InvalidLegacyText_IsReportedAsInvalidDataInNativeMode()
        {
            StringArray text = new StringArray.Builder()
                .Append("not valid WKT")
                .Build();
            const string sqlType = "GEOMETRY(4326)";
            using GeospatialTransformingStream stream = CreateStream(
                sqlType,
                StringType.Default,
                text,
                enableGeospatialSupport: true);

            DatabricksException error = await Assert.ThrowsAsync<DatabricksException>(async () =>
                await stream.ReadNextRecordBatchAsync());
            Assert.Equal(AdbcStatusCode.InvalidData, error.Status);
            Assert.Contains("invalid WKT / EWKT", error.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public async Task InvalidWkb_IsReportedAsInvalidData()
        {
            StructArray physical = NativeGeoArray(
                new[] { 4326 },
                new[] { new byte[] { 1, 2, 3 } },
                new[] { true });
            using GeospatialTransformingStream stream = CreateStream(
                "GEOMETRY(4326)",
                StringType.Default,
                physical,
                enableGeospatialSupport: false);

            DatabricksException error = await Assert.ThrowsAsync<DatabricksException>(async () =>
                await stream.ReadNextRecordBatchAsync());
            Assert.Equal(AdbcStatusCode.InvalidData, error.Status);
            Assert.Contains("invalid WKB", error.Message);
        }

        private static GeospatialTransformingStream CreateStream(
            string sqlType,
            IArrowType outputType,
            IArrowArray physical,
            bool enableGeospatialSupport)
        {
            Schema schema = SchemaFor(sqlType, outputType);
            var source = new StubArrowArrayStream(
                schema,
                new[] { PhysicalBatch(physical) });
            return new GeospatialTransformingStream(
                source,
                enableGeospatialSupport);
        }

        private static Schema SchemaFor(string sqlType, IArrowType outputType) =>
            new Schema.Builder()
                .Field(new Field(
                    "g",
                    outputType,
                    nullable: true,
                    new Dictionary<string, string>
                    {
                        ["Spark:DataType:SqlName"] = sqlType,
                    }))
                .Build();

        private static RecordBatch PhysicalBatch(IArrowArray physical)
        {
            Schema physicalSchema = new Schema.Builder()
                .Field(new Field("g", physical.Data.DataType, nullable: true))
                .Build();
            return new RecordBatch(physicalSchema, new[] { physical }, physical.Length);
        }

        private static StructArray NativeGeoArray(
            IReadOnlyList<int> srids,
            IReadOnlyList<byte[]> wkbs,
            IReadOnlyList<bool> valid)
        {
            Assert.Equal(srids.Count, wkbs.Count);
            Assert.Equal(srids.Count, valid.Count);

            var sridBuilder = new Int32Array.Builder();
            var wkbBuilder = new BinaryArray.Builder();
            var validity = new ArrowBuffer.BitmapBuilder();
            int nullCount = 0;
            for (int i = 0; i < srids.Count; i++)
            {
                sridBuilder.Append(srids[i]);
                wkbBuilder.Append(wkbs[i].AsSpan());
                validity.Append(valid[i]);
                if (!valid[i]) nullCount++;
            }

            var physicalType = new StructType(new[]
            {
                new Field("srid", Int32Type.Default, nullable: false),
                new Field("wkb", BinaryType.Default, nullable: false),
            });
            return new StructArray(
                physicalType,
                srids.Count,
                new IArrowArray[] { sridBuilder.Build(), wkbBuilder.Build() },
                nullCount == 0 ? ArrowBuffer.Empty : validity.Build(),
                nullCount);
        }

        private static byte[] FromHex(string hex)
        {
            var bytes = new byte[hex.Length / 2];
            for (int i = 0; i < bytes.Length; i++)
            {
                bytes[i] = byte.Parse(
                    hex.Substring(i * 2, 2),
                    System.Globalization.NumberStyles.HexNumber,
                    System.Globalization.CultureInfo.InvariantCulture);
            }
            return bytes;
        }

        private sealed class StubArrowArrayStream : IArrowArrayStream
        {
            private readonly Queue<RecordBatch> _batches;

            internal StubArrowArrayStream(Schema schema, IEnumerable<RecordBatch> batches)
            {
                Schema = schema;
                _batches = new Queue<RecordBatch>(batches);
            }

            public Schema Schema { get; }

            public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(
                CancellationToken cancellationToken = default) =>
                new ValueTask<RecordBatch?>(
                    _batches.Count == 0 ? null : _batches.Dequeue());

            public void Dispose() { }
        }
    }
}
