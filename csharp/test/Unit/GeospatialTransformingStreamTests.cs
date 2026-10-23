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
using Apache.Arrow.Scalars;
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
        public void Wkb_EmbedsSridAsEwkb()
        {
            byte[] ewkb = GeospatialWkb.ToEwkb(s_pointOneTwo, 4326);
            Assert.Equal(
                FromHex("0101000020e6100000000000000000f03f0000000000000040"),
                ewkb);
            Assert.Equal("POINT(1 2)", GeospatialWkb.ToWkt(ewkb));
        }

        [Theory]
        [InlineData(
            "0101000000000000000000f03f0000000000000040",
            "0101000020e6100000000000000000f03f0000000000000040")]
        [InlineData(
            "01e9030000000000000000f03f00000000000000400000000000000840",
            "01010000a0e6100000000000000000f03f00000000000000400000000000000840")]
        [InlineData(
            "01d1070000000000000000f03f00000000000000400000000000001040",
            "0101000060e6100000000000000000f03f00000000000000400000000000001040")]
        [InlineData(
            "01b90b0000000000000000f03f000000000000004000000000000008400000000000001040",
            "01010000e0e6100000000000000000f03f000000000000004000000000000008400000000000001040")]
        public void Wkb_ConvertsIsoDimensionsToCanonicalEwkb(
            string input,
            string expected)
        {
            Assert.Equal(
                FromHex(expected),
                GeospatialWkb.ToEwkb(FromHex(input), 4326));
        }

        [Fact]
        public void Wkb_ConvertsBigEndianIsoDimensionsToCanonicalEwkb()
        {
            byte[] input = FromHex(
                "00000003e9000000000000000000000000000000000000000000000000");
            byte[] output = GeospatialWkb.ToEwkb(input, 3857);

            Assert.Equal(
                FromHex("00a000000100000f11000000000000000000000000000000000000000000000000"),
                output);
        }

        [Fact]
        public void Wkb_ConvertsNestedCollectionHeadersToCanonicalEwkb()
        {
            byte[] input = FromHex(
                "01ef0300000100000001e9030000000000000000f03f00000000000000400000000000000840");

            Assert.Equal(
                FromHex(
                    "01070000a0e6100000010000000101000080000000000000f03f00000000000000400000000000000840"),
                GeospatialWkb.ToEwkb(input, 4326));
        }

        [Theory]
        [InlineData("")]
        [InlineData("0201000000")]
        [InlineData("0101000000")]
        [InlineData("0101000000000000000000f03f000000000000004000")]
        [InlineData("0101000020e6100000000000000000f03f0000000000000040")]
        [InlineData("010400000001000000010200000000000000")]
        public void Wkb_EwkbConversionRejectsMalformedPayload(string input)
        {
            Assert.Throws<FormatException>(() =>
                GeospatialWkb.ToEwkb(FromHex(input), 4326));
        }

        [Theory]
        [InlineData("MULTIPOINT((1 2),(3 4))", "MULTIPOINT((1 2),(3 4))")]
        [InlineData(
            "POLYGON((0 0,1 0,1 1,0 0))",
            "POLYGON((0 0,1 0,1 1,0 0))")]
        [InlineData(
            "GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,1 1))",
            "GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,1 1))")]
        [InlineData("POINT EMPTY", "POINT EMPTY")]
        public void Wkt_RoundTripsAdditionalGeometryShapes(string input, string expected)
        {
            byte[] wkb = GeospatialWkb.ToWkb(input, 0, out int srid);

            Assert.Equal(0, srid);
            Assert.Equal(expected, GeospatialWkb.ToWkt(wkb));
        }

        [Fact]
        public async Task NativeMode_ReturnsGeoArrowAndEmbedsAnySridInEwkb()
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
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: true);

            Field field = stream.Schema.GetFieldByIndex(0);
            Assert.IsType<BinaryType>(field.DataType);
            Assert.Equal("geoarrow.wkb", field.Metadata["ARROW:extension:name"]);
            Assert.False(field.Metadata.ContainsKey("ARROW:extension:metadata"));

            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            BinaryArray values = Assert.IsType<BinaryArray>(batch.Column(0));
            Assert.Equal(
                GeospatialWkb.ToEwkb(s_pointOneTwo, 4326),
                values.GetBytes(0).ToArray());
            Assert.True(values.IsNull(1));
        }

        [Fact]
        public async Task FixedSridNativeMode_ReusesWkbBuffersAndOuterValidity()
        {
            StructArray physical = NativeGeoArray(
                new[] { 4326, 4326 },
                new[] { s_pointOneTwo, s_line },
                new[] { true, false });
            BinaryArray physicalWkb = Assert.IsType<BinaryArray>(physical.Fields[1]);

            using GeospatialTransformingStream stream = CreateStream(
                "GEOMETRY(4326)",
                StringType.Default,
                physical,
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: true);
            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();

            Assert.NotNull(batch);
            BinaryArray output = Assert.IsType<BinaryArray>(batch.Column(0));
            Assert.Equal(s_pointOneTwo, output.GetBytes(0).ToArray());
            Assert.True(output.IsNull(1));
            Assert.True(output.ValueOffsetsBuffer.Memory.Equals(physicalWkb.ValueOffsetsBuffer.Memory));
            Assert.True(output.ValueBuffer.Memory.Equals(physicalWkb.ValueBuffer.Memory));
            Assert.True(output.NullBitmapBuffer.Memory.Equals(physical.NullBitmapBuffer.Memory));
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task AnySrid_EmbedsEachRowSridInEwkb(bool nativeInput)
        {
            int[] srids = { 4326, 3857, 0 };
            IArrowArray physical = nativeInput
                ? NativeGeoArray(
                    srids,
                    new[] { s_pointOneTwo, s_pointOneTwo, s_pointOneTwo },
                    new[] { true, true, true })
                : new StringArray.Builder()
                    .Append("SRID=4326;POINT(1 2)")
                    .Append("SRID=3857;POINT(1 2)")
                    .Append("POINT(1 2)")
                    .Build();

            using GeospatialTransformingStream stream = CreateStream(
                "GEOMETRY(ANY)",
                physical.Data.DataType,
                physical,
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: true);
            Field field = stream.Schema.GetFieldByIndex(0);
            Assert.Equal("geoarrow.wkb", field.Metadata["ARROW:extension:name"]);
            Assert.False(field.Metadata.ContainsKey("ARROW:extension:metadata"));

            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            BinaryArray values = Assert.IsType<BinaryArray>(batch.Column(0));
            string[] expected =
            {
                "0101000020e6100000000000000000f03f0000000000000040",
                "0101000020110f0000000000000000f03f0000000000000040",
                "010100002000000000000000000000f03f0000000000000040",
            };
            for (int row = 0; row < srids.Length; row++)
            {
                Assert.Equal(FromHex(expected[row]), values.GetBytes(row).ToArray());
            }
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
                enableGeospatialSupport: false,
                enableComplexDatatypeSupport: true);

            Assert.IsType<StringType>(stream.Schema.GetFieldByIndex(0).DataType);
            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            StringArray values = Assert.IsType<StringArray>(batch.Column(0));
            Assert.Equal("SRID=4326;POINT(1 2)", values.GetString(0));
            Assert.Equal("LINESTRING(0 0,1 1)", values.GetString(1));
            Assert.True(values.IsNull(2));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(4326)]
        public async Task StringMode_RetainsEmptyCollectionDimensionsAndSrid(int srid)
        {
            const string wkt = "GEOMETRYCOLLECTION ZM (POINT ZM EMPTY,MULTIPOINT ZM (EMPTY,EMPTY))";
            byte[] wkb = GeospatialWkb.ToWkb(wkt, 0, out _);
            StructArray physical = NativeGeoArray(
                new[] { srid }, new[] { wkb }, new[] { true });

            using GeospatialTransformingStream stream = CreateStream(
                "GEOMETRY(ANY)", StringType.Default, physical,
                enableGeospatialSupport: false, enableComplexDatatypeSupport: true);
            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            string expected = srid == 0 ? wkt : $"SRID={srid};{wkt}";
            Assert.Equal(expected, Assert.IsType<StringArray>(batch.Column(0)).GetString(0));
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task EmptyResult_StillExposesConfiguredGeospatialSchema(bool enabled)
        {
            const string sqlType = "GEOMETRY(4326)";
            Schema schema = SchemaFor(
                sqlType,
                ArrowTypeParser.MapToArrowType(sqlType, true));
            using IArrowArrayStream source = new StubArrowArrayStream(
                schema,
                System.Array.Empty<RecordBatch>());
            using var stream = new GeospatialTransformingStream(
                source,
                enableGeospatialSupport: enabled,
                enableComplexDatatypeSupport: true);

            Field output = stream.Schema.GetFieldByIndex(0);
            if (enabled)
            {
                Assert.IsType<BinaryType>(output.DataType);
                Assert.Equal("geoarrow.wkb", output.Metadata["ARROW:extension:name"]);
            }
            else
            {
                Assert.IsType<StringType>(output.DataType);
            }
            Assert.Null(await stream.ReadNextRecordBatchAsync());
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
                enableGeospatialSupport: false,
                enableComplexDatatypeSupport: true);

            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            StringArray values = Assert.IsType<StringArray>(batch.Column(0));
            Assert.Equal("LINESTRING(0 0,1 1)", values.GetString(0));
            Assert.Equal("SRID=3857;POINT(1 2)", values.GetString(1));
        }

        [Fact]
        public async Task FixedSridNativeMode_RespectsSlicedStructOffset()
        {
            StructArray physical = NativeGeoArray(
                new[] { 4326, 4326, 4326 },
                new[] { s_line, s_pointOneTwo, s_line },
                new[] { true, true, true });
            using RecordBatch fullBatch = PhysicalBatch(physical);
            using RecordBatch slicedBatch = fullBatch.Slice(1, 2);
            Schema schema = SchemaFor("GEOMETRY(4326)", StringType.Default);
            using IArrowArrayStream source = new StubArrowArrayStream(schema, new[] { slicedBatch });
            using var stream = new GeospatialTransformingStream(
                source,
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: true);

            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            BinaryArray values = Assert.IsType<BinaryArray>(batch.Column(0));
            Assert.Equal(s_pointOneTwo, values.GetBytes(0).ToArray());
            Assert.Equal(s_line, values.GetBytes(1).ToArray());
        }

        [Fact]
        public async Task Geography_UsesGeoArrowCrsAndSphericalEdges()
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
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: true);

            Field field = stream.Schema.GetFieldByIndex(0);
            Assert.IsType<BinaryType>(field.DataType);
            Assert.Equal("geoarrow.wkb", field.Metadata["ARROW:extension:name"]);
            Assert.Equal(
                "{\"crs\":\"4326\",\"crs_type\":\"srid\",\"edges\":\"spherical\"}",
                field.Metadata["ARROW:extension:metadata"]);
            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.Equal(
                s_pointOneTwo,
                Assert.IsType<BinaryArray>(batch.Column(0)).GetBytes(0).ToArray());
        }

        [Theory]
        [InlineData(
            "GEOMETRY(3857)",
            3857,
            "{\"crs\":\"3857\",\"crs_type\":\"srid\"}")]
        [InlineData(
            "GEOMETRY(103192)",
            103192,
            "{\"crs\":\"103192\",\"crs_type\":\"srid\"}")]
        [InlineData("GEOMETRY", 0, null)]
        public async Task Geometry_UsesGeoArrowColumnCrsWhenKnown(
            string sqlType,
            int srid,
            string? expectedMetadata)
        {
            StructArray physical = NativeGeoArray(
                new[] { srid },
                new[] { s_pointOneTwo },
                new[] { true });

            using GeospatialTransformingStream stream = CreateStream(
                sqlType,
                ArrowTypeParser.MapToArrowType(sqlType, true),
                physical,
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: true);

            Field field = stream.Schema.GetFieldByIndex(0);
            Assert.IsType<BinaryType>(field.DataType);
            Assert.Equal("geoarrow.wkb", field.Metadata["ARROW:extension:name"]);
            if (expectedMetadata == null)
            {
                Assert.False(field.Metadata.ContainsKey("ARROW:extension:metadata"));
            }
            else
            {
                Assert.Equal(
                    expectedMetadata,
                    field.Metadata["ARROW:extension:metadata"]);
            }

            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.Equal(
                s_pointOneTwo,
                Assert.IsType<BinaryArray>(batch.Column(0)).GetBytes(0).ToArray());
        }

        [Fact]
        public async Task FixedSrid_RejectsMismatchedRowSrid()
        {
            StructArray physical = NativeGeoArray(
                new[] { 3857 },
                new[] { s_pointOneTwo },
                new[] { true });
            using GeospatialTransformingStream stream = CreateStream(
                "GEOMETRY(4326)",
                StringType.Default,
                physical,
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: true);

            DatabricksException error = await Assert.ThrowsAsync<DatabricksException>(async () =>
                await stream.ReadNextRecordBatchAsync());
            Assert.Equal(AdbcStatusCode.InvalidData, error.Status);
            Assert.Contains("requires SRID 4326", error.Message);
        }

        [Fact]
        public async Task FixedSrid_StringModeRejectsMismatchedRowSrid()
        {
            StructArray physical = NativeGeoArray(
                new[] { 3857 },
                new[] { s_pointOneTwo },
                new[] { true });
            using GeospatialTransformingStream stream = CreateStream(
                "GEOMETRY(4326)",
                StringType.Default,
                physical,
                enableGeospatialSupport: false,
                enableComplexDatatypeSupport: true);

            DatabricksException error = await Assert.ThrowsAsync<DatabricksException>(async () =>
                await stream.ReadNextRecordBatchAsync());
            Assert.Equal(AdbcStatusCode.InvalidData, error.Status);
            Assert.Contains("requires SRID 4326", error.Message);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task AnySrid_RejectsNegativeRowSrid(bool nativeInput)
        {
            IArrowArray physical = nativeInput
                ? NativeGeoArray(
                    new[] { -2 },
                    new[] { s_pointOneTwo },
                    new[] { true })
                : new StringArray.Builder()
                    .Append("SRID=-2;POINT(1 2)")
                    .Build();
            using GeospatialTransformingStream stream = CreateStream(
                "GEOMETRY(ANY)",
                nativeInput ? physical.Data.DataType : StringType.Default,
                physical,
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: true);

            DatabricksException error = await Assert.ThrowsAsync<DatabricksException>(async () =>
                await stream.ReadNextRecordBatchAsync());
            Assert.Equal(AdbcStatusCode.InvalidData, error.Status);
            Assert.Contains("negative SRID", error.Message);
        }

        [Fact]
        public void NonGeospatialLogicalType_RejectsContradictoryPhysicalTag()
        {
            StructArray physical = TaggedGeoArray(
                "GEOMETRY(4326)",
                4326,
                s_pointOneTwo);
            Schema schema = SchemaFor("STRING", physical.Data.DataType);
            using IArrowArrayStream source = new StubArrowArrayStream(
                schema,
                System.Array.Empty<RecordBatch>());

            DatabricksException error = Assert.Throws<DatabricksException>(() =>
            {
                using var stream = new GeospatialTransformingStream(
                    source,
                    enableGeospatialSupport: true,
                    enableComplexDatatypeSupport: true);
            });

            Assert.Equal(AdbcStatusCode.InvalidData, error.Status);
            Assert.Contains("Spark:DataType:SqlName", error.Message);
            Assert.Contains("non-geospatial", error.Message);
        }

        [Fact]
        public async Task NestedPhysicalTag_DoesNotOverrideLogicalMetadataInLaterBatch()
        {
            const string sqlType = "STRUCT<label:STRING,geom:GEOMETRY(ANY)>";
            StructArray first = TwoFieldStruct(
                "label",
                new StringArray.Builder().Append("home").Build(),
                "geom",
                NativeGeoArray(
                    new[] { 4326 },
                    new[] { s_pointOneTwo },
                    new[] { true }));
            StructArray second = TwoFieldStruct(
                "label",
                TaggedGeoArray("GEOMETRY(4326)", 4326, s_pointOneTwo),
                "geom",
                NativeGeoArray(
                    new[] { 4326 },
                    new[] { s_pointOneTwo },
                    new[] { true }));
            Schema schema = SchemaFor(
                sqlType,
                ArrowTypeParser.MapToArrowType(sqlType, true));
            using IArrowArrayStream source = new StubArrowArrayStream(
                schema,
                new[] { PhysicalBatch(first), PhysicalBatch(second) });
            using GeospatialTransformingStream stream = await GeospatialTransformingStream.CreateAsync(
                source,
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: true);

            using RecordBatch? firstResult = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(firstResult);
            DatabricksException error = await Assert.ThrowsAsync<DatabricksException>(async () =>
                await stream.ReadNextRecordBatchAsync());

            Assert.Equal(AdbcStatusCode.InvalidData, error.Status);
            Assert.Contains("g.label", error.Message);
            Assert.Contains("non-geospatial", error.Message);
        }

        [Fact]
        public async Task NestedTaggedPhysicalType_IsDetectionFallbackWithoutLogicalMetadata()
        {
            StructArray physical = OuterStruct(
                "geom",
                TaggedGeoArray("GEOGRAPHY(4326)", 4326, s_pointOneTwo));
            Schema schema = new Schema.Builder()
                .Field(new Field("g", physical.Data.DataType, nullable: true))
                .Build();
            using IArrowArrayStream source = new StubArrowArrayStream(
                schema,
                new[] { PhysicalBatch(physical) });
            using GeospatialTransformingStream stream = await GeospatialTransformingStream.CreateAsync(
                source,
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: true);

            StructType outputType = Assert.IsType<StructType>(
                stream.Schema.GetFieldByIndex(0).DataType);
            Assert.Equal(
                "geoarrow.wkb",
                outputType.Fields[0].Metadata["ARROW:extension:name"]);
            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            BinaryArray value = Assert.IsType<BinaryArray>(
                Assert.IsType<StructArray>(batch.Column(0)).Fields[0]);
            Assert.Equal(s_pointOneTwo, value.GetBytes(0).ToArray());
        }

        [Fact]
        public async Task NestedStruct_UsesNativeGeoWhenBothFeaturesEnabled()
        {
            StructArray geo = NativeGeoArray(
                new[] { 3857 },
                new[] { s_pointOneTwo },
                new[] { true });
            StructArray physical = OuterStruct("geom", geo);
            const string sqlType = "STRUCT<geom:GEOMETRY(ANY)>";

            using GeospatialTransformingStream stream = CreateStream(
                sqlType,
                ArrowTypeParser.MapToArrowType(sqlType, true),
                physical,
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: true);

            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            StructArray outer = Assert.IsType<StructArray>(batch.Column(0));
            BinaryArray nested = Assert.IsType<BinaryArray>(outer.Fields[0]);
            StructType outerType = Assert.IsType<StructType>(outer.Data.DataType);
            Assert.Equal(
                "geoarrow.wkb",
                outerType.Fields[0].Metadata["ARROW:extension:name"]);
            Assert.Equal(
                GeospatialWkb.ToEwkb(s_pointOneTwo, 3857),
                nested.GetBytes(0).ToArray());
        }

        [Fact]
        public async Task NestedStruct_ConvertsVoidAndIntervalSiblingsToDeclaredStrings()
        {
            StructArray geo = NativeGeoArray(
                new[] { 3857 },
                new[] { s_pointOneTwo },
                new[] { true });
            var months = new YearMonthIntervalArray.Builder();
            months.Append(new YearMonthInterval(30));
            YearMonthIntervalArray tenure = months.Build();
            var missing = new NullArray(1);
            var physicalType = new StructType(new[]
            {
                new Field("geom", geo.Data.DataType, nullable: true),
                new Field("missing", missing.Data.DataType, nullable: true),
                new Field("tenure", tenure.Data.DataType, nullable: true),
            });
            var physical = new StructArray(
                physicalType,
                1,
                new IArrowArray[] { geo, missing, tenure },
                ArrowBuffer.Empty);
            const string sqlType =
                "STRUCT<geom:GEOMETRY(ANY),missing:VOID,tenure:INTERVAL YEAR TO MONTH>";

            using GeospatialTransformingStream stream = CreateStream(
                sqlType,
                ArrowTypeParser.MapToArrowType(sqlType, true),
                physical,
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: true);

            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            StructArray outer = Assert.IsType<StructArray>(batch.Column(0));
            Assert.True(Assert.IsType<StringArray>(outer.Fields[1]).IsNull(0));
            Assert.Equal("2-6", Assert.IsType<StringArray>(outer.Fields[2]).GetString(0));
            Assert.True(stream.Schema.GetFieldByIndex(0).DataType.Equals(outer.Data.DataType));
        }

        [Fact]
        public async Task NestedStruct_RejectsUnrelatedSiblingTypeMismatch()
        {
            StructArray geo = NativeGeoArray(
                new[] { 3857 },
                new[] { s_pointOneTwo },
                new[] { true });
            StringArray wrongValue = new StringArray.Builder().Append("not an int").Build();
            var physicalType = new StructType(new[]
            {
                new Field("geom", geo.Data.DataType, nullable: true),
                new Field("value", StringType.Default, nullable: true),
            });
            var physical = new StructArray(
                physicalType,
                1,
                new IArrowArray[] { geo, wrongValue },
                ArrowBuffer.Empty);
            const string sqlType = "STRUCT<geom:GEOMETRY(ANY),value:INT>";

            using GeospatialTransformingStream stream = CreateStream(
                sqlType,
                ArrowTypeParser.MapToArrowType(sqlType, true),
                physical,
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: true);

            DatabricksException error = await Assert.ThrowsAsync<DatabricksException>(async () =>
                await stream.ReadNextRecordBatchAsync());
            Assert.Equal(AdbcStatusCode.InvalidData, error.Status);
            Assert.Contains("g.value", error.Message);
        }

        [Theory]
        [InlineData(true, true, TimeUnit.Microsecond, "UTC")]
        [InlineData(false, true, TimeUnit.Microsecond, "UTC")]
        [InlineData(true, false, TimeUnit.Microsecond, "UTC")]
        [InlineData(false, false, TimeUnit.Microsecond, "UTC")]
        [InlineData(true, true, TimeUnit.Nanosecond, "Asia/Kathmandu")]
        [InlineData(false, true, TimeUnit.Millisecond, null)]
        public async Task NestedStruct_PreservesParameterizedNonGeoSiblings(
            bool native, bool complex, TimeUnit unit, string? timezone)
        {
            StructArray geo = NativeGeoArray(
                new[] { 3857 },
                new[] { s_pointOneTwo },
                new[] { true });
            var decimalType = new Decimal128Type(10, 2);
            var amountBuilder = new Decimal128Array.Builder(decimalType);
            amountBuilder.Append("12.34");
            Decimal128Array amount = amountBuilder.Build();
            var timestampType = new TimestampType(unit, timezone);
            var timestampBuilder = new TimestampArray.Builder(timestampType);
            timestampBuilder.Append(
                new DateTimeOffset(2026, 10, 13, 12, 34, 56, TimeSpan.Zero).AddTicks(1234560));
            TimestampArray timestamp = timestampBuilder.Build();
            var physicalType = new StructType(new[]
            {
                new Field("geom", geo.Data.DataType, nullable: true),
                new Field("amount", amount.Data.DataType, nullable: true),
                new Field("ts", timestamp.Data.DataType, nullable: true),
            });
            var physical = new StructArray(
                physicalType,
                1,
                new IArrowArray[] { geo, amount, timestamp },
                ArrowBuffer.Empty);
            string sqlType = "STRUCT<geom:GEOMETRY(ANY),amount:DECIMAL(10,2),ts:"
                + (timezone == null ? "TIMESTAMP_NTZ>" : "TIMESTAMP>");

            using GeospatialTransformingStream stream = CreateStream(
                sqlType,
                ArrowTypeParser.MapToArrowType(sqlType, complex),
                physical,
                enableGeospatialSupport: native,
                enableComplexDatatypeSupport: complex);

            StructType advertised = Assert.IsType<StructType>(stream.Schema.GetFieldByIndex(0).DataType);
            Assert.Same(timestampType, advertised.Fields[2].DataType);

            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            StructArray output = Assert.IsType<StructArray>(batch.Column(0));
            Assert.Equal("12.34", Assert.IsType<Decimal128Array>(output.Fields[1]).GetString(0));
            TimestampArray outputTimestamp = Assert.IsType<TimestampArray>(output.Fields[2]);
            Assert.Same(timestampType, outputTimestamp.Data.DataType);
            Assert.Equal(timestamp.Data.Buffers[1], outputTimestamp.Data.Buffers[1]);
            Assert.Equal(timestamp.GetTimestamp(0), outputTimestamp.GetTimestamp(0));
            Assert.Null(await stream.ReadNextRecordBatchAsync());
        }

        [Fact]
        public async Task NestedStruct_RejectsParameterizedSiblingMismatch()
        {
            StructArray geo = NativeGeoArray(
                new[] { 3857 },
                new[] { s_pointOneTwo },
                new[] { true });
            var amountBuilder = new Decimal128Array.Builder(new Decimal128Type(10, 3));
            amountBuilder.Append("12.340");
            Decimal128Array amount = amountBuilder.Build();
            var physicalType = new StructType(new[]
            {
                new Field("geom", geo.Data.DataType, nullable: true),
                new Field("amount", amount.Data.DataType, nullable: true),
            });
            var physical = new StructArray(
                physicalType,
                1,
                new IArrowArray[] { geo, amount },
                ArrowBuffer.Empty);
            const string sqlType = "STRUCT<geom:GEOMETRY(ANY),amount:DECIMAL(10,2)>";

            using GeospatialTransformingStream stream = CreateStream(
                sqlType,
                StringType.Default,
                physical,
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: false);

            DatabricksException error = await Assert.ThrowsAsync<DatabricksException>(async () =>
                await stream.ReadNextRecordBatchAsync());
            Assert.Equal(AdbcStatusCode.InvalidData, error.Status);
            Assert.Contains("g.amount", error.Message);
        }

        [Fact]
        public async Task NullOuterStruct_MasksInvalidTextGeoPlaceholder()
        {
            StringArray invalidGeo = new StringArray.Builder()
                .Append("not valid WKT")
                .Build();
            StructArray physical = OuterStruct("geom", invalidGeo, outerValid: false);
            const string sqlType = "STRUCT<geom:GEOMETRY(ANY)>";

            using GeospatialTransformingStream stream = CreateStream(
                sqlType,
                ArrowTypeParser.MapToArrowType(sqlType, true),
                physical,
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: true);

            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            StructArray output = Assert.IsType<StructArray>(batch.Column(0));
            Assert.True(output.IsNull(0));
            Assert.True(Assert.IsType<BinaryArray>(output.Fields[0]).IsNull(0));
        }

        [Theory]
        [InlineData("GEOMETRY(ANY)", false)]
        [InlineData("GEOMETRY(ANY)", true)]
        [InlineData("GEOMETRY(4326)", false)]
        [InlineData("GEOMETRY(4326)", true)]
        public async Task NullOuterStruct_MasksInvalidNativeGeoPlaceholder(
            string geoType, bool enableGeospatialSupport)
        {
            StructArray invalidGeo = NativeGeoArray(
                new[] { 4326 },
                new[] { new byte[] { 1, 1 } },
                new[] { true });
            StructArray physical = OuterStruct("geom", invalidGeo, outerValid: false);
            string sqlType = $"STRUCT<geom:{geoType}>";

            using GeospatialTransformingStream stream = CreateStream(
                sqlType,
                ArrowTypeParser.MapToArrowType(sqlType, true),
                physical,
                enableGeospatialSupport,
                enableComplexDatatypeSupport: true);

            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            StructArray output = Assert.IsType<StructArray>(batch.Column(0));
            Assert.True(output.IsNull(0));
            if (enableGeospatialSupport)
                Assert.True(Assert.IsType<BinaryArray>(output.Fields[0]).IsNull(0));
            else
                Assert.True(Assert.IsType<StringArray>(output.Fields[0]).IsNull(0));
        }

        [Fact]
        public async Task NestedStruct_RespectsSlicedParentOffset()
        {
            StructArray geo = NativeGeoArray(
                new[] { 4326, 3857, 0 },
                new[] { s_pointOneTwo, s_pointOneTwo, s_pointOneTwo },
                new[] { true, true, true });
            StructArray physical = OuterStruct("geom", geo);
            const string sqlType = "STRUCT<geom:GEOMETRY(ANY)>";
            using RecordBatch fullBatch = PhysicalBatch(physical);
            using RecordBatch slicedBatch = fullBatch.Slice(1, 1);
            Schema schema = SchemaFor(
                sqlType,
                ArrowTypeParser.MapToArrowType(sqlType, true));
            using IArrowArrayStream source = new StubArrowArrayStream(
                schema,
                new[] { slicedBatch });
            using var stream = new GeospatialTransformingStream(
                source,
                enableGeospatialSupport: false,
                enableComplexDatatypeSupport: true);

            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            StructArray output = Assert.IsType<StructArray>(batch.Column(0));
            Assert.Equal(1, output.Length);
            Assert.Equal(
                "SRID=3857;POINT(1 2)",
                Assert.IsType<StringArray>(output.Fields[0]).GetString(0));
        }

        [Theory]
        [InlineData("GEOMETRY(ANY)", "SRID=4326;POINT(1 2)", 4326)]
        [InlineData("GEOMETRY(ANY)", "POINT(1 2)", 0)]
        [InlineData("GEOGRAPHY(ANY)", "POINT(1 2)", 4326)]
        public async Task NestedStruct_ConvertsReportedLegacyGeoTextInNativeMode(
            string geoType, string wkt, int expectedSrid)
        {
            StringArray geo = new StringArray.Builder()
                .Append(wkt)
                .Build();
            Int32Array id = new Int32Array.Builder().Append(7).Build();
            var physicalType = new StructType(new[]
            {
                new Field("geom", StringType.Default, nullable: true),
                new Field("id", Int32Type.Default, nullable: true),
            });
            var physical = new StructArray(
                physicalType,
                1,
                new IArrowArray[] { geo, id },
                ArrowBuffer.Empty);
            string sqlType = $"STRUCT<geom:{geoType},id:INT>";

            using GeospatialTransformingStream stream = CreateStream(
                sqlType,
                physicalType,
                physical,
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: true);

            StructType schemaType = Assert.IsType<StructType>(
                stream.Schema.GetFieldByIndex(0).DataType);
            Assert.IsType<BinaryType>(schemaType.Fields[0].DataType);
            Assert.Equal(
                "geoarrow.wkb",
                schemaType.Fields[0].Metadata["ARROW:extension:name"]);
            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            StructArray result = Assert.IsType<StructArray>(batch.Column(0));
            BinaryArray native = Assert.IsType<BinaryArray>(result.Fields[0]);
            Assert.Equal(
                GeospatialWkb.ToEwkb(s_pointOneTwo, expectedSrid),
                native.GetBytes(0).ToArray());
            Assert.Equal(7, Assert.IsType<Int32Array>(result.Fields[1]).GetValue(0));
        }

        [Theory]
        [InlineData("POINT(1 2)")]
        [InlineData("MULTIPOINT M (EMPTY,EMPTY)")]
        [InlineData("GEOMETRYCOLLECTION ZM (POINT ZM EMPTY,MULTIPOINT ZM (EMPTY,EMPTY))")]
        public async Task NestedStruct_BecomesEwktInsideOuterJsonWhenComplexSupportDisabled(string wkt)
        {
            byte[] wkb = GeospatialWkb.ToWkb(wkt, 0, out _);
            StructArray geo = NativeGeoArray(
                new[] { 3857 },
                new[] { wkb },
                new[] { true });
            StructArray physical = OuterStruct("geom", geo);
            const string sqlType = "STRUCT<geom:GEOMETRY(ANY)>";
            Schema schema = SchemaFor(sqlType, StringType.Default);
            using IArrowArrayStream source = new StubArrowArrayStream(
                schema,
                new[] { PhysicalBatch(physical) });
            using var geospatial = new GeospatialTransformingStream(
                source,
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: false);
            Assert.IsType<StructType>(geospatial.Schema.GetFieldByIndex(0).DataType);
            using var stream = new ComplexTypeSerializingStream(geospatial);

            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            StringArray values = Assert.IsType<StringArray>(batch.Column(0));
            Assert.Equal("{\"geom\":\"SRID=3857;" + wkt + "\"}", values.GetString(0));
        }

        [Theory]
        [InlineData(true, "POINT(1 2)")]
        [InlineData(false, "POINT(1 2)")]
        [InlineData(true, "GEOMETRYCOLLECTION M (POINT M EMPTY,MULTILINESTRING M (EMPTY,EMPTY))")]
        [InlineData(false, "GEOMETRYCOLLECTION M (POINT M EMPTY,MULTILINESTRING M (EMPTY,EMPTY))")]
        public async Task NestedArray_UsesConfiguredGeoRepresentation(bool enabled, string wkt)
        {
            byte[] wkb = GeospatialWkb.ToWkb(wkt, 0, out _);
            StructArray geo = NativeGeoArray(
                new[] { 4326 },
                new[] { wkb },
                new[] { true });
            StructType physicalGeoType = (StructType)geo.Data.DataType;
            var offsets = new ArrowBuffer.Builder<int>();
            offsets.Append(0);
            offsets.Append(1);
            var list = new ListArray(
                new ListType(new Field("item", physicalGeoType, nullable: true)),
                length: 1,
                offsets.Build(),
                geo,
                ArrowBuffer.Empty);
            const string sqlType = "ARRAY<GEOMETRY(ANY)>";

            using GeospatialTransformingStream stream = CreateStream(
                sqlType,
                ArrowTypeParser.MapToArrowType(sqlType, true),
                list,
                enableGeospatialSupport: enabled,
                enableComplexDatatypeSupport: true);

            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            ListArray output = Assert.IsType<ListArray>(batch.Column(0));
            if (enabled)
            {
                BinaryArray item = Assert.IsType<BinaryArray>(output.Values);
                ListType outputType = Assert.IsType<ListType>(output.Data.DataType);
                Assert.Equal(
                    "geoarrow.wkb",
                    outputType.ValueField.Metadata["ARROW:extension:name"]);
                Assert.Equal(
                    GeospatialWkb.ToEwkb(wkb, 4326),
                    item.GetBytes(0).ToArray());
            }
            else
            {
                Assert.Equal(
                    "SRID=4326;" + wkt,
                    Assert.IsType<StringArray>(output.Values).GetString(0));
            }
        }

        [Fact]
        public async Task NullNestedArray_MasksInvalidGeoPlaceholder()
        {
            StringArray invalidGeo = new StringArray.Builder()
                .Append("not valid WKT")
                .Build();
            var offsets = new ArrowBuffer.Builder<int>();
            offsets.Append(0);
            offsets.Append(1);
            var validity = new ArrowBuffer.BitmapBuilder();
            validity.Append(false);
            var list = new ListArray(
                new ListType(new Field("item", StringType.Default, nullable: true)),
                length: 1,
                offsets.Build(),
                invalidGeo,
                validity.Build(),
                nullCount: 1);
            const string sqlType = "ARRAY<GEOMETRY(ANY)>";

            using GeospatialTransformingStream stream = CreateStream(
                sqlType,
                ArrowTypeParser.MapToArrowType(sqlType, true),
                list,
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: true);

            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            ListArray output = Assert.IsType<ListArray>(batch.Column(0));
            Assert.True(output.IsNull(0));
            Assert.True(Assert.IsType<BinaryArray>(output.Values).IsNull(0));
        }

        [Fact]
        public async Task SlicedNestedArray_DoesNotReadValuesOutsideSlice()
        {
            StringArray text = new StringArray.Builder()
                .Append("not valid WKT")
                .Append("SRID=4326;POINT(1 2)")
                .Append("also not valid WKT")
                .Build();
            var offsets = new ArrowBuffer.Builder<int>();
            offsets.Append(0);
            offsets.Append(1);
            offsets.Append(2);
            offsets.Append(3);
            var list = new ListArray(
                new ListType(new Field("item", StringType.Default, nullable: true)),
                length: 3,
                offsets.Build(),
                text,
                ArrowBuffer.Empty);
            const string sqlType = "ARRAY<GEOMETRY(ANY)>";
            Schema schema = SchemaFor(
                sqlType,
                ArrowTypeParser.MapToArrowType(sqlType, true));
            using RecordBatch fullBatch = PhysicalBatch(list);
            using RecordBatch slicedBatch = fullBatch.Slice(1, 1);
            using IArrowArrayStream source = new StubArrowArrayStream(
                schema,
                new[] { slicedBatch });
            using var stream = new GeospatialTransformingStream(
                source,
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: true);

            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            ListArray output = Assert.IsType<ListArray>(batch.Column(0));
            BinaryArray values = Assert.IsType<BinaryArray>(output.Values);
            Assert.True(values.IsNull(0));
            Assert.Equal(
                GeospatialWkb.ToEwkb(s_pointOneTwo, 4326),
                values.GetBytes(1).ToArray());
            Assert.True(values.IsNull(2));
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task NestedMapValue_UsesConfiguredGeoRepresentation(bool enabled)
        {
            StructArray geo = NativeGeoArray(
                new[] { 4326 },
                new[] { s_pointOneTwo },
                new[] { true });
            StringArray keys = new StringArray.Builder().Append("home").Build();
            var entriesType = new StructType(new[]
            {
                new Field("key", StringType.Default, nullable: false),
                new Field("value", geo.Data.DataType, nullable: true),
            });
            var entries = new StructArray(
                entriesType,
                1,
                new IArrowArray[] { keys, geo },
                ArrowBuffer.Empty);
            var offsets = new ArrowBuffer.Builder<int>();
            offsets.Append(0);
            offsets.Append(1);
            var map = new MapArray(
                new MapType(StringType.Default, geo.Data.DataType),
                1,
                offsets.Build(),
                entries,
                ArrowBuffer.Empty);
            const string sqlType = "MAP<STRING,GEOGRAPHY(ANY)>";

            using GeospatialTransformingStream stream = CreateStream(
                sqlType,
                ArrowTypeParser.MapToArrowType(sqlType, true),
                map,
                enableGeospatialSupport: enabled,
                enableComplexDatatypeSupport: true);

            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            MapArray output = Assert.IsType<MapArray>(batch.Column(0));
            if (enabled)
            {
                MapType outputType = Assert.IsType<MapType>(output.Data.DataType);
                Assert.Equal(
                    "geoarrow.wkb",
                    outputType.ValueField.Metadata["ARROW:extension:name"]);
                Assert.Equal(
                    "{\"edges\":\"spherical\"}",
                    outputType.ValueField.Metadata["ARROW:extension:metadata"]);
                Assert.Equal(
                    GeospatialWkb.ToEwkb(s_pointOneTwo, 4326),
                    Assert.IsType<BinaryArray>(output.Values).GetBytes(0).ToArray());
            }
            else
            {
                Assert.Equal(
                    "SRID=4326;POINT(1 2)",
                    Assert.IsType<StringArray>(output.Values).GetString(0));
            }
        }

        [Fact]
        public async Task NullNestedMap_MasksInvalidGeoPlaceholder()
        {
            StringArray keys = new StringArray.Builder().Append("home").Build();
            StringArray invalidGeo = new StringArray.Builder()
                .Append("not valid WKT")
                .Build();
            var entriesType = new StructType(new[]
            {
                new Field("key", StringType.Default, nullable: false),
                new Field("value", StringType.Default, nullable: true),
            });
            var entries = new StructArray(
                entriesType,
                1,
                new IArrowArray[] { keys, invalidGeo },
                ArrowBuffer.Empty);
            var offsets = new ArrowBuffer.Builder<int>();
            offsets.Append(0);
            offsets.Append(1);
            var validity = new ArrowBuffer.BitmapBuilder();
            validity.Append(false);
            var map = new MapArray(
                new MapType(StringType.Default, StringType.Default),
                1,
                offsets.Build(),
                entries,
                validity.Build(),
                nullCount: 1);
            const string sqlType = "MAP<STRING,GEOGRAPHY(ANY)>";

            using GeospatialTransformingStream stream = CreateStream(
                sqlType,
                ArrowTypeParser.MapToArrowType(sqlType, true),
                map,
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: true);

            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            MapArray output = Assert.IsType<MapArray>(batch.Column(0));
            Assert.True(output.IsNull(0));
            Assert.True(Assert.IsType<BinaryArray>(output.Values).IsNull(0));
        }

        [Fact]
        public async Task LegacyText_IsPreservedWithoutRevalidatingEmbeddedSridInStringMode()
        {
            StringArray text = new StringArray.Builder()
                .Append("SRID=3857;POINT (1 2)")
                .Build();

            using GeospatialTransformingStream stream = CreateStream(
                "GEOMETRY(4326)",
                StringType.Default,
                text,
                enableGeospatialSupport: false,
                enableComplexDatatypeSupport: true);

            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.Equal(
                "SRID=3857;POINT (1 2)",
                Assert.IsType<StringArray>(batch.Column(0)).GetString(0));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task LegacyTextBatch_IsHandledWhenManifestDeclaresTaggedStruct(bool enabled)
        {
            const string sqlType = "GEOMETRY(4326)";
            IArrowType manifestType = ArrowTypeParser.MapToArrowType(sqlType, true);
            StringArray text = new StringArray.Builder()
                .Append("SRID=4326;POINT(1 2)")
                .Build();

            using GeospatialTransformingStream stream = CreateStream(
                sqlType,
                manifestType,
                text,
                enableGeospatialSupport: enabled,
                enableComplexDatatypeSupport: true);
            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();

            Assert.NotNull(batch);
            if (enabled)
            {
                Assert.Equal(
                    s_pointOneTwo,
                    Assert.IsType<BinaryArray>(batch.Column(0)).GetBytes(0).ToArray());
            }
            else
            {
                Assert.Equal(
                    "SRID=4326;POINT(1 2)",
                    Assert.IsType<StringArray>(batch.Column(0)).GetString(0));
            }
        }

        [Fact]
        public void TaggedPhysicalType_IsDetectionFallbackWithoutLogicalMetadata()
        {
            StructType taggedType = ArrowTypeParser.MapToArrowType(
                "GEOGRAPHY(4326)", true) as StructType
                ?? throw new InvalidOperationException();
            Schema schema = new Schema.Builder()
                .Field(new Field("g", taggedType, nullable: true))
                .Build();
            using IArrowArrayStream source = new StubArrowArrayStream(
                schema,
                System.Array.Empty<RecordBatch>());
            using var stream = new GeospatialTransformingStream(
                source,
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: true);

            Field field = stream.Schema.GetFieldByIndex(0);
            Assert.IsType<BinaryType>(field.DataType);
            Assert.Equal("geoarrow.wkb", field.Metadata["ARROW:extension:name"]);
            Assert.Equal(
                "{\"crs\":\"4326\",\"crs_type\":\"srid\",\"edges\":\"spherical\"}",
                field.Metadata["ARROW:extension:metadata"]);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ManifestTaggedGeo_WithLegacyTextBatch_UsesConfiguredRepresentation(
            bool enableGeospatialSupport)
        {
            const string sqlType = "GEOMETRY(ANY)";
            const string ewkt = "SRID=4326;POINT(1.000 2.00)";
            StringArray text = new StringArray.Builder().Append(ewkt).Build();
            Schema schema = SchemaFor(
                sqlType,
                ArrowTypeParser.MapToArrowType(sqlType, true));
            using IArrowArrayStream source = new StubArrowArrayStream(
                schema,
                new[] { PhysicalBatch(text) });
            using GeospatialTransformingStream stream = await GeospatialTransformingStream.CreateAsync(
                source,
                enableGeospatialSupport,
                enableComplexDatatypeSupport: true);

            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            if (enableGeospatialSupport)
            {
                Assert.Equal(
                    GeospatialWkb.ToEwkb(s_pointOneTwo, 4326),
                    Assert.IsType<BinaryArray>(batch.Column(0)).GetBytes(0).ToArray());
            }
            else
            {
                Assert.Equal(ewkt, Assert.IsType<StringArray>(batch.Column(0)).GetString(0));
            }
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
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: true);

            Field field = stream.Schema.GetFieldByIndex(0);
            Assert.IsType<BinaryType>(field.DataType);
            Assert.Equal("geoarrow.wkb", field.Metadata["ARROW:extension:name"]);
            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            BinaryArray values = Assert.IsType<BinaryArray>(batch.Column(0));
            Assert.Equal(
                GeospatialWkb.ToEwkb(s_pointOneTwo, 4326),
                values.GetBytes(0).ToArray());
            Assert.Equal(
                GeospatialWkb.ToEwkb(s_line, 0),
                values.GetBytes(1).ToArray());
            Assert.True(values.IsNull(2));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task LegacyAnyGeographyTextWithoutSrid_UsesWgs84(bool large)
        {
            IArrowArray text = large
                ? new LargeStringArray.Builder().Append("POINT(1 2)").AppendNull().Build()
                : new StringArray.Builder().Append("POINT(1 2)").AppendNull().Build();

            using GeospatialTransformingStream stream = CreateStream(
                "GEOGRAPHY(ANY)",
                StringType.Default,
                text,
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: true);

            Field field = stream.Schema.GetFieldByIndex(0);
            Assert.IsType<BinaryType>(field.DataType);
            Assert.Equal("geoarrow.wkb", field.Metadata["ARROW:extension:name"]);
            Assert.Equal(
                "{\"edges\":\"spherical\"}",
                field.Metadata["ARROW:extension:metadata"]);
            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            BinaryArray values = Assert.IsType<BinaryArray>(batch.Column(0));
            Assert.Equal(
                GeospatialWkb.ToEwkb(s_pointOneTwo, 4326),
                values.GetBytes(0).ToArray());
            Assert.True(values.IsNull(1));
        }

        [Fact]
        public async Task LegacyFixedGeographyTextWithoutSrid_UsesDeclaredSrid()
        {
            StringArray text = new StringArray.Builder()
                .Append("POINT(1 2)")
                .Build();

            using GeospatialTransformingStream stream = CreateStream(
                "GEOGRAPHY",
                StringType.Default,
                text,
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: true);

            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.Equal(
                s_pointOneTwo,
                Assert.IsType<BinaryArray>(batch.Column(0)).GetBytes(0).ToArray());
            Assert.Equal(
                "{\"crs\":\"4326\",\"crs_type\":\"srid\",\"edges\":\"spherical\"}",
                stream.Schema.GetFieldByIndex(0).Metadata["ARROW:extension:metadata"]);
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
                enableGeospatialSupport: false,
                enableComplexDatatypeSupport: true);

            Assert.IsType<StringType>(stream.Schema.GetFieldByIndex(0).DataType);
            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            StringArray values = Assert.IsType<StringArray>(batch.Column(0));
            Assert.Equal("SRID=4326;POINT(1 2)", values.GetString(0));
            Assert.True(values.IsNull(1));
        }

        [Fact]
        public async Task LegacyLargeText_IsConvertedInNativeMode()
        {
            LargeStringArray text = new LargeStringArray.Builder()
                .Append("SRID=4326;POINT(1 2)")
                .AppendNull()
                .Build();

            using GeospatialTransformingStream stream = CreateStream(
                "GEOMETRY(ANY)",
                LargeStringType.Default,
                text,
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: true);

            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            BinaryArray values = Assert.IsType<BinaryArray>(batch.Column(0));
            Assert.Equal(
                GeospatialWkb.ToEwkb(s_pointOneTwo, 4326),
                values.GetBytes(0).ToArray());
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
                enableGeospatialSupport: true,
                enableComplexDatatypeSupport: true);

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
                enableGeospatialSupport: false,
                enableComplexDatatypeSupport: true);

            DatabricksException error = await Assert.ThrowsAsync<DatabricksException>(async () =>
                await stream.ReadNextRecordBatchAsync());
            Assert.Equal(AdbcStatusCode.InvalidData, error.Status);
            Assert.Contains("invalid WKB", error.Message);
        }

        [Fact]
        public async Task SchemaInitializationDisposesInnerWhenInitialPlanFails()
        {
            var source = new StubArrowArrayStream(
                SchemaFor("STRING", ArrowTypeParser.MapToArrowType("GEOMETRY(4326)", true)),
                System.Array.Empty<RecordBatch>());

            DatabricksException error = await Assert.ThrowsAsync<DatabricksException>(async () =>
                await GeospatialTransformingStream.CreateAsync(source, true, true));

            Assert.Equal(AdbcStatusCode.InvalidData, error.Status);
            Assert.Equal(0, source.ReadCount);
            Assert.Equal(1, source.DisposeCount);
        }

        [Fact]
        public async Task SchemaInitializationDisposesInnerWhenPrefetchFails()
        {
            var expected = new InvalidOperationException("first batch fetch failed");
            var source = new StubArrowArrayStream(
                SchemaFor("GEOMETRY(4326)", StringType.Default),
                System.Array.Empty<RecordBatch>())
            {
                ReadException = expected
            };

            InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await GeospatialTransformingStream.CreateAsync(source, true, true));

            Assert.Same(expected, error);
            Assert.Equal(1, source.ReadCount);
            Assert.Equal(1, source.DisposeCount);
        }

        [Fact]
        public async Task SchemaInitializationPreservesEveryBatchAndEof()
        {
            var source = new StubArrowArrayStream(
                SchemaFor("GEOMETRY(ANY)", StringType.Default),
                new[]
                {
                    PhysicalBatch(NativeGeoArray(new[] { 0 }, new[] { s_pointOneTwo }, new[] { true })),
                    PhysicalBatch(NativeGeoArray(new[] { 3857 }, new[] { s_pointOneTwo }, new[] { true })),
                });
            using GeospatialTransformingStream stream = await GeospatialTransformingStream.CreateAsync(
                source, enableGeospatialSupport: false, enableComplexDatatypeSupport: true);
            Assert.Equal(1, source.ReadCount);
            Assert.IsType<StringType>(stream.Schema.GetFieldByIndex(0).DataType);
            using RecordBatch? first = await stream.ReadNextRecordBatchAsync();
            Assert.Equal("POINT(1 2)", Assert.IsType<StringArray>(first!.Column(0)).GetString(0));
            Assert.Equal(1, source.ReadCount);
            using RecordBatch? second = await stream.ReadNextRecordBatchAsync();
            Assert.Equal("SRID=3857;POINT(1 2)", Assert.IsType<StringArray>(second!.Column(0)).GetString(0));
            Assert.Equal(2, source.ReadCount);
            Assert.Null(await stream.ReadNextRecordBatchAsync());
        }

        [Fact]
        public async Task SchemaInitializationRejectsMissingGeospatialContainer()
        {
            const string sqlType = "ARRAY<GEOMETRY(ANY)>";
            var source = new StubArrowArrayStream(
                SchemaFor(sqlType, ArrowTypeParser.MapToArrowType(sqlType, true)),
                new[] { PhysicalBatch(new Int32Array.Builder().Append(1).Build()) });
            DatabricksException error = await Assert.ThrowsAsync<DatabricksException>(async () =>
                await GeospatialTransformingStream.CreateAsync(source, true, true));
            Assert.Equal(AdbcStatusCode.InvalidData, error.Status);
            Assert.Equal(1, source.DisposeCount);
        }

        [Fact]
        public async Task SchemaInitializationPreservesPhysicalTypesOfOtherColumns()
        {
            StructArray geo = NativeGeoArray(new[] { 0 }, new[] { s_pointOneTwo }, new[] { true });
            var timestampType = new TimestampType(TimeUnit.Microsecond, "UTC");
            var timestamp = new TimestampArray.Builder(timestampType)
                .Append(new DateTimeOffset(2026, 10, 15, 12, 34, 56, TimeSpan.Zero).AddTicks(1234560))
                .Build();
            var reported = new Schema(new[]
            {
                SchemaFor("GEOMETRY(0)", StringType.Default).GetFieldByIndex(0),
                new Field("ts", TimestampType.Default, true),
            }, null);
            var physical = new Schema(new[]
            {
                new Field("g", geo.Data.DataType, true),
                new Field("ts", timestampType, true),
            }, null);
            var source = new StubArrowArrayStream(reported,
                new[] { new RecordBatch(physical, new IArrowArray[] { geo, timestamp }, 1) });
            using GeospatialTransformingStream stream = await GeospatialTransformingStream.CreateAsync(
                source, true, true);
            Assert.Same(timestampType, stream.Schema.GetFieldByIndex(1).DataType);
            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.Same(timestamp, batch!.Column(1));
            Assert.Same(timestampType, batch.Schema.GetFieldByIndex(1).DataType);
        }

        [Fact]
        public async Task NonGeospatialSchemaDoesNotPrefetch()
        {
            var source = new StubArrowArrayStream(
                SchemaFor("INT", Int32Type.Default),
                new[] { PhysicalBatch(new Int32Array.Builder().Append(1).Build()) });
            using GeospatialTransformingStream stream = await GeospatialTransformingStream.CreateAsync(
                source, true, true);
            Assert.Equal(0, source.ReadCount);
            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.Equal(1, Assert.IsType<Int32Array>(batch!.Column(0)).GetValue(0));
        }

        private static GeospatialTransformingStream CreateStream(
            string sqlType,
            IArrowType outputType,
            IArrowArray physical,
            bool enableGeospatialSupport,
            bool enableComplexDatatypeSupport)
        {
            Schema schema = SchemaFor(sqlType, outputType);
            var source = new StubArrowArrayStream(
                schema,
                new[] { PhysicalBatch(physical) });
            return GeospatialTransformingStream.CreateAsync(
                source,
                enableGeospatialSupport,
                enableComplexDatatypeSupport).GetAwaiter().GetResult();
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

        private static StructArray TaggedGeoArray(
            string sqlType,
            int srid,
            byte[] wkb)
        {
            StructType type = Assert.IsType<StructType>(
                ArrowTypeParser.MapToArrowType(sqlType, true));
            return new StructArray(
                type,
                1,
                new IArrowArray[]
                {
                    new Int32Array.Builder().Append(srid).Build(),
                    new BinaryArray.Builder().Append(wkb.AsSpan()).Build(),
                },
                ArrowBuffer.Empty);
        }

        private static StructArray TwoFieldStruct(
            string firstName,
            IArrowArray first,
            string secondName,
            IArrowArray second)
        {
            var type = new StructType(new[]
            {
                new Field(firstName, first.Data.DataType, nullable: true),
                new Field(secondName, second.Data.DataType, nullable: true),
            });
            return new StructArray(
                type,
                first.Length,
                new[] { first, second },
                ArrowBuffer.Empty);
        }

        private static StructArray OuterStruct(
            string fieldName,
            IArrowArray child,
            bool outerValid = true)
        {
            var type = new StructType(new[]
            {
                new Field(fieldName, child.Data.DataType, nullable: true),
            });
            var validity = new ArrowBuffer.BitmapBuilder();
            for (int row = 0; row < child.Length; row++)
                validity.Append(outerValid);
            return new StructArray(
                type,
                child.Length,
                new[] { child },
                outerValid ? ArrowBuffer.Empty : validity.Build(),
                outerValid ? 0 : child.Length);
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

            internal int ReadCount { get; private set; }
            internal int DisposeCount { get; private set; }
            internal Exception? ReadException { get; set; }

            public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(
                CancellationToken cancellationToken = default)
            {
                ReadCount++;
                if (ReadException != null)
                    throw ReadException;
                return new ValueTask<RecordBatch?>(
                    _batches.Count == 0 ? null : _batches.Dequeue());
            }

            public void Dispose() => DisposeCount++;
        }
    }
}
