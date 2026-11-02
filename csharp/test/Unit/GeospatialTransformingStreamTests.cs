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
                enableGeospatialSupport: true);

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
                enableGeospatialSupport: true);
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
                enableGeospatialSupport: true);
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
                enableGeospatialSupport: false);

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
                "GEOMETRY(ANY)", StringType.Default, physical, enableGeospatialSupport: false);
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
                enableGeospatialSupport: enabled);

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
                enableGeospatialSupport: false);

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
                enableGeospatialSupport: true);

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
                enableGeospatialSupport: true);

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
                enableGeospatialSupport: true);

            Field field = stream.Schema.GetFieldByIndex(0);
            Assert.IsType<BinaryType>(field.DataType);
            Assert.Equal("geoarrow.wkb", field.Metadata["ARROW:extension:name"]);
            if (expectedMetadata == null)
            {
                Assert.False(field.Metadata.ContainsKey("ARROW:extension:metadata"));
            }
            else
            {
                Assert.Equal(expectedMetadata, field.Metadata["ARROW:extension:metadata"]);
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
                enableGeospatialSupport: true);

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
                enableGeospatialSupport: false);

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
                enableGeospatialSupport: true);

            DatabricksException error = await Assert.ThrowsAsync<DatabricksException>(async () =>
                await stream.ReadNextRecordBatchAsync());
            Assert.Equal(AdbcStatusCode.InvalidData, error.Status);
            Assert.Contains("negative SRID", error.Message);
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
                enableGeospatialSupport: false);

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
                enableGeospatialSupport: enabled);
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
        public void NonGeospatialLogicalType_RejectsContradictoryPhysicalTag()
        {
            StructType taggedType = ArrowTypeParser.MapToArrowType(
                "GEOMETRY(4326)", true) as StructType
                ?? throw new InvalidOperationException();
            Schema schema = SchemaFor("STRING", taggedType);
            using IArrowArrayStream source = new StubArrowArrayStream(
                schema,
                System.Array.Empty<RecordBatch>());

            DatabricksException error = Assert.Throws<DatabricksException>(() =>
                new GeospatialTransformingStream(source, enableGeospatialSupport: true));

            Assert.Equal(AdbcStatusCode.InvalidData, error.Status);
            Assert.Contains("non-geospatial SQL type", error.Message);
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
                enableGeospatialSupport: true);

            Field field = stream.Schema.GetFieldByIndex(0);
            Assert.IsType<BinaryType>(field.DataType);
            Assert.Equal("geoarrow.wkb", field.Metadata["ARROW:extension:name"]);
            Assert.Equal(
                "{\"crs\":\"4326\",\"crs_type\":\"srid\",\"edges\":\"spherical\"}",
                field.Metadata["ARROW:extension:metadata"]);
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
                enableGeospatialSupport: true);

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
                enableGeospatialSupport: true);

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
                enableGeospatialSupport: false);

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
                enableGeospatialSupport: true);

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
