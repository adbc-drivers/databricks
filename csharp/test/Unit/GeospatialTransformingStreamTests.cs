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
                "{\"crs\":\"EPSG:4326\",\"crs_type\":\"authority_code\",\"edges\":\"spherical\"}",
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
            "{\"crs\":\"EPSG:3857\",\"crs_type\":\"authority_code\"}")]
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

        [Fact]
        public async Task NestedStruct_ConvertsReportedLegacyGeoTextInNativeMode()
        {
            StringArray geo = new StringArray.Builder()
                .Append("SRID=4326;POINT(1 2)")
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
            const string sqlType = "STRUCT<geom:GEOMETRY(ANY),id:INT>";

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
                GeospatialWkb.ToEwkb(s_pointOneTwo, 4326),
                native.GetBytes(0).ToArray());
        }

        [Fact]
        public async Task NestedStruct_BecomesEwktInsideOuterJsonWhenComplexSupportDisabled()
        {
            StructArray geo = NativeGeoArray(
                new[] { 3857 },
                new[] { s_pointOneTwo },
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
            using var stream = new ComplexTypeSerializingStream(geospatial);

            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            StringArray values = Assert.IsType<StringArray>(batch.Column(0));
            Assert.Equal("{\"geom\":\"SRID=3857;POINT(1 2)\"}", values.GetString(0));
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task NestedArray_UsesConfiguredGeoRepresentation(bool enabled)
        {
            StructArray geo = NativeGeoArray(
                new[] { 4326 },
                new[] { s_pointOneTwo },
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
                    GeospatialWkb.ToEwkb(s_pointOneTwo, 4326),
                    item.GetBytes(0).ToArray());
            }
            else
            {
                Assert.Equal(
                    "SRID=4326;POINT(1 2)",
                    Assert.IsType<StringArray>(output.Values).GetString(0));
            }
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
        public async Task LegacyText_IsPreservedInStringMode()
        {
            StringArray text = new StringArray.Builder()
                .Append("SRID=4326;POINT(1 2)")
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
            return new GeospatialTransformingStream(
                source,
                enableGeospatialSupport,
                enableComplexDatatypeSupport);
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

        private static StructArray OuterStruct(string fieldName, IArrowArray child)
        {
            var type = new StructType(new[]
            {
                new Field(fieldName, child.Data.DataType, nullable: true),
            });
            return new StructArray(
                type,
                child.Length,
                new[] { child },
                ArrowBuffer.Empty);
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
