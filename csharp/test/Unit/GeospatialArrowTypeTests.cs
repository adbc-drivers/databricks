/*
 * Copyright (c) 2026 ADBC Drivers Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.Collections.Generic;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Types;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    public class GeospatialArrowTypeTests
    {
        [Theory]
        [InlineData("GEOMETRY", "geometry", "0")]
        [InlineData("GEOMETRY(0)", "geometry", "0")]
        [InlineData("GEOMETRY(3857)", "geometry", "3857")]
        [InlineData("GEOMETRY(ANY)", "geometry", "-1")]
        [InlineData("GEOGRAPHY", "geography", "4326")]
        [InlineData("GEOGRAPHY(4326)", "geography", "4326")]
        [InlineData("GEOGRAPHY(ANY)", "geography", "-1")]
        public void TryCreate_ReturnsTaggedInternalStruct(
            string sqlType,
            string familyKey,
            string expectedSrid)
        {
            Assert.True(GeospatialArrowType.TryCreate(sqlType, out StructType type));

            Assert.Equal(2, type.Fields.Count);
            Assert.Equal("srid", type.Fields[0].Name);
            Assert.IsType<Int32Type>(type.Fields[0].DataType);
            Assert.False(type.Fields[0].IsNullable);
            Assert.Equal("wkb", type.Fields[1].Name);
            Assert.IsType<BinaryType>(type.Fields[1].DataType);
            Assert.False(type.Fields[1].IsNullable);
            Assert.Equal("true", type.Fields[1].Metadata[familyKey]);
            Assert.Equal(expectedSrid, type.Fields[1].Metadata["srid"]);
            Assert.True(GeospatialArrowType.ResolveTag(type, out _));
        }

        [Theory]
        [InlineData("GEOMETRY(foo)")]
        [InlineData("GEOGRAPHY()")]
        [InlineData("GEOMETRY(-1)")]
        [InlineData("GEOGRAPHY(-2)")]
        [InlineData("GEOGRAPHY(0)")]
        [InlineData("GEOGRAPHY(3857)")]
        [InlineData("GEOGRAPHY(2147483647)")]
        [InlineData("GEOMETRY(2147483648)")]
        [InlineData("GEOGRAPHY(4326) trailing")]
        public void TryCreate_RejectsMalformedType(string sqlType)
        {
            Assert.False(GeospatialArrowType.TryCreate(sqlType, out _));
        }

        [Fact]
        public void PartialArrowTag_IsRejected()
        {
            StructType type = TaggedType(
                new Dictionary<string, string> { ["geometry"] = "true" });

            DatabricksException error = Assert.Throws<DatabricksException>(() =>
                GeospatialArrowType.ResolveTag(type, out _));
            Assert.Equal(AdbcStatusCode.InvalidData, error.Status);
            Assert.Contains("srid", error.Message, System.StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void ConflictingArrowTag_IsRejected()
        {
            StructType type = TaggedType(new Dictionary<string, string>
            {
                ["geometry"] = "true",
                ["geography"] = "true",
                ["srid"] = "4326",
            });

            DatabricksException error = Assert.Throws<DatabricksException>(() =>
                GeospatialArrowType.ResolveTag(type, out _));
            Assert.Equal(AdbcStatusCode.InvalidData, error.Status);
            Assert.Contains("exactly one", error.Message, System.StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void NegativeFixedSridArrowTag_IsRejected()
        {
            StructType type = TaggedType(new Dictionary<string, string>
            {
                ["geometry"] = "true",
                ["srid"] = "-2",
            });

            DatabricksException error = Assert.Throws<DatabricksException>(() =>
                GeospatialArrowType.ResolveTag(type, out _));
            Assert.Equal(AdbcStatusCode.InvalidData, error.Status);
            Assert.Contains("nonnegative", error.Message, System.StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void UntaggedTransportShape_IsNotGeospatial()
        {
            StructType type = TaggedType(new Dictionary<string, string>());

            Assert.False(GeospatialArrowType.ResolveTag(type, out _));
        }

        [Theory]
        [InlineData("0")]
        [InlineData("3857")]
        [InlineData("2147483647")]
        public void UnsupportedFixedGeographySridArrowTag_IsRejected(string srid)
        {
            StructType type = TaggedType(new Dictionary<string, string>
            {
                ["geography"] = "true",
                ["srid"] = srid,
            });

            DatabricksException error = Assert.Throws<DatabricksException>(() =>
                GeospatialArrowType.ResolveTag(type, out _));
            Assert.Equal(AdbcStatusCode.InvalidData, error.Status);
            Assert.Contains("4326", error.Message);
        }

        [Theory]
        [InlineData("geometry", "false", "4326", "must be true")]
        [InlineData("geography", "false", "4326", "must be true")]
        [InlineData("geometry", "true", "not-an-integer", "invalid type-level srid")]
        [InlineData("geometry", "true", "2147483648", "invalid type-level srid")]
        [InlineData(null, null, "4326", "exactly one")]
        public void MalformedArrowTag_IsRejected(
            string? familyKey,
            string? familyValue,
            string srid,
            string expectedError)
        {
            var metadata = new Dictionary<string, string> { ["srid"] = srid };
            if (familyKey != null)
                metadata[familyKey] = familyValue!;

            DatabricksException error = Assert.Throws<DatabricksException>(() =>
                GeospatialArrowType.ResolveTag(TaggedType(metadata), out _));
            Assert.Equal(AdbcStatusCode.InvalidData, error.Status);
            Assert.Contains(expectedError, error.Message);
        }

        [Fact]
        public void NearMissTransportShapes_AreNotGeospatial()
        {
            StructType canonical = TaggedType(new Dictionary<string, string>
            {
                ["geometry"] = "true",
                ["srid"] = "4326",
            });
            Field srid = canonical.Fields[0];
            Field wkb = canonical.Fields[1];
            var nearMisses = new[]
            {
                new StructType(new[]
                {
                    srid,
                    new Field("wkb", LargeBinaryType.Default, nullable: false, wkb.Metadata),
                }),
                new StructType(new[] { wkb, srid }),
                new StructType(new[] { srid, wkb, new Field("extra", Int32Type.Default, false) }),
            };

            foreach (StructType type in nearMisses)
            {
                Assert.False(GeospatialArrowType.IsStructShape(type));
                Assert.False(GeospatialArrowType.ResolveTag(type, out _));
            }
        }

        [Theory]
        [InlineData("GEOMETRY(103192)", "{\"crs\":\"103192\",\"crs_type\":\"srid\"}")]
        [InlineData("GEOGRAPHY(4326)", "{\"crs\":\"4326\",\"crs_type\":\"srid\",\"edges\":\"spherical\"}")]
        [InlineData("GEOMETRY", null)]
        [InlineData("GEOMETRY(0)", null)]
        [InlineData("GEOMETRY(ANY)", null)]
        [InlineData("GEOGRAPHY(ANY)", "{\"edges\":\"spherical\"}")]
        public void GeoArrowMetadata_ReplacesStaleMetadataForSridAndFamily(
            string sqlType,
            string? expectedMetadata)
        {
            Assert.True(GeospatialArrowType.TryCreate(sqlType, out StructType type));
            Assert.True(GeospatialArrowType.ResolveTag(type, out GeospatialArrowType.Tag tag));
            var sourceMetadata = new Dictionary<string, string>
            {
                ["ARROW:extension:name"] = "stale.extension",
                ["ARROW:extension:metadata"] = "{\"crs\":\"stale\"}",
                ["custom"] = "preserved",
            };

            Field output = GeospatialArrowType.CreateGeoArrowField(
                new Field("geo", type, nullable: true, sourceMetadata),
                tag);

            Assert.Equal("geo", output.Name);
            Assert.True(output.IsNullable);
            Assert.IsType<BinaryType>(output.DataType);
            Assert.Equal("geoarrow.wkb", output.Metadata["ARROW:extension:name"]);
            Assert.Equal("preserved", output.Metadata["custom"]);
            if (expectedMetadata == null)
                Assert.False(output.Metadata.ContainsKey("ARROW:extension:metadata"));
            else
                Assert.Equal(expectedMetadata, output.Metadata["ARROW:extension:metadata"]);
            Assert.Equal("stale.extension", sourceMetadata["ARROW:extension:name"]);
            Assert.Equal("{\"crs\":\"stale\"}", sourceMetadata["ARROW:extension:metadata"]);
        }

        private static StructType TaggedType(Dictionary<string, string> metadata) =>
            new StructType(new[]
            {
                new Field("srid", Int32Type.Default, nullable: false),
                new Field("wkb", BinaryType.Default, nullable: false, metadata),
            });
    }
}
