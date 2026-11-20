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
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    public class GeospatialWkbTests
    {
        [Theory]
        [InlineData("POINT")]
        [InlineData("LINESTRING")]
        [InlineData("POLYGON")]
        [InlineData("MULTIPOINT")]
        [InlineData("MULTILINESTRING")]
        [InlineData("MULTIPOLYGON")]
        [InlineData("GEOMETRYCOLLECTION")]
        public void EmptyGeometryRetainsEncodedDimensions(string shape)
        {
            foreach (string dimension in new[] { "", " Z", " M", " ZM" })
            {
                string expected = shape + dimension + " EMPTY";
                byte[] wkb = GeospatialWkb.ToWkb(expected, 0, out _);
                Assert.Equal(expected, GeospatialWkb.ToWkt(wkb));
                Assert.Equal(expected, GeospatialWkb.ToWkt(GeospatialWkb.ToEwkb(wkb, 4326)));
            }
        }

        [Theory]
        [InlineData("MULTIPOINT Z (EMPTY,EMPTY)")]
        [InlineData("MULTIPOINT M (EMPTY,EMPTY)")]
        [InlineData("MULTIPOINT ZM (EMPTY,EMPTY)")]
        [InlineData("MULTILINESTRING Z (EMPTY,EMPTY)")]
        [InlineData("MULTILINESTRING M (EMPTY,EMPTY)")]
        [InlineData("MULTILINESTRING ZM (EMPTY,EMPTY)")]
        [InlineData("MULTIPOLYGON Z (EMPTY,EMPTY)")]
        [InlineData("MULTIPOLYGON M (EMPTY,EMPTY)")]
        [InlineData("MULTIPOLYGON ZM (EMPTY,EMPTY)")]
        [InlineData("GEOMETRYCOLLECTION Z (POINT Z EMPTY,LINESTRING Z EMPTY)")]
        [InlineData("GEOMETRYCOLLECTION M (POINT M EMPTY,LINESTRING M EMPTY)")]
        [InlineData("GEOMETRYCOLLECTION ZM (POINT ZM EMPTY,LINESTRING ZM EMPTY)")]
        [InlineData("GEOMETRYCOLLECTION ZM (POINT ZM EMPTY,GEOMETRYCOLLECTION ZM (MULTIPOINT ZM (EMPTY,EMPTY)))")]
        public void EmptyMembersRetainEncodedDimensions(string expected)
        {
            byte[] wkb = GeospatialWkb.ToWkb(expected, 0, out _);
            Assert.Equal(expected, GeospatialWkb.ToWkt(wkb));
            Assert.Equal(expected, GeospatialWkb.ToWkt(GeospatialWkb.ToEwkb(wkb, 4326)));
            Assert.Equal(wkb, GeospatialWkb.ToWkb(GeospatialWkb.ToWkt(wkb), 0, out _));
        }

        [Theory]
        [InlineData("MULTIPOINT Z (EMPTY,(1 2 3),EMPTY)")]
        [InlineData("MULTILINESTRING M (EMPTY,(1 2 3,4 5 6))")]
        [InlineData("MULTIPOLYGON ZM (EMPTY,((0 0 1 2,0 1 2 3,1 0 3 4,0 0 1 2)))")]
        [InlineData("GEOMETRYCOLLECTION ZM (POINT ZM (1 2 3 4),GEOMETRYCOLLECTION ZM EMPTY,MULTIPOINT ZM (EMPTY,EMPTY))")]
        [InlineData("POINT Z (1 2 NaN)")]
        [InlineData("POINT M (1 2 NaN)")]
        [InlineData("POINT ZM (1 2 NaN NaN)")]
        [InlineData("POINT ZM (0.12345678901234566 -0.0000000000000001 12345678901234566 -0.12345678901234566)")]
        public void DimensionalCoordinatesAndEmptyMembersRoundTrip(string expected)
        {
            byte[] wkb = GeospatialWkb.ToWkb(expected, 0, out _);
            Assert.Equal(expected, GeospatialWkb.ToWkt(wkb));
            Assert.Equal(wkb, GeospatialWkb.ToWkb(GeospatialWkb.ToWkt(wkb), 0, out _));
        }

        [Theory]
        [InlineData(
            "01bf0b0000020000000000000bbf0000000101ba0b0000000000000000000bbb00000000",
            "GEOMETRYCOLLECTION ZM (GEOMETRYCOLLECTION ZM (LINESTRING ZM EMPTY),POLYGON ZM EMPTY)")]
        [InlineData(
            "00000007d70000000200000007d20000000000000007d40000000100000007d17ff80000000000007ff80000000000007ff8000000000000",
            "GEOMETRYCOLLECTION M (LINESTRING M EMPTY,MULTIPOINT M (EMPTY))")]
        [InlineData(
            "01070000000300000000000003e93ff00000000000004000000000000000400800000000000001d1070000000000000000f87f000000000000f87f000000000000f87f0000000bbf00000000",
            "GEOMETRYCOLLECTION(POINT Z (1 2 3),POINT M EMPTY,GEOMETRYCOLLECTION ZM EMPTY)")]
        public void CollectionsRetainEachWireHeaderDimensionAndByteOrder(string hex, string expected)
        {
            var wkb = new byte[hex.Length / 2];
            for (int i = 0; i < wkb.Length; i++)
                wkb[i] = Convert.ToByte(hex.Substring(i * 2, 2), 16);
            Assert.Equal(expected, GeospatialWkb.ToWkt(wkb));
            Assert.Equal(expected, GeospatialWkb.ToWkt(GeospatialWkb.ToEwkb(wkb, 4326)));
        }

        [Theory]
        [InlineData("01e9030000000000000000f87f000000000000f87f000000000000f87f", "POINT Z EMPTY")]
        [InlineData("01d1070000000000000000f87f000000000000f87f000000000000f87f", "POINT M EMPTY")]
        [InlineData("01b90b0000000000000000f87f000000000000f87f000000000000f87f000000000000f87f", "POINT ZM EMPTY")]
        [InlineData("00000003e97ff80000000000007ff80000000000007ff8000000000000", "POINT Z EMPTY")]
        public void EmptyPointRetainsWireDimensions(string hex, string expected)
        {
            var wkb = new byte[hex.Length / 2];
            for (int i = 0; i < wkb.Length; i++)
                wkb[i] = Convert.ToByte(hex.Substring(i * 2, 2), 16);
            Assert.Equal(expected, GeospatialWkb.ToWkt(wkb));
        }
    }
}
