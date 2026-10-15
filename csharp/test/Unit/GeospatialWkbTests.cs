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
