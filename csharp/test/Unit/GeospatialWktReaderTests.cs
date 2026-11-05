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
using System.Globalization;
using System.Text;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    public class GeospatialWktReaderTests
    {
        // Fixed ISO WKB fixtures independently verify every geometry type and
        // dimensional header without using the reader to compute expected bytes.
        [Theory]
        [InlineData(
            "POINT(1 2)",
            "0101000000000000000000f03f0000000000000040")]
        [InlineData(
            "LINESTRING M(1 2 3,5 6 7)",
            "01d207000002000000000000000000f03f0000000000000040000000000000084000000000000014" +
            "4000000000000018400000000000001c40")]
        [InlineData(
            "POLYGON Z((1 2 3,5 6 7,9 10 11,1 2 3))",
            "01eb0300000100000004000000000000000000f03f00000000000000400000000000000840000000" +
            "000000144000000000000018400000000000001c4000000000000022400000000000002440000000" +
            "0000002640000000000000f03f00000000000000400000000000000840")]
        [InlineData(
            "MULTIPOINT ZM(1 2 3 4,5 6 7 8)",
            "01bc0b00000200000001b90b0000000000000000f03f000000000000004000000000000008400000" +
            "00000000104001b90b0000000000000000144000000000000018400000000000001c400000000000" +
            "002040")]
        [InlineData(
            "MULTILINESTRING(EMPTY,(1 2,5 6))",
            "010500000002000000010200000000000000010200000002000000000000000000f03f0000000000" +
            "00004000000000000014400000000000001840")]
        [InlineData(
            "MULTIPOLYGON M(EMPTY,((1 2 3,5 6 7,9 10 11,1 2 3)))",
            "01d60700000200000001d30700000000000001d30700000100000004000000000000000000f03f00" +
            "000000000000400000000000000840000000000000144000000000000018400000000000001c4000" +
            "0000000000224000000000000024400000000000002640000000000000f03f000000000000004000" +
            "00000000000840")]
        [InlineData(
            "GEOMETRYCOLLECTION ZM(POINT ZM(1 2 3 4),LINESTRING ZM EMPTY,MULTIPOLYGON ZM(((1 2 3 4,5 6 7 8,9 10 11 12,1 2 3 4))))",
            "01bf0b00000300000001b90b0000000000000000f03f000000000000004000000000000008400000" +
            "00000000104001ba0b00000000000001be0b00000100000001bb0b00000100000004000000000000" +
            "000000f03f0000000000000040000000000000084000000000000010400000000000001440000000" +
            "00000018400000000000001c40000000000000204000000000000022400000000000002440000000" +
            "00000026400000000000002840000000000000f03f00000000000000400000000000000840000000" +
            "0000001040")]
        public void ReadsKnownIsoWkb(string wkt, string expectedHex)
        {
            byte[] actual = GeospatialWkb.ToWkb(wkt, 4326, out int srid);
            Assert.Equal(4326, srid);
            Assert.Equal(FromHex(expectedHex), actual);
        }

        [Fact]
        public void CollectionRetainsEachMembersDimension()
        {
            const string wkt = "GEOMETRYCOLLECTION(POINT Z (1 2 3),POINT M (4 5 6))";
            byte[] expected = FromHex(
                "010700000002000000" +
                "01e9030000000000000000f03f00000000000000400000000000000840" +
                "01d1070000000000000000104000000000000014400000000000001840");
            byte[] actual = GeospatialWkb.ToWkb(wkt, 0, out _);
            Assert.Equal(expected, actual);
            Assert.Equal(wkt, GeospatialWkb.ToWkt(actual));
        }

        [Fact]
        public void UnmarkedCollectionMemberInheritsExplicitDimension()
        {
            byte[] actual = GeospatialWkb.ToWkb("GEOMETRYCOLLECTION M (POINT(1 2 3))", 0, out _);
            Assert.Equal(
                FromHex("01d70700000100000001d1070000000000000000f03f00000000000000400000000000000840"),
                actual);
            Assert.Equal("GEOMETRYCOLLECTION M (POINT M (1 2 3))", GeospatialWkb.ToWkt(actual));
        }

        // Fixed ISO WKB checks every nested header and ordinate. An explicit child
        // dimension applies to its descendants without changing a sibling's inheritance.
        [Theory]
        [InlineData(
            "GEOMETRYCOLLECTION Z (GEOMETRYCOLLECTION(POINT(1 2 3)))",
            "01ef03000001000000" + "01ef03000001000000" +
            "01e9030000000000000000f03f00000000000000400000000000000840")]
        [InlineData(
            "GEOMETRYCOLLECTION M (GEOMETRYCOLLECTION(POINT(1 2 3)))",
            "01d707000001000000" + "01d707000001000000" +
            "01d1070000000000000000f03f00000000000000400000000000000840")]
        [InlineData(
            "GEOMETRYCOLLECTION ZM (GEOMETRYCOLLECTION(POINT(1 2 3 4)))",
            "01bf0b000001000000" + "01bf0b000001000000" +
            "01b90b0000000000000000f03f000000000000004000000000000008400000000000001040")]
        [InlineData(
            "GEOMETRYCOLLECTION Z (GEOMETRYCOLLECTION M (POINT(1 2 3)),POINT(4 5 6))",
            "01ef03000002000000" + "01d707000001000000" +
            "01d1070000000000000000f03f00000000000000400000000000000840" +
            "01e9030000000000000000104000000000000014400000000000001840")]
        [InlineData(
            "GEOMETRYCOLLECTION M (GEOMETRYCOLLECTION ZM (POINT(1 2 3 4)),POINT(5 6 7))",
            "01d707000002000000" + "01bf0b000001000000" +
            "01b90b0000000000000000f03f000000000000004000000000000008400000000000001040" +
            "01d1070000000000000000144000000000000018400000000000001c40")]
        [InlineData(
            "GEOMETRYCOLLECTION ZM (GEOMETRYCOLLECTION Z (POINT(1 2 3)),POINT(4 5 6 7))",
            "01bf0b000002000000" + "01ef03000001000000" +
            "01e9030000000000000000f03f00000000000000400000000000000840" +
            "01b90b00000000000000001040000000000000144000000000000018400000000000001c40")]
        public void NestedCollectionsInheritAndOverrideDimensions(string wkt, string expectedHex)
        {
            Assert.Equal(FromHex(expectedHex), GeospatialWkb.ToWkb(wkt, 0, out _));
        }

        [Theory]
        [InlineData("POINT(1 2)", 0, 0)]
        [InlineData("POINT(1 2)", 4326, 4326)]
        [InlineData("SRID=0;POINT(1 2)", 4326, 0)]
        [InlineData("SRID=3857;POINT(1 2)", 4326, 3857)]
        [InlineData("  srid = +4326 ; point (1 2)  ", 0, 4326)]
        public void ReadsEwktSridSeparately(string wkt, int defaultSrid, int expectedSrid)
        {
            byte[] actual = GeospatialWkb.ToWkb(wkt, defaultSrid, out int srid);
            Assert.Equal(expectedSrid, srid);
            Assert.Equal(FromHex("0101000000000000000000f03f0000000000000040"), actual);
        }

        [Theory]
        [InlineData("POLYGON((0 0,8 0,0 8,0 0),(1 1,2 1,1 2,1 1))")]
        [InlineData("MULTIPOLYGON(((0 0,8 0,0 8,0 0),(1 1,2 1,1 2,1 1)),EMPTY,((0 0,1 0,0 1,0 0)))")]
        [InlineData("GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT EMPTY),MULTIPOINT((1 2),EMPTY))")]
        [InlineData("POINT ZM (Inf -Inf NaN NaN)")]
        public void ReadsRingsCollectionsAndNonFiniteOrdinates(string wkt)
        {
            Assert.Equal(wkt, GeospatialWkb.ToWkt(GeospatialWkb.ToWkb(wkt, 0, out _)));
        }

        [Theory]
        [InlineData("fr-FR")]
        [InlineData("tr-TR")]
        public void NumbersAndKeywordsAreCultureIndependent(string culture)
        {
            CultureInfo previous = CultureInfo.CurrentCulture;
            try
            {
                CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(culture);
                byte[] wkb = GeospatialWkb.ToWkb("srid=4326;point (1.25e-3 -2.5E+2)", 0, out int srid);
                Assert.Equal(4326, srid);
                Assert.Equal("POINT(0.00125 -250)", GeospatialWkb.ToWkt(wkb));
            }
            finally
            {
                CultureInfo.CurrentCulture = previous;
            }
        }

        [Theory]
        [InlineData(double.Epsilon)]
        [InlineData(-double.Epsilon)]
        [InlineData(double.MaxValue)]
        [InlineData(double.MinValue)]
        [InlineData(2.2250738585072014e-308)]
        [InlineData(0.12345678901234566)]
        [InlineData(-0.0000000000000001)]
        [InlineData(12345678901234566d)]
        [InlineData(0.1)]
        public void CoordinateFormattingPreservesDoublePrecision(double value)
        {
            byte[] expected = FromHex("010100000000000000000000000000000000000000");
            byte[] ordinate = BitConverter.GetBytes(value);
            if (!BitConverter.IsLittleEndian)
                System.Array.Reverse(ordinate);
            ordinate.CopyTo(expected, 5);
            string text = GeospatialWkb.ToWkt(expected);
            Assert.DoesNotContain("E", text);
            Assert.Equal(expected, GeospatialWkb.ToWkb(text, 0, out _));
        }

        [Theory]
        [InlineData("")]
        [InlineData("  ")]
        [InlineData("POINT")]
        [InlineData("POINT()")]
        [InlineData("POINT(1)")]
        [InlineData("POINT(1,2)")]
        [InlineData("POINT(1 2 3)")]
        [InlineData("POINT Z (1 2)")]
        [InlineData("POINT M (1 2 3 4)")]
        [InlineData("POINT ZM (1 2 3)")]
        [InlineData("POINT ZZ (1 2 3)")]
        [InlineData("POINT(1-2)")]
        [InlineData("POINT(1.2.3 4)")]
        [InlineData("POINT(1e+ 2)")]
        [InlineData("POINT(1e999 2)")]
        [InlineData("POINT(1 2) trailing")]
        [InlineData("POINT EMPTY trailing")]
        [InlineData("LINESTRING(1 2)")]
        [InlineData("LINESTRING(1 2,)")]
        [InlineData("POLYGON((0 0,1 0,0 0))")]
        [InlineData("POLYGON((0 0,1 0,1 1,0 1))")]
        [InlineData("MULTIPOINT((1 2),(3))")]
        [InlineData("MULTILINESTRING((1 2))")]
        [InlineData("GEOMETRYCOLLECTION()")]
        [InlineData("GEOMETRYCOLLECTION(POINT(1 2),)")]
        [InlineData("GEOMETRYCOLLECTION(POINT(1 2)")]
        [InlineData("CIRCULARSTRING(0 0,1 1,2 0)")]
        [InlineData("SRID=2147483648;POINT(1 2)")]
        [InlineData("SRID=bad;POINT(1 2)")]
        [InlineData("SRID=4326;")]
        public void MalformedWktIsRejected(string wkt)
        {
            Assert.Throws<FormatException>(() => GeospatialWkb.ToWkb(wkt, 0, out _));
        }

        [Fact]
        public void ExcessiveCollectionNestingIsRejected()
        {
            var input = new StringBuilder();
            for (int i = 0; i < 129; i++)
                input.Append("GEOMETRYCOLLECTION(");
            input.Append("POINT(1 2)").Append(')', 129);
            FormatException error = Assert.Throws<FormatException>(() =>
                GeospatialWkb.ToWkb(input.ToString(), 0, out _));
            Assert.Contains("nesting", error.Message);
        }

        private static byte[] FromHex(string hex)
        {
            var bytes = new byte[hex.Length / 2];
            for (int i = 0; i < bytes.Length; i++)
                bytes[i] = Convert.ToByte(hex.Substring(i * 2, 2), 16);
            return bytes;
        }
    }
}
