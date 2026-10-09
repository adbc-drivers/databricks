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
using System.Text.RegularExpressions;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Decodes the OGC/ISO WKB emitted by Reyden and renders WKT.
    /// The SRID is intentionally not read from WKB: Reyden carries it in the sibling
    /// <c>srid</c> Arrow field.
    /// </summary>
    internal static class GeospatialWkb
    {
        private const uint EwkbSridFlag = 0x20000000;

        private static readonly Regex s_ewktPattern = new Regex(
            @"^\s*(?:SRID\s*=\s*(?<srid>[+-]?\d+)\s*;\s*)?(?<wkt>.+?)\s*$",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Singleline);

        private static readonly Regex s_wktDimensionPattern = new Regex(
            @"^\s*[A-Z]+(?:\s+(?<dimension>ZM|Z|M))?\s*(?:\(|EMPTY\b)",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

        internal static string ToWkt(ReadOnlySpan<byte> wkb)
        {
            try
            {
                var reader = new WKBReader { IsStrict = true };
                Geometry geometry = reader.Read(wkb.ToArray());

                var writer = new WKTWriter(4)
                {
                    OutputOrdinates = Ordinates.XYZM,
                };
                return Compact(writer.Write(geometry));
            }
            catch (ParseException ex)
            {
                throw new FormatException(ex.Message, ex);
            }
        }

        /// <summary>
        /// Parses server-rendered WKT / EWKT and emits the same strict, little-endian
        /// OGC/ISO WKB representation used by Reyden. The SRID is returned separately
        /// because the Arrow representation stores it in a sibling field, not in WKB.
        /// </summary>
        internal static byte[] ToWkb(string ewkt, int defaultSrid, out int srid)
        {
            Match ewktMatch = s_ewktPattern.Match(ewkt ?? string.Empty);
            if (!ewktMatch.Success)
                throw new FormatException("Empty WKT / EWKT value");

            srid = defaultSrid;
            Group sridGroup = ewktMatch.Groups["srid"];
            if (sridGroup.Success
                && !int.TryParse(
                    sridGroup.Value,
                    NumberStyles.Integer,
                    CultureInfo.InvariantCulture,
                    out srid))
            {
                throw new FormatException($"Invalid EWKT SRID '{sridGroup.Value}'");
            }

            string wkt = ewktMatch.Groups["wkt"].Value;
            Match dimensionMatch = s_wktDimensionPattern.Match(wkt);
            if (!dimensionMatch.Success)
                throw new FormatException("Invalid WKT geometry type or dimensional marker");

            string dimension = dimensionMatch.Groups["dimension"].Value.ToUpperInvariant();
            bool emitZ = dimension is "Z" or "ZM";
            bool emitM = dimension is "M" or "ZM";

            try
            {
                var reader = new WKTReader
                {
                    IsStrict = true,
                    // Databricks emits an explicit Z/M/ZM marker for dimensional values.
                    // Reject ambiguous unmarked three-dimensional input.
                    IsOldNtsCoordinateSyntaxAllowed = false,
                };
                Geometry geometry = reader.Read(wkt);
                geometry.SRID = srid;

                // Strict mode (the default) uses ISO dimension offsets (1000/2000/3000)
                // and handleSRID=false keeps the bytes as WKB rather than EWKB.
                var writer = new WKBWriter(
                    ByteOrder.LittleEndian,
                    handleSRID: false,
                    emitZ,
                    emitM);
                return writer.Write(geometry);
            }
            catch (ParseException ex)
            {
                throw new FormatException(ex.Message, ex);
            }
            catch (ArgumentException ex)
            {
                throw new FormatException(ex.Message, ex);
            }
        }

        /// <summary>
        /// Embeds a row's sibling SRID in the outer WKB header. Child geometries
        /// inherit the outer SRID, so collection payloads and ISO Z/M/ZM type codes
        /// remain unchanged.
        /// </summary>
        internal static byte[] ToEwkb(ReadOnlySpan<byte> wkb, int srid)
        {
            if (wkb.Length < 5)
                throw new FormatException("Malformed WKB: geometry header is truncated");

            bool littleEndian;
            switch (wkb[0])
            {
                case 0:
                    littleEndian = false;
                    break;
                case 1:
                    littleEndian = true;
                    break;
                default:
                    throw new FormatException($"Malformed WKB: invalid byte order {wkb[0]}");
            }

            uint geometryType = ReadUInt32(wkb.Slice(1, 4), littleEndian);
            if ((geometryType & EwkbSridFlag) != 0)
                throw new FormatException("Expected OGC WKB without an embedded SRID");

            var ewkb = new byte[wkb.Length + 4];
            ewkb[0] = wkb[0];
            WriteUInt32(ewkb.AsSpan(1, 4), geometryType | EwkbSridFlag, littleEndian);
            WriteUInt32(ewkb.AsSpan(5, 4), unchecked((uint)srid), littleEndian);
            wkb.Slice(5).CopyTo(ewkb.AsSpan(9));
            return ewkb;
        }

        private static uint ReadUInt32(ReadOnlySpan<byte> value, bool littleEndian)
        {
            if (littleEndian)
            {
                return value[0]
                    | ((uint)value[1] << 8)
                    | ((uint)value[2] << 16)
                    | ((uint)value[3] << 24);
            }
            return ((uint)value[0] << 24)
                | ((uint)value[1] << 16)
                | ((uint)value[2] << 8)
                | value[3];
        }

        private static void WriteUInt32(Span<byte> output, uint value, bool littleEndian)
        {
            for (int i = 0; i < 4; i++)
            {
                int shift = littleEndian ? i * 8 : (3 - i) * 8;
                output[i] = (byte)(value >> shift);
            }
        }

        private static string Compact(string wkt) =>
            wkt.Replace("POINT (", "POINT(")
                .Replace("LINESTRING (", "LINESTRING(")
                .Replace("POLYGON (", "POLYGON(")
                .Replace("MULTIPOINT (", "MULTIPOINT(")
                .Replace("MULTILINESTRING (", "MULTILINESTRING(")
                .Replace("MULTIPOLYGON (", "MULTIPOLYGON(")
                .Replace("GEOMETRYCOLLECTION (", "GEOMETRYCOLLECTION(")
                .Replace(", ", ",")
                .Replace(" ZM(", " ZM (")
                .Replace(" Z(", " Z (")
                .Replace(" M(", " M (");
    }
}
