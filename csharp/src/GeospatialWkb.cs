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
