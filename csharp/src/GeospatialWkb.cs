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
        private const uint EwkbZFlag = 0x80000000;
        private const uint EwkbMFlag = 0x40000000;
        private const uint EwkbSridFlag = 0x20000000;
        private const uint EwkbBboxFlag = 0x10000000;

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
        /// Converts ISO/OGC WKB dimensional type offsets to EWKB flags at every
        /// geometry header and embeds the row's sibling SRID in the outer header.
        /// </summary>
        internal static byte[] ToEwkb(ReadOnlySpan<byte> wkb, int srid)
        {
            return new WkbToEwkbRewriter(wkb.ToArray()).Rewrite(srid);
        }

        private readonly struct WkbType
        {
            internal WkbType(uint baseType, int dimensions, uint ewkbFlags)
            {
                BaseType = baseType;
                Dimensions = dimensions;
                EwkbFlags = ewkbFlags;
            }

            internal uint BaseType { get; }

            internal int Dimensions { get; }

            internal uint EwkbFlags { get; }
        }

        private sealed class WkbToEwkbRewriter
        {
            private readonly byte[] _input;
            private readonly List<byte> _output;
            private int _offset;

            internal WkbToEwkbRewriter(byte[] input)
            {
                _input = input;
                _output = new List<byte>(input.Length + 4);
            }

            internal byte[] Rewrite(int srid)
            {
                RewriteGeometry(includeSrid: true, srid);
                if (_offset != _input.Length)
                {
                    throw new FormatException(
                        $"Malformed WKB: {_input.Length - _offset} trailing bytes");
                }
                return _output.ToArray();
            }

            private uint RewriteGeometry(bool includeSrid, int srid)
            {
                if (_input.Length - _offset < 5)
                    throw new FormatException("Malformed WKB: geometry header is truncated");

                byte byteOrder = _input[_offset++];
                bool littleEndian;
                switch (byteOrder)
                {
                    case 0:
                        littleEndian = false;
                        break;
                    case 1:
                        littleEndian = true;
                        break;
                    default:
                        throw new FormatException(
                            $"Malformed WKB: invalid byte order {byteOrder}");
                }

                WkbType type = DecodeIsoWkbType(ReadUInt32(littleEndian));
                _output.Add(byteOrder);
                uint outputType = type.BaseType | type.EwkbFlags;
                if (includeSrid)
                    outputType |= EwkbSridFlag;
                AppendUInt32(outputType, littleEndian);
                if (includeSrid)
                    AppendUInt32(unchecked((uint)srid), littleEndian);

                switch (type.BaseType)
                {
                    case 1: // POINT
                        CopyBytes((ulong)type.Dimensions * sizeof(double));
                        break;
                    case 2: // LINESTRING
                        CopyCoordinateSequence(littleEndian, type.Dimensions);
                        break;
                    case 3: // POLYGON
                        {
                            uint rings = CopyUInt32(littleEndian);
                            for (uint ring = 0; ring < rings; ring++)
                                CopyCoordinateSequence(littleEndian, type.Dimensions);
                            break;
                        }
                    case 4: // MULTIPOINT
                    case 5: // MULTILINESTRING
                    case 6: // MULTIPOLYGON
                    case 7: // GEOMETRYCOLLECTION
                        {
                            uint children = CopyUInt32(littleEndian);
                            for (uint child = 0; child < children; child++)
                            {
                                uint childType = RewriteGeometry(includeSrid: false, srid: 0);
                                if (type.BaseType != 7 && childType != type.BaseType - 3)
                                {
                                    throw new FormatException(
                                        $"Malformed WKB: geometry type {type.BaseType} "
                                        + $"contains child type {childType}");
                                }
                            }
                            break;
                        }
                    default:
                        throw new InvalidOperationException(
                            "Decoded an unsupported WKB geometry type");
                }

                return type.BaseType;
            }

            private uint ReadUInt32(bool littleEndian)
            {
                if (_input.Length - _offset < 4)
                    throw new FormatException("Malformed WKB: truncated uint32");
                uint value = GeospatialWkb.ReadUInt32(
                    new ReadOnlySpan<byte>(_input, _offset, 4),
                    littleEndian);
                _offset += 4;
                return value;
            }

            private void AppendUInt32(uint value, bool littleEndian)
            {
                var encoded = new byte[4];
                WriteUInt32(encoded, value, littleEndian);
                _output.AddRange(encoded);
            }

            private uint CopyUInt32(bool littleEndian)
            {
                uint value = ReadUInt32(littleEndian);
                AppendUInt32(value, littleEndian);
                return value;
            }

            private void CopyCoordinateSequence(bool littleEndian, int dimensions)
            {
                uint points = CopyUInt32(littleEndian);
                CopyBytes((ulong)points * (ulong)dimensions * sizeof(double));
            }

            private void CopyBytes(ulong count)
            {
                int remaining = _input.Length - _offset;
                if (count > (ulong)remaining)
                    throw new FormatException("Malformed WKB: payload is truncated");

                int byteCount = (int)count;
                for (int i = 0; i < byteCount; i++)
                    _output.Add(_input[_offset + i]);
                _offset += byteCount;
            }
        }

        private static WkbType DecodeIsoWkbType(uint rawType)
        {
            if ((rawType & (EwkbZFlag | EwkbMFlag | EwkbSridFlag | EwkbBboxFlag)) != 0)
            {
                throw new FormatException(
                    $"Malformed WKB: expected ISO/OGC type code, got EWKB flags in 0x{rawType:x8}");
            }

            uint baseType;
            int dimensions;
            uint flags;
            if (rawType >= 3000 && rawType < 4000)
            {
                baseType = rawType - 3000;
                dimensions = 4;
                flags = EwkbZFlag | EwkbMFlag;
            }
            else if (rawType >= 2000 && rawType < 3000)
            {
                baseType = rawType - 2000;
                dimensions = 3;
                flags = EwkbMFlag;
            }
            else if (rawType >= 1000 && rawType < 2000)
            {
                baseType = rawType - 1000;
                dimensions = 3;
                flags = EwkbZFlag;
            }
            else
            {
                baseType = rawType;
                dimensions = 2;
                flags = 0;
            }

            if (baseType < 1 || baseType > 7)
            {
                throw new FormatException(
                    $"Malformed WKB: unsupported geometry type {rawType}");
            }
            return new WkbType(baseType, dimensions, flags);
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
