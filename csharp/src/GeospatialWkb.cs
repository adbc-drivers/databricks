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
            return new WkbWktWriter(wkb).Write();
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
            byte[] output = new byte[checked(wkb.Length + sizeof(uint))];
            new WkbToEwkbRewriter(wkb, output).Rewrite(srid);
            return output;
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

        // Render directly from the validated WKB structure. This retains explicit
        // dimensions on empty members without allocating an NTS Geometry tree.
        private ref struct WkbWktWriter
        {
            private static readonly OrdinateFormat s_ordinateFormat = new OrdinateFormat(16);

            private readonly ReadOnlySpan<byte> _input;
            private readonly StringBuilder _output;
            private int _offset;

            internal WkbWktWriter(ReadOnlySpan<byte> input)
            {
                _input = input;
                _output = new StringBuilder();
                _offset = 0;
            }

            internal string Write()
            {
                WriteGeometry(tagged: true);
                if (_offset != _input.Length)
                {
                    throw new FormatException(
                        $"Malformed WKB: {_input.Length - _offset} trailing bytes");
                }
                return _output.ToString();
            }

            private uint WriteGeometry(bool tagged)
            {
                if (_input.Length - _offset < 5)
                    throw new FormatException("Malformed WKB: geometry header is truncated");

                byte byteOrder = _input[_offset++];
                if (byteOrder > 1)
                    throw new FormatException($"Malformed WKB: invalid byte order {byteOrder}");
                bool littleEndian = byteOrder == 1;
                uint rawType = ReadUInt32(littleEndian);
                WkbType isoType = DecodeIsoWkbType(
                    rawType & ~(EwkbZFlag | EwkbMFlag | EwkbSridFlag));
                uint flags = isoType.EwkbFlags | (rawType & (EwkbZFlag | EwkbMFlag));
                bool hasZ = (flags & EwkbZFlag) != 0;
                bool hasM = (flags & EwkbMFlag) != 0;
                var type = new WkbType(isoType.BaseType, 2 + (hasZ ? 1 : 0) + (hasM ? 1 : 0), flags);
                if ((rawType & EwkbSridFlag) != 0)
                    ReadUInt32(littleEndian);

                switch (type.BaseType)
                {
                    case 1: // POINT
                        {
                            double x = ReadDouble(littleEndian);
                            double y = ReadDouble(littleEndian);
                            double z = hasZ ? ReadDouble(littleEndian) : 0;
                            double m = hasM ? ReadDouble(littleEndian) : 0;
                            bool empty = double.IsNaN(x) && double.IsNaN(y);
                            WriteTag(type, tagged, empty);
                            if (empty)
                            {
                                _output.Append("EMPTY");
                            }
                            else
                            {
                                _output.Append('(');
                                WriteCoordinate(x, y, z, m, hasZ, hasM);
                                _output.Append(')');
                            }
                            break;
                        }
                    case 2: // LINESTRING
                        {
                            uint points = ReadUInt32(littleEndian);
                            if (points == 1)
                            {
                                throw new FormatException(
                                    "Malformed WKB: linestring must contain zero or at least two points");
                            }
                            WriteTag(type, tagged, points == 0);
                            WriteSequenceBody(points, littleEndian, hasZ, hasM);
                            break;
                        }
                    case 3: // POLYGON
                        {
                            uint rings = ReadUInt32(littleEndian);
                            WriteTag(type, tagged, rings == 0);
                            if (rings == 0)
                            {
                                _output.Append("EMPTY");
                                break;
                            }
                            _output.Append('(');
                            for (uint ring = 0; ring < rings; ring++)
                            {
                                if (ring != 0)
                                    _output.Append(',');
                                uint points = ReadUInt32(littleEndian);
                                if (points < 4)
                                {
                                    throw new FormatException(
                                        "Malformed WKB: polygon ring must contain at least four points");
                                }
                                WriteSequenceBody(
                                    points,
                                    littleEndian,
                                    hasZ,
                                    hasM,
                                    requireClosed: true);
                            }
                            _output.Append(')');
                            break;
                        }
                    case 4: // MULTIPOINT
                    case 5: // MULTILINESTRING
                    case 6: // MULTIPOLYGON
                    case 7: // GEOMETRYCOLLECTION
                        {
                            uint children = ReadUInt32(littleEndian);
                            WriteTag(type, tagged, children == 0);
                            if (children == 0)
                            {
                                _output.Append("EMPTY");
                                break;
                            }
                            _output.Append('(');
                            for (uint child = 0; child < children; child++)
                            {
                                if (child != 0)
                                    _output.Append(',');
                                uint childType = WriteGeometry(tagged: type.BaseType == 7);
                                if (type.BaseType != 7 && childType != type.BaseType - 3)
                                {
                                    throw new FormatException(
                                        $"Malformed WKB: geometry type {type.BaseType} "
                                        + $"contains child type {childType}");
                                }
                            }
                            _output.Append(')');
                            break;
                        }
                    default:
                        throw new FormatException("Unsupported WKB geometry type");
                }

                return type.BaseType;
            }

            private void WriteTag(WkbType type, bool tagged, bool empty)
            {
                if (!tagged)
                    return;

                _output.Append(TypeName(type.BaseType));
                bool hasZ = (type.EwkbFlags & EwkbZFlag) != 0;
                bool hasM = (type.EwkbFlags & EwkbMFlag) != 0;
                _output.Append(hasZ ? (hasM ? " ZM" : " Z") : (hasM ? " M" : ""));
                if (hasZ || hasM || empty)
                    _output.Append(' ');
            }

            private void WriteSequenceBody(
                uint points,
                bool littleEndian,
                bool hasZ,
                bool hasM,
                bool requireClosed = false)
            {
                if (points == 0)
                {
                    _output.Append("EMPTY");
                    return;
                }
                _output.Append('(');
                double firstX = 0;
                double firstY = 0;
                double lastX = 0;
                double lastY = 0;
                for (uint i = 0; i < points; i++)
                {
                    if (i != 0)
                        _output.Append(',');
                    double x = ReadDouble(littleEndian);
                    double y = ReadDouble(littleEndian);
                    double z = hasZ ? ReadDouble(littleEndian) : 0;
                    double m = hasM ? ReadDouble(littleEndian) : 0;
                    if (i == 0)
                    {
                        firstX = x;
                        firstY = y;
                    }
                    lastX = x;
                    lastY = y;
                    WriteCoordinate(x, y, z, m, hasZ, hasM);
                }
                _output.Append(')');
                if (requireClosed && (firstX != lastX || firstY != lastY))
                    throw new FormatException("Malformed WKB: polygon ring is not closed");
            }

            private void WriteCoordinate(
                double x,
                double y,
                double z,
                double m,
                bool hasZ,
                bool hasM)
            {
                _output.Append(s_ordinateFormat.Format(x));
                _output.Append(' ').Append(s_ordinateFormat.Format(y));
                if (hasZ)
                    _output.Append(' ').Append(s_ordinateFormat.Format(z));
                if (hasM)
                    _output.Append(' ').Append(s_ordinateFormat.Format(m));
            }

            private uint ReadUInt32(bool littleEndian)
            {
                if (_input.Length - _offset < 4)
                    throw new FormatException("Malformed WKB: truncated uint32");
                uint value = GeospatialWkb.ReadUInt32(
                    _input.Slice(_offset, 4), littleEndian);
                _offset += 4;
                return value;
            }

            private double ReadDouble(bool littleEndian)
            {
                if (_input.Length - _offset < sizeof(double))
                    throw new FormatException("Malformed WKB: truncated coordinate");
                ulong bits = GeospatialWkb.ReadUInt64(
                    _input.Slice(_offset, sizeof(double)),
                    littleEndian);
                _offset += sizeof(double);
                return BitConverter.Int64BitsToDouble(unchecked((long)bits));
            }

            private static string TypeName(uint baseType)
            {
                switch (baseType)
                {
                    case 1: return "POINT";
                    case 2: return "LINESTRING";
                    case 3: return "POLYGON";
                    case 4: return "MULTIPOINT";
                    case 5: return "MULTILINESTRING";
                    case 6: return "MULTIPOLYGON";
                    case 7: return "GEOMETRYCOLLECTION";
                    default: throw new FormatException("Unsupported WKB geometry type");
                }
            }
        }

        private ref struct WkbToEwkbRewriter
        {
            private readonly ReadOnlySpan<byte> _input;
            private readonly Span<byte> _output;
            private int _offset;
            private int _outputOffset;

            internal WkbToEwkbRewriter(ReadOnlySpan<byte> input, Span<byte> output)
            {
                _input = input;
                _output = output;
                _offset = 0;
                _outputOffset = 0;
            }

            internal void Rewrite(int srid)
            {
                RewriteGeometry(includeSrid: true, srid);
                if (_offset != _input.Length)
                {
                    throw new FormatException(
                        $"Malformed WKB: {_input.Length - _offset} trailing bytes");
                }
                if (_outputOffset != _output.Length)
                    throw new InvalidOperationException("Internal EWKB output length mismatch");
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
                _output[_outputOffset++] = byteOrder;
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
                    _input.Slice(_offset, 4),
                    littleEndian);
                _offset += 4;
                return value;
            }

            private void AppendUInt32(uint value, bool littleEndian)
            {
                WriteUInt32(_output.Slice(_outputOffset, sizeof(uint)), value, littleEndian);
                _outputOffset += sizeof(uint);
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
                _input.Slice(_offset, byteCount).CopyTo(
                    _output.Slice(_outputOffset, byteCount));
                _offset += byteCount;
                _outputOffset += byteCount;
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

        private static ulong ReadUInt64(ReadOnlySpan<byte> value, bool littleEndian)
        {
            ulong result = 0;
            for (int i = 0; i < sizeof(ulong); i++)
            {
                int shift = littleEndian ? i * 8 : (7 - i) * 8;
                result |= (ulong)value[i] << shift;
            }
            return result;
        }

        private static void WriteUInt32(Span<byte> output, uint value, bool littleEndian)
        {
            for (int i = 0; i < 4; i++)
            {
                int shift = littleEndian ? i * 8 : (3 - i) * 8;
                output[i] = (byte)(value >> shift);
            }
        }
    }
}
