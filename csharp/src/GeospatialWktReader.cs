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
using System.IO;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Reads the seven OGC WKT geometry types directly into little-endian ISO WKB.
    /// Each geometry retains its Z/M marker, including empty collection members.
    /// SRIDs are handled by the caller because they are not part of ISO WKB.
    /// </summary>
    internal sealed class GeospatialWktReader
    {
        private const int MaxDepth = 128;

        private readonly string _input;
        private readonly BinaryWriter _output;
        private int _offset;

        private GeospatialWktReader(string input, BinaryWriter output)
        {
            _input = input;
            _output = output;
        }

        internal static byte[] Read(string wkt)
        {
            using var stream = new MemoryStream();
            using var writer = new BinaryWriter(stream);
            var reader = new GeospatialWktReader(wkt, writer);
            reader.ReadGeometry(inheritedDimension: 0, depth: 0);
            reader.SkipWhitespace();
            if (reader._offset != wkt.Length)
                throw reader.Error("unexpected trailing content");
            return stream.ToArray();
        }

        private void ReadGeometry(uint inheritedDimension, int depth)
        {
            if (depth >= MaxDepth)
                throw Error("geometry collection nesting is too deep");

            string name = ReadWord();
            uint type = name switch
            {
                "POINT" => 1,
                "LINESTRING" => 2,
                "POLYGON" => 3,
                "MULTIPOINT" => 4,
                "MULTILINESTRING" => 5,
                "MULTIPOLYGON" => 6,
                "GEOMETRYCOLLECTION" => 7,
                _ => throw Error($"unsupported geometry type '{name}'"),
            };

            uint dimension = inheritedDimension;
            if (TryReadWord("ZM"))
                dimension = 3000;
            else if (TryReadWord("Z"))
                dimension = 1000;
            else if (TryReadWord("M"))
                dimension = 2000;

            WriteHeader(type, dimension);
            ReadBody(type, dimension, depth);
        }

        private void WriteHeader(uint type, uint dimension)
        {
            _output.Write((byte)1);
            _output.Write(type + dimension);
        }

        private void ReadBody(uint type, uint dimension, int depth)
        {
            if (type == 1)
            {
                ReadPoint(dimension, allowUnwrapped: false);
                return;
            }

            if (TryReadWord("EMPTY"))
            {
                _output.Write(0u);
                return;
            }

            if (type == 2)
            {
                ReadSequence(dimension, ring: false);
                return;
            }

            Require('(');
            long countPosition = _output.BaseStream.Position;
            _output.Write(0u);
            uint count = 0;
            do
            {
                if (type == 3)
                {
                    ReadSequence(dimension, ring: true);
                }
                else if (type == 7)
                {
                    // An explicit child marker takes precedence over the collection
                    // marker, so mixed-dimensional collections do not lose ordinates.
                    ReadGeometry(dimension, depth + 1);
                }
                else
                {
                    uint childType = type - 3;
                    WriteHeader(childType, dimension);
                    if (childType == 1)
                        ReadPoint(dimension, allowUnwrapped: true);
                    else
                        ReadBody(childType, dimension, depth + 1);
                }
                count++;
            }
            while (TryRead(','));
            Require(')');
            WriteCount(countPosition, count);
        }

        private void ReadPoint(uint dimension, bool allowUnwrapped)
        {
            if (TryReadWord("EMPTY"))
            {
                for (int i = 0; i < Dimensions(dimension); i++)
                    _output.Write(double.NaN);
                return;
            }

            // Both MULTIPOINT(1 2,3 4) and MULTIPOINT((1 2),(3 4)) are WKT.
            bool wrapped = TryRead('(');
            if (!wrapped && !allowUnwrapped)
                throw Error("expected '(' or EMPTY for point");
            ReadCoordinate(dimension, out _, out _);
            if (wrapped)
                Require(')');
        }

        private void ReadSequence(uint dimension, bool ring)
        {
            Require('(');
            long countPosition = _output.BaseStream.Position;
            _output.Write(0u);
            uint count = 0;
            double firstX = 0;
            double firstY = 0;
            double lastX;
            double lastY;
            do
            {
                ReadCoordinate(dimension, out lastX, out lastY);
                if (count == 0)
                {
                    firstX = lastX;
                    firstY = lastY;
                }
                count++;
            }
            while (TryRead(','));
            Require(')');

            if (count < (ring ? 4u : 2u))
                throw Error(ring ? "polygon ring requires at least four points" : "linestring requires at least two points");
            if (ring && (firstX != lastX || firstY != lastY))
                throw Error("polygon ring is not closed");
            WriteCount(countPosition, count);
        }

        private void WriteCount(long position, uint count)
        {
            long end = _output.BaseStream.Position;
            _output.BaseStream.Position = position;
            _output.Write(count);
            _output.BaseStream.Position = end;
        }

        private void ReadCoordinate(uint dimension, out double x, out double y)
        {
            x = ReadNumber();
            y = ReadNumber();
            _output.Write(x);
            _output.Write(y);
            for (int i = 2; i < Dimensions(dimension); i++)
                _output.Write(ReadNumber());
        }

        private static int Dimensions(uint dimension)
        {
            return dimension == 0 ? 2 : dimension == 3000 ? 4 : 3;
        }

        private double ReadNumber()
        {
            SkipWhitespace();
            int start = _offset;
            while (_offset < _input.Length
                && !char.IsWhiteSpace(_input[_offset])
                && _input[_offset] != ',' && _input[_offset] != '(' && _input[_offset] != ')')
            {
                _offset++;
            }
            string token = _input.Substring(start, _offset - start);
            if (token.Equals("NaN", StringComparison.OrdinalIgnoreCase))
                return double.NaN;
            if (token.Equals("Inf", StringComparison.OrdinalIgnoreCase)
                || token.Equals("+Inf", StringComparison.OrdinalIgnoreCase)
                || token.Equals("Infinity", StringComparison.OrdinalIgnoreCase)
                || token.Equals("+Infinity", StringComparison.OrdinalIgnoreCase))
            {
                return double.PositiveInfinity;
            }
            if (token.Equals("-Inf", StringComparison.OrdinalIgnoreCase)
                || token.Equals("-Infinity", StringComparison.OrdinalIgnoreCase))
            {
                return double.NegativeInfinity;
            }
            if (!double.TryParse(token, NumberStyles.Float, CultureInfo.InvariantCulture, out double value)
                || double.IsInfinity(value))
            {
                throw Error($"invalid coordinate '{token}'");
            }
            return value;
        }

        private string ReadWord()
        {
            SkipWhitespace();
            int start = _offset;
            while (_offset < _input.Length && char.IsLetter(_input[_offset]))
                _offset++;
            return _input.Substring(start, _offset - start).ToUpperInvariant();
        }

        private bool TryReadWord(string word)
        {
            int start = _offset;
            if (ReadWord() == word)
                return true;
            _offset = start;
            return false;
        }

        private bool TryRead(char value)
        {
            SkipWhitespace();
            if (_offset == _input.Length || _input[_offset] != value)
                return false;
            _offset++;
            return true;
        }

        private void Require(char value)
        {
            if (!TryRead(value))
                throw Error($"expected '{value}'");
        }

        private void SkipWhitespace()
        {
            while (_offset < _input.Length && char.IsWhiteSpace(_input[_offset]))
                _offset++;
        }

        private FormatException Error(string message)
        {
            return new FormatException($"Invalid WKT at character {_offset}: {message}");
        }
    }
}
