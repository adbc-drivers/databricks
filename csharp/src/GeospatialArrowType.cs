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
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Types;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Builds and recognizes the canonical Arrow representation used by Reyden for
    /// Databricks GEOMETRY and GEOGRAPHY values.
    /// </summary>
    internal static class GeospatialArrowType
    {
        internal const string GeometryMetadataKey = "geometry";
        internal const string GeographyMetadataKey = "geography";
        internal const string SridMetadataKey = "srid";

        private static readonly Regex s_typePattern = new Regex(
            @"^\s*(GEOMETRY|GEOGRAPHY)\s*(?:\(\s*(ANY|-?\d+)\s*\))?\s*$",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

        internal enum Family
        {
            Geometry,
            Geography,
        }

        internal readonly struct Tag : IEquatable<Tag>
        {
            internal Tag(Family family, int srid)
            {
                Family = family;
                Srid = srid;
            }

            internal Family Family { get; }

            internal int Srid { get; }

            public bool Equals(Tag other) => Family == other.Family && Srid == other.Srid;

            public override bool Equals(object? obj) => obj is Tag other && Equals(other);

            public override int GetHashCode() => ((int)Family * 397) ^ Srid;
        }

        internal static bool TryCreate(string typeText, out StructType type)
        {
            type = null!;
            Match match = s_typePattern.Match(typeText ?? string.Empty);
            if (!match.Success)
                return false;

            Family family = match.Groups[1].Value.Equals(
                "GEOGRAPHY",
                StringComparison.OrdinalIgnoreCase)
                ? Family.Geography
                : Family.Geometry;
            if (!TryParseTypeSrid(match, family, out int srid))
                return false;

            type = Create(new Tag(family, srid));
            return true;
        }

        internal static StructType Create(Tag tag)
        {
            var metadata = new Dictionary<string, string>
            {
                [tag.Family == Family.Geometry ? GeometryMetadataKey : GeographyMetadataKey] = "true",
                [SridMetadataKey] = tag.Srid.ToString(CultureInfo.InvariantCulture),
            };
            return new StructType(new[]
            {
                new Field("srid", Int32Type.Default, nullable: false),
                new Field("wkb", BinaryType.Default, nullable: false, metadata),
            });
        }

        internal static bool IsStructShape(IArrowType type)
        {
            if (!(type is StructType structType) || structType.Fields.Count != 2)
                return false;

            return structType.Fields[0].Name == "srid"
                && structType.Fields[0].DataType.TypeId == ArrowTypeId.Int32
                && structType.Fields[1].Name == "wkb"
                && structType.Fields[1].DataType.TypeId == ArrowTypeId.Binary;
        }

        /// <summary>
        /// Returns the canonical geo tag when present. An exact untagged
        /// struct&lt;srid,wkb&gt; returns false. Partial or conflicting tags are invalid.
        /// </summary>
        internal static bool TryGetTag(IArrowType type, out Tag tag)
        {
            tag = default;
            if (!IsStructShape(type))
                return false;

            StructType structType = (StructType)type;
            IReadOnlyDictionary<string, string>? metadata = structType.Fields[1].Metadata;
            bool hasGeometry = metadata?.ContainsKey(GeometryMetadataKey) == true;
            bool hasGeography = metadata?.ContainsKey(GeographyMetadataKey) == true;
            bool geometry = MetadataIsTrue(metadata, GeometryMetadataKey);
            bool geography = MetadataIsTrue(metadata, GeographyMetadataKey);
            bool hasMetadata = hasGeometry
                || hasGeography
                || metadata?.ContainsKey(SridMetadataKey) == true;
            if (!hasMetadata)
                return false;

            if ((hasGeometry && !geometry) || (hasGeography && !geography))
            {
                throw InvalidTag("geometry and geography metadata values must be true");
            }

            if (geometry == geography)
            {
                throw InvalidTag(
                    "expected exactly one of geometry=true or geography=true");
            }

            if (metadata == null
                || !metadata.TryGetValue(SridMetadataKey, out string? sridText)
                || !int.TryParse(
                    sridText,
                    NumberStyles.Integer,
                    CultureInfo.InvariantCulture,
                    out int srid))
            {
                throw InvalidTag("missing or invalid type-level srid");
            }

            tag = new Tag(geometry ? Family.Geometry : Family.Geography, srid);
            return true;
        }

        private static bool TryParseTypeSrid(
            Match match,
            Family family,
            out int srid)
        {
            if (!match.Groups[2].Success)
            {
                // Bare SQL types use their Databricks defaults. ANY is distinct
                // and uses Reyden's -1 type-level sentinel.
                srid = family == Family.Geography ? 4326 : 0;
                return true;
            }

            if (match.Groups[2].Value.Equals("ANY", StringComparison.OrdinalIgnoreCase))
            {
                srid = -1;
                return true;
            }

            return int.TryParse(
                match.Groups[2].Value,
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out srid);
        }

        private static bool MetadataIsTrue(
            IReadOnlyDictionary<string, string>? metadata,
            string key) =>
            metadata != null
            && metadata.TryGetValue(key, out string? value)
            && value.Equals("true", StringComparison.OrdinalIgnoreCase);

        private static DatabricksException InvalidTag(string detail) =>
            new DatabricksException(
                $"Malformed geospatial Arrow tag: {detail}",
                AdbcStatusCode.InvalidData);
    }
}
