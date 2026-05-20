/*
* Copyright (c) 2025 ADBC Drivers Contributors
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
using Apache.Arrow;
using Apache.Arrow.Types;
using AdbcDrivers.Databricks.StatementExecution;
using AdbcDrivers.HiveServer2.Hive2;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Parses Databricks SQL type strings (as they appear in manifest <c>type_text</c>)
    /// into native Apache Arrow types.
    ///
    /// <para>
    /// Used by the SEA manifest-schema mapper when
    /// <c>EnableComplexDatatypeSupport=true</c> so the exposed schema reports native
    /// <see cref="ListType"/> / <see cref="MapType"/> / <see cref="StructType"/>
    /// matching the unwrapped batches. The inverse direction — converting native
    /// Arrow complex arrays to JSON strings — is handled by
    /// <see cref="ComplexTypeSerializingStream"/>.
    /// </para>
    ///
    /// <para>
    /// Grammar (per Databricks SQL docs):
    /// <list type="bullet">
    ///   <item><description><c>ARRAY &lt; elementType &gt;</c></description></item>
    ///   <item><description><c>MAP &lt; keyType, valueType &gt;</c></description></item>
    ///   <item><description><c>STRUCT &lt; [fieldName [:] fieldType [NOT NULL] [COLLATE collationName] [COMMENT str] [, ...]] &gt;</c></description></item>
    /// </list>
    /// </para>
    /// </summary>
    internal static class ArrowTypeParser
    {
        /// <summary>
        /// Maps a Databricks SQL type name from the SEA manifest to its Arrow output
        /// type. The behavior for ARRAY / MAP / STRUCT depends on
        /// <paramref name="enableComplexDatatypeSupport"/>:
        /// <list type="bullet">
        ///   <item><description><c>false</c>: <see cref="StringType"/> (paired with
        ///   <see cref="ComplexTypeSerializingStream"/> that serializes the native
        ///   arrays to JSON strings).</description></item>
        ///   <item><description><c>true</c>: native nested Arrow types parsed from
        ///   the manifest's <c>type_text</c> (see <see cref="ParseComplexType"/>).</description></item>
        /// </list>
        /// <para>
        /// <paramref name="dataTypeConversion"/> controls how the conversion-sensitive
        /// scalar types — DATE / DECIMAL / TIMESTAMP / FLOAT — are surfaced, mirroring
        /// <see cref="HiveServer2SchemaParser.GetArrowType"/>:
        /// <list type="bullet">
        ///   <item><description><c>scalar</c> (default): DATE→Date32, DECIMAL→Decimal128,
        ///   TIMESTAMP→Timestamp, FLOAT→Float (native Arrow types).</description></item>
        ///   <item><description><c>none</c>: DATE/DECIMAL/TIMESTAMP→String,
        ///   FLOAT→Double (widening). Paired with <see cref="ScalarConversionStream"/>
        ///   which converts the native arrays to match.</description></item>
        /// </list>
        /// Other primitives (BOOLEAN/INT/BIGINT/STRING/BINARY/INTERVAL/NULL) ignore the flag.
        /// </para>
        /// </summary>
        internal static IArrowType MapToArrowType(string typeText, bool enableComplexDatatypeSupport, DataTypeConversion dataTypeConversion)
        {
            var baseType = ColumnMetadataHelper.GetBaseTypeName(typeText).ToUpperInvariant();
            if (baseType is "ARRAY" or "MAP" or "STRUCT")
            {
                return enableComplexDatatypeSupport
                    ? ParseComplexType(typeText)
                    : StringType.Default;
            }
            return MapPrimitiveType(typeText, dataTypeConversion);
        }

        /// <summary>
        /// Backward-compatible overload that defaults to <see cref="DataTypeConversion.Scalar"/>.
        /// Existing callers (e.g. unit tests that pre-date PECO-3060) keep their current behaviour.
        /// </summary>
        internal static IArrowType MapToArrowType(string typeText, bool enableComplexDatatypeSupport)
            => MapToArrowType(typeText, enableComplexDatatypeSupport, DataTypeConversion.Scalar);

        /// <summary>
        /// Parses <paramref name="typeText"/> into a native Arrow type. Returns
        /// <see cref="StringType"/> on any parse failure — callers can rely on this,
        /// the method never throws. Exposed for tests; production callers should use
        /// <see cref="MapToArrowType"/> which handles the user flag.
        /// </summary>
        internal static IArrowType ParseComplexType(string typeText)
        {
            if (string.IsNullOrWhiteSpace(typeText)) return StringType.Default;
            try { return ParseSqlType(typeText.Trim()); }
            catch (FormatException) { return StringType.Default; }
        }

        /// <summary>
        /// Maps a primitive (non-complex) Databricks SQL type name to its Arrow type.
        /// Used by <see cref="MapToArrowType"/> for top-level columns and by
        /// <see cref="ParseComplexType"/> for primitive leaves inside ARRAY/MAP/STRUCT.
        /// </summary>
        /// <param name="typeText">The manifest type text (may include parameters like <c>DECIMAL(10,2)</c>).</param>
        /// <param name="dataTypeConversion">Controls DATE/DECIMAL/TIMESTAMP/FLOAT handling — see <see cref="MapToArrowType(string, bool, DataTypeConversion)"/>.</param>
        private static IArrowType MapPrimitiveType(string typeText, DataTypeConversion dataTypeConversion)
        {
            bool convertScalar = dataTypeConversion.HasFlag(DataTypeConversion.Scalar);
            var baseType = ColumnMetadataHelper.GetBaseTypeName(typeText).ToUpperInvariant();
            return baseType switch
            {
                "BOOLEAN" => BooleanType.Default,
                "BYTE" or "TINYINT" => Int8Type.Default,
                "SHORT" or "SMALLINT" => Int16Type.Default,
                "INT" or "INTEGER" => Int32Type.Default,
                "LONG" or "BIGINT" => Int64Type.Default,
                // FLOAT: scalar→Float (native), none→Double (widening), matching HiveServer2SchemaParser.
                "FLOAT" or "REAL" => convertScalar ? FloatType.Default : DoubleType.Default,
                "DOUBLE" => DoubleType.Default,
                // DECIMAL: scalar→Decimal128, none→String, matching HiveServer2SchemaParser.
                "DECIMAL" or "NUMERIC" => convertScalar ? ParseDecimalType(typeText) : StringType.Default,
                "STRING" or "VARCHAR" or "CHAR" => StringType.Default,
                "BINARY" or "VARBINARY" => BinaryType.Default,
                // DATE: scalar→Date32, none→String, matching HiveServer2SchemaParser.
                "DATE" => convertScalar ? Date32Type.Default : StringType.Default,
                // TIMESTAMP: scalar→Timestamp, none→String, matching HiveServer2SchemaParser.
                "TIMESTAMP" or "TIMESTAMP_NTZ" or "TIMESTAMP_LTZ" => convertScalar ? TimestampType.Default : StringType.Default,
                // INTERVAL is converted to string by IntervalSerializingStream; StringType is the output contract.
                "INTERVAL" => StringType.Default,
                "NULL" or "VOID" => NullType.Default,
                _ => StringType.Default,
            };
        }

        /// <summary>
        /// Backward-compatible overload defaulting to <see cref="DataTypeConversion.Scalar"/>
        /// for the recursive complex-type parser, which always uses native scalar mapping
        /// for leaves regardless of the user's flag (Thrift behaves the same — the flag
        /// only governs the schema's top-level type for the affected scalars).
        /// </summary>
        private static IArrowType MapPrimitiveType(string typeText)
            => MapPrimitiveType(typeText, DataTypeConversion.Scalar);

        private static IArrowType ParseDecimalType(string typeText)
        {
            int precision = 38;
            int scale = 18;

            var match = System.Text.RegularExpressions.Regex.Match(
                typeText,
                @"DECIMAL\((\d+),\s*(\d+)\)",
                System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            if (match.Success)
            {
                precision = int.Parse(match.Groups[1].Value);
                scale = int.Parse(match.Groups[2].Value);
            }
            return new Decimal128Type(precision, scale);
        }

        private static IArrowType ParseSqlType(string s)
        {
            s = s.Trim();
            int lt = s.IndexOf('<');
            if (lt < 0) return MapPrimitiveType(s);
            // The matching '>' must be the last character; reject trailing junk.
            if (!s.EndsWith(">") || FindMatching(s, lt) != s.Length - 1) throw new FormatException();
            string baseName = s.Substring(0, lt).Trim().ToUpperInvariant();
            string inner = s.Substring(lt + 1, s.Length - lt - 2);

            if (baseName == "ARRAY") return new ListType(ParseSqlType(inner));
            if (baseName == "MAP")
            {
                var kv = SplitTopLevel(inner, ',');
                if (kv.Count != 2) throw new FormatException();
                return new MapType(ParseSqlType(kv[0]), ParseSqlType(kv[1]));
            }
            if (baseName == "STRUCT")
            {
                var fields = new List<Field>();
                foreach (string part in SplitTopLevel(inner, ','))
                {
                    string p = part.Trim();
                    // Field syntax: "name:type" or "name type" (colon optional per Databricks grammar).
                    int sep = p.IndexOf(':');
                    if (sep < 0) sep = p.IndexOf(' ');
                    if (sep < 0) throw new FormatException();
                    string name = p.Substring(0, sep).Trim();
                    string typeStr = StripFieldModifiers(p.Substring(sep + 1));
                    fields.Add(new Field(name, ParseSqlType(typeStr), nullable: true));
                }
                return new StructType(fields);
            }
            return StringType.Default;
        }

        /// <summary>
        /// Strips trailing modifiers from a struct field's type portion. Per the
        /// Databricks STRUCT grammar:
        /// <c>fieldName [:] fieldType [NOT NULL] [COLLATE collationName] [COMMENT str]</c>.
        /// Modifiers appear at bracket depth 0 (outside any nested <c>&lt;&gt;</c> / <c>()</c>).
        /// </summary>
        private static string StripFieldModifiers(string s)
        {
            int depth = 0;
            for (int i = 0; i < s.Length; i++)
            {
                char c = s[i];
                if (c == '<' || c == '(') depth++;
                else if (c == '>' || c == ')') depth--;
                else if (depth == 0 && c == ' ')
                {
                    int j = i + 1;
                    while (j < s.Length && s[j] == ' ') j++;
                    if (StartsWithKeyword(s, j, "NOT") ||
                        StartsWithKeyword(s, j, "COLLATE") ||
                        StartsWithKeyword(s, j, "COMMENT"))
                    {
                        return s.Substring(0, i);
                    }
                }
            }
            return s;
        }

        private static bool StartsWithKeyword(string s, int pos, string keyword)
        {
            if (pos + keyword.Length > s.Length) return false;
            if (string.Compare(s, pos, keyword, 0, keyword.Length, StringComparison.OrdinalIgnoreCase) != 0) return false;
            int end = pos + keyword.Length;
            return end == s.Length || char.IsWhiteSpace(s[end]);
        }

        /// <summary>Splits <paramref name="s"/> at occurrences of <paramref name="sep"/> that are at bracket depth 0.</summary>
        private static List<string> SplitTopLevel(string s, char sep)
        {
            var parts = new List<string>();
            int depth = 0, start = 0;
            for (int i = 0; i < s.Length; i++)
            {
                char c = s[i];
                if (c == '<' || c == '(') depth++;
                else if (c == '>' || c == ')') depth--;
                else if (c == sep && depth == 0) { parts.Add(s.Substring(start, i - start)); start = i + 1; }
            }
            parts.Add(s.Substring(start));
            return parts;
        }

        /// <summary>Returns the index of the '>' that matches the '&lt;' at <paramref name="ltPos"/>, or -1 if unbalanced.</summary>
        private static int FindMatching(string s, int ltPos)
        {
            int depth = 0;
            for (int i = ltPos; i < s.Length; i++)
            {
                if (s[i] == '<') depth++;
                else if (s[i] == '>' && --depth == 0) return i;
            }
            return -1;
        }
    }
}
