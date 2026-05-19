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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using AdbcDrivers.Databricks.StatementExecution;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Wraps an <see cref="IArrowArrayStream"/> and converts ARRAY, MAP, and STRUCT columns
    /// into STRING columns containing their JSON representation.
    ///
    /// <para>
    /// Applied when <c>EnableComplexDatatypeSupport=false</c> (the default) so that SEA
    /// results match the legacy Thrift behavior of returning JSON strings for complex types.
    /// </para>
    ///
    /// <para><strong>Why both schema and data must be converted:</strong>
    /// Arrow streaming is strongly typed: the <see cref="Schema"/> and the arrays inside each
    /// <see cref="RecordBatch"/> must agree on the column type. The manifest schema (built by
    /// <c>TryGetSchemaFromManifest</c>) already declares complex columns as
    /// <see cref="StringType"/>, so this stream only needs to convert the native Arrow arrays
    /// (<c>ListArray</c>, <c>StructArray</c>, etc.) to <see cref="StringArray"/> at read time.
    /// The schema it exposes to callers is the inner stream's schema unchanged.
    /// </para>
    ///
    /// <para><strong>Column detection:</strong>
    /// Complex columns are identified by the <c>Spark:DataType:SqlName</c> field metadata
    /// (<see cref="ColumnMetadataHelper.ArrowMetadataKey"/>) that
    /// <c>TryGetSchemaFromManifest</c> embeds when building the manifest schema. This is
    /// the same key the Databricks server embeds in Arrow IPC field metadata for Thrift results
    /// (and that the JDBC driver reads as <c>ARROW_METADATA_KEY</c>). Detecting via this
    /// metadata — rather than by inspecting the Arrow field type — is necessary because the
    /// manifest schema already uses <see cref="StringType"/> for complex columns, making
    /// Arrow-type-based detection always return false.
    /// </para>
    /// </summary>
    internal sealed class ComplexTypeSerializingStream : IArrowArrayStream
    {
        private readonly IArrowArrayStream _inner;
        private readonly Schema _schema;
        private readonly HashSet<int> _complexColumnIndices;

        public ComplexTypeSerializingStream(IArrowArrayStream inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _schema = inner.Schema;
            _complexColumnIndices = DetectComplexColumns(_schema);
        }

        public Schema Schema => _schema;

        public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            RecordBatch? batch = await _inner.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false);
            if (batch == null)
                return null;

            if (_complexColumnIndices.Count == 0)
                return batch;

            return ConvertComplexColumns(batch);
        }

        public void Dispose() => _inner.Dispose();

        private RecordBatch ConvertComplexColumns(RecordBatch batch)
        {
            IArrowArray[] arrays = new IArrowArray[batch.ColumnCount];
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                arrays[i] = _complexColumnIndices.Contains(i) ? SerializeToStringArray(batch.Column(i)) : batch.Column(i);
            }
            return new RecordBatch(_schema, arrays, batch.Length);
        }

        private static StringArray SerializeToStringArray(IArrowArray array)
        {
            StringArray.Builder builder = new StringArray.Builder();
            for (int i = 0; i < array.Length; i++)
            {
                if (array.IsNull(i))
                    builder.AppendNull();
                else
                    builder.Append(JsonSerializer.Serialize(ToObject(array, i)));
            }
            return builder.Build();
        }

        /// <summary>
        /// Detects complex columns by inspecting the <c>Spark:DataType:SqlName</c> metadata
        /// on each field. This works for all result paths because they all expose the manifest
        /// schema, which carries that metadata and already types complex columns as StringType.
        /// </summary>
        private static HashSet<int> DetectComplexColumns(Schema schema)
        {
            HashSet<int> indices = new HashSet<int>();
            for (int i = 0; i < schema.FieldsList.Count; i++)
            {
                Field field = schema.FieldsList[i];
                if (field.Metadata != null &&
                    field.Metadata.TryGetValue(ColumnMetadataHelper.ArrowMetadataKey, out string? sqlName) &&
                    sqlName != null &&
                    (sqlName.StartsWith("ARRAY", StringComparison.OrdinalIgnoreCase) ||
                     sqlName.StartsWith("MAP", StringComparison.OrdinalIgnoreCase) ||
                     sqlName.StartsWith("STRUCT", StringComparison.OrdinalIgnoreCase)))
                {
                    indices.Add(i);
                }
            }
            return indices;
        }

        // --- JSON serialization helpers ---

        private static object? ToObject(IArrowArray array, int index)
        {
            if (array.IsNull(index))
                return null;

            // Handle complex types with recursive traversal, and types needing specific
            // string formatting. All other primitives delegate to ValueAt().
            return array switch
            {
                ListArray la => ToListOrMap(la, index),
                StructArray sa => ToDict(sa, index),
                Decimal128Array dec => dec.GetString(index),            // preserve precision as string
                Date32Array d32 => d32.GetDateTime(index)?.ToString("yyyy-MM-dd"),
                _ => array.ValueAt(index, StructResultType.Object)      // int, long, float, bool, string, timestamp, etc.
            };
        }

        private static object ToListOrMap(ListArray listArray, int index)
        {
            IArrowArray values = listArray.Values;
            int start = (int)listArray.ValueOffsets[index];
            int end = (int)listArray.ValueOffsets[index + 1];

            // Arrow MAP is stored as List<Struct<key, value>>
            if (values is StructArray structValues && IsMapStruct(structValues))
                return ToMapDict(structValues, start, end);

            List<object?> list = new List<object?>();
            for (int i = start; i < end; i++)
                list.Add(ToObject(values, i));
            return list;
        }

        private static bool IsMapStruct(StructArray structArray)
        {
            StructType type = (StructType)structArray.Data.DataType;
            return type.Fields.Count == 2 &&
                   type.Fields[0].Name == "key" &&
                   type.Fields[1].Name == "value";
        }

        private static SortedDictionary<string, object?> ToMapDict(StructArray entries, int start, int end)
        {
            IArrowArray keyArray = entries.Fields[0];
            IArrowArray valueArray = entries.Fields[1];
            // Use SortedDictionary for deterministic key ordering in the JSON output
            SortedDictionary<string, object?> result = new SortedDictionary<string, object?>();
            for (int i = start; i < end; i++)
            {
                // Convert any key type to its string representation; treat null keys as "null"
                string key = ToObject(keyArray, i)?.ToString() ?? "null";
                result[key] = ToObject(valueArray, i);
            }
            return result;
        }

        private static Dictionary<string, object?> ToDict(StructArray structArray, int index)
        {
            StructType type = (StructType)structArray.Data.DataType;
            Dictionary<string, object?> dict = new Dictionary<string, object?>();
            for (int i = 0; i < type.Fields.Count; i++)
                dict[type.Fields[i].Name] = ToObject(structArray.Fields[i], index);
            return dict;
        }
    }

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
    internal static class ComplexTypeParser
    {
        /// <summary>
        /// Parses <paramref name="typeName"/> into a native Arrow type. Returns
        /// <see cref="StringType"/> on any parse failure — callers can rely on this,
        /// the method never throws.
        /// </summary>
        internal static IArrowType ParseComplexType(string typeName)
        {
            if (string.IsNullOrWhiteSpace(typeName)) return StringType.Default;
            try { return ParseSqlType(typeName.Trim()); }
            catch (FormatException) { return StringType.Default; }
        }

        /// <summary>
        /// Maps a primitive (non-complex) Databricks SQL type name to its Arrow type.
        /// Used by the SEA manifest mapper for top-level columns and by
        /// <see cref="ParseComplexType"/> for primitive leaves inside ARRAY/MAP/STRUCT.
        /// </summary>
        internal static IArrowType MapPrimitiveType(string typeName)
        {
            var baseType = ColumnMetadataHelper.GetBaseTypeName(typeName).ToUpperInvariant();
            return baseType switch
            {
                "BOOLEAN" => BooleanType.Default,
                "BYTE" or "TINYINT" => Int8Type.Default,
                "SHORT" or "SMALLINT" => Int16Type.Default,
                "INT" or "INTEGER" => Int32Type.Default,
                "LONG" or "BIGINT" => Int64Type.Default,
                "FLOAT" or "REAL" => FloatType.Default,
                "DOUBLE" => DoubleType.Default,
                "DECIMAL" or "NUMERIC" => ParseDecimalType(typeName),
                "STRING" or "VARCHAR" or "CHAR" => StringType.Default,
                "BINARY" or "VARBINARY" => BinaryType.Default,
                "DATE" => Date32Type.Default,
                "TIMESTAMP" or "TIMESTAMP_NTZ" or "TIMESTAMP_LTZ" => TimestampType.Default,
                // INTERVAL is converted to string by IntervalSerializingStream; StringType is the output contract.
                "INTERVAL" => StringType.Default,
                "NULL" or "VOID" => NullType.Default,
                _ => StringType.Default,
            };
        }

        private static IArrowType ParseDecimalType(string typeName)
        {
            int precision = 38;
            int scale = 18;

            var match = System.Text.RegularExpressions.Regex.Match(
                typeName,
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
