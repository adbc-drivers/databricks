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
using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Serializes Arrow complex type values (LIST, MAP, STRUCT) to JSON strings,
    /// matching the behavior of JDBC's ComplexDataTypeParser when EnableComplexDatatypeSupport=false.
    /// </summary>
    internal static class ArrowComplexTypeJsonSerializer
    {
        /// <summary>
        /// Serializes the element at <paramref name="index"/> of an Arrow array to a JSON string.
        /// Returns null if the element is null.
        /// </summary>
        public static string? SerializeElement(IArrowArray array, int index)
        {
            if (array.IsNull(index))
                return null;

            using var writer = new System.IO.MemoryStream();
            using var jsonWriter = new Utf8JsonWriter(writer);
            WriteValue(jsonWriter, array, index);
            jsonWriter.Flush();
            return Encoding.UTF8.GetString(writer.ToArray());
        }

        private static void WriteValue(Utf8JsonWriter writer, IArrowArray array, int index)
        {
            if (array.IsNull(index))
            {
                writer.WriteNullValue();
                return;
            }

            switch (array)
            {
                case ListArray listArray:
                    WriteList(writer, listArray, index);
                    break;
                case StructArray structArray:
                    WriteStruct(writer, structArray, index);
                    break;
                // MapArray in Apache.Arrow.NET is represented as a ListArray of StructArrays (key/value pairs)
                // The Arrow .NET library does not have a distinct MapArray — Map columns are ListArray<Struct<key,value>>
                // We detect this via the schema type
                case StringArray stringArray:
                    writer.WriteStringValue(stringArray.GetString(index));
                    break;
                case Int32Array int32Array:
                    writer.WriteNumberValue(int32Array.GetValue(index)!.Value);
                    break;
                case Int64Array int64Array:
                    writer.WriteNumberValue(int64Array.GetValue(index)!.Value);
                    break;
                case Int16Array int16Array:
                    writer.WriteNumberValue(int16Array.GetValue(index)!.Value);
                    break;
                case Int8Array int8Array:
                    writer.WriteNumberValue(int8Array.GetValue(index)!.Value);
                    break;
                case FloatArray floatArray:
                    writer.WriteNumberValue(floatArray.GetValue(index)!.Value);
                    break;
                case DoubleArray doubleArray:
                    writer.WriteNumberValue(doubleArray.GetValue(index)!.Value);
                    break;
                case BooleanArray boolArray:
                    writer.WriteBooleanValue(boolArray.GetValue(index)!.Value);
                    break;
                case Decimal128Array decimalArray:
                    // Write as string to preserve precision
                    writer.WriteStringValue(decimalArray.GetString(index));
                    break;
                case Date32Array date32Array:
                    writer.WriteStringValue(date32Array.GetDateTime(index)?.ToString("yyyy-MM-dd") ?? "null");
                    break;
                case TimestampArray timestampArray:
                    writer.WriteStringValue(timestampArray.GetTimestamp(index)!.Value.ToString("o"));
                    break;
                default:
                    // Fallback: use ToString
                    writer.WriteStringValue(array.ToString());
                    break;
            }
        }

        private static void WriteList(Utf8JsonWriter writer, ListArray listArray, int index)
        {
            var values = listArray.Values;
            int start = (int)listArray.ValueOffsets[index];
            int end = (int)listArray.ValueOffsets[index + 1];

            // Check if this is a MAP (ListArray<StructArray<key, value>>)
            if (values is StructArray structValues && IsMapStruct(structValues))
            {
                WriteMapFromListOfStructs(writer, structValues, start, end);
            }
            else
            {
                writer.WriteStartArray();
                for (int i = start; i < end; i++)
                {
                    WriteValue(writer, values, i);
                }
                writer.WriteEndArray();
            }
        }

        private static bool IsMapStruct(StructArray structArray)
        {
            // Arrow maps are stored as List<Struct<key, value>> where the struct has exactly 2 fields named "key" and "value"
            var type = (StructType)structArray.Data.DataType;
            return type.Fields.Count == 2 &&
                   type.Fields[0].Name == "key" &&
                   type.Fields[1].Name == "value";
        }

        private static void WriteMapFromListOfStructs(Utf8JsonWriter writer, StructArray entries, int start, int end)
        {
            var keyArray = entries.Fields[0];
            var valueArray = entries.Fields[1];

            writer.WriteStartObject();
            for (int i = start; i < end; i++)
            {
                // Key must be a string for JSON object keys
                string? key = keyArray is StringArray sa ? sa.GetString(i) : keyArray.ToString();
                writer.WritePropertyName(key ?? "null");
                WriteValue(writer, valueArray, i);
            }
            writer.WriteEndObject();
        }

        private static void WriteStruct(Utf8JsonWriter writer, StructArray structArray, int index)
        {
            var type = (StructType)structArray.Data.DataType;
            writer.WriteStartObject();
            for (int fieldIdx = 0; fieldIdx < type.Fields.Count; fieldIdx++)
            {
                writer.WritePropertyName(type.Fields[fieldIdx].Name);
                WriteValue(writer, structArray.Fields[fieldIdx], index);
            }
            writer.WriteEndObject();
        }
    }
}
