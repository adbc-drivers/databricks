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

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Wraps an <see cref="IArrowArrayStream"/> and converts columns of complex Arrow types
    /// (LIST, MAP represented as LIST of STRUCTs, STRUCT) into STRING columns containing
    /// their JSON representation.
    ///
    /// This is applied when EnableComplexDatatypeSupport=false (the default), so that SEA
    /// results match the legacy Thrift behavior of returning JSON strings for complex types.
    /// </summary>
    internal sealed class ComplexTypeSerializingStream : IArrowArrayStream
    {
        private readonly IArrowArrayStream _inner;
        private readonly Schema _schema;
        private readonly HashSet<int> _complexColumnIndices;

        public ComplexTypeSerializingStream(IArrowArrayStream inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            (_schema, _complexColumnIndices) = BuildStringSchema(inner.Schema);
        }

        public Schema Schema => _schema;

        public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            var batch = await _inner.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false);
            if (batch == null)
                return null;

            if (_complexColumnIndices.Count == 0)
                return batch;

            return ConvertComplexColumns(batch);
        }

        public void Dispose() => _inner.Dispose();

        private RecordBatch ConvertComplexColumns(RecordBatch batch)
        {
            var arrays = new IArrowArray[batch.ColumnCount];
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                arrays[i] = _complexColumnIndices.Contains(i) ? SerializeToStringArray(batch.Column(i)) : batch.Column(i);
            }
            return new RecordBatch(_schema, arrays, batch.Length);
        }

        private static StringArray SerializeToStringArray(IArrowArray array)
        {
            var builder = new StringArray.Builder();
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
        /// Builds a new schema where all complex-type fields are replaced with StringType,
        /// and returns the set of column indices that were converted.
        /// </summary>
        private static (Schema schema, HashSet<int> complexIndices) BuildStringSchema(Schema original)
        {
            var fields = new List<Field>(original.FieldsList.Count);
            var indices = new HashSet<int>();

            for (int i = 0; i < original.FieldsList.Count; i++)
            {
                var field = original.FieldsList[i];
                if (IsComplexType(field.DataType))
                {
                    fields.Add(new Field(field.Name, StringType.Default, nullable: true, field.Metadata));
                    indices.Add(i);
                }
                else
                {
                    fields.Add(field);
                }
            }

            return (new Schema(fields, original.Metadata), indices);
        }

        private static bool IsComplexType(IArrowType type) =>
            type is ListType || type is MapType || type is StructType;

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
            var values = listArray.Values;
            int start = (int)listArray.ValueOffsets[index];
            int end = (int)listArray.ValueOffsets[index + 1];

            // Arrow MAP is stored as List<Struct<key, value>>
            if (values is StructArray structValues && IsMapStruct(structValues))
                return ToMapDict(structValues, start, end);

            var list = new List<object?>();
            for (int i = start; i < end; i++)
                list.Add(ToObject(values, i));
            return list;
        }

        private static bool IsMapStruct(StructArray structArray)
        {
            var type = (StructType)structArray.Data.DataType;
            return type.Fields.Count == 2 &&
                   type.Fields[0].Name == "key" &&
                   type.Fields[1].Name == "value";
        }

        private static Dictionary<string, object?> ToMapDict(StructArray entries, int start, int end)
        {
            var keyArray = entries.Fields[0];
            var valueArray = entries.Fields[1];
            var result = new Dictionary<string, object?>();
            for (int i = start; i < end; i++)
            {
                string key = keyArray is StringArray sa ? sa.GetString(i) ?? "null" : "null";
                result[key] = ToObject(valueArray, i);
            }
            return result;
        }

        private static Dictionary<string, object?> ToDict(StructArray structArray, int index)
        {
            var type = (StructType)structArray.Data.DataType;
            var dict = new Dictionary<string, object?>();
            for (int i = 0; i < type.Fields.Count; i++)
                dict[type.Fields[i].Name] = ToObject(structArray.Fields[i], index);
            return dict;
        }
    }
}
