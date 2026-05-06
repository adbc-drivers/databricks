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
}
