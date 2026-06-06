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
using System.IO;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.StatementExecution;
using Apache.Arrow;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Wraps an <see cref="IArrowArrayStream"/> and converts ARRAY, MAP, and STRUCT columns
    /// into STRING columns containing their JSON representation.
    ///
    /// <para>
    /// Applied when <c>EnableComplexDatatypeSupport=false</c> (the default) on both the Thrift
    /// and SEA paths so that both return consistent, correctly-escaped JSON strings for complex
    /// types regardless of what the server emits.
    /// </para>
    ///
    /// <para><strong>Why both schema and data must be converted:</strong>
    /// Arrow streaming is strongly typed: the <see cref="Schema"/> and the arrays inside each
    /// <see cref="RecordBatch"/> must agree on the column type.
    /// <list type="bullet">
    ///   <item><description>
    ///     <strong>SEA path:</strong> the manifest schema already declares complex columns as
    ///     <see cref="StringType"/>, so only the batch data (native <c>ListArray</c> /
    ///     <c>StructArray</c> / etc.) needs converting; the schema is passed through unchanged.
    ///   </description></item>
    ///   <item><description>
    ///     <strong>Thrift path:</strong> the driver requests <c>ComplexTypesAsArrow=true</c>
    ///     from the server, so the inner IPC schema carries native
    ///     <c>ListType</c> / <c>MapType</c> / <c>StructType</c>. Both the schema and the batch
    ///     data must be flattened to <see cref="StringType"/> / <see cref="StringArray"/> to
    ///     keep them in agreement. <see cref="FlattenComplexColumns"/> handles the schema side.
    ///   </description></item>
    /// </list>
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
        // Use the relaxed encoder so complex-type values are serialized as data, not HTML:
        // a double quote becomes \" (not ") and non-ASCII / < > & are emitted verbatim
        // rather than \uXXXX-escaped. The output is still valid JSON; "unsafe" only refers to
        // embedding directly in HTML, which is the consuming application's concern, not ours.
        private static readonly JsonWriterOptions WriterOptions = new JsonWriterOptions
        {
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        };

        private readonly IArrowArrayStream _inner;
        private readonly Schema _schema;
        private readonly HashSet<int> _complexColumnIndices;

        public ComplexTypeSerializingStream(IArrowArrayStream inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _complexColumnIndices = DetectComplexColumns(inner.Schema);
            // Flatten complex columns to StringType so the exposed schema matches the
            // string-serialized batches this stream produces. For SEA the inner schema
            // already has StringType for complex columns (manifest-driven), so this is a
            // no-op. For Thrift with ComplexTypesAsArrow=true, the inner IPC schema has
            // native ListType/MapType/StructType — we must report StringType to keep
            // schema and batch types consistent.
            _schema = FlattenComplexColumns(inner.Schema, _complexColumnIndices);
        }

        private static Schema FlattenComplexColumns(Schema schema, HashSet<int> complexIndices)
        {
            if (complexIndices.Count == 0) return schema;
            List<Field> fields = new List<Field>(schema.FieldsList.Count);
            for (int i = 0; i < schema.FieldsList.Count; i++)
            {
                Field f = schema.FieldsList[i];
                IArrowType arrowType = complexIndices.Contains(i) ? StringType.Default : f.DataType;
                fields.Add(new Field(f.Name, arrowType, f.IsNullable, f.Metadata));
            }
            return new Schema(fields, schema.Metadata);
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

            // Write each value with a manual Utf8JsonWriter rather than JsonSerializer.Serialize.
            // The value graph (ToObject) is a closed set of Arrow scalar types, lists, and
            // dictionaries, so we can emit it without reflection — keeping this trim- and
            // NativeAOT-safe. A single stream/writer pair is reused across rows to avoid
            // per-row allocations.
            using MemoryStream stream = new MemoryStream();
            using Utf8JsonWriter writer = new Utf8JsonWriter(stream, WriterOptions);
            for (int i = 0; i < array.Length; i++)
            {
                if (array.IsNull(i))
                {
                    builder.AppendNull();
                    continue;
                }

                stream.SetLength(0);
                writer.Reset(stream);
                SerializeStructuredValue(writer, array, i);
                writer.Flush();
                builder.Append(Encoding.UTF8.GetString(stream.GetBuffer(), 0, (int)stream.Length));
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

        private static void SerializeStructuredValue(Utf8JsonWriter writer, IArrowArray array, int index)
        {
            if (array.IsNull(index))
            {
                writer.WriteNullValue();
                return;
            }

            switch (array.Data.DataType.TypeId)
            {
                case ArrowTypeId.List:
                case ArrowTypeId.Map: SerializeListOrMap(writer, (ListArray)array, index); break;
                case ArrowTypeId.Struct: SerializeDict(writer, (StructArray)array, index); break;
                // DECIMAL: emit as a bare JSON number (not a quoted string) so the output matches
                // the JDBC driver and is valid JSON. The decimal's string form is written raw so
                // values beyond C# decimal's ~28-digit range (DECIMAL(38, …)) keep full precision.
                case ArrowTypeId.Decimal32: writer.WriteRawValue(((Decimal32Array)array).GetString(index)); break;
                case ArrowTypeId.Decimal64: writer.WriteRawValue(((Decimal64Array)array).GetString(index)); break;
                case ArrowTypeId.Decimal128: writer.WriteRawValue(((Decimal128Array)array).GetString(index)); break;
                case ArrowTypeId.Decimal256: writer.WriteRawValue(((Decimal256Array)array).GetString(index)); break;
                case ArrowTypeId.Date32: writer.WriteStringValue(((Date32Array)array).GetDateTime(index)!.Value.ToString("yyyy-MM-dd")); break;
                case ArrowTypeId.Date64: writer.WriteStringValue(((Date64Array)array).GetDateTime(index)!.Value.ToString("yyyy-MM-dd")); break;
                // INTERVAL: native YearMonth/Duration arrays serialize to {} via System.Text.Json
                // (no public properties). Render the same "Y-M" / "D HH:MM:SS.nnnnnnnnn" strings
                // IntervalSerializingStream produces for top-level interval columns.
                case ArrowTypeId.Interval:
                    switch (((IntervalType)array.Data.DataType).Unit)
                    {
                        case IntervalUnit.DayTime:
                            var dayTime = ((DayTimeIntervalArray)array).GetValue(index)!.Value;
                            var timeSpan = TimeSpan.FromDays(dayTime.Days) + TimeSpan.FromMilliseconds(dayTime.Milliseconds);
                            writer.WriteStringValue(timeSpan.ToString());
                            break;
                        case IntervalUnit.MonthDayNanosecond:
                            var monthDayNano = ((MonthDayNanosecondIntervalArray)array).GetValue(index)!.Value;
                            timeSpan = TimeSpan.FromDays(monthDayNano.Days) + TimeSpan.FromTicks(monthDayNano.Nanoseconds / 100);
                            writer.WriteStringValue(IntervalSerializingStream.FormatYearMonth(monthDayNano.Months) + " " + timeSpan.ToString());
                            break;
                        case IntervalUnit.YearMonth:
                            writer.WriteStringValue(IntervalSerializingStream.FormatYearMonth(
                                ((YearMonthIntervalArray)array).GetValue(index)!.Value.Months));
                            break;
                        default: writer.WriteNullValue(); break;
                    }
                    break;
                case ArrowTypeId.Duration:
                    DurationArray dur = (DurationArray)array;
                    writer.WriteStringValue(IntervalSerializingStream.FormatDuration(dur.GetValue(index)!.Value, ((DurationType)dur.Data.DataType).Unit));
                    break;

                case ArrowTypeId.Boolean: writer.WriteBooleanValue(((BooleanArray)array).GetValue(index)!.Value); break;
                case ArrowTypeId.Double: writer.WriteNumberValue(((DoubleArray)array).GetValue(index)!.Value); break;
                case ArrowTypeId.Float: writer.WriteNumberValue(((FloatArray)array).GetValue(index)!.Value); break;
#if NET5_0_OR_GREATER
                case ArrowTypeId.HalfFloat:
                    var halfValue = ((HalfFloatArray)array).GetValue(index)!.Value;
                    writer.WriteNumberValue((double)halfValue); break;
#endif
                case ArrowTypeId.Int8: writer.WriteNumberValue(((Int8Array)array).GetValue(index)!.Value); break;
                case ArrowTypeId.Int16: writer.WriteNumberValue(((Int16Array)array).GetValue(index)!.Value); break;
                case ArrowTypeId.Int32: writer.WriteNumberValue(((Int32Array)array).GetValue(index)!.Value); break;
                case ArrowTypeId.Int64: writer.WriteNumberValue(((Int64Array)array).GetValue(index)!.Value); break;
                case ArrowTypeId.String: writer.WriteStringValue(((StringArray)array).GetString(index)); break;
                case ArrowTypeId.LargeString: writer.WriteStringValue(((LargeStringArray)array).GetString(index)); break;
#if NET6_0_OR_GREATER
                case ArrowTypeId.Time32: writer.WriteRawValue(((Time32Array)array).GetTime(index)!.Value.ToString("HH:mm:ss.ffffff")); break;
                case ArrowTypeId.Time64: writer.WriteRawValue(((Time64Array)array).GetTime(index)!.Value.ToString("HH:mm:ss.ffffff")); break;
#else
                case ArrowTypeId.Time32:
                    Time32Array time32Array = (Time32Array)array;
                    int time32 = time32Array.GetValue(index)!.Value;
                    switch (((Time32Type)time32Array.Data.DataType).Unit)
                    {
                        case TimeUnit.Second: writer.WriteStringValue(TimeSpan.FromSeconds(time32).ToString()); break;
                        case TimeUnit.Millisecond: writer.WriteStringValue(TimeSpan.FromMilliseconds(time32).ToString()); break;
                        default: writer.WriteNullValue(); break;
                    };
                    break;
                case ArrowTypeId.Time64:
                    Time64Array time64Array = (Time64Array)array;
                    long time64 = time64Array.GetValue(index)!.Value;
                    switch (((Time64Type)time64Array.Data.DataType).Unit)
                    {
                        case TimeUnit.Microsecond: writer.WriteStringValue(TimeSpan.FromTicks(time64 * 10).ToString()); break;
                        case TimeUnit.Nanosecond: writer.WriteStringValue(TimeSpan.FromTicks(time64 / 100).ToString()); break;
                        default: writer.WriteNullValue(); break;
                    };
                    break;
#endif
                case ArrowTypeId.Timestamp: writer.WriteStringValue(((TimestampArray)array).GetTimestamp(index)!.Value); break;
                case ArrowTypeId.UInt8: writer.WriteNumberValue(((UInt8Array)array).GetValue(index)!.Value); break;
                case ArrowTypeId.UInt16: writer.WriteNumberValue(((UInt16Array)array).GetValue(index)!.Value); break;
                case ArrowTypeId.UInt32: writer.WriteNumberValue(((UInt32Array)array).GetValue(index)!.Value); break;
                case ArrowTypeId.UInt64: writer.WriteNumberValue(((UInt64Array)array).GetValue(index)!.Value); break;
                case ArrowTypeId.Binary: writer.WriteBase64StringValue(((BinaryArray)array).GetBytes(index)); break;
                default: writer.WriteNullValue(); break;
            }
        }

        private static void SerializeListOrMap(Utf8JsonWriter writer, ListArray listArray, int index)
        {
            IArrowArray values = listArray.Values;
            int start = (int)listArray.ValueOffsets[index];
            int end = (int)listArray.ValueOffsets[index + 1];

            // Arrow MAP is stored as List<Struct<key, value>>
            if (values is StructArray structValues && IsMapStruct(structValues))
            {
                SerializeMapDict(writer, structValues, start, end);
                return;
            }

            writer.WriteStartArray();
            List<object?> list = new List<object?>();
            for (int i = start; i < end; i++)
                SerializeStructuredValue(writer, values, i);
            writer.WriteEndArray();
        }

        private static bool IsMapStruct(StructArray structArray)
        {
            StructType type = (StructType)structArray.Data.DataType;
            return type.Fields.Count == 2 &&
                   type.Fields[0].Name == "key" &&
                   type.Fields[1].Name == "value";
        }

        private static void SerializeMapDict(Utf8JsonWriter writer, StructArray entries, int start, int end)
        {
            IArrowArray keyArray = entries.Fields[0];
            IArrowArray valueArray = entries.Fields[1];
            writer.WriteStartObject();
            for (int i = start; i < end; i++)
            {
                // Convert any key type to its string representation; treat null keys as "null"
                string key = keyArray.ValueAt(i)?.ToString() ?? "null";
                writer.WritePropertyName(key);
                SerializeStructuredValue(writer, valueArray, i);
            }
            writer.WriteEndObject();
        }

        private static void SerializeDict(Utf8JsonWriter writer, StructArray structArray, int index)
        {
            StructType type = (StructType)structArray.Data.DataType;
            writer.WriteStartObject();
            for (int i = 0; i < type.Fields.Count; i++)
            {
                writer.WritePropertyName(type.Fields[i].Name);
                SerializeStructuredValue(writer, structArray.Fields[i], index);
            }
            writer.WriteEndObject();
        }
    }
}
