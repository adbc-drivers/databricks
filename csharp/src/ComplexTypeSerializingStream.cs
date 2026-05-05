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
    /// Wraps an <see cref="IArrowArrayStream"/> and converts columns that carry
    /// native Arrow types that must be serialized to strings into STRING columns:
    ///
    /// <list type="bullet">
    ///   <item>
    ///     <description>
    ///       Complex types (LIST, MAP represented as LIST of STRUCTs, STRUCT) are
    ///       converted into STRING columns containing their JSON representation.
    ///       Applied when EnableComplexDatatypeSupport=false (the default) so that SEA
    ///       results match the legacy Thrift behavior of returning JSON strings for complex types.
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///       <see cref="ArrowTypeId.Interval"/> with <see cref="IntervalUnit.YearMonth"/>
    ///       (<c>YearMonthIntervalArray</c>) → "Y-M" string,
    ///       e.g. 30 months → "2-6" (2 years, 6 months).
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///       <see cref="ArrowTypeId.Duration"/> (<c>DurationArray</c>) → "D HH:MM:SS.nnnnnnnnn"
    ///       string, e.g. 3 days + 12 h + 30 min + 15 s → "3 12:30:15.000000000".
    ///       The conversion respects the <see cref="DurationType.Unit"/> of the column.
    ///     </description>
    ///   </item>
    /// </list>
    ///
    /// The schema reported to callers is rewritten so that converted columns have
    /// <see cref="StringType"/> instead of the native type, exactly mirroring what
    /// the Thrift protocol returns.
    /// </summary>
    internal sealed class ComplexTypeSerializingStream : IArrowArrayStream
    {
        private readonly IArrowArrayStream _inner;
        private readonly Schema _schema;
        private readonly HashSet<int> _complexColumnIndices;
        // For interval/duration columns we need to know the original type to pick the right formatter
        private readonly Dictionary<int, IArrowType> _intervalColumnOriginalTypes;

        /// <summary>
        /// Initializes a new instance of <see cref="ComplexTypeSerializingStream"/>.
        /// </summary>
        /// <param name="inner">The underlying Arrow stream to wrap.</param>
        /// <param name="serializeComplexTypes">
        /// When <c>true</c> (the default), LIST/MAP/STRUCT columns are serialized to JSON strings.
        /// Set to <c>false</c> when EnableComplexDatatypeSupport=true so that complex columns are
        /// passed through as native Arrow types. Interval/duration columns are always converted to
        /// strings regardless of this flag.
        /// </param>
        public ComplexTypeSerializingStream(IArrowArrayStream inner, bool serializeComplexTypes = true)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            (_schema, _complexColumnIndices, _intervalColumnOriginalTypes) = BuildStringSchema(inner.Schema, serializeComplexTypes);
        }

        public Schema Schema => _schema;

        public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            RecordBatch? batch = await _inner.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false);
            if (batch == null)
                return null;

            if (_complexColumnIndices.Count == 0 && _intervalColumnOriginalTypes.Count == 0)
                return batch;

            return ConvertColumns(batch);
        }

        public void Dispose() => _inner.Dispose();

        private RecordBatch ConvertColumns(RecordBatch batch)
        {
            IArrowArray[] arrays = new IArrowArray[batch.ColumnCount];
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                if (_complexColumnIndices.Contains(i))
                    arrays[i] = SerializeComplexToStringArray(batch.Column(i));
                else if (_intervalColumnOriginalTypes.TryGetValue(i, out IArrowType? originalType))
                    arrays[i] = SerializeIntervalToStringArray(batch.Column(i), originalType);
                else
                    arrays[i] = batch.Column(i);
            }
            return new RecordBatch(_schema, arrays, batch.Length);
        }

        private static StringArray SerializeComplexToStringArray(IArrowArray array)
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

        private static StringArray SerializeIntervalToStringArray(IArrowArray array, IArrowType originalType)
        {
            StringArray.Builder builder = new StringArray.Builder();
            for (int i = 0; i < array.Length; i++)
            {
                if (array.IsNull(i))
                {
                    builder.AppendNull();
                }
                else if (originalType is IntervalType intervalType &&
                         intervalType.Unit == IntervalUnit.YearMonth)
                {
                    YearMonthIntervalArray ymArray = (YearMonthIntervalArray)array;
                    var value = ymArray.GetValue(i);
                    builder.Append(value.HasValue
                        ? FormatYearMonth(value.Value.Months)
                        : null);
                }
                else if (originalType is DurationType durationType)
                {
                    DurationArray durationArray = (DurationArray)array;
                    long? rawValue = durationArray.GetValue(i);
                    builder.Append(rawValue.HasValue
                        ? FormatDuration(rawValue.Value, durationType.Unit)
                        : null);
                }
                else
                {
                    builder.AppendNull();
                }
            }
            return builder.Build();
        }

        /// <summary>
        /// Builds a new schema where all converted-type fields are replaced with StringType,
        /// and returns the sets of column indices that were converted.
        /// </summary>
        private static (Schema schema, HashSet<int> complexIndices, Dictionary<int, IArrowType> intervalColumns) BuildStringSchema(Schema original, bool serializeComplexTypes)
        {
            List<Field> fields = new List<Field>(original.FieldsList.Count);
            HashSet<int> complexIndices = new HashSet<int>();
            Dictionary<int, IArrowType> intervalColumns = new Dictionary<int, IArrowType>();

            for (int i = 0; i < original.FieldsList.Count; i++)
            {
                Field field = original.FieldsList[i];
                if (serializeComplexTypes && IsComplexType(field.DataType))
                {
                    fields.Add(new Field(field.Name, StringType.Default, field.IsNullable, field.Metadata));
                    complexIndices.Add(i);
                }
                else if (IsIntervalOrDurationType(field.DataType))
                {
                    fields.Add(new Field(field.Name, StringType.Default, field.IsNullable, field.Metadata));
                    intervalColumns[i] = field.DataType;
                }
                else
                {
                    fields.Add(field);
                }
            }

            return (new Schema(fields, original.Metadata), complexIndices, intervalColumns);
        }

        private static bool IsComplexType(IArrowType type) =>
            type is ListType || type is MapType || type is StructType;

        private static bool IsIntervalOrDurationType(IArrowType type) =>
            (type is IntervalType intervalType && intervalType.Unit == IntervalUnit.YearMonth) ||
            type is DurationType;

        // --- Interval/duration formatting helpers ---

        /// <summary>
        /// Formats a year-month interval (total months) as "Y-M", matching the Thrift output.
        /// Example: 30 months → "2-6"
        /// </summary>
        internal static string FormatYearMonth(int totalMonths)
        {
            int years = totalMonths / 12;
            int months = totalMonths % 12;
            return $"{years}-{months}";
        }

        /// <summary>
        /// Formats a duration value as "D HH:MM:SS.nnnnnnnnn", matching the Thrift output.
        /// Example: 3 days + 12 h + 30 min + 15 s 500 ms → "3 12:30:15.500000000"
        /// The <paramref name="unit"/> determines how to interpret <paramref name="rawValue"/>.
        /// </summary>
        internal static string FormatDuration(long rawValue, TimeUnit unit)
        {
            // Convert to nanoseconds first for uniform handling
            long nanoseconds = unit switch
            {
                TimeUnit.Second => rawValue * 1_000_000_000L,
                TimeUnit.Millisecond => rawValue * 1_000_000L,
                TimeUnit.Microsecond => rawValue * 1_000L,
                TimeUnit.Nanosecond => rawValue,
                _ => rawValue * 1_000L // default to microseconds
            };

            // Separate the nanosecond sub-second part from whole seconds
            long wholeSeconds = nanoseconds / 1_000_000_000L;
            long subNanos = nanoseconds % 1_000_000_000L;

            // Handle negative values: keep subNanos non-negative
            if (subNanos < 0)
            {
                wholeSeconds -= 1;
                subNanos += 1_000_000_000L;
            }

            long days = wholeSeconds / 86400L;
            long remainderSeconds = wholeSeconds % 86400L;

            // Handle negative days: ensure remainderSeconds is non-negative
            if (remainderSeconds < 0)
            {
                days -= 1;
                remainderSeconds += 86400L;
            }

            int hours = (int)(remainderSeconds / 3600L);
            int minutes = (int)((remainderSeconds % 3600L) / 60L);
            int seconds = (int)(remainderSeconds % 60L);

            return $"{days} {hours:D2}:{minutes:D2}:{seconds:D2}.{subNanos:D9}";
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
