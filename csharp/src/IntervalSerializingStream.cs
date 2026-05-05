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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Scalars;
using Apache.Arrow.Types;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Wraps an <see cref="IArrowArrayStream"/> and converts native Arrow interval and duration
    /// columns to canonical UTF-8 string columns, matching the string representation that the
    /// Thrift protocol returns:
    ///
    /// <list type="bullet">
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
    /// <see cref="StringType"/> instead of the native type.
    /// </summary>
    internal sealed class IntervalSerializingStream : IArrowArrayStream
    {
        private readonly IArrowArrayStream _inner;
        private readonly Schema _schema;
        private readonly HashSet<int> _intervalColumnIndices;

        // Arrow field metadata key set by the Databricks server and by TryGetSchemaFromManifest.
        private const string SparkSqlNameKey = "Spark:DataType:SqlName";

        public IntervalSerializingStream(IArrowArrayStream inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            (_schema, _intervalColumnIndices) = BuildStringSchema(inner.Schema);
        }

        public Schema Schema => _schema;

        public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            RecordBatch? batch = await _inner.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false);
            if (batch == null)
                return null;

            if (_intervalColumnIndices.Count == 0)
                return batch;

            return ConvertColumns(batch);
        }

        public void Dispose() => _inner.Dispose();

        private RecordBatch ConvertColumns(RecordBatch batch)
        {
            IArrowArray[] arrays = new IArrowArray[batch.ColumnCount];
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                arrays[i] = _intervalColumnIndices.Contains(i)
                    ? SerializeIntervalToStringArray(batch.Column(i))
                    : batch.Column(i);
            }
            return new RecordBatch(_schema, arrays, batch.Length);
        }

        private static StringArray SerializeIntervalToStringArray(IArrowArray array)
        {
            StringArray.Builder builder = new StringArray.Builder();
            if (array is YearMonthIntervalArray ymArray)
            {
                for (int i = 0; i < array.Length; i++)
                {
                    if (array.IsNull(i)) { builder.AppendNull(); continue; }
                    var value = ymArray.GetValue(i);
                    builder.Append(value.HasValue ? FormatYearMonth(value.Value.Months) : null);
                }
            }
            else if (array is DurationArray durationArray)
            {
                DurationType durationType = (DurationType)durationArray.Data.DataType;
                for (int i = 0; i < array.Length; i++)
                {
                    if (array.IsNull(i)) { builder.AppendNull(); continue; }
                    long? rawValue = durationArray.GetValue(i);
                    builder.Append(rawValue.HasValue ? FormatDuration(rawValue.Value, durationType.Unit) : null);
                }
            }
            else
            {
                for (int i = 0; i < array.Length; i++)
                    builder.AppendNull();
            }
            return builder.Build();
        }

        private static (Schema schema, HashSet<int> intervalIndices) BuildStringSchema(Schema original)
        {
            List<Field> fields = new List<Field>(original.FieldsList.Count);
            HashSet<int> indices = new HashSet<int>();

            for (int i = 0; i < original.FieldsList.Count; i++)
            {
                Field field = original.FieldsList[i];
                if (IsIntervalByMetadata(field))
                {
                    fields.Add(new Field(field.Name, StringType.Default, field.IsNullable, field.Metadata));
                    indices.Add(i);
                }
                else
                {
                    fields.Add(field);
                }
            }

            return (new Schema(fields, original.Metadata), indices);
        }

        private static bool IsIntervalByMetadata(Field field)
        {
            return field.Metadata != null &&
                   field.Metadata.TryGetValue(SparkSqlNameKey, out string? sqlName) &&
                   sqlName != null &&
                   sqlName.StartsWith("INTERVAL", StringComparison.OrdinalIgnoreCase);
        }

        // --- Formatting helpers ---

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
    }
}
