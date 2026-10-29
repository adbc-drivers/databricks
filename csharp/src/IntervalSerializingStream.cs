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
using Apache.Arrow.Types;
using AdbcDrivers.Databricks.StatementExecution;

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
    /// <para><strong>Why both schema and data must be converted:</strong>
    /// Unlike the JDBC driver, which returns <c>java.lang.Object</c> from <c>getObject()</c>
    /// and can freely convert a native interval value to a string without touching the declared
    /// type, <see cref="IArrowArrayStream"/> is a strongly-typed Arrow contract: the
    /// <see cref="Schema"/> and the arrays inside each <see cref="RecordBatch"/> must agree on
    /// the column type. This stream therefore changes both — the schema it exposes is already
    /// <see cref="StringType"/> (coming from the manifest via
    /// <c>TryGetSchemaFromManifest</c>), and the data arrays are converted to
    /// <see cref="StringArray"/> at read time.
    /// </para>
    ///
    /// <para><strong>Column detection:</strong>
    /// Interval columns are identified by the <c>Spark:DataType:SqlName</c> field metadata
    /// (<see cref="ColumnMetadataHelper.ArrowMetadataKey"/>) that
    /// <c>TryGetSchemaFromManifest</c> embeds when building the manifest schema.
    /// For SEA results the server may not include this key in the Arrow IPC field metadata
    /// (unlike Thrift, where the server always embeds it); by computing it from the manifest
    /// upfront we make detection consistent across all three result paths — inline, CloudFetch,
    /// and empty. The actual Arrow subtype (year-month vs. day-time) is resolved at read time
    /// by inspecting the incoming array type, not the metadata string.
    /// </para>
    /// </summary>
    internal sealed class IntervalSerializingStream : IArrowArrayStream
    {
        private readonly IArrowArrayStream _inner;
        private readonly Schema _schema;
        private readonly HashSet<int> _intervalColumnIndices;

        public IntervalSerializingStream(IArrowArrayStream inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _schema = inner.Schema;
            _intervalColumnIndices = DetectIntervalColumns(_schema);
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

        /// <summary>
        /// Detects interval columns by inspecting the <c>Spark:DataType:SqlName</c> metadata
        /// on each field. This works for all result paths because they all expose the manifest
        /// schema, which carries that metadata.
        /// </summary>
        private static HashSet<int> DetectIntervalColumns(Schema schema)
        {
            var indices = new HashSet<int>();
            for (int i = 0; i < schema.FieldsList.Count; i++)
            {
                Field field = schema.FieldsList[i];
                if (field.Metadata != null &&
                    field.Metadata.TryGetValue(ColumnMetadataHelper.ArrowMetadataKey, out string? sqlName) &&
                    sqlName != null &&
                    sqlName.StartsWith("INTERVAL", StringComparison.OrdinalIgnoreCase))
                {
                    indices.Add(i);
                }
            }
            return indices;
        }

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
                // The column was detected as INTERVAL via SqlName metadata but the
                // underlying array is neither YearMonthIntervalArray nor DurationArray.
                // This should not happen in practice — the SEA server always sends native
                // interval types for INTERVAL columns — but null-fill defensively.
                for (int i = 0; i < array.Length; i++)
                    builder.AppendNull();
            }
            return builder.Build();
        }

        // --- Formatting helpers ---

        /// <summary>
        /// Formats a year-month interval (total months) as "Y-M", matching the Thrift output.
        /// Example: 30 months → "2-6"
        /// </summary>
        internal static string FormatYearMonth(int totalMonths)
        {
            bool neg = totalMonths < 0;
            long abs = Math.Abs((long)totalMonths);  // (long) cast guards int.MinValue
            long years = abs / 12;
            long months = abs % 12;
            return (neg ? "-" : "") + $"{years}-{months}";
        }

        /// <summary>
        /// Formats a duration value as "D HH:MM:SS.nnnnnnnnn", matching the Thrift output.
        /// Example: 3 days + 12 h + 30 min + 15 s 500 ms → "3 12:30:15.500000000"
        /// The <paramref name="unit"/> determines how to interpret <paramref name="rawValue"/>.
        /// </summary>
        internal static string FormatDuration(long rawValue, TimeUnit unit)
        {
            // Derive (wholeSeconds, subNanos) directly in the source unit to avoid intermediate
            // nanosecond overflow. Multiplying rawValue * 1e9 (or * 1e6) to convert to nanoseconds
            // first overflows int64 for values near the Databricks limit of ±106,751,991 days,
            // which is representable in microseconds (Long.MAX_VALUE µs) but not in nanoseconds.
            if (rawValue == long.MinValue) rawValue += 1;  // guard negation overflow
            bool neg = rawValue < 0;
            long abs = neg ? -rawValue : rawValue;

            long wholeSeconds, subNanos;
            switch (unit)
            {
                case TimeUnit.Second:
                    wholeSeconds = abs;
                    subNanos = 0;
                    break;
                case TimeUnit.Millisecond:
                    wholeSeconds = abs / 1_000L;
                    subNanos = (abs % 1_000L) * 1_000_000L;
                    break;
                case TimeUnit.Nanosecond:
                    wholeSeconds = abs / 1_000_000_000L;
                    subNanos = abs % 1_000_000_000L;
                    break;
                default: // Microsecond (Databricks SEA default) and unknown units
                    wholeSeconds = abs / 1_000_000L;
                    subNanos = (abs % 1_000_000L) * 1_000L;
                    break;
            }

            long days = wholeSeconds / 86400L;
            long remainderSeconds = wholeSeconds % 86400L;

            int hours = (int)(remainderSeconds / 3600L);
            int minutes = (int)((remainderSeconds % 3600L) / 60L);
            int seconds = (int)(remainderSeconds % 60L);

            return (neg ? "-" : "") + $"{days} {hours:D2}:{minutes:D2}:{seconds:D2}.{subNanos:D9}";
        }
    }
}
