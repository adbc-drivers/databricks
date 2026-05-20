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
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using AdbcDrivers.Databricks.StatementExecution;
using AdbcDrivers.HiveServer2.Hive2;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// PECO-3060: Wraps an <see cref="IArrowArrayStream"/> and converts native Arrow
    /// arrays for the conversion-sensitive scalar types into the shape Thrift's
    /// <c>data_type_conv=none</c> mode produces, so SEA and Thrift agree on the output
    /// contract regardless of protocol.
    ///
    /// <para>Active only when the user sets <c>adbc.spark.data_type_conv=none</c>
    /// (default is <c>scalar</c>, which keeps native types and bypasses this stream).
    /// Mirrors <see cref="HiveServer2SchemaParser.GetArrowType"/>:</para>
    ///
    /// <list type="bullet">
    ///   <item><description>DATE (Date32Array) → "yyyy-MM-dd" string</description></item>
    ///   <item><description>TIMESTAMP (TimestampArray) → "yyyy-MM-dd HH:mm:ss[.fffffff]" string</description></item>
    ///   <item><description>DECIMAL (Decimal128Array) → plain string (preserves precision)</description></item>
    ///   <item><description>FLOAT (FloatArray) → widened to DoubleArray</description></item>
    /// </list>
    ///
    /// <para>The declared schema (from <c>TryGetSchemaFromManifest</c>) already reports
    /// StringType / DoubleType for these columns; this stream only converts the data
    /// arrays so Arrow's strongly-typed contract holds.</para>
    ///
    /// <para><strong>Column detection:</strong> Uses the same <c>Spark:DataType:SqlName</c>
    /// metadata pattern as <see cref="IntervalSerializingStream"/> and
    /// <see cref="ComplexTypeSerializingStream"/> — the manifest schema embeds the raw
    /// SQL type text on every field, which is reliable across inline / CloudFetch / empty paths.</para>
    /// </summary>
    internal sealed class ScalarConversionStream : IArrowArrayStream
    {
        private readonly IArrowArrayStream _inner;
        private readonly Schema _schema;
        // index -> kind of conversion to apply. We do this once up front so per-batch
        // work is just a dictionary lookup.
        private readonly Dictionary<int, ScalarConversion> _conversions;

        private enum ScalarConversion
        {
            DateToString,
            TimestampToString,
            DecimalToString,
            FloatToDouble,
        }

        public ScalarConversionStream(IArrowArrayStream inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _schema = inner.Schema;
            _conversions = DetectConversions(_schema);
        }

        public Schema Schema => _schema;

        public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            RecordBatch? batch = await _inner.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false);
            if (batch == null) return null;
            if (_conversions.Count == 0) return batch;
            return ConvertColumns(batch);
        }

        public void Dispose() => _inner.Dispose();

        private static Dictionary<int, ScalarConversion> DetectConversions(Schema schema)
        {
            var result = new Dictionary<int, ScalarConversion>();
            for (int i = 0; i < schema.FieldsList.Count; i++)
            {
                Field field = schema.FieldsList[i];
                if (field.Metadata == null) continue;
                if (!field.Metadata.TryGetValue(ColumnMetadataHelper.ArrowMetadataKey, out string? sqlName) || sqlName == null)
                    continue;

                // Match by SQL name prefix to keep the detection logic in sync with
                // HiveServer2SchemaParser (which switches on TTypeId). Parametrised types
                // like DECIMAL(10,2) and TIMESTAMP_NTZ are handled by prefix checks.
                var upper = sqlName.ToUpperInvariant();
                if (upper.StartsWith("DATE", StringComparison.Ordinal) && !upper.StartsWith("DATETIME", StringComparison.Ordinal))
                {
                    result[i] = ScalarConversion.DateToString;
                }
                else if (upper.StartsWith("TIMESTAMP", StringComparison.Ordinal))
                {
                    result[i] = ScalarConversion.TimestampToString;
                }
                else if (upper.StartsWith("DECIMAL", StringComparison.Ordinal) || upper.StartsWith("NUMERIC", StringComparison.Ordinal))
                {
                    result[i] = ScalarConversion.DecimalToString;
                }
                else if (upper.Equals("FLOAT", StringComparison.Ordinal) || upper.Equals("REAL", StringComparison.Ordinal))
                {
                    result[i] = ScalarConversion.FloatToDouble;
                }
            }
            return result;
        }

        private RecordBatch ConvertColumns(RecordBatch batch)
        {
            var arrays = new IArrowArray[batch.ColumnCount];
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                arrays[i] = _conversions.TryGetValue(i, out var conv)
                    ? Convert(batch.Column(i), conv)
                    : batch.Column(i);
            }
            return new RecordBatch(_schema, arrays, batch.Length);
        }

        private static IArrowArray Convert(IArrowArray array, ScalarConversion conv)
        {
            return conv switch
            {
                ScalarConversion.DateToString => ConvertDateToString(array),
                ScalarConversion.TimestampToString => ConvertTimestampToString(array),
                ScalarConversion.DecimalToString => ConvertDecimalToString(array),
                ScalarConversion.FloatToDouble => ConvertFloatToDouble(array),
                _ => array,
            };
        }

        private static StringArray ConvertDateToString(IArrowArray array)
        {
            var builder = new StringArray.Builder();
            if (array is Date32Array d32)
            {
                for (int i = 0; i < d32.Length; i++)
                {
                    if (d32.IsNull(i)) { builder.AppendNull(); continue; }
                    DateTime? dt = d32.GetDateTime(i);
                    builder.Append(dt?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
                }
            }
            else if (array is Date64Array d64)
            {
                for (int i = 0; i < d64.Length; i++)
                {
                    if (d64.IsNull(i)) { builder.AppendNull(); continue; }
                    DateTime? dt = d64.GetDateTime(i);
                    builder.Append(dt?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
                }
            }
            else
            {
                // Defensive: column was detected as DATE via SqlName metadata but the
                // underlying array isn't a Date{32,64}Array. Null-fill to keep row counts
                // consistent rather than throw.
                NullFill(builder, array.Length);
            }
            return builder.Build();
        }

        private static StringArray ConvertTimestampToString(IArrowArray array)
        {
            var builder = new StringArray.Builder();
            if (array is TimestampArray ts)
            {
                var unit = ((TimestampType)ts.Data.DataType).Unit;
                for (int i = 0; i < ts.Length; i++)
                {
                    if (ts.IsNull(i)) { builder.AppendNull(); continue; }
                    DateTimeOffset? dto = ts.GetTimestamp(i);
                    builder.Append(dto.HasValue ? FormatTimestamp(dto.Value, unit) : null);
                }
            }
            else
            {
                NullFill(builder, array.Length);
            }
            return builder.Build();
        }

        /// <summary>
        /// Formats a timestamp as Thrift would emit it for none mode: "yyyy-MM-dd HH:mm:ss"
        /// with a fractional-seconds suffix matching the column unit (omitted for second
        /// precision). The value is rendered in UTC because SEA returns TIMESTAMP / TIMESTAMP_NTZ
        /// values without timezone info and Thrift renders them as wall-clock strings.
        /// </summary>
        internal static string FormatTimestamp(DateTimeOffset value, TimeUnit unit)
        {
            // Use UTC wall-clock representation (matches Thrift's TIMESTAMP_NTZ rendering).
            var utc = value.UtcDateTime;
            string format = unit switch
            {
                TimeUnit.Second => "yyyy-MM-dd HH:mm:ss",
                TimeUnit.Millisecond => "yyyy-MM-dd HH:mm:ss.fff",
                TimeUnit.Nanosecond => "yyyy-MM-dd HH:mm:ss.fffffff",
                _ => "yyyy-MM-dd HH:mm:ss.ffffff", // Microsecond (SEA default) and unknown
            };
            return utc.ToString(format, CultureInfo.InvariantCulture);
        }

        private static StringArray ConvertDecimalToString(IArrowArray array)
        {
            var builder = new StringArray.Builder();
            if (array is Decimal128Array dec)
            {
                for (int i = 0; i < dec.Length; i++)
                {
                    if (dec.IsNull(i)) { builder.AppendNull(); continue; }
                    // Decimal128Array.GetString preserves the full precision/scale of the
                    // declared type — exactly what Thrift returns for none mode.
                    builder.Append(dec.GetString(i));
                }
            }
            else
            {
                NullFill(builder, array.Length);
            }
            return builder.Build();
        }

        private static DoubleArray ConvertFloatToDouble(IArrowArray array)
        {
            var builder = new DoubleArray.Builder().Reserve(array.Length);
            if (array is FloatArray f)
            {
                for (int i = 0; i < f.Length; i++)
                {
                    if (f.IsNull(i)) { builder.AppendNull(); continue; }
                    float? v = f.GetValue(i);
                    builder.Append(v.HasValue ? (double)v.Value : (double?)null);
                }
            }
            else if (array is DoubleArray d)
            {
                // Already double — return as-is by rebuilding so the caller's contract
                // is preserved (DoubleArray output regardless of input).
                for (int i = 0; i < d.Length; i++)
                {
                    if (d.IsNull(i)) { builder.AppendNull(); continue; }
                    builder.Append(d.GetValue(i));
                }
            }
            else
            {
                for (int i = 0; i < array.Length; i++) builder.AppendNull();
            }
            return builder.Build();
        }

        private static void NullFill(StringArray.Builder builder, int length)
        {
            for (int i = 0; i < length; i++) builder.AppendNull();
        }
    }
}
