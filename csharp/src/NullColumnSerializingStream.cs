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
    /// Wraps an <see cref="IArrowArrayStream"/> and converts an untyped-NULL column
    /// (SQL type VOID, e.g. <c>SELECT NULL</c>) from a native Arrow <c>NullArray</c> to an
    /// all-null <see cref="StringArray"/>, matching the Thrift protocol which reports STRING
    /// for untyped NULL.
    ///
    /// <para>
    /// The SEA result manifest reports the SQL type name <c>VOID</c>/<c>NULL</c> for such a
    /// column; <see cref="ArrowTypeParser"/> maps it to <see cref="StringType"/> so the declared
    /// schema is Utf8 (identical to a <c>CAST(NULL AS STRING)</c> sibling). The Arrow IPC data,
    /// however, arrives as a <c>NullArray</c>. Because <see cref="IArrowArrayStream"/> is a
    /// strongly-typed contract where the <see cref="Schema"/> and the arrays inside each
    /// <see cref="RecordBatch"/> must agree on column type, this stream converts the incoming
    /// <c>NullArray</c> to an all-null <see cref="StringArray"/> of the same length — mirroring
    /// <see cref="IntervalSerializingStream"/> and <see cref="ComplexTypeSerializingStream"/>.
    /// </para>
    ///
    /// <para><strong>Column detection:</strong>
    /// Untyped-NULL columns are identified by the <c>Spark:DataType:SqlName</c> field metadata
    /// (<see cref="ColumnMetadataHelper.ArrowMetadataKey"/>) that <c>TryGetSchemaFromManifest</c>
    /// embeds when building the manifest schema — the same reliable signal the sibling serializing
    /// streams use across all result paths (inline, CloudFetch, empty).
    /// </para>
    /// </summary>
    internal sealed class NullColumnSerializingStream : IArrowArrayStream
    {
        private readonly IArrowArrayStream _inner;
        private readonly Schema _schema;
        private readonly HashSet<int> _nullColumnIndices;

        public NullColumnSerializingStream(IArrowArrayStream inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _schema = inner.Schema;
            _nullColumnIndices = DetectNullColumns(_schema);
        }

        public Schema Schema => _schema;

        public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            RecordBatch? batch = await _inner.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false);
            if (batch == null)
                return null;

            if (_nullColumnIndices.Count == 0)
                return batch;

            return ConvertColumns(batch);
        }

        public void Dispose() => _inner.Dispose();

        /// <summary>
        /// Detects untyped-NULL columns by inspecting the <c>Spark:DataType:SqlName</c> metadata
        /// on each field. This works for all result paths because they all expose the manifest
        /// schema, which carries that metadata.
        /// </summary>
        private static HashSet<int> DetectNullColumns(Schema schema)
        {
            var indices = new HashSet<int>();
            for (int i = 0; i < schema.FieldsList.Count; i++)
            {
                Field field = schema.FieldsList[i];
                if (field.Metadata != null &&
                    field.Metadata.TryGetValue(ColumnMetadataHelper.ArrowMetadataKey, out string? sqlName) &&
                    sqlName != null)
                {
                    string baseType = ColumnMetadataHelper.GetBaseTypeName(sqlName).ToUpperInvariant();
                    if (baseType == "NULL" || baseType == "VOID")
                    {
                        indices.Add(i);
                    }
                }
            }
            return indices;
        }

        private RecordBatch ConvertColumns(RecordBatch batch)
        {
            IArrowArray[] arrays = new IArrowArray[batch.ColumnCount];
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                arrays[i] = _nullColumnIndices.Contains(i)
                    ? SerializeNullToStringArray(batch.Column(i))
                    : batch.Column(i);
            }
            return new RecordBatch(_schema, arrays, batch.Length);
        }

        private static StringArray SerializeNullToStringArray(IArrowArray array)
        {
            // An untyped-NULL column has no non-null values; emit an all-null StringArray of the
            // same length so the declared StringType schema and the batch array agree.
            StringArray.Builder builder = new StringArray.Builder();
            for (int i = 0; i < array.Length; i++)
            {
                builder.AppendNull();
            }
            return builder.Build();
        }
    }
}
