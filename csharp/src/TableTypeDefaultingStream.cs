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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Wraps a <c>GetTables</c> result and substitutes <c>"TABLE"</c> for any null/empty
    /// <c>TABLE_TYPE</c>, matching databricks-jdbc's <c>MetadataResultSetBuilder</c>. The Thrift
    /// server returns an empty <c>TABLE_TYPE</c> for some tables (e.g. legacy <c>hive_metastore</c>
    /// tables in a broad enumeration), which the ADBC driver otherwise passed through. Blunt like
    /// JDBC: a view with an empty server type becomes <c>"TABLE"</c>. Non-empty values pass through.
    /// Modeled on <see cref="NullColumnSerializingStream"/>.
    /// </summary>
    internal sealed class TableTypeDefaultingStream : IArrowArrayStream
    {
        private const string DefaultTableType = "TABLE";

        private readonly IArrowArrayStream _inner;
        private readonly int _tableTypeIndex;

        public TableTypeDefaultingStream(IArrowArrayStream inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _tableTypeIndex = inner.Schema.GetFieldIndex("TABLE_TYPE");
        }

        public Schema Schema => _inner.Schema;

        public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            RecordBatch? batch = await _inner.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false);
            // The Thrift GetTables reader this wraps always materializes TABLE_TYPE as a plain
            // StringArray, so only that encoding is handled. If TABLE_TYPE ever arrives dictionary-
            // encoded on some path, the batch passes through unchanged (defaulting becomes a no-op)
            // rather than risking a wrong-type cast — matching the sibling streams' behavior.
            if (batch == null || _tableTypeIndex < 0 || batch.Column(_tableTypeIndex) is not StringArray tableTypes)
                return batch;

            StringArray.Builder builder = new StringArray.Builder();
            for (int i = 0; i < tableTypes.Length; i++)
            {
                string? value = tableTypes.IsNull(i) ? null : tableTypes.GetString(i);
                builder.Append(string.IsNullOrEmpty(value) ? DefaultTableType : value);
            }
            StringArray defaulted = builder.Build();

            IArrowArray[] arrays = new IArrowArray[batch.ColumnCount];
            for (int i = 0; i < batch.ColumnCount; i++)
                arrays[i] = i == _tableTypeIndex ? defaulted : batch.Column(i);
            return new RecordBatch(Schema, arrays, batch.Length);
        }

        public void Dispose() => _inner.Dispose();
    }
}
