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
using Apache.Arrow.Memory;
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
        private readonly IReadOnlyList<int> _complexColumnIndices;

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
                arrays[i] = IsComplexIndex(i) ? SerializeToStringArray(batch.Column(i)) : batch.Column(i);
            }
            return new RecordBatch(_schema, arrays, batch.Length);
        }

        private bool IsComplexIndex(int index)
        {
            foreach (int idx in _complexColumnIndices)
                if (idx == index) return true;
            return false;
        }

        private static StringArray SerializeToStringArray(IArrowArray array)
        {
            var builder = new StringArray.Builder();
            for (int i = 0; i < array.Length; i++)
            {
                string? json = ArrowComplexTypeJsonSerializer.SerializeElement(array, i);
                if (json == null)
                    builder.AppendNull();
                else
                    builder.Append(json);
            }
            return builder.Build();
        }

        /// <summary>
        /// Builds a new schema where all complex-type fields are replaced with StringType,
        /// and returns the list of column indices that were converted.
        /// </summary>
        private static (Schema schema, IReadOnlyList<int> complexIndices) BuildStringSchema(Schema original)
        {
            var fields = new List<Field>(original.FieldsList.Count);
            var indices = new List<int>();

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

            var schema = new Schema(fields, original.Metadata);
            return (schema, indices);
        }

        private static bool IsComplexType(IArrowType type) =>
            type is ListType || type is MapType || type is StructType;
    }
}
