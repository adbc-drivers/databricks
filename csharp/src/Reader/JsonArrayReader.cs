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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Apache.Arrow.Adbc.Drivers.Databricks.StatementExecution;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Reader
{
    /// <summary>
    /// Reader for JSON_ARRAY format results from Statement Execution API.
    /// Converts JSON data to Arrow format.
    /// </summary>
    internal class JsonArrayReader : IArrowArrayStream
    {
        private readonly Schema _schema;
        private readonly List<List<string>> _data;
        private bool _hasReadBatch;
        private bool _disposed;

        public JsonArrayReader(ResultManifest manifest, List<List<string>> data)
        {
            if (manifest?.Schema == null)
            {
                throw new ArgumentException("Manifest must contain schema", nameof(manifest));
            }

            _schema = ConvertSchema(manifest.Schema);
            _data = data ?? new List<List<string>>();
            _hasReadBatch = false;
            _disposed = false;
        }

        public Schema Schema => _schema;

        public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(JsonArrayReader));
            }

            // JSON_ARRAY format returns all data in a single batch
            if (_hasReadBatch || _data.Count == 0)
            {
                return null;
            }

            _hasReadBatch = true;

            return await Task.Run(() => ConvertToRecordBatch(), cancellationToken);
        }

        private RecordBatch ConvertToRecordBatch()
        {
            int rowCount = _data.Count;
            var arrays = new IArrowArray[_schema.FieldsList.Count];

            for (int colIndex = 0; colIndex < _schema.FieldsList.Count; colIndex++)
            {
                var field = _schema.FieldsList[colIndex];
                arrays[colIndex] = ConvertColumnToArrowArray(field, colIndex, rowCount);
            }

            return new RecordBatch(_schema, arrays, rowCount);
        }

        private IArrowArray ConvertColumnToArrowArray(Field field, int columnIndex, int rowCount)
        {
            var dataType = field.DataType;

            // Handle different Arrow types
            switch (dataType.TypeId)
            {
                case ArrowTypeId.Int32:
                    return ConvertToInt32Array(columnIndex, rowCount);
                case ArrowTypeId.Int64:
                    return ConvertToInt64Array(columnIndex, rowCount);
                case ArrowTypeId.Double:
                    return ConvertToDoubleArray(columnIndex, rowCount);
                case ArrowTypeId.String:
                    return ConvertToStringArray(columnIndex, rowCount);
                case ArrowTypeId.Boolean:
                    return ConvertToBooleanArray(columnIndex, rowCount);
                case ArrowTypeId.Date32:
                    return ConvertToDate32Array(columnIndex, rowCount);
                case ArrowTypeId.Timestamp:
                    return ConvertToTimestampArray(columnIndex, rowCount, (TimestampType)dataType);
                default:
                    // Default to string for unknown types
                    return ConvertToStringArray(columnIndex, rowCount);
            }
        }

        private IArrowArray ConvertToInt32Array(int columnIndex, int rowCount)
        {
            var builder = new Int32Array.Builder();
            for (int i = 0; i < rowCount; i++)
            {
                var value = GetCellValue(i, columnIndex);
                if (string.IsNullOrEmpty(value) || value == "null")
                {
                    builder.AppendNull();
                }
                else if (int.TryParse(value, out int result))
                {
                    builder.Append(result);
                }
                else
                {
                    builder.AppendNull();
                }
            }
            return builder.Build();
        }

        private IArrowArray ConvertToInt64Array(int columnIndex, int rowCount)
        {
            var builder = new Int64Array.Builder();
            for (int i = 0; i < rowCount; i++)
            {
                var value = GetCellValue(i, columnIndex);
                if (string.IsNullOrEmpty(value) || value == "null")
                {
                    builder.AppendNull();
                }
                else if (long.TryParse(value, out long result))
                {
                    builder.Append(result);
                }
                else
                {
                    builder.AppendNull();
                }
            }
            return builder.Build();
        }

        private IArrowArray ConvertToDoubleArray(int columnIndex, int rowCount)
        {
            var builder = new DoubleArray.Builder();
            for (int i = 0; i < rowCount; i++)
            {
                var value = GetCellValue(i, columnIndex);
                if (string.IsNullOrEmpty(value) || value == "null")
                {
                    builder.AppendNull();
                }
                else if (double.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out double result))
                {
                    builder.Append(result);
                }
                else
                {
                    builder.AppendNull();
                }
            }
            return builder.Build();
        }

        private IArrowArray ConvertToStringArray(int columnIndex, int rowCount)
        {
            var builder = new StringArray.Builder();
            for (int i = 0; i < rowCount; i++)
            {
                var value = GetCellValue(i, columnIndex);
                if (value == "null")
                {
                    builder.AppendNull();
                }
                else
                {
                    builder.Append(value ?? string.Empty);
                }
            }
            return builder.Build();
        }

        private IArrowArray ConvertToBooleanArray(int columnIndex, int rowCount)
        {
            var builder = new BooleanArray.Builder();
            for (int i = 0; i < rowCount; i++)
            {
                var value = GetCellValue(i, columnIndex);
                if (string.IsNullOrEmpty(value) || value == "null")
                {
                    builder.AppendNull();
                }
                else if (bool.TryParse(value, out bool result))
                {
                    builder.Append(result);
                }
                else
                {
                    builder.AppendNull();
                }
            }
            return builder.Build();
        }

        private IArrowArray ConvertToDate32Array(int columnIndex, int rowCount)
        {
            var builder = new Date32Array.Builder();
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            for (int i = 0; i < rowCount; i++)
            {
                var value = GetCellValue(i, columnIndex);
                if (string.IsNullOrEmpty(value) || value == "null")
                {
                    builder.AppendNull();
                }
                else if (DateTime.TryParse(value, out DateTime date))
                {
                    builder.Append(date.Date);
                }
                else
                {
                    builder.AppendNull();
                }
            }
            return builder.Build();
        }

        private IArrowArray ConvertToTimestampArray(int columnIndex, int rowCount, TimestampType timestampType)
        {
            var builder = new TimestampArray.Builder(timestampType);

            for (int i = 0; i < rowCount; i++)
            {
                var value = GetCellValue(i, columnIndex);
                if (string.IsNullOrEmpty(value) || value == "null")
                {
                    builder.AppendNull();
                }
                else if (DateTimeOffset.TryParse(value, out DateTimeOffset timestamp))
                {
                    builder.Append(timestamp);
                }
                else
                {
                    builder.AppendNull();
                }
            }
            return builder.Build();
        }

        private string? GetCellValue(int rowIndex, int columnIndex)
        {
            if (rowIndex >= _data.Count)
            {
                return null;
            }

            var row = _data[rowIndex];
            if (columnIndex >= row.Count)
            {
                return null;
            }

            return row[columnIndex];
        }

        private static Schema ConvertSchema(ResultSchema schema)
        {
            if (schema.Columns == null || schema.Columns.Count == 0)
            {
                return new Schema.Builder().Build();
            }

            var fields = new List<Field>();
            foreach (var column in schema.Columns.OrderBy(c => c.Position))
            {
                var arrowType = ConvertType(column.TypeName, column.TypeText);
                fields.Add(new Field(column.Name, arrowType, nullable: true));
            }

            return new Schema(fields, null);
        }

        private static IArrowType ConvertType(string? typeName, string? typeText)
        {
            // Use typeText if available, fall back to typeName
            string type = (typeText ?? typeName ?? "STRING").ToUpperInvariant();

            // Map Databricks types to Arrow types
            if (type.Contains("INT") || type == "INTEGER")
            {
                return Int32Type.Default;
            }
            else if (type.Contains("BIGINT") || type == "LONG")
            {
                return Int64Type.Default;
            }
            else if (type.Contains("DOUBLE") || type == "FLOAT")
            {
                return DoubleType.Default;
            }
            else if (type.Contains("BOOLEAN") || type == "BOOL")
            {
                return BooleanType.Default;
            }
            else if (type.Contains("DATE"))
            {
                return Date32Type.Default;
            }
            else if (type.Contains("TIMESTAMP"))
            {
                return new TimestampType(TimeUnit.Microsecond, timezone: "UTC");
            }
            else
            {
                // Default to string for all other types
                return StringType.Default;
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _disposed = true;
            }
        }
    }
}
