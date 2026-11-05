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
using System.Linq;
using AdbcDrivers.HiveServer2;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace AdbcDrivers.Databricks.StatementExecution
{
    internal sealed class NativeMetadataColumns
    {
        private readonly Dictionary<string, IArrowArray> _columns = new(StringComparer.Ordinal);

        internal NativeMetadataColumns(RecordBatch batch, Schema schema, MetadataOperation operation)
        {
            var fields = schema.FieldsList.Where(field =>
                operation != MetadataOperation.GetColumns || field.Name != "BASE_TYPE_NAME").ToList();
            int count = fields.Count;
            if (batch.ColumnCount != count)
                throw new DatabricksException($"Invalid native {operation} result: expected {count} columns, found {batch.ColumnCount}");

            foreach (var expected in fields)
            {
                int index = batch.Schema.GetFieldIndex(expected.Name);
                if (index < 0)
                    throw new DatabricksException($"Invalid native {operation} result: missing {expected.Name}");

                IArrowArray array = batch.Column(index);
                bool valid = expected.DataType.TypeId == ArrowTypeId.String
                    ? array is StringArray
                    : IsInteger(expected.DataType.TypeId) && IsInteger(batch.Schema.FieldsList[index].DataType.TypeId)
                      && array is Int8Array or Int16Array or Int32Array or Int64Array;
                if (!valid || _columns.ContainsKey(expected.Name))
                    throw new DatabricksException($"Invalid native {operation} result: unexpected type or duplicate {expected.Name}");
                _columns.Add(expected.Name, array);
            }
        }

        internal string? String(string name, int row)
        {
            var array = (StringArray)_columns[name];
            return array.IsNull(row) ? null : array.GetString(row);
        }

        internal long? Integer(string name, int row)
        {
            var array = _columns[name];
            if (array.IsNull(row)) return null;
            return array switch
            {
                Int8Array values => values.GetValue(row),
                Int16Array values => values.GetValue(row),
                Int32Array values => values.GetValue(row),
                Int64Array values => values.GetValue(row),
                _ => throw new DatabricksException($"Expected a native metadata integer, found {array.GetType().Name}")
            };
        }

        private static bool IsInteger(ArrowTypeId type) => type is
            ArrowTypeId.Int8 or ArrowTypeId.Int16 or ArrowTypeId.Int32 or ArrowTypeId.Int64;
    }
}
