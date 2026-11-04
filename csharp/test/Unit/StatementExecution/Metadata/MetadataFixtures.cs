/*
 * Copyright (c) 2026 ADBC Drivers Contributors
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
using AdbcDrivers.HiveServer2.Hive2;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution.Metadata
{
    internal static class MetadataFixtures
    {
        internal static RecordBatch Strings(params (string Name, string?[] Values)[] columns)
        {
            var schema = new Schema(columns.Select(column =>
                new Field(column.Name, StringType.Default, true)), null);
            var arrays = columns.Select(column =>
            {
                var builder = new StringArray.Builder();
                foreach (string? value in column.Values)
                {
                    if (value == null) builder.AppendNull(); else builder.Append(value);
                }
                return (IArrowArray)builder.Build();
            }).ToArray();
            return new RecordBatch(schema, arrays, columns[0].Values.Length);
        }

        // A native response with the Thrift columns of schema; value(field, row) supplies each cell.
        internal static RecordBatch NativeBatch(Schema schema, int length, Func<string, int, object?> value)
        {
            var fields = schema.FieldsList.Where(field => field.Name != "BASE_TYPE_NAME").ToList();
            var arrays = new List<IArrowArray>();
            foreach (var field in fields)
            {
                switch (field.DataType.TypeId)
                {
                    case ArrowTypeId.String:
                        var strings = new StringArray.Builder();
                        for (int row = 0; row < length; row++)
                            if (value(field.Name, row) is string text) strings.Append(text); else strings.AppendNull();
                        arrays.Add(strings.Build());
                        break;
                    case ArrowTypeId.Int16:
                        var shorts = new Int16Array.Builder();
                        for (int row = 0; row < length; row++)
                            if (value(field.Name, row) is int number) shorts.Append((short)number); else shorts.AppendNull();
                        arrays.Add(shorts.Build());
                        break;
                    default:
                        var ints = new Int32Array.Builder();
                        for (int row = 0; row < length; row++)
                            if (value(field.Name, row) is int number) ints.Append(number); else ints.AppendNull();
                        arrays.Add(ints.Build());
                        break;
                }
            }
            return new RecordBatch(new Schema(fields, null), arrays, length);
        }

        internal static RecordBatch ShowColumns(params (string Table, string Name)[] rows)
            => Strings(
                ("catalogName", rows.Select(_ => (string?)"main").ToArray()),
                ("namespace", rows.Select(_ => (string?)"default").ToArray()),
                ("tableName", rows.Select(row => (string?)row.Table).ToArray()),
                ("col_name", rows.Select(row => (string?)row.Name).ToArray()),
                ("columnType", rows.Select(_ => (string?)"INT").ToArray()));

        // Native main.default.t.a rows (ordinal 7, default "7", auto-increment) in the given catalogs.
        internal static RecordBatch NativeColumns(params string?[] catalogs)
            => NativeBatch(MetadataSchemaFactory.CreateColumnMetadataSchema(), catalogs.Length, (field, row) => field switch
            {
                "TABLE_CAT" => catalogs[row],
                "TABLE_SCHEM" => "default",
                "TABLE_NAME" => "t",
                "COLUMN_NAME" => "a",
                "TYPE_NAME" => "INT",
                "COLUMN_DEF" => "7",
                "IS_AUTO_INCREMENT" => "YES",
                "ORDINAL_POSITION" => 7,
                "NULLABLE" => 1,
                _ => 0,
            });
    }
}
