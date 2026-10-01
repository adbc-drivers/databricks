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

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.StatementExecution;
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.Hive2;
using Apache.Arrow;
using Apache.Arrow.Types;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    public class NativeMetadataResultBuilderTests
    {
        [Fact]
        public async Task GetColumns_CastsIntegersAndMakesOrdinalOneBased()
        {
            var target = MetadataSchemaFactory.CreateColumnMetadataSchema();
            Assert.Equal(24, target.FieldsList.Count);
            var fields = target.FieldsList.Take(23).Select(field =>
                field.Name == "ORDINAL_POSITION"
                    ? new Field(field.Name, Int64Type.Default, true)
                    : field).ToArray();
            var source = new Schema(fields, null);
            var arrays = new List<IArrowArray>();
            foreach (var field in fields)
            {
                if (field.Name == "ORDINAL_POSITION")
                    arrays.Add(new Int64Array.Builder().Append(0).Build());
                else if (field.DataType.TypeId == ArrowTypeId.String)
                    arrays.Add(new StringArray.Builder().Append(field.Name == "TYPE_NAME" ? "DECIMAL(10,2)" : "value").Build());
                else if (field.DataType.TypeId == ArrowTypeId.Int8)
                    arrays.Add(new Int8Array.Builder().Append(0).Build());
                else if (field.DataType.TypeId == ArrowTypeId.Int16)
                    arrays.Add(new Int16Array.Builder().Append(0).Build());
                else
                    arrays.Add(new Int32Array.Builder().Append(0).Build());
            }

            using var nativeBatch = new RecordBatch(source, arrays.ToArray(), 1);
            var result = NativeMetadataResultBuilder.Build(
                new[] { nativeBatch }, target, MetadataOperation.GetColumns);
            using var reader = result.Stream!;
            using var batch = await reader.ReadNextRecordBatchAsync();

            Assert.NotNull(batch);
            Assert.Equal(1, ((Int32Array)batch.Column(16)).GetValue(0));
            Assert.Equal("DECIMAL", ((StringArray)batch.Column(23)).GetString(0));
        }
    }
}
