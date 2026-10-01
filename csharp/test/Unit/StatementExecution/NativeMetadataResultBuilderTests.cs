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
        public async Task GetColumns_NormalizesSizeAndScaleAndKeepsFlatOrdinalZeroBased()
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
                else if (field.Name == "TABLE_CAT")
                    arrays.Add(new StringArray.Builder().AppendNull().Build());
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
                new[] { nativeBatch }, target, MetadataOperation.GetColumns,
                sourceCatalogs: new[] { "main" });
            using var reader = result.Stream!;
            using var batch = await reader.ReadNextRecordBatchAsync();

            Assert.NotNull(batch);
            Assert.Equal(10, ((Int32Array)batch.Column(6)).GetValue(0));
            Assert.Equal(2, ((Int32Array)batch.Column(8)).GetValue(0));
            Assert.Equal(0, ((Int32Array)batch.Column(16)).GetValue(0));
            Assert.Equal("main", ((StringArray)batch.Column(0)).GetString(0));
            Assert.Equal("DECIMAL", ((StringArray)batch.Column(23)).GetString(0));
        }

        [Fact]
        public async Task GetTables_DefaultsEmptyTypeBeforeFilteringAndFillsRequestedCatalog()
        {
            var schema = MetadataSchemaFactory.CreateTablesSchema();
            var arrays = schema.FieldsList.Select(field => (IArrowArray)(field.Name switch
            {
                "TABLE_CAT" => new StringArray.Builder().AppendNull().Build(),
                "TABLE_TYPE" => new StringArray.Builder().Append("").Build(),
                _ => new StringArray.Builder().Append(field.Name).Build(),
            })).ToArray();
            using var nativeBatch = new RecordBatch(schema, arrays, 1);

            var result = NativeMetadataResultBuilder.Build(
                new[] { nativeBatch }, schema, MetadataOperation.GetTables,
                requestedCatalog: "main", tableTypes: new[] { "TABLE" });
            using var reader = result.Stream!;
            using var batch = await reader.ReadNextRecordBatchAsync();

            Assert.Equal(1, result.RowCount);
            Assert.Equal("main", ((StringArray)batch!.Column(0)).GetString(0));
            Assert.Equal("TABLE", ((StringArray)batch.Column(3)).GetString(0));
        }

        [Theory]
        [InlineData((int)MetadataOperation.GetCatalogs)]
        [InlineData((int)MetadataOperation.GetSchemas)]
        [InlineData((int)MetadataOperation.GetPrimaryKeys)]
        [InlineData((int)MetadataOperation.GetCrossReference)]
        public async Task OtherNativeOperations_PreserveTheirSchemas(int operationCode)
        {
            var operation = (MetadataOperation)operationCode;
            var schema = operation switch
            {
                MetadataOperation.GetCatalogs => MetadataSchemaFactory.CreateCatalogsSchema(),
                MetadataOperation.GetSchemas => MetadataSchemaFactory.CreateSchemasSchema(),
                MetadataOperation.GetPrimaryKeys => MetadataSchemaFactory.CreatePrimaryKeysSchema(),
                _ => MetadataSchemaFactory.CreateCrossReferenceSchema(),
            };
            var arrays = schema.FieldsList.Select(field => field.DataType.TypeId switch
            {
                ArrowTypeId.String => (IArrowArray)new StringArray.Builder().Append("value").Build(),
                ArrowTypeId.Int8 => new Int8Array.Builder().Append(1).Build(),
                ArrowTypeId.Int16 => new Int16Array.Builder().Append(1).Build(),
                ArrowTypeId.Int32 => new Int32Array.Builder().Append(1).Build(),
                _ => (IArrowArray)new Int64Array.Builder().Append(1).Build(),
            }).ToArray();
            using var nativeBatch = new RecordBatch(schema, arrays, 1);

            var result = NativeMetadataResultBuilder.Build(new[] { nativeBatch }, schema, operation);
            using var reader = result.Stream!;
            using var batch = await reader.ReadNextRecordBatchAsync();

            Assert.Equal(1, result.RowCount);
            Assert.Equal(schema.FieldsList.Count, batch!.ColumnCount);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void GetColumns_RejectsWrongColumnNameOrType(bool wrongType)
        {
            var target = MetadataSchemaFactory.CreateColumnMetadataSchema();
            var fields = target.FieldsList.Take(23).Select(field =>
                field.Name == "COLUMN_NAME"
                    ? new Field(wrongType ? field.Name : "wrong_name", wrongType ? (IArrowType)Int32Type.Default : field.DataType, true)
                    : field).ToArray();
            var schema = new Schema(fields, null);
            var arrays = fields.Select(field => field.DataType.TypeId switch
            {
                ArrowTypeId.String => (IArrowArray)new StringArray.Builder().Append("value").Build(),
                ArrowTypeId.Int8 => new Int8Array.Builder().Append(0).Build(),
                ArrowTypeId.Int16 => new Int16Array.Builder().Append(0).Build(),
                ArrowTypeId.Int32 => new Int32Array.Builder().Append(0).Build(),
                _ => (IArrowArray)new Int64Array.Builder().Append(0).Build(),
            }).ToArray();
            using var nativeBatch = new RecordBatch(schema, arrays, 1);

            Assert.Throws<DatabricksException>(() => NativeMetadataResultBuilder.Build(
                new[] { nativeBatch }, target, MetadataOperation.GetColumns));
        }
    }
}
