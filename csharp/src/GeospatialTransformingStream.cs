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
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using AdbcDrivers.Databricks.StatementExecution;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Reconciles logical GEOMETRY / GEOGRAPHY types from result metadata with
    /// either Reyden's physical Arrow <c>struct&lt;srid:int32,wkb:binary&gt;</c> values
    /// or legacy server-rendered WKT / EWKT. Enabled mode returns GeoArrow WKB;
    /// disabled mode returns WKT / EWKT strings.
    /// </summary>
    internal sealed class GeospatialTransformingStream : IArrowArrayStream
    {
        private sealed class ColumnPlan
        {
            internal ColumnPlan(GeospatialArrowType.Tag tag, IArrowType outputType)
            {
                Tag = tag;
                OutputType = outputType;
            }

            internal GeospatialArrowType.Tag Tag { get; }

            internal IArrowType OutputType { get; }
        }

        private readonly IArrowArrayStream _inner;
        private readonly ColumnPlan?[] _plans;
        private readonly Schema _schema;

        internal GeospatialTransformingStream(
            IArrowArrayStream inner,
            bool enableGeospatialSupport)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _plans = new ColumnPlan?[inner.Schema.FieldsList.Count];

            var fields = new List<Field>(inner.Schema.FieldsList.Count);
            for (int i = 0; i < inner.Schema.FieldsList.Count; i++)
            {
                Field field = inner.Schema.FieldsList[i];
                ColumnPlan? plan = CreatePlan(
                    field,
                    enableGeospatialSupport,
                    out Field outputField);
                _plans[i] = plan;
                fields.Add(outputField);
            }
            _schema = new Schema(fields, inner.Schema.Metadata);
        }

        public Schema Schema => _schema;

        public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(
            CancellationToken cancellationToken = default)
        {
            RecordBatch? batch = await _inner
                .ReadNextRecordBatchAsync(cancellationToken)
                .ConfigureAwait(false);
            if (batch == null)
                return null;

            IArrowArray[]? rewritten = null;
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                ColumnPlan? plan = _plans[i];
                if (plan == null)
                    continue;

                rewritten ??= CopyColumns(batch);
                rewritten[i] = ArrowArrayFactory.BuildArray(
                    RewriteGeo(
                        batch.Column(i).Data,
                        plan.OutputType,
                        plan.Tag,
                        _schema.GetFieldByIndex(i).Name));
            }

            return rewritten == null
                ? batch
                : new RecordBatch(_schema, rewritten, batch.Length);
        }

        public void Dispose() => _inner.Dispose();

        private static ColumnPlan? CreatePlan(
            Field field,
            bool enableGeospatialSupport,
            out Field outputField)
        {
            bool hasLogicalTag = false;
            GeospatialArrowType.Tag logicalTag = default;
            if (field.Metadata?.TryGetValue(
                    ColumnMetadataHelper.ArrowMetadataKey,
                    out string? typeText) == true
                && !string.IsNullOrWhiteSpace(typeText))
            {
                IArrowType logicalType = ArrowTypeParser.MapToArrowType(
                    typeText!,
                    enableComplexDatatypeSupport: true);
                hasLogicalTag = GeospatialArrowType.TryGetTag(logicalType, out logicalTag);
            }

            bool hasPhysicalTag = GeospatialArrowType.TryGetTag(
                field.DataType,
                out GeospatialArrowType.Tag physicalTag);
            if (!hasLogicalTag && !hasPhysicalTag)
            {
                outputField = field;
                return null;
            }

            GeospatialArrowType.Tag tag = hasLogicalTag ? logicalTag : physicalTag;
            if (hasLogicalTag && hasPhysicalTag && !logicalTag.Equals(physicalTag))
            {
                throw new DatabricksException(
                    $"Geospatial column '{field.Name}' is declared {logicalTag.Family}({logicalTag.Srid}) "
                    + $"but its Arrow schema is tagged {physicalTag.Family}({physicalTag.Srid})",
                    AdbcStatusCode.InvalidData);
            }

            outputField = enableGeospatialSupport
                ? GeospatialArrowType.CreateGeoArrowField(field, tag)
                : new Field(field.Name, StringType.Default, field.IsNullable, field.Metadata);
            return new ColumnPlan(tag, outputField.DataType);
        }

        private static IArrowArray[] CopyColumns(RecordBatch batch)
        {
            var arrays = new IArrowArray[batch.ColumnCount];
            for (int i = 0; i < arrays.Length; i++)
            {
                arrays[i] = batch.Column(i);
            }
            return arrays;
        }

        private static ArrayData RewriteGeo(
            ArrayData source,
            IArrowType outputType,
            GeospatialArrowType.Tag expectedTag,
            string path)
        {
            if (source.DataType.TypeId is ArrowTypeId.String or ArrowTypeId.LargeString)
            {
                if (outputType.TypeId == ArrowTypeId.String)
                {
                    return source.DataType.TypeId == ArrowTypeId.String
                        ? source
                        : ConvertLargeStringToString(source).Data;
                }
                if (outputType.TypeId != ArrowTypeId.Binary)
                {
                    throw new DatabricksException(
                        $"Internal geospatial schema mismatch for '{path}'",
                        AdbcStatusCode.InternalError);
                }
                return ConvertTextToGeoArrow(
                    source,
                    expectedTag,
                    path).Data;
            }

            if (!GeospatialArrowType.IsStructShape(source.DataType))
            {
                throw new DatabricksException(
                    $"Geospatial value '{path}' is declared {expectedTag.Family} but its "
                    + $"Arrow type is {source.DataType.Name}; expected struct<srid:int32,wkb:binary>",
                    AdbcStatusCode.InvalidData);
            }

            if (GeospatialArrowType.TryGetTag(
                    source.DataType,
                    out GeospatialArrowType.Tag actualTag)
                && !actualTag.Equals(expectedTag))
            {
                throw new DatabricksException(
                    $"Geospatial value '{path}' is declared {expectedTag.Family}({expectedTag.Srid}) "
                    + $"but its Arrow payload is tagged {actualTag.Family}({actualTag.Srid})",
                    AdbcStatusCode.InvalidData);
            }

            if (outputType.TypeId == ArrowTypeId.String)
                return ConvertToEwkt(source, path).Data;

            if (outputType.TypeId != ArrowTypeId.Binary)
            {
                throw new DatabricksException(
                    $"Internal geospatial schema mismatch for '{path}'",
                    AdbcStatusCode.InternalError);
            }
            return ConvertNativeToGeoArrow(source, expectedTag, path).Data;
        }

        private static BinaryArray ConvertTextToGeoArrow(
            ArrayData source,
            GeospatialArrowType.Tag tag,
            string path)
        {
            IArrowArray values = ArrowArrayFactory.BuildArray(source);
            var output = new BinaryArray.Builder();
            int defaultSrid = tag.Srid >= 0
                ? tag.Srid
                : tag.Family == GeospatialArrowType.Family.Geography ? 4326 : 0;

            for (int row = 0; row < values.Length; row++)
            {
                if (values.IsNull(row))
                {
                    output.AppendNull();
                    continue;
                }

                string text = values is StringArray strings
                    ? strings.GetString(row)
                    : ((LargeStringArray)values).GetString(row);
                try
                {
                    byte[] wkb = GeospatialWkb.ToWkb(text, defaultSrid, out int srid);
                    ValidateFixedSrid(tag, srid, path, row);
                    output.Append(tag.Srid == -1
                        ? GeospatialWkb.ToEwkb(wkb, srid).AsSpan()
                        : wkb.AsSpan());
                }
                catch (FormatException ex)
                {
                    throw new DatabricksException(
                        $"Geospatial value '{path}' row {row} contains invalid WKT / EWKT: {ex.Message}",
                        AdbcStatusCode.InvalidData,
                        ex);
                }
            }

            return output.Build();
        }

        private static BinaryArray ConvertNativeToGeoArrow(
            ArrayData source,
            GeospatialArrowType.Tag tag,
            string path)
        {
            var values = new StructArray(source);
            if (!(values.Fields[0] is Int32Array srids)
                || !(values.Fields[1] is BinaryArray wkbs))
            {
                throw new DatabricksException(
                    $"Geospatial value '{path}' has malformed native Arrow children",
                    AdbcStatusCode.InvalidData);
            }

            var output = new BinaryArray.Builder();
            for (int row = 0; row < values.Length; row++)
            {
                if (values.IsNull(row))
                {
                    output.AppendNull();
                    continue;
                }
                if (srids.IsNull(row) || wkbs.IsNull(row))
                {
                    throw new DatabricksException(
                        $"Geospatial value '{path}' row {row} has a null child under a non-null struct",
                        AdbcStatusCode.InvalidData);
                }

                int srid = srids.GetValue(row)!.Value;
                ValidateFixedSrid(tag, srid, path, row);
                ReadOnlySpan<byte> wkb = wkbs.GetBytes(row);
                if (tag.Srid == -1)
                {
                    try
                    {
                        output.Append(GeospatialWkb.ToEwkb(wkb, srid).AsSpan());
                    }
                    catch (FormatException ex)
                    {
                        throw new DatabricksException(
                            $"Geospatial value '{path}' row {row} contains invalid WKB: {ex.Message}",
                            AdbcStatusCode.InvalidData,
                            ex);
                    }
                }
                else
                {
                    output.Append(wkb);
                }
            }
            return output.Build();
        }

        private static void ValidateFixedSrid(
            GeospatialArrowType.Tag tag,
            int actualSrid,
            string path,
            int row)
        {
            if (tag.Srid != -1 && actualSrid != tag.Srid)
            {
                throw new DatabricksException(
                    $"Geospatial value '{path}' row {row} has SRID {actualSrid}, "
                    + $"but its declared type requires SRID {tag.Srid}",
                    AdbcStatusCode.InvalidData);
            }
        }

        private static StringArray ConvertToEwkt(ArrayData source, string path)
        {
            var values = new StructArray(source);
            if (!(values.Fields[0] is Int32Array srids)
                || !(values.Fields[1] is BinaryArray wkbs))
            {
                throw new DatabricksException(
                    $"Geospatial value '{path}' has malformed native Arrow children",
                    AdbcStatusCode.InvalidData);
            }

            var output = new StringArray.Builder();
            for (int row = 0; row < values.Length; row++)
            {
                if (values.IsNull(row))
                {
                    output.AppendNull();
                    continue;
                }
                if (srids.IsNull(row) || wkbs.IsNull(row))
                {
                    throw new DatabricksException(
                        $"Geospatial value '{path}' row {row} has a null child under a non-null struct",
                        AdbcStatusCode.InvalidData);
                }

                int srid = srids.GetValue(row)!.Value;
                string wkt;
                try
                {
                    wkt = GeospatialWkb.ToWkt(wkbs.GetBytes(row));
                }
                catch (FormatException ex)
                {
                    throw new DatabricksException(
                        $"Geospatial value '{path}' row {row} contains invalid WKB: {ex.Message}",
                        AdbcStatusCode.InvalidData,
                        ex);
                }

                output.Append(srid == 0
                    ? wkt
                    : $"SRID={srid.ToString(CultureInfo.InvariantCulture)};{wkt}");
            }
            return output.Build();
        }

        private static StringArray ConvertLargeStringToString(ArrayData source)
        {
            var values = (LargeStringArray)ArrowArrayFactory.BuildArray(source);
            var output = new StringArray.Builder();
            for (int row = 0; row < values.Length; row++)
            {
                if (values.IsNull(row))
                    output.AppendNull();
                else
                    output.Append(values.GetString(row));
            }
            return output.Build();
        }

    }
}
