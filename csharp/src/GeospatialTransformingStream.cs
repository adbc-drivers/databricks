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
            internal ColumnPlan(IArrowType logicalType, IArrowType rewriteType)
            {
                LogicalType = logicalType;
                RewriteType = rewriteType;
            }

            internal IArrowType LogicalType { get; }

            internal IArrowType RewriteType { get; }
        }

        private readonly IArrowArrayStream _inner;
        private readonly ColumnPlan?[] _plans;
        private readonly Schema _schema;
        private RecordBatch? _firstBatch;

        internal static async ValueTask<GeospatialTransformingStream> CreateAsync(
            IArrowArrayStream inner,
            bool enableGeospatialSupport,
            bool enableComplexDatatypeSupport,
            CancellationToken cancellationToken = default)
        {
            var stream = new GeospatialTransformingStream(
                inner, enableGeospatialSupport, enableComplexDatatypeSupport);
            if (System.Array.TrueForAll(stream._plans, plan => plan == null))
                return stream;

            // SEA's reported schema is manifest-derived, not the IPC schema. Read
            // one batch before publishing the output schema so non-geo siblings
            // retain their physical units/timezones. Keep it for the first read.
            RecordBatch? firstBatch = null;
            try
            {
                firstBatch = await inner.ReadNextRecordBatchAsync(cancellationToken)
                    .ConfigureAwait(false);
                if (firstBatch == null)
                    return stream;
                var reconciled = new GeospatialTransformingStream(
                    inner, enableGeospatialSupport, enableComplexDatatypeSupport, firstBatch);
                for (int i = 0; i < stream._plans.Length; i++)
                {
                    if (stream._plans[i] != null && reconciled._plans[i] == null)
                        throw ShapeMismatch(inner.Schema.GetFieldByIndex(i).Name,
                            firstBatch.Column(i).Data.DataType, stream._plans[i]!.LogicalType);
                }
                return reconciled;
            }
            catch
            {
                firstBatch?.Dispose();
                inner.Dispose();
                throw;
            }
        }

        internal GeospatialTransformingStream(
            IArrowArrayStream inner,
            bool enableGeospatialSupport,
            bool enableComplexDatatypeSupport,
            RecordBatch? firstBatch = null)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _plans = new ColumnPlan?[inner.Schema.FieldsList.Count];
            _firstBatch = firstBatch;
            if (firstBatch != null && firstBatch.ColumnCount != _plans.Length)
                throw new DatabricksException("Geospatial result column count changed", AdbcStatusCode.InvalidData);

            var fields = new List<Field>(inner.Schema.FieldsList.Count);
            for (int i = 0; i < inner.Schema.FieldsList.Count; i++)
            {
                Field field = inner.Schema.FieldsList[i];
                ColumnPlan? plan = CreatePlan(
                    field,
                    enableGeospatialSupport,
                    enableComplexDatatypeSupport,
                    firstBatch?.Column(i).Data.DataType,
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
            cancellationToken.ThrowIfCancellationRequested();
            RecordBatch? batch = _firstBatch;
            _firstBatch = null;
            batch ??= await _inner.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false);
            if (batch == null)
                return null;

            IArrowArray[]? rewritten = null;
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                ColumnPlan? plan = _plans[i];
                if (plan == null)
                    continue;

                rewritten ??= CopyColumns(batch);
                rewritten[i] = RewriteArray(
                    batch.Column(i),
                    plan.LogicalType,
                    plan.RewriteType,
                    _schema.GetFieldByIndex(i).Name);
            }

            return rewritten == null
                ? batch
                : new RecordBatch(_schema, rewritten, batch.Length);
        }

        public void Dispose()
        {
            _firstBatch?.Dispose();
            _firstBatch = null;
            _inner.Dispose();
        }

        private static ColumnPlan? CreatePlan(
            Field field,
            bool enableGeospatialSupport,
            bool enableComplexDatatypeSupport,
            IArrowType? physicalType,
            out Field outputField)
        {
            string? typeText = null;
            field.Metadata?.TryGetValue(
                ColumnMetadataHelper.ArrowMetadataKey,
                out typeText);

            IArrowType logicalType = !string.IsNullOrWhiteSpace(typeText)
                && MayContainGeospatialType(typeText!)
                ? ArrowTypeParser.MapToArrowType(
                    typeText!,
                    enableComplexDatatypeSupport: true)
                : field.DataType;

            string baseType = string.IsNullOrWhiteSpace(typeText)
                ? string.Empty
                : ColumnMetadataHelper.GetBaseTypeName(typeText!).ToUpperInvariant();
            bool serializesOuterComplexValue = !enableComplexDatatypeSupport
                && (baseType is "ARRAY" or "MAP" or "STRUCT");

            // A complex value that will become one JSON string cannot retain a binary
            // nested leaf. Render nested geo values to EWKT before the complex serializer.
            bool preserveBinary = enableGeospatialSupport && !serializesOuterComplexValue;
            // SEA reports the logical manifest type even when its IPC attachment lacks
            // geospatial tags. Thrift reports the physical UTF-8 type it sends. Use the
            // reported shape to plan conversion between those inputs and the configured
            // representation. When an outer complex value will be serialized, the manifest
            // exposes StringType, so its parsed logical shape is the only available
            // description of the incoming native attachment.
            IArrowType reportedType = physicalType
                ?? (serializesOuterComplexValue && field.DataType.TypeId == ArrowTypeId.String
                    ? logicalType
                    : field.DataType);
            Field rewriteField = RewriteGeospatialField(
                WithType(field, logicalType),
                WithType(field, reportedType),
                preserveBinary,
                out bool containsGeo);
            if (!containsGeo)
            {
                outputField = physicalType == null ? field : WithType(field, physicalType);
                return null;
            }

            // Keep this stream's schema consistent with the arrays it emits. When
            // complex support is disabled, ComplexTypeSerializingStream flattens this
            // rewritten complex field to the final JSON string in the next stage.
            outputField = rewriteField;
            return new ColumnPlan(logicalType, rewriteField.DataType);
        }

        private static bool MayContainGeospatialType(string typeText) =>
            typeText.IndexOf("GEOMETRY", StringComparison.OrdinalIgnoreCase) >= 0
            || typeText.IndexOf("GEOGRAPHY", StringComparison.OrdinalIgnoreCase) >= 0;

        private static IArrowArray[] CopyColumns(RecordBatch batch)
        {
            var arrays = new IArrowArray[batch.ColumnCount];
            for (int i = 0; i < arrays.Length; i++)
            {
                arrays[i] = batch.Column(i);
            }
            return arrays;
        }

        private static IArrowArray RewriteArray(
            IArrowArray source,
            IArrowType logicalType,
            IArrowType outputType,
            string path) =>
            ArrowArrayFactory.BuildArray(
                RewriteData(
                    source.Data,
                    logicalType,
                    outputType,
                    path,
                    parentValidity: null));

        private static ArrayData RewriteData(
            ArrayData source,
            IArrowType logicalType,
            IArrowType outputType,
            string path,
            bool[]? parentValidity)
        {
            if (GeospatialArrowType.ResolveTag(logicalType, out GeospatialArrowType.Tag logicalTag))
            {
                return RewriteGeo(source, outputType, logicalTag, path, parentValidity);
            }

            if (GeospatialArrowType.ResolveTag(source.DataType, out GeospatialArrowType.Tag physicalTag))
            {
                return RewriteGeo(source, outputType, physicalTag, path, parentValidity);
            }

            switch (logicalType)
            {
                case StructType logicalStruct when outputType is StructType outputStruct:
                    {
                        if (!(source.DataType is StructType sourceStruct)
                            || sourceStruct.Fields.Count != logicalStruct.Fields.Count
                            || outputStruct.Fields.Count != logicalStruct.Fields.Count
                            || source.Children.Length != logicalStruct.Fields.Count)
                        {
                            throw ShapeMismatch(path, source.DataType, logicalType);
                        }

                        var values = new StructArray(source);
                        bool[]? childValidity = EffectiveStructValidity(
                            values,
                            parentValidity,
                            path);
                        var children = new IArrowArray[source.Children.Length];
                        for (int i = 0; i < children.Length; i++)
                        {
                            children[i] = ArrowArrayFactory.BuildArray(
                                RewriteData(
                                    values.Fields[i].Data,
                                    logicalStruct.Fields[i].DataType,
                                    outputStruct.Fields[i].DataType,
                                    $"{path}.{logicalStruct.Fields[i].Name}",
                                    childValidity));
                        }
                        return RebuildStruct(values, outputType, children).Data;
                    }
                case ListType logicalList when outputType is ListType outputList:
                    return RewriteSingleChild(
                        source,
                        ArrowTypeId.List,
                        logicalList.ValueDataType,
                        outputList.ValueDataType,
                        outputType,
                        path,
                        parentValidity);
                case LargeListType logicalList when outputType is LargeListType outputList:
                    return RewriteSingleChild(
                        source,
                        ArrowTypeId.LargeList,
                        logicalList.ValueDataType,
                        outputList.ValueDataType,
                        outputType,
                        path,
                        parentValidity);
                case FixedSizeListType logicalList when outputType is FixedSizeListType outputList:
                    if (logicalList.ListSize != outputList.ListSize)
                        throw ShapeMismatch(path, source.DataType, logicalType);
                    return RewriteSingleChild(
                        source,
                        ArrowTypeId.FixedSizeList,
                        logicalList.ValueDataType,
                        outputList.ValueDataType,
                        outputType,
                        path,
                        parentValidity);
                case MapType logicalMap when outputType is MapType outputMap:
                    return RewriteSingleChild(
                        source,
                        ArrowTypeId.Map,
                        logicalMap.KeyValueType,
                        outputMap.KeyValueType,
                        outputType,
                        path,
                        parentValidity);
                default:
                    return RewriteLeaf(source, logicalType, outputType, path);
            }
        }

        private static ArrayData RewriteLeaf(
            ArrayData source,
            IArrowType logicalType,
            IArrowType outputType,
            string path)
        {
            // SQL TIMESTAMP has no Arrow unit or timezone qualifier. The physical
            // timestamp is authoritative, but other type/decimal mismatches still
            // indicate malformed data and must not be silently passed through.
            bool compatible = LeafTypesMatch(source.DataType, logicalType)
                || (source.DataType is TimestampType && logicalType is TimestampType)
                || (logicalType.TypeId == ArrowTypeId.String
                    && source.DataType.TypeId is ArrowTypeId.Null or ArrowTypeId.Interval
                        or ArrowTypeId.Duration or ArrowTypeId.LargeString);
            if (!compatible)
                throw ShapeMismatch(path, source.DataType, logicalType);

            if (LeafTypesMatch(source.DataType, outputType))
                return source;

            IArrowArray sourceArray = ArrowArrayFactory.BuildArray(source);
            if (outputType.TypeId == ArrowTypeId.String)
            {
                switch (source.DataType.TypeId)
                {
                    case ArrowTypeId.Null:
                        return NullColumnSerializingStream
                            .SerializeNullToStringArray(sourceArray).Data;
                    case ArrowTypeId.Interval:
                    case ArrowTypeId.Duration:
                        return IntervalSerializingStream
                            .SerializeIntervalToStringArray(sourceArray).Data;
                    case ArrowTypeId.LargeString:
                        return ConvertLargeStringToString(source).Data;
                }
            }

            throw ShapeMismatch(path, source.DataType, outputType);
        }

        private static bool LeafTypesMatch(IArrowType source, IArrowType output)
        {
            if (source.TypeId != output.TypeId)
                return false;

            if (source is TimestampType sourceTimestamp && output is TimestampType outputTimestamp)
            {
                return sourceTimestamp.Unit == outputTimestamp.Unit
                    && sourceTimestamp.Timezone == outputTimestamp.Timezone;
            }

            if (source is Decimal128Type sourceDecimal && output is Decimal128Type outputDecimal)
            {
                return sourceDecimal.Precision == outputDecimal.Precision
                    && sourceDecimal.Scale == outputDecimal.Scale;
            }

            return true;
        }

        private static ArrayData RewriteSingleChild(
            ArrayData source,
            ArrowTypeId expectedPhysicalType,
            IArrowType logicalChild,
            IArrowType outputChild,
            IArrowType outputType,
            string path,
            bool[]? parentValidity)
        {
            if (source.DataType.TypeId != expectedPhysicalType
                || source.Children == null
                || source.Children.Length != 1)
            {
                throw ShapeMismatch(path, source.DataType, outputType);
            }

            bool[]? childValidity = EffectiveChildValidity(
                source,
                expectedPhysicalType,
                parentValidity,
                path);
            ArrayData child = RewriteData(
                source.Children[0],
                logicalChild,
                outputChild,
                path,
                childValidity);
            return Retype(source, outputType, new[] { child });
        }

        private static ArrayData RewriteGeo(
            ArrayData source,
            IArrowType outputType,
            GeospatialArrowType.Tag expectedTag,
            string path,
            bool[]? parentValidity)
        {
            if (source.DataType.TypeId is ArrowTypeId.String or ArrowTypeId.LargeString)
            {
                if (outputType.TypeId == ArrowTypeId.String)
                    return source.DataType.TypeId == ArrowTypeId.String
                        ? source
                        : ConvertLargeStringToString(source).Data;

                if (outputType.TypeId != ArrowTypeId.Binary)
                {
                    throw new DatabricksException(
                        $"Internal geospatial schema mismatch for '{path}'",
                        AdbcStatusCode.InternalError);
                }
                return ConvertTextToGeoArrow(
                    source,
                    expectedTag,
                    path,
                    parentValidity).Data;
            }

            if (!GeospatialArrowType.IsStructShape(source.DataType))
            {
                throw new DatabricksException(
                    $"Geospatial value '{path}' is declared {expectedTag.Family} but its "
                    + $"Arrow type is {source.DataType.Name}; expected struct<srid:int32,wkb:binary>",
                    AdbcStatusCode.InvalidData);
            }

            if (GeospatialArrowType.ResolveTag(
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
            {
                return ConvertToEwkt(source, expectedTag, path, parentValidity).Data;
            }

            if (outputType.TypeId != ArrowTypeId.Binary)
            {
                throw new DatabricksException(
                    $"Internal geospatial schema mismatch for '{path}'",
                    AdbcStatusCode.InternalError);
            }
            return ConvertNativeToGeoArrow(
                source,
                expectedTag,
                path,
                parentValidity).Data;
        }

        private static BinaryArray ConvertTextToGeoArrow(
            ArrayData source,
            GeospatialArrowType.Tag tag,
            string path,
            bool[]? parentValidity)
        {
            IArrowArray values = ArrowArrayFactory.BuildArray(source);
            var output = new BinaryArray.Builder();
            ValidateParentValidity(parentValidity, values.Length, path);
            int defaultSrid = tag.Srid >= 0 ? tag.Srid : 0;

            for (int row = 0; row < values.Length; row++)
            {
                if (IsMasked(parentValidity, row) || values.IsNull(row))
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
            string path,
            bool[]? parentValidity)
        {
            var values = new StructArray(source);
            if (!(values.Fields[0] is Int32Array srids)
                || !(values.Fields[1] is BinaryArray wkbs))
            {
                throw new DatabricksException(
                    $"Geospatial value '{path}' has malformed native Arrow children",
                    AdbcStatusCode.InvalidData);
            }

            ValidateParentValidity(parentValidity, values.Length, path);
            var output = new BinaryArray.Builder();
            for (int row = 0; row < values.Length; row++)
            {
                if (IsMasked(parentValidity, row) || values.IsNull(row))
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
            if (actualSrid < 0)
            {
                throw new DatabricksException(
                    $"Geospatial value '{path}' row {row} has invalid negative SRID {actualSrid}",
                    AdbcStatusCode.InvalidData);
            }
            if (tag.Srid != -1 && actualSrid != tag.Srid)
            {
                throw new DatabricksException(
                    $"Geospatial value '{path}' row {row} has SRID {actualSrid}, "
                    + $"but its declared type requires SRID {tag.Srid}",
                    AdbcStatusCode.InvalidData);
            }
        }

        private static StringArray ConvertToEwkt(
            ArrayData source,
            GeospatialArrowType.Tag tag,
            string path,
            bool[]? parentValidity)
        {
            var values = new StructArray(source);
            if (!(values.Fields[0] is Int32Array srids)
                || !(values.Fields[1] is BinaryArray wkbs))
            {
                throw new DatabricksException(
                    $"Geospatial value '{path}' has malformed native Arrow children",
                    AdbcStatusCode.InvalidData);
            }

            ValidateParentValidity(parentValidity, values.Length, path);
            var output = new StringArray.Builder();
            for (int row = 0; row < values.Length; row++)
            {
                if (IsMasked(parentValidity, row) || values.IsNull(row))
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
                {
                    output.AppendNull();
                }
                else
                {
                    output.Append(values.GetString(row));
                }
            }
            return output.Build();
        }

        private static bool[]? EffectiveStructValidity(
            StructArray values,
            bool[]? parentValidity,
            string path)
        {
            ValidateParentValidity(parentValidity, values.Length, path);
            if (parentValidity == null && values.NullCount == 0)
                return null;

            var effective = new bool[values.Length];
            for (int row = 0; row < values.Length; row++)
            {
                effective[row] = !IsMasked(parentValidity, row) && !values.IsNull(row);
            }
            return effective;
        }

        private static bool[]? EffectiveChildValidity(
            ArrayData source,
            ArrowTypeId typeId,
            bool[]? parentValidity,
            string path)
        {
            IArrowArray values = ArrowArrayFactory.BuildArray(source);
            ValidateParentValidity(parentValidity, values.Length, path);

            int childLength = source.Children[0].Length;
            var effective = new bool[childLength];
            switch (typeId)
            {
                case ArrowTypeId.List:
                case ArrowTypeId.Map:
                    {
                        var list = (ListArray)values;
                        ReadOnlySpan<int> offsets = list.ValueOffsets;
                        for (int row = 0; row < list.Length; row++)
                        {
                            if (IsMasked(parentValidity, row) || list.IsNull(row))
                                continue;
                            MarkValidRange(effective, offsets[row], offsets[row + 1], path);
                        }
                        break;
                    }
                case ArrowTypeId.LargeList:
                    {
                        var list = (LargeListArray)values;
                        ReadOnlySpan<long> offsets = list.ValueOffsets;
                        for (int row = 0; row < list.Length; row++)
                        {
                            if (IsMasked(parentValidity, row) || list.IsNull(row))
                                continue;
                            MarkValidRange(effective, offsets[row], offsets[row + 1], path);
                        }
                        break;
                    }
                case ArrowTypeId.FixedSizeList:
                    {
                        var list = (FixedSizeListArray)values;
                        int listSize = ((FixedSizeListType)source.DataType).ListSize;
                        for (int row = 0; row < list.Length; row++)
                        {
                            if (IsMasked(parentValidity, row) || list.IsNull(row))
                                continue;
                            long start = ((long)source.Offset + row) * listSize;
                            MarkValidRange(effective, start, start + listSize, path);
                        }
                        break;
                    }
                default:
                    throw new DatabricksException(
                        $"Internal geospatial child-validity mismatch for '{path}'",
                        AdbcStatusCode.InternalError);
            }

            for (int i = 0; i < effective.Length; i++)
            {
                if (!effective[i])
                    return effective;
            }
            return null;
        }

        private static void MarkValidRange(
            bool[] validity,
            long start,
            long end,
            string path)
        {
            if (start < 0 || end < start || end > validity.Length)
            {
                throw new DatabricksException(
                    $"Geospatial value '{path}' has invalid nested Arrow offsets",
                    AdbcStatusCode.InvalidData);
            }
            for (long index = start; index < end; index++)
            {
                validity[(int)index] = true;
            }
        }

        private static StructArray RebuildStruct(
            StructArray source,
            IArrowType outputType,
            IArrowArray[] children)
        {
            var validity = new ArrowBuffer.BitmapBuilder();
            int nullCount = 0;
            for (int row = 0; row < source.Length; row++)
            {
                bool valid = !source.IsNull(row);
                validity.Append(valid);
                if (!valid) nullCount++;
            }

            return new StructArray(
                outputType,
                source.Length,
                children,
                nullCount == 0 ? ArrowBuffer.Empty : validity.Build(),
                nullCount);
        }

        private static void ValidateParentValidity(
            bool[]? parentValidity,
            int length,
            string path)
        {
            if (parentValidity != null && parentValidity.Length != length)
            {
                throw new DatabricksException(
                    $"Internal geospatial validity mismatch for '{path}'",
                    AdbcStatusCode.InternalError);
            }
        }

        private static bool IsMasked(bool[]? parentValidity, int row) =>
            parentValidity != null && !parentValidity[row];

        private static ArrayData Retype(
            ArrayData source,
            IArrowType outputType,
            ArrayData[] children) =>
            new ArrayData(
                outputType,
                source.Length,
                source.NullCount,
                source.Offset,
                source.Buffers,
                children,
                source.Dictionary);

        private static Field RewriteGeospatialField(
            Field logicalField,
            Field reportedField,
            bool preserveBinary,
            out bool containsGeo)
        {
            if (GeospatialArrowType.ResolveTag(
                    logicalField.DataType,
                    out GeospatialArrowType.Tag tag))
            {
                containsGeo = true;
                return preserveBinary
                    ? GeospatialArrowType.CreateGeoArrowField(reportedField, tag)
                    : WithType(reportedField, StringType.Default);
            }

            IArrowType outputType;
            switch (logicalField.DataType)
            {
                case StructType logicalStruct:
                    {
                        if (!(reportedField.DataType is StructType reportedStruct)
                            || reportedStruct.Fields.Count != logicalStruct.Fields.Count)
                        {
                            containsGeo = false;
                            return reportedField;
                        }

                        var fields = new List<Field>(logicalStruct.Fields.Count);
                        containsGeo = false;
                        for (int i = 0; i < logicalStruct.Fields.Count; i++)
                        {
                            fields.Add(RewriteGeospatialField(
                                logicalStruct.Fields[i],
                                reportedStruct.Fields[i],
                                preserveBinary,
                                out bool childContainsGeo));
                            containsGeo |= childContainsGeo;
                        }
                        outputType = containsGeo
                            ? new StructType(fields)
                            : reportedField.DataType;
                        break;
                    }
                case ListType logicalList
                    when reportedField.DataType is ListType reportedList:
                    {
                        Field value = RewriteGeospatialField(
                            logicalList.ValueField,
                            reportedList.ValueField,
                            preserveBinary,
                            out containsGeo);
                        outputType = containsGeo
                            ? new ListType(value)
                            : reportedField.DataType;
                        break;
                    }
                case LargeListType logicalList
                    when reportedField.DataType is LargeListType reportedList:
                    {
                        Field value = RewriteGeospatialField(
                            logicalList.ValueField,
                            reportedList.ValueField,
                            preserveBinary,
                            out containsGeo);
                        outputType = containsGeo
                            ? new LargeListType(value)
                            : reportedField.DataType;
                        break;
                    }
                case FixedSizeListType logicalList
                    when reportedField.DataType is FixedSizeListType reportedList
                    && logicalList.ListSize == reportedList.ListSize:
                    {
                        Field value = RewriteGeospatialField(
                            logicalList.ValueField,
                            reportedList.ValueField,
                            preserveBinary,
                            out containsGeo);
                        outputType = containsGeo
                            ? new FixedSizeListType(value, reportedList.ListSize)
                            : reportedField.DataType;
                        break;
                    }
                case MapType logicalMap
                    when reportedField.DataType is MapType reportedMap:
                    {
                        Field key = RewriteGeospatialField(
                            logicalMap.KeyField,
                            reportedMap.KeyField,
                            preserveBinary,
                            out bool keyContainsGeo);
                        Field value = RewriteGeospatialField(
                            logicalMap.ValueField,
                            reportedMap.ValueField,
                            preserveBinary,
                            out bool valueContainsGeo);
                        containsGeo = keyContainsGeo || valueContainsGeo;
                        outputType = containsGeo
                            ? new MapType(key, value, reportedMap.KeySorted)
                            : reportedField.DataType;
                        break;
                    }
                default:
                    containsGeo = false;
                    if (logicalField.DataType.TypeId == ArrowTypeId.String
                        && reportedField.DataType.TypeId is ArrowTypeId.Null or ArrowTypeId.Interval
                            or ArrowTypeId.Duration or ArrowTypeId.LargeString)
                        return WithType(reportedField, StringType.Default);
                    return reportedField;
            }

            return outputType.Equals(reportedField.DataType)
                ? reportedField
                : WithType(reportedField, outputType);
        }

        private static Field WithType(Field field, IArrowType dataType) =>
            new Field(field.Name, dataType, field.IsNullable, field.Metadata);

        private static DatabricksException ShapeMismatch(
            string path,
            IArrowType physical,
            IArrowType logical) =>
            new DatabricksException(
                $"Geospatial value '{path}' has Arrow type {physical.Name}, "
                + $"which does not match logical type {logical.Name}",
                AdbcStatusCode.InvalidData);
    }
}
