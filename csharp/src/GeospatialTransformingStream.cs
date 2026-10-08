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
    /// or legacy server-rendered WKT / EWKT. Enabled mode returns the canonical
    /// native struct for both inputs. Disabled mode returns WKT / EWKT strings.
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

        internal GeospatialTransformingStream(
            IArrowArrayStream inner,
            bool enableGeospatialSupport,
            bool enableComplexDatatypeSupport)
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
                    enableComplexDatatypeSupport,
                    out IArrowType outputType);
                _plans[i] = plan;
                fields.Add(outputType.Equals(field.DataType)
                    ? field
                    : new Field(field.Name, outputType, field.IsNullable, field.Metadata));
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

        public void Dispose() => _inner.Dispose();

        private static ColumnPlan? CreatePlan(
            Field field,
            bool enableGeospatialSupport,
            bool enableComplexDatatypeSupport,
            out IArrowType outputType)
        {
            string? typeText = null;
            field.Metadata?.TryGetValue(
                ColumnMetadataHelper.ArrowMetadataKey,
                out typeText);

            IArrowType logicalType = !string.IsNullOrWhiteSpace(typeText)
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
            IArrowType reportedType = serializesOuterComplexValue
                ? logicalType
                : field.DataType;
            IArrowType rewriteType = RewriteGeospatialTypes(
                logicalType,
                reportedType,
                preserveBinary,
                out bool containsGeo);
            if (!containsGeo)
            {
                outputType = field.DataType;
                return null;
            }

            outputType = serializesOuterComplexValue
                ? StringType.Default
                : rewriteType;
            return new ColumnPlan(logicalType, rewriteType);
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

        private static IArrowArray RewriteArray(
            IArrowArray source,
            IArrowType logicalType,
            IArrowType outputType,
            string path) =>
            ArrowArrayFactory.BuildArray(
                RewriteData(source.Data, logicalType, outputType, path));

        private static ArrayData RewriteData(
            ArrayData source,
            IArrowType logicalType,
            IArrowType outputType,
            string path)
        {
            if (GeospatialArrowType.TryGetTag(logicalType, out GeospatialArrowType.Tag logicalTag))
            {
                return RewriteGeo(source, outputType, logicalTag, path);
            }

            if (GeospatialArrowType.TryGetTag(source.DataType, out GeospatialArrowType.Tag physicalTag))
            {
                return RewriteGeo(source, outputType, physicalTag, path);
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

                    var children = new ArrayData[source.Children.Length];
                    for (int i = 0; i < children.Length; i++)
                    {
                        children[i] = RewriteData(
                            source.Children[i],
                            logicalStruct.Fields[i].DataType,
                            outputStruct.Fields[i].DataType,
                            $"{path}.{logicalStruct.Fields[i].Name}");
                    }
                    return Retype(source, outputType, children);
                }
                case ListType logicalList when outputType is ListType outputList:
                    return RewriteSingleChild(
                        source,
                        ArrowTypeId.List,
                        logicalList.ValueDataType,
                        outputList.ValueDataType,
                        outputType,
                        path);
                case LargeListType logicalList when outputType is LargeListType outputList:
                    return RewriteSingleChild(
                        source,
                        ArrowTypeId.LargeList,
                        logicalList.ValueDataType,
                        outputList.ValueDataType,
                        outputType,
                        path);
                case FixedSizeListType logicalList when outputType is FixedSizeListType outputList:
                    if (logicalList.ListSize != outputList.ListSize)
                        throw ShapeMismatch(path, source.DataType, logicalType);
                    return RewriteSingleChild(
                        source,
                        ArrowTypeId.FixedSizeList,
                        logicalList.ValueDataType,
                        outputList.ValueDataType,
                        outputType,
                        path);
                case MapType logicalMap when outputType is MapType outputMap:
                    return RewriteSingleChild(
                        source,
                        ArrowTypeId.Map,
                        logicalMap.KeyValueType,
                        outputMap.KeyValueType,
                        outputType,
                        path);
                default:
                    return RewriteLeaf(source, outputType, path);
            }
        }

        private static ArrayData RewriteLeaf(
            ArrayData source,
            IArrowType outputType,
            string path)
        {
            if (source.DataType.Equals(outputType))
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

        private static ArrayData RewriteSingleChild(
            ArrayData source,
            ArrowTypeId expectedPhysicalType,
            IArrowType logicalChild,
            IArrowType outputChild,
            IArrowType outputType,
            string path)
        {
            if (source.DataType.TypeId != expectedPhysicalType
                || source.Children == null
                || source.Children.Length != 1)
            {
                throw ShapeMismatch(path, source.DataType, outputType);
            }

            ArrayData child = RewriteData(
                source.Children[0],
                logicalChild,
                outputChild,
                path);
            return Retype(source, outputType, new[] { child });
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
                    return source.DataType.TypeId == ArrowTypeId.String
                        ? source
                        : ConvertLargeStringToString(source).Data;

                if (!GeospatialArrowType.TryGetTag(
                        outputType,
                        out GeospatialArrowType.Tag textOutputTag)
                    || !textOutputTag.Equals(expectedTag))
                {
                    throw new DatabricksException(
                        $"Internal geospatial schema mismatch for '{path}'",
                        AdbcStatusCode.InternalError);
                }
                return ConvertTextToNative(
                    source,
                    (StructType)outputType,
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

            if (GeospatialArrowType.TryGetTag(source.DataType, out GeospatialArrowType.Tag actualTag)
                && !actualTag.Equals(expectedTag))
            {
                throw new DatabricksException(
                    $"Geospatial value '{path}' is declared {expectedTag.Family}({expectedTag.Srid}) "
                    + $"but its Arrow payload is tagged {actualTag.Family}({actualTag.Srid})",
                    AdbcStatusCode.InvalidData);
            }

            if (outputType.TypeId == ArrowTypeId.String)
            {
                return ConvertToEwkt(source, path).Data;
            }

            if (!GeospatialArrowType.TryGetTag(outputType, out GeospatialArrowType.Tag outputTag)
                || !outputTag.Equals(expectedTag))
            {
                throw new DatabricksException(
                    $"Internal geospatial schema mismatch for '{path}'",
                    AdbcStatusCode.InternalError);
            }
            return Retype(source, outputType, source.Children);
        }

        private static StructArray ConvertTextToNative(
            ArrayData source,
            StructType outputType,
            GeospatialArrowType.Tag tag,
            string path)
        {
            IArrowArray values = ArrowArrayFactory.BuildArray(source);
            var srids = new Int32Array.Builder();
            var wkbs = new BinaryArray.Builder();
            var validity = new ArrowBuffer.BitmapBuilder();
            int nullCount = 0;
            int defaultSrid = tag.Srid >= 0
                ? tag.Srid
                : tag.Family == GeospatialArrowType.Family.Geography ? 4326 : 0;

            for (int row = 0; row < values.Length; row++)
            {
                if (values.IsNull(row))
                {
                    // Children are non-nullable; their values are ignored when the outer
                    // struct is null, so append harmless placeholders.
                    srids.Append(0);
                    wkbs.Append(ReadOnlySpan<byte>.Empty);
                    validity.Append(false);
                    nullCount++;
                    continue;
                }

                string text = values is StringArray strings
                    ? strings.GetString(row)
                    : ((LargeStringArray)values).GetString(row);
                try
                {
                    byte[] wkb = GeospatialWkb.ToWkb(text, defaultSrid, out int srid);
                    srids.Append(srid);
                    wkbs.Append(wkb.AsSpan());
                    validity.Append(true);
                }
                catch (FormatException ex)
                {
                    throw new DatabricksException(
                        $"Geospatial value '{path}' row {row} contains invalid WKT / EWKT: {ex.Message}",
                        AdbcStatusCode.InvalidData,
                        ex);
                }
            }

            return new StructArray(
                outputType,
                values.Length,
                new IArrowArray[] { srids.Build(), wkbs.Build() },
                nullCount == 0 ? ArrowBuffer.Empty : validity.Build(),
                nullCount);
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

        private static IArrowType RewriteGeospatialTypes(
            IArrowType logicalType,
            IArrowType reportedType,
            bool preserveBinary,
            out bool containsGeo)
        {
            if (GeospatialArrowType.TryGetTag(logicalType, out _))
            {
                containsGeo = true;
                return preserveBinary ? logicalType : StringType.Default;
            }

            switch (logicalType)
            {
                case StructType logicalStruct:
                {
                    if (!(reportedType is StructType reportedStruct)
                        || reportedStruct.Fields.Count != logicalStruct.Fields.Count)
                    {
                        containsGeo = false;
                        return reportedType;
                    }

                    var fields = new List<Field>(logicalStruct.Fields.Count);
                    containsGeo = false;
                    for (int i = 0; i < logicalStruct.Fields.Count; i++)
                    {
                        fields.Add(RewriteField(
                            logicalStruct.Fields[i],
                            reportedStruct.Fields[i],
                            preserveBinary,
                            out bool childContainsGeo));
                        containsGeo |= childContainsGeo;
                    }
                    return containsGeo ? new StructType(fields) : reportedType;
                }
                case ListType logicalList when reportedType is ListType reportedList:
                {
                    Field value = RewriteField(
                        logicalList.ValueField,
                        reportedList.ValueField,
                        preserveBinary,
                        out containsGeo);
                    return containsGeo ? new ListType(value) : reportedType;
                }
                case LargeListType logicalList when reportedType is LargeListType reportedList:
                {
                    Field value = RewriteField(
                        logicalList.ValueField,
                        reportedList.ValueField,
                        preserveBinary,
                        out containsGeo);
                    return containsGeo ? new LargeListType(value) : reportedType;
                }
                case FixedSizeListType logicalList
                    when reportedType is FixedSizeListType reportedList
                    && logicalList.ListSize == reportedList.ListSize:
                {
                    Field value = RewriteField(
                        logicalList.ValueField,
                        reportedList.ValueField,
                        preserveBinary,
                        out containsGeo);
                    return containsGeo
                        ? new FixedSizeListType(value, reportedList.ListSize)
                        : reportedType;
                }
                case MapType logicalMap when reportedType is MapType reportedMap:
                {
                    Field key = RewriteField(
                        logicalMap.KeyField,
                        reportedMap.KeyField,
                        preserveBinary,
                        out bool keyContainsGeo);
                    Field value = RewriteField(
                        logicalMap.ValueField,
                        reportedMap.ValueField,
                        preserveBinary,
                        out bool valueContainsGeo);
                    containsGeo = keyContainsGeo || valueContainsGeo;
                    return containsGeo
                        ? new MapType(key, value, reportedMap.KeySorted)
                        : reportedType;
                }
                default:
                    containsGeo = false;
                    return reportedType;
            }
        }

        private static Field RewriteField(
            Field logicalField,
            Field reportedField,
            bool preserveBinary,
            out bool containsGeo)
        {
            IArrowType dataType = RewriteGeospatialTypes(
                logicalField.DataType,
                reportedField.DataType,
                preserveBinary,
                out containsGeo);
            return dataType.Equals(reportedField.DataType)
                ? reportedField
                : new Field(
                    reportedField.Name,
                    dataType,
                    reportedField.IsNullable,
                    reportedField.Metadata);
        }

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
