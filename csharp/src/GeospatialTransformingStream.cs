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
    /// disabled mode returns WKT / EWKT strings. The logical SQL type is authoritative
    /// when present; the tagged struct is a fallback only when logical metadata is absent.
    /// Plain Binary WKB and untagged structs are not Databricks result contracts.
    /// </summary>
    internal sealed class GeospatialTransformingStream : IArrowArrayStream
    {
        private sealed class NodePlan
        {
            internal NodePlan(
                IArrowType outputType,
                GeospatialArrowType.Tag? tag = null,
                NodePlan?[]? children = null)
            {
                OutputType = outputType;
                Tag = tag;
                Children = children ?? System.Array.Empty<NodePlan?>();
            }

            internal IArrowType OutputType { get; }

            internal GeospatialArrowType.Tag? Tag { get; }

            internal NodePlan?[] Children { get; }
        }

        private readonly IArrowArrayStream _inner;
        private readonly NodePlan?[] _plans;
        private readonly Schema _schema;
        private RecordBatch? _firstBatch;

        /// <summary>
        /// Creates the SEA result wrapper. SEA derives its exposed schema from the
        /// manifest, so a complex column containing nested geospatial values needs
        /// one IPC batch to recover the physical container and sibling field types.
        /// Top-level geospatial columns and non-SEA readers stay lazy.
        /// </summary>
        internal static async ValueTask<GeospatialTransformingStream> CreateForStatementExecutionAsync(
            IArrowArrayStream inner,
            bool enableGeospatialSupport,
            bool enableComplexDatatypeSupport,
            CancellationToken cancellationToken = default)
        {
            if (inner == null)
                throw new ArgumentNullException(nameof(inner));

            RecordBatch? firstBatch = null;
            try
            {
                bool[] reconcileColumns = ColumnsRequiringPhysicalSchema(
                    inner.Schema,
                    enableComplexDatatypeSupport);
                if (!System.Array.Exists(reconcileColumns, reconcile => reconcile))
                {
                    return new GeospatialTransformingStream(
                        inner,
                        enableGeospatialSupport,
                        enableComplexDatatypeSupport);
                }

                firstBatch = await inner.ReadNextRecordBatchAsync(cancellationToken)
                    .ConfigureAwait(false);
                if (firstBatch == null)
                {
                    return new GeospatialTransformingStream(
                        inner,
                        enableGeospatialSupport,
                        enableComplexDatatypeSupport);
                }

                return new GeospatialTransformingStream(
                    inner,
                    enableGeospatialSupport,
                    enableComplexDatatypeSupport,
                    firstBatch,
                    reconcileColumns);
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
            bool enableComplexDatatypeSupport)
            : this(
                inner,
                enableGeospatialSupport,
                enableComplexDatatypeSupport,
                firstBatch: null,
                reconcileColumns: null)
        {
        }

        private GeospatialTransformingStream(
            IArrowArrayStream inner,
            bool enableGeospatialSupport,
            bool enableComplexDatatypeSupport,
            RecordBatch? firstBatch,
            bool[]? reconcileColumns)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _plans = new NodePlan?[inner.Schema.FieldsList.Count];
            _firstBatch = firstBatch;
            if (firstBatch != null && firstBatch.ColumnCount != _plans.Length)
            {
                throw new DatabricksException(
                    "Geospatial result column count changed",
                    AdbcStatusCode.InvalidData);
            }

            var fields = new List<Field>(inner.Schema.FieldsList.Count);
            for (int i = 0; i < inner.Schema.FieldsList.Count; i++)
            {
                Field field = inner.Schema.FieldsList[i];
                IArrowType? physicalType = firstBatch != null
                    && reconcileColumns != null
                    && reconcileColumns[i]
                    ? firstBatch.Column(i).Data.DataType
                    : null;
                _plans[i] = CreatePlan(
                    field,
                    enableGeospatialSupport,
                    enableComplexDatatypeSupport,
                    physicalType,
                    out Field outputField);
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
            if (batch.ColumnCount != _plans.Length)
            {
                throw new DatabricksException(
                    "Geospatial result column count changed",
                    AdbcStatusCode.InvalidData);
            }

            IArrowArray[]? rewritten = null;
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                NodePlan? plan = _plans[i];
                if (plan == null)
                    continue;

                rewritten ??= CopyColumns(batch);
                rewritten[i] = ArrowArrayFactory.BuildArray(
                    RewriteNode(
                        batch.Column(i).Data,
                        plan,
                        _schema.GetFieldByIndex(i).Name,
                        parentValidity: null));
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

        private static bool[] ColumnsRequiringPhysicalSchema(
            Schema schema,
            bool enableComplexDatatypeSupport)
        {
            var result = new bool[schema.FieldsList.Count];
            if (!enableComplexDatatypeSupport)
                return result;

            for (int i = 0; i < schema.FieldsList.Count; i++)
            {
                Field field = schema.FieldsList[i];
                if (field.Metadata?.TryGetValue(
                        ColumnMetadataHelper.ArrowMetadataKey,
                        out string? typeText) != true
                    || string.IsNullOrWhiteSpace(typeText)
                    || !MayContainGeospatialType(typeText))
                {
                    continue;
                }

                string baseType = ColumnMetadataHelper.GetBaseTypeName(typeText)
                    .ToUpperInvariant();
                result[i] = baseType is "ARRAY" or "MAP" or "STRUCT";
            }
            return result;
        }

        private static NodePlan? CreatePlan(
            Field field,
            bool enableGeospatialSupport,
            bool enableComplexDatatypeSupport,
            IArrowType? physicalType,
            out Field outputField)
        {
            string? typeText = null;
            bool hasLogicalMetadata = field.Metadata?.TryGetValue(
                    ColumnMetadataHelper.ArrowMetadataKey,
                    out typeText) == true
                && !string.IsNullOrWhiteSpace(typeText);
            bool logicalMayContainGeo = hasLogicalMetadata
                && MayContainGeospatialType(typeText!);

            // Logical SQL metadata is authoritative when present. Physical tags
            // are a fallback only when no logical type was provided.
            if (hasLogicalMetadata && !logicalMayContainGeo)
            {
                RejectTopLevelPhysicalGeospatialTag(field.DataType, field.Name);
                if (physicalType != null)
                    RejectTopLevelPhysicalGeospatialTag(physicalType, field.Name);
                outputField = field;
                return null;
            }

            string baseType = hasLogicalMetadata
                ? ColumnMetadataHelper.GetBaseTypeName(typeText!).ToUpperInvariant()
                : string.Empty;
            if (!enableComplexDatatypeSupport
                && (baseType is "ARRAY" or "MAP" or "STRUCT"))
            {
                // ComplexTypeSerializingStream walks the logical type while it
                // builds JSON and renders nested geospatial leaves as EWKT.
                outputField = field;
                return null;
            }

            IArrowType logicalType = logicalMayContainGeo
                ? ArrowTypeParser.MapToArrowType(
                    typeText!,
                    enableComplexDatatypeSupport: true)
                : field.DataType;
            Field physicalField = physicalType == null
                ? field
                : WithType(field, physicalType);
            return CreateNodePlan(
                WithType(field, logicalType),
                physicalField,
                enableGeospatialSupport,
                field.Name,
                out outputField);
        }

        /// <summary>
        /// Builds the output field and its matching geo-only transformation plan in
        /// one traversal. Null children are passed through without validation or copying.
        /// </summary>
        private static NodePlan? CreateNodePlan(
            Field logicalField,
            Field physicalField,
            bool enableGeospatialSupport,
            string path,
            out Field outputField)
        {
            if (GeospatialArrowType.ResolveTag(
                    logicalField.DataType,
                    out GeospatialArrowType.Tag logicalTag))
            {
                return CreateGeospatialPlan(
                    physicalField,
                    logicalTag,
                    enableGeospatialSupport,
                    path,
                    out outputField);
            }

            switch (logicalField.DataType)
            {
                case StructType logicalStruct:
                    {
                        if (!(physicalField.DataType is StructType physicalStruct)
                            || physicalStruct.Fields.Count != logicalStruct.Fields.Count)
                        {
                            if (ContainsGeospatialType(logicalField.DataType))
                            {
                                throw ShapeMismatch(
                                    path,
                                    physicalField.DataType,
                                    logicalField.DataType);
                            }
                            outputField = physicalField;
                            return null;
                        }

                        var children = new NodePlan?[logicalStruct.Fields.Count];
                        var fields = new List<Field>(logicalStruct.Fields.Count);
                        bool hasPlan = false;
                        for (int i = 0; i < logicalStruct.Fields.Count; i++)
                        {
                            string childPath = $"{path}.{logicalStruct.Fields[i].Name}";
                            children[i] = CreateNodePlan(
                                logicalStruct.Fields[i],
                                physicalStruct.Fields[i],
                                enableGeospatialSupport,
                                childPath,
                                out Field outputChild);
                            fields.Add(outputChild);
                            hasPlan |= children[i] != null;
                        }

                        if (!hasPlan)
                        {
                            outputField = physicalField;
                            return null;
                        }

                        outputField = WithType(physicalField, new StructType(fields));
                        return new NodePlan(outputField.DataType, children: children);
                    }

                case ListType logicalList:
                    {
                        if (physicalField.DataType is LargeListType or FixedSizeListType)
                        {
                            if (ContainsGeospatialType(logicalList.ValueDataType))
                                throw UnsupportedArrayEncoding(path, physicalField.DataType);
                            outputField = physicalField;
                            return null;
                        }
                        if (!(physicalField.DataType is ListType physicalList))
                        {
                            if (ContainsGeospatialType(logicalList.ValueDataType))
                            {
                                throw ShapeMismatch(
                                    path,
                                    physicalField.DataType,
                                    logicalField.DataType);
                            }
                            outputField = physicalField;
                            return null;
                        }

                        NodePlan? child = CreateNodePlan(
                            logicalList.ValueField,
                            physicalList.ValueField,
                            enableGeospatialSupport,
                            $"{path}[]",
                            out Field outputChild);
                        if (child == null)
                        {
                            outputField = physicalField;
                            return null;
                        }

                        outputField = WithType(physicalField, new ListType(outputChild));
                        return new NodePlan(
                            outputField.DataType,
                            children: new NodePlan?[] { child });
                    }

                case LargeListType logicalList:
                    if (ContainsGeospatialType(logicalList.ValueDataType)
                        || ContainsGeospatialType(physicalField.DataType))
                    {
                        throw UnsupportedArrayEncoding(path, physicalField.DataType);
                    }
                    outputField = physicalField;
                    return null;

                case FixedSizeListType logicalList:
                    if (ContainsGeospatialType(logicalList.ValueDataType)
                        || ContainsGeospatialType(physicalField.DataType))
                    {
                        throw UnsupportedArrayEncoding(path, physicalField.DataType);
                    }
                    outputField = physicalField;
                    return null;

                case MapType logicalMap:
                    {
                        if (!(physicalField.DataType is MapType physicalMap))
                        {
                            if (ContainsGeospatialType(logicalField.DataType))
                            {
                                throw ShapeMismatch(
                                    path,
                                    physicalField.DataType,
                                    logicalField.DataType);
                            }
                            outputField = physicalField;
                            return null;
                        }

                        var logicalEntries = new Field(
                            "entries",
                            logicalMap.KeyValueType,
                            nullable: false);
                        var physicalEntries = new Field(
                            "entries",
                            physicalMap.KeyValueType,
                            nullable: false);
                        NodePlan? entries = CreateNodePlan(
                            logicalEntries,
                            physicalEntries,
                            enableGeospatialSupport,
                            path,
                            out Field outputEntries);
                        if (entries == null)
                        {
                            outputField = physicalField;
                            return null;
                        }

                        StructType entriesType = (StructType)outputEntries.DataType;
                        outputField = WithType(
                            physicalField,
                            new MapType(
                                entriesType.Fields[0],
                                entriesType.Fields[1],
                                physicalMap.KeySorted));
                        return new NodePlan(
                            outputField.DataType,
                            children: new NodePlan?[] { entries });
                    }

                default:
                    outputField = physicalField;
                    return null;
            }
        }

        private static NodePlan CreateGeospatialPlan(
            Field physicalField,
            GeospatialArrowType.Tag tag,
            bool enableGeospatialSupport,
            string path,
            out Field outputField)
        {
            IArrowType physicalType = physicalField.DataType;
            if (physicalType.TypeId is not ArrowTypeId.String and not ArrowTypeId.LargeString
                && !GeospatialArrowType.IsStructShape(physicalType))
            {
                throw ShapeMismatch(path, physicalType, GeospatialArrowType.Create(tag));
            }

            if (GeospatialArrowType.ResolveTag(
                    physicalType,
                    out GeospatialArrowType.Tag physicalTag)
                && !physicalTag.Equals(tag))
            {
                throw new DatabricksException(
                    $"Geospatial value '{path}' is declared {tag.Family}({tag.Srid}) "
                    + $"but its Arrow payload is tagged {physicalTag.Family}({physicalTag.Srid})",
                    AdbcStatusCode.InvalidData);
            }

            outputField = enableGeospatialSupport
                ? GeospatialArrowType.CreateGeoArrowField(physicalField, tag)
                : WithType(physicalField, StringType.Default);
            return new NodePlan(outputField.DataType, tag);
        }

        private static bool MayContainGeospatialType(string typeText) =>
            typeText.IndexOf("GEOMETRY", StringComparison.OrdinalIgnoreCase) >= 0
            || typeText.IndexOf("GEOGRAPHY", StringComparison.OrdinalIgnoreCase) >= 0;

        private static bool ContainsGeospatialType(IArrowType type)
        {
            if (GeospatialArrowType.ResolveTag(type, out _))
                return true;

            return type switch
            {
                StructType structType => ContainsGeospatialType(structType.Fields),
                ListType listType => ContainsGeospatialType(listType.ValueDataType),
                LargeListType listType => ContainsGeospatialType(listType.ValueDataType),
                FixedSizeListType listType => ContainsGeospatialType(listType.ValueDataType),
                MapType mapType => ContainsGeospatialType(mapType.KeyField.DataType)
                    || ContainsGeospatialType(mapType.ValueField.DataType),
                _ => false,
            };
        }

        private static bool ContainsGeospatialType(IReadOnlyList<Field> fields)
        {
            foreach (Field field in fields)
            {
                if (ContainsGeospatialType(field.DataType))
                    return true;
            }
            return false;
        }

        private static void RejectTopLevelPhysicalGeospatialTag(
            IArrowType type,
            string path)
        {
            if (GeospatialArrowType.ResolveTag(type, out GeospatialArrowType.Tag tag))
                throw UnexpectedPhysicalTag(path, tag);
        }

        private static IArrowArray[] CopyColumns(RecordBatch batch)
        {
            var arrays = new IArrowArray[batch.ColumnCount];
            for (int i = 0; i < arrays.Length; i++)
                arrays[i] = batch.Column(i);
            return arrays;
        }

        private static ArrayData RewriteNode(
            ArrayData source,
            NodePlan plan,
            string path,
            bool[]? parentValidity)
        {
            if (plan.Tag.HasValue)
            {
                return RewriteGeo(
                    source,
                    plan.OutputType,
                    plan.Tag.Value,
                    path,
                    parentValidity);
            }

            switch (plan.OutputType.TypeId)
            {
                case ArrowTypeId.Struct:
                    {
                        if (!(source.DataType is StructType)
                            || source.Children.Length != plan.Children.Length)
                        {
                            throw ShapeMismatch(path, source.DataType, plan.OutputType);
                        }

                        var values = new StructArray(source);
                        bool[]? childValidity = EffectiveStructValidity(
                            values,
                            parentValidity,
                            path);
                        var children = new ArrayData[source.Children.Length];
                        StructType outputType = (StructType)plan.OutputType;
                        for (int i = 0; i < children.Length; i++)
                        {
                            NodePlan? childPlan = plan.Children[i];
                            children[i] = childPlan == null
                                ? source.Children[i]
                                : RewriteNode(
                                    source.Children[i],
                                    childPlan,
                                    $"{path}.{outputType.Fields[i].Name}",
                                    childValidity);
                        }
                        return Retype(source, plan.OutputType, children);
                    }

                case ArrowTypeId.List:
                case ArrowTypeId.Map:
                    {
                        if (source.DataType.TypeId != plan.OutputType.TypeId
                            || source.Children.Length != 1
                            || plan.Children.Length != 1
                            || plan.Children[0] == null)
                        {
                            throw ShapeMismatch(path, source.DataType, plan.OutputType);
                        }

                        bool[]? childValidity = EffectiveChildValidity(
                            source,
                            plan.OutputType.TypeId,
                            parentValidity,
                            path);
                        string childPath = plan.OutputType.TypeId == ArrowTypeId.List
                            ? $"{path}[]"
                            : path;
                        ArrayData child = RewriteNode(
                            source.Children[0],
                            plan.Children[0]!,
                            childPath,
                            childValidity);
                        return Retype(source, plan.OutputType, new[] { child });
                    }

                default:
                    throw new DatabricksException(
                        $"Internal geospatial plan mismatch for '{path}'",
                        AdbcStatusCode.InternalError);
            }
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
                {
                    // Preserve legacy server text exactly for backward compatibility.
                    // In particular, do not normalize formatting or revalidate an
                    // embedded EWKT SRID against the declared fixed-SRID type.
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
            int defaultSrid = tag.Srid >= 0
                ? tag.Srid
                : tag.Family == GeospatialArrowType.Family.Geography ? 4326 : 0;

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
            // Reuse the struct bitmap only when no outer validity mask must be applied.
            if (tag.Srid != -1 && parentValidity == null)
            {
                for (int row = 0; row < values.Length; row++)
                {
                    if (values.IsNull(row))
                        continue;
                    if (srids.IsNull(row) || wkbs.IsNull(row))
                    {
                        throw new DatabricksException(
                            $"Geospatial value '{path}' row {row} has a null child under a non-null struct",
                            AdbcStatusCode.InvalidData);
                    }
                    ValidateFixedSrid(tag, srids.GetValue(row)!.Value, path, row);
                }

                // Struct slicing keeps the child and parent offsets aligned. Reuse the
                // WKB offsets/data buffers and apply the outer struct's validity bitmap.
                if (values.Offset == wkbs.Offset)
                {
                    return new BinaryArray(
                        BinaryType.Default,
                        values.Length,
                        wkbs.ValueOffsetsBuffer,
                        wkbs.ValueBuffer,
                        values.NullBitmapBuffer,
                        values.NullCount,
                        wkbs.Offset);
                }
            }

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
                    // Materialize values when applying an outer validity mask or
                    // when parent/child offsets prevent reusing the WKB buffers.
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
            IArrowArray values = ArrowArrayFactory.BuildArray(source);
            ValidateParentValidity(parentValidity, values.Length, path);
            var output = new StringArray.Builder();
            for (int row = 0; row < values.Length; row++)
            {
                if (IsMasked(parentValidity, row) || values.IsNull(row))
                {
                    output.AppendNull();
                    continue;
                }
                output.Append(RenderEwkt(values, tag, path, row));
            }
            return output.Build();
        }

        /// <summary>
        /// Renders one logical geospatial value for complex JSON serialization.
        /// Legacy text is preserved exactly; native struct values are validated and
        /// converted to WKT/EWKT.
        /// </summary>
        internal static string RenderEwkt(
            IArrowArray source,
            GeospatialArrowType.Tag tag,
            string path,
            int row)
        {
            if (source is StringArray strings)
                return strings.GetString(row);
            if (source is LargeStringArray largeStrings)
                return largeStrings.GetString(row);

            if (!(source is StructArray values)
                || !GeospatialArrowType.IsStructShape(source.Data.DataType))
            {
                throw new DatabricksException(
                    $"Geospatial value '{path}' is declared {tag.Family} but its "
                    + $"Arrow type is {source.Data.DataType.Name}; expected struct<srid:int32,wkb:binary>",
                    AdbcStatusCode.InvalidData);
            }

            if (GeospatialArrowType.ResolveTag(
                    source.Data.DataType,
                    out GeospatialArrowType.Tag physicalTag)
                && !physicalTag.Equals(tag))
            {
                throw new DatabricksException(
                    $"Geospatial value '{path}' is declared {tag.Family}({tag.Srid}) "
                    + $"but its Arrow payload is tagged {physicalTag.Family}({physicalTag.Srid})",
                    AdbcStatusCode.InvalidData);
            }

            if (!(values.Fields[0] is Int32Array srids)
                || !(values.Fields[1] is BinaryArray wkbs))
            {
                throw new DatabricksException(
                    $"Geospatial value '{path}' has malformed native Arrow children",
                    AdbcStatusCode.InvalidData);
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

            return srid == 0
                ? wkt
                : $"SRID={srid.ToString(CultureInfo.InvariantCulture)};{wkt}";
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
                effective[row] = !IsMasked(parentValidity, row) && !values.IsNull(row);
            return effective;
        }

        private static bool[]? EffectiveChildValidity(
            ArrayData source,
            ArrowTypeId typeId,
            bool[]? parentValidity,
            string path)
        {
            if (typeId is not ArrowTypeId.List and not ArrowTypeId.Map)
            {
                throw new DatabricksException(
                    $"Internal geospatial child-validity mismatch for '{path}'",
                    AdbcStatusCode.InternalError);
            }

            var values = (ListArray)ArrowArrayFactory.BuildArray(source);
            ValidateParentValidity(parentValidity, values.Length, path);
            var effective = new bool[source.Children[0].Length];
            ReadOnlySpan<int> offsets = values.ValueOffsets;
            for (int row = 0; row < values.Length; row++)
            {
                if (IsMasked(parentValidity, row) || values.IsNull(row))
                    continue;
                MarkValidRange(effective, offsets[row], offsets[row + 1], path);
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
                validity[(int)index] = true;
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

        private static DatabricksException UnsupportedArrayEncoding(
            string path,
            IArrowType physicalType) =>
            new DatabricksException(
                $"Geospatial value '{path}' uses unsupported Arrow type {physicalType.Name}; "
                + "Databricks ARRAY results must use List",
                AdbcStatusCode.InvalidData);

        private static Field WithType(Field field, IArrowType dataType) =>
            new Field(field.Name, dataType, field.IsNullable, field.Metadata);

        private static DatabricksException UnexpectedPhysicalTag(
            string path,
            GeospatialArrowType.Tag tag) =>
            new DatabricksException(
                $"Geospatial value '{path}' is tagged {tag.Family}({tag.Srid}), "
                + "but Spark:DataType:SqlName declares it as non-geospatial",
                AdbcStatusCode.InvalidData);

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
