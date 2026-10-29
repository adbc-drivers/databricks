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
using System.Linq;
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
        /// <summary>
        /// Rewrite plan for one value. A leaf has <see cref="Tag"/>; a container has one
        /// entry per child, where null means the child holds no geospatial value and is
        /// passed through unchanged.
        /// </summary>
        private sealed class Node
        {
            internal Node(Field output, GeospatialArrowType.Tag? tag, Node?[] children)
            {
                Output = output;
                Tag = tag;
                Children = children;
            }

            internal Field Output { get; }

            internal GeospatialArrowType.Tag? Tag { get; }

            internal Node?[] Children { get; }
        }

        private readonly IArrowArrayStream _inner;
        private readonly bool _enableGeospatialSupport;
        private readonly Node?[] _plans;
        // Logical types of complex columns rewritten to strings before JSON
        // serialization. Their plans depend on the physical batch type.
        private readonly IArrowType?[] _serializedLogicalTypes;
        private readonly Schema _schema;
        private RecordBatch? _firstBatch;

        /// <summary>
        /// Creates the SEA result wrapper. SEA derives its schema from the manifest, so a
        /// native complex column containing geospatial values needs one IPC batch to
        /// recover the physical types of its non-geospatial children.
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
                bool needsPhysicalTypes = enableComplexDatatypeSupport
                    && inner.Schema.FieldsList.Any(f => IsComplex(LogicalTypeText(f)));
                if (needsPhysicalTypes)
                {
                    firstBatch = await inner.ReadNextRecordBatchAsync(cancellationToken)
                        .ConfigureAwait(false);
                }
                return new GeospatialTransformingStream(
                    inner, enableGeospatialSupport, enableComplexDatatypeSupport, firstBatch);
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
            _enableGeospatialSupport = enableGeospatialSupport;
            _firstBatch = firstBatch;
            int count = inner.Schema.FieldsList.Count;
            if (firstBatch != null && firstBatch.ColumnCount != count)
                throw ColumnCountChanged();

            _plans = new Node?[count];
            _serializedLogicalTypes = new IArrowType?[count];
            var fields = new List<Field>(count);
            for (int i = 0; i < count; i++)
            {
                Field field = inner.Schema.FieldsList[i];
                string? typeText = LogicalTypeText(field);
                Field output = field;
                if (typeText == null)
                {
                    // Without logical metadata, tagged geospatial structs are the fallback.
                    _plans[i] = Plan(field, field, enableGeospatialSupport, field.Name);
                }
                else if (!MayContainGeospatialType(typeText))
                {
                    if (GeospatialArrowType.ResolveTag(field.DataType, out GeospatialArrowType.Tag tag))
                        throw UnexpectedPhysicalTag(field.Name, tag);
                }
                else if (IsComplex(typeText) && !enableComplexDatatypeSupport)
                {
                    // ComplexTypeSerializingStream turns the whole value into JSON next, so
                    // the schema is unchanged and geospatial leaves become WKT / EWKT text.
                    _serializedLogicalTypes[i] = ParseLogicalType(typeText);
                }
                else
                {
                    Field physical = firstBatch == null
                        ? field
                        : WithType(field, firstBatch.Column(i).Data.DataType);
                    _plans[i] = Plan(
                        WithType(field, ParseLogicalType(typeText)),
                        physical,
                        enableGeospatialSupport,
                        field.Name);
                    output = physical;
                }
                fields.Add(_plans[i]?.Output ?? output);
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
                throw ColumnCountChanged();

            IArrowArray[]? rewritten = null;
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                Node? plan = _plans[i];
                string name = _schema.GetFieldByIndex(i).Name;
                if (plan == null && _serializedLogicalTypes[i] is IArrowType logical)
                {
                    Field field = _schema.GetFieldByIndex(i);
                    plan = Plan(
                        WithType(field, logical),
                        WithType(field, batch.Column(i).Data.DataType),
                        enableGeospatialSupport: false,
                        name);
                }
                if (plan == null)
                    continue;

                rewritten ??= CopyColumns(batch);
                rewritten[i] = ArrowArrayFactory.BuildArray(
                    Rewrite(batch.Column(i).Data, plan, name, mask: null));
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

        private static string? LogicalTypeText(Field field) =>
            field.Metadata?.TryGetValue(ColumnMetadataHelper.ArrowMetadataKey, out string? text) == true
                && !string.IsNullOrWhiteSpace(text)
                ? text
                : null;

        private static bool IsComplex(string? typeText) =>
            typeText != null
            && MayContainGeospatialType(typeText)
            && ColumnMetadataHelper.GetBaseTypeName(typeText).ToUpperInvariant() is "ARRAY" or "MAP" or "STRUCT";

        private static IArrowType ParseLogicalType(string typeText) =>
            ArrowTypeParser.MapToArrowType(typeText, enableComplexDatatypeSupport: true);

        private static bool MayContainGeospatialType(string typeText) =>
            typeText.IndexOf("GEOMETRY", StringComparison.OrdinalIgnoreCase) >= 0
            || typeText.IndexOf("GEOGRAPHY", StringComparison.OrdinalIgnoreCase) >= 0;

        private static Node Leaf(Field physical, GeospatialArrowType.Tag tag, bool enableGeospatialSupport) =>
            new Node(
                enableGeospatialSupport
                    ? GeospatialArrowType.CreateGeoArrowField(physical, tag)
                    : WithType(physical, StringType.Default),
                tag,
                System.Array.Empty<Node?>());

        /// <summary>
        /// Plans the rewrite of one value by walking the logical type and the physical
        /// type together. Returns null when the value holds no geospatial leaf.
        /// </summary>
        private static Node? Plan(Field logical, Field physical, bool enableGeospatialSupport, string path)
        {
            if (GeospatialArrowType.ResolveTag(logical.DataType, out GeospatialArrowType.Tag tag))
                return Leaf(physical, tag, enableGeospatialSupport);

            IReadOnlyList<Field> logicalChildren = Children(logical.DataType);
            if (logicalChildren.Count == 0 || !ContainsGeospatialType(logical.DataType))
                return null;
            IReadOnlyList<Field> physicalChildren = Children(physical.DataType);
            if (physical.DataType.TypeId != logical.DataType.TypeId
                || physicalChildren.Count != logicalChildren.Count)
            {
                throw ShapeMismatch(path, physical.DataType, logical.DataType);
            }

            var children = new Node?[logicalChildren.Count];
            var outputs = new Field[logicalChildren.Count];
            for (int i = 0; i < children.Length; i++)
            {
                string childPath = logical.DataType is StructType ? $"{path}.{logicalChildren[i].Name}"
                    : logical.DataType is ListType ? $"{path}[]"
                    : path;
                children[i] = Plan(logicalChildren[i], physicalChildren[i], enableGeospatialSupport, childPath);
                outputs[i] = children[i]?.Output ?? physicalChildren[i];
            }

            IArrowType outputType = physical.DataType switch
            {
                StructType => new StructType(outputs),
                ListType => new ListType(outputs[0]),
                MapType map => new MapType(
                    ((StructType)outputs[0].DataType).Fields[0],
                    ((StructType)outputs[0].DataType).Fields[1],
                    map.KeySorted),
                _ => throw ShapeMismatch(path, physical.DataType, logical.DataType),
            };
            return new Node(WithType(physical, outputType), null, children);
        }

        /// <summary>
        /// Child fields in the order of <see cref="ArrayData.Children"/>. A map has one
        /// child: its key/value entries struct. Other types are treated as leaves.
        /// </summary>
        private static IReadOnlyList<Field> Children(IArrowType type) => type switch
        {
            StructType structType => structType.Fields,
            ListType listType => new[] { listType.ValueField },
            MapType mapType => new[] { new Field("entries", mapType.KeyValueType, nullable: false) },
            _ => System.Array.Empty<Field>(),
        };

        private static bool ContainsGeospatialType(IArrowType type)
        {
            if (GeospatialArrowType.ResolveTag(type, out _))
                return true;
            foreach (Field child in Children(type))
            {
                if (ContainsGeospatialType(child.DataType))
                    return true;
            }
            return false;
        }

        private static IArrowArray[] CopyColumns(RecordBatch batch)
        {
            var arrays = new IArrowArray[batch.ColumnCount];
            for (int i = 0; i < arrays.Length; i++)
                arrays[i] = batch.Column(i);
            return arrays;
        }

        /// <summary>
        /// Applies a plan to one array. <paramref name="mask"/> marks rows whose ancestors
        /// are all valid (same length as <paramref name="source"/>); masked geospatial rows
        /// become null without being parsed, because null parents may hold placeholder bytes.
        /// </summary>
        private static ArrayData Rewrite(ArrayData source, Node plan, string path, bool[]? mask)
        {
            if (plan.Tag is GeospatialArrowType.Tag tag)
                return RewriteGeo(source, plan.Output.DataType, tag, path, mask);

            ValidateParentValidity(mask, source.Length, path);
            var children = (ArrayData[])source.Children.Clone();
            for (int i = 0; i < children.Length; i++)
            {
                if (!(plan.Children[i] is Node child))
                    continue;
                string childPath = source.DataType is StructType structType
                    ? $"{path}.{structType.Fields[i].Name}"
                    : source.DataType is ListType ? $"{path}[]" : path;
                bool[]? childMask = source.DataType is StructType
                    ? StructChildMask(source, mask, children[i].Length, path)
                    : ListChildMask(source, mask, path);
                children[i] = Rewrite(children[i], child, childPath, childMask);
            }
            return new ArrayData(
                plan.Output.DataType,
                source.Length,
                source.NullCount,
                source.Offset,
                source.Buffers,
                children,
                source.Dictionary);
        }

        /// <summary>
        /// Struct children are not sliced with their parent: row r of the struct is row
        /// <c>Offset + r</c> of each child. Rows outside the struct's window are masked.
        /// </summary>
        private static bool[]? StructChildMask(ArrayData source, bool[]? mask, int childLength, string path)
        {
            var values = new StructArray(source);
            if (source.Offset + source.Length > childLength)
                throw InvalidOffsets(path);
            if (mask == null && values.NullCount == 0 && source.Offset == 0 && childLength == source.Length)
                return null;

            var childMask = new bool[childLength];
            for (int row = 0; row < source.Length; row++)
                childMask[source.Offset + row] = !IsMasked(mask, row) && !values.IsNull(row);
            return childMask;
        }

        /// <summary>
        /// List and map offsets index the child directly. Child rows not referenced by a
        /// valid, unmasked parent row are masked.
        /// </summary>
        private static bool[]? ListChildMask(ArrayData source, bool[]? mask, string path)
        {
            var values = (ListArray)ArrowArrayFactory.BuildArray(source);
            var childMask = new bool[source.Children[0].Length];
            ReadOnlySpan<int> offsets = values.ValueOffsets;
            for (int row = 0; row < values.Length; row++)
            {
                if (IsMasked(mask, row) || values.IsNull(row))
                    continue;
                if (offsets[row] < 0 || offsets[row + 1] < offsets[row] || offsets[row + 1] > childMask.Length)
                    throw InvalidOffsets(path);
                for (int index = offsets[row]; index < offsets[row + 1]; index++)
                    childMask[index] = true;
            }
            return System.Array.TrueForAll(childMask, valid => valid) ? null : childMask;
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

        private static Field WithType(Field field, IArrowType dataType) =>
            new Field(field.Name, dataType, field.IsNullable, field.Metadata);

        private static DatabricksException ColumnCountChanged() =>
            new DatabricksException("Geospatial result column count changed", AdbcStatusCode.InvalidData);

        private static DatabricksException InvalidOffsets(string path) =>
            new DatabricksException(
                $"Geospatial value '{path}' has invalid nested Arrow offsets",
                AdbcStatusCode.InvalidData);

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
