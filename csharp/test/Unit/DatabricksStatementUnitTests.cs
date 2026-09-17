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
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Types;
using AdbcDrivers.HiveServer2.Spark;
using AdbcDrivers.Databricks;
using Xunit;
using OperationType = AdbcDrivers.Databricks.Telemetry.Proto.Operation.Types.Type;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    /// <summary>
    /// Unit tests for DatabricksStatement class methods.
    /// </summary>
    public class DatabricksStatementTests
    {
        /// <summary>
        /// Creates a minimal DatabricksStatement for testing internal methods.
        /// </summary>
        private DatabricksStatement CreateStatement()
        {
            var properties = new Dictionary<string, string>
            {
                [SparkParameters.HostName] = "test.databricks.com",
                [SparkParameters.Token] = "test-token"
            };

            // Create connection directly without opening database
            var connection = new DatabricksConnection(properties);
            return new DatabricksStatement(connection);
        }

        /// <summary>
        /// Helper method to access private confOverlay field using reflection.
        /// </summary>
        private Dictionary<string, string>? GetConfOverlay(DatabricksStatement statement)
        {
            var field = typeof(DatabricksStatement).GetField("confOverlay",
                BindingFlags.NonPublic | BindingFlags.Instance);
            return (Dictionary<string, string>?)field?.GetValue(statement);
        }

        /// <summary>
        /// Tests that query_tags parameter is captured and added to confOverlay.
        /// </summary>
        [Fact]
        public void SetOption_WithQueryTags_AddsToConfOverlay()
        {
            // Arrange
            using var statement = CreateStatement();

            // Act
            statement.SetOption(DatabricksParameters.QueryTags, "team:engineering,app:myapp");

            // Assert
            var confOverlay = GetConfOverlay(statement);
            Assert.NotNull(confOverlay);
            Assert.Single(confOverlay);
            Assert.Equal("team:engineering,app:myapp", confOverlay["query_tags"]);
        }

        /// <summary>
        /// Tests that parameters without query_tags don't get added to confOverlay.
        /// </summary>
        [Fact]
        public void SetOption_WithoutQueryTags_DoesNotAddToConfOverlay()
        {
            // Arrange
            using var statement = CreateStatement();

            // Act
            statement.SetOption(DatabricksParameters.UseCloudFetch, "true");

            // Assert
            var confOverlay = GetConfOverlay(statement);
            Assert.True(confOverlay == null || confOverlay.Count == 0);
        }

        /// <summary>
        /// Tests that query_tags works alongside regular parameters.
        /// </summary>
        [Fact]
        public void SetOption_MixedQueryTagsAndRegularParameters_BothWork()
        {
            // Arrange
            using var statement = CreateStatement();

            // Act
            statement.SetOption(DatabricksParameters.QueryTags, "k1:v1,k2:v2");
            statement.SetOption(DatabricksParameters.UseCloudFetch, "false");

            // Assert - Check conf overlay has query_tags
            var confOverlay = GetConfOverlay(statement);
            Assert.NotNull(confOverlay);
            Assert.Single(confOverlay);
            Assert.Equal("k1:v1,k2:v2", confOverlay["query_tags"]);

            // Assert - Regular parameter was set
            Assert.False(statement.UseCloudFetch);
        }

        /// <summary>
        /// Tests that confOverlay dictionary is initially null before any conf overlay parameters are set.
        /// </summary>
        [Fact]
        public void CreateStatement_ConfOverlayInitiallyNull()
        {
            // Arrange & Act
            using var statement = CreateStatement();

            // Assert
            var confOverlay = GetConfOverlay(statement);
            Assert.Null(confOverlay);
        }

        /// <summary>
        /// Tests that unrecognized options are silently dropped instead of throwing (PECO-2952).
        /// </summary>
        [Fact]
        public void SetOption_UnrecognizedKey_DoesNotThrow()
        {
            using var statement = CreateStatement();
            statement.SetOption("adbc.databricks.unknown_future_option", "some_value");
        }

        [Theory]
        [InlineData("getcatalogs", OperationType.ListCatalogs)]
        [InlineData("getschemas", OperationType.ListSchemas)]
        [InlineData("gettables", OperationType.ListTables)]
        [InlineData("getcolumns", OperationType.ListColumns)]
        [InlineData("getcolumnsextended", OperationType.ListColumns)]
        [InlineData("gettabletypes", OperationType.ListTableTypes)]
        [InlineData("getprimarykeys", OperationType.ListPrimaryKeys)]
        [InlineData("getcrossreference", OperationType.ListCrossReferences)]
        public void GetMetadataOperationType_ReturnsCorrectType(string command, OperationType expected)
        {
            Assert.Equal(expected, DatabricksStatement.GetMetadataOperationType(command));
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("SELECT 1")]
        [InlineData("unknown_command")]
        public void GetMetadataOperationType_ReturnsNull_ForNonMetadataCommands(string? command)
        {
            Assert.Null(DatabricksStatement.GetMetadataOperationType(command));
        }

        [Theory]
        [InlineData("GETCATALOGS")]
        [InlineData("GetCatalogs")]
        [InlineData("GetTables")]
        public void GetMetadataOperationType_IsCaseInsensitive(string command)
        {
            Assert.NotNull(DatabricksStatement.GetMetadataOperationType(command));
        }

        /// <summary>
        /// Invokes the private static DatabricksStatement.BuildSparkParameters via reflection,
        /// unwrapping the reflection exception wrapper so callers observe the real exception.
        /// </summary>
        private static object? InvokeBuildSparkParameters(RecordBatch batch)
        {
            var method = typeof(DatabricksStatement).GetMethod("BuildSparkParameters",
                BindingFlags.NonPublic | BindingFlags.Static);
            Assert.NotNull(method);
            try
            {
                return method!.Invoke(null, new object[] { batch, batch.Schema });
            }
            catch (TargetInvocationException ex) when (ex.InnerException != null)
            {
                throw ex.InnerException;
            }
        }

        private static StringArray SingleStringColumn(params string[] values)
        {
            var builder = new StringArray.Builder();
            foreach (var value in values)
            {
                builder.Append(value);
            }
            return builder.Build();
        }

        /// <summary>
        /// A zero-row parameter batch must be rejected with a clear error rather than
        /// indexing past the end of a zero-length Arrow array.
        /// </summary>
        [Fact]
        public void BuildSparkParameters_EmptyBatch_Throws()
        {
            var schema = new Schema(new[] { new Field("p", StringType.Default, true) }, null);
            var batch = new RecordBatch(schema, new IArrowArray[] { SingleStringColumn() }, 0);

            Assert.Throws<NotSupportedException>(() => InvokeBuildSparkParameters(batch));
        }

        /// <summary>
        /// A multi-row parameter batch must be rejected explicitly rather than silently
        /// dropping rows 1..N.
        /// </summary>
        [Fact]
        public void BuildSparkParameters_MultiRowBatch_Throws()
        {
            var schema = new Schema(new[] { new Field("p", StringType.Default, true) }, null);
            var batch = new RecordBatch(schema, new IArrowArray[] { SingleStringColumn("a", "b") }, 2);

            Assert.Throws<NotSupportedException>(() => InvokeBuildSparkParameters(batch));
        }

        /// <summary>
        /// A single-row parameter batch is the supported shape and maps each column to a
        /// named TSparkParameter.
        /// </summary>
        [Fact]
        public void BuildSparkParameters_SingleRowBatch_Succeeds()
        {
            var schema = new Schema(new[] { new Field("p", StringType.Default, true) }, null);
            var batch = new RecordBatch(schema, new IArrowArray[] { SingleStringColumn("value") }, 1);

            var result = InvokeBuildSparkParameters(batch);

            Assert.NotNull(result);
            var parameters = (System.Collections.IList)result!;
            Assert.Single(parameters);
        }

        /// <summary>
        /// A null-valued parameter must be forwarded with a declared VOID type and no value
        /// so the server binds SQL NULL rather than treating it as an unbound placeholder,
        /// mirroring the JDBC driver (inferDatabricksType(null) => VOID; the type is always set).
        /// </summary>
        [Fact]
        public void BuildSparkParameters_NullValue_SetsVoidTypeAndNoValue()
        {
            var schema = new Schema(new[] { new Field("p", StringType.Default, true) }, null);
            var nullColumn = new StringArray.Builder().AppendNull().Build();
            var batch = new RecordBatch(schema, new IArrowArray[] { nullColumn }, 1);

            var result = InvokeBuildSparkParameters(batch);

            Assert.NotNull(result);
            var parameters = (System.Collections.IList)result!;
            var parameter = Assert.Single(parameters.Cast<Apache.Hive.Service.Rpc.Thrift.TSparkParameter>().ToList());
            Assert.Equal("p", parameter.Name);
            Assert.Equal("VOID", parameter.Type);
            Assert.Null(parameter.Value);
        }

        /// <summary>
        /// A UInt64 value above Int64.MaxValue must map to DECIMAL(20,0) (not signed BIGINT, and
        /// not a bare DECIMAL — which resolves to DECIMAL(10,0) in Spark and would overflow) so
        /// the full 20-digit ulong range survives the server-side cast without overflow.
        /// </summary>
        [Fact]
        public void BuildSparkParameters_UInt64AboveInt64Max_MapsToDecimal()
        {
            const ulong value = ulong.MaxValue; // 18446744073709551615, > Int64.MaxValue
            var schema = new Schema(new[] { new Field("p", UInt64Type.Default, true) }, null);
            var column = new UInt64Array.Builder().Append(value).Build();
            var batch = new RecordBatch(schema, new IArrowArray[] { column }, 1);

            var result = InvokeBuildSparkParameters(batch);

            Assert.NotNull(result);
            var parameters = (System.Collections.IList)result!;
            var parameter = Assert.Single(parameters.Cast<Apache.Hive.Service.Rpc.Thrift.TSparkParameter>().ToList());
            Assert.Equal("DECIMAL(20,0)", parameter.Type);
            Assert.Equal(value.ToString(System.Globalization.CultureInfo.InvariantCulture), parameter.Value.StringValue);
        }

        /// <summary>
        /// Decimal128/Decimal256 parameters must carry the fully-qualified DECIMAL(precision,scale)
        /// type derived from the Arrow decimal type — a bare DECIMAL means DECIMAL(10,0) in Spark,
        /// which silently rounds a fractional value and overflows values with >10 integer digits.
        /// Mirrors the JDBC driver's getDecimalTypeString.
        /// </summary>
        [Fact]
        public void BuildSparkParameters_Decimal_EmitsPrecisionAndScale()
        {
            // Decimal128(38,10): a value with a fractional scale that a bare DECIMAL(10,0) would round.
            var dec128Type = new Decimal128Type(38, 10);
            var dec128Column = new Decimal128Array.Builder(dec128Type).Append(123.45m).Build();
            var dec128Param = BuildSingleParameter(dec128Type, dec128Column);
            Assert.Equal("DECIMAL(38,10)", dec128Param.Type);
            Assert.Equal("123.4500000000", dec128Param.Value.StringValue);

            // Decimal256(50,4): precision above 38 would be truncated by a bare DECIMAL.
            var dec256Type = new Decimal256Type(50, 4);
            var dec256Column = new Decimal256Array.Builder(dec256Type).Append(9.9999m).Build();
            var dec256Param = BuildSingleParameter(dec256Type, dec256Column);
            Assert.Equal("DECIMAL(50,4)", dec256Param.Type);
            Assert.Equal("9.9999", dec256Param.Value.StringValue);
        }

        /// <summary>
        /// Builds a single-row, single-column parameter batch from the supplied Arrow array
        /// and returns the one TSparkParameter produced by BuildSparkParameters, so per-type
        /// mapping/encoding assertions stay terse.
        /// </summary>
        private static Apache.Hive.Service.Rpc.Thrift.TSparkParameter BuildSingleParameter(IArrowType type, IArrowArray column)
        {
            var schema = new Schema(new[] { new Field("p", type, true) }, null);
            var batch = new RecordBatch(schema, new IArrowArray[] { column }, 1);

            var result = InvokeBuildSparkParameters(batch);

            Assert.NotNull(result);
            var parameters = (System.Collections.IList)result!;
            return Assert.Single(parameters.Cast<Apache.Hive.Service.Rpc.Thrift.TSparkParameter>().ToList());
        }

        /// <summary>
        /// Locks in the Arrow-to-Databricks scalar type mapping and invariant string value
        /// encoding (the Type + Value.StringValue that reach TExecuteStatementReq.Parameters)
        /// so the named-parameter path has a CI-runnable regression gate independent of the
        /// live-warehouse E2E tests. Mirrors the JDBC driver's SQL-type-name + string-form mapping.
        /// </summary>
        [Fact]
        public void BuildSparkParameters_ScalarTypes_MapTypeAndEncodeValue()
        {
            void AssertMapping(IArrowType type, IArrowArray column, string expectedType, string expectedValue)
            {
                var parameter = BuildSingleParameter(type, column);
                Assert.Equal("p", parameter.Name);
                Assert.Equal(expectedType, parameter.Type);
                Assert.Equal(expectedValue, parameter.Value.StringValue);
            }

            AssertMapping(StringType.Default, new StringArray.Builder().Append("hello world").Build(), "STRING", "hello world");
            AssertMapping(BooleanType.Default, new BooleanArray.Builder().Append(true).Build(), "BOOLEAN", "true");
            AssertMapping(BooleanType.Default, new BooleanArray.Builder().Append(false).Build(), "BOOLEAN", "false");
            AssertMapping(Int8Type.Default, new Int8Array.Builder().Append((sbyte)-7).Build(), "TINYINT", "-7");
            AssertMapping(Int16Type.Default, new Int16Array.Builder().Append((short)1234).Build(), "SMALLINT", "1234");
            AssertMapping(Int32Type.Default, new Int32Array.Builder().Append(100000).Build(), "INT", "100000");
            AssertMapping(Int64Type.Default, new Int64Array.Builder().Append(9000000000L).Build(), "BIGINT", "9000000000");
            AssertMapping(UInt8Type.Default, new UInt8Array.Builder().Append((byte)200).Build(), "SMALLINT", "200");
            AssertMapping(UInt16Type.Default, new UInt16Array.Builder().Append((ushort)60000).Build(), "INT", "60000");
            AssertMapping(UInt32Type.Default, new UInt32Array.Builder().Append(4000000000U).Build(), "BIGINT", "4000000000");
        }

        /// <summary>
        /// Floating-point values must be encoded losslessly so no precision is lost before
        /// the server casts the string back to FLOAT/DOUBLE (the default "G" format truncates
        /// on net472/netstandard2.0). Double uses "G17" and Single uses "G9" — both ".ToString("R")"
        /// can fail to round-trip when compiled for x64 (net472/netstandard2.0), so the assertions
        /// verify an actual parse-back round-trip rather than comparing against a format specifier.
        /// </summary>
        [Fact]
        public void BuildSparkParameters_FloatingPoint_UsesRoundTripEncoding()
        {
            var invariant = System.Globalization.CultureInfo.InvariantCulture;

            const double doubleValue = 1.5;
            var doubleParam = BuildSingleParameter(DoubleType.Default, new DoubleArray.Builder().Append(doubleValue).Build());
            Assert.Equal("DOUBLE", doubleParam.Type);
            Assert.Equal(doubleValue, double.Parse(doubleParam.Value.StringValue, invariant));

            const float floatValue = 0.1f;
            var floatParam = BuildSingleParameter(FloatType.Default, new FloatArray.Builder().Append(floatValue).Build());
            Assert.Equal("FLOAT", floatParam.Type);
            Assert.Equal("0.100000001", floatParam.Value.StringValue);
            Assert.Equal(floatValue, float.Parse(floatParam.Value.StringValue, invariant));
        }

        /// <summary>
        /// Every column in a multi-column single-row batch becomes a named TSparkParameter,
        /// preserving both field name and per-column type mapping (the shape the E2E
        /// TypedNamedParameters test round-trips against a live warehouse).
        /// </summary>
        [Fact]
        public void BuildSparkParameters_MultipleColumns_MapsEachNamedParameter()
        {
            var schema = new Schema(
                new[]
                {
                    new Field("n", Int64Type.Default, true),
                    new Field("d", DoubleType.Default, true),
                    new Field("b", BooleanType.Default, true),
                },
                null);
            var batch = new RecordBatch(
                schema,
                new IArrowArray[]
                {
                    new Int64Array.Builder().Append(41L).Build(),
                    new DoubleArray.Builder().Append(1.5).Build(),
                    new BooleanArray.Builder().Append(true).Build(),
                },
                1);

            var result = InvokeBuildSparkParameters(batch);

            Assert.NotNull(result);
            var parameters = ((System.Collections.IList)result!)
                .Cast<Apache.Hive.Service.Rpc.Thrift.TSparkParameter>()
                .ToList();
            Assert.Equal(3, parameters.Count);

            Assert.Equal("n", parameters[0].Name);
            Assert.Equal("BIGINT", parameters[0].Type);
            Assert.Equal("41", parameters[0].Value.StringValue);

            Assert.Equal("d", parameters[1].Name);
            Assert.Equal("DOUBLE", parameters[1].Type);
            Assert.Equal((1.5).ToString("R", System.Globalization.CultureInfo.InvariantCulture), parameters[1].Value.StringValue);

            Assert.Equal("b", parameters[2].Name);
            Assert.Equal("BOOLEAN", parameters[2].Type);
            Assert.Equal("true", parameters[2].Value.StringValue);
        }

        /// <summary>
        /// Reads the private _boundParameters field (now the materialized TSparkParameter
        /// list) so the binding lifecycle can be asserted.
        /// </summary>
        private static object? GetBoundParameters(DatabricksStatement statement)
        {
            var field = typeof(DatabricksStatement).GetField("_boundParameters",
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(field);
            return field!.GetValue(statement);
        }

        /// <summary>
        /// Invokes the protected DatabricksStatement.SetStatementProperties via reflection,
        /// unwrapping the reflection exception wrapper so callers observe the real exception.
        /// </summary>
        private static void InvokeSetStatementProperties(
            DatabricksStatement statement,
            Apache.Hive.Service.Rpc.Thrift.TExecuteStatementReq request)
        {
            var method = typeof(DatabricksStatement).GetMethod("SetStatementProperties",
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(method);
            try
            {
                method!.Invoke(statement, new object[] { request });
            }
            catch (TargetInvocationException ex) when (ex.InnerException != null)
            {
                throw ex.InnerException;
            }
        }

        /// <summary>
        /// A bound parameter batch persists across repeated executions of the same query, so
        /// prepared-statement-style reuse (Bind once, ExecuteQuery in a loop) and post-failure
        /// retries (re-ExecuteQuery without re-Bind) both resend the parameters — matching the
        /// usual ADBC/JDBC contract where a binding survives until it is replaced (Issue #648).
        /// </summary>
        [Fact]
        public void Bind_ParametersPersistAcrossRepeatedExecutionsOfSameQuery()
        {
            using var statement = CreateStatement();
            statement.SqlQuery = "SELECT :p1 AS v";
            var schema = new Schema(new[] { new Field("p1", StringType.Default, true) }, null);
            var batch = new RecordBatch(schema, new IArrowArray[] { SingleStringColumn("value") }, 1);

            statement.Bind(batch, schema);
            Assert.NotNull(GetBoundParameters(statement));

            // First execution forwards the bound parameters and keeps the captured batch.
            var firstRequest = new Apache.Hive.Service.Rpc.Thrift.TExecuteStatementReq();
            InvokeSetStatementProperties(statement, firstRequest);
            Assert.NotNull(GetBoundParameters(statement));
            Assert.NotNull(firstRequest.Parameters);
            var firstParam = Assert.Single(firstRequest.Parameters);
            Assert.Equal("p1", firstParam.Name);

            // A subsequent execution of the SAME query (no re-Bind) re-ships the parameters.
            var secondRequest = new Apache.Hive.Service.Rpc.Thrift.TExecuteStatementReq();
            InvokeSetStatementProperties(statement, secondRequest);
            Assert.NotNull(secondRequest.Parameters);
            var secondParam = Assert.Single(secondRequest.Parameters);
            Assert.Equal("p1", secondParam.Name);
        }

        /// <summary>
        /// Reassigning SqlQuery drops the binding: a batch bound for one query must not leak
        /// onto a later, unrelated query when the statement is reused, so a follow-up query
        /// that never bound its own parameters ships none (reviewer finding on Issue #648).
        /// </summary>
        [Fact]
        public void Bind_ParametersAreDroppedWhenSqlQueryIsReassigned()
        {
            using var statement = CreateStatement();
            statement.SqlQuery = "SELECT :p1 AS v";
            var schema = new Schema(new[] { new Field("p1", StringType.Default, true) }, null);
            var batch = new RecordBatch(schema, new IArrowArray[] { SingleStringColumn("value") }, 1);

            statement.Bind(batch, schema);
            Assert.NotNull(GetBoundParameters(statement));

            // Reassigning the query text drops the binding (binding follows the query).
            statement.SqlQuery = "SELECT 1 AS v";
            Assert.Null(GetBoundParameters(statement));

            // The follow-up query with no Bind of its own must ship no parameters.
            var request = new Apache.Hive.Service.Rpc.Thrift.TExecuteStatementReq();
            InvokeSetStatementProperties(statement, request);
            Assert.True(request.Parameters == null || request.Parameters.Count == 0);
        }

        /// <summary>
        /// The bound parameters are materialized (copied to string form) at Bind time, so
        /// the statement retains no reference to the caller's RecordBatch or its backing
        /// Arrow buffers. Disposing the bound batch immediately after Bind must not affect
        /// later executions: they still ship the correct values and never touch disposed
        /// Arrow arrays (reviewer finding on Issue #648 — ownership/lifetime contract).
        /// </summary>
        [Fact]
        public void Bind_ParametersSurviveDisposalOfBoundBatch()
        {
            using var statement = CreateStatement();
            statement.SqlQuery = "SELECT :p1 AS v";
            var schema = new Schema(new[] { new Field("p1", StringType.Default, true) }, null);
            var batch = new RecordBatch(schema, new IArrowArray[] { SingleStringColumn("value") }, 1);

            statement.Bind(batch, schema);

            // The caller disposes the batch (and its Arrow buffers) right after Bind —
            // exactly the prepared-statement reuse pattern the binding is designed for.
            batch.Dispose();

            // A later execution must still forward the captured value without reading the
            // now-disposed Arrow arrays (no ObjectDisposedException / stale data).
            var request = new Apache.Hive.Service.Rpc.Thrift.TExecuteStatementReq();
            InvokeSetStatementProperties(statement, request);
            Assert.NotNull(request.Parameters);
            var parameter = Assert.Single(request.Parameters);
            Assert.Equal("p1", parameter.Name);
            Assert.Equal("STRING", parameter.Type);
            Assert.Equal("value", parameter.Value.StringValue);
        }

        /// <summary>
        /// The ADBC Bind(batch, schema) contract treats the passed-in schema as the
        /// authoritative structure of the batch. A caller may build the RecordBatch from
        /// bare arrays whose own batch.Schema has empty/default field names while supplying
        /// the intended ":name" placeholder names via the schema argument. The placeholder
        /// names must be taken from that schema argument, not from batch.Schema (reviewer
        /// finding on Issue #648).
        /// </summary>
        [Fact]
        public void Bind_PlaceholderNamesComeFromSuppliedSchemaNotBatchSchema()
        {
            using var statement = CreateStatement();
            statement.SqlQuery = "SELECT :p1 AS v";

            // The batch carries an empty field name; the intended name lives on the
            // schema passed to Bind.
            var batchSchema = new Schema(new[] { new Field("", StringType.Default, true) }, null);
            var batch = new RecordBatch(batchSchema, new IArrowArray[] { SingleStringColumn("value") }, 1);
            var bindSchema = new Schema(new[] { new Field("p1", StringType.Default, true) }, null);

            statement.Bind(batch, bindSchema);

            var request = new Apache.Hive.Service.Rpc.Thrift.TExecuteStatementReq();
            InvokeSetStatementProperties(statement, request);
            var parameter = Assert.Single(request.Parameters);
            Assert.Equal("p1", parameter.Name);
            Assert.Equal("value", parameter.Value.StringValue);
        }

        /// <summary>
        /// A schema whose field count disagrees with the batch's column count cannot be
        /// paired name-to-value, so Bind rejects it rather than binding mismatched names
        /// and values (reviewer finding on Issue #648).
        /// </summary>
        [Fact]
        public void Bind_SchemaColumnCountMismatch_Throws()
        {
            using var statement = CreateStatement();
            statement.SqlQuery = "SELECT :p1 AS v";

            var batchSchema = new Schema(new[] { new Field("p1", StringType.Default, true) }, null);
            var batch = new RecordBatch(batchSchema, new IArrowArray[] { SingleStringColumn("value") }, 1);
            var bindSchema = new Schema(
                new[] { new Field("p1", StringType.Default, true), new Field("p2", StringType.Default, true) },
                null);

            Assert.Throws<ArgumentException>(() => statement.Bind(batch, bindSchema));
        }
    }
}
