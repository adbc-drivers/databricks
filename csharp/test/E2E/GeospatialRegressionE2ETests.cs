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
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Apache.Arrow.Types;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E
{
    /// <summary>
    /// Read-only tests through the public Arrow API. Run with DATABRICKS_TEST_CONFIG_FILE
    /// pointing at each warehouse: protocol=rest on DBSQL (UTF-8 wire values) and Reyden
    /// (native struct wire values), and protocol=thrift on DBSQL. Expected values and WKB
    /// headers are independent of the driver's geospatial codec.
    /// </summary>
    public class GeospatialRegressionE2ETests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public GeospatialRegressionE2ETests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
        }

        public static IEnumerable<object?[]> DimensionalCases()
        {
            foreach (string? option in new[] { null, "false", "true" })
            foreach (bool geography in new[] { false, true })
            foreach (var dimension in new[] { ("Z", 1000u, 3), ("M", 2000u, 3), ("ZM", 3000u, 4) })
                yield return new object?[] { option, geography, dimension.Item1, dimension.Item2, dimension.Item3 };
        }

        [SkippableTheory]
        [MemberData(nameof(DimensionalCases))]
        public async Task FixedSridPreservesDimensionsEmptiesAndNulls(
            string? option, bool geography, string marker, uint dimension, int ordinateCount)
        {
            using var driver = new DatabricksDriver();
            using var database = driver.Open(GetProperties(option));
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();
            bool binary = option == "true";
            int srid = geography ? 4326 : 3857;
            string family = geography ? "GEOGRAPHY" : "GEOMETRY";
            string[] shapes = { "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION" };
            string coordinates = ordinateCount == 4 ? "1 2 3 4" : "1 2 3";
            string[] wkts = new[] { $"POINT {marker} ({coordinates})" }
                .Concat(shapes.Select(shape => $"{shape} {marker} EMPTY")).ToArray();
            statement.SqlQuery = "SELECT " + string.Join(", ", wkts.Select((wkt, index) =>
                geography
                    ? $"ST_GeogFromText('{wkt}') AS geo_{index}"
                    : $"ST_GeomFromText('{wkt}', {srid}) AS geo_{index}")) +
                $", CAST(NULL AS {family}({srid})) AS null_geo";

            QueryResult result = await statement.ExecuteQueryAsync();
            using var stream = result.Stream;
            Assert.NotNull(stream);
            Assert.Equal(wkts.Length + 1, stream.Schema.FieldsList.Count);
            foreach (Field field in stream.Schema.FieldsList)
                AssertField(field, binary, geography, srid);

            int rows = 0;
            while (true)
            {
                using var batch = await stream.ReadNextRecordBatchAsync();
                if (batch == null) break;
                for (int row = 0; row < batch.Length; row++)
                {
                    for (int column = 0; column < wkts.Length; column++)
                    {
                        IArrowArray values = batch.Column(column);
                        Assert.False(values.IsNull(row));
                        if (binary)
                        {
                            byte[] wkb = Assert.IsType<BinaryArray>(values).GetBytes(row).ToArray();
                            AssertDimensionalWkb(wkb, column == 0 ? 1u : (uint)column,
                                dimension, ordinateCount, empty: column != 0);
                        }
                        else
                        {
                            Assert.Equal($"SRID={srid};{wkts[column]}",
                                Assert.IsType<StringArray>(values).GetString(row));
                        }
                    }
                    IArrowArray nulls = batch.Column(wkts.Length);
                    if (binary) Assert.IsType<BinaryArray>(nulls);
                    else Assert.IsType<StringArray>(nulls);
                    Assert.True(nulls.IsNull(row));
                    rows++;
                }
            }
            Assert.Equal(1, rows);
        }

        [SkippableTheory]
        [InlineData(null)]
        [InlineData("false")]
        [InlineData("true")]
        public async Task AnySridPreservesEachRowsSrid(string? option)
        {
            using var driver = new DatabricksDriver();
            using var database = driver.Open(GetProperties(option));
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT row_id, ST_GeomFromText('POINT(1 2)', srid) AS geo " +
                "FROM VALUES (0, 0), (1, 4326), (2, 3857) AS cases(row_id, srid) ORDER BY row_id";

            QueryResult result = await statement.ExecuteQueryAsync();
            using var stream = result.Stream;
            Assert.NotNull(stream);
            Assert.Equal(2, stream.Schema.FieldsList.Count);
            bool binary = option == "true";
            AssertField(stream.Schema.GetFieldByIndex(1), binary, geography: false, srid: null);
            int[] expectedSrids = { 0, 4326, 3857 };
            int rows = 0;
            while (true)
            {
                using var batch = await stream.ReadNextRecordBatchAsync();
                if (batch == null) break;
                var ids = Assert.IsType<Int32Array>(batch.Column(0));
                for (int row = 0; row < batch.Length; row++)
                {
                    Assert.InRange(rows, 0, expectedSrids.Length - 1);
                    Assert.Equal(rows, ids.GetValue(row));
                    int srid = expectedSrids[rows];
                    IArrowArray values = batch.Column(1);
                    Assert.False(values.IsNull(row));
                    if (binary)
                    {
                        byte[] ewkb = Assert.IsType<BinaryArray>(values).GetBytes(row).ToArray();
                        Assert.Equal(25, ewkb.Length);
                        Assert.Contains(ewkb[0], new byte[] { 0, 1 });
                        Assert.Equal(0x20000001u, ReadUInt32(ewkb, 1));
                        Assert.Equal((uint)srid, ReadUInt32(ewkb, 5));
                        Assert.Equal(1d, ReadDouble(ewkb, 9));
                        Assert.Equal(2d, ReadDouble(ewkb, 17));
                    }
                    else
                    {
                        string prefix = srid == 0 ? "" : $"SRID={srid};";
                        Assert.Equal(prefix + "POINT(1 2)", Assert.IsType<StringArray>(values).GetString(row));
                    }
                    rows++;
                }
            }
            Assert.Equal(expectedSrids.Length, rows);
        }

        private Dictionary<string, string> GetProperties(string? option)
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable), "Test configuration not available");
            var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
            // Omitting the key exercises the actual connection default on each transport.
            if (option != null)
                properties[DatabricksParameters.EnableGeospatialSupport] = option;
            OutputHelper?.WriteLine($"Protocol: {TestConfiguration.Protocol}; geospatial support: {option ?? "omitted"}");
            return properties;
        }

        private static void AssertField(Field field, bool binary, bool geography, int? srid)
        {
            string family = geography ? "GEOGRAPHY" : "GEOMETRY";
            string typeSrid = srid?.ToString(CultureInfo.InvariantCulture) ?? "ANY";
            Assert.Equal($"{family}({typeSrid})", field.Metadata["Spark:DataType:SqlName"]);
            if (!binary)
            {
                Assert.IsType<StringType>(field.DataType);
                Assert.False(field.Metadata.ContainsKey("ARROW:extension:name"));
                Assert.False(field.Metadata.ContainsKey("ARROW:extension:metadata"));
                return;
            }

            Assert.IsType<BinaryType>(field.DataType);
            Assert.Equal("geoarrow.wkb", field.Metadata["ARROW:extension:name"]);
            if (!srid.HasValue)
            {
                Assert.False(field.Metadata.ContainsKey("ARROW:extension:metadata"));
                return;
            }

            using var metadata = JsonDocument.Parse(field.Metadata["ARROW:extension:metadata"]);
            Assert.Equal(typeSrid, metadata.RootElement.GetProperty("crs").GetString());
            Assert.Equal("srid", metadata.RootElement.GetProperty("crs_type").GetString());
            if (geography)
                Assert.Equal("spherical", metadata.RootElement.GetProperty("edges").GetString());
            else
                Assert.False(metadata.RootElement.TryGetProperty("edges", out _));
        }

        private static void AssertDimensionalWkb(byte[] wkb, uint shape, uint dimension, int ordinateCount, bool empty)
        {
            Assert.True(wkb.Length >= 5);
            Assert.Contains(wkb[0], new byte[] { 0, 1 });
            Assert.Equal(shape + dimension, ReadUInt32(wkb, 1));
            if (shape == 1)
            {
                Assert.Equal(5 + ordinateCount * 8, wkb.Length);
                for (int ordinate = 0; ordinate < ordinateCount; ordinate++)
                {
                    double value = ReadDouble(wkb, 5 + ordinate * 8);
                    // WKB empty points use NaNs; their sign and payload bits are unspecified.
                    if (empty) Assert.True(double.IsNaN(value));
                    else Assert.Equal(ordinate + 1d, value);
                }
            }
            else
            {
                Assert.True(empty);
                Assert.Equal(9, wkb.Length);
                Assert.Equal(0u, ReadUInt32(wkb, 5));
            }
        }

        private static uint ReadUInt32(byte[] wkb, int offset) => wkb[0] == 1
            ? BinaryPrimitives.ReadUInt32LittleEndian(wkb.AsSpan(offset, 4))
            : BinaryPrimitives.ReadUInt32BigEndian(wkb.AsSpan(offset, 4));

        private static double ReadDouble(byte[] wkb, int offset) => BitConverter.Int64BitsToDouble(wkb[0] == 1
            ? BinaryPrimitives.ReadInt64LittleEndian(wkb.AsSpan(offset, 8))
            : BinaryPrimitives.ReadInt64BigEndian(wkb.AsSpan(offset, 8)));
    }
}
