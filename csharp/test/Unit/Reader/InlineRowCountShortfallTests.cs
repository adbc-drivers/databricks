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

using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Reader;
using AdbcDrivers.Databricks.StatementExecution;
using AdbcDrivers.Databricks.Tests.Unit.Reader.CloudFetch;
using AdbcDrivers.HiveServer2.Hive2;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tracing;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;
using Microsoft.IO;
using Moq;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Reader
{
    // Inline (non-CloudFetch) results: rows decoded are compared with the server's declared counts
    // (Thrift: each TSparkArrowBatch.RowCount; SEA: the manifest total_row_count). A shortfall is always recorded;
    // with adbc.databricks.fail_on_row_count_mismatch=true the read throws instead of returning the partial result.
    public class InlineRowCountShortfallTests
    {
        private static readonly Schema TestSchema =
            new Schema.Builder().Field(f => f.Name("id").DataType(Int64Type.Default)).Build();

        private static RecordBatch Batch(int rows)
        {
            var ids = new Int64Array.Builder();
            for (int i = 0; i < rows; i++) ids.Append(i);
            return new RecordBatch(TestSchema, new IArrowArray[] { ids.Build() }, rows);
        }

        // ---- Thrift inline: DatabricksReader ----

        // A TSparkArrowBatch carries one Arrow record batch message, without the schema message.
        private static byte[] RecordBatchMessage(int rows)
        {
            using var schemaOnly = new MemoryStream();
            using (var writer = new ArrowStreamWriter(schemaOnly, TestSchema, leaveOpen: true)) writer.WriteStart();
            using var withBatch = new MemoryStream();
            using (var writer = new ArrowStreamWriter(withBatch, TestSchema, leaveOpen: true)) writer.WriteRecordBatch(Batch(rows));
            return withBatch.ToArray().Skip((int)schemaOnly.Length).ToArray();
        }

        internal static DatabricksReader CreateThriftInlineReader(bool failOnRowCountMismatch, params (long Declared, int Actual)[] batches)
        {
            var statement = new Mock<IHiveServer2Statement>();
            statement.Setup(s => s.Trace).Returns(new ActivityTrace("TestActivitySource"));
            TSparkDirectResults? noDirectResults = null;
            statement.Setup(s => s.TryGetDirectResults(It.IsAny<IResponse>(), out noDirectResults)).Returns(false);
            var initialResults = new TFetchResultsResp
            {
                HasMoreRows = false,
                Results = new TRowSet
                {
                    ArrowBatches = batches.Select(b => new TSparkArrowBatch { Batch = RecordBatchMessage(b.Actual), RowCount = b.Declared }).ToList()
                }
            };
            return new DatabricksReader(statement.Object, TestSchema, new Mock<IResponse>().Object, initialResults,
                isLz4Compressed: false, failOnRowCountMismatch);
        }

        [Theory]
        [InlineData(100, 100, 50, 30, 130, 20)] // second batch short of its RowCount
        [InlineData(100, 100, 50, 80, 150, 0)]  // excess rows beyond RowCount are trimmed, not a shortfall
        [InlineData(0, 40, 50, 50, 90, 0)]      // RowCount 0 means unknown: not checked
        public async Task ThriftInline_FlagOff_RecordsShortfallWithoutThrowing(
            long declared1, int actual1, long declared2, int actual2, long expectedRows, long expectedShortfall)
        {
            using var reader = CreateThriftInlineReader(false, (declared1, actual1), (declared2, actual2));

            Assert.Equal(expectedRows, await CloudFetchReaderRowCountShortfallTests.DrainAsync(reader));
            Assert.Null(await reader.ReadNextRecordBatchAsync());
            Assert.Equal(expectedShortfall, reader.RowCountShortfall);
        }

        [Fact]
        public async Task ThriftInline_FlagOn_Shortfall_Throws()
        {
            using var reader = CreateThriftInlineReader(true, (100, 100), (50, 30));

            var ex = await Assert.ThrowsAsync<DatabricksException>(() => CloudFetchReaderRowCountShortfallTests.DrainAsync(reader));
            Assert.Equal(AdbcStatusCode.InvalidData, ex.Status);
            Assert.Contains("Result has fewer rows than the server declared: expected 50, read 30", ex.Message);
            Assert.Contains(DatabricksParameters.FailOnRowCountMismatch, ex.Message);
            Assert.Equal(20, reader.RowCountShortfall);
            Assert.Null(await reader.ReadNextRecordBatchAsync()); // not counted or thrown twice
            Assert.Equal(20, reader.RowCountShortfall);
        }

        [Fact]
        public async Task ThriftInline_FlagOn_ExactOrExcessRows_DoesNotThrow()
        {
            using var reader = CreateThriftInlineReader(true, (100, 100), (50, 80));

            Assert.Equal(150, await CloudFetchReaderRowCountShortfallTests.DrainAsync(reader));
            Assert.Equal(0, reader.RowCountShortfall);
        }

        // ---- SEA inline: StatementExecutionStatement.InlineArrowStreamReader ----

        private static byte[] ArrowStream(params int[] batchRows)
        {
            using var ms = new MemoryStream();
            using (var writer = new ArrowStreamWriter(ms, TestSchema, leaveOpen: true))
            {
                foreach (int rows in batchRows) writer.WriteRecordBatch(Batch(rows));
                writer.WriteEnd();
            }
            return ms.ToArray();
        }

        private static StatementExecutionStatement.InlineArrowStreamReader CreateSeaInlineReader(
            long declaredRowCount, bool failOnRowCountMismatch, params int[] batchRows) =>
            new StatementExecutionStatement.InlineArrowStreamReader(
                new Mock<IStatementExecutionClient>().Object, "statement-id", ArrowStream(batchRows),
                isLz4Compressed: false, totalChunks: 1, new RecyclableMemoryStreamManager(), ArrayPool<byte>.Shared,
                CancellationToken.None, TestSchema, declaredRowCount, failOnRowCountMismatch);

        [Theory]
        [InlineData(150, 50)] // manifest declares more rows than the stream has
        [InlineData(100, 0)]  // exact
        [InlineData(80, 0)]   // more rows than declared (e.g. last batch of a LIMIT query): not a shortfall
        [InlineData(0, 0)]    // total_row_count unknown: not checked
        public async Task SeaInline_FlagOff_RecordsShortfallWithoutThrowing(long declared, long expectedShortfall)
        {
            using var reader = CreateSeaInlineReader(declared, false, 60, 40);

            Assert.Equal(100, await CloudFetchReaderRowCountShortfallTests.DrainAsync(reader));
            Assert.Null(await reader.ReadNextRecordBatchAsync());
            Assert.Equal(expectedShortfall, reader.RowCountShortfall);
        }

        [Fact]
        public async Task SeaInline_FlagOn_Shortfall_Throws()
        {
            using var reader = CreateSeaInlineReader(150, true, 60, 40);

            var ex = await Assert.ThrowsAsync<DatabricksException>(() => CloudFetchReaderRowCountShortfallTests.DrainAsync(reader));
            Assert.Equal(AdbcStatusCode.InvalidData, ex.Status);
            Assert.Contains("expected 150, read 100", ex.Message);
            Assert.Equal(50, reader.RowCountShortfall);
            Assert.Null(await reader.ReadNextRecordBatchAsync()); // checked once
        }

        [Theory]
        [InlineData(100)]
        [InlineData(80)]
        [InlineData(0)]
        public async Task SeaInline_FlagOn_ExactExcessOrUnknown_DoesNotThrow(long declared)
        {
            using var reader = CreateSeaInlineReader(declared, true, 60, 40);

            Assert.Equal(100, await CloudFetchReaderRowCountShortfallTests.DrainAsync(reader));
            Assert.Equal(0, reader.RowCountShortfall);
        }

        [Fact]
        public void IsFailEnabled_ParsesFlag()
        {
            Assert.False(RowCountShortfall.IsFailEnabled(null));
            Assert.False(RowCountShortfall.IsFailEnabled(new Dictionary<string, string>()));
            Assert.True(RowCountShortfall.IsFailEnabled(new Dictionary<string, string> { [DatabricksParameters.FailOnRowCountMismatch] = "true" }));
            Assert.ThrowsAny<System.ArgumentException>(() =>
                RowCountShortfall.IsFailEnabled(new Dictionary<string, string> { [DatabricksParameters.FailOnRowCountMismatch] = "yes" }));
        }
    }
}
