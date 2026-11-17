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

using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Reader.CloudFetch;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.HiveServer2.Hive2;
using Apache.Arrow;
using Apache.Arrow.Adbc.Tracing;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Moq;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Reader.CloudFetch
{
    // When files decode to fewer rows than the server declared, the reader records the shortfall, which statement
    // telemetry reports as CloudFetchRowCountShortfall. By default it returns what it has; with
    // adbc.databricks.fail_on_row_count_mismatch=true it throws instead.
    public class CloudFetchReaderRowCountShortfallTests
    {
        private static readonly Schema TestSchema =
            new Schema.Builder().Field(f => f.Name("id").DataType(Int64Type.Default)).Build();

        private static IDownloadResult File(long declaredRows, int actualRows)
        {
            var ms = new MemoryStream();
            using (var writer = new ArrowStreamWriter(ms, TestSchema, leaveOpen: true))
            {
                var ids = new Int64Array.Builder();
                for (int i = 0; i < actualRows; i++) ids.Append(i);
                writer.WriteRecordBatch(new RecordBatch(TestSchema, new IArrowArray[] { ids.Build() }, actualRows));
                writer.WriteEnd();
            }
            ms.Position = 0;
            var result = new Mock<IDownloadResult>();
            result.Setup(r => r.RowCount).Returns(declaredRows);
            result.Setup(r => r.DownloadCompletedTask).Returns(Task.CompletedTask);
            result.Setup(r => r.DataStream).Returns(ms);
            return result.Object;
        }

        /// <summary>Thrift when <paramref name="totalExpectedRows"/> is 0 (per-file counts), SEA otherwise.</summary>
        internal static CloudFetchReader CreateReader(long totalExpectedRows, params (long Declared, int Actual)[] files)
            => CreateReader(totalExpectedRows, failOnRowCountMismatch: false, files);

        internal static CloudFetchReader CreateReader(long totalExpectedRows, bool failOnRowCountMismatch, params (long Declared, int Actual)[] files)
        {
            var queue = new Queue<IDownloadResult>();
            foreach (var (declared, actual) in files) queue.Enqueue(File(declared, actual));
            var manager = new Mock<ICloudFetchDownloadManager>();
            // Like the real downloader, a cancelled wait returns null instead of throwing.
            manager.Setup(m => m.GetNextDownloadedFileAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync((CancellationToken ct) => ct.IsCancellationRequested || queue.Count == 0 ? null : queue.Dequeue());
            manager.Setup(m => m.GetChunkMetrics()).Returns(() => new ChunkMetrics());

            var statement = new Mock<IHiveServer2Statement>();
            statement.Setup(s => s.Trace).Returns(new ActivityTrace("TestActivitySource"));
            return new CloudFetchReader(statement.Object, TestSchema, null, manager.Object, totalExpectedRows, failOnRowCountMismatch);
        }

        internal static async Task<long> DrainAsync(IArrowArrayStream reader)
        {
            long rows = 0;
            RecordBatch? batch;
            while ((batch = await reader.ReadNextRecordBatchAsync()) != null)
            {
                using (batch) rows += batch.Length;
            }
            return rows;
        }

        [Theory]
        [InlineData(0, 875_733, 24_576, 100, 100, 24_676, 851_157)] // Thrift: incident shape, file short of its link RowCount
        [InlineData(0, 100, 100, 50, 80, 150, 0)]                   // Thrift: exact match, and padding rows beyond RowCount
        [InlineData(1_000, 500, 500, 500, 200, 700, 300)]           // SEA: total short of the manifest
        [InlineData(1_000, 500, 500, 500, 500, 1_000, 0)]           // SEA: total matches
        public async Task RecordsShortfallWithoutThrowing(
            long total, long declared1, int actual1, long declared2, int actual2, long expectedRows, long expectedShortfall)
        {
            using var reader = CreateReader(total, (declared1, actual1), (declared2, actual2));

            Assert.Equal(expectedRows, await DrainAsync(reader));
            Assert.Null(await reader.ReadNextRecordBatchAsync()); // repeated end-of-stream reads don't double count
            Assert.Equal(expectedShortfall, reader.GetChunkMetrics().RowCountShortfall);
        }

        [Fact]
        public async Task EarlyStopOrCancel_RecordsNothing()
        {
            using var thrift = CreateReader(0, (1_000, 1_000));
            using (var first = await thrift.ReadNextRecordBatchAsync()) Assert.NotNull(first);
            Assert.Equal(0, thrift.GetChunkMetrics().RowCountShortfall);

            using var sea = CreateReader(1_000, (1_000, 1_000));
            Assert.Null(await sea.ReadNextRecordBatchAsync(new CancellationToken(canceled: true)));
            Assert.Equal(0, sea.GetChunkMetrics().RowCountShortfall);
        }

        [Theory]
        [InlineData(0, 875_733, 24_576, 851_157)] // Thrift: file short of its link RowCount
        [InlineData(1_000, 1_000, 700, 300)]      // SEA: total short of the manifest
        public async Task FailOnMismatch_Shortfall_Throws(long total, long declared, int actual, long expectedShortfall)
        {
            using var reader = CreateReader(total, failOnRowCountMismatch: true, (declared, actual));

            var ex = await Assert.ThrowsAsync<DatabricksException>(() => DrainAsync(reader));
            Assert.Equal(AdbcStatusCode.InvalidData, ex.Status);
            Assert.Contains($"expected {declared}, read {actual}", ex.Message);
            Assert.Contains(DatabricksParameters.FailOnRowCountMismatch, ex.Message);
            // Telemetry still sees the shortfall, and it isn't counted again by a later read.
            Assert.Equal(expectedShortfall, reader.GetChunkMetrics().RowCountShortfall);
            if (total > 0) Assert.Null(await reader.ReadNextRecordBatchAsync());
            Assert.Equal(expectedShortfall, reader.GetChunkMetrics().RowCountShortfall);
        }

        [Fact]
        public async Task FailOnMismatch_SeaShortfall_IsNotTracedAsFileFetchError()
        {
            var events = new List<string>();
            using var listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == "TestActivitySource",
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity => { lock (events) events.AddRange(activity.Events.Select(e => e.Name)); }
            };
            ActivitySource.AddActivityListener(listener);
            using var reader = CreateReader(1_000, failOnRowCountMismatch: true, (1_000, 700));

            await Assert.ThrowsAsync<DatabricksException>(() => DrainAsync(reader));

            Assert.Contains("cloudfetch.row_count_shortfall", events);
            Assert.DoesNotContain("cloudfetch.get_next_file_error", events);
        }

        [Theory]
        [InlineData(0, 100, 100, 50, 80, 150)]          // Thrift: exact match, and padding rows beyond RowCount (trimmed)
        [InlineData(1_000, 500, 500, 500, 600, 1_000)]  // SEA: more rows than the manifest total (trimmed)
        public async Task FailOnMismatch_ExactOrExcessRows_DoesNotThrow(
            long total, long declared1, int actual1, long declared2, int actual2, long expectedRows)
        {
            using var reader = CreateReader(total, failOnRowCountMismatch: true, (declared1, actual1), (declared2, actual2));

            Assert.Equal(expectedRows, await DrainAsync(reader));
            Assert.Equal(0, reader.GetChunkMetrics().RowCountShortfall);
        }

        [Fact]
        public async Task FailOnMismatch_EarlyStopOrCancel_DoesNotThrow()
        {
            using var thrift = CreateReader(0, failOnRowCountMismatch: true, (1_000, 1_000));
            using (var first = await thrift.ReadNextRecordBatchAsync()) Assert.NotNull(first);

            using var sea = CreateReader(1_000, failOnRowCountMismatch: true, (1_000, 1_000));
            Assert.Null(await sea.ReadNextRecordBatchAsync(new CancellationToken(canceled: true)));
            Assert.Equal(0, sea.GetChunkMetrics().RowCountShortfall);
        }

        [Theory]
        [InlineData(null, false)]
        [InlineData("false", false)]
        [InlineData("true", true)]
        [InlineData("True", true)]
        public void Configuration_FailOnRowCountMismatch_ParsesFlag(string? value, bool expected)
        {
            var properties = new Dictionary<string, string>();
            if (value != null) properties[DatabricksParameters.FailOnRowCountMismatch] = value;

            Assert.Equal(expected, CloudFetchConfiguration.FromProperties(properties, TestSchema, isLz4Compressed: true).FailOnRowCountMismatch);
        }

        [Fact]
        public void Configuration_FailOnRowCountMismatch_InvalidValue_Throws()
        {
            var properties = new Dictionary<string, string> { [DatabricksParameters.FailOnRowCountMismatch] = "yes" };

            Assert.ThrowsAny<System.ArgumentException>(() => CloudFetchConfiguration.FromProperties(properties, TestSchema, isLz4Compressed: true));
        }

        [Fact]
        public void TelemetryLog_ErrorNameWithoutError_IsReported()
        {
            var ctx = new StatementTelemetryContext(new TelemetrySessionContext { SessionId = "s", WorkspaceId = 1 });
            Assert.Null(ctx.BuildTelemetryLog().ErrorInfo);

            ctx.ErrorName = "CloudFetchRowCountShortfall";
            Assert.Equal("CloudFetchRowCountShortfall", ctx.BuildTelemetryLog().ErrorInfo.ErrorName);
        }
    }
}
