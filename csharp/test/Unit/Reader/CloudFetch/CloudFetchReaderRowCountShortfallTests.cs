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
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Reader.CloudFetch;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.HiveServer2.Hive2;
using Apache.Arrow;
using Apache.Arrow.Adbc.Tracing;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Moq;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.CloudFetch
{
    /// <summary>
    /// ES-2356918 follow-up: when a CloudFetch file decodes to fewer rows than the server declared,
    /// the reader keeps returning what it has (no exception) but records the shortfall in
    /// <see cref="ChunkMetrics"/> so it reaches telemetry as error_name=CloudFetchRowCountShortfall.
    /// </summary>
    public class CloudFetchReaderRowCountShortfallTests
    {
        private static readonly Schema TestSchema =
            new Schema.Builder().Field(f => f.Name("id").DataType(Int64Type.Default)).Build();

        private static Stream ArrowFile(int rows)
        {
            var ms = new MemoryStream();
            using (var writer = new ArrowStreamWriter(ms, TestSchema, leaveOpen: true))
            {
                var ids = new Int64Array.Builder();
                for (int i = 0; i < rows; i++) ids.Append(i);
                writer.WriteRecordBatch(new RecordBatch(TestSchema, new IArrowArray[] { ids.Build() }, rows));
                writer.WriteEnd();
            }
            ms.Position = 0;
            return ms;
        }

        private static IDownloadResult File(int chunkIndex, long declaredRows, int actualRows)
        {
            var result = new Mock<IDownloadResult>();
            result.Setup(r => r.ChunkIndex).Returns(chunkIndex);
            result.Setup(r => r.RowCount).Returns(declaredRows);
            result.Setup(r => r.DownloadCompletedTask).Returns(Task.CompletedTask);
            result.Setup(r => r.DataStream).Returns(ArrowFile(actualRows));
            result.Setup(r => r.Size).Returns(1024);
            return result.Object;
        }

        private static CloudFetchReader CreateReader(long totalExpectedRows, params IDownloadResult[] files)
        {
            var queue = new Queue<IDownloadResult>(files);
            var manager = new Mock<ICloudFetchDownloadManager>();
            manager.Setup(m => m.GetNextDownloadedFileAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(() => queue.Count > 0 ? queue.Dequeue() : null);
            manager.Setup(m => m.GetChunkMetrics()).Returns(() => new ChunkMetrics());

            var statement = new Mock<IHiveServer2Statement>();
            statement.Setup(s => s.Trace).Returns(new ActivityTrace("TestActivitySource"));
            statement.Setup(s => s.TraceParent).Returns((string?)null);
            statement.Setup(s => s.AssemblyVersion).Returns("1.0.0");
            statement.Setup(s => s.AssemblyName).Returns("TestAssembly");

            return new CloudFetchReader(statement.Object, TestSchema, null, manager.Object, totalExpectedRows);
        }

        private static async Task<long> DrainAsync(CloudFetchReader reader)
        {
            long rows = 0;
            RecordBatch? batch;
            while ((batch = await reader.ReadNextRecordBatchAsync()) != null)
            {
                using (batch) rows += batch.Length;
            }
            return rows;
        }

        [Fact]
        public async Task Thrift_FileShorterThanRowCount_RecordsShortfallWithoutThrowing()
        {
            // Mirrors the incident: link says 875,733 rows, file decodes to 24,576.
            using var reader = CreateReader(0, File(0, 875_733, 24_576), File(1, 100, 100));

            long rows = await DrainAsync(reader);

            Assert.Equal(24_576 + 100, rows);
            var metrics = reader.GetChunkMetrics();
            Assert.Equal(1, metrics.RowCountShortfallChunks);
            Assert.Equal(875_733 - 24_576, metrics.RowCountShortfallRows);
        }

        [Fact]
        public async Task Thrift_FileMatchingOrExceedingRowCount_RecordsNothing()
        {
            // Exact match, and a file with padding rows beyond RowCount (trimmed, not a shortfall).
            using var reader = CreateReader(0, File(0, 100, 100), File(1, 50, 80));

            long rows = await DrainAsync(reader);

            Assert.Equal(150, rows);
            Assert.Equal(0, reader.GetChunkMetrics().RowCountShortfallChunks);
            Assert.Equal(0, reader.GetChunkMetrics().RowCountShortfallRows);
        }

        [Fact]
        public async Task Thrift_ConsumerStopsEarly_RecordsNothing()
        {
            using var reader = CreateReader(0, File(0, 1_000, 1_000));

            using (var first = await reader.ReadNextRecordBatchAsync()) { Assert.NotNull(first); }

            Assert.Equal(0, reader.GetChunkMetrics().RowCountShortfallChunks);
        }

        [Fact]
        public async Task Sea_TotalShorterThanManifest_RecordsShortfallOnce()
        {
            using var reader = CreateReader(1_000, File(0, 500, 500), File(1, 500, 200));

            long rows = await DrainAsync(reader);
            Assert.Null(await reader.ReadNextRecordBatchAsync()); // repeated end-of-stream reads don't double count

            Assert.Equal(700, rows);
            var metrics = reader.GetChunkMetrics();
            Assert.Equal(1, metrics.RowCountShortfallChunks);
            Assert.Equal(300, metrics.RowCountShortfallRows);
        }

        [Fact]
        public async Task Sea_TotalMatchesManifest_RecordsNothing()
        {
            using var reader = CreateReader(1_000, File(0, 500, 500), File(1, 500, 500));

            Assert.Equal(1_000, await DrainAsync(reader));
            Assert.Equal(0, reader.GetChunkMetrics().RowCountShortfallChunks);
        }

        [Fact]
        public void TelemetryLog_WarningOnly_EmitsShortfallErrorName()
        {
            var ctx = NewContext();
            ctx.WarningName = StatementTelemetryContext.RowCountShortfallErrorName;

            var log = ctx.BuildTelemetryLog();

            Assert.Equal("CloudFetchRowCountShortfall", log.ErrorInfo.ErrorName);
        }

        [Fact]
        public void TelemetryLog_RealErrorWinsOverWarning()
        {
            var ctx = NewContext();
            ctx.WarningName = StatementTelemetryContext.RowCountShortfallErrorName;
            ctx.HasError = true;
            ctx.ErrorName = "AdbcException";

            var log = ctx.BuildTelemetryLog();

            Assert.Equal("AdbcException", log.ErrorInfo.ErrorName);
        }

        [Fact]
        public void TelemetryLog_NoWarningNoError_HasNoErrorInfo()
        {
            var log = NewContext().BuildTelemetryLog();
            Assert.Null(log.ErrorInfo);
        }

        private static StatementTelemetryContext NewContext() =>
            new StatementTelemetryContext(new TelemetrySessionContext { SessionId = "test-session", WorkspaceId = 1 });
    }
}
