/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*        http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Concurrent;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Reader.CloudFetch;
using AdbcDrivers.HiveServer2.Hive2;
using Apache.Arrow.Adbc.Tracing;
using Moq;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Reader.CloudFetch
{
    /// <summary>
    /// Regression test for the CloudFetch dispose/cancel hang. The reader parks on
    /// <see cref="IDownloadResult.DownloadCompletedTask"/> for a chunk that was enqueued while its
    /// download is still in flight. On statement cancel / connection dispose the pipeline token
    /// tears the download down, and the downloader must complete that task (with an
    /// <see cref="OperationCanceledException"/>) so the reader unblocks promptly instead of parking
    /// on a download that will never finish.
    /// </summary>
    public class CloudFetchReaderCancellationTests
    {
        /// <summary>An HTTP handler whose response never arrives until its token is cancelled.</summary>
        private sealed class HangingHttpHandler : HttpMessageHandler
        {
            protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            {
                await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
                return new HttpResponseMessage(); // unreachable — the delay always cancels first
            }
        }

        [Fact]
        public async Task PipelineCancelled_WhileDownloadInFlight_CompletesDownloadResult()
        {
            // Arrange — a real downloader whose download hangs until the pipeline token fires.
            var mockStatement = new Mock<IHiveServer2Statement>();
            mockStatement.Setup(s => s.Trace).Returns(new ActivityTrace("TestActivitySource"));
            mockStatement.Setup(s => s.TraceParent).Returns((string?)null);
            mockStatement.Setup(s => s.AssemblyVersion).Returns("1.0.0");
            mockStatement.Setup(s => s.AssemblyName).Returns("TestAssembly");

            var mockFetcher = new Mock<ICloudFetchResultFetcher>();
            mockFetcher.Setup(f => f.HasError).Returns(false);
            mockFetcher.Setup(f => f.Error).Returns((Exception?)null);
            mockFetcher.Setup(f => f.StartAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
            mockFetcher.Setup(f => f.StopAsync()).Returns(Task.CompletedTask);

            var mockMemoryManager = new Mock<ICloudFetchMemoryBufferManager>();
            mockMemoryManager.Setup(m => m.AcquireMemoryAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var downloadQueue = new BlockingCollection<IDownloadResult>(new ConcurrentQueue<IDownloadResult>(), 10);
            var resultQueue = new BlockingCollection<IDownloadResult>(new ConcurrentQueue<IDownloadResult>(), 10);
            using var httpClient = new HttpClient(new HangingHttpHandler());

            var downloader = new CloudFetchDownloader(
                mockStatement.Object,
                downloadQueue,
                resultQueue,
                mockMemoryManager.Object,
                httpClient,
                mockFetcher.Object,
                maxParallelDownloads: 3,
                isLz4Compressed: false);

            var config = new CloudFetchConfiguration();
            var manager = new CloudFetchDownloadManager(
                mockFetcher.Object,
                downloader,
                mockMemoryManager.Object,
                downloadQueue,
                resultQueue,
                config);

            using var shutdownCts = new CancellationTokenSource();
            await manager.StartAsync(shutdownCts.Token);

            // Enqueue one chunk; the downloader picks it up, starts the (hanging) download, and
            // enqueues the result before the download completes — exactly the state in which the
            // reader ends up parked on DownloadCompletedTask.
            var result = new DownloadResult(
                chunkIndex: 0,
                fileUrl: "https://example.invalid/chunk0",
                startRowOffset: 0,
                rowCount: 10,
                byteCount: 100,
                expirationTime: DateTime.UtcNow.AddHours(1),
                memoryManager: mockMemoryManager.Object);
            downloadQueue.Add(result);

            // The reader dequeues the in-flight result, then waits on its download.
            IDownloadResult? dequeued = await manager.GetNextDownloadedFileAsync(CancellationToken.None);
            Assert.NotNull(dequeued);
            Assert.False(dequeued!.DownloadCompletedTask.IsCompleted, "download should still be in flight before cancel");

            // Act — simulate statement cancel / connection dispose.
            shutdownCts.Cancel();

            // Assert — the parked wait unblocks promptly with a cancellation, not a hang.
            var completed = await Task.WhenAny(dequeued.DownloadCompletedTask, Task.Delay(5000));
            Assert.Same(dequeued.DownloadCompletedTask, completed);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => dequeued.DownloadCompletedTask);

            manager.Dispose();
        }
    }
}
