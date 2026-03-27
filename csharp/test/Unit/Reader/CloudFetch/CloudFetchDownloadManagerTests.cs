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
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks;
using AdbcDrivers.Databricks.Reader.CloudFetch;
using Apache.Arrow.Adbc;
using Moq;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Reader.CloudFetch
{
    public class CloudFetchDownloadManagerTests
    {
        /// <summary>
        /// Verifies that when the downloader returns null (end of results)
        /// but the fetcher has a stored error, the download manager throws
        /// instead of silently returning null (which would cause partial data).
        /// This is the root cause of PECO-2971.
        /// </summary>
        [Fact]
        public async Task GetNextDownloadedFileAsync_DownloaderReturnsNull_FetcherHasError_ThrowsFetcherError()
        {
            // Arrange
            var fetcherError = new Exception("Thrift server error: warehouse stopped");

            var mockFetcher = new Mock<ICloudFetchResultFetcher>();
            mockFetcher.Setup(f => f.HasError).Returns(true);
            mockFetcher.Setup(f => f.Error).Returns(fetcherError);
            mockFetcher.Setup(f => f.IsCompleted).Returns(true);
            mockFetcher.Setup(f => f.StartAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
            mockFetcher.Setup(f => f.StopAsync()).Returns(Task.CompletedTask);

            var mockDownloader = new Mock<ICloudFetchDownloader>();
            mockDownloader.Setup(d => d.GetNextDownloadedFileAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync((IDownloadResult?)null);
            mockDownloader.Setup(d => d.IsCompleted).Returns(true);
            mockDownloader.Setup(d => d.StartAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
            mockDownloader.Setup(d => d.StopAsync()).Returns(Task.CompletedTask);

            var mockMemoryManager = new Mock<ICloudFetchMemoryBufferManager>();
            var downloadQueue = new BlockingCollection<IDownloadResult>();
            var resultQueue = new BlockingCollection<IDownloadResult>();
            var config = new CloudFetchConfiguration();

            var manager = new CloudFetchDownloadManager(
                mockFetcher.Object,
                mockDownloader.Object,
                mockMemoryManager.Object,
                downloadQueue,
                resultQueue,
                config);

            await manager.StartAsync();

            // Act & Assert
            var ex = await Assert.ThrowsAsync<DatabricksException>(
                () => manager.GetNextDownloadedFileAsync(CancellationToken.None));

            Assert.Same(fetcherError, ex.InnerException);
            Assert.Contains("warehouse stopped", ex.Message);

            // Cleanup
            await manager.StopAsync();
            downloadQueue.Dispose();
            resultQueue.Dispose();
        }

        /// <summary>
        /// Verifies that when the downloader returns null and the fetcher
        /// has no error, the download manager returns null normally
        /// (legitimate end of results).
        /// </summary>
        [Fact]
        public async Task GetNextDownloadedFileAsync_DownloaderReturnsNull_NoFetcherError_ReturnsNull()
        {
            // Arrange
            var mockFetcher = new Mock<ICloudFetchResultFetcher>();
            mockFetcher.Setup(f => f.HasError).Returns(false);
            mockFetcher.Setup(f => f.Error).Returns((Exception?)null);
            mockFetcher.Setup(f => f.IsCompleted).Returns(true);
            mockFetcher.Setup(f => f.StartAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
            mockFetcher.Setup(f => f.StopAsync()).Returns(Task.CompletedTask);

            var mockDownloader = new Mock<ICloudFetchDownloader>();
            mockDownloader.Setup(d => d.GetNextDownloadedFileAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync((IDownloadResult?)null);
            mockDownloader.Setup(d => d.IsCompleted).Returns(true);
            mockDownloader.Setup(d => d.StartAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
            mockDownloader.Setup(d => d.StopAsync()).Returns(Task.CompletedTask);

            var mockMemoryManager = new Mock<ICloudFetchMemoryBufferManager>();
            var downloadQueue = new BlockingCollection<IDownloadResult>();
            var resultQueue = new BlockingCollection<IDownloadResult>();
            var config = new CloudFetchConfiguration();

            var manager = new CloudFetchDownloadManager(
                mockFetcher.Object,
                mockDownloader.Object,
                mockMemoryManager.Object,
                downloadQueue,
                resultQueue,
                config);

            await manager.StartAsync();

            // Act
            var result = await manager.GetNextDownloadedFileAsync(CancellationToken.None);

            // Assert
            Assert.Null(result);

            // Cleanup
            await manager.StopAsync();
            downloadQueue.Dispose();
            resultQueue.Dispose();
        }

        /// <summary>
        /// Verifies that when the downloader returns a valid result and the
        /// fetcher has an error, the result is returned normally (the error
        /// will be surfaced on the next null return).
        /// </summary>
        [Fact]
        public async Task GetNextDownloadedFileAsync_DownloaderReturnsResult_FetcherHasError_ReturnsResult()
        {
            // Arrange
            var mockResult = new Mock<IDownloadResult>();
            var fetcherError = new Exception("warehouse stopped");

            var mockFetcher = new Mock<ICloudFetchResultFetcher>();
            mockFetcher.Setup(f => f.HasError).Returns(true);
            mockFetcher.Setup(f => f.Error).Returns(fetcherError);
            mockFetcher.Setup(f => f.IsCompleted).Returns(false);
            mockFetcher.Setup(f => f.StartAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
            mockFetcher.Setup(f => f.StopAsync()).Returns(Task.CompletedTask);

            var mockDownloader = new Mock<ICloudFetchDownloader>();
            mockDownloader.Setup(d => d.GetNextDownloadedFileAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockResult.Object);
            mockDownloader.Setup(d => d.IsCompleted).Returns(false);
            mockDownloader.Setup(d => d.StartAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
            mockDownloader.Setup(d => d.StopAsync()).Returns(Task.CompletedTask);

            var mockMemoryManager = new Mock<ICloudFetchMemoryBufferManager>();
            var downloadQueue = new BlockingCollection<IDownloadResult>();
            var resultQueue = new BlockingCollection<IDownloadResult>();
            var config = new CloudFetchConfiguration();

            var manager = new CloudFetchDownloadManager(
                mockFetcher.Object,
                mockDownloader.Object,
                mockMemoryManager.Object,
                downloadQueue,
                resultQueue,
                config);

            await manager.StartAsync();

            // Act
            var result = await manager.GetNextDownloadedFileAsync(CancellationToken.None);

            // Assert — result is returned; error will surface when downloader returns null
            Assert.Same(mockResult.Object, result);

            // Cleanup
            await manager.StopAsync();
            downloadQueue.Dispose();
            resultQueue.Dispose();
        }
    }
}
