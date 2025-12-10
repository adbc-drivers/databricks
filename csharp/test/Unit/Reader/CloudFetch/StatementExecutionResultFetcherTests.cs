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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Reader.CloudFetch;
using AdbcDrivers.Databricks.StatementExecution;
using Moq;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Reader.CloudFetch
{
    public class StatementExecutionResultFetcherTests : IDisposable
    {
        private readonly Mock<IStatementExecutionClient> _mockClient;
        private readonly Mock<ICloudFetchMemoryBufferManager> _mockMemoryManager;
        private readonly BlockingCollection<IDownloadResult> _downloadQueue;
        private readonly string _testStatementId = "test-statement-123";

        public StatementExecutionResultFetcherTests()
        {
            _mockClient = new Mock<IStatementExecutionClient>();
            _mockMemoryManager = new Mock<ICloudFetchMemoryBufferManager>();
            _downloadQueue = new BlockingCollection<IDownloadResult>();
        }

        #region Constructor Tests

        [Fact]
        public void Constructor_WithValidParameters_CreatesFetcher()
        {
            var manifest = CreateTestManifest(1);
            var fetcher = new StatementExecutionResultFetcher(
                _mockClient.Object,
                _testStatementId,
                manifest,
                initialExternalLinks: null,
                _mockMemoryManager.Object,
                _downloadQueue);

            Assert.NotNull(fetcher);
        }

        [Fact]
        public void Constructor_WithNullClient_ThrowsArgumentNullException()
        {
            var manifest = CreateTestManifest(1);

            Assert.Throws<ArgumentNullException>(() =>
                new StatementExecutionResultFetcher(
                    null!,
                    _testStatementId,
                    manifest,
                    initialExternalLinks: null,
                    _mockMemoryManager.Object,
                    _downloadQueue));
        }

        [Fact]
        public void Constructor_WithNullStatementId_ThrowsArgumentNullException()
        {
            var manifest = CreateTestManifest(1);

            Assert.Throws<ArgumentNullException>(() =>
                new StatementExecutionResultFetcher(
                    _mockClient.Object,
                    null!,
                    manifest,
                    initialExternalLinks: null,
                    _mockMemoryManager.Object,
                    _downloadQueue));
        }

        [Fact]
        public void Constructor_WithNullManifest_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() =>
                new StatementExecutionResultFetcher(
                    _mockClient.Object,
                    _testStatementId,
                    null!,
                    initialExternalLinks: null,
                    _mockMemoryManager.Object,
                    _downloadQueue));
        }

        [Fact]
        public void Constructor_WithNullMemoryManager_ThrowsArgumentNullException()
        {
            var manifest = CreateTestManifest(1);

            Assert.Throws<ArgumentNullException>(() =>
                new StatementExecutionResultFetcher(
                    _mockClient.Object,
                    _testStatementId,
                    manifest,
                    initialExternalLinks: null,
                    null!,
                    _downloadQueue));
        }

        [Fact]
        public void Constructor_WithNullDownloadQueue_ThrowsArgumentNullException()
        {
            var manifest = CreateTestManifest(1);

            Assert.Throws<ArgumentNullException>(() =>
                new StatementExecutionResultFetcher(
                    _mockClient.Object,
                    _testStatementId,
                    manifest,
                    initialExternalLinks: null,
                    _mockMemoryManager.Object,
                    null!));
        }

        #endregion

        #region Manifest-Based Fetching Tests

        [Fact]
        public async Task StartAsync_WithManifestExternalLinks_AddsDownloadResultsToQueue()
        {
            var (manifest, initialExternalLinks) = CreateTestManifestWithExternalLinks(2);

            // Setup mock to return external links for chunk 1 (chunk 0 is provided as initialExternalLinks)
            _mockClient.Setup(c => c.GetResultChunkAsync(_testStatementId, 1, It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ResultData
                {
                    ChunkIndex = 1,
                    RowOffset = 100,
                    RowCount = 100,
                    ByteCount = 1024,
                    ExternalLinks = new List<ExternalLink>
                    {
                        new ExternalLink
                        {
                            ChunkIndex = 1,
                            ExternalLinkUrl = "https://storage.example.com/result1.arrow",
                            RowOffset = 100,
                            RowCount = 100,
                            ByteCount = 1024,
                            Expiration = DateTime.UtcNow.AddHours(1).ToString("O")
                        }
                    }
                });

            var fetcher = new StatementExecutionResultFetcher(
                _mockClient.Object,
                _testStatementId,
                manifest,
                initialExternalLinks,
                _mockMemoryManager.Object,
                _downloadQueue);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await fetcher.StartAsync(cts.Token);

            // Wait for the fetcher to complete
            await WaitForFetcherCompletionAsync(fetcher, cts.Token);

            // Should have 2 download results + end guard
            var results = new List<IDownloadResult>();
            while (_downloadQueue.TryTake(out var result, TimeSpan.FromMilliseconds(100)))
            {
                results.Add(result);
            }

            // 2 external links (one per chunk) + end guard
            Assert.Equal(3, results.Count);
            Assert.IsType<EndOfResultsGuard>(results.Last());

            var downloadResults = results.Take(2).ToList();
            Assert.All(downloadResults, r => Assert.IsType<DownloadResult>(r));
        }

        [Fact]
        public async Task StartAsync_WithManifestExternalLinks_SetsCorrectProperties()
        {
            var (manifest, initialExternalLinks) = CreateTestManifestWithExternalLinks(1);
            var fetcher = new StatementExecutionResultFetcher(
                _mockClient.Object,
                _testStatementId,
                manifest,
                initialExternalLinks,
                _mockMemoryManager.Object,
                _downloadQueue);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await fetcher.StartAsync(cts.Token);
            await WaitForFetcherCompletionAsync(fetcher, cts.Token);

            Assert.True(_downloadQueue.TryTake(out var result));
            var downloadResult = Assert.IsType<DownloadResult>(result);

            Assert.Equal("https://storage.example.com/result0.arrow", downloadResult.FileUrl);
            Assert.Equal(0, downloadResult.ChunkIndex);
            Assert.Equal(0, downloadResult.StartRowOffset);
            Assert.Equal(100, downloadResult.RowCount);
            Assert.Equal(1024, downloadResult.ByteCount);
        }

        [Fact]
        public async Task StartAsync_WithHttpHeaders_PassesHeadersToDownloadResult()
        {
            var manifest = new ResultManifest
            {
                TotalChunkCount = 1,
                TotalRowCount = 100,
                Chunks = new List<ResultChunk>
                {
                    new ResultChunk
                    {
                        ChunkIndex = 0,
                        RowOffset = 0,
                        RowCount = 100,
                        ByteCount = 1024
                    }
                }
            };

            var initialExternalLinks = new List<ExternalLink>
            {
                new ExternalLink
                {
                    ChunkIndex = 0,
                    ExternalLinkUrl = "https://storage.example.com/result.arrow",
                    RowOffset = 0,
                    RowCount = 100,
                    ByteCount = 1024,
                    Expiration = DateTime.UtcNow.AddHours(1).ToString("O"),
                    HttpHeaders = new Dictionary<string, string>
                    {
                        { "x-amz-server-side-encryption-customer-algorithm", "AES256" },
                        { "x-amz-server-side-encryption-customer-key", "test-key" }
                    }
                }
            };

            var fetcher = new StatementExecutionResultFetcher(
                _mockClient.Object,
                _testStatementId,
                manifest,
                initialExternalLinks,
                _mockMemoryManager.Object,
                _downloadQueue);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await fetcher.StartAsync(cts.Token);
            await WaitForFetcherCompletionAsync(fetcher, cts.Token);

            Assert.True(_downloadQueue.TryTake(out var result));
            var downloadResult = Assert.IsType<DownloadResult>(result);

            Assert.NotNull(downloadResult.HttpHeaders);
            Assert.Equal(2, downloadResult.HttpHeaders.Count);
            Assert.Equal("AES256", downloadResult.HttpHeaders["x-amz-server-side-encryption-customer-algorithm"]);
        }

        #endregion

        #region Incremental Chunk Fetching Tests

        [Fact]
        public async Task StartAsync_WithChunksWithoutExternalLinks_FetchesFromApi()
        {
            // Manifest has chunks without external links (need incremental fetching)
            var manifest = new ResultManifest
            {
                TotalChunkCount = 2,
                TotalRowCount = 200,
                Chunks = new List<ResultChunk>
                {
                    new ResultChunk { ChunkIndex = 0, RowOffset = 0, RowCount = 100, ByteCount = 1024 },
                    new ResultChunk { ChunkIndex = 1, RowOffset = 100, RowCount = 100, ByteCount = 1024 }
                }
            };

            // Setup mock to return external links when GetResultChunkAsync is called
            _mockClient.Setup(c => c.GetResultChunkAsync(_testStatementId, 0, It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ResultData
                {
                    ChunkIndex = 0,
                    RowOffset = 0,
                    RowCount = 100,
                    ByteCount = 1024,
                    ExternalLinks = new List<ExternalLink>
                    {
                        new ExternalLink
                        {
                            ChunkIndex = 0,
                            ExternalLinkUrl = "https://storage.example.com/chunk0.arrow",
                            RowOffset = 0,
                            RowCount = 100,
                            ByteCount = 1024,
                            Expiration = DateTime.UtcNow.AddHours(1).ToString("O")
                        }
                    }
                });

            _mockClient.Setup(c => c.GetResultChunkAsync(_testStatementId, 1, It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ResultData
                {
                    ChunkIndex = 1,
                    RowOffset = 100,
                    RowCount = 100,
                    ByteCount = 1024,
                    ExternalLinks = new List<ExternalLink>
                    {
                        new ExternalLink
                        {
                            ChunkIndex = 1,
                            ExternalLinkUrl = "https://storage.example.com/chunk1.arrow",
                            RowOffset = 100,
                            RowCount = 100,
                            ByteCount = 1024,
                            Expiration = DateTime.UtcNow.AddHours(1).ToString("O")
                        }
                    }
                });

            var fetcher = new StatementExecutionResultFetcher(
                _mockClient.Object,
                _testStatementId,
                manifest,
                initialExternalLinks: null,
                _mockMemoryManager.Object,
                _downloadQueue);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await fetcher.StartAsync(cts.Token);
            await WaitForFetcherCompletionAsync(fetcher, cts.Token);

            // Verify GetResultChunkAsync was called for both chunks
            _mockClient.Verify(c => c.GetResultChunkAsync(_testStatementId, 0, It.IsAny<CancellationToken>()), Times.Once);
            _mockClient.Verify(c => c.GetResultChunkAsync(_testStatementId, 1, It.IsAny<CancellationToken>()), Times.Once);

            // Collect results
            var results = new List<IDownloadResult>();
            while (_downloadQueue.TryTake(out var result, TimeSpan.FromMilliseconds(100)))
            {
                results.Add(result);
            }

            // 2 download results + end guard
            Assert.Equal(3, results.Count);
        }

        #endregion

        #region URL Refresh Tests

        [Fact]
        public async Task RefreshUrlsAsync_CallsGetResultChunkAsync()
        {
            var (manifest, _) = CreateTestManifestWithExternalLinks(2);
            var fetcher = new StatementExecutionResultFetcher(
                _mockClient.Object,
                _testStatementId,
                manifest,
                initialExternalLinks: null,
                _mockMemoryManager.Object,
                _downloadQueue);

            _mockClient.Setup(c => c.GetResultChunkAsync(_testStatementId, 0, It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ResultData
                {
                    ChunkIndex = 0,
                    ExternalLinks = new List<ExternalLink>
                    {
                        new ExternalLink
                        {
                            ChunkIndex = 0,
                            ExternalLinkUrl = "https://storage.example.com/refreshed.arrow",
                            RowOffset = 0,
                            RowCount = 100,
                            ByteCount = 1024,
                            Expiration = DateTime.UtcNow.AddHours(1).ToString("O")
                        }
                    }
                });

            var refreshedResults = await fetcher.RefreshUrlsAsync(0, CancellationToken.None);

            _mockClient.Verify(c => c.GetResultChunkAsync(_testStatementId, 0, It.IsAny<CancellationToken>()), Times.Once);
            Assert.Single(refreshedResults);
            var downloadResult = Assert.IsType<DownloadResult>(refreshedResults.First());
            Assert.Equal("https://storage.example.com/refreshed.arrow", downloadResult.FileUrl);
        }

        [Fact]
        public async Task RefreshUrlsAsync_WithRowOffsetInMiddleOfChunk_FindsCorrectChunk()
        {
            var manifest = new ResultManifest
            {
                TotalChunkCount = 2,
                TotalRowCount = 200,
                Chunks = new List<ResultChunk>
                {
                    new ResultChunk { ChunkIndex = 0, RowOffset = 0, RowCount = 100, ByteCount = 1024 },
                    new ResultChunk { ChunkIndex = 1, RowOffset = 100, RowCount = 100, ByteCount = 1024 }
                }
            };

            var fetcher = new StatementExecutionResultFetcher(
                _mockClient.Object,
                _testStatementId,
                manifest,
                initialExternalLinks: null,
                _mockMemoryManager.Object,
                _downloadQueue);

            _mockClient.Setup(c => c.GetResultChunkAsync(_testStatementId, 1, It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ResultData
                {
                    ChunkIndex = 1,
                    ExternalLinks = new List<ExternalLink>
                    {
                        new ExternalLink
                        {
                            ChunkIndex = 1,
                            ExternalLinkUrl = "https://storage.example.com/chunk1.arrow",
                            RowOffset = 100,
                            RowCount = 100,
                            ByteCount = 1024,
                            Expiration = DateTime.UtcNow.AddHours(1).ToString("O")
                        }
                    }
                });

            // Refresh with row offset 150, which is in the middle of chunk 1
            var refreshedResults = await fetcher.RefreshUrlsAsync(150, CancellationToken.None);

            _mockClient.Verify(c => c.GetResultChunkAsync(_testStatementId, 1, It.IsAny<CancellationToken>()), Times.Once);
            Assert.Single(refreshedResults);
        }

        #endregion

        #region Error Handling Tests

        [Fact]
        public async Task StartAsync_WithApiError_SetsErrorState()
        {
            // Manifest has chunks without external links (need incremental fetching)
            var manifest = new ResultManifest
            {
                TotalChunkCount = 1,
                TotalRowCount = 100,
                Chunks = new List<ResultChunk>
                {
                    new ResultChunk { ChunkIndex = 0, RowOffset = 0, RowCount = 100, ByteCount = 1024 }
                }
            };

            _mockClient.Setup(c => c.GetResultChunkAsync(_testStatementId, 0, It.IsAny<CancellationToken>()))
                .ThrowsAsync(new DatabricksException("API Error"));

            var fetcher = new StatementExecutionResultFetcher(
                _mockClient.Object,
                _testStatementId,
                manifest,
                initialExternalLinks: null,
                _mockMemoryManager.Object,
                _downloadQueue);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await fetcher.StartAsync(cts.Token);
            await WaitForFetcherCompletionAsync(fetcher, cts.Token);

            Assert.True(fetcher.IsCompleted);
            Assert.True(fetcher.HasError);
            Assert.NotNull(fetcher.Error);
            Assert.IsType<DatabricksException>(fetcher.Error);
        }

        [Fact]
        public async Task StartAsync_WithEmptyManifest_CompletesWithNoResults()
        {
            var manifest = new ResultManifest
            {
                TotalChunkCount = 0,
                TotalRowCount = 0,
                Chunks = new List<ResultChunk>()
            };

            var fetcher = new StatementExecutionResultFetcher(
                _mockClient.Object,
                _testStatementId,
                manifest,
                initialExternalLinks: null,
                _mockMemoryManager.Object,
                _downloadQueue);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await fetcher.StartAsync(cts.Token);
            await WaitForFetcherCompletionAsync(fetcher, cts.Token);

            Assert.True(fetcher.IsCompleted);
            Assert.False(fetcher.HasError);
            Assert.False(fetcher.HasMoreResults);

            // Only end guard should be in queue
            Assert.True(_downloadQueue.TryTake(out var result));
            Assert.IsType<EndOfResultsGuard>(result);
        }

        #endregion

        #region Cancellation Tests

        [Fact]
        public async Task StopAsync_StopsFetching()
        {
            var (manifest, initialExternalLinks) = CreateTestManifestWithExternalLinks(10);
            var fetcher = new StatementExecutionResultFetcher(
                _mockClient.Object,
                _testStatementId,
                manifest,
                initialExternalLinks,
                _mockMemoryManager.Object,
                _downloadQueue);

            using var cts = new CancellationTokenSource();
            await fetcher.StartAsync(cts.Token);

            // Stop immediately
            await fetcher.StopAsync();

            Assert.True(fetcher.IsCompleted);
        }

        #endregion

        #region Helper Methods

        private ResultManifest CreateTestManifest(int chunkCount)
        {
            var chunks = new List<ResultChunk>();
            for (int i = 0; i < chunkCount; i++)
            {
                chunks.Add(new ResultChunk
                {
                    ChunkIndex = i,
                    RowOffset = i * 100,
                    RowCount = 100,
                    ByteCount = 1024
                });
            }

            return new ResultManifest
            {
                TotalChunkCount = chunkCount,
                TotalRowCount = chunkCount * 100,
                Chunks = chunks
            };
        }

        /// <summary>
        /// Creates a test manifest and returns the initial external links for chunk 0.
        /// The manifest contains chunk metadata without external links (as the server returns),
        /// and the initial external links are returned separately (as they come from result.external_links).
        /// </summary>
        private (ResultManifest manifest, List<ExternalLink> initialExternalLinks) CreateTestManifestWithExternalLinks(int chunkCount)
        {
            var chunks = new List<ResultChunk>();
            for (int i = 0; i < chunkCount; i++)
            {
                chunks.Add(new ResultChunk
                {
                    ChunkIndex = i,
                    RowOffset = i * 100,
                    RowCount = 100,
                    ByteCount = 1024
                });
            }

            var manifest = new ResultManifest
            {
                TotalChunkCount = chunkCount,
                TotalRowCount = chunkCount * 100,
                Chunks = chunks
            };

            // Initial external links for chunk 0 (from result.external_links)
            var initialExternalLinks = new List<ExternalLink>
            {
                new ExternalLink
                {
                    ChunkIndex = 0,
                    ExternalLinkUrl = "https://storage.example.com/result0.arrow",
                    RowOffset = 0,
                    RowCount = 100,
                    ByteCount = 1024,
                    Expiration = DateTime.UtcNow.AddHours(1).ToString("O")
                }
            };

            return (manifest, initialExternalLinks);
        }

        private async Task WaitForFetcherCompletionAsync(StatementExecutionResultFetcher fetcher, CancellationToken cancellationToken)
        {
            var timeout = TimeSpan.FromSeconds(5);
            var startTime = DateTime.UtcNow;

            while (!fetcher.IsCompleted && DateTime.UtcNow - startTime < timeout)
            {
                await Task.Delay(50, cancellationToken);
            }
        }

        public void Dispose()
        {
            _downloadQueue?.Dispose();
            GC.SuppressFinalize(this);
        }

        #endregion
    }
}
