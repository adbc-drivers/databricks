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
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch;
using Apache.Arrow.Adbc.Drivers.Databricks.StatementExecution;
using Moq;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.CloudFetch
{
    /// <summary>
    /// Tests for StatementExecutionResultFetcher
    /// </summary>
    public class StatementExecutionResultFetcherTest : IDisposable
    {
        private readonly Mock<IStatementExecutionClient> _mockClient;
        private readonly Mock<ICloudFetchMemoryBufferManager> _mockMemoryManager;
        private readonly BlockingCollection<IDownloadResult> _downloadQueue;
        private readonly string _testStatementId = "test-statement-123";

        public StatementExecutionResultFetcherTest()
        {
            _mockClient = new Mock<IStatementExecutionClient>();
            _mockMemoryManager = new Mock<ICloudFetchMemoryBufferManager>();
            _downloadQueue = new BlockingCollection<IDownloadResult>(new ConcurrentQueue<IDownloadResult>(), 10);
        }

        #region Manifest-Based Fetching Tests

        [Fact]
        public async Task FetchResultsAsync_WithManifestExternalLinks_SuccessfullyFetchesResults()
        {
            // Arrange
            var (manifest, initialExternalLinks) = CreateTestManifestWithExternalLinks(3);

            // Setup mock to return external links for chunks 1 and 2 (chunk 0 is provided as initialExternalLinks)
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

            _mockClient.Setup(c => c.GetResultChunkAsync(_testStatementId, 2, It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ResultData
                {
                    ChunkIndex = 2,
                    RowOffset = 200,
                    RowCount = 100,
                    ByteCount = 1024,
                    ExternalLinks = new List<ExternalLink>
                    {
                        new ExternalLink
                        {
                            ChunkIndex = 2,
                            ExternalLinkUrl = "https://storage.example.com/result2.arrow",
                            RowOffset = 200,
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

            // Act
            await fetcher.StartAsync(CancellationToken.None);

            // Wait for the fetcher to process the results
            await Task.Delay(200);

            // Assert
            // The download queue should contain our result links
            var downloadResults = new List<IDownloadResult>();
            while (_downloadQueue.TryTake(out var result))
            {
                if (result == EndOfResultsGuard.Instance)
                {
                    continue;
                }
                downloadResults.Add(result);
            }

            Assert.Equal(3, downloadResults.Count);

            // Verify each download result has the correct link
            for (int i = 0; i < 3; i++)
            {
                Assert.Equal($"https://storage.example.com/result{i}.arrow", downloadResults[i].FileUrl);
                Assert.Equal(i * 100, downloadResults[i].StartRowOffset);
                Assert.Equal(100, downloadResults[i].RowCount);
            }

            // Verify the fetcher state
            Assert.False(fetcher.HasMoreResults);
            Assert.True(fetcher.IsCompleted);
            Assert.False(fetcher.HasError);
            Assert.Null(fetcher.Error);

            // Cleanup
            await fetcher.StopAsync();
        }

        [Fact]
        public async Task FetchResultsAsync_WithHttpHeaders_PassesHeadersCorrectly()
        {
            // Arrange
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

            // Act
            await fetcher.StartAsync(CancellationToken.None);
            await Task.Delay(200);

            // Assert
            var downloadResults = new List<IDownloadResult>();
            while (_downloadQueue.TryTake(out var result))
            {
                if (result != EndOfResultsGuard.Instance)
                {
                    downloadResults.Add(result);
                }
            }

            Assert.Single(downloadResults);
            var downloadResult = downloadResults[0] as DownloadResult;
            Assert.NotNull(downloadResult);
            Assert.NotNull(downloadResult.HttpHeaders);
            Assert.Equal(2, downloadResult.HttpHeaders.Count);
            Assert.Equal("AES256", downloadResult.HttpHeaders["x-amz-server-side-encryption-customer-algorithm"]);

            await fetcher.StopAsync();
        }

        #endregion

        #region Incremental Chunk Fetching Tests

        [Fact]
        public async Task FetchResultsAsync_WithChunksWithoutLinks_FetchesFromApi()
        {
            // Arrange - Manifest has chunks without external links (need incremental fetching)
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

            // Act
            await fetcher.StartAsync(CancellationToken.None);
            await Task.Delay(300);

            // Assert
            // Verify GetResultChunkAsync was called for both chunks
            _mockClient.Verify(c => c.GetResultChunkAsync(_testStatementId, 0, It.IsAny<CancellationToken>()), Times.Once);
            _mockClient.Verify(c => c.GetResultChunkAsync(_testStatementId, 1, It.IsAny<CancellationToken>()), Times.Once);

            var downloadResults = new List<IDownloadResult>();
            while (_downloadQueue.TryTake(out var result))
            {
                if (result != EndOfResultsGuard.Instance)
                {
                    downloadResults.Add(result);
                }
            }

            Assert.Equal(2, downloadResults.Count);
            Assert.True(fetcher.IsCompleted);
            Assert.False(fetcher.HasError);

            await fetcher.StopAsync();
        }

        #endregion

        #region URL Refresh Tests

        [Fact]
        public async Task RefreshUrlsAsync_FetchesFreshUrls()
        {
            // Arrange
            var (manifest, _) = CreateTestManifestWithExternalLinks(2);

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

            var fetcher = new StatementExecutionResultFetcher(
                _mockClient.Object,
                _testStatementId,
                manifest,
                initialExternalLinks: null,
                _mockMemoryManager.Object,
                _downloadQueue);

            // Act
            var results = await fetcher.RefreshUrlsAsync(0, CancellationToken.None);

            // Assert
            _mockClient.Verify(c => c.GetResultChunkAsync(_testStatementId, 0, It.IsAny<CancellationToken>()), Times.Once);
            Assert.NotNull(results);
            var resultList = results.ToList();
            Assert.Single(resultList);
            Assert.Equal("https://storage.example.com/refreshed.arrow", resultList[0].FileUrl);
        }

        [Fact]
        public async Task RefreshUrlsAsync_WithRowOffsetInMiddleOfChunk_FindsCorrectChunk()
        {
            // Arrange
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

            var fetcher = new StatementExecutionResultFetcher(
                _mockClient.Object,
                _testStatementId,
                manifest,
                initialExternalLinks: null,
                _mockMemoryManager.Object,
                _downloadQueue);

            // Act - Refresh with row offset 150, which is in the middle of chunk 1
            var results = await fetcher.RefreshUrlsAsync(150, CancellationToken.None);

            // Assert
            _mockClient.Verify(c => c.GetResultChunkAsync(_testStatementId, 1, It.IsAny<CancellationToken>()), Times.Once);
            Assert.NotNull(results);
            Assert.Single(results);
        }

        #endregion

        #region Error Handling Tests

        [Fact]
        public async Task FetchResultsAsync_WithApiError_SetsErrorState()
        {
            // Arrange - Manifest without external links to trigger API fetch
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

            // Act
            await fetcher.StartAsync(CancellationToken.None);
            await Task.Delay(200);

            // Assert
            Assert.False(fetcher.HasMoreResults);
            Assert.True(fetcher.IsCompleted);
            Assert.True(fetcher.HasError);
            Assert.NotNull(fetcher.Error);
            Assert.IsType<DatabricksException>(fetcher.Error);

            await fetcher.StopAsync();
        }

        [Fact]
        public async Task FetchResultsAsync_WithEmptyResults_CompletesGracefully()
        {
            // Arrange
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

            // Act
            await fetcher.StartAsync(CancellationToken.None);
            await Task.Delay(100);

            // Assert
            var nonGuardItems = new List<IDownloadResult>();
            while (_downloadQueue.TryTake(out var result))
            {
                if (result != EndOfResultsGuard.Instance)
                {
                    nonGuardItems.Add(result);
                }
            }
            Assert.Empty(nonGuardItems);

            Assert.False(fetcher.HasMoreResults);
            Assert.True(fetcher.IsCompleted);
            Assert.False(fetcher.HasError);

            await fetcher.StopAsync();
        }

        #endregion

        #region Cancellation Tests

        [Fact]
        public async Task StopAsync_CancelsFetching()
        {
            // Arrange
            var fetchStarted = new TaskCompletionSource<bool>();
            var fetchCancelled = new TaskCompletionSource<bool>();

            var manifest = new ResultManifest
            {
                TotalChunkCount = 10, // Many chunks to ensure fetching continues
                TotalRowCount = 1000,
                Chunks = new List<ResultChunk>
                {
                    new ResultChunk { ChunkIndex = 0, RowOffset = 0, RowCount = 100, ByteCount = 1024 }
                }
            };

            _mockClient.Setup(c => c.GetResultChunkAsync(_testStatementId, It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .Returns(async (string statementId, long chunkIndex, CancellationToken token) =>
                {
                    fetchStarted.TrySetResult(true);

                    try
                    {
                        // Wait for a long time or until cancellation
                        await Task.Delay(10000, token);
                    }
                    catch (OperationCanceledException)
                    {
                        fetchCancelled.TrySetResult(true);
                        throw;
                    }

                    return new ResultData
                    {
                        ChunkIndex = chunkIndex,
                        ExternalLinks = new List<ExternalLink>()
                    };
                });

            var fetcher = new StatementExecutionResultFetcher(
                _mockClient.Object,
                _testStatementId,
                manifest,
                initialExternalLinks: null,
                _mockMemoryManager.Object,
                _downloadQueue);

            // Act
            await fetcher.StartAsync(CancellationToken.None);

            // Wait for fetch to start
            await fetchStarted.Task;

            // Stop the fetcher
            await fetcher.StopAsync();

            // Assert
            var cancelled = await Task.WhenAny(fetchCancelled.Task, Task.Delay(1000)) == fetchCancelled.Task;
            Assert.True(cancelled, "Fetch operation should have been cancelled");
            Assert.True(fetcher.IsCompleted);
        }

        #endregion

        #region Helper Methods

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

        public void Dispose()
        {
            _downloadQueue?.Dispose();
            GC.SuppressFinalize(this);
        }

        #endregion
    }
}
