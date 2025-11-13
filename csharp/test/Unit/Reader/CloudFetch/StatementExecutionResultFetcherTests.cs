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
using Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch;
using Apache.Arrow.Adbc.Drivers.Databricks.StatementExecution;
using Moq;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Unit.Reader.CloudFetch
{
    public class StatementExecutionResultFetcherTests
    {
        private readonly Mock<IStatementExecutionClient> _mockClient;
        private readonly Mock<ICloudFetchMemoryBufferManager> _mockMemoryManager;
        private readonly BlockingCollection<IDownloadResult> _downloadQueue;
        private const string TestStatementId = "test-statement-123";

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
            var response = new GetStatementResponse
            {
                StatementId = TestStatementId,
                Result = new ResultData()
            };

            var fetcher = new StatementExecutionResultFetcher(
                _mockClient.Object,
                TestStatementId,
                response);

            Assert.NotNull(fetcher);
        }

        [Fact]
        public void Constructor_WithNullClient_ThrowsArgumentNullException()
        {
            var response = new GetStatementResponse
            {
                StatementId = TestStatementId,
                Result = new ResultData()
            };

            Assert.Throws<ArgumentNullException>(() =>
                new StatementExecutionResultFetcher(
                    null!,
                    TestStatementId,
                    response));
        }

        [Fact]
        public void Constructor_WithNullStatementId_ThrowsArgumentNullException()
        {
            var response = new GetStatementResponse
            {
                StatementId = TestStatementId,
                Result = new ResultData()
            };

            Assert.Throws<ArgumentNullException>(() =>
                new StatementExecutionResultFetcher(
                    _mockClient.Object,
                    null!,
                    response));
        }

        [Fact]
        public void Constructor_WithNullResponse_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() =>
                new StatementExecutionResultFetcher(
                    _mockClient.Object,
                    TestStatementId,
                    null!));
        }

        #endregion

        #region Manifest-Based Fetching Tests

        [Fact]
        public async Task FetchAllResultsAsync_WithManifestLinks_AddsAllDownloadResults()
        {
            // Arrange: Create manifest with 2 chunks, each with 1 external link
            var manifest = CreateTestManifest(chunkCount: 2, linksPerChunk: 1);
            var fetcher = CreateFetcher(manifest);

            // Act
            await fetcher.StartAsync(CancellationToken.None);
            await Task.Delay(100); // Give time for background task to complete

            // Assert
            var results = new List<IDownloadResult>();
            while (_downloadQueue.TryTake(out var result, 0))
            {
                results.Add(result);
            }

            // Should have 2 download results + 1 EndOfResultsGuard
            Assert.Equal(3, results.Count);
            Assert.True(results[2] is EndOfResultsGuard);
            Assert.True(fetcher.IsCompleted);
        }

        [Fact]
        public async Task FetchAllResultsAsync_WithMultipleLinksPerChunk_AddsAllLinks()
        {
            // Arrange: Create manifest with 1 chunk containing 3 external links
            var manifest = CreateTestManifest(chunkCount: 1, linksPerChunk: 3);
            var fetcher = CreateFetcher(manifest);

            // Act
            await fetcher.StartAsync(CancellationToken.None);
            await Task.Delay(100);

            // Assert
            var results = new List<IDownloadResult>();
            while (_downloadQueue.TryTake(out var result, 0))
            {
                results.Add(result);
            }

            // Should have 3 download results + 1 EndOfResultsGuard
            Assert.Equal(4, results.Count);
            Assert.True(results[3] is EndOfResultsGuard);
        }

        [Fact]
        public async Task FetchAllResultsAsync_WithExternalLinks_CreatesCorrectDownloadResults()
        {
            // Arrange
            var externalLink = CreateTestExternalLink(
                url: "https://s3.amazonaws.com/bucket/file1.arrow",
                rowOffset: 0,
                rowCount: 1000,
                byteCount: 10240,
                expiration: DateTime.UtcNow.AddHours(1).ToString("o"));

            var manifest = CreateTestManifestWithLinks(new[] { externalLink });
            var fetcher = CreateFetcher(manifest);

            // Act
            await fetcher.StartAsync(CancellationToken.None);
            await Task.Delay(100);

            // Assert
            _downloadQueue.TryTake(out var result, 0);
            Assert.NotNull(result);
            Assert.NotEqual(typeof(EndOfResultsGuard), result.GetType());
            Assert.Equal("https://s3.amazonaws.com/bucket/file1.arrow", result.FileUrl);
            Assert.Equal(0, result.StartRowOffset);
            Assert.Equal(1000, result.RowCount);
            Assert.Equal(10240, result.ByteCount);
        }

        [Fact]
        public async Task FetchAllResultsAsync_WithHttpHeaders_PassesHeadersToDownloadResult()
        {
            // Arrange
            var headers = new Dictionary<string, string>
            {
                { "x-amz-server-side-encryption-customer-algorithm", "AES256" },
                { "x-amz-server-side-encryption-customer-key", "test-key" }
            };

            var externalLink = CreateTestExternalLink(
                url: "https://s3.amazonaws.com/bucket/file1.arrow",
                rowOffset: 0,
                rowCount: 1000,
                byteCount: 10240,
                httpHeaders: headers);

            var manifest = CreateTestManifestWithLinks(new[] { externalLink });
            var fetcher = CreateFetcher(manifest);

            // Act
            await fetcher.StartAsync(CancellationToken.None);
            await Task.Delay(100);

            // Assert
            _downloadQueue.TryTake(out var result, 0);
            Assert.NotNull(result);
            Assert.NotNull(result.HttpHeaders);
            Assert.Equal(2, result.HttpHeaders.Count);
            Assert.Equal("AES256", result.HttpHeaders["x-amz-server-side-encryption-customer-algorithm"]);
            Assert.Equal("test-key", result.HttpHeaders["x-amz-server-side-encryption-customer-key"]);
        }

        #endregion

        #region Incremental Chunk Fetching Tests

        [Fact]
        public async Task FetchAllResultsAsync_WithoutManifestLinks_CallsGetResultChunkAsync()
        {
            // Arrange: Create manifest with chunks but no external links
            var manifest = CreateTestManifest(chunkCount: 2, linksPerChunk: 0);

            var resultData1 = CreateTestResultData(1, new[]
            {
                CreateTestExternalLink("https://s3.amazonaws.com/file1.arrow", 0, 500, 5120)
            });

            var resultData2 = CreateTestResultData(2, new[]
            {
                CreateTestExternalLink("https://s3.amazonaws.com/file2.arrow", 500, 500, 5120)
            });

            _mockClient.Setup(c => c.GetResultChunkAsync(TestStatementId, 0, It.IsAny<CancellationToken>()))
                .ReturnsAsync(resultData1);

            _mockClient.Setup(c => c.GetResultChunkAsync(TestStatementId, 1, It.IsAny<CancellationToken>()))
                .ReturnsAsync(resultData2);

            var fetcher = CreateFetcher(manifest);

            // Act
            await fetcher.StartAsync(CancellationToken.None);
            await Task.Delay(100);

            // Assert
            _mockClient.Verify(c => c.GetResultChunkAsync(TestStatementId, 0, It.IsAny<CancellationToken>()), Times.Once);
            _mockClient.Verify(c => c.GetResultChunkAsync(TestStatementId, 1, It.IsAny<CancellationToken>()), Times.Once);

            var results = new List<IDownloadResult>();
            while (_downloadQueue.TryTake(out var result, 0))
            {
                results.Add(result);
            }

            // Should have 2 download results + 1 EndOfResultsGuard
            Assert.Equal(3, results.Count);
        }

        [Fact]
        public async Task FetchAllResultsAsync_IncrementalFetching_CreatesCorrectDownloadResults()
        {
            // Arrange
            var manifest = CreateTestManifest(chunkCount: 1, linksPerChunk: 0);

            var resultData = CreateTestResultData(0, new[]
            {
                CreateTestExternalLink("https://s3.amazonaws.com/file1.arrow", 0, 1000, 10240)
            });

            _mockClient.Setup(c => c.GetResultChunkAsync(TestStatementId, 0, It.IsAny<CancellationToken>()))
                .ReturnsAsync(resultData);

            var fetcher = CreateFetcher(manifest);

            // Act
            await fetcher.StartAsync(CancellationToken.None);
            await Task.Delay(100);

            // Assert
            _downloadQueue.TryTake(out var result, 0);
            Assert.NotNull(result);
            Assert.Equal("https://s3.amazonaws.com/file1.arrow", result.FileUrl);
            Assert.Equal(0, result.StartRowOffset);
            Assert.Equal(1000, result.RowCount);
            Assert.Equal(10240, result.ByteCount);
        }

        #endregion

        #region GetDownloadResultAsync Tests

        [Fact]
        public async Task GetDownloadResultAsync_ReturnsNull()
        {
            // Arrange
            var manifest = CreateTestManifest(chunkCount: 1);
            var fetcher = CreateFetcher(manifest);

            // Act
            var result = await fetcher.GetDownloadResultAsync(0, CancellationToken.None);

            // Assert
            Assert.Null(result);
        }

        #endregion

        #region Error Handling Tests

        [Fact]
        public async Task FetchAllResultsAsync_WithEmptyManifest_CompletesSuccessfully()
        {
            // Arrange
            var manifest = CreateTestManifest(chunkCount: 0);
            var fetcher = CreateFetcher(manifest);

            // Act
            await fetcher.StartAsync(CancellationToken.None);
            await Task.Delay(100);

            // Assert
            Assert.True(fetcher.IsCompleted);
            Assert.False(fetcher.HasError);
        }

        [Fact]
        public async Task FetchAllResultsAsync_WithCancellation_StopsGracefully()
        {
            // Arrange
            var manifest = CreateTestManifest(chunkCount: 10, linksPerChunk: 1);
            var cts = new CancellationTokenSource();
            var fetcher = CreateFetcher(manifest);

            // Act
            await fetcher.StartAsync(cts.Token);
            cts.Cancel();
            await Task.Delay(100);

            // Assert
            Assert.True(fetcher.IsCompleted);
        }

        [Fact]
        public async Task FetchAllResultsAsync_WhenGetResultChunkThrows_SetsError()
        {
            // Arrange
            var manifest = CreateTestManifest(chunkCount: 1, linksPerChunk: 0);

            _mockClient.Setup(c => c.GetResultChunkAsync(TestStatementId, It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new Exception("Network error"));

            var fetcher = CreateFetcher(manifest);

            // Act
            await fetcher.StartAsync(CancellationToken.None);
            await Task.Delay(100);

            // Assert
            Assert.True(fetcher.HasError);
            Assert.NotNull(fetcher.Error);
            Assert.Contains("Network error", fetcher.Error.Message);
        }

        #endregion

        #region Expiration Time Parsing Tests

        [Fact]
        public async Task FetchAllResultsAsync_WithValidISO8601Expiration_ParsesCorrectly()
        {
            // Arrange
            var expectedExpiration = DateTime.UtcNow.AddHours(2);
            var externalLink = CreateTestExternalLink(
                url: "https://s3.amazonaws.com/file1.arrow",
                rowOffset: 0,
                rowCount: 1000,
                byteCount: 10240,
                expiration: expectedExpiration.ToString("o"));

            var manifest = CreateTestManifestWithLinks(new[] { externalLink });
            var fetcher = CreateFetcher(manifest);

            // Act
            await fetcher.StartAsync(CancellationToken.None);
            await Task.Delay(100);

            // Assert
            _downloadQueue.TryTake(out var result, 0);
            Assert.NotNull(result);
            // Allow small time difference due to parsing
            Assert.True(Math.Abs((result.ExpirationTime - expectedExpiration).TotalSeconds) < 1);
        }

        [Fact]
        public async Task FetchAllResultsAsync_WithInvalidExpiration_UsesDefaultExpiration()
        {
            // Arrange
            var externalLink = CreateTestExternalLink(
                url: "https://s3.amazonaws.com/file1.arrow",
                rowOffset: 0,
                rowCount: 1000,
                byteCount: 10240,
                expiration: "invalid-date");

            var manifest = CreateTestManifestWithLinks(new[] { externalLink });
            var fetcher = CreateFetcher(manifest);

            // Act
            await fetcher.StartAsync(CancellationToken.None);
            await Task.Delay(100);

            // Assert
            _downloadQueue.TryTake(out var result, 0);
            Assert.NotNull(result);
            // Should default to ~1 hour from now
            var timeDiff = (result.ExpirationTime - DateTime.UtcNow).TotalMinutes;
            Assert.InRange(timeDiff, 50, 70); // Allow some variance
        }

        [Fact]
        public async Task FetchAllResultsAsync_WithNullExpiration_UsesDefaultExpiration()
        {
            // Arrange
            var externalLink = CreateTestExternalLink(
                url: "https://s3.amazonaws.com/file1.arrow",
                rowOffset: 0,
                rowCount: 1000,
                byteCount: 10240,
                expiration: null);

            var manifest = CreateTestManifestWithLinks(new[] { externalLink });
            var fetcher = CreateFetcher(manifest);

            // Act
            await fetcher.StartAsync(CancellationToken.None);
            await Task.Delay(100);

            // Assert
            _downloadQueue.TryTake(out var result, 0);
            Assert.NotNull(result);
            // Should default to ~1 hour from now
            var timeDiff = (result.ExpirationTime - DateTime.UtcNow).TotalMinutes;
            Assert.InRange(timeDiff, 50, 70);
        }

        #endregion

        #region Helper Methods

        private StatementExecutionResultFetcher CreateFetcher(ResultManifest manifest)
        {
            // Create a GetStatementResponse with the first chunk's external links in Result field
            var firstChunk = manifest.Chunks?.FirstOrDefault();
            var response = new GetStatementResponse
            {
                StatementId = TestStatementId,
                Manifest = manifest,
                Result = new ResultData
                {
                    ChunkIndex = 0,
                    ExternalLinks = firstChunk?.ExternalLinks,
                    NextChunkIndex = manifest.TotalChunkCount > 1 ? 1 : null
                }
            };

            // Set up mock to return subsequent chunks when GetResultChunkAsync is called
            if (manifest.Chunks != null && manifest.Chunks.Count > 1)
            {
                for (int i = 1; i < manifest.Chunks.Count; i++)
                {
                    var chunkIndex = i;
                    var chunk = manifest.Chunks[i];
                    var resultData = new ResultData
                    {
                        ChunkIndex = chunkIndex,
                        ExternalLinks = chunk.ExternalLinks,
                        NextChunkIndex = chunkIndex + 1 < manifest.TotalChunkCount ? chunkIndex + 1 : null
                    };

                    _mockClient.Setup(c => c.GetResultChunkAsync(
                        TestStatementId,
                        chunkIndex,
                        It.IsAny<CancellationToken>()))
                        .ReturnsAsync(resultData);
                }
            }

            var fetcher = new StatementExecutionResultFetcher(
                _mockClient.Object,
                TestStatementId,
                response);

            // Initialize with resources (simulating what CloudFetchDownloadManager does)
            fetcher.Initialize(_mockMemoryManager.Object, _downloadQueue);

            return fetcher;
        }

        private ResultManifest CreateTestManifest(int chunkCount, int linksPerChunk = 1)
        {
            var chunks = new List<ResultChunk>();
            for (int i = 0; i < chunkCount; i++)
            {
                var chunk = new ResultChunk
                {
                    ChunkIndex = i,
                    RowCount = 1000,
                    RowOffset = i * 1000,
                    ByteCount = 10240
                };

                if (linksPerChunk > 0)
                {
                    chunk.ExternalLinks = new List<ExternalLink>();
                    for (int j = 0; j < linksPerChunk; j++)
                    {
                        chunk.ExternalLinks.Add(CreateTestExternalLink(
                            $"https://s3.amazonaws.com/file{i}_{j}.arrow",
                            i * 1000 + j * 100,
                            100,
                            1024));
                    }
                }

                chunks.Add(chunk);
            }

            return new ResultManifest
            {
                TotalChunkCount = chunkCount,
                Chunks = chunks,
                TotalRowCount = chunkCount * 1000,
                TotalByteCount = chunkCount * 10240
            };
        }

        private ResultManifest CreateTestManifestWithLinks(ExternalLink[] links)
        {
            return new ResultManifest
            {
                TotalChunkCount = 1,
                Chunks = new List<ResultChunk>
                {
                    new ResultChunk
                    {
                        ChunkIndex = 0,
                        RowCount = links.Sum(l => l.RowCount),
                        RowOffset = 0,
                        ByteCount = links.Sum(l => l.ByteCount),
                        ExternalLinks = links.ToList()
                    }
                },
                TotalRowCount = links.Sum(l => l.RowCount),
                TotalByteCount = links.Sum(l => l.ByteCount)
            };
        }

        private ExternalLink CreateTestExternalLink(
            string url,
            long rowOffset,
            long rowCount,
            long byteCount,
            string? expiration = null,
            Dictionary<string, string>? httpHeaders = null)
        {
            return new ExternalLink
            {
                ExternalLinkUrl = url,
                RowOffset = rowOffset,
                RowCount = rowCount,
                ByteCount = byteCount,
                Expiration = expiration ?? DateTime.UtcNow.AddHours(1).ToString("o"),
                HttpHeaders = httpHeaders
            };
        }

        private ResultData CreateTestResultData(long chunkIndex, ExternalLink[] links)
        {
            return new ResultData
            {
                ChunkIndex = chunkIndex,
                ExternalLinks = links.ToList()
            };
        }

        #endregion
    }
}
