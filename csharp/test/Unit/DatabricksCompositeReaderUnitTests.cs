/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* This file has been modified from its original version, which is
* under the Apache License:
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Reader;
using AdbcDrivers.Databricks.Reader.CloudFetch;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using AdbcDrivers.HiveServer2.Hive2;
using Apache.Arrow.Adbc.Tracing;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;
using Moq;
using Moq.Protected;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    /// <summary>
    /// Testable subclass of DatabricksCompositeReader for unit testing
    /// </summary>
    internal class TestableDatabricksCompositeReader : DatabricksCompositeReader
    {
        public bool CloudFetchReaderCreated { get; private set; }
        public bool DatabricksReaderCreated { get; private set; }
        public BaseDatabricksReader? MockReader { get; set; }

        public TestableDatabricksCompositeReader(
            IHiveServer2Statement statement,
            Schema schema,
            IResponse response,
            bool isLz4Compressed,
            HttpClient httpClient,
            IOperationStatusPoller? operationPoller = null)
            : base(statement, schema, response, isLz4Compressed, httpClient, operationPoller)
        {
        }

        protected override BaseDatabricksReader CreateCloudFetchReader(TFetchResultsResp initialResults)
        {
            CloudFetchReaderCreated = true;
            return MockReader!;
        }

        protected override BaseDatabricksReader CreateDatabricksReader(TFetchResultsResp initialResults)
        {
            DatabricksReaderCreated = true;
            return MockReader!;
        }
    }

    public class DatabricksCompositeReaderTests : IDisposable
    {
        private readonly Schema _testSchema;
        private readonly HttpClient _httpClient;

        public DatabricksCompositeReaderTests()
        {
            _testSchema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, true))
                .Field(new Field("name", StringType.Default, true))
                .Build();

            _httpClient = new HttpClient();
        }

        private Mock<IHiveServer2Statement> CreateMockStatement(Mock<TCLIService.IAsync>? mockClient = null, TSparkDirectResults? directResults = null)
        {
            var mockStatement = new Mock<IHiveServer2Statement>();
            mockStatement.Setup(s => s.QueryTimeoutSeconds).Returns(10);

            // Set up ActivityTrace for tracing infrastructure
            var activityTrace = new ActivityTrace("Apache.Arrow.Adbc.Tests", "1.0.0");
            mockStatement.As<IActivityTracer>().Setup(t => t.Trace).Returns(activityTrace);
            mockStatement.As<IActivityTracer>().Setup(t => t.TraceParent).Returns((string?)null);

            if (mockClient != null)
            {
                mockStatement.Setup(s => s.Client).Returns(mockClient.Object);
            }

            if (directResults != null)
            {
                mockStatement
                    .Setup(s => s.TryGetDirectResults(It.IsAny<IResponse>(), out It.Ref<TSparkDirectResults?>.IsAny))
                    .Returns((IResponse response, out TSparkDirectResults? result) =>
                    {
                        result = directResults;
                        return true;
                    });
            }
            else
            {
                TSparkDirectResults? nullDirectResults = null;
                mockStatement.Setup(s => s.TryGetDirectResults(It.IsAny<IResponse>(), out nullDirectResults))
                    .Returns(false);
            }

            return mockStatement;
        }

        private Mock<TCLIService.IAsync> CreateMockClient()
        {
            var mockClient = new Mock<TCLIService.IAsync>();
            var closeOperationResponse = new TCloseOperationResp
            {
                Status = new TStatus { StatusCode = TStatusCode.SUCCESS_STATUS }
            };
            mockClient.Setup(c => c.CloseOperation(It.IsAny<TCloseOperationReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(closeOperationResponse);
            return mockClient;
        }

        private Mock<IResponse> CreateMockResponse(TSparkDirectResults? directResults = null, TOperationHandle? operationHandle = null)
        {
            var mockResponse = new Mock<IResponse>();
            mockResponse.Setup(r => r.OperationHandle).Returns(operationHandle ?? new TOperationHandle());

            if (directResults != null)
            {
                mockResponse.Setup(r => r.DirectResults).Returns(directResults);
            }

            return mockResponse;
        }

        [Fact]
        public void Constructor_WithValidParameters_InitializesSuccessfully()
        {
            // Arrange
            var mockClient = CreateMockClient();
            var mockStatement = CreateMockStatement(mockClient);
            var mockPoller = new Mock<IOperationStatusPoller>();
            var mockResponse = CreateMockResponse();

            // Act
            using var reader = new DatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                mockResponse.Object,
                false,
                _httpClient,
                mockPoller.Object);

            // Assert
            Assert.NotNull(reader);
            Assert.Equal(_testSchema, reader.Schema);
            mockPoller.Verify(p => p.Start(It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public void Constructor_WithDirectResultsAndNoMoreRows_DoesNotStartPoller()
        {
            // Arrange
            var directResults = new TSparkDirectResults
            {
                ResultSet = new TFetchResultsResp
                {
                    HasMoreRows = false,
                    Results = new TRowSet()
                },
                ResultSetMetadata = new TGetResultSetMetadataResp
                { },
                __isset = new TSparkDirectResults.Isset { resultSet = true }
            };

            var mockClient = CreateMockClient();
            var mockStatement = CreateMockStatement(mockClient, directResults: directResults);
            var mockResponse = CreateMockResponse(directResults: directResults);
            var mockPoller = new Mock<IOperationStatusPoller>();

            // Act
            using var reader = new TestableDatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                mockResponse.Object,
                false,
                _httpClient,
                mockPoller.Object);

            // Assert
            mockPoller.Verify(p => p.Start(It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public void ShouldUseCloudFetch_WithResultLinks_ReturnsTrue()
        {
            // Arrange
            var fetchResults = new TFetchResultsResp
            {
                Results = new TRowSet
                {
                    ResultLinks = new List<TSparkArrowResultLink>
                    {
                        new TSparkArrowResultLink {}
                    }
                }
            };

            // Act
            var result = DatabricksCompositeReader.ShouldUseCloudFetch(fetchResults);

            // Assert
            Assert.True(result);
        }

        [Fact]
        public void ShouldUseCloudFetch_WithoutResultLinks_ReturnsFalse()
        {
            // Arrange
            var fetchResults = new TFetchResultsResp
            {
                Results = new TRowSet()
            };

            // Act
            var result = DatabricksCompositeReader.ShouldUseCloudFetch(fetchResults);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void ShouldUseCloudFetch_WithEmptyResultLinks_ReturnsFalse()
        {
            // Arrange
            var fetchResults = new TFetchResultsResp
            {
                Results = new TRowSet
                {
                    ResultLinks = new List<TSparkArrowResultLink> { }
                }
            };

            // Act
            var result = DatabricksCompositeReader.ShouldUseCloudFetch(fetchResults);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public async Task ReadNextRecordBatchAsync_WithNoActiveReader_FetchesAndDelegates()
        {
            // Arrange
            var mockClient = CreateMockClient();
            var fetchResponse = new TFetchResultsResp
            {
                HasMoreRows = false,
                Results = new TRowSet()
            };
            mockClient.Setup(c => c.FetchResults(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(fetchResponse);
            var mockStatement = CreateMockStatement(mockClient);
            var mockPoller = new Mock<IOperationStatusPoller>();
            var mockResponse = CreateMockResponse();
            var expectedBatch = new RecordBatch(_testSchema, new IArrowArray[] { }, 0);
            var mockReader = new Mock<BaseDatabricksReader>(mockStatement.Object, _testSchema, mockResponse.Object, false);
            mockReader.Setup(r => r.ReadNextRecordBatchAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(expectedBatch);

            using var reader = new TestableDatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                mockResponse.Object,
                false,
                _httpClient,
                mockPoller.Object)
            {
                MockReader = mockReader.Object
            };

            // Act
            var result = await reader.ReadNextRecordBatchAsync();

            // Assert
            Assert.NotNull(result);
            Assert.Equal(expectedBatch, result);
            mockClient.Verify(c => c.FetchResults(
                It.IsAny<TFetchResultsReq>(),
                It.IsAny<CancellationToken>()), Times.Once);
            // verify that the reader was created
            Assert.True(reader.DatabricksReaderCreated);
        }

        [Fact]
        public async Task ReadNextRecordBatchAsync_WhenReturnsNull_StopsPoller()
        {
            // Arrange
            var mockClient = CreateMockClient();
            var fetchResponse = new TFetchResultsResp
            {
                HasMoreRows = false,
                Results = new TRowSet()
            };
            mockClient.Setup(c => c.FetchResults(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(fetchResponse);
            var mockStatement = CreateMockStatement(mockClient);
            var mockPoller = new Mock<IOperationStatusPoller>();
            mockPoller.Setup(p => p.IsStarted).Returns(true);
            var mockResponse = CreateMockResponse();
            var mockReader = new Mock<BaseDatabricksReader>(mockStatement.Object, _testSchema, mockResponse.Object, false);
            mockReader.Setup(r => r.ReadNextRecordBatchAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync((RecordBatch?)null);

            using var reader = new TestableDatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                mockResponse.Object,
                false,
                _httpClient,
                mockPoller.Object)
            {
                MockReader = mockReader.Object
            };

            // Act
            var result = await reader.ReadNextRecordBatchAsync();

            // Assert
            Assert.Null(result);
            mockPoller.Verify(p => p.Stop(), Times.Once);
        }

        [Fact]
        public void Constructor_WithDirectResultsAndMoreRows_StartsPoller()
        {
            // Arrange
            var directResults = new TSparkDirectResults
            {
                ResultSet = new TFetchResultsResp
                {
                    HasMoreRows = true,
                    Results = new TRowSet()
                },
                __isset = new TSparkDirectResults.Isset { resultSet = true }
            };

            var mockClient = CreateMockClient();
            var mockStatement = CreateMockStatement(mockClient, directResults: directResults);
            var mockResponse = CreateMockResponse(directResults: directResults);
            var mockPoller = new Mock<IOperationStatusPoller>();
            var mockReader = new Mock<BaseDatabricksReader>(mockStatement.Object, _testSchema, mockResponse.Object, false);

            // Act
            using var reader = new TestableDatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                mockResponse.Object,
                false,
                _httpClient,
                mockPoller.Object)
            {
                MockReader = mockReader.Object
            };

            // Assert
            Assert.True(reader.DatabricksReaderCreated); // Should create reader from direct results
            mockPoller.Verify(p => p.Start(It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public void Dispose_WithNoActiveReader_CallsCloseOperationDirectly()
        {
            // Arrange
            var mockClient = CreateMockClient();
            var mockStatement = CreateMockStatement(mockClient);

            var mockPoller = new Mock<IOperationStatusPoller>();
            var operationHandle = new TOperationHandle();
            var mockResponse = CreateMockResponse(directResults: null, operationHandle: operationHandle);

            var reader = new DatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                mockResponse.Object,
                false,
                _httpClient,
                mockPoller.Object);

            // Act
            reader.Dispose();

            // Assert
            mockClient.Verify(c => c.CloseOperation(
                It.Is<TCloseOperationReq>(req => req.OperationHandle == operationHandle),
                It.IsAny<CancellationToken>()), Times.Once);
            mockPoller.Verify(p => p.Stop(), Times.Once);
            mockPoller.Verify(p => p.Dispose(), Times.Once);
        }

        [Fact]
        public async Task Dispose_WithActiveReader_CallsReaderDispose()
        {
            // Arrange
            var mockClient = CreateMockClient();
            var fetchResponse = new TFetchResultsResp
            {
                HasMoreRows = false,
                Results = new TRowSet()
            };
            mockClient.Setup(c => c.FetchResults(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(fetchResponse);
            var mockStatement = CreateMockStatement(mockClient);

            var mockPoller = new Mock<IOperationStatusPoller>();
            var mockResponse = CreateMockResponse();
            var mockReader = new Mock<BaseDatabricksReader>(mockStatement.Object, _testSchema, mockResponse.Object, false);
            mockReader.Setup(r => r.ReadNextRecordBatchAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync((RecordBatch?)null);

            mockReader.Protected()
                .Setup("Dispose", ItExpr.IsAny<bool>())
                .CallBase()
                .Verifiable();

            var reader = new TestableDatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                mockResponse.Object,
                false,
                _httpClient,
                mockPoller.Object)
            {
                MockReader = mockReader.Object
            };

            // Trigger creation of active reader
            _ = await reader.ReadNextRecordBatchAsync();

            // Act
            reader.Dispose();

            mockReader.Protected().Verify("Dispose", Times.Once(), ItExpr.IsAny<bool>());
            mockPoller.Verify(p => p.Stop(), Times.Once);
            mockPoller.Verify(p => p.Dispose(), Times.Once);
        }

        /// <summary>
        /// Regression test for the reviewer gap (ES-2356918 follow-up): a reader disposed before its
        /// statement — the default with `using` declarations, where the reader is declared after the
        /// statement and so disposed first — used to leave <see cref="DatabricksCompositeReader.GetChunkMetrics"/>
        /// returning null (because Dispose set _activeReader = null), losing both the chunk-detail
        /// telemetry and the row-count shortfall warning. The composite reader now snapshots the
        /// CloudFetch metrics in Dispose, so GetChunkMetrics still reports them afterwards.
        /// </summary>
        [Fact]
        public async Task GetChunkMetrics_AfterDispose_ReturnsSnapshotIncludingShortfall()
        {
            // Arrange: a real CloudFetchReader that records a row-count shortfall (file decodes to
            // fewer rows than the server declared) when drained through the composite reader.
            var cloudFetchReader = CreateShortfallCloudFetchReader();

            // Non-empty ResultLinks on the fetch response makes ShouldUseCloudFetch true, so the lazy
            // read path routes through CreateCloudFetchReader (overridden to return our real reader).
            // Note: the fetch path is used rather than direct results because CreateCloudFetchReader is
            // called from the base constructor, before the object initializer assigns MockReader.
            var mockClient = CreateMockClient();
            var fetchResponse = new TFetchResultsResp
            {
                HasMoreRows = false,
                Results = new TRowSet
                {
                    ResultLinks = new List<TSparkArrowResultLink> { new TSparkArrowResultLink() }
                }
            };
            mockClient.Setup(c => c.FetchResults(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(fetchResponse);
            var mockStatement = CreateMockStatement(mockClient);
            var mockResponse = CreateMockResponse();

            var reader = new TestableDatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                mockResponse.Object,
                false,
                _httpClient)
            {
                MockReader = cloudFetchReader
            };

            // Drain through the composite, which initializes the active reader and records the shortfall.
            long drained = 0;
            RecordBatch? batch;
            while ((batch = await reader.ReadNextRecordBatchAsync()) != null)
            {
                using (batch) drained += batch.Length;
            }
            Assert.Equal(24_576, drained);
            Assert.True(reader.CloudFetchReaderCreated);

            // Sanity: metrics are available while the reader is still alive.
            Assert.Equal(1, reader.GetChunkMetrics()?.RowCountShortfallChunks);

            // Act: dispose the reader first (as `using var reader` after the statement would).
            reader.Dispose();

            // Assert: the snapshot is still returned after dispose, carrying the shortfall that
            // DatabricksStatement.Dispose turns into the CloudFetchRowCountShortfall warning.
            ChunkMetrics? metrics = reader.GetChunkMetrics();
            Assert.NotNull(metrics);
            Assert.Equal(1, metrics!.RowCountShortfallChunks);
            Assert.Equal(875_733 - 24_576, metrics.RowCountShortfallRows);
        }

        private static readonly Schema s_cloudFetchSchema =
            new Schema.Builder().Field(f => f.Name("id").DataType(Int64Type.Default)).Build();

        private static Stream ArrowFile(int rows)
        {
            var ms = new MemoryStream();
            using (var writer = new ArrowStreamWriter(ms, s_cloudFetchSchema, leaveOpen: true))
            {
                var ids = new Int64Array.Builder();
                for (int i = 0; i < rows; i++) ids.Append(i);
                writer.WriteRecordBatch(new RecordBatch(s_cloudFetchSchema, new IArrowArray[] { ids.Build() }, rows));
                writer.WriteEnd();
            }
            ms.Position = 0;
            return ms;
        }

        private CloudFetchReader CreateShortfallCloudFetchReader()
        {
            // Link declares 875,733 rows but the file decodes to 24,576 — the incident shape.
            var file = new Mock<IDownloadResult>();
            file.Setup(r => r.ChunkIndex).Returns(0);
            file.Setup(r => r.RowCount).Returns(875_733);
            file.Setup(r => r.DownloadCompletedTask).Returns(Task.CompletedTask);
            file.Setup(r => r.DataStream).Returns(ArrowFile(24_576));
            file.Setup(r => r.Size).Returns(1024);

            var queue = new Queue<IDownloadResult>(new[] { file.Object });
            var manager = new Mock<ICloudFetchDownloadManager>();
            manager.Setup(m => m.GetNextDownloadedFileAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(() => queue.Count > 0 ? queue.Dequeue() : null);
            manager.Setup(m => m.GetChunkMetrics()).Returns(() => new ChunkMetrics());

            var statement = new Mock<IHiveServer2Statement>();
            statement.Setup(s => s.Trace).Returns(new ActivityTrace("TestActivitySource"));
            statement.Setup(s => s.TraceParent).Returns((string?)null);
            statement.Setup(s => s.AssemblyVersion).Returns("1.0.0");
            statement.Setup(s => s.AssemblyName).Returns("TestAssembly");

            return new CloudFetchReader(statement.Object, s_cloudFetchSchema, null, manager.Object, totalExpectedRows: 0);
        }

        public void Dispose()
        {
            _httpClient?.Dispose();
        }
    }
}
