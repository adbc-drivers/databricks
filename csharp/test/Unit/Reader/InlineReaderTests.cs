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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Databricks.Reader;
using Apache.Arrow.Adbc.Drivers.Databricks.StatementExecution;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Unit.Reader
{
    public class InlineReaderTests
    {
        /// <summary>
        /// Helper method to create an Arrow IPC stream as byte array.
        /// </summary>
        private byte[] CreateArrowIpcStream(Schema schema, params RecordBatch[] batches)
        {
            using var memoryStream = new MemoryStream();
            using var writer = new ArrowStreamWriter(memoryStream, schema);

            foreach (var batch in batches)
            {
                writer.WriteRecordBatch(batch);
            }

            writer.WriteEnd();
            return memoryStream.ToArray();
        }

        /// <summary>
        /// Helper method to create a simple test schema.
        /// </summary>
        private Schema CreateTestSchema()
        {
            return new Schema.Builder()
                .Field(f => f.Name("id").DataType(Int32Type.Default).Nullable(false))
                .Field(f => f.Name("name").DataType(StringType.Default).Nullable(true))
                .Build();
        }

        /// <summary>
        /// Helper method to create a test record batch.
        /// </summary>
        private RecordBatch CreateTestBatch(Schema schema, int startId, int count)
        {
            var idBuilder = new Int32Array.Builder();
            var nameBuilder = new StringArray.Builder();

            for (int i = 0; i < count; i++)
            {
                idBuilder.Append(startId + i);
                nameBuilder.Append($"Name{startId + i}");
            }

            return new RecordBatch(
                schema,
                new IArrowArray[] { idBuilder.Build(), nameBuilder.Build() },
                count);
        }

        [Fact]
        public void Constructor_NullManifest_ThrowsArgumentNullException()
        {
            // Act & Assert
            var exception = Assert.Throws<ArgumentNullException>(() => new InlineReader(null!));
            Assert.Equal("manifest", exception.ParamName);
        }

        [Fact]
        public void Constructor_InvalidFormat_ThrowsArgumentException()
        {
            // Arrange
            var manifest = new ResultManifest
            {
                Format = "json_array",
                Chunks = new List<ResultChunk>()
            };

            // Act & Assert
            var exception = Assert.Throws<ArgumentException>(() => new InlineReader(manifest));
            Assert.Contains("InlineReader only supports arrow_stream format", exception.Message);
            Assert.Equal("manifest", exception.ParamName);
        }

        [Fact]
        public void Schema_NoChunks_ThrowsInvalidOperationException()
        {
            // Arrange
            var manifest = new ResultManifest
            {
                Format = "arrow_stream",
                Chunks = new List<ResultChunk>()
            };
            var reader = new InlineReader(manifest);

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => reader.Schema);
            Assert.Contains("No chunks with attachment data found", exception.Message);
        }

        [Fact]
        public void Schema_SingleChunk_ReturnsCorrectSchema()
        {
            // Arrange
            var schema = CreateTestSchema();
            var batch = CreateTestBatch(schema, 0, 5);
            var ipcData = CreateArrowIpcStream(schema, batch);

            var manifest = new ResultManifest
            {
                Format = "arrow_stream",
                Chunks = new List<ResultChunk>
                {
                    new ResultChunk
                    {
                        ChunkIndex = 0,
                        RowCount = 5,
                        Attachment = ipcData
                    }
                }
            };

            // Act
            using var reader = new InlineReader(manifest);
            var resultSchema = reader.Schema;

            // Assert
            Assert.NotNull(resultSchema);
            Assert.Equal(2, resultSchema.FieldsList.Count);
            Assert.Equal("id", resultSchema.FieldsList[0].Name);
            Assert.Equal("name", resultSchema.FieldsList[1].Name);
        }

        [Fact]
        public async Task ReadNextRecordBatchAsync_SingleChunkSingleBatch_ReturnsOneBatch()
        {
            // Arrange
            var schema = CreateTestSchema();
            var batch = CreateTestBatch(schema, 0, 5);
            var ipcData = CreateArrowIpcStream(schema, batch);

            var manifest = new ResultManifest
            {
                Format = "arrow_stream",
                Chunks = new List<ResultChunk>
                {
                    new ResultChunk
                    {
                        ChunkIndex = 0,
                        RowCount = 5,
                        Attachment = ipcData
                    }
                }
            };

            // Act
            using var reader = new InlineReader(manifest);
            var resultBatch1 = await reader.ReadNextRecordBatchAsync();
            var resultBatch2 = await reader.ReadNextRecordBatchAsync();

            // Assert
            Assert.NotNull(resultBatch1);
            Assert.Equal(5, resultBatch1.Length);
            Assert.Null(resultBatch2);
        }

        [Fact]
        public async Task ReadNextRecordBatchAsync_SingleChunkMultipleBatches_ReturnsAllBatches()
        {
            // Arrange
            var schema = CreateTestSchema();
            var batch1 = CreateTestBatch(schema, 0, 3);
            var batch2 = CreateTestBatch(schema, 3, 2);
            var ipcData = CreateArrowIpcStream(schema, batch1, batch2);

            var manifest = new ResultManifest
            {
                Format = "arrow_stream",
                Chunks = new List<ResultChunk>
                {
                    new ResultChunk
                    {
                        ChunkIndex = 0,
                        RowCount = 5,
                        Attachment = ipcData
                    }
                }
            };

            // Act
            using var reader = new InlineReader(manifest);
            var resultBatch1 = await reader.ReadNextRecordBatchAsync();
            var resultBatch2 = await reader.ReadNextRecordBatchAsync();
            var resultBatch3 = await reader.ReadNextRecordBatchAsync();

            // Assert
            Assert.NotNull(resultBatch1);
            Assert.Equal(3, resultBatch1.Length);
            Assert.NotNull(resultBatch2);
            Assert.Equal(2, resultBatch2.Length);
            Assert.Null(resultBatch3);
        }

        [Fact]
        public async Task ReadNextRecordBatchAsync_MultipleChunks_ReturnsAllBatchesInOrder()
        {
            // Arrange
            var schema = CreateTestSchema();
            var batch1 = CreateTestBatch(schema, 0, 3);
            var batch2 = CreateTestBatch(schema, 3, 2);
            var ipcData1 = CreateArrowIpcStream(schema, batch1);
            var ipcData2 = CreateArrowIpcStream(schema, batch2);

            var manifest = new ResultManifest
            {
                Format = "arrow_stream",
                Chunks = new List<ResultChunk>
                {
                    new ResultChunk
                    {
                        ChunkIndex = 0,
                        RowCount = 3,
                        Attachment = ipcData1
                    },
                    new ResultChunk
                    {
                        ChunkIndex = 1,
                        RowCount = 2,
                        Attachment = ipcData2
                    }
                }
            };

            // Act
            using var reader = new InlineReader(manifest);
            var resultBatch1 = await reader.ReadNextRecordBatchAsync();
            var resultBatch2 = await reader.ReadNextRecordBatchAsync();
            var resultBatch3 = await reader.ReadNextRecordBatchAsync();

            // Assert
            Assert.NotNull(resultBatch1);
            Assert.Equal(3, resultBatch1.Length);
            Assert.NotNull(resultBatch2);
            Assert.Equal(2, resultBatch2.Length);
            Assert.Null(resultBatch3);
        }

        [Fact]
        public async Task ReadNextRecordBatchAsync_ChunksOutOfOrder_ReturnsInCorrectOrder()
        {
            // Arrange
            var schema = CreateTestSchema();
            var batch1 = CreateTestBatch(schema, 0, 3);
            var batch2 = CreateTestBatch(schema, 3, 2);
            var ipcData1 = CreateArrowIpcStream(schema, batch1);
            var ipcData2 = CreateArrowIpcStream(schema, batch2);

            var manifest = new ResultManifest
            {
                Format = "arrow_stream",
                Chunks = new List<ResultChunk>
                {
                    // Intentionally out of order
                    new ResultChunk
                    {
                        ChunkIndex = 1,
                        RowCount = 2,
                        Attachment = ipcData2
                    },
                    new ResultChunk
                    {
                        ChunkIndex = 0,
                        RowCount = 3,
                        Attachment = ipcData1
                    }
                }
            };

            // Act
            using var reader = new InlineReader(manifest);
            var batches = new List<RecordBatch>();
            RecordBatch? batch;
            while ((batch = await reader.ReadNextRecordBatchAsync()) != null)
            {
                batches.Add(batch);
            }

            // Assert
            Assert.Equal(2, batches.Count);
            Assert.Equal(3, batches[0].Length); // First batch should be from chunk 0
            Assert.Equal(2, batches[1].Length); // Second batch should be from chunk 1
        }

        [Fact]
        public async Task ReadNextRecordBatchAsync_ChunksWithoutAttachment_SkipsThoseChunks()
        {
            // Arrange
            var schema = CreateTestSchema();
            var batch = CreateTestBatch(schema, 0, 3);
            var ipcData = CreateArrowIpcStream(schema, batch);

            var manifest = new ResultManifest
            {
                Format = "arrow_stream",
                Chunks = new List<ResultChunk>
                {
                    new ResultChunk
                    {
                        ChunkIndex = 0,
                        RowCount = 3,
                        Attachment = ipcData
                    },
                    new ResultChunk
                    {
                        ChunkIndex = 1,
                        RowCount = 0,
                        Attachment = null // No attachment
                    }
                }
            };

            // Act
            using var reader = new InlineReader(manifest);
            var resultBatch1 = await reader.ReadNextRecordBatchAsync();
            var resultBatch2 = await reader.ReadNextRecordBatchAsync();

            // Assert
            Assert.NotNull(resultBatch1);
            Assert.Equal(3, resultBatch1.Length);
            Assert.Null(resultBatch2); // Should skip the chunk without attachment
        }

        [Fact]
        public async Task ReadNextRecordBatchAsync_ChunksWithEmptyAttachment_SkipsThoseChunks()
        {
            // Arrange
            var schema = CreateTestSchema();
            var batch = CreateTestBatch(schema, 0, 3);
            var ipcData = CreateArrowIpcStream(schema, batch);

            var manifest = new ResultManifest
            {
                Format = "arrow_stream",
                Chunks = new List<ResultChunk>
                {
                    new ResultChunk
                    {
                        ChunkIndex = 0,
                        RowCount = 3,
                        Attachment = ipcData
                    },
                    new ResultChunk
                    {
                        ChunkIndex = 1,
                        RowCount = 0,
                        Attachment = new byte[0] // Empty attachment
                    }
                }
            };

            // Act
            using var reader = new InlineReader(manifest);
            var resultBatch1 = await reader.ReadNextRecordBatchAsync();
            var resultBatch2 = await reader.ReadNextRecordBatchAsync();

            // Assert
            Assert.NotNull(resultBatch1);
            Assert.Equal(3, resultBatch1.Length);
            Assert.Null(resultBatch2);
        }

        [Fact]
        public async Task ReadNextRecordBatchAsync_InvalidArrowData_ThrowsInvalidOperationException()
        {
            // Arrange
            var manifest = new ResultManifest
            {
                Format = "arrow_stream",
                Chunks = new List<ResultChunk>
                {
                    new ResultChunk
                    {
                        ChunkIndex = 0,
                        RowCount = 3,
                        Attachment = new byte[] { 1, 2, 3, 4 } // Invalid Arrow IPC data
                    }
                }
            };

            // Act & Assert
            using var reader = new InlineReader(manifest);
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await reader.ReadNextRecordBatchAsync());
            Assert.Contains("Failed to read Arrow stream from chunk 0", exception.Message);
        }

        [Fact]
        public async Task ReadNextRecordBatchAsync_AfterDispose_ThrowsObjectDisposedException()
        {
            // Arrange
            var schema = CreateTestSchema();
            var batch = CreateTestBatch(schema, 0, 3);
            var ipcData = CreateArrowIpcStream(schema, batch);

            var manifest = new ResultManifest
            {
                Format = "arrow_stream",
                Chunks = new List<ResultChunk>
                {
                    new ResultChunk
                    {
                        ChunkIndex = 0,
                        RowCount = 3,
                        Attachment = ipcData
                    }
                }
            };

            var reader = new InlineReader(manifest);
            reader.Dispose();

            // Act & Assert
            await Assert.ThrowsAsync<ObjectDisposedException>(
                async () => await reader.ReadNextRecordBatchAsync());
        }

        [Fact]
        public void Schema_AfterDispose_ThrowsObjectDisposedException()
        {
            // Arrange
            var schema = CreateTestSchema();
            var batch = CreateTestBatch(schema, 0, 3);
            var ipcData = CreateArrowIpcStream(schema, batch);

            var manifest = new ResultManifest
            {
                Format = "arrow_stream",
                Chunks = new List<ResultChunk>
                {
                    new ResultChunk
                    {
                        ChunkIndex = 0,
                        RowCount = 3,
                        Attachment = ipcData
                    }
                }
            };

            var reader = new InlineReader(manifest);
            reader.Dispose();

            // Act & Assert
            Assert.Throws<ObjectDisposedException>(() => reader.Schema);
        }

        [Fact]
        public void Dispose_MultipleTimesSchema_DoesNotThrow()
        {
            // Arrange
            var schema = CreateTestSchema();
            var batch = CreateTestBatch(schema, 0, 3);
            var ipcData = CreateArrowIpcStream(schema, batch);

            var manifest = new ResultManifest
            {
                Format = "arrow_stream",
                Chunks = new List<ResultChunk>
                {
                    new ResultChunk
                    {
                        ChunkIndex = 0,
                        RowCount = 3,
                        Attachment = ipcData
                    }
                }
            };

            var reader = new InlineReader(manifest);

            // Act & Assert
            reader.Dispose();
            reader.Dispose(); // Should not throw
        }

        [Fact]
        public async Task ReadNextRecordBatchAsync_VerifyDataValues_ReturnsCorrectData()
        {
            // Arrange
            var schema = CreateTestSchema();
            var batch = CreateTestBatch(schema, 100, 3); // Start from 100 for clear verification
            var ipcData = CreateArrowIpcStream(schema, batch);

            var manifest = new ResultManifest
            {
                Format = "arrow_stream",
                Chunks = new List<ResultChunk>
                {
                    new ResultChunk
                    {
                        ChunkIndex = 0,
                        RowCount = 3,
                        Attachment = ipcData
                    }
                }
            };

            // Act
            using var reader = new InlineReader(manifest);
            var resultBatch = await reader.ReadNextRecordBatchAsync();

            // Assert
            Assert.NotNull(resultBatch);
            Assert.Equal(3, resultBatch.Length);

            var idArray = resultBatch.Column(0) as Int32Array;
            var nameArray = resultBatch.Column(1) as StringArray;

            Assert.NotNull(idArray);
            Assert.NotNull(nameArray);
            Assert.Equal(100, idArray.GetValue(0));
            Assert.Equal(101, idArray.GetValue(1));
            Assert.Equal(102, idArray.GetValue(2));
            Assert.Equal("Name100", nameArray.GetString(0));
            Assert.Equal("Name101", nameArray.GetString(1));
            Assert.Equal("Name102", nameArray.GetString(2));
        }
    }
}
