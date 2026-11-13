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
using System.Buffers;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using K4os.Compression.LZ4.Streams;
using Microsoft.IO;
using Xunit;

namespace AdbcDrivers.Databricks.Tests
{
    public class Lz4UtilitiesTests
    {
        private readonly ArrayPool<byte> _bufferPool = ArrayPool<byte>.Create(4 * 1024 * 1024, 10);
        private readonly RecyclableMemoryStreamManager _memoryManager = new();

        private byte[] Compress(string input)
        {
            var bytes = Encoding.UTF8.GetBytes(input);
            using var output = new MemoryStream();
            using (var compressor = LZ4Stream.Encode(output, leaveOpen: true))
                compressor.Write(bytes, 0, bytes.Length);
            return output.ToArray();
        }

        [Fact]
        public void DecompressLz4_ValidData_Success()
        {
            var compressed = Compress("test data");
            var result = Lz4Utilities.DecompressLz4(compressed, _memoryManager, _bufferPool);
            Assert.Equal("test data", Encoding.UTF8.GetString(result.ToArray()));
        }

        [Fact]
        public void DecompressLz4_InvalidData_ThrowsAdbcException()
        {
            var invalid = new byte[] { 0x00, 0x01, 0x02 };
            var ex = Assert.Throws<AdbcException>(() => Lz4Utilities.DecompressLz4(invalid, _memoryManager, _bufferPool));
            Assert.Contains("Failed to decompress LZ4 data", ex.Message);
        }

        [Fact]
        public async Task DecompressLz4Async_ValidData_Success()
        {
            var compressed = Compress("async test");
            using var result = await Lz4Utilities.DecompressLz4Async(compressed, _memoryManager, _bufferPool);
            Assert.Equal("async test", Encoding.UTF8.GetString(result.ToArray()));
            Assert.Equal(0, result.Position); // Stream positioned at start
        }

        [Fact]
        public async Task DecompressLz4Async_InvalidData_ThrowsAdbcException()
        {
            var invalid = new byte[] { 0xFF, 0xFE };
            await Assert.ThrowsAsync<AdbcException>(() =>
                Lz4Utilities.DecompressLz4Async(invalid, _memoryManager, _bufferPool));
        }

        // ES-2356918: the server can emit a CloudFetch file as several concatenated LZ4 frames
        // (including empty frames). The K4os frame reader returns 0 from a read that hits an empty
        // frame, and CopyTo treated that as end-of-stream, silently dropping every later frame.

        private static byte[] CompressFrame(byte[] data)
        {
            using var output = new MemoryStream();
            using (var compressor = LZ4Stream.Encode(output, leaveOpen: true))
                compressor.Write(data, 0, data.Length);
            return output.ToArray();
        }

        private static byte[] Payload(int length, int seed)
        {
            var bytes = new byte[length];
            new Random(seed).NextBytes(bytes);
            return bytes;
        }

        private static (byte[] Compressed, byte[] Expected) MultiFrame(params int[] frameSizes)
        {
            using var compressed = new MemoryStream();
            using var expected = new MemoryStream();
            for (int i = 0; i < frameSizes.Length; i++)
            {
                var data = Payload(frameSizes[i], i);
                expected.Write(data, 0, data.Length);
                var frame = CompressFrame(data);
                compressed.Write(frame, 0, frame.Length);
            }
            return (compressed.ToArray(), expected.ToArray());
        }

        public static TheoryData<int[]> MultiFrameLayouts => new()
        {
            // Empty frame between data frames (the layout reproduced from the incident).
            { new[] { 100_000, 0, 100_000 } },
            // First frame ends exactly on the default CopyTo buffer size (81920).
            { new[] { 81_920, 50_000 } },
            // Several consecutive empty frames, then data.
            { new[] { 4_096, 0, 0, 0, 4_096 } },
            // Leading empty frame.
            { new[] { 0, 70_000 } },
            // Trailing empty frame.
            { new[] { 70_000, 0 } },
            // Many frames of mixed sizes, some aligned to the buffer.
            { new[] { 81_920 * 2, 1, 0, 81_920, 12_345, 0, 81_920 * 3, 7 } },
        };

        [Theory]
        [MemberData(nameof(MultiFrameLayouts))]
        public void DecompressLz4_MultipleFrames_ReturnsAllData(int[] frameSizes)
        {
            var (compressed, expected) = MultiFrame(frameSizes);
            var result = Lz4Utilities.DecompressLz4(compressed, _memoryManager, _bufferPool);
            Assert.Equal(expected.Length, result.Length);
            Assert.True(expected.AsSpan().SequenceEqual(result.Span));
        }

        [Theory]
        [MemberData(nameof(MultiFrameLayouts))]
        public async Task DecompressLz4Async_MultipleFrames_ReturnsAllData(int[] frameSizes)
        {
            var (compressed, expected) = MultiFrame(frameSizes);
            using var result = await Lz4Utilities.DecompressLz4Async(compressed, _memoryManager, _bufferPool);
            Assert.Equal(expected.Length, result.Length);
            Assert.True(expected.AsSpan().SequenceEqual(result.ToArray()));
        }

        [Fact]
        public void DecompressLz4_OnlyEmptyFrames_ReturnsEmpty()
        {
            var (compressed, _) = MultiFrame(0, 0, 0);
            var result = Lz4Utilities.DecompressLz4(compressed, _memoryManager, _bufferPool);
            Assert.Equal(0, result.Length);
        }

        [Fact]
        public void DecompressLz4_TruncatedSecondFrame_Throws()
        {
            var (compressed, _) = MultiFrame(50_000, 50_000);
            var truncated = compressed.AsSpan(0, compressed.Length - 10).ToArray();
            Assert.Throws<AdbcException>(() => Lz4Utilities.DecompressLz4(truncated, _memoryManager, _bufferPool));
        }

        [Fact]
        public async Task DecompressLz4Async_ArrowStreamSplitByEmptyFrame_ReturnsAllRows()
        {
            // Mirrors the incident: an Arrow IPC result whose LZ4 encoding contains an empty frame
            // after the 6th 4,096-row batch. Before the fix the reader saw 24,576 rows and no error.
            const int batchRows = 4096, batchCount = 12, splitAfter = 6;
            var schema = new Schema.Builder().Field(f => f.Name("id").DataType(Int64Type.Default)).Build();

            byte[] head, tail;
            using (var ipc = new MemoryStream())
            {
                using (var writer = new ArrowStreamWriter(ipc, schema, leaveOpen: true))
                {
                    long next = 0;
                    long headLength = 0;
                    for (int b = 0; b < batchCount; b++)
                    {
                        var ids = new Int64Array.Builder();
                        for (int r = 0; r < batchRows; r++) ids.Append(next++);
                        await writer.WriteRecordBatchAsync(new RecordBatch(schema, new IArrowArray[] { ids.Build() }, batchRows));
                        if (b == splitAfter - 1) headLength = ipc.Length;
                    }
                    await writer.WriteEndAsync();
                    var all = ipc.ToArray();
                    head = all.AsSpan(0, (int)headLength).ToArray();
                    tail = all.AsSpan((int)headLength).ToArray();
                }
            }

            using var compressed = new MemoryStream();
            foreach (var part in new[] { head, System.Array.Empty<byte>(), tail })
            {
                var frame = CompressFrame(part);
                compressed.Write(frame, 0, frame.Length);
            }

            using var decompressed = await Lz4Utilities.DecompressLz4Async(compressed.ToArray(), _memoryManager, _bufferPool);
            using var reader = new ArrowStreamReader(decompressed);
            long rows = 0;
            RecordBatch? batch;
            while ((batch = await reader.ReadNextRecordBatchAsync()) != null)
            {
                using (batch) rows += batch.Length;
            }
            Assert.Equal((long)batchRows * batchCount, rows);
        }
    }
}
