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
using Apache.Arrow.Adbc;
using K4os.Compression.LZ4.Streams;
using Microsoft.IO;
using Moq;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit
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

        // ES-2356918: a CloudFetch file can be many concatenated LZ4 frames. K4os returns 0 from a read
        // that starts on a frame's end mark: after a read that filled the buffer exactly as a frame ended,
        // or at an empty frame. That 0 used to end the stream and drop every later frame.
        private static (byte[] Compressed, byte[] Expected) MultiFrame(params int[] frameSizes)
        {
            using var compressed = new MemoryStream();
            using var expected = new MemoryStream();
            for (int i = 0; i < frameSizes.Length; i++)
            {
                var data = new byte[frameSizes[i]];
                new Random(i).NextBytes(data);
                expected.Write(data, 0, data.Length);
                using var compressor = LZ4Stream.Encode(compressed, leaveOpen: true);
                compressor.Write(data, 0, data.Length);
            }
            return (compressed.ToArray(), expected.ToArray());
        }

        // Read with an explicit buffer size: the customer's .NET Framework CopyToAsync reads 81,920 bytes,
        // while .NET 8's CopyTo rents a 131,072-byte buffer (which is why CI on .NET 8 never hit this).
        [Theory]
        [InlineData(81_920, 81_920)]
        [InlineData(163_840, 81_920)]
        [InlineData(131_072, 131_072)]
        [InlineData(0, 81_920)] // empty frame
        public async Task DecoderStream_FrameEndingAtReadBufferEnd_ReadsAllFrames(int frameSize, int bufferSize)
        {
            var (compressed, expected) = MultiFrame(400, frameSize, 30_000);
            foreach (var useAsync in new[] { false, true })
            {
                using var decoder = new CustomLZ4DecoderStream(new MemoryStream(compressed), d => d.CreateDecoder(), _bufferPool);
                using var output = new MemoryStream();
                var buffer = new byte[bufferSize];
                int read;
                while ((read = useAsync ? await decoder.ReadAsync(buffer, 0, buffer.Length) : decoder.Read(buffer, 0, buffer.Length)) > 0)
                    output.Write(buffer, 0, read);
                Assert.Equal(expected, output.ToArray());
            }
        }

        [Fact]
        public void DecoderStream_ReadByte_ReadsAllFrames()
        {
            var (compressed, expected) = MultiFrame(3, 0, 2);
            using var decoder = new CustomLZ4DecoderStream(new MemoryStream(compressed), d => d.CreateDecoder(), _bufferPool);
            var output = new MemoryStream();
            int value;
            while ((value = decoder.ReadByte()) >= 0) output.WriteByte((byte)value);
            Assert.Equal(expected, output.ToArray());
        }

        [Fact]
        public void DecoderStream_NonSeekableInput_Throws()
        {
            var nonSeekable = new Mock<Stream>();
            nonSeekable.Setup(s => s.CanSeek).Returns(false);
            Assert.Throws<ArgumentException>(() =>
                new CustomLZ4DecoderStream(nonSeekable.Object, d => d.CreateDecoder(), _bufferPool));
        }

        [Fact]
        public async Task DecompressLz4_MultipleFramesIncludingEmpty_ReturnsAllData()
        {
            var (compressed, expected) = MultiFrame(0, 81_920, 0, 131_072, 70_000, 0);
            Assert.Equal(expected, Lz4Utilities.DecompressLz4(compressed, _memoryManager, _bufferPool).ToArray());
            using var asyncResult = await Lz4Utilities.DecompressLz4Async(compressed, _memoryManager, _bufferPool);
            Assert.Equal(expected, asyncResult.ToArray());
        }
    }
}
