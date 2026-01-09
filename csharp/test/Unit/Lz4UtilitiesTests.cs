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
            var result = Lz4Utilities.DecompressLz4(compressed, _bufferPool);
            Assert.Equal("test data", Encoding.UTF8.GetString(result.ToArray()));
        }

        [Fact]
        public void DecompressLz4_InvalidData_ThrowsAdbcException()
        {
            var invalid = new byte[] { 0x00, 0x01, 0x02 };
            var ex = Assert.Throws<AdbcException>(() => Lz4Utilities.DecompressLz4(invalid, _bufferPool));
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
    }
}
