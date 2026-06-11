/*
* Copyright (c) 2026 ADBC Drivers Contributors
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
using System.Buffers;
using System.IO;
using K4os.Compression.LZ4.Streams;
using Microsoft.IO;
using Xunit;

namespace AdbcDrivers.Databricks.Tests
{
    /// <summary>
    /// Safety proof for fix #4: <see cref="CustomLZ4FrameReader.ReleaseBuffer"/> now returns the LZ4
    /// decoder's work buffer to the pool WITHOUT clearing it. This is only safe if the decoder never
    /// surfaces bytes it did not decode into the buffer. These tests force exactly the worst case —
    /// a pooled buffer pre-filled with poison (0xFF) on every rent — and assert decompression output
    /// is still byte-exact, demonstrating the clear was unnecessary for correctness.
    /// </summary>
    public class Lz4DecompressNoStaleDataTest
    {
        /// <summary>
        /// ArrayPool that hands back a single retained buffer, re-poisoned with 0xFF on every rent,
        /// simulating a pooled work buffer that still holds bytes from a prior (unrelated) decompression.
        /// </summary>
        private sealed class PoisonPool : ArrayPool<byte>
        {
            private byte[]? _buffer;

            public override byte[] Rent(int minimumLength)
            {
                if (_buffer == null || _buffer.Length < minimumLength)
                {
                    _buffer = new byte[minimumLength];
                }
                _buffer.AsSpan().Fill(0xFF); // worst-case stale data
                return _buffer;
            }

            public override void Return(byte[] array, bool clearArray = false)
            {
                if (clearArray)
                {
                    Array.Clear(array, 0, array.Length);
                }
                // Retain the (already-stored) buffer so the next Rent reuses it.
            }
        }

        private static byte[] Lz4Frame(byte[] src)
        {
            using var ms = new MemoryStream();
            using (var enc = LZ4Stream.Encode(ms, leaveOpen: true))
            {
                enc.Write(src, 0, src.Length);
            }
            return ms.ToArray();
        }

        [Theory]
        [InlineData(100)]            // tiny
        [InlineData(64 * 1024)]      // ~one default block
        [InlineData(1024 * 1024)]    // multi-block
        public void Decompress_OverPoisonedReusedBuffer_ReturnsExactBytes(int size)
        {
            var payload = new byte[size];
            new Random(99).NextBytes(payload);
            byte[] compressed = Lz4Frame(payload);

            var manager = new RecyclableMemoryStreamManager();
            var poison = new PoisonPool();

            // Decompress twice through the same never-cleared, re-poisoned work buffer.
            byte[] first = Lz4Utilities.DecompressLz4(compressed, manager, poison).ToArray();
            byte[] second = Lz4Utilities.DecompressLz4(compressed, manager, poison).ToArray();

            // Exact equality proves no poison bytes leaked into the output and the length is exact:
            // the decoder exposed only the bytes it decoded, so clearing on return is unnecessary.
            Assert.Equal(payload, first);
            Assert.Equal(payload, second);
        }
    }
}
