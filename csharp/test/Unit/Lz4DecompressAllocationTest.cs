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

#if NET6_0_OR_GREATER

using System;
using System.Buffers;
using System.IO;
using K4os.Compression.LZ4.Streams;
using Microsoft.IO;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests
{
    /// <summary>
    /// Validates SEA/inline fix #3: <see cref="Lz4Utilities.DecompressLz4(byte[], RecyclableMemoryStreamManager, ArrayPool{byte})"/>
    /// now decompresses into a pooled RecyclableMemoryStream + exact ToArray, instead of a plain
    /// MemoryStream that grows geometrically (LOH churn) and returns an over-sized GetBuffer.
    /// Compares the real method's allocation against the previous approach, using the same decoder
    /// (<see cref="CustomLZ4DecoderStream"/>) so only the output-buffer strategy differs.
    /// </summary>
    public class Lz4DecompressAllocationTest
    {
        private readonly ITestOutputHelper _out;

        public Lz4DecompressAllocationTest(ITestOutputHelper output) => _out = output;

        private static byte[] MakeCompressible(int size)
        {
            var rng = new Random(31);
            var data = new byte[size];
            int i = 0;
            while (i < size)
            {
                if (rng.Next(100) < 60)
                {
                    int run = Math.Min(rng.Next(8, 64), size - i);
                    byte v = (byte)rng.Next(256);
                    for (int j = 0; j < run; j++) data[i++] = v;
                }
                else
                {
                    int noise = Math.Min(rng.Next(4, 24), size - i);
                    for (int j = 0; j < noise; j++) data[i++] = (byte)rng.Next(256);
                }
            }
            return data;
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

        // Previous behavior: decompress into a plain (growing) MemoryStream and hand back GetBuffer.
        // Uses the same CustomLZ4DecoderStream as the real method so only the output buffer differs.
        private static ReadOnlyMemory<byte> OldDecompress(byte[] compressed, ArrayPool<byte> pool)
        {
            using var output = new MemoryStream();
            using (var input = new MemoryStream(compressed))
            using (var dec = new CustomLZ4DecoderStream(input, d => d.CreateDecoder(), pool, leaveOpen: false, interactive: false))
            {
                dec.CopyTo(output, 81920);
            }
            byte[] buf = output.GetBuffer();
            return new ReadOnlyMemory<byte>(buf, 0, (int)output.Length);
        }

        [Fact]
        public void DecompressLz4_RoundTripsAndAllocatesLess()
        {
            const int size = 4 * 1024 * 1024;
            byte[] original = MakeCompressible(size);
            byte[] compressed = Lz4Frame(original);

            var pool = ArrayPool<byte>.Create(4 * 1024 * 1024, 16);
            var manager = new RecyclableMemoryStreamManager();

            // Correctness: the real method reproduces the original bytes exactly...
            byte[] decompressed = Lz4Utilities.DecompressLz4(compressed, manager, pool).ToArray();
            Assert.Equal(original, decompressed);
            // ...and matches the previous approach's output.
            Assert.Equal(OldDecompress(compressed, pool).ToArray(), decompressed);

            const int iters = 100;
            for (int i = 0; i < 10; i++) // warm both buffer pools
            {
                _ = OldDecompress(compressed, pool);
                _ = Lz4Utilities.DecompressLz4(compressed, manager, pool);
            }

            long oldBytes = Measure(() => OldDecompress(compressed, pool), iters);
            long newBytes = Measure(() => Lz4Utilities.DecompressLz4(compressed, manager, pool), iters);

            double oldMb = oldBytes / (double)iters / 1024.0 / 1024.0;
            double newMb = newBytes / (double)iters / 1024.0 / 1024.0;
            string line = $"LZ4 decompress {size / 1024 / 1024} MB (decompressed): OLD {oldMb:F2} MB/op  NEW {newMb:F2} MB/op  ratio NEW/OLD {newMb / oldMb:F2}";
            _out.WriteLine(line);

            Assert.True(newBytes < oldBytes,
                $"Expected new allocations ({newMb:F2} MB/op) < old ({oldMb:F2} MB/op)");
        }

        private static long Measure(Func<ReadOnlyMemory<byte>> action, int iters)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < iters; i++)
            {
                ReadOnlyMemory<byte> r = action();
                GC.KeepAlive(r);
            }
            return GC.GetAllocatedBytesForCurrentThread() - before;
        }
    }
}

#endif
