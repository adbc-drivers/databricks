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
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.StatementExecution;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.StatementExecution
{
    /// <summary>
    /// Validates <see cref="ConcatenatedReadOnlyMemoryStream"/> (the core of SEA fix #2): reading an
    /// Arrow IPC stream split across multiple segments must be byte-equivalent to the old approach of
    /// concatenating every chunk into one buffer, and must avoid that whole-result allocation.
    /// </summary>
    public class ConcatenatedReadOnlyMemoryStreamTest
    {
        private readonly ITestOutputHelper _out;

        public ConcatenatedReadOnlyMemoryStreamTest(ITestOutputHelper output) => _out = output;

        private static byte[] BuildArrowIpc(int rows)
        {
            var schema = new Schema(
                new[]
                {
                    new Field("id", Int32Type.Default, nullable: false),
                    new Field("name", StringType.Default, nullable: true),
                },
                metadata: null);

            var idBuilder = new Int32Array.Builder();
            var nameBuilder = new StringArray.Builder();
            for (int i = 0; i < rows; i++)
            {
                idBuilder.Append(i);
                if (i % 7 == 0) nameBuilder.AppendNull();
                else nameBuilder.Append($"row-{i}-some-padding-text-to-grow-the-buffer");
            }

            var batch = new RecordBatch(schema, new IArrowArray[] { idBuilder.Build(), nameBuilder.Build() }, rows);

            using var ms = new MemoryStream();
            using (var writer = new ArrowStreamWriter(ms, schema))
            {
                writer.WriteRecordBatch(batch);
                writer.WriteEnd();
            }
            return ms.ToArray();
        }

        /// <summary>Split a byte[] into <paramref name="parts"/> segments of nearly-equal size.</summary>
        private static List<ReadOnlyMemory<byte>> SplitEqual(byte[] data, int parts)
        {
            var segments = new List<ReadOnlyMemory<byte>>(parts);
            int chunk = Math.Max(1, data.Length / parts);
            int offset = 0;
            while (offset < data.Length)
            {
                int len = Math.Min(chunk, data.Length - offset);
                segments.Add(new ReadOnlyMemory<byte>(data, offset, len));
                offset += len;
            }
            return segments;
        }

        private static async Task<(int batches, long rows, int firstId, string? lastName)> ReadAllAsync(Stream stream)
        {
            using var reader = new ArrowStreamReader(stream);
            int batches = 0;
            long rows = 0;
            int firstId = -1;
            string? lastName = null;
            RecordBatch? batch;
            while ((batch = await reader.ReadNextRecordBatchAsync()) != null)
            {
                using (batch)
                {
                    batches++;
                    rows += batch.Length;
                    var ids = (Int32Array)batch.Column(0);
                    var names = (StringArray)batch.Column(1);
                    if (firstId == -1 && batch.Length > 0) firstId = ids.GetValue(0)!.Value;
                    if (batch.Length > 0) lastName = names.GetString(batch.Length - 1);
                }
            }
            return (batches, rows, firstId, lastName);
        }

        [Theory]
        [InlineData(1)]     // single segment
        [InlineData(2)]
        [InlineData(5)]     // forces reads across message boundaries
        [InlineData(1000)]  // many tiny segments: stress cross-segment partial reads
        public async Task ConcatenatedStream_MatchesContiguousBuffer(int parts)
        {
            const int rows = 5000;
            byte[] ipc = BuildArrowIpc(rows);
            var segments = SplitEqual(ipc, parts);

            // Baseline: old approach — concatenate segments into one buffer, wrap in MemoryStream.
            byte[] concatenated = new byte[ipc.Length];
            int off = 0;
            foreach (var s in segments) { s.Span.CopyTo(concatenated.AsSpan(off)); off += s.Length; }
            Assert.Equal(ipc, concatenated); // sanity: split+concat round-trips

            var expected = await ReadAllAsync(new MemoryStream(concatenated));
            var actual = await ReadAllAsync(new ConcatenatedReadOnlyMemoryStream(segments));

            Assert.Equal(expected.batches, actual.batches);
            Assert.Equal(rows, actual.rows);
            Assert.Equal(expected.rows, actual.rows);
            Assert.Equal(expected.firstId, actual.firstId);
            Assert.Equal(expected.lastName, actual.lastName);
        }

        [Fact]
        public void ConcatenatedStream_ReportsLengthAndIsForwardOnly()
        {
            var segments = new List<ReadOnlyMemory<byte>>
            {
                new byte[] { 1, 2, 3 },
                ReadOnlyMemory<byte>.Empty,   // empty segment must be skipped
                new byte[] { 4, 5 },
            };
            using var stream = new ConcatenatedReadOnlyMemoryStream(segments);
            Assert.Equal(5, stream.Length);
            Assert.False(stream.CanSeek);
            Assert.True(stream.CanRead);

            // Reading 1 byte at a time crosses every segment boundary (incl. the empty one).
            var collected = new List<byte>();
            var one = new byte[1];
            int n;
            while ((n = stream.Read(one, 0, 1)) > 0) collected.Add(one[0]);
            Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, collected.ToArray());
            Assert.Equal(5, stream.Position);
        }

#if NET6_0_OR_GREATER
        [Fact]
        public void ConcatenatedStream_AvoidsWholeResultAllocation()
        {
            const int parts = 8;
            const int size = 8 * 1024 * 1024;
            var data = new byte[size];
            new Random(11).NextBytes(data);
            var segments = SplitEqual(data, parts);
            const int iters = 200;

            // OLD: concatenate all segments into one buffer (as FetchAllChunksAsync did) + wrap.
            long oldBytes = Measure(() =>
            {
                byte[] result = new byte[size];
                int off = 0;
                foreach (var s in segments) { s.Span.CopyTo(result.AsSpan(off)); off += s.Length; }
                return new MemoryStream(result);
            }, iters);

            // NEW: present segments as a stream — no whole-result buffer.
            long newBytes = Measure(() => new ConcatenatedReadOnlyMemoryStream(segments), iters);

            double oldMb = oldBytes / (double)iters / 1024.0 / 1024.0;
            double newKb = newBytes / (double)iters / 1024.0;
            string line = $"concat {size / 1024 / 1024} MB across {parts} segments: OLD {oldMb:F2} MB/op  NEW {newKb:F3} KB/op";
            _out.WriteLine(line);

            // The new path should allocate essentially nothing per op (no whole-result copy).
            Assert.True(newBytes * 100 < oldBytes,
                $"Expected new allocations ({newKb:F3} KB/op) to be far below old ({oldMb:F2} MB/op)");
        }

        private static long Measure(Func<Stream> action, int iters)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < iters; i++)
            {
                Stream s = action();
                GC.KeepAlive(s);
            }
            return GC.GetAllocatedBytesForCurrentThread() - before;
        }
#endif
    }
}
