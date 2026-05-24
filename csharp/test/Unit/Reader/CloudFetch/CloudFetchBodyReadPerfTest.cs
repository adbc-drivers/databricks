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
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Reader.CloudFetch;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.CloudFetch
{
    /// <summary>
    /// Measures the effect of fix #1 (eliminating the redundant ToArray copy in the CloudFetch
    /// body read) against the REAL <see cref="CloudFetchDownloader.ReadResponseBodyAsync"/> method.
    /// The body is read from a genuine HttpContent response stream (the same type the production
    /// path consumes via response.Content.ReadAsStreamAsync()).
    /// </summary>
    public class CloudFetchBodyReadPerfTest
    {
        private readonly ITestOutputHelper _out;

        public CloudFetchBodyReadPerfTest(ITestOutputHelper output) => _out = output;

        private static byte[] MakePayload(int size)
        {
            var data = new byte[size];
            new Random(7).NextBytes(data);
            return data;
        }

        /// <summary>A fresh HttpContent body stream of the given bytes, with Content-Length set.</summary>
        private static async Task<Stream> BodyStreamAsync(byte[] payload)
        {
            var content = new ByteArrayContent(payload);
            content.Headers.ContentLength = payload.Length;
            return await content.ReadAsStreamAsync().ConfigureAwait(false);
        }

        /// <summary>Replica of the pre-fix body read (CloudFetchDownloader before fix #1).</summary>
        private static async Task<byte[]> OldPattern(Stream contentStream, long? contentLength, CancellationToken ct)
        {
            int capacity = contentLength.HasValue && contentLength.Value > 0 ? (int)contentLength.Value : 0;
            using var ms = new MemoryStream(capacity);
            await contentStream.CopyToAsync(ms, 81920, ct).ConfigureAwait(false);
            return ms.ToArray();
        }

        [Theory]
        [InlineData(1 * 1024 * 1024)]
        [InlineData(8 * 1024 * 1024)]
        public async Task BodyRead_RealMethod_AllocationsAndTime(int size)
        {
            byte[] payload = MakePayload(size);
            const int iters = 200;
            long len = payload.Length;

            // --- Correctness: the real method returns exactly the same bytes as the old pattern. ---
            byte[] oldBytes = await OldPattern(await BodyStreamAsync(payload), len, CancellationToken.None);
            byte[] newBytes = await CloudFetchDownloader.ReadResponseBodyAsync(await BodyStreamAsync(payload), len, CancellationToken.None);
            Assert.Equal(payload.Length, newBytes.Length);
            Assert.Equal(payload, newBytes);
            Assert.Equal(oldBytes, newBytes);

            // Also exercise the unknown-length fallback path for correctness.
            byte[] unknownLen = await CloudFetchDownloader.ReadResponseBodyAsync(await BodyStreamAsync(payload), null, CancellationToken.None);
            Assert.Equal(payload, unknownLen);

            // --- Warm up both arms. ---
            for (int i = 0; i < 20; i++)
            {
                _ = await OldPattern(await BodyStreamAsync(payload), len, CancellationToken.None);
                _ = await CloudFetchDownloader.ReadResponseBodyAsync(await BodyStreamAsync(payload), len, CancellationToken.None);
            }

            var old = await Measure(async () => await OldPattern(await BodyStreamAsync(payload), len, CancellationToken.None), iters);
            var neo = await Measure(async () => await CloudFetchDownloader.ReadResponseBodyAsync(await BodyStreamAsync(payload), len, CancellationToken.None), iters);

            double oldMb = old.bytes / (double)iters / 1024.0 / 1024.0;
            double newMb = neo.bytes / (double)iters / 1024.0 / 1024.0;
            string l1 = $"payload = {size / 1024 / 1024} MB ({Environment.Version}), iterations = {iters}";
            string l2 = $"  OLD (MemoryStream + ToArray) : {oldMb,6:F2} MB/op   {old.ms / iters,7:F3} ms/op";
            string l3 = $"  NEW (ReadResponseBodyAsync)  : {newMb,6:F2} MB/op   {neo.ms / iters,7:F3} ms/op";
            string l4 = $"  alloc ratio NEW/OLD = {(double)neo.bytes / old.bytes:F2}   time ratio NEW/OLD = {neo.ms / old.ms:F2}";
            _out.WriteLine(l1); _out.WriteLine(l2); _out.WriteLine(l3); _out.WriteLine(l4);
            // Also persist to a file so results are captured regardless of test-logger verbosity.
            try
            {
                string path = Path.Combine(Path.GetTempPath(), "bodyread_perf.txt");
                File.AppendAllText(path, l1 + "\n" + l2 + "\n" + l3 + "\n" + l4 + "\n\n");
            }
            catch { /* best effort */ }

            // The fix should not allocate more than the old pattern; in practice it roughly halves it.
            Assert.True(neo.bytes <= old.bytes,
                $"Expected new allocations ({newMb:F2} MB/op) <= old ({oldMb:F2} MB/op)");
        }

        private static async Task<(long bytes, double ms)> Measure(Func<Task<byte[]>> action, int iters)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            long before = GC.GetAllocatedBytesForCurrentThread();
            var sw = Stopwatch.StartNew();
            for (int i = 0; i < iters; i++)
            {
                byte[] r = await action().ConfigureAwait(false);
                GC.KeepAlive(r);
            }
            sw.Stop();
            long after = GC.GetAllocatedBytesForCurrentThread();
            return (after - before, sw.Elapsed.TotalMilliseconds);
        }
    }
}
