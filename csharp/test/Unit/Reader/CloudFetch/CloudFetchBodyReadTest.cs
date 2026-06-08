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
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Reader.CloudFetch;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.CloudFetch
{
    /// <summary>
    /// Correctness tests for <see cref="CloudFetchDownloader.ReadResponseBodyAsync"/>, focused on the
    /// paths where the stream's actual length disagrees with the declared Content-Length. The method
    /// must always return exactly the bytes the stream produced, regardless of the declared length.
    /// These run on all target frameworks (unlike the NET6+-only allocation benchmark in
    /// CloudFetchBodyReadPerfTest), and exercise the abnormal branch that the (offset + extra) sizing
    /// optimizes — a truncated body must not over-allocate, an over-read must still drain fully.
    /// </summary>
    public class CloudFetchBodyReadTest
    {
        private static byte[] MakePayload(int size)
        {
            var data = new byte[size];
            new Random(13).NextBytes(data);
            return data;
        }

        [Fact]
        public async Task ExactContentLength_ReturnsAllBytes()
        {
            byte[] payload = MakePayload(64 * 1024);
            byte[] result = await CloudFetchDownloader.ReadResponseBodyAsync(
                new MemoryStream(payload, writable: false), payload.Length, CancellationToken.None);
            Assert.Equal(payload, result);
        }

        [Fact]
        public async Task UnknownContentLength_ReturnsAllBytes()
        {
            byte[] payload = MakePayload(64 * 1024);
            byte[] result = await CloudFetchDownloader.ReadResponseBodyAsync(
                new MemoryStream(payload, writable: false), contentLength: null, CancellationToken.None);
            Assert.Equal(payload, result);
        }

        [Fact]
        public async Task ZeroContentLength_WithData_ReturnsAllBytes()
        {
            // Content-Length 0 falls through to the unknown-length (growable) branch.
            byte[] payload = MakePayload(4096);
            byte[] result = await CloudFetchDownloader.ReadResponseBodyAsync(
                new MemoryStream(payload, writable: false), contentLength: 0, CancellationToken.None);
            Assert.Equal(payload, result);
        }

        [Fact]
        public async Task TruncatedBody_DeclaredLargerThanActual_ReturnsBytesReceived()
        {
            byte[] payload = MakePayload(500_000);
            // Declare far more than the stream actually contains: the abnormal branch must return
            // only what was received (and, with offset + extra sizing, not over-allocate to 4 MB).
            byte[] result = await CloudFetchDownloader.ReadResponseBodyAsync(
                new MemoryStream(payload, writable: false), contentLength: 4 * 1024 * 1024, CancellationToken.None);
            Assert.Equal(payload, result);
        }

        [Fact]
        public async Task MoreThanDeclared_DeclaredSmallerThanActual_ReturnsAllBytes()
        {
            byte[] payload = MakePayload(1_000_000);
            // Declare fewer bytes than the stream actually contains: the over-read probe + drain must
            // recover every byte.
            byte[] result = await CloudFetchDownloader.ReadResponseBodyAsync(
                new MemoryStream(payload, writable: false), contentLength: 500_000, CancellationToken.None);
            Assert.Equal(payload, result);
        }

        [Fact]
        public async Task EmptyBodyWithDeclaredLength_ReturnsEmpty()
        {
            byte[] result = await CloudFetchDownloader.ReadResponseBodyAsync(
                new MemoryStream(Array.Empty<byte>()), contentLength: 1024, CancellationToken.None);
            Assert.Empty(result);
        }
    }
}
