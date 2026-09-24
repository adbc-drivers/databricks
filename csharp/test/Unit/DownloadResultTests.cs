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
using System.IO;
using AdbcDrivers.Databricks.Reader.CloudFetch;
using Moq;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    public class DownloadResultTests
    {
        private static DownloadResult NewResult() =>
            new DownloadResult(
                chunkIndex: 0,
                fileUrl: "https://example.com/chunk0.arrow",
                startRowOffset: 0,
                rowCount: 100,
                byteCount: 1024,
                expirationTime: DateTime.UtcNow.AddHours(1),
                memoryManager: new Mock<ICloudFetchMemoryBufferManager>().Object,
                httpHeaders: null);

        [Fact]
        public void SetFailed_AfterDispose_DoesNotThrow()
        {
            var result = NewResult();
            result.Dispose();

            // When a result set is abandoned mid-stream, a download's fire-and-forget continuation can
            // report a late failure on an already-disposed DownloadResult. That must no-op rather than
            // throw ObjectDisposedException, which would surface as an unobserved task exception.
            var ex = Record.Exception(() => result.SetFailed(new InvalidOperationException("boom")));

            Assert.Null(ex);
        }

        [Fact]
        public void SetCompleted_AfterDispose_DoesNotThrow_AndDisposesStream()
        {
            var result = NewResult();
            result.Dispose();
            var stream = new MemoryStream(new byte[] { 1, 2, 3 });

            var ex = Record.Exception(() => result.SetCompleted(stream, stream.Length));

            Assert.Null(ex);
            Assert.False(stream.CanRead); // the stream is disposed so it isn't leaked
        }

        [Fact]
        public void SetFailed_NullException_Throws()
        {
            var result = NewResult();

            Assert.Throws<ArgumentNullException>(() => result.SetFailed(null!));
        }
    }
}
