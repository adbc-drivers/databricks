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
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Reader.CloudFetch;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Reader.CloudFetch
{
    public class CloudFetchMemoryBufferManagerTests
    {
        [Fact]
        public void Constructor_DefaultMaxMemory_Is200MB()
        {
            var manager = new CloudFetchMemoryBufferManager();
            Assert.Equal(200 * 1024L * 1024L, manager.MaxMemory);
            Assert.Equal(0, manager.UsedMemory);
        }

        [Fact]
        public void Constructor_InvalidMaxMemory_Throws()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new CloudFetchMemoryBufferManager(0));
            Assert.Throws<ArgumentOutOfRangeException>(() => new CloudFetchMemoryBufferManager(-1));
        }

        [Fact]
        public void TryAcquireMemory_WithinLimit_Success()
        {
            var manager = new CloudFetchMemoryBufferManager(maxMemoryMB: 10);
            Assert.True(manager.TryAcquireMemory(1024));
            Assert.Equal(1024, manager.UsedMemory);
        }

        [Fact]
        public void TryAcquireMemory_ExceedingLimit_Fails()
        {
            var manager = new CloudFetchMemoryBufferManager(maxMemoryMB: 1);
            Assert.False(manager.TryAcquireMemory(2 * 1024 * 1024));
            Assert.Equal(0, manager.UsedMemory);
        }

        [Fact]
        public void TryAcquireMemory_InvalidSize_Throws()
        {
            var manager = new CloudFetchMemoryBufferManager();
            Assert.Throws<ArgumentOutOfRangeException>(() => manager.TryAcquireMemory(0));
            Assert.Throws<ArgumentOutOfRangeException>(() => manager.TryAcquireMemory(-1));
        }

        [Fact]
        public void ReleaseMemory_Success()
        {
            var manager = new CloudFetchMemoryBufferManager(maxMemoryMB: 10);
            manager.TryAcquireMemory(1024);
            manager.ReleaseMemory(1024);
            Assert.Equal(0, manager.UsedMemory);
        }

        [Fact]
        public void ReleaseMemory_MoreThanAcquired_Throws()
        {
            var manager = new CloudFetchMemoryBufferManager(maxMemoryMB: 10);
            manager.TryAcquireMemory(1024);
            Assert.Throws<InvalidOperationException>(() => manager.ReleaseMemory(2048));
        }

        [Fact]
        public void ReleaseMemory_InvalidSize_Throws()
        {
            var manager = new CloudFetchMemoryBufferManager();
            Assert.Throws<ArgumentOutOfRangeException>(() => manager.ReleaseMemory(0));
            Assert.Throws<ArgumentOutOfRangeException>(() => manager.ReleaseMemory(-1));
        }

        [Fact]
        public void MultipleAcquireRelease_TracksCorrectly()
        {
            var manager = new CloudFetchMemoryBufferManager(maxMemoryMB: 10);
            manager.TryAcquireMemory(1024);
            manager.TryAcquireMemory(2048);
            Assert.Equal(3072, manager.UsedMemory);
            manager.ReleaseMemory(1024);
            Assert.Equal(2048, manager.UsedMemory);
        }

        [Fact]
        public async Task AcquireMemoryAsync_Success()
        {
            var manager = new CloudFetchMemoryBufferManager(maxMemoryMB: 10);
            await manager.AcquireMemoryAsync(1024, CancellationToken.None);
            Assert.Equal(1024, manager.UsedMemory);
        }

        [Fact]
        public async Task AcquireMemoryAsync_ExceedingMax_Throws()
        {
            var manager = new CloudFetchMemoryBufferManager(maxMemoryMB: 1);
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
                manager.AcquireMemoryAsync(2 * 1024 * 1024, CancellationToken.None));
        }

        [Fact]
        public async Task AcquireMemoryAsync_WaitsForRelease()
        {
            var manager = new CloudFetchMemoryBufferManager(maxMemoryMB: 1);
            long halfMax = 512 * 1024;
            manager.TryAcquireMemory(halfMax);

            var acquireTask = Task.Run(() => manager.AcquireMemoryAsync(halfMax + 1, CancellationToken.None));
            await Task.Delay(50);
            Assert.False(acquireTask.IsCompleted);

            manager.ReleaseMemory(halfMax);
            await acquireTask;
            Assert.Equal(halfMax + 1, manager.UsedMemory);
        }

        [Fact]
        public async Task ConcurrentOperations_ThreadSafe()
        {
            var manager = new CloudFetchMemoryBufferManager(maxMemoryMB: 100);
            var tasks = new Task<bool>[10];
            for (int i = 0; i < 10; i++)
                tasks[i] = Task.Run(() => manager.TryAcquireMemory(1024));

            await Task.WhenAll(tasks);
            Assert.Equal(10 * 1024, manager.UsedMemory);
        }
    }
}
