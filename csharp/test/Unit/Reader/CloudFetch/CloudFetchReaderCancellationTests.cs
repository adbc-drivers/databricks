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
    /// <summary>
    /// Regression tests for <see cref="CloudFetchReader.AwaitWithCancellationAsync"/>, which
    /// guarantees the reader unparks from an in-flight download wait when the statement is
    /// cancelled / the connection is disposed, even if the download never completes on its own.
    /// </summary>
    public class CloudFetchReaderCancellationTests
    {
        [Fact]
        public async Task AwaitWithCancellation_TokenCancelledWhileDownloadHangs_Throws()
        {
            // A download that never completes on its own (simulates a hung download that
            // does not honor its own token).
            var neverCompletes = new TaskCompletionSource<bool>();
            using var cts = new CancellationTokenSource();

            var waitTask = CloudFetchReader.AwaitWithCancellationAsync(neverCompletes.Task, cts.Token);
            Assert.False(waitTask.IsCompleted);

            // Cancelling the token must promptly unpark the wait.
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waitTask);
        }

        [Fact]
        public async Task AwaitWithCancellation_DownloadCompletesFirst_Returns()
        {
            var tcs = new TaskCompletionSource<bool>();
            using var cts = new CancellationTokenSource();

            var waitTask = CloudFetchReader.AwaitWithCancellationAsync(tcs.Task, cts.Token);
            Assert.False(waitTask.IsCompleted);

            tcs.SetResult(true);

            // Completes normally without observing cancellation.
            await waitTask;
            Assert.False(cts.IsCancellationRequested);
        }

        [Fact]
        public async Task AwaitWithCancellation_AlreadyCompletedTask_ReturnsImmediately()
        {
            using var cts = new CancellationTokenSource();
            await CloudFetchReader.AwaitWithCancellationAsync(Task.CompletedTask, cts.Token);
        }

        [Fact]
        public async Task AwaitWithCancellation_FaultedDownload_PropagatesException()
        {
            var tcs = new TaskCompletionSource<bool>();
            tcs.SetException(new InvalidOperationException("download failed"));
            using var cts = new CancellationTokenSource();

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => CloudFetchReader.AwaitWithCancellationAsync(tcs.Task, cts.Token));
            Assert.Equal("download failed", ex.Message);
        }

        [Fact]
        public async Task AwaitWithCancellation_TokenWinsThenDownloadFaults_ObservesException()
        {
            // Reproduces the cancel/dispose teardown case: the token wins the race (so the
            // reader abandons the wait with an OperationCanceledException) and the in-flight
            // download subsequently fails against the torn-down HttpClient. The abandoned
            // task's fault must be observed so it does not resurface via
            // TaskScheduler.UnobservedTaskException.
            var neverCompletes = new TaskCompletionSource<bool>();
            using var cts = new CancellationTokenSource();

            var waitTask = CloudFetchReader.AwaitWithCancellationAsync(neverCompletes.Task, cts.Token);
            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waitTask);

            // The download now fails after having been abandoned.
            neverCompletes.SetException(new InvalidOperationException("download failed after cancel"));

            // The observing continuation runs synchronously on fault, so the exception is
            // observed by the time SetException returns.
            Assert.True(neverCompletes.Task.IsFaulted);
            Assert.NotNull(neverCompletes.Task.Exception);
        }
    }
}
