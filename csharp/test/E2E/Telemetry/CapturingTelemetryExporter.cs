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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// A test implementation of <see cref="ITelemetryExporter"/> that captures all exported
    /// <see cref="TelemetryFrontendLog"/> instances in memory for test assertions.
    /// </summary>
    /// <remarks>
    /// This exporter never makes HTTP calls. It stores all exported logs in a thread-safe
    /// collection that tests can inspect after driver operations complete.
    /// Supports configurable failure simulation for circuit breaker and error path testing.
    /// </remarks>
    internal sealed class CapturingTelemetryExporter : ITelemetryExporter
    {
        private readonly ConcurrentBag<TelemetryFrontendLog> _capturedLogs = new ConcurrentBag<TelemetryFrontendLog>();
        private volatile bool _shouldFail;
        private volatile int _exportCallCount;

        /// <summary>
        /// Gets all captured telemetry frontend logs.
        /// </summary>
        public IReadOnlyList<TelemetryFrontendLog> CapturedLogs => _capturedLogs.ToList().AsReadOnly();

        /// <summary>
        /// Gets the total number of captured logs.
        /// </summary>
        public int CapturedLogCount => _capturedLogs.Count;

        /// <summary>
        /// Gets the total number of times <see cref="ExportAsync"/> was called.
        /// </summary>
        public int ExportCallCount => _exportCallCount;

        /// <summary>
        /// Gets or sets whether the exporter should simulate failures.
        /// When true, <see cref="ExportAsync"/> returns false to simulate export failure.
        /// </summary>
        public bool ShouldFail
        {
            get => _shouldFail;
            set => _shouldFail = value;
        }

        /// <summary>
        /// Export telemetry frontend logs by capturing them in memory.
        /// </summary>
        /// <param name="logs">The list of telemetry frontend logs to capture.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>
        /// True if capture succeeded (and <see cref="ShouldFail"/> is false),
        /// false if <see cref="ShouldFail"/> is true.
        /// Returns true for empty/null logs.
        /// </returns>
        public Task<bool> ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
        {
            Interlocked.Increment(ref _exportCallCount);

            if (logs == null || logs.Count == 0)
            {
                return Task.FromResult(true);
            }

            if (_shouldFail)
            {
                return Task.FromResult(false);
            }

            foreach (TelemetryFrontendLog log in logs)
            {
                _capturedLogs.Add(log);
            }

            return Task.FromResult(true);
        }

        /// <summary>
        /// Clears all captured logs and resets the call counter.
        /// </summary>
        public void Reset()
        {
            while (_capturedLogs.TryTake(out _))
            {
                // Drain the bag
            }
            _exportCallCount = 0;
            _shouldFail = false;
        }

        /// <summary>
        /// Waits until at least the specified number of logs have been captured,
        /// or the timeout is exceeded.
        /// </summary>
        /// <param name="expectedCount">The minimum number of logs to wait for.</param>
        /// <param name="timeout">Maximum time to wait.</param>
        /// <returns>True if the expected count was reached; false if timed out.</returns>
        public async Task<bool> WaitForLogsAsync(int expectedCount, TimeSpan timeout)
        {
            DateTime deadline = DateTime.UtcNow + timeout;
            while (_capturedLogs.Count < expectedCount && DateTime.UtcNow < deadline)
            {
                await Task.Delay(50).ConfigureAwait(false);
            }
            return _capturedLogs.Count >= expectedCount;
        }
    }
}
