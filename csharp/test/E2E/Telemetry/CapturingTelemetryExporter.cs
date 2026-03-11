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

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// A telemetry exporter that captures all exported logs for test assertions.
    /// Thread-safe for concurrent export calls.
    /// </summary>
    internal sealed class CapturingTelemetryExporter : ITelemetryExporter
    {
        private readonly ConcurrentBag<TelemetryFrontendLog> _exportedLogs = new ConcurrentBag<TelemetryFrontendLog>();
        private int _exportCallCount;

        /// <summary>
        /// Gets all captured telemetry logs.
        /// </summary>
        public IReadOnlyCollection<TelemetryFrontendLog> ExportedLogs => _exportedLogs;

        /// <summary>
        /// Gets the number of times ExportAsync was called.
        /// </summary>
        public int ExportCallCount => _exportCallCount;

        public Task<bool> ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
        {
            Interlocked.Increment(ref _exportCallCount);

            if (logs != null)
            {
                foreach (var log in logs)
                {
                    _exportedLogs.Add(log);
                }
            }

            return Task.FromResult(true);
        }

        /// <summary>
        /// Clears all captured logs and resets the call count.
        /// </summary>
        public void Reset()
        {
            while (_exportedLogs.TryTake(out _)) { }
            Interlocked.Exchange(ref _exportCallCount, 0);
        }
    }
}
