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

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Holds a telemetry client and its reference count.
    /// Used by TelemetryClientManager to track active connections per host.
    /// Reference counting ensures client is only closed when the last connection closes.
    /// </summary>
    internal sealed class TelemetryClientHolder
    {
        /// <summary>
        /// Gets the telemetry client instance.
        /// </summary>
        public ITelemetryClient Client { get; }

        /// <summary>
        /// The reference count.
        /// Tracks the number of active connections using this client.
        /// Must be modified atomically using Interlocked operations.
        /// </summary>
        public int RefCount;

        /// <summary>
        /// Initializes a new instance of TelemetryClientHolder.
        /// </summary>
        /// <param name="client">The telemetry client instance.</param>
        /// <param name="refCount">Initial reference count (typically 1).</param>
        public TelemetryClientHolder(ITelemetryClient client, int refCount = 1)
        {
            Client = client;
            RefCount = refCount;
        }
    }
}
