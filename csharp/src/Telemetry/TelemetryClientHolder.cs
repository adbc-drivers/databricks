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

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Holds a telemetry client and its reference count.
    /// Used by <see cref="TelemetryClientManager"/> to track the number of active connections
    /// sharing a single <see cref="ITelemetryClient"/> instance for a given host.
    /// </summary>
    /// <remarks>
    /// The reference count is managed via <see cref="System.Threading.Interlocked"/> methods
    /// in <see cref="TelemetryClientManager"/> to ensure thread safety. The holder is created
    /// with an initial reference count of 1.
    /// </remarks>
    internal sealed class TelemetryClientHolder
    {
        /// <summary>
        /// The reference count tracking how many connections are using this client.
        /// Managed via <see cref="System.Threading.Interlocked"/> for thread safety.
        /// </summary>
        internal int _refCount = 1;

        /// <summary>
        /// Initializes a new instance of <see cref="TelemetryClientHolder"/> with the specified client.
        /// </summary>
        /// <param name="client">The telemetry client to hold.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="client"/> is null.</exception>
        public TelemetryClientHolder(ITelemetryClient client)
        {
            Client = client ?? throw new ArgumentNullException(nameof(client));
        }

        /// <summary>
        /// Gets the telemetry client managed by this holder.
        /// </summary>
        public ITelemetryClient Client { get; }
    }
}
