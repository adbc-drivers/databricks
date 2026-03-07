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

using System.Threading;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Holds a telemetry client and its reference count.
    /// Used by TelemetryClientManager to track how many connections are using a client.
    /// </summary>
    /// <remarks>
    /// Thread Safety: Access to the _refCount field is coordinated via Interlocked operations
    /// (AddRef/Release) for thread-safe increment and decrement from concurrent connections.
    /// </remarks>
    internal sealed class TelemetryClientHolder
    {
        /// <summary>
        /// Reference count tracking the number of connections using this client.
        /// Access to this field must be done via AddRef/Release methods for thread safety.
        /// </summary>
        private int _refCount = 1;

        /// <summary>
        /// Gets the current reference count.
        /// </summary>
        public int RefCount => Volatile.Read(ref _refCount);

        /// <summary>
        /// Gets the telemetry client instance.
        /// </summary>
        public ITelemetryClient Client { get; }

        /// <summary>
        /// Creates a new TelemetryClientHolder with the specified client and initial ref count of 1.
        /// </summary>
        /// <param name="client">The telemetry client to hold.</param>
        public TelemetryClientHolder(ITelemetryClient client)
        {
            Client = client;
        }

        /// <summary>
        /// Atomically increments the reference count.
        /// </summary>
        /// <returns>The new reference count after incrementing.</returns>
        public int AddRef()
        {
            return Interlocked.Increment(ref _refCount);
        }

        /// <summary>
        /// Atomically decrements the reference count.
        /// </summary>
        /// <returns>The new reference count after decrementing.</returns>
        public int Release()
        {
            return Interlocked.Decrement(ref _refCount);
        }
    }
}
