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
using System.Threading;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Holds a telemetry client and its reference count.
    /// </summary>
    /// <remarks>
    /// This class is used by <see cref="TelemetryClientManager"/> to manage
    /// shared telemetry clients per host with reference counting. When the
    /// reference count reaches zero, the client should be closed and removed.
    ///
    /// Thread-safety is ensured using Interlocked operations for the reference count.
    ///
    /// JDBC Reference: TelemetryClientHolder.java
    /// </remarks>
    internal sealed class TelemetryClientHolder
    {
        private readonly ITelemetryClient _client;
        private int _refCount;

        /// <summary>
        /// Gets the telemetry client.
        /// </summary>
        public ITelemetryClient Client => _client;

        /// <summary>
        /// Gets the current reference count.
        /// </summary>
        public int RefCount => Volatile.Read(ref _refCount);

        /// <summary>
        /// Creates a new TelemetryClientHolder with the specified client.
        /// </summary>
        /// <param name="client">The telemetry client to hold.</param>
        /// <exception cref="ArgumentNullException">Thrown when client is null.</exception>
        /// <remarks>
        /// The initial reference count is 0. Call <see cref="IncrementRefCount"/>
        /// after creation to register the first reference.
        /// </remarks>
        public TelemetryClientHolder(ITelemetryClient client)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _refCount = 0;
        }

        /// <summary>
        /// Increments the reference count.
        /// </summary>
        /// <returns>The new reference count.</returns>
        public int IncrementRefCount()
        {
            return Interlocked.Increment(ref _refCount);
        }

        /// <summary>
        /// Decrements the reference count.
        /// </summary>
        /// <returns>The new reference count.</returns>
        /// <remarks>
        /// When the reference count reaches zero, the caller is responsible
        /// for calling <see cref="ITelemetryClient.CloseAsync"/> on the client
        /// and removing this holder from the manager.
        /// </remarks>
        public int DecrementRefCount()
        {
            return Interlocked.Decrement(ref _refCount);
        }
    }
}
