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
using System.Diagnostics;
using System.Net.Http;
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Singleton factory that manages one telemetry client per host.
    /// Prevents rate limiting by sharing clients across connections.
    /// </summary>
    /// <remarks>
    /// This class implements the per-host client management pattern from the JDBC driver:
    /// - One telemetry client per host to prevent rate limiting from concurrent connections
    /// - Large customers (e.g., Celonis) open many parallel connections to the same host
    /// - Shared client batches events from all connections, avoiding multiple concurrent flushes
    /// - Reference counting tracks active connections, closes client when last connection closes
    /// - Thread-safe using ConcurrentDictionary and atomic reference counting
    ///
    /// JDBC Reference: TelemetryClientFactory.java
    /// </remarks>
    internal sealed class TelemetryClientManager
    {
        private static readonly TelemetryClientManager s_instance = new TelemetryClientManager();

        private readonly ConcurrentDictionary<string, TelemetryClientHolder> _clients;
        private readonly Func<string, HttpClient, TelemetryConfiguration, ITelemetryClient>? _clientFactory;

        /// <summary>
        /// Gets the singleton instance of the TelemetryClientManager.
        /// </summary>
        public static TelemetryClientManager GetInstance() => s_instance;

        /// <summary>
        /// Creates a new TelemetryClientManager.
        /// </summary>
        internal TelemetryClientManager()
            : this(null)
        {
        }

        /// <summary>
        /// Creates a new TelemetryClientManager with a custom client factory.
        /// </summary>
        /// <param name="clientFactory">
        /// Factory function to create telemetry clients.
        /// If null, uses the default factory that creates CircuitBreakerTelemetryExporter
        /// wrapped around DatabricksTelemetryExporter.
        /// </param>
        /// <remarks>
        /// This constructor is primarily for testing to allow injecting mock clients.
        /// </remarks>
        internal TelemetryClientManager(Func<string, HttpClient, TelemetryConfiguration, ITelemetryClient>? clientFactory)
        {
            _clients = new ConcurrentDictionary<string, TelemetryClientHolder>(StringComparer.OrdinalIgnoreCase);
            _clientFactory = clientFactory;
        }

        /// <summary>
        /// Gets or creates a telemetry client for the host.
        /// Increments reference count.
        /// </summary>
        /// <param name="host">The Databricks host URL.</param>
        /// <param name="httpClient">The HTTP client to use for sending requests.</param>
        /// <param name="config">The telemetry configuration.</param>
        /// <returns>The telemetry client for the host.</returns>
        /// <exception cref="ArgumentException">Thrown when host is null or whitespace.</exception>
        /// <exception cref="ArgumentNullException">Thrown when httpClient or config is null.</exception>
        public ITelemetryClient GetOrCreateClient(string host, HttpClient httpClient, TelemetryConfiguration config)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("Host cannot be null or whitespace.", nameof(host));
            }

            if (httpClient == null)
            {
                throw new ArgumentNullException(nameof(httpClient));
            }

            if (config == null)
            {
                throw new ArgumentNullException(nameof(config));
            }

            // Use GetOrAdd with a value factory to ensure thread-safety
            var holder = _clients.GetOrAdd(host, h =>
            {
                var client = CreateClient(h, httpClient, config);
                return new TelemetryClientHolder(client);
            });

            // Increment the reference count
            var newRefCount = holder.IncrementRefCount();
            Debug.WriteLine($"[TRACE] TelemetryClientManager: GetOrCreateClient for host '{host}', RefCount={newRefCount}");

            return holder.Client;
        }

        /// <summary>
        /// Decrements reference count for the host.
        /// Closes and removes client when ref count reaches zero.
        /// </summary>
        /// <param name="host">The host to release the client for.</param>
        /// <returns>A task representing the asynchronous release operation.</returns>
        /// <remarks>
        /// This method is thread-safe. If the reference count reaches zero,
        /// the client is closed and removed from the cache. If multiple threads
        /// try to release the same client simultaneously, only one will successfully
        /// close and remove it.
        /// </remarks>
        public async Task ReleaseClientAsync(string host)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                return;
            }

            if (_clients.TryGetValue(host, out var holder))
            {
                var newRefCount = holder.DecrementRefCount();
                Debug.WriteLine($"[TRACE] TelemetryClientManager: ReleaseClientAsync for host '{host}', RefCount={newRefCount}");

                if (newRefCount <= 0)
                {
                    // Try to remove the holder. Use TryRemove to avoid race conditions
                    // where a new connection added a reference.
                    if (holder.RefCount <= 0)
                    {
                        // Check RefCount again because another thread might have
                        // incremented it between our check and the removal attempt.
#if NET5_0_OR_GREATER
                        if (_clients.TryRemove(new System.Collections.Generic.KeyValuePair<string, TelemetryClientHolder>(host, holder)))
#else
                        // For netstandard2.0, we need to be more careful about the removal
                        if (_clients.TryGetValue(host, out var currentHolder) && currentHolder == holder && currentHolder.RefCount <= 0)
#endif
                        {
#if !NET5_0_OR_GREATER
                            // For netstandard2.0, attempt to remove
                            ((System.Collections.Generic.IDictionary<string, TelemetryClientHolder>)_clients).Remove(
                                new System.Collections.Generic.KeyValuePair<string, TelemetryClientHolder>(host, holder));
#endif
                            Debug.WriteLine($"[TRACE] TelemetryClientManager: Closing client for host '{host}'");

                            try
                            {
                                await holder.Client.CloseAsync().ConfigureAwait(false);
                                Debug.WriteLine($"[TRACE] TelemetryClientManager: Closed and removed client for host '{host}'");
                            }
                            catch (Exception ex)
                            {
                                // Swallow all exceptions per telemetry requirement
                                Debug.WriteLine($"[TRACE] TelemetryClientManager: Error closing client for host '{host}': {ex.Message}");
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Gets the number of hosts with active clients.
        /// </summary>
        internal int ClientCount => _clients.Count;

        /// <summary>
        /// Checks if a client exists for the specified host.
        /// </summary>
        /// <param name="host">The host to check.</param>
        /// <returns>True if a client exists, false otherwise.</returns>
        internal bool HasClient(string host)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                return false;
            }

            return _clients.ContainsKey(host);
        }

        /// <summary>
        /// Gets the holder for the specified host, if it exists.
        /// Does not create a new client or modify reference count.
        /// </summary>
        /// <param name="host">The host to get the holder for.</param>
        /// <param name="holder">The holder if found, null otherwise.</param>
        /// <returns>True if the holder was found, false otherwise.</returns>
        internal bool TryGetHolder(string host, out TelemetryClientHolder? holder)
        {
            holder = null;

            if (string.IsNullOrWhiteSpace(host))
            {
                return false;
            }

            if (_clients.TryGetValue(host, out var foundHolder))
            {
                holder = foundHolder;
                return true;
            }

            return false;
        }

        /// <summary>
        /// Clears all clients.
        /// This is primarily for testing purposes.
        /// </summary>
        internal void Clear()
        {
            _clients.Clear();
        }

        /// <summary>
        /// Creates a new telemetry client for the specified host.
        /// </summary>
        private ITelemetryClient CreateClient(string host, HttpClient httpClient, TelemetryConfiguration config)
        {
            if (_clientFactory != null)
            {
                return _clientFactory(host, httpClient, config);
            }

            // Default factory: Create CircuitBreakerTelemetryExporter wrapping DatabricksTelemetryExporter
            // This creates an adapter that implements ITelemetryClient
            var innerExporter = new DatabricksTelemetryExporter(httpClient, host, isAuthenticated: true, config);
            var circuitBreakerExporter = new CircuitBreakerTelemetryExporter(host, innerExporter);
            return new TelemetryClientAdapter(host, circuitBreakerExporter);
        }
    }
}
