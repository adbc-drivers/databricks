/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* This file has been modified from its original version, which is
* under the Apache License:
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.StatementExecution;
using Apache.Arrow.Adbc;
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.Spark;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Databricks-specific implementation of <see cref="AdbcDatabase"/>
    /// </summary>
    public class DatabricksDatabase : AdbcDatabase
    {
        /// <summary>
        /// The environment variable name that contains the path to the default Databricks configuration file.
        /// Takes precedence over <see cref="DefaultConfigEnvironmentVariable"/> when both are set.
        /// </summary>
        public const string AdbcConfigEnvironmentVariable = "ADBC_DATABRICKS_CONFIG_FILE";

        /// <summary>
        /// The environment variable name that contains the path to the default Databricks configuration file.
        /// </summary>
        public const string DefaultConfigEnvironmentVariable = "DATABRICKS_CONFIG_FILE";

        internal static readonly string s_assemblyVersion = ApacheUtility.GetAssemblyVersion(typeof(DatabricksDatabase));

        readonly IReadOnlyDictionary<string, string> properties;

        /// <summary>
        /// RecyclableMemoryStreamManager for LZ4 decompression output streams.
        /// Shared across all connections from this database to enable memory pooling.
        /// This manager is instance-based to allow cleanup when the database is disposed.
        /// </summary>
        internal readonly Microsoft.IO.RecyclableMemoryStreamManager RecyclableMemoryStreamManager =
            new Microsoft.IO.RecyclableMemoryStreamManager();

        /// <summary>
        /// LZ4 buffer pool for decompression shared across all connections from this database.
        /// Sized for 4MB buffers (Databricks maxBlockSize) with capacity for 10 buffers.
        /// This pool is instance-based to allow cleanup when the database is disposed.
        /// </summary>
        /// <remarks>
        /// IMPORTANT: This pool MUST remain database-scoped — do not promote it to a static or
        /// otherwise global/process-wide pool. <see cref="CustomLZ4FrameReader.ReleaseBuffer"/>
        /// returns the decoder's work buffers <b>without</b> clearing them (a measured perf win;
        /// K4os bounds every read to the bytes it actually decoded, so stale bytes are never
        /// surfaced on the normal path). Because the buffers are not zeroed on return, the pool's
        /// reuse boundary is also a data-isolation boundary: a database-scoped pool only ever
        /// recycles buffers among connections of that one database. A global pool would let a
        /// buffer rented by one DatabricksDatabase's decode be handed to another's — surfacing
        /// stale decode bytes across tenants in a multi-tenant host. Keep it instance-scoped.
        /// </remarks>
        internal readonly System.Buffers.ArrayPool<byte> Lz4BufferPool =
            System.Buffers.ArrayPool<byte>.Create(maxArrayLength: 4 * 1024 * 1024, maxArraysPerBucket: 10);

        public DatabricksDatabase(IReadOnlyDictionary<string, string> properties)
        {
            this.properties = properties;
        }

        public override AdbcConnection Connect(IReadOnlyDictionary<string, string>? options)
        {
            try
            {
                IReadOnlyDictionary<string, string> mergedProperties = options == null
                    ? properties
                    : options
                        .Concat(properties.Where(x => !options.Keys.Contains(x.Key, StringComparer.OrdinalIgnoreCase)))
                        .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

                // Merge with environment config (DATABRICKS_CONFIG_FILE) and feature flags from server
                mergedProperties = MergeWithEnvironmentConfigAndFeatureFlags(mergedProperties);

                // Select the protocol and route to the right connection, applying the Reyden /
                // Lakehouse-RT fallback (pre-check + Thrift-rejection interception). Extracted into a
                // seam so the routing glue is unit-testable without a live warehouse (see
                // ReydenFallbackTests.RouteConnection_*).
                return RouteConnection(mergedProperties, OpenThriftConnection, OpenStatementExecutionConnection);

                // Builds and opens a Statement Execution API (SEA) connection. It creates its own HTTP
                // client with the proper handler chain (TracingDelegatingHandler, RetryHttpHandler, and
                // OAuth handlers when OAuth is configured). Disposes the connection if the open fails so
                // a fallback (or a propagated error) does not leak the failed connection's HTTP client /
                // handler chain.
                AdbcConnection OpenStatementExecutionConnection(IReadOnlyDictionary<string, string> props) =>
                    OpenOrDispose(
                        new StatementExecutionConnection(props, this.RecyclableMemoryStreamManager, this.Lz4BufferPool),
                        seaConnection =>
                        {
                            seaConnection.OpenAsync().Wait();
                            // When apply_ssp_with_queries=true, run post-open SET statements for each
                            // adbc.databricks.ssp_*. No-op when false (SSPs already in CreateSession.session_confs).
                            seaConnection.ApplyServerSidePropertiesAsync().Wait();
                        });

                // Builds and opens a traditional Thrift/HiveServer2 connection, disposing it if the open
                // fails so a fallback to SEA does not leak the failed connection's resources.
                AdbcConnection OpenThriftConnection(IReadOnlyDictionary<string, string> props) =>
                    OpenOrDispose(
                        new DatabricksConnection(props, this.RecyclableMemoryStreamManager, this.Lz4BufferPool),
                        thriftConnection =>
                        {
                            thriftConnection.OpenAsync().Wait();
                            thriftConnection.ApplyServerSidePropertiesAsync().Wait();
                        });
            }
            catch (AggregateException ae)
            {
                // Unwrap AggregateException to AdbcException if possible
                // to better conform to the ADBC standard
                if (ApacheUtility.ContainsException(ae, out AdbcException? adbcException) && adbcException != null)
                {
                    // keep the entire chain, but throw the AdbcException
                    throw new AdbcException(adbcException.Message, adbcException.Status, ae);
                }

                throw;
            }
        }

        /// <summary>
        /// Runs <paramref name="open"/> against a freshly-constructed connection, disposing it if the
        /// open throws so a propagated error (or a fallback to another protocol) does not leak the failed
        /// connection's HTTP client / handler chain. Extracted as a seam so the dispose-on-failure
        /// contract is unit-testable without opening a real connection.
        /// </summary>
        internal static TConnection OpenOrDispose<TConnection>(TConnection connection, Action<TConnection> open)
            where TConnection : IDisposable
        {
            try
            {
                open(connection);
                return connection;
            }
            catch
            {
                connection.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Selects the protocol and routes to the appropriate connection factory, applying the
        /// Reyden / Lakehouse-RT fallback. This is the routing glue extracted from <see cref="Connect"/>
        /// so it can be unit-tested without opening real connections: the caller supplies the Thrift and
        /// SEA factories.
        ///
        /// Behavior:
        /// <list type="bullet">
        /// <item>When Thrift is in effect (explicit <c>protocol=thrift</c> or the default) and this
        /// warehouse is already marked Reyden, the pre-check flips to SEA and skips the doomed Thrift
        /// open. This downgrade is deliberate and transparent; the mark self-heals after
        /// <see cref="ReydenWarehouseCache"/>'s TTL. An explicit <c>protocol=rest</c> never enters the
        /// Thrift branch.</item>
        /// <item>When a Thrift open fails with the server's "Thrift not supported" rejection, the
        /// warehouse is marked Reyden and the connection is transparently retried over SEA. Any other
        /// failure (or a warehouse whose cache key could not be resolved) propagates unchanged.</item>
        /// </list>
        /// </summary>
        /// <typeparam name="TConnection">The connection type produced by the factories (production uses
        /// <see cref="AdbcConnection"/>; tests use a sentinel so no real connection is opened).</typeparam>
        /// <param name="mergedProperties">Fully merged connection properties.</param>
        /// <param name="openThriftConnection">Opens a Thrift/HiveServer2 connection (or throws).</param>
        /// <param name="openStatementExecutionConnection">Opens a Statement Execution (SEA) connection.</param>
        internal static TConnection RouteConnection<TConnection>(
            IReadOnlyDictionary<string, string> mergedProperties,
            Func<IReadOnlyDictionary<string, string>, TConnection> openThriftConnection,
            Func<IReadOnlyDictionary<string, string>, TConnection> openStatementExecutionConnection)
        {
            // Check protocol selection
            string protocol = "thrift"; // default
            if (mergedProperties.TryGetValue(DatabricksParameters.Protocol, out var protocolValue))
            {
                protocol = protocolValue.ToLowerInvariant();
            }

            // Reyden / Lakehouse-RT pre-check: if this warehouse was previously observed to reject
            // Thrift, skip the doomed Thrift OpenSession and go straight to SEA.
            //
            // Resolve the cache key's components (host, warehouse id) once and compose the composite
            // key from them, rather than resolving the composite key and then resolving the same
            // components a second time — the resolution re-parses the URI/path and re-runs the
            // warehouse regex. We keep the components as discrete telemetry dimensions rather than the
            // raw composite key: the composite embeds a literal newline (ReydenFallback.CacheKeySeparator),
            // which some OpenTelemetry exporters/log sinks mangle or truncate, and separate tags are
            // easier to query.
            string? warehouseId = ReydenFallback.TryGetWarehouseId(mergedProperties);
            string? host = FeatureFlagCache.TryGetHost(mergedProperties);
            string? warehouseCacheKey = ReydenFallback.BuildWarehouseCacheKey(host, warehouseId);
            if (protocol == "thrift" && ReydenWarehouseCache.IsReyden(warehouseCacheKey))
            {
                protocol = "rest";
                Activity.Current?.AddEvent(new ActivityEvent("reyden_fallback.skip_thrift",
                    tags: new ActivityTagsCollection
                    {
                        { "host", host },
                        { "warehouse_id", warehouseId },
                    }));
            }

            if (protocol == "rest")
            {
                return openStatementExecutionConnection(mergedProperties);
            }
            else if (protocol == "thrift")
            {
                try
                {
                    return openThriftConnection(mergedProperties);
                }
                catch (Exception ex) when (warehouseCacheKey != null && ReydenFallback.IsThriftRejection(ex))
                {
                    // This warehouse is Reyden / Lakehouse-RT: Thrift is not supported. Remember it
                    // so subsequent connects skip Thrift, and transparently retry over SEA (the
                    // driver-side equivalent of forcing the kernel/Statement Execution path).
                    ReydenWarehouseCache.Mark(warehouseCacheKey);
                    Activity.Current?.AddEvent(new ActivityEvent("reyden_fallback.thrift_rejected",
                        tags: new ActivityTagsCollection
                        {
                            { "host", host },
                            { "warehouse_id", warehouseId },
                            { "error", ex.Message },
                        }));
                    try
                    {
                        return openStatementExecutionConnection(mergedProperties);
                    }
                    catch (Exception seaEx)
                    {
                        // Both protocols failed: Thrift was rejected (Reyden / Lakehouse-RT) and the
                        // transparent Statement Execution (SEA) fallback also failed. Retain BOTH causal
                        // chains so the failure stays diagnosable outside of tracing — otherwise the
                        // original Thrift rejection is silently discarded and the user sees only the SEA
                        // error with no hint that a protocol downgrade was attempted. The SEA error is
                        // listed first so Connect()'s AggregateException unwrap surfaces it as the primary
                        // AdbcException, while the original Thrift rejection is kept as an inner exception.
                        throw new AggregateException(
                            "Thrift is not supported for this warehouse (Reyden / Lakehouse-RT) and the " +
                            "automatic Statement Execution (SEA) fallback also failed. See inner exceptions " +
                            "for both the original Thrift rejection and the SEA error.",
                            seaEx,
                            ex);
                    }
                }
            }
            else
            {
                throw new ArgumentException(
                    $"Unsupported protocol: '{protocol}'. Supported values are 'thrift' and 'rest'.",
                    nameof(mergedProperties));
            }
        }

        /// <summary>
        /// Merges properties with environment config and server feature flags.
        /// This is the single place where all property merging happens for both Thrift and REST connections.
        /// </summary>
        /// <param name="properties">Properties to merge.</param>
        /// <returns>Merged properties dictionary with any server feature flags applied.</returns>
        private static IReadOnlyDictionary<string, string> MergeWithEnvironmentConfigAndFeatureFlags(IReadOnlyDictionary<string, string> properties)
        {
            var mergedWithEnvConfig = MergeWithDefaultEnvironmentConfig(properties);

            // Apply server feature flags synchronously so this connection carries them from the
            // start: on a warm per-host cache the flags are merged in without a network call; on a
            // cold cache this blocks for the initial fetch (bounded by the feature-flag HTTP
            // timeout, default 5s). Local properties always win on conflict, and a failed or
            // disabled fetch returns the properties unchanged so Connect() is never broken.
            return FeatureFlagCache.GetInstance()
                .MergePropertiesWithFeatureFlags(mergedWithEnvConfig, s_assemblyVersion);
        }

        /// <summary>
        /// Automatically merges properties from the default config environment variable with passed-in properties.
        /// Checks ADBC_DATABRICKS_CONFIG_FILE first, falling back to DATABRICKS_CONFIG_FILE.
        /// The merge priority is controlled by the "adbc.databricks.driver_config_take_precedence" property.
        /// If neither environment variable is set or valid, only passed-in properties are used.
        /// </summary>
        /// <param name="properties">Properties passed to constructor.</param>
        /// <returns>Merged properties dictionary.</returns>
        private static IReadOnlyDictionary<string, string> MergeWithDefaultEnvironmentConfig(IReadOnlyDictionary<string, string> properties)
        {
            // Try to load configuration: ADBC_DATABRICKS_CONFIG_FILE takes precedence over DATABRICKS_CONFIG_FILE
            var environmentConfig = DatabricksConfiguration.TryFromEnvironmentVariable(AdbcConfigEnvironmentVariable)
                ?? DatabricksConfiguration.TryFromEnvironmentVariable(DefaultConfigEnvironmentVariable);

            if (environmentConfig != null)
            {
                // Determine precedence setting - check passed-in properties first, then environment config
                bool driverConfigTakesPrecedence = DetermineDriverConfigPrecedence(properties, environmentConfig.Properties);

                if (driverConfigTakesPrecedence)
                {
                    // Environment config properties override passed-in properties
                    return MergeProperties(properties, environmentConfig.Properties);
                }
                else
                {
                    // Passed-in properties override environment config properties (default behavior)
                    return MergeProperties(environmentConfig.Properties, properties);
                }
            }

            // No environment config available, use only passed-in properties
            return properties;
        }

        /// <summary>
        /// Determines whether driver configuration should take precedence based on the precedence property.
        /// Checks passed-in properties first, then environment properties, defaulting to false.
        /// </summary>
        /// <param name="passedInProperties">Properties passed to constructor.</param>
        /// <param name="environmentProperties">Properties loaded from environment configuration.</param>
        /// <returns>True if driver config should take precedence, false otherwise.</returns>
        private static bool DetermineDriverConfigPrecedence(IReadOnlyDictionary<string, string> passedInProperties, IReadOnlyDictionary<string, string> environmentProperties)
        {
            // Priority 1: Check passed-in properties for precedence setting
            if (passedInProperties.TryGetValue(DatabricksParameters.DriverConfigTakePrecedence, out string? passedInValue))
            {
                if (bool.TryParse(passedInValue, out bool passedInPrecedence))
                {
                    return passedInPrecedence;
                }
            }

            // Priority 2: Check environment config for precedence setting
            if (environmentProperties.TryGetValue(DatabricksParameters.DriverConfigTakePrecedence, out string? environmentValue))
            {
                if (bool.TryParse(environmentValue, out bool environmentPrecedence))
                {
                    return environmentPrecedence;
                }
            }

            // Default: Passed-in properties override environment config (current behavior)
            return false;
        }

        /// <summary>
        /// Merges two property dictionaries, with additional properties taking precedence.
        /// </summary>
        /// <param name="baseProperties">Base properties dictionary.</param>
        /// <param name="additionalProperties">Additional properties to merge. These take precedence over base properties.</param>
        /// <returns>Merged properties dictionary.</returns>
        private static IReadOnlyDictionary<string, string> MergeProperties(IReadOnlyDictionary<string, string> baseProperties, IReadOnlyDictionary<string, string>? additionalProperties)
        {
            if (additionalProperties == null || additionalProperties.Count == 0)
            {
                return baseProperties;
            }

            var merged = new Dictionary<string, string>();

            // Add base properties first
            foreach (var kvp in baseProperties)
            {
                merged[kvp.Key] = kvp.Value;
            }

            // Additional properties override base properties
            foreach (var kvp in additionalProperties)
            {
                merged[kvp.Key] = kvp.Value;
            }

            return merged;
        }
    }
}
