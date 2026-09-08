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
using System.Text.RegularExpressions;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow.Adbc;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Process-wide cache of warehouses known to be Reyden / Lakehouse-RT, which reject the
    /// Thrift protocol and must be driven over the Statement Execution API (SEA) instead.
    ///
    /// Entries are keyed on a <c>(host, warehouse-id)</c> composite (see
    /// <see cref="ReydenFallback.TryGetWarehouseCacheKey"/>), not the bare warehouse id: warehouse
    /// ids are only unique within a workspace, so two workspaces served by the same process can
    /// legitimately present the same id. Including the host prevents a mark in one workspace from
    /// leaking to a healthy, Thrift-capable warehouse with the same id in another.
    ///
    /// The cache is static so it lives for the process (a fresh driver load starts empty, matching
    /// the documented "cleared on driver-instance restart" behavior) and entries expire after a TTL,
    /// so a warehouse that is later reconfigured is re-probed rather than pinned to SEA forever.
    /// </summary>
    internal static class ReydenWarehouseCache
    {
        /// <summary>How long a warehouse stays marked as Reyden before it is re-probed.</summary>
        internal static readonly TimeSpan Ttl = TimeSpan.FromHours(6);

        // Maps warehouse cache key -> UTC instant at which the entry expires.
        private static readonly ConcurrentDictionary<string, DateTime> s_expiryByCacheKey =
            new ConcurrentDictionary<string, DateTime>();

        /// <summary>Marks a warehouse cache key as Reyden, expiring <see cref="Ttl"/> from now.</summary>
        internal static void Mark(string? cacheKey) => Mark(cacheKey, DateTime.UtcNow);

        /// <summary>Test seam: mark using an explicit clock so TTL behavior is deterministic.</summary>
        internal static void Mark(string? cacheKey, DateTime nowUtc)
        {
            if (string.IsNullOrEmpty(cacheKey))
            {
                return;
            }

            s_expiryByCacheKey[cacheKey!] = nowUtc + Ttl;
        }

        /// <summary>Returns true if the warehouse cache key is a live (non-expired) Reyden entry.</summary>
        internal static bool IsReyden(string? cacheKey) => IsReyden(cacheKey, DateTime.UtcNow);

        /// <summary>Test seam: evaluate against an explicit clock so TTL behavior is deterministic.</summary>
        internal static bool IsReyden(string? cacheKey, DateTime nowUtc)
        {
            if (string.IsNullOrEmpty(cacheKey))
            {
                return false;
            }

            if (s_expiryByCacheKey.TryGetValue(cacheKey!, out DateTime expiry))
            {
                if (nowUtc < expiry)
                {
                    return true;
                }

                // Expired: drop it so a reconfigured warehouse is re-probed over Thrift. Use a
                // value-conditional remove so a concurrent Mark() that races in between the
                // TryGetValue above and here — writing a fresh, non-expired expiry — is not
                // clobbered. ConcurrentDictionary's ICollection<KeyValuePair<,>>.Remove removes
                // only when both key and value match, and (unlike the TryRemove(KeyValuePair<,>)
                // overload) is available on netstandard2.0.
                ((ICollection<KeyValuePair<string, DateTime>>)s_expiryByCacheKey)
                    .Remove(new KeyValuePair<string, DateTime>(cacheKey!, expiry));
            }

            return false;
        }

        /// <summary>Test seam: reset the cache between tests.</summary>
        internal static void Clear() => s_expiryByCacheKey.Clear();
    }

    /// <summary>
    /// Helpers for the Reyden/Lakehouse-RT fallback: detecting the server's "Thrift not supported"
    /// rejection and resolving the composite key that keys <see cref="ReydenWarehouseCache"/>.
    /// </summary>
    internal static class ReydenFallback
    {
        // Separator between the host and warehouse-id components of the cache key. A newline can never
        // appear in a host or warehouse id, so it can't be used to forge a collision between keys.
        private const char CacheKeySeparator = '\n';
        // The Reyden signal surfaced by the SQL proxy in the x-thriftserver-error-message header and
        // propagated into the thrown exception's message (see ThriftErrorMessageHandler). The full text
        // is "BAD_REQUEST: Lakehouse/RT is not supported for Thrift protocol. Please update your ...".
        // Matched case-insensitively as a substring so wrapping/prefix changes don't defeat detection.
        private const string ThriftNotSupportedMarker = "not supported for Thrift protocol";

        // Path form for a SQL warehouse: /sql/1.0/warehouses/{id} or /sql/1.0/endpoints/{id}.
        // Mirrors the pattern in StatementExecutionConnection so the cache key matches the SEA path.
        private static readonly Regex s_warehousePathPattern =
            new Regex(@"^/sql/1\.0/(warehouses|endpoints)/([^/]+)/?$", RegexOptions.Compiled);

        /// <summary>
        /// True if the exception (or anything in its inner/aggregate chain) is the Reyden Thrift
        /// rejection, meaning the connection should be retried over the Statement Execution API.
        /// </summary>
        internal static bool IsThriftRejection(Exception? exception)
        {
            for (Exception? current = exception; current != null; )
            {
                if (current.Message != null &&
                    current.Message.IndexOf(ThriftNotSupportedMarker, StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    return true;
                }

                if (current is AggregateException aggregate)
                {
                    foreach (Exception inner in aggregate.InnerExceptions)
                    {
                        if (IsThriftRejection(inner))
                        {
                            return true;
                        }
                    }
                    return false;
                }

                current = current.InnerException;
            }

            return false;
        }

        /// <summary>
        /// Resolves the warehouse id from connection properties the same way SEA does: an explicit
        /// warehouse_id parameter wins, otherwise it is parsed from the URI or path. Returns null when
        /// the target is not a SQL warehouse (e.g. a general cluster), in which case no fallback applies.
        /// </summary>
        internal static string? TryGetWarehouseId(IReadOnlyDictionary<string, string> properties)
        {
            string? warehouseId = PropertyHelper.GetStringProperty(properties, DatabricksParameters.WarehouseId, string.Empty);
            if (!string.IsNullOrEmpty(warehouseId))
            {
                return warehouseId;
            }

            string? path = null;
            if (properties.TryGetValue(SparkParameters.Path, out string? explicitPath) && !string.IsNullOrEmpty(explicitPath))
            {
                path = explicitPath;
            }
            else if (properties.TryGetValue(AdbcOptions.Uri, out string? uri) &&
                     !string.IsNullOrEmpty(uri) &&
                     Uri.TryCreate(uri, UriKind.Absolute, out Uri? parsedUri))
            {
                path = parsedUri.AbsolutePath;
            }

            if (string.IsNullOrEmpty(path))
            {
                return null;
            }

            int queryIndex = path!.IndexOf('?');
            if (queryIndex >= 0)
            {
                path = path.Substring(0, queryIndex);
            }

            Match match = s_warehousePathPattern.Match(path);
            return match.Success ? match.Groups[2].Value : null;
        }

        /// <summary>
        /// Resolves the process-wide cache key for the target warehouse as <c>"{host}\n{warehouseId}"</c>.
        /// The host is included because a warehouse id is only unique within a workspace, so keying on
        /// the bare id would let a Reyden mark in one workspace incorrectly route a healthy,
        /// Thrift-capable warehouse with the same id in another workspace to SEA. Returns null when the
        /// target is not a SQL warehouse (e.g. a general cluster), in which case no fallback applies.
        /// </summary>
        internal static string? TryGetWarehouseCacheKey(IReadOnlyDictionary<string, string> properties)
        {
            string? warehouseId = TryGetWarehouseId(properties);
            if (string.IsNullOrEmpty(warehouseId))
            {
                return null;
            }

            // Fall back to an empty host component when the host can't be resolved; the warehouse id
            // still keys the entry, so behavior degrades to the previous (id-only) semantics rather
            // than dropping the fallback entirely. Lowercase the host so that connects supplying the
            // same host with different casing hash to one entry — matching the normalization
            // FeatureFlagCache already applies to its per-host key (host.ToLowerInvariant()).
            string host = (FeatureFlagCache.TryGetHost(properties) ?? string.Empty).ToLowerInvariant();
            return host + CacheKeySeparator + warehouseId;
        }
    }
}
