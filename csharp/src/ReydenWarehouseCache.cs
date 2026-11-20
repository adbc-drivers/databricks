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
using System.Collections.Generic;
using System.Text.RegularExpressions;
using AdbcDrivers.HiveServer2.Hive2;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow.Adbc;
using Microsoft.Extensions.Caching.Memory;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Process-wide cache of warehouses known to be Reyden / Lakehouse-RT (which reject Thrift and must
    /// use the Statement Execution API). Keyed on a (host, warehouse-id) composite since warehouse ids are
    /// only workspace-unique. Backed by <see cref="IMemoryCache"/> like <see cref="FeatureFlagCache"/>:
    /// starts empty on driver load and each mark auto-expires after <see cref="Ttl"/>.
    /// </summary>
    internal static class ReydenWarehouseCache
    {
        /// <summary>How long a warehouse stays marked as Reyden before it is re-probed.</summary>
        internal static readonly TimeSpan Ttl = TimeSpan.FromHours(6);

        private static readonly MemoryCache s_cache = new MemoryCache(new MemoryCacheOptions());

        /// <summary>Marks a warehouse cache key as Reyden, expiring <see cref="Ttl"/> from now.</summary>
        internal static void Mark(string? cacheKey)
        {
            if (!string.IsNullOrEmpty(cacheKey))
            {
                s_cache.Set(cacheKey!, true, Ttl);
            }
        }

        /// <summary>True if the warehouse cache key is a live (non-expired) Reyden entry.</summary>
        internal static bool IsReyden(string? cacheKey)
            => !string.IsNullOrEmpty(cacheKey) && s_cache.TryGetValue(cacheKey!, out _);

        /// <summary>Test seam: reset the cache between tests.</summary>
        internal static void Clear() => s_cache.Compact(1.0);
    }

    /// <summary>Detects the Reyden Thrift rejection and resolves the cache key for <see cref="ReydenWarehouseCache"/>.</summary>
    internal static class ReydenFallback
    {
        private const char CacheKeySeparator = '\n';

        // SQLSTATE the gateway returns for a Reyden Thrift OpenSession rejection
        // (universe SqlState.REYDEN_THRIFT_PROTOCOL_UNSUPPORTED), on HiveServer2Exception.SqlState.
        private const string ReydenSqlState = "KP001";

        // SQL warehouse path: /sql/1.0/warehouses/{id} or /sql/1.0/endpoints/{id} (mirrors SEA).
        private static readonly Regex s_warehousePathPattern =
            new Regex(@"^/sql/1\.0/(warehouses|endpoints)/([^/]+)/?$", RegexOptions.Compiled);

        /// <summary>True if the exception chain carries the Reyden Thrift rejection (sqlState KP001).</summary>
        internal static bool IsThriftRejection(Exception? exception)
        {
            for (Exception? current = exception; current != null; )
            {
                if (current is HiveServer2Exception hive &&
                    string.Equals(hive.SqlState, ReydenSqlState, StringComparison.OrdinalIgnoreCase))
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
        /// Resolves the warehouse id like SEA does (explicit warehouse_id, else parsed from URI/path).
        /// Null when the target is not a SQL warehouse (e.g. a general cluster).
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

        /// <summary>Cache key <c>"{host}\n{warehouseId}"</c>; null when the target is not a SQL warehouse.</summary>
        internal static string? TryGetWarehouseCacheKey(IReadOnlyDictionary<string, string> properties)
        {
            string? warehouseId = TryGetWarehouseId(properties);
            if (string.IsNullOrEmpty(warehouseId))
            {
                return null;
            }

            return BuildWarehouseCacheKey(FeatureFlagCache.TryGetHost(properties), warehouseId);
        }

        /// <summary>
        /// Builds the cache key from pre-resolved components (avoids re-parsing the URI/path). The host is
        /// lowercased to match FeatureFlagCache's normalization. Null when <paramref name="warehouseId"/> is empty.
        /// </summary>
        internal static string? BuildWarehouseCacheKey(string? host, string? warehouseId)
        {
            if (string.IsNullOrEmpty(warehouseId))
            {
                return null;
            }

            string normalizedHost = (host ?? string.Empty).ToLowerInvariant();
            return normalizedHost + CacheKeySeparator + warehouseId;
        }
    }
}
