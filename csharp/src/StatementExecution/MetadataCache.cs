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

namespace AdbcDrivers.Databricks.StatementExecution
{
    /// <summary>
    /// Thread-safe cache for metadata query results with TTL-based expiration.
    /// Caches catalog, schema, and table lists to reduce repeated metadata queries.
    /// </summary>
    internal class MetadataCache
    {
        private readonly ConcurrentDictionary<string, CachedEntry> _cache = new();
        private readonly bool _enabled;
        private readonly int _catalogTtlSeconds;
        private readonly int _schemaTtlSeconds;
        private readonly int _tableTtlSeconds;
        private readonly int _columnTtlSeconds;

        /// <summary>
        /// Creates a new metadata cache instance.
        /// </summary>
        /// <param name="enabled">Enable caching (default: false)</param>
        /// <param name="catalogTtlSeconds">TTL for catalog list (default: 300 = 5 minutes)</param>
        /// <param name="schemaTtlSeconds">TTL for schema lists (default: 120 = 2 minutes)</param>
        /// <param name="tableTtlSeconds">TTL for table lists (default: 60 = 1 minute)</param>
        /// <param name="columnTtlSeconds">TTL for column lists (default: 30 seconds)</param>
        public MetadataCache(
            bool enabled = false,
            int catalogTtlSeconds = 300,
            int schemaTtlSeconds = 120,
            int tableTtlSeconds = 60,
            int columnTtlSeconds = 30)
        {
            _enabled = enabled;
            _catalogTtlSeconds = catalogTtlSeconds;
            _schemaTtlSeconds = schemaTtlSeconds;
            _tableTtlSeconds = tableTtlSeconds;
            _columnTtlSeconds = columnTtlSeconds;
        }

        /// <summary>
        /// Tries to get cached catalog list.
        /// </summary>
        /// <param name="catalogPattern">Catalog pattern filter</param>
        /// <param name="result">Cached catalog list if found and not expired</param>
        /// <returns>True if cache hit, false if cache miss or expired</returns>
        public bool TryGetCatalogs(string? catalogPattern, out List<string>? result)
        {
            result = null;
            if (!_enabled) return false;

            var key = $"catalogs:{catalogPattern ?? "*"}";
            if (_cache.TryGetValue(key, out var entry))
            {
                if (DateTime.UtcNow < entry.ExpiresAt)
                {
                    result = entry.Data as List<string>;
                    return result != null;
                }
                // Expired - remove from cache
                _cache.TryRemove(key, out _);
            }
            return false;
        }

        /// <summary>
        /// Caches a catalog list.
        /// </summary>
        /// <param name="catalogPattern">Catalog pattern filter</param>
        /// <param name="catalogs">Catalog list to cache</param>
        public void PutCatalogs(string? catalogPattern, List<string> catalogs)
        {
            if (!_enabled) return;

            var key = $"catalogs:{catalogPattern ?? "*"}";
            var entry = new CachedEntry
            {
                Data = new List<string>(catalogs), // Defensive copy
                ExpiresAt = DateTime.UtcNow.AddSeconds(_catalogTtlSeconds)
            };
            _cache[key] = entry;
        }

        /// <summary>
        /// Tries to get cached schema list for a catalog.
        /// </summary>
        /// <param name="catalog">Catalog name</param>
        /// <param name="schemaPattern">Schema pattern filter</param>
        /// <param name="result">Cached schema list if found and not expired</param>
        /// <returns>True if cache hit, false if cache miss or expired</returns>
        public bool TryGetSchemas(string catalog, string? schemaPattern, out List<string>? result)
        {
            result = null;
            if (!_enabled) return false;

            var key = $"schemas:{catalog}:{schemaPattern ?? "*"}";
            if (_cache.TryGetValue(key, out var entry))
            {
                if (DateTime.UtcNow < entry.ExpiresAt)
                {
                    result = entry.Data as List<string>;
                    return result != null;
                }
                _cache.TryRemove(key, out _);
            }
            return false;
        }

        /// <summary>
        /// Caches a schema list.
        /// </summary>
        /// <param name="catalog">Catalog name</param>
        /// <param name="schemaPattern">Schema pattern filter</param>
        /// <param name="schemas">Schema list to cache</param>
        public void PutSchemas(string catalog, string? schemaPattern, List<string> schemas)
        {
            if (!_enabled) return;

            var key = $"schemas:{catalog}:{schemaPattern ?? "*"}";
            var entry = new CachedEntry
            {
                Data = new List<string>(schemas), // Defensive copy
                ExpiresAt = DateTime.UtcNow.AddSeconds(_schemaTtlSeconds)
            };
            _cache[key] = entry;
        }

        /// <summary>
        /// Tries to get cached table list for a schema.
        /// </summary>
        /// <param name="catalog">Catalog name</param>
        /// <param name="schema">Schema name</param>
        /// <param name="tableNamePattern">Table name pattern filter</param>
        /// <param name="tableTypes">Table type filter</param>
        /// <param name="result">Cached table list if found and not expired</param>
        /// <returns>True if cache hit, false if cache miss or expired</returns>
        public bool TryGetTables(string catalog, string schema, string? tableNamePattern, IReadOnlyList<string>? tableTypes, out List<(string tableName, string tableType)>? result)
        {
            result = null;
            if (!_enabled) return false;

            var typesKey = tableTypes != null ? string.Join(",", tableTypes) : "*";
            var key = $"tables:{catalog}:{schema}:{tableNamePattern ?? "*"}:{typesKey}";
            if (_cache.TryGetValue(key, out var entry))
            {
                if (DateTime.UtcNow < entry.ExpiresAt)
                {
                    result = entry.Data as List<(string, string)>;
                    return result != null;
                }
                _cache.TryRemove(key, out _);
            }
            return false;
        }

        /// <summary>
        /// Caches a table list.
        /// </summary>
        /// <param name="catalog">Catalog name</param>
        /// <param name="schema">Schema name</param>
        /// <param name="tableNamePattern">Table name pattern filter</param>
        /// <param name="tableTypes">Table type filter</param>
        /// <param name="tables">Table list to cache</param>
        public void PutTables(string catalog, string schema, string? tableNamePattern, IReadOnlyList<string>? tableTypes, List<(string tableName, string tableType)> tables)
        {
            if (!_enabled) return;

            var typesKey = tableTypes != null ? string.Join(",", tableTypes) : "*";
            var key = $"tables:{catalog}:{schema}:{tableNamePattern ?? "*"}:{typesKey}";
            var entry = new CachedEntry
            {
                Data = new List<(string, string)>(tables), // Defensive copy
                ExpiresAt = DateTime.UtcNow.AddSeconds(_tableTtlSeconds)
            };
            _cache[key] = entry;
        }

        /// <summary>
        /// Clears all cached metadata.
        /// </summary>
        public void Clear()
        {
            _cache.Clear();
        }

        /// <summary>
        /// Removes expired entries from the cache.
        /// </summary>
        /// <returns>Number of entries removed</returns>
        public int RemoveExpired()
        {
            var now = DateTime.UtcNow;
            int removed = 0;
            foreach (var kvp in _cache)
            {
                if (now >= kvp.Value.ExpiresAt)
                {
                    if (_cache.TryRemove(kvp.Key, out _))
                    {
                        removed++;
                    }
                }
            }
            return removed;
        }

        private class CachedEntry
        {
            public object Data { get; set; } = null!;
            public DateTime ExpiresAt { get; set; }
        }
    }
}
