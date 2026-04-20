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

using System.Collections.Generic;

namespace AdbcDrivers.Databricks.Auth
{
    /// <summary>
    /// A fixed-capacity cache mapping original tokens to exchanged/refreshed tokens.
    /// When capacity is reached, the oldest-inserted entry is evicted.
    /// Not thread-safe — callers must provide external synchronization.
    /// </summary>
    internal class BoundedTokenCache
    {
        private readonly int _maxSize;
        private readonly Dictionary<string, string> _cache;
        private readonly Queue<string> _insertionOrder;

        public BoundedTokenCache(int maxSize = 10)
        {
            _maxSize = maxSize;
            _cache = new Dictionary<string, string>(maxSize);
            _insertionOrder = new Queue<string>(maxSize);
        }

        public bool TryGetValue(string key, out string? value)
        {
            return _cache.TryGetValue(key, out value);
        }

        /// <summary>
        /// Adds or updates a cache entry. If the key is new and capacity is reached,
        /// the oldest entry is evicted first.
        /// </summary>
        public void Set(string key, string value)
        {
            if (_cache.ContainsKey(key))
            {
                _cache[key] = value;
                return;
            }

            while (_cache.Count >= _maxSize && _insertionOrder.Count > 0)
            {
                _cache.Remove(_insertionOrder.Dequeue());
            }

            _cache[key] = value;
            _insertionOrder.Enqueue(key);
        }
    }
}
