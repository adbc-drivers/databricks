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
using AdbcDrivers.Databricks;
using Xunit;

namespace AdbcDrivers.Databricks.Tests
{
    /// <summary>
    /// Unit tests for <see cref="ReydenWarehouseCache"/> — presence tracking and TTL expiry.
    /// </summary>
    public class ReydenWarehouseCacheTests
    {
        public ReydenWarehouseCacheTests()
        {
            // The cache is process-wide static; isolate each test.
            ReydenWarehouseCache.Clear();
        }

        [Fact]
        public void UnmarkedWarehouseIsNotReyden()
        {
            Assert.False(ReydenWarehouseCache.IsReyden("wh-unknown"));
        }

        [Fact]
        public void MarkedWarehouseIsReyden()
        {
            ReydenWarehouseCache.Mark("wh-1");
            Assert.True(ReydenWarehouseCache.IsReyden("wh-1"));
            // Marking one warehouse must not leak to another.
            Assert.False(ReydenWarehouseCache.IsReyden("wh-2"));
        }

        [Fact]
        public void EntryIsLiveWithinTtlAndExpiresAfter()
        {
            var t0 = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            ReydenWarehouseCache.Mark("wh-ttl", t0);

            // Just before expiry: still Reyden.
            Assert.True(ReydenWarehouseCache.IsReyden("wh-ttl", t0 + ReydenWarehouseCache.Ttl - TimeSpan.FromMinutes(1)));
            // At/after expiry: no longer Reyden.
            Assert.False(ReydenWarehouseCache.IsReyden("wh-ttl", t0 + ReydenWarehouseCache.Ttl + TimeSpan.FromMinutes(1)));
        }

        [Fact]
        public void ExpiredEntryIsReProbedNotPinned()
        {
            var t0 = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            ReydenWarehouseCache.Mark("wh-reprobe", t0);

            // Reading after expiry evicts the stale entry...
            Assert.False(ReydenWarehouseCache.IsReyden("wh-reprobe", t0 + ReydenWarehouseCache.Ttl + TimeSpan.FromHours(1)));
            // ...so a default-clock read (now, well past t0) is also false: the warehouse is re-probed over Thrift.
            Assert.False(ReydenWarehouseCache.IsReyden("wh-reprobe"));
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        public void NullOrEmptyIdIsNeverCachedOrReyden(string? id)
        {
            ReydenWarehouseCache.Mark(id);
            Assert.False(ReydenWarehouseCache.IsReyden(id));
        }
    }
}
