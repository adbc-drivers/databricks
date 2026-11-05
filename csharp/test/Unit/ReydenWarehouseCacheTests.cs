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
    /// Unit tests for <see cref="ReydenWarehouseCache"/> — presence tracking and the configured TTL.
    /// Actual time-based eviction is delegated to (and covered by) IMemoryCache, mirroring how
    /// FeatureFlagCache tests its cache layer: assert the TTL value and presence, not the framework's clock.
    /// </summary>
    public class ReydenWarehouseCacheTests
    {
        public ReydenWarehouseCacheTests()
        {
            // The cache is process-wide; isolate each test.
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
        public void ClearRemovesEntries()
        {
            ReydenWarehouseCache.Mark("wh-clear");
            Assert.True(ReydenWarehouseCache.IsReyden("wh-clear"));

            ReydenWarehouseCache.Clear();
            Assert.False(ReydenWarehouseCache.IsReyden("wh-clear"));
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        public void NullOrEmptyIdIsNeverCachedOrReyden(string? id)
        {
            ReydenWarehouseCache.Mark(id);
            Assert.False(ReydenWarehouseCache.IsReyden(id));
        }

        [Fact]
        public void TtlIsSixHours()
        {
            // A mark is written to IMemoryCache with this absolute expiration; eviction after it lapses
            // is IMemoryCache's contract. Asserting the value guards against an accidental TTL change.
            Assert.Equal(TimeSpan.FromHours(6), ReydenWarehouseCache.Ttl);
        }
    }
}
