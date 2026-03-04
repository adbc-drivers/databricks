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
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for CircuitBreakerManager singleton class.
    /// </summary>
    public class CircuitBreakerManagerTests : IDisposable
    {
        private readonly CircuitBreakerManager _manager;

        public CircuitBreakerManagerTests()
        {
            _manager = CircuitBreakerManager.GetInstance();
            // Reset state before each test to ensure isolation
            _manager.Reset();
        }

        public void Dispose()
        {
            // Clean up after each test
            _manager.Reset();
        }

        #region Singleton Tests

        [Fact]
        public void GetInstance_ReturnsSingleton()
        {
            // Act
            CircuitBreakerManager instance1 = CircuitBreakerManager.GetInstance();
            CircuitBreakerManager instance2 = CircuitBreakerManager.GetInstance();

            // Assert
            Assert.NotNull(instance1);
            Assert.NotNull(instance2);
            Assert.Same(instance1, instance2);
        }

        #endregion

        #region GetCircuitBreaker - New Host Tests

        [Fact]
        public void GetCircuitBreaker_NewHost_CreatesBreaker()
        {
            // Act
            CircuitBreaker breaker = _manager.GetCircuitBreaker("host1.databricks.com");

            // Assert
            Assert.NotNull(breaker);
            Assert.Equal(CircuitBreakerState.Closed, breaker.State);
        }

        [Fact]
        public void GetCircuitBreaker_NewHostWithConfig_CreatesBreaker()
        {
            // Act
            CircuitBreaker breaker = _manager.GetCircuitBreaker(
                "host1.databricks.com",
                failureThreshold: 10,
                timeout: TimeSpan.FromMinutes(2));

            // Assert
            Assert.NotNull(breaker);
            Assert.Equal(CircuitBreakerState.Closed, breaker.State);
        }

        #endregion

        #region GetCircuitBreaker - Same Host Tests

        [Fact]
        public void GetCircuitBreaker_SameHost_ReturnsSameBreaker()
        {
            // Act
            CircuitBreaker breaker1 = _manager.GetCircuitBreaker("host1.databricks.com");
            CircuitBreaker breaker2 = _manager.GetCircuitBreaker("host1.databricks.com");

            // Assert
            Assert.Same(breaker1, breaker2);
        }

        [Fact]
        public void GetCircuitBreaker_SameHostCaseInsensitive_ReturnsSameBreaker()
        {
            // Act
            CircuitBreaker breaker1 = _manager.GetCircuitBreaker("Host1.Databricks.Com");
            CircuitBreaker breaker2 = _manager.GetCircuitBreaker("host1.databricks.com");

            // Assert
            Assert.Same(breaker1, breaker2);
        }

        [Fact]
        public void GetCircuitBreaker_SameHostWithConfigOverload_ReturnsSameBreaker()
        {
            // Arrange - create with default config
            CircuitBreaker breaker1 = _manager.GetCircuitBreaker("host1.databricks.com");

            // Act - request again with config (should return existing, not create new)
            CircuitBreaker breaker2 = _manager.GetCircuitBreaker(
                "host1.databricks.com",
                failureThreshold: 10,
                timeout: TimeSpan.FromMinutes(5));

            // Assert
            Assert.Same(breaker1, breaker2);
        }

        #endregion

        #region GetCircuitBreaker - Different Hosts Tests

        [Fact]
        public void GetCircuitBreaker_DifferentHosts_CreatesSeparateBreakers()
        {
            // Act
            CircuitBreaker breaker1 = _manager.GetCircuitBreaker("host1.databricks.com");
            CircuitBreaker breaker2 = _manager.GetCircuitBreaker("host2.databricks.com");

            // Assert
            Assert.NotSame(breaker1, breaker2);
        }

        [Fact]
        public void GetCircuitBreaker_MultipleHosts_AllSeparate()
        {
            // Act
            CircuitBreaker breaker1 = _manager.GetCircuitBreaker("host1.databricks.com");
            CircuitBreaker breaker2 = _manager.GetCircuitBreaker("host2.databricks.com");
            CircuitBreaker breaker3 = _manager.GetCircuitBreaker("host3.databricks.com");

            // Assert
            Assert.NotSame(breaker1, breaker2);
            Assert.NotSame(breaker2, breaker3);
            Assert.NotSame(breaker1, breaker3);
        }

        [Fact]
        public async Task GetCircuitBreaker_DifferentHosts_IndependentState()
        {
            // Arrange
            CircuitBreaker breaker1 = _manager.GetCircuitBreaker("host1.databricks.com",
                failureThreshold: 2, timeout: TimeSpan.FromMinutes(5));
            CircuitBreaker breaker2 = _manager.GetCircuitBreaker("host2.databricks.com");

            // Act - Trip breaker1 by causing failures
            for (int i = 0; i < 2; i++)
            {
                try
                {
                    await breaker1.ExecuteAsync(() => throw new Exception("Failure"));
                }
                catch { }
            }

            // Assert - breaker1 is open, breaker2 is still closed
            Assert.Equal(CircuitBreakerState.Open, breaker1.State);
            Assert.Equal(CircuitBreakerState.Closed, breaker2.State);
        }

        #endregion

        #region Thread Safety Tests

        [Fact]
        public async Task GetCircuitBreaker_ThreadSafe_NoDuplicates()
        {
            // Arrange
            int threadCount = 10;
            ConcurrentBag<CircuitBreaker> breakers = new ConcurrentBag<CircuitBreaker>();
            using ManualResetEventSlim barrier = new ManualResetEventSlim(false);
            Task[] tasks = new Task[threadCount];

            // Act - 10 threads all requesting the same host concurrently
            for (int i = 0; i < threadCount; i++)
            {
                tasks[i] = Task.Run(() =>
                {
                    barrier.Wait(); // Wait for all threads to be ready
                    CircuitBreaker breaker = _manager.GetCircuitBreaker("shared-host.databricks.com");
                    breakers.Add(breaker);
                });
            }

            // Release all threads simultaneously
            barrier.Set();
            await Task.WhenAll(tasks);

            // Assert - All threads got the same instance
            CircuitBreaker[] breakerArray = breakers.ToArray();
            Assert.Equal(threadCount, breakerArray.Length);
            CircuitBreaker first = breakerArray[0];
            for (int i = 1; i < breakerArray.Length; i++)
            {
                Assert.Same(first, breakerArray[i]);
            }
        }

        [Fact]
        public async Task GetCircuitBreaker_ThreadSafe_ConcurrentDifferentHosts()
        {
            // Arrange
            int hostCount = 20;
            ConcurrentDictionary<string, CircuitBreaker> results = new ConcurrentDictionary<string, CircuitBreaker>();
            Task[] tasks = new Task[hostCount];

            // Act - Multiple threads requesting different hosts
            for (int i = 0; i < hostCount; i++)
            {
                int index = i;
                tasks[i] = Task.Run(() =>
                {
                    string host = $"host{index}.databricks.com";
                    CircuitBreaker breaker = _manager.GetCircuitBreaker(host);
                    results.TryAdd(host, breaker);
                });
            }

            await Task.WhenAll(tasks);

            // Assert - Each host has a unique breaker
            Assert.Equal(hostCount, results.Count);
            List<CircuitBreaker> uniqueBreakers = new List<CircuitBreaker>(results.Values);
            for (int i = 0; i < uniqueBreakers.Count; i++)
            {
                for (int j = i + 1; j < uniqueBreakers.Count; j++)
                {
                    Assert.NotSame(uniqueBreakers[i], uniqueBreakers[j]);
                }
            }
        }

        [Fact]
        public async Task GetCircuitBreaker_ThreadSafe_MixedSameAndDifferentHosts()
        {
            // Arrange - 50 threads, 5 hosts, 10 threads per host
            int hostsPerGroup = 5;
            int threadsPerHost = 10;
            int totalThreads = hostsPerGroup * threadsPerHost;
            ConcurrentDictionary<string, ConcurrentBag<CircuitBreaker>> hostBreakers =
                new ConcurrentDictionary<string, ConcurrentBag<CircuitBreaker>>();
            using ManualResetEventSlim barrier = new ManualResetEventSlim(false);
            Task[] tasks = new Task[totalThreads];

            // Initialize bags
            for (int h = 0; h < hostsPerGroup; h++)
            {
                hostBreakers[$"host{h}.databricks.com"] = new ConcurrentBag<CircuitBreaker>();
            }

            // Act
            int taskIndex = 0;
            for (int h = 0; h < hostsPerGroup; h++)
            {
                int hostIndex = h;
                for (int t = 0; t < threadsPerHost; t++)
                {
                    tasks[taskIndex++] = Task.Run(() =>
                    {
                        barrier.Wait();
                        string host = $"host{hostIndex}.databricks.com";
                        CircuitBreaker breaker = _manager.GetCircuitBreaker(host);
                        hostBreakers[host].Add(breaker);
                    });
                }
            }

            barrier.Set();
            await Task.WhenAll(tasks);

            // Assert - Each host group should have the same breaker
            foreach (KeyValuePair<string, ConcurrentBag<CircuitBreaker>> entry in hostBreakers)
            {
                CircuitBreaker[] breakers = entry.Value.ToArray();
                Assert.Equal(threadsPerHost, breakers.Length);
                CircuitBreaker first = breakers[0];
                for (int i = 1; i < breakers.Length; i++)
                {
                    Assert.Same(first, breakers[i]);
                }
            }

            // Assert - Different hosts have different breakers
            List<CircuitBreaker> distinctBreakers = new List<CircuitBreaker>();
            foreach (KeyValuePair<string, ConcurrentBag<CircuitBreaker>> entry in hostBreakers)
            {
                distinctBreakers.Add(entry.Value.ToArray()[0]);
            }
            for (int i = 0; i < distinctBreakers.Count; i++)
            {
                for (int j = i + 1; j < distinctBreakers.Count; j++)
                {
                    Assert.NotSame(distinctBreakers[i], distinctBreakers[j]);
                }
            }
        }

        #endregion

        #region Input Validation Tests

        [Fact]
        public void GetCircuitBreaker_NullHost_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => _manager.GetCircuitBreaker(null!));
        }

        [Fact]
        public void GetCircuitBreaker_EmptyHost_ThrowsArgumentException()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() => _manager.GetCircuitBreaker(""));
        }

        [Fact]
        public void GetCircuitBreaker_WhitespaceHost_ThrowsArgumentException()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() => _manager.GetCircuitBreaker("   "));
        }

        [Fact]
        public void GetCircuitBreakerWithConfig_NullHost_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                _manager.GetCircuitBreaker(null!, 5, TimeSpan.FromMinutes(1)));
        }

        [Fact]
        public void GetCircuitBreakerWithConfig_EmptyHost_ThrowsArgumentException()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                _manager.GetCircuitBreaker("", 5, TimeSpan.FromMinutes(1)));
        }

        #endregion

        #region RemoveCircuitBreaker Tests

        [Fact]
        public void RemoveCircuitBreaker_ExistingHost_ReturnsTrue()
        {
            // Arrange
            _manager.GetCircuitBreaker("host1.databricks.com");

            // Act
            bool removed = _manager.RemoveCircuitBreaker("host1.databricks.com");

            // Assert
            Assert.True(removed);
        }

        [Fact]
        public void RemoveCircuitBreaker_NonExistingHost_ReturnsFalse()
        {
            // Act
            bool removed = _manager.RemoveCircuitBreaker("nonexistent.databricks.com");

            // Assert
            Assert.False(removed);
        }

        [Fact]
        public void RemoveCircuitBreaker_AfterRemove_GetCreatesNew()
        {
            // Arrange
            CircuitBreaker original = _manager.GetCircuitBreaker("host1.databricks.com");
            _manager.RemoveCircuitBreaker("host1.databricks.com");

            // Act
            CircuitBreaker newBreaker = _manager.GetCircuitBreaker("host1.databricks.com");

            // Assert
            Assert.NotSame(original, newBreaker);
        }

        [Fact]
        public void RemoveCircuitBreaker_NullHost_ReturnsFalse()
        {
            // Act
            bool removed = _manager.RemoveCircuitBreaker(null!);

            // Assert
            Assert.False(removed);
        }

        [Fact]
        public void RemoveCircuitBreaker_EmptyHost_ReturnsFalse()
        {
            // Act
            bool removed = _manager.RemoveCircuitBreaker("");

            // Assert
            Assert.False(removed);
        }

        #endregion

        #region Reset Tests

        [Fact]
        public void Reset_ClearsAllBreakers()
        {
            // Arrange
            CircuitBreaker breaker1 = _manager.GetCircuitBreaker("host1.databricks.com");
            CircuitBreaker breaker2 = _manager.GetCircuitBreaker("host2.databricks.com");

            // Act
            _manager.Reset();

            // Assert - New breakers should be different instances
            CircuitBreaker newBreaker1 = _manager.GetCircuitBreaker("host1.databricks.com");
            CircuitBreaker newBreaker2 = _manager.GetCircuitBreaker("host2.databricks.com");
            Assert.NotSame(breaker1, newBreaker1);
            Assert.NotSame(breaker2, newBreaker2);
        }

        #endregion
    }
}
