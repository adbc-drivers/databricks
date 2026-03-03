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
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for CircuitBreakerManager singleton.
    /// Uses the "CircuitBreakerTests" collection to avoid parallel execution
    /// with other tests that call <see cref="CircuitBreakerManager.Reset"/>.
    /// </summary>
    [Collection("CircuitBreakerTests")]
    public class CircuitBreakerManagerTests : IDisposable
    {
        private readonly CircuitBreakerManager _manager;

        public CircuitBreakerManagerTests()
        {
            _manager = CircuitBreakerManager.GetInstance();
            // Reset state before each test to avoid cross-test contamination
            _manager.Reset();
        }

        public void Dispose()
        {
            // Clean up after each test
            _manager.Reset();
        }

        #region Singleton Tests

        [Fact]
        public void GetInstance_ReturnsSameInstance()
        {
            // Arrange & Act
            var instance1 = CircuitBreakerManager.GetInstance();
            var instance2 = CircuitBreakerManager.GetInstance();

            // Assert
            Assert.Same(instance1, instance2);
        }

        [Fact]
        public void GetInstance_ReturnsNonNullInstance()
        {
            // Act
            var instance = CircuitBreakerManager.GetInstance();

            // Assert
            Assert.NotNull(instance);
        }

        #endregion

        #region GetCircuitBreaker - New Host Tests

        [Fact]
        public void GetCircuitBreaker_NewHost_CreatesBreaker()
        {
            // Arrange
            var host = "host1.databricks.com";

            // Act
            var breaker = _manager.GetCircuitBreaker(host);

            // Assert
            Assert.NotNull(breaker);
            Assert.Equal(CircuitBreakerState.Closed, breaker.State);
        }

        [Fact]
        public void GetCircuitBreaker_MultipleNewHosts_CreatesBreakers()
        {
            // Arrange & Act
            var breaker1 = _manager.GetCircuitBreaker("host1.databricks.com");
            var breaker2 = _manager.GetCircuitBreaker("host2.databricks.com");
            var breaker3 = _manager.GetCircuitBreaker("host3.databricks.com");

            // Assert
            Assert.NotNull(breaker1);
            Assert.NotNull(breaker2);
            Assert.NotNull(breaker3);
        }

        #endregion

        #region GetCircuitBreaker - Same Host Returns Same Breaker

        [Fact]
        public void GetCircuitBreaker_SameHost_ReturnsSameBreaker()
        {
            // Arrange
            var host = "host1.databricks.com";

            // Act
            var breaker1 = _manager.GetCircuitBreaker(host);
            var breaker2 = _manager.GetCircuitBreaker(host);

            // Assert
            Assert.Same(breaker1, breaker2);
        }

        [Fact]
        public void GetCircuitBreaker_SameHostCalledMultipleTimes_ReturnsSameBreaker()
        {
            // Arrange
            var host = "host1.databricks.com";

            // Act
            var breaker1 = _manager.GetCircuitBreaker(host);
            var breaker2 = _manager.GetCircuitBreaker(host);
            var breaker3 = _manager.GetCircuitBreaker(host);
            var breaker4 = _manager.GetCircuitBreaker(host);

            // Assert
            Assert.Same(breaker1, breaker2);
            Assert.Same(breaker2, breaker3);
            Assert.Same(breaker3, breaker4);
        }

        [Fact]
        public void GetCircuitBreaker_SameHostDifferentCase_ReturnsSameBreaker()
        {
            // Arrange & Act
            var breaker1 = _manager.GetCircuitBreaker("HOST1.DATABRICKS.COM");
            var breaker2 = _manager.GetCircuitBreaker("host1.databricks.com");
            var breaker3 = _manager.GetCircuitBreaker("Host1.Databricks.Com");

            // Assert - Case-insensitive comparison
            Assert.Same(breaker1, breaker2);
            Assert.Same(breaker2, breaker3);
        }

        #endregion

        #region GetCircuitBreaker - Different Hosts Return Different Breakers

        [Fact]
        public void GetCircuitBreaker_DifferentHosts_CreatesSeparateBreakers()
        {
            // Arrange & Act
            var breaker1 = _manager.GetCircuitBreaker("host1.databricks.com");
            var breaker2 = _manager.GetCircuitBreaker("host2.databricks.com");

            // Assert
            Assert.NotSame(breaker1, breaker2);
        }

        [Fact]
        public void GetCircuitBreaker_DifferentHosts_BreakerStatesAreIndependent()
        {
            // Arrange
            var breaker1 = _manager.GetCircuitBreaker("host1.databricks.com");
            var breaker2 = _manager.GetCircuitBreaker("host2.databricks.com");

            // Assert - Both start in Closed state
            Assert.Equal(CircuitBreakerState.Closed, breaker1.State);
            Assert.Equal(CircuitBreakerState.Closed, breaker2.State);
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

        #endregion

        #region Thread Safety Tests

        [Fact]
        public async Task GetCircuitBreaker_ThreadSafe_NoDuplicates()
        {
            // Arrange
            var host = "concurrent-host.databricks.com";
            var threadCount = 10;
            var breakers = new CircuitBreaker[threadCount];
            var barrier = new ManualResetEventSlim(false);
            var tasks = new Task[threadCount];

            // Act - 10 concurrent threads requesting the same host
            for (int i = 0; i < threadCount; i++)
            {
                int index = i;
                tasks[i] = Task.Run(() =>
                {
                    barrier.Wait(); // Ensure all threads start concurrently
                    breakers[index] = _manager.GetCircuitBreaker(host);
                });
            }

            // Release all threads at the same time
            barrier.Set();
            await Task.WhenAll(tasks);

            // Assert - All threads should get the same instance
            for (int i = 1; i < threadCount; i++)
            {
                Assert.Same(breakers[0], breakers[i]);
            }
        }

        [Fact]
        public async Task GetCircuitBreaker_ConcurrentDifferentHosts_CreatesAllBreakers()
        {
            // Arrange
            var hostCount = 20;
            var breakers = new CircuitBreaker[hostCount];
            var barrier = new ManualResetEventSlim(false);
            var tasks = new Task[hostCount];

            // Act - 20 concurrent threads requesting different hosts
            for (int i = 0; i < hostCount; i++)
            {
                int index = i;
                tasks[i] = Task.Run(() =>
                {
                    barrier.Wait();
                    breakers[index] = _manager.GetCircuitBreaker($"host{index}.databricks.com");
                });
            }

            barrier.Set();
            await Task.WhenAll(tasks);

            // Assert - All breakers should be created and unique
            for (int i = 0; i < hostCount; i++)
            {
                Assert.NotNull(breakers[i]);
                for (int j = i + 1; j < hostCount; j++)
                {
                    Assert.NotSame(breakers[i], breakers[j]);
                }
            }
        }

        [Fact]
        public async Task GetCircuitBreaker_ConcurrentMixedHosts_CorrectInstances()
        {
            // Arrange - Multiple threads requesting a mix of 3 hosts
            var hosts = new[] { "host-a.databricks.com", "host-b.databricks.com", "host-c.databricks.com" };
            var threadCount = 30;
            var breakers = new CircuitBreaker[threadCount];
            var barrier = new ManualResetEventSlim(false);
            var tasks = new Task[threadCount];

            // Act
            for (int i = 0; i < threadCount; i++)
            {
                int index = i;
                tasks[i] = Task.Run(() =>
                {
                    barrier.Wait();
                    breakers[index] = _manager.GetCircuitBreaker(hosts[index % hosts.Length]);
                });
            }

            barrier.Set();
            await Task.WhenAll(tasks);

            // Assert - Threads requesting the same host should get the same instance
            for (int i = 0; i < threadCount; i++)
            {
                for (int j = i + 1; j < threadCount; j++)
                {
                    if (hosts[i % hosts.Length] == hosts[j % hosts.Length])
                    {
                        Assert.Same(breakers[i], breakers[j]);
                    }
                    else
                    {
                        Assert.NotSame(breakers[i], breakers[j]);
                    }
                }
            }
        }

        #endregion

        #region Reset Tests

        [Fact]
        public void Reset_ClearsAllBreakers()
        {
            // Arrange
            var breaker1 = _manager.GetCircuitBreaker("host1.databricks.com");
            var breaker2 = _manager.GetCircuitBreaker("host2.databricks.com");

            // Act
            _manager.Reset();

            // Assert - New breakers should be created after reset
            var newBreaker1 = _manager.GetCircuitBreaker("host1.databricks.com");
            var newBreaker2 = _manager.GetCircuitBreaker("host2.databricks.com");

            Assert.NotSame(breaker1, newBreaker1);
            Assert.NotSame(breaker2, newBreaker2);
        }

        #endregion
    }
}
