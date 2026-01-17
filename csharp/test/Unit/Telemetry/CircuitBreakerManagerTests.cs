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
using System.Linq;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for CircuitBreakerManager to ensure correct per-host management.
    /// </summary>
    public class CircuitBreakerManagerTests : IDisposable
    {
        private readonly CircuitBreakerManager _manager;

        public CircuitBreakerManagerTests()
        {
            _manager = CircuitBreakerManager.GetInstance();
            // Clean up before each test
            _manager.Clear();
        }

        public void Dispose()
        {
            // Clean up after each test
            _manager.Clear();
        }

        [Fact]
        public void CircuitBreakerManager_GetInstance_ReturnsSingleton()
        {
            // Act
            var instance1 = CircuitBreakerManager.GetInstance();
            var instance2 = CircuitBreakerManager.GetInstance();

            // Assert
            Assert.NotNull(instance1);
            Assert.NotNull(instance2);
            Assert.Same(instance1, instance2);
        }

        [Fact]
        public void CircuitBreakerManager_GetCircuitBreaker_NewHost_CreatesBreaker()
        {
            // Arrange
            const string host = "host1.databricks.com";

            // Act
            var breaker = _manager.GetCircuitBreaker(host);

            // Assert
            Assert.NotNull(breaker);
            Assert.Equal(CircuitBreakerState.Closed, breaker.State);
        }

        [Fact]
        public void CircuitBreakerManager_GetCircuitBreaker_SameHost_ReturnsSameBreaker()
        {
            // Arrange
            const string host = "host1.databricks.com";

            // Act
            var breaker1 = _manager.GetCircuitBreaker(host);
            var breaker2 = _manager.GetCircuitBreaker(host);

            // Assert
            Assert.NotNull(breaker1);
            Assert.NotNull(breaker2);
            Assert.Same(breaker1, breaker2);
        }

        [Fact]
        public void CircuitBreakerManager_GetCircuitBreaker_DifferentHosts_CreatesSeparateBreakers()
        {
            // Arrange
            const string host1 = "host1.databricks.com";
            const string host2 = "host2.databricks.com";

            // Act
            var breaker1 = _manager.GetCircuitBreaker(host1);
            var breaker2 = _manager.GetCircuitBreaker(host2);

            // Assert
            Assert.NotNull(breaker1);
            Assert.NotNull(breaker2);
            Assert.NotSame(breaker1, breaker2);
        }

        [Fact]
        public void CircuitBreakerManager_GetCircuitBreaker_NullHost_ThrowsArgumentException()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() => _manager.GetCircuitBreaker(null!));
        }

        [Fact]
        public void CircuitBreakerManager_GetCircuitBreaker_EmptyHost_ThrowsArgumentException()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() => _manager.GetCircuitBreaker(string.Empty));
        }

        [Fact]
        public void CircuitBreakerManager_GetCircuitBreaker_WithConfig_UsesCustomConfig()
        {
            // Arrange
            const string host = "host1.databricks.com";
            var customConfig = new CircuitBreakerConfig
            {
                FailureThreshold = 3,
                Timeout = TimeSpan.FromSeconds(2),
                SuccessThreshold = 1
            };

            // Act
            var breaker = _manager.GetCircuitBreaker(host, customConfig);

            // Assert
            Assert.NotNull(breaker);
            Assert.Equal(customConfig.FailureThreshold, breaker.Config.FailureThreshold);
            Assert.Equal(customConfig.Timeout, breaker.Config.Timeout);
            Assert.Equal(customConfig.SuccessThreshold, breaker.Config.SuccessThreshold);
        }

        [Fact]
        public void CircuitBreakerManager_GetCircuitBreaker_WithConfig_NullConfig_ThrowsArgumentNullException()
        {
            // Arrange
            const string host = "host1.databricks.com";

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => _manager.GetCircuitBreaker(host, null!));
        }

        [Fact]
        public async Task CircuitBreakerManager_PerHostIsolation_OneHostOpenDoesNotAffectOther()
        {
            // Arrange
            const string host1 = "host1.databricks.com";
            const string host2 = "host2.databricks.com";
            var config = new CircuitBreakerConfig
            {
                FailureThreshold = 2,
                Timeout = TimeSpan.FromMinutes(10)
            };

            var breaker1 = _manager.GetCircuitBreaker(host1, config);
            var breaker2 = _manager.GetCircuitBreaker(host2, config);

            // Act - Open circuit for host1
            for (int i = 0; i < 2; i++)
            {
                try
                {
                    await breaker1.ExecuteAsync(async () =>
                    {
                        await Task.CompletedTask;
                        throw new Exception("Test failure");
                    });
                }
                catch (Exception)
                {
                    // Expected
                }
            }

            // Assert - host1 is open, host2 is still closed
            Assert.Equal(CircuitBreakerState.Open, breaker1.State);
            Assert.Equal(CircuitBreakerState.Closed, breaker2.State);

            // Verify host2 can still execute
            var executed = false;
            await breaker2.ExecuteAsync(async () =>
            {
                executed = true;
                await Task.CompletedTask;
            });
            Assert.True(executed);
        }

        [Fact]
        public void CircuitBreakerManager_RemoveCircuitBreaker_RemovesBreaker()
        {
            // Arrange
            const string host = "host1.databricks.com";
            var breaker1 = _manager.GetCircuitBreaker(host);

            // Act
            var removed = _manager.RemoveCircuitBreaker(host);
            var breaker2 = _manager.GetCircuitBreaker(host);

            // Assert
            Assert.True(removed);
            Assert.NotSame(breaker1, breaker2); // Should be a new instance
        }

        [Fact]
        public void CircuitBreakerManager_RemoveCircuitBreaker_NonExistentHost_ReturnsFalse()
        {
            // Act
            var removed = _manager.RemoveCircuitBreaker("nonexistent.host.com");

            // Assert
            Assert.False(removed);
        }

        [Fact]
        public void CircuitBreakerManager_Clear_RemovesAllBreakers()
        {
            // Arrange
            const string host1 = "host1.databricks.com";
            const string host2 = "host2.databricks.com";
            var breaker1 = _manager.GetCircuitBreaker(host1);
            var breaker2 = _manager.GetCircuitBreaker(host2);

            // Act
            _manager.Clear();
            var newBreaker1 = _manager.GetCircuitBreaker(host1);
            var newBreaker2 = _manager.GetCircuitBreaker(host2);

            // Assert
            Assert.NotSame(breaker1, newBreaker1);
            Assert.NotSame(breaker2, newBreaker2);
        }

        [Fact]
        public void CircuitBreakerManager_ConcurrentAccess_NoDuplicates()
        {
            // Arrange
            const string host = "host1.databricks.com";
            const int threadCount = 10;
            var breakers = new CircuitBreaker[threadCount];

            // Act - Get circuit breaker from multiple threads concurrently
            Parallel.For(0, threadCount, i =>
            {
                breakers[i] = _manager.GetCircuitBreaker(host);
            });

            // Assert - All should be the same instance
            var firstBreaker = breakers[0];
            Assert.All(breakers, breaker => Assert.Same(firstBreaker, breaker));
        }
    }
}
