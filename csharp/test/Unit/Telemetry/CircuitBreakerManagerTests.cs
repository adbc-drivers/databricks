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
    /// Tests for CircuitBreakerManager class.
    /// </summary>
    public class CircuitBreakerManagerTests
    {
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

        [Fact]
        public void GetCircuitBreaker_NewHost_CreatesBreaker()
        {
            // Arrange
            CircuitBreakerManager manager = CircuitBreakerManager.GetInstance();
            string host = $"test-host-{Guid.NewGuid()}.databricks.com";

            // Act
            CircuitBreaker circuitBreaker = manager.GetCircuitBreaker(host);

            // Assert
            Assert.NotNull(circuitBreaker);
            Assert.Equal(CircuitBreakerState.Closed, circuitBreaker.State);
        }

        [Fact]
        public void GetCircuitBreaker_SameHost_ReturnsSameBreaker()
        {
            // Arrange
            CircuitBreakerManager manager = CircuitBreakerManager.GetInstance();
            string host = $"test-host-{Guid.NewGuid()}.databricks.com";

            // Act
            CircuitBreaker breaker1 = manager.GetCircuitBreaker(host);
            CircuitBreaker breaker2 = manager.GetCircuitBreaker(host);

            // Assert
            Assert.NotNull(breaker1);
            Assert.NotNull(breaker2);
            Assert.Same(breaker1, breaker2);
        }

        [Fact]
        public void GetCircuitBreaker_DifferentHosts_CreatesSeparateBreakers()
        {
            // Arrange
            CircuitBreakerManager manager = CircuitBreakerManager.GetInstance();
            string host1 = $"test-host1-{Guid.NewGuid()}.databricks.com";
            string host2 = $"test-host2-{Guid.NewGuid()}.databricks.com";

            // Act
            CircuitBreaker breaker1 = manager.GetCircuitBreaker(host1);
            CircuitBreaker breaker2 = manager.GetCircuitBreaker(host2);

            // Assert
            Assert.NotNull(breaker1);
            Assert.NotNull(breaker2);
            Assert.NotSame(breaker1, breaker2);
        }

        [Fact]
        public void GetCircuitBreaker_NullHost_ThrowsArgumentException()
        {
            // Arrange
            CircuitBreakerManager manager = CircuitBreakerManager.GetInstance();

            // Act & Assert
            Assert.Throws<ArgumentException>(() => manager.GetCircuitBreaker(null!));
        }

        [Fact]
        public void GetCircuitBreaker_EmptyHost_ThrowsArgumentException()
        {
            // Arrange
            CircuitBreakerManager manager = CircuitBreakerManager.GetInstance();

            // Act & Assert
            Assert.Throws<ArgumentException>(() => manager.GetCircuitBreaker(string.Empty));
        }

        [Fact]
        public void GetCircuitBreaker_WhitespaceHost_ThrowsArgumentException()
        {
            // Arrange
            CircuitBreakerManager manager = CircuitBreakerManager.GetInstance();

            // Act & Assert
            Assert.Throws<ArgumentException>(() => manager.GetCircuitBreaker("   "));
        }

        [Fact]
        public async Task GetCircuitBreaker_ThreadSafe_NoDuplicates()
        {
            // Arrange
            CircuitBreakerManager manager = CircuitBreakerManager.GetInstance();
            string host = $"test-host-{Guid.NewGuid()}.databricks.com";
            int threadCount = 10;
            List<CircuitBreaker> breakers = new List<CircuitBreaker>();
            object lockObj = new object();

            // Act - create circuit breakers concurrently from multiple threads
            Task[] tasks = Enumerable.Range(0, threadCount).Select(_ => Task.Run(() =>
            {
                CircuitBreaker breaker = manager.GetCircuitBreaker(host);
                lock (lockObj)
                {
                    breakers.Add(breaker);
                }
            })).ToArray();

            await Task.WhenAll(tasks);

            // Assert - all circuit breakers should be the same instance
            Assert.Equal(threadCount, breakers.Count);
            CircuitBreaker firstBreaker = breakers[0];
            foreach (CircuitBreaker breaker in breakers)
            {
                Assert.Same(firstBreaker, breaker);
            }
        }

        [Fact]
        public async Task GetCircuitBreaker_IsolatesFailuresAcrossHosts()
        {
            // Arrange
            CircuitBreakerManager manager = CircuitBreakerManager.GetInstance();
            string host1 = $"test-host1-{Guid.NewGuid()}.databricks.com";
            string host2 = $"test-host2-{Guid.NewGuid()}.databricks.com";

            CircuitBreaker breaker1 = manager.GetCircuitBreaker(host1);
            CircuitBreaker breaker2 = manager.GetCircuitBreaker(host2);

            // Act - Cause failures on breaker1 to open it (need at least 5 failures)
            for (int i = 0; i < 5; i++)
            {
                try
                {
                    await breaker1.ExecuteAsync(() => throw new Exception("Simulated failure"));
                }
                catch { }
            }

            // Assert - breaker1 should be open, breaker2 should still be closed
            Assert.Equal(CircuitBreakerState.Open, breaker1.State);
            Assert.Equal(CircuitBreakerState.Closed, breaker2.State);

            // Verify breaker2 can still execute successfully
            bool executed = false;
            await breaker2.ExecuteAsync(async () =>
            {
                executed = true;
                await Task.CompletedTask;
            });
            Assert.True(executed);
        }

        [Fact]
        public void GetCircuitBreaker_UsesDefaultConfiguration()
        {
            // Arrange
            CircuitBreakerManager manager = CircuitBreakerManager.GetInstance();
            string host = $"test-host-{Guid.NewGuid()}.databricks.com";

            // Act
            CircuitBreaker breaker = manager.GetCircuitBreaker(host);

            // Assert - verify it starts in Closed state (default initial state)
            Assert.Equal(CircuitBreakerState.Closed, breaker.State);
        }
    }
}
