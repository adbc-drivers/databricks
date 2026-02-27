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
    /// Tests for CircuitBreakerManager class.
    /// </summary>
    public class CircuitBreakerManagerTests
    {
        #region Singleton Tests

        [Fact]
        public void CircuitBreakerManager_GetInstance_ReturnsSingleton()
        {
            // Act
            var instance1 = CircuitBreakerManager.GetInstance();
            var instance2 = CircuitBreakerManager.GetInstance();

            // Assert
            Assert.NotNull(instance1);
            Assert.Same(instance1, instance2);
        }

        #endregion

        #region GetCircuitBreaker - New Host Tests

        [Fact]
        public void CircuitBreakerManager_GetCircuitBreaker_NewHost_CreatesBreaker()
        {
            // Arrange
            var manager = new CircuitBreakerManager();
            var host = "test-host-new.databricks.com";

            // Act
            var circuitBreaker = manager.GetCircuitBreaker(host);

            // Assert
            Assert.NotNull(circuitBreaker);
            Assert.Equal(1, manager.CircuitBreakerCount);
            Assert.True(manager.HasCircuitBreaker(host));
        }

        [Fact]
        public void CircuitBreakerManager_GetCircuitBreaker_NewHost_UsesDefaultConfig()
        {
            // Arrange
            var defaultConfig = new CircuitBreakerConfig
            {
                FailureThreshold = 10,
                Timeout = TimeSpan.FromMinutes(2),
                SuccessThreshold = 3
            };
            var manager = new CircuitBreakerManager(defaultConfig);
            var host = "test-host-config.databricks.com";

            // Act
            var circuitBreaker = manager.GetCircuitBreaker(host);

            // Assert
            Assert.NotNull(circuitBreaker);
            Assert.Equal(10, circuitBreaker.Config.FailureThreshold);
            Assert.Equal(TimeSpan.FromMinutes(2), circuitBreaker.Config.Timeout);
            Assert.Equal(3, circuitBreaker.Config.SuccessThreshold);
        }

        [Fact]
        public void CircuitBreakerManager_GetCircuitBreaker_NullHost_ThrowsException()
        {
            // Arrange
            var manager = new CircuitBreakerManager();

            // Act & Assert
            Assert.Throws<ArgumentException>(() => manager.GetCircuitBreaker(null!));
        }

        [Fact]
        public void CircuitBreakerManager_GetCircuitBreaker_EmptyHost_ThrowsException()
        {
            // Arrange
            var manager = new CircuitBreakerManager();

            // Act & Assert
            Assert.Throws<ArgumentException>(() => manager.GetCircuitBreaker(string.Empty));
        }

        [Fact]
        public void CircuitBreakerManager_GetCircuitBreaker_WhitespaceHost_ThrowsException()
        {
            // Arrange
            var manager = new CircuitBreakerManager();

            // Act & Assert
            Assert.Throws<ArgumentException>(() => manager.GetCircuitBreaker("   "));
        }

        #endregion

        #region GetCircuitBreaker - Same Host Tests

        [Fact]
        public void CircuitBreakerManager_GetCircuitBreaker_SameHost_ReturnsSameBreaker()
        {
            // Arrange
            var manager = new CircuitBreakerManager();
            var host = "test-host-same.databricks.com";

            // Act
            var circuitBreaker1 = manager.GetCircuitBreaker(host);
            var circuitBreaker2 = manager.GetCircuitBreaker(host);

            // Assert
            Assert.Same(circuitBreaker1, circuitBreaker2);
            Assert.Equal(1, manager.CircuitBreakerCount);
        }

        [Fact]
        public void CircuitBreakerManager_GetCircuitBreaker_SameHostDifferentCase_ReturnsSameBreaker()
        {
            // Arrange
            var manager = new CircuitBreakerManager();

            // Act
            var circuitBreaker1 = manager.GetCircuitBreaker("TEST-HOST.databricks.com");
            var circuitBreaker2 = manager.GetCircuitBreaker("test-host.databricks.com");
            var circuitBreaker3 = manager.GetCircuitBreaker("Test-Host.Databricks.Com");

            // Assert
            Assert.Same(circuitBreaker1, circuitBreaker2);
            Assert.Same(circuitBreaker2, circuitBreaker3);
            Assert.Equal(1, manager.CircuitBreakerCount);
        }

        [Fact]
        public void CircuitBreakerManager_GetCircuitBreaker_MultipleCallsSameHost_ReturnsSameInstance()
        {
            // Arrange
            var manager = new CircuitBreakerManager();
            var host = "test-host-multiple.databricks.com";
            var instances = new List<CircuitBreaker>();

            // Act
            for (int i = 0; i < 100; i++)
            {
                instances.Add(manager.GetCircuitBreaker(host));
            }

            // Assert
            Assert.All(instances, cb => Assert.Same(instances[0], cb));
            Assert.Equal(1, manager.CircuitBreakerCount);
        }

        #endregion

        #region GetCircuitBreaker - Different Hosts Tests

        [Fact]
        public void CircuitBreakerManager_GetCircuitBreaker_DifferentHosts_CreatesSeparateBreakers()
        {
            // Arrange
            var manager = new CircuitBreakerManager();
            var host1 = "host1.databricks.com";
            var host2 = "host2.databricks.com";

            // Act
            var circuitBreaker1 = manager.GetCircuitBreaker(host1);
            var circuitBreaker2 = manager.GetCircuitBreaker(host2);

            // Assert
            Assert.NotNull(circuitBreaker1);
            Assert.NotNull(circuitBreaker2);
            Assert.NotSame(circuitBreaker1, circuitBreaker2);
            Assert.Equal(2, manager.CircuitBreakerCount);
        }

        [Fact]
        public void CircuitBreakerManager_GetCircuitBreaker_ManyHosts_CreatesAllBreakers()
        {
            // Arrange
            var manager = new CircuitBreakerManager();
            var hosts = new string[]
            {
                "host1.databricks.com",
                "host2.databricks.com",
                "host3.databricks.com",
                "host4.databricks.com",
                "host5.databricks.com"
            };

            // Act
            var circuitBreakers = new Dictionary<string, CircuitBreaker>();
            foreach (var host in hosts)
            {
                circuitBreakers[host] = manager.GetCircuitBreaker(host);
            }

            // Assert
            Assert.Equal(5, manager.CircuitBreakerCount);
            foreach (var host in hosts)
            {
                Assert.True(manager.HasCircuitBreaker(host));
            }
        }

        #endregion

        #region GetCircuitBreaker with Config Tests

        [Fact]
        public void CircuitBreakerManager_GetCircuitBreakerWithConfig_NewHost_UsesProvidedConfig()
        {
            // Arrange
            var manager = new CircuitBreakerManager();
            var host = "test-host-custom.databricks.com";
            var customConfig = new CircuitBreakerConfig
            {
                FailureThreshold = 15,
                Timeout = TimeSpan.FromMinutes(5),
                SuccessThreshold = 4
            };

            // Act
            var circuitBreaker = manager.GetCircuitBreaker(host, customConfig);

            // Assert
            Assert.NotNull(circuitBreaker);
            Assert.Equal(15, circuitBreaker.Config.FailureThreshold);
            Assert.Equal(TimeSpan.FromMinutes(5), circuitBreaker.Config.Timeout);
            Assert.Equal(4, circuitBreaker.Config.SuccessThreshold);
        }

        [Fact]
        public void CircuitBreakerManager_GetCircuitBreakerWithConfig_ExistingHost_ReturnsExistingBreaker()
        {
            // Arrange
            var manager = new CircuitBreakerManager();
            var host = "test-host-existing.databricks.com";
            var originalConfig = new CircuitBreakerConfig
            {
                FailureThreshold = 10
            };
            var newConfig = new CircuitBreakerConfig
            {
                FailureThreshold = 20
            };

            // Act
            var circuitBreaker1 = manager.GetCircuitBreaker(host, originalConfig);
            var circuitBreaker2 = manager.GetCircuitBreaker(host, newConfig);

            // Assert
            Assert.Same(circuitBreaker1, circuitBreaker2);
            Assert.Equal(10, circuitBreaker2.Config.FailureThreshold); // Original config retained
        }

        [Fact]
        public void CircuitBreakerManager_GetCircuitBreakerWithConfig_NullConfig_ThrowsException()
        {
            // Arrange
            var manager = new CircuitBreakerManager();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                manager.GetCircuitBreaker("valid-host.databricks.com", null!));
        }

        #endregion

        #region Thread Safety Tests

        [Fact]
        public async Task CircuitBreakerManager_ConcurrentGetCircuitBreaker_SameHost_ThreadSafe()
        {
            // Arrange
            var manager = new CircuitBreakerManager();
            var host = "concurrent-host.databricks.com";
            var circuitBreakers = new CircuitBreaker[100];
            var tasks = new Task[100];

            // Act
            for (int i = 0; i < 100; i++)
            {
                int index = i;
                tasks[i] = Task.Run(() =>
                {
                    circuitBreakers[index] = manager.GetCircuitBreaker(host);
                });
            }

            await Task.WhenAll(tasks);

            // Assert
            Assert.Equal(1, manager.CircuitBreakerCount);
            Assert.All(circuitBreakers, cb => Assert.Same(circuitBreakers[0], cb));
        }

        [Fact]
        public async Task CircuitBreakerManager_ConcurrentGetCircuitBreaker_DifferentHosts_ThreadSafe()
        {
            // Arrange
            var manager = new CircuitBreakerManager();
            var hostCount = 50;
            var tasks = new Task[hostCount];

            // Act
            for (int i = 0; i < hostCount; i++)
            {
                int index = i;
                tasks[i] = Task.Run(() =>
                {
                    manager.GetCircuitBreaker($"host{index}.databricks.com");
                });
            }

            await Task.WhenAll(tasks);

            // Assert
            Assert.Equal(hostCount, manager.CircuitBreakerCount);
        }

        #endregion

        #region Per-Host Isolation Tests

        [Fact]
        public async Task CircuitBreakerManager_PerHostIsolation_FailureInOneHostDoesNotAffectOther()
        {
            // Arrange
            var config = new CircuitBreakerConfig { FailureThreshold = 2 };
            var manager = new CircuitBreakerManager(config);
            var host1 = "host1-isolation.databricks.com";
            var host2 = "host2-isolation.databricks.com";

            var cb1 = manager.GetCircuitBreaker(host1);
            var cb2 = manager.GetCircuitBreaker(host2);

            // Act - Cause failures on host1 to open its circuit
            for (int i = 0; i < 2; i++)
            {
                try
                {
                    await cb1.ExecuteAsync(() => throw new Exception("Failure"));
                }
                catch { }
            }

            // Assert - Host1 circuit is open, Host2 circuit is still closed
            Assert.Equal(CircuitBreakerState.Open, cb1.State);
            Assert.Equal(CircuitBreakerState.Closed, cb2.State);

            // Host2 should still execute successfully
            var executed = false;
            await cb2.ExecuteAsync(async () =>
            {
                executed = true;
                await Task.CompletedTask;
            });

            Assert.True(executed);
        }

        [Fact]
        public async Task CircuitBreakerManager_PerHostIsolation_IndependentStateTransitions()
        {
            // Arrange
            var config = new CircuitBreakerConfig
            {
                FailureThreshold = 1,
                Timeout = TimeSpan.FromMilliseconds(100),
                SuccessThreshold = 1
            };
            var manager = new CircuitBreakerManager(config);
            var host1 = "host1-state.databricks.com";
            var host2 = "host2-state.databricks.com";
            var host3 = "host3-state.databricks.com";

            var cb1 = manager.GetCircuitBreaker(host1);
            var cb2 = manager.GetCircuitBreaker(host2);
            var cb3 = manager.GetCircuitBreaker(host3);

            // Act - Put cb1 in Open state
            try { await cb1.ExecuteAsync(() => throw new Exception("Failure")); } catch { }
            Assert.Equal(CircuitBreakerState.Open, cb1.State);

            // Act - Put cb2 in HalfOpen state (Open then wait for timeout)
            try { await cb2.ExecuteAsync(() => throw new Exception("Failure")); } catch { }
            await Task.Delay(150);
            // Transition to HalfOpen happens on next execute attempt
            await cb2.ExecuteAsync(async () => await Task.CompletedTask);

            // cb3 stays Closed (no failures)

            // Assert - Each host has independent state
            Assert.Equal(CircuitBreakerState.Open, cb1.State);
            // cb2 is either HalfOpen or Closed depending on SuccessThreshold
            Assert.True(cb2.State == CircuitBreakerState.HalfOpen || cb2.State == CircuitBreakerState.Closed);
            Assert.Equal(CircuitBreakerState.Closed, cb3.State);
        }

        #endregion

        #region HasCircuitBreaker Tests

        [Fact]
        public void CircuitBreakerManager_HasCircuitBreaker_ExistingHost_ReturnsTrue()
        {
            // Arrange
            var manager = new CircuitBreakerManager();
            var host = "existing-host.databricks.com";
            manager.GetCircuitBreaker(host);

            // Act
            var exists = manager.HasCircuitBreaker(host);

            // Assert
            Assert.True(exists);
        }

        [Fact]
        public void CircuitBreakerManager_HasCircuitBreaker_NonExistingHost_ReturnsFalse()
        {
            // Arrange
            var manager = new CircuitBreakerManager();

            // Act
            var exists = manager.HasCircuitBreaker("non-existing.databricks.com");

            // Assert
            Assert.False(exists);
        }

        [Fact]
        public void CircuitBreakerManager_HasCircuitBreaker_NullHost_ReturnsFalse()
        {
            // Arrange
            var manager = new CircuitBreakerManager();

            // Act
            var exists = manager.HasCircuitBreaker(null!);

            // Assert
            Assert.False(exists);
        }

        [Fact]
        public void CircuitBreakerManager_HasCircuitBreaker_EmptyHost_ReturnsFalse()
        {
            // Arrange
            var manager = new CircuitBreakerManager();

            // Act
            var exists = manager.HasCircuitBreaker(string.Empty);

            // Assert
            Assert.False(exists);
        }

        #endregion

        #region TryGetCircuitBreaker Tests

        [Fact]
        public void CircuitBreakerManager_TryGetCircuitBreaker_ExistingHost_ReturnsTrue()
        {
            // Arrange
            var manager = new CircuitBreakerManager();
            var host = "try-get-host.databricks.com";
            var originalCircuitBreaker = manager.GetCircuitBreaker(host);

            // Act
            var found = manager.TryGetCircuitBreaker(host, out var circuitBreaker);

            // Assert
            Assert.True(found);
            Assert.Same(originalCircuitBreaker, circuitBreaker);
        }

        [Fact]
        public void CircuitBreakerManager_TryGetCircuitBreaker_NonExistingHost_ReturnsFalse()
        {
            // Arrange
            var manager = new CircuitBreakerManager();

            // Act
            var found = manager.TryGetCircuitBreaker("non-existing.databricks.com", out var circuitBreaker);

            // Assert
            Assert.False(found);
            Assert.Null(circuitBreaker);
        }

        [Fact]
        public void CircuitBreakerManager_TryGetCircuitBreaker_NullHost_ReturnsFalse()
        {
            // Arrange
            var manager = new CircuitBreakerManager();

            // Act
            var found = manager.TryGetCircuitBreaker(null!, out var circuitBreaker);

            // Assert
            Assert.False(found);
            Assert.Null(circuitBreaker);
        }

        #endregion

        #region RemoveCircuitBreaker Tests

        [Fact]
        public void CircuitBreakerManager_RemoveCircuitBreaker_ExistingHost_ReturnsTrue()
        {
            // Arrange
            var manager = new CircuitBreakerManager();
            var host = "remove-host.databricks.com";
            manager.GetCircuitBreaker(host);

            // Act
            var removed = manager.RemoveCircuitBreaker(host);

            // Assert
            Assert.True(removed);
            Assert.False(manager.HasCircuitBreaker(host));
            Assert.Equal(0, manager.CircuitBreakerCount);
        }

        [Fact]
        public void CircuitBreakerManager_RemoveCircuitBreaker_NonExistingHost_ReturnsFalse()
        {
            // Arrange
            var manager = new CircuitBreakerManager();

            // Act
            var removed = manager.RemoveCircuitBreaker("non-existing.databricks.com");

            // Assert
            Assert.False(removed);
        }

        [Fact]
        public void CircuitBreakerManager_RemoveCircuitBreaker_NullHost_ReturnsFalse()
        {
            // Arrange
            var manager = new CircuitBreakerManager();

            // Act
            var removed = manager.RemoveCircuitBreaker(null!);

            // Assert
            Assert.False(removed);
        }

        [Fact]
        public void CircuitBreakerManager_RemoveCircuitBreaker_AfterRemoval_GetCreatesNewBreaker()
        {
            // Arrange
            var manager = new CircuitBreakerManager();
            var host = "remove-recreate-host.databricks.com";
            var originalCircuitBreaker = manager.GetCircuitBreaker(host);
            manager.RemoveCircuitBreaker(host);

            // Act
            var newCircuitBreaker = manager.GetCircuitBreaker(host);

            // Assert
            Assert.NotSame(originalCircuitBreaker, newCircuitBreaker);
        }

        #endregion

        #region Clear Tests

        [Fact]
        public void CircuitBreakerManager_Clear_RemovesAllBreakers()
        {
            // Arrange
            var manager = new CircuitBreakerManager();
            manager.GetCircuitBreaker("host1.databricks.com");
            manager.GetCircuitBreaker("host2.databricks.com");
            manager.GetCircuitBreaker("host3.databricks.com");

            // Act
            manager.Clear();

            // Assert
            Assert.Equal(0, manager.CircuitBreakerCount);
            Assert.False(manager.HasCircuitBreaker("host1.databricks.com"));
            Assert.False(manager.HasCircuitBreaker("host2.databricks.com"));
            Assert.False(manager.HasCircuitBreaker("host3.databricks.com"));
        }

        #endregion

        #region Constructor Tests

        [Fact]
        public void CircuitBreakerManager_Constructor_NullConfig_ThrowsException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new CircuitBreakerManager(null!));
        }

        [Fact]
        public void CircuitBreakerManager_DefaultConstructor_UsesDefaultConfig()
        {
            // Arrange
            var manager = new CircuitBreakerManager();
            var host = "default-config-host.databricks.com";

            // Act
            var circuitBreaker = manager.GetCircuitBreaker(host);

            // Assert - Default config values
            Assert.Equal(5, circuitBreaker.Config.FailureThreshold);
            Assert.Equal(TimeSpan.FromMinutes(1), circuitBreaker.Config.Timeout);
            Assert.Equal(2, circuitBreaker.Config.SuccessThreshold);
        }

        #endregion
    }
}
