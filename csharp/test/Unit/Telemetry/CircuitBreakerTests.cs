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
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for CircuitBreaker to ensure correct three-state behavior.
    /// </summary>
    public class CircuitBreakerTests
    {
        private readonly CircuitBreakerConfig _config = new CircuitBreakerConfig
        {
            FailureThreshold = 5,
            Timeout = TimeSpan.FromMilliseconds(100), // Short timeout for testing
            SuccessThreshold = 2
        };

        [Fact]
        public void CircuitBreaker_Constructor_InitializesCorrectly()
        {
            // Arrange & Act
            var breaker = new CircuitBreaker("test-host.databricks.com", _config);

            // Assert
            Assert.NotNull(breaker);
            Assert.Equal(CircuitBreakerState.Closed, breaker.State);
            Assert.Equal(_config, breaker.Config);
        }

        [Fact]
        public void CircuitBreaker_Constructor_NullHost_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new CircuitBreaker(null!, _config));
        }

        [Fact]
        public void CircuitBreaker_Constructor_NullConfig_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new CircuitBreaker("test-host", null!));
        }

        [Fact]
        public async Task CircuitBreaker_Closed_SuccessfulExecution_StaysClosed()
        {
            // Arrange
            var breaker = new CircuitBreaker("test-host", _config);
            var executed = false;

            // Act
            await breaker.ExecuteAsync(async () =>
            {
                executed = true;
                await Task.CompletedTask;
            });

            // Assert
            Assert.True(executed);
            Assert.Equal(CircuitBreakerState.Closed, breaker.State);
        }

        [Fact]
        public async Task CircuitBreaker_Closed_FailuresBelowThreshold_StaysClosed()
        {
            // Arrange
            var breaker = new CircuitBreaker("test-host", _config);

            // Act - Fail 4 times (below threshold of 5)
            for (int i = 0; i < 4; i++)
            {
                try
                {
                    await breaker.ExecuteAsync(async () =>
                    {
                        await Task.CompletedTask;
                        throw new Exception($"Test failure {i}");
                    });
                }
                catch (Exception)
                {
                    // Expected
                }
            }

            // Assert
            Assert.Equal(CircuitBreakerState.Closed, breaker.State);
        }

        [Fact]
        public async Task CircuitBreaker_Closed_FailuresAtThreshold_TransitionsToOpen()
        {
            // Arrange
            var breaker = new CircuitBreaker("test-host", _config);

            // Act - Fail exactly 5 times (at threshold)
            for (int i = 0; i < 5; i++)
            {
                try
                {
                    await breaker.ExecuteAsync(async () =>
                    {
                        await Task.CompletedTask;
                        throw new Exception($"Test failure {i}");
                    });
                }
                catch (Exception)
                {
                    // Expected
                }
            }

            // Assert
            Assert.Equal(CircuitBreakerState.Open, breaker.State);
        }

        [Fact]
        public async Task CircuitBreaker_Open_RejectsRequests_ThrowsException()
        {
            // Arrange
            var breaker = new CircuitBreaker("test-host", _config);

            // Fail enough times to open circuit
            for (int i = 0; i < 5; i++)
            {
                try
                {
                    await breaker.ExecuteAsync(async () =>
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

            // Act & Assert - Next request should be rejected
            var exception = await Assert.ThrowsAsync<CircuitBreakerOpenException>(async () =>
            {
                await breaker.ExecuteAsync(async () =>
                {
                    await Task.CompletedTask;
                });
            });

            Assert.Contains("Circuit breaker is OPEN", exception.Message);
            Assert.Contains("test-host", exception.Message);
        }

        [Fact]
        public async Task CircuitBreaker_Open_AfterTimeout_TransitionsToHalfOpen()
        {
            // Arrange
            var breaker = new CircuitBreaker("test-host", _config);

            // Open the circuit
            for (int i = 0; i < 5; i++)
            {
                try
                {
                    await breaker.ExecuteAsync(async () =>
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

            Assert.Equal(CircuitBreakerState.Open, breaker.State);

            // Wait for timeout (100ms)
            await Task.Delay(150);

            // Act - Try execution after timeout
            var executed = false;
            await breaker.ExecuteAsync(async () =>
            {
                executed = true;
                await Task.CompletedTask;
            });

            // Assert - Should have transitioned to HalfOpen and executed
            Assert.True(executed);
        }

        [Fact]
        public async Task CircuitBreaker_HalfOpen_Success_TransitionsToClosed()
        {
            // Arrange
            var breaker = new CircuitBreaker("test-host", _config);

            // Open the circuit
            for (int i = 0; i < 5; i++)
            {
                try
                {
                    await breaker.ExecuteAsync(async () =>
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

            // Wait for timeout to transition to HalfOpen
            await Task.Delay(150);

            // Act - Execute successfully enough times to close circuit (success threshold = 2)
            for (int i = 0; i < 2; i++)
            {
                await breaker.ExecuteAsync(async () =>
                {
                    await Task.CompletedTask;
                });
            }

            // Assert
            Assert.Equal(CircuitBreakerState.Closed, breaker.State);
        }

        [Fact]
        public async Task CircuitBreaker_HalfOpen_Failure_TransitionsToOpen()
        {
            // Arrange
            var breaker = new CircuitBreaker("test-host", _config);

            // Open the circuit
            for (int i = 0; i < 5; i++)
            {
                try
                {
                    await breaker.ExecuteAsync(async () =>
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

            // Wait for timeout to transition to HalfOpen
            await Task.Delay(150);

            // Act - Fail in HalfOpen state
            try
            {
                await breaker.ExecuteAsync(async () =>
                {
                    await Task.CompletedTask;
                    throw new Exception("Test failure in HalfOpen");
                });
            }
            catch (Exception)
            {
                // Expected
            }

            // Assert - Should transition back to Open
            Assert.Equal(CircuitBreakerState.Open, breaker.State);
        }

        [Fact]
        public async Task CircuitBreaker_SuccessAfterFailure_ResetsFailureCount()
        {
            // Arrange
            var breaker = new CircuitBreaker("test-host", _config);

            // Fail a few times (below threshold)
            for (int i = 0; i < 3; i++)
            {
                try
                {
                    await breaker.ExecuteAsync(async () =>
                    {
                        await Task.CompletedTask;
                        throw new Exception($"Test failure {i}");
                    });
                }
                catch (Exception)
                {
                    // Expected
                }
            }

            // Succeed once (should reset failure count)
            await breaker.ExecuteAsync(async () =>
            {
                await Task.CompletedTask;
            });

            // Act - Now fail 4 more times (should stay closed since count was reset)
            for (int i = 0; i < 4; i++)
            {
                try
                {
                    await breaker.ExecuteAsync(async () =>
                    {
                        await Task.CompletedTask;
                        throw new Exception($"Test failure after reset {i}");
                    });
                }
                catch (Exception)
                {
                    // Expected
                }
            }

            // Assert - Should still be closed (4 failures < threshold of 5)
            Assert.Equal(CircuitBreakerState.Closed, breaker.State);
        }

        [Fact]
        public async Task CircuitBreaker_Reset_TransitionsToClosed()
        {
            // Arrange
            var breaker = new CircuitBreaker("test-host", _config);

            // Open the circuit
            for (int i = 0; i < 5; i++)
            {
                try
                {
                    await breaker.ExecuteAsync(async () =>
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

            Assert.Equal(CircuitBreakerState.Open, breaker.State);

            // Act
            breaker.Reset();

            // Assert
            Assert.Equal(CircuitBreakerState.Closed, breaker.State);
        }

        [Fact]
        public async Task CircuitBreaker_ConfigFromTelemetryConfiguration_UsesCorrectValues()
        {
            // Arrange
            var telemetryConfig = new TelemetryConfiguration
            {
                CircuitBreakerThreshold = 3,
                CircuitBreakerTimeout = TimeSpan.FromMilliseconds(50)
            };
            var config = CircuitBreakerConfig.FromTelemetryConfiguration(telemetryConfig);
            var breaker = new CircuitBreaker("test-host", config);

            // Act - Fail 3 times (custom threshold)
            for (int i = 0; i < 3; i++)
            {
                try
                {
                    await breaker.ExecuteAsync(async () =>
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

            // Assert
            Assert.Equal(CircuitBreakerState.Open, breaker.State);
            Assert.Equal(3, config.FailureThreshold);
            Assert.Equal(TimeSpan.FromMilliseconds(50), config.Timeout);
        }
    }
}
