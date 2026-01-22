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
    /// Tests for CircuitBreaker class.
    /// </summary>
    public class CircuitBreakerTests
    {
        #region Closed State - Successful Execution Tests

        [Fact]
        public async Task CircuitBreaker_Closed_SuccessfulExecution_StaysClosed()
        {
            // Arrange
            var circuitBreaker = new CircuitBreaker();
            var executionCount = 0;

            // Act
            await circuitBreaker.ExecuteAsync(async () =>
            {
                executionCount++;
                await Task.CompletedTask;
            });

            // Assert
            Assert.Equal(CircuitBreakerState.Closed, circuitBreaker.State);
            Assert.Equal(1, executionCount);
            Assert.Equal(0, circuitBreaker.ConsecutiveFailures);
        }

        [Fact]
        public async Task CircuitBreaker_Closed_MultipleSuccesses_StaysClosed()
        {
            // Arrange
            var circuitBreaker = new CircuitBreaker();
            var executionCount = 0;

            // Act
            for (int i = 0; i < 10; i++)
            {
                await circuitBreaker.ExecuteAsync(async () =>
                {
                    executionCount++;
                    await Task.CompletedTask;
                });
            }

            // Assert
            Assert.Equal(CircuitBreakerState.Closed, circuitBreaker.State);
            Assert.Equal(10, executionCount);
            Assert.Equal(0, circuitBreaker.ConsecutiveFailures);
        }

        [Fact]
        public async Task CircuitBreaker_Closed_SuccessResetsFailureCounter()
        {
            // Arrange
            var config = new CircuitBreakerConfig { FailureThreshold = 5 };
            var circuitBreaker = new CircuitBreaker(config);

            // Act - Cause some failures then succeed
            for (int i = 0; i < 3; i++)
            {
                try
                {
                    await circuitBreaker.ExecuteAsync(() => throw new Exception("Simulated failure"));
                }
                catch { }
            }

            Assert.Equal(3, circuitBreaker.ConsecutiveFailures);

            // Now succeed
            await circuitBreaker.ExecuteAsync(async () => await Task.CompletedTask);

            // Assert
            Assert.Equal(CircuitBreakerState.Closed, circuitBreaker.State);
            Assert.Equal(0, circuitBreaker.ConsecutiveFailures);
        }

        #endregion

        #region Closed State - Failure Tests

        [Fact]
        public async Task CircuitBreaker_Closed_FailuresBelowThreshold_StaysClosed()
        {
            // Arrange
            var config = new CircuitBreakerConfig { FailureThreshold = 5 };
            var circuitBreaker = new CircuitBreaker(config);

            // Act - Cause 4 failures (threshold is 5)
            for (int i = 0; i < 4; i++)
            {
                try
                {
                    await circuitBreaker.ExecuteAsync(() => throw new Exception("Simulated failure"));
                }
                catch { }
            }

            // Assert
            Assert.Equal(CircuitBreakerState.Closed, circuitBreaker.State);
            Assert.Equal(4, circuitBreaker.ConsecutiveFailures);
        }

        [Fact]
        public async Task CircuitBreaker_Closed_FailuresAtThreshold_TransitionsToOpen()
        {
            // Arrange
            var config = new CircuitBreakerConfig { FailureThreshold = 5 };
            var circuitBreaker = new CircuitBreaker(config);

            // Act - Cause exactly 5 failures (threshold)
            for (int i = 0; i < 5; i++)
            {
                try
                {
                    await circuitBreaker.ExecuteAsync(() => throw new Exception("Simulated failure"));
                }
                catch { }
            }

            // Assert
            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);
        }

        [Fact]
        public async Task CircuitBreaker_Closed_FailuresAboveThreshold_StaysOpen()
        {
            // Arrange
            var config = new CircuitBreakerConfig { FailureThreshold = 5 };
            var circuitBreaker = new CircuitBreaker(config);

            // Act - Cause 10 failures
            for (int i = 0; i < 10; i++)
            {
                try
                {
                    await circuitBreaker.ExecuteAsync(() => throw new Exception("Simulated failure"));
                }
                catch { }
            }

            // Assert - Should be open after threshold reached
            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);
        }

        [Fact]
        public async Task CircuitBreaker_Closed_Failure_PropagatesException()
        {
            // Arrange
            var circuitBreaker = new CircuitBreaker();
            var expectedException = new InvalidOperationException("Test exception");

            // Act & Assert
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => circuitBreaker.ExecuteAsync(() => throw expectedException));

            Assert.Same(expectedException, ex);
        }

        #endregion

        #region Open State Tests

        [Fact]
        public async Task CircuitBreaker_Open_RejectsRequests_ThrowsException()
        {
            // Arrange
            var config = new CircuitBreakerConfig { FailureThreshold = 1, Timeout = TimeSpan.FromMinutes(5) };
            var circuitBreaker = new CircuitBreaker(config);

            // Cause the circuit to open
            try
            {
                await circuitBreaker.ExecuteAsync(() => throw new Exception("Simulated failure"));
            }
            catch { }

            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);

            // Act & Assert
            await Assert.ThrowsAsync<CircuitBreakerOpenException>(
                () => circuitBreaker.ExecuteAsync(async () => await Task.CompletedTask));
        }

        [Fact]
        public async Task CircuitBreaker_Open_RejectsRequests_DoesNotExecuteAction()
        {
            // Arrange
            var config = new CircuitBreakerConfig { FailureThreshold = 1, Timeout = TimeSpan.FromMinutes(5) };
            var circuitBreaker = new CircuitBreaker(config);
            var wasExecuted = false;

            // Cause the circuit to open
            try
            {
                await circuitBreaker.ExecuteAsync(() => throw new Exception("Simulated failure"));
            }
            catch { }

            // Act
            try
            {
                await circuitBreaker.ExecuteAsync(async () =>
                {
                    wasExecuted = true;
                    await Task.CompletedTask;
                });
            }
            catch (CircuitBreakerOpenException) { }

            // Assert
            Assert.False(wasExecuted);
        }

        [Fact]
        public async Task CircuitBreaker_Open_ExceptionContainsOpenedAt()
        {
            // Arrange
            var config = new CircuitBreakerConfig { FailureThreshold = 1, Timeout = TimeSpan.FromMinutes(1) };
            var circuitBreaker = new CircuitBreaker(config);
            var beforeOpen = DateTime.UtcNow;

            // Cause the circuit to open
            try
            {
                await circuitBreaker.ExecuteAsync(() => throw new Exception("Simulated failure"));
            }
            catch { }

            var afterOpen = DateTime.UtcNow;

            // Act
            var ex = await Assert.ThrowsAsync<CircuitBreakerOpenException>(
                () => circuitBreaker.ExecuteAsync(async () => await Task.CompletedTask));

            // Assert
            Assert.True(ex.OpenedAt >= beforeOpen);
            Assert.True(ex.OpenedAt <= afterOpen);
            Assert.Equal(config.Timeout, ex.Timeout);
        }

        [Fact]
        public async Task CircuitBreaker_Open_AfterTimeout_TransitionsToHalfOpen()
        {
            // Arrange
            var config = new CircuitBreakerConfig
            {
                FailureThreshold = 1,
                Timeout = TimeSpan.FromMilliseconds(100)
            };
            var circuitBreaker = new CircuitBreaker(config);

            // Cause the circuit to open
            try
            {
                await circuitBreaker.ExecuteAsync(() => throw new Exception("Simulated failure"));
            }
            catch { }

            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);

            // Wait for timeout to expire
            await Task.Delay(150);

            // Act - Execute should succeed and transition to HalfOpen
            await circuitBreaker.ExecuteAsync(async () => await Task.CompletedTask);

            // Assert - Should transition through HalfOpen and possibly to Closed
            // The state depends on the success threshold
            Assert.True(circuitBreaker.State == CircuitBreakerState.HalfOpen ||
                       circuitBreaker.State == CircuitBreakerState.Closed);
        }

        #endregion

        #region HalfOpen State Tests

        [Fact]
        public async Task CircuitBreaker_HalfOpen_Success_TransitionsToClosed()
        {
            // Arrange
            var config = new CircuitBreakerConfig
            {
                FailureThreshold = 1,
                Timeout = TimeSpan.FromMilliseconds(100),
                SuccessThreshold = 2
            };
            var circuitBreaker = new CircuitBreaker(config);

            // Cause the circuit to open
            try
            {
                await circuitBreaker.ExecuteAsync(() => throw new Exception("Simulated failure"));
            }
            catch { }

            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);

            // Wait for timeout
            await Task.Delay(150);

            // Act - Execute successes to close the circuit
            await circuitBreaker.ExecuteAsync(async () => await Task.CompletedTask);

            // Should be in HalfOpen after first success (success count = 1)
            // or Closed if SuccessThreshold = 1
            if (config.SuccessThreshold > 1)
            {
                Assert.Equal(CircuitBreakerState.HalfOpen, circuitBreaker.State);
            }

            await circuitBreaker.ExecuteAsync(async () => await Task.CompletedTask);

            // Assert - Should be closed after reaching success threshold
            Assert.Equal(CircuitBreakerState.Closed, circuitBreaker.State);
        }

        [Fact]
        public async Task CircuitBreaker_HalfOpen_Failure_TransitionsToOpen()
        {
            // Arrange
            var config = new CircuitBreakerConfig
            {
                FailureThreshold = 1,
                Timeout = TimeSpan.FromMilliseconds(100),
                SuccessThreshold = 3
            };
            var circuitBreaker = new CircuitBreaker(config);

            // Cause the circuit to open
            try
            {
                await circuitBreaker.ExecuteAsync(() => throw new Exception("Simulated failure"));
            }
            catch { }

            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);

            // Wait for timeout to transition to HalfOpen
            await Task.Delay(150);

            // First success to enter HalfOpen
            await circuitBreaker.ExecuteAsync(async () => await Task.CompletedTask);
            Assert.Equal(CircuitBreakerState.HalfOpen, circuitBreaker.State);

            // Act - Now fail in HalfOpen state
            try
            {
                await circuitBreaker.ExecuteAsync(() => throw new Exception("Failure in HalfOpen"));
            }
            catch { }

            // Assert - Should transition back to Open
            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);
        }

        [Fact]
        public async Task CircuitBreaker_HalfOpen_SuccessBelowThreshold_StaysHalfOpen()
        {
            // Arrange
            var config = new CircuitBreakerConfig
            {
                FailureThreshold = 1,
                Timeout = TimeSpan.FromMilliseconds(100),
                SuccessThreshold = 5
            };
            var circuitBreaker = new CircuitBreaker(config);

            // Cause the circuit to open
            try
            {
                await circuitBreaker.ExecuteAsync(() => throw new Exception("Simulated failure"));
            }
            catch { }

            // Wait for timeout
            await Task.Delay(150);

            // Act - Execute 3 successes (below threshold of 5)
            for (int i = 0; i < 3; i++)
            {
                await circuitBreaker.ExecuteAsync(async () => await Task.CompletedTask);
            }

            // Assert - Should still be in HalfOpen
            Assert.Equal(CircuitBreakerState.HalfOpen, circuitBreaker.State);
            Assert.Equal(3, circuitBreaker.ConsecutiveSuccesses);
        }

        #endregion

        #region Configuration Tests

        [Fact]
        public void CircuitBreakerConfig_DefaultValues_AreCorrect()
        {
            // Arrange & Act
            var config = new CircuitBreakerConfig();

            // Assert
            Assert.Equal(5, config.FailureThreshold);
            Assert.Equal(TimeSpan.FromMinutes(1), config.Timeout);
            Assert.Equal(2, config.SuccessThreshold);
        }

        [Fact]
        public void CircuitBreakerConfig_FromTelemetryConfiguration_MapsCorrectly()
        {
            // Arrange
            var telemetryConfig = new TelemetryConfiguration
            {
                CircuitBreakerThreshold = 10,
                CircuitBreakerTimeout = TimeSpan.FromSeconds(30)
            };

            // Act
            var config = CircuitBreakerConfig.FromTelemetryConfiguration(telemetryConfig);

            // Assert
            Assert.Equal(10, config.FailureThreshold);
            Assert.Equal(TimeSpan.FromSeconds(30), config.Timeout);
            Assert.Equal(2, config.SuccessThreshold); // Default value
        }

        [Fact]
        public void CircuitBreakerConfig_FromTelemetryConfiguration_NullArgument_ThrowsException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                CircuitBreakerConfig.FromTelemetryConfiguration(null!));
        }

        [Fact]
        public void CircuitBreaker_Constructor_NullConfig_ThrowsException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new CircuitBreaker(null!));
        }

        #endregion

        #region ExecuteAsync Generic Tests

        [Fact]
        public async Task CircuitBreaker_ExecuteAsyncGeneric_ReturnsResult()
        {
            // Arrange
            var circuitBreaker = new CircuitBreaker();
            var expectedResult = 42;

            // Act
            var result = await circuitBreaker.ExecuteAsync(async () =>
            {
                await Task.CompletedTask;
                return expectedResult;
            });

            // Assert
            Assert.Equal(expectedResult, result);
        }

        [Fact]
        public async Task CircuitBreaker_ExecuteAsyncGeneric_Open_ThrowsException()
        {
            // Arrange
            var config = new CircuitBreakerConfig { FailureThreshold = 1 };
            var circuitBreaker = new CircuitBreaker(config);

            // Cause the circuit to open
            try
            {
                await circuitBreaker.ExecuteAsync(() => throw new Exception("Failure"));
            }
            catch { }

            // Act & Assert
            await Assert.ThrowsAsync<CircuitBreakerOpenException>(
                () => circuitBreaker.ExecuteAsync(async () =>
                {
                    await Task.CompletedTask;
                    return 42;
                }));
        }

        #endregion

        #region Null Action Tests

        [Fact]
        public async Task CircuitBreaker_ExecuteAsync_NullAction_ThrowsException()
        {
            // Arrange
            var circuitBreaker = new CircuitBreaker();

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentNullException>(
                () => circuitBreaker.ExecuteAsync((Func<Task>)null!));
        }

        [Fact]
        public async Task CircuitBreaker_ExecuteAsyncGeneric_NullAction_ThrowsException()
        {
            // Arrange
            var circuitBreaker = new CircuitBreaker();

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentNullException>(
                () => circuitBreaker.ExecuteAsync<int>((Func<Task<int>>)null!));
        }

        #endregion

        #region Reset Tests

        [Fact]
        public async Task CircuitBreaker_Reset_ClosesCircuit()
        {
            // Arrange
            var config = new CircuitBreakerConfig { FailureThreshold = 1 };
            var circuitBreaker = new CircuitBreaker(config);

            // Cause the circuit to open
            try
            {
                await circuitBreaker.ExecuteAsync(() => throw new Exception("Failure"));
            }
            catch { }

            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);

            // Act
            circuitBreaker.Reset();

            // Assert
            Assert.Equal(CircuitBreakerState.Closed, circuitBreaker.State);
            Assert.Equal(0, circuitBreaker.ConsecutiveFailures);
            Assert.Equal(0, circuitBreaker.ConsecutiveSuccesses);
        }

        #endregion

        #region Thread Safety Tests

        [Fact]
        public async Task CircuitBreaker_ConcurrentSuccesses_ThreadSafe()
        {
            // Arrange
            var circuitBreaker = new CircuitBreaker();
            var executionCount = 0;
            var tasks = new Task[100];

            // Act
            for (int i = 0; i < 100; i++)
            {
                tasks[i] = circuitBreaker.ExecuteAsync(async () =>
                {
                    Interlocked.Increment(ref executionCount);
                    await Task.Delay(1);
                });
            }

            await Task.WhenAll(tasks);

            // Assert
            Assert.Equal(100, executionCount);
            Assert.Equal(CircuitBreakerState.Closed, circuitBreaker.State);
        }

        [Fact]
        public async Task CircuitBreaker_ConcurrentFailures_ThreadSafe()
        {
            // Arrange
            var config = new CircuitBreakerConfig { FailureThreshold = 10 };
            var circuitBreaker = new CircuitBreaker(config);
            var failureCount = 0;
            var tasks = new Task[100];

            // Act
            for (int i = 0; i < 100; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    try
                    {
                        await circuitBreaker.ExecuteAsync(async () =>
                        {
                            Interlocked.Increment(ref failureCount);
                            await Task.Delay(1);
                            throw new Exception("Simulated failure");
                        });
                    }
                    catch (CircuitBreakerOpenException) { }
                    catch { }
                });
            }

            await Task.WhenAll(tasks);

            // Assert - Circuit should eventually open
            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);
        }

        #endregion

        #region CircuitBreakerOpenException Tests

        [Fact]
        public void CircuitBreakerOpenException_DefaultConstructor_HasDefaultValues()
        {
            // Act
            var ex = new CircuitBreakerOpenException();

            // Assert
            Assert.Equal("Circuit breaker is open and rejecting requests.", ex.Message);
            Assert.Equal(TimeSpan.FromMinutes(1), ex.Timeout);
        }

        [Fact]
        public void CircuitBreakerOpenException_MessageConstructor_SetsMessage()
        {
            // Arrange
            var message = "Custom message";

            // Act
            var ex = new CircuitBreakerOpenException(message);

            // Assert
            Assert.Equal(message, ex.Message);
        }

        [Fact]
        public void CircuitBreakerOpenException_StateConstructor_SetsStateInfo()
        {
            // Arrange
            var openedAt = new DateTime(2025, 1, 22, 12, 0, 0, DateTimeKind.Utc);
            var timeout = TimeSpan.FromSeconds(30);

            // Act
            var ex = new CircuitBreakerOpenException(openedAt, timeout);

            // Assert
            Assert.Equal(openedAt, ex.OpenedAt);
            Assert.Equal(timeout, ex.Timeout);
            Assert.Equal(openedAt + timeout, ex.EstimatedRecoveryTime);
        }

        [Fact]
        public void CircuitBreakerOpenException_InnerExceptionConstructor_SetsInnerException()
        {
            // Arrange
            var innerException = new Exception("Inner");

            // Act
            var ex = new CircuitBreakerOpenException("Message", innerException);

            // Assert
            Assert.Same(innerException, ex.InnerException);
        }

        #endregion
    }
}
