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
using Polly.CircuitBreaker;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for CircuitBreaker class (Polly-based implementation).
    /// </summary>
    public class CircuitBreakerTests
    {
        /// <summary>
        /// Creates a default CircuitBreakerConfig for testing.
        /// </summary>
        private static CircuitBreakerConfig CreateDefaultConfig() => new CircuitBreakerConfig();

        /// <summary>
        /// Creates a CircuitBreakerConfig with specified failure threshold.
        /// </summary>
        private static CircuitBreakerConfig CreateConfig(int failureThreshold) => new CircuitBreakerConfig
        {
            FailureThreshold = failureThreshold
        };

        /// <summary>
        /// Creates a CircuitBreakerConfig with specified failure threshold and timeout.
        /// </summary>
        private static CircuitBreakerConfig CreateConfig(int failureThreshold, TimeSpan timeout) => new CircuitBreakerConfig
        {
            FailureThreshold = failureThreshold,
            Timeout = timeout
        };

        #region Closed State - Successful Execution Tests

        [Fact]
        public async Task CircuitBreaker_Closed_SuccessfulExecution_StaysClosed()
        {
            // Arrange
            var circuitBreaker = new CircuitBreaker(CreateDefaultConfig());
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
        }

        [Fact]
        public async Task CircuitBreaker_Closed_MultipleSuccesses_StaysClosed()
        {
            // Arrange
            var circuitBreaker = new CircuitBreaker(CreateDefaultConfig());
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
        }

        [Fact]
        public async Task CircuitBreaker_Closed_SuccessAfterFailures_StaysClosed()
        {
            // Arrange
            var circuitBreaker = new CircuitBreaker(CreateConfig(failureThreshold: 5));

            // Act - Cause some failures then succeed (not enough to trip the breaker)
            for (int i = 0; i < 3; i++)
            {
                try
                {
                    await circuitBreaker.ExecuteAsync(() => throw new Exception("Simulated failure"));
                }
                catch { }
            }

            // Now succeed
            await circuitBreaker.ExecuteAsync(async () => await Task.CompletedTask);

            // Assert - Should still be closed since we didn't reach threshold
            Assert.Equal(CircuitBreakerState.Closed, circuitBreaker.State);
        }

        #endregion

        #region Closed State - Failure Tests

        [Fact]
        public async Task CircuitBreaker_Closed_FailuresBelowThreshold_StaysClosed()
        {
            // Arrange
            var circuitBreaker = new CircuitBreaker(CreateConfig(failureThreshold: 5));

            // Act - Cause 4 failures (threshold is 5)
            for (int i = 0; i < 4; i++)
            {
                try
                {
                    await circuitBreaker.ExecuteAsync(() => throw new Exception("Simulated failure"));
                }
                catch { }
            }

            // Assert - Circuit should still be closed
            Assert.Equal(CircuitBreakerState.Closed, circuitBreaker.State);
        }

        [Fact]
        public async Task CircuitBreaker_Closed_FailuresAtThreshold_TransitionsToOpen()
        {
            // Arrange
            var circuitBreaker = new CircuitBreaker(CreateConfig(failureThreshold: 5));

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
            var circuitBreaker = new CircuitBreaker(CreateConfig(failureThreshold: 5));

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
            var circuitBreaker = new CircuitBreaker(CreateDefaultConfig());
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
            // Arrange - Polly requires MinimumThroughput >= 2
            var circuitBreaker = new CircuitBreaker(CreateConfig(failureThreshold: 2, timeout: TimeSpan.FromMinutes(5)));

            // Cause the circuit to open
            for (int i = 0; i < 2; i++)
            {
                try
                {
                    await circuitBreaker.ExecuteAsync(() => throw new Exception("Simulated failure"));
                }
                catch { }
            }

            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);

            // Act & Assert
            await Assert.ThrowsAsync<BrokenCircuitException>(
                () => circuitBreaker.ExecuteAsync(async () => await Task.CompletedTask));
        }

        [Fact]
        public async Task CircuitBreaker_Open_RejectsRequests_DoesNotExecuteAction()
        {
            // Arrange - Polly requires MinimumThroughput >= 2
            var circuitBreaker = new CircuitBreaker(CreateConfig(failureThreshold: 2, timeout: TimeSpan.FromMinutes(5)));
            var wasExecuted = false;

            // Cause the circuit to open
            for (int i = 0; i < 2; i++)
            {
                try
                {
                    await circuitBreaker.ExecuteAsync(() => throw new Exception("Simulated failure"));
                }
                catch { }
            }

            // Act
            try
            {
                await circuitBreaker.ExecuteAsync(async () =>
                {
                    wasExecuted = true;
                    await Task.CompletedTask;
                });
            }
            catch (BrokenCircuitException) { }

            // Assert
            Assert.False(wasExecuted);
        }

        [Fact]
        public async Task CircuitBreaker_Open_AfterTimeout_TransitionsToHalfOpen()
        {
            // Arrange - Polly requires MinimumThroughput >= 2 and BreakDuration >= 500ms
            var circuitBreaker = new CircuitBreaker(CreateConfig(failureThreshold: 2, timeout: TimeSpan.FromMilliseconds(500)));

            // Cause the circuit to open
            for (int i = 0; i < 2; i++)
            {
                try
                {
                    await circuitBreaker.ExecuteAsync(() => throw new Exception("Simulated failure"));
                }
                catch { }
            }

            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);

            // Wait for timeout to expire
            await Task.Delay(600);

            // Act - Execute should succeed and transition through HalfOpen
            await circuitBreaker.ExecuteAsync(async () => await Task.CompletedTask);

            // Assert - Should transition through HalfOpen to Closed after success
            // Polly automatically closes after successful execution in HalfOpen
            Assert.True(circuitBreaker.State == CircuitBreakerState.HalfOpen ||
                       circuitBreaker.State == CircuitBreakerState.Closed);
        }

        #endregion

        #region HalfOpen State Tests

        [Fact]
        public async Task CircuitBreaker_HalfOpen_Success_TransitionsToClosed()
        {
            // Arrange - Polly requires MinimumThroughput >= 2 and BreakDuration >= 500ms
            var circuitBreaker = new CircuitBreaker(CreateConfig(failureThreshold: 2, timeout: TimeSpan.FromMilliseconds(500)));

            // Cause the circuit to open
            for (int i = 0; i < 2; i++)
            {
                try
                {
                    await circuitBreaker.ExecuteAsync(() => throw new Exception("Simulated failure"));
                }
                catch { }
            }

            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);

            // Wait for timeout
            await Task.Delay(600);

            // Act - Execute success to close the circuit
            await circuitBreaker.ExecuteAsync(async () => await Task.CompletedTask);

            // Assert - Should be closed after success in HalfOpen (Polly closes on first success)
            Assert.Equal(CircuitBreakerState.Closed, circuitBreaker.State);
        }

        [Fact]
        public async Task CircuitBreaker_HalfOpen_Failure_TransitionsToOpen()
        {
            // Arrange - Polly requires MinimumThroughput >= 2 and BreakDuration >= 500ms
            var circuitBreaker = new CircuitBreaker(CreateConfig(failureThreshold: 2, timeout: TimeSpan.FromMilliseconds(500)));

            // Cause the circuit to open
            for (int i = 0; i < 2; i++)
            {
                try
                {
                    await circuitBreaker.ExecuteAsync(() => throw new Exception("Simulated failure"));
                }
                catch { }
            }

            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);

            // Wait for timeout to transition to HalfOpen
            await Task.Delay(600);

            // Act - Fail in HalfOpen state
            try
            {
                await circuitBreaker.ExecuteAsync(() => throw new Exception("Failure in HalfOpen"));
            }
            catch { }

            // Assert - Should transition back to Open
            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);
        }

        #endregion

        #region ExecuteAsync Generic Tests

        [Fact]
        public async Task CircuitBreaker_ExecuteAsyncGeneric_ReturnsResult()
        {
            // Arrange
            var circuitBreaker = new CircuitBreaker(CreateDefaultConfig());
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
            // Arrange - Polly requires MinimumThroughput >= 2
            var circuitBreaker = new CircuitBreaker(CreateConfig(failureThreshold: 2));

            // Cause the circuit to open
            for (int i = 0; i < 2; i++)
            {
                try
                {
                    await circuitBreaker.ExecuteAsync(() => throw new Exception("Failure"));
                }
                catch { }
            }

            // Act & Assert
            await Assert.ThrowsAsync<BrokenCircuitException>(
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
            var circuitBreaker = new CircuitBreaker(CreateDefaultConfig());

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentNullException>(
                () => circuitBreaker.ExecuteAsync((Func<Task>)null!));
        }

        [Fact]
        public async Task CircuitBreaker_ExecuteAsyncGeneric_NullAction_ThrowsException()
        {
            // Arrange
            var circuitBreaker = new CircuitBreaker(CreateDefaultConfig());

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentNullException>(
                () => circuitBreaker.ExecuteAsync<int>((Func<Task<int>>)null!));
        }

        #endregion

        #region Constructor Tests

        [Fact]
        public void CircuitBreaker_NullConfig_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new CircuitBreaker(null!));
        }

        [Fact]
        public void CircuitBreaker_WithConfig_StoresConfig()
        {
            // Arrange
            var config = new CircuitBreakerConfig
            {
                FailureThreshold = 10,
                Timeout = TimeSpan.FromMinutes(5),
                SuccessThreshold = 3
            };

            // Act
            var circuitBreaker = new CircuitBreaker(config);

            // Assert
            Assert.Same(config, circuitBreaker.Config);
            Assert.Equal(10, circuitBreaker.Config.FailureThreshold);
            Assert.Equal(TimeSpan.FromMinutes(5), circuitBreaker.Config.Timeout);
            Assert.Equal(3, circuitBreaker.Config.SuccessThreshold);
        }

        #endregion

        #region Reset Tests

        [Fact]
        public async Task CircuitBreaker_Reset_ClosesCircuit()
        {
            // Arrange - Polly requires MinimumThroughput >= 2
            var circuitBreaker = new CircuitBreaker(CreateConfig(failureThreshold: 2));

            // Cause the circuit to open
            for (int i = 0; i < 2; i++)
            {
                try
                {
                    await circuitBreaker.ExecuteAsync(() => throw new Exception("Failure"));
                }
                catch { }
            }

            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);

            // Act
            circuitBreaker.Reset();

            // Assert
            Assert.Equal(CircuitBreakerState.Closed, circuitBreaker.State);
        }

        #endregion

        #region Thread Safety Tests

        [Fact]
        public async Task CircuitBreaker_ConcurrentSuccesses_ThreadSafe()
        {
            // Arrange
            var circuitBreaker = new CircuitBreaker(CreateDefaultConfig());
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
            var circuitBreaker = new CircuitBreaker(CreateConfig(failureThreshold: 10));
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
                    catch (BrokenCircuitException) { }
                    catch { }
                });
            }

            await Task.WhenAll(tasks);

            // Assert - Circuit should eventually open
            Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);
        }

        #endregion
    }
}
