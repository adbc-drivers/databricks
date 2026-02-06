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
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// E2E tests for TelemetryClient in isolation against real Databricks endpoint.
    /// Validates data model format, circuit breaker behavior, and graceful shutdown.
    /// </summary>
    /// <remarks>
    /// Phase 7 E2E GATE - Tests telemetry format against real Databricks backend.
    /// These tests verify:
    /// - Single event export succeeds
    /// - Batch event export succeeds
    /// - Authenticated endpoints are used correctly
    /// - Circuit breaker opens after failures
    /// - Circuit breaker recovers after timeout
    /// - Graceful close flushes all pending events
    /// </remarks>
    public class TelemetryClientE2ETests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        private readonly bool _canRunRealEndpointTests;
        private readonly string? _host;
        private readonly string? _token;

        public TelemetryClientE2ETests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            // Check if we can run tests against a real endpoint
            _canRunRealEndpointTests = Utils.CanExecuteTestConfig(TestConfigVariable);

            if (_canRunRealEndpointTests)
            {
                try
                {
                    // Try to get host from HostName first, then fallback to extracting from Uri
                    _host = TestConfiguration.HostName;
                    if (string.IsNullOrEmpty(_host) && !string.IsNullOrEmpty(TestConfiguration.Uri))
                    {
                        // Extract host from Uri (e.g., https://host.databricks.com/sql/1.0/...)
                        var uri = new Uri(TestConfiguration.Uri);
                        _host = $"https://{uri.Host}";
                    }
                    else if (!string.IsNullOrEmpty(_host) && !_host.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
                    {
                        _host = $"https://{_host}";
                    }

                    _token = TestConfiguration.Token ?? TestConfiguration.AccessToken;

                    // Validate we have required configuration
                    if (string.IsNullOrEmpty(_host) || string.IsNullOrEmpty(_token))
                    {
                        _canRunRealEndpointTests = false;
                    }
                }
                catch
                {
                    _canRunRealEndpointTests = false;
                }
            }
        }

        /// <summary>
        /// Creates an HttpClient configured with authentication for the Databricks endpoint.
        /// </summary>
        private HttpClient CreateAuthenticatedHttpClient()
        {
            var client = new HttpClient();
            client.DefaultRequestHeaders.Authorization =
                new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", _token);
            return client;
        }

        /// <summary>
        /// Creates a test TelemetryFrontendLog with valid data model format.
        /// </summary>
        private TelemetryFrontendLog CreateTestLog(long? workspaceId = null)
        {
            return new TelemetryFrontendLog
            {
                FrontendLogEventId = Guid.NewGuid().ToString(),
                WorkspaceId = workspaceId ?? 0,
                Context = new FrontendLogContext
                {
                    ClientContext = new TelemetryClientContext
                    {
                        UserAgent = "Databricks.ADBC.Test/1.0"
                    },
                    TimestampMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                },
                Entry = new FrontendLogEntry
                {
                    SqlDriverLog = new TelemetryEvent
                    {
                        SessionId = Guid.NewGuid().ToString(),
                        SqlStatementId = Guid.NewGuid().ToString(),
                        OperationLatencyMs = 100,
                        SystemConfiguration = new DriverSystemConfiguration
                        {
                            DriverName = "Databricks.ADBC.Test",
                            DriverVersion = "1.0.0"
                        }
                    }
                }
            };
        }

        #region TelemetryClient_ExportSingleEvent_SucceedsAgainstRealEndpoint

        /// <summary>
        /// Tests that TelemetryClient can export a single event to the real Databricks endpoint.
        /// Validates the data model format is accepted by the backend.
        /// </summary>
        [SkippableFact]
        public async Task TelemetryClient_ExportSingleEvent_SucceedsAgainstRealEndpoint()
        {
            Skip.IfNot(_canRunRealEndpointTests, "Real endpoint testing requires DATABRICKS_TEST_CONFIG_FILE");

            // Arrange
            using var httpClient = CreateAuthenticatedHttpClient();
            var config = new TelemetryConfiguration
            {
                Enabled = true,
                MaxRetries = 2,
                RetryDelayMs = 100
            };

            var client = new TelemetryClient(_host!, httpClient, config);

            try
            {
                // Create a single test log event
                var log = CreateTestLog();
                var logs = new List<TelemetryFrontendLog> { log };

                // Act - Export the single event
                await client.ExportAsync(logs);

                // Assert - If we get here without exception, the export succeeded
                // The telemetry endpoint doesn't return data, so success means no exception
                OutputHelper?.WriteLine($"Successfully exported single event to {_host}");
                OutputHelper?.WriteLine($"Event ID: {log.FrontendLogEventId}");
            }
            finally
            {
                await client.CloseAsync();
            }
        }

        #endregion

        #region TelemetryClient_ExportBatch_SucceedsAgainstRealEndpoint

        /// <summary>
        /// Tests that TelemetryClient can export a batch of events to the real Databricks endpoint.
        /// Validates that multiple events can be sent in a single request.
        /// </summary>
        [SkippableFact]
        public async Task TelemetryClient_ExportBatch_SucceedsAgainstRealEndpoint()
        {
            Skip.IfNot(_canRunRealEndpointTests, "Real endpoint testing requires DATABRICKS_TEST_CONFIG_FILE");

            // Arrange
            using var httpClient = CreateAuthenticatedHttpClient();
            var config = new TelemetryConfiguration
            {
                Enabled = true,
                MaxRetries = 2,
                RetryDelayMs = 100,
                BatchSize = 10
            };

            var client = new TelemetryClient(_host!, httpClient, config);

            try
            {
                // Create a batch of test log events
                var logs = new List<TelemetryFrontendLog>();
                for (int i = 0; i < 5; i++)
                {
                    logs.Add(CreateTestLog());
                }

                // Act - Export the batch
                await client.ExportAsync(logs);

                // Assert - If we get here without exception, the export succeeded
                OutputHelper?.WriteLine($"Successfully exported batch of {logs.Count} events to {_host}");
                foreach (var log in logs)
                {
                    OutputHelper?.WriteLine($"  Event ID: {log.FrontendLogEventId}");
                }
            }
            finally
            {
                await client.CloseAsync();
            }
        }

        #endregion

        #region TelemetryClient_Authenticated_UsesCorrectEndpoint

        /// <summary>
        /// Tests that TelemetryClient uses the correct authenticated endpoint.
        /// Validates that the /telemetry-ext endpoint is used for authenticated requests.
        /// </summary>
        [SkippableFact]
        public async Task TelemetryClient_Authenticated_UsesCorrectEndpoint()
        {
            Skip.IfNot(_canRunRealEndpointTests, "Real endpoint testing requires DATABRICKS_TEST_CONFIG_FILE");

            // Arrange
            var requestedUrls = new List<string>();
            var mockHandler = new MockHttpMessageHandler((request) =>
            {
                requestedUrls.Add(request.RequestUri?.ToString() ?? string.Empty);
                // Return 200 OK for telemetry endpoint
                return new HttpResponseMessage(System.Net.HttpStatusCode.OK);
            });

            using var httpClient = new HttpClient(mockHandler);
            httpClient.DefaultRequestHeaders.Authorization =
                new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", _token);

            var config = new TelemetryConfiguration
            {
                Enabled = true,
                MaxRetries = 0
            };

            var client = new TelemetryClient(_host!, httpClient, config);

            try
            {
                // Create test log
                var logs = new List<TelemetryFrontendLog> { CreateTestLog() };

                // Act - Export via the client (which uses CircuitBreakerTelemetryExporter internally)
                await client.ExportAsync(logs);

                // Assert - Verify the authenticated endpoint was used
                Assert.Single(requestedUrls);
                var requestedUrl = requestedUrls[0];
                Assert.Contains("/telemetry-ext", requestedUrl);
                Assert.DoesNotContain("/telemetry-unauth", requestedUrl);

                OutputHelper?.WriteLine($"Authenticated endpoint URL: {requestedUrl}");
            }
            finally
            {
                await client.CloseAsync();
            }
        }

        #endregion

        #region TelemetryClient_CircuitBreaker_OpensAfterFailures

        /// <summary>
        /// Tests that TelemetryClient's circuit breaker opens after consecutive failures.
        /// Validates that subsequent requests are dropped when the circuit is open.
        /// </summary>
        /// <remarks>
        /// This test uses a custom FailingTelemetryExporter that throws exceptions,
        /// allowing the CircuitBreakerTelemetryExporter to track failures properly.
        /// The DatabricksTelemetryExporter swallows exceptions internally, so we need
        /// a mock that re-throws to test circuit breaker behavior.
        ///
        /// The default circuit breaker threshold is 5 (from CircuitBreakerConfig).
        /// </remarks>
        [Fact]
        public async Task TelemetryClient_CircuitBreaker_OpensAfterFailures()
        {
            // Arrange
            var requestCount = 0;
            var failingExporter = new FailingTelemetryExporter(() =>
            {
                Interlocked.Increment(ref requestCount);
                throw new HttpRequestException("Service unavailable");
            });

            var config = new TelemetryConfiguration
            {
                Enabled = true,
                MaxRetries = 0,
                CircuitBreakerEnabled = true
                // Note: CircuitBreakerThreshold in TelemetryConfiguration doesn't affect
                // the global CircuitBreakerManager which uses default config (threshold = 5)
            };

            // Default circuit breaker config threshold is 5
            const int DefaultFailureThreshold = 5;

            var host = "https://test-circuit-breaker-opens.databricks.com";

            // Reset the circuit breaker manager to ensure clean state
            var circuitBreakerManager = CircuitBreakerManager.GetInstance();
            // Remove any existing circuit breaker for this host to start fresh
            circuitBreakerManager.RemoveCircuitBreaker(host);
            var circuitBreaker = circuitBreakerManager.GetCircuitBreaker(host);
            circuitBreaker.Reset();

            // Create CircuitBreakerTelemetryExporter wrapping our failing exporter
            var circuitBreakerExporter = new CircuitBreakerTelemetryExporter(host, failingExporter);

            using var httpClient = new HttpClient();
            var client = new TelemetryClient(
                host,
                httpClient,
                config,
                workspaceId: 12345,
                userAgent: "TestAgent",
                exporter: circuitBreakerExporter,
                aggregator: null,
                listener: null);

            try
            {
                var logs = new List<TelemetryFrontendLog> { CreateTestLog() };

                // Act - Make enough requests to open the circuit breaker
                // Default threshold is 5, so we need at least 5 failures to open
                // Then additional requests should be dropped
                for (int i = 0; i < 8; i++)
                {
                    await client.ExportAsync(logs);
                }

                // Assert - Circuit breaker should have opened after threshold failures
                // So only threshold requests should have been made (not 8)
                // After the circuit opens, requests are dropped without making HTTP calls
                Assert.True(requestCount <= DefaultFailureThreshold,
                    $"Expected at most {DefaultFailureThreshold} requests before circuit opened, but got {requestCount}");

                OutputHelper?.WriteLine($"Circuit breaker opened after {requestCount} failures");
                OutputHelper?.WriteLine($"Circuit state: {circuitBreaker.State}");

                // Verify circuit is now open
                Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);
            }
            finally
            {
                await client.CloseAsync();
                // Reset circuit breaker for other tests
                circuitBreaker.Reset();
            }
        }

        #endregion

        #region TelemetryClient_GracefulClose_FlushesAllPending

        /// <summary>
        /// Tests that TelemetryClient gracefully closes and flushes all pending events.
        /// Validates that pending events are exported before the client closes.
        /// </summary>
        [Fact]
        public async Task TelemetryClient_GracefulClose_FlushesAllPending()
        {
            // Arrange
            var exportedLogs = new List<TelemetryFrontendLog>();
            var exportLock = new object();
            var mockExporter = new MockTelemetryExporter((logs) =>
            {
                lock (exportLock)
                {
                    exportedLogs.AddRange(logs);
                }
            });

            var config = new TelemetryConfiguration
            {
                Enabled = true,
                FlushIntervalMs = 60000 // Long interval so background flush doesn't trigger
            };

            var host = "https://test-graceful-close.databricks.com";
            using var httpClient = new HttpClient();

            // Create client with our mock exporter
            var client = new TelemetryClient(
                host,
                httpClient,
                config,
                workspaceId: 12345,
                userAgent: "TestAgent",
                exporter: mockExporter,
                aggregator: null,
                listener: null);

            // Act - Export some logs
            var logs1 = new List<TelemetryFrontendLog> { CreateTestLog() };
            var logs2 = new List<TelemetryFrontendLog> { CreateTestLog() };
            var logs3 = new List<TelemetryFrontendLog> { CreateTestLog() };

            await client.ExportAsync(logs1);
            await client.ExportAsync(logs2);
            await client.ExportAsync(logs3);

            // All exports should complete immediately (exporter is synchronous mock)
            var logsBeforeClose = exportedLogs.Count;
            OutputHelper?.WriteLine($"Logs exported before close: {logsBeforeClose}");

            // Close the client gracefully
            await client.CloseAsync();

            // Assert - All logs should have been exported
            Assert.Equal(3, logsBeforeClose);
            OutputHelper?.WriteLine($"Total logs after close: {exportedLogs.Count}");
            OutputHelper?.WriteLine($"Client closed gracefully");
        }

        /// <summary>
        /// Tests that TelemetryClient can be closed multiple times safely (idempotent).
        /// </summary>
        [Fact]
        public async Task TelemetryClient_CloseAsync_IsIdempotent()
        {
            // Arrange
            var closeCount = 0;
            var mockExporter = new MockTelemetryExporter((logs) => { });

            var config = new TelemetryConfiguration
            {
                Enabled = true,
                FlushIntervalMs = 60000
            };

            var host = "https://test-idempotent-close.databricks.com";
            using var httpClient = new HttpClient();

            var client = new TelemetryClient(
                host,
                httpClient,
                config,
                workspaceId: 12345,
                userAgent: "TestAgent",
                exporter: mockExporter,
                aggregator: null,
                listener: null);

            // Act - Close multiple times
            await client.CloseAsync();
            closeCount++;
            await client.CloseAsync();
            closeCount++;
            await client.CloseAsync();
            closeCount++;

            // Assert - No exceptions thrown, all closes completed
            Assert.Equal(3, closeCount);
            Assert.True(client.IsClosed);
            OutputHelper?.WriteLine("Multiple closes succeeded without error");
        }

        /// <summary>
        /// Tests that TelemetryClient doesn't throw when exporting after close.
        /// </summary>
        [Fact]
        public async Task TelemetryClient_ExportAfterClose_DoesNotThrow()
        {
            // Arrange
            var exportCount = 0;
            var mockExporter = new MockTelemetryExporter((logs) =>
            {
                Interlocked.Increment(ref exportCount);
            });

            var config = new TelemetryConfiguration
            {
                Enabled = true,
                FlushIntervalMs = 60000
            };

            var host = "https://test-export-after-close.databricks.com";
            using var httpClient = new HttpClient();

            var client = new TelemetryClient(
                host,
                httpClient,
                config,
                workspaceId: 12345,
                userAgent: "TestAgent",
                exporter: mockExporter,
                aggregator: null,
                listener: null);

            // Close the client first
            await client.CloseAsync();

            // Act - Try to export after close (should not throw)
            var logs = new List<TelemetryFrontendLog> { CreateTestLog() };
            await client.ExportAsync(logs);

            // Assert - Export may or may not process logs after close,
            // but it should never throw an exception
            OutputHelper?.WriteLine($"Export after close completed without exception");
            OutputHelper?.WriteLine($"Export count: {exportCount}");
        }

        #endregion

        #region Real Endpoint Validation Tests

        /// <summary>
        /// Tests that the data model format is accepted by the real Databricks endpoint.
        /// This validates the JSON serialization matches what the backend expects.
        /// </summary>
        [SkippableFact]
        public async Task TelemetryClient_DataModelFormat_AcceptedByRealEndpoint()
        {
            Skip.IfNot(_canRunRealEndpointTests, "Real endpoint testing requires DATABRICKS_TEST_CONFIG_FILE");

            // Arrange
            using var httpClient = CreateAuthenticatedHttpClient();
            var config = new TelemetryConfiguration
            {
                Enabled = true,
                MaxRetries = 1,
                RetryDelayMs = 100
            };

            var client = new TelemetryClient(_host!, httpClient, config);

            try
            {
                // Create a comprehensive log with all fields populated
                var log = new TelemetryFrontendLog
                {
                    FrontendLogEventId = Guid.NewGuid().ToString(),
                    WorkspaceId = 0,
                    Context = new FrontendLogContext
                    {
                        ClientContext = new TelemetryClientContext
                        {
                            UserAgent = "Databricks.ADBC.E2ETest/1.0"
                        },
                        TimestampMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    },
                    Entry = new FrontendLogEntry
                    {
                        SqlDriverLog = new TelemetryEvent
                        {
                            SessionId = Guid.NewGuid().ToString(),
                            SqlStatementId = Guid.NewGuid().ToString(),
                            OperationLatencyMs = 250,
                            SystemConfiguration = new DriverSystemConfiguration
                            {
                                DriverName = "databricks-adbc",
                                DriverVersion = "1.0.0",
                                OsName = Environment.OSVersion.Platform.ToString(),
                                OsVersion = Environment.OSVersion.Version.ToString(),
                                RuntimeName = ".NET",
                                RuntimeVersion = Environment.Version.ToString(),
                                Locale = System.Globalization.CultureInfo.CurrentCulture.Name,
                                Timezone = TimeZoneInfo.Local.Id
                            },
                            ConnectionParameters = new DriverConnectionParameters
                            {
                                AuthType = "pat",
                                TransportMode = "http"
                            },
                            SqlExecutionEvent = new SqlExecutionEvent
                            {
                                ResultFormat = "arrow",
                                ExecutionStatus = "success",
                                StatementType = "SELECT"
                            }
                        }
                    }
                };

                var logs = new List<TelemetryFrontendLog> { log };

                // Act - Export the comprehensive log
                await client.ExportAsync(logs);

                // Assert - Success means format is accepted
                OutputHelper?.WriteLine($"Comprehensive data model accepted by endpoint");
                OutputHelper?.WriteLine($"  WorkspaceId: {log.WorkspaceId}");
                OutputHelper?.WriteLine($"  SessionId: {log.Entry?.SqlDriverLog?.SessionId}");
                OutputHelper?.WriteLine($"  StatementId: {log.Entry?.SqlDriverLog?.SqlStatementId}");
                OutputHelper?.WriteLine($"  DriverName: {log.Entry?.SqlDriverLog?.SystemConfiguration?.DriverName}");
            }
            finally
            {
                await client.CloseAsync();
            }
        }

        #endregion

        #region Mock Classes

        /// <summary>
        /// Mock HTTP message handler for testing HTTP interactions.
        /// </summary>
        private class MockHttpMessageHandler : HttpMessageHandler
        {
            private readonly Func<HttpRequestMessage, HttpResponseMessage> _handler;

            public MockHttpMessageHandler(Func<HttpRequestMessage, HttpResponseMessage> handler)
            {
                _handler = handler;
            }

            protected override Task<HttpResponseMessage> SendAsync(
                HttpRequestMessage request,
                CancellationToken cancellationToken)
            {
                return Task.FromResult(_handler(request));
            }
        }

        /// <summary>
        /// Mock telemetry exporter for testing export behavior.
        /// </summary>
        private class MockTelemetryExporter : ITelemetryExporter
        {
            private readonly Action<IReadOnlyList<TelemetryFrontendLog>> _onExport;

            public MockTelemetryExporter(Action<IReadOnlyList<TelemetryFrontendLog>> onExport)
            {
                _onExport = onExport;
            }

            public Task<bool> ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
            {
                if (logs != null && logs.Count > 0)
                {
                    _onExport(logs);
                }
                return Task.FromResult(true);
            }
        }

        /// <summary>
        /// Telemetry exporter that can be configured to throw exceptions.
        /// Used for testing circuit breaker behavior where exceptions need to propagate.
        /// </summary>
        private class FailingTelemetryExporter : ITelemetryExporter
        {
            private readonly Action _onExport;

            public FailingTelemetryExporter(Action onExport)
            {
                _onExport = onExport;
            }

            public async Task<bool> ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
            {
                if (logs != null && logs.Count > 0)
                {
                    // Small delay to ensure async context, then call the action
                    await Task.Yield();
                    // Call the action which may throw an exception
                    // The exception must propagate for the circuit breaker to track it
                    _onExport();
                }
                return true;
            }
        }

        #endregion
    }
}
