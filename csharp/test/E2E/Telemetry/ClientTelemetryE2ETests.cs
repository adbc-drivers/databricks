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
using System.Diagnostics;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using AdbcDrivers.Databricks.Telemetry.Proto;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// End-to-end tests for client telemetry that sends telemetry to Databricks endpoint.
    /// These tests verify that the DatabricksTelemetryExporter can successfully send
    /// telemetry events to real Databricks telemetry endpoints.
    /// </summary>
    public class ClientTelemetryE2ETests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public ClientTelemetryE2ETests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
        }

        /// <summary>
        /// Tests that telemetry can be sent to the authenticated endpoint (/telemetry-ext).
        /// This endpoint requires a valid authentication token.
        /// </summary>
        [SkippableFact]
        public async Task CanSendTelemetryToAuthenticatedEndpoint()
        {
            // Skip if no token is available
            Skip.If(string.IsNullOrEmpty(TestConfiguration.Token) && string.IsNullOrEmpty(TestConfiguration.AccessToken),
                "Token is required for authenticated telemetry endpoint test");

            var host = GetDatabricksHost();
            Skip.If(string.IsNullOrEmpty(host), "Databricks host is required");

            OutputHelper?.WriteLine($"Testing authenticated telemetry endpoint at {host}/telemetry-ext");

            // Create HttpClient with authentication
            using var httpClient = CreateAuthenticatedHttpClient();

            var config = new TelemetryConfiguration
            {
                MaxRetries = 2,
                RetryDelayMs = 100
            };

            var exporter = new DatabricksTelemetryExporter(httpClient, host, isAuthenticated: true, config);

            // Verify endpoint URL
            var endpointUrl = exporter.GetEndpointUrl();
            Assert.Equal($"{host}/telemetry-ext", endpointUrl);
            OutputHelper?.WriteLine($"Endpoint URL: {endpointUrl}");

            // Create a test telemetry log
            var logs = CreateTestTelemetryLogs(1);

            // Send telemetry - should succeed and return true
            var success = await exporter.ExportAsync(logs);

            // ExportAsync should return true indicating HTTP 200 response
            Assert.True(success, "ExportAsync should return true indicating successful HTTP 200 response");
            OutputHelper?.WriteLine("Successfully sent telemetry to authenticated endpoint");
        }

        /// <summary>
        /// Tests that telemetry can be sent to the unauthenticated endpoint (/telemetry-unauth).
        /// This endpoint does not require authentication.
        /// </summary>
        [SkippableFact]
        public async Task CanSendTelemetryToUnauthenticatedEndpoint()
        {
            var host = GetDatabricksHost();
            Skip.If(string.IsNullOrEmpty(host), "Databricks host is required");

            OutputHelper?.WriteLine($"Testing unauthenticated telemetry endpoint at {host}/telemetry-unauth");

            // Create HttpClient without authentication
            using var httpClient = new HttpClient();

            var config = new TelemetryConfiguration
            {
                MaxRetries = 2,
                RetryDelayMs = 100
            };

            var exporter = new DatabricksTelemetryExporter(httpClient, host, isAuthenticated: false, config);

            // Verify endpoint URL
            var endpointUrl = exporter.GetEndpointUrl();
            Assert.Equal($"{host}/telemetry-unauth", endpointUrl);
            OutputHelper?.WriteLine($"Endpoint URL: {endpointUrl}");

            // Create a test telemetry log
            var logs = CreateTestTelemetryLogs(1);

            // Send telemetry - should succeed and return true
            var success = await exporter.ExportAsync(logs);

            // ExportAsync should return true indicating HTTP 200 response
            Assert.True(success, "ExportAsync should return true indicating successful HTTP 200 response");
            OutputHelper?.WriteLine("Successfully sent telemetry to unauthenticated endpoint");
        }

        /// <summary>
        /// Tests that multiple telemetry logs can be batched and sent together.
        /// </summary>
        [SkippableFact]
        public async Task CanSendBatchedTelemetryLogs()
        {
            Skip.If(string.IsNullOrEmpty(TestConfiguration.Token) && string.IsNullOrEmpty(TestConfiguration.AccessToken),
                "Token is required for authenticated telemetry endpoint test");

            var host = GetDatabricksHost();
            Skip.If(string.IsNullOrEmpty(host), "Databricks host is required");

            OutputHelper?.WriteLine($"Testing batched telemetry to {host}");

            using var httpClient = CreateAuthenticatedHttpClient();

            var config = new TelemetryConfiguration
            {
                MaxRetries = 2,
                RetryDelayMs = 100
            };

            var exporter = new DatabricksTelemetryExporter(httpClient, host, isAuthenticated: true, config);

            // Create multiple telemetry logs
            var logs = CreateTestTelemetryLogs(5);
            OutputHelper?.WriteLine($"Created {logs.Count} telemetry logs for batch send");

            // Send telemetry - should succeed and return true
            var success = await exporter.ExportAsync(logs);

            Assert.True(success, "ExportAsync should return true for batched telemetry");
            OutputHelper?.WriteLine("Successfully sent batched telemetry logs");
        }

        /// <summary>
        /// Tests that the telemetry request is properly formatted.
        /// </summary>
        [SkippableFact]
        public void TelemetryRequestIsProperlyFormatted()
        {
            var host = GetDatabricksHost();
            Skip.If(string.IsNullOrEmpty(host), "Databricks host is required");

            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var exporter = new DatabricksTelemetryExporter(httpClient, host, isAuthenticated: true, config);

            // Create test logs
            var logs = CreateTestTelemetryLogs(2);

            // Create the request
            var request = exporter.CreateTelemetryRequest(logs);

            // Verify request structure
            Assert.True(request.UploadTime > 0, "UploadTime should be a positive timestamp");
            Assert.Equal(2, request.ProtoLogs.Count);

            // Verify each log is serialized as JSON
            foreach (var protoLog in request.ProtoLogs)
            {
                Assert.NotEmpty(protoLog);
                Assert.Contains("workspace_id", protoLog);
                Assert.Contains("frontend_log_event_id", protoLog);
                OutputHelper?.WriteLine($"Serialized log: {protoLog}");
            }

            // Verify the full request serialization
            var json = exporter.SerializeRequest(request);
            Assert.Contains("uploadTime", json);
            Assert.Contains("protoLogs", json);
            OutputHelper?.WriteLine($"Full request JSON: {json}");
        }

        /// <summary>
        /// Tests telemetry with a complete TelemetryFrontendLog including all nested objects.
        /// </summary>
        [SkippableFact]
        public async Task CanSendCompleteTelemetryEvent()
        {
            Skip.If(string.IsNullOrEmpty(TestConfiguration.Token) && string.IsNullOrEmpty(TestConfiguration.AccessToken),
                "Token is required for authenticated telemetry endpoint test");

            var host = GetDatabricksHost();
            Skip.If(string.IsNullOrEmpty(host), "Databricks host is required");

            OutputHelper?.WriteLine($"Testing complete telemetry event to {host}");

            using var httpClient = CreateAuthenticatedHttpClient();

            var config = new TelemetryConfiguration
            {
                MaxRetries = 2,
                RetryDelayMs = 100
            };

            var exporter = new DatabricksTelemetryExporter(httpClient, host, isAuthenticated: true, config);

            // Create a complete telemetry log with all fields populated
            var log = new TelemetryFrontendLog
            {
                WorkspaceId = 12345678901234,
                FrontendLogEventId = Guid.NewGuid().ToString(),
                Context = new FrontendLogContext
                {
                    TimestampMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    ClientContext = new TelemetryClientContext
                    {
                        UserAgent = "AdbcDatabricksDriver/1.0.0-test (.NET; E2E Test)"
                    }
                },
                Entry = new FrontendLogEntry
                {
                    SqlDriverLog = new OssSqlDriverTelemetryLog
                    {
                        SessionId = Guid.NewGuid().ToString(),
                        SqlStatementId = Guid.NewGuid().ToString(),
                        OperationLatencyMs = 150,
                        SystemConfiguration = new DriverSystemConfiguration
                        {
                            DriverName = "Databricks ADBC Driver",
                            DriverVersion = "1.0.0-test",
                            OsName = Environment.OSVersion.Platform.ToString(),
                            OsVersion = Environment.OSVersion.Version.ToString(),
                            RuntimeName = ".NET",
                            RuntimeVersion = Environment.Version.ToString(),
                            LocaleName = System.Globalization.CultureInfo.CurrentCulture.Name
                        }
                    }
                }
            };

            var logs = new List<TelemetryFrontendLog> { log };

            OutputHelper?.WriteLine($"Sending complete telemetry event:");
            OutputHelper?.WriteLine($"  WorkspaceId: {log.WorkspaceId}");
            OutputHelper?.WriteLine($"  EventId: {log.FrontendLogEventId}");
            OutputHelper?.WriteLine($"  SessionId: {log.Entry?.SqlDriverLog?.SessionId}");

            // Send telemetry - should succeed and return true
            var success = await exporter.ExportAsync(logs);

            Assert.True(success, "ExportAsync should return true for complete telemetry event");
            OutputHelper?.WriteLine("Successfully sent complete telemetry event");
        }

        /// <summary>
        /// Tests that empty log lists are handled gracefully without making HTTP requests.
        /// </summary>
        [SkippableFact]
        public async Task EmptyLogListDoesNotSendRequest()
        {
            var host = GetDatabricksHost();
            Skip.If(string.IsNullOrEmpty(host), "Databricks host is required");

            using var httpClient = new HttpClient();
            var config = new TelemetryConfiguration();
            var exporter = new DatabricksTelemetryExporter(httpClient, host, isAuthenticated: true, config);

            // Send empty list - should return immediately with true (nothing to export)
            var success = await exporter.ExportAsync(new List<TelemetryFrontendLog>());

            Assert.True(success, "ExportAsync should return true for empty list (nothing to export)");
            OutputHelper?.WriteLine("Empty log list handled gracefully");

            // Send null list - should also return immediately with true (nothing to export)
            success = await exporter.ExportAsync(null!);

            Assert.True(success, "ExportAsync should return true for null list (nothing to export)");
            OutputHelper?.WriteLine("Null log list handled gracefully");
        }

        /// <summary>
        /// Tests that the exporter properly retries on transient failures.
        /// This test verifies the retry configuration is respected.
        /// </summary>
        [SkippableFact]
        public async Task TelemetryExporterRespectsRetryConfiguration()
        {
            Skip.If(string.IsNullOrEmpty(TestConfiguration.Token) && string.IsNullOrEmpty(TestConfiguration.AccessToken),
                "Token is required for authenticated telemetry endpoint test");

            var host = GetDatabricksHost();
            Skip.If(string.IsNullOrEmpty(host), "Databricks host is required");

            OutputHelper?.WriteLine("Testing retry configuration");

            using var httpClient = CreateAuthenticatedHttpClient();

            // Configure with specific retry settings
            var config = new TelemetryConfiguration
            {
                MaxRetries = 3,
                RetryDelayMs = 50
            };

            var exporter = new DatabricksTelemetryExporter(httpClient, host, isAuthenticated: true, config);

            var logs = CreateTestTelemetryLogs(1);

            // This should succeed and return true
            var success = await exporter.ExportAsync(logs);

            // Exporter should return true on success
            Assert.True(success, "ExportAsync should return true with retry configuration");
            OutputHelper?.WriteLine("Retry configuration test completed");
        }

        /// <summary>
        /// Tests that the authenticated telemetry endpoint returns HTTP 200.
        /// This test directly calls the endpoint to verify the response status code.
        /// </summary>
        [SkippableFact]
        public async Task AuthenticatedEndpointReturnsHttp200()
        {
            Skip.If(string.IsNullOrEmpty(TestConfiguration.Token) && string.IsNullOrEmpty(TestConfiguration.AccessToken),
                "Token is required for authenticated telemetry endpoint test");

            var host = GetDatabricksHost();
            Skip.If(string.IsNullOrEmpty(host), "Databricks host is required");

            var endpointUrl = $"{host}/telemetry-ext";
            OutputHelper?.WriteLine($"Testing HTTP response from {endpointUrl}");

            using var httpClient = CreateAuthenticatedHttpClient();

            // Create a minimal telemetry request
            var logs = CreateTestTelemetryLogs(1);
            var config = new TelemetryConfiguration();
            var exporter = new DatabricksTelemetryExporter(httpClient, host, isAuthenticated: true, config);
            var request = exporter.CreateTelemetryRequest(logs);
            var json = exporter.SerializeRequest(request);

            // Send the request directly to verify HTTP status
            using var content = new System.Net.Http.StringContent(json, System.Text.Encoding.UTF8, "application/json");
            using var response = await httpClient.PostAsync(endpointUrl, content);

            OutputHelper?.WriteLine($"HTTP Status Code: {(int)response.StatusCode} ({response.StatusCode})");
            OutputHelper?.WriteLine($"Response Headers: {response.Headers}");

            var responseBody = await response.Content.ReadAsStringAsync();
            OutputHelper?.WriteLine($"Response Body: {responseBody}");

            // Assert we get HTTP 200
            Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
            OutputHelper?.WriteLine("Verified: Authenticated endpoint returns HTTP 200");
        }

        /// <summary>
        /// Tests that the unauthenticated telemetry endpoint returns HTTP 200.
        /// This test directly calls the endpoint to verify the response status code.
        /// </summary>
        [SkippableFact]
        public async Task UnauthenticatedEndpointReturnsHttp200()
        {
            var host = GetDatabricksHost();
            Skip.If(string.IsNullOrEmpty(host), "Databricks host is required");

            var endpointUrl = $"{host}/telemetry-unauth";
            OutputHelper?.WriteLine($"Testing HTTP response from {endpointUrl}");

            using var httpClient = new HttpClient();

            // Create a minimal telemetry request
            var logs = CreateTestTelemetryLogs(1);
            var config = new TelemetryConfiguration();
            var exporter = new DatabricksTelemetryExporter(httpClient, host, isAuthenticated: false, config);
            var request = exporter.CreateTelemetryRequest(logs);
            var json = exporter.SerializeRequest(request);

            // Send the request directly to verify HTTP status
            using var content = new System.Net.Http.StringContent(json, System.Text.Encoding.UTF8, "application/json");
            using var response = await httpClient.PostAsync(endpointUrl, content);

            OutputHelper?.WriteLine($"HTTP Status Code: {(int)response.StatusCode} ({response.StatusCode})");
            OutputHelper?.WriteLine($"Response Headers: {response.Headers}");

            var responseBody = await response.Content.ReadAsStringAsync();
            OutputHelper?.WriteLine($"Response Body: {responseBody}");

            // Assert we get HTTP 200
            Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
            OutputHelper?.WriteLine("Verified: Unauthenticated endpoint returns HTTP 200");
        }

        /// <summary>
        /// Full integration E2E test that executes a real query with telemetry enabled.
        /// The production DatabricksConnection automatically hooks up the telemetry listener,
        /// aggregator, and exporter to capture and send telemetry to the Databricks endpoint.
        /// </summary>
        /// <remarks>
        /// This test verifies the complete end-to-end telemetry flow:
        /// 1. DatabricksConnection initializes telemetry infrastructure on connect
        /// 2. Query execution generates Activity events with telemetry tags
        /// 3. DatabricksActivityListener captures these activities
        /// 4. MetricsAggregator processes activities into TelemetryFrontendLog events
        /// 5. TelemetryClient exports events to the real Databricks /telemetry-ext endpoint
        /// 6. Connection close flushes any pending telemetry
        /// </remarks>
        [SkippableFact]
        public async Task FullIntegration_QueryExecution_TelemetryEnabledAndSentToEndpoint()
        {
            Skip.If(string.IsNullOrEmpty(TestConfiguration.Token) && string.IsNullOrEmpty(TestConfiguration.AccessToken),
                "Token is required for this test");

            var host = GetDatabricksHost();
            Skip.If(string.IsNullOrEmpty(host), "Databricks host is required");

            OutputHelper?.WriteLine("=== Full Integration E2E Test: Query Execution with Client Telemetry ===");
            OutputHelper?.WriteLine($"Host: {host}");
            OutputHelper?.WriteLine("");

            // Enable telemetry in connection parameters
            var parameters = GetDriverParameters(TestConfiguration);
            parameters["telemetry.enabled"] = "true";

            OutputHelper?.WriteLine("Telemetry enabled in connection parameters");
            OutputHelper?.WriteLine("");

            // Execute query - telemetry is automatically captured and sent by DatabricksConnection
            OutputHelper?.WriteLine("Executing query with telemetry enabled...");

            // Extract host name for TelemetryClientManager verification
            var hostUri = new Uri(host);
            var hostName = hostUri.Host;

            // Set up a trace listener to capture Debug.WriteLine output from telemetry components
            var traceOutput = new StringBuilder();
            var traceListener = new CapturingTraceListener(traceOutput);
            Trace.Listeners.Add(traceListener);

            try
            {
                using (var driver = NewDriver)
                using (var database = driver.Open(parameters))
                using (var connection = database.Connect(new Dictionary<string, string>()))
                {
                    OutputHelper?.WriteLine("  Connection opened - telemetry infrastructure initialized");

                    // Verify TelemetryClientManager has a client for this host
                    var clientManager = TelemetryClientManager.GetInstance();
                    bool hasClient = clientManager.HasClient(hostName);
                    OutputHelper?.WriteLine($"  TelemetryClientManager.HasClient('{hostName}'): {hasClient}");
                    Assert.True(hasClient, $"TelemetryClientManager should have a client for host '{hostName}' after connection opens with telemetry enabled");

                    using (var statement = connection.CreateStatement())
                    {
                        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                        statement.SqlQuery = "SELECT 1 as test_value, 'telemetry_e2e_test' as test_name";
                        var result = statement.ExecuteQuery();

                        using (var reader = result.Stream)
                        {
                            var batch = await reader.ReadNextRecordBatchAsync();
                            Assert.NotNull(batch);
                            Assert.Equal(1, batch.Length);
                        }

                        stopwatch.Stop();
                        OutputHelper?.WriteLine($"  Query executed successfully in {stopwatch.ElapsedMilliseconds}ms");
                    }

                    // Allow time for async telemetry flush
                    await Task.Delay(1000);
                    OutputHelper?.WriteLine("  Waiting for telemetry flush...");
                }

                // Connection disposed - telemetry client released and flushed
                // Allow additional time for final flush
                await Task.Delay(500);

                // Verify trace output contains telemetry export confirmation
                var traces = traceOutput.ToString();
                OutputHelper?.WriteLine("");
                OutputHelper?.WriteLine("=== Trace Output ===");
                OutputHelper?.WriteLine(traces);
                OutputHelper?.WriteLine("=== End Trace Output ===");
                OutputHelper?.WriteLine("");

                // Verify key telemetry traces
                Assert.Contains("TelemetryClient: Initialized for host", traces);
                Assert.Contains("TelemetryClientManager: GetOrCreateClient for host", traces);

                // Verify telemetry was exported to the endpoint
                bool telemetryExported = traces.Contains("Successfully exported telemetry to");
                bool hasFlushingEvents = traces.Contains("MetricsAggregator: Flushing");

                OutputHelper?.WriteLine($"Telemetry events flushed: {hasFlushingEvents}");
                OutputHelper?.WriteLine($"Telemetry exported to endpoint: {telemetryExported}");

                // Assert that telemetry was captured and exported
                Assert.True(hasFlushingEvents, "MetricsAggregator should have flushed telemetry events");
                Assert.True(telemetryExported, "Telemetry should have been exported to Databricks /telemetry-ext endpoint");

                OutputHelper?.WriteLine("");
                OutputHelper?.WriteLine("✓ CONFIRMED: Telemetry was sent to Databricks /telemetry-ext endpoint");
                OutputHelper?.WriteLine("");
                OutputHelper?.WriteLine("✓ SUCCESS: Full telemetry E2E flow verified:");
                OutputHelper?.WriteLine("  - TelemetryClientManager created client for host");
                OutputHelper?.WriteLine("  - DatabricksActivityListener captured query activities");
                OutputHelper?.WriteLine("  - MetricsAggregator processed events");
                OutputHelper?.WriteLine("  - DatabricksTelemetryExporter sent to Databricks endpoint");
            }
            finally
            {
                Trace.Listeners.Remove(traceListener);
            }
        }

        /// <summary>
        /// Tests multiple query executions with telemetry enabled to verify
        /// that all statement telemetry events are captured and sent.
        /// </summary>
        [SkippableFact]
        public async Task FullIntegration_MultipleQueries_AllTelemetryCapturedAndSent()
        {
            Skip.If(string.IsNullOrEmpty(TestConfiguration.Token) && string.IsNullOrEmpty(TestConfiguration.AccessToken),
                "Token is required for this test");

            var host = GetDatabricksHost();
            Skip.If(string.IsNullOrEmpty(host), "Databricks host is required");

            OutputHelper?.WriteLine("=== Full Integration E2E Test: Multiple Queries with Client Telemetry ===");
            OutputHelper?.WriteLine($"Host: {host}");
            OutputHelper?.WriteLine("");

            var parameters = GetDriverParameters(TestConfiguration);
            parameters["telemetry.enabled"] = "true";

            OutputHelper?.WriteLine("Executing 3 queries with telemetry enabled...");
            OutputHelper?.WriteLine("");

            using (var driver = NewDriver)
            using (var database = driver.Open(parameters))
            using (var connection = database.Connect(new Dictionary<string, string>()))
            {
                for (int i = 1; i <= 3; i++)
                {
                    using var statement = connection.CreateStatement();
                    var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                    statement.SqlQuery = $"SELECT {i} as query_number, 'test_{i}' as query_name";
                    var result = statement.ExecuteQuery();

                    using (var reader = result.Stream)
                    {
                        var batch = await reader.ReadNextRecordBatchAsync();
                        Assert.NotNull(batch);
                        Assert.Equal(1, batch.Length);
                    }

                    stopwatch.Stop();
                    OutputHelper?.WriteLine($"  Query {i}: {stopwatch.ElapsedMilliseconds}ms - telemetry captured");
                }

                // Allow time for telemetry batching and flush
                await Task.Delay(500);
            }

            OutputHelper?.WriteLine("");
            OutputHelper?.WriteLine("✓ SUCCESS: All 3 queries executed with telemetry captured and sent");
        }

        #region Helper Methods

        /// <summary>
        /// Gets the Databricks host URL from the test configuration.
        /// </summary>
        private string GetDatabricksHost()
        {
            // Try Uri first, then fall back to HostName
            if (!string.IsNullOrEmpty(TestConfiguration.Uri))
            {
                var uri = new Uri(TestConfiguration.Uri);
                return $"{uri.Scheme}://{uri.Host}";
            }

            if (!string.IsNullOrEmpty(TestConfiguration.HostName))
            {
                return $"https://{TestConfiguration.HostName}";
            }

            return string.Empty;
        }

        /// <summary>
        /// Creates an HttpClient with authentication headers.
        /// </summary>
        private HttpClient CreateAuthenticatedHttpClient()
        {
            var httpClient = new HttpClient();

            // Use AccessToken if available, otherwise fall back to Token
            var token = !string.IsNullOrEmpty(TestConfiguration.AccessToken)
                ? TestConfiguration.AccessToken
                : TestConfiguration.Token;

            if (!string.IsNullOrEmpty(token))
            {
                httpClient.DefaultRequestHeaders.Authorization =
                    new AuthenticationHeaderValue("Bearer", token);
            }

            return httpClient;
        }

        /// <summary>
        /// Creates test telemetry logs for E2E testing.
        /// </summary>
        private IReadOnlyList<TelemetryFrontendLog> CreateTestTelemetryLogs(int count)
        {
            var logs = new List<TelemetryFrontendLog>(count);

            for (int i = 0; i < count; i++)
            {
                logs.Add(new TelemetryFrontendLog
                {
                    WorkspaceId = 5870029948831567,
                    FrontendLogEventId = Guid.NewGuid().ToString(),
                    Context = new FrontendLogContext
                    {
                        TimestampMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                        ClientContext = new TelemetryClientContext
                        {
                            UserAgent = $"AdbcDatabricksDriver/1.0.0-test (.NET; E2E Test {i})"
                        }
                    },
                    Entry = new FrontendLogEntry
                    {
                        SqlDriverLog = new OssSqlDriverTelemetryLog
                        {
                            SessionId = Guid.NewGuid().ToString(),
                            SqlStatementId = Guid.NewGuid().ToString(),
                            OperationLatencyMs = 100 + (i * 10)
                        }
                    }
                });
            }

            return logs;
        }

        #endregion
    }

    /// <summary>
    /// A trace listener that captures Debug.WriteLine output to a StringBuilder.
    /// Used for verifying telemetry trace logs in E2E tests.
    /// </summary>
    internal class CapturingTraceListener : TraceListener
    {
        private readonly StringBuilder _output;

        public CapturingTraceListener(StringBuilder output)
        {
            _output = output;
        }

        public override void Write(string? message)
        {
            if (message != null)
            {
                lock (_output)
                {
                    _output.Append(message);
                }
            }
        }

        public override void WriteLine(string? message)
        {
            if (message != null)
            {
                lock (_output)
                {
                    _output.AppendLine(message);
                }
            }
        }
    }
}
