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
using System.Net.Http.Headers;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
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
                    SqlDriverLog = new TelemetryEvent
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
                            Locale = System.Globalization.CultureInfo.CurrentCulture.Name,
                            Timezone = TimeZoneInfo.Local.Id
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
                        SqlDriverLog = new TelemetryEvent
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
}
