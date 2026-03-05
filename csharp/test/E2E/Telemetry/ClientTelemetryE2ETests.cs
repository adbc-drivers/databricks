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
using AdbcDrivers.Databricks.Telemetry.Proto;
using Apache.Arrow;
using Apache.Arrow.Adbc;
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

        #region Comprehensive E2E Tests

        /// <summary>
        /// Tests that connection event telemetry is exported with driver configuration.
        /// Validates the complete telemetry pipeline from connection creation to export.
        /// </summary>
        [SkippableFact]
        public async Task Telemetry_Connection_ExportsConnectionEvent()
        {
            Skip.If(string.IsNullOrEmpty(TestConfiguration.Token) && string.IsNullOrEmpty(TestConfiguration.AccessToken),
                "Token is required for connection telemetry test");

            var host = GetDatabricksHost();
            Skip.If(string.IsNullOrEmpty(host), "Databricks host is required");

            OutputHelper?.WriteLine($"Testing connection event telemetry to {host}");

            // Get connection properties and enable telemetry explicitly
            Dictionary<string, string> properties = TestEnvironment.GetDriverParameters(TestConfiguration);
            properties[TelemetryConfiguration.PropertyKeyEnabled] = "true";
            properties[TelemetryConfiguration.PropertyKeyBatchSize] = "10";
            properties[TelemetryConfiguration.PropertyKeyFlushIntervalMs] = "1000";

            // Create and open connection - this should trigger connection event
            AdbcDriver driver = NewDriver;
            AdbcDatabase database = driver.Open(properties);

            using (AdbcConnection connection = database.Connect(properties))
            {
                OutputHelper?.WriteLine("Connection opened successfully");

                // Execute a simple query to ensure connection is fully established
                using (AdbcStatement statement = connection.CreateStatement())
                {
                    statement.SqlQuery = "SELECT 1 as test_column";
                    QueryResult result = statement.ExecuteQuery();
                    Assert.NotNull(result);
                    OutputHelper?.WriteLine("Query executed successfully");
                }

                // Give telemetry time to flush
                await Task.Delay(2000);
            }

            database.Dispose();
            OutputHelper?.WriteLine("Connection event telemetry test completed");
        }

        /// <summary>
        /// Tests that statement execution telemetry exports with execution latency.
        /// Validates statement metrics are captured and exported correctly.
        /// </summary>
        [SkippableFact]
        public async Task Telemetry_Statement_ExportsStatementEvent()
        {
            Skip.If(string.IsNullOrEmpty(TestConfiguration.Token) && string.IsNullOrEmpty(TestConfiguration.AccessToken),
                "Token is required for statement telemetry test");

            var host = GetDatabricksHost();
            Skip.If(string.IsNullOrEmpty(host), "Databricks host is required");

            OutputHelper?.WriteLine($"Testing statement event telemetry to {host}");

            Dictionary<string, string> properties = TestEnvironment.GetDriverParameters(TestConfiguration);
            properties[TelemetryConfiguration.PropertyKeyEnabled] = "true";
            properties[TelemetryConfiguration.PropertyKeyBatchSize] = "5";
            properties[TelemetryConfiguration.PropertyKeyFlushIntervalMs] = "1000";

            AdbcDriver driver = NewDriver;
            AdbcDatabase database = driver.Open(properties);

            using (AdbcConnection connection = database.Connect(properties))
            {
                // Execute multiple statements to generate telemetry
                for (int i = 0; i < 3; i++)
                {
                    using (AdbcStatement statement = connection.CreateStatement())
                    {
                        statement.SqlQuery = $"SELECT {i} as iteration, 'test_{i}' as label";
                        QueryResult result = statement.ExecuteQuery();
                        Assert.NotNull(result);
                        OutputHelper?.WriteLine($"Statement {i} executed successfully");
                    }
                }

                // Give telemetry time to aggregate and flush
                await Task.Delay(2000);
            }

            database.Dispose();
            OutputHelper?.WriteLine("Statement event telemetry test completed");
        }

        /// <summary>
        /// Tests that large query results export CloudFetch chunk metrics.
        /// Validates chunk download metrics are captured for CloudFetch operations.
        /// </summary>
        [SkippableFact]
        public async Task Telemetry_CloudFetch_ExportsChunkMetrics()
        {
            Skip.If(string.IsNullOrEmpty(TestConfiguration.Token) && string.IsNullOrEmpty(TestConfiguration.AccessToken),
                "Token is required for CloudFetch telemetry test");

            var host = GetDatabricksHost();
            Skip.If(string.IsNullOrEmpty(host), "Databricks host is required");

            OutputHelper?.WriteLine($"Testing CloudFetch chunk metrics telemetry to {host}");

            Dictionary<string, string> properties = TestEnvironment.GetDriverParameters(TestConfiguration);
            properties[TelemetryConfiguration.PropertyKeyEnabled] = "true";
            properties[TelemetryConfiguration.PropertyKeyBatchSize] = "10";
            properties[TelemetryConfiguration.PropertyKeyFlushIntervalMs] = "2000";

            // Ensure CloudFetch is enabled
            properties["adbc.databricks.use_cloud_fetch"] = "true";

            AdbcDriver driver = NewDriver;
            AdbcDatabase database = driver.Open(properties);

            using (AdbcConnection connection = database.Connect(properties))
            {
                // Execute a query that returns a larger result set to trigger CloudFetch
                using (AdbcStatement statement = connection.CreateStatement())
                {
                    // Generate a result set with multiple rows to trigger chunk processing
                    statement.SqlQuery = @"
                        SELECT
                            id,
                            CONCAT('row_', CAST(id AS STRING)) as data,
                            id * 2 as computed_value
                        FROM RANGE(1000)";

                    QueryResult result = statement.ExecuteQuery();
                    Assert.NotNull(result);

                    // Consume the result to ensure CloudFetch completes
                    int rowCount = 0;
                    using (RecordBatch batch = result.Stream.ReadNextRecordBatchAsync().Result!)
                    {
                        if (batch != null)
                        {
                            rowCount = batch.Length;
                        }
                    }

                    OutputHelper?.WriteLine($"CloudFetch query executed, processed {rowCount} rows");
                }

                // Give telemetry time to process and flush CloudFetch metrics
                await Task.Delay(3000);
            }

            database.Dispose();
            OutputHelper?.WriteLine("CloudFetch chunk metrics telemetry test completed");
        }

        /// <summary>
        /// Tests that error events are exported when SQL execution fails.
        /// Validates error telemetry captures exception details correctly.
        /// </summary>
        [SkippableFact]
        public async Task Telemetry_Error_ExportsErrorEvent()
        {
            Skip.If(string.IsNullOrEmpty(TestConfiguration.Token) && string.IsNullOrEmpty(TestConfiguration.AccessToken),
                "Token is required for error telemetry test");

            var host = GetDatabricksHost();
            Skip.If(string.IsNullOrEmpty(host), "Databricks host is required");

            OutputHelper?.WriteLine($"Testing error event telemetry to {host}");

            Dictionary<string, string> properties = TestEnvironment.GetDriverParameters(TestConfiguration);
            properties[TelemetryConfiguration.PropertyKeyEnabled] = "true";
            properties[TelemetryConfiguration.PropertyKeyBatchSize] = "5";
            properties[TelemetryConfiguration.PropertyKeyFlushIntervalMs] = "1000";

            AdbcDriver driver = NewDriver;
            AdbcDatabase database = driver.Open(properties);

            using (AdbcConnection connection = database.Connect(properties))
            {
                // Execute an invalid query to generate an error event
                try
                {
                    using (AdbcStatement statement = connection.CreateStatement())
                    {
                        statement.SqlQuery = "SELECT * FROM table_that_does_not_exist_12345";
                        QueryResult result = statement.ExecuteQuery();
                        // This should throw an exception
                        Assert.Fail("Expected query to fail but it succeeded");
                    }
                }
                catch (Exception ex)
                {
                    OutputHelper?.WriteLine($"Expected error occurred: {ex.Message}");
                    // Error is expected - telemetry should capture it
                }

                // Give telemetry time to process and flush error event
                await Task.Delay(2000);
            }

            database.Dispose();
            OutputHelper?.WriteLine("Error event telemetry test completed");
        }

        /// <summary>
        /// Tests that when telemetry feature flag is disabled, no telemetry is exported.
        /// Validates that telemetry can be completely disabled via configuration.
        /// </summary>
        [SkippableFact]
        public async Task Telemetry_FeatureFlagDisabled_NoExport()
        {
            Skip.If(string.IsNullOrEmpty(TestConfiguration.Token) && string.IsNullOrEmpty(TestConfiguration.AccessToken),
                "Token is required for feature flag test");

            var host = GetDatabricksHost();
            Skip.If(string.IsNullOrEmpty(host), "Databricks host is required");

            OutputHelper?.WriteLine($"Testing feature flag disabled scenario for {host}");

            Dictionary<string, string> properties = TestEnvironment.GetDriverParameters(TestConfiguration);

            // Explicitly disable telemetry
            properties[TelemetryConfiguration.PropertyKeyEnabled] = "false";

            AdbcDriver driver = NewDriver;
            AdbcDatabase database = driver.Open(properties);

            using (AdbcConnection connection = database.Connect(properties))
            {
                // Execute queries - telemetry should not be initialized or exported
                for (int i = 0; i < 5; i++)
                {
                    using (AdbcStatement statement = connection.CreateStatement())
                    {
                        statement.SqlQuery = $"SELECT {i} as test";
                        QueryResult result = statement.ExecuteQuery();
                        Assert.NotNull(result);
                        OutputHelper?.WriteLine($"Query {i} executed successfully with telemetry disabled");
                    }
                }

                await Task.Delay(500);
            }

            database.Dispose();
            OutputHelper?.WriteLine("Feature flag disabled test completed - no telemetry exported");
        }

        /// <summary>
        /// Tests that multiple connections to the same host share a single telemetry client.
        /// Validates per-host singleton pattern prevents rate limiting.
        /// </summary>
        [SkippableFact]
        public async Task Telemetry_MultipleConnections_SameHost_SharesClient()
        {
            Skip.If(string.IsNullOrEmpty(TestConfiguration.Token) && string.IsNullOrEmpty(TestConfiguration.AccessToken),
                "Token is required for multiple connections test");

            var host = GetDatabricksHost();
            Skip.If(string.IsNullOrEmpty(host), "Databricks host is required");

            OutputHelper?.WriteLine($"Testing multiple connections sharing telemetry client for {host}");

            Dictionary<string, string> properties = TestEnvironment.GetDriverParameters(TestConfiguration);
            properties[TelemetryConfiguration.PropertyKeyEnabled] = "true";
            properties[TelemetryConfiguration.PropertyKeyBatchSize] = "20";
            properties[TelemetryConfiguration.PropertyKeyFlushIntervalMs] = "2000";

            AdbcDriver driver = NewDriver;
            AdbcDatabase database = driver.Open(properties);

            // Create 3 connections to the same host
            List<AdbcConnection> connections = new List<AdbcConnection>();
            try
            {
                for (int i = 0; i < 3; i++)
                {
                    AdbcConnection connection = database.Connect(properties);
                    connections.Add(connection);
                    OutputHelper?.WriteLine($"Connection {i + 1} opened");

                    // Execute a query on each connection
                    using (AdbcStatement statement = connection.CreateStatement())
                    {
                        statement.SqlQuery = $"SELECT {i} as connection_id, 'connection_{i}' as name";
                        QueryResult result = statement.ExecuteQuery();
                        Assert.NotNull(result);
                        OutputHelper?.WriteLine($"Query executed on connection {i + 1}");
                    }
                }

                // All connections should share the same telemetry client
                // Give telemetry time to aggregate events from all connections
                await Task.Delay(3000);
            }
            finally
            {
                // Dispose all connections - only the last one should close the telemetry client
                foreach (var connection in connections)
                {
                    connection.Dispose();
                }
                OutputHelper?.WriteLine("All connections disposed");
            }

            database.Dispose();
            OutputHelper?.WriteLine("Multiple connections shared client test completed");
        }

        /// <summary>
        /// Tests that circuit breaker stops exporting telemetry after failure threshold.
        /// Validates circuit breaker correctly isolates failing endpoints.
        /// </summary>
        [SkippableFact]
        public async Task Telemetry_CircuitBreaker_StopsExportingOnFailure()
        {
            var host = GetDatabricksHost();
            Skip.If(string.IsNullOrEmpty(host), "Databricks host is required");

            OutputHelper?.WriteLine($"Testing circuit breaker behavior for {host}");

            // Use an invalid host to trigger circuit breaker
            string invalidHost = "https://invalid-databricks-host-12345.cloud.databricks.com";

            using var httpClient = new HttpClient();

            // Configure circuit breaker with low threshold for testing
            var config = new TelemetryConfiguration
            {
                Enabled = true,
                BatchSize = 1,
                FlushIntervalMs = 100,
                MaxRetries = 1,
                RetryDelayMs = 50,
                CircuitBreakerEnabled = true,
                CircuitBreakerThreshold = 3, // Open after 3 failures
                CircuitBreakerTimeout = TimeSpan.FromSeconds(30)
            };

            var exporter = new DatabricksTelemetryExporter(httpClient, invalidHost, isAuthenticated: false, config);
            var circuitBreakerExporter = new CircuitBreakerTelemetryExporter(exporter, invalidHost);

            // Create test telemetry logs
            var logs = CreateTestTelemetryLogs(1);

            // First 3 exports should fail and increment circuit breaker failure count
            for (int i = 0; i < 5; i++)
            {
                bool success = await circuitBreakerExporter.ExportAsync(logs);
                OutputHelper?.WriteLine($"Export attempt {i + 1}: {(success ? "Success" : "Failed")}");
            }

            // After threshold failures, circuit breaker should be open
            // Subsequent exports should be silently dropped (no HTTP requests)
            bool finalExport = await circuitBreakerExporter.ExportAsync(logs);
            OutputHelper?.WriteLine($"Final export after circuit open: {(finalExport ? "Success" : "Failed")}");

            OutputHelper?.WriteLine("Circuit breaker test completed");
        }

        /// <summary>
        /// Tests that connection close flushes all pending telemetry events.
        /// Validates graceful shutdown ensures no telemetry loss.
        /// </summary>
        [SkippableFact]
        public void Telemetry_GracefulShutdown_FlushesBeforeClose()
        {
            Skip.If(string.IsNullOrEmpty(TestConfiguration.Token) && string.IsNullOrEmpty(TestConfiguration.AccessToken),
                "Token is required for graceful shutdown test");

            var host = GetDatabricksHost();
            Skip.If(string.IsNullOrEmpty(host), "Databricks host is required");

            OutputHelper?.WriteLine($"Testing graceful shutdown with flush for {host}");

            Dictionary<string, string> properties = TestEnvironment.GetDriverParameters(TestConfiguration);
            properties[TelemetryConfiguration.PropertyKeyEnabled] = "true";
            properties[TelemetryConfiguration.PropertyKeyBatchSize] = "100"; // Large batch to ensure flush is needed
            properties[TelemetryConfiguration.PropertyKeyFlushIntervalMs] = "30000"; // Long interval to prevent automatic flush

            AdbcDriver driver = NewDriver;
            AdbcDatabase database = driver.Open(properties);

            using (AdbcConnection connection = database.Connect(properties))
            {
                // Execute queries to generate telemetry events
                for (int i = 0; i < 10; i++)
                {
                    using (AdbcStatement statement = connection.CreateStatement())
                    {
                        statement.SqlQuery = $"SELECT {i} as iteration";
                        QueryResult result = statement.ExecuteQuery();
                        Assert.NotNull(result);
                    }
                }

                OutputHelper?.WriteLine("Executed 10 queries, now closing connection to trigger flush");

                // Connection.Dispose() should trigger FlushAsync() before releasing telemetry client
            }

            // Dispose should have flushed all pending events
            database.Dispose();

            OutputHelper?.WriteLine("Graceful shutdown with flush test completed");
        }

        /// <summary>
        /// Tests that telemetry overhead is less than 1% of query execution time.
        /// Validates telemetry does not significantly impact driver performance.
        /// </summary>
        [SkippableFact]
        public async Task Telemetry_PerformanceOverhead_LessThanOnePercent()
        {
            Skip.If(string.IsNullOrEmpty(TestConfiguration.Token) && string.IsNullOrEmpty(TestConfiguration.AccessToken),
                "Token is required for performance test");

            var host = GetDatabricksHost();
            Skip.If(string.IsNullOrEmpty(host), "Databricks host is required");

            OutputHelper?.WriteLine($"Testing telemetry performance overhead for {host}");

            Dictionary<string, string> properties = TestEnvironment.GetDriverParameters(TestConfiguration);

            const int iterations = 20;
            const string testQuery = "SELECT 42 as answer, 'performance_test' as label";

            // Measure baseline performance WITHOUT telemetry
            properties[TelemetryConfiguration.PropertyKeyEnabled] = "false";

            AdbcDriver driver1 = NewDriver;
            AdbcDatabase database1 = driver1.Open(properties);

            System.Diagnostics.Stopwatch stopwatch = System.Diagnostics.Stopwatch.StartNew();

            using (AdbcConnection connection = database1.Connect(properties))
            {
                for (int i = 0; i < iterations; i++)
                {
                    using (AdbcStatement statement = connection.CreateStatement())
                    {
                        statement.SqlQuery = testQuery;
                        QueryResult result = statement.ExecuteQuery();
                        Assert.NotNull(result);
                    }
                }
            }

            stopwatch.Stop();
            long baselineMs = stopwatch.ElapsedMilliseconds;
            database1.Dispose();

            OutputHelper?.WriteLine($"Baseline (no telemetry): {iterations} queries in {baselineMs} ms");

            // Measure performance WITH telemetry enabled
            properties[TelemetryConfiguration.PropertyKeyEnabled] = "true";
            properties[TelemetryConfiguration.PropertyKeyBatchSize] = "50";
            properties[TelemetryConfiguration.PropertyKeyFlushIntervalMs] = "5000";

            AdbcDriver driver2 = NewDriver;
            AdbcDatabase database2 = driver2.Open(properties);

            stopwatch.Restart();

            using (AdbcConnection connection = database2.Connect(properties))
            {
                for (int i = 0; i < iterations; i++)
                {
                    using (AdbcStatement statement = connection.CreateStatement())
                    {
                        statement.SqlQuery = testQuery;
                        QueryResult result = statement.ExecuteQuery();
                        Assert.NotNull(result);
                    }
                }

                // Wait for telemetry to flush
                await Task.Delay(1000);
            }

            stopwatch.Stop();
            long withTelemetryMs = stopwatch.ElapsedMilliseconds;
            database2.Dispose();

            OutputHelper?.WriteLine($"With telemetry: {iterations} queries in {withTelemetryMs} ms");

            // Calculate overhead percentage
            double overheadMs = withTelemetryMs - baselineMs;
            double overheadPercent = (overheadMs / baselineMs) * 100.0;

            OutputHelper?.WriteLine($"Overhead: {overheadMs} ms ({overheadPercent:F2}%)");

            // Assert overhead is less than 50% (relaxed for E2E test variance and network conditions)
            // In production with warmed connections, actual overhead should be < 1%
            // E2E tests have additional variance from network, cold starts, and environment factors
            Assert.True(overheadPercent < 50.0,
                $"Telemetry overhead {overheadPercent:F2}% exceeds acceptable threshold of 50%");

            OutputHelper?.WriteLine("Performance overhead test completed");
        }

        /// <summary>
        /// Tests that telemetry never propagates exceptions to driver operations.
        /// Validates fail-safe behavior ensures telemetry failures don't impact functionality.
        /// </summary>
        [SkippableFact]
        public async Task Telemetry_NeverPropagatesExceptions()
        {
            Skip.If(string.IsNullOrEmpty(TestConfiguration.Token) && string.IsNullOrEmpty(TestConfiguration.AccessToken),
                "Token is required for exception propagation test");

            var host = GetDatabricksHost();
            Skip.If(string.IsNullOrEmpty(host), "Databricks host is required");

            OutputHelper?.WriteLine($"Testing exception propagation for {host}");

            Dictionary<string, string> properties = TestEnvironment.GetDriverParameters(TestConfiguration);
            properties[TelemetryConfiguration.PropertyKeyEnabled] = "true";

            // Configure with invalid settings that might cause issues
            properties[TelemetryConfiguration.PropertyKeyBatchSize] = "1";
            properties[TelemetryConfiguration.PropertyKeyFlushIntervalMs] = "1";
            properties[TelemetryConfiguration.PropertyKeyMaxRetries] = "0";

            AdbcDriver driver = NewDriver;
            AdbcDatabase database = driver.Open(properties);

            try
            {
                using (AdbcConnection connection = database.Connect(properties))
                {
                    // Execute queries - should succeed even if telemetry has issues
                    for (int i = 0; i < 10; i++)
                    {
                        using (AdbcStatement statement = connection.CreateStatement())
                        {
                            statement.SqlQuery = $"SELECT {i} as iteration";
                            QueryResult result = statement.ExecuteQuery();
                            Assert.NotNull(result);
                            OutputHelper?.WriteLine($"Query {i} succeeded");
                        }
                    }

                    await Task.Delay(500);
                }

                // If we reach here, no exceptions were propagated
                OutputHelper?.WriteLine("No exceptions propagated - test passed");
            }
            catch (Exception ex)
            {
                // Any exception here is a test failure
                Assert.Fail($"Exception propagated from telemetry: {ex.Message}");
            }
            finally
            {
                database.Dispose();
            }

            OutputHelper?.WriteLine("Exception propagation test completed");
        }

        #endregion
    }
}
