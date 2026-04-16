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
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// E2E tests for auth_type field population in telemetry.
    /// Tests that auth_type is correctly set based on authentication method: 'pat', 'oauth-m2m', 'oauth-u2m', 'other'
    /// </summary>
    public class AuthTypeTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public AuthTypeTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Tests that auth_type is set to 'pat' when using Personal Access Token authentication.
        /// </summary>
        [SkippableFact]
        public async Task AuthType_PAT_SetsToPat()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);

                // Ensure PAT authentication is configured
                // The test configuration should have a token set
                if (!properties.ContainsKey(SparkParameters.Token))
                {
                    Skip.If(true, "Test requires PAT authentication (token) to be configured");
                }

                // Remove any OAuth settings to ensure PAT auth is used
                properties.Remove(DatabricksParameters.OAuthGrantType);
                properties.Remove(DatabricksParameters.OAuthClientId);
                properties.Remove(DatabricksParameters.OAuthClientSecret);

                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Execute a simple query to trigger telemetry
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 1 AS test_value";
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;

                statement.Dispose();

                // Wait for telemetry to be captured
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);
                TelemetryTestHelpers.AssertLogCount(logs, 1);

                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Assert auth_type is set to "pat"
                Assert.NotNull(protoLog);
                Assert.Equal("pat", protoLog.AuthType);

                OutputHelper?.WriteLine($"✓ auth_type correctly set to: {protoLog.AuthType}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that auth_type is set to 'oauth-client_credentials' when using OAuth client_credentials flow.
        /// </summary>
        [SkippableFact]
        public async Task AuthType_OAuthClientCredentials_SetsToOAuthClientCredentials()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);

                // Check if OAuth M2M is configured in the test environment
                if (!properties.ContainsKey(DatabricksParameters.OAuthClientId) ||
                    !properties.ContainsKey(DatabricksParameters.OAuthClientSecret))
                {
                    Skip.If(true, "Test requires OAuth M2M authentication (client_id and client_secret) to be configured");
                }

                // Ensure OAuth client_credentials grant type is set
                properties[DatabricksParameters.OAuthGrantType] = DatabricksConstants.OAuthGrantTypes.ClientCredentials;
                properties[SparkParameters.AuthType] = "oauth";

                // Remove PAT token if present
                properties.Remove(SparkParameters.Token);

                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Execute a simple query to trigger telemetry
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 1 AS test_value";
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;

                statement.Dispose();

                // Wait for telemetry to be captured
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);
                TelemetryTestHelpers.AssertLogCount(logs, 1);

                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Assert auth_type is set to "oauth-client_credentials"
                Assert.NotNull(protoLog);
                Assert.Equal("oauth-client_credentials", protoLog.AuthType);

                OutputHelper?.WriteLine($"✓ auth_type correctly set to: {protoLog.AuthType}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that auth_type is set to 'oauth-access_token' when using OAuth access_token flow.
        /// </summary>
        [SkippableFact]
        public async Task AuthType_OAuthAccessToken_SetsToOAuthAccessToken()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);

                // Check if OAuth access token is configured
                if (!properties.ContainsKey(SparkParameters.AccessToken))
                {
                    Skip.If(true, "Test requires OAuth U2M authentication (access_token) to be configured");
                }

                // Ensure OAuth access_token grant type is set
                properties[DatabricksParameters.OAuthGrantType] = DatabricksConstants.OAuthGrantTypes.AccessToken;
                properties[SparkParameters.AuthType] = "oauth";

                // Remove PAT token and OAuth M2M credentials if present
                properties.Remove(SparkParameters.Token);
                properties.Remove(DatabricksParameters.OAuthClientId);
                properties.Remove(DatabricksParameters.OAuthClientSecret);

                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Execute a simple query to trigger telemetry
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 1 AS test_value";
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;

                statement.Dispose();

                // Wait for telemetry to be captured
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);
                TelemetryTestHelpers.AssertLogCount(logs, 1);

                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Assert auth_type is set to "oauth-access_token"
                Assert.NotNull(protoLog);
                Assert.Equal("oauth-access_token", protoLog.AuthType);

                OutputHelper?.WriteLine($"✓ auth_type correctly set to: {protoLog.AuthType}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that auth_type is set to 'other' when no recognized authentication is configured.
        /// </summary>
        [SkippableFact]
        public async Task AuthType_NoAuth_SetsToOther()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);

                // Remove all authentication credentials to test 'other' fallback
                properties.Remove(SparkParameters.Token);
                properties.Remove(SparkParameters.AccessToken);
                properties.Remove(DatabricksParameters.OAuthGrantType);
                properties.Remove(DatabricksParameters.OAuthClientId);
                properties.Remove(DatabricksParameters.OAuthClientSecret);

                // This test might fail to connect if auth is required
                // We'll skip if connection fails
                try
                {
                    (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);
                }
                catch
                {
                    Skip.If(true, "Connection requires authentication - cannot test 'other' auth type");
                }

                // Execute a simple query to trigger telemetry
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 1 AS test_value";
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;

                statement.Dispose();

                // Wait for telemetry to be captured
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);
                TelemetryTestHelpers.AssertLogCount(logs, 1);

                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Assert auth_type is set to "other"
                Assert.NotNull(protoLog);
                Assert.Equal("other", protoLog.AuthType);

                OutputHelper?.WriteLine($"✓ auth_type correctly set to: {protoLog.AuthType}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }

        /// <summary>
        /// Tests that auth_type field is always populated (never null or empty) for any connection.
        /// </summary>
        [SkippableFact]
        public async Task AuthType_AlwaysPopulated()
        {
            CapturingTelemetryExporter exporter = null!;
            AdbcConnection? connection = null;

            try
            {
                var properties = TestEnvironment.GetDriverParameters(TestConfiguration);
                (connection, exporter) = TelemetryTestHelpers.CreateConnectionWithCapturingTelemetry(properties);

                // Execute a simple query to trigger telemetry
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 1 AS test_value";
                var result = statement.ExecuteQuery();
                using var reader = result.Stream;

                statement.Dispose();

                // Wait for telemetry to be captured
                var logs = await TelemetryTestHelpers.WaitForTelemetryEvents(exporter, expectedCount: 1);
                TelemetryTestHelpers.AssertLogCount(logs, 1);

                var protoLog = TelemetryTestHelpers.GetProtoLog(logs[0]);

                // Assert auth_type is populated
                Assert.NotNull(protoLog);
                Assert.False(string.IsNullOrEmpty(protoLog.AuthType), "auth_type should never be null or empty");

                // Assert it's one of the expected values
                var validAuthTypes = new[] { "pat", "oauth-client_credentials", "oauth-access_token", "other" };
                Assert.Contains(protoLog.AuthType, validAuthTypes);

                OutputHelper?.WriteLine($"✓ auth_type populated with valid value: {protoLog.AuthType}");
            }
            finally
            {
                connection?.Dispose();
                TelemetryTestHelpers.ClearExporterOverride();
            }
        }
    }
}
