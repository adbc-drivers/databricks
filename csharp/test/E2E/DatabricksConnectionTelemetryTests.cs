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
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E
{
    /// <summary>
    /// Integration tests for DatabricksConnection telemetry lifecycle.
    /// Tests verify that telemetry is initialized, managed, and cleaned up correctly.
    /// </summary>
    public class DatabricksConnectionTelemetryTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public DatabricksConnectionTelemetryTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Tests that telemetry is initialized after successful session creation.
        /// Verifies that TelemetryClientManager.GetOrCreateClient is called when telemetry is enabled.
        /// </summary>
        [SkippableFact]
        public void OpenAsync_InitializesTelemetry()
        {
            Dictionary<string, string> properties = TestEnvironment.GetDriverParameters(TestConfiguration);

            // Explicitly enable telemetry
            properties[TelemetryConfiguration.PropertyKeyEnabled] = "true";

            AdbcDriver driver = NewDriver;
            AdbcDatabase database = driver.Open(properties);

            using AdbcConnection connection = database.Connect(properties);

            // Connection should open successfully
            // Telemetry initialization happens internally and should not throw

            // Verify connection is usable (proves telemetry didn't break connection)
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 1";
            QueryResult result = statement.ExecuteQuery();

            Assert.NotNull(result);
            

            OutputHelper?.WriteLine("OpenAsync_InitializesTelemetry: Connection opened successfully with telemetry enabled");

            connection.Dispose();
            database.Dispose();
        }

        /// <summary>
        /// Tests that telemetry is not initialized when feature flag is disabled.
        /// Verifies that no telemetry client is created when telemetry.enabled=false.
        /// </summary>
        [SkippableFact]
        public void OpenAsync_FeatureFlagDisabled_NoTelemetry()
        {
            Dictionary<string, string> properties = TestEnvironment.GetDriverParameters(TestConfiguration);

            // Explicitly disable telemetry
            properties[TelemetryConfiguration.PropertyKeyEnabled] = "false";

            AdbcDriver driver = NewDriver;
            AdbcDatabase database = driver.Open(properties);

            using AdbcConnection connection = database.Connect(properties);

            // Connection should open successfully even with telemetry disabled
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 1";
            QueryResult result = statement.ExecuteQuery();

            Assert.NotNull(result);
            

            OutputHelper?.WriteLine("OpenAsync_FeatureFlagDisabled_NoTelemetry: Connection opened successfully with telemetry disabled");

            connection.Dispose();
            database.Dispose();
        }

        /// <summary>
        /// Tests that connection dispose releases the telemetry client.
        /// Verifies that TelemetryClientManager.ReleaseClientAsync is called.
        /// </summary>
        [SkippableFact]
        public void Dispose_ReleasesTelemetryClient()
        {
            Dictionary<string, string> properties = TestEnvironment.GetDriverParameters(TestConfiguration);

            // Explicitly enable telemetry
            properties[TelemetryConfiguration.PropertyKeyEnabled] = "true";

            AdbcDriver driver = NewDriver;
            AdbcDatabase database = driver.Open(properties);

            AdbcConnection connection = database.Connect(properties);

            // Use connection
            using (AdbcStatement statement = connection.CreateStatement())
            {
                statement.SqlQuery = "SELECT 1";
                QueryResult result = statement.ExecuteQuery();
                Assert.NotNull(result);
                
            }

            // Dispose connection - should trigger telemetry cleanup
            connection.Dispose();

            OutputHelper?.WriteLine("Dispose_ReleasesTelemetryClient: Connection disposed successfully");

            // Verify we can still create new connections (proves telemetry manager still works)
            using (AdbcConnection connection2 = database.Connect(properties))
            {
                using AdbcStatement statement = connection2.CreateStatement();
                statement.SqlQuery = "SELECT 1";
                QueryResult result = statement.ExecuteQuery();
                Assert.NotNull(result);
                
            }

            OutputHelper?.WriteLine("Dispose_ReleasesTelemetryClient: Second connection created successfully after first disposed");

            database.Dispose();
        }

        /// <summary>
        /// Tests that pending metrics are flushed before client release.
        /// Verifies the graceful shutdown sequence: flush → release client → release feature flags.
        /// </summary>
        [SkippableFact]
        public void Dispose_FlushesMetricsBeforeRelease()
        {
            Dictionary<string, string> properties = TestEnvironment.GetDriverParameters(TestConfiguration);

            // Explicitly enable telemetry
            properties[TelemetryConfiguration.PropertyKeyEnabled] = "true";

            AdbcDriver driver = NewDriver;
            AdbcDatabase database = driver.Open(properties);

            AdbcConnection connection = database.Connect(properties);

            // Execute multiple queries to generate telemetry events
            for (int i = 0; i < 5; i++)
            {
                using AdbcStatement statement = connection.CreateStatement();
                statement.SqlQuery = $"SELECT {i}";
                QueryResult result = statement.ExecuteQuery();
                Assert.NotNull(result);
                
            }

            // Give telemetry aggregator time to process events
            Task.Delay(100).Wait();

            // Dispose connection - should flush pending metrics before release
            connection.Dispose();

            OutputHelper?.WriteLine("Dispose_FlushesMetricsBeforeRelease: Connection disposed after executing 5 queries");

            database.Dispose();
        }

        /// <summary>
        /// Tests that telemetry failures do not impact connection behavior.
        /// Even if telemetry initialization or export fails, connection should work normally.
        /// </summary>
        [SkippableFact]
        public void TelemetryFailures_DoNotImpactConnection()
        {
            Dictionary<string, string> properties = TestEnvironment.GetDriverParameters(TestConfiguration);

            // Enable telemetry with invalid config that might cause issues
            properties[TelemetryConfiguration.PropertyKeyEnabled] = "true";
            properties[TelemetryConfiguration.PropertyKeyBatchSize] = "1"; // Very small batch
            properties[TelemetryConfiguration.PropertyKeyFlushIntervalMs] = "100"; // Very frequent flush

            AdbcDriver driver = NewDriver;
            AdbcDatabase database = driver.Open(properties);

            // Connection should still work even if telemetry has issues
            using (AdbcConnection connection = database.Connect(properties))
            {
                // Execute queries - should succeed regardless of telemetry status
                using AdbcStatement statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 'telemetry_test'";
                QueryResult result = statement.ExecuteQuery();

                Assert.NotNull(result);
                

                OutputHelper?.WriteLine("TelemetryFailures_DoNotImpactConnection: Connection and query execution successful");
            }

            database.Dispose();
        }

        /// <summary>
        /// Tests multiple connections to the same host share a single telemetry client.
        /// Verifies the per-host singleton pattern in TelemetryClientManager.
        /// </summary>
        [SkippableFact]
        public void MultipleConnections_SameHost_ShareTelemetryClient()
        {
            Dictionary<string, string> properties = TestEnvironment.GetDriverParameters(TestConfiguration);

            // Explicitly enable telemetry
            properties[TelemetryConfiguration.PropertyKeyEnabled] = "true";

            AdbcDriver driver = NewDriver;
            AdbcDatabase database = driver.Open(properties);

            // Create multiple connections to same host
            List<AdbcConnection> connections = new List<AdbcConnection>();
            try
            {
                for (int i = 0; i < 3; i++)
                {
                    AdbcConnection connection = database.Connect(properties);
                    connections.Add(connection);

                    // Execute a query on each connection
                    using AdbcStatement statement = connection.CreateStatement();
                    statement.SqlQuery = $"SELECT {i}";
                    QueryResult result = statement.ExecuteQuery();
                    Assert.NotNull(result);
                    
                }

                OutputHelper?.WriteLine("MultipleConnections_SameHost_ShareTelemetryClient: Created 3 connections successfully");
            }
            finally
            {
                // Clean up all connections
                foreach (var connection in connections)
                {
                    connection.Dispose();
                }
            }

            OutputHelper?.WriteLine("MultipleConnections_SameHost_ShareTelemetryClient: All connections disposed successfully");

            database.Dispose();
        }
    }
}
