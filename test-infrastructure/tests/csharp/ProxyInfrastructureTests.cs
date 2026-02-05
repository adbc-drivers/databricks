/*
 * Copyright (c) 2025 ADBC Drivers Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.ThriftProtocol
{
    /// <summary>
    /// Tests that validate the proxy server infrastructure integration works correctly.
    /// These tests verify that the proxy server can be started, controlled, and used by the driver.
    /// </summary>
    public class ProxyInfrastructureTests : ProxyTestBase
    {
        [Fact]
        public async Task ListScenarios_ReturnsAvailableScenarios()
        {
            // Act
            var scenarios = await ControlClient.ListScenariosAsync();

            // Assert
            Assert.NotNull(scenarios);
            Assert.NotEmpty(scenarios);

            // Verify we have the expected CloudFetch scenarios from mitmproxy addon
            Assert.Contains(scenarios, s => s.Name == "cloudfetch_expired_link");
            Assert.Contains(scenarios, s => s.Name == "cloudfetch_400");
            Assert.Contains(scenarios, s => s.Name == "cloudfetch_403");
            Assert.Contains(scenarios, s => s.Name == "cloudfetch_404");
            Assert.Contains(scenarios, s => s.Name == "cloudfetch_405");
            Assert.Contains(scenarios, s => s.Name == "cloudfetch_412");
            Assert.Contains(scenarios, s => s.Name == "cloudfetch_500");
            Assert.Contains(scenarios, s => s.Name == "cloudfetch_503");
            Assert.Contains(scenarios, s => s.Name == "cloudfetch_timeout");
            Assert.Contains(scenarios, s => s.Name == "cloudfetch_connection_reset");
        }

        [Fact]
        public async Task EnableDisableScenario_TogglesScenarioState()
        {
            // Arrange
            const string scenarioName = "cloudfetch_expired_link";

            // Act - Enable
            var enableResult = await ControlClient.EnableScenarioAsync(scenarioName);
            var statusAfterEnable = await ControlClient.GetScenarioStatusAsync(scenarioName);

            // Assert - Enabled
            Assert.True(enableResult);
            Assert.NotNull(statusAfterEnable);
            Assert.True(statusAfterEnable.Enabled);

            // Act - Disable
            var disableResult = await ControlClient.DisableScenarioAsync(scenarioName);
            var statusAfterDisable = await ControlClient.GetScenarioStatusAsync(scenarioName);

            // Assert - Disabled
            Assert.False(disableResult);
            Assert.NotNull(statusAfterDisable);
            Assert.False(statusAfterDisable.Enabled);
        }

        [Fact]
        public async Task DisableAllScenarios_DisablesAllEnabledScenarios()
        {
            // Arrange - Enable a few scenarios
            await ControlClient.EnableScenarioAsync("cloudfetch_expired_link");
            await ControlClient.EnableScenarioAsync("cloudfetch_timeout");

            // Act
            await ControlClient.DisableAllScenariosAsync();

            // Assert
            var scenarios = await ControlClient.ListScenariosAsync();
            Assert.NotEmpty(scenarios);
            Assert.True(scenarios.All(s => !s.Enabled), "All scenarios should be disabled");
        }

        [Fact]
        public void ProxiedConnection_CanConnectThroughProxy()
        {
            // Act
            using var connection = CreateProxiedConnection();

            // Assert - Connection should succeed
            Assert.NotNull(connection);

            // Execute a simple query to verify connectivity through proxy
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 1 as test_value";

            var result = statement.ExecuteQuery();
            Assert.NotNull(result);

            using var reader = result.Stream;
            Assert.NotNull(reader);

            // Verify the schema
            var schema = reader.Schema;
            Assert.NotNull(schema);
            Assert.Single(schema.FieldsList);
            Assert.Equal("test_value", schema.FieldsList[0].Name);

            // Verify we can read the result
            var batch = reader.ReadNextRecordBatchAsync().GetAwaiter().GetResult();
            Assert.NotNull(batch);
            Assert.Equal(1, batch.Length);
        }
    }
}
