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
using AdbcDrivers.Databricks.Telemetry;
using Apache.Arrow.Adbc;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// Helper methods for creating Databricks connections with telemetry test injection.
    /// </summary>
    internal static class TelemetryTestHelpers
    {
        /// <summary>
        /// Creates a fresh <see cref="TelemetryClientManager"/> test instance scope and a
        /// <see cref="DatabricksConnection"/> with the given telemetry exporter factory injected.
        /// The returned scope must be disposed to restore the original TelemetryClientManager.
        /// </summary>
        /// <param name="parameters">The driver connection parameters.</param>
        /// <param name="exporterFactory">Factory function that creates the test exporter.</param>
        /// <returns>An open <see cref="AdbcConnection"/> with telemetry routed through the injected exporter.</returns>
        public static AdbcConnection CreateConnectionWithTelemetry(
            Dictionary<string, string> parameters,
            Func<ITelemetryExporter> exporterFactory)
        {
            // Merge with environment config and feature flags
            IReadOnlyDictionary<string, string> mergedProperties = MergeProperties(parameters);

            // Create the connection directly so we can inject TestExporterFactory before OpenAsync
            DatabricksConnection connection = new DatabricksConnection(mergedProperties);
            connection.TestExporterFactory = exporterFactory;

            try
            {
                connection.OpenAsync().Wait();
                connection.ApplyServerSidePropertiesAsync().Wait();
            }
            catch (Exception)
            {
                connection.Dispose();
                throw;
            }

            return connection;
        }

        /// <summary>
        /// Creates a <see cref="DatabricksConnection"/> with a <see cref="CapturingTelemetryExporter"/>
        /// injected, returning both the connection and the capturing exporter for test assertions.
        /// </summary>
        /// <param name="parameters">The driver connection parameters.</param>
        /// <returns>
        /// A tuple of (connection, capturingExporter) where the connection is open and all
        /// telemetry events are captured by the exporter.
        /// </returns>
        public static (AdbcConnection Connection, CapturingTelemetryExporter Exporter) CreateConnectionWithCapturingTelemetry(
            Dictionary<string, string> parameters)
        {
            CapturingTelemetryExporter exporter = new CapturingTelemetryExporter();
            AdbcConnection connection = CreateConnectionWithTelemetry(parameters, () => exporter);
            return (connection, exporter);
        }

        /// <summary>
        /// Replaces the global <see cref="TelemetryClientManager"/> singleton with a fresh
        /// isolated instance for test isolation. This ensures that each test gets its own
        /// TelemetryClient per host, and the test exporter factory is always called.
        /// </summary>
        /// <returns>An <see cref="IDisposable"/> that restores the original TelemetryClientManager.</returns>
        public static IDisposable UseFreshTelemetryClientManager()
        {
            TelemetryClientManager testManager = TelemetryClientManager.CreateForTesting();
            return TelemetryClientManager.UseTestInstance(testManager);
        }

        /// <summary>
        /// Merges properties with the environment config and feature flags,
        /// mirroring the logic in <c>DatabricksDatabase.Connect</c>.
        /// </summary>
        /// <param name="properties">Base connection properties.</param>
        /// <returns>Merged properties ready for connection construction.</returns>
        private static IReadOnlyDictionary<string, string> MergeProperties(Dictionary<string, string> properties)
        {
            // Merge with feature flags from server (cached per host)
            // This mimics DatabricksDatabase.MergeWithEnvironmentConfigAndFeatureFlags
            string assemblyVersion = FileVersionInfo.GetVersionInfo(typeof(DatabricksConnection).Assembly.Location).ProductVersion ?? string.Empty;
            return FeatureFlagCache.GetInstance()
                .MergePropertiesWithFeatureFlags(properties, assemblyVersion);
        }
    }
}
