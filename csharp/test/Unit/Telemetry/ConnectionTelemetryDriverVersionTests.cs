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

using System.Reflection;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Regression tests pinning that the telemetry <c>driver_version</c> reports the assembly's
    /// <see cref="AssemblyInformationalVersionAttribute"/> (e.g. <c>"1.1.4-SNAPSHOT+&lt;sha&gt;"</c>)
    /// rather than the bare numeric <c>AssemblyVersion</c> (e.g. <c>"1.1.4"</c>).
    ///
    /// <para>
    /// Background: <see cref="DatabricksConnection.s_assemblyVersion"/> is
    /// <c>Assembly.GetName().Version.ToString(3)</c> — purely numeric, so it drops the
    /// <c>-SNAPSHOT</c> prerelease label and the <c>+&lt;commit-sha&gt;</c> build metadata.
    /// Production lumberjack data shows builds reporting <c>"0.23.0-SNAPSHOT+&lt;sha&gt;"</c>
    /// (an informational version), which is what lets analysts pinpoint the exact build/commit
    /// and matches the JDBC driver's version format. Feeding the numeric version into telemetry
    /// makes every <c>1.1.4</c> build indistinguishable.
    /// </para>
    ///
    /// <para>
    /// The fix introduces <see cref="DatabricksConnection.s_telemetryDriverVersion"/>
    /// (= <c>ApacheUtility.GetAssemblyProductVersion</c>, the informational version) and feeds
    /// that into <c>ConnectionTelemetry.Create</c> instead of the numeric version.
    /// </para>
    /// </summary>
    public class ConnectionTelemetryDriverVersionTests
    {
        private static string? InformationalVersion =>
            typeof(DatabricksConnection).Assembly
                .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;

        [Fact]
        public void TelemetryDriverVersion_EqualsAssemblyInformationalVersion()
        {
            string? info = InformationalVersion;
            Assert.False(
                string.IsNullOrEmpty(info),
                "The Databricks driver assembly must carry an AssemblyInformationalVersion.");

            // The value fed to ConnectionTelemetry.Create (and therefore the telemetry payload's
            // driver_version) must be the informational version, not the numeric AssemblyVersion.
            Assert.Equal(info, DatabricksConnection.s_telemetryDriverVersion);
        }

        [Fact]
        public void TelemetryDriverVersion_IsRicherThanNumericAssemblyVersion()
        {
            string numeric = DatabricksConnection.s_assemblyVersion;          // e.g. "1.1.4"
            string telemetry = DatabricksConnection.s_telemetryDriverVersion; // e.g. "1.1.4-SNAPSHOT+<sha>"

            // The informational version is a superset of the numeric base version.
            Assert.StartsWith(numeric, telemetry);

            // VersionSuffix=SNAPSHOT (Directory.Build.props) makes every dev/CI build carry a
            // "-SNAPSHOT" (and usually "+<sha>") suffix. When that metadata is present, telemetry
            // must NOT collapse to the bare numeric version. This is the discriminating regression
            // guard: before the fix, driver_version was GetAssemblyVersion (numeric), so it would
            // have equalled `numeric` and this assertion would fail.
            // Use string overloads of Contains: the driver multi-targets net472/netstandard2.0,
            // where string.Contains(char) does not exist (only string.Contains(string)).
            bool hasMetadata = telemetry.Contains("-") || telemetry.Contains("+");
            if (hasMetadata)
            {
                Assert.NotEqual(numeric, telemetry);
            }
        }
    }
}
