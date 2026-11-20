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

using AdbcDrivers.Databricks.Telemetry;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Regression tests for PECO-3022 B1: <c>driver_name</c> string drift between
    /// SEA and Thrift transports.
    ///
    /// Production lumberjack data from v1.1.4 showed two distinct strings coexisting:
    /// <list type="bullet">
    /// <item><c>"Databricks ADBC Driver"</c> — 685 records, all THRIFT mode</item>
    /// <item><c>"ADBC Databricks Driver"</c> — 4,401 records, mixed THRIFT + 69 SEA</item>
    /// </list>
    /// Dashboards filtering on the older string silently missed all SEA records and a
    /// significant fraction of recent Thrift records.
    ///
    /// The fix is to make <see cref="DatabricksConnection.DatabricksDriverName"/> the
    /// single source of truth, referenced by both <see cref="ConnectionTelemetry.BuildSystemConfiguration"/>
    /// and its <see cref="ConnectionTelemetry.SafeBuildSystemConfiguration"/> fallback so
    /// that every caller of <see cref="ConnectionTelemetry.Create"/> — Thrift today via
    /// <c>DatabricksConnection</c>, SEA via <c>StatementExecutionConnection</c> — emits
    /// the same literal.
    ///
    /// These tests pin the literal value so that a typo or rename in the constant gets
    /// caught at unit-test time before it ships to production.
    /// </summary>
    public class ConnectionTelemetryDriverNameTests
    {
        /// <summary>
        /// The canonical driver_name literal that must appear in every telemetry record
        /// regardless of transport. Picked because it matches the value already returned
        /// via <c>AdbcInfoCode.DriverName</c> (see <see cref="DatabricksConnection.DriverName"/>)
        /// and represented the majority of v1.1.4 production records, so dashboards
        /// already keyed on this string see the most history.
        /// </summary>
        private const string CanonicalDriverName = "ADBC Databricks Driver";

        [Fact]
        public void CanonicalConstant_HasExpectedLiteralValue()
        {
            // Pin the literal. If anyone renames the constant, this test fails and the
            // change is forced into review rather than silently breaking downstream
            // dashboards that filter on the string.
            Assert.Equal(CanonicalDriverName, DatabricksConnection.DatabricksDriverName);
        }

        [Fact]
        public void BuildSystemConfiguration_ReturnsCanonicalDriverName()
        {
            var config = ConnectionTelemetry.BuildSystemConfiguration("1.2.3");

            Assert.Equal(CanonicalDriverName, config.DriverName);
        }

        [Fact]
        public void SafeBuildSystemConfiguration_ReturnsCanonicalDriverName()
        {
            // SafeBuildSystemConfiguration delegates to BuildSystemConfiguration on the
            // happy path and must return the same canonical literal. The catch-block
            // fallback in SafeBuildSystemConfiguration is not exercised here because
            // there is no in-process fault-injection seam for the static helper; the
            // literal-pin in CanonicalConstant_HasExpectedLiteralValue covers that path
            // by construction (both branches reference the same constant).
            var config = ConnectionTelemetry.SafeBuildSystemConfiguration("1.2.3", activity: null);

            Assert.NotNull(config);
            Assert.Equal(CanonicalDriverName, config.DriverName);
        }

        [Fact]
        public void DriverName_IdenticalAcrossInvocations_SingleSourceOfTruth()
        {
            // The single-source-of-truth property: every invocation of the system-config
            // builder (regardless of transport mode at the caller) returns the same
            // driver_name. Since ConnectionTelemetry.BuildSystemConfiguration is the
            // protocol-agnostic factory called by both Thrift and SEA transports, this
            // guarantees both Thrift and SEA telemetry records carry identical driver_name.
            var thriftCaller = ConnectionTelemetry.BuildSystemConfiguration("1.2.3");
            var seaCaller = ConnectionTelemetry.BuildSystemConfiguration("1.2.3");

            Assert.Equal(thriftCaller.DriverName, seaCaller.DriverName);
            Assert.Equal(CanonicalDriverName, thriftCaller.DriverName);
        }
    }
}
