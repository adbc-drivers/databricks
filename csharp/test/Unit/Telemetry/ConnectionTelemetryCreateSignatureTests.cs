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

using System.Collections.Generic;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.HiveServer2.Spark;
using DriverModeType = AdbcDrivers.Databricks.Telemetry.Proto.DriverMode.Types.Type;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for PECO-3022 (TELEM/SEA T1): <see cref="ConnectionTelemetry.Create"/>
    /// is now transport-agnostic — it takes a <c>string sessionId</c> (converted at the
    /// caller's boundary) and a <c>DriverMode.Types.Type mode</c> threaded through to
    /// <c>driver_connection_params.mode</c>. The two formerly hardcoded
    /// <c>DriverMode.Types.Type.Thrift</c> literals in <c>BuildDriverConnectionParams</c>
    /// and the fallback in <c>SafeBuildDriverConnectionParams</c> are gone; the mode is
    /// always the value supplied by the caller (THRIFT for Thrift, SEA for the upcoming
    /// SEA transport).
    /// </summary>
    public class ConnectionTelemetryCreateSignatureTests
    {
        private const string AssemblyVersion = "1.2.3-test";
        private const int DefaultTimeoutMs = 30_000;

        private static IReadOnlyDictionary<string, string> TelemetryEnabledProperties() =>
            new Dictionary<string, string>
            {
                { TelemetryConfiguration.PropertyKeyEnabled, "true" },
                { SparkParameters.AuthType, SparkAuthTypeConstants.Token },
                { SparkParameters.Token, "dapi-redacted" },
                { SparkParameters.Path, "/sql/1.0/warehouses/abc123" },
            };

        // Tests share a TelemetryClient cache keyed by host (TelemetryClientManager
        // singleton). Use distinct hosts per test to keep them isolated.
        [Fact]
        public async Task Create_AcceptsStringSessionId()
        {
            // Regression: the original signature took TSessionHandle?, forcing the
            // (Thrift) caller to leak its transport handle through telemetry. The new
            // signature accepts the already-stringified id so the SEA caller can pass
            // its server-assigned id without inventing a fake TSessionHandle.
            const string Host = "create-string-sid.databricks.com";
            const string SessionId = "9e6a3f88-1234-4321-abcd-deadbeefcafe";

            IConnectionTelemetry telemetry = ConnectionTelemetry.Create(
                properties: TelemetryEnabledProperties(),
                host: Host,
                assemblyVersion: AssemblyVersion,
                oauthTokenProvider: null,
                sessionId: SessionId,
                mode: DriverModeType.Thrift,
                enableDirectResults: true,
                enableComplexDatatypeSupport: false,
                connectTimeoutMilliseconds: DefaultTimeoutMs,
                activity: null);

            try
            {
                Assert.NotNull(telemetry.Session);
                Assert.Equal(SessionId, telemetry.Session!.SessionId);
            }
            finally
            {
                await telemetry.DisposeAsync();
            }
        }

        [Fact]
        public async Task Create_EmptySessionId_MapsToNullInContext()
        {
            // ConnectionTelemetry.Create maps `string.Empty` -> `SessionId = null` in the
            // resulting TelemetrySessionContext. This matters because Create is called
            // from InitializeTelemetry before OpenSession returns a real handle on some
            // code paths, and the DatabricksConnection caller passes string.Empty rather
            // than null in that window. Pin the mapping so a future refactor that drops
            // the `!string.IsNullOrEmpty` guard at ConnectionTelemetry.cs would surface
            // here, rather than silently emitting empty-string SessionId to lumberjack.
            const string Host = "create-empty-sid.databricks.com";

            IConnectionTelemetry telemetry = ConnectionTelemetry.Create(
                properties: TelemetryEnabledProperties(),
                host: Host,
                assemblyVersion: AssemblyVersion,
                oauthTokenProvider: null,
                sessionId: string.Empty,
                mode: DriverModeType.Thrift,
                enableDirectResults: true,
                enableComplexDatatypeSupport: false,
                connectTimeoutMilliseconds: DefaultTimeoutMs,
                activity: null);

            try
            {
                Assert.NotNull(telemetry.Session);
                Assert.Null(telemetry.Session!.SessionId);
            }
            finally
            {
                await telemetry.DisposeAsync();
            }
        }

        [Fact]
        public async Task Create_ThriftMode_SetsDriverModeThrift()
        {
            // Regression for the literal that used to live at ConnectionTelemetry.cs:642
            // — `Mode = DriverMode.Types.Type.Thrift` is now threaded from the caller.
            const string Host = "create-thrift-mode.databricks.com";

            IConnectionTelemetry telemetry = ConnectionTelemetry.Create(
                properties: TelemetryEnabledProperties(),
                host: Host,
                assemblyVersion: AssemblyVersion,
                oauthTokenProvider: null,
                sessionId: "session-thrift",
                mode: DriverModeType.Thrift,
                enableDirectResults: true,
                enableComplexDatatypeSupport: false,
                connectTimeoutMilliseconds: DefaultTimeoutMs,
                activity: null);

            try
            {
                Assert.NotNull(telemetry.Session);
                Assert.NotNull(telemetry.Session!.DriverConnectionParams);
                Assert.Equal(DriverModeType.Thrift, telemetry.Session.DriverConnectionParams!.Mode);
            }
            finally
            {
                await telemetry.DisposeAsync();
            }
        }

        [Fact]
        public async Task Create_SeaMode_SetsDriverModeSea()
        {
            // The reason this refactor exists: the SEA telemetry caller (added in a later
            // phase) must produce telemetry rows with `driver_connection_params.mode = SEA`.
            const string Host = "create-sea-mode.databricks.com";

            IConnectionTelemetry telemetry = ConnectionTelemetry.Create(
                properties: TelemetryEnabledProperties(),
                host: Host,
                assemblyVersion: AssemblyVersion,
                oauthTokenProvider: null,
                sessionId: "session-sea",
                mode: DriverModeType.Sea,
                enableDirectResults: true,
                enableComplexDatatypeSupport: false,
                connectTimeoutMilliseconds: DefaultTimeoutMs,
                activity: null);

            try
            {
                Assert.NotNull(telemetry.Session);
                Assert.NotNull(telemetry.Session!.DriverConnectionParams);
                Assert.Equal(DriverModeType.Sea, telemetry.Session.DriverConnectionParams!.Mode);
            }
            finally
            {
                await telemetry.DisposeAsync();
            }
        }

        [Fact]
        public void Create_ThrowingHttpClient_ReturnsNoOpConnectionTelemetry()
        {
            // Create() is declared `Never throws`: any initialization failure — HttpClient
            // construction, exporter wire-up, etc. — must surface as NoOpConnectionTelemetry
            // rather than propagate into the connection-open path. We exercise this by
            // enabling telemetry while passing a blank host so `new Uri("https://")` (inside
            // HttpClientFactory.CreateTelemetryHttpClient) and/or
            // TelemetryClientManager.GetOrCreateClient's argument-check throw, both of
            // which land in Create's outer catch.
            //
            // ASSUMPTION: this test depends on either HttpClientFactory.CreateTelemetryHttpClient
            // or TelemetryClientManager.GetOrCreateClient throwing when host is empty. If a
            // future change adds defensive handling of empty host upstream of Create's catch,
            // this test would silently pass for the wrong reason (Create would return
            // NoOpConnectionTelemetry via the disabled/feature-flag path instead of the
            // outer catch). When that happens, swap to a real fault-injection seam (e.g.,
            // an internal overload that accepts a pre-built HttpClient).
            IConnectionTelemetry telemetry = ConnectionTelemetry.Create(
                properties: TelemetryEnabledProperties(),
                host: string.Empty,
                assemblyVersion: AssemblyVersion,
                oauthTokenProvider: null,
                sessionId: "session-throwing-http",
                mode: DriverModeType.Thrift,
                enableDirectResults: true,
                enableComplexDatatypeSupport: false,
                connectTimeoutMilliseconds: DefaultTimeoutMs,
                activity: null);

            Assert.Same(NoOpConnectionTelemetry.Instance, telemetry);
            Assert.Null(telemetry.Session);
        }
    }
}
