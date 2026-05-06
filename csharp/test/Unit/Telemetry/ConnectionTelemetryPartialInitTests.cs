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
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.HiveServer2.Spark;
using DriverAuthFlowType = AdbcDrivers.Databricks.Telemetry.Proto.DriverAuthFlow.Types.Type;
using DriverAuthMechType = AdbcDrivers.Databricks.Telemetry.Proto.DriverAuthMech.Types.Type;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Regression tests for PECO-2998 (TELEM-17) and PECO-2996 (TELEM-18).
    ///
    /// PECO-2998 — top-level <c>auth_type</c> was missing on ~90 OAuth+ClientCredentials rows
    /// even though the nested <c>auth_mech=OAUTH</c>/<c>auth_flow=CLIENT_CREDENTIALS</c> were
    /// populated. Root cause: <see cref="ConnectionTelemetry.BuildDriverConnectionParams"/>
    /// keyed off <c>SparkParameters.AuthType</c> while <c>DetermineAuthType</c> keyed off
    /// <c>OAuthGrantType</c> alone, so the two could disagree. Both are now derived from a
    /// single helper.
    ///
    /// PECO-2996 — the same ~110-row gap appeared in <c>runtime_vendor</c>, <c>os_arch</c>,
    /// <c>enable_arrow</c>, <c>enable_direct_results</c>, <c>socket_timeout</c>, and
    /// <c>auto_commit</c>. Root cause: a single throw inside the system-configuration or
    /// connection-parameters builder propagated to the outer NoOp catch and dropped all
    /// telemetry for the connection, while always-derivable constants like
    /// <c>runtime_vendor="Microsoft"</c> should still have been emitted.
    /// </summary>
    public class ConnectionTelemetryPartialInitTests
    {
        private const string Host = "test.databricks.com";
        private const int DefaultTimeoutMs = 30_000;

        // ── PECO-2998: top-level auth_type and nested auth_mech/auth_flow agree ────────────

        [Fact]
        public void AuthType_AndAuthMech_Consistent_OauthClientCredentials()
        {
            // The 90-row drift case: oauth + client_credentials grant.
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.AuthType, SparkAuthTypeConstants.OAuth },
                { DatabricksParameters.OAuthGrantType, DatabricksConstants.OAuthGrantTypes.ClientCredentials },
                { DatabricksParameters.OAuthClientId, "client-id" },
                { DatabricksParameters.OAuthClientSecret, "client-secret" },
            };

            var connParams = ConnectionTelemetry.BuildDriverConnectionParams(
                properties, Host, enableDirectResults: true, useDescTableExtended: true, DefaultTimeoutMs);
            string authType = ConnectionTelemetry.DetermineAuthType(properties);

            Assert.Equal(DriverAuthMechType.Oauth, connParams.AuthMech);
            Assert.Equal(DriverAuthFlowType.ClientCredentials, connParams.AuthFlow);
            Assert.Equal("oauth-client_credentials", authType);
        }

        [Fact]
        public void AuthType_AndAuthMech_Consistent_OauthAccessTokenWithGrantType()
        {
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.AuthType, SparkAuthTypeConstants.OAuth },
                { DatabricksParameters.OAuthGrantType, DatabricksConstants.OAuthGrantTypes.AccessToken },
                { SparkParameters.AccessToken, "oauth-access-token-redacted" },
            };

            var connParams = ConnectionTelemetry.BuildDriverConnectionParams(
                properties, Host, enableDirectResults: true, useDescTableExtended: true, DefaultTimeoutMs);
            string authType = ConnectionTelemetry.DetermineAuthType(properties);

            Assert.Equal(DriverAuthMechType.Oauth, connParams.AuthMech);
            Assert.Equal(DriverAuthFlowType.TokenPassthrough, connParams.AuthFlow);
            Assert.Equal("oauth-access_token", authType);
        }

        [Fact]
        public void AuthType_AndAuthMech_Consistent_OauthAccessTokenPassthrough_NoGrantType()
        {
            // OAuth U2M token passthrough with no grant_type set. Pre-PECO-2998 the top-level
            // auth_type came back as "pat" (because Token might be present) or "other", which
            // disagreed with auth_mech=OAUTH. The fix derives auth_type from the same branch
            // as auth_mech, so when auth_type=oauth the top-level field is "oauth-*".
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.AuthType, SparkAuthTypeConstants.OAuth },
                { SparkParameters.AccessToken, "oauth-access-token-redacted" },
            };

            var connParams = ConnectionTelemetry.BuildDriverConnectionParams(
                properties, Host, enableDirectResults: true, useDescTableExtended: true, DefaultTimeoutMs);
            string authType = ConnectionTelemetry.DetermineAuthType(properties);

            Assert.Equal(DriverAuthMechType.Oauth, connParams.AuthMech);
            Assert.Equal(DriverAuthFlowType.TokenPassthrough, connParams.AuthFlow);
            Assert.StartsWith("oauth-", authType);
        }

        [Fact]
        public void AuthType_AndAuthMech_Consistent_PatToken()
        {
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.AuthType, SparkAuthTypeConstants.Token },
                { SparkParameters.Token, "dapi-redacted" },
            };

            var connParams = ConnectionTelemetry.BuildDriverConnectionParams(
                properties, Host, enableDirectResults: true, useDescTableExtended: true, DefaultTimeoutMs);
            string authType = ConnectionTelemetry.DetermineAuthType(properties);

            Assert.Equal(DriverAuthMechType.Pat, connParams.AuthMech);
            Assert.Equal(DriverAuthFlowType.TokenPassthrough, connParams.AuthFlow);
            Assert.Equal("pat", authType);
        }

        [Fact]
        public void AuthType_AndAuthMech_Consistent_NoAuth()
        {
            var properties = new Dictionary<string, string>();

            var connParams = ConnectionTelemetry.BuildDriverConnectionParams(
                properties, Host, enableDirectResults: true, useDescTableExtended: true, DefaultTimeoutMs);
            string authType = ConnectionTelemetry.DetermineAuthType(properties);

            Assert.Equal(DriverAuthMechType.Pat, connParams.AuthMech);
            Assert.Equal(DriverAuthFlowType.TokenPassthrough, connParams.AuthFlow);
            Assert.Equal("other", authType);
        }

        // ── PECO-2996: always-derivable system_configuration fields are populated ──────────

        [Fact]
        public void SystemConfiguration_RuntimeVendor_AlwaysPopulated()
        {
            var config = ConnectionTelemetry.BuildSystemConfiguration("1.2.3");

            // runtime_vendor is a process-wide constant for .NET, never platform-dependent.
            // It must always be "Microsoft" so the field is never absent in telemetry rows.
            Assert.Equal("Microsoft", config.RuntimeVendor);
        }

        [Fact]
        public void SystemConfiguration_DriverName_AlwaysPopulated()
        {
            var config = ConnectionTelemetry.BuildSystemConfiguration("1.2.3");

            Assert.Equal(DatabricksConnection.DatabricksDriverName, config.DriverName);
        }

        [Fact]
        public void SystemConfiguration_DriverVersion_AlwaysPopulated()
        {
            var config = ConnectionTelemetry.BuildSystemConfiguration("1.2.3");

            // DriverVersion is the cached value from the first invocation in the process; it
            // may not equal "1.2.3" in a test run that already populated the cache, but it
            // must never be null/empty.
            Assert.False(string.IsNullOrEmpty(config.DriverVersion));
        }

        // ── PECO-2996: connection-param constants are populated independent of negotiation ─

        [Fact]
        public void DriverConnectionParams_ConstantFlags_AlwaysPopulated()
        {
            // These flags reflect driver-side behavior and never depend on a backend round
            // trip, so they should always be set even when other parts of init have failed.
            // The 110-row gap reported in PECO-2996 had all four absent.
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/1.0/warehouses/abc123" },
            };

            var connParams = ConnectionTelemetry.BuildDriverConnectionParams(
                properties, Host, enableDirectResults: true, useDescTableExtended: true, DefaultTimeoutMs);

            Assert.True(connParams.EnableArrow);
            Assert.True(connParams.EnableDirectResults);
            Assert.True(connParams.AutoCommit);
            Assert.Equal(DefaultTimeoutMs / 1000, connParams.SocketTimeout);
        }

        // ── PECO-2996: Safe* wrappers fall back to best-effort defaults on partial init ────

        [Fact]
        public void SafeBuildSystemConfiguration_ReturnsBestEffort_WhenAssemblyVersionIsEmpty()
        {
            var config = ConnectionTelemetry.SafeBuildSystemConfiguration(string.Empty, activity: null);

            Assert.NotNull(config);
            Assert.Equal(DatabricksConnection.DatabricksDriverName, config.DriverName);
            Assert.Equal("Microsoft", config.RuntimeVendor);
        }

        [Fact]
        public void SafeBuildDriverConnectionParams_ReturnsBestEffort_WithMinimalProperties()
        {
            // Even with no properties at all, the always-derivable connection-param flags
            // must be present in the returned proto so dashboards see them populated.
            var properties = new Dictionary<string, string>();

            var connParams = ConnectionTelemetry.SafeBuildDriverConnectionParams(
                properties, Host,
                enableDirectResults: true, useDescTableExtended: false,
                connectTimeoutMilliseconds: DefaultTimeoutMs, activity: null);

            Assert.NotNull(connParams);
            Assert.True(connParams.EnableArrow);
            Assert.True(connParams.EnableDirectResults);
            Assert.True(connParams.AutoCommit);
            Assert.Equal(DefaultTimeoutMs / 1000, connParams.SocketTimeout);
            Assert.NotNull(connParams.HostInfo);
            Assert.Equal(Host, connParams.HostInfo.HostUrl);
        }

        [Fact]
        public void SafeDetermineAuthType_ReturnsValidString_WithEmptyProperties()
        {
            var authType = ConnectionTelemetry.SafeDetermineAuthType(
                new Dictionary<string, string>(), activity: null);

            Assert.False(string.IsNullOrEmpty(authType));
        }

        // ── PECO-2998 + PECO-2996 combined: a session built end-to-end on a minimal
        //   property bag still has all "always-derivable" fields populated. This is the
        //   regression test for the partial-init shape that produced the 110-row gap.

        [Fact]
        public void EndToEnd_MinimalProperties_AllAlwaysDerivableFieldsPopulated()
        {
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.AuthType, SparkAuthTypeConstants.OAuth },
                { DatabricksParameters.OAuthGrantType, DatabricksConstants.OAuthGrantTypes.ClientCredentials },
            };

            var systemConfig = ConnectionTelemetry.SafeBuildSystemConfiguration(
                "1.2.3", activity: null);
            var connParams = ConnectionTelemetry.SafeBuildDriverConnectionParams(
                properties, Host,
                enableDirectResults: true, useDescTableExtended: true,
                connectTimeoutMilliseconds: DefaultTimeoutMs, activity: null);
            string authType = ConnectionTelemetry.SafeDetermineAuthType(properties, activity: null);

            // Assemble a session as Create() would, then sanity-check the seven fields the
            // ticket flagged as missing on the partial-init path.
            var session = new TelemetrySessionContext
            {
                SystemConfiguration = systemConfig,
                DriverConnectionParams = connParams,
                AuthType = authType
            };

            // PECO-2998: top-level auth_type populated and consistent with nested mech/flow.
            Assert.Equal("oauth-client_credentials", session.AuthType);
            Assert.Equal(DriverAuthMechType.Oauth, session.DriverConnectionParams!.AuthMech);
            Assert.Equal(DriverAuthFlowType.ClientCredentials, session.DriverConnectionParams.AuthFlow);

            // PECO-2996: system_configuration.runtime_vendor and os_arch present.
            Assert.Equal("Microsoft", session.SystemConfiguration!.RuntimeVendor);
            Assert.False(string.IsNullOrEmpty(session.SystemConfiguration.OsArch));

            // PECO-2996: connection-param constants present.
            Assert.True(session.DriverConnectionParams.EnableDirectResults);
            Assert.True(session.DriverConnectionParams.EnableArrow);
            Assert.True(session.DriverConnectionParams.AutoCommit);
            Assert.Equal(DefaultTimeoutMs / 1000, session.DriverConnectionParams.SocketTimeout);
        }
    }
}
