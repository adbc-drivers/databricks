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
    /// Regression tests for PECO-2985: OAuth U2M access-token passthrough was mislabeled
    /// as <c>auth_mech=PAT, auth_flow=TOKEN_PASSTHROUGH</c> because any authenticated
    /// connection without the <c>client_credentials</c> grant fell into the PAT branch
    /// of <see cref="ConnectionTelemetry.BuildDriverConnectionParams"/>.
    /// </summary>
    public class ConnectionTelemetryAuthMechTests
    {
        private const string Host = "test.databricks.com";
        private const int DefaultTimeoutMs = 30_000;

        private static Dictionary<string, string> BaseProperties() => new()
        {
            { SparkParameters.Path, "/sql/1.0/warehouses/abc123" },
        };

        [Fact]
        public void AuthMech_Pat_WhenAuthTypeIsTokenAndNoOAuthGrantType()
        {
            var properties = BaseProperties();
            properties[SparkParameters.AuthType] = SparkAuthTypeConstants.Token;
            properties[SparkParameters.Token] = "dapi-redacted";

            var connParams = ConnectionTelemetry.BuildDriverConnectionParams(
                properties, Host, enableDirectResults: true, useDescTableExtended: true, DefaultTimeoutMs);

            Assert.Equal(DriverAuthMechType.Pat, connParams.AuthMech);
            Assert.Equal(DriverAuthFlowType.TokenPassthrough, connParams.AuthFlow);
        }

        [Fact]
        public void AuthMech_Oauth_ClientCredentials_WhenGrantTypeIsClientCredentials()
        {
            var properties = BaseProperties();
            properties[SparkParameters.AuthType] = SparkAuthTypeConstants.OAuth;
            properties[DatabricksParameters.OAuthGrantType] =
                DatabricksConstants.OAuthGrantTypes.ClientCredentials;
            properties[DatabricksParameters.OAuthClientId] = "client-id";
            properties[DatabricksParameters.OAuthClientSecret] = "client-secret";

            var connParams = ConnectionTelemetry.BuildDriverConnectionParams(
                properties, Host, enableDirectResults: true, useDescTableExtended: true, DefaultTimeoutMs);

            Assert.Equal(DriverAuthMechType.Oauth, connParams.AuthMech);
            Assert.Equal(DriverAuthFlowType.ClientCredentials, connParams.AuthFlow);
        }

        /// <summary>
        /// PECO-2985: OAuth U2M access-token passthrough — caller sets <c>auth_type=oauth</c>
        /// with a pre-acquired <c>access_token</c> and no grant_type. This must report
        /// <c>auth_mech=OAUTH, auth_flow=TOKEN_PASSTHROUGH</c>, not PAT.
        /// </summary>
        [Fact]
        public void AuthMech_Oauth_TokenPassthrough_WhenAuthTypeIsOauthWithNoGrantType()
        {
            var properties = BaseProperties();
            properties[SparkParameters.AuthType] = SparkAuthTypeConstants.OAuth;
            properties[SparkParameters.AccessToken] = "oauth-access-token-redacted";

            var connParams = ConnectionTelemetry.BuildDriverConnectionParams(
                properties, Host, enableDirectResults: true, useDescTableExtended: true, DefaultTimeoutMs);

            Assert.Equal(DriverAuthMechType.Oauth, connParams.AuthMech);
            Assert.Equal(DriverAuthFlowType.TokenPassthrough, connParams.AuthFlow);
        }

        /// <summary>
        /// PECO-2985: OAuth U2M access-token passthrough with the grant_type explicitly
        /// set to <c>access_token</c> must also report <c>auth_mech=OAUTH, auth_flow=TOKEN_PASSTHROUGH</c>.
        /// </summary>
        [Fact]
        public void AuthMech_Oauth_TokenPassthrough_WhenGrantTypeIsAccessToken()
        {
            var properties = BaseProperties();
            properties[SparkParameters.AuthType] = SparkAuthTypeConstants.OAuth;
            properties[DatabricksParameters.OAuthGrantType] =
                DatabricksConstants.OAuthGrantTypes.AccessToken;
            properties[SparkParameters.AccessToken] = "oauth-access-token-redacted";

            var connParams = ConnectionTelemetry.BuildDriverConnectionParams(
                properties, Host, enableDirectResults: true, useDescTableExtended: true, DefaultTimeoutMs);

            Assert.Equal(DriverAuthMechType.Oauth, connParams.AuthMech);
            Assert.Equal(DriverAuthFlowType.TokenPassthrough, connParams.AuthFlow);
        }

        [Fact]
        public void AuthMech_Pat_WhenNoAuthConfigured()
        {
            var properties = BaseProperties();

            var connParams = ConnectionTelemetry.BuildDriverConnectionParams(
                properties, Host, enableDirectResults: true, useDescTableExtended: true, DefaultTimeoutMs);

            Assert.Equal(DriverAuthMechType.Pat, connParams.AuthMech);
            Assert.Equal(DriverAuthFlowType.TokenPassthrough, connParams.AuthFlow);
        }
    }
}
