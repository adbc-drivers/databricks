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
using DriverModeType = AdbcDrivers.Databricks.Telemetry.Proto.DriverMode.Types.Type;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Regression tests for PECO-2995 (TELEM-16): the proto fields
    /// <c>discovery_mode_enabled</c>, <c>discovery_url</c>, and
    /// <c>enable_token_cache</c> existed in the telemetry schema but were never
    /// populated by the C# driver, leaving them indistinguishable from "unset"
    /// in the lakehouse.
    ///
    /// This driver does not support OIDC discovery (the OAuth token endpoint is
    /// hardcoded to <c>https://{host}/oidc/v1/token</c> in
    /// <c>OAuthClientCredentialsProvider</c> and <c>TokenExchangeClient</c>) and
    /// does not support a persistent on-disk token cache (JDBC's
    /// <c>enableTokenCache</c> refers to a U2M browser-flow disk cache, which this
    /// driver does not support; the in-memory dedup dictionaries in the delegating
    /// handlers are a different mechanism). We therefore explicitly emit
    /// <c>discovery_mode_enabled=false</c> and <c>enable_token_cache=false</c>
    /// for every connection so analysts can distinguish C# rows from JDBC rows.
    /// <c>discovery_url</c> is intentionally left unset (no value to report).
    /// </summary>
    public class ConnectionTelemetryDiscoveryFieldsTests
    {
        private const string Host = "test.databricks.com";
        private const int DefaultTimeoutMs = 30_000;

        private static Dictionary<string, string> BaseProperties() => new()
        {
            { SparkParameters.Path, "/sql/1.0/warehouses/abc123" },
        };

        [Fact]
        public void DiscoveryModeEnabled_AlwaysFalse_PatAuth()
        {
            var properties = BaseProperties();
            properties[SparkParameters.AuthType] = SparkAuthTypeConstants.Token;
            properties[SparkParameters.Token] = "dapi-redacted";

            var connParams = ConnectionTelemetry.BuildDriverConnectionParams(
                properties, Host, DriverModeType.Thrift,
                enableDirectResults: true, enableComplexDatatypeSupport: true, DefaultTimeoutMs);

            Assert.True(connParams.HasDiscoveryModeEnabled);
            Assert.False(connParams.DiscoveryModeEnabled);
        }

        [Fact]
        public void DiscoveryModeEnabled_AlwaysFalse_OAuthClientCredentials()
        {
            var properties = BaseProperties();
            properties[SparkParameters.AuthType] = SparkAuthTypeConstants.OAuth;
            properties[DatabricksParameters.OAuthGrantType] =
                DatabricksConstants.OAuthGrantTypes.ClientCredentials;
            properties[DatabricksParameters.OAuthClientId] = "client-id";
            properties[DatabricksParameters.OAuthClientSecret] = "client-secret";

            var connParams = ConnectionTelemetry.BuildDriverConnectionParams(
                properties, Host, DriverModeType.Thrift,
                enableDirectResults: true, enableComplexDatatypeSupport: true, DefaultTimeoutMs);

            Assert.True(connParams.HasDiscoveryModeEnabled);
            Assert.False(connParams.DiscoveryModeEnabled);
        }

        [Fact]
        public void DiscoveryUrl_LeftUnset_NoDiscoverySupported()
        {
            var properties = BaseProperties();
            properties[SparkParameters.AuthType] = SparkAuthTypeConstants.OAuth;
            properties[DatabricksParameters.OAuthGrantType] =
                DatabricksConstants.OAuthGrantTypes.ClientCredentials;

            var connParams = ConnectionTelemetry.BuildDriverConnectionParams(
                properties, Host, DriverModeType.Thrift,
                enableDirectResults: true, enableComplexDatatypeSupport: true, DefaultTimeoutMs);

            // The C# driver hardcodes the OIDC token endpoint and never performs
            // .well-known discovery, so there is no URL to report. Leaving the
            // optional field unset is the honest signal.
            Assert.False(connParams.HasDiscoveryUrl);
        }

        [Fact]
        public void EnableTokenCache_AlwaysFalse_PatAuth()
        {
            var properties = BaseProperties();
            properties[SparkParameters.AuthType] = SparkAuthTypeConstants.Token;
            properties[SparkParameters.Token] = "dapi-redacted";

            var connParams = ConnectionTelemetry.BuildDriverConnectionParams(
                properties, Host, DriverModeType.Thrift,
                enableDirectResults: true, enableComplexDatatypeSupport: true, DefaultTimeoutMs);

            Assert.True(connParams.HasEnableTokenCache);
            Assert.False(connParams.EnableTokenCache);
        }

        [Fact]
        public void EnableTokenCache_AlwaysFalse_OAuthClientCredentials()
        {
            var properties = BaseProperties();
            properties[SparkParameters.AuthType] = SparkAuthTypeConstants.OAuth;
            properties[DatabricksParameters.OAuthGrantType] =
                DatabricksConstants.OAuthGrantTypes.ClientCredentials;
            properties[DatabricksParameters.OAuthClientId] = "client-id";
            properties[DatabricksParameters.OAuthClientSecret] = "client-secret";

            var connParams = ConnectionTelemetry.BuildDriverConnectionParams(
                properties, Host, DriverModeType.Thrift,
                enableDirectResults: true, enableComplexDatatypeSupport: true, DefaultTimeoutMs);

            Assert.True(connParams.HasEnableTokenCache);
            Assert.False(connParams.EnableTokenCache);
        }
    }
}
