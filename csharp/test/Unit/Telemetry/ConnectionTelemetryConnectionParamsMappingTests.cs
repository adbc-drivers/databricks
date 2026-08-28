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
using AdbcDrivers.HiveServer2.Hive2;
using AdbcDrivers.HiveServer2.Spark;
using DriverModeType = AdbcDrivers.Databricks.Telemetry.Proto.DriverMode.Types.Type;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Verifies <see cref="ConnectionTelemetry.BuildDriverConnectionParams"/> maps ADBC
    /// connection properties onto the <c>driver_connection_params</c> proto:
    /// <list type="bullet">
    /// <item>the <c>enable_complex_datatype_support</c> fix — it must reflect its own
    /// setting, NOT <c>use_desc_table_extended</c> (which is a distinct property and has
    /// no proto home);</item>
    /// <item>newly-mapped fields: <c>query_tags</c>, <c>auth_scope</c>, proxy
    /// (<c>use_proxy</c>/<c>proxy_host_info</c>/<c>non_proxy_hosts</c>), and
    /// <c>allow_self_signed_support</c>.</item>
    /// </list>
    /// </summary>
    public class ConnectionTelemetryConnectionParamsMappingTests
    {
        private const string Host = "params-mapping.databricks.com";
        private const int TimeoutMs = 900_000;

        [Fact]
        public void NewConnectionParamFields_AreMappedFromProperties()
        {
            var props = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/1.0/warehouses/abc123" },
                { DatabricksParameters.QueryTags, "team:peco,env:test" },
                { DatabricksParameters.OAuthScope, "all-apis" },
                { HttpProxyOptions.UseProxy, "true" },
                { HttpProxyOptions.ProxyHost, "proxy.internal" },
                { HttpProxyOptions.ProxyPort, "8080" },
                { HttpProxyOptions.ProxyIgnoreList, "localhost, .corp.example.com" },
                { HttpTlsOptions.AllowSelfSigned, "true" },
            };

            var result = ConnectionTelemetry.BuildDriverConnectionParams(
                props, Host, DriverModeType.Thrift,
                enableDirectResults: true,
                enableComplexDatatypeSupport: false,
                connectTimeoutMilliseconds: TimeoutMs);

            Assert.Equal("team:peco,env:test", result.QueryTags);
            Assert.Equal("all-apis", result.AuthScope);
            Assert.True(result.UseProxy);
            Assert.NotNull(result.ProxyHostInfo);
            Assert.Equal("proxy.internal", result.ProxyHostInfo.HostUrl);
            Assert.Equal(8080, result.ProxyHostInfo.Port);
            Assert.Equal(new[] { "localhost", ".corp.example.com" }, result.NonProxyHosts);
            Assert.True(result.AllowSelfSignedSupport);
        }

        [Fact]
        public void EnableComplexDatatypeSupport_ComesFromOwnSetting_NotUseDescTableExtended()
        {
            // Regression for the mis-wired field: enable_complex_datatype_support must reflect
            // its own setting, independent of use_desc_table_extended. Set use_desc_table_extended
            // = true but request complex-datatype support = false -> proto field must be false.
            var props = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/1.0/warehouses/abc123" },
                { DatabricksParameters.UseDescTableExtended, "true" },
            };

            var disabled = ConnectionTelemetry.BuildDriverConnectionParams(
                props, Host, DriverModeType.Thrift,
                enableDirectResults: true,
                enableComplexDatatypeSupport: false,
                connectTimeoutMilliseconds: TimeoutMs);
            Assert.False(disabled.EnableComplexDatatypeSupport);

            var enabled = ConnectionTelemetry.BuildDriverConnectionParams(
                props, Host, DriverModeType.Thrift,
                enableDirectResults: true,
                enableComplexDatatypeSupport: true,
                connectTimeoutMilliseconds: TimeoutMs);
            Assert.True(enabled.EnableComplexDatatypeSupport);
        }

        [Fact]
        public void OptionalFields_LeftUnset_WhenPropertiesAbsent()
        {
            var props = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/1.0/warehouses/abc123" },
            };

            var result = ConnectionTelemetry.BuildDriverConnectionParams(
                props, Host, DriverModeType.Thrift,
                enableDirectResults: true,
                enableComplexDatatypeSupport: false,
                connectTimeoutMilliseconds: TimeoutMs);

            // Absent properties leave the proto fields unset (null/empty), matching JDBC.
            Assert.False(result.HasQueryTags);
            Assert.False(result.HasAuthScope);
            Assert.False(result.HasUseProxy);
            Assert.Null(result.ProxyHostInfo);
            Assert.Empty(result.NonProxyHosts);
            Assert.False(result.HasAllowSelfSignedSupport);
        }
    }
}
