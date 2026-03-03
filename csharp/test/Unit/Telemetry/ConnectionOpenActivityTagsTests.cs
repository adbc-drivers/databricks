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
using System.Globalization;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using AdbcDrivers.Databricks.Telemetry.TagDefinitions;
using AdbcDrivers.HiveServer2.Spark;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for Connection.Open activity tags set by SetSystemConfigurationTags
    /// and SetConnectionParameterTags in DatabricksConnection.
    /// Verifies that all required telemetry tags are correctly populated on activities.
    /// </summary>
    public class ConnectionOpenActivityTagsTests : IDisposable
    {
        private readonly ActivitySource _activitySource;
        private readonly ActivityListener _listener;

        /// <summary>
        /// Minimum required properties for creating a DatabricksConnection.
        /// </summary>
        private static readonly Dictionary<string, string> MinimalProperties = new Dictionary<string, string>
        {
            { SparkParameters.HostName, "test-host.databricks.com" },
            { SparkParameters.AuthType, SparkAuthType.Token.ToString() },
            { SparkParameters.Token, "dummy-token" },
            { SparkParameters.Path, "/sql/1.0/warehouses/abc123" },
        };

        public ConnectionOpenActivityTagsTests()
        {
            _activitySource = new ActivitySource("ConnectionOpenActivityTagsTests");
            _listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == "ConnectionOpenActivityTagsTests",
                Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded
            };
            ActivitySource.AddActivityListener(_listener);
        }

        public void Dispose()
        {
            _listener.Dispose();
            _activitySource.Dispose();
        }

        #region SetSystemConfigurationTags Tests

        [Fact]
        public void SetSystemConfigurationTags_SetsDriverVersion()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetSystemConfigurationTags(activity);

            // Assert
            var tag = activity?.GetTagItem(ConnectionOpenEvent.DriverVersion);
            Assert.NotNull(tag);
            Assert.IsType<string>(tag);
            Assert.False(string.IsNullOrEmpty((string)tag));
        }

        [Fact]
        public void SetSystemConfigurationTags_SetsDriverName()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetSystemConfigurationTags(activity);

            // Assert
            Assert.Equal("Databricks ADBC Driver", activity?.GetTagItem(ConnectionOpenEvent.DriverName));
        }

        [Fact]
        public void SetSystemConfigurationTags_SetsRuntimeName()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetSystemConfigurationTags(activity);

            // Assert
            var runtimeName = activity?.GetTagItem(ConnectionOpenEvent.RuntimeName) as string;
            Assert.NotNull(runtimeName);
            Assert.StartsWith(".NET", runtimeName);
        }

        [Fact]
        public void SetSystemConfigurationTags_SetsRuntimeVersion()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetSystemConfigurationTags(activity);

            // Assert
            var runtimeVersion = activity?.GetTagItem(ConnectionOpenEvent.RuntimeVersion) as string;
            Assert.NotNull(runtimeVersion);
            Assert.NotEqual("unknown", runtimeVersion);
            // Version should contain digits
            Assert.Matches(@"\d+\.\d+", runtimeVersion);
        }

        [Fact]
        public void SetSystemConfigurationTags_SetsRuntimeVendor()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetSystemConfigurationTags(activity);

            // Assert
            Assert.Equal("Microsoft", activity?.GetTagItem(ConnectionOpenEvent.RuntimeVendor));
        }

        [Fact]
        public void SetSystemConfigurationTags_SetsOsName()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetSystemConfigurationTags(activity);

            // Assert
            var osName = activity?.GetTagItem(ConnectionOpenEvent.OsName) as string;
            Assert.NotNull(osName);
            Assert.False(string.IsNullOrEmpty(osName));
        }

        [Fact]
        public void SetSystemConfigurationTags_SetsOsVersion()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetSystemConfigurationTags(activity);

            // Assert
            var osVersion = activity?.GetTagItem(ConnectionOpenEvent.OsVersion) as string;
            Assert.NotNull(osVersion);
            Assert.False(string.IsNullOrEmpty(osVersion));
        }

        [Fact]
        public void SetSystemConfigurationTags_SetsOsArch()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetSystemConfigurationTags(activity);

            // Assert
            var osArch = activity?.GetTagItem(ConnectionOpenEvent.OsArch) as string;
            Assert.NotNull(osArch);
            // Should be one of the known architectures
            Assert.Contains(osArch, new[] { "X64", "X86", "Arm", "Arm64", "Wasm", "S390x", "LoongArch64", "Armv6", "Ppc64le" });
        }

        [Fact]
        public void SetSystemConfigurationTags_SetsClientAppName()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetSystemConfigurationTags(activity);

            // Assert
            var clientAppName = activity?.GetTagItem(ConnectionOpenEvent.ClientAppName) as string;
            Assert.NotNull(clientAppName);
            Assert.False(string.IsNullOrEmpty(clientAppName));
        }

        [Fact]
        public void SetSystemConfigurationTags_SetsClientAppName_FromUserAgentEntry()
        {
            // Arrange
            var properties = new Dictionary<string, string>(MinimalProperties)
            {
                { SparkParameters.UserAgentEntry, "MyTestApp/1.0" }
            };
            var connection = CreateConnection(properties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetSystemConfigurationTags(activity);

            // Assert
            Assert.Equal("MyTestApp/1.0", activity?.GetTagItem(ConnectionOpenEvent.ClientAppName));
        }

        [Fact]
        public void SetSystemConfigurationTags_SetsLocaleName()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetSystemConfigurationTags(activity);

            // Assert
            var localeName = activity?.GetTagItem(ConnectionOpenEvent.LocaleName) as string;
            Assert.NotNull(localeName);
            Assert.Equal(CultureInfo.CurrentCulture.Name, localeName);
        }

        [Fact]
        public void SetSystemConfigurationTags_SetsCharSetEncoding()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetSystemConfigurationTags(activity);

            // Assert
            var encoding = activity?.GetTagItem(ConnectionOpenEvent.CharSetEncoding) as string;
            Assert.NotNull(encoding);
            Assert.Equal(Encoding.Default.WebName, encoding);
        }

        [Fact]
        public void SetSystemConfigurationTags_SetsProcessName()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetSystemConfigurationTags(activity);

            // Assert
            var processName = activity?.GetTagItem(ConnectionOpenEvent.ProcessName) as string;
            Assert.NotNull(processName);
            Assert.False(string.IsNullOrEmpty(processName));
        }

        [Fact]
        public void SetSystemConfigurationTags_NullActivity_DoesNotThrow()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);

            // Act & Assert - should not throw
            var exception = Record.Exception(() => connection.SetSystemConfigurationTags(null));
            Assert.Null(exception);
        }

        [Fact]
        public void SetSystemConfigurationTags_SetsAllExpectedTags()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetSystemConfigurationTags(activity);

            // Assert - verify all system config tags are set
            Assert.NotNull(activity?.GetTagItem(ConnectionOpenEvent.DriverVersion));
            Assert.NotNull(activity?.GetTagItem(ConnectionOpenEvent.DriverName));
            Assert.NotNull(activity?.GetTagItem(ConnectionOpenEvent.RuntimeName));
            Assert.NotNull(activity?.GetTagItem(ConnectionOpenEvent.RuntimeVersion));
            Assert.NotNull(activity?.GetTagItem(ConnectionOpenEvent.RuntimeVendor));
            Assert.NotNull(activity?.GetTagItem(ConnectionOpenEvent.OsName));
            Assert.NotNull(activity?.GetTagItem(ConnectionOpenEvent.OsVersion));
            Assert.NotNull(activity?.GetTagItem(ConnectionOpenEvent.OsArch));
            Assert.NotNull(activity?.GetTagItem(ConnectionOpenEvent.ClientAppName));
            Assert.NotNull(activity?.GetTagItem(ConnectionOpenEvent.LocaleName));
            Assert.NotNull(activity?.GetTagItem(ConnectionOpenEvent.CharSetEncoding));
            Assert.NotNull(activity?.GetTagItem(ConnectionOpenEvent.ProcessName));
        }

        #endregion

        #region SetConnectionParameterTags Tests

        [Fact]
        public void SetConnectionParameterTags_SetsConnectionHost()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetConnectionParameterTags(activity);

            // Assert
            Assert.Equal("test-host.databricks.com", activity?.GetTagItem(ConnectionOpenEvent.ConnectionHost));
        }

        [Fact]
        public void SetConnectionParameterTags_SetsConnectionHttpPath()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetConnectionParameterTags(activity);

            // Assert
            Assert.Equal("/sql/1.0/warehouses/abc123", activity?.GetTagItem(ConnectionOpenEvent.ConnectionHttpPath));
        }

        [Fact]
        public void SetConnectionParameterTags_SetsDefaultPort()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetConnectionParameterTags(activity);

            // Assert
            Assert.Equal("443", activity?.GetTagItem(ConnectionOpenEvent.ConnectionPort));
        }

        [Fact]
        public void SetConnectionParameterTags_SetsCustomPort()
        {
            // Arrange
            var properties = new Dictionary<string, string>(MinimalProperties)
            {
                { SparkParameters.Port, "8443" }
            };
            var connection = CreateConnection(properties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetConnectionParameterTags(activity);

            // Assert
            Assert.Equal("8443", activity?.GetTagItem(ConnectionOpenEvent.ConnectionPort));
        }

        [Fact]
        public void SetConnectionParameterTags_SetsDefaultMode()
        {
            // Arrange - no protocol specified means thrift
            var connection = CreateConnection(MinimalProperties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetConnectionParameterTags(activity);

            // Assert
            Assert.Equal("thrift", activity?.GetTagItem(ConnectionOpenEvent.ConnectionMode));
        }

        [Fact]
        public void SetConnectionParameterTags_SetsAuthMech_Pat()
        {
            // Arrange - token-based auth
            var connection = CreateConnection(MinimalProperties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetConnectionParameterTags(activity);

            // Assert
            Assert.Equal("pat", activity?.GetTagItem(ConnectionOpenEvent.ConnectionAuthMech));
        }

        [Fact]
        public void SetConnectionParameterTags_SetsAuthFlow_TokenPassthrough()
        {
            // Arrange - token-based auth
            var connection = CreateConnection(MinimalProperties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetConnectionParameterTags(activity);

            // Assert
            Assert.Equal("token_passthrough", activity?.GetTagItem(ConnectionOpenEvent.ConnectionAuthFlow));
        }

        [Fact]
        public void SetConnectionParameterTags_SetsFeatureArrow_AlwaysTrue()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetConnectionParameterTags(activity);

            // Assert
            Assert.Equal(true, activity?.GetTagItem(ConnectionOpenEvent.FeatureArrow));
        }

        [Fact]
        public void SetConnectionParameterTags_SetsFeatureDirectResults()
        {
            // Arrange - direct results enabled by default
            var connection = CreateConnection(MinimalProperties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetConnectionParameterTags(activity);

            // Assert
            Assert.Equal(true, activity?.GetTagItem(ConnectionOpenEvent.FeatureDirectResults));
        }

        [Fact]
        public void SetConnectionParameterTags_SetsFeatureDirectResults_Disabled()
        {
            // Arrange - direct results explicitly disabled
            var properties = new Dictionary<string, string>(MinimalProperties)
            {
                { DatabricksParameters.EnableDirectResults, "false" }
            };
            var connection = CreateConnection(properties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetConnectionParameterTags(activity);

            // Assert
            Assert.Equal(false, activity?.GetTagItem(ConnectionOpenEvent.FeatureDirectResults));
        }

        [Fact]
        public void SetConnectionParameterTags_SetsFeatureCloudFetch()
        {
            // Arrange - cloud fetch enabled by default
            var connection = CreateConnection(MinimalProperties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetConnectionParameterTags(activity);

            // Assert
            Assert.Equal(true, activity?.GetTagItem(ConnectionOpenEvent.FeatureCloudFetch));
        }

        [Fact]
        public void SetConnectionParameterTags_SetsFeatureCloudFetch_Disabled()
        {
            // Arrange - cloud fetch explicitly disabled
            var properties = new Dictionary<string, string>(MinimalProperties)
            {
                { DatabricksParameters.UseCloudFetch, "false" }
            };
            var connection = CreateConnection(properties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetConnectionParameterTags(activity);

            // Assert
            Assert.Equal(false, activity?.GetTagItem(ConnectionOpenEvent.FeatureCloudFetch));
        }

        [Fact]
        public void SetConnectionParameterTags_SetsFeatureLz4()
        {
            // Arrange - LZ4 enabled by default
            var connection = CreateConnection(MinimalProperties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetConnectionParameterTags(activity);

            // Assert
            Assert.Equal(true, activity?.GetTagItem(ConnectionOpenEvent.FeatureLz4));
        }

        [Fact]
        public void SetConnectionParameterTags_SetsFeatureLz4_Disabled()
        {
            // Arrange - LZ4 explicitly disabled
            var properties = new Dictionary<string, string>(MinimalProperties)
            {
                { DatabricksParameters.CanDecompressLz4, "false" }
            };
            var connection = CreateConnection(properties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetConnectionParameterTags(activity);

            // Assert
            Assert.Equal(false, activity?.GetTagItem(ConnectionOpenEvent.FeatureLz4));
        }

        [Fact]
        public void SetConnectionParameterTags_NullActivity_DoesNotThrow()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);

            // Act & Assert - should not throw
            var exception = Record.Exception(() => connection.SetConnectionParameterTags(null));
            Assert.Null(exception);
        }

        [Fact]
        public void SetConnectionParameterTags_SetsAllExpectedTags()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetConnectionParameterTags(activity);

            // Assert - verify all connection parameter tags are set
            Assert.NotNull(activity?.GetTagItem(ConnectionOpenEvent.ConnectionHttpPath));
            Assert.NotNull(activity?.GetTagItem(ConnectionOpenEvent.ConnectionHost));
            Assert.NotNull(activity?.GetTagItem(ConnectionOpenEvent.ConnectionPort));
            Assert.NotNull(activity?.GetTagItem(ConnectionOpenEvent.ConnectionMode));
            Assert.NotNull(activity?.GetTagItem(ConnectionOpenEvent.ConnectionAuthMech));
            Assert.NotNull(activity?.GetTagItem(ConnectionOpenEvent.ConnectionAuthFlow));
            Assert.NotNull(activity?.GetTagItem(ConnectionOpenEvent.FeatureArrow));
            Assert.NotNull(activity?.GetTagItem(ConnectionOpenEvent.FeatureDirectResults));
            Assert.NotNull(activity?.GetTagItem(ConnectionOpenEvent.FeatureCloudFetch));
            Assert.NotNull(activity?.GetTagItem(ConnectionOpenEvent.FeatureLz4));
        }

        #endregion

        #region DetermineAuthType Tests

        [Fact]
        public void DetermineAuthType_WithToken_ReturnsPat()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);

            // Act
            var authType = connection.DetermineAuthType();

            // Assert
            Assert.Equal("pat", authType);
        }

        [Fact]
        public void DetermineAuthType_WithOAuthClientCredentials_ReturnsOAuthM2M()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.HostName, "test-host.databricks.com" },
                { SparkParameters.AuthType, SparkAuthTypeConstants.OAuth },
                { SparkParameters.AccessToken, "dummy-access-token" },
                { SparkParameters.Path, "/sql/1.0/warehouses/abc123" },
                { DatabricksParameters.OAuthGrantType, "client_credentials" },
                { DatabricksParameters.OAuthClientId, "test-client-id" },
                { DatabricksParameters.OAuthClientSecret, "test-secret" },
            };
            var connection = CreateConnection(properties);

            // Act
            var authType = connection.DetermineAuthType();

            // Assert
            Assert.Equal("oauth-m2m", authType);
        }

        [Fact]
        public void DetermineAuthType_WithOAuthClientId_NoGrantType_ReturnsOAuthM2M()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.HostName, "test-host.databricks.com" },
                { SparkParameters.AuthType, SparkAuthTypeConstants.OAuth },
                { SparkParameters.AccessToken, "dummy-access-token" },
                { SparkParameters.Path, "/sql/1.0/warehouses/abc123" },
                { DatabricksParameters.OAuthClientId, "test-client-id" },
                { DatabricksParameters.OAuthClientSecret, "test-secret" },
            };
            var connection = CreateConnection(properties);

            // Act
            var authType = connection.DetermineAuthType();

            // Assert
            Assert.Equal("oauth-m2m", authType);
        }

        [Fact]
        public void DetermineAuthType_WithNoAuthProps_ReturnsUnknown()
        {
            // Arrange - no token, no oauth
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.HostName, "test-host.databricks.com" },
                { SparkParameters.AuthType, SparkAuthTypeConstants.None },
                { SparkParameters.Path, "/sql/1.0/warehouses/abc123" },
            };
            var connection = CreateConnection(properties);

            // Act
            var authType = connection.DetermineAuthType();

            // Assert
            Assert.Equal("unknown", authType);
        }

        #endregion

        #region DetermineDriverMode Tests

        [Fact]
        public void DetermineDriverMode_Default_ReturnsThrift()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);

            // Act
            var mode = connection.DetermineDriverMode();

            // Assert
            Assert.Equal("thrift", mode);
        }

        [Fact]
        public void DetermineDriverMode_SeaProtocol_ReturnsSea()
        {
            // Arrange
            var properties = new Dictionary<string, string>(MinimalProperties)
            {
                { DatabricksParameters.Protocol, "sea" }
            };
            var connection = CreateConnection(properties);

            // Act
            var mode = connection.DetermineDriverMode();

            // Assert
            Assert.Equal("sea", mode);
        }

        [Fact]
        public void DetermineDriverMode_ThriftProtocol_ReturnsThrift()
        {
            // Arrange
            var properties = new Dictionary<string, string>(MinimalProperties)
            {
                { DatabricksParameters.Protocol, "thrift" }
            };
            var connection = CreateConnection(properties);

            // Act
            var mode = connection.DetermineDriverMode();

            // Assert
            Assert.Equal("thrift", mode);
        }

        #endregion

        #region DetermineAuthMech Tests

        [Fact]
        public void DetermineAuthMech_WithToken_ReturnsPat()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);

            // Act
            var mech = connection.DetermineAuthMech();

            // Assert
            Assert.Equal("pat", mech);
        }

        [Fact]
        public void DetermineAuthMech_WithOAuth_ReturnsOAuth()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.HostName, "test-host.databricks.com" },
                { SparkParameters.AuthType, SparkAuthTypeConstants.OAuth },
                { SparkParameters.AccessToken, "dummy-access-token" },
                { SparkParameters.Path, "/sql/1.0/warehouses/abc123" },
                { DatabricksParameters.OAuthClientId, "test-client-id" },
                { DatabricksParameters.OAuthClientSecret, "test-secret" },
            };
            var connection = CreateConnection(properties);

            // Act
            var mech = connection.DetermineAuthMech();

            // Assert
            Assert.Equal("oauth", mech);
        }

        [Fact]
        public void DetermineAuthMech_WithNoAuth_ReturnsOther()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.HostName, "test-host.databricks.com" },
                { SparkParameters.AuthType, SparkAuthTypeConstants.None },
                { SparkParameters.Path, "/sql/1.0/warehouses/abc123" },
            };
            var connection = CreateConnection(properties);

            // Act
            var mech = connection.DetermineAuthMech();

            // Assert
            Assert.Equal("other", mech);
        }

        #endregion

        #region DetermineAuthFlow Tests

        [Fact]
        public void DetermineAuthFlow_WithToken_ReturnsTokenPassthrough()
        {
            // Arrange
            var connection = CreateConnection(MinimalProperties);

            // Act
            var flow = connection.DetermineAuthFlow();

            // Assert
            Assert.Equal("token_passthrough", flow);
        }

        [Fact]
        public void DetermineAuthFlow_WithOAuthClientId_ReturnsClientCredentials()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.HostName, "test-host.databricks.com" },
                { SparkParameters.AuthType, SparkAuthTypeConstants.OAuth },
                { SparkParameters.AccessToken, "dummy-access-token" },
                { SparkParameters.Path, "/sql/1.0/warehouses/abc123" },
                { DatabricksParameters.OAuthClientId, "test-client-id" },
                { DatabricksParameters.OAuthClientSecret, "test-secret" },
            };
            var connection = CreateConnection(properties);

            // Act
            var flow = connection.DetermineAuthFlow();

            // Assert
            Assert.Equal("client_credentials", flow);
        }

        [Fact]
        public void DetermineAuthFlow_WithNoAuth_ReturnsUnspecified()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.HostName, "test-host.databricks.com" },
                { SparkParameters.AuthType, SparkAuthTypeConstants.None },
                { SparkParameters.Path, "/sql/1.0/warehouses/abc123" },
            };
            var connection = CreateConnection(properties);

            // Act
            var flow = connection.DetermineAuthFlow();

            // Assert
            Assert.Equal("unspecified", flow);
        }

        #endregion

        #region ConnectionOpenEvent Tag Constants Tests

        [Fact]
        public void ConnectionOpenEvent_GetDatabricksExportTags_ContainsAllRequiredTags()
        {
            // Act
            var tags = ConnectionOpenEvent.GetDatabricksExportTags();

            // Assert - verify all required tags are included
            Assert.Contains(ConnectionOpenEvent.SessionId, tags);
            Assert.Contains(ConnectionOpenEvent.DriverVersion, tags);
            Assert.Contains(ConnectionOpenEvent.DriverName, tags);
            Assert.Contains(ConnectionOpenEvent.RuntimeName, tags);
            Assert.Contains(ConnectionOpenEvent.RuntimeVersion, tags);
            Assert.Contains(ConnectionOpenEvent.RuntimeVendor, tags);
            Assert.Contains(ConnectionOpenEvent.OsName, tags);
            Assert.Contains(ConnectionOpenEvent.OsVersion, tags);
            Assert.Contains(ConnectionOpenEvent.OsArch, tags);
            Assert.Contains(ConnectionOpenEvent.ClientAppName, tags);
            Assert.Contains(ConnectionOpenEvent.LocaleName, tags);
            Assert.Contains(ConnectionOpenEvent.CharSetEncoding, tags);
            Assert.Contains(ConnectionOpenEvent.ProcessName, tags);
            Assert.Contains(ConnectionOpenEvent.AuthType, tags);
            Assert.Contains(ConnectionOpenEvent.ConnectionHttpPath, tags);
            Assert.Contains(ConnectionOpenEvent.ConnectionHost, tags);
            Assert.Contains(ConnectionOpenEvent.ConnectionPort, tags);
            Assert.Contains(ConnectionOpenEvent.ConnectionMode, tags);
            Assert.Contains(ConnectionOpenEvent.ConnectionAuthMech, tags);
            Assert.Contains(ConnectionOpenEvent.ConnectionAuthFlow, tags);
            Assert.Contains(ConnectionOpenEvent.FeatureArrow, tags);
            Assert.Contains(ConnectionOpenEvent.FeatureDirectResults, tags);
            Assert.Contains(ConnectionOpenEvent.FeatureCloudFetch, tags);
            Assert.Contains(ConnectionOpenEvent.FeatureLz4, tags);
        }

        #endregion

        #region OAuth Grant Type Auth Tests

        [Fact]
        public void SetConnectionParameterTags_OAuthClientCredentials_SetsAuthMechOAuth()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.HostName, "test-host.databricks.com" },
                { SparkParameters.AuthType, SparkAuthTypeConstants.OAuth },
                { SparkParameters.AccessToken, "dummy-access-token" },
                { SparkParameters.Path, "/sql/1.0/warehouses/abc123" },
                { DatabricksParameters.OAuthGrantType, "client_credentials" },
                { DatabricksParameters.OAuthClientId, "test-client-id" },
                { DatabricksParameters.OAuthClientSecret, "test-secret" },
            };
            var connection = CreateConnection(properties);
            using var activity = _activitySource.StartActivity("Connection.Open");

            // Act
            connection.SetConnectionParameterTags(activity);

            // Assert
            Assert.Equal("oauth", activity?.GetTagItem(ConnectionOpenEvent.ConnectionAuthMech));
            Assert.Equal("client_credentials", activity?.GetTagItem(ConnectionOpenEvent.ConnectionAuthFlow));
        }

        #endregion

        #region Helper Methods

        /// <summary>
        /// Creates a DatabricksConnection with the given properties for testing.
        /// </summary>
        private static DatabricksConnection CreateConnection(Dictionary<string, string> properties)
        {
            return new DatabricksConnection(properties);
        }

        #endregion
    }
}
