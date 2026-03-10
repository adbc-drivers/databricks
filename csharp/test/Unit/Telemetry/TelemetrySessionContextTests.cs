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
using AdbcDrivers.Databricks.Telemetry.Proto;
using DriverModeType = AdbcDrivers.Databricks.Telemetry.Proto.DriverMode.Types.Type;
using ExecutionResultFormat = AdbcDrivers.Databricks.Telemetry.Proto.ExecutionResult.Types.Format;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for TelemetrySessionContext immutable session-level data.
    /// </summary>
    public class TelemetrySessionContextTests
    {
        [Fact]
        public void TelemetrySessionContext_Construction_SetsAllProperties()
        {
            // Arrange
            DriverSystemConfiguration systemConfig = new DriverSystemConfiguration
            {
                DriverVersion = "1.0.0",
                DriverName = "Apache Arrow ADBC Databricks Driver",
                RuntimeName = ".NET 8.0",
                RuntimeVersion = "8.0.0",
                OsName = "Linux",
                OsVersion = "5.15.0",
                OsArch = "x86_64"
            };

            DriverConnectionParameters connParams = new DriverConnectionParameters
            {
                HttpPath = "/sql/1.0/warehouses/abc123",
                Mode = DriverModeType.Thrift,
                HostInfo = new HostDetails
                {
                    HostUrl = "https://dbc-12345678-abcd.cloud.databricks.com",
                    Port = 443
                },
                EnableArrow = true,
                EnableDirectResults = true
            };

            // Act
            TelemetrySessionContext context = new TelemetrySessionContext
            {
                SessionId = "session-uuid-123",
                AuthType = "PAT",
                WorkspaceId = 12345678901234L,
                SystemConfiguration = systemConfig,
                DriverConnectionParams = connParams,
                DefaultResultFormat = ExecutionResultFormat.ExternalLinks,
                DefaultCompressionEnabled = true,
                TelemetryClient = null
            };

            // Assert
            Assert.Equal("session-uuid-123", context.SessionId);
            Assert.Equal("PAT", context.AuthType);
            Assert.Equal(12345678901234L, context.WorkspaceId);
            Assert.Same(systemConfig, context.SystemConfiguration);
            Assert.Same(connParams, context.DriverConnectionParams);
            Assert.Equal(ExecutionResultFormat.ExternalLinks, context.DefaultResultFormat);
            Assert.True(context.DefaultCompressionEnabled);
            Assert.Null(context.TelemetryClient);
        }

        [Fact]
        public void TelemetrySessionContext_Immutability_InternalSetters()
        {
            // Arrange & Act
            TelemetrySessionContext context = new TelemetrySessionContext
            {
                SessionId = "initial-session-id",
                WorkspaceId = 123L
            };

            // Assert - verify properties are set
            Assert.Equal("initial-session-id", context.SessionId);
            Assert.Equal(123L, context.WorkspaceId);

            // Note: Properties use internal set, so they are immutable outside the driver assembly.
            // External consumers cannot reassign these properties after construction.
        }

        [Fact]
        public void TelemetrySessionContext_NullableProperties_CanBeNull()
        {
            // Act
            TelemetrySessionContext context = new TelemetrySessionContext
            {
                SessionId = null,
                AuthType = null,
                SystemConfiguration = null,
                DriverConnectionParams = null,
                TelemetryClient = null
            };

            // Assert
            Assert.Null(context.SessionId);
            Assert.Null(context.AuthType);
            Assert.Null(context.SystemConfiguration);
            Assert.Null(context.DriverConnectionParams);
            Assert.Null(context.TelemetryClient);
        }

        [Fact]
        public void TelemetrySessionContext_DefaultValues_AreExpected()
        {
            // Act - create context with minimal initialization
            TelemetrySessionContext context = new TelemetrySessionContext();

            // Assert - verify default values
            Assert.Null(context.SessionId);
            Assert.Null(context.AuthType);
            Assert.Equal(0L, context.WorkspaceId);
            Assert.Null(context.SystemConfiguration);
            Assert.Null(context.DriverConnectionParams);
            Assert.Equal((ExecutionResultFormat)0, context.DefaultResultFormat);
            Assert.False(context.DefaultCompressionEnabled);
            Assert.Null(context.TelemetryClient);
        }

        [Fact]
        public void TelemetrySessionContext_WithThriftMode_SetsCorrectProtocol()
        {
            // Arrange & Act
            TelemetrySessionContext context = new TelemetrySessionContext
            {
                DriverConnectionParams = new DriverConnectionParameters
                {
                    Mode = DriverModeType.Thrift
                }
            };

            // Assert
            Assert.NotNull(context.DriverConnectionParams);
            Assert.Equal(DriverModeType.Thrift, context.DriverConnectionParams.Mode);
        }

        [Fact]
        public void TelemetrySessionContext_WithSeaMode_SetsCorrectProtocol()
        {
            // Arrange & Act
            TelemetrySessionContext context = new TelemetrySessionContext
            {
                DriverConnectionParams = new DriverConnectionParameters
                {
                    Mode = DriverModeType.Sea
                }
            };

            // Assert
            Assert.NotNull(context.DriverConnectionParams);
            Assert.Equal(DriverModeType.Sea, context.DriverConnectionParams.Mode);
        }
    }
}
