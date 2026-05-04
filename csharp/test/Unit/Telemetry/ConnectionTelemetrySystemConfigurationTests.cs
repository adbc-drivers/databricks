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

using System.Diagnostics;
using System.Reflection;
using AdbcDrivers.Databricks.Telemetry;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Regression tests for PECO-2989: <c>process_name</c> was populated from
    /// <see cref="Process.GetCurrentProcess"/>, which returns the .NET host executable
    /// name ("dotnet") for any .NET Core / .NET 5+ application and is worthless for
    /// customer attribution. The field must reflect the entry assembly name, falling
    /// back to the OS process name when no entry assembly is available.
    /// </summary>
    public class ConnectionTelemetrySystemConfigurationTests
    {
        [Fact]
        public void ProcessName_UsesEntryAssemblyName_NotDotnetHost()
        {
            var config = ConnectionTelemetry.BuildSystemConfiguration(assemblyVersion: "1.2.3");

            string expected = Assembly.GetEntryAssembly()?.GetName().Name
                ?? Process.GetCurrentProcess().ProcessName;

            Assert.Equal(expected, config.ProcessName);
            Assert.NotEqual("dotnet", config.ProcessName);
        }

        [Fact]
        public void ClientAppName_DefaultsToEntryAssemblyName_NotDotnetHost()
        {
            var config = ConnectionTelemetry.BuildSystemConfiguration(assemblyVersion: "1.2.3");

            string expected = Assembly.GetEntryAssembly()?.GetName().Name
                ?? Process.GetCurrentProcess().ProcessName;

            Assert.Equal(expected, config.ClientAppName);
            Assert.NotEqual("dotnet", config.ClientAppName);
        }
    }
}
