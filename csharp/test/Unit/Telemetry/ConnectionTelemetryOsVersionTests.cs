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
using System.Runtime.InteropServices;
using AdbcDrivers.Databricks.Telemetry;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Regression tests for PECO-2986: telemetry's <c>os_version</c> field previously reported
    /// the Linux kernel release (e.g. <c>"5.4.0.1156"</c>) instead of the distro version
    /// (e.g. <c>"Ubuntu 22.04.3 LTS"</c>) because it used <see cref="Environment.OSVersion"/>.
    /// </summary>
    public class ConnectionTelemetryOsVersionTests
    {
        [Fact]
        public void GetOsVersion_IsPopulated()
        {
            string version = ConnectionTelemetry.GetOsVersion();
            Assert.False(string.IsNullOrEmpty(version));
        }

        [Fact]
        public void GetOsVersion_OnLinux_DoesNotReturnBareKernelVersion()
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                return;
            }

            string kernelVersion = Environment.OSVersion.Version.ToString();
            string reported = ConnectionTelemetry.GetOsVersion();

            Assert.NotEqual(kernelVersion, reported);
        }

        [Theory]
        [InlineData("Ubuntu 22.04.3 LTS", "Ubuntu 22.04.3 LTS")]
        [InlineData("\"Ubuntu 22.04.3 LTS\"", "Ubuntu 22.04.3 LTS")]
        [InlineData("'Red Hat Enterprise Linux 9.2'", "Red Hat Enterprise Linux 9.2")]
        [InlineData("\"\"", "")]
        [InlineData("", "")]
        [InlineData("\"", "\"")]
        [InlineData("NoQuotes", "NoQuotes")]
        public void UnquoteOsReleaseValue_StripsMatchingQuotes(string raw, string expected)
        {
            Assert.Equal(expected, ConnectionTelemetry.UnquoteOsReleaseValue(raw));
        }

        [Fact]
        public void BuildSystemConfiguration_IsCachedAcrossCalls()
        {
            var first = ConnectionTelemetry.BuildSystemConfiguration("1.0.0-test");
            var second = ConnectionTelemetry.BuildSystemConfiguration("1.0.0-test");

            // Same reference — the proto is built once per process and reused.
            Assert.Same(first, second);
        }
    }
}
