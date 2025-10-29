/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using Apache.Arrow.Adbc.Drivers.Databricks;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Unit
{
    /// <summary>
    /// Tests for DatabricksParameters constants to ensure they follow correct naming conventions.
    /// </summary>
    public class DatabricksParametersTests
    {
        [Fact]
        public void TestProtocolParameterExists()
        {
            Assert.Equal("adbc.databricks.protocol", DatabricksParameters.Protocol);
        }

        [Fact]
        public void TestResultDispositionParameterExists()
        {
            Assert.Equal("adbc.databricks.rest.result_disposition", DatabricksParameters.ResultDisposition);
        }

        [Fact]
        public void TestResultFormatParameterExists()
        {
            Assert.Equal("adbc.databricks.rest.result_format", DatabricksParameters.ResultFormat);
        }

        [Fact]
        public void TestResultCompressionParameterExists()
        {
            Assert.Equal("adbc.databricks.rest.result_compression", DatabricksParameters.ResultCompression);
        }

        [Fact]
        public void TestWaitTimeoutParameterExists()
        {
            Assert.Equal("adbc.databricks.rest.wait_timeout", DatabricksParameters.WaitTimeout);
        }

        [Fact]
        public void TestPollingIntervalParameterExists()
        {
            Assert.Equal("adbc.databricks.rest.polling_interval_ms", DatabricksParameters.PollingInterval);
        }

        [Fact]
        public void TestEnableSessionManagementParameterExists()
        {
            Assert.Equal("adbc.databricks.enable_session_management", DatabricksParameters.EnableSessionManagement);
        }

        [Fact]
        public void TestEnableDirectResultsParameterExists()
        {
            // This parameter already existed, verify it's still there
            Assert.Equal("adbc.databricks.enable_direct_results", DatabricksParameters.EnableDirectResults);
        }

        [Fact]
        public void TestAllRestParametersUseCorrectPrefix()
        {
            // Verify REST-specific parameters use "adbc.databricks.rest." prefix
            Assert.StartsWith("adbc.databricks.rest.", DatabricksParameters.ResultDisposition);
            Assert.StartsWith("adbc.databricks.rest.", DatabricksParameters.ResultFormat);
            Assert.StartsWith("adbc.databricks.rest.", DatabricksParameters.ResultCompression);
            Assert.StartsWith("adbc.databricks.rest.", DatabricksParameters.WaitTimeout);
            Assert.StartsWith("adbc.databricks.rest.", DatabricksParameters.PollingInterval);
        }

        [Fact]
        public void TestProtocolAgnosticParametersUseCorrectPrefix()
        {
            // Verify protocol-agnostic parameters use "adbc.databricks." prefix
            Assert.StartsWith("adbc.databricks.", DatabricksParameters.Protocol);
            Assert.StartsWith("adbc.databricks.", DatabricksParameters.EnableSessionManagement);
            Assert.StartsWith("adbc.databricks.", DatabricksParameters.EnableDirectResults);
        }
    }
}
