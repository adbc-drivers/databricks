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
using AdbcDrivers.Databricks.Http;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Http
{
    /// <summary>
    /// Pins the User-Agent contract: capital-ADBC prefix (shared with the Thrift transport,
    /// derived from DatabricksConnection.DatabricksDriverName), an optional product token, and
    /// the adbc.spark.user_agent_entry suffix.
    /// </summary>
    public class UserAgentHelperTests
    {
        private const string UserAgentEntryKey = "adbc.spark.user_agent_entry";

        [Fact]
        public void GetUserAgent_NoProductNoEntry_UsesCapitalAdbcPrefix()
        {
            Assert.Equal("ADBCDatabricksDriver/1.2.3", UserAgentHelper.GetUserAgent("1.2.3"));
        }

        [Fact]
        public void GetUserAgent_WithProduct_InsertsTokenAfterVersion()
        {
            // The SEA path passes product: "REST".
            Assert.Equal("ADBCDatabricksDriver/1.2.3 REST", UserAgentHelper.GetUserAgent("1.2.3", null, product: "REST"));
        }

        [Fact]
        public void GetUserAgent_WithProductAndEntry_AppendsEntryLast()
        {
            var props = new Dictionary<string, string> { [UserAgentEntryKey] = "custom-entry" };
            Assert.Equal("ADBCDatabricksDriver/1.2.3 REST custom-entry",
                UserAgentHelper.GetUserAgent("1.2.3", props, product: "REST"));
        }

        [Fact]
        public void GetUserAgent_EntryNoProduct_AppendsEntry()
        {
            var props = new Dictionary<string, string> { [UserAgentEntryKey] = "custom-entry" };
            Assert.Equal("ADBCDatabricksDriver/1.2.3 custom-entry", UserAgentHelper.GetUserAgent("1.2.3", props));
        }

        [Fact]
        public void GetUserAgent_BlankEntry_Ignored()
        {
            var props = new Dictionary<string, string> { [UserAgentEntryKey] = "   " };
            Assert.Equal("ADBCDatabricksDriver/1.2.3", UserAgentHelper.GetUserAgent("1.2.3", props));
        }

        [Fact]
        public void GetUserAgent_PrefixMatchesDriverNameConstant()
        {
            // Single source of truth: the prefix is the driver name with spaces removed.
            string expectedPrefix = DatabricksConnection.DatabricksDriverName.Replace(" ", "");
            Assert.StartsWith(expectedPrefix + "/", UserAgentHelper.GetUserAgent("9.9.9"));
        }
    }
}
