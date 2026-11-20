/*
* Copyright (c) 2026 ADBC Drivers Contributors
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
using System.Net.Http;
using System.Threading;
using AdbcDrivers.Databricks.StatementExecution;
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.Spark;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    /// <summary>
    /// SEA metadata operations (GetObjects/GetTableSchema) are just queries, so they share the one
    /// query timeout (adbc.apache.statement.query_timeout_s) via CreateMetadataTimeoutCts — not a
    /// separate metadata-timeout knob. Default matches the Thrift path (3h); 0 means no timeout
    /// (a never-cancelled CTS — guards against the old FromSeconds(0) near-immediate cancel).
    /// </summary>
    public class StatementExecutionMetadataTimeoutTests
    {
        private static Dictionary<string, string> BaseProps() => new Dictionary<string, string>
        {
            { SparkParameters.HostName, "test.databricks.com" },
            { DatabricksParameters.WarehouseId, "wh-1" },
            { SparkParameters.AccessToken, "token" },
        };

        private static StatementExecutionConnection NewConnection(Dictionary<string, string> props)
            => new StatementExecutionConnection(props, new HttpClient());

        [Fact]
        public void MetadataTimeout_QueryTimeoutZero_NeverCancels()
        {
            var props = BaseProps();
            props[ApacheParameters.QueryTimeoutSeconds] = "0"; // 0 = no timeout
            using var connection = NewConnection(props);

            using var cts = connection.CreateMetadataTimeoutCts();
            Assert.False(cts.IsCancellationRequested);
            Thread.Sleep(150); // a FromSeconds(0) CTS would have fired by now
            Assert.False(cts.IsCancellationRequested);
        }

        [Fact]
        public void MetadataTimeout_UsesQueryTimeout_WhenPositive()
        {
            var props = BaseProps();
            props[ApacheParameters.QueryTimeoutSeconds] = "1"; // shares the query timeout
            using var connection = NewConnection(props);

            using var cts = connection.CreateMetadataTimeoutCts();
            Assert.True(cts.Token.CanBeCanceled);
            // ~1s scheduled cancellation; allow generous slack to avoid flakiness.
            Assert.True(SpinWait.SpinUntil(() => cts.IsCancellationRequested, TimeSpan.FromSeconds(5)),
                "metadata CTS should cancel around the query timeout (1s)");
        }
    }
}
