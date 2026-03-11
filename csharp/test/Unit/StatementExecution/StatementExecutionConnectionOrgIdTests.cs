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
using System.Linq;
using System.Net.Http;
using System.Reflection;
using AdbcDrivers.Databricks.StatementExecution;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow.Adbc;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    /// <summary>
    /// Unit tests for parsing ?o=yyy org ID from httpPath and injecting x-databricks-org-id header.
    /// </summary>
    public class StatementExecutionConnectionOrgIdTests
    {
        private static Dictionary<string, string> BaseProperties(string path) => new()
        {
            { SparkParameters.HostName, "test.databricks.com" },
            { SparkParameters.Path, path },
            { SparkParameters.AccessToken, "test-token" }
        };

        private static string? GetOrgId(StatementExecutionConnection connection)
        {
            var field = typeof(StatementExecutionConnection)
                .GetField("_orgId", BindingFlags.NonPublic | BindingFlags.Instance);
            return (string?)field!.GetValue(connection);
        }

        private static string? GetWarehouseId(StatementExecutionConnection connection)
        {
            var field = typeof(StatementExecutionConnection)
                .GetField("_warehouseId", BindingFlags.NonPublic | BindingFlags.Instance);
            return (string?)field!.GetValue(connection);
        }

        private static bool HasOrgIdHeader(StatementExecutionConnection connection, string expectedOrgId)
        {
            var field = typeof(StatementExecutionConnection)
                .GetField("_httpClient", BindingFlags.NonPublic | BindingFlags.Instance);
            var httpClient = (HttpClient?)field!.GetValue(connection);
            return httpClient!.DefaultRequestHeaders.TryGetValues("x-databricks-org-id", out var values)
                && values.Any(v => v == expectedOrgId);
        }

        [Fact]
        public void Constructor_PathWithOrgId_ExtractsWarehouseIdAndOrgId()
        {
            using var connection = new StatementExecutionConnection(
                BaseProperties("/sql/1.0/warehouses/abc123?o=987654"));

            Assert.Equal("abc123", GetWarehouseId(connection));
            Assert.Equal("987654", GetOrgId(connection));
        }

        [Fact]
        public void Constructor_PathWithoutQueryString_OrgIdIsNull()
        {
            using var connection = new StatementExecutionConnection(
                BaseProperties("/sql/1.0/warehouses/abc123"));

            Assert.Equal("abc123", GetWarehouseId(connection));
            Assert.Null(GetOrgId(connection));
        }

        [Fact]
        public void Constructor_PathWithOtherQueryParams_ExtractsOrgIdCorrectly()
        {
            using var connection = new StatementExecutionConnection(
                BaseProperties("/sql/1.0/warehouses/abc123?foo=bar&o=111222&baz=qux"));

            Assert.Equal("abc123", GetWarehouseId(connection));
            Assert.Equal("111222", GetOrgId(connection));
        }

        [Fact]
        public void Constructor_PathWithEmptyOrgId_OrgIdIsNull()
        {
            using var connection = new StatementExecutionConnection(
                BaseProperties("/sql/1.0/warehouses/abc123?o="));

            Assert.Equal("abc123", GetWarehouseId(connection));
            Assert.Null(GetOrgId(connection));
        }

        [Fact]
        public void Constructor_EndpointsPathWithOrgId_ExtractsWarehouseIdAndOrgId()
        {
            using var connection = new StatementExecutionConnection(
                BaseProperties("/sql/1.0/endpoints/wh456?o=999000"));

            Assert.Equal("wh456", GetWarehouseId(connection));
            Assert.Equal("999000", GetOrgId(connection));
        }

        [Fact]
        public void Constructor_PathWithOrgId_InjectsOrgIdHeader()
        {
            using var connection = new StatementExecutionConnection(
                BaseProperties("/sql/1.0/warehouses/abc123?o=987654"));

            Assert.True(HasOrgIdHeader(connection, "987654"),
                "Expected x-databricks-org-id: 987654 in HttpClient DefaultRequestHeaders");
        }

        [Fact]
        public void Constructor_PathWithoutOrgId_DoesNotInjectOrgIdHeader()
        {
            using var connection = new StatementExecutionConnection(
                BaseProperties("/sql/1.0/warehouses/abc123"));

            var field = typeof(StatementExecutionConnection)
                .GetField("_httpClient", BindingFlags.NonPublic | BindingFlags.Instance);
            var httpClient = (HttpClient?)field!.GetValue(connection);

            Assert.False(httpClient!.DefaultRequestHeaders.TryGetValues("x-databricks-org-id", out _),
                "x-databricks-org-id header should NOT be present when path has no org ID");
        }

        [Fact]
        public void Constructor_UriWithOrgIdInQueryString_ExtractsOrgId()
        {
            var properties = new Dictionary<string, string>
            {
                { AdbcOptions.Uri, "https://test.databricks.com/sql/1.0/warehouses/abc123?o=555777" },
                { SparkParameters.AccessToken, "test-token" }
            };

            using var connection = new StatementExecutionConnection(properties);

            Assert.Equal("abc123", GetWarehouseId(connection));
            Assert.Equal("555777", GetOrgId(connection));
        }
    }
}
