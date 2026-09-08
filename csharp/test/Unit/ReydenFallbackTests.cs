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
using System.Net.Http;
using AdbcDrivers.Databricks;
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow.Adbc;
using Xunit;

namespace AdbcDrivers.Databricks.Tests
{
    /// <summary>
    /// Unit tests for the Reyden/Lakehouse-RT fallback helpers: detecting the server's
    /// "Thrift not supported" rejection, resolving the warehouse id, and the end-to-end
    /// decision (detect → cache → subsequent connects route to SEA).
    /// </summary>
    public class ReydenFallbackTests
    {
        // The exact message surfaced by the SQL proxy for a Reyden warehouse (via ThriftErrorMessageHandler).
        private const string ReydenErrorText =
            "Thrift server error: BAD_REQUEST: Lakehouse/RT is not supported for Thrift protocol. " +
            "Please update your Databricks SQL Driver version to the latest version, which supports " +
            "the Statement Execution API protocol (HTTP 400 Bad Request)";

        public ReydenFallbackTests()
        {
            ReydenWarehouseCache.Clear();
        }

        [Fact]
        public void IsThriftRejection_DetectsDirectMessage()
        {
            Assert.True(ReydenFallback.IsThriftRejection(new HttpRequestException(ReydenErrorText)));
        }

        [Fact]
        public void IsThriftRejection_WalksInnerExceptionChain()
        {
            var chained = new InvalidOperationException(
                "An unexpected error occurred while opening the session.",
                new Exception("Couldn't connect to server", new HttpRequestException(ReydenErrorText)));
            Assert.True(ReydenFallback.IsThriftRejection(chained));
        }

        [Fact]
        public void IsThriftRejection_WalksAggregateException()
        {
            var aggregate = new AggregateException(
                new Exception("unrelated"),
                new HttpRequestException(ReydenErrorText));
            Assert.True(ReydenFallback.IsThriftRejection(aggregate));
        }

        [Fact]
        public void IsThriftRejection_FalseForUnrelatedErrorsAndNull()
        {
            Assert.False(ReydenFallback.IsThriftRejection(null));
            Assert.False(ReydenFallback.IsThriftRejection(
                new HttpRequestException("Thrift server error: TFetchOrientation ... (HTTP 500)")));
        }

        [Theory]
        [InlineData("/sql/1.0/warehouses/000000000107b7e3", "000000000107b7e3")]
        [InlineData("/sql/1.0/endpoints/abc123", "abc123")]
        [InlineData("/sql/1.0/warehouses/000000000107b7e3?catalog=main", "000000000107b7e3")]
        public void TryGetWarehouseId_ParsesFromUriPath(string absolutePath, string expected)
        {
            var props = new Dictionary<string, string>
            {
                [AdbcOptions.Uri] = "https://adb-6436897454825492.12.azuredatabricks.net" + absolutePath,
            };
            Assert.Equal(expected, ReydenFallback.TryGetWarehouseId(props));
        }

        [Fact]
        public void TryGetWarehouseId_PrefersExplicitParameter()
        {
            var props = new Dictionary<string, string>
            {
                [AdbcOptions.Uri] = "https://host/sql/1.0/warehouses/from-path",
                [DatabricksParameters.WarehouseId] = "from-param",
            };
            Assert.Equal("from-param", ReydenFallback.TryGetWarehouseId(props));
        }

        [Fact]
        public void TryGetWarehouseId_ParsesFromExplicitPath()
        {
            var props = new Dictionary<string, string>
            {
                [SparkParameters.Path] = "/sql/1.0/warehouses/wh-from-path",
            };
            Assert.Equal("wh-from-path", ReydenFallback.TryGetWarehouseId(props));
        }

        [Fact]
        public void TryGetWarehouseId_NullForGeneralClusterPath()
        {
            var props = new Dictionary<string, string>
            {
                [AdbcOptions.Uri] = "https://host/sql/protocolv1/o/1234567890/0101-cluster",
            };
            Assert.Null(ReydenFallback.TryGetWarehouseId(props));
        }

        /// <summary>
        /// The fallback decision Connect makes: a Reyden OpenSession error is recognized, the warehouse
        /// is cached, and a subsequent connect's pre-check now routes that warehouse to SEA.
        /// </summary>
        [Fact]
        public void ReydenErrorMarksWarehouseSoNextConnectPrefersSea()
        {
            var props = new Dictionary<string, string>
            {
                [AdbcOptions.Uri] = "https://host/sql/1.0/warehouses/wh-reyden",
            };
            string? warehouseId = ReydenFallback.TryGetWarehouseId(props);
            Assert.Equal("wh-reyden", warehouseId);

            // Before the error, Thrift is attempted (warehouse not yet known to be Reyden).
            Assert.False(ReydenWarehouseCache.IsReyden(warehouseId));

            // Simulate the failed Thrift OpenSession surfacing the Reyden rejection.
            var openSessionError = new AggregateException(new HttpRequestException(ReydenErrorText));
            Assert.True(ReydenFallback.IsThriftRejection(openSessionError));
            ReydenWarehouseCache.Mark(warehouseId);

            // Next connect's pre-check now short-circuits Thrift and uses SEA.
            Assert.True(ReydenWarehouseCache.IsReyden(warehouseId));
        }
    }
}
