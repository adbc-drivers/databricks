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
using System.Linq;
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
            string? cacheKey = ReydenFallback.TryGetWarehouseCacheKey(props);
            Assert.NotNull(cacheKey);

            // Before the error, Thrift is attempted (warehouse not yet known to be Reyden).
            Assert.False(ReydenWarehouseCache.IsReyden(cacheKey));

            // Simulate the failed Thrift OpenSession surfacing the Reyden rejection.
            var openSessionError = new AggregateException(new HttpRequestException(ReydenErrorText));
            Assert.True(ReydenFallback.IsThriftRejection(openSessionError));
            ReydenWarehouseCache.Mark(cacheKey);

            // Next connect's pre-check now short-circuits Thrift and uses SEA.
            Assert.True(ReydenWarehouseCache.IsReyden(cacheKey));
        }

        [Fact]
        public void TryGetWarehouseCacheKey_IncludesHostComponent()
        {
            // Same warehouse id on two different workspaces must yield distinct cache keys.
            var workspaceA = new Dictionary<string, string>
            {
                [AdbcOptions.Uri] = "https://workspace-a.databricks.com/sql/1.0/warehouses/abc123",
            };
            var workspaceB = new Dictionary<string, string>
            {
                [AdbcOptions.Uri] = "https://workspace-b.databricks.com/sql/1.0/warehouses/abc123",
            };

            string? keyA = ReydenFallback.TryGetWarehouseCacheKey(workspaceA);
            string? keyB = ReydenFallback.TryGetWarehouseCacheKey(workspaceB);

            Assert.NotNull(keyA);
            Assert.NotNull(keyB);
            Assert.NotEqual(keyA, keyB);
        }

        /// <summary>
        /// The same warehouse reached with the host in different casing must hash to one cache key,
        /// matching the host normalization FeatureFlagCache applies (host.ToLowerInvariant()). The
        /// HostName-without-scheme path preserves the caller's casing, so the key builder must lowercase.
        /// </summary>
        [Fact]
        public void TryGetWarehouseCacheKey_NormalizesHostCasing()
        {
            var upper = new Dictionary<string, string>
            {
                [SparkParameters.HostName] = "MyHost.Databricks.Com",
                [DatabricksParameters.WarehouseId] = "abc123",
            };
            var lower = new Dictionary<string, string>
            {
                [SparkParameters.HostName] = "myhost.databricks.com",
                [DatabricksParameters.WarehouseId] = "abc123",
            };

            string? keyUpper = ReydenFallback.TryGetWarehouseCacheKey(upper);
            string? keyLower = ReydenFallback.TryGetWarehouseCacheKey(lower);

            Assert.NotNull(keyUpper);
            Assert.Equal(keyLower, keyUpper);
        }

        /// <summary>
        /// Routing glue: when a warehouse is already marked Reyden, the pre-check in RouteConnection
        /// flips the effective protocol to SEA and never invokes the Thrift factory — even though the
        /// caller left the protocol at the Thrift default.
        /// </summary>
        [Fact]
        public void RouteConnection_MarkedWarehouseSkipsThriftAndUsesSea()
        {
            var props = new Dictionary<string, string>
            {
                [AdbcOptions.Uri] = "https://host/sql/1.0/warehouses/wh-reyden",
            };
            ReydenWarehouseCache.Mark(ReydenFallback.TryGetWarehouseCacheKey(props));

            bool thriftCalled = false;
            string result = DatabricksDatabase.RouteConnection<string>(
                props,
                _ => { thriftCalled = true; return "thrift"; },
                _ => "sea");

            Assert.Equal("sea", result);
            Assert.False(thriftCalled);
        }

        /// <summary>
        /// Routing glue: an unmarked warehouse opens over Thrift; when that open throws the Reyden
        /// "Thrift not supported" rejection, RouteConnection intercepts it, marks the warehouse, and
        /// transparently retries over SEA.
        /// </summary>
        [Fact]
        public void RouteConnection_ThriftRejectionMarksWarehouseAndFallsBackToSea()
        {
            var props = new Dictionary<string, string>
            {
                [AdbcOptions.Uri] = "https://host/sql/1.0/warehouses/wh-reyden",
            };
            string? cacheKey = ReydenFallback.TryGetWarehouseCacheKey(props);
            Assert.False(ReydenWarehouseCache.IsReyden(cacheKey));

            int thriftAttempts = 0;
            string result = DatabricksDatabase.RouteConnection<string>(
                props,
                _ =>
                {
                    thriftAttempts++;
                    // Mirror the real failed-open surface: OpenAsync().Wait() wraps in AggregateException.
                    throw new AggregateException(new HttpRequestException(ReydenErrorText));
                },
                _ => "sea");

            Assert.Equal("sea", result);
            Assert.Equal(1, thriftAttempts);
            // The warehouse is now marked, so the next connect's pre-check short-circuits Thrift.
            Assert.True(ReydenWarehouseCache.IsReyden(cacheKey));
        }

        /// <summary>
        /// Routing glue: when the SEA fallback itself fails after a Thrift rejection, both causal
        /// chains must survive — the original Thrift rejection must not be silently discarded. The
        /// thrown exception retains both the SEA error and the Thrift rejection as inner exceptions.
        /// </summary>
        [Fact]
        public void RouteConnection_SeaFallbackFailureRetainsBothCauses()
        {
            var props = new Dictionary<string, string>
            {
                [AdbcOptions.Uri] = "https://host/sql/1.0/warehouses/wh-reyden-sea-fail",
            };
            string? cacheKey = ReydenFallback.TryGetWarehouseCacheKey(props);
            Assert.False(ReydenWarehouseCache.IsReyden(cacheKey));

            var thriftRejection = new HttpRequestException(ReydenErrorText);
            var seaFailure = new HttpRequestException("SEA auth failed");

            var thrown = Assert.Throws<AggregateException>(() =>
                DatabricksDatabase.RouteConnection<string>(
                    props,
                    _ => throw new AggregateException(thriftRejection),
                    _ => throw new AggregateException(seaFailure)));

            // Both the SEA error and the original Thrift rejection are reachable from the chain.
            var innerMessages = thrown.Flatten().InnerExceptions.Select(e => e.Message).ToList();
            Assert.Contains(ReydenErrorText, innerMessages);
            Assert.Contains("SEA auth failed", innerMessages);
            // The warehouse is still marked so future connects skip the doomed Thrift open.
            Assert.True(ReydenWarehouseCache.IsReyden(cacheKey));
        }

        /// <summary>
        /// Routing glue: a Thrift open failure that is NOT the Reyden rejection propagates unchanged —
        /// the warehouse must not be marked and no SEA fallback is attempted.
        /// </summary>
        [Fact]
        public void RouteConnection_NonReydenThriftFailurePropagatesWithoutFallback()
        {
            var props = new Dictionary<string, string>
            {
                [AdbcOptions.Uri] = "https://host/sql/1.0/warehouses/wh-healthy",
            };
            string? cacheKey = ReydenFallback.TryGetWarehouseCacheKey(props);

            bool seaCalled = false;
            var thrown = Assert.Throws<AggregateException>(() =>
                DatabricksDatabase.RouteConnection<string>(
                    props,
                    _ => throw new AggregateException(new HttpRequestException("some transient network error")),
                    _ => { seaCalled = true; return "sea"; }));

            Assert.False(seaCalled);
            Assert.False(ReydenWarehouseCache.IsReyden(cacheKey));
            Assert.NotNull(thrown);
        }

        /// <summary>
        /// Routing glue: when the warehouse cache key cannot be resolved (e.g. a general cluster path),
        /// the Reyden interception is disabled — a Thrift rejection propagates rather than falling back,
        /// because there is nothing to mark or key a future pre-check on.
        /// </summary>
        [Fact]
        public void RouteConnection_NoCacheKeyDisablesFallback()
        {
            var props = new Dictionary<string, string>
            {
                [AdbcOptions.Uri] = "https://host/sql/protocolv1/o/1234567890/0101-cluster",
            };
            Assert.Null(ReydenFallback.TryGetWarehouseCacheKey(props));

            bool seaCalled = false;
            Assert.Throws<AggregateException>(() =>
                DatabricksDatabase.RouteConnection<string>(
                    props,
                    _ => throw new AggregateException(new HttpRequestException(ReydenErrorText)),
                    _ => { seaCalled = true; return "sea"; }));

            Assert.False(seaCalled);
        }

        /// <summary>
        /// Routing glue: an explicit protocol=rest is routed straight to SEA and never touches the
        /// Thrift factory or the Reyden pre-check.
        /// </summary>
        [Fact]
        public void RouteConnection_ExplicitRestUsesSeaDirectly()
        {
            var props = new Dictionary<string, string>
            {
                [AdbcOptions.Uri] = "https://host/sql/1.0/warehouses/wh-any",
                [DatabricksParameters.Protocol] = "rest",
            };

            bool thriftCalled = false;
            string result = DatabricksDatabase.RouteConnection<string>(
                props,
                _ => { thriftCalled = true; return "thrift"; },
                _ => "sea");

            Assert.Equal("sea", result);
            Assert.False(thriftCalled);
        }

        /// <summary>
        /// Dispose-on-failure contract: when the open action throws, OpenOrDispose disposes the
        /// connection before rethrowing so a failed open (e.g. the one that triggers the SEA fallback)
        /// does not leak the connection's HTTP client / handler chain.
        /// </summary>
        [Fact]
        public void OpenOrDispose_DisposesConnectionWhenOpenThrows()
        {
            var connection = new TrackingDisposable();
            var thrown = Assert.Throws<InvalidOperationException>(() =>
                DatabricksDatabase.OpenOrDispose(connection, _ => throw new InvalidOperationException("open failed")));

            Assert.True(connection.Disposed);
            Assert.Equal("open failed", thrown.Message);
        }

        /// <summary>
        /// Dispose-on-failure contract: a successful open returns the connection undisposed.
        /// </summary>
        [Fact]
        public void OpenOrDispose_ReturnsConnectionWhenOpenSucceeds()
        {
            var connection = new TrackingDisposable();
            var result = DatabricksDatabase.OpenOrDispose(connection, _ => { });

            Assert.Same(connection, result);
            Assert.False(connection.Disposed);
        }

        private sealed class TrackingDisposable : IDisposable
        {
            public bool Disposed { get; private set; }

            public void Dispose() => Disposed = true;
        }

        [Fact]
        public void TryGetWarehouseCacheKey_NullForGeneralClusterPath()
        {
            var props = new Dictionary<string, string>
            {
                [AdbcOptions.Uri] = "https://host/sql/protocolv1/o/1234567890/0101-cluster",
            };
            Assert.Null(ReydenFallback.TryGetWarehouseCacheKey(props));
        }

        /// <summary>
        /// A Reyden mark for a warehouse id in one workspace must not route a healthy, Thrift-capable
        /// warehouse with the SAME id in a different workspace to SEA. Regression for the multi-tenant
        /// host scenario where warehouse ids (workspace-local counters) can collide across workspaces.
        /// </summary>
        [Fact]
        public void ReydenMarkDoesNotLeakAcrossWorkspacesWithSameWarehouseId()
        {
            var workspaceA = new Dictionary<string, string>
            {
                [AdbcOptions.Uri] = "https://workspace-a.databricks.com/sql/1.0/warehouses/000000000107b7e3",
            };
            var workspaceB = new Dictionary<string, string>
            {
                [AdbcOptions.Uri] = "https://workspace-b.databricks.com/sql/1.0/warehouses/000000000107b7e3",
            };

            string? keyA = ReydenFallback.TryGetWarehouseCacheKey(workspaceA);
            string? keyB = ReydenFallback.TryGetWarehouseCacheKey(workspaceB);

            // Workspace A's warehouse is marked Reyden.
            ReydenWarehouseCache.Mark(keyA);

            Assert.True(ReydenWarehouseCache.IsReyden(keyA));
            // Workspace B's identically-numbered warehouse is unaffected and still tries Thrift.
            Assert.False(ReydenWarehouseCache.IsReyden(keyB));
        }
    }
}
