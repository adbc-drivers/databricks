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
using System.Net;
using System.Net.Http;
using System.Reflection;
using AdbcDrivers.Databricks;
using AdbcDrivers.Databricks.Http;
using AdbcDrivers.Databricks.StatementExecution;
using AdbcDrivers.HiveServer2.Spark;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Http
{
    /// <summary>
    /// Tests for the SEA response-compression toggle
    /// (<see cref="DatabricksParameters.SeaResponseCompressionEnabled"/>): the SEA inline result is
    /// base64-of-LZ4-Arrow-in-JSON, so gzip-ing it at the proxy is wasteful double-compression. The
    /// SEA statements client disables gzip by default; other clients (CloudFetch, Thrift) keep it.
    /// </summary>
    public class HttpClientFactoryCompressionTests
    {
        private static readonly IReadOnlyDictionary<string, string> EmptyProps = new Dictionary<string, string>();

        [Fact]
        public void CreateHandler_DefaultsToGzipEnabled()
        {
            // Existing callers (CloudFetch, feature-flag, Thrift) must keep negotiating gzip.
            using var handler = HttpClientFactory.CreateHandler(EmptyProps);
            Assert.Equal(DecompressionMethods.GZip | DecompressionMethods.Deflate, handler.AutomaticDecompression);
        }

        [Fact]
        public void CreateHandler_CompressionEnabled_NegotiatesGzip()
        {
            using var handler = HttpClientFactory.CreateHandler(EmptyProps, enableResponseCompression: true);
            Assert.Equal(DecompressionMethods.GZip | DecompressionMethods.Deflate, handler.AutomaticDecompression);
        }

        [Fact]
        public void CreateHandler_CompressionDisabled_SendsNoAcceptEncoding()
        {
            // No AutomaticDecompression => the handler adds no Accept-Encoding of its own; the SEA
            // statements client sends Accept-Encoding: identity on top (see the tests below).
            using var handler = HttpClientFactory.CreateHandler(EmptyProps, enableResponseCompression: false);
            Assert.Equal(DecompressionMethods.None, handler.AutomaticDecompression);
        }

        [Fact]
        public void SeaResponseCompression_DefaultsToDisabled()
        {
            // The SEA statements client passes this default into CreateHandler; absent the property it
            // must resolve to false (compression off) for the in-region fast path.
            bool enabledByDefault = PropertyHelper.GetBooleanPropertyWithValidation(
                EmptyProps, DatabricksParameters.SeaResponseCompressionEnabled, false);
            Assert.False(enabledByDefault);
        }

        [Fact]
        public void SeaResponseCompression_CanBeReEnabled()
        {
            var props = new Dictionary<string, string>
            {
                [DatabricksParameters.SeaResponseCompressionEnabled] = "true"
            };
            bool enabled = PropertyHelper.GetBooleanPropertyWithValidation(
                props, DatabricksParameters.SeaResponseCompressionEnabled, false);
            Assert.True(enabled);
        }

        private static string[] SeaAcceptEncoding(Dictionary<string, string> extraProperties)
        {
            var properties = new Dictionary<string, string>(extraProperties)
            {
                [SparkParameters.HostName] = "test.databricks.com",
                [SparkParameters.Path] = "/sql/1.0/warehouses/abc123",
                [SparkParameters.AccessToken] = "test-token",
            };
            using var connection = new StatementExecutionConnection(properties);
            var field = typeof(StatementExecutionConnection)
                .GetField("_httpClient", BindingFlags.NonPublic | BindingFlags.Instance);
            var httpClient = (HttpClient)field!.GetValue(connection)!;
            return httpClient.DefaultRequestHeaders.AcceptEncoding.Select(v => v.ToString()).ToArray();
        }

        [Fact]
        public void SeaClient_CompressionDisabledByDefault_RequestsIdentity()
        {
            // Without Accept-Encoding a server may pick any coding (RFC 9110 §12.5.3), and the
            // handler won't decompress, so the SEA client must ask for an uncompressed body.
            Assert.Equal(new[] { "identity" }, SeaAcceptEncoding(new Dictionary<string, string>()));
        }

        [Fact]
        public void SeaClient_CompressionEnabled_LeavesAcceptEncodingToHandler()
        {
            var props = new Dictionary<string, string>
            {
                [DatabricksParameters.SeaResponseCompressionEnabled] = "true"
            };
            Assert.Empty(SeaAcceptEncoding(props));
        }
    }
}
