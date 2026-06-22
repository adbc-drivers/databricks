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
using System.Net.Http;
using AdbcDrivers.Databricks;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    /// <summary>
    /// Unit tests for <see cref="DatabricksSessionExpiredException"/>'s detection logic.
    /// These run without a workspace, so they cover the classifier in CI.
    /// </summary>
    public class DatabricksSessionExpiredExceptionTests
    {
        // The shape the driver actually produces when a stale session handle hits the server:
        // HiveServer2Reader wraps a TTransportException ("Couldn't connect to server") that wraps
        // the HttpRequestException carrying the Thrift "Invalid SessionHandle" header text.
        private static Exception BuildRealWorldChain()
        {
            var httpEx = new HttpRequestException(
                "Thrift server error: INVALID_STATE: Invalid SessionHandle: SessionHandle " +
                "[01f1604b-abec-124f-a31b-292033255223]. (HTTP 400 Bad Request)");
            var transportEx = new Exception("Couldn't connect to server: " + httpEx.Message, httpEx);
            return new Exception("An unexpected error occurred while fetching results. '" + transportEx.Message + "'", transportEx);
        }

        [Fact]
        public void IsSessionExpired_DirectInvalidSessionHandleMessage_ReturnsTrue()
        {
            var ex = new Exception("INVALID_STATE: Invalid SessionHandle: SessionHandle [abc].");
            Assert.True(DatabricksSessionExpiredException.IsSessionExpired(ex));
        }

        [Fact]
        public void IsSessionExpired_TimeoutClosedVariant_ReturnsTrue()
        {
            // The inactivity-timeout variant the customer reported.
            var ex = new Exception("BAD_REQUEST: Invalid SessionHandle: Session [abc] is closed");
            Assert.True(DatabricksSessionExpiredException.IsSessionExpired(ex));
        }

        [Fact]
        public void IsSessionExpired_NestedTransportWrappedChain_ReturnsTrue()
        {
            Assert.True(DatabricksSessionExpiredException.IsSessionExpired(BuildRealWorldChain()));
        }

        [Fact]
        public void IsSessionExpired_InsideAggregateException_ReturnsTrue()
        {
            var agg = new AggregateException("One or more errors occurred.", BuildRealWorldChain());
            Assert.True(DatabricksSessionExpiredException.IsSessionExpired(agg));
        }

        [Fact]
        public void IsSessionExpired_AlreadyTypedException_ReturnsTrue()
        {
            var ex = new DatabricksSessionExpiredException(DatabricksSessionExpiredException.FastFailMessage);
            Assert.True(DatabricksSessionExpiredException.IsSessionExpired(ex));
        }

        [Fact]
        public void IsSessionExpired_Null_ReturnsFalse()
        {
            Assert.False(DatabricksSessionExpiredException.IsSessionExpired(null));
        }

        [Fact]
        public void IsSessionExpired_UnrelatedError_ReturnsFalse()
        {
            // A genuine connectivity failure must NOT be misclassified as a session expiry.
            var ex = new Exception(
                "An unexpected error occurred while fetching results. " +
                "'Couldn't connect to server: System.Net.Http.HttpRequestException: " +
                "Connection refused (HTTP 503 Service Unavailable)'");
            Assert.False(DatabricksSessionExpiredException.IsSessionExpired(ex));
        }

        [Fact]
        public void IsSessionExpired_OtherInvalidHandleButNotSession_ReturnsFalse()
        {
            // An invalid *operation* handle is a different condition and must not be treated
            // as a session expiry (the connection's session is still valid).
            var ex = new Exception("INVALID_STATE: Invalid OperationHandle: OperationHandle [abc].");
            Assert.False(DatabricksSessionExpiredException.IsSessionExpired(ex));
        }
    }
}
