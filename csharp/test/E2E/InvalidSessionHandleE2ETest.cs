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
using System.Threading.Tasks;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Apache.Hive.Service.Rpc.Thrift;
using AdbcDrivers.HiveServer2.Hive2;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests
{
    /// <summary>
    /// Reproduces the customer-reported "Invalid SessionHandle: Session [...] is closed"
    /// (HTTP 400 BAD_REQUEST) error WITHOUT waiting for the server-side session timeout
    /// (8-12 hours).
    ///
    /// Context: a customer hit
    ///   HTTP Response code: 400, BAD_REQUEST: Invalid SessionHandle: Session [..] is closed
    /// because the server closed the session due to inactivity timeout while the driver
    /// still held the (now stale) session handle. Lowering the server timeout to make this
    /// testable was rejected as too risky (impacts other workspaces).
    ///
    /// This test induces the identical server-side condition deterministically:
    ///   1. Open a connection (server opens a session, driver stores the TSessionHandle).
    ///   2. Send a CloseSession RPC for that handle directly, WITHOUT disposing the
    ///      connection object — so the driver keeps the now-stale handle, exactly as it
    ///      would after a server-side timeout. The HTTP transport stays alive.
    ///   3. Execute a query, which sends ExecuteStatement with the stale handle.
    ///   4. Server returns INVALID_HANDLE_STATUS / "Invalid SessionHandle ... is closed".
    ///
    /// This lets us exercise any Thrift API call against a closed session on demand,
    /// covering the scenarios the customer needs without a config change or a multi-hour wait.
    /// </summary>
    public class InvalidSessionHandleE2ETest : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public InvalidSessionHandleE2ETest(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        [SkippableFact]
        public async Task ExecuteAgainstClosedSessionReturnsInvalidSessionHandle()
        {
            var parameters = new Dictionary<string, string>
            {
                [DatabricksParameters.Protocol] = "thrift",
            };

            var connection = NewConnection(TestConfiguration, parameters);
            try
            {
                // Reach the underlying driver connection to access the live session handle
                // and the Thrift client. (Test assembly has InternalsVisibleTo access.)
                var databricksConnection = (DatabricksConnection)connection;

                // 1. Establish the session by running a trivial query.
                using (var warmup = connection.CreateStatement())
                {
                    warmup.SqlQuery = "SELECT 1";
                    await warmup.ExecuteQueryAsync();
                }

                TSessionHandle? sessionHandle = databricksConnection.SessionHandle;
                Assert.NotNull(sessionHandle);
                OutputHelper?.WriteLine(
                    $"Session opened: {new Guid(sessionHandle!.SessionId.Guid)}");

                // 2. Close the session server-side WITHOUT disposing the connection.
                //    This mimics the server's inactivity-timeout cleanup: the session is
                //    gone server-side, but the driver still holds the handle and the
                //    HTTP transport is still open.
                var closeResp = await databricksConnection.Client.CloseSession(
                    new TCloseSessionReq(sessionHandle));
                OutputHelper?.WriteLine(
                    $"CloseSession status: {closeResp.Status.StatusCode}");

                // 3. + 4. Execute against the now-closed session. Tier 1: the driver must
                //    surface a clear, typed DatabricksSessionExpiredException rather than the
                //    old opaque "An unexpected error occurred while fetching results /
                //    Couldn't connect to server" wrapper.
                using var statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 1";

                var ex = await Assert.ThrowsAnyAsync<DatabricksSessionExpiredException>(
                    () => statement.ExecuteQueryAsync().AsTask());

                OutputHelper?.WriteLine($"Tier 1 — got typed exception: {ex.GetType().Name}: {ex.Message}");

                // Message clearly states the session expired/closed (not a connectivity error)...
                Assert.Contains("session has expired or was closed", ex.Message, StringComparison.OrdinalIgnoreCase);
                // ...and the underlying server signature is preserved for diagnostics.
                Assert.Contains("Invalid SessionHandle", ex.Message, StringComparison.OrdinalIgnoreCase);

                // Tier 2: the connection is now marked invalid, so a subsequent execute must
                // fail fast with the same typed exception instead of reusing the stale handle.
                using var secondStatement = connection.CreateStatement();
                secondStatement.SqlQuery = "SELECT 1";

                var fastFailEx = await Assert.ThrowsAnyAsync<DatabricksSessionExpiredException>(
                    () => secondStatement.ExecuteQueryAsync().AsTask());

                OutputHelper?.WriteLine($"Tier 2 — fast-fail exception: {fastFailEx.GetType().Name}: {fastFailEx.Message}");
                Assert.Contains("no longer usable", fastFailEx.Message, StringComparison.OrdinalIgnoreCase);
            }
            finally
            {
                // Connection is already server-side-closed; Dispose may emit a benign
                // CloseSession failure. Swallow it so the test result reflects the assertions.
                try { connection.Dispose(); } catch { /* session already closed */ }
            }
        }
    }
}
