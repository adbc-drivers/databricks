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

using Apache.Arrow.Adbc;
using AdbcDrivers.HiveServer2.Hive2;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    /// <summary>
    /// Tests for DatabricksException helper methods.
    /// </summary>
    public class DatabricksExceptionTests
    {
        [Theory]
        [InlineData("42601")]
        [InlineData("20000")]
        public void IsDescTableExtendedUnsupportedException_KnownSqlState_ReturnsTrue(string sqlState)
        {
            var ex = new DatabricksException("some message").SetSqlState(sqlState);
            Assert.True(ex.IsDescTableExtendedUnsupportedException());
        }

        [Fact]
        public void IsDescTableExtendedUnsupportedException_SqlStateInMessage_ReturnsTrue()
        {
            // SEA path: StatementExecutionClient builds the exception with only a message, so
            // SqlState is null and the SQL state is carried inside the text. This is the exact
            // shape that red-failed the REST merge-queue job for STATIC ONLY on a pre-rollout DBR.
            var ex = new DatabricksException(
                "Statement execution failed. State: FAILED. Error Code: BAD_REQUEST, Message: " +
                "[PARSE_SYNTAX_ERROR] Syntax error at or near 'STATIC'. SQLSTATE: 42601 (line 1, pos 94)");
            Assert.Null(ex.SqlState);
            Assert.True(ex.IsDescTableExtendedUnsupportedException());
        }

        [Fact]
        public void IsDescTableExtendedUnsupportedException_InternalErrorInMessage_ReturnsTrue()
        {
            var ex = new DatabricksException("Statement execution failed. ... SQLSTATE: 20000");
            Assert.True(ex.IsDescTableExtendedUnsupportedException());
        }

        [Fact]
        public void IsDescTableExtendedUnsupportedException_ObjectNotFound_ReturnsFalse()
        {
            // Not-found errors take the other fallback path (empty result), not the three-call path.
            var ex = new DatabricksException("TABLE_OR_VIEW_NOT_FOUND occurred").SetSqlState("42704");
            Assert.False(ex.IsDescTableExtendedUnsupportedException());
        }

        [Fact]
        public void IsDescTableExtendedUnsupportedException_UnrelatedError_ReturnsFalse()
        {
            var ex = new DatabricksException("Connection timeout while executing query");
            Assert.False(ex.IsDescTableExtendedUnsupportedException());
        }

        [Fact]
        public void IsDescTableExtendedUnsupportedException_EmptyMessageNoSqlState_ReturnsFalse()
        {
            var ex = new DatabricksException("");
            Assert.False(ex.IsDescTableExtendedUnsupportedException());
        }

        // ─── IsObjectNotFoundException ───────────────────────────────────────────────
        // Load-bearing for the SEA metadata catch: it must return true for object-not-found
        // (→ swallow to empty, matching Thrift + JDBC) and false for anything else (→ let it
        // propagate). The negative cases pin it against over-matching.

        [Fact]
        public void IsObjectNotFoundException_SqlState42704_ReturnsTrue()
        {
            var ex = new DatabricksException("some failure").SetSqlState("42704");
            Assert.True(ex.IsObjectNotFoundException());
        }

        [Theory]
        [InlineData("... [NO_SUCH_CATALOG_EXCEPTION] Catalog 'x' was not found ...")]
        [InlineData("Statement execution failed: [SCHEMA_NOT_FOUND] The schema ...")]
        [InlineData("Error Code: TABLE_OR_VIEW_NOT_FOUND, Message: The table ...")]
        [InlineData("... INVALID_PARAMETER_VALUE ... name \"\" is not a valid name ...")]
        public void IsObjectNotFoundException_KnownMessage_ReturnsTrue(string message)
        {
            var ex = new DatabricksException(message);
            Assert.True(ex.IsObjectNotFoundException());
        }

        [Fact]
        public void IsObjectNotFoundException_UnrelatedMessage_ReturnsFalse()
        {
            // ACCESS_DENIED / permission errors must NOT be swallowed.
            var ex = new DatabricksException("[ACCESS_DENIED] Permission denied on catalog 'main'")
                .SetSqlState("42501");
            Assert.False(ex.IsObjectNotFoundException());
        }

        [Fact]
        public void IsObjectNotFoundException_UnrelatedSqlState_ReturnsFalse()
        {
            var ex = new DatabricksException("Connection timeout while executing query")
                .SetSqlState("08000");
            Assert.False(ex.IsObjectNotFoundException());
        }

        [Fact]
        public void IsObjectNotFoundException_EmptyMessageNoSqlState_ReturnsFalse()
        {
            var ex = new DatabricksException("");
            Assert.False(ex.IsObjectNotFoundException());
        }

        // ─── Static overload over AdbcException (Thrift GetCrossReference catch) ──────
        // The Thrift metadata path throws HiveServer2Exception (via ThrowErrorResponse),
        // not DatabricksException, so DatabricksStatement.GetCrossReferenceAsync catches
        // `AdbcException ex when DatabricksException.IsObjectNotFoundException(ex)`. These
        // pin that the static form recognizes the HiveServer2Exception the server raises
        // for an empty-string parent catalog (SHOW FOREIGN KEYS IN CATALOG `` →
        // TABLE_OR_VIEW_NOT_FOUND / 42P01), so the catch swallows it to an empty result
        // like the JDBC reference Thrift client.
        [Fact]
        public void IsObjectNotFoundException_Static_HiveServer2ExceptionEmptyParent_ReturnsTrue()
        {
            // Exact shape captured live: SHOW FOREIGN KEYS IN CATALOG `` throws
            // TABLE_OR_VIEW_NOT_FOUND with SQLSTATE 42P01.
            var ex = new HiveServer2Exception(
                "[TABLE_OR_VIEW_NOT_FOUND] The table or view ``.``.`` cannot be found.",
                AdbcStatusCode.InternalError)
                .SetSqlState("42P01");
            Assert.True(DatabricksException.IsObjectNotFoundException(ex));
        }

        [Fact]
        public void IsObjectNotFoundException_Static_HiveServer2ExceptionInvalidParam_ReturnsTrue()
        {
            var ex = new HiveServer2Exception(
                "[INVALID_PARAMETER_VALUE] name \"\" is not a valid name.",
                AdbcStatusCode.InternalError);
            Assert.True(DatabricksException.IsObjectNotFoundException(ex));
        }

        [Fact]
        public void IsObjectNotFoundException_Static_HiveServer2ExceptionUnrelated_ReturnsFalse()
        {
            // A genuine execution failure on the Thrift path must still propagate.
            var ex = new HiveServer2Exception(
                "[ACCESS_DENIED] Permission denied", AdbcStatusCode.InternalError)
                .SetSqlState("42501");
            Assert.False(DatabricksException.IsObjectNotFoundException(ex));
        }
    }
}
