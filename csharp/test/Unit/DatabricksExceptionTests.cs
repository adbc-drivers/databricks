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
    }
}
