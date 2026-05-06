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
    /// Tests for DatabricksException.IsObjectNotFoundException, which mirrors the JDBC
    /// driver's MetadataResultConstants.isObjectNotFoundException helper. Per JDBC spec,
    /// metadata methods should return empty result sets for non-existent catalogs,
    /// schemas, or tables rather than throwing.
    /// </summary>
    public class DatabricksExceptionTests
    {
        [Fact]
        public void IsObjectNotFoundException_SqlState42704_ReturnsTrue()
        {
            var ex = new DatabricksException("some message").SetSqlState("42704");
            Assert.True(ex.IsObjectNotFoundException());
        }

        [Fact]
        public void IsObjectNotFoundException_NoSuchCatalogInMessage_ReturnsTrue()
        {
            var ex = new DatabricksException(
                "Statement execution failed. Error Code: NO_SUCH_CATALOG_EXCEPTION, Message: Catalog 'foo' not found");
            Assert.True(ex.IsObjectNotFoundException());
        }

        [Fact]
        public void IsObjectNotFoundException_TableOrViewNotFoundInMessage_ReturnsTrue()
        {
            var ex = new DatabricksException(
                "Statement execution failed. Error Code: TABLE_OR_VIEW_NOT_FOUND, Message: Table 'foo.bar' not found");
            Assert.True(ex.IsObjectNotFoundException());
        }

        [Fact]
        public void IsObjectNotFoundException_SchemaNotFoundInMessage_ReturnsTrue()
        {
            var ex = new DatabricksException(
                "Statement execution failed. Error Code: SCHEMA_NOT_FOUND, Message: Schema 'foo' not found");
            Assert.True(ex.IsObjectNotFoundException());
        }

        [Fact]
        public void IsObjectNotFoundException_InvalidParameterValueInMessage_ReturnsTrue()
        {
            var ex = new DatabricksException(
                "Statement execution failed. Error Code: INVALID_PARAMETER_VALUE, Message: Catalog name is invalid");
            Assert.True(ex.IsObjectNotFoundException());
        }

        [Fact]
        public void IsObjectNotFoundException_LowerCaseMessage_ReturnsTrue()
        {
            // Match is case-insensitive (StringComparison.OrdinalIgnoreCase)
            var ex = new DatabricksException("error: no_such_catalog_exception thrown");
            Assert.True(ex.IsObjectNotFoundException());
        }

        [Fact]
        public void IsObjectNotFoundException_MixedCaseMessage_ReturnsTrue()
        {
            var ex = new DatabricksException("Error: Schema_Not_Found at path foo.bar");
            Assert.True(ex.IsObjectNotFoundException());
        }

        [Fact]
        public void IsObjectNotFoundException_UnrelatedSqlState_ReturnsFalse()
        {
            // 42601 is PARSE_SYNTAX_ERROR, not object-not-found
            var ex = new DatabricksException("Syntax error near 'FROM'").SetSqlState("42601");
            Assert.False(ex.IsObjectNotFoundException());
        }

        [Fact]
        public void IsObjectNotFoundException_UnrelatedMessage_ReturnsFalse()
        {
            var ex = new DatabricksException("Connection timeout while executing query");
            Assert.False(ex.IsObjectNotFoundException());
        }

        [Fact]
        public void IsObjectNotFoundException_EmptyMessageNoSqlState_ReturnsFalse()
        {
            var ex = new DatabricksException("");
            Assert.False(ex.IsObjectNotFoundException());
        }

        [Fact]
        public void IsObjectNotFoundException_DefaultConstructor_ReturnsFalse()
        {
            // Default ctor produces a generic "Exception of type ... was thrown" message — must not match
            var ex = new DatabricksException();
            Assert.False(ex.IsObjectNotFoundException());
        }

        [Fact]
        public void IsObjectNotFoundException_SqlStateTakesPrecedenceOverMessage()
        {
            // SQL state 42704 is sufficient even when the message says nothing about not-found
            var ex = new DatabricksException("Generic error message with no keywords").SetSqlState("42704");
            Assert.True(ex.IsObjectNotFoundException());
        }

        [Fact]
        public void IsObjectNotFoundException_MessageMatchEvenWithUnrelatedSqlState()
        {
            // Either condition is sufficient — message match wins even if SQL state is something else
            var ex = new DatabricksException("TABLE_OR_VIEW_NOT_FOUND occurred").SetSqlState("99999");
            Assert.True(ex.IsObjectNotFoundException());
        }
    }
}
