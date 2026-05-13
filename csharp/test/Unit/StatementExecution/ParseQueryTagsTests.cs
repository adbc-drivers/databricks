/*
 * Copyright (c) 2025 ADBC Drivers Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.Text.Json;
using AdbcDrivers.Databricks.StatementExecution;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    public class ParseQueryTagsTests
    {
        [Fact]
        public void ReturnsNull_WhenInputIsNull()
        {
            Assert.Null(StatementExecutionStatement.ParseQueryTags(null));
        }

        [Fact]
        public void ReturnsNull_WhenInputIsEmpty()
        {
            Assert.Null(StatementExecutionStatement.ParseQueryTags(""));
        }

        [Fact]
        public void ReturnsNull_WhenInputIsWhitespace()
        {
            Assert.Null(StatementExecutionStatement.ParseQueryTags("   "));
        }

        [Fact]
        public void ParsesSingleTag()
        {
            var tags = StatementExecutionStatement.ParseQueryTags("team:eng");
            Assert.NotNull(tags);
            Assert.Single(tags);
            Assert.Equal("team", tags[0].Key);
            Assert.Equal("eng", tags[0].Value);
        }

        [Fact]
        public void ParsesMultipleTags()
        {
            var tags = StatementExecutionStatement.ParseQueryTags("team:eng,env:prod,app:myapp");
            Assert.NotNull(tags);
            Assert.Equal(3, tags.Count);
            Assert.Equal("team", tags[0].Key);
            Assert.Equal("eng", tags[0].Value);
            Assert.Equal("env", tags[1].Key);
            Assert.Equal("prod", tags[1].Value);
            Assert.Equal("app", tags[2].Key);
            Assert.Equal("myapp", tags[2].Value);
        }

        [Fact]
        public void ParsesKeyOnlyTag()
        {
            var tags = StatementExecutionStatement.ParseQueryTags("exp");
            Assert.NotNull(tags);
            Assert.Single(tags);
            Assert.Equal("exp", tags[0].Key);
            Assert.Null(tags[0].Value);
        }

        [Fact]
        public void ParsesMixOfKeyValueAndKeyOnly()
        {
            var tags = StatementExecutionStatement.ParseQueryTags("team:eng,exp,env:prod");
            Assert.NotNull(tags);
            Assert.Equal(3, tags.Count);
            Assert.Equal("team", tags[0].Key);
            Assert.Equal("eng", tags[0].Value);
            Assert.Equal("exp", tags[1].Key);
            Assert.Null(tags[1].Value);
            Assert.Equal("env", tags[2].Key);
            Assert.Equal("prod", tags[2].Value);
        }

        [Fact]
        public void PreservesEscapedCommaInValue()
        {
            var tags = StatementExecutionStatement.ParseQueryTags(@"metadata:foo\,bar");
            Assert.NotNull(tags);
            Assert.Single(tags);
            Assert.Equal("metadata", tags[0].Key);
            Assert.Equal(@"foo\,bar", tags[0].Value);
        }

        [Fact]
        public void PreservesEscapedColonInValue()
        {
            var tags = StatementExecutionStatement.ParseQueryTags(@"metadata:{""foo""\: ""bar""}");
            Assert.NotNull(tags);
            Assert.Single(tags);
            Assert.Equal("metadata", tags[0].Key);
            Assert.Equal(@"{""foo""\: ""bar""}", tags[0].Value);
        }

        [Fact]
        public void PreservesEscapedBackslashInValue()
        {
            var tags = StatementExecutionStatement.ParseQueryTags(@"path:C\\temp");
            Assert.NotNull(tags);
            Assert.Single(tags);
            Assert.Equal("path", tags[0].Key);
            Assert.Equal(@"C\\temp", tags[0].Value);
        }

        [Fact]
        public void HandlesEscapedCommaWithMultipleTags()
        {
            var tags = StatementExecutionStatement.ParseQueryTags(@"data:a\,b,team:eng");
            Assert.NotNull(tags);
            Assert.Equal(2, tags.Count);
            Assert.Equal("data", tags[0].Key);
            Assert.Equal(@"a\,b", tags[0].Value);
            Assert.Equal("team", tags[1].Key);
            Assert.Equal("eng", tags[1].Value);
        }

        [Fact]
        public void TrimsWhitespaceAroundTokens()
        {
            var tags = StatementExecutionStatement.ParseQueryTags("  team:eng , env:prod  ");
            Assert.NotNull(tags);
            Assert.Equal(2, tags.Count);
            Assert.Equal("team", tags[0].Key);
            Assert.Equal("eng", tags[0].Value);
            Assert.Equal("env", tags[1].Key);
            Assert.Equal("prod", tags[1].Value);
        }

        [Fact]
        public void SkipsEmptyTokensBetweenCommas()
        {
            var tags = StatementExecutionStatement.ParseQueryTags("team:eng,,env:prod");
            Assert.NotNull(tags);
            Assert.Equal(2, tags.Count);
            Assert.Equal("team", tags[0].Key);
            Assert.Equal("eng", tags[0].Value);
            Assert.Equal("env", tags[1].Key);
            Assert.Equal("prod", tags[1].Value);
        }

        [Fact]
        public void HandlesValueWithMultipleColons()
        {
            var tags = StatementExecutionStatement.ParseQueryTags("url:https://example.com:8080/path");
            Assert.NotNull(tags);
            Assert.Single(tags);
            Assert.Equal("url", tags[0].Key);
            Assert.Equal("https://example.com:8080/path", tags[0].Value);
        }

        [Fact]
        public void HandlesEmptyValue()
        {
            var tags = StatementExecutionStatement.ParseQueryTags("team:");
            Assert.NotNull(tags);
            Assert.Single(tags);
            Assert.Equal("team", tags[0].Key);
            Assert.Equal("", tags[0].Value);
        }

        [Fact]
        public void SerializesToExpectedJsonFormat()
        {
            var tags = StatementExecutionStatement.ParseQueryTags("team:eng,env:prod");
            var json = JsonSerializer.Serialize(tags);
            Assert.Contains(@"""key"":""team""", json);
            Assert.Contains(@"""value"":""eng""", json);
            Assert.Contains(@"""key"":""env""", json);
            Assert.Contains(@"""value"":""prod""", json);
        }

        [Fact]
        public void HandlesAutoQueryTagWithDoubleAtPrefix()
        {
            var tags = StatementExecutionStatement.ParseQueryTags(@"@@powerbi_activity_id:abc123,team:eng");
            Assert.NotNull(tags);
            Assert.Equal(2, tags.Count);
            Assert.Equal("@@powerbi_activity_id", tags[0].Key);
            Assert.Equal("abc123", tags[0].Value);
            Assert.Equal("team", tags[1].Key);
            Assert.Equal("eng", tags[1].Value);
        }

        [Fact]
        public void HandlesMultipleAutoQueryTags()
        {
            var tags = StatementExecutionStatement.ParseQueryTags(@"@@powerbi_activity_id:abc123,@@powerbi_dataset_id:ds456,team:eng");
            Assert.NotNull(tags);
            Assert.Equal(3, tags.Count);
            Assert.Equal("@@powerbi_activity_id", tags[0].Key);
            Assert.Equal("abc123", tags[0].Value);
            Assert.Equal("@@powerbi_dataset_id", tags[1].Key);
            Assert.Equal("ds456", tags[1].Value);
            Assert.Equal("team", tags[2].Key);
            Assert.Equal("eng", tags[2].Value);
        }

        [Fact]
        public void NullQueryTags_OmittedFromRequestJson()
        {
            var request = new ExecuteStatementRequest
            {
                Statement = "SELECT 1",
                WarehouseId = "abc",
                QueryTags = null
            };
            var options = new JsonSerializerOptions { DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull };
            var json = JsonSerializer.Serialize(request, options);
            Assert.DoesNotContain("query_tags", json);
        }
    }
}
