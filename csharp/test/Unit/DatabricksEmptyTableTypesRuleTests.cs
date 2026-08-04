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
using AdbcDrivers.Databricks;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    /// <summary>
    /// METADATA-035: pins the empty-table-types substitution decision shared by the two
    /// Thrift metadata surfaces — <c>DatabricksConnection.GetObjects</c> (list overload)
    /// and <c>DatabricksStatement.GetTablesAsync</c> (string overload). An EMPTY (non-null)
    /// filter must be rewritten to the unmatchable <c>NoMatchTableTypeSentinel</c> so the
    /// shared HiveServer2 base returns zero tables (databricks-jdbc parity); a <c>null</c>
    /// filter must be left untouched (matches ALL types); any non-empty filter must pass
    /// through unchanged.
    ///
    /// These are the gating unit tests for the Thrift half of the fix: the live server
    /// round-trip is additionally covered by EmptyTableTypesE2ETests, but that suite is
    /// gated on a live workspace and does not run in ordinary CI. Reverting the
    /// substitution (or the base coalescing empty→all) fails here, in the standard unit run.
    /// </summary>
    public class DatabricksEmptyTableTypesRuleTests
    {
        // ---- GetObjects surface: IReadOnlyList<string>? overload ----

        [Fact]
        public void ListOverload_Null_IsUnchanged_MatchesAllTypes()
        {
            Assert.Null(DatabricksConstants.ApplyEmptyTableTypesRule((IReadOnlyList<string>?)null));
        }

        [Fact]
        public void ListOverload_Empty_IsRewrittenToSentinel()
        {
            var result = DatabricksConstants.ApplyEmptyTableTypesRule(new List<string>());

            Assert.NotNull(result);
            Assert.Single(result!);
            Assert.Equal(DatabricksConstants.NoMatchTableTypeSentinel, result![0]);
        }

        [Fact]
        public void ListOverload_NonEmpty_IsUnchanged()
        {
            var input = new List<string> { "TABLE", "VIEW" };

            var result = DatabricksConstants.ApplyEmptyTableTypesRule(input);

            Assert.Same(input, result);
        }

        // ---- GetTablesAsync surface: string? overload ----

        [Fact]
        public void StringOverload_Null_IsUnchanged_MatchesAllTypes()
        {
            Assert.Null(DatabricksConstants.ApplyEmptyTableTypesRule((string?)null));
        }

        [Fact]
        public void StringOverload_Empty_IsRewrittenToSentinel()
        {
            Assert.Equal(
                DatabricksConstants.NoMatchTableTypeSentinel,
                DatabricksConstants.ApplyEmptyTableTypesRule(string.Empty));
        }

        [Theory]
        [InlineData("TABLE")]
        [InlineData("TABLE,VIEW")]
        public void StringOverload_NonEmpty_IsUnchanged(string input)
        {
            Assert.Equal(input, DatabricksConstants.ApplyEmptyTableTypesRule(input));
        }

        /// <summary>
        /// The sentinel must stay a plain-ASCII, control-character-free token: an embedded
        /// NUL is the most likely thing to trip strict server/transport string validation,
        /// turning an empty filter into a hard error instead of an empty result.
        /// </summary>
        [Fact]
        public void Sentinel_IsControlCharacterFree()
        {
            foreach (char c in DatabricksConstants.NoMatchTableTypeSentinel)
            {
                Assert.False(char.IsControl(c), $"sentinel contains control char U+{(int)c:X4}");
            }
        }
    }
}
