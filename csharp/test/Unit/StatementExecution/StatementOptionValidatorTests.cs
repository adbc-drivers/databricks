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
using AdbcDrivers.Databricks.StatementExecution;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    /// <summary>
    /// Pins the SEA statement-option validation to match the Thrift path (PECO-3011): every invalid
    /// value throws <see cref="ArgumentOutOfRangeException"/>. The InlineData mirrors the inherited
    /// CanSetOption{PollTime,BatchSize,QueryTimeout} theories so the local check tracks the E2E ones.
    /// </summary>
    public class StatementOptionValidatorTests
    {
        // --- PollTime: numeric >= 0 ---
        [Theory]
        [InlineData("-1")]
        [InlineData("zero")]
        [InlineData("-2147483648")]
        [InlineData("2147483648")] // overflows int
        [InlineData("")]
        public void ValidatePollTime_Invalid_Throws(string value) =>
            Assert.Throws<ArgumentOutOfRangeException>(() => StatementOptionValidator.ValidatePollTime("k", value));

        [Theory]
        [InlineData("0", 0)]
        [InlineData("1", 1)]
        [InlineData("2147483647", 2147483647)]
        public void ValidatePollTime_Valid_ReturnsValue(string value, int expected) =>
            Assert.Equal(expected, StatementOptionValidator.ValidatePollTime("k", value));

        // --- BatchSize: numeric (long) > 0 ---
        [Theory]
        [InlineData("-1")]
        [InlineData("one")]
        [InlineData("-2147483648")]
        [InlineData("9223372036854775808")] // overflows long
        [InlineData("0")]
        [InlineData("")]
        public void ValidateBatchSize_Invalid_Throws(string value) =>
            Assert.Throws<ArgumentOutOfRangeException>(() => StatementOptionValidator.ValidateBatchSize("k", value));

        [Theory]
        [InlineData("1", 1L)]
        [InlineData("2147483648", 2147483648L)]
        [InlineData("9223372036854775807", 9223372036854775807L)]
        public void ValidateBatchSize_Valid_ReturnsValue(string value, long expected) =>
            Assert.Equal(expected, StatementOptionValidator.ValidateBatchSize("k", value));

        // --- QueryTimeout: numeric >= 0 (0 = infinite) ---
        [Theory]
        [InlineData("-1")]
        [InlineData("zero")]
        [InlineData("-2147483648")]
        [InlineData("2147483648")]
        [InlineData("")]
        public void ValidateQueryTimeout_Invalid_Throws(string value) =>
            Assert.Throws<ArgumentOutOfRangeException>(() => StatementOptionValidator.ValidateQueryTimeout("k", value));

        [Theory]
        [InlineData("0", 0)]
        [InlineData("60", 60)]
        public void ValidateQueryTimeout_Valid_ReturnsValue(string value, int expected) =>
            Assert.Equal(expected, StatementOptionValidator.ValidateQueryTimeout("k", value));

        [Fact]
        public void Messages_MatchThriftWording()
        {
            var poll = Assert.Throws<ArgumentOutOfRangeException>(() => StatementOptionValidator.ValidatePollTime("k", "-1"));
            Assert.Contains("Must be a numeric value greater than or equal to 0.", poll.Message);

            var batch = Assert.Throws<ArgumentOutOfRangeException>(() => StatementOptionValidator.ValidateBatchSize("k", "0"));
            Assert.Contains("Must be a numeric value greater than zero.", batch.Message);

            var timeout = Assert.Throws<ArgumentOutOfRangeException>(() => StatementOptionValidator.ValidateQueryTimeout("k", "-1"));
            Assert.Contains("Must be a numeric value of 0 (infinite) or greater.", timeout.Message);
        }
    }
}
