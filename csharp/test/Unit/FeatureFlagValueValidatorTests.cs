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

using AdbcDrivers.Databricks;
using AdbcDrivers.HiveServer2;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    /// <summary>
    /// Tests for FeatureFlagValueValidator: server-sourced flag values for strictly-typed
    /// parameters are accepted only when they parse exactly as the connect-path getter requires;
    /// unknown keys always pass through.
    /// </summary>
    public class FeatureFlagValueValidatorTests
    {
        [Theory]
        [InlineData("true")]
        [InlineData("false")]
        [InlineData("TRUE")]
        public void Boolean_Valid_IsAcceptable(string value)
        {
            Assert.True(FeatureFlagValueValidator.IsAcceptable(DatabricksParameters.EnableFastMetadataQuery, value));
        }

        [Theory]
        [InlineData("null")]   // the observed placeholder that broke the merge queue
        [InlineData("")]
        [InlineData("yes")]
        [InlineData("1")]
        public void Boolean_Invalid_IsDropped(string value)
        {
            Assert.False(FeatureFlagValueValidator.IsAcceptable(DatabricksParameters.EnableFastMetadataQuery, value));
        }

        [Theory]
        [InlineData("3")]
        [InlineData("1")]
        public void PositiveInt_Valid_IsAcceptable(string value)
        {
            Assert.True(FeatureFlagValueValidator.IsAcceptable(DatabricksParameters.CloudFetchParallelDownloads, value));
        }

        [Theory]
        [InlineData("null")]
        [InlineData("0")]
        [InlineData("-1")]
        [InlineData("notanumber")]
        public void PositiveInt_InvalidOrNonPositive_IsDropped(string value)
        {
            Assert.False(FeatureFlagValueValidator.IsAcceptable(DatabricksParameters.CloudFetchParallelDownloads, value));
        }

        [Theory]
        [InlineData("5", true)]
        [InlineData("-5", true)]   // Int allows negatives
        [InlineData("notanumber", false)]
        [InlineData("null", false)]
        public void Int_ValidatesAsInteger(string value, bool expected)
        {
            Assert.Equal(expected, FeatureFlagValueValidator.IsAcceptable(DatabricksParameters.WaitTimeout, value));
        }

        [Fact]
        public void BaseClassApacheParameters_AreRegistered()
        {
            // PollTimeMilliseconds is a positive-int; QueryTimeoutSeconds is a plain int.
            Assert.True(FeatureFlagValueValidator.IsAcceptable(ApacheParameters.PollTimeMilliseconds, "1000"));
            Assert.False(FeatureFlagValueValidator.IsAcceptable(ApacheParameters.PollTimeMilliseconds, "null"));
            Assert.False(FeatureFlagValueValidator.IsAcceptable(ApacheParameters.PollTimeMilliseconds, "0"));

            Assert.True(FeatureFlagValueValidator.IsAcceptable(ApacheParameters.QueryTimeoutSeconds, "-5"));
            Assert.False(FeatureFlagValueValidator.IsAcceptable(ApacheParameters.QueryTimeoutSeconds, "null"));
        }

        [Theory]
        [InlineData("null")]
        [InlineData("garbage")]
        [InlineData("")]
        public void UnknownKey_AlwaysAcceptable(string value)
        {
            // Keys that aren't strictly-typed parameters are never parsed strictly on the connect
            // path, so they pass through untouched.
            Assert.True(FeatureFlagValueValidator.IsAcceptable("databricks.partnerplatform.clientConfigsFeatureFlags.someToggle", value));
        }
    }
}
