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

using System.Diagnostics;
using System.Linq;
using AdbcDrivers.Databricks.Telemetry.TagDefinitions;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    /// <summary>
    /// Tests for telemetry activity tags being set correctly.
    /// These tests verify that the tag constants are properly defined
    /// and can be used with System.Diagnostics.Activity.
    /// </summary>
    public class TelemetryActivityTagsTests
    {
        #region Statement Activity Tags Tests

        [Fact]
        public void StatementActivity_HasResultFormatTag()
        {
            // Arrange
            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            };
            ActivitySource.AddActivityListener(listener);

            using var activitySource = new ActivitySource("test.source");
            using var activity = activitySource.StartActivity("test.activity");

            // Act
            activity?.SetTag(StatementExecutionEvent.ResultFormat, "cloudfetch");

            // Assert
            Assert.NotNull(activity);
            var tag = activity.Tags.FirstOrDefault(t => t.Key == StatementExecutionEvent.ResultFormat);
            Assert.Equal("result.format", tag.Key);
            Assert.Equal("cloudfetch", tag.Value);
        }

        [Fact]
        public void StatementActivity_HasChunkCountTag()
        {
            // Arrange
            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            };
            ActivitySource.AddActivityListener(listener);

            using var activitySource = new ActivitySource("test.source");
            using var activity = activitySource.StartActivity("test.activity");

            // Act
            activity?.SetTag(StatementExecutionEvent.ResultChunkCount, 5);

            // Assert
            Assert.NotNull(activity);
            var tagValue = activity.GetTagItem(StatementExecutionEvent.ResultChunkCount);
            Assert.NotNull(tagValue);
            Assert.Equal(5, tagValue);
        }

        [Fact]
        public void StatementActivity_HasBytesDownloadedTag()
        {
            // Arrange
            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            };
            ActivitySource.AddActivityListener(listener);

            using var activitySource = new ActivitySource("test.source");
            using var activity = activitySource.StartActivity("test.activity");

            // Act
            activity?.SetTag(StatementExecutionEvent.ResultBytesDownloaded, 1048576L);

            // Assert
            Assert.NotNull(activity);
            var tagValue = activity.GetTagItem(StatementExecutionEvent.ResultBytesDownloaded);
            Assert.NotNull(tagValue);
            Assert.Equal(1048576L, tagValue);
        }

        [Fact]
        public void StatementActivity_HasPollCountTag()
        {
            // Arrange
            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            };
            ActivitySource.AddActivityListener(listener);

            using var activitySource = new ActivitySource("test.source");
            using var activity = activitySource.StartActivity("test.activity");

            // Act
            activity?.SetTag(StatementExecutionEvent.PollCount, 10);

            // Assert
            Assert.NotNull(activity);
            var tagValue = activity.GetTagItem(StatementExecutionEvent.PollCount);
            Assert.NotNull(tagValue);
            Assert.Equal(10, tagValue);
        }

        [Fact]
        public void StatementActivity_HasPollLatencyMsTag()
        {
            // Arrange
            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            };
            ActivitySource.AddActivityListener(listener);

            using var activitySource = new ActivitySource("test.source");
            using var activity = activitySource.StartActivity("test.activity");

            // Act
            activity?.SetTag(StatementExecutionEvent.PollLatencyMs, 500L);

            // Assert
            Assert.NotNull(activity);
            var tagValue = activity.GetTagItem(StatementExecutionEvent.PollLatencyMs);
            Assert.NotNull(tagValue);
            Assert.Equal(500L, tagValue);
        }

        #endregion

        #region Connection Activity Tags Tests

        [Fact]
        public void ConnectionActivity_HasDriverVersionTag()
        {
            // Arrange
            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            };
            ActivitySource.AddActivityListener(listener);

            using var activitySource = new ActivitySource("test.source");
            using var activity = activitySource.StartActivity("test.activity");

            // Act
            activity?.SetTag(ConnectionOpenEvent.DriverVersion, "1.0.0");

            // Assert
            Assert.NotNull(activity);
            var tag = activity.Tags.FirstOrDefault(t => t.Key == ConnectionOpenEvent.DriverVersion);
            Assert.Equal("driver.version", tag.Key);
            Assert.Equal("1.0.0", tag.Value);
        }

        [Fact]
        public void ConnectionActivity_HasDriverOSTag()
        {
            // Arrange
            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            };
            ActivitySource.AddActivityListener(listener);

            using var activitySource = new ActivitySource("test.source");
            using var activity = activitySource.StartActivity("test.activity");

            // Act
            activity?.SetTag(ConnectionOpenEvent.DriverOS, "Windows 10");

            // Assert
            Assert.NotNull(activity);
            var tag = activity.Tags.FirstOrDefault(t => t.Key == ConnectionOpenEvent.DriverOS);
            Assert.Equal("driver.os", tag.Key);
            Assert.Equal("Windows 10", tag.Value);
        }

        [Fact]
        public void ConnectionActivity_HasDriverRuntimeTag()
        {
            // Arrange
            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            };
            ActivitySource.AddActivityListener(listener);

            using var activitySource = new ActivitySource("test.source");
            using var activity = activitySource.StartActivity("test.activity");

            // Act
            activity?.SetTag(ConnectionOpenEvent.DriverRuntime, ".NET 8.0");

            // Assert
            Assert.NotNull(activity);
            var tag = activity.Tags.FirstOrDefault(t => t.Key == ConnectionOpenEvent.DriverRuntime);
            Assert.Equal("driver.runtime", tag.Key);
            Assert.Equal(".NET 8.0", tag.Value);
        }

        [Fact]
        public void ConnectionActivity_HasFeatureFlagsTag()
        {
            // Arrange
            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            };
            ActivitySource.AddActivityListener(listener);

            using var activitySource = new ActivitySource("test.source");
            using var activity = activitySource.StartActivity("test.activity");

            // Act
            activity?.SetTag(ConnectionOpenEvent.FeatureCloudFetch, true);
            activity?.SetTag(ConnectionOpenEvent.FeatureLz4, false);

            // Assert
            Assert.NotNull(activity);

            var cloudFetchTagValue = activity.GetTagItem(ConnectionOpenEvent.FeatureCloudFetch);
            Assert.NotNull(cloudFetchTagValue);
            Assert.Equal(true, cloudFetchTagValue);

            var lz4TagValue = activity.GetTagItem(ConnectionOpenEvent.FeatureLz4);
            Assert.NotNull(lz4TagValue);
            Assert.Equal(false, lz4TagValue);
        }

        #endregion

        #region Tag Constants Consistency Tests

        [Fact]
        public void StatementExecutionEvent_TagConstants_AreConsistent()
        {
            // Verify tag constants match the expected telemetry schema
            Assert.Equal("result.format", StatementExecutionEvent.ResultFormat);
            Assert.Equal("result.chunk_count", StatementExecutionEvent.ResultChunkCount);
            Assert.Equal("result.bytes_downloaded", StatementExecutionEvent.ResultBytesDownloaded);
            Assert.Equal("poll.count", StatementExecutionEvent.PollCount);
            Assert.Equal("poll.latency_ms", StatementExecutionEvent.PollLatencyMs);
        }

        [Fact]
        public void ConnectionOpenEvent_TagConstants_AreConsistent()
        {
            // Verify tag constants match the expected telemetry schema
            Assert.Equal("driver.version", ConnectionOpenEvent.DriverVersion);
            Assert.Equal("driver.os", ConnectionOpenEvent.DriverOS);
            Assert.Equal("driver.runtime", ConnectionOpenEvent.DriverRuntime);
            Assert.Equal("feature.cloudfetch", ConnectionOpenEvent.FeatureCloudFetch);
            Assert.Equal("feature.lz4", ConnectionOpenEvent.FeatureLz4);
        }

        #endregion
    }
}
