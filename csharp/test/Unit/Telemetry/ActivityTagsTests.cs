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
using AdbcDrivers.Databricks.Telemetry.TagDefinitions;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests that Activity tags are properly set according to TagDefinitions registry.
    /// </summary>
    public class ActivityTagsTests
    {
        [Fact]
        public void StatementActivity_HasResultFormatTag()
        {
            // Arrange
            using ActivitySource activitySource = new ActivitySource("Test");
            using ActivityListener listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            };
            ActivitySource.AddActivityListener(listener);

            // Act
            using Activity? activity = activitySource.StartActivity("TestActivity");
            activity?.SetTag(StatementExecutionEvent.ResultFormat, "cloudfetch");

            // Assert
            Assert.NotNull(activity);
            string? resultFormat = activity?.GetTagItem(StatementExecutionEvent.ResultFormat) as string;
            Assert.Equal("cloudfetch", resultFormat);
        }

        [Fact]
        public void StatementActivity_HasChunkCountTag()
        {
            // Arrange
            using ActivitySource activitySource = new ActivitySource("Test");
            using ActivityListener listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            };
            ActivitySource.AddActivityListener(listener);

            // Act
            using Activity? activity = activitySource.StartActivity("TestActivity");
            activity?.SetTag(StatementExecutionEvent.ResultChunkCount, 5);

            // Assert
            Assert.NotNull(activity);
            int? chunkCount = activity?.GetTagItem(StatementExecutionEvent.ResultChunkCount) as int?;
            Assert.Equal(5, chunkCount);
        }

        [Fact]
        public void StatementActivity_HasBytesDownloadedTag()
        {
            // Arrange
            using ActivitySource activitySource = new ActivitySource("Test");
            using ActivityListener listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            };
            ActivitySource.AddActivityListener(listener);

            // Act
            using Activity? activity = activitySource.StartActivity("TestActivity");
            activity?.SetTag(StatementExecutionEvent.ResultBytesDownloaded, 1024000L);

            // Assert
            Assert.NotNull(activity);
            long? bytesDownloaded = activity?.GetTagItem(StatementExecutionEvent.ResultBytesDownloaded) as long?;
            Assert.Equal(1024000L, bytesDownloaded);
        }

        [Fact]
        public void StatementActivity_HasCompressionEnabledTag()
        {
            // Arrange
            using ActivitySource activitySource = new ActivitySource("Test");
            using ActivityListener listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            };
            ActivitySource.AddActivityListener(listener);

            // Act
            using Activity? activity = activitySource.StartActivity("TestActivity");
            activity?.SetTag(StatementExecutionEvent.ResultCompressionEnabled, true);

            // Assert
            Assert.NotNull(activity);
            bool? compressionEnabled = activity?.GetTagItem(StatementExecutionEvent.ResultCompressionEnabled) as bool?;
            Assert.True(compressionEnabled);
        }

        [Fact]
        public void StatementActivity_HasPollCountTag()
        {
            // Arrange
            using ActivitySource activitySource = new ActivitySource("Test");
            using ActivityListener listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            };
            ActivitySource.AddActivityListener(listener);

            // Act
            using Activity? activity = activitySource.StartActivity("TestActivity");
            activity?.SetTag(StatementExecutionEvent.PollCount, 3);

            // Assert
            Assert.NotNull(activity);
            int? pollCount = activity?.GetTagItem(StatementExecutionEvent.PollCount) as int?;
            Assert.Equal(3, pollCount);
        }

        [Fact]
        public void StatementActivity_HasPollLatencyTag()
        {
            // Arrange
            using ActivitySource activitySource = new ActivitySource("Test");
            using ActivityListener listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            };
            ActivitySource.AddActivityListener(listener);

            // Act
            using Activity? activity = activitySource.StartActivity("TestActivity");
            activity?.SetTag(StatementExecutionEvent.PollLatencyMs, 450L);

            // Assert
            Assert.NotNull(activity);
            long? pollLatencyMs = activity?.GetTagItem(StatementExecutionEvent.PollLatencyMs) as long?;
            Assert.Equal(450L, pollLatencyMs);
        }

        [Fact]
        public void ConnectionActivity_HasDriverVersionTag()
        {
            // Arrange
            using ActivitySource activitySource = new ActivitySource("Test");
            using ActivityListener listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            };
            ActivitySource.AddActivityListener(listener);

            // Act
            using Activity? activity = activitySource.StartActivity("TestActivity");
            activity?.SetTag(ConnectionOpenEvent.DriverVersion, "1.0.0");

            // Assert
            Assert.NotNull(activity);
            string? driverVersion = activity?.GetTagItem(ConnectionOpenEvent.DriverVersion) as string;
            Assert.Equal("1.0.0", driverVersion);
        }

        [Fact]
        public void ConnectionActivity_HasDriverOSTag()
        {
            // Arrange
            using ActivitySource activitySource = new ActivitySource("Test");
            using ActivityListener listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            };
            ActivitySource.AddActivityListener(listener);

            // Act
            using Activity? activity = activitySource.StartActivity("TestActivity");
            activity?.SetTag(ConnectionOpenEvent.DriverOS, "Linux");

            // Assert
            Assert.NotNull(activity);
            string? driverOS = activity?.GetTagItem(ConnectionOpenEvent.DriverOS) as string;
            Assert.Equal("Linux", driverOS);
        }

        [Fact]
        public void ConnectionActivity_HasDriverRuntimeTag()
        {
            // Arrange
            using ActivitySource activitySource = new ActivitySource("Test");
            using ActivityListener listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            };
            ActivitySource.AddActivityListener(listener);

            // Act
            using Activity? activity = activitySource.StartActivity("TestActivity");
            activity?.SetTag(ConnectionOpenEvent.DriverRuntime, ".NET 8.0");

            // Assert
            Assert.NotNull(activity);
            string? driverRuntime = activity?.GetTagItem(ConnectionOpenEvent.DriverRuntime) as string;
            Assert.Equal(".NET 8.0", driverRuntime);
        }

        [Fact]
        public void ConnectionActivity_HasFeatureCloudFetchTag()
        {
            // Arrange
            using ActivitySource activitySource = new ActivitySource("Test");
            using ActivityListener listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            };
            ActivitySource.AddActivityListener(listener);

            // Act
            using Activity? activity = activitySource.StartActivity("TestActivity");
            activity?.SetTag(ConnectionOpenEvent.FeatureCloudFetch, true);

            // Assert
            Assert.NotNull(activity);
            bool? cloudFetchEnabled = activity?.GetTagItem(ConnectionOpenEvent.FeatureCloudFetch) as bool?;
            Assert.True(cloudFetchEnabled);
        }

        [Fact]
        public void ConnectionActivity_HasFeatureLz4Tag()
        {
            // Arrange
            using ActivitySource activitySource = new ActivitySource("Test");
            using ActivityListener listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            };
            ActivitySource.AddActivityListener(listener);

            // Act
            using Activity? activity = activitySource.StartActivity("TestActivity");
            activity?.SetTag(ConnectionOpenEvent.FeatureLz4, true);

            // Assert
            Assert.NotNull(activity);
            bool? lz4Enabled = activity?.GetTagItem(ConnectionOpenEvent.FeatureLz4) as bool?;
            Assert.True(lz4Enabled);
        }

        [Fact]
        public void TagDefinitions_AllTagsAreRegistered()
        {
            // Test that all expected tags are present in the registry
            var connectionTags = ConnectionOpenEvent.GetDatabricksExportTags();
            var statementTags = StatementExecutionEvent.GetDatabricksExportTags();

            // Connection tags
            Assert.Contains(ConnectionOpenEvent.DriverVersion, connectionTags);
            Assert.Contains(ConnectionOpenEvent.DriverOS, connectionTags);
            Assert.Contains(ConnectionOpenEvent.DriverRuntime, connectionTags);
            Assert.Contains(ConnectionOpenEvent.FeatureCloudFetch, connectionTags);
            Assert.Contains(ConnectionOpenEvent.FeatureLz4, connectionTags);

            // Statement tags
            Assert.Contains(StatementExecutionEvent.ResultFormat, statementTags);
            Assert.Contains(StatementExecutionEvent.ResultChunkCount, statementTags);
            Assert.Contains(StatementExecutionEvent.ResultBytesDownloaded, statementTags);
            Assert.Contains(StatementExecutionEvent.ResultCompressionEnabled, statementTags);
            Assert.Contains(StatementExecutionEvent.PollCount, statementTags);
            Assert.Contains(StatementExecutionEvent.PollLatencyMs, statementTags);
        }
    }
}
