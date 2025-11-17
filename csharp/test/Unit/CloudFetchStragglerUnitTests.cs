/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* This file has been modified from its original version, which is
* under the Apache License:
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Unit
{
    /// <summary>
    /// Unit tests for FileDownloadMetrics functionality.
    /// Tests cover throughput calculation and straggler flag management.
    /// </summary>
    public class CloudFetchStragglerUnitTests
    {
        #region FileDownloadMetrics Tests

        [Fact]
        public void FileDownloadMetrics_CalculateThroughput_BeforeCompletion_ReturnsNull()
        {
            // Arrange
            var metrics = new FileDownloadMetrics(fileOffset: 0, fileSizeBytes: 1024);

            // Act
            var throughput = metrics.CalculateThroughputBytesPerSecond();

            // Assert
            Assert.Null(throughput);
        }

        [Fact]
        public void FileDownloadMetrics_CalculateThroughput_AfterCompletion_ReturnsValue()
        {
            // Arrange
            var metrics = new FileDownloadMetrics(fileOffset: 0, fileSizeBytes: 10 * 1024 * 1024);
            System.Threading.Thread.Sleep(100); // Simulate download time

            // Act
            metrics.MarkDownloadCompleted();
            var throughput = metrics.CalculateThroughputBytesPerSecond();

            // Assert
            Assert.NotNull(throughput);
            Assert.True(throughput.Value > 0);
        }

        [Fact]
        public void FileDownloadMetrics_MarkCancelledAsStragler_SetsFlag()
        {
            // Arrange
            var metrics = new FileDownloadMetrics(fileOffset: 0, fileSizeBytes: 1024);

            // Act
            metrics.MarkCancelledAsStragler();

            // Assert
            Assert.True(metrics.WasCancelledAsStragler);
        }

        #endregion
    }
}
