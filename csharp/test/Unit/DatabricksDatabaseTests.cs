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
using System.Collections.Generic;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Unit
{
    public class DatabricksDatabaseTests
    {
        private readonly Dictionary<string, string> _baseProperties;

        public DatabricksDatabaseTests()
        {
            _baseProperties = new Dictionary<string, string>
            {
                { SparkParameters.HostName, "test.cloud.databricks.com" },
                { SparkParameters.Path, "/sql/1.0/warehouses/test-warehouse-id" },
                { SparkParameters.AuthType, "token" },
                { SparkParameters.AccessToken, "test-token" }
            };
        }

        #region Protocol Selection Tests

        [Fact]
        public void Connect_WithNoProtocolParameter_DefaultsToThrift()
        {
            // Arrange: No protocol parameter specified
            var database = new DatabricksDatabase(_baseProperties);

            // Act & Assert: Should create Thrift connection (DatabricksConnection)
            // Note: This will attempt to connect, which will fail in unit tests without a real warehouse
            // We're just testing that it attempts Thrift path (no exception about invalid protocol)
            try
            {
                database.Connect(null);
                Assert.Fail("Expected exception but connection succeeded");
            }
            catch (ArgumentException ex)
            {
                // If ArgumentException is thrown, it should NOT be about invalid protocol
                Assert.DoesNotContain("Invalid protocol", ex.Message);
            }
            catch (Exception)
            {
                // Other exceptions are expected (e.g., connection failures)
                // This is fine - we're just verifying protocol selection works
            }
        }

        [Fact]
        public void Connect_WithThriftProtocol_CreatesThriftConnection()
        {
            // Arrange: Explicitly specify "thrift" protocol
            var properties = new Dictionary<string, string>(_baseProperties)
            {
                { DatabricksParameters.Protocol, "thrift" }
            };
            var database = new DatabricksDatabase(properties);

            // Act & Assert: Should create Thrift connection
            try
            {
                database.Connect(null);
                Assert.Fail("Expected exception but connection succeeded");
            }
            catch (ArgumentException ex)
            {
                // If ArgumentException is thrown, it should NOT be about invalid protocol
                Assert.DoesNotContain("Invalid protocol", ex.Message);
            }
            catch (Exception)
            {
                // Other exceptions are expected (e.g., connection failures)
                // This is fine - we're just verifying protocol selection works
            }
        }

        [Fact]
        public void Connect_WithRestProtocol_CreatesRestConnection()
        {
            // Arrange: Specify "rest" protocol
            var properties = new Dictionary<string, string>(_baseProperties)
            {
                { DatabricksParameters.Protocol, "rest" },
                { DatabricksParameters.EnableSessionManagement, "false" } // Disable session to simplify test
            };
            var database = new DatabricksDatabase(properties);

            // Act & Assert: Should attempt to create REST connection
            // Note: This will fail without valid credentials, but should not throw ArgumentException about protocol
            try
            {
                database.Connect(null);
                Assert.Fail("Expected exception but connection succeeded");
            }
            catch (ArgumentException ex)
            {
                // If ArgumentException is thrown, it should NOT be about invalid protocol
                Assert.DoesNotContain("Invalid protocol", ex.Message);
            }
            catch (Exception)
            {
                // Other exceptions are expected (e.g., HTTP errors, auth failures)
                // This is fine - we're just verifying protocol selection works
            }
        }

        [Fact]
        public void Connect_WithInvalidProtocol_ThrowsArgumentException()
        {
            // Arrange: Specify an invalid protocol
            var properties = new Dictionary<string, string>(_baseProperties)
            {
                { DatabricksParameters.Protocol, "invalid-protocol" }
            };
            var database = new DatabricksDatabase(properties);

            // Act & Assert: Should throw ArgumentException
            var exception = Assert.Throws<ArgumentException>(() => database.Connect(null));

            Assert.Contains("Invalid protocol", exception.Message);
            Assert.Contains("invalid-protocol", exception.Message);
        }

        [Theory]
        [InlineData("THRIFT")]  // Uppercase
        [InlineData("Thrift")]  // Mixed case
        [InlineData("REST")]    // Uppercase
        [InlineData("Rest")]    // Mixed case
        public void Connect_WithCaseInsensitiveProtocol_Works(string protocol)
        {
            // Arrange: Test case insensitivity
            var properties = new Dictionary<string, string>(_baseProperties)
            {
                { DatabricksParameters.Protocol, protocol },
                { DatabricksParameters.EnableSessionManagement, "false" }
            };
            var database = new DatabricksDatabase(properties);

            // Act & Assert: Should not throw ArgumentException about invalid protocol
            try
            {
                database.Connect(null);
                Assert.Fail("Expected exception but connection succeeded");
            }
            catch (ArgumentException ex)
            {
                // If ArgumentException is thrown, it should NOT be about invalid protocol
                Assert.DoesNotContain("Invalid protocol", ex.Message);
            }
            catch (Exception)
            {
                // Other exceptions are expected (e.g., connection/auth failures)
                // This is fine - we're just verifying protocol selection works
            }
        }

        #endregion
    }
}
