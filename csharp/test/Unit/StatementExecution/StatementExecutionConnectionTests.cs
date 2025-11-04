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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Arrow.Adbc.Drivers.Databricks.StatementExecution;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Moq;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Unit.StatementExecution
{
    public class StatementExecutionConnectionTests
    {
        private readonly Mock<IStatementExecutionClient> _mockClient;

        public StatementExecutionConnectionTests()
        {
            _mockClient = new Mock<IStatementExecutionClient>();
        }

        #region Constructor Tests

        [Fact]
        public void Constructor_WithValidParameters_CreatesConnection()
        {
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/1.0/warehouses/test-warehouse-id" }
            };

            var connection = new StatementExecutionConnection(_mockClient.Object, properties);

            Assert.NotNull(connection);
            Assert.Equal("test-warehouse-id", connection.WarehouseId);
        }

        [Fact]
        public void Constructor_WithNullClient_ThrowsArgumentNullException()
        {
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/1.0/warehouses/test-warehouse-id" }
            };

            Assert.Throws<ArgumentNullException>(() =>
                new StatementExecutionConnection(null!, properties));
        }

        [Fact]
        public void Constructor_WithNullProperties_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() =>
                new StatementExecutionConnection(_mockClient.Object, null!));
        }

        [Fact]
        public void Constructor_WithMissingHttpPath_ThrowsArgumentException()
        {
            var properties = new Dictionary<string, string>();

            var exception = Assert.Throws<ArgumentException>(() =>
                new StatementExecutionConnection(_mockClient.Object, properties));

            Assert.Contains("Missing required property", exception.Message);
        }

        [Fact]
        public void Constructor_WithEmptyHttpPath_ThrowsArgumentException()
        {
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.Path, "" }
            };

            var exception = Assert.Throws<ArgumentException>(() =>
                new StatementExecutionConnection(_mockClient.Object, properties));

            Assert.Contains("cannot be null or empty", exception.Message);
        }

        [Fact]
        public void Constructor_WithCatalogAndSchema_ExtractsValues()
        {
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/1.0/warehouses/test-warehouse-id" },
                { AdbcOptions.Connection.CurrentCatalog, "my_catalog" },
                { AdbcOptions.Connection.CurrentDbSchema, "my_schema" }
            };

            var connection = new StatementExecutionConnection(_mockClient.Object, properties);

            Assert.NotNull(connection);
            // Catalog and Schema are not exposed as public properties, but they should be used in OpenAsync
        }

        #endregion

        #region Warehouse ID Extraction Tests

        [Fact]
        public void Constructor_WithStandardHttpPath_ExtractsWarehouseId()
        {
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/1.0/warehouses/abc123def456" }
            };

            var connection = new StatementExecutionConnection(_mockClient.Object, properties);

            Assert.Equal("abc123def456", connection.WarehouseId);
        }

        [Fact]
        public void Constructor_WithTrailingSlash_ExtractsWarehouseId()
        {
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/1.0/warehouses/abc123def456/" }
            };

            var connection = new StatementExecutionConnection(_mockClient.Object, properties);

            Assert.Equal("abc123def456", connection.WarehouseId);
        }

        [Fact]
        public void Constructor_WithSparkParametersPath_ExtractsWarehouseId()
        {
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/1.0/warehouses/fallback-warehouse" }
            };

            var connection = new StatementExecutionConnection(_mockClient.Object, properties);

            Assert.Equal("fallback-warehouse", connection.WarehouseId);
        }

        [Fact]
        public void Constructor_WithUppercaseWarehouses_ExtractsWarehouseId()
        {
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/1.0/WAREHOUSES/uppercase-id" }
            };

            var connection = new StatementExecutionConnection(_mockClient.Object, properties);

            Assert.Equal("uppercase-id", connection.WarehouseId);
        }

        [Fact]
        public void Constructor_WithInvalidHttpPath_ThrowsArgumentException()
        {
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/1.0/invalid/path" }
            };

            var exception = Assert.Throws<ArgumentException>(() =>
                new StatementExecutionConnection(_mockClient.Object, properties));

            Assert.Contains("Invalid http_path format", exception.Message);
        }

        [Fact]
        public void Constructor_WithMissingWarehouseId_ThrowsArgumentException()
        {
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/1.0/warehouses/" }
            };

            var exception = Assert.Throws<ArgumentException>(() =>
                new StatementExecutionConnection(_mockClient.Object, properties));

            Assert.Contains("Invalid http_path format", exception.Message);
        }

        #endregion

        #region Session Management Tests

        [Fact]
        public async Task OpenAsync_WithSessionManagementEnabled_CreatesSession()
        {
            var expectedSessionId = "test-session-123";
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/1.0/warehouses/test-warehouse" },
                { AdbcOptions.Connection.CurrentCatalog, "main" },
                { AdbcOptions.Connection.CurrentDbSchema, "default" }
            };

            _mockClient
                .Setup(c => c.CreateSessionAsync(
                    It.IsAny<CreateSessionRequest>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new CreateSessionResponse { SessionId = expectedSessionId });

            var connection = new StatementExecutionConnection(_mockClient.Object, properties);
            await connection.OpenAsync();

            Assert.Equal(expectedSessionId, connection.SessionId);

            _mockClient.Verify(c => c.CreateSessionAsync(
                It.Is<CreateSessionRequest>(req =>
                    req.WarehouseId == "test-warehouse" &&
                    req.Catalog == "main" &&
                    req.Schema == "default"),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task OpenAsync_WithSessionManagementDisabled_DoesNotCreateSession()
        {
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/1.0/warehouses/test-warehouse" },
                { DatabricksParameters.EnableSessionManagement, "false" }
            };

            var connection = new StatementExecutionConnection(_mockClient.Object, properties);
            await connection.OpenAsync();

            Assert.Null(connection.SessionId);

            _mockClient.Verify(c => c.CreateSessionAsync(
                It.IsAny<CreateSessionRequest>(),
                It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public async Task OpenAsync_WithoutCatalogAndSchema_CreatesSessionWithoutThem()
        {
            var expectedSessionId = "test-session-456";
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/1.0/warehouses/test-warehouse" }
            };

            _mockClient
                .Setup(c => c.CreateSessionAsync(
                    It.IsAny<CreateSessionRequest>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new CreateSessionResponse { SessionId = expectedSessionId });

            var connection = new StatementExecutionConnection(_mockClient.Object, properties);
            await connection.OpenAsync();

            Assert.Equal(expectedSessionId, connection.SessionId);

            _mockClient.Verify(c => c.CreateSessionAsync(
                It.Is<CreateSessionRequest>(req =>
                    req.WarehouseId == "test-warehouse" &&
                    req.Catalog == null &&
                    req.Schema == null),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task CloseAsync_WithActiveSession_DeletesSession()
        {
            var sessionId = "test-session-789";
            var warehouseId = "test-warehouse";
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.Path, $"/sql/1.0/warehouses/{warehouseId}" }
            };

            _mockClient
                .Setup(c => c.CreateSessionAsync(
                    It.IsAny<CreateSessionRequest>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new CreateSessionResponse { SessionId = sessionId });

            var connection = new StatementExecutionConnection(_mockClient.Object, properties);
            await connection.OpenAsync();
            await connection.CloseAsync();

            Assert.Null(connection.SessionId);

            _mockClient.Verify(c => c.DeleteSessionAsync(
                sessionId,
                warehouseId,
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task CloseAsync_WithSessionManagementDisabled_DoesNotDeleteSession()
        {
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/1.0/warehouses/test-warehouse" },
                { DatabricksParameters.EnableSessionManagement, "false" }
            };

            var connection = new StatementExecutionConnection(_mockClient.Object, properties);
            await connection.OpenAsync();
            await connection.CloseAsync();

            _mockClient.Verify(c => c.DeleteSessionAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public async Task CloseAsync_WhenDeleteSessionFails_SwallowsException()
        {
            var sessionId = "test-session-999";
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/1.0/warehouses/test-warehouse" }
            };

            _mockClient
                .Setup(c => c.CreateSessionAsync(
                    It.IsAny<CreateSessionRequest>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new CreateSessionResponse { SessionId = sessionId });

            _mockClient
                .Setup(c => c.DeleteSessionAsync(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<CancellationToken>()))
                .ThrowsAsync(new Exception("Session deletion failed"));

            var connection = new StatementExecutionConnection(_mockClient.Object, properties);
            await connection.OpenAsync();

            // Should not throw
            await connection.CloseAsync();

            Assert.Null(connection.SessionId);
        }

        [Fact]
        public async Task Dispose_WithActiveSession_ClosesSession()
        {
            var sessionId = "test-session-dispose";
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/1.0/warehouses/test-warehouse" }
            };

            _mockClient
                .Setup(c => c.CreateSessionAsync(
                    It.IsAny<CreateSessionRequest>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new CreateSessionResponse { SessionId = sessionId });

            var connection = new StatementExecutionConnection(_mockClient.Object, properties);
            await connection.OpenAsync();

            connection.Dispose();

            _mockClient.Verify(c => c.DeleteSessionAsync(
                sessionId,
                "test-warehouse",
                It.IsAny<CancellationToken>()), Times.Once);
        }

        #endregion

        #region CreateStatement Tests

        [Fact]
        public void CreateStatement_ThrowsNotImplementedException()
        {
            var properties = new Dictionary<string, string>
            {
                { SparkParameters.Path, "/sql/1.0/warehouses/test-warehouse" }
            };

            var connection = new StatementExecutionConnection(_mockClient.Object, properties);

            var exception = Assert.Throws<NotImplementedException>(() => connection.CreateStatement());
            Assert.Contains("PECO-2791-B", exception.Message);
        }

        #endregion
    }
}
