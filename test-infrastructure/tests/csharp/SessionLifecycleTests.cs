/*
* Copyright (c) 2025 ADBC Drivers Contributors
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
using System.Threading.Tasks;
using Apache.Arrow.Adbc;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.ThriftProtocol
{
    /// <summary>
    /// Tests that validate driver behavior when session lifecycle operations fail.
    /// These tests verify proper error handling during OpenSession, CloseSession,
    /// and session invalidation scenarios.
    /// </summary>
    public class SessionLifecycleTests : ProxyTestBase
    {
        [Fact]
        public async Task InvalidSessionHandle_ThrowsAdbcException()
        {
            // Arrange
            await ControlClient.EnableScenarioAsync("invalid_session_handle");

            // Act & Assert
            using var connection = CreateProxiedConnection();
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 1 as test_value";

            // The driver should throw when it receives an invalid session handle error
            var exception = Assert.Throws<AdbcException>(() => statement.ExecuteQuery());
            Assert.Contains("Invalid SessionHandle", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public async Task SessionTerminatedWithActiveQuery_ThrowsAdbcException()
        {
            // Arrange
            await ControlClient.EnableScenarioAsync("session_terminated_active_query");

            // Act & Assert
            using var connection = CreateProxiedConnection();
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 1 as test_value";

            // The driver should throw when the session is terminated
            var exception = Assert.Throws<AdbcException>(() => statement.ExecuteQuery());
            Assert.Contains("Session terminated", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public async Task ConnectionResetDuringSessionEstablishment_ThrowsAdbcException()
        {
            // Arrange - Enable connection reset during session operations
            await ControlClient.EnableScenarioAsync("connection_reset_during_fetch");

            // Act & Assert
            // The driver should handle connection reset gracefully during session establishment
            var exception = Assert.Throws<AdbcException>(() =>
            {
                using var connection = CreateProxiedConnection();
            });

            // Verify the exception indicates a connection problem
            Assert.True(
                exception.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) ||
                exception.Message.Contains("reset", StringComparison.OrdinalIgnoreCase) ||
                exception.InnerException != null,
                $"Expected connection-related error, got: {exception.Message}");
        }

        [Fact]
        public async Task NormalSessionLifecycle_SucceedsWithoutFailureScenarios()
        {
            // Arrange - No failure scenarios enabled (all disabled by ProxyTestBase.InitializeAsync)

            // Act - Establish connection and execute query
            using var connection = CreateProxiedConnection();
            Assert.NotNull(connection);

            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 1 as test_value";

            using var reader = statement.ExecuteQuery();
            Assert.NotNull(reader);

            // Assert - Verify query executed successfully
            bool hasData = reader.Read();
            Assert.True(hasData);

            var schema = reader.Schema;
            Assert.NotNull(schema);
            Assert.Single(schema.FieldsList);
            Assert.Equal("test_value", schema.FieldsList[0].Name);
        }

        [Fact]
        public async Task MultipleConnectionsWithSessionFailure_EachConnectionHandlesIndependently()
        {
            // Arrange
            await ControlClient.EnableScenarioAsync("invalid_session_handle");

            // Act & Assert - Create multiple connections
            // Each should fail independently with session handle errors
            for (int i = 0; i < 3; i++)
            {
                using var connection = CreateProxiedConnection();
                using var statement = connection.CreateStatement();
                statement.SqlQuery = $"SELECT {i} as iteration";

                var exception = Assert.Throws<AdbcException>(() => statement.ExecuteQuery());
                Assert.Contains("Invalid SessionHandle", exception.Message, StringComparison.OrdinalIgnoreCase);
            }
        }

        [Fact]
        public async Task SessionFailure_ThenRecovery_SucceedsAfterDisablingScenario()
        {
            // Arrange - Enable failure scenario
            await ControlClient.EnableScenarioAsync("invalid_session_handle");

            // Act - First connection should fail
            using (var connection1 = CreateProxiedConnection())
            {
                using var statement1 = connection1.CreateStatement();
                statement1.SqlQuery = "SELECT 1 as test_value";

                var exception = Assert.Throws<AdbcException>(() => statement1.ExecuteQuery());
                Assert.Contains("Invalid SessionHandle", exception.Message, StringComparison.OrdinalIgnoreCase);
            }

            // Arrange - Disable failure scenario
            await ControlClient.DisableScenarioAsync("invalid_session_handle");

            // Act - Second connection should succeed
            using (var connection2 = CreateProxiedConnection())
            {
                using var statement2 = connection2.CreateStatement();
                statement2.SqlQuery = "SELECT 2 as test_value";

                using var reader2 = statement2.ExecuteQuery();
                Assert.NotNull(reader2);

                bool hasData = reader2.Read();
                Assert.True(hasData);
            }
        }

        [Fact]
        public async Task ConnectionClose_WithActiveSession_DisposesCleanly()
        {
            // Arrange - Create connection and verify it works
            var connection = CreateProxiedConnection();
            Assert.NotNull(connection);

            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 1 as test_value";

            using var reader = statement.ExecuteQuery();
            Assert.True(reader.Read());

            // Act - Dispose connection (which should close session)
            connection.Dispose();

            // Assert - Connection disposal should complete without exceptions
            // (xUnit will fail the test if exceptions are thrown during disposal)
        }

        [Fact]
        public async Task OpenSessionTimeout_ThrowsAdbcException()
        {
            // Arrange - Enable timeout during OpenSession
            await ControlClient.EnableScenarioAsync("open_session_timeout");

            // Act & Assert
            // The driver should timeout when OpenSession takes too long
            var exception = Assert.Throws<AdbcException>(() =>
            {
                using var connection = CreateProxiedConnection();
            });

            // Verify the exception indicates a timeout problem
            Assert.True(
                exception.Message.Contains("timeout", StringComparison.OrdinalIgnoreCase) ||
                exception.Message.Contains("timed out", StringComparison.OrdinalIgnoreCase) ||
                exception.InnerException != null,
                $"Expected timeout-related error, got: {exception.Message}");
        }

        [Fact]
        public async Task CloseSessionError_LogsButDoesNotThrow()
        {
            // Arrange - Create a working connection first
            var connection = CreateProxiedConnection();
            Assert.NotNull(connection);

            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 1 as test_value";

            using var reader = statement.ExecuteQuery();
            Assert.True(reader.Read());

            // Enable close session error scenario
            await ControlClient.EnableScenarioAsync("close_session_error");

            // Act - Dispose connection (which should attempt to close session)
            // Assert - Disposal should complete even if CloseSession fails
            // Most drivers log close errors but don't throw exceptions during disposal
            connection.Dispose();

            // If we reach here without exceptions, the test passes
            // The driver should handle CloseSession errors gracefully
        }
    }
}
