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
using AdbcDrivers.Databricks.Reader.CloudFetch;
using AdbcDrivers.Databricks.Telemetry;
using ExecutionResultFormat = AdbcDrivers.Databricks.Telemetry.Proto.ExecutionResult.Types.Format;
using OperationType = AdbcDrivers.Databricks.Telemetry.Proto.Operation.Types.Type;
using StatementType = AdbcDrivers.Databricks.Telemetry.Proto.Statement.Types.Type;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for <see cref="NullObserver"/> — verifies it satisfies the
    /// <see cref="IStatementOperationObserver"/> fail-open / no-op / singleton contract.
    /// </summary>
    public class NullObserverTests
    {
        [Fact]
        public void NullObserver_AllMethods_AreNoOps()
        {
            // Arrange
            IStatementOperationObserver observer = NullObserver.Instance;

            // Act + Assert: every method must complete without throwing and without
            // observable side effects. We exercise the full surface twice to also
            // confirm idempotency of OnFinalized.
            for (int i = 0; i < 2; i++)
            {
                observer.OnExecuteStarted(StatementType.Query, OperationType.ExecuteStatement, isCompressed: true);
                observer.OnExecuteSucceeded("stmt-id-123", ExecutionResultFormat.InlineArrow);
                observer.OnPollCompleted(count: 3, latencyMs: 42);
                observer.OnFirstBatchReady(latencyMs: 100);
                observer.OnConsumed(latencyMs: 200);
                observer.OnChunksDownloaded(new ChunkMetrics());
                observer.OnReaderInspected(ExecutionResultFormat.ExternalLinks, isCompressed: true);
                observer.OnError(new InvalidOperationException("boom"));
                observer.OnFinalized();
            }

            // No state to inspect — passing the calls is the assertion.
        }

        [Fact]
        public void NullObserver_IsSingleton()
        {
            // Arrange + Act
            NullObserver first = NullObserver.Instance;
            NullObserver second = NullObserver.Instance;

            // Assert: same reference, and the only way to obtain an instance.
            Assert.NotNull(first);
            Assert.Same(first, second);

            // There must be no public constructor: callers should be forced to
            // use the singleton accessor.
            System.Reflection.ConstructorInfo[] publicCtors = typeof(NullObserver)
                .GetConstructors(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
            Assert.Empty(publicCtors);
        }
    }
}
