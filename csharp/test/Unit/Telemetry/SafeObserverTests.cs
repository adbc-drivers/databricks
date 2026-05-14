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
using System.Diagnostics;
using System.Linq;
using AdbcDrivers.Databricks.Reader.CloudFetch;
using AdbcDrivers.Databricks.Telemetry;
using ExecutionResultFormat = AdbcDrivers.Databricks.Telemetry.Proto.ExecutionResult.Types.Format;
using OperationType = AdbcDrivers.Databricks.Telemetry.Proto.Operation.Types.Type;
using StatementType = AdbcDrivers.Databricks.Telemetry.Proto.Statement.Types.Type;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for <see cref="SafeObserver"/> — the optional decorator that wraps any
    /// inner <see cref="IStatementOperationObserver"/> with per-method try/catch so a
    /// third-party observer that violates the fail-open contract cannot leak its
    /// exception to the caller.
    /// </summary>
    public class SafeObserverTests
    {
        // ── Test doubles ─────────────────────────────────────────────────────────────

        /// <summary>
        /// Records each method invocation so propagation tests can assert that calls
        /// reach the inner observer with the exact arguments supplied by the caller.
        /// </summary>
        private sealed class RecordingObserver : IStatementOperationObserver
        {
            public List<string> Calls { get; } = new List<string>();

            public StatementType? StmtType;
            public OperationType? OpType;
            public bool? IsCompressed;
            public string? StatementId;
            public ExecutionResultFormat? ResultFormat;
            public int? PollCount;
            public long? PollLatencyMs;
            public long? FirstBatchReadyMs;
            public long? ConsumedMs;
            public ChunkMetrics? ChunkMetrics;
            public Exception? Error;
            public int FinalizedCallCount;

            public void OnExecuteStarted(StatementType stmtType, OperationType opType, bool isCompressed)
            {
                Calls.Add(nameof(OnExecuteStarted));
                StmtType = stmtType;
                OpType = opType;
                IsCompressed = isCompressed;
            }

            public void OnExecuteSucceeded(string statementId, ExecutionResultFormat resultFormat)
            {
                Calls.Add(nameof(OnExecuteSucceeded));
                StatementId = statementId;
                ResultFormat = resultFormat;
            }

            public void OnPollCompleted(int count, long latencyMs)
            {
                Calls.Add(nameof(OnPollCompleted));
                PollCount = count;
                PollLatencyMs = latencyMs;
            }

            public void OnFirstBatchReady(long latencyMs)
            {
                Calls.Add(nameof(OnFirstBatchReady));
                FirstBatchReadyMs = latencyMs;
            }

            public void OnConsumed(long latencyMs)
            {
                Calls.Add(nameof(OnConsumed));
                ConsumedMs = latencyMs;
            }

            public void OnChunksDownloaded(ChunkMetrics metrics)
            {
                Calls.Add(nameof(OnChunksDownloaded));
                ChunkMetrics = metrics;
            }

            public void OnError(Exception ex)
            {
                Calls.Add(nameof(OnError));
                Error = ex;
            }

            public void OnFinalized()
            {
                Calls.Add(nameof(OnFinalized));
                FinalizedCallCount++;
            }
        }

        /// <summary>
        /// Simulates a misbehaving third-party observer: every method on
        /// <see cref="IStatementOperationObserver"/> throws. SafeObserver must absorb
        /// each exception so the caller is unaffected.
        /// </summary>
        private sealed class ThrowingObserver : IStatementOperationObserver
        {
            public int CallCount;

            public void OnExecuteStarted(StatementType stmtType, OperationType opType, bool isCompressed)
            {
                CallCount++;
                throw new InvalidOperationException("OnExecuteStarted boom");
            }

            public void OnExecuteSucceeded(string statementId, ExecutionResultFormat resultFormat)
            {
                CallCount++;
                throw new InvalidOperationException("OnExecuteSucceeded boom");
            }

            public void OnPollCompleted(int count, long latencyMs)
            {
                CallCount++;
                throw new InvalidOperationException("OnPollCompleted boom");
            }

            public void OnFirstBatchReady(long latencyMs)
            {
                CallCount++;
                throw new InvalidOperationException("OnFirstBatchReady boom");
            }

            public void OnConsumed(long latencyMs)
            {
                CallCount++;
                throw new InvalidOperationException("OnConsumed boom");
            }

            public void OnChunksDownloaded(ChunkMetrics metrics)
            {
                CallCount++;
                throw new InvalidOperationException("OnChunksDownloaded boom");
            }

            public void OnError(Exception ex)
            {
                CallCount++;
                throw new InvalidOperationException("OnError boom");
            }

            public void OnFinalized()
            {
                CallCount++;
                throw new InvalidOperationException("OnFinalized boom");
            }
        }

        // ── Required tests ───────────────────────────────────────────────────────────

        [Fact]
        public void SafeObserver_PropagatesNormalCallsToInner()
        {
            // Arrange
            RecordingObserver inner = new RecordingObserver();
            SafeObserver safe = new SafeObserver(inner);
            ChunkMetrics metrics = new ChunkMetrics
            {
                TotalChunksPresent = 4,
                TotalChunksIterated = 4,
                InitialChunkLatencyMs = 11,
                SlowestChunkLatencyMs = 99,
                SumChunksDownloadTimeMs = 137,
            };
            InvalidOperationException error = new InvalidOperationException("query failed");

            // Act: exercise the full surface in a realistic lifecycle order.
            safe.OnExecuteStarted(StatementType.Query, OperationType.ExecuteStatement, isCompressed: true);
            safe.OnExecuteSucceeded("stmt-77", ExecutionResultFormat.ExternalLinks);
            safe.OnPollCompleted(count: 5, latencyMs: 87);
            safe.OnFirstBatchReady(latencyMs: 120);
            safe.OnChunksDownloaded(metrics);
            safe.OnConsumed(latencyMs: 350);
            safe.OnError(error);
            safe.OnFinalized();

            // Assert: every call reached the inner observer, in order, with original args.
            Assert.Equal(
                new[]
                {
                    nameof(IStatementOperationObserver.OnExecuteStarted),
                    nameof(IStatementOperationObserver.OnExecuteSucceeded),
                    nameof(IStatementOperationObserver.OnPollCompleted),
                    nameof(IStatementOperationObserver.OnFirstBatchReady),
                    nameof(IStatementOperationObserver.OnChunksDownloaded),
                    nameof(IStatementOperationObserver.OnConsumed),
                    nameof(IStatementOperationObserver.OnError),
                    nameof(IStatementOperationObserver.OnFinalized),
                },
                inner.Calls);

            Assert.Equal(StatementType.Query, inner.StmtType);
            Assert.Equal(OperationType.ExecuteStatement, inner.OpType);
            Assert.True(inner.IsCompressed);
            Assert.Equal("stmt-77", inner.StatementId);
            Assert.Equal(ExecutionResultFormat.ExternalLinks, inner.ResultFormat);
            Assert.Equal(5, inner.PollCount);
            Assert.Equal(87, inner.PollLatencyMs);
            Assert.Equal(120, inner.FirstBatchReadyMs);
            Assert.Equal(350, inner.ConsumedMs);
            Assert.Same(metrics, inner.ChunkMetrics);
            Assert.Same(error, inner.Error);
            Assert.Equal(1, inner.FinalizedCallCount);

            // Also confirm the decorator exposes the wrapped instance.
            Assert.Same(inner, safe.Inner);
        }

        [Fact]
        public void SafeObserver_SwallowsExceptionsFromInner_LogsAtTrace()
        {
            // Arrange: subscribe an ActivityListener so the trace-level activity event
            // emitted on exception suppression is observable. We must have an ambient
            // Activity for the AddEvent call to take effect.
            using ActivitySource source = new ActivitySource("SafeObserverTests");
            List<ActivityEvent> capturedEvents = new List<ActivityEvent>();
            using ActivityListener listener = new ActivityListener
            {
                ShouldListenTo = s => s.Name == "SafeObserverTests",
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
                ActivityStopped = activity =>
                {
                    foreach (ActivityEvent ev in activity.Events)
                    {
                        capturedEvents.Add(ev);
                    }
                },
            };
            ActivitySource.AddActivityListener(listener);

            ThrowingObserver throwing = new ThrowingObserver();
            SafeObserver safe = new SafeObserver(throwing);

            // Act + Assert: exercise the entire surface; nothing must escape SafeObserver.
            using (Activity? activity = source.StartActivity("safe-observer-suppression-test"))
            {
                Assert.NotNull(activity); // sanity: listener is wired so an activity exists

                Exception? captured = Record.Exception(() =>
                {
                    safe.OnExecuteStarted(StatementType.Query, OperationType.ExecuteStatement, isCompressed: false);
                    safe.OnExecuteSucceeded("s1", ExecutionResultFormat.InlineArrow);
                    safe.OnPollCompleted(count: 1, latencyMs: 1);
                    safe.OnFirstBatchReady(latencyMs: 1);
                    safe.OnConsumed(latencyMs: 1);
                    safe.OnChunksDownloaded(new ChunkMetrics());
                    safe.OnError(new InvalidOperationException("propagated error"));
                    safe.OnFinalized();
                });
                Assert.Null(captured);

                // Inner observer was invoked exactly once per method (eight methods).
                Assert.Equal(8, throwing.CallCount);
            }

            // Activity has now stopped; ActivityStopped callback populates capturedEvents.
            // SafeObserver emits one suppression event per swallowed exception.
            List<ActivityEvent> suppressions = capturedEvents
                .Where(e => e.Name == "telemetry.observer.suppressed")
                .ToList();
            Assert.Equal(8, suppressions.Count);

            // Each suppression event must carry diagnostic tags identifying SafeObserver
            // as the source and including the inner exception's type and message —
            // this is what makes the suppression visible at trace level.
            foreach (ActivityEvent ev in suppressions)
            {
                Dictionary<string, object?> tags = ev.Tags.ToDictionary(kv => kv.Key, kv => kv.Value);
                Assert.Equal(nameof(InvalidOperationException), tags["error.type"]);
                Assert.Equal("SafeObserver", tags["observer.suppressed.source"]);
                Assert.Contains("boom", (string)tags["error.message"]!);
            }
        }

        // ── Additional coverage ──────────────────────────────────────────────────────

        [Fact]
        public void SafeObserver_Constructor_RejectsNullInner()
        {
            Assert.Throws<ArgumentNullException>(() => new SafeObserver(null!));
        }

        [Fact]
        public void SafeObserver_SwallowsException_WithoutAmbientActivity()
        {
            // Arrange: no ActivityListener subscribed → Activity.Current is null. The
            // suppression path must still succeed without throwing.
            ThrowingObserver throwing = new ThrowingObserver();
            SafeObserver safe = new SafeObserver(throwing);

            // Act + Assert
            Exception? captured = Record.Exception(() =>
            {
                safe.OnExecuteStarted(StatementType.Query, OperationType.ExecuteStatement, isCompressed: false);
                safe.OnFinalized();
            });
            Assert.Null(captured);
            Assert.Equal(2, throwing.CallCount);
        }
    }
}
