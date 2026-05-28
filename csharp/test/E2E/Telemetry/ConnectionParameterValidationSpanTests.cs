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
using AdbcDrivers.HiveServer2.Spark;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests.E2E.Telemetry
{
    /// <summary>
    /// E2E regression test for issue #488: connection-parameter-validation
    /// failures emit zero spans.
    ///
    /// <see cref="DatabricksConnectionTest.CanDetectConnectionParameterErrors"/>
    /// runs 93 parameterized sub-cases that intentionally throw
    /// <see cref="ArgumentException"/> / <see cref="ArgumentOutOfRangeException"/>
    /// from the connection-parameter parser. All 93 throw as expected — but no
    /// span is recorded for any of them because the parser runs synchronously
    /// inside the <see cref="DatabricksConnection"/> constructor (invoked from
    /// <see cref="DatabricksDatabase.Connect"/>) BEFORE any <see cref="Activity"/>
    /// is opened.
    ///
    /// Contrast: <c>DriverTests.CanDetectInvalidServer</c> /
    /// <c>CanDetectInvalidAuthentication</c> DO emit Status=Error spans because
    /// those failures happen inside <c>HiveServer2Connection.OpenAsync</c>, which
    /// is already wrapped in a <c>TraceActivityAsync</c>. The gap is specifically
    /// the synchronous-throw-before-Activity code path.
    ///
    /// For fleet-level observability ("X% of connection attempts fail at param
    /// validation") this signal is currently invisible. The fix is to open an
    /// Activity at the outermost synchronous entry point (<c>Connect</c>) BEFORE
    /// the parser runs, so the throw is recorded as a Status=Error span with an
    /// <c>exception</c> event carrying <c>exception.type</c>.
    ///
    /// This test asserts that at least one span emitted during a failing
    /// <c>database.Connect(parameters)</c> call has
    /// <see cref="ActivityStatusCode.Error"/> AND carries an <c>exception</c>
    /// event whose <c>exception.type</c> tag matches the thrown exception. Before
    /// the fix, no spans are emitted at all, so the test fails with
    /// "no error spans captured".
    /// </summary>
    public class ConnectionParameterValidationSpanTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>, IDisposable
    {
        private readonly List<CapturedActivity> _capturedActivities = new();
        private readonly object _capturedLock = new();
        private readonly ActivityListener _activityListener;
        private bool _disposed;

        public ConnectionParameterValidationSpanTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            // This test does not require a live workspace — it deliberately
            // triggers a parameter-validation failure with bogus inputs. Run it
            // unconditionally so the visibility gap can be measured even in
            // environments without DATABRICKS_TEST_CONFIG_FILE.
            _activityListener = new ActivityListener
            {
                ShouldListenTo = source =>
                    source.Name.StartsWith("AdbcDrivers.", StringComparison.Ordinal),
                Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity =>
                {
                    lock (_capturedLock)
                    {
                        // Snapshot both tags and events: Activity recycles its
                        // internal storage after Stop, so a deferred read would
                        // see stale or empty data.
                        var events = activity.Events
                            .Select(e => new CapturedEvent(
                                e.Name,
                                e.Tags.ToList()))
                            .ToList();
                        _capturedActivities.Add(new CapturedActivity(
                            activity.Source.Name,
                            activity.OperationName,
                            activity.Status,
                            activity.TagObjects.ToList(),
                            events));
                    }
                }
            };
            ActivitySource.AddActivityListener(_activityListener);
        }

        protected override void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _activityListener.Dispose();
                }
                _disposed = true;
            }
            base.Dispose(disposing);
        }

        /// <summary>
        /// Regression test for #488: a connection-parameter-validation failure
        /// (here: <c>adbc.databricks.fetch_heartbeat_interval = "notanumber"</c>,
        /// which throws <see cref="ArgumentException"/> from the parser) must
        /// produce at least one <see cref="ActivityStatusCode.Error"/> span with
        /// an <c>exception</c> event whose <c>exception.type</c> matches the
        /// thrown exception.
        ///
        /// Before the fix, the validation throws synchronously from the
        /// <see cref="DatabricksConnection"/> constructor BEFORE any Activity is
        /// opened, so the listener captures zero spans for this code path.
        /// </summary>
        [Fact]
        public void Connect_WithInvalidParameter_EmitsValidationErrorSpan_Issue488()
        {
            lock (_capturedLock) { _capturedActivities.Clear(); }

            // Bogus value for a parsed-as-int property — the parser throws
            // ArgumentException synchronously from inside the DatabricksConnection
            // constructor, invoked by DatabricksDatabase.Connect.
            var parameters = new Dictionary<string, string>
            {
                [SparkParameters.Type] = SparkServerTypeConstants.Http,
                [SparkParameters.HostName] = "valid.server.com",
                [AdbcOptions.Username] = "user",
                [AdbcOptions.Password] = "myPassword",
                [DatabricksParameters.FetchHeartbeatInterval] = "notanumber",
            };

            AdbcDriver driver = NewDriver;
            AdbcDatabase database = driver.Open(parameters);

            var thrown = Assert.ThrowsAny<ArgumentException>(() => database.Connect(parameters));
            OutputHelper?.WriteLine($"Threw {thrown.GetType().FullName}: {thrown.Message}");

            // Snapshot what the listener saw. Before the fix this is empty for
            // the Connect call — that is precisely the gap #488 describes.
            List<CapturedActivity> activities;
            lock (_capturedLock)
            {
                activities = _capturedActivities.ToList();
            }

            OutputHelper?.WriteLine($"Captured {activities.Count} activities total during Connect():");
            foreach (var a in activities)
            {
                OutputHelper?.WriteLine(
                    $"  source={a.SourceName} op={a.OperationName} status={a.Status} events=[{string.Join(",", a.Events.Select(e => e.Name))}]");
            }

            // Core assertion: at least one captured activity must be the
            // validation-failure span — Status=Error with an "exception" event
            // carrying exception.type matching the thrown exception.
            var errorActivities = activities
                .Where(a => a.Status == ActivityStatusCode.Error)
                .ToList();

            Assert.True(
                errorActivities.Count > 0,
                "Issue #488: connection-parameter-validation failed (threw " +
                $"{thrown.GetType().Name}) but no Status=Error spans were captured. " +
                "The validation runs synchronously before any Activity is opened, " +
                "so fleet-level dashboards cannot aggregate parameter-mistake errors. " +
                $"Total spans captured: {activities.Count}.");

            // The Status=Error span must also carry the exception detail so
            // dashboards can group by exception.type (ArgumentException vs
            // ArgumentOutOfRangeException, etc.). AddException emits an event
            // named "exception" with tags including "exception.type".
            var matchingException = errorActivities
                .SelectMany(a => a.Events.Select(e => new { Activity = a, Event = e }))
                .Where(x => string.Equals(x.Event.Name, "exception", StringComparison.Ordinal))
                .Select(x => new
                {
                    x.Activity,
                    ExceptionType = x.Event.Tags
                        .FirstOrDefault(t => string.Equals(t.Key, "exception.type", StringComparison.Ordinal))
                        .Value?.ToString(),
                })
                .FirstOrDefault(x => x.ExceptionType != null &&
                                     x.ExceptionType.EndsWith(thrown.GetType().Name, StringComparison.Ordinal));

            Assert.True(
                matchingException != null,
                "Issue #488: an Error span was captured but it did not carry an " +
                "'exception' event with exception.type matching the thrown " +
                $"{thrown.GetType().Name}. Without this, fleet dashboards cannot " +
                "distinguish ArgumentException (bad bool/number) from " +
                "ArgumentOutOfRangeException (out-of-range value). Error spans: [" +
                string.Join(", ", errorActivities.Select(a => $"{a.SourceName}/{a.OperationName}")) +
                "].");

            OutputHelper?.WriteLine(
                $"Validation error span captured: source={matchingException!.Activity.SourceName} " +
                $"op={matchingException.Activity.OperationName} exception.type={matchingException.ExceptionType}");
        }

        private sealed class CapturedActivity
        {
            public CapturedActivity(
                string sourceName,
                string operationName,
                ActivityStatusCode status,
                IReadOnlyList<KeyValuePair<string, object?>> tags,
                IReadOnlyList<CapturedEvent> events)
            {
                SourceName = sourceName;
                OperationName = operationName;
                Status = status;
                Tags = tags;
                Events = events;
            }

            public string SourceName { get; }
            public string OperationName { get; }
            public ActivityStatusCode Status { get; }
            public IReadOnlyList<KeyValuePair<string, object?>> Tags { get; }
            public IReadOnlyList<CapturedEvent> Events { get; }
        }

        private sealed class CapturedEvent
        {
            public CapturedEvent(string name, IReadOnlyList<KeyValuePair<string, object?>> tags)
            {
                Name = name;
                Tags = tags;
            }

            public string Name { get; }
            public IReadOnlyList<KeyValuePair<string, object?>> Tags { get; }
        }
    }
}
