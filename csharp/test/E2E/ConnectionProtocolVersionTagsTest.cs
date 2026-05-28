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
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests
{
    /// <summary>
    /// E2E regression test for issue #486: the OpenSession activity emits two
    /// protocol-version tags with inconsistent types
    /// (<c>connection.client_protocol</c> as string,
    /// <c>connection.server_protocol_version</c> as int).
    ///
    /// Dashboards that group/filter by Thrift protocol version had to handle both
    /// representations. The fix is additive: a new int companion tag
    /// <c>connection.client_protocol_version</c> is emitted alongside the existing
    /// string tag, with the same numeric source-of-truth value (e.g.
    /// <c>SPARK_CLI_SERVICE_PROTOCOL_V7</c> -> 7). The string tag is preserved
    /// unchanged for backward compatibility.
    ///
    /// After the fix:
    ///   connection.client_protocol         = "SPARK_CLI_SERVICE_PROTOCOL_V7" (string, existing)
    ///   connection.client_protocol_version = 7                              (int,    NEW)
    ///   connection.server_protocol_version = 7                              (int,    existing)
    ///
    /// This test asserts the new int tag exists on the same activity that already
    /// carries the string tag (<c>DatabricksConnection.CreateSessionRequest</c>),
    /// and that its numeric value matches the version suffix of the string.
    /// </summary>
    public class ConnectionProtocolVersionTagsTest : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>, IDisposable
    {
        private readonly List<(string ActivityName, IReadOnlyList<KeyValuePair<string, object?>> Tags)> _capturedTags = new();
        private readonly object _capturedTagsLock = new();
        private readonly ActivityListener _activityListener;
        private bool _disposed;

        public ConnectionProtocolVersionTagsTest(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));

            _activityListener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == "AdbcDrivers.Databricks",
                Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity =>
                {
                    lock (_capturedTagsLock)
                    {
                        // Materialize the tag list now — Activity.TagObjects is recycled
                        // after the activity stops, so we snapshot to a stable list.
                        _capturedTags.Add((activity.OperationName, activity.TagObjects.ToList()));
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
        /// Regression test for #486: when the driver opens a session, the OpenSession
        /// activity must emit an int <c>connection.client_protocol_version</c> tag
        /// alongside the existing string <c>connection.client_protocol</c> tag, with
        /// matching version numbers. The existing string tag is preserved for
        /// backward compatibility (additive fix).
        /// </summary>
        [SkippableFact]
        public void OpenAsync_EmitsIntegerClientProtocolVersion_Issue486()
        {
            lock (_capturedTagsLock) { _capturedTags.Clear(); }

            // Default Thrift parameters — issue #486 is about the OpenSession span
            // emitted on the Thrift code path (DatabricksConnection.CreateSessionRequest).
            var parameters = new Dictionary<string, string>
            {
                [DatabricksParameters.Protocol] = "thrift",
            };

            var connection = NewConnection(TestConfiguration, parameters);
            try
            {
                // Force OpenSession to run by exercising the connection.
                using AdbcStatement statement = connection.CreateStatement();
                statement.SqlQuery = "SELECT 1";
                QueryResult result = statement.ExecuteQuery();
                Assert.NotNull(result);
            }
            finally
            {
                connection.Dispose();
            }

            // Find the activity that emits connection.client_protocol today
            // (DatabricksConnection.CreateSessionRequest). The new int tag must
            // ride on the same span so dashboards can correlate the two.
            List<KeyValuePair<string, object?>> createSessionTags;
            lock (_capturedTagsLock)
            {
                createSessionTags = _capturedTags
                    .Where(e => e.ActivityName == "CreateSessionRequest")
                    .SelectMany(e => e.Tags)
                    .ToList();
            }

            OutputHelper?.WriteLine(
                "CreateSessionRequest tags: [" +
                string.Join(", ", createSessionTags.Select(t => $"{t.Key}={t.Value} ({t.Value?.GetType().Name ?? "null"})")) +
                "]");

            // Backward-compat sanity: the existing string tag must still be present.
            // Without it we cannot tell whether the activity we captured is the
            // OpenSession span at all, so this guards against the test silently
            // looking at the wrong activity.
            var stringTag = createSessionTags.FirstOrDefault(t => t.Key == "connection.client_protocol");
            Assert.True(stringTag.Value is string,
                "Expected existing string 'connection.client_protocol' tag on CreateSessionRequest. " +
                "Got: " + (stringTag.Value?.ToString() ?? "<missing>"));
            string protocolString = (string)stringTag.Value!;
            OutputHelper?.WriteLine($"connection.client_protocol (string) = '{protocolString}'");

            // Derive the expected int from the string's numeric suffix
            // (e.g. SPARK_CLI_SERVICE_PROTOCOL_V7 -> 7) so the assertion stays
            // valid even if the server bumps to V8.
            int lastUnderscore = protocolString.LastIndexOf('_');
            Assert.True(
                lastUnderscore >= 0 && lastUnderscore < protocolString.Length - 1,
                $"Cannot parse version suffix from '{protocolString}' " +
                "(expected SPARK_CLI_SERVICE_PROTOCOL_V<n>).");
            string suffix = protocolString.Substring(lastUnderscore + 1);
            Assert.StartsWith("V", suffix);
            int expectedVersion = int.Parse(suffix.Substring(1));
            OutputHelper?.WriteLine($"Expected connection.client_protocol_version (int) = {expectedVersion}");

            // Core assertion for #486: the new int companion tag must be present
            // on the same activity, with a matching numeric value. Before the fix
            // this tag does not exist, so the test fails with "tag missing".
            var intTag = createSessionTags
                .Where(t => t.Key == "connection.client_protocol_version")
                .Cast<KeyValuePair<string, object?>?>()
                .FirstOrDefault();

            Assert.True(intTag != null,
                "Expected new int 'connection.client_protocol_version' tag on " +
                "CreateSessionRequest (issue #486). Before the fix, only the string " +
                "'connection.client_protocol' tag is emitted, leaving dashboards to " +
                "handle a mix of string and int representations for the same " +
                "Thrift protocol version. Got tags: [" +
                string.Join(", ", createSessionTags.Select(t => t.Key)) + "]");

            // The new tag must be a numeric primitive (not a string), and must match
            // the version number embedded in the existing string tag.
            object? rawValue = intTag!.Value.Value;
            Assert.True(rawValue is int || rawValue is long,
                $"connection.client_protocol_version must be an integer type for " +
                $"dashboard uniformity with connection.server_protocol_version (int). " +
                $"Got type: {rawValue?.GetType().FullName ?? "<null>"}");
            long actualVersion = Convert.ToInt64(rawValue);
            Assert.Equal(expectedVersion, actualVersion);
        }
    }
}
