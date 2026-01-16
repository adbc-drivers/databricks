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

using System.Collections.Generic;
using System.Linq;

namespace AdbcDrivers.Databricks.Telemetry.TagDefinitions
{
    /// <summary>
    /// Telemetry event types.
    /// </summary>
    internal enum TelemetryEventType
    {
        /// <summary>
        /// Connection open event.
        /// </summary>
        ConnectionOpen,

        /// <summary>
        /// Statement execution event.
        /// </summary>
        StatementExecution,

        /// <summary>
        /// Error event.
        /// </summary>
        Error
    }

    /// <summary>
    /// Central registry for all telemetry tags and events.
    /// Provides filtering logic to determine which tags should be exported based on event type.
    /// </summary>
    internal static class TelemetryTagRegistry
    {
        /// <summary>
        /// Gets all tags allowed for Databricks export by event type.
        /// </summary>
        /// <param name="eventType">The type of telemetry event.</param>
        /// <returns>A read-only collection of tag names that should be exported to Databricks for this event type.</returns>
        public static IReadOnlyCollection<string> GetDatabricksExportTags(TelemetryEventType eventType)
        {
            return eventType switch
            {
                TelemetryEventType.ConnectionOpen => ConnectionOpenEvent.GetDatabricksExportTags(),
                TelemetryEventType.StatementExecution => StatementExecutionEvent.GetDatabricksExportTags(),
                TelemetryEventType.Error => ErrorEvent.GetDatabricksExportTags(),
                _ => new HashSet<string>()
            };
        }

        /// <summary>
        /// Checks if a tag should be exported to Databricks for a given event type.
        /// </summary>
        /// <param name="eventType">The type of telemetry event.</param>
        /// <param name="tagName">The name of the tag to check.</param>
        /// <returns>True if the tag should be exported to Databricks for this event type, false otherwise.</returns>
        public static bool ShouldExportToDatabricks(TelemetryEventType eventType, string tagName)
        {
            var allowedTags = GetDatabricksExportTags(eventType);
            return allowedTags.Contains(tagName);
        }
    }
}
