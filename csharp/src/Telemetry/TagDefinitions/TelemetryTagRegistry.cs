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

namespace AdbcDrivers.Databricks.Telemetry.TagDefinitions
{
    /// <summary>
    /// Central registry for all telemetry tags and events.
    /// Provides methods to query which tags should be exported to which destinations.
    /// </summary>
    internal static class TelemetryTagRegistry
    {
        /// <summary>
        /// Get all tags allowed for Databricks export by event type.
        /// </summary>
        /// <param name="eventType">The telemetry event type.</param>
        /// <returns>Collection of tag names that should be exported to Databricks for this event type.</returns>
        public static IReadOnlyCollection<string> GetDatabricksExportTags(TelemetryEventType eventType)
        {
            switch (eventType)
            {
                case TelemetryEventType.ConnectionOpen:
                    return ConnectionOpenEvent.GetDatabricksExportTags();
                case TelemetryEventType.StatementExecution:
                    return StatementExecutionEvent.GetDatabricksExportTags();
                case TelemetryEventType.Error:
                    return ErrorEvent.GetDatabricksExportTags();
                default:
                    return Array.Empty<string>();
            }
        }

        /// <summary>
        /// Check if a tag should be exported to Databricks for a given event type.
        /// </summary>
        /// <param name="eventType">The telemetry event type.</param>
        /// <param name="tagName">The tag name to check.</param>
        /// <returns>True if the tag should be exported to Databricks, false otherwise.</returns>
        public static bool ShouldExportToDatabricks(TelemetryEventType eventType, string tagName)
        {
            if (string.IsNullOrEmpty(tagName))
            {
                return false;
            }

            switch (eventType)
            {
                case TelemetryEventType.ConnectionOpen:
                    return ConnectionOpenEvent.ShouldExportToDatabricks(tagName);
                case TelemetryEventType.StatementExecution:
                    return StatementExecutionEvent.ShouldExportToDatabricks(tagName);
                case TelemetryEventType.Error:
                    return ErrorEvent.ShouldExportToDatabricks(tagName);
                default:
                    return false;
            }
        }

        /// <summary>
        /// Check if a tag should be exported to local diagnostics for a given event type.
        /// All tags can be exported to local diagnostics by default.
        /// </summary>
        /// <param name="eventType">The telemetry event type.</param>
        /// <param name="tagName">The tag name to check.</param>
        /// <returns>True if the tag should be exported to local diagnostics.</returns>
        public static bool ShouldExportToLocal(TelemetryEventType eventType, string tagName)
        {
            // All tags can be exported to local diagnostics
            // This provides full visibility for debugging while protecting privacy on Databricks
            return !string.IsNullOrEmpty(tagName);
        }

        /// <summary>
        /// Filter a collection of tags to only those that should be exported to Databricks.
        /// </summary>
        /// <param name="eventType">The telemetry event type.</param>
        /// <param name="tags">The tags to filter.</param>
        /// <returns>A new dictionary containing only the tags that should be exported.</returns>
        public static IReadOnlyDictionary<string, string?> FilterForDatabricksExport(
            TelemetryEventType eventType,
            IEnumerable<KeyValuePair<string, string?>> tags)
        {
            if (tags == null)
            {
                return new Dictionary<string, string?>();
            }

            var result = new Dictionary<string, string?>();

            foreach (var tag in tags)
            {
                if (ShouldExportToDatabricks(eventType, tag.Key))
                {
                    result[tag.Key] = tag.Value;
                }
            }

            return result;
        }

        /// <summary>
        /// Get the event name for a given event type.
        /// </summary>
        /// <param name="eventType">The telemetry event type.</param>
        /// <returns>The event name string.</returns>
        public static string GetEventName(TelemetryEventType eventType)
        {
            switch (eventType)
            {
                case TelemetryEventType.ConnectionOpen:
                    return ConnectionOpenEvent.EventName;
                case TelemetryEventType.StatementExecution:
                    return StatementExecutionEvent.EventName;
                case TelemetryEventType.Error:
                    return ErrorEvent.EventName;
                default:
                    return "Unknown";
            }
        }

        /// <summary>
        /// Determine the event type from an Activity operation name.
        /// </summary>
        /// <param name="operationName">The Activity operation name.</param>
        /// <param name="hasErrorTag">Whether the activity has an error.type tag.</param>
        /// <returns>The determined TelemetryEventType.</returns>
        public static TelemetryEventType DetermineEventType(string operationName, bool hasErrorTag)
        {
            // Check for errors first (error tag presence takes precedence)
            if (hasErrorTag)
            {
                return TelemetryEventType.Error;
            }

            // Map based on operation name prefix
            if (!string.IsNullOrEmpty(operationName))
            {
                if (operationName.StartsWith("Connection.", StringComparison.OrdinalIgnoreCase))
                {
                    return TelemetryEventType.ConnectionOpen;
                }

                if (operationName.StartsWith("Statement.", StringComparison.OrdinalIgnoreCase))
                {
                    return TelemetryEventType.StatementExecution;
                }
            }

            // Default to StatementExecution for unknown operations
            return TelemetryEventType.StatementExecution;
        }
    }
}
