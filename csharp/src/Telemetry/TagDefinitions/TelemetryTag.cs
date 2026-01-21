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

namespace AdbcDrivers.Databricks.Telemetry.TagDefinitions
{
    /// <summary>
    /// Defines export scope for telemetry tags.
    /// Controls which destinations receive each tag.
    /// </summary>
    [Flags]
    internal enum TagExportScope
    {
        /// <summary>
        /// Tag is not exported to any destination.
        /// </summary>
        None = 0,

        /// <summary>
        /// Export to local diagnostics only (file listener, console, etc.).
        /// Suitable for sensitive data that should not leave the client.
        /// </summary>
        ExportLocal = 1,

        /// <summary>
        /// Export to Databricks telemetry service.
        /// Only non-sensitive data should use this scope.
        /// </summary>
        ExportDatabricks = 2,

        /// <summary>
        /// Export to all destinations (local and Databricks).
        /// </summary>
        ExportAll = ExportLocal | ExportDatabricks
    }

    /// <summary>
    /// Telemetry event types for categorizing activities.
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
    /// Attribute to annotate Activity tag definitions with metadata.
    /// Used to centrally define which tags are exported to which destinations.
    /// </summary>
    [AttributeUsage(AttributeTargets.Field, AllowMultiple = false)]
    internal sealed class TelemetryTagAttribute : Attribute
    {
        /// <summary>
        /// Gets the tag name as it appears in Activity tags.
        /// </summary>
        public string TagName { get; }

        /// <summary>
        /// Gets or sets the export scope for this tag.
        /// Default is ExportAll (exported to both local and Databricks).
        /// </summary>
        public TagExportScope ExportScope { get; set; }

        /// <summary>
        /// Gets or sets a human-readable description of this tag.
        /// </summary>
        public string? Description { get; set; }

        /// <summary>
        /// Gets or sets whether this tag is required for the event.
        /// </summary>
        public bool Required { get; set; }

        /// <summary>
        /// Creates a new TelemetryTagAttribute with the specified tag name.
        /// </summary>
        /// <param name="tagName">The tag name as it appears in Activity tags.</param>
        public TelemetryTagAttribute(string tagName)
        {
            TagName = tagName ?? throw new ArgumentNullException(nameof(tagName));
            ExportScope = TagExportScope.ExportAll;
            Required = false;
        }
    }
}
