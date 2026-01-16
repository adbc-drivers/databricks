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
    /// </summary>
    [Flags]
    internal enum TagExportScope
    {
        /// <summary>
        /// Tag is not exported.
        /// </summary>
        None = 0,

        /// <summary>
        /// Tag is exported to local diagnostics (file listener, etc.).
        /// </summary>
        ExportLocal = 1,

        /// <summary>
        /// Tag is exported to Databricks telemetry service.
        /// </summary>
        ExportDatabricks = 2,

        /// <summary>
        /// Tag is exported to both local diagnostics and Databricks telemetry service.
        /// </summary>
        ExportAll = ExportLocal | ExportDatabricks
    }

    /// <summary>
    /// Attribute to annotate Activity tag definitions with export scope and metadata.
    /// </summary>
    [AttributeUsage(AttributeTargets.Field, AllowMultiple = false)]
    internal sealed class TelemetryTagAttribute : Attribute
    {
        /// <summary>
        /// Gets the tag name.
        /// </summary>
        public string TagName { get; }

        /// <summary>
        /// Gets or sets the export scope for this tag.
        /// Default is ExportAll.
        /// </summary>
        public TagExportScope ExportScope { get; set; }

        /// <summary>
        /// Gets or sets a description of what this tag represents.
        /// </summary>
        public string? Description { get; set; }

        /// <summary>
        /// Gets or sets whether this tag is required for the event.
        /// </summary>
        public bool Required { get; set; }

        /// <summary>
        /// Initializes a new instance of the TelemetryTagAttribute class.
        /// </summary>
        /// <param name="tagName">The name of the tag.</param>
        public TelemetryTagAttribute(string tagName)
        {
            TagName = tagName;
            ExportScope = TagExportScope.ExportAll;
        }
    }
}
