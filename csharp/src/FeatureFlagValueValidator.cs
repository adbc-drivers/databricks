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
using AdbcDrivers.HiveServer2;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Validates server-sourced feature-flag values before they are applied as connection
    /// properties.
    /// <para>
    /// Feature flags fetched from the connector-service are merged into the connection properties,
    /// where strictly-typed parameters are later read with the throwing
    /// <c>PropertyHelper.Get*PropertyWithValidation</c> helpers. A single malformed value pushed by
    /// the service (e.g. the literal <c>"null"</c>) would therefore throw and break connection setup
    /// for <em>every</em> client of that workspace. To keep server config fail-safe, an invalid
    /// value for a strictly-typed parameter is dropped here (so the driver falls back to its default)
    /// instead of reaching the connection. User-supplied local properties are unaffected and continue
    /// to fail fast.
    /// </para>
    /// <para>
    /// The parse rules below intentionally mirror the corresponding
    /// <c>PropertyHelper.Get*PropertyWithValidation</c> getter for each parameter, so a value is
    /// dropped here exactly when the getter would have thrown. Keys not listed (untyped/string
    /// parameters, or values the connection never strictly parses) pass through unchanged.
    /// </para>
    /// </summary>
    internal static class FeatureFlagValueValidator
    {
        private enum ValueKind
        {
            Boolean,
            Int,
            PositiveInt,
            Long,
            PositiveLong,
        }

        // Strictly-typed connection parameters, keyed by their property name (== the flag name the
        // service returns). Mirrors the PropertyHelper.Get*PropertyWithValidation call sites.
        private static readonly IReadOnlyDictionary<string, ValueKind> s_typedParameters =
            new Dictionary<string, ValueKind>
            {
                // Boolean (GetBooleanPropertyWithValidation)
                [DatabricksParameters.ApplySSPWithQueries] = ValueKind.Boolean,
                [DatabricksParameters.CanDecompressLz4] = ValueKind.Boolean,
                [DatabricksParameters.EnableComplexDatatypeSupport] = ValueKind.Boolean,
                [DatabricksParameters.EnableDirectResults] = ValueKind.Boolean,
                [DatabricksParameters.EnableFastMetadataQuery] = ValueKind.Boolean,
                [DatabricksParameters.EnableMultipleCatalogSupport] = ValueKind.Boolean,
                [DatabricksParameters.EnablePKFK] = ValueKind.Boolean,
                [DatabricksParameters.EnableRunAsyncInThriftOp] = ValueKind.Boolean,
                [DatabricksParameters.RateLimitRetry] = ValueKind.Boolean,
                [DatabricksParameters.TemporarilyUnavailableRetry] = ValueKind.Boolean,
                [DatabricksParameters.TracePropagationEnabled] = ValueKind.Boolean,
                [DatabricksParameters.TraceStateEnabled] = ValueKind.Boolean,
                [DatabricksParameters.TransportErrorRetry] = ValueKind.Boolean,
                [DatabricksParameters.UseCloudFetch] = ValueKind.Boolean,
                [DatabricksParameters.UseDescTableExtended] = ValueKind.Boolean,

                // Int (GetIntPropertyWithValidation)
                [DatabricksParameters.RateLimitRetryTimeout] = ValueKind.Int,
                [DatabricksParameters.TemporarilyUnavailableRetryTimeout] = ValueKind.Int,
                [DatabricksParameters.WaitTimeout] = ValueKind.Int,

                // Positive int (GetPositiveIntPropertyWithValidation)
                [DatabricksParameters.CloudFetchMaxUrlRefreshAttempts] = ValueKind.PositiveInt,
                [DatabricksParameters.CloudFetchMemoryBufferSize] = ValueKind.PositiveInt,
                [DatabricksParameters.CloudFetchParallelDownloads] = ValueKind.PositiveInt,
                [DatabricksParameters.CloudFetchPrefetchCount] = ValueKind.PositiveInt,
                [DatabricksParameters.CloudFetchRetryDelayMs] = ValueKind.PositiveInt,
                [DatabricksParameters.CloudFetchRetryTimeoutSeconds] = ValueKind.PositiveInt,
                [DatabricksParameters.CloudFetchTimeoutMinutes] = ValueKind.PositiveInt,
                [DatabricksParameters.CloudFetchUrlExpirationBufferSeconds] = ValueKind.PositiveInt,
                [DatabricksParameters.FeatureFlagTimeoutSeconds] = ValueKind.PositiveInt,
                [DatabricksParameters.FetchHeartbeatInterval] = ValueKind.PositiveInt,
                [DatabricksParameters.OperationStatusRequestTimeout] = ValueKind.PositiveInt,

                // Positive long (GetPositiveLongPropertyWithValidation)
                [DatabricksParameters.MaxBytesPerFile] = ValueKind.PositiveLong,

                // Base-class (Apache) parameters read via PropertyHelper on the connect path.
                [ApacheParameters.QueryTimeoutSeconds] = ValueKind.Int,
                [ApacheParameters.PollTimeMilliseconds] = ValueKind.PositiveInt,
            };

        /// <summary>
        /// Returns true if <paramref name="value"/> is acceptable for the given feature-flag
        /// <paramref name="key"/>. Unknown keys always pass through. For a strictly-typed parameter,
        /// returns true only when the value parses exactly as the connection's getter requires.
        /// </summary>
        public static bool IsAcceptable(string key, string? value)
        {
            if (!s_typedParameters.TryGetValue(key, out ValueKind kind))
            {
                // Not a strictly-typed parameter; the connection never throws parsing it.
                return true;
            }

            switch (kind)
            {
                case ValueKind.Boolean:
                    return bool.TryParse(value, out _);
                case ValueKind.Int:
                    return int.TryParse(value, out _);
                case ValueKind.PositiveInt:
                    return int.TryParse(value, out int i) && i > 0;
                case ValueKind.Long:
                    return long.TryParse(value, out _);
                case ValueKind.PositiveLong:
                    return long.TryParse(value, out long l) && l > 0;
                default:
                    return true;
            }
        }
    }
}
