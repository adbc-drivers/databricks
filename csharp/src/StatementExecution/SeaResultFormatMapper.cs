/*
 * Copyright (c) 2025 ADBC Drivers Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
using ExecutionResultFormat = AdbcDrivers.Databricks.Telemetry.Proto.ExecutionResult.Types.Format;

namespace AdbcDrivers.Databricks.StatementExecution
{
    /// <summary>
    /// Maps the SEA wire-level (disposition, format) request pair plus the observed response
    /// shape into the typed <see cref="ExecutionResultFormat"/> proto enum used by telemetry.
    ///
    /// <para>
    /// SEA does not expose a typed result-format field on the statement — it uses string
    /// disposition + format on the request and reports back inline data or external-link
    /// references in the manifest/result. The telemetry proto, however, expects one of
    /// <c>INLINE_ARROW</c>, <c>EXTERNAL_LINKS</c>, etc. This helper applies the four-cell
    /// table from <c>PECO-3022-sea-telemetry-integration-design.md §8</c>:
    /// </para>
    ///
    /// <list type="bullet">
    ///   <item><c>INLINE</c> + <c>ARROW_STREAM</c> → <c>INLINE_ARROW</c></item>
    ///   <item><c>EXTERNAL_LINKS</c> + <c>ARROW_STREAM</c> → <c>EXTERNAL_LINKS</c></item>
    ///   <item><c>INLINE_OR_EXTERNAL_LINKS</c> + <c>ARROW_STREAM</c>, external links populated
    ///     in manifest/result → <c>EXTERNAL_LINKS</c></item>
    ///   <item><c>INLINE_OR_EXTERNAL_LINKS</c> + <c>ARROW_STREAM</c>, no external links →
    ///     <c>INLINE_ARROW</c></item>
    /// </list>
    ///
    /// <para>
    /// Non-ARROW_STREAM formats (e.g. <c>JSON_ARRAY</c>, <c>CSV</c>) are not represented in the
    /// proto enum and map to <see cref="ExecutionResultFormat.Unspecified"/>. Unknown disposition
    /// strings also map to <see cref="ExecutionResultFormat.Unspecified"/> rather than guessing.
    /// </para>
    ///
    /// <para>
    /// Pure-function: no side effects, no allocations beyond the enum return value. Safe to call
    /// once at <c>OnExecuteSucceeded</c> time without peeking into the reader.
    /// </para>
    /// </summary>
    internal static class SeaResultFormatMapper
    {
        private const string DispositionInline = "INLINE";
        private const string DispositionExternalLinks = "EXTERNAL_LINKS";
        private const string DispositionInlineOrExternalLinks = "INLINE_OR_EXTERNAL_LINKS";
        private const string FormatArrowStream = "ARROW_STREAM";

        /// <summary>
        /// Maps a SEA request's (disposition, format) and the observed response shape to a
        /// telemetry-proto <see cref="ExecutionResultFormat"/>. See class docs for the table.
        /// </summary>
        /// <param name="disposition">Request <c>disposition</c> string (case-insensitive).</param>
        /// <param name="format">Request <c>format</c> string (case-insensitive).</param>
        /// <param name="response">Response from <c>ExecuteStatementAsync</c>; may have empty
        /// or null <c>Manifest</c>/<c>Result</c> (e.g. <c>PENDING</c> state), in which case the
        /// caller may receive <see cref="ExecutionResultFormat.Unspecified"/> for the
        /// auto-disposition cells where the result shape is not yet known.</param>
        /// <returns>The mapped proto enum; <see cref="ExecutionResultFormat.Unspecified"/> for
        /// unknown disposition, non-ARROW_STREAM format, or auto-disposition with no
        /// manifest/result data yet.</returns>
        public static ExecutionResultFormat Map(
            string? disposition,
            string? format,
            ExecuteStatementResponse? response)
        {
            // The §8 mapping table covers ARROW_STREAM only — the proto enum has no entries
            // for JSON_ARRAY or CSV, so anything else falls through to Unspecified.
            if (!string.Equals(format, FormatArrowStream, StringComparison.OrdinalIgnoreCase))
            {
                return ExecutionResultFormat.Unspecified;
            }

            if (string.Equals(disposition, DispositionInline, StringComparison.OrdinalIgnoreCase))
            {
                return ExecutionResultFormat.InlineArrow;
            }

            if (string.Equals(disposition, DispositionExternalLinks, StringComparison.OrdinalIgnoreCase))
            {
                return ExecutionResultFormat.ExternalLinks;
            }

            if (string.Equals(disposition, DispositionInlineOrExternalLinks, StringComparison.OrdinalIgnoreCase))
            {
                // Auto disposition: distinguish by what the server actually produced.
                if (response == null)
                {
                    return ExecutionResultFormat.Unspecified;
                }

                if (HasExternalLinks(response))
                {
                    return ExecutionResultFormat.ExternalLinks;
                }

                // The server picks inline for small results in auto disposition. We only call
                // this when the response is non-null AND has no external links, which matches
                // the inline-attachment row of the table.
                if (HasInlineResult(response))
                {
                    return ExecutionResultFormat.InlineArrow;
                }

                // No manifest and no result data yet (e.g. PENDING) — can't safely pick.
                return ExecutionResultFormat.Unspecified;
            }

            return ExecutionResultFormat.Unspecified;
        }

        private static bool HasExternalLinks(ExecuteStatementResponse response)
        {
            // External links can be reported either on the result chunks in the manifest or
            // directly on the (hybrid) Result payload. Either is sufficient evidence.
            if (response.Manifest?.Chunks != null)
            {
                foreach (var chunk in response.Manifest.Chunks)
                {
                    if (chunk.ExternalLinks != null && chunk.ExternalLinks.Count > 0)
                    {
                        return true;
                    }
                }
            }

            if (response.Result?.ExternalLinks != null && response.Result.ExternalLinks.Count > 0)
            {
                return true;
            }

            return false;
        }

        private static bool HasInlineResult(ExecuteStatementResponse response)
        {
            // Treat presence of a Manifest as sufficient evidence the server has produced a
            // shape — combined with the no-external-links check above, that places us in the
            // inline-attachment cell of the table. The attachment bytes themselves may be
            // empty (zero-row result) but the format is still INLINE_ARROW.
            return response.Manifest != null || response.Result != null;
        }
    }
}
