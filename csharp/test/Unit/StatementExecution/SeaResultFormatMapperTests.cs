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
using AdbcDrivers.Databricks.StatementExecution;
using Xunit;
using ExecutionResultFormat = AdbcDrivers.Databricks.Telemetry.Proto.ExecutionResult.Types.Format;

namespace AdbcDrivers.Databricks.Tests.Unit.StatementExecution
{
    /// <summary>
    /// Verifies the four-cell mapping table from
    /// <c>PECO-3022-sea-telemetry-integration-design.md §8</c> implemented by
    /// <see cref="SeaResultFormatMapper"/>:
    ///
    /// <list type="bullet">
    ///   <item><c>INLINE</c> + <c>ARROW_STREAM</c> → <c>INLINE_ARROW</c></item>
    ///   <item><c>EXTERNAL_LINKS</c> + <c>ARROW_STREAM</c> → <c>EXTERNAL_LINKS</c></item>
    ///   <item><c>INLINE_OR_EXTERNAL_LINKS</c> + external_links populated → <c>EXTERNAL_LINKS</c></item>
    ///   <item><c>INLINE_OR_EXTERNAL_LINKS</c> + inline attachment → <c>INLINE_ARROW</c></item>
    /// </list>
    ///
    /// Plus defensive edge cases: non-ARROW_STREAM format, unknown disposition, and missing
    /// response data all fall back to <see cref="ExecutionResultFormat.Unspecified"/> rather
    /// than guessing.
    /// </summary>
    public class SeaResultFormatMapperTests
    {
        // ── Helpers ───────────────────────────────────────────────────────────────

        private static ExecuteStatementResponse BuildInlineResponse()
        {
            // An "inline attachment" response: manifest present (so the server has produced a
            // result shape), no external_links anywhere, attachment bytes available on Result.
            return new ExecuteStatementResponse
            {
                StatementId = "stmt-inline",
                Status = new StatementStatus { State = "SUCCEEDED" },
                Manifest = new ResultManifest
                {
                    Format = "ARROW_STREAM",
                    TotalRowCount = 1,
                    Chunks = new List<ResultChunk>(),
                },
                Result = new ResultData
                {
                    Attachment = new byte[] { 0x01, 0x02, 0x03 },
                    RowCount = 1,
                },
            };
        }

        private static ExecuteStatementResponse BuildExternalLinksResponseInChunks()
        {
            // External-links response where the link sits on a manifest chunk — this is the
            // shape EXTERNAL_LINKS disposition uses and the most common shape that hybrid
            // (INLINE_OR_EXTERNAL_LINKS) takes when the result is large.
            return new ExecuteStatementResponse
            {
                StatementId = "stmt-ext-chunk",
                Status = new StatementStatus { State = "SUCCEEDED" },
                Manifest = new ResultManifest
                {
                    Format = "ARROW_STREAM",
                    TotalRowCount = 1000,
                    Chunks = new List<ResultChunk>
                    {
                        new()
                        {
                            ChunkIndex = 0,
                            ExternalLinks = new List<ExternalLink>
                            {
                                new() { ExternalLinkUrl = "https://example.com/chunk-0" },
                            },
                        },
                    },
                },
            };
        }

        private static ExecuteStatementResponse BuildExternalLinksResponseInResult()
        {
            // External-links response where the link sits on the Result payload (hybrid mode
            // can produce this shape when the first chunk is delivered out-of-band).
            return new ExecuteStatementResponse
            {
                StatementId = "stmt-ext-result",
                Status = new StatementStatus { State = "SUCCEEDED" },
                Manifest = new ResultManifest
                {
                    Format = "ARROW_STREAM",
                    Chunks = new List<ResultChunk>(),
                },
                Result = new ResultData
                {
                    ExternalLinks = new List<ExternalLink>
                    {
                        new() { ExternalLinkUrl = "https://example.com/result-0" },
                    },
                },
            };
        }

        // ── Four-cell table tests (per design §8) ─────────────────────────────────

        [Fact]
        public void Map_InlineDisposition_ReturnsInlineArrow()
        {
            // Cell 1: INLINE + ARROW_STREAM → INLINE_ARROW. With INLINE disposition the server
            // is contractually required to deliver inline data; the mapper does not need to
            // inspect the response shape — disposition alone determines the answer.
            var response = BuildInlineResponse();

            var result = SeaResultFormatMapper.Map("INLINE", "ARROW_STREAM", response);

            Assert.Equal(ExecutionResultFormat.InlineArrow, result);
        }

        [Fact]
        public void Map_ExternalLinksDisposition_ReturnsExternalLinks()
        {
            // Cell 2: EXTERNAL_LINKS + ARROW_STREAM → EXTERNAL_LINKS. Mirror of Cell 1 — the
            // server is contractually required to deliver external links, so we do not need to
            // verify their presence in the response. (Doing so would still pass, but the design
            // explicitly does not require it.)
            var response = BuildExternalLinksResponseInChunks();

            var result = SeaResultFormatMapper.Map("EXTERNAL_LINKS", "ARROW_STREAM", response);

            Assert.Equal(ExecutionResultFormat.ExternalLinks, result);
        }

        [Fact]
        public void Map_AutoDisposition_WithExternalLinks_ReturnsExternalLinks()
        {
            // Cell 3: INLINE_OR_EXTERNAL_LINKS + external_links populated → EXTERNAL_LINKS. The
            // mapper distinguishes auto-disposition results by inspecting the response. The
            // canonical shape places external links on a manifest chunk.
            var response = BuildExternalLinksResponseInChunks();

            var result = SeaResultFormatMapper.Map("INLINE_OR_EXTERNAL_LINKS", "ARROW_STREAM", response);

            Assert.Equal(ExecutionResultFormat.ExternalLinks, result);
        }

        [Fact]
        public void Map_AutoDisposition_WithExternalLinksOnResult_ReturnsExternalLinks()
        {
            // Cell 3 variant: hybrid mode may surface external links on the Result payload
            // rather than the manifest chunks. The mapper must catch both shapes — if it only
            // looked at manifest.chunks it would mis-classify this as inline.
            var response = BuildExternalLinksResponseInResult();

            var result = SeaResultFormatMapper.Map("INLINE_OR_EXTERNAL_LINKS", "ARROW_STREAM", response);

            Assert.Equal(ExecutionResultFormat.ExternalLinks, result);
        }

        [Fact]
        public void Map_AutoDisposition_WithInlineResult_ReturnsInlineArrow()
        {
            // Cell 4: INLINE_OR_EXTERNAL_LINKS + inline attachment → INLINE_ARROW. The
            // response has a manifest (server produced a shape) but no external_links anywhere
            // — this is the row of the §8 table that maps to InlineArrow.
            var response = BuildInlineResponse();

            var result = SeaResultFormatMapper.Map("INLINE_OR_EXTERNAL_LINKS", "ARROW_STREAM", response);

            Assert.Equal(ExecutionResultFormat.InlineArrow, result);
        }

        // ── Defensive edge cases ──────────────────────────────────────────────────

        [Fact]
        public void Map_NonArrowStreamFormat_ReturnsUnspecified()
        {
            // The §8 table only covers ARROW_STREAM. Other server-side formats (JSON_ARRAY,
            // CSV) have no corresponding proto enum value, so the mapper falls back to
            // Unspecified rather than guessing. This keeps telemetry honest for any future
            // format the server adds without a paired proto change.
            var response = BuildInlineResponse();

            var result = SeaResultFormatMapper.Map("INLINE", "JSON_ARRAY", response);

            Assert.Equal(ExecutionResultFormat.Unspecified, result);
        }

        [Fact]
        public void Map_UnknownDisposition_ReturnsUnspecified()
        {
            // Defensive: if a future server adds a new disposition string we do not recognise,
            // emit Unspecified rather than picking a fallback that may silently mis-label the
            // record. Telemetry consumers can spot Unspecified easily; they cannot spot
            // an InlineArrow that should have been ExternalLinks.
            var response = BuildInlineResponse();

            var result = SeaResultFormatMapper.Map("WHATEVER_DISPOSITION", "ARROW_STREAM", response);

            Assert.Equal(ExecutionResultFormat.Unspecified, result);
        }

        [Fact]
        public void Map_AutoDisposition_WithNullResponse_ReturnsUnspecified()
        {
            // Auto-disposition needs the response shape to disambiguate. With no response at
            // all, the mapper cannot tell inline from external — Unspecified is the only
            // honest answer. The INLINE / EXTERNAL_LINKS dispositions still work without a
            // response because their answer is determined by the request alone.
            var result = SeaResultFormatMapper.Map("INLINE_OR_EXTERNAL_LINKS", "ARROW_STREAM", response: null);

            Assert.Equal(ExecutionResultFormat.Unspecified, result);
        }

        [Fact]
        public void Map_AutoDisposition_NoManifestNoResult_ReturnsUnspecified()
        {
            // Phase-5 fires OnExecuteSucceeded after the initial ExecuteStatementAsync call,
            // before polling. For PENDING responses there is no manifest or result yet — we
            // explicitly do not want to guess "inline" by default here, since the eventual
            // result may be external. Unspecified preserves accuracy in the PENDING case;
            // accurate telemetry can be backfilled when the polling loop terminates if a
            // future change moves the callsite.
            var pendingResponse = new ExecuteStatementResponse
            {
                StatementId = "stmt-pending",
                Status = new StatementStatus { State = "PENDING" },
            };

            var result = SeaResultFormatMapper.Map("INLINE_OR_EXTERNAL_LINKS", "ARROW_STREAM", pendingResponse);

            Assert.Equal(ExecutionResultFormat.Unspecified, result);
        }

        [Fact]
        public void Map_DispositionAndFormatAreCaseInsensitive()
        {
            // Server clients and configuration sources may produce either-case strings. The
            // mapper normalises via OrdinalIgnoreCase comparison so callers do not need to
            // pre-uppercase. Combined with the four-cell tests above this gives total coverage
            // of the public surface.
            var response = BuildInlineResponse();

            Assert.Equal(
                ExecutionResultFormat.InlineArrow,
                SeaResultFormatMapper.Map("inline", "arrow_stream", response));
            Assert.Equal(
                ExecutionResultFormat.ExternalLinks,
                SeaResultFormatMapper.Map("External_Links", "Arrow_Stream", BuildExternalLinksResponseInChunks()));
        }
    }
}
