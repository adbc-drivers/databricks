/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* This file has been modified from its original version, which is
* under the Apache License:
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
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
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc;
using K4os.Compression.LZ4.Streams;
using Microsoft.IO;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Utility class for LZ4 compression/decompression operations.
    /// Uses CustomLZ4DecoderStream with custom buffer pooling to handle Databricks' 4MB LZ4 frames efficiently.
    /// </summary>
    internal static class Lz4Utilities
    {
        /// <summary>
        /// Default buffer size for LZ4 decompression operations (80KB).
        /// </summary>
        private const int DefaultBufferSize = 81920;

        /// <summary>
        /// Decompresses LZ4 compressed data into memory.
        /// </summary>
        /// <param name="compressedData">The compressed data bytes.</param>
        /// <param name="memoryStreamManager">The RecyclableMemoryStreamManager used for the (pooled) decompression buffer.</param>
        /// <param name="bufferPool">The ArrayPool to use for buffer allocation (from DatabricksDatabase).</param>
        /// <returns>A ReadOnlyMemory containing the decompressed data.</returns>
        /// <exception cref="AdbcException">Thrown when decompression fails.</exception>
        public static ReadOnlyMemory<byte> DecompressLz4(byte[] compressedData, RecyclableMemoryStreamManager memoryStreamManager, ArrayPool<byte> bufferPool)
        {
            return DecompressLz4(compressedData, DefaultBufferSize, memoryStreamManager, bufferPool);
        }

        /// <summary>
        /// Decompresses LZ4 compressed data into memory with a specified buffer size.
        /// </summary>
        /// <remarks>
        /// Decompresses into a pooled <see cref="RecyclableMemoryStream"/> rather than a plain
        /// <see cref="MemoryStream"/>. A plain MemoryStream starts empty and doubles its backing array
        /// as it grows, repeatedly reallocating and copying — landing on the Large Object Heap for the
        /// multi-MB Arrow batches Databricks returns — and then leaves an over-sized buffer. The
        /// recyclable stream instead writes into reused fixed-size blocks (no per-call LOH churn).
        /// <see cref="RecyclableMemoryStream.ToArray"/> then produces a single exact-size, GC-owned
        /// array, which is safe for the caller to retain after the pooled blocks are returned — the
        /// Arrow <c>RecordBatch</c> built from it references this memory beyond this method's scope.
        /// </remarks>
        /// <param name="compressedData">The compressed data bytes.</param>
        /// <param name="bufferSize">The buffer size to use for decompression operations.</param>
        /// <param name="memoryStreamManager">The RecyclableMemoryStreamManager used for the (pooled) decompression buffer.</param>
        /// <param name="bufferPool">The ArrayPool to use for buffer allocation (from DatabricksDatabase).</param>
        /// <returns>A ReadOnlyMemory containing the decompressed data.</returns>
        /// <exception cref="AdbcException">Thrown when decompression fails.</exception>
        public static ReadOnlyMemory<byte> DecompressLz4(byte[] compressedData, int bufferSize, RecyclableMemoryStreamManager memoryStreamManager, ArrayPool<byte> bufferPool)
        {
            try
            {
                using (var outputStream = memoryStreamManager.GetStream())
                using (var inputStream = new MemoryStream(compressedData))
                using (var decompressor = new CustomLZ4DecoderStream(
                    inputStream,
                    descriptor => descriptor.CreateDecoder(),
                    bufferPool,
                    leaveOpen: false,
                    interactive: false))
                {
                    decompressor.CopyTo(outputStream, bufferSize);

                    // Copy to an exact-size, GC-owned array before the pooled stream/blocks are
                    // returned on dispose. The caller retains this beyond the stream's lifetime.
                    return outputStream.ToArray();
                }
            }
            catch (Exception ex)
            {
                throw new AdbcException($"Failed to decompress LZ4 data: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Wraps LZ4-framed compressed bytes in a forward-only stream that decompresses
        /// <b>incrementally as it is read</b>, rather than decompressing the whole payload up front.
        /// </summary>
        /// <remarks>
        /// Unlike <see cref="DecompressLz4(byte[], RecyclableMemoryStreamManager, ArrayPool{byte})"/>,
        /// which fully materializes the decompressed result before returning, this hands back a lazy
        /// decoder over the compressed bytes. When the consumer (e.g. <c>ArrowStreamReader</c>) reads
        /// only part of the result and disposes, only the batches actually read are decompressed —
        /// the whole result is never materialized. Peak/allocation is bounded to what is consumed
        /// plus the (smaller) compressed buffer. The caller MUST dispose the returned stream; disposal
        /// closes the inner compressed-bytes stream. See #573.
        /// </remarks>
        /// <param name="compressedData">The LZ4-framed compressed bytes.</param>
        /// <param name="bufferPool">The ArrayPool for the decoder's work buffers (from DatabricksDatabase).</param>
        /// <returns>A forward-only, self-decompressing <see cref="Stream"/>. Caller must dispose.</returns>
        public static Stream CreateDecompressingStream(byte[] compressedData, ArrayPool<byte> bufferPool)
        {
            return new CustomLZ4DecoderStream(
                new MemoryStream(compressedData),
                descriptor => descriptor.CreateDecoder(),
                bufferPool,
                leaveOpen: false,
                interactive: false);
        }

        /// <summary>
        /// Asynchronously decompresses LZ4 compressed data into memory.
        /// Returns a RecyclableMemoryStream that must be disposed by the caller.
        /// </summary>
        /// <param name="compressedData">The compressed data bytes.</param>
        /// <param name="memoryStreamManager">The RecyclableMemoryStreamManager to use (from DatabricksDatabase).</param>
        /// <param name="bufferPool">The ArrayPool to use for buffer allocation (from DatabricksDatabase).</param>
        /// <param name="cancellationToken">Cancellation token for the async operation.</param>
        /// <returns>A RecyclableMemoryStream containing the decompressed data. Caller must dispose.</returns>
        /// <exception cref="AdbcException">Thrown when decompression fails.</exception>
        public static Task<RecyclableMemoryStream> DecompressLz4Async(
            byte[] compressedData,
            RecyclableMemoryStreamManager memoryStreamManager,
            ArrayPool<byte> bufferPool,
            CancellationToken cancellationToken = default)
        {
            return DecompressLz4Async(compressedData, DefaultBufferSize, memoryStreamManager, bufferPool, cancellationToken);
        }

        /// <summary>
        /// Asynchronously decompresses LZ4 compressed data into memory with a specified buffer size.
        /// Returns a RecyclableMemoryStream that must be disposed by the caller to return the buffer to the pool.
        /// Uses CustomLZ4DecoderStream with custom ArrayPool to efficiently pool 4MB+ buffers.
        /// </summary>
        /// <param name="compressedData">The compressed data bytes.</param>
        /// <param name="bufferSize">The buffer size to use for decompression operations.</param>
        /// <param name="memoryStreamManager">The RecyclableMemoryStreamManager to use (from DatabricksDatabase).</param>
        /// <param name="bufferPool">The ArrayPool to use for buffer allocation (from DatabricksDatabase).</param>
        /// <param name="cancellationToken">Cancellation token for the async operation.</param>
        /// <returns>A RecyclableMemoryStream containing the decompressed data. Caller must dispose.</returns>
        /// <exception cref="AdbcException">Thrown when decompression fails.</exception>
        public static async Task<RecyclableMemoryStream> DecompressLz4Async(
            byte[] compressedData,
            int bufferSize,
            RecyclableMemoryStreamManager memoryStreamManager,
            ArrayPool<byte> bufferPool,
            CancellationToken cancellationToken = default)
        {
            try
            {
                // Use RecyclableMemoryStream for pooled memory allocation
                var outputStream = memoryStreamManager.GetStream();
                try
                {
                    using (var inputStream = new MemoryStream(compressedData))
                    using (var decompressor = new CustomLZ4DecoderStream(
                        inputStream,
                        descriptor => descriptor.CreateDecoder(),
                        bufferPool,
                        leaveOpen: false,
                        interactive: false))
                    {
                        await decompressor.CopyToAsync(outputStream, bufferSize, cancellationToken).ConfigureAwait(false);
                    }

                    // Reset position to beginning for reading
                    outputStream.Position = 0;
                    return outputStream;
                }
                catch
                {
                    // If an error occurs, dispose the stream to return it to the pool
                    outputStream?.Dispose();
                    throw;
                }
            }
            catch (Exception ex)
            {
                throw new AdbcException($"Failed to decompress LZ4 data: {ex.Message}", ex);
            }
        }
    }
}
