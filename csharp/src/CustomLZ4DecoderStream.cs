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
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using K4os.Compression.LZ4.Encoders;
using K4os.Compression.LZ4.Streams;

namespace AdbcDrivers.Databricks
{
    /// <summary>
    /// Custom LZ4 decoder stream that uses CustomLZ4FrameReader for buffer pooling.
    /// This replaces K4os.Compression.LZ4.Streams.LZ4DecoderStream to use our custom reader
    /// that pools 4MB+ buffers.
    ///
    /// Why not inherit from LZ4DecoderStream or LZ4StreamOnStreamEssentials?
    /// - LZ4DecoderStream directly instantiates StreamLZ4FrameReader (no injection point)
    /// - LZ4StreamOnStreamEssentials has a 'private protected' constructor (inaccessible from external assemblies)
    ///
    /// What features from K4os base classes are intentionally omitted:
    /// - Timeout support: Not needed since inner stream (MemoryStream) doesn't support timeouts
    /// - Write operations: This is a read-only decompression stream
    /// - DisposeAsync: Optional - base Stream.DisposeAsync() calls our Dispose(bool) which is sufficient
    /// </summary>
    internal sealed class CustomLZ4DecoderStream : Stream
    {
        private readonly CustomLZ4FrameReader _reader;
        private readonly Stream _inner;
        private readonly bool _leaveOpen;
        private readonly bool _interactive;
        private bool _disposed;

        /// <summary>
        /// Creates a new CustomLZ4DecoderStream instance.
        /// </summary>
        /// <param name="inner">The inner stream containing compressed LZ4 data. Must be seekable so the
        /// stream can tell a frame boundary from the end of the input (all callers pass a MemoryStream).</param>
        /// <param name="decoderFactory">Factory function to create the LZ4 decoder.</param>
        /// <param name="bufferPool">The ArrayPool to use for buffer allocation (from DatabricksDatabase).</param>
        /// <param name="leaveOpen">Whether to leave the inner stream open when disposing.</param>
        /// <param name="interactive">Interactive mode - provide bytes as soon as available.</param>
        public CustomLZ4DecoderStream(
            Stream inner,
            Func<ILZ4Descriptor, ILZ4Decoder> decoderFactory,
            System.Buffers.ArrayPool<byte> bufferPool,
            bool leaveOpen = false,
            bool interactive = false)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            if (!inner.CanSeek)
                throw new ArgumentException("Inner stream must be seekable.", nameof(inner));
            _reader = new CustomLZ4FrameReader(inner, true, decoderFactory, bufferPool);
            _leaveOpen = leaveOpen;
            _interactive = interactive;
        }

        public override bool CanRead => !_disposed && _inner.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => false;

        // Timeout properties are not implemented since:
        // - The inner stream (MemoryStream in our use case) doesn't support timeouts
        // - LZ4 decompression is CPU-bound, not I/O-bound, so timeouts don't apply
        public override bool CanTimeout => false;
        public override int ReadTimeout
        {
            get => throw new InvalidOperationException("LZ4 decoder stream does not support timeouts");
            set => throw new InvalidOperationException("LZ4 decoder stream does not support timeouts");
        }
        public override int WriteTimeout
        {
            get => throw new InvalidOperationException("LZ4 decoder stream does not support timeouts");
            set => throw new InvalidOperationException("LZ4 decoder stream does not support timeouts");
        }

        public override long Length => _reader.GetFrameLength() ?? -1;
        public override long Position
        {
            get => _reader.GetBytesRead();
            set => throw new NotSupportedException("LZ4 stream does not support setting position");
        }

        public override long Seek(long offset, SeekOrigin origin) =>
            throw new NotSupportedException("LZ4 stream does not support seeking");

        public override void SetLength(long value) =>
            throw new NotSupportedException("LZ4 stream does not support SetLength");

        public override void Write(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException("LZ4 decoder stream does not support writing");

        // A Databricks result file may contain many concatenated LZ4 frames (one per Arrow batch).
        // The K4os frame reader never reads across frames, and a read that starts on a frame's unread
        // end mark returns 0 even when more frames follow. That happens whenever the previous read
        // filled the caller's buffer exactly as a frame's data ended (a frame whose decoded size is a
        // multiple of the read size, e.g. an 81,920-byte Arrow batch with .NET Framework's
        // CopyToAsync), and for empty frames. Stream consumers (CopyTo, Arrow readers) treat 0 as
        // end-of-stream, so without this loop every later frame was silently dropped (ES-2356918).
        // Only the end of the compressed input means end-of-stream: keep reading while input bytes
        // remain. Each retry consumes at least one frame header/end mark, so the loop always makes
        // progress. (Same rule as the Rust kernel's MultiFrameLz4Reader and Python's _decompress_data.)
        //
        // Progress guard: with K4os 1.3.8 a 0-byte read at a non-EOF position always consumes a
        // frame header/end mark (or throws). If a future reader ever returned 0 without advancing the
        // inner stream while input remains, we throw instead of looping forever or returning 0 —
        // returning 0 there would silently truncate the result, the very bug this loop fixes. (The
        // Rust kernel's MultiFrameLz4Reader raises "LZ4 decoder stalled" in the same situation.)
        private bool HasMoreInput => _inner.Position < _inner.Length;

        /// <summary>
        /// Returns true if the read loop should retry after a read that produced no data; throws if
        /// that read made no progress while compressed input remains.
        /// </summary>
        private bool ShouldRetryEmptyRead(long positionBeforeRead)
        {
            if (!HasMoreInput) return false;
            if (_inner.Position == positionBeforeRead)
            {
                throw new InvalidDataException(
                    $"LZ4 decoder made no progress with {_inner.Length - _inner.Position} compressed bytes remaining");
            }
            return true;
        }

        public override int ReadByte()
        {
            int value;
            long previousPosition;
            do
            {
                previousPosition = _inner.Position;
                value = _reader.ReadOneByte();
            } while (value < 0 && ShouldRetryEmptyRead(previousPosition));
            return value;
        }

        public override int Read(byte[] buffer, int offset, int count) =>
            Read(buffer.AsSpan(offset, count));

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            ReadManyAsync(buffer.AsMemory(offset, count), cancellationToken);

#if NETSTANDARD2_1_OR_GREATER || NET5_0_OR_GREATER
        public override int Read(Span<byte> buffer)
#else
        private int Read(Span<byte> buffer)
#endif
        {
            if (buffer.IsEmpty) return 0;
            int read;
            long previousPosition;
            do
            {
                previousPosition = _inner.Position;
                read = _reader.ReadManyBytes(buffer, _interactive);
            } while (read == 0 && ShouldRetryEmptyRead(previousPosition));
            return read;
        }

#if NETSTANDARD2_1_OR_GREATER || NET5_0_OR_GREATER
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            new(ReadManyAsync(buffer, cancellationToken));
#endif

        private async Task<int> ReadManyAsync(Memory<byte> buffer, CancellationToken cancellationToken)
        {
            if (buffer.IsEmpty) return 0;
            int read;
            long previousPosition;
            do
            {
                cancellationToken.ThrowIfCancellationRequested();
                previousPosition = _inner.Position;
                read = await _reader.ReadManyBytesAsync(cancellationToken, buffer, _interactive).ConfigureAwait(false);
            } while (read == 0 && ShouldRetryEmptyRead(previousPosition));
            return read;
        }

        public override void Flush()
        {
            // No-op for read-only stream - nothing to flush since we only read
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            // No-op for read-only stream - nothing to flush since we only read
            return Task.CompletedTask;
        }

        protected override void Dispose(bool disposing)
        {
            // Double-dispose protection: only dispose once
            if (!_disposed)
            {
                if (disposing)
                {
                    // Dispose managed resources
                    _reader.Dispose();  // Returns 4MB buffer to pool
                    if (!_leaveOpen)
                    {
                        _inner?.Dispose();  // Dispose inner stream if we own it
                    }
                }
                // No unmanaged resources to clean up (no finalizer needed)
                _disposed = true;
            }
            base.Dispose(disposing);
        }
    }
}
