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
        /// <param name="inner">The inner stream containing compressed LZ4 data.</param>
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

        // ES-2356918: a result file can hold many concatenated LZ4 frames (one per Arrow batch), but the
        // K4os reader never reads across frames. A read that starts on a frame's end mark (the previous
        // read filled the buffer exactly as the frame ended, or the frame is empty) returns 0 even though
        // more frames follow, and CopyTo treats 0 as end-of-stream. Only the end of the compressed input
        // is end-of-stream, so keep reading while input remains.
        private long InputPosition => _inner.CanSeek ? _inner.Position : -1;

        private bool MoreFramesFollow(bool gotData, long positionBefore)
        {
            if (gotData || !_inner.CanSeek || _inner.Position >= _inner.Length) return false;
            if (_inner.Position == positionBefore)
                throw new InvalidDataException("LZ4 decoder made no progress before the end of the input");
            return true;
        }

        public override int ReadByte()
        {
            int value;
            long before;
            do { before = InputPosition; value = _reader.ReadOneByte(); }
            while (MoreFramesFollow(value >= 0, before));
            return value;
        }

        public override int Read(byte[] buffer, int offset, int count) =>
            ReadCore(buffer.AsSpan(offset, count));

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            ReadCoreAsync(buffer.AsMemory(offset, count), cancellationToken);

#if NETSTANDARD2_1_OR_GREATER || NET5_0_OR_GREATER
        public override int Read(Span<byte> buffer) => ReadCore(buffer);

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            new(ReadCoreAsync(buffer, cancellationToken));
#endif

        private int ReadCore(Span<byte> buffer)
        {
            int read;
            long before;
            do { before = InputPosition; read = _reader.ReadManyBytes(buffer, _interactive); }
            while (!buffer.IsEmpty && MoreFramesFollow(read > 0, before));
            return read;
        }

        private async Task<int> ReadCoreAsync(Memory<byte> buffer, CancellationToken cancellationToken)
        {
            int read;
            long before;
            do
            {
                before = InputPosition;
                read = await _reader.ReadManyBytesAsync(cancellationToken, buffer, _interactive).ConfigureAwait(false);
            }
            while (!buffer.IsEmpty && MoreFramesFollow(read > 0, before));
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
