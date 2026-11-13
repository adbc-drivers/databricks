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
        private readonly EofTrackingStream _source;
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
            _source = new EofTrackingStream(inner);
            _reader = new CustomLZ4FrameReader(_source, true, decoderFactory, bufferPool);
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

        // A Databricks result file may contain several concatenated LZ4 frames, some of them empty.
        // The K4os frame reader returns 0 from a read that starts at a frame's end mark, even when
        // more frames follow. Stream consumers (CopyTo, Arrow readers) treat 0 as end-of-stream, so
        // without this loop every frame after an empty one was silently dropped (ES-2356918).
        // Keep reading until we get data or the compressed input is exhausted. Each retry consumes
        // at least one frame header/end mark, so the loop always makes progress.

        public override int ReadByte()
        {
            int value;
            while ((value = _reader.ReadOneByte()) < 0 && !_source.ReachedEnd) { }
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
            while ((read = _reader.ReadManyBytes(buffer, _interactive)) == 0 && !_source.ReachedEnd) { }
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
            while ((read = await _reader.ReadManyBytesAsync(cancellationToken, buffer, _interactive).ConfigureAwait(false)) == 0
                && !_source.ReachedEnd)
            {
                cancellationToken.ThrowIfCancellationRequested();
            }
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

    /// <summary>
    /// Read-only pass-through stream that records when the underlying stream reports end-of-stream,
    /// so <see cref="CustomLZ4DecoderStream"/> can tell a frame boundary apart from the true end of input.
    /// </summary>
    internal sealed class EofTrackingStream : Stream
    {
        private readonly Stream _inner;

        public EofTrackingStream(Stream inner) => _inner = inner;

        /// <summary>True once a non-empty read of the underlying stream returned 0 bytes.</summary>
        public bool ReachedEnd { get; private set; }

        private int Track(int read, int requested)
        {
            if (read == 0 && requested > 0) ReachedEnd = true;
            return read;
        }

        public override int Read(byte[] buffer, int offset, int count) =>
            Track(_inner.Read(buffer, offset, count), count);

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            Track(await _inner.ReadAsync(buffer, offset, count, cancellationToken).ConfigureAwait(false), count);

#if NETSTANDARD2_1_OR_GREATER || NET5_0_OR_GREATER
        public override int Read(Span<byte> buffer) =>
            Track(_inner.Read(buffer), buffer.Length);

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            Track(await _inner.ReadAsync(buffer, cancellationToken).ConfigureAwait(false), buffer.Length);
#endif

        public override int ReadByte()
        {
            int value = _inner.ReadByte();
            if (value < 0) ReachedEnd = true;
            return value;
        }

        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
