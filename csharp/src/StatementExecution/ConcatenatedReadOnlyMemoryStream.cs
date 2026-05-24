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
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.StatementExecution
{
    /// <summary>
    /// A forward-only, read-only <see cref="Stream"/> over an ordered list of
    /// <see cref="ReadOnlyMemory{Byte}"/> segments, presenting them as one continuous stream.
    ///
    /// <para>
    /// Used by the SEA inline result path to feed multi-chunk Arrow IPC data to
    /// <c>ArrowStreamReader</c> without first concatenating every chunk into a single large
    /// buffer. The SEA server splits one Arrow IPC stream across N attachment chunks, so reading
    /// the segments back-to-back is byte-equivalent to the concatenated buffer — but avoids the
    /// extra whole-result allocation and copy (and lets each decompressed chunk stay as the
    /// <see cref="ReadOnlyMemory{Byte}"/> returned by decompression rather than a trimmed copy).
    /// </para>
    ///
    /// <para>Not seekable: the Arrow IPC <em>stream</em> format is read strictly forward, which is
    /// all <c>ArrowStreamReader</c> requires.</para>
    /// </summary>
    internal sealed class ConcatenatedReadOnlyMemoryStream : Stream
    {
        private readonly IReadOnlyList<ReadOnlyMemory<byte>> _segments;
        private readonly long _length;
        private int _segmentIndex;
        private int _segmentOffset;
        private long _position;

        public ConcatenatedReadOnlyMemoryStream(IReadOnlyList<ReadOnlyMemory<byte>> segments)
        {
            _segments = segments ?? throw new ArgumentNullException(nameof(segments));
            long total = 0;
            for (int i = 0; i < _segments.Count; i++)
            {
                total += _segments[i].Length;
            }
            _length = total;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => _length;

        public override long Position
        {
            get => _position;
            set => throw new NotSupportedException("ConcatenatedReadOnlyMemoryStream is forward-only.");
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (buffer == null) throw new ArgumentNullException(nameof(buffer));
            if (offset < 0 || count < 0 || offset + count > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(count));
            return ReadCore(buffer.AsSpan(offset, count));
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
                return Task.FromCanceled<int>(cancellationToken);
            return Task.FromResult(Read(buffer, offset, count));
        }

#if NETSTANDARD2_1_OR_GREATER || NET5_0_OR_GREATER
        public override int Read(Span<byte> buffer) => ReadCore(buffer);

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
                return new ValueTask<int>(Task.FromCanceled<int>(cancellationToken));
            return new ValueTask<int>(ReadCore(buffer.Span));
        }
#endif

        private int ReadCore(Span<byte> destination)
        {
            int copied = 0;
            while (copied < destination.Length && _segmentIndex < _segments.Count)
            {
                ReadOnlySpan<byte> segment = _segments[_segmentIndex].Span;
                int available = segment.Length - _segmentOffset;
                if (available <= 0)
                {
                    _segmentIndex++;
                    _segmentOffset = 0;
                    continue;
                }

                int toCopy = Math.Min(available, destination.Length - copied);
                segment.Slice(_segmentOffset, toCopy).CopyTo(destination.Slice(copied, toCopy));
                _segmentOffset += toCopy;
                copied += toCopy;
            }

            _position += copied;
            return copied;
        }

        public override void Flush()
        {
            // No-op: read-only stream.
        }

        public override long Seek(long offset, SeekOrigin origin) =>
            throw new NotSupportedException("ConcatenatedReadOnlyMemoryStream is forward-only.");

        public override void SetLength(long value) =>
            throw new NotSupportedException("ConcatenatedReadOnlyMemoryStream is read-only.");

        public override void Write(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException("ConcatenatedReadOnlyMemoryStream is read-only.");
    }
}
