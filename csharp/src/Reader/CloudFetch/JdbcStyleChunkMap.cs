/*
* JDBC/Rust-style chunk map for .NET Framework CloudFetch.
*
* Replaces the queue-based downloader with an indexed map:
* - Downloads store results by chunk index (ConcurrentDictionary)
* - Consumer waits for chunk N, takes it, signals new downloads
* - No head-of-line blocking: chunk 5 completing before chunk 3 doesn't block anything
*
* This matches:
* - JDBC's chunkIndexToChunksMap (ConcurrentHashMap<Long, ArrowResultChunk>)
* - Rust's DashMap<i64, ChunkEntry>
*/

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Microsoft.IO;

namespace AdbcDrivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// JDBC/Rust-style parallel chunk downloader using indexed map storage.
    /// Each dedicated thread independently downloads, decompresses, and pre-parses Arrow.
    /// Results are stored by chunk index, not in a sequential queue.
    /// Consumer retrieves chunks in order via WaitForChunk(index).
    /// </summary>
    internal sealed class JdbcStyleChunkMap : IDisposable
    {
        // Native memory allocator for Arrow buffers — allocates off-heap via Marshal.AllocHGlobal.
        // This eliminates LOH/GC pressure from Arrow column data, matching JDBC's off-heap DirectByteBuffer.
        // CRITICAL: RecordBatch.Dispose() MUST be called to free native memory (no GC safety net).
        private static readonly Apache.Arrow.Memory.NativeMemoryAllocator NativeMemoryAllocator
            = new Apache.Arrow.Memory.NativeMemoryAllocator();

        private readonly ConcurrentDictionary<long, ChunkMapEntry> _chunks = new ConcurrentDictionary<long, ChunkMapEntry>();
        private readonly BlockingCollection<IDownloadResult> _downloadQueue;
        private readonly ICloudFetchMemoryBufferManager _memoryManager;
        private readonly ICloudFetchResultFetcher _resultFetcher;
        private readonly int _maxChunksInMemory;
        private readonly bool _isLz4Compressed;
        private readonly int _timeoutMinutes;
        private readonly int _urlExpirationBufferSeconds;
        private readonly ArrayPool<byte>? _lz4BufferPool;
        private readonly RecyclableMemoryStreamManager? _memoryStreamManager;

        private int _chunksInMemory;
        private volatile bool _endOfStream;
        private volatile bool _hasError;
        private Exception? _error;
        private Thread[]? _workers;
        private CancellationTokenSource? _cts;
        private bool _disposed;

        internal JdbcStyleChunkMap(
            BlockingCollection<IDownloadResult> downloadQueue,
            ICloudFetchMemoryBufferManager memoryManager,
            ICloudFetchResultFetcher resultFetcher,
            CloudFetchConfiguration config)
        {
            _downloadQueue = downloadQueue;
            _memoryManager = memoryManager;
            _resultFetcher = resultFetcher;
            _maxChunksInMemory = config.PrefetchCount;
            _isLz4Compressed = config.IsLz4Compressed;
            _timeoutMinutes = config.TimeoutMinutes;
            _urlExpirationBufferSeconds = config.UrlExpirationBufferSeconds;
            _lz4BufferPool = config.Lz4BufferPool;
            _memoryStreamManager = config.MemoryStreamManager;
        }

        internal bool HasError => _hasError;
        internal Exception? Error => _error;

        /// <summary>
        /// Start dedicated worker threads (JDBC's ExecutorService equivalent).
        /// </summary>
        internal void Start(int parallelism, CancellationToken cancellationToken)
        {
            // Tell GC to minimize blocking collections during data processing.
            // This reduces Gen2 pause frequency (each Gen2 pauses ALL threads on .NET Framework).
            try { System.Runtime.GCSettings.LatencyMode = System.Runtime.GCLatencyMode.SustainedLowLatency; }
            catch { /* Not critical — .NET Framework may not support all modes */ }

            _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var token = _cts.Token;
            _workers = new Thread[parallelism];
            for (int i = 0; i < parallelism; i++)
            {
                int id = i;
                _workers[i] = new Thread(() => WorkerLoop(id, token))
                {
                    IsBackground = true,
                    Name = $"CloudFetch-{id}"
                };
                _workers[i].Start();
            }
        }

        /// <summary>
        /// Consumer waits for chunk at given index. Blocks until available.
        /// Equivalent to Rust's wait_for_chunk() and JDBC's chunk.waitForChunkReady().
        /// </summary>
        internal List<RecordBatch>? WaitForChunk(long chunkIndex, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (_chunks.TryGetValue(chunkIndex, out var entry))
                {
                    // Wait up to 15 minutes for this chunk to be downloaded+parsed.
                    // Prevents infinite hang if a worker thread crashes silently.
                    if (!entry.ReadyEvent.Wait(TimeSpan.FromMinutes(15), cancellationToken))
                        throw new TimeoutException($"Chunk {chunkIndex} download timed out after 15 minutes");

                    if (entry.Error != null)
                        throw entry.Error;

                    _chunks.TryRemove(chunkIndex, out _);
                    Interlocked.Decrement(ref _chunksInMemory);

                    // Only release if worker didn't already release after parsing
                    if (entry.CompressedSize > 0 && !entry.CompressedSizeReleased)
                        _memoryManager.ReleaseMemory(entry.CompressedSize);

                    return entry.Batches;
                }

                if (_endOfStream && !_chunks.ContainsKey(chunkIndex))
                    return null;

                if (_hasError)
                    throw _error ?? new Exception("Download error");

                // Check if all workers are dead (no one will create this chunk)
                if (_workers != null && !System.Array.Exists(_workers, w => w.IsAlive) && !_chunks.ContainsKey(chunkIndex))
                {
                    _endOfStream = true;
                    return null;
                }

                Thread.Sleep(5);
            }

            cancellationToken.ThrowIfCancellationRequested();
            return null;
        }

        /// <summary>
        /// Worker thread loop — pulls download items, processes them, stores by index.
        /// Each thread runs independently (JDBC's ChunkDownloadTask pattern).
        /// </summary>
        private void WorkerLoop(int workerId, CancellationToken ct)
        {
            try
            {
                foreach (var item in _downloadQueue.GetConsumingEnumerable(ct))
                {
                    if (_hasError || ct.IsCancellationRequested) break;

                    if (item is EndOfResultsGuard)
                    {
                        _endOfStream = true;
                        // Complete the queue so other worker threads unblock from
                        // GetConsumingEnumerable() — without this they wait forever.
                        try { _downloadQueue.CompleteAdding(); } catch (InvalidOperationException) { }
                        break;
                    }

                    long chunkIndex = item.ChunkIndex;

                    // Create map entry with signal event
                    var entry = new ChunkMapEntry(item.Size);
                    _chunks[chunkIndex] = entry;
                    Interlocked.Increment(ref _chunksInMemory);

                    // Wait for memory slot
                    while (!_memoryManager.TryAcquireMemory(item.Size))
                    {
                        if (ct.IsCancellationRequested) return;
                        Thread.Sleep(5);
                    }

                    try
                    {
                        // Check URL expiration
                        string url = item.FileUrl;
                        if (item.IsExpiredOrExpiringSoon(_urlExpirationBufferSeconds))
                        {
                            var refreshed = _resultFetcher.RefreshUrlsAsync(item.StartRowOffset, ct)
                                .ConfigureAwait(false).GetAwaiter().GetResult();
                            var match = refreshed.FirstOrDefault(r => r.StartRowOffset == item.StartRowOffset);
                            if (match != null)
                            {
                                item.UpdateWithRefreshedUrl(match.FileUrl, match.ExpirationTime, match.HttpHeaders);
                                url = match.FileUrl;
                            }
                        }

                        // Synchronous HTTP download
                        byte[] fileData = DownloadBytes(url, item.HttpHeaders);

                        // Synchronous LZ4 decompression
                        Stream arrowStream;
                        if (_isLz4Compressed)
                        {
                            var bufPool = _lz4BufferPool ?? ArrayPool<byte>.Shared;
                            ReadOnlyMemory<byte> decompressed = Lz4Utilities.DecompressLz4(fileData, bufPool);
                            if (System.Runtime.InteropServices.MemoryMarshal.TryGetArray(decompressed, out ArraySegment<byte> seg))
                                arrowStream = new MemoryStream(seg.Array!, seg.Offset, seg.Count, false);
                            else
                                arrowStream = new MemoryStream(decompressed.ToArray());
                        }
                        else
                        {
                            arrowStream = new MemoryStream(fileData);
                        }

                        // Synchronous Arrow IPC parse with NATIVE memory allocator.
                        // This is the key optimization for .NET Framework: Arrow column buffers
                        // are allocated via Marshal.AllocHGlobal (off-heap, like JDBC's DirectByteBuffer)
                        // instead of managed byte[] (on-heap, tracked by GC).
                        // Eliminates LOH allocations and Gen2 GC pressure entirely for data buffers.
                        var batches = new List<RecordBatch>();
                        using (var reader = new ArrowStreamReader(arrowStream, NativeMemoryAllocator, leaveOpen: false))
                        {
                            RecordBatch? batch;
                            while ((batch = reader.ReadNextRecordBatch()) != null)
                                batches.Add(batch);
                        }

                        // Release compressed memory immediately — the compressed bytes are
                        // gone after decompression. Don't hold this reservation until the
                        // consumer reads the chunk, or we deadlock with 8 workers each
                        // holding 20MB of a 200MB budget.
                        if (item.Size > 0)
                            _memoryManager.ReleaseMemory(item.Size);
                        entry.CompressedSizeReleased = true;

                        entry.Batches = batches;
                        entry.ReadyEvent.Set(); // Signal consumer
                    }
                    catch (Exception ex)
                    {
                        if (item.Size > 0 && !entry.CompressedSizeReleased)
                            _memoryManager.ReleaseMemory(item.Size);
                        entry.CompressedSizeReleased = true;
                        entry.Error = ex;
                        entry.ReadyEvent.Set(); // Signal consumer (with error)
                        _hasError = true;
                        _error = ex;
                        break;
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (InvalidOperationException) when (_downloadQueue.IsCompleted) { }
        }

        #pragma warning disable SYSLIB0014 // WebRequest is obsolete on .NET 8+ but required for .NET Framework
        private byte[] DownloadBytes(string url, IReadOnlyDictionary<string, string>? headers)
        {
            var req = (System.Net.HttpWebRequest)System.Net.WebRequest.Create(url);
            req.Method = "GET";
            req.Timeout = (int)TimeSpan.FromMinutes(_timeoutMinutes).TotalMilliseconds;
            req.ReadWriteTimeout = req.Timeout;
            req.ServicePoint.Expect100Continue = false;
            req.ServicePoint.UseNagleAlgorithm = false;
            req.ServicePoint.ConnectionLimit = Math.Max(req.ServicePoint.ConnectionLimit, 64);

            if (headers != null)
                foreach (var h in headers)
                    req.Headers[h.Key] = h.Value;

            using (var resp = (System.Net.HttpWebResponse)req.GetResponse())
            {
                long cl = resp.ContentLength;
                int cap = (cl > 0 && cl <= 100 * 1024 * 1024) ? (int)cl : 0;
                using (var rs = resp.GetResponseStream())
                {
                    var ms = cap > 0 ? new MemoryStream(cap) : new MemoryStream();
                    rs.CopyTo(ms, 81920);
                    if (ms.TryGetBuffer(out ArraySegment<byte> buf) && buf.Count == (int)ms.Length)
                        return buf.Array!;
                    return ms.ToArray();
                }
            }
        }

        #pragma warning restore SYSLIB0014

        internal void Stop()
        {
            // Restore normal GC behavior
            try { System.Runtime.GCSettings.LatencyMode = System.Runtime.GCLatencyMode.Interactive; }
            catch { }

            _cts?.Cancel();

            // Complete the queue to unblock workers stuck in GetConsumingEnumerable
            try { _downloadQueue.CompleteAdding(); } catch { }

            // Wait for workers to exit
            if (_workers != null)
            {
                foreach (var w in _workers)
                    w.Join(TimeSpan.FromSeconds(10));
            }

            _cts?.Dispose();
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            foreach (var kvp in _chunks)
            {
                if (kvp.Value.Batches != null)
                    foreach (var b in kvp.Value.Batches)
                        b.Dispose();
                kvp.Value.ReadyEvent.Dispose();
            }
            _chunks.Clear();
        }

        internal sealed class ChunkMapEntry
        {
            internal readonly ManualResetEventSlim ReadyEvent = new ManualResetEventSlim(false);
            internal List<RecordBatch>? Batches;
            internal Exception? Error;
            internal readonly long CompressedSize;
            internal bool CompressedSizeReleased;

            internal ChunkMapEntry(long compressedSize)
            {
                CompressedSize = compressedSize;
            }
        }
    }
}
