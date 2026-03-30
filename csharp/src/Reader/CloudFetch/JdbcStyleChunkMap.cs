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
using System.Net.Http;
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
        private readonly HttpClient _httpClient;
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

        // Per-stage timing accumulators (thread-safe via Interlocked)
        private long _totalDownloadMs;
        private long _totalDecompressMs;
        private long _totalArrowParseMs;
        private long _totalMemoryWaitMs;
        private long _chunksProcessed;
        private long _totalCompressedBytes;
        private long _totalDecompressedBytes;
        private long _totalBatches;
        private readonly System.Diagnostics.Stopwatch _overallStopwatch = new System.Diagnostics.Stopwatch();

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

            // Use WinHttpHandler to bypass .NET Framework's ServicePoint/SslStream serialization.
            // The managed HTTP stack (HttpWebRequest → ServicePoint → Connection → SslStream)
            // serializes HTTPS requests through a per-ServicePoint lock in SslStream/Schannel,
            // achieving only 0.55 effective parallelism despite 8 threads and 8 connections.
            // WinHttpHandler calls winhttp.dll directly — its own connection pool + TLS stack
            // with true parallel HTTPS support.
            try
            {
#pragma warning disable CA1416 // WinHttpHandler is Windows-only; fallback in catch for non-Windows
                var winHttpHandler = new System.Net.Http.WinHttpHandler
                {
                    MaxConnectionsPerServer = 128,
                    SendTimeout = TimeSpan.FromMinutes(config.TimeoutMinutes),
                    ReceiveHeadersTimeout = TimeSpan.FromMinutes(config.TimeoutMinutes),
                    ReceiveDataTimeout = TimeSpan.FromMinutes(config.TimeoutMinutes),
                    WindowsProxyUsePolicy = System.Net.Http.WindowsProxyUsePolicy.DoNotUseProxy,
                    CookieUsePolicy = System.Net.Http.CookieUsePolicy.IgnoreCookies,
                };
                _httpClient = new HttpClient(winHttpHandler) { Timeout = TimeSpan.FromMinutes(config.TimeoutMinutes) };
#pragma warning restore CA1416
                Console.WriteLine("[CloudFetch] Using WinHttpHandler for parallel HTTPS downloads");
            }
            catch
            {
                // Fallback: WinHttpHandler not available (non-Windows)
                _httpClient = new HttpClient() { Timeout = TimeSpan.FromMinutes(config.TimeoutMinutes) };
                Console.WriteLine("[CloudFetch] WinHttpHandler not available, using default HttpClient");
            }
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

            // Set DefaultConnectionLimit EARLY — before any ServicePoint is created.
            // On .NET Framework, ServicePoints snapshot this value at creation time.
            // If ANY HTTP request happened before this (Thrift, auth), it's too late for those hosts,
            // but CloudFetch downloads go to DIFFERENT hosts (cloud storage), so setting it here
            // ensures the cloud storage ServicePoints get the right limit.
            System.Net.ServicePointManager.DefaultConnectionLimit = Math.Max(128, parallelism);
            // Also disable default proxy — WPAD lookup is synchronous and serializes first requests.
            try { System.Net.WebRequest.DefaultWebProxy = null; } catch { }

            Console.WriteLine($"[CloudFetch] DefaultConnectionLimit={System.Net.ServicePointManager.DefaultConnectionLimit}, Workers={parallelism}");

            _overallStopwatch.Start();
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

                    // ---- TIMING: Memory wait ----
                    var memWaitSw = System.Diagnostics.Stopwatch.StartNew();
                    while (!_memoryManager.TryAcquireMemory(item.Size))
                    {
                        if (ct.IsCancellationRequested) return;
                        Thread.Sleep(5);
                    }
                    memWaitSw.Stop();
                    Interlocked.Add(ref _totalMemoryWaitMs, memWaitSw.ElapsedMilliseconds);

                    try
                    {
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

                        // ---- TIMING: HTTP download ----
                        var dlSw = System.Diagnostics.Stopwatch.StartNew();
                        byte[] fileData = DownloadBytes(url, item.HttpHeaders);
                        dlSw.Stop();
                        Interlocked.Add(ref _totalDownloadMs, dlSw.ElapsedMilliseconds);
                        Interlocked.Add(ref _totalCompressedBytes, fileData.Length);

                        // ---- TIMING: LZ4 decompression ----
                        var decompSw = System.Diagnostics.Stopwatch.StartNew();
                        Stream arrowStream;
                        if (_isLz4Compressed)
                        {
                            var bufPool = _lz4BufferPool ?? ArrayPool<byte>.Shared;
                            ReadOnlyMemory<byte> decompressed = Lz4Utilities.DecompressLz4(fileData, bufPool);
                            Interlocked.Add(ref _totalDecompressedBytes, decompressed.Length);
                            if (System.Runtime.InteropServices.MemoryMarshal.TryGetArray(decompressed, out ArraySegment<byte> seg))
                                arrowStream = new MemoryStream(seg.Array!, seg.Offset, seg.Count, false);
                            else
                                arrowStream = new MemoryStream(decompressed.ToArray());
                        }
                        else
                        {
                            arrowStream = new MemoryStream(fileData);
                            Interlocked.Add(ref _totalDecompressedBytes, fileData.Length);
                        }
                        decompSw.Stop();
                        Interlocked.Add(ref _totalDecompressMs, decompSw.ElapsedMilliseconds);

                        // ---- TIMING: Arrow IPC parse ----
                        var parseSw = System.Diagnostics.Stopwatch.StartNew();
                        var batches = new List<RecordBatch>();
                        using (var reader = new ArrowStreamReader(arrowStream, NativeMemoryAllocator, leaveOpen: false))
                        {
                            RecordBatch? batch;
                            while ((batch = reader.ReadNextRecordBatch()) != null)
                                batches.Add(batch);
                        }
                        parseSw.Stop();
                        Interlocked.Add(ref _totalArrowParseMs, parseSw.ElapsedMilliseconds);
                        Interlocked.Add(ref _totalBatches, batches.Count);
                        Interlocked.Increment(ref _chunksProcessed);

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
        /// <summary>
        /// Downloads bytes using HttpClient backed by WinHttpHandler.
        /// WinHttpHandler bypasses .NET Framework's ServicePoint/SslStream stack which
        /// serializes HTTPS requests to ~0.55 effective parallelism despite 8 threads.
        /// WinHTTP (winhttp.dll) has its own connection pool + TLS stack with true parallelism.
        /// </summary>
        private byte[] DownloadBytes(string url, IReadOnlyDictionary<string, string>? headers)
        {
            using (var request = new HttpRequestMessage(HttpMethod.Get, url))
            {
                if (headers != null)
                    foreach (var h in headers)
                        request.Headers.TryAddWithoutValidation(h.Key, h.Value);

                // SendAsync + .Result is safe here — we're on a dedicated thread, not ThreadPool.
                using (var response = _httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead).Result)
                {
                    response.EnsureSuccessStatusCode();
                    return response.Content.ReadAsByteArrayAsync().Result;
                }
            }
        }

        internal void PrintTimingReport()
        {
            _overallStopwatch.Stop();
            long chunks = Interlocked.Read(ref _chunksProcessed);
            long batches = Interlocked.Read(ref _totalBatches);
            long dlMs = Interlocked.Read(ref _totalDownloadMs);
            long decompMs = Interlocked.Read(ref _totalDecompressMs);
            long parseMs = Interlocked.Read(ref _totalArrowParseMs);
            long memWaitMs = Interlocked.Read(ref _totalMemoryWaitMs);
            long compBytes = Interlocked.Read(ref _totalCompressedBytes);
            long decompBytes = Interlocked.Read(ref _totalDecompressedBytes);
            long wallMs = _overallStopwatch.ElapsedMilliseconds;

            Console.WriteLine();
            Console.WriteLine("=== CloudFetch Pipeline Timing Report ===");
            Console.WriteLine($"  Wall clock:           {wallMs:N0} ms");
            Console.WriteLine($"  Chunks processed:     {chunks:N0}");
            Console.WriteLine($"  Batches parsed:       {batches:N0}");
            Console.WriteLine($"  Worker threads:       {_workers?.Length ?? 0}");
            Console.WriteLine($"  Compressed bytes:     {compBytes / 1024.0 / 1024.0:F1} MB");
            Console.WriteLine($"  Decompressed bytes:   {decompBytes / 1024.0 / 1024.0:F1} MB");
            Console.WriteLine($"  Compression ratio:    {(compBytes > 0 ? (double)decompBytes / compBytes : 0):F2}x");
            Console.WriteLine();
            Console.WriteLine("  Per-stage cumulative time (sum across all worker threads):");
            Console.WriteLine($"    HTTP download:      {dlMs:N0} ms ({(chunks > 0 ? dlMs / chunks : 0):N0} ms/chunk avg)");
            Console.WriteLine($"    LZ4 decompress:     {decompMs:N0} ms ({(chunks > 0 ? decompMs / chunks : 0):N0} ms/chunk avg)");
            Console.WriteLine($"    Arrow IPC parse:    {parseMs:N0} ms ({(chunks > 0 ? parseMs / chunks : 0):N0} ms/chunk avg)");
            Console.WriteLine($"    Memory wait:        {memWaitMs:N0} ms ({(chunks > 0 ? memWaitMs / chunks : 0):N0} ms/chunk avg)");
            Console.WriteLine();
            long totalPipelineMs = dlMs + decompMs + parseMs + memWaitMs;
            if (totalPipelineMs > 0)
            {
                Console.WriteLine($"  Time distribution (% of pipeline):");
                Console.WriteLine($"    HTTP download:      {100.0 * dlMs / totalPipelineMs:F1}%");
                Console.WriteLine($"    LZ4 decompress:     {100.0 * decompMs / totalPipelineMs:F1}%");
                Console.WriteLine($"    Arrow IPC parse:    {100.0 * parseMs / totalPipelineMs:F1}%");
                Console.WriteLine($"    Memory wait:        {100.0 * memWaitMs / totalPipelineMs:F1}%");
            }
            Console.WriteLine("==========================================");
            Console.WriteLine();
        }

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
