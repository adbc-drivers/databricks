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
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace AdbcDrivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// JDBC/Rust-style parallel chunk downloader using indexed map storage and dedicated threads.
    ///
    /// Architecture (matches JDBC's RemoteChunkProvider + ChunkDownloadTask):
    /// - Dedicated Thread[] workers pull links from downloadQueue independently
    /// - Each worker: HTTP download → LZ4 decompress → Arrow IPC parse (all synchronous)
    /// - Results stored by chunk index in ConcurrentDictionary (not a sequential queue)
    /// - Consumer retrieves chunks in order via WaitForChunk(index)
    /// - No head-of-line blocking: chunk 5 completing before chunk 3 doesn't block anything
    ///
    /// Uses NativeMemoryAllocator for Arrow buffers to bypass GC (like JDBC's DirectByteBuffer).
    /// </summary>
    internal sealed class JdbcStyleChunkMap : IDisposable
    {
        private static readonly Apache.Arrow.Memory.NativeMemoryAllocator NativeMemoryAllocator
            = new Apache.Arrow.Memory.NativeMemoryAllocator();

        private readonly ConcurrentDictionary<long, ChunkMapEntry> _chunks = new ConcurrentDictionary<long, ChunkMapEntry>();
        private readonly HttpClient _httpClient;
        private readonly BlockingCollection<IDownloadResult> _downloadQueue;
        private readonly ICloudFetchMemoryBufferManager _memoryManager;
        private readonly ICloudFetchResultFetcher _resultFetcher;
        private readonly int _maxChunksInMemory;
        private readonly bool _isLz4Compressed;
        private readonly int _urlExpirationBufferSeconds;
        private readonly ArrayPool<byte>? _lz4BufferPool;

        private volatile bool _endOfStream;
        private volatile bool _hasError;
        private Exception? _error;
        private Thread[]? _workers;
        private CancellationTokenSource? _cts;
        private bool _disposed;

        // Per-stage timing accumulators
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
            HttpClient fallbackHttpClient,
            CloudFetchConfiguration config)
        {
            _downloadQueue = downloadQueue;
            _memoryManager = memoryManager;
            _resultFetcher = resultFetcher;
            _maxChunksInMemory = config.PrefetchCount;
            _isLz4Compressed = config.IsLz4Compressed;
            _urlExpirationBufferSeconds = config.UrlExpirationBufferSeconds;
            _lz4BufferPool = config.Lz4BufferPool;

            // Use WinHttpHandler for faster per-connection HTTPS throughput.
            // On .NET Framework, the managed HTTP stack (HttpWebRequest/HttpClientHandler) has
            // higher per-byte overhead than WinHTTP (winhttp.dll). Measured: 306ms/chunk with
            // WinHttpHandler vs 555ms/chunk with default HttpClient on the same connection.
            try
            {
#pragma warning disable CA1416 // WinHttpHandler is Windows-only; fallback for non-Windows
                var handler = new System.Net.Http.WinHttpHandler
                {
                    MaxConnectionsPerServer = 128,
                    SendTimeout = TimeSpan.FromMinutes(config.TimeoutMinutes),
                    ReceiveHeadersTimeout = TimeSpan.FromMinutes(config.TimeoutMinutes),
                    ReceiveDataTimeout = TimeSpan.FromMinutes(config.TimeoutMinutes),
                    WindowsProxyUsePolicy = System.Net.Http.WindowsProxyUsePolicy.DoNotUseProxy,
                    CookieUsePolicy = System.Net.Http.CookieUsePolicy.IgnoreCookies,
                };
#pragma warning restore CA1416
                _httpClient = new HttpClient(handler) { Timeout = TimeSpan.FromMinutes(config.TimeoutMinutes) };
            }
            catch
            {
                _httpClient = fallbackHttpClient;
            }
        }

        internal bool HasError => _hasError;
        internal Exception? Error => _error;

        internal void Start(int parallelism, CancellationToken cancellationToken)
        {
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

        internal List<RecordBatch>? WaitForChunk(long chunkIndex, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (_chunks.TryGetValue(chunkIndex, out var entry))
                {
                    if (!entry.ReadyEvent.Wait(TimeSpan.FromMinutes(15), cancellationToken))
                        throw new TimeoutException($"Chunk {chunkIndex} download timed out after 15 minutes");

                    if (entry.Error != null)
                        throw entry.Error;

                    _chunks.TryRemove(chunkIndex, out _);

                    if (entry.CompressedSize > 0 && !entry.CompressedSizeReleased)
                        _memoryManager.ReleaseMemory(entry.CompressedSize);

                    return entry.Batches;
                }

                if (_endOfStream && !_chunks.ContainsKey(chunkIndex))
                    return null;

                if (_hasError)
                    throw _error ?? new Exception("Download error");

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
                        try { _downloadQueue.CompleteAdding(); } catch (InvalidOperationException) { }
                        break;
                    }

                    long chunkIndex = item.ChunkIndex;
                    var entry = new ChunkMapEntry(item.Size);
                    _chunks[chunkIndex] = entry;

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

                        // HTTP download via shared HttpClient
                        var dlSw = System.Diagnostics.Stopwatch.StartNew();
                        byte[] fileData;
                        using (var request = new HttpRequestMessage(HttpMethod.Get, url))
                        {
                            if (item.HttpHeaders != null)
                                foreach (var h in item.HttpHeaders)
                                    request.Headers.TryAddWithoutValidation(h.Key, h.Value);

                            using (var response = _httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead).Result)
                            {
                                response.EnsureSuccessStatusCode();
                                fileData = response.Content.ReadAsByteArrayAsync().Result;
                            }
                        }

                        dlSw.Stop();
                        Interlocked.Add(ref _totalDownloadMs, dlSw.ElapsedMilliseconds);
                        Interlocked.Add(ref _totalCompressedBytes, fileData.Length);

                        // LZ4 decompression (synchronous)
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
                        }

                        decompSw.Stop();
                        Interlocked.Add(ref _totalDecompressMs, decompSw.ElapsedMilliseconds);

                        // Arrow IPC parse with NativeMemoryAllocator (off-heap, like JDBC's DirectByteBuffer)
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

                        // Release compressed memory immediately after parsing
                        if (item.Size > 0)
                            _memoryManager.ReleaseMemory(item.Size);
                        entry.CompressedSizeReleased = true;

                        entry.Batches = batches;
                        entry.ReadyEvent.Set();
                    }
                    catch (Exception ex)
                    {
                        if (item.Size > 0 && !entry.CompressedSizeReleased)
                            _memoryManager.ReleaseMemory(item.Size);
                        entry.CompressedSizeReleased = true;
                        entry.Error = ex;
                        entry.ReadyEvent.Set();
                        _hasError = true;
                        _error = ex;
                        break;
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (InvalidOperationException) when (_downloadQueue.IsCompleted) { }
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
            Console.WriteLine($"  Per-stage cumulative time (sum across all worker threads):");
            Console.WriteLine($"    HTTP download:      {dlMs:N0} ms ({(chunks > 0 ? dlMs / chunks : 0):N0} ms/chunk avg)");
            Console.WriteLine($"    LZ4 decompress:     {decompMs:N0} ms ({(chunks > 0 ? decompMs / chunks : 0):N0} ms/chunk avg)");
            Console.WriteLine($"    Arrow IPC parse:    {parseMs:N0} ms ({(chunks > 0 ? parseMs / chunks : 0):N0} ms/chunk avg)");
            Console.WriteLine($"    Memory wait:        {memWaitMs:N0} ms ({(chunks > 0 ? memWaitMs / chunks : 0):N0} ms/chunk avg)");
            long total = dlMs + decompMs + parseMs + memWaitMs;
            if (total > 0)
            {
                Console.WriteLine($"  Time distribution (% of pipeline):");
                Console.WriteLine($"    HTTP download:      {100.0 * dlMs / total:F1}%");
                Console.WriteLine($"    LZ4 decompress:     {100.0 * decompMs / total:F1}%");
                Console.WriteLine($"    Arrow IPC parse:    {100.0 * parseMs / total:F1}%");
                Console.WriteLine($"    Memory wait:        {100.0 * memWaitMs / total:F1}%");
            }
            Console.WriteLine("==========================================");
        }

        internal void Stop()
        {
            _cts?.Cancel();
            try { _downloadQueue.CompleteAdding(); } catch { }
            if (_workers != null)
                foreach (var w in _workers)
                    w.Join(TimeSpan.FromSeconds(10));
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
            internal ChunkMapEntry(long compressedSize) { CompressedSize = compressedSize; }
        }
    }
}
