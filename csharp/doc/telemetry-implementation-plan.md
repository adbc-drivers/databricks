<!--
Copyright (c) 2025 ADBC Drivers Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Telemetry Implementation Plan: Activity Tag to Proto Schema Mapping

This document outlines the implementation plan for the telemetry changes documented in Section 4.4 of `telemetry-design.md`.

## Overview

The implementation ensures that:
1. Activity tags are properly mapped to proto schema fields
2. MetricsAggregator uses proto types (`OssSqlDriverTelemetryLog`) instead of old model classes
3. New chunk latency metrics are collected from CloudFetchDownloader
4. All enum values are correctly mapped to proto enums

## Implementation Phases

### Phase 1: Proto Type Migration (COMPLETED)

**Status**: Done in commits c7d8ae8 and subsequent fixes

**Files Changed**:
- `csharp/src/Telemetry/MetricsAggregator.cs`
- `csharp/test/Unit/Telemetry/MetricsAggregatorTests.cs`

**Changes**:
- [x] Replace `TelemetryEvent` with `OssSqlDriverTelemetryLog`
- [x] Replace `TelemetryEvent.SqlExecutionEvent` with `OssSqlDriverTelemetryLog.SqlOperation`
- [x] Replace `TelemetryEvent.ConnectionParameters` with `OssSqlDriverTelemetryLog.DriverConnectionParams`
- [x] Update `DriverErrorInfo` to use proto fields (`ErrorName`, `StackTrace`)
- [x] Add `using AdbcDrivers.Databricks.Telemetry.Proto;` import
- [x] Update `WrapInFrontendLog` method signature
- [x] Remove unused `ExtractHttpStatusCode` method
- [x] Update tests to use proto types

---

### Phase 2: Tag Definitions Update

**Status**: COMPLETED

**Files to Change**:
- `csharp/src/Telemetry/TagDefinitions/StatementExecutionEvent.cs`

**Changes**:

#### 2.1 Add Chunk Latency Tags

```csharp
// Add after PollLatencyMs definition (around line 103)

/// <summary>
/// Latency of first chunk download in milliseconds.
/// Exported to Databricks telemetry service.
/// </summary>
[TelemetryTag("chunk.initial_latency_ms",
    ExportScope = TagExportScope.ExportDatabricks,
    Description = "Latency of first chunk download in milliseconds")]
public const string ChunkInitialLatencyMs = "chunk.initial_latency_ms";

/// <summary>
/// Latency of slowest chunk download in milliseconds.
/// Exported to Databricks telemetry service.
/// </summary>
[TelemetryTag("chunk.slowest_latency_ms",
    ExportScope = TagExportScope.ExportDatabricks,
    Description = "Latency of slowest chunk download in milliseconds")]
public const string ChunkSlowestLatencyMs = "chunk.slowest_latency_ms";
```

#### 2.2 Update GetDatabricksExportTags()

```csharp
public static HashSet<string> GetDatabricksExportTags()
{
    return new HashSet<string>
    {
        StatementId,
        SessionId,
        ResultFormat,
        ResultChunkCount,
        ResultBytesDownloaded,
        ResultCompressionEnabled,
        PollCount,
        PollLatencyMs,
        ChunkInitialLatencyMs,    // NEW
        ChunkSlowestLatencyMs     // NEW
    };
}
```

---

### Phase 3: CloudFetchDownloader Latency Tracking

**Status**: COMPLETED

**Files to Change**:
- `csharp/src/Reader/CloudFetch/CloudFetchDownloader.cs`

**Changes**:

#### 3.1 Add Tracking Fields (around line 60)

```csharp
// Add after existing fields
private long _initialChunkLatencyMs = -1;
private long _slowestChunkLatencyMs = 0;
private readonly object _latencyLock = new object();
```

#### 3.2 Track Latency After Each Download (in DownloadFileAsync, after line 652)

```csharp
// After: activity?.AddEvent("cloudfetch.download_complete", [...]);
// Add latency tracking:
lock (_latencyLock)
{
    long latencyMs = stopwatch.ElapsedMilliseconds;

    // Track initial chunk latency (first successful download)
    if (_initialChunkLatencyMs < 0)
    {
        _initialChunkLatencyMs = latencyMs;
    }

    // Track slowest chunk latency (max)
    if (latencyMs > _slowestChunkLatencyMs)
    {
        _slowestChunkLatencyMs = latencyMs;
    }
}
```

#### 3.3 Update Download Summary Event (around line 430)

```csharp
activity?.AddEvent("cloudfetch.download_summary", [
    new("total_files", totalFiles),
    new("successful_downloads", successfulDownloads),
    new("failed_downloads", failedDownloads),
    new("total_bytes", totalBytes),
    new("total_mb", totalBytes / 1024.0 / 1024.0),
    new("total_time_ms", overallStopwatch.ElapsedMilliseconds),
    new("total_time_sec", overallStopwatch.ElapsedMilliseconds / 1000.0),
    new("initial_chunk_latency_ms", _initialChunkLatencyMs),  // NEW
    new("slowest_chunk_latency_ms", _slowestChunkLatencyMs),  // NEW
]);
```

#### 3.4 Reset Latency Tracking (optional, for reuse scenarios)

If the downloader can be reused, add a reset method or reset in StartAsync:

```csharp
public async Task StartAsync(CancellationToken cancellationToken)
{
    // Reset latency tracking
    lock (_latencyLock)
    {
        _initialChunkLatencyMs = -1;
        _slowestChunkLatencyMs = 0;
    }

    // ... existing code ...
}
```

---

### Phase 4: StatementTelemetryContext Update

**Status**: COMPLETED

**Files to Change**:
- `csharp/src/Telemetry/MetricsAggregator.cs` (inner class `StatementTelemetryContext`)

**Changes**:

#### 4.1 Add Backing Fields (around line 813)

```csharp
// Add after existing backing fields
private long _initialChunkLatencyMs = -1;
private long _slowestChunkLatencyMs;
private volatile bool _hasInitialChunkLatency;
private volatile bool _hasSlowestChunkLatency;
```

#### 4.2 Add Properties (around line 834)

```csharp
// Add after existing properties
public long? InitialChunkLatencyMs => _hasInitialChunkLatency ? _initialChunkLatencyMs : (long?)null;
public long? SlowestChunkLatencyMs => _hasSlowestChunkLatency ? Interlocked.Read(ref _slowestChunkLatencyMs) : (long?)null;
```

#### 4.3 Add Methods (around line 855)

```csharp
/// <summary>
/// Sets the initial chunk latency. Only the first call has effect.
/// </summary>
public void SetInitialChunkLatency(long latencyMs)
{
    // Only set once (first chunk)
    if (!_hasInitialChunkLatency && latencyMs >= 0)
    {
        _initialChunkLatencyMs = latencyMs;
        _hasInitialChunkLatency = true;
    }
}

/// <summary>
/// Updates the slowest chunk latency if the new value is greater.
/// Thread-safe using Interlocked.
/// </summary>
public void UpdateSlowestChunkLatency(long latencyMs)
{
    if (latencyMs <= 0) return;

    // Thread-safe max update using compare-and-swap
    long current;
    do
    {
        current = Interlocked.Read(ref _slowestChunkLatencyMs);
        if (latencyMs <= current) return;
    } while (Interlocked.CompareExchange(ref _slowestChunkLatencyMs, latencyMs, current) != current);

    _hasSlowestChunkLatency = true;
}
```

---

### Phase 5: MetricsAggregator Tag Processing

**Status**: COMPLETED

**Files to Change**:
- `csharp/src/Telemetry/MetricsAggregator.cs`

**Changes**:

#### 5.1 Update AggregateActivityTags Method

Add cases for chunk latency tags in the switch statement:

```csharp
private void AggregateActivityTags(StatementTelemetryContext context, Activity activity)
{
    foreach (var tag in activity.Tags)
    {
        var tagValue = tag.Value;
        if (string.IsNullOrEmpty(tagValue)) continue;

        switch (tag.Key)
        {
            // ... existing cases ...

            // Chunk latency metrics (from CloudFetch download_summary event)
            case "chunk.initial_latency_ms":
            case "initial_chunk_latency_ms":  // Alternative name from download_summary
                if (long.TryParse(tagValue, out var initialLatency))
                {
                    context.SetInitialChunkLatency(initialLatency);
                }
                break;

            case "chunk.slowest_latency_ms":
            case "slowest_chunk_latency_ms":  // Alternative name from download_summary
                if (long.TryParse(tagValue, out var slowestLatency))
                {
                    context.UpdateSlowestChunkLatency(slowestLatency);
                }
                break;

            // ... existing cases ...
        }
    }
}
```

#### 5.2 Update CreateTelemetryEvent Method

Update ChunkDetails to include latency fields:

```csharp
private OssSqlDriverTelemetryLog CreateTelemetryEvent(StatementTelemetryContext context)
{
    var telemetryLog = new OssSqlDriverTelemetryLog
    {
        SessionId = context.SessionId ?? string.Empty,
        SqlStatementId = context.StatementId,
        OperationLatencyMs = context.TotalLatencyMs,
        SqlOperation = new SqlExecutionEvent
        {
            IsCompressed = context.CompressionEnabled ?? false,
            ChunkDetails = new ChunkDetails
            {
                TotalChunksPresent = context.ChunkCount ?? 0,
                SumChunksDownloadTimeMillis = context.BytesDownloaded ?? 0,
                InitialChunkLatencyMillis = context.InitialChunkLatencyMs ?? 0,  // NEW
                SlowestChunkLatencyMillis = context.SlowestChunkLatencyMs ?? 0   // NEW
            },
            OperationDetail = new OperationDetail
            {
                NOperationStatusCalls = context.PollCount ?? 0,
                OperationStatusLatencyMillis = context.PollLatencyMs ?? 0
            }
        }
    };

    // ... rest of method unchanged ...
}
```

---

### Phase 6: Activity Event Processing

**Status**: COMPLETED

**Files to Change**:
- `csharp/src/Telemetry/MetricsAggregator.cs`

**Changes**:

#### 6.1 Process Activity Events (cloudfetch.download_summary)

The `MetricsAggregator` needs to process Activity events to extract chunk latency metrics. Add event processing in `ProcessActivity` or `AggregateActivityTags`:

```csharp
private void ProcessActivityEvents(StatementTelemetryContext context, Activity activity)
{
    foreach (var activityEvent in activity.Events)
    {
        if (activityEvent.Name == "cloudfetch.download_summary")
        {
            foreach (var tag in activityEvent.Tags)
            {
                switch (tag.Key)
                {
                    case "initial_chunk_latency_ms":
                        if (tag.Value is long initialLatency)
                        {
                            context.SetInitialChunkLatency(initialLatency);
                        }
                        break;

                    case "slowest_chunk_latency_ms":
                        if (tag.Value is long slowestLatency)
                        {
                            context.UpdateSlowestChunkLatency(slowestLatency);
                        }
                        break;

                    case "total_files":
                        if (tag.Value is int totalFiles)
                        {
                            context.AddChunkCount(totalFiles);
                        }
                        break;

                    case "total_bytes":
                        if (tag.Value is long totalBytes)
                        {
                            context.AddBytesDownloaded(totalBytes);
                        }
                        break;
                }
            }
        }
    }
}
```

#### 6.2 Call Event Processing

In `ProcessStatementActivity`, call the event processor:

```csharp
private void ProcessStatementActivity(Activity activity)
{
    // ... existing code to get statementId, sessionId ...

    // Get or create context for this statement
    var context = _statementContexts.GetOrAdd(
        statementId,
        _ => new StatementTelemetryContext(statementId, sessionId));

    // Aggregate metrics from tags
    context.AddLatency((long)activity.Duration.TotalMilliseconds);
    AggregateActivityTags(context, activity);

    // Process Activity events (like cloudfetch.download_summary)
    ProcessActivityEvents(context, activity);  // NEW
}
```

---

### Phase 7: Unit Tests

**Status**: COMPLETED

**Files to Change**:
- `csharp/test/Unit/Telemetry/MetricsAggregatorTests.cs`
- `csharp/test/Unit/Reader/CloudFetch/CloudFetchDownloaderTests.cs` (new or existing)

**Changes**:

#### 7.1 MetricsAggregator Tests

```csharp
[Fact]
public void StatementTelemetryContext_ChunkLatency_TracksInitialAndSlowest()
{
    // Arrange
    var context = new StatementTelemetryContext("stmt-1", "session-1");

    // Act - simulate multiple chunk downloads
    context.SetInitialChunkLatency(100);  // First chunk
    context.SetInitialChunkLatency(50);   // Should be ignored (not first)
    context.UpdateSlowestChunkLatency(100);
    context.UpdateSlowestChunkLatency(200);  // New max
    context.UpdateSlowestChunkLatency(150);  // Should not update

    // Assert
    Assert.Equal(100, context.InitialChunkLatencyMs);
    Assert.Equal(200, context.SlowestChunkLatencyMs);
}

[Fact]
public void StatementTelemetryContext_ChunkLatency_NullWhenNotSet()
{
    // Arrange
    var context = new StatementTelemetryContext("stmt-1", "session-1");

    // Assert - no values set
    Assert.Null(context.InitialChunkLatencyMs);
    Assert.Null(context.SlowestChunkLatencyMs);
}

[Fact]
public async Task MetricsAggregator_CreateTelemetryEvent_IncludesChunkLatency()
{
    // Arrange
    _aggregator = new MetricsAggregator(_mockExporter, _config, TestWorkspaceId, TestUserAgent);

    // Create activity with chunk latency tags
    using var activity = CreateStatementActivity("stmt-chunk-latency", "session-1");
    activity.SetTag("chunk.initial_latency_ms", "100");
    activity.SetTag("chunk.slowest_latency_ms", "500");
    activity.Stop();

    // Act
    _aggregator.ProcessActivity(activity);
    _aggregator.CompleteStatement("stmt-chunk-latency");
    await _aggregator.FlushAsync();

    // Assert
    Assert.Equal(1, _mockExporter.ExportCallCount);
    var log = _mockExporter.ExportedLogs.First();
    Assert.NotNull(log.Entry?.SqlDriverLog?.SqlOperation?.ChunkDetails);
    Assert.Equal(100, log.Entry.SqlDriverLog.SqlOperation.ChunkDetails.InitialChunkLatencyMillis);
    Assert.Equal(500, log.Entry.SqlDriverLog.SqlOperation.ChunkDetails.SlowestChunkLatencyMillis);
}
```

#### 7.2 CloudFetchDownloader Tests

```csharp
[Fact]
public async Task CloudFetchDownloader_TracksChunkLatency_InDownloadSummary()
{
    // Arrange - set up mock HTTP client, queues, etc.
    // ...

    // Act - download multiple chunks
    // ...

    // Assert - verify download_summary event contains latency metrics
    var summaryEvent = activity.Events.FirstOrDefault(e => e.Name == "cloudfetch.download_summary");
    Assert.NotNull(summaryEvent);

    var initialLatency = summaryEvent.Tags.FirstOrDefault(t => t.Key == "initial_chunk_latency_ms");
    var slowestLatency = summaryEvent.Tags.FirstOrDefault(t => t.Key == "slowest_chunk_latency_ms");

    Assert.NotNull(initialLatency.Value);
    Assert.NotNull(slowestLatency.Value);
    Assert.True((long)slowestLatency.Value >= (long)initialLatency.Value);
}
```

---

## Implementation Order

Recommended order to minimize integration issues:

1. **Phase 2**: Tag Definitions (no dependencies)
2. **Phase 4**: StatementTelemetryContext (no dependencies)
3. **Phase 3**: CloudFetchDownloader latency tracking
4. **Phase 5**: MetricsAggregator tag processing
5. **Phase 6**: Activity event processing
6. **Phase 7**: Unit tests

Phase 1 is already completed.

---

## Verification Checklist

After implementation, verify:

- [ ] `dotnet build` succeeds for all target frameworks (netstandard2.0, net472, net8.0)
- [ ] `dotnet test` passes all existing tests
- [ ] New unit tests pass
- [ ] CloudFetch downloads emit `initial_chunk_latency_ms` and `slowest_chunk_latency_ms` in download_summary
- [ ] MetricsAggregator correctly maps chunk latency to proto `ChunkDetails`
- [ ] Exported telemetry contains non-zero chunk latency values for CloudFetch queries

---

## Files Summary

| File | Phase | Changes |
|------|-------|---------|
| `TagDefinitions/StatementExecutionEvent.cs` | 2 | Add chunk latency tag definitions |
| `Reader/CloudFetch/CloudFetchDownloader.cs` | 3 | Track initial/slowest chunk latency |
| `Telemetry/MetricsAggregator.cs` | 4, 5, 6 | Add context fields, tag processing, event processing |
| `test/Unit/Telemetry/MetricsAggregatorTests.cs` | 7 | Add chunk latency tests |
| `test/Unit/Reader/CloudFetch/CloudFetchDownloaderTests.cs` | 7 | Add latency tracking tests |

---

## Rollback Plan

If issues arise:
1. Chunk latency fields are optional (nullable) - missing values won't break telemetry
2. Proto schema supports the fields - no backend changes needed
3. Can revert individual phases independently since they're loosely coupled
