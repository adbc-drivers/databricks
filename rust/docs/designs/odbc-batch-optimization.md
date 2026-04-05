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

# Batch Merging for Large Result Sets

## Overview

This document describes an opt-in optimization in the Rust ADBC driver that
merges small Arrow RecordBatches into larger ones before serving them via the
Arrow C Data Interface (`ArrowArrayStream::get_next`). This is a generic
performance optimization that benefits any consumer processing large result
sets, particularly the ODBC driver used with Tableau.

## Motivation

CloudFetch downloads produce Arrow IPC chunks that are split by the server.
Each chunk typically contains batches of ~500-600 rows. For consumers that
process data in larger units (e.g., Tableau fetches 800 rows per `SQLFetch`),
small batches cause significant per-batch overhead:

**Profiling data** (51M rows, 89 columns, Tableau extract):

| Per-batch cost | Avg time | Total (91,952 batches) |
|----------------|----------|------------------------|
| `get_next` (FFI export) | 0.30ms | 28s |
| Consumer batch import | 0.25ms | 23s |
| Previous batch release (FFI callbacks) | 0.11ms | 10s |
| Consumer accessor setup | 0.60ms | 55s |
| **Total per-batch overhead** | **~1.1ms** | **~102s (1.7 min)** |

With 91,952 small batches, per-batch overhead alone accounts for 102 seconds.
Merging into ~8000-row batches reduces the batch count to ~6,400, cutting this
overhead by 93%.

Additionally, larger batches improve cache locality — the consumer reads each
column's data buffer sequentially over more rows before moving to the next
column, reducing CPU cache thrashing.

## Configuration

### `databricks.cloudfetch.batch_merge_target_rows`

- **Type:** String (parsed as `usize`)
- **Default:** `"0"` (disabled — batches pass through unchanged)
- **Recommended for ODBC:** `"8000"`
- **Behavior:** When > 0, the driver accumulates consecutive batches and
  concatenates them into larger batches of approximately this many rows before
  serving via `get_next`. The last batch in the stream may be smaller.

Set via `DatabaseSetOption`:

```cpp
// C++ (ODBC layer sets this during database initialization)
set_db_option("databricks.cloudfetch.batch_merge_target_rows", "8000");
```

```python
# Python (if desired, though not typically needed)
db.set_option("databricks.cloudfetch.batch_merge_target_rows", "8000")
```

## Implementation

### Config storage

Add to `CloudFetchConfig` in `src/types/cloudfetch.rs`:

```rust
pub struct CloudFetchConfig {
    // ... existing fields ...

    /// Target number of rows per merged batch. 0 = disabled (pass through as-is).
    /// When > 0, consecutive small batches are concatenated into larger batches
    /// of approximately this many rows before being served to consumers.
    pub batch_merge_target_rows: usize,
}
```

Default: `0`.

### Option parsing

Add to `Database::set_option` in `src/database.rs`, alongside existing
CloudFetch options:

```rust
"databricks.cloudfetch.batch_merge_target_rows" => {
    if let OptionValue::String(v) = value {
        self.cloudfetch_config.batch_merge_target_rows =
            v.parse().map_err(|_| /* invalid option error */)?;
        Ok(())
    } else {
        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
    }
}
```

### Batch merging in the download pipeline

Merging happens in the background download tasks inside
`StreamingCloudFetchProvider::schedule_downloads()` in
`src/reader/cloudfetch/streaming_provider.rs`. After a chunk is downloaded and
parsed into `Vec<RecordBatch>`, the batches are merged before being stored in
the chunks map.

This design means the merge cost is hidden behind download latency — the
consumer always receives pre-merged batches with no additional blocking.

Two helper functions handle the merge:

```rust
/// Merge batches into larger batches up to `target_rows` each.
fn merge_batches_to_target(
    batches: Vec<RecordBatch>,
    target_rows: usize,
) -> Result<Vec<RecordBatch>> {
    // Accumulates consecutive batches until target_rows is reached,
    // then flushes (concat_batches) and starts a new accumulator.
    // Last batch may be smaller than target. Single batches that
    // exceed the target are passed through as-is.
}

/// Concatenate pending batches into a single batch.
fn flush_pending(schema: &SchemaRef, pending: &mut Vec<RecordBatch>) -> Result<RecordBatch>
```

In the download task:

```rust
self.runtime_handle.spawn(async move {
    let result = Self::download_chunk_with_retry(...).await;
    match result {
        Ok(batches) => {
            // Merge in the download task — async, parallel with consumer
            let batches = if batch_merge_target_rows > 0 {
                merge_batches_to_target(batches, batch_merge_target_rows)?
            } else {
                batches
            };
            entry.batches = Some(batches);
            entry.state = ChunkState::Downloaded;
        }
        ...
    }
});
```

The existing `next_batch()` is unchanged — it simply drains `current_batch_buffer`
as before. Since each chunk's batches are already merged, the buffer contains
fewer, larger batches.

### Key considerations

**`concat_batches` copies data.** This is intentional and beneficial:
- Produces a single contiguous buffer per column (better cache locality)
- The sequential copy acts as a cache-warming scan
- Cost is proportional to data size: ~12 MB for 8000 rows of 89 columns,
  taking ~1-2ms — negligible vs the 102s overhead it eliminates

**Async merging.** Because merging runs in the download task (a tokio spawn),
it overlaps with downloading of other chunks and with the consumer processing
previous batches. The merge latency is fully hidden.

**Row-target configurability.** The `batch_merge_target_rows` setting controls
the maximum rows per merged batch, not "merge all batches in a chunk". If a
chunk has 24K rows across 44 batches and the target is 20K, it produces two
batches (one ~20K, one ~4K). This keeps the optimization general-purpose
rather than tied to a specific workload's chunk sizes.

**Memory management.** After merging, the source batches are dropped (copied
into the merged batch). The `chunks_in_memory` counter is decremented per
consumed chunk as usual — merging doesn't affect the memory accounting.

**Last batch.** The final merged batch within a chunk may have fewer than
`batch_merge_target_rows` rows. This is expected and correct.

**Inline provider.** The inline result provider (for small result sets returned
directly in the API response) is unaffected — its batches are typically already
consolidated and don't go through CloudFetch's download pipeline.

## Expected Impact

| Batch size | Batches (51M rows) | Batch overhead | Savings |
|------------|-------------------|----------------|---------|
| 556 (current) | 91,952 | 102s | — |
| 4000 | 12,774 | 14s | 88s |
| **8000 (recommended)** | **6,387** | **7s** | **95s** |
| 16000 | 3,194 | 4s | 98s |

Diminishing returns above 8000. The recommended value of 8000 captures 93% of
the possible overhead reduction while keeping per-batch memory at ~12 MB.

Additional cache locality improvement is harder to quantify but was measured at
5-10% of data conversion time in benchmarks, corresponding to ~50-100s on the
production workload.

**Total estimated savings: ~150-200s (2.5-3.3 min).**

## Testing

### Unit tests

1. **Disabled (default):** `next_batch()` with `batch_merge_target_rows=0`
   returns individual batches unchanged. Verify batch count and row counts
   match input.

2. **Merging enabled:** With `batch_merge_target_rows=100` and input batches
   of 30 rows each, verify:
   - First 3 calls return batches of ~100 rows (3-4 source batches merged)
   - Data values are preserved and in correct order
   - Schema is preserved

3. **End of stream:** With 250 total rows and `target=100`, verify:
   - First 2 calls return ~100-row batches
   - Third call returns ~50-row batch (remainder)
   - Fourth call returns `None`

4. **Single batch exceeds target:** A source batch with 200 rows and
   `target=100` is returned as-is (no splitting).

5. **Empty batches:** Source stream with 0-row batches interspersed — verify
   they are skipped during merging.

### Integration test

Verify end-to-end with the ODBC replay benchmark:

```bash
# Should produce identical row counts and data as without merging
./build/Release/tableau_replay_benchmark ~/odbc-wbd 3 --bind --fetch-size 800
```

## File Changes Summary

| File | Change |
|------|--------|
| `src/types/cloudfetch.rs` | Add `batch_merge_target_rows: usize` to `CloudFetchConfig`, default `0` |
| `src/database.rs` | Parse `databricks.cloudfetch.batch_merge_target_rows` in `set_option` |
| `src/reader/cloudfetch/streaming_provider.rs` | Add `merge_batches_to_target()` + `flush_pending()`, merge in download task |
