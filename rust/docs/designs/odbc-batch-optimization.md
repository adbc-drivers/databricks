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

# ODBC Batch Optimization: Merging and Pre-Formatting

## Overview

This document describes two opt-in optimizations in the Rust ADBC driver that
dramatically improve ODBC data extraction performance. When the C++ ODBC layer
sets specific database options, the Rust driver merges small batches into larger
ones and pre-formats non-string columns to UTF-8 strings before FFI export.

These optimizations are driven by profiling data from a Tableau Server workload:
51M rows, 89 columns, ~85 GB, extracting from a Databricks warehouse via ODBC.

### Motivation

The ODBC driver wraps the Rust ADBC driver, receiving Arrow RecordBatches via
the Arrow C Data Interface (`ArrowArrayStream`). Profiling revealed two major
costs in the ODBC layer:

**1. Batch boundary overhead (102s total, 1.7 min)**

CloudFetch produces batches averaging 556 rows. Tableau fetches 800 rows at a
time via `SQLFetch`. Every `SQLFetch` crosses ~1.44 batch boundaries, each
triggering:
- FFI `get_next` call (0.3ms)
- `ImportRecordBatch` to wrap C structs in C++ Arrow objects (0.25ms)
- Release of the previous batch via FFI callbacks (0.11ms)
- Accessor cache invalidation and re-creation (0.6ms)

With 91,952 batches, this adds up to 102s of pure overhead.

**2. Per-cell type formatting (1120s total, 18.7 min)**

Tableau binds all columns as `SQL_C_CHAR` (strings). For 47 non-string columns
(int64, decimal128, timestamp, date, bool), the ODBC driver must format each
value to a string on every row. Over 4.5 billion cells, per-cell formatting
costs dominate:

| Type | us/cell | Cost for 6 cols |
|------|---------|-----------------|
| timestamp | 0.280 | 259s |
| date32 | 0.186 | 115s |
| decimal128 | 0.090 | 277s |
| int64 | 0.051 | 126s |
| string (memcpy only) | 0.052 | 335s |

The string memcpy path (0.052 us/cell) is the floor — it's just an offset
lookup + memcpy. If all columns arrived as pre-formatted strings, the entire
data conversion phase would drop from 1120s to ~236s.

### Design Goals

1. **Opt-in via database options** — no behavior change for non-ODBC consumers
2. **Zero changes to the ADBC spec** — the driver still exports standard
   `ArrowArrayStream` with valid Arrow RecordBatches
3. **Minimal Rust code changes** — modifications only in the batch production
   path, not in CloudFetch downloading or connection management
4. **Pipelineable** — formatting work in Rust overlaps with Tableau's processing
   of the previous batch, hiding most of the formatting latency

## Configuration

Two new database options, both strings, set via `DatabaseSetOption`:

### `databricks.odbc.batch_merge_target_rows`

- **Type:** String (parsed as `usize`)
- **Default:** `"0"` (disabled — batches pass through unchanged)
- **Recommended:** `"8000"` (10x Tableau's fetch size of 800)
- **Behavior:** When > 0, the driver accumulates small batches from CloudFetch
  and concatenates them into larger batches of approximately this many rows
  before serving them via `get_next`.

### `databricks.odbc.string_preformat`

- **Type:** String (`"true"` / `"false"`)
- **Default:** `"false"` (disabled)
- **Behavior:** When `"true"`, before exporting a batch via FFI, cast all
  non-Utf8 columns to Utf8 using Arrow's `cast` kernel. The exported batch
  has all-string columns. The original Arrow schema metadata (column names,
  nullability) is preserved; only the data types change to Utf8.

### Setting from C++ ODBC layer

The ODBC driver sets these unconditionally in `SetAdbcDatabaseOptions`:

```cpp
set_db_option("databricks.odbc.batch_merge_target_rows", "8000");
set_db_option("databricks.odbc.string_preformat", "true");
```

Other ADBC consumers (Python, Go, direct ADBC users) never set these options
and get standard ADBC behavior.

## Implementation: Batch Merging

### Where

`StreamingCloudFetchProvider::next_batch()` in
`src/reader/cloudfetch/streaming_provider.rs`

### Current behavior

```
next_batch() → pop one RecordBatch from current_batch_buffer → return it
```

Each batch is ~556 rows (determined by server-side CloudFetch chunk splitting).

### New behavior (when `batch_merge_target_rows > 0`)

```
next_batch():
    accumulated_rows = 0
    pending_batches = vec![]
    
    while accumulated_rows < merge_target_rows:
        batch = pop from current_batch_buffer (or fetch next chunk)
        if batch is None:
            break  // end of stream
        accumulated_rows += batch.num_rows()
        pending_batches.push(batch)
    
    if pending_batches.is_empty():
        return None
    if pending_batches.len() == 1:
        return pending_batches.pop()
    
    return arrow::compute::concat_batches(&schema, &pending_batches)
```

### Key considerations

- **`concat_batches` copies data.** This is intentional — the copy produces a
  single contiguous buffer per column, which improves cache locality when the
  C++ ODBC layer reads it. The copy cost is small (~12 MB for 8000 rows) and
  acts as a cache-warming sequential scan.

- **Last batch may be smaller.** When the stream is exhausted, the final merged
  batch may have fewer than `merge_target_rows` rows. This is fine.

- **Memory accounting.** After merging, release the source batches immediately
  (they're copied into the merged batch). The `chunks_in_memory` counter should
  be decremented for each consumed chunk, not per merged batch. This ensures the
  download pipeline continues to run at full speed.

- **Schema must be captured before merging.** The `schema` OnceLock should be
  initialized from the first batch before any merging occurs.

### Config storage

Add to `CloudFetchConfig`:

```rust
pub struct CloudFetchConfig {
    // ... existing fields ...
    
    /// Target number of rows per merged batch. 0 = disabled.
    pub batch_merge_target_rows: usize,
}
```

Default: `0` (disabled).

Add option parsing in `Database::set_option` alongside existing CloudFetch
options:

```rust
"databricks.odbc.batch_merge_target_rows" => {
    if let OptionValue::String(v) = value {
        self.cloudfetch_config.batch_merge_target_rows = v.parse().map_err(|_| ...)?;
        Ok(())
    } else { ... }
}
```

## Implementation: String Pre-Formatting

### Where

In the batch production path, after merging (if enabled) and before the batch
is returned from `next_batch()`. This could also be placed in the
`ResultReaderAdapter::next()` method that wraps `next_batch()` for FFI export.

### Algorithm

```rust
fn preformat_batch(batch: RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    let mut new_fields: Vec<Field> = Vec::with_capacity(schema.fields().len());
    
    for (i, field) in schema.fields().iter().enumerate() {
        let col = batch.column(i);
        if col.data_type() == &DataType::Utf8 || col.data_type() == &DataType::LargeUtf8 {
            // Already a string — pass through
            new_columns.push(col.clone());
            new_fields.push(field.as_ref().clone());
        } else {
            // Cast to Utf8
            let string_col = arrow::compute::cast(col, &DataType::Utf8)?;
            new_fields.push(Field::new(field.name(), DataType::Utf8, field.is_nullable()));
            new_columns.push(string_col);
        }
    }
    
    let new_schema = Arc::new(Schema::new(new_fields));
    Ok(RecordBatch::try_new(new_schema, new_columns)?)
}
```

### Formatting requirements by type

The ODBC layer requires specific string formats for some types. Arrow's default
`cast` kernel produces correct output for most types, but the following need
attention:

| Arrow Type | Arrow `cast` output | ODBC requirement | Action needed |
|------------|-------------------|-----------------|---------------|
| Boolean | `"true"` / `"false"` | `"1"` / `"0"` | Custom cast |
| Timestamp(tz) | `"2021-01-01T00:00:00.000000+0000"` | `"2021-01-01 00:00:00"` (no tz, no trailing zeros) | Custom format |
| Double | Full precision | `"%.15g"` format (15 significant digits) | Custom format |
| Date32 | `"2021-01-01"` | `"2021-01-01"` | Arrow cast is fine |
| Int64 | `"12345"` | `"12345"` | Arrow cast is fine |
| Decimal128 | `"123.4500000000"` | `"123.4500000000"` (preserves scale) | Arrow cast is fine |

For types where Arrow's `cast` output doesn't match ODBC conventions, use a
custom formatting kernel instead of `arrow::compute::cast`. The custom formats
are:

**Boolean:** Build a StringArray with `"1"` for true, `"0"` for false.

**Timestamp with timezone:**
1. If timezone is UTC/Etc/UTC/GMT/Etc/GMT: values are already in UTC, no
   conversion needed.
2. Otherwise: convert UTC epoch to wall-clock time in the target timezone
   (use `chrono` or `chrono-tz`), then re-encode as naive epoch.
3. Format as `"YYYY-MM-DD HH:MM:SS[.frac]"` — no timezone suffix.
4. Strip trailing fractional zeros (`.123000` → `.123`, `.000000` → omitted).

**Double:** Format with `format!("{:.15e}", value)` or equivalent to match
`%.15g` behavior (15 significant digits, strip trailing zeros).

### Config storage

Add to `CloudFetchConfig` (or a separate ODBC config struct):

```rust
pub struct CloudFetchConfig {
    // ... existing fields ...
    
    /// Pre-format all non-string columns to Utf8 before FFI export.
    pub string_preformat: bool,
}
```

Default: `false`.

### Schema change implications

When `string_preformat` is enabled, the Arrow schema exported via
`get_schema` will have all Utf8 fields instead of the original types.

The C++ ODBC layer handles this correctly because:
- It already has the original schema metadata from the SEA manifest (column
  type names, precision, scale) stored in `AdbcResultSetMetadata`
- The Arrow schema is only used for type dispatch in `CreateAccessor`, and
  with all-Utf8 columns, it will use `DirectStringAccessor` for every column
  (pure memcpy, no formatting)
- `SQLDescribeCol` and `SQLColAttribute` return metadata from the manifest,
  not from the Arrow schema

## Performance Projections

### Current production numbers (51M rows, 89 cols)

| Component | Time |
|-----------|------|
| Data conversion (formatting + copy) | 1120s (18.7 min) |
| Batch overhead (FFI + import + release + cache) | 102s (1.7 min) |
| Tableau processing | 780s (13.0 min) |
| Connection/metadata | 300s (5.0 min) |
| **Total** | **~2300s (38.4 min)** |

### With batch merging only (8000-row batches)

| Component | Time | Change |
|-----------|------|--------|
| Data conversion | ~1050s | -70s (better cache) |
| Batch overhead | ~7s | -95s (14x fewer batches) |
| **Total** | **~2135s (35.6 min)** | **-2.8 min** |

### With batch merging + string pre-formatting

| Component | Time | Change |
|-----------|------|--------|
| Data conversion | ~236s | -884s (memcpy only) |
| Batch overhead | ~7s | -95s |
| Rust formatting | ~0s additional | Overlapped with Tableau |
| **Total** | **~1320s (22.0 min)** | **-16.4 min** |

The Rust formatting cost is hidden because it happens while Tableau is
ingesting the previous batch into its Hyper engine (~13 min of gaps between
SQLFetch calls). As long as Rust can format a batch faster than Tableau can
ingest one, the formatting is fully pipelined.

Formatting speed estimate: Arrow-rs `cast` processes ~100M values/sec for
integer→string. With 47 non-string columns × 8000 rows = 376K values per
batch, formatting takes ~4ms per batch. Tableau spends ~12ms between fetches.
Formatting is 3x faster than consumption — fully pipelined.

## Testing

### Unit tests

1. **Batch merging disabled (default):** `next_batch()` returns individual
   batches unchanged.
2. **Batch merging enabled:** With target=100 and input batches of 30 rows
   each, verify merged output has ~100 rows (3-4 batches merged).
3. **Batch merging at end of stream:** Last batch may be smaller than target.
4. **String pre-formatting disabled (default):** Batch schema and types
   unchanged.
5. **String pre-formatting enabled:** All columns become Utf8, values match
   expected format (especially boolean "1"/"0", timestamp without tz suffix).
6. **Both enabled:** Merged + pre-formatted batches.

### Integration test with ODBC layer

Run the Tableau replay benchmark with both options enabled:

```bash
# In the ODBC benchmark, after setting database options:
set_db_option("databricks.odbc.batch_merge_target_rows", "8000");
set_db_option("databricks.odbc.string_preformat", "true");
```

Verify:
- Row counts match (no data loss from merging)
- String values match expected ODBC format
- Performance improvement vs baseline

## File Changes Summary

| File | Change |
|------|--------|
| `src/types/cloudfetch.rs` | Add `batch_merge_target_rows: usize` and `string_preformat: bool` to `CloudFetchConfig` |
| `src/database.rs` | Parse new options in `set_option` match arm |
| `src/reader/cloudfetch/streaming_provider.rs` | Implement batch accumulation + `concat_batches` in `next_batch()` |
| `src/reader/mod.rs` or new `src/reader/preformat.rs` | Implement `preformat_batch()` function |
| `src/reader/mod.rs` | Call `preformat_batch()` in `ResultReaderAdapter::next()` when enabled |
