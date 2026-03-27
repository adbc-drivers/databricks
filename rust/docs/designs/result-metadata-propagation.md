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

# Result Metadata Propagation: SEA Manifest → Arrow Schema → ODBC

## Overview

This document describes the design for propagating Databricks SEA (Statement Execution API)
result manifest metadata through the Arrow C Data Interface FFI boundary, so that the ODBC
driver can access server-provided column metadata (type name, precision, scale, etc.) instead
of reverse-engineering it from Arrow type IDs.

### Motivation

The OSS ODBC driver's result metadata has systematic differences from the reference driver,
as measured by the cross-driver comparator. These differences fall into clear categories:

| Issue | Volume | Root Cause |
|-------|--------|------------|
| nullable always 0 (should be 1) | Every type | Arrow schema `nullable` flag ≠ logical nullability |
| STRING octet_length 255 vs 1020 | All string types | Not multiplying by UTF-8 max bytes |
| DECIMAL display_size/octet_length wrong | All DECIMAL cols | Hardcoded from Arrow precision, not server metadata |
| DATE/TIMESTAMP precision/scale swapped | All datetime types | Precision vs length vs scale semantics confused |
| Complex types (ARRAY/MAP/STRUCT) all zeros | Complex types | Arrow type falls through to default case |
| STRING precision 255 vs 0 | All string types | Reference returns 0 for variable-length |
| searchable=2 vs 3 for strings | All string types | Hardcoded ALL_EXCEPT_LIKE for everything |

The fundamental problem is **information loss at the ADBC Rust layer**. The SEA API provides
rich column metadata in the result manifest, but the ODBC driver only receives an Arrow schema
across the FFI boundary. The Arrow schema carries type information but not Databricks-specific
metadata like the server's type name, precision, scale, or type text.

### Reference: How JDBC Solves This

The Databricks JDBC driver consumes the SEA response directly. Its `DatabricksResultSetMetaData`
SEA constructor:

1. Reads `ColumnInfo` from `ResultManifest.schema.columns` — which includes `type_name`,
   `type_text`, `type_precision`, `type_scale`
2. Uses server-provided precision/scale when available, with per-type defaults as fallback
3. Defaults nullable to `NULLABLE` for all result columns (not derived from Arrow)
4. Computes display_size from `(type_name, precision, scale)` — a pure function of resolved metadata
5. Sets searchable=true for all result columns

This design brings the same approach to the ODBC driver by propagating the manifest metadata
through Arrow field-level key-value metadata.

---

## Architecture

### Current Flow (Information Loss)

```
SEA API Response
  ├─ ResultManifest
  │   └─ ColumnInfo[] { name, type_name, type_text, position }
  │      ← type_precision, type_scale, type_interval_type NOT deserialized
  │      ← Used ONLY for empty results (schema fallback)
  │
  └─ ResultData
     ├─ attachment (inline Arrow IPC)
     └─ external_links (CloudFetch)

     ↓ Arrow IPC parsed → ArrowSchema
     ↓ Manifest DISCARDED for inline/CloudFetch paths
     ↓ ArrowSchema crosses FFI boundary

C++ ODBC Driver
  └─ adbc_result_set_metadata.cpp
     └─ Reverse-engineers type_name, precision, scale, etc. from arrow::Type IDs
        ← Gets it wrong for nullable, complex types, DECIMAL sizes, etc.
```

### Target Flow (Metadata Preserved)

```
SEA API Response
  ├─ ResultManifest
  │   └─ ColumnInfo[] { name, type_name, type_text, position,
  │                      type_precision, type_scale, type_interval_type }
  │      ← Full deserialization
  │
  └─ ResultData (same as before)

     ↓ Arrow IPC parsed → ArrowSchema
     ↓ ResultReaderAdapter augments schema with manifest metadata
     ↓ Each Arrow Field gets databricks.* key-value metadata
     ↓ Augmented ArrowSchema crosses FFI boundary

C++ ODBC Driver
  └─ adbc_result_set_metadata.cpp
     └─ Reads databricks.* metadata as PRIMARY source
     └─ Falls back to Arrow type ID inference when metadata absent
```

### Arrow Field Metadata Keys

```
databricks.type_name          "DECIMAL", "STRING", "ARRAY", "TIMESTAMP", ...
databricks.type_text          "DECIMAL(10,2)", "ARRAY<INT>", "STRING", ...
databricks.type_precision     "10"  (string-encoded i64, optional)
databricks.type_scale         "2"   (string-encoded i64, optional)
databricks.type_interval_type "YEAR_MONTH"  (optional, for INTERVAL types)
```

These keys are additive — they don't conflict with any existing Arrow metadata conventions.
Values are string-encoded because Arrow field metadata is `HashMap<String, String>`.

---

## Detailed Design

### Change 1: Extend `ColumnInfo` Deserialization

**File:** `src/types/sea.rs`

The SEA API already returns `type_precision`, `type_scale`, and `type_interval_type` in the
manifest column schema. The Rust struct just isn't deserializing them.

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub type_name: String,
    pub type_text: String,
    pub position: i32,
    #[serde(default)]
    pub type_precision: Option<i64>,
    #[serde(default)]
    pub type_scale: Option<i64>,
    #[serde(default)]
    pub type_interval_type: Option<String>,
}
```

`serde(default)` ensures backward compatibility — responses without these fields parse
identically to today.

**Verification:** Log the deserialized manifest for a `SELECT CAST(1.23 AS DECIMAL(10,2))`
query and confirm `type_precision=10`, `type_scale=2` appear.

### Change 2: Define Metadata Key Constants

**File:** `src/reader/mod.rs` (new submodule `metadata_keys`)

```rust
/// Arrow field metadata keys for Databricks column information.
///
/// These propagate SEA manifest metadata through the Arrow C Data Interface
/// so downstream consumers (ODBC driver) can access server-provided metadata
/// without reverse-engineering from Arrow type IDs.
///
/// Keys use the `databricks.` prefix to avoid conflicts with other metadata
/// conventions (e.g., Arrow's own `ARROW:extension:name`).
pub mod metadata_keys {
    pub const TYPE_NAME: &str = "databricks.type_name";
    pub const TYPE_TEXT: &str = "databricks.type_text";
    pub const TYPE_PRECISION: &str = "databricks.type_precision";
    pub const TYPE_SCALE: &str = "databricks.type_scale";
    pub const TYPE_INTERVAL_TYPE: &str = "databricks.type_interval_type";
}
```

### Change 3: Schema Augmentation Function

**File:** `src/reader/mod.rs`

New method on `ResultReaderFactory`:

```rust
/// Augment an Arrow schema with Databricks column metadata from the manifest.
///
/// For each field in the schema, if a matching ColumnInfo exists (by position),
/// attach `databricks.*` key-value metadata to the field. This preserves
/// server-provided type_name, type_text, precision, scale, etc. through the
/// Arrow C Data Interface FFI boundary.
///
/// Design decisions:
/// - Matches by position index (0-based), not name, to handle duplicate column names
/// - Preserves any existing field metadata from the Arrow IPC stream
/// - Only adds keys when values are present (no empty/null entries)
/// - Returns a new schema — does not mutate the input
fn augment_schema_with_manifest(
    schema: &SchemaRef,
    manifest: &ResultManifest,
) -> SchemaRef {
    let columns = &manifest.schema.columns;

    let new_fields: Vec<Field> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let col_info = columns.iter().find(|c| c.position as usize == i);

            match col_info {
                Some(info) => {
                    let mut metadata = field.metadata().clone();
                    metadata.insert(
                        metadata_keys::TYPE_NAME.to_string(),
                        info.type_name.clone(),
                    );
                    metadata.insert(
                        metadata_keys::TYPE_TEXT.to_string(),
                        info.type_text.clone(),
                    );
                    if let Some(prec) = info.type_precision {
                        metadata.insert(
                            metadata_keys::TYPE_PRECISION.to_string(),
                            prec.to_string(),
                        );
                    }
                    if let Some(scale) = info.type_scale {
                        metadata.insert(
                            metadata_keys::TYPE_SCALE.to_string(),
                            scale.to_string(),
                        );
                    }
                    if let Some(ref interval) = info.type_interval_type {
                        metadata.insert(
                            metadata_keys::TYPE_INTERVAL_TYPE.to_string(),
                            interval.clone(),
                        );
                    }
                    field.as_ref().clone().with_metadata(metadata)
                }
                None => field.as_ref().clone(),
            }
        })
        .collect();

    Arc::new(Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ))
}
```

### Change 4: Pass Manifest Through to `ResultReaderAdapter`

The augmentation is applied in `ResultReaderAdapter::new()` — the single chokepoint that all
reader paths (CloudFetch, inline, empty) flow through before crossing the FFI boundary.

**Why this location:**
- Single augmentation point — no per-reader-type changes
- `ResultReaderAdapter` is already where the schema is extracted and cached
- Clean separation: readers provide raw Arrow schema, adapter adds Databricks metadata

#### 4a. `ExecuteResult` carries the manifest

**File:** `src/client/mod.rs`

```rust
pub struct ExecuteResult {
    pub statement_id: String,
    pub reader: Box<dyn ResultReader + Send>,
    pub manifest: Option<ResultManifest>,  // NEW
}
```

#### 4b. `SeaClient::execute_statement()` passes manifest through

**File:** `src/client/sea.rs`

```rust
async fn execute_statement(...) -> Result<ExecuteResult> {
    let response = self.call_execute_api(session_id, sql, params).await?;
    let response = self.wait_for_completion(response).await?;
    let reader_factory = self.reader_factory()?;
    let reader = reader_factory.create_reader(&response.statement_id, &response)?;

    Ok(ExecuteResult {
        statement_id: response.statement_id,
        reader,
        manifest: response.manifest,  // NEW — pass through
    })
}
```

#### 4c. `ResultReaderAdapter::new()` augments schema

**File:** `src/reader/mod.rs`

```rust
impl ResultReaderAdapter {
    pub fn new(
        inner: Box<dyn ResultReader + Send>,
        manifest: Option<&ResultManifest>,
    ) -> Result<Self> {
        let schema = inner.schema()?;
        let schema = match manifest {
            Some(m) => ResultReaderFactory::augment_schema_with_manifest(&schema, m),
            None => schema,
        };
        Ok(Self { inner, schema })
    }
}
```

#### 4d. `Statement::execute()` passes manifest to adapter

**File:** `src/statement.rs`

```rust
fn execute(&mut self) -> Result<impl RecordBatchReader + Send> {
    // ... existing code ...
    let result = self.runtime_handle
        .block_on(self.client.execute_statement(&self.session_id, query, &ExecuteParams::default()))
        .map_err(|e| e.to_adbc())?;

    self.current_statement_id = Some(result.statement_id);

    ResultReaderAdapter::new(result.reader, result.manifest.as_ref())
        .map_err(|e| e.to_adbc())
}
```

---

## Impact on Other Callers of `ResultReaderAdapter`

Any code that calls `ResultReaderAdapter::new()` needs to be updated to pass the manifest
(or `None` for non-SEA paths like metadata queries). Search for all call sites:

- `Statement::execute()` — pass `result.manifest.as_ref()`
- FFI metadata functions (`ffi/metadata.rs`) — pass `None` (these use SQL queries that
  return Arrow results without a SEA manifest structure, OR could be extended to carry
  manifests in the future)

For metadata functions that don't have manifests, passing `None` preserves current behavior.

---

## Downstream: C++ ODBC Driver (Separate PR)

Once the Rust side ships `databricks.*` field metadata, the C++ ODBC driver changes are
confined to `src/adbc_result_set_metadata.cpp`. The pattern for each metadata method:

```cpp
std::string AdbcResultSetMetadata::GetTypeName(int column_position, int16_t data_type) {
    auto field = GetField(column_position);

    // Primary: server-provided metadata
    auto metadata = field->metadata();
    if (metadata) {
        auto idx = metadata->FindKey("databricks.type_name");
        if (idx >= 0) {
            return metadata->value(idx);
        }
    }

    // Fallback: infer from Arrow type ID (current behavior)
    // ... existing switch statement ...
}
```

### Specific ODBC fixes enabled by this metadata

| Metadata field | ODBC fix |
|---------------|----------|
| `databricks.type_name` | Return "STRING" not "VARCHAR" for complex types; correct type names for all types |
| `databricks.type_text` | Full type text including parameters ("DECIMAL(10,2)", "ARRAY<INT>") |
| `databricks.type_precision` | Correct DECIMAL precision from server, not Arrow physical type |
| `databricks.type_scale` | Correct DECIMAL scale from server |
| (nullable default) | Default to SQL_NULLABLE for all result columns (matches JDBC) |

The nullable fix is a policy change in the ODBC driver (default to NULLABLE) rather than
a metadata propagation issue — the SEA manifest doesn't include per-column nullability either.
The JDBC driver simply defaults all result columns to NULLABLE, which is the correct semantic
for query results (any expression can return NULL).

---

## Testing Strategy

### Unit Tests (Rust)

1. **`ColumnInfo` deserialization** — JSON with and without optional fields parses correctly
2. **`augment_schema_with_manifest`** — verify metadata is attached to correct fields:
   - Basic types: INT, STRING, BOOLEAN get `type_name` and `type_text`
   - DECIMAL: gets `type_precision` and `type_scale`
   - INTERVAL: gets `type_interval_type`
   - Missing ColumnInfo for a position: field unchanged
   - Existing field metadata preserved (not overwritten)
3. **`ResultReaderAdapter`** — with and without manifest produces correct schema

### Integration Tests (ODBC comparator)

After both Rust and C++ changes land, re-run the cross-driver comparator. Expected fixes:

- All `nullable` diffs resolve (default to NULLABLE)
- All `type_name` diffs resolve (server-provided names)
- DECIMAL `display_size`/`octet_length` diffs resolve (correct precision from server)
- STRING `octet_length`/`precision`/`searchable` diffs resolve
- Complex type (ARRAY/MAP/STRUCT) metadata diffs resolve

---

## File Change Summary

| File | Change | Risk |
|------|--------|------|
| `src/types/sea.rs` | Add 3 optional fields to `ColumnInfo` | None — `serde(default)` |
| `src/reader/mod.rs` | Add `metadata_keys` module | None — new code |
| `src/reader/mod.rs` | Add `augment_schema_with_manifest()` | Low — pure function |
| `src/reader/mod.rs` | Update `ResultReaderAdapter::new()` signature | Medium — all callers must update |
| `src/client/mod.rs` | Add `manifest` field to `ExecuteResult` | Low — additive |
| `src/client/sea.rs` | Pass `response.manifest` to `ExecuteResult` | None — data already available |
| `src/statement.rs` | Pass manifest to `ResultReaderAdapter::new()` | Low |
| `src/ffi/metadata.rs` | Pass `None` for manifest in metadata paths | None |

---

## Alternatives Considered

### Alternative A: Custom ADBC extension functions

Add non-standard ADBC functions like `AdbcStatementGetManifest()` that the ODBC driver calls
separately to get manifest metadata.

**Rejected:** Breaks the ADBC C interface contract. Would require custom FFI beyond the
standard `ArrowArrayStream` export. More coupling, harder to maintain.

### Alternative B: Compute correct metadata in C++ from Arrow types alone

Fix each metadata method in `adbc_result_set_metadata.cpp` to return the right values by
improving the Arrow type ID → ODBC metadata mapping.

**Rejected:** This is the current approach and it's fundamentally wrong. Arrow type IDs
don't carry enough information (no `type_text` for complex types, no server-side precision
for DECIMAL, no nullability intent). It forces the ODBC driver to guess what the server
meant, and different servers/versions may encode differently. Using server-provided metadata
is the only long-term correct solution.

### Alternative C: Augment schema inside each reader (InlineArrowReader, CloudFetchResultReader, EmptyReader)

Pass manifest to each reader type and augment in their `schema()` methods.

**Rejected in favor of `ResultReaderAdapter` approach:** Three augmentation points instead
of one. More code to maintain, easier to miss a path. The adapter is already the chokepoint.

### Alternative D: Schema-level metadata instead of field-level

Store all column metadata as a single JSON blob in schema-level metadata.

**Rejected:** Field-level metadata is the natural fit — each field's metadata travels with
that field through schema operations (projection, reordering). Schema-level metadata would
require index-based lookups that break if fields are reordered.
