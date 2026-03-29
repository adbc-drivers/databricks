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

# C# Driver Release Process

## Overview

The C# driver uses a release branch model because downstream consumers (e.g., PowerBI) ship a specific driver version and cannot freely upgrade. When PowerBI ships `v1.0.0`, they may remain on that version for months. If a bug is found, they need a `v1.0.1` hotfix delivered to their `v1.0.0` release line — without being forced to take new features from `v1.1.x` or later.

This means **multiple release branches coexist simultaneously**, each independently maintainable via cherry-picks:

```
release/csharp/v1.0.0:  [v1.0.0] → cherry-pick fix → [v1.0.1] → cherry-pick fix → [v1.0.2]
release/csharp/v1.1.0:  [v1.1.0] → cherry-pick fix → [v1.1.1]
release/csharp/v1.2.0:  [v1.2.0]
```

This differs from the Go and Rust drivers, which tag directly on `main` and cannot hotfix a specific past version — consumers must upgrade to get fixes.

The ADBC driver version is included in the user agent string sent to Databricks, making it visible in query history for tracing exactly which version a consumer is running.

## Branch and Tag Naming

| Artifact | Pattern | Example |
|----------|---------|---------|
| Release branch | `release/csharp/vX.Y.Z` | `release/csharp/v1.1.0` |
| Tag | `csharp/vX.Y.Z` | `csharp/v1.1.1` |
| Stable branch | `stable/csharp` | — |

## Versioning Rules

- **New release branch** (cutoff): bump the **minor** version — `v1.1.0` → `v1.2.0`
- **Hotfix on existing release branch**: bump the **patch** version — `v1.1.0` → `v1.1.1` → `v1.1.2`

The major version is reserved for breaking changes.

## Lifecycle

```mermaid
flowchart LR
    subgraph main
        A((A)) --> B((B)) --> C((C)) --> D((D)) --> E((E)) --> F((F))
    end
    subgraph release/csharp/v1.1.0
        B -.->|branch| R1((start))
        R1 --> R2((merges\nfrom main))
        R2 -->|final merge + tag csharp/v1.1.0| R3((cutoff))
        R3 -->|cherry-pick| R4((fix A\ntag csharp/v1.1.1))
        R4 -->|cherry-pick| R5((fix B\ntag csharp/v1.1.2))
    end
    subgraph stable/csharp
        R3 -.->|updated at cutoff| S1((csharp/v1.1.0))
        R4 -.->|updated after patch| S2((csharp/v1.1.1))
        R5 -.->|updated after patch| S3((csharp/v1.1.2))
    end
```

### Phase 1: Pre-Cutoff

- All new commits go to `main` as usual.
- The release branch is created early from `main` and periodically merged from `main` to stay current.
- **Never commit directly to the release branch during this phase.**

### Phase 2: Cutoff

1. Final merge of `main` into the release branch
2. Tag the cutoff commit (e.g., `csharp/v1.1.0`)
3. Update `stable/csharp` to point to this tag
4. Create the next release branch (e.g., `release/csharp/v1.2.0`) immediately, to give the team a landing place for new work

### Phase 3: Post-Cutoff (Maintenance)

- Fixes go to `main` first, then cherry-picked to the release branch via PR.
- Each cherry-pick batch gets a new patch tag (e.g., `csharp/v1.1.1`, `csharp/v1.1.2`).
- Update `stable/csharp` after each patch tag: `git push origin release/csharp/v1.1.0:stable/csharp --force`

## Scope

This applies **only to the C# driver**. Other drivers in the monorepo are unaffected:

| Driver | Release Mechanism | Can hotfix old patch? |
|--------|------------------|-----------------------|
| **C#** | Release branches + tags | Yes — each minor version has its own branch |
| **Go** | Tags on `main` (`go/v0.1.x`) | No — consumers must upgrade |
| **Rust** | None | — |

The release branch contains the full monorepo (Git doesn't support partial branches), but only C# changes are cherry-picked and built from it.

## Stable Branch

`stable/csharp` always points to the latest release. It exists for consumers that run a plain `git clone` with no `--branch` flag. All other consumers should pin to a specific tag or release branch directly.

## CI/CD

```yaml
on:
  push:
    branches:
      - 'release/csharp/*'
      - 'stable/csharp'
    paths: ['csharp/**']
  push:
    tags:
      - 'csharp/v*'
```

On tag push, the workflow publishes the NuGet package to NuGet.org in addition to running build and tests. Release branches require PR and passing CI to merge.

## Consumer Mapping (e.g., PowerBI)

- **Pin to a tag** (e.g., `csharp/v1.1.0`) — most stable, no automatic updates
- **Track a release branch** (e.g., `release/csharp/v1.1.0`) — receives cherry-picks automatically
- **Clone `stable/csharp`** — always tracks the latest release, no branch name needed
