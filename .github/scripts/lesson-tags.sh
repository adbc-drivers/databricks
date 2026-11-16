#!/bin/bash
# Copyright (c) 2026 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Print the LESSONS_TAGS value for one bot session: the driver languages it works
# on, so it gets only the lessons tagged for those languages (plus the untagged,
# shared ones). See retrospective.lesson_tags in .bot/config.yaml.
#
#   PR session:    changed file paths on stdin, one per line: the driver directories
#                  (csharp/, go/, rust/) and their language-named CI files.
#   Issue session: ISSUE_TITLE and ISSUE_LABELS (comma-separated) in the environment.
#
# When no language can be told, every language is declared, so the session gets
# every lesson (never fewer than before languages were split).
set -euo pipefail

langs=""
add() { case ",$langs," in *",$1,"*) ;; *) langs="${langs:+$langs,}$1" ;; esac; }

if [ -n "${ISSUE_TITLE:-}${ISSUE_LABELS:-}" ]; then
  title=$(printf '%s' "${ISSUE_TITLE:-}" | tr '[:upper:]' '[:lower:]')
  labels=",$(printf '%s' "${ISSUE_LABELS:-}" | tr '[:upper:]' '[:lower:]'),"
  if [[ $labels == *",.net,"* || $labels == *",csharp,"* || $title =~ csharp|c#|\.net|dotnet ]]; then
    add csharp
  fi
  if [[ $labels == *",go,"* || $title =~ \((go|golang)\)|^(go|golang):|golang|go\ driver ]]; then
    add go
  fi
  if [[ $labels == *",rust,"* || $title =~ \(rust\)|^rust:|rust\ driver ]]; then
    add rust
  fi
else
  while IFS= read -r path || [ -n "$path" ]; do
    case "$path" in
      csharp/* | ci/scripts/csharp* | .github/workflows/csharp*) add csharp ;;
      go/* | ci/scripts/go_* | .github/workflows/go_*) add go ;;
      rust/* | ci/scripts/rust* | .github/workflows/rust*) add rust ;;
    esac
    # Language-specific files that live outside the driver dirs (CI scripts,
    # per-language workflows). Match distinctive tokens so e.g. a PR touching
    # go/ plus ci/scripts/csharp_test.sh still gets csharp lessons too.
    case "$path" in
      *csharp*|*dotnet*) add csharp ;;
    esac
    case "$path" in
      *rust*) add rust ;;
    esac
    case "$path" in
      *.go|*/go.mod|*/go.sum|go.mod|go.sum|*/go_*|go_*|*/go-*|go-*) add go ;;
    esac
  done
fi

echo "lang=${langs:-csharp,go,rust}"
