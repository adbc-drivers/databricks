# Engineer-Bot Learning Log

Append-only log of durable, non-obvious engineering lessons from fixes the
engineer-bot makes in this repo, so future runs (and humans) don't relearn them.
The bot reads this as system-prompt context (newest entries first) on every run —
**consult past entries before fixing a similar area** (grep by keyword or scan
titles below).

> Lives at the repo-root `.claude/` for now (one log for the whole repo); we can
> split it per-language (`csharp/`, `go/`, `rust/`) later if it grows.

## When to add an entry

Add an entry when a fix surfaced a **durable, reusable** lesson:

- A recurring gotcha in the driver (Thrift vs SEA path, metadata handling, etc.).
- A protocol/server behavior that contradicted the obvious mental model.
- A fix pattern worth reusing — or an anti-pattern to avoid.
- A cross-driver parity rule (e.g. how the JDBC driver or the vendored
  `hiveserver2` base behaves) confirmed against real source.

Skip one-shots: typos, stale builds, local-machine flukes. Entries are for things
the next fix will hit again.

## Format

```
### YYYY-MM-DD: <area> — <brief headline>

**Issue:** what went wrong
**Root Cause:** why it happened
**Fix:** what changed, and where (file/symbol)
**Rule:** the one-line do/don't takeaway — write it greppable
```

The `**Rule:**` line is what makes an entry searchable — phrase it so a future
reader can grep for it.

## Entries

(no entries yet)
