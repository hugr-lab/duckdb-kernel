# 007 — Spool TTL & Session Recovery

## Status: Draft

## Problem

1. Page reload (JupyterLab / VS Code) → Arrow URLs still valid but kernel may have restarted
2. Kernel restart → new sessionID → old spool files orphaned
3. `/tmp/` fills up with orphaned Arrow files
4. TTL too aggressive (1 hour, 5 files) — loses results during normal work
5. No way to recover results after browser refresh without re-running queries

## Goals

- Results survive browser/IDE reload (without kernel restart)
- Kernel restart = results lost (accepted)
- Disk cleanup via TTL — no manual intervention
- Configurable limits (TTL, max disk usage)
- Frontend shows friendly message when result expired

## Design

### Flat spool directory (no session nesting)

```
/tmp/duckdb-kernel/
  ├── {queryID}.arrow              ← Arrow IPC file
  ├── {queryID}.arrow
  ├── ...
  └── history_{sessionID}.json     ← per-session command history
```

No session subdirectories. All Arrow files in one flat dir.
QueryID is UUID — globally unique, no collisions between sessions.

### TTL in file metadata (not filename)

Use file modification time (`ModTime`) + configurable TTL duration.

```
Expired = now > file.ModTime() + TTL
```

Default TTL: **24 hours**. Configurable via environment variable or kernel arg.

Filename format: `{queryID}.arrow` (clean, no TTL encoded).

### Disk usage limit

In addition to TTL, enforce max total disk usage for spool dir.

Default: **2 GB**. Configurable.

Cleanup strategy (on kernel start + after each query):
1. Delete files where `ModTime + TTL < now`
2. If total size > max, delete oldest files until under limit

### Kernel startup: register existing files

On kernel start:
1. Scan spool dir
2. Delete expired files (TTL check)
3. Register remaining files as available for `/arrow/stream`
4. No need to parse Arrow content — just track `queryID → filepath`

This means: if kernel restarts but files are still on disk and not expired,
they are available. But since kernel restart generates new Arrow URLs
(new port), the old URLs from frontend are invalid.

**Key insight:** kernel restart = new HTTP port → old URLs broken anyway.
So recovery only matters for browser/IDE reload **without** kernel restart.

### Browser/IDE reload recovery

When browser reloads:
1. JupyterLab re-renders notebook cells
2. Each cell has stored metadata with `arrow_url`
3. Widget fetches Arrow data from `arrow_url`
4. If kernel is still running (same port) → files exist → works
5. If kernel restarted (different port) → 404 → show "Re-run cell"

**This already works** — the only change needed is:
- Increase TTL from 1h to 24h
- Increase max files from 5 to unlimited (use disk limit instead)
- Frontend: graceful 404 handling instead of error

### Frontend 404 handling

When Arrow fetch returns 404 or connection error:

```typescript
// In map-plugin.ts loadGeoArrowData():
const response = await fetch(url);
if (!response.ok) {
  // Show friendly message instead of error
  return null; // triggers "Result unavailable" placeholder
}
```

In widget.ts for Perspective:
```typescript
try {
  const table = await streamArrowToTable(url, client, signal);
} catch (err) {
  // Show "Result expired. Re-run the cell to refresh."
  container.innerHTML = '<div class="hugr-result-expired">Result expired. Re-run the cell.</div>';
  return;
}
```

### Configuration

| Parameter | Default | Env var | Kernel arg |
| --- | --- | --- | --- |
| TTL | 24h | `DUCKDB_KERNEL_SPOOL_TTL` | `--spool-ttl=24h` |
| Max disk | 2GB | `DUCKDB_KERNEL_SPOOL_MAX_SIZE` | `--spool-max-size=2g` |
| Spool dir | `/tmp/duckdb-kernel` | `DUCKDB_KERNEL_SPOOL_DIR` | `--spool-dir=/path` |

### LZ4 compression

Arrow IPC supports LZ4 frame compression natively:

```go
// Writer
w := ipc.NewWriter(f, ipc.WithSchema(schema), ipc.WithLZ4())

// Reader
r, err := ipc.NewReader(f) // auto-detects compression
```

Reduces disk usage ~2-5x for typical data. Slight CPU overhead on write/read.

Enable by default. Can be disabled via config if CPU is bottleneck.

### Changes required

**Spool package (`internal/spool/`):**
- Remove session subdirectory creation
- Flat file structure: `{dir}/{queryID}.arrow`
- `Cleanup()`: TTL-based + disk size limit
- `NewSpool()`: takes dir path, TTL, max size (not sessionID)
- On init: scan dir, delete expired, register existing

**Session (`internal/session/`):**
- Don't pass sessionID to spool (spool is shared)
- History file: `{dir}/history_{sessionID}.json`

**Kernel (`internal/kernel/`):**
- Parse config (env vars / args) for TTL, max size, spool dir
- Pass config to spool

**Frontend (widget.ts, map-plugin.ts):**
- Graceful 404 handling → "Result expired" message

### Migration

Old format: `/tmp/duckdb-kernel/{sessionID}/{queryID}.arrow`
New format: `/tmp/duckdb-kernel/{queryID}.arrow`

On first start with new kernel: old session dirs are treated as unknown files,
cleaned up by TTL/size limits. No explicit migration needed.

### Persistent results ("Pin")

Two storage locations:

```
/tmp/duckdb-kernel/                ← volatile spool (TTL, auto-cleanup)
  ├── {queryID}.arrow

{notebook_dir}/.duckdb-results/    ← persistent (no TTL, user-managed)
  ├── {queryID}.arrow
```

**Pin action:** user clicks "Save Results" button (in output toolbar) or runs `\save_results`.
This copies `.arrow` file from tmp spool to `.duckdb-results/` next to notebook.

**Lookup order:** kernel resolves queryID:
1. Check persistent dir (CWD/.duckdb-results/)
2. Check volatile spool (/tmp/duckdb-kernel/)
3. 404 if not found in either

**Persistent dir properties:**
- No TTL — files stay until user deletes
- No size limit — user is responsible
- `.gitignore` entry added automatically on first pin
- Survives kernel restart (kernel scans on startup)
- Portable — move project folder, results come along

**Frontend "Save Results" button:**
- Added next to "Open in Tab" in output toolbar
- Sends message to kernel: `{ type: "pin_result", query_id: "..." }`
- Kernel copies file → responds with success/error
- Button changes to "Saved" (disabled) after pin

**Meta command:**
```
\save_results          -- pin last query result
\save_results <id>     -- pin specific query by ID
\clear_results         -- delete all pinned results
```

### Without kernel

When notebook opened without running kernel:
- Perspective output shows saved preview (text table from display_data)
- Map shows "Start kernel to load map data"
- If kernel starts and finds pinned .arrow files → full data loads

## Implementation Plan

### Phase 1: Spool refactor (backend)

| # | Task | Files | Notes |
| --- | --- | --- | --- |
| 1.1 | Flat spool dir (remove session nesting) | `internal/spool/spool.go` | `NewSpool(dir)` instead of `NewSpool(sessionID)` |
| 1.2 | TTL config (default 24h) | `internal/spool/spool.go` | `Cleanup()` uses `ModTime + TTL` |
| 1.3 | Disk size limit (default 2GB) | `internal/spool/spool.go` | Delete oldest when over limit |
| 1.4 | Kernel startup: scan + register existing files | `internal/spool/spool.go`, `internal/kernel/kernel.go` | `spool.Scan()` returns known queryIDs |
| 1.5 | Config: env vars + kernel args | `cmd/duckdb-kernel/main.go`, `internal/kernel/kernel.go` | `DUCKDB_KERNEL_SPOOL_TTL`, `DUCKDB_KERNEL_SPOOL_MAX_SIZE`, `DUCKDB_KERNEL_SPOOL_DIR` |
| 1.6 | History file: move to spool dir | `internal/kernel/history.go` | `{spool_dir}/history_{sessionID}.json` |
| 1.7 | Update tests | `internal/spool/`, tests/ | Adapt integration tests to new spool structure |

### Phase 2: Persistent results ("Pin")

| # | Task | Files | Notes |
| --- | --- | --- | --- |
| 2.1 | Persistent dir support in spool | `internal/spool/spool.go` | Second dir (CWD/.duckdb-results/), no TTL |
| 2.2 | Dual lookup: persistent → volatile | `internal/spool/spool.go`, `internal/kernel/arrowhttp.go` | `OpenReader` checks both dirs |
| 2.3 | Pin endpoint | `internal/kernel/arrowhttp.go` | `POST /spool/pin?q={queryID}` → copies to persistent |
| 2.4 | Auto-create `.gitignore` | `internal/spool/spool.go` | On first pin, write `.duckdb-results/.gitignore` with `*` |
| 2.5 | Meta command `\save_results` | `internal/meta/commands.go` | Calls pin endpoint internally |
| 2.6 | Meta command `\clear_results` | `internal/meta/commands.go` | Deletes persistent dir contents |

### Phase 3: Frontend

| # | Task | Files | Notes |
| --- | --- | --- | --- |
| 3.1 | Graceful 404 in widget.ts | `extensions/jupyterlab/src/widget.ts` | "Result expired. Re-run cell." placeholder |
| 3.2 | Graceful 404 in map-plugin.ts | `extensions/jupyterlab/src/map-plugin.ts` | "Result expired" instead of error |
| 3.3 | "Save Results" button in output toolbar | `extensions/jupyterlab/src/widget.ts` | Next to "Open in Tab", calls kernel pin endpoint |
| 3.4 | Button state: saved/unsaved indicator | `extensions/jupyterlab/src/widget.ts` | Changes to "Saved ✓" after pin |
| 3.5 | VS Code: same 404 handling + save button | `extensions/vscode/src/renderer.ts` | Mirror JupyterLab changes |

### Phase 4: Compression

| # | Task | Files | Notes |
| --- | --- | --- | --- |
| 4.1 | LZ4 compression in spool writer | `internal/spool/spool.go` | `ipc.WithLZ4()` option |
| 4.2 | Verify reader auto-detects compression | `internal/spool/spool.go` | Arrow IPC reader handles LZ4 transparently |
| 4.3 | Compression config (on/off) | Config | Default: on |

### Dependencies

```
Phase 1 (spool refactor) → independent, start first
Phase 2 (pin) → depends on 1.1-1.4
Phase 3 (frontend) → depends on 1.1 (404 behavior)
Phase 4 (compression) → depends on 1.1
```

### Effort estimate

| Phase | Effort |
| --- | --- |
| Phase 1 | M (spool refactor + config + tests) |
| Phase 2 | M (pin endpoint + meta commands + dual lookup) |
| Phase 3 | S (404 handling + button) |
| Phase 4 | XS (one-line LZ4 flag) |
