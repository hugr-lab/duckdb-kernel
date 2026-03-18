# DuckDB Kernel — Architecture

## Overview

DuckDB Kernel is a Jupyter kernel that executes SQL queries against DuckDB and renders results
with interactive Perspective viewer and Geo Map visualization.

It consists of three components:

1. **Kernel** — Go binary implementing Jupyter kernel protocol + Arrow HTTP server
2. **JupyterLab Extension** — TypeScript labextension with Perspective viewer + Geo Map plugin
3. **VS Code Extension** — TypeScript notebook renderer with same viewer + map plugin

```
┌─────────────────────────────────────────────────────────────────┐
│                        User Notebook                            │
│   SQL Cell → Execute → Results (Datagrid / Geo Map / Charts)    │
└──────────────┬──────────────────────────────────┬───────────────┘
               │                                  │
    ┌──────────▼──────────┐            ┌──────────▼──────────┐
    │   JupyterLab        │            │   VS Code           │
    │   labextension      │            │   notebook renderer  │
    │   (widget.ts)       │            │   (renderer.ts)      │
    │                     │            │                      │
    │   Perspective       │            │   Perspective        │
    │   (bundled in       │            │   (loaded from       │
    │    labextension)    │            │    kernel /static/)  │
    │   + Geo Map plugin  │            │   + Geo Map plugin   │
    └──────────┬──────────┘            └──────┬───────┬───────┘
               │ HTTP (Arrow data)            │       │
               │                    HTTP (data)│       │ HTTP (JS/WASM)
    ┌──────────▼──────────────────────────────▼───────▼───────┐
    │                  DuckDB Kernel (Go)                      │
    │                                                          │
    │  ┌─────────┐  ┌──────────────┐  ┌──────────┐ ┌─────────┐  │
    │  │ Jupyter  │  │ Arrow HTTP   │  │ Spool    │ │ Static  │  │
    │  │ Protocol │  │ Server       │  │ (Arrow   │ │ Files   │  │
    │  │ (ZMQ)    │  │ /arrow/stream│  │  IPC)    │ │ /static/│  │
    │  └────┬─────┘  └──────┬───────┘  └────┬─────┘ └────┬────┘  │
    │       │               │               │            │        │
    │       │               │               │    Perspective      │
    │       │               │               │    JS/WASM/CSS      │
    │       │               │               │    (for VS Code)    │
    │       │               │                    │              │
    │  ┌────▼───────────────▼────────────────────▼───────────┐  │
    │  │              DuckDB Engine                           │  │
    │  │  SQL Execution → Arrow RecordReader                  │  │
    │  │  → Flatten → GeoArrow Convert → Spool Write          │  │
    │  └──────────────────────────────────────────────────────┘  │
    └──────────────────────────────────────────────────────────────┘
```

---

## Component Details

### 1. Kernel (Go binary)

**Location:** `cmd/duckdb-kernel/`, `internal/`

**Responsibilities:**
- Jupyter kernel protocol over ZMQ (shell, IOPub, stdin, control, heartbeat)
- SQL execution via DuckDB (go-duckdb/v2)
- Arrow IPC streaming to disk (spool)
- Arrow HTTP server for data delivery to frontend
- Database introspection API

**Data pipeline:**

```
DuckDB RecordReader
  → pkg/flatten.Converter     (Struct/List/Map → flat columns)
  → pkg/geoarrow.Converter    (WKB binary → native GeoArrow, parallel)
  → spool.StreamWriter         (Arrow IPC file on disk)
```

**HTTP endpoints:**

| Endpoint | Purpose |
| --- | --- |
| `/arrow/stream?q={id}` | Arrow IPC stream for Perspective (geometry → `{geometry}` string) |
| `/arrow/stream?q={id}&geoarrow=1` | Arrow IPC stream for Geo Map (native GeoArrow binary) |
| `/arrow?q={id}` | Raw Arrow IPC file (small datasets) |
| `/introspect?type=...` | Database metadata (schemas, tables, columns) |
| `/spool/delete?query_id=...` | Delete spool file |
| `/static/perspective/` | Perspective JS/WASM assets (for VS Code) |

**Key packages:**

| Package | Purpose |
| --- | --- |
| `pkg/geoarrow` | Zero-alloc WKB→GeoArrow converter, RecordReader pipeline |
| `pkg/flatten` | Flatten nested Arrow types (Struct/List/Map) |
| `internal/engine` | DuckDB execution, geometry detection |
| `internal/spool` | Arrow IPC file management |
| `internal/kernel` | Jupyter protocol, Arrow HTTP server |
| `internal/session` | Session management, execution pipeline |

### 2. JupyterLab Extension

**Location:** `extensions/jupyterlab/`

**Package:** `@hugr-lab/perspective-viewer` (installed as labextension)

**Responsibilities:**
- Renders query results in Perspective viewer (Datagrid, Charts)
- Geo Map plugin for spatial data visualization
- Open in Tab functionality (separate perspective tab)

**Key files:**

| File | Purpose |
| --- | --- |
| `src/widget.ts` | Main output widget, loads Arrow data, creates Perspective viewer |
| `src/map-plugin.ts` | Geo Map deck.gl plugin (shared with VS Code) |
| `src/plugin.ts` | JupyterLab plugin registration |
| `src/perspectiveTab.ts` | Separate tab for full-screen Perspective |

**How it loads data:**
1. Kernel sends `display_data` with metadata (arrow_url, geometry_columns, etc.)
2. Widget creates `<perspective-viewer>`, loads Arrow Table from Perspective client
3. When Geo Map selected: fetches `/arrow/stream?geoarrow=1` for binary geometry
4. Perspective handles Datagrid/Charts; Geo Map handles map rendering independently

### 3. VS Code Extension

**Location:** `extensions/vscode/`

**Package:** `hugr-result-renderer` (published to VS Code Marketplace)

**Responsibilities:**
- Notebook output renderer for DuckDB kernel results
- Database explorer panel (tree view)
- Same Perspective viewer + Geo Map as JupyterLab

**Key files:**

| File | Purpose |
| --- | --- |
| `src/renderer.ts` | Notebook renderer, loads Perspective from kernel static files |
| `src/map-plugin.ts` | Geo Map plugin (same as JupyterLab) |
| `src/extension.ts` | Extension host (commands, tree views) |
| `src/perspectivePanel.ts` | Webview panel for full-screen Perspective |

**How Perspective loads:**
1. Renderer gets kernel HTTP base URL from result metadata
2. Loads Perspective JS/WASM from `{kernelUrl}/static/perspective/`
3. Creates `<perspective-viewer>`, streams Arrow data

---

## Installation

### Kernel

```bash
# Download binary for your platform from GitHub Releases
# or build from source:
go build -tags duckdb_arrow -o duckdb-kernel ./cmd/duckdb-kernel

# Install kernel spec
mkdir -p ~/Library/Jupyter/kernels/duckdb
cp duckdb-kernel ~/Library/Jupyter/kernels/duckdb/
cp kernel/kernel.json ~/Library/Jupyter/kernels/duckdb/

# Copy perspective static files for VS Code (from release tarball)
mkdir -p ~/Library/Jupyter/kernels/duckdb/static/perspective
tar -xzf perspective-static.tar.gz -C ~/Library/Jupyter/kernels/duckdb/static/
```

`kernel.json`:
```json
{
  "argv": ["{kernel_dir}/duckdb-kernel", "--connection-file", "{connection_file}"],
  "display_name": "DuckDB",
  "language": "sql"
}
```

### JupyterLab Extension

```bash
pip install hugr-perspective-viewer
# or from source:
cd extensions/jupyterlab
pip install -e .
```

The labextension bundles Perspective JS/WASM inside the Python wheel.
No separate perspective installation needed.

### VS Code Extension

Install from VS Code Marketplace: search "HUGR Result Viewer"

Or from VSIX:
```bash
code --install-extension hugr-result-renderer.vsix
```

The extension loads Perspective from kernel's static files at runtime.
Kernel must be installed for VS Code rendering to work.

---

## Data Flow

### Query Execution

```
1. User types SQL in notebook cell
2. Jupyter client sends execute_request via ZMQ
3. Kernel executes SQL via DuckDB → gets Arrow RecordReader
4. Pipeline: Flatten → GeoArrow Convert → Spool Write (Arrow IPC to disk)
5. Kernel sends display_data with metadata:
   {
     "arrow_url": "http://127.0.0.1:PORT/arrow/stream?q=UUID",
     "geometry_columns": [{"name":"geom","srid":4326,"format":"GeoArrow:point"}],
     "columns": ["geom","city","population"],
     "total_rows": 749147,
     "query_time_ms": 50
   }
```

### Perspective (Datagrid/Charts) Path

```
Frontend receives metadata
  → fetches /arrow/stream?q=UUID
  → StringReplacer converts geometry → "{geometry}" string
  → Perspective loads Arrow table
  → Renders Datagrid with {geometry} in geom column
```

### Geo Map Path

```
Frontend receives metadata
  → detects geometry_columns
  → fetches /arrow/stream?q=UUID&geoarrow=1
  → Receives native GeoArrow columns (List<List<FixedSizeList<Float64>>>)
  → Extracts Float64Array coordinates from Arrow nested structure
  → Passes binary data to deck.gl layers (zero JS objects, GPU-native)
  → Renders ScatterplotLayer / PathLayer / SolidPolygonLayer / ColumnLayer
```

### Spool File Format

Arrow IPC streaming format. One file per query result.

```
Location: /tmp/duckdb-kernel/{session_id}/{query_id}.arrow
Format: Apache Arrow IPC (streaming)
Schema: original columns with geometry converted to GeoArrow
Retention: last 5 files, max 1 hour
```

---

## Geo Map Plugin

### Slot Configuration

| Slot | Purpose | Multiple |
| --- | --- | --- |
| Geometry | Geometry column (auto-detected) | No |
| Color | Fill color field or fixed color | No |
| Size | Point radius / line width | No |
| Height | 3D extrusion (points only) | No |
| Tooltip | Fields shown on hover | Yes |

### Settings Panel

Slide-out panel (☰ button) with:
- Color: scale mode (quantize/quantile/log/category/identity), palette, default color
- Size: scale mode (linear/sqrt/log/identity), min/max range, default size
- Appearance: opacity slider, basemap selector (auto/voyager/positron/dark)

### Supported Geometry Types

| Source | GeoArrow Type | Deck.gl Layer |
| --- | --- | --- |
| ST_Point | geoarrow.multipoint | ScatterplotLayer / ColumnLayer |
| ST_LineString | geoarrow.multilinestring | PathLayer |
| ST_Polygon | geoarrow.multipolygon | SolidPolygonLayer |
| H3 index | (string column) | H3HexagonLayer |
| GeoJSON text | (fallback, JS parsing) | Same as above |

### Theming

Uses Perspective CSS custom properties for auto-theming:
- `--plugin--background` — panel/legend background
- `--map-element-background` — map overlay background
- `--icon--color` — text color
- `--inactive--border-color` — borders
- `--warning--background/--warning--color` — tooltip colors
- Auto basemap: dark themes → CARTO Dark Matter, light → CARTO Voyager

---

## Development

### Build from source

```bash
# Kernel
go build -tags duckdb_arrow -o duckdb-kernel ./cmd/duckdb-kernel

# JupyterLab extension
cd extensions/jupyterlab
jlpm install && jlpm build

# VS Code extension
cd extensions/vscode
npm install && npm run build

# Run tests
go test -tags duckdb_arrow ./...
```

### Dev kernel installation

```bash
# Symlink for development
ln -s $(pwd) ~/Library/Jupyter/kernels/duckdb-dev/static

# Build and install kernel
go build -tags duckdb_arrow -o ~/Library/Jupyter/kernels/duckdb-dev/duckdb-kernel ./cmd/duckdb-kernel

# Deploy JupyterLab extension to local env
cp -r extensions/jupyterlab/hugr_perspective/labextension .venv/share/jupyter/labextensions/@hugr-lab/perspective-viewer

# VS Code: symlink extension
ln -s $(pwd)/extensions/vscode ~/.vscode/extensions/hugr-lab.hugr-result-renderer-0.2.0
```
