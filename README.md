# DuckDB Kernel

[![CI](https://github.com/hugr-lab/duckdb-kernel/actions/workflows/ci.yml/badge.svg)](https://github.com/hugr-lab/duckdb-kernel/actions/workflows/ci.yml)
[![Release](https://github.com/hugr-lab/duckdb-kernel/actions/workflows/release.yml/badge.svg)](https://github.com/hugr-lab/duckdb-kernel/actions/workflows/release.yml)
[![PyPI](https://img.shields.io/pypi/v/hugr-perspective-viewer)](https://pypi.org/project/hugr-perspective-viewer/)

A Jupyter-compatible kernel implemented in Go that provides a SQL execution environment backed by DuckDB with interactive result visualization via Perspective and deck.gl maps.

## Features

- SQL execution via embedded DuckDB
- Interactive result viewer powered by [Perspective](https://perspective.finos.org/) (WebAssembly)
- Geo Map plugin — render millions of geometries (points, lines, polygons) on interactive maps via deck.gl
- Arrow IPC streaming with LZ4 compression — large datasets without blocking the UI
- Database Explorer sidebar — browse catalogs, tables, schemas, functions, extensions
- Pin/unpin results — save query results next to your notebook, survive kernel restarts
- Session recovery — results reload automatically after page refresh
- Meta commands for database exploration
- Compatible with JupyterLab, JupyterHub, and VS Code

## Install

### From release (recommended)

```bash
curl -fsSL https://raw.githubusercontent.com/hugr-lab/duckdb-kernel/main/install.sh | bash
```

To install a specific version:

```bash
curl -fsSL https://raw.githubusercontent.com/hugr-lab/duckdb-kernel/main/install.sh | bash -s v0.2.0
```

### From source

Requires Go 1.22+ and a C compiler (CGo is used by the DuckDB driver).

```bash
git clone https://github.com/hugr-lab/duckdb-kernel.git
cd duckdb-kernel
make install
```

Verify the installation:

```bash
jupyter kernelspec list
```

## Usage

1. Start JupyterLab: `jupyter lab`
2. Create a new notebook and select the **DuckDB** kernel
3. Execute SQL queries in notebook cells

### Example

```sql
CREATE TABLE users (id INTEGER, name VARCHAR);
INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
SELECT * FROM users;
```

### Meta Commands

| Command | Description |
| --- | --- |
| `:help` | Show available commands |
| `:version` | Show kernel and DuckDB version |
| `:tables` | List all tables |
| `:schemas` | List all schemas |
| `:describe <table>` | Show table columns and types |
| `:explain <SQL>` | Show query execution plan (EXPLAIN ANALYZE) |
| `:limit <n>` | Set preview row limit |
| `:save_results <id>` | Pin query result to persistent storage |
| `:clear_results` | Delete all pinned results |

### Shared Sessions

Set the `DUCKDB_SHARED_SESSION` environment variable to share a DuckDB session across multiple notebooks:

```bash
export DUCKDB_SHARED_SESSION=my-analysis
jupyter lab
```

## Perspective Viewer (Interactive Visualization)

The DuckDB Kernel emits result metadata via a custom MIME type (`application/vnd.hugr.result+json`) alongside plain text output. When paired with the Perspective viewer extension, query results render as interactive visualizations.

### JupyterLab Extension

```bash
pip install hugr-perspective-viewer
```

### VS Code Extension

Install from [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=hugr-lab.hugr-result-renderer) or download `hugr-result-renderer.vsix` from the [latest release](https://github.com/hugr-lab/duckdb-kernel/releases/latest):

```bash
code --install-extension hugr-result-renderer.vsix
```

### Capabilities

- **Interactive table**: sort, filter, and scroll through results
- **Pivot tables**: group by rows and columns with aggregations (sum, count, average)
- **Charts**: bar, line, and scatter visualizations
- **Geo Map**: render spatial data on interactive maps with tooltips, legend, 3D extrusion
- **Large datasets**: up to 5M rows streamed directly from Arrow IPC files
- **Multipart results**: multiple result sets rendered on separate tabs (Arrow, JSON, errors)

The viewer loads data directly from Arrow IPC files — large datasets are never transmitted through the Jupyter protocol.

Environments without the extension installed continue to display plain text ASCII tables.

## Geo Map

Query results containing geometry columns (WKB, GeoJSON, H3Cell) are automatically detected and rendered on an interactive deck.gl map. The kernel converts WKB to native GeoArrow at query time, and deck.gl reads Arrow buffers directly — no GeoJSON serialization overhead.

Tested with up to 5M objects (points, lines, complex polygons like administrative boundaries).

Configure basemap tile sources via `--basemaps` flag or `DUCKDB_KERNEL_BASEMAPS` environment variable:

```bash
export DUCKDB_KERNEL_BASEMAPS='[{"name":"OSM","url":"https://tile.openstreetmap.org/{z}/{x}/{y}.png","type":"raster"}]'
```

## Arrow Result Files

Query results are written as LZ4-compressed Arrow IPC files to a flat spool directory:

```text
$TMPDIR/duckdb-kernel/
  {queryID}.arrow
```

- **TTL**: 24 hours (configurable via `DUCKDB_KERNEL_SPOOL_TTL`)
- **Max disk**: 2 GB (configurable via `DUCKDB_KERNEL_SPOOL_MAX_SIZE`)
- **Spool dir**: configurable via `DUCKDB_KERNEL_SPOOL_DIR`
- Files survive kernel shutdown for reload recovery

### Pin Results (JupyterLab)

Click the pin button to save results to `duckdb-results/` next to your notebook. Pinned results survive kernel restarts and have no TTL.

## Integration

For kernels that want to use the Perspective viewer, see [docs/perspective-viewer-integration.md](docs/perspective-viewer-integration.md) — full protocol spec including metadata format, Arrow streaming wire format, spool management endpoints, and `pkg/geoarrow` API reference.

## Development

### Building Extensions Locally

**JupyterLab extension:**

```bash
cd extensions/jupyterlab
jlpm install
jlpm build        # dev build
jlpm build:prod   # production build
```

**VS Code extension:**

```bash
cd extensions/vscode
npm install
npm run build
npx @vscode/vsce package --no-dependencies -o hugr-result-renderer.vsix
```

### CI & Release

CI runs on every push/PR: kernel build + integration tests (Linux, macOS, Windows), JupyterLab extension build, and VS Code extension build.

On release tags (`v*`), the release workflow:

- Builds kernel binaries for linux/amd64, linux/arm64, darwin/arm64, windows/amd64
- Builds and publishes the JupyterLab extension to PyPI
- Packages the VS Code extension as `.vsix`
- Bundles Perspective static files as `perspective-static.tar.gz`
- Creates a GitHub Release with all artifacts

## License

MIT
