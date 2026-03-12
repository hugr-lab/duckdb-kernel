# DuckDB Kernel

[![CI](https://github.com/hugr-lab/duckdb-kernel/actions/workflows/ci.yml/badge.svg)](https://github.com/hugr-lab/duckdb-kernel/actions/workflows/ci.yml)
[![Release](https://github.com/hugr-lab/duckdb-kernel/actions/workflows/release.yml/badge.svg)](https://github.com/hugr-lab/duckdb-kernel/actions/workflows/release.yml)
[![PyPI](https://img.shields.io/pypi/v/hugr-perspective-viewer)](https://pypi.org/project/hugr-perspective-viewer/)

A Jupyter-compatible kernel implemented in Go that provides a SQL execution environment backed by DuckDB.

## Features

- SQL execution via embedded DuckDB
- Plain text table result rendering
- Arrow IPC result materialization
- Shared sessions across notebooks
- Meta commands for database exploration
- Compatible with JupyterLab, JupyterHub, and VS Code

## Install

### From release (recommended)

```bash
curl -fsSL https://raw.githubusercontent.com/hugr-lab/duckdb-kernel/main/install.sh | bash
```

To install a specific version:

```bash
curl -fsSL https://raw.githubusercontent.com/hugr-lab/duckdb-kernel/main/install.sh | bash -s v0.1.0
```

### From source

Requires Go 1.26+ and a C compiler (CGo is used by the DuckDB driver).

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

| Command             | Description                    |
|---------------------|--------------------------------|
| `:help`             | Show available commands        |
| `:version`          | Show kernel and DuckDB version |
| `:tables`           | List all tables                |
| `:schemas`          | List all schemas               |
| `:describe <table>` | Show table columns and types   |
| `:limit <n>`        | Set preview row limit          |

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

Download `hugr-result-renderer.vsix` from the [latest release](https://github.com/hugr-lab/duckdb-kernel/releases/latest) and install it:

```bash
code --install-extension hugr-result-renderer.vsix
```

### Perspective Static Files (install.sh)

The `install.sh` script automatically downloads Perspective static files (JS/WASM) alongside the kernel binary. These enable the built-in result viewer without requiring additional extension installs.

### Capabilities

- **Interactive table**: sort, filter, and scroll through results
- **Pivot tables**: group by rows and columns with aggregations (sum, count, average)
- **Charts**: bar, line, and scatter visualizations
- **Large datasets**: up to 1M rows without blocking the UI (data loaded from Arrow files)

The viewer loads data directly from Arrow IPC files — large datasets are never transmitted through the Jupyter protocol.

Environments without the extension installed continue to display plain text ASCII tables.

## Arrow Result Files

Query results are automatically written as Arrow IPC files to:

```
/tmp/duckdb-kernel/<session-id>/<query-id>.arrow
```

Cleanup policy: last 5 results retained, files older than 1 hour removed.

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

CI runs three parallel jobs on every push/PR: kernel build + integration tests, JupyterLab extension build, and VS Code extension build.

On release tags (`v*`), the release workflow:
- Builds kernel binaries for linux/amd64, linux/arm64, darwin/arm64, windows/amd64
- Builds and publishes the JupyterLab extension to PyPI
- Packages the VS Code extension as `.vsix`
- Bundles Perspective static files as `perspective-static.tar.gz`
- Creates a GitHub Release with all artifacts

## License

MIT
