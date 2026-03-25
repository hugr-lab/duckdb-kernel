# Release Verification Checklist

Manual verification after publishing a new release.
Run on a clean environment (not your dev setup).

## 1. Kernel Installation

### Via install script (Linux/macOS)

```bash
curl -fsSL https://raw.githubusercontent.com/hugr-lab/duckdb-kernel/main/install.sh | bash
```

Verify:
```bash
jupyter kernelspec list | grep duckdb
ls ~/Library/Jupyter/kernels/duckdb/          # macOS
ls ~/.local/share/jupyter/kernels/duckdb/     # Linux
```

Expected: `duckdb-kernel` binary, `kernel.json`, logos, `static/perspective/` directory.

### Via VS Code extension

1. Install VSIX from Marketplace or `code --install-extension hugr-result-renderer.vsix`
2. Open DuckDB Explorer sidebar → click "Install / Update DuckDB Kernel"
3. Verify kernel installed (notification + sidebar shows "Session" info after restart)

---

## 2. JupyterLab (clean venv)

```bash
# Create clean environment
python3 -m venv /tmp/test-duckdb-kernel
source /tmp/test-duckdb-kernel/bin/activate

# Install JupyterLab + extensions
pip install jupyterlab
pip install hugr-perspective-viewer hugr-duckdb-explorer

# Verify extensions
jupyter labextension list    # should show @hugr-lab/perspective-viewer, @hugr-lab/duckdb-explorer
jupyter server extension list  # should show hugr_perspective

# Install kernel (if not already installed via install.sh)
curl -fsSL https://raw.githubusercontent.com/hugr-lab/duckdb-kernel/main/install.sh | bash

# Start JupyterLab
jupyter lab
```

### Test cases

- [ ] Create notebook, select DuckDB kernel
- [ ] `SELECT 1` → Perspective table renders
- [ ] `INSTALL spatial;` → shows "Success (X ms)"
- [ ] `SELECT * FROM duckdb_extensions()` → multipart tabs work
- [ ] DuckDB Explorer sidebar shows schema tree (SESSION, CATALOG sections)
- [ ] Sidebar works independently (schema tree populates after first query)
- [ ] Pin/unpin result → pin survives kernel restart
- [ ] "Open in Tab" → result opens in separate JupyterLab tab
- [ ] JSON result → tree/raw toggle works

### Cleanup

```bash
deactivate
rm -rf /tmp/test-duckdb-kernel
```

---

## 3. VS Code

### Setup

1. Install extension from Marketplace (or VSIX from release)
2. Kernel should be available (installed via script or VS Code "Install DuckDB Kernel")

### Test cases

- [ ] Create `.ipynb`, select DuckDB kernel
- [ ] `SELECT 1` → Perspective table renders in cell output
- [ ] `INSTALL spatial;` → shows "Success (X ms)"
- [ ] DuckDB Explorer sidebar shows schema tree (8 sections)
- [ ] Sidebar shows "Run a query to connect" before first query (not errors)
- [ ] After first query, sidebar populates with session/catalog info
- [ ] "Open in Perspective Tab" → result opens in separate editor tab
- [ ] Map visualization works (if geometry data available)
- [ ] Save CSV/JSON/Arrow buttons work on perspective tab

---

## 4. Cross-kernel (hugr-kernel)

If testing with hugr-kernel (separate repo):

```bash
pip install hugr-perspective-viewer hugr-duckdb-explorer
```

- [ ] `hugr-perspective-viewer` installs without errors
- [ ] Spool proxy routes registered (`jupyter server extension list`)
- [ ] GraphQL query results render in Perspective viewer
- [ ] Arrow streaming through spool proxy works

---

## 5. Release Artifacts Checklist

Verify all artifacts present in GitHub Release:

- [ ] `duckdb-kernel-linux-amd64`
- [ ] `duckdb-kernel-linux-arm64`
- [ ] `duckdb-kernel-darwin-arm64`
- [ ] `duckdb-kernel-windows-amd64.exe`
- [ ] `hugr_perspective_viewer-*.whl`
- [ ] `hugr_duckdb_explorer-*.whl`
- [ ] `hugr-result-renderer.vsix`
- [ ] `perspective-static.tar.gz`
- [ ] `kernel.json`
- [ ] `logo-32x32.png`, `logo-64x64.png`
- [ ] `install.sh`
