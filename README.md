# DuckDB Kernel

A Jupyter-compatible kernel implemented in Go that provides a SQL execution environment backed by DuckDB.

## Features

- SQL execution via embedded DuckDB
- Plain text table result rendering
- Arrow IPC result materialization
- Shared sessions across notebooks
- Meta commands for database exploration
- Compatible with JupyterLab, JupyterHub, and VS Code

## Build

```bash
make build
```

## Install

```bash
make install
```

This copies the binary and `kernel.json` to the Jupyter kernel directory.

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

## Arrow Result Files

Query results are automatically written as Arrow IPC files to:

```
/tmp/duckdb-kernel/<session-id>/<query-id>.arrow
```

Cleanup policy: last 5 results retained, files older than 1 hour removed.

## License

MIT
