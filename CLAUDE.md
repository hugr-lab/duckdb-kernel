# duckdb-kernel Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-03-12

## Active Technologies
- Go 1.26 (kernel), TypeScript 5.x (frontend extensions), Python 3.12 (server extension packaging) (002-perspective-viewer)
- Arrow IPC files on local filesystem (existing spool mechanism) (002-perspective-viewer)
- Go 1.26 (kernel), TypeScript 5.x (extensions) + go-duckdb v2 (kernel), VS Code API (vscode ext), @jupyterlab/application + @jupyterlab/notebook (jupyterlab ext) (003-database-explorer-panel)
- Arrow IPC files on local filesystem (spool), DuckDB in-memory/attached databases (003-database-explorer-panel)
- Go 1.22+ (kernel), TypeScript 5.x (extensions) + `github.com/paulmach/orb` (Go WKB/WKT), `@deck.gl/core` + `@deck.gl/layers` + `@deck.gl/geo-layers` (map rendering), `wellknown` (WKT→GeoJSON parser) (005-geometry-perspective-support)
- TypeScript 5.x (extensions), Python 3.12 (server extensions), Go 1.26 (kernel — unchanged) + `@finos/perspective` 4.3.0, `@deck.gl/*` 9.2.11, `apache-arrow` 21.1.0, `@jupyterlab/*` 4.0.0, webpack 5 (JupyterLab), esbuild (VS Code) (006-split-perspective-extensions)
- Arrow IPC files on local filesystem (spool mechanism — unchanged) (006-split-perspective-extensions)
- Go 1.26.1 (kernel), TypeScript 5.x (JupyterLab extension) + go-zeromq/zmq4 v0.17.0 (ZMQ), @jupyterlab/services (new — for IComm), @jupyterlab/notebook ^4.0.0 (007-comm-introspect)
- N/A (in-memory introspection queries against DuckDB) (007-comm-introspect)

- Go 1.22+ (001-duckdb-kernel-mvp)

## Project Structure

```text
src/
tests/
```

## Commands

# Add commands for Go 1.22+

## Code Style

Go 1.22+: Follow standard conventions

## Recent Changes
- 007-comm-introspect: Added Go 1.26.1 (kernel), TypeScript 5.x (JupyterLab extension) + go-zeromq/zmq4 v0.17.0 (ZMQ), @jupyterlab/services (new — for IComm), @jupyterlab/notebook ^4.0.0
- 006-split-perspective-extensions: Added TypeScript 5.x (extensions), Python 3.12 (server extensions), Go 1.26 (kernel — unchanged) + `@finos/perspective` 4.3.0, `@deck.gl/*` 9.2.11, `apache-arrow` 21.1.0, `@jupyterlab/*` 4.0.0, webpack 5 (JupyterLab), esbuild (VS Code)
- 005-geometry-perspective-support: Added Go 1.22+ (kernel), TypeScript 5.x (extensions) + `github.com/paulmach/orb` (Go WKB/WKT), `@deck.gl/core` + `@deck.gl/layers` + `@deck.gl/geo-layers` (map rendering), `wellknown` (WKT→GeoJSON parser)


<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
