# Perspective Viewer Integration Guide

This document describes the protocol and requirements for Jupyter kernels that want to use the HUGR Perspective Viewer extension for rendering query results.

The viewer is distributed as:
- **JupyterLab extension**: `@hugr-lab/perspective-viewer`
- **VS Code extension**: `hugr-lab.hugr-result-renderer`

## Overview

The viewer renders structured query results (tables, JSON, errors) using [Perspective](https://perspective.finos.org/) with optional geographic map visualization via deck.gl.

The kernel sends results as `display_data` messages with MIME type `application/vnd.hugr.result+json`. Arrow data is served over a local HTTP server managed by the kernel.

```
Kernel                          Viewer (JupyterLab / VS Code)
  |                                      |
  |-- display_data (metadata JSON) ----->|
  |                                      |-- GET /arrow/stream?q=<id> -->|
  |<-- Arrow IPC chunks (streaming) -----|<-----------------------------|
  |                                      |-- render Perspective table ---|
```

## MIME Type

```
application/vnd.hugr.result+json
```

The viewer registers as a renderer for this MIME type. The kernel must emit `display_data` messages with this type.

## Metadata Format

### Full Multipart Response

```json
{
  "parts": [
    {
      "id": "query-uuid",
      "type": "arrow",
      "title": "Result",
      "arrow_url": "http://127.0.0.1:PORT/arrow/stream?q=query-uuid",
      "rows": 1000,
      "columns": [
        { "name": "id", "type": "int64" },
        { "name": "name", "type": "string" },
        { "name": "geom", "type": "binary" }
      ],
      "data_size_bytes": 65432,
      "geometry_columns": [
        { "name": "geom", "srid": 4326, "format": "WKB" }
      ],
      "tile_sources": [
        {
          "name": "OSM",
          "url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
          "type": "raster"
        }
      ]
    },
    {
      "id": "extensions",
      "type": "json",
      "title": "Extensions",
      "data": { "timing": { "planning": 12, "execution": 45 } }
    },
    {
      "id": "errors",
      "type": "error",
      "title": "Errors",
      "errors": [
        {
          "message": "Field 'foo' not found",
          "path": ["data", "users", "foo"],
          "extensions": { "code": "FIELD_NOT_FOUND" }
        }
      ]
    }
  ],
  "base_url": "http://127.0.0.1:PORT",
  "query_time_ms": 234,

  "query_id": "query-uuid",
  "arrow_url": "http://127.0.0.1:PORT/arrow/stream?q=query-uuid",
  "rows": 1000,
  "columns": [
    { "name": "id", "type": "int64" },
    { "name": "name", "type": "string" }
  ],
  "data_size_bytes": 65432,
  "geometry_columns": [
    { "name": "geom", "srid": 4326, "format": "WKB" }
  ],
  "tile_sources": [
    { "name": "OSM", "url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png", "type": "raster" }
  ]
}
```

### Field Reference

#### Top-level fields

| Field | Type | Required | Description |
|---|---|---|---|
| `parts` | `PartDef[]` | Yes | Array of result parts (arrow, json, error). Each renders on its own tab. |
| `base_url` | `string` | Yes | Base URL of the kernel's Arrow HTTP server (e.g., `http://127.0.0.1:PORT`). Used for URL rebuild on port change. |
| `query_time_ms` | `number` | No | Query execution time in milliseconds. Shown in status banner. |
| `query_id` | `string` | No | UUID of the first Arrow part. Backward-compatible flat field. |
| `arrow_url` | `string` | No | Arrow URL of the first Arrow part. Backward-compatible flat field. |
| `rows` | `number` | No | Row count of the first Arrow part. Backward-compatible flat field. |
| `columns` | `ColumnDef[]` | No | Columns of the first Arrow part. Backward-compatible flat field. |
| `data_size_bytes` | `number` | No | File size of the first Arrow part. Backward-compatible flat field. |
| `geometry_columns` | `GeometryColumnMeta[]` | No | Geometry columns of the first Arrow part. Backward-compatible flat field. |
| `tile_sources` | `TileSourceMeta[]` | No | Tile sources. Backward-compatible flat field. |
| `transfer_time_ms` | `number` | No | Data transfer time. Shown in status banner. |

> **Backward compatibility**: If `parts` is absent or empty, the viewer falls back to the flat top-level fields (`query_id`, `arrow_url`, `rows`, `columns`, etc.) and renders a single Arrow result.

#### PartDef

| Field | Type | Required | Description |
|---|---|---|---|
| `id` | `string` | Yes | Unique ID for this part (typically UUID or dotted path like `data.users`). |
| `type` | `string` | Yes | One of `"arrow"`, `"json"`, `"error"`. |
| `title` | `string` | Yes | Tab label shown in the viewer. |
| `arrow_url` | `string` | For `arrow` | URL to fetch Arrow IPC stream. |
| `rows` | `number` | No | Total row count. |
| `columns` | `ColumnDef[]` | No | Column name/type pairs. |
| `data_size_bytes` | `number` | No | Arrow IPC file size in bytes. |
| `geometry_columns` | `GeometryColumnMeta[]` | No | Geometry column metadata for map rendering. |
| `tile_sources` | `TileSourceMeta[]` | No | Basemap tile sources for map rendering. |
| `data` | `any` | For `json` | JSON data to render as collapsible tree + raw view. |
| `errors` | `ErrorDef[]` | For `error` | Array of error objects. |

#### ColumnDef

| Field | Type | Description |
|---|---|---|
| `name` | `string` | Column name. |
| `type` | `string` | Column type (e.g., `int64`, `string`, `float64`, `binary`, `timestamp`). |

#### GeometryColumnMeta

| Field | Type | Description |
|---|---|---|
| `name` | `string` | Column name containing geometry data. |
| `srid` | `number` | Spatial Reference ID (typically `4326` for WGS84). |
| `format` | `string` | Encoding format: `"WKB"`, `"GeoJSON"`, or `"H3Cell"`. |

When geometry columns are present, the viewer registers a "Geo Map" plugin in Perspective that renders the data on an interactive deck.gl map. The Arrow streaming endpoint supports a `geoarrow=1` parameter to convert WKB binary columns to native GeoArrow format for deck.gl rendering.

#### TileSourceMeta

| Field | Type | Description |
|---|---|---|
| `name` | `string` | Display name (e.g., `"OSM"`, `"Satellite"`). |
| `url` | `string` | Tile URL template with `{x}`, `{y}`, `{z}` placeholders. |
| `type` | `string` | Tile type: `"raster"`, `"vector"`, or `"tilejson"`. |
| `attribution` | `string` | Optional attribution text. |
| `min_zoom` | `number` | Optional minimum zoom level. |
| `max_zoom` | `number` | Optional maximum zoom level. |

#### ErrorDef

| Field | Type | Description |
|---|---|---|
| `message` | `string` | Error message. |
| `path` | `string[]` | Optional path to the field that caused the error. |
| `extensions` | `any` | Optional additional error metadata. |

## Arrow HTTP Server Requirements

The kernel must run a local HTTP server on `127.0.0.1` (random port) to serve Arrow data and support viewer features.

### Required Endpoints

#### `GET /arrow/stream`

Streaming Arrow IPC endpoint. Returns data as length-prefixed chunks.

**Parameters:**
| Param | Required | Description |
|---|---|---|
| `q` | Yes | Query ID (UUID). |
| `limit` | No | Maximum number of rows to return. |
| `geoarrow` | No | If `"1"`, convert WKB geometry columns to native GeoArrow format. |
| `columns` | No | Comma-separated list of columns to include (projection). |

**Wire format:**
```
[4-byte LE length][Arrow IPC stream bytes]  (repeated per batch)
[0x00, 0x00, 0x00, 0x00]                   (zero-length terminator)
```

Each chunk is a complete Arrow IPC stream (schema + one record batch + EOS marker). The viewer concatenates batches into a Perspective table incrementally.

**Response headers:**
```
Content-Type: application/octet-stream
Access-Control-Allow-Origin: *
```

#### `GET /arrow`

Direct file serving (optional, for small datasets).

**Parameters:**
| Param | Required | Description |
|---|---|---|
| `q` | Yes | Query ID (UUID). |

**Response headers:**
```
Content-Type: application/vnd.apache.arrow.stream
Access-Control-Allow-Origin: *
```

### Optional Endpoints (Spool Management)

These endpoints enable spool file cleanup and pin/unpin in JupyterLab:

| Endpoint | Method | Params | Description |
|---|---|---|---|
| `/spool/delete` | `DELETE` | `query_id` | Delete a spool file. |
| `/spool/pin` | `POST` | `query_id` | Pin result to persistent storage. |
| `/spool/unpin` | `POST` | `query_id` | Unpin result (move back to volatile). |
| `/spool/is_pinned` | `GET` | `query_id` | Check if result is pinned. |
| `/introspect` | `GET` | `type`, ... | Database introspection metadata (for sidebar). |

All `query_id` values must be valid UUIDs (`[0-9a-f]{8}-...-[0-9a-f]{12}`).

### CORS

All endpoints must include:
```
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, DELETE, OPTIONS
```

The `*` origin is required because:
- JupyterLab origin varies (`http://localhost:PORT`)
- VS Code webview origin is dynamic (`vscode-webview://<id>`)
- The server binds to `127.0.0.1` only, so it is not network-accessible.

### Kernel Info

The kernel should expose its Arrow HTTP server base URL via `kernel_info_reply`:

```json
{
  "content": {
    "implementation": "hugr-kernel",
    "hugr_base_url": "http://127.0.0.1:PORT"
  }
}
```

The viewer uses `hugr_base_url` from `kernel_info_reply` to discover the current base URL after page reload or kernel restart.

## Spool (Arrow IPC File Storage)

### Current format (duckdb-kernel v0.2+)

```
$TMPDIR/duckdb-kernel/
  {queryID}.arrow          # Flat directory, no session nesting
  {queryID}.arrow
  history_{sessionID}.json
```

- **TTL**: 24 hours (configurable via `DUCKDB_KERNEL_SPOOL_TTL`)
- **Max disk**: 2 GB (configurable via `DUCKDB_KERNEL_SPOOL_MAX_SIZE`)
- **Compression**: LZ4 (Arrow IPC native)
- **Cleanup**: On kernel start + after each query (TTL + size limit)
- Files survive kernel shutdown (no cleanup on exit)

### Persistent storage (Pin)

```
{notebook_dir}/duckdb-results/
  {queryID}.arrow          # Pinned results, no TTL
  .gitignore               # Auto-created: *.arrow
```

- Created by `POST /spool/pin` or `:save_results` meta command
- No TTL, no size limit
- Lookup order: persistent dir first, then volatile spool

### Legacy format (hugr-kernel v0.1)

```
$TMPDIR/hugr-kernel/{sessionID}/
  {queryID}.arrow          # Session-nested directories
```

- MaxFiles: 10
- MaxAge: 1 hour
- No compression
- No pin support

### Migration

Kernels upgrading to the new viewer should:
1. Switch to flat spool directory (no session nesting)
2. Increase TTL to 24 hours
3. Enable LZ4 compression (`ipc.WithLZ4()`)
4. Remove spool cleanup on kernel shutdown (files should survive for reload recovery)

## Arrow URL Generation

The kernel generates Arrow URLs using:

```go
func ArrowURL(queryID string, totalRows int64) string {
    const maxArrowRows = 5_000_000
    if totalRows > maxArrowRows {
        return fmt.Sprintf(
            "http://127.0.0.1:%d/arrow/stream?q=%s&limit=%d&total=%d",
            port, queryID, maxArrowRows, totalRows,
        )
    }
    return fmt.Sprintf("http://127.0.0.1:%d/arrow/stream?q=%s", port, queryID)
}
```

When `limit` and `total` are present, the viewer shows a truncation banner with a "Load all N rows" button.

## URL Resilience

After page reload or kernel restart, the kernel port changes. The viewer handles this by:

1. **`base_url` caching**: Each `display_data` includes `base_url`. The viewer caches the latest value.
2. **`kernel_info_reply`**: The viewer queries `hugr_base_url` from kernel info on reconnect.
3. **URL rebuilding**: `rebuildArrowUrl(oldUrl)` extracts the query ID and rebuilds the URL with the current base URL, preserving all query parameters.
4. **Retry with backoff**: On connection error, the viewer retries up to 3 times with 1-3 second delays.

## Multipart Rendering

When `parts` contains multiple entries, the viewer renders a tabbed interface:

- **Arrow parts** → Perspective datagrid + optional Geo Map plugin
- **JSON parts** → Collapsible tree view + raw JSON view
- **Error parts** → Styled error list with path and extensions

Tab icons indicate the part type. The first tab is active by default. Overflow tabs are accessible via a "more" dropdown menu.

## Geometry Map Plugin

When `geometry_columns` is present on an Arrow part, the viewer registers a "Geo Map" perspective plugin that:

1. Fetches Arrow data with `geoarrow=1` parameter (converts WKB → native GeoArrow)
2. Renders geometry on a deck.gl map
3. Supports tile basemaps from `tile_sources`
4. Supports WKB, GeoJSON, and H3Cell formats

### How the kernel should detect geometry columns

Scan Arrow schema for columns with:
- Binary type containing WKB data (check first value for WKB magic byte `0x01` or `0x00`)
- String type containing GeoJSON or WKT
- Extension type metadata (`ARROW:extension:name` = `geoarrow.wkb`, `geoarrow.point`, etc.)

Include detected geometry columns in `geometry_columns` array with appropriate `format` and `srid`.

### GeoArrow streaming endpoint

When the viewer requests `/arrow/stream?q=ID&geoarrow=1`:

- WKB binary columns should be converted to native GeoArrow struct arrays (Point, LineString, Polygon)
- This enables direct deck.gl rendering without client-side WKB parsing

When `geoarrow=1` is NOT set:

- WKB binary columns should be replaced with string `"{geometry}"` for Perspective compatibility (binary columns crash Perspective)

## Arrow Streaming Pipeline

The `/arrow/stream` endpoint applies a pipeline of transformations to Arrow record batches before sending them to the viewer. The pipeline is built from query parameters:

```
Spool file → IPC Reader → [Column Projection] → [GeoArrow Convert | String Replace] → IPC Stream → HTTP
```

### Column Projection (`columns` parameter)

When `columns=col1,col2,col3` is passed, only the listed columns are included in the output. This reduces data transfer for wide tables.

Implementation uses `pkg/geoarrow.NewProjector`:

```go
import "github.com/hugr-lab/duckdb-kernel/pkg/geoarrow"

// Create a projector that selects only requested columns
projCols := map[string]bool{"col1": true, "col2": true}
source := geoarrow.NewProjector(reader, projCols)
// source implements array.RecordReader, streams projected batches
```

`NewProjector` returns the original reader unchanged if no projection is needed (all columns requested or empty set).

### GeoArrow Conversion (`geoarrow=1` parameter)

When `geoarrow=1` is set, WKB binary geometry columns are converted to native GeoArrow struct arrays on-the-fly. This is required for deck.gl map rendering.

Implementation uses `pkg/geoarrow.NewConverter`:

```go
import "github.com/hugr-lab/duckdb-kernel/pkg/geoarrow"

// Auto-detect geometry columns and convert WKB → GeoArrow
converter := geoarrow.NewConverter(source,
    geoarrow.WithAllocator(memory.DefaultAllocator),
    geoarrow.WithContext(ctx),
)
defer converter.Release()
// converter implements array.RecordReader, streams converted batches
```

The converter:

- Auto-detects geometry columns via `DetectGeometryColumns(schema)` (checks `ARROW:extension:name` metadata)
- Converts WKB binary to native GeoArrow types (Point, LineString, Polygon, Multi*)
- Auto-detects geometry type from first batch (Point vs Polygon etc.)
- Runs conversion in a pipelined goroutine for better throughput

### String Replacement (default, no `geoarrow` parameter)

When `geoarrow=1` is NOT set, geometry columns are replaced with the string `"{geometry}"` to prevent Perspective from crashing on binary data.

Implementation uses `pkg/geoarrow.NewStringReplacer`:

```go
import "github.com/hugr-lab/duckdb-kernel/pkg/geoarrow"

replacer := geoarrow.NewStringReplacer(source)
defer replacer.Release()
// replacer implements array.RecordReader
```

### Full pipeline example

```go
// Build reader pipeline for /arrow/stream
var source array.RecordReader = ipcReader

// 1. Column projection (if requested)
if projCols != nil {
    source = geoarrow.NewProjector(source, projCols)
}

// 2. Geometry handling
if wantGeoArrow {
    // Convert WKB → native GeoArrow for deck.gl
    source = geoarrow.NewConverter(source,
        geoarrow.WithAllocator(mem),
        geoarrow.WithContext(ctx),
    )
} else {
    // Replace geometry with "{geometry}" string for Perspective
    source = geoarrow.NewStringReplacer(source)
}

// 3. Stream batches as length-prefixed chunks
for source.Next() {
    rec := source.RecordBatch()
    // serialize rec as [4-byte len][Arrow IPC bytes]
    // flush to HTTP response
}
```

### Geometry detection

The `geoarrow.DetectGeometryColumns` function scans an Arrow schema for columns with GeoArrow extension metadata:

```go
cols := geoarrow.DetectGeometryColumns(schema)
// Returns []GeometryColumn with Name, Index, SRID, Format, ExtensionMeta
```

It checks `ARROW:extension:name` field metadata for known GeoArrow types (`geoarrow.wkb`, `geoarrow.point`, `geoarrow.linestring`, `geoarrow.polygon`, etc.).

For kernels that produce WKB columns without GeoArrow metadata, set the extension metadata explicitly:

```go
field := arrow.Field{
    Name: "geom",
    Type: arrow.BinaryTypes.Binary,
    Metadata: arrow.MetadataFrom(map[string]string{
        "ARROW:extension:name":     "geoarrow.wkb",
        "ARROW:extension:metadata": `{"crs":{"type":"name","properties":{"name":"urn:ogc:def:crs:OGC:1.3:CRS84"}}}`,
    }),
}
```

### Package reference

The `pkg/geoarrow` package is part of the [duckdb-kernel](https://github.com/hugr-lab/duckdb-kernel) repository:

| Type | Description |
| --- | --- |
| `geoarrow.Converter` | Streaming WKB → GeoArrow converter (implements `array.RecordReader`) |
| `geoarrow.NewConverter(source, opts...)` | Create converter with auto-detection and pipelined processing |
| `geoarrow.Projector` | Streaming column projection (implements `array.RecordReader`) |
| `geoarrow.NewProjector(source, cols)` | Create projector; returns source unchanged if no projection needed |
| `geoarrow.StringReplacer` | Replace geometry columns with `"{geometry}"` string |
| `geoarrow.NewStringReplacer(source)` | Create replacer with auto-detection |
| `geoarrow.DetectGeometryColumns(schema)` | Find geometry columns by Arrow extension metadata |
| `geoarrow.ConvertBatch(rec, col, geoType, mem)` | Convert a single batch's WKB column to GeoArrow |
| `geoarrow.GeometryColumn` | Struct: Name, Index, SRID, Format, ExtensionMeta |
| `geoarrow.GeoType` | Enum: GeoTypeUnknown, GeoTypePoint, GeoTypeLine, GeoTypePolygon |

**Options for `NewConverter`:**

| Option | Description |
| --- | --- |
| `WithBufferSize(n)` | Parallel conversion buffer size (default 1) |
| `WithAllocator(mem)` | Memory allocator for Arrow arrays |
| `WithColumns(cols)` | Override auto-detected geometry columns |
| `WithContext(ctx)` | Context for pipeline cancellation |

## Example: Emitting Results from a Kernel

### Go (single Arrow result)

```go
// After executing query and writing Arrow IPC to spool:
metadata := map[string]any{
    "parts": []map[string]any{
        {
            "id":              queryID,
            "type":            "arrow",
            "title":           "Result",
            "arrow_url":       arrowServer.ArrowURL(queryID, totalRows),
            "rows":            totalRows,
            "columns":         columnDefs,
            "data_size_bytes": fileSize,
            "geometry_columns": geoCols,  // optional
            "tile_sources":     tiles,    // optional
        },
    },
    "base_url":      arrowServer.BaseURL(),
    "query_time_ms": elapsed.Milliseconds(),
    // Backward-compatible flat fields:
    "query_id":       queryID,
    "arrow_url":      arrowServer.ArrowURL(queryID, totalRows),
    "rows":           totalRows,
    "columns":        columnDefs,
    "data_size_bytes": fileSize,
}

// Send as display_data:
displayData := map[string]any{
    "data": map[string]any{
        "text/plain":                          textPreview,
        "application/vnd.hugr.result+json":    metadata,
    },
    "metadata":  map[string]any{},
    "transient": map[string]any{},
}
```

### Go (multipart with Arrow + JSON + errors)

```go
metadata := map[string]any{
    "parts": []map[string]any{
        {
            "id":    queryID,
            "type":  "arrow",
            "title": "data.users",
            "arrow_url": arrowServer.ArrowURL(queryID, rows),
            "rows":  rows,
            "columns": cols,
        },
        {
            "id":    "extensions",
            "type":  "json",
            "title": "Extensions",
            "data":  extensionsMap,
        },
        {
            "id":    "errors",
            "type":  "error",
            "title": "Errors (2)",
            "errors": []map[string]any{
                {"message": "Field not found", "path": []string{"data", "users", "foo"}},
                {"message": "Permission denied", "path": []string{"data", "admin"}},
            },
        },
    },
    "base_url":      arrowServer.BaseURL(),
    "query_time_ms": elapsed.Milliseconds(),
}
```
