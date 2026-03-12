
# Perspective Visualization Integration for DuckDB Kernel

SpecKit Extension

Repository:
https://github.com/hugr-lab/duckdb-kernel

Status:
Planned

---

# 1. Goal

Integrate Perspective as the primary interactive data viewer for results produced by the DuckDB Kernel.

Perspective will enable:

- interactive table visualization
- pivoting and aggregation
- filtering and sorting
- chart generation
- large dataset exploration

The integration must work in:

- JupyterLab
- VS Code notebooks
- JupyterHub environments

The visualization must operate directly on Arrow data produced by the kernel.

---

# 2. Design Principles

The Perspective integration must follow these principles:

1. Visualization must be frontend-driven.
2. The kernel must not embed heavy UI frameworks.
3. Results must remain Arrow-native.
4. Large datasets must be loaded lazily.
5. Rendering must be independent of query execution.

The kernel is responsible only for:

- executing queries
- generating Arrow result files
- returning metadata describing the result

---

# 3. Architecture

Perspective integration sits between result metadata and notebook output rendering.

Pipeline:

Query Execution
↓
Arrow Result File
↓
Result Metadata
↓
Perspective Viewer
↓
Interactive UI

The kernel does not render Perspective itself.

Instead it emits metadata that allows a frontend renderer to initialize Perspective.

---

# 4. Result Metadata Extension

The kernel returns visualization metadata using a custom MIME type:

application/vnd.hugr.result+json

Example output:

{
  "query_id": "q_123",
  "arrow_path": "/tmp/duckdb-kernel/default/q_123.arrow",
  "rows": 50000,
  "columns": [
    {"name":"id","type":"int"},
    {"name":"country","type":"varchar"},
    {"name":"revenue","type":"double"}
  ]
}

The frontend viewer uses this metadata to initialize Perspective.

---

# 5. Perspective Initialization

The viewer performs the following steps:

1. Load the Arrow file.
2. Create a Perspective table from Arrow.
3. Attach the table to a Perspective viewer component.
4. Render the viewer in the notebook output.

Flow:

Arrow file
↓
Perspective Table
↓
Perspective Viewer
↓
Interactive UI

---

# 6. Default Viewer Configuration

Default configuration:

view: table
row pivot: none
column pivot: none
sort: none

Users may later configure:

- pivots
- filters
- chart types
- aggregations

---

# 7. Supported Visualization Types

Initial supported views:

Table
Pivot Table
Charts

Chart types:

- bar
- line
- scatter

Charts are generated from pivoted data.

---

# 8. Large Dataset Handling

Large datasets must not be transmitted through the Jupyter protocol.

Instead:

- Arrow file is stored locally
- Perspective reads Arrow data directly
- Notebook output contains only metadata

Preview rows shown in plain text remain limited.

Default preview: 100 rows.

---

# 9. Data Type Support

Supported types:

Scalar:
- integer
- float
- boolean
- string
- timestamp

Complex:
- arrays
- maps
- structs

Nested types may be flattened for visualization.

---

# 10. Notebook Rendering

Perspective viewers are rendered inside notebook outputs as an interactive component.

Users can:

- filter
- sort
- pivot
- switch chart types

Works in:

- JupyterLab
- VS Code notebooks

---

# 11. Lazy Loading

Perspective supports:

- chunked loading
- virtualized tables
- incremental aggregation

This prevents UI blocking for large datasets.

---

# 12. Interaction with Kernel

Perspective does not execute queries.

All queries are executed by the kernel.

Perspective only visualizes Arrow data returned by the kernel.

---

# 13. Future Extensions

Possible future enhancements:

- streaming updates
- Hugr GraphQL kernel integration
- deck.gl geospatial rendering
- export visualization state
- embedding viewers in generated applications

---

# 14. Definition of Done

Feature is complete when:

- kernel emits result metadata
- Arrow result files are produced
- notebook output initializes Perspective
- users can interactively explore results
- pivoting and charts work
- large datasets render without blocking the UI
