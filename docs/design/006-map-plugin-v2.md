# 006 — Map Plugin v2: Rendering, Interactions & Architecture

## Status: Approved

## Context

The Geo Map plugin renders geometry from DuckDB via GeoArrow binary pipeline.
Works: points, lines, polygons on a basemap with basic color slot.
Missing: rich layer configuration, tooltips, legend, VS Code self-contained architecture.

---

## Decisions

| # | Decision | Rationale |
| --- | --- | --- |
| D1 | Custom settings panel in plugin shadow DOM (left side of map) | Perspective column style panel is hardcoded (number/date/string controls only). We need scale, palette, steps, range controls. |
| D2 | Perspective = slot UI only. No Group By / Split By / Where / Expressions. | `view.to_columns()` for 2M+ rows kills GeoArrow perf. Filter/compute in SQL. |
| D3 | Hide Group By, Split By, Order By, Where, NEW COLUMN via CSS injection | Only when Geo Map is active plugin. Reappears on switch to Datagrid/Charts. |
| D4 | Auto basemap by theme (light → Voyager, dark → Dark Matter) | Detect via `--plugin--background` CSS variable luminance. |
| D5 | Column projection on Arrow stream | `/arrow/stream?geoarrow=1&columns=geom,pop,size` — only needed columns. |
| D6 | Predefined palettes only (no custom hex editor) | Keep it simple. 8-10 sequential + 3 diverging + 1 qualitative. |
| D7 | Height in meters with auto-scale | Auto: `elevationScale = 50000 / dataMax`. Slider for manual override. Binary Float32Array attribute, no callback. |
| D11 | Opacity: global slider only, no per-feature slot | Per-feature alpha via identity color mode (rgba in SQL). |
| D12 | Color modes: quantize, quantile, log, category, identity, fixed | Precompute once on slot change → O(1) per row. Quantile: sort once. |
| D13 | Extrusion via binary attribute, not callback | Build Float32Array elevation once on slot change. Zero per-frame cost. |
| D8 | Settings panel state in plugin save/restore | Stored as `{ slotConfig, opacity, basemap }` in Perspective plugin state. |
| D9 | No standalone map component | Perspective gives slot UI free. Standalone = rewrite slot management. |
| D10 | VS Code extension bundles perspective assets | Kernel stops serving static files. Extension is self-contained. |

---

## 1. Layer Configuration

### Parameters per geometry type

#### Points (ScatterplotLayer / ColumnLayer)

| Parameter | Type | Default | Notes |
| --- | --- | --- | --- |
| Fill Color | fixed / field-based | `#4187dc` | Color picker or column |
| Color Scale | quantize / quantile / category / log | quantize | How to map values to colors |
| Color Palette | sequential / diverging / qualitative | Viridis | Predefined set |
| Color Steps | 3–12 | 6 | Number of color buckets |
| Opacity | 0–1 slider | 0.85 | Layer transparency |
| Outline | on/off | on | Stroke around points |
| Stroke Width | pixels | 1 | |
| Radius | fixed / field-based | 6 px | |
| Radius Scale | linear / sqrt / log | linear | |
| Radius Range | [min, max] pixels | [2, 40] | |
| Height | field-based (optional) | — | Enables ColumnLayer extrusion |
| Elevation Scale | multiplier slider | auto | |

#### Lines (PathLayer)

| Parameter | Type | Default | Notes |
| --- | --- | --- | --- |
| Color | fixed / field-based | `#4187dc` | |
| Color Scale | quantize / quantile / category / log | quantize | |
| Opacity | 0–1 | 0.85 | |
| Width | fixed / field-based | 2 px | |
| Width Scale | linear / sqrt / log | linear | |
| Width Range | [min, max] | [1, 10] | |

#### Polygons (SolidPolygonLayer)

| Parameter | Type | Default | Notes |
| --- | --- | --- | --- |
| Fill Color | fixed / field-based | `#4187dc` | |
| Color Scale | quantize / quantile / category / log | quantize | |
| Fill Opacity | 0–1 | 0.85 | |
| Outline | on/off | on | |
| Stroke Width | pixels | 1 | |
| Height | field-based (optional) | — | 3D extrusion |
| Elevation Scale | multiplier slider | auto | |

#### H3 (H3HexagonLayer)

| Parameter | Type | Default | Notes |
| --- | --- | --- | --- |
| Fill Color | fixed / field-based | `#ff8c00` | |
| Color Scale | quantize / quantile / category / log | quantize | |
| Opacity | 0–1 | 0.85 | |
| Coverage | 0–1 | 1 | |
| Height | field-based (optional) | — | 3D extrusion |

### Color modes

| Mode | Description | Use case | Computation |
| --- | --- | --- | --- |
| quantize | Equal-interval breaks (min-max / N) | Default for numeric | O(1) per row, precompute min/max |
| quantile | Equal-count breaks (rank-based) | Skewed distributions | O(N log N) sort once, then O(1) per row |
| log | Logarithmic intervals | Exponential data (population) | O(1) per row |
| category | Distinct color per unique value | String/boolean fields | Map lookup O(1) |
| identity | Raw CSS color values from field | `#hex`, `rgb()`, named | parseCssColor O(1) |
| fixed | Single color for all features | No color column assigned | Color picker in UI |

All modes precompute breakpoints/maps once on slot change → per-row render is O(1).
Quantile: sort N values once (~200ms for 2M rows), cache breakpoints array.

### Color palettes (predefined)

Sequential (6): Viridis, Plasma, Blues, Reds, YlOrRd, Greens
Diverging (2): RdBu, RdYlGn
Qualitative (1): Category10

Palettes stored as constant RGB arrays (~2KB total). No custom hex editor.

### Color steps

- Continuous mode: smooth gradient (current behavior) — default
- Discrete mode: 3–12 steps — N discrete colors from palette

### Opacity

Global slider only (0–1). No per-feature opacity slot.
If user needs per-feature alpha: use `identity` color mode with rgba() values in SQL column.

### Size/Radius scales

| Scale | Description |
| --- | --- |
| linear | Direct proportional mapping |
| sqrt | Square root (area-proportional for circles) |
| log | Logarithmic |
| identity | Raw pixel values from column |

### Height / Extrusion

When Height slot assigned:

- Points → switch to ColumnLayer with `extruded: true`
- Polygons → SolidPolygonLayer with `extruded: true`
- H3 → `extruded: true`
- Lines → no extrusion (deck.gl PathLayer doesn't support it)

**Binary attribute (no callback):** Build `Float32Array` elevation attribute
from property values × elevationScale. One O(N) pass on slot change.

```typescript
// Build elevation attribute
const elevArr = new Float32Array(numRows);
for (let i = 0; i < numRows; i++) {
  elevArr[i] = Number(heightValues[i]) * elevationScale;
}
// Pass as binary attribute
data: {
  length: numRows,
  startIndices,
  attributes: {
    getPolygon: { value: flatCoords, size: 2 },
    getElevation: { value: elevArr, size: 1 },
  }
}
```

**Height scale modes:**

| Mode | Description | Use case |
| --- | --- | --- |
| auto | `elevationScale = 50000 / dataMax` | Relative values (population, revenue) |
| identity | `elevationScale = 1`, raw meters | Real-world heights (building floors, terrain) |
| manual | User-set multiplier (0.1x – 100x) | Fine-tuning |

Default: `auto`. Toggle to `identity` when values are real meters (e.g. building height).
Manual slider available in both modes for fine-tuning.

**Auto camera:** On first draw with Height assigned: `pitch: 45`, `bearing: 15`.

---

## 2. Settings Panel UI

### Decision: Custom panel in plugin shadow DOM (D1)

Perspective's built-in column style panel (`perspective-number-column-style` etc.) is
hardcoded to number format / fg-bg gradient controls. Cannot add scale type, palette picker,
steps slider. We build our own.

### Layout

```
┌─── settings panel ──┬──── MAP ──────────────────┐
│ ☰ Layer Settings    │                            │
│                     │                            │
│ ▼ Color: population │                            │
│   Scale: [quantize▾]│                            │
│   Palette: [██████] │                            │
│   Steps: [6 ▾]      │                            │
│   Opacity: [──●──]  │                            │
│                     │                            │
│ ▼ Size: radius_km   │                            │
│   Scale: [sqrt ▾]   │                            │
│   Range: [2──●──40] │                            │
│                     │                            │
│ ▼ Height: elevation │                            │
│   Scale: [linear ▾] │                            │
│   Multiplier: [1x]  │                            │
│                     │                            │
│ ▼ Appearance        │          [Legend]           │
│   Basemap: [auto ▾] │                            │
│   3D Pitch: [45°]   │                            │
└─────────────────────┴────────────────────────────┘
```

- Toggle via ☰ button on map (top-left corner)
- Panel slides from left edge, overlays the map
- Sections are collapsible (▼/▶)
- Only shows sections for assigned slots
- Styled with Perspective CSS variables (auto-themes)

### State persistence (D8)

```typescript
interface MapPluginState {
  viewState: MapViewState;
  slotConfig: {
    color?: { scale: string; palette: string; steps: number; domain?: [number, number] };
    size?: { scale: string; range: [number, number] };
    height?: { scale: string; elevationScale: number };
  };
  opacity: number;
  basemap: 'auto' | 'voyager' | 'dark' | 'positron';
}
```

Stored via plugin `save()` / `restore()`.

---

## 3. Tooltips

### Hover tooltip

- Deck.gl `getTooltip({index})` on hover
- Binary data layers: `index` maps to merged `propColData` arrays
- Shows all non-geometry columns as key-value table
- If Tooltip slot assigned: that column as bold title
- Styled with `--warning--background` / `--warning--color` CSS variables

```
┌──────────────────────┐
│ Moscow               │  ← Tooltip slot value (bold)
│ ──────────────────── │
│ osm_id: 123456       │
│ population: 12M      │
│ admin_level: 4       │
└──────────────────────┘
```

### Click popup (phase 2)

- Click → persistent panel, stays until close
- All columns + geometry area/length
- Defer to later iteration

### Throttle

50ms debounce on tooltip updates.

---

## 4. Perspective Role (D2)

**Perspective = slot UI + plugin switching. Nothing else for map.**

Map plugin fetches data independently from `/arrow/stream?geoarrow=1`.
Users filter/compute in SQL queries.

### What we hide (D3)

When Geo Map is active plugin, inject CSS into viewer shadow DOM:

- Group By section → `display: none`
- Split By section → `display: none`
- Order By section → `display: none`
- Where/Filter section → `display: none`
- `fx NEW COLUMN` button → `display: none`

When switching to Datagrid/Charts → remove hiding CSS.

---

## 5. Scale & Palette Configuration (D6)

### Predefined palettes

Sequential: Viridis, Plasma, Inferno, Magma, Blues, Reds, YlOrRd, Greens

Diverging: RdBu, RdYlGn, BrBG

Qualitative: Category10 (current 10-color palette)

No custom hex editor — keep it simple.

### SlotConfig interface

```typescript
interface SlotConfig {
  colorScale: 'quantize' | 'quantile' | 'log' | 'category' | 'identity';
  colorPalette: string;
  colorSteps: number;
  colorDomain?: [number, number];
  sizeScale: 'linear' | 'sqrt' | 'log' | 'identity';
  sizeRange: [number, number];
  heightScale: 'linear' | 'log';
  elevationScale: number;
}
```

---

## 6. Legend

### Position

Bottom-left of map, inside plugin shadow DOM.

### Gradient legend (numeric color)

```
┌───────────────────────┐
│ population            │
│ ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓ │
│ 1,200      →  850,000 │
└───────────────────────┘
```

### Category legend (string color)

```
┌───────────────────────┐
│ category              │
│ ● Residential  (423)  │
│ ● Commercial   (156)  │
│ ● Industrial    (89)  │
└───────────────────────┘
```

### Implementation

- Built together with color accessor → returns `{ accessor, legendData }`
- `LegendData: { type, title, domain, palette, categories, counts }`
- Collapsible (click title to toggle)
- Styled with Perspective CSS variables

---

## 7. Auto Basemap by Theme (D4)

### Decision

Detect theme luminance from `--plugin--background` CSS variable:

```typescript
function isDarkTheme(): boolean {
  const bg = getComputedStyle(this.parentElement).getPropertyValue('--plugin--background').trim();
  if (!bg || bg === 'transparent') return false;
  // Parse hex → luminance
  const hex = bg.replace('#', '');
  const r = parseInt(hex.slice(0, 2), 16);
  const g = parseInt(hex.slice(2, 4), 16);
  const b = parseInt(hex.slice(4, 6), 16);
  return (r * 299 + g * 587 + b * 114) / 1000 < 128;
}
```

### Basemap URLs

| Mode | URL |
| --- | --- |
| auto (light) | `https://a.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}@2x.png` |
| auto (dark) | `https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png` |
| voyager | Voyager (forced light) |
| positron | `https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}@2x.png` |
| dark | Dark Matter (forced dark) |

Default: `auto`. Switchable in settings panel.

Re-evaluated on `restyle()` callback (theme change triggers restyle).

---

## 8. Column Projection (D5)

Map plugin sends only needed column names:

```
/arrow/stream?q=...&geoarrow=1&columns=geom,population,radius_km
```

Backend projects (selects) only requested columns from spool RecordBatch.
Geometry column always included.

---

## 9. Theming with CSS Variables

All UI elements use Perspective CSS custom properties:

| Element | Variables |
| --- | --- |
| Settings panel bg | `--map-element-background` / `--plugin--background` |
| Settings panel text | `--icon--color` |
| Settings panel border | `--inactive--border-color` |
| Active/selected | `--active--color` |
| Legend bg | `--map-element-background` |
| Tooltip bg | `--warning--background` |
| Tooltip text | `--warning--color` |

Custom properties cross shadow DOM boundaries → auto-themes work.

---

## 10. VS Code Self-Contained Extension (D10)

### Plan

1. Bundle perspective JS/WASM in `out/perspective/` of VS Code extension
2. Renderer loads from extension assets via `webview.asWebviewUri()`
3. Remove `/static/perspective/` handler from kernel `arrowhttp.go`
4. Backward compat: renderer falls back to kernel HTTP if local assets missing

---

## 11. Hide Unused Perspective UI (D3)

### Implementation

In `registerMapPlugin()`:
- Listen for `perspective-config-update` on viewers
- When active plugin = "Geo Map" → inject CSS hiding group/split/order/where/expressions
- When active plugin changes away → remove CSS

CSS selectors determined by inspecting viewer shadow DOM structure.

---

## Implementation Priority

| # | Task | Effort | Impact |
| --- | --- | --- | --- |
| 1 | Tooltips (hover) | S | High — core usability |
| 2 | Auto basemap by theme | XS | Medium — visual polish |
| 3 | Hide unused Perspective UI | S | Medium — cleaner UX |
| 4 | Extrusion (Height slot in GeoArrow path) | S | Medium — 3D capability |
| 5 | Legend (gradient + category) | M | High — data interpretation |
| 6 | Column projection | S | Medium — performance |
| 7 | Settings panel (scale, palette, steps) | L | High — configurability |
| 8 | Opacity slider | XS | Low — in settings panel |
| 9 | VS Code self-contained | M | Medium — distribution |
| 10 | Click popup | S | Low — can defer |
