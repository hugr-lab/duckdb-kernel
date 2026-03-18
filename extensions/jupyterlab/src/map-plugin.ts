/**
 * Perspective Map Plugin — renders geometry data on an interactive Deck.gl map.
 *
 * Pipeline:
 * 1. Fetches raw Arrow IPC from the kernel (bypassing Perspective to preserve WKB binary)
 * 2. Parses WKB binary → coordinate arrays
 * 3. Renders via Deck.gl layers (ScatterplotLayer, PathLayer, SolidPolygonLayer)
 * 4. H3 cells rendered via H3HexagonLayer
 *
 * Works offline — all assets bundled, no CDN/internet required.
 */

import { Deck } from '@deck.gl/core';
import { ScatterplotLayer, SolidPolygonLayer, PathLayer, BitmapLayer, ColumnLayer } from '@deck.gl/layers';
import { H3HexagonLayer, TileLayer } from '@deck.gl/geo-layers';

/** Geometry column metadata passed from the renderer. */
interface GeometryColumnMeta {
  name: string;
  srid: number;
  format: string; // "WKB" | "GeoJSON" | "H3Cell"
}

/** Tile source configuration for basemaps. */
interface TileSourceMeta {
  name: string;
  url: string;
  type: string;
  attribution?: string;
  min_zoom?: number;
  max_zoom?: number;
}

// WKB geometry type constants
const WKB_POINT = 1;
const WKB_LINESTRING = 2;
const WKB_POLYGON = 3;
const WKB_MULTIPOINT = 4;
const WKB_MULTILINESTRING = 5;
const WKB_MULTIPOLYGON = 6;

interface ParsedGeometry {
  type: number; // WKB type constant
  coordinates: number[][]; // For points: [[lon,lat]], for lines: [[lon,lat],...], for polygons: rings
}

interface BBox {
  minLon: number;
  maxLon: number;
  minLat: number;
  maxLat: number;
}

interface MapViewState {
  longitude: number;
  latitude: number;
  zoom: number;
  pitch: number;
  bearing: number;
}

/** Parse a single WKB geometry into coordinates. */
function parseWKB(buffer: Uint8Array): ParsedGeometry | null {
  if (!buffer || buffer.length < 5) return null;

  const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
  const le = buffer[0] === 1; // byte order: 1=little-endian, 0=big-endian
  let offset = 1;

  const getUint32 = () => {
    const v = le ? view.getUint32(offset, true) : view.getUint32(offset, false);
    offset += 4;
    return v;
  };
  const getFloat64 = () => {
    const v = le ? view.getFloat64(offset, true) : view.getFloat64(offset, false);
    offset += 8;
    return v;
  };

  let geomType = getUint32();
  // Strip SRID flag and Z/M flags (high bits)
  const hasZ = (geomType & 0x80000000) !== 0 || (geomType & 0xFFFF) >= 1001;
  if (geomType > 1000000) geomType = geomType % 1000; // ISO WKB Z/M
  else if (geomType > 1000) geomType = geomType - 1000;
  geomType = geomType & 0xFF;

  // If SRID flag is set, skip 4 bytes
  if ((view.getUint32(1, le) & 0x20000000) !== 0) {
    offset += 4;
  }

  const coordDim = hasZ ? 3 : 2;

  const readCoord = (): [number, number] => {
    const x = getFloat64();
    const y = getFloat64();
    if (hasZ) getFloat64(); // skip Z
    return [x, y];
  };

  const readRing = (): number[][] => {
    const n = getUint32();
    const coords: number[][] = [];
    for (let i = 0; i < n; i++) coords.push(readCoord());
    return coords;
  };

  switch (geomType) {
    case WKB_POINT:
      return { type: WKB_POINT, coordinates: [readCoord()] };

    case WKB_LINESTRING:
      return { type: WKB_LINESTRING, coordinates: readRing() };

    case WKB_POLYGON: {
      const numRings = getUint32();
      const rings: number[][] = [];
      for (let i = 0; i < numRings; i++) {
        rings.push(...readRing());
      }
      return { type: WKB_POLYGON, coordinates: rings.length > 0 ? rings : [] };
    }

    case WKB_MULTIPOINT: {
      const n = getUint32();
      const coords: number[][] = [];
      for (let i = 0; i < n; i++) {
        // Each sub-geometry has its own WKB header
        const sub = parseWKB(buffer.slice(offset));
        if (sub) coords.push(...sub.coordinates);
        offset += 5 + coordDim * 8; // header + point
      }
      return { type: WKB_POINT, coordinates: coords };
    }

    case WKB_MULTILINESTRING: {
      const n = getUint32();
      // Return as separate linestrings (flatten)
      const allCoords: number[][] = [];
      for (let i = 0; i < n; i++) {
        const sub = parseWKB(buffer.slice(offset));
        if (sub) allCoords.push(...sub.coordinates);
        // Can't easily skip without parsing, so this is approximate
        // In practice, we'll handle this at a higher level
      }
      return { type: WKB_LINESTRING, coordinates: allCoords };
    }

    case WKB_MULTIPOLYGON: {
      const n = getUint32();
      const allCoords: number[][] = [];
      for (let i = 0; i < n; i++) {
        const sub = parseWKB(buffer.slice(offset));
        if (sub) allCoords.push(...sub.coordinates);
      }
      return { type: WKB_POLYGON, coordinates: allCoords };
    }

    default:
      return null;
  }
}

/** Row data for deck.gl rendering. */
interface FeatureRow {
  position?: [number, number]; // for points
  path?: number[][]; // for linestrings
  polygon?: number[][]; // for polygons (ring coordinates)
  h3Index?: string; // for H3 cells
  properties: Record<string, any>; // non-geometry column values
}

/** Fetch length-prefixed Arrow IPC chunks from the kernel.
 *  Each chunk is a complete IPC message: [4-byte LE length][Arrow IPC bytes].
 *  Returns individual chunks (not concatenated), since each is a standalone IPC stream. */
async function fetchArrowChunks(url: string): Promise<Uint8Array[]> {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Failed to fetch Arrow data (HTTP ${response.status})`);
  if (!response.body) throw new Error('Response body is null');

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let buffer = new Uint8Array(0);

  const concatBuf = (a: Uint8Array, b: Uint8Array): Uint8Array => {
    const result = new Uint8Array(a.length + b.length);
    result.set(a);
    result.set(b, a.length);
    return result;
  };

  try {
    while (true) {
      while (buffer.length < 4) {
        const { done, value } = await reader.read();
        if (done) return chunks;
        buffer = concatBuf(buffer, value);
      }

      const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
      const chunkLen = view.getUint32(0, true);
      if (chunkLen === 0) break;

      while (buffer.length < 4 + chunkLen) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer = concatBuf(buffer, value);
      }

      if (buffer.length < 4 + chunkLen) break;

      chunks.push(buffer.slice(4, 4 + chunkLen));
      buffer = buffer.slice(4 + chunkLen);
    }
  } finally {
    reader.releaseLock();
  }

  return chunks;
}

/** Extract features from a single Arrow IPC chunk. */
function extractFeaturesFromTable(
  table: any,
  geomColumnName: string,
  geomFormat: string,
  features: FeatureRow[],
  bbox: BBox,
): number {
  let detectedType = 0;

  const geomColIndex = table.schema.fields.findIndex((f: any) => f.name === geomColumnName);
  if (geomColIndex === -1) return 0;

  const geomColumn = table.getChildAt(geomColIndex);
  if (!geomColumn) return 0;

  const propFields = table.schema.fields
    .map((f: any, i: number) => ({ name: f.name, index: i }))
    .filter((f: any) => f.name !== geomColumnName);

  for (let rowIdx = 0; rowIdx < table.numRows; rowIdx++) {
    const geomValue = geomColumn.get(rowIdx);
    if (geomValue === null || geomValue === undefined) continue;

    const properties: Record<string, any> = {};
    for (const pf of propFields) {
      const col = table.getChildAt(pf.index);
      if (col) properties[pf.name] = col.get(rowIdx);
    }

    if (geomFormat === 'H3Cell') {
      const h3Index = typeof geomValue === 'string' ? geomValue : String(geomValue);
      features.push({ h3Index, properties });
      continue;
    }

    // GeoJSON text format
    if (geomFormat === 'GeoJSON' && typeof geomValue === 'string') {
      try {
        const gj = JSON.parse(geomValue);
        const feat: FeatureRow = { properties };
        if (gj.type === 'Point') {
          feat.position = gj.coordinates as [number, number];
          if (detectedType === 0) detectedType = WKB_POINT;
        } else if (gj.type === 'LineString' || gj.type === 'MultiLineString') {
          feat.path = gj.type === 'MultiLineString' ? gj.coordinates.flat() : gj.coordinates;
          if (detectedType === 0) detectedType = WKB_LINESTRING;
        } else if (gj.type === 'Polygon' || gj.type === 'MultiPolygon') {
          feat.polygon = gj.type === 'MultiPolygon' ? gj.coordinates.flat(2) : gj.coordinates.flat();
          if (detectedType === 0) detectedType = WKB_POLYGON;
        }
        if (feat.position || feat.path || feat.polygon) {
          // Update bbox
          const coords = feat.position ? [feat.position] : (feat.path || feat.polygon || []);
          for (const c of coords) {
            if (c[0] < bbox.minLon) bbox.minLon = c[0];
            if (c[0] > bbox.maxLon) bbox.maxLon = c[0];
            if (c[1] < bbox.minLat) bbox.minLat = c[1];
            if (c[1] > bbox.maxLat) bbox.maxLat = c[1];
          }
          features.push(feat);
        }
      } catch { /* skip invalid GeoJSON */ }
      continue;
    }

    let wkbBytes: Uint8Array;
    if (geomValue instanceof Uint8Array) {
      wkbBytes = geomValue;
    } else if (ArrayBuffer.isView(geomValue)) {
      wkbBytes = new Uint8Array(geomValue.buffer, geomValue.byteOffset, geomValue.byteLength);
    } else if (typeof geomValue === 'object' && geomValue !== null) {
      const len = geomValue.length || geomValue.byteLength;
      if (len > 0) {
        wkbBytes = new Uint8Array(len);
        for (let i = 0; i < len; i++) wkbBytes[i] = geomValue[i];
      } else {
        continue;
      }
    } else {
      continue;
    }

    const parsed = parseWKB(wkbBytes);
    if (!parsed) continue;

    if (detectedType === 0) detectedType = parsed.type;

    for (const coord of parsed.coordinates) {
      if (coord[0] < bbox.minLon) bbox.minLon = coord[0];
      if (coord[0] > bbox.maxLon) bbox.maxLon = coord[0];
      if (coord[1] < bbox.minLat) bbox.minLat = coord[1];
      if (coord[1] > bbox.maxLat) bbox.maxLat = coord[1];
    }

    const row: FeatureRow = { properties };
    switch (parsed.type) {
      case WKB_POINT:
        row.position = parsed.coordinates[0] as [number, number];
        break;
      case WKB_LINESTRING:
        row.path = parsed.coordinates;
        break;
      case WKB_POLYGON:
        row.polygon = parsed.coordinates;
        break;
    }
    features.push(row);
  }

  return detectedType;
}

/** Parse Arrow IPC chunks and extract features. Each chunk is a standalone IPC message. */
async function parseArrowToFeatures(
  url: string,
  geomColumnName: string,
  geomFormat: string,
): Promise<{ features: FeatureRow[]; geomType: number; bbox: BBox }> {
  const arrow = await import('apache-arrow');
  const chunks = await fetchArrowChunks(url);

  const features: FeatureRow[] = [];
  const bbox: BBox = { minLon: Infinity, maxLon: -Infinity, minLat: Infinity, maxLat: -Infinity };
  let detectedType = 0;

  for (const chunk of chunks) {
    try {
      const table = arrow.tableFromIPC(chunk);
      if (table.numRows === 0) continue;
      const t = extractFeaturesFromTable(table, geomColumnName, geomFormat, features, bbox);
      if (t !== 0 && detectedType === 0) detectedType = t;
    } catch {
      // Skip chunks that fail to parse (e.g. schema-only messages)
    }
  }

  return { features, geomType: detectedType, bbox };
}

// ────────────────────── GeoArrow binary path ──────────────────────
// When the kernel serves GeoArrow-encoded data (?geoarrow=1),
// we extract typed arrays directly from Arrow columns and pass them
// to deck.gl layers via binary data format — zero JS objects, zero-copy to GPU.

/** GeoArrow extension type names for detection. */
const GA_EXT_NAMES = new Set([
  'geoarrow.point', 'geoarrow.linestring', 'geoarrow.polygon',
  'geoarrow.multipoint', 'geoarrow.multilinestring', 'geoarrow.multipolygon',
]);

/** Result from GeoArrow data loading. */
interface GeoArrowData {
  extensionName: string;
  flatCoords: Float64Array;
  startIndices: Uint32Array;
  numRows: number;
  bbox: BBox;
  /** Merged table data for property access */
  table: { schema: any; numRows: number; propColData: Map<number, any[]>; geomColIdx: number };
  geomColIdx: number;
}

/** Extract coordinates from a single GeoArrow Data chunk into arrays. */
function extractCoordsFromData(
  data0: any,
  extensionName: string,
  batchRows: number,
  allCoords: number[],
  allStartIndices: number[],
  rowOffset: number,
  bbox: BBox,
): void {
  // Navigate nested structure to reach flat Float64 coordinates.
  // coordBase = current position in allCoords (in coord pairs, not float64s)
  const coordBase = allCoords.length / 2;

  if (extensionName === 'geoarrow.multipoint') {
    // List<FixedSizeList<Float64>[2]>
    const offsets = data0.valueOffsets as Int32Array;
    const values = data0.children[0].children[0].values as Float64Array;
    for (let i = 0; i < batchRows; i++) {
      allStartIndices.push(coordBase + offsets[i]);
    }
    for (let i = 0; i < values.length; i += 2) {
      allCoords.push(values[i], values[i + 1]);
      if (values[i] < bbox.minLon) bbox.minLon = values[i];
      if (values[i] > bbox.maxLon) bbox.maxLon = values[i];
      if (values[i + 1] < bbox.minLat) bbox.minLat = values[i + 1];
      if (values[i + 1] > bbox.maxLat) bbox.maxLat = values[i + 1];
    }
  } else if (extensionName === 'geoarrow.multilinestring' || extensionName === 'geoarrow.polygon') {
    // List<List<FixedSizeList<Float64>[2]>>
    const outerOffsets = data0.valueOffsets as Int32Array;
    const innerData = data0.children[0];
    const innerOffsets = innerData.valueOffsets as Int32Array;
    const values = innerData.children[0].children[0].values as Float64Array;
    for (let i = 0; i < batchRows; i++) {
      const partIdx = outerOffsets[i];
      const coordIdx = partIdx < innerOffsets.length ? innerOffsets[partIdx] : 0;
      allStartIndices.push(coordBase + coordIdx);
    }
    for (let i = 0; i < values.length; i += 2) {
      allCoords.push(values[i], values[i + 1]);
      if (values[i] < bbox.minLon) bbox.minLon = values[i];
      if (values[i] > bbox.maxLon) bbox.maxLon = values[i];
      if (values[i + 1] < bbox.minLat) bbox.minLat = values[i + 1];
      if (values[i + 1] > bbox.maxLat) bbox.maxLat = values[i + 1];
    }
  } else if (extensionName === 'geoarrow.multipolygon') {
    // List<List<List<FixedSizeList<Float64>[2]>>>
    const geomOffsets = data0.valueOffsets as Int32Array;
    const polyData = data0.children[0];
    const polyOffsets = polyData.valueOffsets as Int32Array;
    const ringData = polyData.children[0];
    const ringOffsets = ringData.valueOffsets as Int32Array;
    const values = ringData.children[0].children[0].values as Float64Array;
    for (let i = 0; i < batchRows; i++) {
      const polyIdx = geomOffsets[i];
      const ringIdx = polyIdx < polyOffsets.length ? polyOffsets[polyIdx] : 0;
      const coordIdx = ringIdx < ringOffsets.length ? ringOffsets[ringIdx] : 0;
      allStartIndices.push(coordBase + coordIdx);
    }
    for (let i = 0; i < values.length; i += 2) {
      allCoords.push(values[i], values[i + 1]);
      if (values[i] < bbox.minLon) bbox.minLon = values[i];
      if (values[i] > bbox.maxLon) bbox.maxLon = values[i];
      if (values[i + 1] < bbox.minLat) bbox.minLat = values[i + 1];
      if (values[i + 1] > bbox.maxLat) bbox.maxLat = values[i + 1];
    }
  }
}

/** Load Arrow IPC with GeoArrow geometry columns.
 *  Extracts typed arrays from nested Arrow structure for binary deck.gl rendering. */
async function loadGeoArrowData(
  url: string,
  geomColumnName: string,
): Promise<GeoArrowData | null> {
  const arrow = await import('apache-arrow');
  const chunks = await fetchArrowChunks(url);

  // Parse chunks into tables
  const tables: any[] = [];
  for (const chunk of chunks) {
    try {
      const t = arrow.tableFromIPC(chunk);
      if (t.numRows > 0) tables.push(t);
    } catch { /* skip */ }
  }
  if (tables.length === 0) return null;

  // Detect extension type from first table
  const schema = tables[0].schema;
  const geomColIdx = schema.fields.findIndex((f: any) => f.name === geomColumnName);
  if (geomColIdx === -1) return null;

  const field = schema.fields[geomColIdx];
  const extensionName = field.metadata?.get('ARROW:extension:name') || null;
  if (!extensionName || !GA_EXT_NAMES.has(extensionName)) return null;

  // Collect coordinates and offsets from ALL batches.
  // Each batch has its own Data structure; we merge them into single typed arrays.
  const allCoords: number[] = [];
  const allStartIndices: number[] = [];
  let totalRows = 0;
  const bbox: BBox = { minLon: Infinity, maxLon: -Infinity, minLat: Infinity, maxLat: -Infinity };

  // Also collect property columns for accessors
  const propColData: Map<number, any[]> = new Map(); // colIdx → values[]
  for (let ci = 0; ci < schema.fields.length; ci++) {
    if (ci !== geomColIdx) propColData.set(ci, []);
  }

  for (const t of tables) {
    for (const batch of t.batches) {
      const geomCol = batch.getChildAt(geomColIdx);
      if (!geomCol) continue;
      const data0 = geomCol.data[0];
      const batchRows = batch.numRows;

      // Extract coords from this batch's geometry data
      extractCoordsFromData(data0, extensionName, batchRows, allCoords, allStartIndices, totalRows, bbox);

      // Collect property values
      for (const [ci, values] of propColData) {
        const col = batch.getChildAt(ci);
        for (let r = 0; r < batchRows; r++) {
          values.push(col ? col.get(r) : null);
        }
      }

      totalRows += batchRows;
    }
  }

  if (totalRows === 0) return null;

  const flatCoords = new Float64Array(allCoords);
  const startIndices = new Uint32Array(allStartIndices);

  // Build a merged table for property access (store as simple object)
  const mergedTable = { schema, numRows: totalRows, propColData, geomColIdx };

  return { extensionName, flatCoords, startIndices, numRows: totalRows, bbox, table: mergedTable, geomColIdx };
}

/** Build Float32Array elevation from height column values with auto-scale.
 *  Returns null if no height column. */
function buildElevationAttribute(
  geoData: GeoArrowData,
  heightColName: string | null,
): { elevationArr: Float32Array; elevationScale: number } | null {
  if (!heightColName) return null;
  const colIdx = geoData.table.schema.fields.findIndex((f: any) => f.name === heightColName);
  if (colIdx === -1) return null;
  const values = geoData.table.propColData?.get(colIdx);
  if (!values) return null;

  const elevationArr = new Float32Array(geoData.numRows);
  let dataMax = 0;
  for (let i = 0; i < geoData.numRows; i++) {
    const v = Number(values[i]);
    elevationArr[i] = isFinite(v) ? Math.max(0, v) : 0;
    if (elevationArr[i] > dataMax) dataMax = elevationArr[i];
  }

  // Auto-scale: target max visual height ~500m for sensible 3D at city zoom
  const elevationScale = dataMax > 0 ? 500 / dataMax : 1;
  return { elevationArr, elevationScale };
}

/** Build deck.gl layers from GeoArrow binary data — zero JS objects. */
function buildGeoArrowLayers(
  geoData: GeoArrowData,
  colorAcc: any,
  sizeAcc: any,
  heightColName: string | null,
): any[] {
  const layers: any[] = [];
  const ext = geoData.extensionName;
  const isPoint = ext === 'geoarrow.point' || ext === 'geoarrow.multipoint';
  const isLine = ext === 'geoarrow.linestring' || ext === 'geoarrow.multilinestring';
  const isPolygon = ext === 'geoarrow.polygon' || ext === 'geoarrow.multipolygon';

  const elev = buildElevationAttribute(geoData, heightColName);

  if (isPoint) {
    if (elev) {
      // Extruded columns for points with height — binary elevation attribute
      layers.push(new ColumnLayer({
        id: 'hugr-map-point-columns',
        data: {
          length: geoData.numRows,
          attributes: {
            getPosition: { value: geoData.flatCoords, size: 2 },
            getElevation: { value: elev.elevationArr, size: 1 },
          },
        },
        getFillColor: colorAcc || currentDefaultColor,
        getLineColor: STROKE_COLOR,
        diskResolution: 20,
        radius: 30,
        elevationScale: elev.elevationScale,
        extruded: true,
        filled: true,
        flatShading: true,
        opacity: currentOpacity,
        pickable: true,
      }));
    } else {
      layers.push(new ScatterplotLayer({
        id: 'hugr-map-points',
        data: {
          length: geoData.numRows,
          attributes: {
            getPosition: { value: geoData.flatCoords, size: 2 },
          },
        },
        getFillColor: colorAcc || currentDefaultColor,
        getLineColor: STROKE_COLOR,
        getRadius: sizeAcc || currentDefaultSize,
        radiusUnits: 'pixels' as any,
        radiusMinPixels: 2,
        stroked: true,
        lineWidthMinPixels: 1,
        opacity: currentOpacity,
        pickable: true,
      }));
    }
  }

  if (isLine) {
    layers.push(new PathLayer({
      id: 'hugr-map-lines',
      data: {
        length: geoData.numRows,
        startIndices: geoData.startIndices,
        attributes: {
          getPath: { value: geoData.flatCoords, size: 2 },
        },
      },
      _pathType: 'open',
      getColor: colorAcc || currentDefaultColor,
      getWidth: sizeAcc || currentDefaultSize,
      widthUnits: 'pixels' as any,
      widthMinPixels: 1,
      opacity: currentOpacity,
      pickable: true,
    } as any));
  }

  if (isPolygon) {
    layers.push(new SolidPolygonLayer({
      id: 'hugr-map-polygons',
      data: {
        length: geoData.numRows,
        startIndices: geoData.startIndices,
        attributes: {
          getPolygon: { value: geoData.flatCoords, size: 2 },
        },
      },
      _normalize: false,
      getFillColor: colorAcc || currentDefaultColor,
      getLineColor: STROKE_COLOR,
      opacity: currentOpacity,
      pickable: true,
      filled: true,
    } as any));
  }

  return layers;
}

/** Color scale mode. */
type ColorScaleMode = 'quantize' | 'quantile' | 'log' | 'category' | 'identity';

/** Current color scale mode — configurable via settings panel. */
let currentColorScale: ColorScaleMode = 'quantize';

/** Current layer opacity — configurable via settings panel. */
let currentOpacity = 0.85;

/** Build a color accessor for binary data format (index-based).
 *  Supports multiple scale modes: quantize, quantile, log, category, identity. */
function buildBinaryColorAccessor(
  table: any, colName: string, colType: string, geomColIdx: number,
): ((obj: any, info: { index: number }) => [number, number, number, number]) | null {
  if (!colName) return null;
  const colIdx = table.schema.fields.findIndex((f: any) => f.name === colName);
  if (colIdx === -1) return null;
  const values: any[] = table.propColData?.get(colIdx);
  if (!values || values.length === 0) return null;

  // Auto-detect scale: string → category, number → currentColorScale
  const mode: ColorScaleMode = (colType === 'string' || colType === 'boolean')
    ? 'category' : currentColorScale;

  switch (mode) {
    case 'category': {
      const valMap = new Map<string, number>();
      for (const v of values) {
        const s = String(v ?? '');
        if (!valMap.has(s)) valMap.set(s, valMap.size);
      }
      return (_obj: any, info: { index: number }) => {
        const s = String(values[info.index] ?? '');
        const idx = valMap.get(s) ?? 0;
        return CATEGORY_COLORS[idx % CATEGORY_COLORS.length];
      };
    }

    case 'identity': {
      // CSS color values from field
      return (_obj: any, info: { index: number }) => {
        const c = parseCssColor(String(values[info.index] ?? ''));
        return c || FILL_COLOR;
      };
    }

    case 'quantile': {
      // Sort values to get quantile breaks
      const nums = values.map(Number).filter(isFinite).sort((a, b) => a - b);
      if (nums.length === 0) return null;
      return (_obj: any, info: { index: number }) => {
        const v = Number(values[info.index]);
        if (!isFinite(v)) return FILL_COLOR;
        // Binary search for quantile position
        let lo = 0, hi = nums.length - 1;
        while (lo < hi) { const mid = (lo + hi) >> 1; nums[mid] < v ? lo = mid + 1 : hi = mid; }
        return paletteColor(lo / (nums.length - 1 || 1));
      };
    }

    case 'log': {
      // Logarithmic scale
      let min = Infinity, max = -Infinity;
      for (const v of values) {
        const n = Number(v);
        if (isFinite(n) && n > 0) { if (n < min) min = n; if (n > max) max = n; }
      }
      if (!isFinite(min)) return null;
      const logMin = Math.log(min), logRange = Math.log(max) - logMin || 1;
      return (_obj: any, info: { index: number }) => {
        const v = Number(values[info.index]);
        if (!isFinite(v) || v <= 0) return FILL_COLOR;
        return paletteColor((Math.log(v) - logMin) / logRange);
      };
    }

    case 'quantize':
    default: {
      // Equal-interval (linear)
      let min = Infinity, max = -Infinity;
      for (const v of values) {
        const n = Number(v);
        if (isFinite(n)) { if (n < min) min = n; if (n > max) max = n; }
      }
      if (!isFinite(min)) return null;
      const range = max - min || 1;
      return (_obj: any, info: { index: number }) => {
        const v = Number(values[info.index]);
        return isFinite(v) ? paletteColor((v - min) / range) : FILL_COLOR;
      };
    }
  }
}

/** Size scale mode. */
type SizeScaleMode = 'linear' | 'sqrt' | 'log' | 'identity';
let currentSizeScale: SizeScaleMode = 'linear';
let currentSizeRange: [number, number] = [2, 30];
let currentDefaultSize = 6;
let currentDefaultColor: [number, number, number, number] = [65, 135, 220, 180];

/** Build a float accessor for binary data (index-based) with scale. */
function buildBinaryFloatAccessor(
  table: any, colName: string,
): ((obj: any, info: { index: number }) => number) | null {
  if (!colName) return null;
  const colIdx = table.schema.fields.findIndex((f: any) => f.name === colName);
  if (colIdx === -1) return null;
  const values: any[] = table.propColData?.get(colIdx);
  if (!values || values.length === 0) return null;

  if (currentSizeScale === 'identity') {
    return (_obj: any, info: { index: number }) => {
      const v = Number(values[info.index]);
      return isFinite(v) ? Math.max(0, v) : 0;
    };
  }

  // Compute data range
  let dataMin = Infinity, dataMax = -Infinity;
  for (const v of values) {
    const n = Number(v);
    if (isFinite(n) && n > 0) { if (n < dataMin) dataMin = n; if (n > dataMax) dataMax = n; }
  }
  if (!isFinite(dataMin)) return null;

  const [sMin, sMax] = currentSizeRange;

  switch (currentSizeScale) {
    case 'sqrt': {
      const sqrtMin = Math.sqrt(dataMin), sqrtRange = Math.sqrt(dataMax) - sqrtMin || 1;
      return (_obj: any, info: { index: number }) => {
        const v = Number(values[info.index]);
        if (!isFinite(v) || v <= 0) return sMin;
        return sMin + ((Math.sqrt(v) - sqrtMin) / sqrtRange) * (sMax - sMin);
      };
    }
    case 'log': {
      const logMin = Math.log(dataMin), logRange = Math.log(dataMax) - logMin || 1;
      return (_obj: any, info: { index: number }) => {
        const v = Number(values[info.index]);
        if (!isFinite(v) || v <= 0) return sMin;
        return sMin + ((Math.log(v) - logMin) / logRange) * (sMax - sMin);
      };
    }
    default: { // linear
      const dataRange = dataMax - dataMin || 1;
      return (_obj: any, info: { index: number }) => {
        const v = Number(values[info.index]);
        if (!isFinite(v)) return sMin;
        return sMin + ((v - dataMin) / dataRange) * (sMax - sMin);
      };
    }
  }
}

/** Compute initial view state that fits all features. */
function computeViewState(bbox: BBox): MapViewState {
  if (!isFinite(bbox.minLon)) {
    return { longitude: 0, latitude: 0, zoom: 2, pitch: 0, bearing: 0 };
  }

  const centerLon = (bbox.minLon + bbox.maxLon) / 2;
  const centerLat = (bbox.minLat + bbox.maxLat) / 2;
  const dLon = bbox.maxLon - bbox.minLon;
  const dLat = bbox.maxLat - bbox.minLat;
  const maxSpan = Math.max(dLon, dLat);

  let zoom = 12;
  if (maxSpan > 0) {
    zoom = Math.max(1, Math.min(18, Math.floor(-Math.log2(maxSpan / 360))));
  }

  return { longitude: centerLon, latitude: centerLat, zoom, pitch: 0, bearing: 0 };
}

/** Default colors for geometry layers. */
const FILL_COLOR: [number, number, number, number] = [65, 135, 220, 180];
const STROKE_COLOR: [number, number, number, number] = [40, 90, 160, 220];
const H3_FILL_COLOR: [number, number, number, number] = [255, 140, 0, 180];

/** Map styling configuration. */
interface MapStyle {
  fillColor: [number, number, number, number];
  strokeColor: [number, number, number, number];
  opacity: number;
  pointRadius: number;
  colorColumn?: string; // numeric column for data-driven color
  sizeColumn?: string; // numeric column for data-driven point size
}

const DEFAULT_STYLE: MapStyle = {
  fillColor: [...FILL_COLOR] as [number, number, number, number],
  strokeColor: [...STROKE_COLOR] as [number, number, number, number],
  opacity: 1,
  pointRadius: 5,
};

/** Parse a CSS color string to RGBA. Returns null if not a color. */
/** Escape HTML special characters to prevent XSS in tooltips. */
function escHtml(s: string): string {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function parseCssColor(s: string): [number, number, number, number] | null {
  if (!s || typeof s !== 'string') return null;
  const t = s.trim().toLowerCase();
  const hexMatch = t.match(/^#([0-9a-f]{3,8})$/);
  if (hexMatch) {
    const h = hexMatch[1];
    if (h.length === 3) return [parseInt(h[0]+h[0],16), parseInt(h[1]+h[1],16), parseInt(h[2]+h[2],16), 255];
    if (h.length === 6) return [parseInt(h.slice(0,2),16), parseInt(h.slice(2,4),16), parseInt(h.slice(4,6),16), 255];
    if (h.length === 8) return [parseInt(h.slice(0,2),16), parseInt(h.slice(2,4),16), parseInt(h.slice(4,6),16), parseInt(h.slice(6,8),16)];
  }
  const rgbMatch = t.match(/^rgba?\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*([\d.]+)\s*)?\)$/);
  if (rgbMatch) {
    const a = rgbMatch[4] !== undefined ? Math.round(parseFloat(rgbMatch[4]) * 255) : 255;
    return [+rgbMatch[1], +rgbMatch[2], +rgbMatch[3], a];
  }
  const NAMED: Record<string, [number, number, number, number]> = {
    red: [255,0,0,255], green: [0,128,0,255], blue: [0,0,255,255],
    white: [255,255,255,255], black: [0,0,0,255], yellow: [255,255,0,255],
    cyan: [0,255,255,255], magenta: [255,0,255,255], orange: [255,165,0,255],
    purple: [128,0,128,255], gray: [128,128,128,255], grey: [128,128,128,255],
  };
  return NAMED[t] || null;
}

// ────────────────────── Color Palettes ──────────────────────
// Each palette is an array of [R,G,B] stops. Interpolated for continuous scales.

type RGB = [number, number, number];

const PALETTES: Record<string, RGB[]> = {
  // Sequential
  viridis: [[68,1,84],[72,35,116],[64,67,135],[52,94,141],[41,120,142],[32,144,140],[34,167,132],[68,190,112],[121,209,81],[189,222,38],[253,231,37]],
  plasma: [[13,8,135],[75,3,161],[126,3,167],[168,34,150],[199,72,121],[222,108,89],[238,145,62],[249,184,41],[251,224,37],[240,249,33]],
  blues: [[247,251,255],[222,235,247],[198,219,239],[158,202,225],[107,174,214],[66,146,198],[33,113,181],[8,81,156],[8,48,107]],
  reds: [[255,245,240],[254,224,210],[252,187,161],[252,146,114],[251,106,74],[239,59,44],[203,24,29],[165,15,21],[103,0,13]],
  ylOrRd: [[255,255,204],[255,237,160],[254,217,118],[254,178,76],[253,141,60],[252,78,42],[227,26,28],[189,0,38],[128,0,38]],
  greens: [[247,252,245],[229,245,224],[199,233,192],[161,217,155],[116,196,118],[65,171,93],[35,139,69],[0,109,44],[0,68,27]],

  // Diverging
  rdBu: [[103,0,31],[178,24,43],[214,96,77],[244,165,130],[253,219,199],[247,247,247],[209,229,240],[146,197,222],[67,147,195],[33,102,172],[5,48,97]],
  rdYlGn: [[165,0,38],[215,48,39],[244,109,67],[253,174,97],[254,224,139],[255,255,191],[217,239,139],[166,217,106],[102,189,99],[26,152,80],[0,104,55]],
};

/** Default palette name. */
let currentPalette = 'viridis';

/** Interpolate a color from palette at position t (0-1). */
function paletteColor(t: number, paletteName: string = currentPalette): [number, number, number, number] {
  const stops = PALETTES[paletteName] || PALETTES.viridis;
  t = Math.max(0, Math.min(1, t));
  const idx = t * (stops.length - 1);
  const lo = Math.floor(idx);
  const hi = Math.min(lo + 1, stops.length - 1);
  const f = idx - lo;
  return [
    Math.round(stops[lo][0] + (stops[hi][0] - stops[lo][0]) * f),
    Math.round(stops[lo][1] + (stops[hi][1] - stops[lo][1]) * f),
    Math.round(stops[lo][2] + (stops[hi][2] - stops[lo][2]) * f),
    200,
  ];
}

/** Legacy colorScale — now delegates to palette. */
function colorScale(t: number): [number, number, number, number] {
  return paletteColor(t);
}

/** Compute min/max for a numeric property across features. */
function computeMinMax(features: FeatureRow[], propName: string): [number, number] {
  let min = Infinity;
  let max = -Infinity;
  for (const f of features) {
    const v = Number(f.properties[propName]);
    if (isFinite(v)) {
      if (v < min) min = v;
      if (v > max) max = v;
    }
  }
  return [min, max];
}

/** Build deck.gl layers from parsed features with optional styling. */
function buildLayers(features: FeatureRow[], geomType: number, style: MapStyle = DEFAULT_STYLE): any[] {
  const layers: any[] = [];

  const points = features.filter(f => f.position);
  const lines = features.filter(f => f.path);
  const polygons = features.filter(f => f.polygon);
  const h3Cells = features.filter(f => f.h3Index);

  // Data-driven color accessor
  let colorAccessor: any = style.fillColor;
  if (style.colorColumn) {
    const [cMin, cMax] = computeMinMax(features, style.colorColumn);
    const range = cMax - cMin || 1;
    colorAccessor = (d: FeatureRow) => {
      const v = Number(d.properties[style.colorColumn!]);
      return isFinite(v) ? colorScale((v - cMin) / range) : style.fillColor;
    };
  }

  // Data-driven size accessor for points
  let radiusAccessor: any = style.pointRadius;
  if (style.sizeColumn) {
    const [sMin, sMax] = computeMinMax(features, style.sizeColumn);
    const range = sMax - sMin || 1;
    radiusAccessor = (d: FeatureRow) => {
      const v = Number(d.properties[style.sizeColumn!]);
      return isFinite(v) ? 3 + ((v - sMin) / range) * 17 : style.pointRadius;
    };
  }

  if (points.length > 0) {
    layers.push(new ScatterplotLayer({
      id: 'hugr-map-points',
      data: points,
      getPosition: (d: FeatureRow) => d.position!,
      getFillColor: colorAccessor,
      getLineColor: style.strokeColor,
      getRadius: radiusAccessor,
      radiusUnits: 'pixels' as any,
      radiusMinPixels: 2,
      radiusMaxPixels: 30,
      stroked: true,
      lineWidthMinPixels: 1,
      opacity: style.opacity,
      pickable: true,
      updateTriggers: {
        getFillColor: [style.colorColumn, style.fillColor],
        getRadius: [style.sizeColumn, style.pointRadius],
      },
    }));
  }

  if (lines.length > 0) {
    layers.push(new PathLayer({
      id: 'hugr-map-lines',
      data: lines,
      getPath: (d: FeatureRow) => d.path! as any,
      getColor: colorAccessor,
      getWidth: 2,
      widthUnits: 'pixels' as any,
      widthMinPixels: 1,
      opacity: style.opacity,
      pickable: true,
      updateTriggers: {
        getColor: [style.colorColumn, style.fillColor],
      },
    }));
  }

  if (polygons.length > 0) {
    layers.push(new SolidPolygonLayer({
      id: 'hugr-map-polygons',
      data: polygons,
      getPolygon: (d: FeatureRow) => d.polygon! as any,
      getFillColor: colorAccessor,
      getLineColor: style.strokeColor,
      opacity: style.opacity,
      pickable: true,
      filled: true,
      extruded: false,
      updateTriggers: {
        getFillColor: [style.colorColumn, style.fillColor],
      },
    }));
  }

  if (h3Cells.length > 0) {
    layers.push(new H3HexagonLayer({
      id: 'hugr-map-h3',
      data: h3Cells,
      getHexagon: (d: FeatureRow) => d.h3Index!,
      getFillColor: colorAccessor !== style.fillColor ? colorAccessor : H3_FILL_COLOR,
      getLineColor: style.strokeColor,
      extruded: false,
      opacity: style.opacity,
      pickable: true,
      updateTriggers: {
        getFillColor: [style.colorColumn, style.fillColor],
      },
    }));
  }

  return layers;
}

/** Build basemap tile layers from tile source configuration. */
function buildBasemapLayers(tileSources: TileSourceMeta[]): any[] {
  if (!tileSources || tileSources.length === 0) return [];

  // Use the first tile source as the active basemap
  const source = tileSources[0];

  if (source.type === 'raster' || source.type === 'tilejson') {
    return [new TileLayer({
      id: 'hugr-basemap-tiles',
      data: source.url,
      minZoom: source.min_zoom ?? 0,
      maxZoom: source.max_zoom ?? 19,
      renderSubLayers: (props: any) => {
        const { boundingBox } = props.tile;
        return new BitmapLayer(props, {
          data: undefined,
          image: props.data,
          bounds: [boundingBox[0][0], boundingBox[0][1], boundingBox[1][0], boundingBox[1][1]],
        });
      },
    })];
  }

  // Vector/MVT tiles — not implemented yet, return empty
  return [];
}

/** Approximate area of a polygon ring (Shoelace formula, in degrees²). */
function ringArea(ring: number[][]): number {
  let area = 0;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    area += (ring[j][0] - ring[i][0]) * (ring[j][1] + ring[i][1]);
  }
  return Math.abs(area / 2);
}

/** Approximate length of a path (Haversine, in km). */
function pathLength(coords: number[][]): number {
  let total = 0;
  for (let i = 1; i < coords.length; i++) {
    const [lon1, lat1] = coords[i - 1];
    const [lon2, lat2] = coords[i];
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLon = (lon2 - lon1) * Math.PI / 180;
    const a = Math.sin(dLat / 2) ** 2 +
      Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLon / 2) ** 2;
    total += 6371 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  }
  return total;
}

/** Format a tooltip from feature properties, with optional measurement info. */
function formatTooltip(feature: FeatureRow): string {
  const parts: string[] = [];
  const entries = Object.entries(feature.properties).filter(([, v]) => v !== null && v !== undefined);
  if (entries.length > 0) {
    parts.push(...entries.map(([k, v]) => `<b>${escHtml(k)}</b>: ${escHtml(String(v))}`));
  }
  // Measurement info
  if (feature.polygon && feature.polygon.length >= 3) {
    const a = ringArea(feature.polygon);
    // Convert degree² to approximate km² (very rough at equator)
    const aKm = a * 111.32 * 111.32;
    parts.push(`<i>Area: ~${aKm < 1 ? (aKm * 1e6).toFixed(0) + ' m²' : aKm.toFixed(2) + ' km²'}</i>`);
  }
  if (feature.path && feature.path.length >= 2) {
    const len = pathLength(feature.path);
    parts.push(`<i>Length: ~${len < 1 ? (len * 1000).toFixed(0) + ' m' : len.toFixed(2) + ' km'}</i>`);
  }
  return parts.join('<br/>');
}

/**
 * Register the map plugin with Perspective.
 * Must be called after Perspective custom elements are defined.
 */
/** CARTO basemap URLs. Free, no API key required. */
const BASEMAP_LIGHT = 'https://a.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}@2x.png';
const BASEMAP_DARK = 'https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png';

/** Detect if the current Perspective theme is dark by checking --plugin--background luminance. */
function isDarkTheme(el: Element): boolean {
  const bg = getComputedStyle(el).getPropertyValue('--plugin--background').trim();
  if (!bg || bg === 'transparent' || !bg.startsWith('#')) return false;
  const hex = bg.replace('#', '');
  if (hex.length < 6) return false;
  const r = parseInt(hex.slice(0, 2), 16);
  const g = parseInt(hex.slice(2, 4), 16);
  const b = parseInt(hex.slice(4, 6), 16);
  return (r * 299 + g * 587 + b * 114) / 1000 < 128;
}

/** Current basemap mode — configurable via settings panel. */
let currentBasemapMode = 'auto';

const BASEMAP_POSITRON = 'https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}@2x.png';

/** Get basemap URL based on mode and theme. */
function getBasemapUrl(el: Element): string {
  switch (currentBasemapMode) {
    case 'voyager': return BASEMAP_LIGHT;
    case 'positron': return BASEMAP_POSITRON;
    case 'dark': return BASEMAP_DARK;
    default: return isDarkTheme(el) ? BASEMAP_DARK : BASEMAP_LIGHT;
  }
}

/** Categorical color palette (10 distinct colors). */
const CATEGORY_COLORS: [number, number, number, number][] = [
  [31, 119, 180, 200], [255, 127, 14, 200], [44, 160, 44, 200],
  [214, 39, 40, 200], [148, 103, 189, 200], [140, 86, 75, 200],
  [227, 119, 194, 200], [127, 127, 127, 200], [188, 189, 34, 200],
  [23, 190, 207, 200],
];

/** Build a color accessor from features for a given column name and schema type. */
function buildColorAccessor(
  features: FeatureRow[], colName: string, colType: string
): ((d: FeatureRow) => [number, number, number, number]) | null {
  if (!colName || features.length === 0) return null;

  if (colType === 'string' || colType === 'boolean') {
    // Categorical: assign distinct colors
    const uniqueVals = [...new Set(features.map(f => String(f.properties[colName] ?? '')))];
    const valToIdx = new Map(uniqueVals.map((v, i) => [v, i]));
    return (d: FeatureRow) => {
      const idx = valToIdx.get(String(d.properties[colName] ?? '')) ?? 0;
      return CATEGORY_COLORS[idx % CATEGORY_COLORS.length];
    };
  } else {
    // Numeric: continuous gradient
    const [cMin, cMax] = computeMinMax(features, colName);
    const range = cMax - cMin || 1;
    return (d: FeatureRow) => {
      const v = Number(d.properties[colName]);
      return isFinite(v) ? colorScale((v - cMin) / range) : FILL_COLOR;
    };
  }
}

/** Build a size accessor from features for a given column name. */
function buildSizeAccessor(
  features: FeatureRow[], colName: string, minSize: number, maxSize: number
): ((d: FeatureRow) => number) | null {
  if (!colName || features.length === 0) return null;
  const [sMin, sMax] = computeMinMax(features, colName);
  const range = sMax - sMin || 1;
  return (d: FeatureRow) => {
    const v = Number(d.properties[colName]);
    return isFinite(v) ? minSize + ((v - sMin) / range) * (maxSize - minSize) : minSize;
  };
}

/** Build deck.gl layers using column slot accessors. */
function buildMappedLayers(
  features: FeatureRow[],
  geomType: number,
  colorAccessor: ((d: FeatureRow) => [number, number, number, number]) | null,
  sizeAccessor: ((d: FeatureRow) => number) | null,
  heightAccessor: ((d: FeatureRow) => number) | null,
): any[] {
  const layers: any[] = [];
  const defaultColor = FILL_COLOR;

  const points = features.filter(f => f.position);
  const lines = features.filter(f => f.path);
  const polygons = features.filter(f => f.polygon);
  const h3Cells = features.filter(f => f.h3Index);

  if (points.length > 0) {
    layers.push(new ScatterplotLayer({
      id: 'hugr-map-points',
      data: points,
      getPosition: (d: FeatureRow) => d.position!,
      getFillColor: colorAccessor || defaultColor,
      getLineColor: STROKE_COLOR,
      getRadius: sizeAccessor || 6,
      radiusUnits: 'pixels' as any,
      radiusMinPixels: 2,
      radiusMaxPixels: 40,
      stroked: true,
      lineWidthMinPixels: 1,
      opacity: currentOpacity,
      pickable: true,
      updateTriggers: {
        getFillColor: [colorAccessor],
        getRadius: [sizeAccessor],
      },
    }));
  }

  if (lines.length > 0) {
    layers.push(new PathLayer({
      id: 'hugr-map-lines',
      data: lines,
      getPath: (d: FeatureRow) => d.path! as any,
      getColor: colorAccessor || defaultColor,
      getWidth: sizeAccessor || 2,
      widthUnits: 'pixels' as any,
      widthMinPixels: 1,
      opacity: currentOpacity,
      pickable: true,
      updateTriggers: {
        getColor: [colorAccessor],
        getWidth: [sizeAccessor],
      },
    }));
  }

  if (polygons.length > 0) {
    const extruded = !!heightAccessor;
    layers.push(new SolidPolygonLayer({
      id: 'hugr-map-polygons',
      data: polygons,
      getPolygon: (d: FeatureRow) => d.polygon! as any,
      getFillColor: colorAccessor || defaultColor,
      getLineColor: STROKE_COLOR,
      getElevation: heightAccessor || 0,
      extruded,
      opacity: currentOpacity,
      pickable: true,
      filled: true,
      updateTriggers: {
        getFillColor: [colorAccessor],
        getElevation: [heightAccessor],
      },
    }));
  }

  if (h3Cells.length > 0) {
    layers.push(new H3HexagonLayer({
      id: 'hugr-map-h3',
      data: h3Cells,
      getHexagon: (d: FeatureRow) => d.h3Index!,
      getFillColor: colorAccessor || H3_FILL_COLOR,
      getLineColor: STROKE_COLOR,
      getElevation: heightAccessor || 0,
      extruded: !!heightAccessor,
      opacity: currentOpacity,
      pickable: true,
      updateTriggers: {
        getFillColor: [colorAccessor],
        getElevation: [heightAccessor],
      },
    }));
  }

  return layers;
}

// Globe/map SVG icon for the plugin selector (16x16)
const MAP_ICON_SVG = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 16 16" fill="none">
  <circle cx="8" cy="8" r="6.5" stroke="currentColor" stroke-width="1.2"/>
  <ellipse cx="8" cy="8" rx="3" ry="6.5" stroke="currentColor" stroke-width="1"/>
  <line x1="1.5" y1="5.5" x2="14.5" y2="5.5" stroke="currentColor" stroke-width="0.8"/>
  <line x1="1.5" y1="10.5" x2="14.5" y2="10.5" stroke="currentColor" stroke-width="0.8"/>
  <line x1="8" y1="1.5" x2="8" y2="14.5" stroke="currentColor" stroke-width="0.8"/>
</svg>`;
const MAP_ICON_B64 = 'data:image/svg+xml;base64,' + btoa(MAP_ICON_SVG);

export async function registerMapPlugin(): Promise<void> {
  if (customElements.get('perspective-viewer-map')) return;

  // Inject plugin icon CSS variable on host (inherits into shadow DOM)
  // and add the selector rule for our plugin name
  const iconStyle = document.createElement('style');
  iconStyle.textContent = `
    perspective-viewer, perspective-workspace {
      --plugin-selector-geo-map--content: url("${MAP_ICON_B64}");
    }
  `;
  document.head.appendChild(iconStyle);

  // Icon CSS variable on host — always available (inherits into all shadow DOMs).
  // Hide-UI styles are injected from draw() via _injectViewerStyles().
  //
  // Also inject icon mask-image rule into any viewer shadow DOM we can find.
  // This handles the case where Datagrid is active (our draw() not called).
  const iconRule = `.plugin-select-item[data-plugin="Geo Map"]:before { -webkit-mask-image: var(--plugin-selector-geo-map--content); mask-image: var(--plugin-selector-geo-map--content); }`;
  const injectIcon = (viewer: Element) => {
    const sr = viewer.shadowRoot;
    if (!sr || sr.querySelector('#geo-map-icon')) return;
    const s = document.createElement('style');
    s.id = 'geo-map-icon';
    s.textContent = iconRule;
    sr.appendChild(s);
  };
  document.querySelectorAll('perspective-viewer').forEach(injectIcon);
  new MutationObserver((mutations) => {
    for (const m of mutations) {
      for (const n of m.addedNodes) {
        if (n instanceof Element) {
          if (n.tagName === 'PERSPECTIVE-VIEWER') injectIcon(n);
          n.querySelectorAll?.('perspective-viewer').forEach(injectIcon);
        }
      }
    }
  }).observe(document.body, { childList: true, subtree: true });

  customElements.define('perspective-viewer-map', class extends HTMLElement {
    private _deck: any = null;
    private _container: HTMLDivElement | null = null;
    private _features: FeatureRow[] = [];
    private _geomType: number = 0;
    private _viewState: MapViewState = { longitude: 0, latitude: 0, zoom: 2, pitch: 0, bearing: 0 };
    private _tileSources: TileSourceMeta[] = [];
    private _geoData: GeoArrowData | null = null;
    private _tooltipColNames: string[] = [];
    private _lastView: any = null;
    private _lastSchema: Record<string, string> = {};
    private _lastColumns: (string | null)[] = [];
    // Per-instance settings (synced to module-level before draw)
    private _defaultColor: [number, number, number, number] = [65, 135, 220, 180];
    private _defaultSize: number = 6;
    private _colorScale: ColorScaleMode = 'quantize';
    private _palette: string = 'viridis';
    private _opacity: number = 0.85;
    private _basemap: string = 'auto';
    private _sizeScale: SizeScaleMode = 'linear';
    private _sizeRange: [number, number] = [2, 30];

    // --- Plugin identity ---
    get name() { return 'Geo Map'; }
    get select_mode() { return 'select'; }
    get category() { return 'Map'; }
    get priority() { return 0; }
    get group_rollups() { return ['flat']; }
    get render_warning() { return false; }
    set render_warning(_: boolean) { /* no-op */ }
    get max_cells() { return 2_000_000_000; }
    get max_columns() { return 1000; }

    // --- Column slot configuration ---
    // Slots: [0]=Geometry, [1]=Color, [2]=Size, [3]=Height, [4]=Tooltip
    get config_column_names() { return ['Geometry', 'Color', 'Size', 'Height', 'Tooltip']; }
    get min_config_columns() { return 1; }

    connectedCallback() {
      if (this.shadowRoot) return; // already attached
      this.attachShadow({ mode: 'open' });
      this.shadowRoot!.innerHTML = `
        <style>
          :host { display:block; width:100%; height:100%; position:relative; }
          #map-container { width:100%; height:100%; position:relative; background:#e5e3df; }
          #map-container canvas { position:absolute; top:0; left:0; }
          #no-geom { display:flex; align-items:center; justify-content:center;
            width:100%; height:100%; color:#888; font:14px sans-serif; }
          #legend {
            position:absolute; bottom:8px; left:8px; z-index:5;
            background: var(--map-element-background, rgba(255,255,255,0.92));
            color: var(--icon--color, #161616);
            border: 1px solid var(--inactive--border-color, #dadada);
            border-radius: 4px;
            padding: 8px 10px;
            font: 11px/1.4 sans-serif;
            max-width: 200px;
            pointer-events: auto;
          }
          #legend .legend-title {
            font-weight: bold;
            margin-bottom: 4px;
            font-size: 12px;
          }
          #legend .legend-gradient {
            height: 12px;
            border-radius: 2px;
            margin-bottom: 2px;
          }
          #legend .legend-labels {
            display: flex;
            justify-content: space-between;
            font-size: 10px;
            color: var(--inactive--color, #888);
          }
          #legend .legend-cat-item {
            display: flex;
            align-items: center;
            gap: 6px;
            margin: 2px 0;
          }
          #legend .legend-cat-dot {
            width: 10px;
            height: 10px;
            border-radius: 50%;
            flex-shrink: 0;
          }
          /* Settings panel */
          #settings-toggle {
            position:absolute; top:8px; left:8px; z-index:6;
            width:32px; height:32px;
            background: var(--map-element-background, rgba(255,255,255,0.92));
            border: 1px solid var(--inactive--border-color, #dadada);
            border-radius: 4px;
            cursor: pointer;
            display: flex; align-items: center; justify-content: center;
            font-size: 16px;
            color: var(--icon--color, #161616);
          }
          #settings-toggle:hover { background: var(--active--color, #2670a9); color: #fff; }
          #settings-panel {
            position:absolute; top:0; left:0; bottom:0; z-index:5;
            width: 220px;
            background: var(--map-element-background, rgba(255,255,255,0.95));
            border-right: 1px solid var(--inactive--border-color, #dadada);
            color: var(--icon--color, #161616);
            font: 11px/1.5 sans-serif;
            overflow-y: auto;
            padding: 44px 10px 8px 10px;
            transform: translateX(-100%);
            transition: transform 0.2s ease;
          }
          #settings-panel.open { transform: translateX(0); }
          #settings-panel .sp-section {
            margin-bottom: 10px;
            border-bottom: 1px solid var(--inactive--border-color, #dadada);
            padding-bottom: 8px;
          }
          #settings-panel .sp-section:last-child { border-bottom: none; }
          #settings-panel .sp-title {
            font-weight: bold;
            font-size: 12px;
            margin-bottom: 4px;
          }
          #settings-panel label {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin: 3px 0;
          }
          #settings-panel select, #settings-panel input[type="range"] {
            width: 110px;
            background: var(--plugin--background, #fff);
            color: var(--icon--color, #161616);
            border: 1px solid var(--inactive--border-color, #dadada);
            border-radius: 3px;
            font-size: 11px;
            padding: 1px 2px;
          }
        </style>
        <div id="map-container"></div>
        <div id="legend" style="display:none"></div>
        <div id="settings-toggle" title="Layer settings">☰</div>
        <div id="settings-panel">
          <div class="sp-section" id="sp-color">
            <div class="sp-title">Color</div>
            <label>Default <input type="color" id="sp-default-color" value="#4187dc" style="width:40px;height:22px;padding:0;border:1px solid var(--inactive--border-color,#dadada);cursor:pointer"></label>
            <label>Scale <select id="sp-color-scale">
              <option value="quantize">Quantize</option>
              <option value="quantile">Quantile</option>
              <option value="log">Log</option>
              <option value="category">Category</option>
              <option value="identity">CSS Color</option>
            </select></label>
            <label>Palette <select id="sp-palette">
              <option value="viridis">Viridis</option>
              <option value="plasma">Plasma</option>
              <option value="blues">Blues</option>
              <option value="reds">Reds</option>
              <option value="ylOrRd">YlOrRd</option>
              <option value="greens">Greens</option>
              <option value="rdBu">RdBu</option>
              <option value="rdYlGn">RdYlGn</option>
            </select></label>
          </div>
          <div class="sp-section" id="sp-size">
            <div class="sp-title">Size / Width</div>
            <label>Default <input type="range" id="sp-default-size" min="1" max="30" value="6"></label>
            <label>Scale <select id="sp-size-scale">
              <option value="linear">Linear</option>
              <option value="sqrt">Sqrt</option>
              <option value="log">Log</option>
              <option value="identity">Identity</option>
            </select></label>
            <label>Min <input type="range" id="sp-size-min" min="1" max="20" value="2"></label>
            <label>Max <input type="range" id="sp-size-max" min="5" max="100" value="30"></label>
          </div>
          <div class="sp-section" id="sp-opacity">
            <div class="sp-title">Appearance</div>
            <label>Opacity <input type="range" id="sp-opacity-slider" min="0" max="100" value="85"></label>
            <label>Basemap <select id="sp-basemap">
              <option value="auto">Auto</option>
              <option value="voyager">Voyager</option>
              <option value="positron">Positron</option>
              <option value="dark">Dark Matter</option>
            </select></label>
          </div>
        </div>
      `;
      this._container = this.shadowRoot!.getElementById('map-container') as HTMLDivElement;

      // Settings panel toggle
      const toggle = this.shadowRoot!.getElementById('settings-toggle')!;
      const panel = this.shadowRoot!.getElementById('settings-panel')!;
      toggle.addEventListener('click', () => panel.classList.toggle('open'));

      // Settings change handlers
      // Lightweight redraw: rebuild layers from cached data, no refetch
      const redraw = () => {
        if (!this._deck || !this._lastView) return;
        this._syncSettings();
        // If we have GeoArrow data cached, rebuild layers from it
        if (this._geoData) {
          const viewer = this.parentElement as any;
          let schema: Record<string, string> = {};
          try {
            // Use cached schema from last draw — avoid async view.schema()
            const cols = this._deck.props?.layers?.length || 0;
            schema = this._lastSchema || {};
          } catch {}
          const columns = this._lastColumns || [];
          const colorColName = columns[1] || null;
          const sizeColName = columns[2] || null;
          const heightColName = columns[3] || null;
          const colorAcc = colorColName
            ? buildBinaryColorAccessor(this._geoData.table, colorColName, this._lastSchema?.[colorColName] || 'string', this._geoData.geomColIdx)
            : null;
          const sizeAcc = sizeColName
            ? buildBinaryFloatAccessor(this._geoData.table, sizeColName)
            : null;
          const dataLayers = buildGeoArrowLayers(this._geoData, colorAcc, sizeAcc, heightColName);
          this._renderDeckLayers(dataLayers);
          this._updateLegend(colorColName, colorColName ? this._lastSchema?.[colorColName] || 'string' : 'string', this._geoData);
        } else {
          this.draw(this._lastView);
        }
      };
      this.shadowRoot!.getElementById('sp-default-color')!.addEventListener('input', (e) => {
        const hex = (e.target as HTMLInputElement).value;
        const r = parseInt(hex.slice(1,3),16), g = parseInt(hex.slice(3,5),16), b = parseInt(hex.slice(5,7),16);
        this._defaultColor = [r, g, b, 200];
        redraw();
      });
      this.shadowRoot!.getElementById('sp-color-scale')!.addEventListener('change', (e) => {
        this._colorScale = (e.target as HTMLSelectElement).value as ColorScaleMode;
        redraw();
      });
      this.shadowRoot!.getElementById('sp-palette')!.addEventListener('change', (e) => {
        this._palette = (e.target as HTMLSelectElement).value;
        redraw();
      });
      this.shadowRoot!.getElementById('sp-default-size')!.addEventListener('input', (e) => {
        this._defaultSize = Number((e.target as HTMLInputElement).value);
        redraw();
      });
      this.shadowRoot!.getElementById('sp-size-scale')!.addEventListener('change', (e) => {
        this._sizeScale = (e.target as HTMLSelectElement).value as SizeScaleMode;
        redraw();
      });
      this.shadowRoot!.getElementById('sp-size-min')!.addEventListener('input', (e) => {
        this._sizeRange = [Number((e.target as HTMLInputElement).value), this._sizeRange[1]];
        redraw();
      });
      this.shadowRoot!.getElementById('sp-size-max')!.addEventListener('input', (e) => {
        this._sizeRange = [this._sizeRange[0], Number((e.target as HTMLInputElement).value)];
        redraw();
      });
      this.shadowRoot!.getElementById('sp-opacity-slider')!.addEventListener('input', (e) => {
        this._opacity = Number((e.target as HTMLInputElement).value) / 100;
        redraw();
      });
      this.shadowRoot!.getElementById('sp-basemap')!.addEventListener('change', (e) => {
        this._basemap = (e.target as HTMLSelectElement).value;
        redraw();
      });
    }

    /** Read null-padded slot columns from the viewer.
     *  Uses viewer.getViewConfig() which is non-blocking (safe from draw). */
    private async _getSlotColumns(view: any, viewer: any): Promise<(string | null)[]> {
      const SLOT_COUNT = 5;
      if (viewer?.getViewConfig) {
        try {
          const config = await viewer.getViewConfig();
          if (config && Array.isArray(config.columns) && config.columns.length >= SLOT_COUNT) {
            return config.columns;
          }
        } catch {}
      }
      let rawCols: (string | null)[] = [];
      try {
        const viewConfig = await view.get_config();
        rawCols = viewConfig.columns || [];
      } catch {}
      while (rawCols.length < SLOT_COUNT) rawCols.push(null);
      return rawCols;
    }

    /** Sync per-instance settings to module-level variables before draw. */
    private _syncSettings() {
      currentColorScale = this._colorScale;
      currentPalette = this._palette;
      currentOpacity = this._opacity;
      currentBasemapMode = this._basemap;
      currentSizeScale = this._sizeScale;
      currentSizeRange = this._sizeRange;
      currentDefaultSize = this._defaultSize;
      currentDefaultColor = this._defaultColor;
    }

    async draw(view: any) {
      if (!this._container) return;
      this._lastView = view;
      this._syncSettings();

      const viewer = this.parentElement as any;

      // Inject/update styles in viewer shadow DOM (icon, hide UI)
      this._injectViewerStyles();

      // Read geometry metadata
      let geomMeta: GeometryColumnMeta[] = [];
      try {
        const geomAttr = viewer?.getAttribute('data-geometry-columns');
        if (geomAttr) geomMeta = JSON.parse(geomAttr);
      } catch {}
      try {
        const tileAttr = viewer?.getAttribute('data-tile-sources');
        if (tileAttr) this._tileSources = JSON.parse(tileAttr);
      } catch {}

      // Get null-padded slot columns
      const columns = await this._getSlotColumns(view, viewer);
      const geomNames = new Set(geomMeta.map(g => g.name));

      // Auto-fix: if slot[0] is not a geometry column, put geometry first
      if (columns[0] && !geomNames.has(columns[0]) && geomMeta.length > 0 && viewer?.restore) {
        const SLOT_COUNT = 5;
        const geomCol = geomMeta[0].name;
        const fixed: (string | null)[] = new Array(SLOT_COUNT).fill(null);
        fixed[0] = geomCol;
        let slot = 1;
        for (const c of columns) {
          if (c === null || c === geomCol) continue;
          if (slot < SLOT_COUNT) { fixed[slot] = c; slot++; }
        }
        viewer.restore({ columns: fixed });
        return;
      }

      // Slots: [0]=Geometry, [1]=Color, [2]=Size, [3]=Height, [4+]=Tooltip (multiple)
      const geomColName = columns[0] || null;
      const colorColName = columns[1] || null;
      const sizeColName = columns[2] || null;
      const heightColName = columns[3] || null;
      // All columns from index 4 onwards are tooltip columns
      const tooltipColNames = columns.slice(4).filter((c): c is string => c != null);

      // Find geometry column metadata
      let activeGeomCol = geomMeta.find(g => g.name === geomColName) || null;
      if (!activeGeomCol && geomMeta.length > 0) activeGeomCol = geomMeta[0];

      if (!activeGeomCol) {
        this._container.innerHTML = '';
        const noGeom = document.createElement('div');
        noGeom.id = 'no-geom';
        noGeom.textContent = 'Drag a geometry column to the Geometry slot';
        this._container.appendChild(noGeom);
        return;
      }

      // Get schema for column type info
      let schema: Record<string, string> = {};
      try { schema = await view.schema(); } catch {}
      this._lastSchema = schema;
      this._lastColumns = columns;

      const arrowUrl = viewer?.getAttribute('data-arrow-url') || null;

      try {
        // Try GeoArrow binary path first (zero-copy to GPU)
        // Build GeoArrow URL with column projection — only fetch needed columns
        let geoArrowUrl: string | null = null;
        if (arrowUrl) {
          const neededCols = [geomColName, colorColName, sizeColName, heightColName, ...tooltipColNames]
            .filter((c): c is string => c != null);
          const uniqueCols = [...new Set(neededCols)];
          // TODO: column projection disabled temporarily — nested GeoArrow types may break with projectColumns
          const params = `geoarrow=1`;
          geoArrowUrl = arrowUrl + (arrowUrl.includes('?') ? '&' : '?') + params;
        }
        let geoData: GeoArrowData | null = null;
        // GeoArrow binary path: only for WKB/GeoArrow formats (not H3Cell, GeoJSON)
        const useGeoArrow = geoArrowUrl && activeGeomCol.format !== 'H3Cell' && activeGeomCol.format !== 'GeoJSON';
        if (useGeoArrow) {
          try {
            geoData = await loadGeoArrowData(geoArrowUrl!, activeGeomCol.name);
          } catch (e) {
            // GeoArrow not available — fall back to WKB parsing
          }
        }

        if (geoData) {
          // ── GeoArrow binary path ──
          const colorAcc = colorColName
            ? buildBinaryColorAccessor(geoData.table, colorColName, schema[colorColName] || 'string', geoData.geomColIdx)
            : null;
          const sizeAcc = sizeColName
            ? buildBinaryFloatAccessor(geoData.table, sizeColName)
            : null;

          const isFirstDraw = this._deck === null;
          if (isFirstDraw) {
            this._viewState = computeViewState(geoData.bbox);
            if (heightColName) {
              this._viewState.pitch = 45;
              this._viewState.bearing = 15;
            }
          }

          this._geoData = geoData;
          this._tooltipColNames = tooltipColNames;

          const dataLayers = buildGeoArrowLayers(geoData, colorAcc, sizeAcc, heightColName);
          this._renderDeckLayers(dataLayers);
          this._updateLegend(colorColName, colorColName ? schema[colorColName] || 'string' : 'string', geoData);
        } else {
          // ── WKB / fallback path ──
          let features: FeatureRow[];
          let geomType: number;
          let bbox: BBox;

          if (arrowUrl) {
            ({ features, geomType, bbox } = await parseArrowToFeatures(
              arrowUrl, activeGeomCol.name, activeGeomCol.format
            ));
          } else {
            ({ features, geomType, bbox } = await this._parseFeaturesFromView(view, activeGeomCol));
          }

          const colorAcc = colorColName
            ? buildColorAccessor(features, colorColName, schema[colorColName] || 'string')
            : null;
          const sizeAcc = sizeColName
            ? buildSizeAccessor(features, sizeColName, 3, 25)
            : null;
          const heightAcc = heightColName
            ? buildSizeAccessor(features, heightColName, 0, 50000)
            : null;

          if (tooltipColNames.length > 0) {
            for (const f of features) f.properties.__tooltipColumns = tooltipColNames;
          }

          const isFirstDraw = this._deck === null;
          this._features = features;
          this._geomType = geomType;

          if (isFirstDraw) {
            this._viewState = computeViewState(bbox);
            if (heightColName) this._viewState.pitch = 45;
          }

          this._renderDeck(colorAcc, sizeAcc, heightAcc);
          // Hide legend for WKB fallback (no GeoArrow data for legend)
          const legendEl = this.shadowRoot?.getElementById('legend');
          if (legendEl) legendEl.style.display = 'none';
        }
      } catch (err) {
        console.error('[map-plugin] draw error:', err);
        this._container.innerHTML = `<div id="no-geom">Error loading map data: ${(err as Error).message}</div>`;
      }
    }

    private async _parseFeaturesFromView(
      view: any, geomCol: GeometryColumnMeta
    ): Promise<{ features: FeatureRow[]; geomType: number; bbox: BBox }> {
      const features: FeatureRow[] = [];
      const bbox: BBox = { minLon: Infinity, maxLon: -Infinity, minLat: Infinity, maxLat: -Infinity };
      let detectedType = 0;

      const json = await view.to_json();
      if (!json || json.length === 0) return { features, geomType: 0, bbox };

      for (const row of json) {
        const geomValue = row[geomCol.name];
        if (!geomValue) continue;

        const properties: Record<string, any> = {};
        for (const [k, v] of Object.entries(row)) {
          if (k !== geomCol.name) properties[k] = v;
        }

        if (geomCol.format === 'H3Cell') {
          features.push({ h3Index: String(geomValue), properties });
          continue;
        }

        let wkbBytes: Uint8Array | null = null;
        if (geomValue instanceof Uint8Array) {
          wkbBytes = geomValue;
        } else if (Array.isArray(geomValue)) {
          wkbBytes = new Uint8Array(geomValue);
        } else if (typeof geomValue === 'object' && geomValue !== null) {
          const len = geomValue.length || geomValue.byteLength;
          if (len > 0) {
            wkbBytes = new Uint8Array(len);
            for (let i = 0; i < len; i++) wkbBytes[i] = geomValue[i];
          }
        }
        if (!wkbBytes) continue;

        const parsed = parseWKB(wkbBytes);
        if (!parsed) continue;
        if (detectedType === 0) detectedType = parsed.type;

        for (const coord of parsed.coordinates) {
          if (coord[0] < bbox.minLon) bbox.minLon = coord[0];
          if (coord[0] > bbox.maxLon) bbox.maxLon = coord[0];
          if (coord[1] < bbox.minLat) bbox.minLat = coord[1];
          if (coord[1] > bbox.maxLat) bbox.maxLat = coord[1];
        }

        const feat: FeatureRow = { properties };
        switch (parsed.type) {
          case WKB_POINT: feat.position = parsed.coordinates[0] as [number, number]; break;
          case WKB_LINESTRING: feat.path = parsed.coordinates; break;
          case WKB_POLYGON: feat.polygon = parsed.coordinates; break;
        }
        features.push(feat);
      }

      return { features, geomType: detectedType, bbox };
    }

    private _renderDeck(
      colorAcc: ((d: FeatureRow) => [number, number, number, number]) | null,
      sizeAcc: ((d: FeatureRow) => number) | null,
      heightAcc: ((d: FeatureRow) => number) | null,
    ) {
      if (!this._container) return;

      // Build basemap
      let basemapLayers: any[];
      if (this._tileSources.length > 0) {
        basemapLayers = buildBasemapLayers(this._tileSources);
      } else {
        basemapLayers = buildBasemapLayers([{
          name: 'CARTO Auto',
          url: getBasemapUrl(this.parentElement || this),
          type: 'raster',
          attribution: '© CARTO © OpenStreetMap contributors',
          min_zoom: 0,
          max_zoom: 19,
        }]);
      }

      const dataLayers = buildMappedLayers(
        this._features, this._geomType, colorAcc, sizeAcc, heightAcc
      );

      if (this._deck) {
        this._deck.setProps({
          layers: [...basemapLayers, ...dataLayers],
          initialViewState: this._viewState,
        });
        return;
      }

      this._deck = new Deck({
        parent: this._container,
        initialViewState: this._viewState,
        controller: true,
        layers: [...basemapLayers, ...dataLayers],
        getTooltip: ({ object }: any) => {
          if (!object) return null;
          const html = formatTooltip(object);
          return html ? { html, style: { background: 'var(--warning--background, rgba(0,0,0,0.85))', color: 'var(--warning--color, #fff)', fontSize: '12px', padding: '8px 12px', borderRadius: '4px', maxWidth: '300px', lineHeight: '1.4' } } : null;
        },
        onViewStateChange: ({ viewState }: any) => {
          this._viewState = viewState;
        },
      });
    }

    /** Render with pre-built data layers (GeoArrow binary path). */
    private _renderDeckLayers(dataLayers: any[]) {
      if (!this._container) return;

      let basemapLayers: any[];
      if (this._tileSources.length > 0) {
        basemapLayers = buildBasemapLayers(this._tileSources);
      } else {
        basemapLayers = buildBasemapLayers([{
          name: 'CARTO Auto',
          url: getBasemapUrl(this.parentElement || this),
          type: 'raster',
          attribution: '© CARTO © OpenStreetMap contributors',
          min_zoom: 0,
          max_zoom: 19,
        }]);
      }

      const allLayers = [...basemapLayers, ...dataLayers];

      if (this._deck) {
        this._deck.setProps({ layers: allLayers, initialViewState: this._viewState });
        return;
      }

      this._deck = new Deck({
        parent: this._container,
        initialViewState: this._viewState,
        controller: true,
        layers: allLayers,
        getTooltip: ({ object, index }: any) => {
          const tooltipStyle = {
            background: 'var(--warning--background, rgba(0,0,0,0.85))',
            color: 'var(--warning--color, #fff)',
            fontSize: '12px',
            padding: '8px 12px',
            borderRadius: '4px',
            maxWidth: '300px',
            lineHeight: '1.4',
          };

          // GeoArrow binary path — lookup by index
          if (this._geoData && index != null && index >= 0) {
            const html = this._formatGeoArrowTooltip(index);
            return html ? { html, style: tooltipStyle } : null;
          }

          // WKB fallback path — object is FeatureRow
          if (object) {
            const html = formatTooltip(object);
            return html ? { html, style: tooltipStyle } : null;
          }

          return null;
        },
        onViewStateChange: ({ viewState }: any) => {
          this._viewState = viewState;
        },
      });
    }

    /** Update legend overlay based on current color accessor data. */
    private _updateLegend(colorColName: string | null, colType: string, geoData: GeoArrowData | null) {
      const legend = this.shadowRoot?.getElementById('legend');
      if (!legend) return;

      if (!colorColName || !geoData) {
        legend.style.display = 'none';
        return;
      }

      const colIdx = geoData.table.schema.fields.findIndex((f: any) => f.name === colorColName);
      if (colIdx === -1) { legend.style.display = 'none'; return; }
      const values = geoData.table.propColData?.get(colIdx);
      if (!values || values.length === 0) { legend.style.display = 'none'; return; }

      legend.style.display = '';

      if (colType === 'string' || colType === 'boolean') {
        // Categorical legend — insertion order matches buildBinaryColorAccessor
        const valMap = new Map<string, number>(); // value → first-seen index
        const counts = new Map<string, number>();
        for (const v of values) {
          const s = String(v ?? '');
          if (!valMap.has(s)) valMap.set(s, valMap.size);
          counts.set(s, (counts.get(s) || 0) + 1);
        }
        const entries = [...valMap.entries()].slice(0, 10);
        const items = entries.map(([val, idx]) => {
          const c = CATEGORY_COLORS[idx % CATEGORY_COLORS.length];
          const count = counts.get(val) || 0;
          return `<div class="legend-cat-item"><span class="legend-cat-dot" style="background:rgb(${c[0]},${c[1]},${c[2]})"></span>${escHtml(val)} <span style="color:var(--inactive--color,#888)">(${count.toLocaleString()})</span></div>`;
        }).join('');
        legend.innerHTML = `<div class="legend-title">${escHtml(colorColName!)}</div>${items}`;
      } else {
        // Numeric gradient legend
        let min = Infinity, max = -Infinity;
        for (const v of values) {
          const n = Number(v);
          if (isFinite(n)) { if (n < min) min = n; if (n > max) max = n; }
        }
        if (!isFinite(min)) { legend.style.display = 'none'; return; }
        // Build gradient from current palette
        const stops: string[] = [];
        for (let t = 0; t <= 1; t += 0.05) {
          const c = paletteColor(t);
          stops.push(`rgb(${c[0]},${c[1]},${c[2]})`);
        }
        legend.innerHTML = `
          <div class="legend-title">${escHtml(colorColName!)}</div>
          <div class="legend-gradient" style="background:linear-gradient(to right,${stops.join(',')})"></div>
          <div class="legend-labels"><span>${min.toLocaleString()}</span><span>${max.toLocaleString()}</span></div>
        `;
      }
    }

    /** Inject icon + hide-UI styles into the parent perspective-viewer shadow DOM. */
    private _injectViewerStyles() {
      const viewer = this.parentElement;
      if (!viewer) return;
      const sr = viewer.shadowRoot;
      if (!sr) return;

      // Icon style (always)
      if (!sr.querySelector('#geo-map-styles')) {
        const s = document.createElement('style');
        s.id = 'geo-map-styles';
        s.textContent = `
          .plugin-select-item[data-plugin="Geo Map"]:before {
            -webkit-mask-image: var(--plugin-selector-geo-map--content);
            mask-image: var(--plugin-selector-geo-map--content);
          }
        `;
        sr.appendChild(s);
      }

      // Hide-UI style (toggled by _updateViewerHideUI)
      if (!sr.querySelector('#geo-map-hide-ui')) {
        const s = document.createElement('style');
        s.id = 'geo-map-hide-ui';
        sr.appendChild(s);

        // Listen for config changes to toggle
        viewer.addEventListener('perspective-config-update', () => this._updateViewerHideUI());
      }

      this._updateViewerHideUI();
    }

    /** Show/hide Perspective UI sections based on whether Geo Map is active. */
    private async _updateViewerHideUI() {
      const viewer = this.parentElement as any;
      if (!viewer?.shadowRoot) return;
      const hideEl = viewer.shadowRoot.querySelector('#geo-map-hide-ui');
      if (!hideEl) return;

      let isGeoMap = false;
      try {
        const plugin = await viewer.getPlugin();
        isGeoMap = plugin?.name === 'Geo Map';
      } catch {}

      hideEl.textContent = isGeoMap ? `
        /* Disable Group By / Split By / Order By / Filter — greyed out, not clickable */
        #top_panel { opacity: 0.25 !important; pointer-events: none !important; }
      ` : '';
    }

    /** Format tooltip HTML from GeoArrow propColData by row index.
     *  Only shows columns assigned to Tooltip slot(s). No tooltip slots = no tooltip. */
    private _formatGeoArrowTooltip(index: number): string | null {
      const gd = this._geoData;
      if (!gd || index < 0 || index >= gd.numRows) return null;
      if (this._tooltipColNames.length === 0) return null;

      const schema = gd.table.schema;
      const propColData = gd.table.propColData;
      const tooltipSet = new Set(this._tooltipColNames);
      const parts: string[] = [];

      for (const [colIdx, values] of propColData) {
        const name = schema.fields[colIdx].name;
        if (!tooltipSet.has(name)) continue;
        const val = values[index];
        if (val === null || val === undefined) continue;
        parts.push(`<b>${escHtml(name)}</b>: ${escHtml(String(val))}`);
      }

      return parts.length > 0
        ? `<div style="font-size:12px;line-height:1.5">${parts.join('<br/>')}</div>`
        : null;
    }

    async update(view: any) { await this.draw(view); }

    disconnectedCallback() {
      if (this._deck) { this._deck.finalize(); this._deck = null; }
      this._geoData = null;
      this._lastView = null;
    }

    async clear() {
      if (this._deck) { this._deck.finalize(); this._deck = null; }
      if (this._container) this._container.innerHTML = '';
      this._geoData = null;
    }

    async resize() { if (this._deck) this._deck.redraw(); }
    async restyle() {
      // Theme changed — update basemap to match light/dark
      if (this._deck && this._tileSources.length === 0) {
        const url = getBasemapUrl(this.parentElement || this);
        const basemapLayers = buildBasemapLayers([{
          name: 'CARTO Auto', url, type: 'raster',
          attribution: '© CARTO © OpenStreetMap contributors',
          min_zoom: 0, max_zoom: 19,
        }]);
        // Re-set layers with new basemap
        const currentLayers = this._deck.props?.layers || [];
        const dataLayers = currentLayers.filter((l: any) => l.id && !l.id.startsWith('hugr-basemap'));
        this._deck.setProps({ layers: [...basemapLayers, ...dataLayers] });
      }
    }
    save() {
      return {
        viewState: this._viewState,
        colorScale: this._colorScale,
        palette: this._palette,
        opacity: this._opacity,
        basemap: this._basemap,
        sizeScale: this._sizeScale,
        sizeRange: this._sizeRange,
      };
    }
    async restore(config: any) {
      if (!config) return;
      if (config.viewState) this._viewState = config.viewState;
      if (config.colorScale) this._colorScale = config.colorScale;
      if (config.palette) this._palette = config.palette;
      if (config.opacity != null) this._opacity = config.opacity;
      if (config.basemap) this._basemap = config.basemap;
      if (config.sizeScale) this._sizeScale = config.sizeScale;
      if (config.sizeRange) this._sizeRange = config.sizeRange;

      // Sync UI controls
      const sr = this.shadowRoot;
      if (sr) {
        const setVal = (id: string, val: string) => {
          const el = sr.getElementById(id) as HTMLSelectElement | HTMLInputElement | null;
          if (el) el.value = val;
        };
        setVal('sp-color-scale', this._colorScale);
        setVal('sp-palette', this._palette);
        setVal('sp-opacity-slider', String(Math.round(this._opacity * 100)));
        setVal('sp-basemap', this._basemap);
        setVal('sp-size-scale', this._sizeScale);
        setVal('sp-size-min', String(this._sizeRange[0]));
        setVal('sp-size-max', String(this._sizeRange[1]));
      }
    }
    delete() { this.clear(); }
  });

  const Viewer = customElements.get('perspective-viewer') as any;
  if (Viewer?.registerPlugin) {
    Viewer.registerPlugin('perspective-viewer-map');
  }
}
