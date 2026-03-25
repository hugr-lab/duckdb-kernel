/**
 * Platform-agnostic types for HUGR result rendering.
 */

/** Geometry column metadata from Arrow schema detection. */
export interface GeometryColumnMeta {
  name: string;
  srid: number;
  format: string; // "WKB" | "GeoJSON" | "H3Cell"
}

/** Tile source configuration for map basemaps. */
export interface TileSourceMeta {
  name: string;
  url: string;
  type: string; // "raster" | "vector" | "tilejson"
  attribution?: string;
  min_zoom?: number;
  max_zoom?: number;
}

/** Backward-compatible flat metadata (single Arrow result). */
export interface FlatMetadata {
  query_id?: string;
  arrow_url?: string;
  rows?: number;
  columns?: { name: string; type: string }[];
  geometry_columns?: GeometryColumnMeta[];
  tile_sources?: TileSourceMeta[];
  data_size_bytes?: number;
  query_time_ms?: number;
  transfer_time_ms?: number;
}

/** A single result part in multipart response. */
export interface PartDef {
  id: string;
  type: string;       // "arrow" | "json" | "error"
  title: string;
  arrow_url?: string;
  rows?: number;
  columns?: { name: string; type: string }[];
  geometry_columns?: GeometryColumnMeta[];
  tile_sources?: TileSourceMeta[];
  data_size_bytes?: number;
  data?: any;
  errors?: { message: string; path?: string[]; extensions?: any }[];
}

/** Full multipart metadata from hugr-kernel. */
export interface MultipartMetadata extends FlatMetadata {
  parts?: PartDef[];
  base_url?: string;
}

/** Payload for "Open in Tab" action. */
export interface OpenInTabPayload {
  arrow_url: string;
  base_url: string;
  title: string;
  geometry_columns?: GeometryColumnMeta[];
  tile_sources?: TileSourceMeta[];
}

/** Payload for "Open JSON in Tab" action. */
export interface OpenJsonInTabPayload {
  json: unknown;
  title: string;
}

/**
 * Platform-specific rendering context — injected by JupyterLab or VS Code.
 *
 * Core rendering functions accept this interface to decouple from
 * platform-specific concerns (auth, URL routing, metadata persistence).
 */
export interface RenderContext {
  // --- Container ---
  container: HTMLElement;

  // --- Fetch ---
  /** RequestInit for GET requests (auth headers, signal). */
  fetchInit: (signal?: AbortSignal) => RequestInit;
  /** RequestInit for POST/DELETE requests (auth + XSRF + method). */
  mutatingFetchInit: (method: string, signal?: AbortSignal) => RequestInit;

  // --- URL Resolution ---
  /** Rewrite Arrow URL through proxy or passthrough. */
  resolveArrowUrl: (url: string) => string;
  /** Build spool action URL (pin, unpin, delete, is_pinned). */
  buildSpoolUrl: (action: string, queryId: string, opts?: Record<string, string>) => string;

  // --- Platform Context ---
  /** Current notebook directory (null if not applicable). */
  getNotebookDir: () => string | null;
  /** Callback when user clicks "Open in Tab". */
  onOpenInTab: (data: OpenInTabPayload) => void;
  /** Callback when user clicks "Open JSON in Tab". */
  onOpenJsonInTab?: (data: OpenJsonInTabPayload) => void;
  /** Persist pin state to notebook metadata (undefined = pinning disabled). */
  savePinMetadata?: (pinned: boolean) => void;

  // --- Asset Paths ---
  /** Base URL for Perspective JS/WASM/CSS assets. */
  staticBase: string;
  /** Base URL for icon assets (pin.png, unpin.png, map.png). */
  iconsBase: string;

  // --- Kernel ---
  /** Current kernel base URL (for URL rebuilding after kernel restart). */
  getKernelBaseUrl: () => string | null;
}

/** Tracked resources for a single Arrow part viewer. */
export interface ArrowPartState {
  viewer: HTMLElement;
  table: any;
  client: any;
  abortController: AbortController;
}

/** Handle returned by renderHugrResult for cleanup. */
export interface RenderHandle {
  dispose(): void;
  /** Current arrow part states for external management. */
  arrowParts: ArrowPartState[];
}

/** JSON line for raw view tokenizer. */
export interface JsonLine {
  indent: string;
  tokens: { cls: string; text: string }[];
  foldable: boolean;
  foldEnd?: number;
  bracketId?: number;
}
