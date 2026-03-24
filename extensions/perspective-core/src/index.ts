/**
 * @hugr-lab/perspective-core
 *
 * Platform-agnostic rendering core for HUGR result viewer.
 * No JupyterLab, VS Code, or other platform dependencies.
 */

// Types
export type {
  GeometryColumnMeta,
  TileSourceMeta,
  FlatMetadata,
  PartDef,
  MultipartMetadata,
  OpenInTabPayload,
  OpenJsonInTabPayload,
  RenderContext,
  ArrowPartState,
  RenderHandle,
  JsonLine,
} from './types.js';

// Main renderer
export { renderHugrResult, createPinButton } from './renderer.js';

// Arrow IPC streaming
export { streamArrowToTable, rebuildArrowUrl } from './streaming.js';

// Perspective loader
export { loadPerspective, resetPerspectiveLoader } from './perspective.js';

// JSON views
export { buildJsonRawView, buildJsonTree, tokenizeJson } from './json-view.js';

// Map plugin
export { registerMapPlugin, setMapPluginFetchInit } from './map-plugin.js';

// Utilities
export { formatNumber, formatBytes, escapeHtml, parseTruncation, buildFullUrl, concatBuffers } from './utils.js';
