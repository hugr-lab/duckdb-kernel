/**
 * Perspective viewer widget for HUGR result metadata.
 *
 * Supports multipart responses: renders each part on its own tab.
 * Arrow parts → Perspective tables, JSON parts → collapsible trees,
 * Error parts → error panels.
 */

import { IRenderMime } from '@jupyterlab/rendermime-interfaces';
import { Widget } from '@lumino/widgets';
import { registerMapPlugin, setMapPluginFetchInit } from '@hugr-lab/perspective-core';
import { hasSpoolProxy, buildArrowStreamUrl, buildSpoolUrl, getSpoolFetchInit, getSpoolMutatingFetchInit, getNotebookDir } from './spoolUrl';

const MIME_TYPE = 'application/vnd.hugr.result+json';

const STATIC_BASE = '/lab/extensions/@hugr-lab/perspective-viewer/static/perspective';
const ICONS_BASE = '/lab/extensions/@hugr-lab/perspective-viewer/static/icons';

/** Geometry column metadata from Arrow schema detection. */
interface GeometryColumnMeta {
  name: string;
  srid: number;
  format: string; // "WKB" | "GeoJSON" | "H3Cell"
}

/** Tile source configuration for map basemaps. */
interface TileSourceMeta {
  name: string;
  url: string;
  type: string; // "raster" | "vector" | "tilejson"
  attribution?: string;
  min_zoom?: number;
  max_zoom?: number;
}

/** Backward-compatible flat metadata (single Arrow result). */
interface FlatMetadata {
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
interface PartDef {
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
interface MultipartMetadata extends FlatMetadata {
  parts?: PartDef[];
  base_url?: string;
}

let _perspectiveReady: Promise<any> | null = null;

function loadPerspective(): Promise<any> {
  if (_perspectiveReady) {
    return _perspectiveReady;
  }
  _perspectiveReady = (async () => {
    const [perspective] = await Promise.all([
      import(/* webpackIgnore: true */ `${STATIC_BASE}/perspective.js`),
      import(/* webpackIgnore: true */ `${STATIC_BASE}/perspective-viewer.js`),
      import(/* webpackIgnore: true */ `${STATIC_BASE}/perspective-viewer-datagrid.js`),
      import(/* webpackIgnore: true */ `${STATIC_BASE}/perspective-viewer-d3fc.js`),
    ]);
    const themeHref = `${STATIC_BASE}/themes.css`;
    if (!document.querySelector(`link[href="${themeHref}"]`)) {
      const link = document.createElement('link');
      link.rel = 'stylesheet';
      link.href = themeHref;
      document.head.appendChild(link);
    }

    await customElements.whenDefined('perspective-viewer');
    // Configure map plugin with auth headers for Arrow fetch
    setMapPluginFetchInit(() => getSpoolFetchInit());
    await registerMapPlugin();
    return perspective;
  })();
  _perspectiveReady.catch(() => {
    _perspectiveReady = null;
  });
  return _perspectiveReady;
}

function formatNumber(n: number): string {
  return n.toLocaleString('en-US');
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB'];
  let i = 0;
  let val = bytes;
  while (val >= 1024 && i < units.length - 1) {
    val /= 1024;
    i++;
  }
  return `${val.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

function parseTruncation(arrowUrl: string): {
  truncated: boolean;
  limit: number;
  total: number;
} {
  try {
    const url = new URL(arrowUrl);
    const limit = parseInt(url.searchParams.get('limit') ?? '0', 10);
    const total = parseInt(url.searchParams.get('total') ?? '0', 10);
    if (limit > 0 && total > limit) {
      return { truncated: true, limit, total };
    }
  } catch {
    // Ignore.
  }
  return { truncated: false, limit: 0, total: 0 };
}

function buildFullUrl(arrowUrl: string): string {
  try {
    // Resolve through spool proxy if available, then remove limit/total
    const resolved = resolveArrowUrl(rebuildArrowUrl(arrowUrl));
    const url = new URL(resolved);
    url.searchParams.delete('limit');
    url.searchParams.delete('total');
    return url.toString();
  } catch {
    return arrowUrl;
  }
}

/** Cache of last known kernel base URL — updated on each execute result
 *  and from plugin.ts kernel_info discovery (survives page reload). */
let _lastKnownBaseUrl: string | null = null;

/** Module-level set of pinned query IDs — survives widget recreation on cell re-run. */
const _pinnedQueryIds = new Set<string>();

/** FIFO queue: when a pinned widget is disposed (cell re-run), old query IDs
 *  are pushed here. The next widget's renderModel shifts them to restore pin
 *  state and clean up old files. Safe because JupyterLab runs cells sequentially. */
const _pendingPinRestore: string[][] = [];

// Listen for base URL updates from plugin.ts (kernel reconnect after reload)
document.addEventListener('hugr:base-url-update', ((e: CustomEvent) => {
  if (e.detail?.baseUrl) {
    _lastKnownBaseUrl = e.detail.baseUrl;
  }
}) as EventListener);

/** Rebuild Arrow URL using the latest known kernel base URL.
 *  Extracts queryID from old URL and builds new URL with current port.
 *  Skips if URL is already a spool proxy URL. */
function rebuildArrowUrl(oldUrl: string, baseUrl?: string): string {
  // Don't rewrite spool proxy URLs — they go through Jupyter server, not kernel
  if (oldUrl.includes('/hugr/spool/')) return oldUrl;
  const base = baseUrl || _lastKnownBaseUrl;
  if (!base) return oldUrl;
  try {
    const old = new URL(oldUrl);
    const q = old.searchParams.get('q');
    if (!q) return oldUrl;
    const rebuilt = new URL(`${base}${old.pathname}`);
    old.searchParams.forEach((v, k) => rebuilt.searchParams.set(k, v));
    return rebuilt.toString();
  } catch {
    return oldUrl;
  }
}

/**
 * Resolve Arrow URL: if spool proxy is available, rewrite kernel direct URL
 * to go through the proxy. Otherwise return as-is.
 */
function resolveArrowUrl(arrowUrl: string): string {
  if (!hasSpoolProxy()) return arrowUrl;
  try {
    const url = new URL(arrowUrl);
    const queryId = url.searchParams.get('q');
    if (!queryId) return arrowUrl;
    return buildArrowStreamUrl(queryId, {
      geoarrow: url.searchParams.has('geoarrow'),
      limit: url.searchParams.has('limit') ? Number(url.searchParams.get('limit')) : undefined,
      total: url.searchParams.has('total') ? Number(url.searchParams.get('total')) : undefined,
      columns: url.searchParams.has('columns') ? url.searchParams.get('columns')!.split(',') : undefined,
    });
  } catch {
    return arrowUrl;
  }
}

/** Escape HTML to prevent XSS in innerHTML. */
function escapeHtml(s: string): string {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

/**
 * Stream length-prefixed Arrow IPC chunks from the Go server into a
 * Perspective table. Each chunk is [4-byte LE length][Arrow IPC bytes].
 * Stream ends with a zero-length marker.
 */
async function streamArrowToTable(
  arrowUrl: string,
  perspectiveWorker: any,
  signal?: AbortSignal,
): Promise<any> {
  const fetchInit = getSpoolFetchInit(signal);

  // Try fetch, retry with rebuilt URL on connection error (port may have changed after reload)
  let response: Response | undefined;
  try {
    response = await fetch(arrowUrl, fetchInit);
  } catch {
    // Connection error — wait for kernel base URL discovery, then retry
    for (let attempt = 0; attempt < 3; attempt++) {
      await new Promise(r => setTimeout(r, 1000 * (attempt + 1)));
      const rebuilt = rebuildArrowUrl(arrowUrl);
      if (rebuilt !== arrowUrl) {
        try {
          response = await fetch(rebuilt, fetchInit);
          break;
        } catch { /* retry */ }
      }
    }
    if (!response) {
      throw new Error(`Result unavailable. Re-run the cell to refresh.`);
    }
  }
  if (!response.ok) {
    throw new Error(`Failed to fetch Arrow data (HTTP ${response.status})`);
  }

  if (!response.body) {
    throw new Error('Response body is null');
  }

  const reader = response.body.getReader();
  let buffer = new Uint8Array(0);
  let table: any = null;

  try {
    while (true) {
      if (signal?.aborted) break;

      while (buffer.length < 4) {
        const { done, value } = await reader.read();
        if (done) return table;
        buffer = concatBuffers(buffer, value);
      }

      const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
      const chunkLen = view.getUint32(0, true);

      if (chunkLen === 0) break;

      while (buffer.length < 4 + chunkLen) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer = concatBuffers(buffer, value);
      }

      if (buffer.length < 4 + chunkLen) {
        break;
      }

      const chunk = buffer.slice(4, 4 + chunkLen);
      buffer = buffer.slice(4 + chunkLen);

      if (table === null) {
        table = await perspectiveWorker.table(chunk.buffer);
      } else {
        await table.update(chunk.buffer);
      }
    }
  } finally {
    reader.releaseLock();
  }

  return table;
}

function concatBuffers(a: Uint8Array, b: Uint8Array): Uint8Array {
  const result = new Uint8Array(a.length + b.length);
  result.set(a);
  result.set(b, a.length);
  return result;
}

/** Tracked resources for a single Arrow part viewer. */
interface ArrowPartState {
  viewer: HTMLElement;
  table: any;
  client: any;
  abortController: AbortController;
}

export class HugrResultWidget extends Widget implements IRenderMime.IRenderer {
  private _mimeType: string;
  private _model: IRenderMime.IMimeModel | null = null;
  private _prevQueryIds: string[] = [];
  private _isPinned = false;
  private _skipRender = false;
  private _arrowParts: ArrowPartState[] = [];
  private _lazyRender: ((idx: number) => Promise<void>) | null = null;
  private _resizeObserver: ResizeObserver | null = null;

  constructor(options: IRenderMime.IRendererOptions) {
    super();
    this._mimeType = options.mimeType;
    this.addClass('hugr-result-viewer');
  }

  async renderModel(model: IRenderMime.IMimeModel): Promise<void> {
    // Guard: setData in _savePinMetadata may trigger re-render callback
    if (this._skipRender) return;
    this._model = model;
    const metadata = model.data[this._mimeType] as unknown as MultipartMetadata;
    if (!metadata) {
      this._showError('No result metadata available.');
      return;
    }

    // Cache kernel base URL for URL rebuilding on page reload
    if (metadata.base_url) {
      _lastKnownBaseUrl = metadata.base_url;
    }

    // Collect previous query IDs for cleanup after new render completes
    const oldQueryIds = this._prevQueryIds;
    const baseUrl = metadata.base_url || _lastKnownBaseUrl;

    // Track current query IDs for cleanup on next re-run
    const newIds: string[] = [];
    if (metadata.query_id) newIds.push(metadata.query_id);
    if (metadata.parts) {
      for (const p of metadata.parts) {
        if (p.arrow_url) {
          try { const u = new URL(p.arrow_url); const q = u.searchParams.get('q'); if (q) newIds.push(q); } catch {}
        }
      }
    }
    if (newIds.length > 0) this._prevQueryIds = newIds;

    // --- Restore pin state ---
    // 1. FIFO: disposed pinned widget pushed old IDs (cell re-run)
    if (!this._isPinned && _pendingPinRestore.length > 0) {
      const restoredIds = _pendingPinRestore.shift()!;
      this._isPinned = true;
      for (const id of restoredIds) {
        if (!oldQueryIds.includes(id)) oldQueryIds.push(id);
      }
    }
    // 2. Module-level set (same widget re-render)
    if (!this._isPinned && oldQueryIds.some(id => _pinnedQueryIds.has(id))) {
      this._isPinned = true;
    }
    // 3. Output metadata (page reload — saved in .ipynb)
    if (!this._isPinned) {
      const meta = model.metadata['application/vnd.hugr.result+json'] as any;
      if (meta?.pinned) {
        this._isPinned = true;
      }
    }
    // 4. Backend is_pinned (fallback — file on disk)
    if (!this._isPinned && baseUrl && newIds.length > 0) {
      for (const id of newIds) {
        try {
          const resp = await fetch(buildSpoolUrl('is_pinned', id, { kernelBaseUrl: baseUrl, dir: getNotebookDir() ?? undefined }), getSpoolFetchInit());
          if (resp.ok) {
            const data = await resp.json();
            if (data.pinned) {
              this._isPinned = true;
              _pinnedQueryIds.add(id);
              break;
            }
          }
        } catch { /* ignore */ }
      }
    }

    // --- Cleanup old + auto-pin new ---
    const cleanupOld = () => {
      const dir = getNotebookDir() ?? undefined;
      // Delete old results (spool + pinned files)
      if (oldQueryIds.length > 0 && baseUrl) {
        for (const id of oldQueryIds) {
          _pinnedQueryIds.delete(id);
          fetch(buildSpoolUrl('delete', id, { kernelBaseUrl: baseUrl, dir }), getSpoolMutatingFetchInit('DELETE')).catch(() => {});
        }
      }
      // Auto-pin new results if this cell was previously pinned
      if (this._isPinned && baseUrl && newIds.length > 0 && dir) {
        for (const id of newIds) {
          if (!_pinnedQueryIds.has(id)) {
            _pinnedQueryIds.add(id);
            fetch(buildSpoolUrl('pin', id, { kernelBaseUrl: baseUrl, dir }), getSpoolMutatingFetchInit('POST')).catch(() => {});
          }
        }
        // Note: pin metadata is saved only on explicit pin button click,
        // not here — model.setData() triggers re-render causing infinite loop.
      }
    };

    // Multipart response with parts array
    if (metadata.parts && metadata.parts.length > 0) {
      await this._renderMultipart(metadata);
      cleanupOld();
      return;
    }

    // Backward-compatible: single Arrow result via flat fields
    if (metadata.arrow_url) {
      await this._renderSingleArrow(metadata);
      cleanupOld();
      return;
    }

    this._showError('No displayable results.');
  }

  /** Render multipart response as tabs — one tab per part. */
  private async _renderMultipart(metadata: MultipartMetadata): Promise<void> {
    await this._cleanup();
    this.node.innerHTML = '';

    const parts = metadata.parts!;

    // Propagate top-level geometry metadata to individual parts
    if (metadata.geometry_columns) {
      for (const p of parts) {
        if (!p.geometry_columns) {
          p.geometry_columns = metadata.geometry_columns;
        }
      }
    }
    if (metadata.tile_sources) {
      for (const p of parts) {
        if (!p.tile_sources) {
          p.tile_sources = metadata.tile_sources;
        }
      }
    }

    // If only one part, render directly without tabs
    if (parts.length === 1) {
      if (metadata.query_time_ms != null) {
        const banner = document.createElement('div');
        banner.className = 'hugr-result-global-banner';
        banner.textContent = `Query: ${metadata.query_time_ms} ms`;
        this.node.appendChild(banner);
      }
      const container = document.createElement('div');
      container.className = 'hugr-result-single';
      this.node.appendChild(container);
      await this._renderPartContent(parts[0], container);
      return;
    }

    // Tab bar wrapper (for overflow handling)
    const tabBarWrap = document.createElement('div');
    tabBarWrap.className = 'hugr-tabs-wrap';

    const tabBar = document.createElement('div');
    tabBar.className = 'hugr-tabs-bar';

    // Tab content panels
    const tabPanels: HTMLElement[] = [];
    const tabButtons: HTMLElement[] = [];

    for (let i = 0; i < parts.length; i++) {
      const part = parts[i];

      // Tab button
      const btn = document.createElement('button');
      btn.className = 'hugr-tab-btn';
      if (i === 0) btn.classList.add('hugr-tab-btn-active');

      const label = part.title || part.id;
      btn.title = label; // tooltip with full name
      const icon = this._partIcon(part);
      const badge = this._partBadge(part);
      btn.innerHTML = `${icon}<span class="hugr-tab-label">${escapeHtml(label)}</span>${badge}`;

      const idx = i;
      btn.addEventListener('click', () => this._switchTab(tabButtons, tabPanels, idx));
      tabBar.appendChild(btn);
      tabButtons.push(btn);

      // Tab panel
      const panel = document.createElement('div');
      panel.className = 'hugr-tab-panel';
      if (i !== 0) panel.style.display = 'none';
      tabPanels.push(panel);
    }

    // Overflow "more" button
    const moreBtn = document.createElement('button');
    moreBtn.className = 'hugr-tabs-more';
    moreBtn.textContent = '\u00B7\u00B7\u00B7';
    moreBtn.title = 'More results';
    moreBtn.style.display = 'none';
    moreBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      this._showOverflowMenu(tabBar, tabButtons, tabPanels, moreBtn);
    });

    tabBarWrap.appendChild(tabBar);
    tabBarWrap.appendChild(moreBtn);
    this.node.appendChild(tabBarWrap);

    // Global status: query time + number of results
    const globalParts: string[] = [];
    globalParts.push(`${parts.length} results`);
    if (metadata.query_time_ms != null) {
      globalParts.push(`Query: ${metadata.query_time_ms} ms`);
    }
    const status = document.createElement('div');
    status.className = 'hugr-result-global-banner';
    status.textContent = globalParts.join(' \u00b7 ');
    this.node.appendChild(status);

    // Append panels
    for (let i = 0; i < parts.length; i++) {
      this.node.appendChild(tabPanels[i]);
    }

    // Render first tab immediately, others lazily on tab switch
    const rendered = new Set<number>();
    rendered.add(0);
    await this._renderPartContent(parts[0], tabPanels[0]);

    // Store lazy render callback
    this._lazyRender = async (idx: number) => {
      if (rendered.has(idx)) return;
      rendered.add(idx);
      await this._renderPartContent(parts[idx], tabPanels[idx]);
    };

    // Check overflow after render
    this._checkTabOverflow(tabBar, moreBtn);
    this._resizeObserver = new ResizeObserver(() => this._checkTabOverflow(tabBar, moreBtn));
    this._resizeObserver.observe(tabBarWrap);
  }

  /** Check if tabs overflow and show/hide more button. */
  private _checkTabOverflow(tabBar: HTMLElement, moreBtn: HTMLElement): void {
    moreBtn.style.display = tabBar.scrollWidth > tabBar.clientWidth ? '' : 'none';
  }

  /** Show dropdown menu for overflowed tabs. */
  private _showOverflowMenu(
    _tabBar: HTMLElement,
    buttons: HTMLElement[],
    panels: HTMLElement[],
    moreBtn: HTMLElement,
  ): void {
    // The menu is appended to tabBarWrap (moreBtn's parent) which has position: relative
    const wrap = moreBtn.parentElement!;

    // Remove existing menu (toggle behavior)
    const existing = wrap.querySelector('.hugr-tabs-overflow-menu');
    if (existing) { existing.remove(); return; }

    const menu = document.createElement('div');
    menu.className = 'hugr-tabs-overflow-menu';

    // Show all parts in the selector
    for (let i = 0; i < buttons.length; i++) {
      const item = document.createElement('button');
      item.className = 'hugr-tabs-overflow-item';
      if (buttons[i].classList.contains('hugr-tab-btn-active')) {
        item.classList.add('hugr-tabs-overflow-item-active');
      }
      item.textContent = buttons[i].title;
      const idx = i;
      item.addEventListener('click', () => {
        menu.remove();
        this._switchTab(buttons, panels, idx);
        buttons[idx].scrollIntoView({ behavior: 'smooth', block: 'nearest', inline: 'nearest' });
      });
      menu.appendChild(item);
    }

    // Position below the tab bar, aligned to the right edge
    menu.style.top = '100%';
    menu.style.right = '0';
    wrap.appendChild(menu);

    // Close on outside click
    const closeHandler = (e: MouseEvent) => {
      if (!menu.contains(e.target as Node) && e.target !== moreBtn) {
        menu.remove();
        document.removeEventListener('click', closeHandler);
      }
    };
    setTimeout(() => document.addEventListener('click', closeHandler), 0);
  }

  private _switchTab(buttons: HTMLElement[], panels: HTMLElement[], activeIdx: number): void {
    for (let i = 0; i < buttons.length; i++) {
      buttons[i].classList.toggle('hugr-tab-btn-active', i === activeIdx);
      panels[i].style.display = i === activeIdx ? '' : 'none';
    }
    // Trigger lazy render
    if (this._lazyRender) {
      void this._lazyRender(activeIdx);
    }
    // Notify perspective viewers in the active panel to recalculate layout
    const activePanel = panels[activeIdx];
    if (activePanel) {
      requestAnimationFrame(() => {
        const viewers = activePanel.querySelectorAll('perspective-viewer');
        for (const v of viewers) {
          (v as any).notifyResize?.();
        }
      });
    }
  }

  private _partIcon(part: PartDef): string {
    // Extensions get a special icon
    if (part.id === 'extensions' || part.title === 'extensions') {
      return '<span class="hugr-tab-icon hugr-tab-icon-ext">\u2699</span>';
    }
    switch (part.type) {
      case 'arrow': return '<span class="hugr-tab-icon hugr-tab-icon-arrow">\u{1F4CA}</span>';
      case 'json':  return '<span class="hugr-tab-icon hugr-tab-icon-json">{}</span>';
      case 'error': return '<span class="hugr-tab-icon hugr-tab-icon-error">\u26A0</span>';
      default:      return '<span class="hugr-tab-icon">\u{1F4C4}</span>';
    }
  }

  private _partBadge(part: PartDef): string {
    if (part.type === 'arrow' && part.rows != null) {
      return `<span class="hugr-tab-badge">${formatNumber(part.rows)}</span>`;
    }
    if (part.type === 'error' && part.errors) {
      return `<span class="hugr-tab-badge hugr-tab-badge-error">${part.errors.length}</span>`;
    }
    return '';
  }

  /** Render content for a single part into its container. */
  private async _renderPartContent(part: PartDef, container: HTMLElement): Promise<void> {
    switch (part.type) {
      case 'arrow':
        await this._renderArrowInto(part, container);
        break;
      case 'json':
        this._renderJsonInto(part, container);
        break;
      case 'error':
        this._renderErrorInto(part, container);
        break;
      default:
        this._renderJsonInto(part, container);
        break;
    }
  }

  /** Render Arrow part into a container. */
  private async _renderArrowInto(part: PartDef, container: HTMLElement): Promise<void> {
    if (!part.arrow_url) {
      const noData = document.createElement('div');
      noData.className = 'hugr-result-no-data';
      noData.textContent = part.rows === 0 ? '(no rows)' : 'No Arrow URL available.';
      container.appendChild(noData);
      return;
    }

    // Rewrite URL to use spool proxy if available
    part = { ...part, arrow_url: resolveArrowUrl(part.arrow_url!) };

    const truncation = parseTruncation(part.arrow_url!);

    // Status banner
    const statusParts: string[] = [];
    if (truncation.truncated) {
      statusParts.push(`Showing ${formatNumber(truncation.limit)} of ${formatNumber(truncation.total)} rows`);
    } else if (part.rows) {
      statusParts.push(`${formatNumber(part.rows)} rows`);
    }
    if (part.data_size_bytes != null && part.data_size_bytes > 0) {
      statusParts.push(formatBytes(part.data_size_bytes));
    }
    if (part.columns) {
      statusParts.push(`${part.columns.length} columns`);
    }

    if (statusParts.length > 0) {
      const banner = document.createElement('div');
      banner.className = 'hugr-result-banner';
      const span = document.createElement('span');
      span.textContent = statusParts.join(' \u00b7 ');
      banner.appendChild(span);

      if (truncation.truncated) {
        const btn = document.createElement('button');
        btn.className = 'hugr-result-load-all';
        btn.textContent = `Load all ${formatNumber(truncation.total)} rows`;
        btn.addEventListener('click', async () => {
          btn.disabled = true;
          btn.textContent = 'Loading...';
          // Clean up the existing ArrowPartState for this container before re-rendering
          await this._cleanupArrowPartsIn(container);
          container.innerHTML = '';
          await this._renderArrowInto(
            { ...part, arrow_url: buildFullUrl(part.arrow_url!) },
            container,
          );
        });
        banner.appendChild(btn);
      }

      const openTabBtn = document.createElement('button');
      openTabBtn.className = 'hugr-open-tab-btn';
      openTabBtn.innerHTML = `<img src="${ICONS_BASE}/open-in-new-tab.png" class="hugr-btn-icon" alt="Open in Tab">`;
      openTabBtn.title = 'Open in Tab';
      openTabBtn.addEventListener('click', () => {
        document.dispatchEvent(new CustomEvent('hugr:open-in-tab', {
          detail: {
            arrowUrl: buildFullUrl(part.arrow_url!),
            title: part.title || part.id || 'Result',
            geometryColumns: part.geometry_columns || [],
            tileSources: part.tile_sources || [],
          }
        }));
      });
      banner.appendChild(openTabBtn);

      // Pin/Unpin toggle button
      const pinBtn = this._createPinButton(() => {
        try {
          const u = new URL(part.arrow_url!);
          return u.searchParams.get('q');
        } catch { return null; }
      });
      banner.appendChild(pinBtn);

      container.appendChild(banner);
    }

    const loading = document.createElement('div');
    loading.className = 'hugr-result-loading';
    loading.textContent = 'Loading viewer...';
    container.appendChild(loading);

    try {
      const perspective = await loadPerspective();
      const abortController = new AbortController();
      const client = await perspective.worker();
      const table = await streamArrowToTable(rebuildArrowUrl(part.arrow_url!), client, abortController.signal);

      loading.remove();

      const viewer = document.createElement('perspective-viewer');
      viewer.setAttribute('plugin', 'Datagrid');
      container.appendChild(viewer);

      // Pass geometry metadata to the map plugin
      const geoCols = part.geometry_columns;
      const tileSources = part.tile_sources;
      if (geoCols && geoCols.length > 0) {
        viewer.setAttribute('data-geometry-columns', JSON.stringify(geoCols));
      }
      if (tileSources && tileSources.length > 0) {
        viewer.setAttribute('data-tile-sources', JSON.stringify(tileSources));
      }
      if (part.arrow_url) {
        viewer.setAttribute('data-arrow-url', rebuildArrowUrl(part.arrow_url));
      }

      this._arrowParts.push({ viewer, table, client, abortController });

      await (viewer as any).load(table);

      // Notify map plugin about geometry metadata
      const mapPlugin = viewer.querySelector('perspective-viewer-map') as any;
      if (mapPlugin && mapPlugin.setGeometryMeta) {
        mapPlugin.setGeometryMeta(geoCols || [], tileSources || [], part.arrow_url || null);
      }
    } catch (err: any) {
      loading.remove();
      const message = err?.message || 'Unknown error';
      const div = document.createElement('div');
      if (message.includes('Result unavailable') || message.includes('Failed to connect') || message.includes('Failed to fetch')) {
        div.className = 'hugr-result-expired';
        div.textContent = 'Result expired. Re-run the cell to refresh.';
      } else {
        div.className = 'hugr-result-error';
        div.textContent = `Failed to load Arrow data: ${message}`;
      }
      container.appendChild(div);
    }
  }

  /** Render JSON part as a collapsible tree with raw toggle. */
  private _renderJsonInto(part: PartDef, container: HTMLElement): void {
    // Toolbar with view toggle and open in tab
    const toolbar = document.createElement('div');
    toolbar.className = 'hugr-json-toolbar';

    const treeBtn = document.createElement('button');
    treeBtn.className = 'hugr-json-raw-btn hugr-json-raw-btn-active';
    treeBtn.textContent = 'Tree';
    toolbar.appendChild(treeBtn);

    const rawBtn = document.createElement('button');
    rawBtn.className = 'hugr-json-raw-btn';
    rawBtn.textContent = 'Raw';
    toolbar.appendChild(rawBtn);

    const openTabBtn = document.createElement('button');
    openTabBtn.className = 'hugr-open-tab-btn';
    openTabBtn.innerHTML = `<img src="${ICONS_BASE}/open-in-new-tab.png" class="hugr-btn-icon" alt="Open in Tab">`;
    openTabBtn.title = 'Open JSON in a separate tab';
    openTabBtn.addEventListener('click', () => {
      document.dispatchEvent(new CustomEvent('hugr:open-json-in-tab', {
        detail: { data: part.data, title: part.title || part.id || 'JSON' }
      }));
    });
    toolbar.appendChild(openTabBtn);

    container.appendChild(toolbar);

    // Tree view
    const tree = document.createElement('div');
    tree.className = 'hugr-json-tree';
    this._buildJsonTree(part.data, tree, true);
    container.appendChild(tree);

    // Raw view with syntax highlighting, line numbers, folding, bracket matching
    const rawWrap = document.createElement('div');
    rawWrap.className = 'hugr-json-raw';
    rawWrap.style.display = 'none';
    buildJsonRawView(part.data, rawWrap);
    container.appendChild(rawWrap);

    let mode: 'tree' | 'raw' = 'tree';
    const setMode = (m: 'tree' | 'raw') => {
      mode = m;
      tree.style.display = m === 'tree' ? '' : 'none';
      rawWrap.style.display = m === 'raw' ? '' : 'none';
      treeBtn.classList.toggle('hugr-json-raw-btn-active', m === 'tree');
      rawBtn.classList.toggle('hugr-json-raw-btn-active', m === 'raw');
    };
    treeBtn.addEventListener('click', () => setMode('tree'));
    rawBtn.addEventListener('click', () => setMode('raw'));
  }

  /** Recursively build a collapsible JSON tree. */
  private _buildJsonTree(data: any, parent: HTMLElement, expanded: boolean): void {
    if (data === null || data === undefined) {
      const val = document.createElement('span');
      val.className = 'hugr-json-null';
      val.textContent = 'null';
      parent.appendChild(val);
      return;
    }

    if (Array.isArray(data)) {
      if (data.length === 0) {
        const val = document.createElement('span');
        val.className = 'hugr-json-bracket';
        val.textContent = '[]';
        parent.appendChild(val);
        return;
      }
      this._buildCollapsible(data, parent, expanded, true);
      return;
    }

    if (typeof data === 'object') {
      const keys = Object.keys(data);
      if (keys.length === 0) {
        const val = document.createElement('span');
        val.className = 'hugr-json-bracket';
        val.textContent = '{}';
        parent.appendChild(val);
        return;
      }
      this._buildCollapsible(data, parent, expanded, false);
      return;
    }

    // Primitive value
    const val = document.createElement('span');
    if (typeof data === 'string') {
      val.className = 'hugr-json-string';
      val.textContent = JSON.stringify(data);
    } else if (typeof data === 'number') {
      val.className = 'hugr-json-number';
      val.textContent = String(data);
    } else if (typeof data === 'boolean') {
      val.className = 'hugr-json-bool';
      val.textContent = String(data);
    } else {
      val.textContent = String(data);
    }
    parent.appendChild(val);
  }

  /** Build a collapsible object/array node. */
  private _buildCollapsible(data: any, parent: HTMLElement, expanded: boolean, isArray: boolean): void {
    const count = isArray ? data.length : Object.keys(data).length;

    const row = document.createElement('div');
    row.className = 'hugr-json-row';

    const toggle = document.createElement('span');
    toggle.className = 'hugr-json-toggle';
    toggle.textContent = expanded ? '\u25BC' : '\u25B6';
    row.appendChild(toggle);

    const summary = document.createElement('span');
    summary.className = 'hugr-json-summary';
    summary.textContent = isArray ? `Array(${count})` : `{${count} keys}`;
    row.appendChild(summary);

    parent.appendChild(row);

    const children = document.createElement('div');
    children.className = 'hugr-json-children';
    children.style.display = expanded ? '' : 'none';

    if (isArray) {
      for (let i = 0; i < data.length; i++) {
        const entry = document.createElement('div');
        entry.className = 'hugr-json-entry';
        const key = document.createElement('span');
        key.className = 'hugr-json-index';
        key.textContent = `${i}: `;
        entry.appendChild(key);
        this._buildJsonTree(data[i], entry, false);
        children.appendChild(entry);
      }
    } else {
      for (const [k, v] of Object.entries(data)) {
        const entry = document.createElement('div');
        entry.className = 'hugr-json-entry';
        const key = document.createElement('span');
        key.className = 'hugr-json-key';
        key.textContent = `${k}: `;
        entry.appendChild(key);
        this._buildJsonTree(v, entry, false);
        children.appendChild(entry);
      }
    }

    parent.appendChild(children);

    const doToggle = () => {
      const isOpen = children.style.display !== 'none';
      children.style.display = isOpen ? 'none' : '';
      toggle.textContent = isOpen ? '\u25B6' : '\u25BC';
    };
    toggle.addEventListener('click', doToggle);
    summary.addEventListener('click', doToggle);
  }

  /** Render error part into a container. */
  private _renderErrorInto(part: PartDef, container: HTMLElement): void {
    if (!part.errors || part.errors.length === 0) {
      container.textContent = 'No errors.';
      return;
    }

    for (const err of part.errors) {
      const item = document.createElement('div');
      item.className = 'hugr-result-error-item';

      const msg = document.createElement('div');
      msg.className = 'hugr-result-error-message';
      msg.textContent = err.message;
      item.appendChild(msg);

      if (err.path && err.path.length > 0) {
        const path = document.createElement('div');
        path.className = 'hugr-result-error-path';
        path.textContent = `Path: ${err.path.join('.')}`;
        item.appendChild(path);
      }

      if (err.extensions) {
        const ext = document.createElement('pre');
        ext.className = 'hugr-result-error-extensions';
        ext.textContent = JSON.stringify(err.extensions, null, 2);
        item.appendChild(ext);
      }

      container.appendChild(item);
    }
  }

  /** Backward-compatible: render single Arrow result from flat fields. */
  private async _renderSingleArrow(metadata: FlatMetadata): Promise<void> {
    const arrowUrl = resolveArrowUrl(rebuildArrowUrl(metadata.arrow_url!));
    const truncation = parseTruncation(arrowUrl);

    this.node.innerHTML = '<div class="hugr-result-loading">Loading viewer...</div>';

    try {
      const perspective = await loadPerspective();

      await this._cleanup();

      const abortController = new AbortController();
      const client = await perspective.worker();
      const table = await streamArrowToTable(arrowUrl, client, abortController.signal);

      this.node.innerHTML = '';

      const statusParts: string[] = [];
      if (truncation.truncated) {
        statusParts.push(`Showing ${formatNumber(truncation.limit)} of ${formatNumber(truncation.total)} rows`);
      } else if (metadata.rows) {
        statusParts.push(`${formatNumber(metadata.rows)} rows`);
      }
      if (metadata.data_size_bytes != null && metadata.data_size_bytes > 0) {
        statusParts.push(formatBytes(metadata.data_size_bytes));
      }
      if (metadata.query_time_ms != null) {
        statusParts.push(`Query: ${metadata.query_time_ms} ms`);
      }
      if (metadata.transfer_time_ms != null) {
        statusParts.push(`Transfer: ${metadata.transfer_time_ms} ms`);
      }

      if (statusParts.length > 0 || truncation.truncated) {
        const banner = document.createElement('div');
        banner.className = 'hugr-result-banner';

        const span = document.createElement('span');
        span.textContent = statusParts.join(' \u00b7 ');
        banner.appendChild(span);

        if (truncation.truncated) {
          const btn = document.createElement('button');
          btn.className = 'hugr-result-load-all';
          btn.textContent = `Load all ${formatNumber(truncation.total)} rows`;
          btn.addEventListener('click', () => {
            btn.disabled = true;
            btn.textContent = 'Loading...';
            this._renderSingleArrow({ ...metadata, arrow_url: buildFullUrl(arrowUrl) });
          });
          banner.appendChild(btn);
        }

        const openTabBtn = document.createElement('button');
        openTabBtn.className = 'hugr-open-tab-btn';
        openTabBtn.innerHTML = `<img src="${ICONS_BASE}/open-in-new-tab.png" class="hugr-btn-icon" alt="Open in Tab">`;
      openTabBtn.title = 'Open in Tab';
        openTabBtn.addEventListener('click', () => {
          document.dispatchEvent(new CustomEvent('hugr:open-in-tab', {
            detail: {
              arrowUrl: buildFullUrl(metadata.arrow_url!),
              title: metadata.query_id || 'Result',
              geometryColumns: metadata.geometry_columns || [],
              tileSources: metadata.tile_sources || [],
            }
          }));
        });
        banner.appendChild(openTabBtn);

        // Pin/Unpin toggle button
        const pinBtn = this._createPinButton(() => {
          try {
            const u = new URL(metadata.arrow_url!);
            return u.searchParams.get('q');
          } catch { return null; }
        });
        banner.appendChild(pinBtn);

        this.node.appendChild(banner);
      }

      const viewer = document.createElement('perspective-viewer');
      viewer.setAttribute('plugin', 'Datagrid');
      this.node.appendChild(viewer);

      // Pass geometry metadata to the map plugin
      const geoCols = metadata.geometry_columns;
      const tileSources = metadata.tile_sources;
      if (geoCols && geoCols.length > 0) {
        viewer.setAttribute('data-geometry-columns', JSON.stringify(geoCols));
      }
      if (tileSources && tileSources.length > 0) {
        viewer.setAttribute('data-tile-sources', JSON.stringify(tileSources));
      }
      if (metadata.arrow_url) {
        viewer.setAttribute('data-arrow-url', rebuildArrowUrl(metadata.arrow_url));
      }

      this._arrowParts.push({ viewer, table, client, abortController });

      await (viewer as any).load(table);

      // Notify map plugin about geometry metadata
      const mapPlugin = viewer.querySelector('perspective-viewer-map') as any;
      if (mapPlugin && mapPlugin.setGeometryMeta) {
        mapPlugin.setGeometryMeta(geoCols || [], tileSources || [], metadata.arrow_url || null);
      }
    } catch (err: any) {
      const message = err?.message || 'Unknown error';
      if (message.includes('Result unavailable') || message.includes('Failed to connect') || message.includes('Failed to fetch')) {
        this._showExpired();
      } else {
        this._showError(`Failed to initialize viewer: ${message}`);
      }
    }
  }

  /** Clean up ArrowPartStates whose viewer is inside the given container. */
  private async _cleanupArrowPartsIn(container: HTMLElement): Promise<void> {
    const remaining: ArrowPartState[] = [];
    for (const part of this._arrowParts) {
      if (container.contains(part.viewer)) {
        part.abortController.abort();
        try { await (part.viewer as any).delete?.(); } catch { /* ignore */ }
        try { await part.table?.delete?.(); } catch { /* ignore */ }
        try { await part.client?.terminate?.(); } catch { /* ignore */ }
      } else {
        remaining.push(part);
      }
    }
    this._arrowParts = remaining;
  }

  /** Save pin state to output metadata — persists in .ipynb across page reload.
   *  Uses _skipRender guard to prevent re-render loop from setData callback. */
  private _savePinMetadata(pinned: boolean): void {
    if (!this._model) return;
    try {
      this._skipRender = true;
      const existing = (this._model.metadata['application/vnd.hugr.result+json'] as any) || {};
      this._model.setData({
        metadata: {
          ...this._model.metadata,
          'application/vnd.hugr.result+json': { ...existing, pinned },
        },
      });
    } catch { /* setData may not be available in all contexts */ }
    finally {
      this._skipRender = false;
    }
  }

  private async _cleanup(): Promise<void> {
    this._lazyRender = null;
    if (this._resizeObserver) { this._resizeObserver.disconnect(); this._resizeObserver = null; }
    for (const part of this._arrowParts) {
      part.abortController.abort();
      try { await (part.viewer as any).delete?.(); } catch { /* ignore */ }
      try { await part.table?.delete?.(); } catch { /* ignore */ }
      try { await part.client?.terminate?.(); } catch { /* ignore */ }
    }
    this._arrowParts = [];
  }

  dispose(): void {
    // Push pin state for FIFO restore (next widget picks it up on cell re-run)
    if (this._isPinned && this._prevQueryIds.length > 0) {
      _pendingPinRestore.push([...this._prevQueryIds]);
    }
    // Delete spool files when cell/widget is removed — skip pinned ones
    if (this._prevQueryIds.length > 0 && _lastKnownBaseUrl) {
      const dir = getNotebookDir() ?? undefined;
      for (const id of this._prevQueryIds) {
        if (!_pinnedQueryIds.has(id)) {
          fetch(buildSpoolUrl('delete', id, { kernelBaseUrl: _lastKnownBaseUrl!, dir }), getSpoolMutatingFetchInit('DELETE')).catch(() => {});
        }
      }
    }
    void this._cleanup();
    super.dispose();
  }

  private _showError(message: string): void {
    const div = document.createElement('div');
    div.className = 'hugr-result-error';
    div.textContent = message;
    this.node.replaceChildren(div);
  }

  private _showExpired(): void {
    const div = document.createElement('div');
    div.className = 'hugr-result-expired';
    div.textContent = 'Result expired. Re-run the cell to refresh.';
    this.node.replaceChildren(div);
  }

  /** Create a Pin/Unpin toggle button. getQueryId returns the query ID from the arrow URL. */
  private _createPinButton(getQueryId: () => string | null): HTMLButtonElement {
    const btn = document.createElement('button');
    btn.className = 'hugr-pin-btn';

    const updateLabel = () => {
      const src = this._isPinned ? `${ICONS_BASE}/unpin.png` : `${ICONS_BASE}/pin.png`;
      const title = this._isPinned ? 'Unpin results' : 'Pin results';
      btn.innerHTML = `<img src="${src}" class="hugr-btn-icon" alt="${title}">`;
      btn.title = title;
      btn.classList.toggle('hugr-pin-btn-active', this._isPinned);
    };
    updateLabel();

    btn.addEventListener('click', async () => {
      const base = _lastKnownBaseUrl;
      if (!base) return;
      const qid = getQueryId();
      if (!qid) return;

      btn.disabled = true;
      const wasPinned = this._isPinned;
      const endpoint = wasPinned ? 'unpin' : 'pin';

      try {
        const dir = getNotebookDir() ?? undefined;
        const url = buildSpoolUrl(endpoint as 'pin' | 'unpin', qid, { kernelBaseUrl: base, dir });
        const resp = await fetch(url, getSpoolMutatingFetchInit('POST'));
        if (resp.ok) {
          this._isPinned = !wasPinned;
          if (this._isPinned) {
            _pinnedQueryIds.add(qid);
          } else {
            _pinnedQueryIds.delete(qid);
          }
          this._savePinMetadata(this._isPinned);
          updateLabel();
        } else {
          btn.textContent = 'Error';
          setTimeout(() => { updateLabel(); btn.disabled = false; }, 2000);
          return;
        }
      } catch {
        btn.textContent = 'Error';
        setTimeout(() => { updateLabel(); btn.disabled = false; }, 2000);
        return;
      }
      btn.disabled = false;
    });

    return btn;
  }
}

// ─── JSON Raw View helpers ──────────────────────────────────────────

interface JsonLine {
  indent: string;
  tokens: { cls: string; text: string }[];
  foldable: boolean;
  foldEnd?: number;
  bracketId?: number;
}

function findClosingQuote(s: string, start: number): number {
  let i = start + 1;
  while (i < s.length) {
    if (s[i] === '\\') { i += 2; continue; }
    if (s[i] === '"') return i;
    i++;
  }
  return s.length - 1;
}

function tokenizeJson(data: any): JsonLine[] {
  let jsonStr: string;
  try {
    jsonStr = JSON.stringify(data, null, 2);
  } catch {
    jsonStr = String(data);
  }

  const rawLines = jsonStr.split('\n');
  const lines: JsonLine[] = [];
  let bracketCounter = 0;

  for (const raw of rawLines) {
    const indent = raw.match(/^(\s*)/)?.[1] ?? '';
    const content = raw.slice(indent.length);
    const tokens: { cls: string; text: string }[] = [];
    let pos = 0;

    while (pos < content.length) {
      const ch = content[pos];

      if (ch === '"') {
        const endQuote = findClosingQuote(content, pos);
        const after = content.slice(endQuote + 1).trimStart();
        const str = content.slice(pos, endQuote + 1);

        if (after.startsWith(':')) {
          tokens.push({ cls: 'hugr-json-key', text: str });
          pos = endQuote + 1;
          const colonMatch = content.slice(pos).match(/^(\s*:\s*)/);
          if (colonMatch) {
            tokens.push({ cls: '', text: colonMatch[1] });
            pos += colonMatch[1].length;
          }
        } else {
          tokens.push({ cls: 'hugr-json-string', text: str });
          pos = endQuote + 1;
        }
      } else if (ch === '{' || ch === '[') {
        tokens.push({ cls: 'hugr-json-bracket', text: ch });
        pos++;
      } else if (ch === '}' || ch === ']') {
        tokens.push({ cls: 'hugr-json-bracket', text: ch });
        pos++;
      } else if (/[0-9\-]/.test(ch)) {
        const numMatch = content.slice(pos).match(/^-?[0-9]+\.?[0-9]*([eE][+-]?[0-9]+)?/);
        if (numMatch) {
          tokens.push({ cls: 'hugr-json-number', text: numMatch[0] });
          pos += numMatch[0].length;
        } else {
          tokens.push({ cls: '', text: ch });
          pos++;
        }
      } else if (content.slice(pos, pos + 4) === 'true') {
        tokens.push({ cls: 'hugr-json-bool', text: 'true' });
        pos += 4;
      } else if (content.slice(pos, pos + 5) === 'false') {
        tokens.push({ cls: 'hugr-json-bool', text: 'false' });
        pos += 5;
      } else if (content.slice(pos, pos + 4) === 'null') {
        tokens.push({ cls: 'hugr-json-null', text: 'null' });
        pos += 4;
      } else {
        tokens.push({ cls: '', text: ch });
        pos++;
      }
    }

    const foldable = /[{\[]$/.test(content.trimEnd().replace(/,\s*$/, ''));
    lines.push({ indent, tokens, foldable });
  }

  // Match bracket pairs for folding
  const stack: { lineIdx: number; id: number }[] = [];
  for (let i = 0; i < lines.length; i++) {
    const content = lines[i].tokens.map(t => t.text).join('').trim();
    if (/[{\[]\s*$/.test(content)) {
      const id = bracketCounter++;
      lines[i].bracketId = id;
      stack.push({ lineIdx: i, id });
    }
    if (/^[}\]]/.test(content) && stack.length > 0) {
      const open = stack.pop()!;
      lines[open.lineIdx].foldEnd = i;
      lines[i].bracketId = open.id;
    }
  }

  return lines;
}

export function buildJsonRawView(data: any, container: HTMLElement): void {
  const lines = tokenizeJson(data);

  const gutter = document.createElement('div');
  gutter.className = 'hugr-json-gutter';

  const code = document.createElement('div');
  code.className = 'hugr-json-code';

  const gutterLines: HTMLElement[] = [];
  const codeLines: HTMLElement[] = [];
  const bracketSpans = new Map<number, HTMLElement[]>();

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    const gutterLine = document.createElement('span');
    gutterLine.className = 'hugr-json-gutter-line';
    gutterLine.textContent = String(i + 1);
    gutter.appendChild(gutterLine);
    gutterLines.push(gutterLine);

    const codeLine = document.createElement('span');
    codeLine.className = 'hugr-json-code-line';

    if (line.foldable && line.foldEnd != null) {
      const foldBtn = document.createElement('span');
      foldBtn.className = 'hugr-json-fold-btn';
      foldBtn.textContent = '\u25BC';
      const foldStart = i;
      const foldEndIdx = line.foldEnd;
      foldBtn.addEventListener('click', () => {
        toggleFold(foldBtn, foldStart, foldEndIdx, gutterLines, codeLines, lines);
      });
      codeLine.appendChild(foldBtn);
    } else {
      const spacer = document.createElement('span');
      spacer.className = 'hugr-json-fold-btn';
      spacer.textContent = ' ';
      codeLine.appendChild(spacer);
    }

    if (line.indent) {
      codeLine.appendChild(document.createTextNode(line.indent));
    }

    for (const token of line.tokens) {
      const span = document.createElement('span');
      if (token.cls) span.className = token.cls;
      span.textContent = token.text;

      if (token.cls === 'hugr-json-bracket' && line.bracketId != null) {
        let arr = bracketSpans.get(line.bracketId);
        if (!arr) { arr = []; bracketSpans.set(line.bracketId, arr); }
        arr.push(span);
      }

      codeLine.appendChild(span);
    }

    code.appendChild(codeLine);
    codeLines.push(codeLine);
  }

  // Bracket matching on hover
  let currentHighlight: HTMLElement[] = [];
  code.addEventListener('mouseover', (e) => {
    const target = e.target as HTMLElement;
    if (!target.classList.contains('hugr-json-bracket')) return;

    for (const el of currentHighlight) el.classList.remove('hugr-json-bracket-highlight');
    currentHighlight = [];

    for (const [, spans] of bracketSpans) {
      if (spans.includes(target)) {
        for (const s of spans) s.classList.add('hugr-json-bracket-highlight');
        currentHighlight = spans;
        break;
      }
    }
  });

  code.addEventListener('mouseleave', () => {
    for (const el of currentHighlight) el.classList.remove('hugr-json-bracket-highlight');
    currentHighlight = [];
  });

  container.appendChild(gutter);
  container.appendChild(code);
}

function toggleFold(
  btn: HTMLElement,
  startLine: number,
  endLine: number,
  gutterLines: HTMLElement[],
  codeLines: HTMLElement[],
  lines: JsonLine[],
): void {
  const isCollapsed = btn.textContent === '\u25B6';

  if (isCollapsed) {
    btn.textContent = '\u25BC';
    for (let i = startLine + 1; i <= endLine; i++) {
      gutterLines[i].style.display = '';
      codeLines[i].style.display = '';
    }
    const placeholder = codeLines[startLine].querySelector('.hugr-json-fold-placeholder');
    if (placeholder) placeholder.remove();
  } else {
    btn.textContent = '\u25B6';
    for (let i = startLine + 1; i <= endLine; i++) {
      gutterLines[i].style.display = 'none';
      codeLines[i].style.display = 'none';
    }
    const hiddenCount = endLine - startLine - 1;
    const placeholder = document.createElement('span');
    placeholder.className = 'hugr-json-fold-placeholder';
    placeholder.textContent = ` ... ${hiddenCount} lines `;
    placeholder.addEventListener('click', () => {
      toggleFold(btn, startLine, endLine, gutterLines, codeLines, lines);
    });
    codeLines[startLine].appendChild(placeholder);
  }
}
