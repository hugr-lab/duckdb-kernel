/**
 * Perspective viewer widget for HUGR result metadata.
 *
 * Supports multipart responses: renders each part on its own tab.
 * Arrow parts → Perspective tables, JSON parts → collapsible trees,
 * Error parts → error panels.
 */

import { IRenderMime } from '@jupyterlab/rendermime-interfaces';
import { Widget } from '@lumino/widgets';

const MIME_TYPE = 'application/vnd.hugr.result+json';

const STATIC_BASE = '/lab/extensions/@hugr-lab/perspective-viewer/static/perspective';

/** Backward-compatible flat metadata (single Arrow result). */
interface FlatMetadata {
  query_id?: string;
  arrow_url?: string;
  rows?: number;
  columns?: { name: string; type: string }[];
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
    const url = new URL(arrowUrl);
    url.searchParams.delete('limit');
    url.searchParams.delete('total');
    return url.toString();
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
  const response = await fetch(arrowUrl, { signal });
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
  private _arrowParts: ArrowPartState[] = [];

  constructor(options: IRenderMime.IRendererOptions) {
    super();
    this._mimeType = options.mimeType;
    this.addClass('hugr-result-viewer');
  }

  async renderModel(model: IRenderMime.IMimeModel): Promise<void> {
    const metadata = model.data[this._mimeType] as unknown as MultipartMetadata;
    if (!metadata) {
      this._showError('No result metadata available.');
      return;
    }

    // Multipart response with parts array
    if (metadata.parts && metadata.parts.length > 0) {
      await this._renderMultipart(metadata);
      return;
    }

    // Backward-compatible: single Arrow result via flat fields
    if (metadata.arrow_url) {
      await this._renderSingleArrow(metadata);
      return;
    }

    this._showError('No displayable results.');
  }

  /** Render multipart response as tabs — one tab per part. */
  private async _renderMultipart(metadata: MultipartMetadata): Promise<void> {
    await this._cleanup();
    this.node.innerHTML = '';

    const parts = metadata.parts!;

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
    (this as any)._lazyRender = async (idx: number) => {
      if (rendered.has(idx)) return;
      rendered.add(idx);
      await this._renderPartContent(parts[idx], tabPanels[idx]);
    };

    // Check overflow after render
    this._checkTabOverflow(tabBar, moreBtn);
    const ro = new ResizeObserver(() => this._checkTabOverflow(tabBar, moreBtn));
    ro.observe(tabBarWrap);
    (this as any)._resizeObserver = ro;
  }

  /** Check if tabs overflow and show/hide more button. */
  private _checkTabOverflow(tabBar: HTMLElement, moreBtn: HTMLElement): void {
    moreBtn.style.display = tabBar.scrollWidth > tabBar.clientWidth ? '' : 'none';
  }

  /** Show dropdown menu for overflowed tabs. */
  private _showOverflowMenu(
    tabBar: HTMLElement,
    buttons: HTMLElement[],
    panels: HTMLElement[],
    moreBtn: HTMLElement,
  ): void {
    // Remove existing menu
    const existing = this.node.querySelector('.hugr-tabs-overflow-menu');
    if (existing) { existing.remove(); return; }

    const menu = document.createElement('div');
    menu.className = 'hugr-tabs-overflow-menu';

    const barRect = moreBtn.getBoundingClientRect();
    const parentRect = this.node.getBoundingClientRect();

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

    menu.style.top = `${barRect.bottom - parentRect.top}px`;
    menu.style.right = '0';
    this.node.appendChild(menu);

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
    const lazyRender = (this as any)._lazyRender;
    if (lazyRender) {
      lazyRender(activeIdx);
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

    const truncation = parseTruncation(part.arrow_url);

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
          container.innerHTML = '';
          await this._renderArrowInto(
            { ...part, arrow_url: buildFullUrl(part.arrow_url!) },
            container,
          );
        });
        banner.appendChild(btn);
      }

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
      const table = await streamArrowToTable(part.arrow_url, client, abortController.signal);

      loading.remove();

      const viewer = document.createElement('perspective-viewer');
      viewer.setAttribute('plugin', 'Datagrid');
      container.appendChild(viewer);

      this._arrowParts.push({ viewer, table, client, abortController });

      await (viewer as any).load(table);
    } catch (err: any) {
      loading.remove();
      const errorDiv = document.createElement('div');
      errorDiv.className = 'hugr-result-error';
      errorDiv.textContent = `Failed to load Arrow data: ${err?.message || 'Unknown error'}`;
      container.appendChild(errorDiv);
    }
  }

  /** Render JSON part as a collapsible tree with raw toggle. */
  private _renderJsonInto(part: PartDef, container: HTMLElement): void {
    // Toolbar with raw toggle
    const toolbar = document.createElement('div');
    toolbar.className = 'hugr-json-toolbar';

    const rawBtn = document.createElement('button');
    rawBtn.className = 'hugr-json-raw-btn';
    rawBtn.textContent = 'Raw';
    rawBtn.title = 'Toggle raw JSON';
    toolbar.appendChild(rawBtn);
    container.appendChild(toolbar);

    // Tree view
    const tree = document.createElement('div');
    tree.className = 'hugr-json-tree';
    this._buildJsonTree(part.data, tree, true);
    container.appendChild(tree);

    // Raw view (hidden)
    const raw = document.createElement('pre');
    raw.className = 'hugr-json-raw';
    raw.style.display = 'none';
    try {
      raw.textContent = JSON.stringify(part.data, null, 2);
    } catch {
      raw.textContent = String(part.data);
    }
    container.appendChild(raw);

    // Toggle
    let showingRaw = false;
    rawBtn.addEventListener('click', () => {
      showingRaw = !showingRaw;
      tree.style.display = showingRaw ? 'none' : '';
      raw.style.display = showingRaw ? '' : 'none';
      rawBtn.classList.toggle('hugr-json-raw-btn-active', showingRaw);
    });
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
    const arrowUrl = metadata.arrow_url!;
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

        this.node.appendChild(banner);
      }

      const viewer = document.createElement('perspective-viewer');
      viewer.setAttribute('plugin', 'Datagrid');
      this.node.appendChild(viewer);

      this._arrowParts.push({ viewer, table, client, abortController });

      await (viewer as any).load(table);
    } catch (err: any) {
      const message = err?.message || 'Unknown error';
      this._showError(`Failed to initialize viewer: ${message}`);
    }
  }

  private async _cleanup(): Promise<void> {
    (this as any)._lazyRender = null;
    const ro = (this as any)._resizeObserver as ResizeObserver | null;
    if (ro) { ro.disconnect(); (this as any)._resizeObserver = null; }
    for (const part of this._arrowParts) {
      part.abortController.abort();
      try { await (part.viewer as any).delete?.(); } catch { /* ignore */ }
      try { await part.table?.delete?.(); } catch { /* ignore */ }
      try { await part.client?.terminate?.(); } catch { /* ignore */ }
    }
    this._arrowParts = [];
  }

  dispose(): void {
    void this._cleanup();
    super.dispose();
  }

  private _showError(message: string): void {
    const div = document.createElement('div');
    div.className = 'hugr-result-error';
    div.textContent = message;
    this.node.replaceChildren(div);
  }
}
