/**
 * VS Code notebook renderer for HUGR result metadata.
 *
 * Supports multipart responses: renders each part on its own tab.
 * Arrow parts → Perspective tables, JSON parts → collapsible trees,
 * Error parts → error panels.
 */

import type { ActivationFunction } from 'vscode-notebook-renderer';

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

function escapeHtml(s: string): string {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function concatBuffers(a: Uint8Array, b: Uint8Array): Uint8Array {
  const result = new Uint8Array(a.length + b.length);
  result.set(a);
  result.set(b, a.length);
  return result;
}

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

let _perspectiveReady: Promise<any> | null = null;
let _perspectiveBaseUrl: string | null = null;

function loadPerspective(baseUrl: string): Promise<any> {
  if (_perspectiveReady && _perspectiveBaseUrl === baseUrl) {
    return _perspectiveReady;
  }
  _perspectiveBaseUrl = baseUrl;
  const staticBase = `${baseUrl}/static/perspective`;
  _perspectiveReady = (async () => {
    let perspective: any;
    const alreadyLoaded = customElements.get('perspective-viewer') !== undefined;
    if (alreadyLoaded) {
      perspective = await import(/* webpackIgnore: true */ `${staticBase}/perspective.js`);
    } else {
      [perspective] = await Promise.all([
        import(/* webpackIgnore: true */ `${staticBase}/perspective.js`),
        import(/* webpackIgnore: true */ `${staticBase}/perspective-viewer.js`),
        import(/* webpackIgnore: true */ `${staticBase}/perspective-viewer-datagrid.js`),
        import(/* webpackIgnore: true */ `${staticBase}/perspective-viewer-d3fc.js`),
      ]);
    }
    const themeTag = 'hugr-perspective-themes';
    if (!document.getElementById(themeTag)) {
      try {
        const cssResp = await fetch(`${staticBase}/themes.css`);
        if (cssResp.ok) {
          const cssText = await cssResp.text();
          const style = document.createElement('style');
          style.id = themeTag;
          style.textContent = cssText;
          document.head.appendChild(style);
        }
      } catch {
        // Theme CSS failed to load — viewer works without it.
      }
    }
    await customElements.whenDefined('perspective-viewer');
    return perspective;
  })();
  _perspectiveReady.catch(() => {
    _perspectiveReady = null;
    _perspectiveBaseUrl = null;
  });
  return _perspectiveReady;
}

/** Inject CSS once for tabs and result styling. */
function injectStyles(): void {
  const TAG = 'hugr-renderer-styles';
  if (document.getElementById(TAG)) return;
  const style = document.createElement('style');
  style.id = TAG;
  style.textContent = `
    .hugr-tabs-wrap {
      position: relative;
      display: flex;
      align-items: center;
      border-bottom: 1px solid var(--vscode-panel-border);
    }
    .hugr-tabs-bar {
      display: flex;
      overflow: hidden;
      flex: 1;
      gap: 0;
    }
    .hugr-tab-btn {
      display: flex;
      align-items: center;
      gap: 4px;
      padding: 4px 12px;
      font-size: 12px;
      background: none;
      border: none;
      border-bottom: 2px solid transparent;
      color: var(--vscode-descriptionForeground);
      cursor: pointer;
      white-space: nowrap;
      flex-shrink: 0;
    }
    .hugr-tab-btn:hover {
      color: var(--vscode-foreground);
      background: var(--vscode-list-hoverBackground);
    }
    .hugr-tab-btn-active {
      color: var(--vscode-foreground);
      border-bottom-color: var(--vscode-focusBorder);
    }
    .hugr-tab-icon { font-size: 11px; }
    .hugr-tab-badge {
      font-size: 10px;
      padding: 0 4px;
      border-radius: 8px;
      background: var(--vscode-badge-background);
      color: var(--vscode-badge-foreground);
    }
    .hugr-tab-badge-error {
      background: var(--vscode-errorForeground);
      color: #fff;
    }
    .hugr-tabs-more {
      padding: 4px 8px;
      font-size: 14px;
      background: none;
      border: none;
      color: var(--vscode-descriptionForeground);
      cursor: pointer;
      flex-shrink: 0;
    }
    .hugr-tabs-more:hover {
      color: var(--vscode-foreground);
    }
    .hugr-tabs-overflow-menu {
      position: absolute;
      z-index: 100;
      background: var(--vscode-dropdown-background);
      border: 1px solid var(--vscode-dropdown-border);
      border-radius: 4px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.2);
      max-height: 300px;
      overflow-y: auto;
      min-width: 160px;
    }
    .hugr-tabs-overflow-item {
      display: block;
      width: 100%;
      padding: 6px 12px;
      font-size: 12px;
      text-align: left;
      background: none;
      border: none;
      color: var(--vscode-dropdown-foreground);
      cursor: pointer;
    }
    .hugr-tabs-overflow-item:hover {
      background: var(--vscode-list-hoverBackground);
    }
    .hugr-tabs-overflow-item-active {
      font-weight: bold;
    }
    .hugr-result-banner {
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 4px 8px;
      font-size: 12px;
      color: var(--vscode-descriptionForeground);
      border-bottom: 1px solid var(--vscode-panel-border);
    }
    .hugr-result-load-all {
      padding: 2px 8px;
      font-size: 11px;
      cursor: pointer;
      background: var(--vscode-button-background);
      color: var(--vscode-button-foreground);
      border: none;
      border-radius: 2px;
    }
    .hugr-result-error {
      color: var(--vscode-errorForeground);
      padding: 8px;
    }
    .hugr-result-error-item {
      padding: 8px;
      border-bottom: 1px solid var(--vscode-panel-border);
    }
    .hugr-result-error-message {
      color: var(--vscode-errorForeground);
      font-weight: bold;
    }
    .hugr-result-error-path {
      font-size: 11px;
      color: var(--vscode-descriptionForeground);
      margin-top: 2px;
    }
    .hugr-result-error-extensions {
      font-size: 11px;
      margin-top: 4px;
      padding: 4px;
      background: var(--vscode-textBlockQuote-background);
      border-radius: 2px;
      overflow-x: auto;
    }
    .hugr-json-toolbar {
      padding: 4px 8px;
      border-bottom: 1px solid var(--vscode-panel-border);
    }
    .hugr-json-raw-btn {
      padding: 2px 8px;
      font-size: 11px;
      cursor: pointer;
      background: var(--vscode-button-secondaryBackground);
      color: var(--vscode-button-secondaryForeground);
      border: none;
      border-radius: 2px;
    }
    .hugr-json-raw-btn-active {
      background: var(--vscode-button-background);
      color: var(--vscode-button-foreground);
    }
    .hugr-json-tree {
      padding: 8px;
      font-family: var(--vscode-editor-font-family);
      font-size: 13px;
    }
    .hugr-json-raw {
      display: flex;
      font-family: var(--vscode-editor-font-family);
      font-size: 13px;
      overflow: auto;
      max-height: 600px;
      line-height: 1.5;
    }
    .hugr-json-gutter {
      flex-shrink: 0;
      padding: 4px 8px 4px 4px;
      text-align: right;
      color: var(--vscode-editorLineNumber-foreground, #858585);
      user-select: none;
      border-right: 1px solid var(--vscode-panel-border);
      min-width: 36px;
    }
    .hugr-json-gutter-line {
      display: block;
      height: 1.5em;
    }
    .hugr-json-code {
      flex: 1;
      padding: 4px 0 4px 8px;
      white-space: pre;
      overflow-x: auto;
    }
    .hugr-json-code-line {
      display: block;
      height: 1.5em;
    }
    .hugr-json-fold-btn {
      display: inline-block;
      width: 1em;
      text-align: center;
      cursor: pointer;
      user-select: none;
      color: var(--vscode-editorCodeLens-foreground, #999);
      font-size: 10px;
    }
    .hugr-json-fold-btn:hover {
      color: var(--vscode-foreground);
    }
    .hugr-json-fold-placeholder {
      color: var(--vscode-descriptionForeground);
      cursor: pointer;
      font-style: italic;
    }
    .hugr-json-fold-placeholder:hover {
      color: var(--vscode-foreground);
    }
    .hugr-json-bracket {
      color: var(--vscode-descriptionForeground);
    }
    .hugr-json-bracket-highlight {
      background: var(--vscode-editorBracketMatch-background, rgba(0,100,200,0.2));
      outline: 1px solid var(--vscode-editorBracketMatch-border, rgba(0,100,200,0.4));
      border-radius: 1px;
    }
    .hugr-json-key { color: var(--vscode-symbolIcon-propertyForeground, #9cdcfe); }
    .hugr-json-string { color: var(--vscode-debugTokenExpression-string, #ce9178); }
    .hugr-json-number { color: var(--vscode-debugTokenExpression-number, #b5cea8); }
    .hugr-json-bool { color: var(--vscode-debugTokenExpression-boolean, #569cd6); }
    .hugr-json-null { color: var(--vscode-descriptionForeground); font-style: italic; }
    .hugr-json-row { cursor: pointer; }
    .hugr-json-toggle { margin-right: 4px; font-size: 10px; user-select: none; }
    .hugr-json-summary { color: var(--vscode-descriptionForeground); }
    .hugr-json-children { padding-left: 16px; }
    .hugr-json-entry { line-height: 1.6; }
    .hugr-json-index { color: var(--vscode-descriptionForeground); }
    .hugr-result-loading {
      padding: 8px;
      color: var(--vscode-descriptionForeground);
    }
    .hugr-result-no-data {
      padding: 8px;
      color: var(--vscode-descriptionForeground);
      font-style: italic;
    }
  `;
  document.head.appendChild(style);
}

// Track active AbortControllers per output
const outputControllers = new Map<string, AbortController[]>();

function getOutputId(data: any): string {
  return data.id ?? 'default';
}

function cleanupOutput(outputId: string): void {
  const controllers = outputControllers.get(outputId);
  if (controllers) {
    for (const c of controllers) c.abort();
    outputControllers.delete(outputId);
  }
}

function trackController(outputId: string, controller: AbortController): void {
  let list = outputControllers.get(outputId);
  if (!list) {
    list = [];
    outputControllers.set(outputId, list);
  }
  list.push(controller);
}

// Renderer messaging for communicating with extension host
let _rendererMessaging: { postMessage(msg: unknown): void } | null = null;

export const activate: ActivationFunction = (context) => {
  _rendererMessaging = context.postMessage ? context : null;

  return {
    async renderOutputItem(data, element) {
      injectStyles();

      const metadata = data.json() as MultipartMetadata;
      const outputId = getOutputId(data);
      cleanupOutput(outputId);

      if (!metadata) {
        element.replaceChildren(makeError('No result metadata available.'));
        return;
      }

      // Multipart response with parts array
      if (metadata.parts && metadata.parts.length > 0) {
        await renderMultipart(element, metadata, outputId);
        return;
      }

      // Backward-compatible: single Arrow result via flat fields
      if (metadata.arrow_url && metadata.base_url) {
        const controller = new AbortController();
        trackController(outputId, controller);
        await renderSingleArrow(element, metadata, metadata.arrow_url, controller.signal);
        return;
      }

      element.replaceChildren(makeError('No displayable results.'));
    },

    disposeOutputItem(id) {
      cleanupOutput(id ?? 'default');
    },
  };
};

function makeError(message: string): HTMLElement {
  const div = document.createElement('div');
  div.className = 'hugr-result-error';
  div.textContent = message;
  return div;
}

// ─── Multipart rendering ────────────────────────────────────────────

async function renderMultipart(
  element: HTMLElement,
  metadata: MultipartMetadata,
  outputId: string,
): Promise<void> {
  element.innerHTML = '';
  const parts = metadata.parts!;

  // Single part — no tabs needed
  if (parts.length === 1) {
    if (metadata.query_time_ms != null) {
      const banner = document.createElement('div');
      banner.className = 'hugr-result-banner';
      banner.textContent = `Query: ${metadata.query_time_ms} ms`;
      element.appendChild(banner);
    }
    const container = document.createElement('div');
    element.appendChild(container);
    await renderPartContent(parts[0], container, metadata, outputId);
    return;
  }

  // Tab bar
  const tabBarWrap = document.createElement('div');
  tabBarWrap.className = 'hugr-tabs-wrap';

  const tabBar = document.createElement('div');
  tabBar.className = 'hugr-tabs-bar';

  const tabPanels: HTMLElement[] = [];
  const tabButtons: HTMLElement[] = [];

  for (let i = 0; i < parts.length; i++) {
    const part = parts[i];

    const btn = document.createElement('button');
    btn.className = 'hugr-tab-btn';
    if (i === 0) btn.classList.add('hugr-tab-btn-active');

    const label = part.title || part.id;
    btn.title = label;
    const icon = partIcon(part);
    const badge = partBadge(part);
    btn.innerHTML = `${icon}<span class="hugr-tab-label">${escapeHtml(label)}</span>${badge}`;

    const idx = i;
    btn.addEventListener('click', () => switchTab(tabButtons, tabPanels, idx, lazyRender));
    tabBar.appendChild(btn);
    tabButtons.push(btn);

    const panel = document.createElement('div');
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
    showOverflowMenu(tabBar, tabButtons, tabPanels, moreBtn, lazyRender);
  });

  tabBarWrap.appendChild(tabBar);
  tabBarWrap.appendChild(moreBtn);
  element.appendChild(tabBarWrap);

  // Global status
  const globalParts: string[] = [];
  globalParts.push(`${parts.length} results`);
  if (metadata.query_time_ms != null) {
    globalParts.push(`Query: ${metadata.query_time_ms} ms`);
  }
  const status = document.createElement('div');
  status.className = 'hugr-result-banner';
  status.textContent = globalParts.join(' \u00b7 ');
  element.appendChild(status);

  for (const panel of tabPanels) {
    element.appendChild(panel);
  }

  // Render first tab, others lazy
  const rendered = new Set<number>();
  rendered.add(0);
  await renderPartContent(parts[0], tabPanels[0], metadata, outputId);

  const lazyRender = async (idx: number) => {
    if (rendered.has(idx)) return;
    rendered.add(idx);
    await renderPartContent(parts[idx], tabPanels[idx], metadata, outputId);
  };

  // Check overflow
  const checkOverflow = () => {
    moreBtn.style.display = tabBar.scrollWidth > tabBar.clientWidth ? '' : 'none';
  };
  checkOverflow();
  const ro = new ResizeObserver(checkOverflow);
  ro.observe(tabBarWrap);
}

function switchTab(
  buttons: HTMLElement[],
  panels: HTMLElement[],
  activeIdx: number,
  lazyRender?: (idx: number) => Promise<void>,
): void {
  for (let i = 0; i < buttons.length; i++) {
    buttons[i].classList.toggle('hugr-tab-btn-active', i === activeIdx);
    panels[i].style.display = i === activeIdx ? '' : 'none';
  }
  if (lazyRender) {
    void lazyRender(activeIdx);
  }
}

function showOverflowMenu(
  _tabBar: HTMLElement,
  buttons: HTMLElement[],
  panels: HTMLElement[],
  moreBtn: HTMLElement,
  lazyRender?: (idx: number) => Promise<void>,
): void {
  const wrap = moreBtn.parentElement!;
  const existing = wrap.querySelector('.hugr-tabs-overflow-menu');
  if (existing) { existing.remove(); return; }

  const menu = document.createElement('div');
  menu.className = 'hugr-tabs-overflow-menu';

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
      switchTab(buttons, panels, idx, lazyRender);
      buttons[idx].scrollIntoView({ behavior: 'smooth', block: 'nearest', inline: 'nearest' });
    });
    menu.appendChild(item);
  }

  menu.style.top = '100%';
  menu.style.right = '0';
  wrap.appendChild(menu);

  const closeHandler = (e: MouseEvent) => {
    if (!menu.contains(e.target as Node) && e.target !== moreBtn) {
      menu.remove();
      document.removeEventListener('click', closeHandler);
    }
  };
  setTimeout(() => document.addEventListener('click', closeHandler), 0);
}

function partIcon(part: PartDef): string {
  if (part.id === 'extensions' || part.title === 'extensions') {
    return '<span class="hugr-tab-icon">\u2699</span>';
  }
  switch (part.type) {
    case 'arrow': return '<span class="hugr-tab-icon">\u{1F4CA}</span>';
    case 'json':  return '<span class="hugr-tab-icon">{}</span>';
    case 'error': return '<span class="hugr-tab-icon">\u26A0</span>';
    default:      return '<span class="hugr-tab-icon">\u{1F4C4}</span>';
  }
}

function partBadge(part: PartDef): string {
  if (part.type === 'arrow' && part.rows != null) {
    return `<span class="hugr-tab-badge">${formatNumber(part.rows)}</span>`;
  }
  if (part.type === 'error' && part.errors) {
    return `<span class="hugr-tab-badge hugr-tab-badge-error">${part.errors.length}</span>`;
  }
  return '';
}

// ─── Part renderers ────────────────────────────────────────────────

async function renderPartContent(
  part: PartDef,
  container: HTMLElement,
  metadata: MultipartMetadata,
  outputId: string,
): Promise<void> {
  switch (part.type) {
    case 'arrow':
      await renderArrowPart(part, container, metadata, outputId);
      break;
    case 'json':
      renderJsonPart(part, container);
      break;
    case 'error':
      renderErrorPart(part, container);
      break;
    default:
      renderJsonPart(part, container);
      break;
  }
}

async function renderArrowPart(
  part: PartDef,
  container: HTMLElement,
  metadata: MultipartMetadata,
  outputId: string,
): Promise<void> {
  if (!part.arrow_url) {
    const noData = document.createElement('div');
    noData.className = 'hugr-result-no-data';
    noData.textContent = part.rows === 0 ? '(no rows)' : 'No Arrow URL available.';
    container.appendChild(noData);
    return;
  }

  const baseUrl = metadata.base_url;
  if (!baseUrl) {
    container.appendChild(makeError('No base_url in metadata.'));
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
        await renderArrowPart(
          { ...part, arrow_url: buildFullUrl(part.arrow_url!) },
          container,
          metadata,
          outputId,
        );
      });
      banner.appendChild(btn);
    }

    // "Open in Tab" button — sends message to extension host
    if (_rendererMessaging) {
      const openBtn = document.createElement('button');
      openBtn.className = 'hugr-result-load-all';
      openBtn.textContent = 'Open in Tab';
      openBtn.addEventListener('click', () => {
        _rendererMessaging!.postMessage({
          type: 'open-in-tab',
          arrow_url: buildFullUrl(part.arrow_url!),
          base_url: metadata.base_url,
          title: part.title || part.id || 'Result',
        });
      });
      banner.appendChild(openBtn);
    }

    container.appendChild(banner);
  }

  const loading = document.createElement('div');
  loading.className = 'hugr-result-loading';
  loading.textContent = 'Loading viewer...';
  container.appendChild(loading);

  try {
    const perspective = await loadPerspective(baseUrl);
    const controller = new AbortController();
    trackController(outputId, controller);

    const client = await perspective.worker();
    const table = await streamArrowToTable(part.arrow_url, client, controller.signal);

    if (controller.signal.aborted) {
      if (table) await table.delete();
      return;
    }

    loading.remove();

    const viewContainer = document.createElement('div');
    viewContainer.style.cssText = 'position: relative; width: 100%; height: 500px; overflow: hidden;';
    viewContainer.addEventListener('wheel', (e) => e.stopPropagation(), { passive: false });
    viewContainer.addEventListener('touchmove', (e) => e.stopPropagation(), { passive: false });
    container.appendChild(viewContainer);

    const viewer = document.createElement('perspective-viewer');
    viewer.setAttribute('plugin', 'Datagrid');
    viewer.style.cssText = 'position: absolute; top: 0; left: 0; width: 100%; height: 100%;';
    viewContainer.appendChild(viewer);

    await (viewer as any).load(table);
  } catch (err: any) {
    loading.remove();
    container.appendChild(makeError(`Failed to load Arrow data: ${err?.message || 'Unknown error'}`));
  }
}

function renderJsonPart(part: PartDef, container: HTMLElement): void {
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

  if (_rendererMessaging) {
    const openBtn = document.createElement('button');
    openBtn.className = 'hugr-json-raw-btn';
    openBtn.textContent = 'Open in Tab';
    openBtn.title = 'Open JSON in a separate tab';
    openBtn.addEventListener('click', () => {
      _rendererMessaging!.postMessage({
        type: 'open-json-in-tab',
        data: part.data,
        title: part.title || part.id || 'JSON',
      });
    });
    toolbar.appendChild(openBtn);
  }

  container.appendChild(toolbar);

  // Tree view
  const tree = document.createElement('div');
  tree.className = 'hugr-json-tree';
  buildJsonTree(part.data, tree, true);
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

/** Tokenize JSON string into typed spans. */
interface JsonLine {
  indent: string;
  tokens: { cls: string; text: string }[];
  foldable: boolean;    // line has opening bracket
  foldEnd?: number;     // line index of matching closing bracket
  bracketId?: number;   // unique ID for bracket pair matching
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

  // First pass: build tokenized lines
  for (const raw of rawLines) {
    const indent = raw.match(/^(\s*)/)?.[1] ?? '';
    const content = raw.slice(indent.length);
    const tokens: { cls: string; text: string }[] = [];
    let pos = 0;

    while (pos < content.length) {
      const ch = content[pos];

      if (ch === '"') {
        // Check if it's a key (followed by : after closing quote)
        const endQuote = findClosingQuote(content, pos);
        const after = content.slice(endQuote + 1).trimStart();
        const str = content.slice(pos, endQuote + 1);

        if (after.startsWith(':')) {
          tokens.push({ cls: 'hugr-json-key', text: str });
          pos = endQuote + 1;
          // consume colon and space
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
        // comma, whitespace, etc
        tokens.push({ cls: '', text: ch });
        pos++;
      }
    }

    const foldable = /[{\[]$/.test(content.trimEnd().replace(/,\s*$/, ''));
    lines.push({ indent, tokens, foldable });
  }

  // Second pass: match bracket pairs for folding
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

function findClosingQuote(s: string, start: number): number {
  let i = start + 1;
  while (i < s.length) {
    if (s[i] === '\\') { i += 2; continue; }
    if (s[i] === '"') return i;
    i++;
  }
  return s.length - 1;
}

function buildJsonRawView(data: any, container: HTMLElement): void {
  const lines = tokenizeJson(data);

  const gutter = document.createElement('div');
  gutter.className = 'hugr-json-gutter';

  const code = document.createElement('div');
  code.className = 'hugr-json-code';

  const gutterLines: HTMLElement[] = [];
  const codeLines: HTMLElement[] = [];
  const bracketSpans = new Map<number, HTMLElement[]>(); // bracketId → spans

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    // Gutter line number
    const gutterLine = document.createElement('span');
    gutterLine.className = 'hugr-json-gutter-line';
    gutterLine.textContent = String(i + 1);
    gutter.appendChild(gutterLine);
    gutterLines.push(gutterLine);

    // Code line
    const codeLine = document.createElement('span');
    codeLine.className = 'hugr-json-code-line';

    // Fold button (or spacer)
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

    // Indent
    if (line.indent) {
      codeLine.appendChild(document.createTextNode(line.indent));
    }

    // Tokens
    for (const token of line.tokens) {
      const span = document.createElement('span');
      if (token.cls) span.className = token.cls;
      span.textContent = token.text;

      // Track bracket spans for highlighting
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

    // Clear previous
    for (const el of currentHighlight) el.classList.remove('hugr-json-bracket-highlight');
    currentHighlight = [];

    // Find which bracketId this span belongs to
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
    // Expand
    btn.textContent = '\u25BC';
    for (let i = startLine + 1; i <= endLine; i++) {
      gutterLines[i].style.display = '';
      codeLines[i].style.display = '';
    }
    // Remove placeholder if any
    const placeholder = codeLines[startLine].querySelector('.hugr-json-fold-placeholder');
    if (placeholder) placeholder.remove();
  } else {
    // Collapse
    btn.textContent = '\u25B6';
    for (let i = startLine + 1; i <= endLine; i++) {
      gutterLines[i].style.display = 'none';
      codeLines[i].style.display = 'none';
    }
    // Add placeholder showing collapsed info
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

function buildJsonTree(data: any, parent: HTMLElement, expanded: boolean): void {
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
    buildCollapsible(data, parent, expanded, true);
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
    buildCollapsible(data, parent, expanded, false);
    return;
  }

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

function buildCollapsible(data: any, parent: HTMLElement, expanded: boolean, isArray: boolean): void {
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
      buildJsonTree(data[i], entry, false);
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
      buildJsonTree(v, entry, false);
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

function renderErrorPart(part: PartDef, container: HTMLElement): void {
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

// ─── Backward-compatible single arrow ──────────────────────────────

async function renderSingleArrow(
  element: HTMLElement,
  metadata: FlatMetadata & { base_url?: string },
  arrowUrl: string,
  signal: AbortSignal,
): Promise<void> {
  const truncation = parseTruncation(arrowUrl);
  const baseUrl = (metadata as any).base_url;

  element.innerHTML =
    '<div class="hugr-result-loading">Loading viewer...</div>';

  try {
    const perspective = await loadPerspective(baseUrl);

    if (signal.aborted) return;

    const client = await perspective.worker();
    const table = await streamArrowToTable(arrowUrl, client, signal);

    if (signal.aborted) {
      if (table) await table.delete();
      return;
    }

    element.innerHTML = '';

    const statusParts: string[] = [];
    if (truncation.truncated) {
      statusParts.push(
        `Showing ${formatNumber(truncation.limit)} of ${formatNumber(truncation.total)} rows`,
      );
    } else if (metadata.rows) {
      statusParts.push(`${formatNumber(metadata.rows)} rows`);
    }
    if (metadata.data_size_bytes != null && metadata.data_size_bytes > 0) {
      statusParts.push(formatBytes(metadata.data_size_bytes));
    }
    if (metadata.query_time_ms != null) {
      statusParts.push(`Query: ${metadata.query_time_ms} ms`);
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
          const newController = new AbortController();
          trackController('load-all', newController);
          renderSingleArrow(element, metadata, buildFullUrl(arrowUrl), newController.signal);
        });
        banner.appendChild(btn);
      }

      if (_rendererMessaging && (metadata as any).base_url) {
        const openBtn = document.createElement('button');
        openBtn.className = 'hugr-result-load-all';
        openBtn.textContent = 'Open in Tab';
        openBtn.addEventListener('click', () => {
          _rendererMessaging!.postMessage({
            type: 'open-in-tab',
            arrow_url: buildFullUrl(arrowUrl),
            base_url: (metadata as any).base_url,
            title: metadata.query_id || 'Result',
          });
        });
        banner.appendChild(openBtn);
      }

      element.appendChild(banner);
    }

    const container = document.createElement('div');
    container.style.cssText = 'position: relative; width: 100%; height: 500px; overflow: hidden;';
    container.addEventListener('wheel', (e) => e.stopPropagation(), { passive: false });
    container.addEventListener('touchmove', (e) => e.stopPropagation(), { passive: false });
    element.appendChild(container);

    const viewer = document.createElement('perspective-viewer');
    viewer.setAttribute('plugin', 'Datagrid');
    viewer.style.cssText = 'position: absolute; top: 0; left: 0; width: 100%; height: 100%;';
    container.appendChild(viewer);

    await (viewer as any).load(table);
  } catch (err: any) {
    if (signal.aborted) return;
    element.replaceChildren(makeError(`Failed to initialize viewer: ${err?.message || 'Unknown error'}`));
  }
}
