/**
 * Platform-agnostic multipart result renderer.
 *
 * Supports Arrow parts → Perspective tables, JSON parts → collapsible trees,
 * Error parts → error panels. All platform-specific behavior is injected
 * via the RenderContext interface.
 */

import type {
  MultipartMetadata,
  PartDef,
  RenderContext,
  RenderHandle,
  ArrowPartState,
} from './types.js';
import { formatNumber, formatBytes, escapeHtml, parseTruncation, buildFullUrl } from './utils.js';
import { streamArrowToTable, rebuildArrowUrl } from './streaming.js';
import { loadPerspective } from './perspective.js';
import { buildJsonTree, buildJsonRawView } from './json-view.js';
import { registerMapPlugin } from './map-plugin.js';

/**
 * Render a HUGR multipart result into the given context.
 * Returns a RenderHandle for cleanup/disposal.
 */
export async function renderHugrResult(
  metadata: MultipartMetadata,
  ctx: RenderContext,
): Promise<RenderHandle> {
  const arrowParts: ArrowPartState[] = [];
  let lazyRender: ((idx: number) => Promise<void>) | null = null;
  let resizeObserver: ResizeObserver | null = null;

  const cleanup = async () => {
    lazyRender = null;
    if (resizeObserver) { resizeObserver.disconnect(); resizeObserver = null; }
    for (const part of arrowParts) {
      part.abortController.abort();
      try { await (part.viewer as any).delete?.(); } catch { /* ignore */ }
      try { await part.table?.delete?.(); } catch { /* ignore */ }
      try { await part.client?.terminate?.(); } catch { /* ignore */ }
    }
    arrowParts.length = 0;
  };

  const handle: RenderHandle = {
    dispose: () => { void cleanup(); },
    arrowParts,
  };

  const container = ctx.container;

  // Multipart response with parts array
  if (metadata.parts && metadata.parts.length > 0) {
    await renderMultipart(metadata, ctx, arrowParts, (lr) => { lazyRender = lr; }, (ro) => { resizeObserver = ro; });
    return handle;
  }

  // Backward-compatible: single Arrow result via flat fields
  if (metadata.arrow_url) {
    await renderSingleArrow(metadata, ctx, arrowParts);
    return handle;
  }

  showError(container, 'No displayable results.');
  return handle;
}

// ─── Multipart Rendering ────────────────────────────────────────────

async function renderMultipart(
  metadata: MultipartMetadata,
  ctx: RenderContext,
  arrowParts: ArrowPartState[],
  setLazyRender: (fn: ((idx: number) => Promise<void>) | null) => void,
  setResizeObserver: (ro: ResizeObserver | null) => void,
): Promise<void> {
  const container = ctx.container;
  container.innerHTML = '';

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
      container.appendChild(banner);
    }
    const partContainer = document.createElement('div');
    partContainer.className = 'hugr-result-single';
    container.appendChild(partContainer);
    await renderPartContent(parts[0], partContainer, ctx, arrowParts);
    return;
  }

  // Tab bar wrapper (for overflow handling)
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
    btn.addEventListener('click', () => switchTab(tabButtons, tabPanels, idx, lazyRenderFn));
    tabBar.appendChild(btn);
    tabButtons.push(btn);

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
    showOverflowMenu(tabBar, tabButtons, tabPanels, moreBtn, lazyRenderFn);
  });

  tabBarWrap.appendChild(tabBar);
  tabBarWrap.appendChild(moreBtn);
  container.appendChild(tabBarWrap);

  // Global status
  const globalParts: string[] = [];
  globalParts.push(`${parts.length} results`);
  if (metadata.query_time_ms != null) {
    globalParts.push(`Query: ${metadata.query_time_ms} ms`);
  }
  const status = document.createElement('div');
  status.className = 'hugr-result-global-banner';
  status.textContent = globalParts.join(' \u00b7 ');
  container.appendChild(status);

  for (let i = 0; i < parts.length; i++) {
    container.appendChild(tabPanels[i]);
  }

  // Render first tab immediately, others lazily on tab switch
  const rendered = new Set<number>();
  rendered.add(0);
  await renderPartContent(parts[0], tabPanels[0], ctx, arrowParts);

  const lazyRenderFn = async (idx: number) => {
    if (rendered.has(idx)) return;
    rendered.add(idx);
    await renderPartContent(parts[idx], tabPanels[idx], ctx, arrowParts);
  };
  setLazyRender(lazyRenderFn);

  // Check overflow after render
  const checkOverflow = () => {
    moreBtn.style.display = tabBar.scrollWidth > tabBar.clientWidth ? '' : 'none';
  };
  checkOverflow();
  const observer = new ResizeObserver(() => checkOverflow());
  observer.observe(tabBarWrap);
  setResizeObserver(observer);
}

// ─── Tab Switching ──────────────────────────────────────────────────

function switchTab(
  buttons: HTMLElement[],
  panels: HTMLElement[],
  activeIdx: number,
  lazyRender: ((idx: number) => Promise<void>) | null,
): void {
  for (let i = 0; i < buttons.length; i++) {
    buttons[i].classList.toggle('hugr-tab-btn-active', i === activeIdx);
    panels[i].style.display = i === activeIdx ? '' : 'none';
  }
  if (lazyRender) {
    void lazyRender(activeIdx);
  }
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

function showOverflowMenu(
  _tabBar: HTMLElement,
  buttons: HTMLElement[],
  panels: HTMLElement[],
  moreBtn: HTMLElement,
  lazyRender: ((idx: number) => Promise<void>) | null,
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

// ─── Part Helpers ───────────────────────────────────────────────────

function partIcon(part: PartDef): string {
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

function partBadge(part: PartDef): string {
  if (part.type === 'arrow' && part.rows != null) {
    return `<span class="hugr-tab-badge">${formatNumber(part.rows)}</span>`;
  }
  if (part.type === 'error' && part.errors) {
    return `<span class="hugr-tab-badge hugr-tab-badge-error">${part.errors.length}</span>`;
  }
  return '';
}

// ─── Part Content Rendering ─────────────────────────────────────────

async function renderPartContent(
  part: PartDef,
  container: HTMLElement,
  ctx: RenderContext,
  arrowParts: ArrowPartState[],
): Promise<void> {
  switch (part.type) {
    case 'arrow':
      await renderArrowInto(part, container, ctx, arrowParts);
      break;
    case 'json':
      renderJsonInto(part, container, ctx);
      break;
    case 'error':
      renderErrorInto(part, container);
      break;
    default:
      renderJsonInto(part, container, ctx);
      break;
  }
}

async function renderArrowInto(
  part: PartDef,
  container: HTMLElement,
  ctx: RenderContext,
  arrowParts: ArrowPartState[],
): Promise<void> {
  if (!part.arrow_url) {
    const noData = document.createElement('div');
    noData.className = 'hugr-result-no-data';
    noData.textContent = part.rows === 0 ? '(no rows)' : 'No Arrow URL available.';
    container.appendChild(noData);
    return;
  }

  // Rewrite URL to use spool proxy if available
  part = { ...part, arrow_url: ctx.resolveArrowUrl(part.arrow_url!) };

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
        // Clean up existing arrow parts in this container
        await cleanupArrowPartsIn(container, arrowParts);
        container.innerHTML = '';
        const fullUrl = buildFullUrl(part.arrow_url!, ctx.resolveArrowUrl, (url) => rebuildArrowUrl(url, ctx.getKernelBaseUrl));
        await renderArrowInto(
          { ...part, arrow_url: fullUrl },
          container, ctx, arrowParts,
        );
      });
      banner.appendChild(btn);
    }

    const openTabBtn = document.createElement('button');
    openTabBtn.className = 'hugr-open-tab-btn';
    openTabBtn.innerHTML = `<img src="${ctx.iconsBase}/open-in-new-tab.png" class="hugr-btn-icon" alt="Open in Tab">`;
    openTabBtn.title = 'Open in Tab';
    openTabBtn.addEventListener('click', () => {
      const fullUrl = buildFullUrl(part.arrow_url!, ctx.resolveArrowUrl, (url) => rebuildArrowUrl(url, ctx.getKernelBaseUrl));
      ctx.onOpenInTab({
        arrow_url: fullUrl,
        base_url: ctx.getKernelBaseUrl() || '',
        title: part.title || part.id || 'Result',
        geometry_columns: part.geometry_columns || [],
        tile_sources: part.tile_sources || [],
      });
    });
    banner.appendChild(openTabBtn);

    // Pin/Unpin toggle button (only if platform supports it)
    if (ctx.savePinMetadata) {
      const pinBtn = createPinButton(ctx, () => {
        try {
          const u = new URL(part.arrow_url!);
          return u.searchParams.get('q');
        } catch { return null; }
      });
      banner.appendChild(pinBtn);
    }

    container.appendChild(banner);
  }

  const loading = document.createElement('div');
  loading.className = 'hugr-result-loading';
  loading.textContent = 'Loading viewer...';
  container.appendChild(loading);

  try {
    const perspective = await loadPerspective(ctx.staticBase, registerMapPlugin);
    const abortController = new AbortController();
    const client = await perspective.worker();
    const resolvedUrl = rebuildArrowUrl(part.arrow_url!, ctx.getKernelBaseUrl);
    const table = await streamArrowToTable(
      resolvedUrl, client,
      ctx.fetchInit(abortController.signal),
      abortController.signal,
      (url) => rebuildArrowUrl(url, ctx.getKernelBaseUrl),
    );

    loading.remove();

    const viewer = document.createElement('perspective-viewer');
    viewer.setAttribute('plugin', 'Datagrid');
    container.appendChild(viewer);

    const geoCols = part.geometry_columns;
    const tileSources = part.tile_sources;
    if (geoCols && geoCols.length > 0) {
      viewer.setAttribute('data-geometry-columns', JSON.stringify(geoCols));
    }
    if (tileSources && tileSources.length > 0) {
      viewer.setAttribute('data-tile-sources', JSON.stringify(tileSources));
    }
    if (part.arrow_url) {
      viewer.setAttribute('data-arrow-url', rebuildArrowUrl(part.arrow_url, ctx.getKernelBaseUrl));
    }

    arrowParts.push({ viewer, table, client, abortController });

    await (viewer as any).load(table);

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

function renderJsonInto(part: PartDef, container: HTMLElement, ctx: RenderContext): void {
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
  openTabBtn.innerHTML = `<img src="${ctx.iconsBase}/open-in-new-tab.png" class="hugr-btn-icon" alt="Open in Tab">`;
  openTabBtn.title = 'Open JSON in a separate tab';
  openTabBtn.addEventListener('click', () => {
    ctx.onOpenJsonInTab?.({
      json: part.data,
      title: part.title || part.id || 'JSON',
    });
  });
  toolbar.appendChild(openTabBtn);

  container.appendChild(toolbar);

  const tree = document.createElement('div');
  tree.className = 'hugr-json-tree';
  buildJsonTree(part.data, tree, true);
  container.appendChild(tree);

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

function renderErrorInto(part: PartDef, container: HTMLElement): void {
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

// ─── Single Arrow (backward-compatible) ─────────────────────────────

async function renderSingleArrow(
  metadata: MultipartMetadata,
  ctx: RenderContext,
  arrowParts: ArrowPartState[],
): Promise<void> {
  // Convert flat metadata to single-part multipart and delegate
  const part: PartDef = {
    id: metadata.query_id || 'result',
    type: 'arrow',
    title: metadata.query_id || 'Result',
    arrow_url: metadata.arrow_url,
    rows: metadata.rows,
    columns: metadata.columns,
    geometry_columns: metadata.geometry_columns,
    tile_sources: metadata.tile_sources,
    data_size_bytes: metadata.data_size_bytes,
  };

  if (metadata.query_time_ms != null) {
    const banner = document.createElement('div');
    banner.className = 'hugr-result-global-banner';
    const parts: string[] = [];
    if (metadata.query_time_ms != null) parts.push(`Query: ${metadata.query_time_ms} ms`);
    if (metadata.transfer_time_ms != null) parts.push(`Transfer: ${metadata.transfer_time_ms} ms`);
    banner.textContent = parts.join(' \u00b7 ');
    ctx.container.appendChild(banner);
  }

  const partContainer = document.createElement('div');
  partContainer.className = 'hugr-result-single';
  ctx.container.appendChild(partContainer);
  await renderArrowInto(part, partContainer, ctx, arrowParts);
}

// ─── Pin Button ─────────────────────────────────────────────────────

/** Create a Pin/Unpin toggle button. Platform-specific persistence via ctx.savePinMetadata. */
export function createPinButton(
  ctx: RenderContext,
  getQueryId: () => string | null,
  isPinnedRef?: { value: boolean },
  pinnedQueryIds?: Set<string>,
): HTMLButtonElement {
  const pinState = isPinnedRef || { value: false };
  const pqids = pinnedQueryIds || new Set<string>();

  const btn = document.createElement('button');
  btn.className = 'hugr-pin-btn';

  const updateLabel = () => {
    const src = pinState.value ? `${ctx.iconsBase}/unpin.png` : `${ctx.iconsBase}/pin.png`;
    const title = pinState.value ? 'Unpin results' : 'Pin results';
    btn.innerHTML = `<img src="${src}" class="hugr-btn-icon" alt="${title}">`;
    btn.title = title;
    btn.classList.toggle('hugr-pin-btn-active', pinState.value);
  };
  updateLabel();

  btn.addEventListener('click', async () => {
    const base = ctx.getKernelBaseUrl();
    if (!base) return;
    const qid = getQueryId();
    if (!qid) return;

    btn.disabled = true;
    const wasPinned = pinState.value;
    const endpoint = wasPinned ? 'unpin' : 'pin';

    try {
      const dir = ctx.getNotebookDir() ?? undefined;
      const opts: Record<string, string> = { kernelBaseUrl: base };
      if (dir) opts.dir = dir;
      const url = ctx.buildSpoolUrl(endpoint, qid, opts);
      const resp = await fetch(url, ctx.mutatingFetchInit('POST'));
      if (resp.ok) {
        pinState.value = !wasPinned;
        if (pinState.value) {
          pqids.add(qid);
        } else {
          pqids.delete(qid);
        }
        ctx.savePinMetadata?.(pinState.value);
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

// ─── Utility ────────────────────────────────────────────────────────

function showError(container: HTMLElement, message: string): void {
  const div = document.createElement('div');
  div.className = 'hugr-result-error';
  div.textContent = message;
  container.replaceChildren(div);
}

/** Clean up ArrowPartStates whose viewer is inside the given container. */
async function cleanupArrowPartsIn(container: HTMLElement, arrowParts: ArrowPartState[]): Promise<void> {
  const remaining: ArrowPartState[] = [];
  for (const part of arrowParts) {
    if (container.contains(part.viewer)) {
      part.abortController.abort();
      try { await (part.viewer as any).delete?.(); } catch { /* ignore */ }
      try { await part.table?.delete?.(); } catch { /* ignore */ }
      try { await part.client?.terminate?.(); } catch { /* ignore */ }
    } else {
      remaining.push(part);
    }
  }
  arrowParts.length = 0;
  arrowParts.push(...remaining);
}
