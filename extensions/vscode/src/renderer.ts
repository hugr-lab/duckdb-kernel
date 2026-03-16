/**
 * VS Code notebook renderer for HUGR result metadata.
 *
 * Loads Perspective from the Go kernel's HTTP server and streams
 * Arrow IPC data into an interactive viewer.
 */

import type { ActivationFunction } from 'vscode-notebook-renderer';

interface ResultMetadata {
  query_id: string;
  arrow_url: string;
  base_url: string;
  rows: number;
  columns: { name: string; type: string }[];
  data_size_bytes?: number;
  query_time_ms?: number;
  transfer_time_ms?: number;
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

      // Verify we got the complete chunk.
      if (buffer.length < 4 + chunkLen) {
        break; // Incomplete chunk — stream ended prematurely.
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
    // Fetch and inject theme CSS inline (VS Code webview CSP may block <link>).
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

// Module-level map so renderViewer's "Load All" button can register new controllers.
const abortControllers = new Map<string, AbortController>();

export const activate: ActivationFunction = (_context) => {
  return {
    async renderOutputItem(data, element) {
      const metadata = data.json() as ResultMetadata;
      if (!metadata || !metadata.arrow_url) {
        const div = document.createElement('div');
        div.style.color = 'var(--vscode-errorForeground)';
        div.textContent = 'No result metadata available.';
        element.replaceChildren(div);
        return;
      }

      const outputId = (data as any).id ?? 'default';
      abortControllers.get(outputId)?.abort();
      const controller = new AbortController();
      abortControllers.set(outputId, controller);

      await renderViewer(element, metadata, metadata.arrow_url, controller.signal);
    },

    disposeOutputItem(id) {
      const ctrl = abortControllers.get(id ?? 'default');
      if (ctrl) {
        ctrl.abort();
        abortControllers.delete(id ?? 'default');
      }
    },
  };
};

async function renderViewer(
  element: HTMLElement,
  metadata: ResultMetadata,
  arrowUrl: string,
  signal: AbortSignal,
): Promise<void> {
  const truncation = parseTruncation(arrowUrl);

  element.innerHTML =
    '<div style="padding: 8px; color: var(--vscode-descriptionForeground);">Loading viewer...</div>';

  try {
    const perspective = await loadPerspective(metadata.base_url);

    if (signal.aborted) return;

    const client = await perspective.worker();
    const table = await streamArrowToTable(arrowUrl, client, signal);

    if (signal.aborted) {
      if (table) await table.delete();
      return;
    }

    element.innerHTML = '';

    // Status bar.
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
    if (metadata.transfer_time_ms != null) {
      statusParts.push(`Transfer: ${metadata.transfer_time_ms} ms`);
    }

    if (statusParts.length > 0 || truncation.truncated) {
      const banner = document.createElement('div');
      banner.style.cssText =
        'display: flex; align-items: center; gap: 12px; padding: 4px 8px; font-size: 12px; color: var(--vscode-descriptionForeground); border-bottom: 1px solid var(--vscode-panel-border);';

      const span = document.createElement('span');
      span.textContent = statusParts.join(' \u00b7 ');
      banner.appendChild(span);

      if (truncation.truncated) {
        const btn = document.createElement('button');
        btn.style.cssText =
          'padding: 2px 8px; font-size: 11px; cursor: pointer; background: var(--vscode-button-background); color: var(--vscode-button-foreground); border: none; border-radius: 2px;';
        btn.textContent = `Load all ${formatNumber(truncation.total)} rows`;
        btn.addEventListener('click', () => {
          btn.disabled = true;
          btn.textContent = 'Loading...';
          btn.style.opacity = '0.6';
          const newController = new AbortController();
          abortControllers.set('load-all', newController);
          renderViewer(element, metadata, buildFullUrl(arrowUrl), newController.signal);
        });
        banner.appendChild(btn);
      }

      element.appendChild(banner);
    }


    // Container with fixed height. Capture wheel events to prevent
    // notebook scroll when user scrolls inside the datagrid.
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
    const div = document.createElement('div');
    div.style.cssText = 'color: var(--vscode-errorForeground); padding: 8px;';
    div.textContent = `Failed to initialize viewer: ${err?.message || 'Unknown error'}`;
    element.replaceChildren(div);
  }
}
