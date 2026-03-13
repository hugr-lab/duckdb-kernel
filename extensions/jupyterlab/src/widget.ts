/**
 * Perspective viewer widget for HUGR result metadata.
 *
 * Streams Arrow IPC data from Go kernel's HTTP server chunk-by-chunk
 * into a Perspective table.
 */

import { IRenderMime } from '@jupyterlab/rendermime-interfaces';
import { Widget } from '@lumino/widgets';

const MIME_TYPE = 'application/vnd.hugr.result+json';

const STATIC_BASE = '/lab/extensions/@hugr-lab/perspective-viewer/static/perspective';

interface ResultMetadata {
  query_id: string;
  arrow_url: string;
  rows: number;
  columns: { name: string; type: string }[];
  data_size_bytes?: number;
  query_time_ms?: number;
  transfer_time_ms?: number;
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

      // Ensure we have at least 4 bytes for the length prefix.
      while (buffer.length < 4) {
        const { done, value } = await reader.read();
        if (done) return table;
        buffer = concatBuffers(buffer, value);
      }

      // Read chunk length (little-endian uint32).
      const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
      const chunkLen = view.getUint32(0, true);

      // Zero length = end of stream.
      if (chunkLen === 0) break;

      // Read the full chunk.
      while (buffer.length < 4 + chunkLen) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer = concatBuffers(buffer, value);
      }

      // Verify we got the complete chunk.
      if (buffer.length < 4 + chunkLen) {
        break; // Incomplete chunk — stream ended prematurely.
      }

      // Extract the Arrow IPC chunk.
      const chunk = buffer.slice(4, 4 + chunkLen);
      buffer = buffer.slice(4 + chunkLen);

      // Feed to perspective.
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

export class HugrResultWidget extends Widget implements IRenderMime.IRenderer {
  private _mimeType: string;
  private _viewer: HTMLElement | null = null;
  private _table: any = null;
  private _client: any = null;
  private _abortController: AbortController | null = null;

  constructor(options: IRenderMime.IRendererOptions) {
    super();
    this._mimeType = options.mimeType;
    this.addClass('hugr-result-viewer');
  }

  async renderModel(model: IRenderMime.IMimeModel): Promise<void> {
    const metadata = model.data[this._mimeType] as unknown as ResultMetadata;
    if (!metadata || !metadata.arrow_url) {
      this._showError('No result metadata available.');
      return;
    }

    await this._loadViewer(metadata.arrow_url, metadata);
  }

  private async _loadViewer(arrowUrl: string, metadata?: ResultMetadata): Promise<void> {
    const truncation = parseTruncation(arrowUrl);

    this.node.innerHTML = '<div class="hugr-result-loading">Loading viewer...</div>';

    try {
      const perspective = await loadPerspective();

      await this._cleanup();

      this._abortController = new AbortController();
      this._client = await perspective.worker();
      this._table = await streamArrowToTable(arrowUrl, this._client, this._abortController.signal);

      // Build UI.
      this.node.innerHTML = '';

      // Status bar: rows info + kernel memory.
      const statusParts: string[] = [];
      if (truncation.truncated) {
        statusParts.push(`Showing ${formatNumber(truncation.limit)} of ${formatNumber(truncation.total)} rows`);
      } else if (metadata?.rows) {
        statusParts.push(`${formatNumber(metadata.rows)} rows`);
      }
      if (metadata?.data_size_bytes != null && metadata.data_size_bytes > 0) {
        statusParts.push(formatBytes(metadata.data_size_bytes));
      }
      if (metadata?.query_time_ms != null) {
        statusParts.push(`Query: ${metadata.query_time_ms} ms`);
      }
      if (metadata?.transfer_time_ms != null) {
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
            this._loadViewer(buildFullUrl(arrowUrl), metadata);
          });
          banner.appendChild(btn);
        }

        this.node.appendChild(banner);
      }

      const viewer = document.createElement('perspective-viewer');
      viewer.setAttribute('plugin', 'Datagrid');
      this.node.appendChild(viewer);
      this._viewer = viewer;

      await (viewer as any).load(this._table);
    } catch (err: any) {
      const message = err?.message || 'Unknown error';
      this._showError(`Failed to initialize viewer: ${message}`);
    }
  }

  private async _cleanup(): Promise<void> {
    if (this._abortController) {
      this._abortController.abort();
      this._abortController = null;
    }
    if (this._viewer) {
      try {
        await (this._viewer as any).delete();
      } catch {
        // Ignore cleanup errors.
      }
      this._viewer = null;
    }
    if (this._table) {
      try {
        await this._table.delete();
      } catch {
        // Ignore cleanup errors.
      }
      this._table = null;
    }
    if (this._client) {
      try {
        await this._client.terminate();
      } catch {
        // Ignore cleanup errors.
      }
      this._client = null;
    }
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
