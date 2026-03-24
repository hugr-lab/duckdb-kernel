/**
 * Perspective viewer widget for opening Arrow data in a JupyterLab main area tab.
 */

import { Widget } from '@lumino/widgets';
import { Message } from '@lumino/messaging';
import { registerMapPlugin } from './map-plugin';

const STATIC_BASE = '/lab/extensions/@hugr-lab/perspective-viewer/static/perspective';

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
    await registerMapPlugin();
    return perspective;
  })();
  _perspectiveReady.catch(() => {
    _perspectiveReady = null;
  });
  return _perspectiveReady;
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
  const { getSpoolFetchInit } = await import('./spoolUrl.js');
  const response = await fetch(arrowUrl, getSpoolFetchInit(signal));
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

/** Geometry column metadata. */
interface GeometryColumnMeta {
  name: string;
  srid: number;
  format: string;
}

/** Tile source configuration. */
interface TileSourceMeta {
  name: string;
  url: string;
  type: string;
  attribution?: string;
  min_zoom?: number;
  max_zoom?: number;
}

export class PerspectiveTabWidget extends Widget {
  private _arrowUrl: string;
  private _title: string;
  private _geometryColumns: GeometryColumnMeta[];
  private _tileSources: TileSourceMeta[];
  private _viewer: HTMLElement | null = null;
  private _table: any = null;
  private _client: any = null;
  private _abortController: AbortController | null = null;

  constructor(
    arrowUrl: string,
    title?: string,
    geometryColumns?: GeometryColumnMeta[],
    tileSources?: TileSourceMeta[],
  ) {
    super();
    this._arrowUrl = arrowUrl;
    this._title = title || 'Result';
    this._geometryColumns = geometryColumns || [];
    this._tileSources = tileSources || [];
    this.addClass('hugr-perspective-tab');
    this.node.style.width = '100%';
    this.node.style.height = '100%';
  }

  onAfterAttach(_msg: Message): void {
    void this._loadViewer();
  }

  private async _loadViewer(): Promise<void> {
    this.node.innerHTML = '<div class="hugr-result-loading">Loading viewer...</div>';

    try {
      const perspective = await loadPerspective();

      this._abortController = new AbortController();
      this._client = await perspective.worker();
      this._table = await streamArrowToTable(
        this._arrowUrl,
        this._client,
        this._abortController.signal,
      );

      this.node.innerHTML = '';

      const viewer = document.createElement('perspective-viewer');
      viewer.setAttribute('plugin', 'Datagrid');
      viewer.style.width = '100%';
      viewer.style.height = '100%';

      // Pass geometry and tile metadata for the map plugin
      if (this._geometryColumns.length > 0) {
        viewer.setAttribute('data-geometry-columns', JSON.stringify(this._geometryColumns));
      }
      if (this._tileSources.length > 0) {
        viewer.setAttribute('data-tile-sources', JSON.stringify(this._tileSources));
      }
      viewer.setAttribute('data-arrow-url', this._arrowUrl);

      this.node.appendChild(viewer);
      this._viewer = viewer;

      await (viewer as any).load(this._table);
    } catch (err: any) {
      this.node.innerHTML = '';
      const errorDiv = document.createElement('div');
      errorDiv.className = 'hugr-result-error';
      errorDiv.textContent = `Failed to load Arrow data: ${err?.message || 'Unknown error'}`;
      this.node.appendChild(errorDiv);
    }
  }

  dispose(): void {
    if (this._abortController) {
      this._abortController.abort();
    }
    if (this._viewer) {
      try { (this._viewer as any).delete?.(); } catch { /* ignore */ }
    }
    if (this._table) {
      try { this._table.delete?.(); } catch { /* ignore */ }
    }
    if (this._client) {
      try { this._client.terminate?.(); } catch { /* ignore */ }
    }
    this._viewer = null;
    this._table = null;
    this._client = null;
    this._abortController = null;
    super.dispose();
  }
}
