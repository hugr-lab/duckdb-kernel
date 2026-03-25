/**
 * Perspective viewer widget for opening Arrow data in a JupyterLab main area tab.
 */

import { Widget } from '@lumino/widgets';
import { Message } from '@lumino/messaging';
import {
  loadPerspective,
  registerMapPlugin,
  streamArrowToTable,
  type GeometryColumnMeta,
  type TileSourceMeta,
} from '@hugr-lab/perspective-core';
import { getSpoolFetchInit, getBaseUrl } from './spoolUrl';

function getStaticBase(): string {
  return `${getBaseUrl()}lab/extensions/@hugr-lab/perspective-viewer/static/perspective`;
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
      const perspective = await loadPerspective(getStaticBase(), registerMapPlugin);

      this._abortController = new AbortController();
      this._client = await perspective.worker();
      this._table = await streamArrowToTable(
        this._arrowUrl,
        this._client,
        getSpoolFetchInit(this._abortController.signal),
        this._abortController.signal,
      );

      this.node.innerHTML = '';

      const viewer = document.createElement('perspective-viewer');
      viewer.setAttribute('plugin', 'Datagrid');
      viewer.style.width = '100%';
      viewer.style.height = '100%';

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
