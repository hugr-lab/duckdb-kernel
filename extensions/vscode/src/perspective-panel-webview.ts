/**
 * Webview entry point for Perspective panel tab.
 * Bundled as IIFE by esbuild, loaded via <script src> in the webview.
 *
 * Expects window.__perspectivePanelConfig to be set before this script runs:
 *   { staticBase: string, arrowUrl: string, geometryColumns: any[], tileSources: any[] }
 */

import { streamArrowToTable, loadPerspective, registerMapPlugin } from '@hugr-lab/perspective-core';

interface PanelConfig {
  staticBase: string;
  arrowUrl: string;
  geometryColumns: any[];
  tileSources: any[];
}

function bufferToBase64(buf: ArrayBuffer): string {
  const bytes = new Uint8Array(buf);
  let binary = '';
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary);
}

async function main() {
  const config: PanelConfig = (window as any).__perspectivePanelConfig;
  if (!config) {
    console.error('[HUGR] No __perspectivePanelConfig found');
    return;
  }

  const vscode = (window as any).acquireVsCodeApi();
  const container = document.getElementById('container')!;
  const toolbar = document.getElementById('toolbar')!;
  const loadingEl = document.getElementById('loading')!;
  const errorEl = document.getElementById('error')!;
  const statusEl = document.getElementById('status')!;

  try {
    const perspective = await loadPerspective(config.staticBase, registerMapPlugin);
    const client = await perspective.worker();
    const table = await streamArrowToTable(
      config.arrowUrl, client, {}, undefined,
    );

    if (!table) {
      throw new Error('No data received from Arrow stream');
    }

    loadingEl.style.display = 'none';
    toolbar.style.display = 'flex';

    const size = await table.size();
    const schema = await table.schema();
    const cols = Object.keys(schema).length;
    statusEl.textContent = size.toLocaleString() + ' rows \u00b7 ' + cols + ' columns';

    const viewer = document.createElement('perspective-viewer');
    viewer.setAttribute('plugin', 'Datagrid');
    viewer.style.cssText = 'flex: 1; width: 100%; height: 100%;';
    container.appendChild(viewer);

    // Pass geometry metadata for the map plugin
    if (config.geometryColumns.length > 0) {
      viewer.setAttribute('data-geometry-columns', JSON.stringify(config.geometryColumns));
    }
    if (config.tileSources.length > 0) {
      viewer.setAttribute('data-tile-sources', JSON.stringify(config.tileSources));
    }
    if (config.arrowUrl) {
      viewer.setAttribute('data-arrow-url', config.arrowUrl);
    }

    await (viewer as any).load(table);

    // Save buttons — export from the viewer's current view
    document.getElementById('save-csv')!.addEventListener('click', async () => {
      statusEl.textContent = 'Exporting CSV...';
      try {
        const view = await (viewer as any).getView();
        const csv = await view.to_csv();
        vscode.postMessage({ type: 'save', format: 'csv', data: csv, binary: false });
      } catch {
        const view = await table.view();
        const csv = await view.to_csv();
        await view.delete();
        vscode.postMessage({ type: 'save', format: 'csv', data: csv, binary: false });
      }
      statusEl.textContent = size.toLocaleString() + ' rows \u00b7 ' + cols + ' columns';
    });

    document.getElementById('save-json')!.addEventListener('click', async () => {
      statusEl.textContent = 'Exporting JSON...';
      try {
        const view = await (viewer as any).getView();
        const json = await view.to_json();
        vscode.postMessage({ type: 'save', format: 'json', data: JSON.stringify(json, null, 2), binary: false });
      } catch {
        const view = await table.view();
        const json = await view.to_json();
        await view.delete();
        vscode.postMessage({ type: 'save', format: 'json', data: JSON.stringify(json, null, 2), binary: false });
      }
      statusEl.textContent = size.toLocaleString() + ' rows \u00b7 ' + cols + ' columns';
    });

    document.getElementById('save-arrow')!.addEventListener('click', async () => {
      statusEl.textContent = 'Exporting Arrow...';
      try {
        const view = await (viewer as any).getView();
        const arrow = await view.to_arrow();
        vscode.postMessage({ type: 'save', format: 'arrow', data: bufferToBase64(arrow), binary: true });
      } catch {
        const view = await table.view();
        const arrow = await view.to_arrow();
        await view.delete();
        vscode.postMessage({ type: 'save', format: 'arrow', data: bufferToBase64(arrow), binary: true });
      }
      statusEl.textContent = size.toLocaleString() + ' rows \u00b7 ' + cols + ' columns';
    });

  } catch (err: any) {
    loadingEl.style.display = 'none';
    errorEl.style.display = 'block';
    errorEl.textContent = 'Failed to load viewer: ' + (err.message || String(err));
  }
}

main();
