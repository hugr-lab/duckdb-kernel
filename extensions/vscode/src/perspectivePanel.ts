/**
 * Webview panel that displays Perspective viewer with Arrow data
 * in a full-size VS Code editor tab.
 */

import * as vscode from 'vscode';

const activePanels = new Map<string, vscode.WebviewPanel>();

export function showPerspectivePanel(metadata: {
  query_id: string;
  arrow_url: string;
  base_url: string;
  geometry_columns?: any[];
  tile_sources?: any[];
}, extensionUri: vscode.Uri): void {
  const existing = activePanels.get(metadata.query_id);
  if (existing) {
    existing.reveal();
    return;
  }

  const title = `Result: ${metadata.query_id}`;
  const panel = vscode.window.createWebviewPanel(
    'duckdb.perspective',
    title,
    vscode.ViewColumn.Active,
    {
      enableScripts: true,
      retainContextWhenHidden: true,
      localResourceRoots: [vscode.Uri.joinPath(extensionUri, 'out')],
    },
  );

  const mapPluginUri = panel.webview.asWebviewUri(
    vscode.Uri.joinPath(extensionUri, 'out', 'map-plugin.js'),
  );

  activePanels.set(metadata.query_id, panel);
  panel.onDidDispose(() => {
    activePanels.delete(metadata.query_id);
  });

  // Handle messages from webview (save file requests)
  panel.webview.onDidReceiveMessage(async (msg) => {
    if (msg.type === 'save') {
      const filters: Record<string, string[]> = {};
      const ext = msg.format === 'csv' ? 'csv' : msg.format === 'json' ? 'json' : 'arrow';
      filters[ext.toUpperCase()] = [ext];

      const uri = await vscode.window.showSaveDialog({
        defaultUri: vscode.Uri.file(`${metadata.query_id}.${ext}`),
        filters,
      });
      if (uri) {
        // msg.data is a base64-encoded string for binary, or plain string for text
        const bytes = msg.binary
          ? Buffer.from(msg.data, 'base64')
          : Buffer.from(msg.data, 'utf-8');
        await vscode.workspace.fs.writeFile(uri, bytes);
        vscode.window.showInformationMessage(`Saved to ${uri.fsPath}`);
      }
    }
  });

  panel.webview.html = buildPerspectiveHtml(metadata, mapPluginUri);
}

function buildPerspectiveHtml(metadata: {
  query_id: string;
  arrow_url: string;
  base_url: string;
  geometry_columns?: any[];
  tile_sources?: any[];
}, mapPluginUri: vscode.Uri): string {
  const staticBase = `${metadata.base_url}/static/perspective`;
  const arrowUrl = metadata.arrow_url;

  return `<!DOCTYPE html>
<html lang="en" style="height: 100%; margin: 0;">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="Content-Security-Policy"
    content="default-src 'none';
      script-src 'unsafe-inline' 'unsafe-eval' http://127.0.0.1:* http://localhost:* https://*.vscode-resource.vscode-cdn.net;
      style-src 'unsafe-inline' http://127.0.0.1:* http://localhost:*;
      connect-src http://127.0.0.1:* http://localhost:* https://*.basemaps.cartocdn.com https://*.tile.openstreetmap.org https://*;
      font-src http://127.0.0.1:* http://localhost:*;
      img-src http://127.0.0.1:* http://localhost:* https://*.basemaps.cartocdn.com https://*.tile.openstreetmap.org https://* data:;
      worker-src blob:;">
  <style>
    html, body {
      height: 100%;
      margin: 0;
      padding: 0;
      overflow: hidden;
      background: var(--vscode-editor-background, #1e1e1e);
      color: var(--vscode-editor-foreground, #ccc);
      font-family: var(--vscode-font-family, sans-serif);
    }
    #container {
      width: 100%;
      height: 100%;
      display: flex;
      flex-direction: column;
    }
    #toolbar {
      display: none;
      align-items: center;
      gap: 8px;
      padding: 4px 8px;
      border-bottom: 1px solid var(--vscode-panel-border, #333);
      font-size: 12px;
    }
    #toolbar button {
      padding: 2px 10px;
      font-size: 11px;
      cursor: pointer;
      background: var(--vscode-button-secondaryBackground, #333);
      color: var(--vscode-button-secondaryForeground, #ccc);
      border: none;
      border-radius: 2px;
    }
    #toolbar button:hover {
      background: var(--vscode-button-secondaryHoverBackground, #444);
    }
    #toolbar .status {
      color: var(--vscode-descriptionForeground, #888);
      margin-left: auto;
    }
    #loading {
      display: flex;
      align-items: center;
      justify-content: center;
      height: 100%;
      font-size: 14px;
      color: var(--vscode-descriptionForeground, #888);
      font-style: italic;
    }
    #error {
      display: none;
      padding: 16px;
      color: var(--vscode-errorForeground, #f44);
    }
    perspective-viewer {
      flex: 1;
      width: 100%;
    }
  </style>
</head>
<body>
  <div id="container">
    <div id="toolbar">
      <button id="save-csv">Save CSV</button>
      <button id="save-json">Save JSON</button>
      <button id="save-arrow">Save Arrow</button>
      <span class="status" id="status"></span>
    </div>
    <div id="loading">Loading Perspective viewer...</div>
    <div id="error"></div>
  </div>
  <script src="${mapPluginUri}"><\/script>
  <script type="module">
    const vscode = acquireVsCodeApi();
    const staticBase = ${JSON.stringify(staticBase)};
    const arrowUrl = ${JSON.stringify(arrowUrl)};

    function concatBuffers(a, b) {
      const result = new Uint8Array(a.length + b.length);
      result.set(a);
      result.set(b, a.length);
      return result;
    }

    async function streamArrowToTable(url, worker) {
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error('Failed to fetch Arrow data (HTTP ' + response.status + ')');
      }
      if (!response.body) {
        throw new Error('Response body is null');
      }

      const reader = response.body.getReader();
      let buffer = new Uint8Array(0);
      let table = null;

      try {
        while (true) {
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

          if (buffer.length < 4 + chunkLen) break;

          const chunk = buffer.slice(4, 4 + chunkLen);
          buffer = buffer.slice(4 + chunkLen);

          if (table === null) {
            table = await worker.table(chunk.buffer);
          } else {
            await table.update(chunk.buffer);
          }
        }
      } finally {
        reader.releaseLock();
      }

      return table;
    }

    async function loadPerspective() {
      const alreadyLoaded = customElements.get('perspective-viewer') !== undefined;
      let perspective;
      if (alreadyLoaded) {
        perspective = await import(staticBase + '/perspective.js');
      } else {
        [perspective] = await Promise.all([
          import(staticBase + '/perspective.js'),
          import(staticBase + '/perspective-viewer.js'),
          import(staticBase + '/perspective-viewer-datagrid.js'),
          import(staticBase + '/perspective-viewer-d3fc.js'),
        ]);
      }

      try {
        const cssResp = await fetch(staticBase + '/themes.css');
        if (cssResp.ok) {
          const cssText = await cssResp.text();
          const style = document.createElement('style');
          style.textContent = cssText;
          document.head.appendChild(style);
        }
      } catch {}

      await customElements.whenDefined('perspective-viewer');
      return perspective;
    }

    // Convert ArrayBuffer to base64
    function bufferToBase64(buf) {
      const bytes = new Uint8Array(buf);
      let binary = '';
      for (let i = 0; i < bytes.length; i++) {
        binary += String.fromCharCode(bytes[i]);
      }
      return btoa(binary);
    }

    async function main() {
      const container = document.getElementById('container');
      const toolbar = document.getElementById('toolbar');
      const loadingEl = document.getElementById('loading');
      const errorEl = document.getElementById('error');
      const statusEl = document.getElementById('status');

      try {
        const perspective = await loadPerspective();
        const client = await perspective.worker();
        const table = await streamArrowToTable(arrowUrl, client);

        if (!table) {
          throw new Error('No data received from Arrow stream');
        }

        loadingEl.style.display = 'none';
        toolbar.style.display = 'flex';

        const size = await table.size();
        const schema = await table.schema();
        const cols = Object.keys(schema).length;
        statusEl.textContent = size.toLocaleString() + ' rows \\u00b7 ' + cols + ' columns';

        const viewer = document.createElement('perspective-viewer');
        viewer.setAttribute('plugin', 'Datagrid');
        viewer.style.cssText = 'flex: 1; width: 100%; height: 100%;';
        container.appendChild(viewer);

        // Wait for map plugin registration (loaded via script tag in head)
        if (window.__mapPluginReady) {
          await window.__mapPluginReady;
        }

        // Pass geometry metadata for the map plugin
        const geoCols = ${JSON.stringify(metadata.geometry_columns || [])};
        const tileSources = ${JSON.stringify(metadata.tile_sources || [])};
        if (geoCols.length > 0) {
          viewer.setAttribute('data-geometry-columns', JSON.stringify(geoCols));
        }
        if (tileSources.length > 0) {
          viewer.setAttribute('data-tile-sources', JSON.stringify(tileSources));
        }
        if (arrowUrl) {
          viewer.setAttribute('data-arrow-url', arrowUrl);
        }

        await viewer.load(table);

        // Save buttons — export from the viewer's current view
        document.getElementById('save-csv').addEventListener('click', async () => {
          statusEl.textContent = 'Exporting CSV...';
          try {
            const csv = await viewer.getView().then(v => v.to_csv());
            vscode.postMessage({ type: 'save', format: 'csv', data: csv, binary: false });
          } catch (e) {
            // Fallback: export full table
            const view = await table.view();
            const csv = await view.to_csv();
            await view.delete();
            vscode.postMessage({ type: 'save', format: 'csv', data: csv, binary: false });
          }
          statusEl.textContent = size.toLocaleString() + ' rows \\u00b7 ' + cols + ' columns';
        });

        document.getElementById('save-json').addEventListener('click', async () => {
          statusEl.textContent = 'Exporting JSON...';
          try {
            const json = await viewer.getView().then(v => v.to_json());
            vscode.postMessage({ type: 'save', format: 'json', data: JSON.stringify(json, null, 2), binary: false });
          } catch (e) {
            const view = await table.view();
            const json = await view.to_json();
            await view.delete();
            vscode.postMessage({ type: 'save', format: 'json', data: JSON.stringify(json, null, 2), binary: false });
          }
          statusEl.textContent = size.toLocaleString() + ' rows \\u00b7 ' + cols + ' columns';
        });

        document.getElementById('save-arrow').addEventListener('click', async () => {
          statusEl.textContent = 'Exporting Arrow...';
          try {
            const arrow = await viewer.getView().then(v => v.to_arrow());
            vscode.postMessage({ type: 'save', format: 'arrow', data: bufferToBase64(arrow), binary: true });
          } catch (e) {
            const view = await table.view();
            const arrow = await view.to_arrow();
            await view.delete();
            vscode.postMessage({ type: 'save', format: 'arrow', data: bufferToBase64(arrow), binary: true });
          }
          statusEl.textContent = size.toLocaleString() + ' rows \\u00b7 ' + cols + ' columns';
        });

      } catch (err) {
        loadingEl.style.display = 'none';
        errorEl.style.display = 'block';
        errorEl.textContent = 'Failed to load viewer: ' + (err.message || String(err));
      }
    }

    main();
  </script>
</body>
</html>`;
}
