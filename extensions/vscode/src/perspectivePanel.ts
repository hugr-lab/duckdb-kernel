/**
 * Webview panel that displays Perspective viewer with Arrow data
 * in a full-size VS Code editor tab.
 *
 * Rendering logic is bundled from @hugr-lab/perspective-core via
 * perspective-panel-webview.ts → out/perspective-panel.js
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

  const panelScriptUri = panel.webview.asWebviewUri(
    vscode.Uri.joinPath(extensionUri, 'out', 'perspective-panel.js'),
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
        const bytes = msg.binary
          ? Buffer.from(msg.data, 'base64')
          : Buffer.from(msg.data, 'utf-8');
        await vscode.workspace.fs.writeFile(uri, bytes);
        vscode.window.showInformationMessage(`Saved to ${uri.fsPath}`);
      }
    }
  });

  panel.webview.html = buildPerspectiveHtml(metadata, panelScriptUri);
}

function buildPerspectiveHtml(metadata: {
  query_id: string;
  arrow_url: string;
  base_url: string;
  geometry_columns?: any[];
  tile_sources?: any[];
}, panelScriptUri: vscode.Uri): string {
  const staticBase = `${metadata.base_url}/static/perspective`;

  const config = JSON.stringify({
    staticBase,
    arrowUrl: metadata.arrow_url,
    geometryColumns: metadata.geometry_columns || [],
    tileSources: metadata.tile_sources || [],
  });

  return `<!DOCTYPE html>
<html lang="en" style="height: 100%; margin: 0;">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="Content-Security-Policy"
    content="default-src 'none';
      script-src 'unsafe-inline' 'unsafe-eval' http://127.0.0.1:* http://localhost:* https://*.vscode-resource.vscode-cdn.net;
      style-src 'unsafe-inline' http://127.0.0.1:* http://localhost:*;
      connect-src http://127.0.0.1:* http://localhost:* https://*.basemaps.cartocdn.com https://*.tile.openstreetmap.org https://*.tiles.mapbox.com;
      font-src http://127.0.0.1:* http://localhost:*;
      img-src http://127.0.0.1:* http://localhost:* https://*.basemaps.cartocdn.com https://*.tile.openstreetmap.org https://*.tiles.mapbox.com data:;
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
  <script>window.__perspectivePanelConfig = ${config};<\/script>
  <script src="${panelScriptUri}"><\/script>
</body>
</html>`;
}
