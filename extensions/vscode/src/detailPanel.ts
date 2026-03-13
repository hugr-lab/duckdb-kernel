/**
 * Webview detail panels for DuckDB Explorer.
 *
 * Opens a webview panel showing detail information about
 * tables, views, databases, secrets, and extensions.
 */

import * as vscode from 'vscode';
import { IntrospectClient } from './introspectClient';

interface DetailTab {
  id: string;
  label: string;
  fetchData: () => Promise<any[]>;
  /** Force horizontal table even for single rows (e.g. describe, summarize). */
  forceHorizontal?: boolean;
}

/**
 * Show a webview panel with detail information for the given item type.
 */
export async function showDetailPanel(
  title: string,
  client: IntrospectClient,
  type: string,
  params: Record<string, string>,
): Promise<void> {
  const panel = vscode.window.createWebviewPanel(
    'duckdb.detail',
    title,
    vscode.ViewColumn.Beside,
    { enableScripts: true, retainContextWhenHidden: false },
  );

  const tabs = buildTabs(client, type, params);

  // Start loading the first tab immediately
  panel.webview.html = buildLoadingHtml(title);

  try {
    const tabData: Record<string, any[]> = {};
    // Fetch all tab data in parallel
    await Promise.all(
      tabs.map(async (tab) => {
        try {
          tabData[tab.id] = await tab.fetchData();
        } catch (err: any) {
          tabData[tab.id] = [{ error: err.message ?? String(err) }];
        }
      }),
    );
    panel.webview.html = buildDetailHtml(title, tabs, tabData);
  } catch (err: any) {
    panel.webview.html = buildErrorHtml(title, err.message ?? String(err));
  }
}

function buildTabs(
  client: IntrospectClient,
  type: string,
  params: Record<string, string>,
): DetailTab[] {
  const db = params.database ?? '';
  const schema = params.schema ?? '';
  const table = params.table ?? '';

  switch (type) {
    case 'table':
      return [
        { id: 'info', label: 'Info', fetchData: () => client.tableInfo(db, schema, table) },
        { id: 'describe', label: 'Describe', fetchData: () => client.describe(db, schema, table), forceHorizontal: true },
        { id: 'summarize', label: 'Summarize', fetchData: () => client.summarize(db, schema, table), forceHorizontal: true },
      ];
    case 'view':
      return [
        { id: 'info', label: 'Info', fetchData: () => client.viewInfo(db, schema, table) },
        { id: 'describe', label: 'Describe', fetchData: () => client.describe(db, schema, table), forceHorizontal: true },
      ];
    case 'database':
      return [
        { id: 'info', label: 'Info', fetchData: () => client.databaseInfo(db) },
      ];
    case 'secret':
      return [
        {
          id: 'info',
          label: 'Info',
          fetchData: async () => {
            // The params.data should contain the secret object already
            // But we can also re-fetch secrets and find this one
            const secrets = await client.secrets();
            const match = secrets.find((s: any) => s.name === params.name);
            return match ? [match] : [];
          },
        },
      ];
    case 'extension':
      return [
        {
          id: 'info',
          label: 'Info',
          fetchData: async () => {
            const exts = await client.extensions();
            const match = exts.find((e: any) => e.extension_name === params.name);
            return match ? [match] : [];
          },
        },
      ];
    case 'function':
    case 'function_group':
      return [
        {
          id: 'info',
          label: 'Info',
          fetchData: async () => {
            const fns = await client.functions();
            const matches = fns.filter((f: any) => f.function_name === params.name);
            if (matches.length > 0) return matches;
            // Try system functions
            const sysFns = await client.systemFunctions();
            return sysFns.filter((f: any) => f.function_name === params.name);
          },
        },
      ];
    default:
      return [];
  }
}

function escapeHtml(str: string): string {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function renderTable(data: any[], vertical?: boolean): string {
  if (!data || data.length === 0) return '<p class="empty">No data available.</p>';

  if (data.length === 1 && data[0].error) {
    return `<p class="error">Error: ${escapeHtml(data[0].error)}</p>`;
  }

  // Collect all keys from all rows
  const keys = new Set<string>();
  for (const row of data) {
    for (const key of Object.keys(row)) {
      keys.add(key);
    }
  }
  const columns = Array.from(keys);

  // Single row with many columns → vertical key-value layout
  if (vertical !== false && data.length === 1 && columns.length > 4) {
    const row = data[0];
    let html = '<table class="kv-table"><tbody>';
    for (const col of columns) {
      const val = row[col];
      const display = val === null || val === undefined ? '' : String(val);
      html += `<tr><th>${escapeHtml(col)}</th><td>${escapeHtml(display)}</td></tr>`;
    }
    html += '</tbody></table>';
    return html;
  }

  let html = '<table><thead><tr>';
  for (const col of columns) {
    html += `<th>${escapeHtml(col)}</th>`;
  }
  html += '</tr></thead><tbody>';
  for (const row of data) {
    html += '<tr>';
    for (const col of columns) {
      const val = row[col];
      const display = val === null || val === undefined ? '' : String(val);
      html += `<td>${escapeHtml(display)}</td>`;
    }
    html += '</tr>';
  }
  html += '</tbody></table>';
  return html;
}

function buildTabBar(tabs: DetailTab[], activeId: string): string {
  if (tabs.length <= 1) return '';
  let html = '<div class="tab-bar">';
  for (const tab of tabs) {
    const cls = tab.id === activeId ? 'tab active' : 'tab';
    html += `<button class="${cls}" onclick="switchTab('${tab.id}')">${escapeHtml(tab.label)}</button>`;
  }
  html += '</div>';
  return html;
}

function baseStyles(): string {
  return `
    body {
      font-family: var(--vscode-font-family, sans-serif);
      font-size: var(--vscode-font-size, 13px);
      color: var(--vscode-editor-foreground);
      background: var(--vscode-editor-background);
      padding: 16px;
      margin: 0;
    }
    h1 {
      font-size: 1.3em;
      margin: 0 0 12px 0;
      font-weight: 600;
    }
    .tab-bar {
      display: flex;
      gap: 0;
      border-bottom: 1px solid var(--vscode-panel-border, #444);
      margin-bottom: 12px;
    }
    .tab {
      padding: 6px 16px;
      border: none;
      background: none;
      color: var(--vscode-foreground, #ccc);
      cursor: pointer;
      font-size: inherit;
      font-family: inherit;
      border-bottom: 2px solid transparent;
    }
    .tab:hover {
      color: var(--vscode-editor-foreground);
      background: var(--vscode-list-hoverBackground, rgba(255,255,255,0.04));
    }
    .tab.active {
      color: var(--vscode-editor-foreground);
      border-bottom-color: var(--vscode-focusBorder, #007acc);
    }
    .tab-content {
      display: none;
    }
    .tab-content.active {
      display: block;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 4px;
    }
    th, td {
      text-align: left;
      padding: 4px 10px;
      border-bottom: 1px solid var(--vscode-panel-border, #333);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      max-width: 400px;
    }
    th {
      font-weight: 600;
      background: var(--vscode-editorGroupHeader-tabsBackground, rgba(255,255,255,0.04));
      position: sticky;
      top: 0;
    }
    tr:hover td {
      background: var(--vscode-list-hoverBackground, rgba(255,255,255,0.04));
    }
    .empty {
      color: var(--vscode-descriptionForeground, #888);
      font-style: italic;
    }
    .error {
      color: var(--vscode-errorForeground, #f44);
    }
    .kv-table th {
      text-align: right;
      color: var(--vscode-descriptionForeground, #888);
      font-weight: 600;
      white-space: nowrap;
      width: 1%;
      padding-right: 16px;
      vertical-align: top;
    }
    .kv-table td {
      word-break: break-all;
      white-space: normal;
      max-width: none;
    }
    .loading {
      color: var(--vscode-descriptionForeground, #888);
      font-style: italic;
    }
  `;
}

function buildLoadingHtml(title: string): string {
  return `<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><style>${baseStyles()}</style></head>
<body>
  <h1>${escapeHtml(title)}</h1>
  <p class="loading">Loading...</p>
</body>
</html>`;
}

function buildErrorHtml(title: string, message: string): string {
  return `<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><style>${baseStyles()}</style></head>
<body>
  <h1>${escapeHtml(title)}</h1>
  <p class="error">Error: ${escapeHtml(message)}</p>
</body>
</html>`;
}

function buildDetailHtml(
  title: string,
  tabs: DetailTab[],
  tabData: Record<string, any[]>,
): string {
  const firstTab = tabs.length > 0 ? tabs[0].id : '';

  let tabContents = '';
  for (const tab of tabs) {
    const activeCls = tab.id === firstTab ? ' active' : '';
    const vertical = tab.forceHorizontal ? false : undefined;
    tabContents += `<div id="tab-${tab.id}" class="tab-content${activeCls}">${renderTable(tabData[tab.id] ?? [], vertical)}</div>`;
  }

  const tabScript = tabs.length > 1 ? `
    <script>
      function switchTab(tabId) {
        document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
        document.querySelectorAll('.tab').forEach(el => el.classList.remove('active'));
        const content = document.getElementById('tab-' + tabId);
        if (content) content.classList.add('active');
        // Find the button for this tab
        document.querySelectorAll('.tab').forEach(el => {
          if (el.getAttribute('onclick')?.includes("'" + tabId + "'")) {
            el.classList.add('active');
          }
        });
      }
    </script>` : '';

  return `<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><style>${baseStyles()}</style></head>
<body>
  <h1>${escapeHtml(title)}</h1>
  ${buildTabBar(tabs, firstTab)}
  ${tabContents}
  ${tabScript}
</body>
</html>`;
}
