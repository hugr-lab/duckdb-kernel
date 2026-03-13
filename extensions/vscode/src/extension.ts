/**
 * VS Code extension host for the HUGR Result Viewer and DuckDB Explorer.
 *
 * Discovers the kernel's base URL from cell outputs,
 * creates an IntrospectClient, and registers multiple tree views
 * with inline action buttons and detail webview panels.
 */

import * as vscode from 'vscode';
import { IntrospectClient } from './introspectClient';
import {
  SessionProvider,
  CatalogProvider,
  SystemFunctionsProvider,
  SimpleListProvider,
  extensionsFetch,
  secretsFetch,
  settingsFetch,
  memoryFetch,
  resultFilesFetch,
} from './treeProvider';
import { showDetailPanel } from './detailPanel';
import { installKernel } from './installKernel';

let log: vscode.OutputChannel;
let currentClient: IntrospectClient | null = null;

// Map notebook URI → kernel base URL
const notebookClients = new Map<string, { url: string; client: IntrospectClient }>();

// All providers
let sessionProvider: SessionProvider;
let catalogProvider: CatalogProvider;
let systemFunctionsProvider: SystemFunctionsProvider;
let extensionsProvider: SimpleListProvider;
let secretsProvider: SimpleListProvider;
let settingsProvider: SimpleListProvider;
let memoryProvider: SimpleListProvider;
let resultFilesProvider: SimpleListProvider;

function allProviders() {
  return [
    sessionProvider,
    catalogProvider,
    systemFunctionsProvider,
    extensionsProvider,
    secretsProvider,
    settingsProvider,
    memoryProvider,
    resultFilesProvider,
  ];
}

export function activate(context: vscode.ExtensionContext) {
  log = vscode.window.createOutputChannel('DuckDB Explorer');
  context.subscriptions.push(log);

  log.appendLine('DuckDB Explorer extension activated');

  // Create providers
  sessionProvider = new SessionProvider();
  catalogProvider = new CatalogProvider();
  systemFunctionsProvider = new SystemFunctionsProvider();
  extensionsProvider = new SimpleListProvider(extensionsFetch);
  secretsProvider = new SimpleListProvider(secretsFetch);
  settingsProvider = new SimpleListProvider(settingsFetch);
  memoryProvider = new SimpleListProvider(memoryFetch);
  resultFilesProvider = new SimpleListProvider(resultFilesFetch);

  // Register tree views
  context.subscriptions.push(
    vscode.window.createTreeView('duckdb.session', {
      treeDataProvider: sessionProvider,
    }),
    vscode.window.createTreeView('duckdb.catalog', {
      treeDataProvider: catalogProvider,
      showCollapseAll: true,
    }),
    vscode.window.createTreeView('duckdb.systemFunctions', {
      treeDataProvider: systemFunctionsProvider,
      showCollapseAll: true,
    }),
    vscode.window.createTreeView('duckdb.extensions', {
      treeDataProvider: extensionsProvider,
    }),
    vscode.window.createTreeView('duckdb.secrets', {
      treeDataProvider: secretsProvider,
    }),
    vscode.window.createTreeView('duckdb.settings', {
      treeDataProvider: settingsProvider,
    }),
    vscode.window.createTreeView('duckdb.memory', {
      treeDataProvider: memoryProvider,
    }),
    vscode.window.createTreeView('duckdb.resultFiles', {
      treeDataProvider: resultFilesProvider,
    }),
  );

  // Register refresh commands
  context.subscriptions.push(
    vscode.commands.registerCommand('duckdb.refreshCatalog', () => {
      catalogProvider.refresh();
    }),
    vscode.commands.registerCommand('duckdb.refreshExtensions', () => {
      extensionsProvider.refresh();
    }),
    vscode.commands.registerCommand('duckdb.refreshSecrets', () => {
      secretsProvider.refresh();
    }),
    vscode.commands.registerCommand('duckdb.refreshSettings', () => {
      settingsProvider.refresh();
    }),
    vscode.commands.registerCommand('duckdb.refreshMemory', () => {
      memoryProvider.refresh();
    }),
    vscode.commands.registerCommand('duckdb.refreshResultFiles', () => {
      resultFilesProvider.refresh();
    }),
    vscode.commands.registerCommand('duckdb.refreshSystemFunctions', () => {
      systemFunctionsProvider.refresh();
    }),
    // Inline refresh on database/schema/category nodes — refreshes children of that node
    vscode.commands.registerCommand('duckdb.refreshNode', (node: any) => {
      catalogProvider.refreshNode(node);
    }),
  );

  // Register showInfo command - opens a detail webview panel
  context.subscriptions.push(
    vscode.commands.registerCommand('duckdb.showInfo', (node: any) => {
      if (!currentClient || !node) return;

      const type: string = node.type ?? node.contextValue ?? '';
      const params: Record<string, string> = {};

      switch (type) {
        case 'table':
        case 'view':
          params.database = node.database ?? '';
          params.schema = node.schema ?? '';
          params.table = node.table ?? '';
          break;
        case 'database':
          params.database = node.database ?? node.label ?? '';
          break;
        case 'secret':
          params.name = node.label ?? '';
          break;
        case 'extension':
          params.name = node.label ?? '';
          break;
        case 'function':
        case 'function_group':
          params.name = node.label ?? '';
          break;
        default:
          return;
      }

      const title = `${type.charAt(0).toUpperCase() + type.slice(1)}: ${node.label ?? ''}`;
      showDetailPanel(title, currentClient, type, params);
    }),
  );

  // Register installKernel command
  context.subscriptions.push(
    vscode.commands.registerCommand('duckdb.installKernel', async () => {
      try {
        await installKernel(log);
      } catch (err: any) {
        vscode.window.showErrorMessage(`Kernel install failed: ${err.message}`);
        log.appendLine(`Install error: ${err.message}`);
      }
    }),
  );

  // Register deleteResultFile command
  context.subscriptions.push(
    vscode.commands.registerCommand('duckdb.deleteResultFile', async (node: any) => {
      if (!currentClient || !node) return;

      const queryId = node.label ?? node.data?.query_id ?? '';
      if (!queryId) return;

      const answer = await vscode.window.showWarningMessage(
        `Delete result file "${queryId}"?`,
        { modal: true },
        'Delete',
      );

      if (answer !== 'Delete') return;

      try {
        await currentClient.deleteSpoolFile(queryId);
        resultFilesProvider.refresh();
      } catch (err: any) {
        vscode.window.showErrorMessage(`Failed to delete: ${err.message}`);
      }
    }),
  );

  // Watch for notebook document changes (includes output additions).
  context.subscriptions.push(
    vscode.workspace.onDidChangeNotebookDocument((e) => {
      if (e.cellChanges.some((c) => c.outputs !== undefined)) {
        discoverKernel(e.notebook);
      }
    }),
  );

  // Watch for newly opened notebooks.
  context.subscriptions.push(
    vscode.workspace.onDidOpenNotebookDocument((nb) => {
      discoverKernel(nb);
    }),
  );

  // Switch client when user switches between notebook tabs.
  context.subscriptions.push(
    vscode.window.onDidChangeActiveNotebookEditor((editor) => {
      if (editor) {
        const key = editor.notebook.uri.toString();
        const entry = notebookClients.get(key);
        if (entry && entry.client !== currentClient) {
          log.appendLine(`Switching to kernel: ${entry.url} (${key})`);
          currentClient = entry.client;
          for (const provider of allProviders()) {
            provider.setClient(currentClient);
          }
        } else if (!entry) {
          // Try to discover from this notebook's outputs
          discoverKernel(editor.notebook);
        }
      }
    }),
  );

  // Clean up when notebook is closed.
  context.subscriptions.push(
    vscode.workspace.onDidCloseNotebookDocument((nb) => {
      notebookClients.delete(nb.uri.toString());
    }),
  );

  // Try to discover kernel from any already open notebook.
  log.appendLine(`Open notebooks: ${vscode.workspace.notebookDocuments.length}`);
  for (const nb of vscode.workspace.notebookDocuments) {
    discoverKernel(nb);
  }
}

function discoverKernel(notebook: vscode.NotebookDocument): void {
  const key = notebook.uri.toString();

  for (const cell of notebook.getCells()) {
    for (const output of cell.outputs) {
      for (const item of output.items) {
        if (item.mime === 'application/vnd.hugr.result+json') {
          try {
            const metadata = JSON.parse(new TextDecoder().decode(item.data));
            if (metadata.base_url) {
              const existing = notebookClients.get(key);
              if (!existing || existing.url !== metadata.base_url) {
                const client = new IntrospectClient(metadata.base_url);
                notebookClients.set(key, { url: metadata.base_url, client });
                log.appendLine(`Discovered kernel URL: ${metadata.base_url} for ${key}`);

                // Update providers if this is the active notebook
                const activeUri = vscode.window.activeNotebookEditor?.notebook.uri.toString();
                if (activeUri === key || !currentClient) {
                  currentClient = client;
                  for (const provider of allProviders()) {
                    provider.setClient(currentClient);
                  }
                }
              }
              return;
            }
          } catch (e: any) {
            log.appendLine(`Parse error: ${e.message}`);
          }
        }
      }
    }
  }
}

export function deactivate() {
  // Cleanup handled by disposables.
}
