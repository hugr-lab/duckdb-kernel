/**
 * VS Code extension host for the HUGR Result Viewer and DuckDB Explorer.
 *
 * Discovers the kernel's base URL from kernel_info_reply,
 * creates an IntrospectClient, and registers the tree view.
 */

import * as vscode from 'vscode';
import { IntrospectClient } from './introspectClient';
import { DuckDBTreeProvider } from './treeProvider';

let treeProvider: DuckDBTreeProvider;

export function activate(context: vscode.ExtensionContext) {
  treeProvider = new DuckDBTreeProvider();

  const treeView = vscode.window.createTreeView('duckdb.objectTree', {
    treeDataProvider: treeProvider,
    showCollapseAll: true,
  });
  context.subscriptions.push(treeView);

  context.subscriptions.push(
    vscode.commands.registerCommand('duckdb.refreshExplorer', () => {
      treeProvider.refresh();
    }),
  );

  // Watch for notebook kernel changes to discover base_url.
  context.subscriptions.push(
    vscode.notebooks.onDidChangeNotebookCellExecutionState((e) => {
      discoverKernel(e.cell.notebook);
    }),
  );

  // Try to discover kernel from any open notebook.
  for (const nb of vscode.workspace.notebookDocuments) {
    discoverKernel(nb);
  }
}

let discoveredUrl: string | null = null;

async function discoverKernel(notebook: vscode.NotebookDocument): Promise<void> {
  // VS Code notebook API doesn't expose kernel_info_reply directly.
  // We look for hugr result outputs that contain base_url.
  for (const cell of notebook.getCells()) {
    for (const output of cell.outputs) {
      for (const item of output.items) {
        if (item.mime === 'application/vnd.hugr.result+json') {
          try {
            const metadata = JSON.parse(new TextDecoder().decode(item.data));
            if (metadata.base_url && metadata.base_url !== discoveredUrl) {
              discoveredUrl = metadata.base_url;
              treeProvider.setClient(new IntrospectClient(discoveredUrl));
              return;
            }
          } catch {
            // Ignore parse errors.
          }
        }
      }
    }
  }
}

export function deactivate() {
  // Cleanup handled by disposables.
}
