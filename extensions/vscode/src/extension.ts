/**
 * VS Code extension host for the HUGR Result Viewer.
 *
 * Arrow data is fetched directly by the renderer from the Go kernel's
 * HTTP server. This extension host is kept for future functionality.
 */

import * as vscode from 'vscode';

export function activate(_context: vscode.ExtensionContext) {
  // Renderer fetches Arrow data directly from Go kernel HTTP server.
  // No extension host messaging needed.
}

export function deactivate() {
  // No cleanup needed.
}
