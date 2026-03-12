/**
 * JupyterLab application plugin: DuckDB Explorer sidebar.
 *
 * Discovers the kernel's base_url and shows a database object tree.
 */

import {
  JupyterFrontEnd,
  JupyterFrontEndPlugin,
  ILayoutRestorer,
} from '@jupyterlab/application';
import { INotebookTracker } from '@jupyterlab/notebook';
import { DuckDBSidebarWidget } from './sidebar.js';
import { IntrospectClient } from './introspectClient.js';

const sidebarPlugin: JupyterFrontEndPlugin<void> = {
  id: '@hugr-lab/perspective-viewer:sidebar',
  autoStart: true,
  requires: [INotebookTracker],
  optional: [ILayoutRestorer],
  activate: (
    app: JupyterFrontEnd,
    notebookTracker: INotebookTracker,
    restorer: ILayoutRestorer | null,
  ) => {
    console.log('[DuckDB Explorer] Sidebar plugin activated');
    const sidebar = new DuckDBSidebarWidget();

    app.shell.add(sidebar, 'right', { rank: 500 });

    if (restorer) {
      restorer.add(sidebar, 'duckdb-explorer-sidebar');
    }

    let discoveredUrl: string | null = null;

    const tryDiscover = async () => {
      const panel = notebookTracker.currentWidget;
      if (!panel) return;

      const sessionContext = panel.sessionContext;
      if (!sessionContext?.session?.kernel) return;

      const kernel = sessionContext.session.kernel;

      try {
        const reply = await kernel.requestKernelInfo();
        if (!reply) return;
        const content = reply.content as any;
        const baseUrl = content?.hugr_base_url;
        if (baseUrl && baseUrl !== discoveredUrl) {
          discoveredUrl = baseUrl;
          sidebar.setClient(new IntrospectClient(baseUrl));
        }
      } catch {
        // Kernel may not support hugr_base_url.
      }
    };

    notebookTracker.currentChanged.connect(() => void tryDiscover());
    notebookTracker.widgetAdded.connect((_sender, panel) => {
      panel.sessionContext.statusChanged.connect(() => void tryDiscover());
    });
  },
};

export default sidebarPlugin;
