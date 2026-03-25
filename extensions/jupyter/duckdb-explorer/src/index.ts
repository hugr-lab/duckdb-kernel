/**
 * JupyterLab application plugin: DuckDB Explorer sidebar.
 *
 * Discovers the kernel via notebook tracker and connects the sidebar
 * using Jupyter Comm protocol for introspection.
 * Independent of perspective-viewer -- works without it installed.
 */

import {
  JupyterFrontEnd,
  JupyterFrontEndPlugin,
  ILayoutRestorer,
} from '@jupyterlab/application';
import { INotebookTracker } from '@jupyterlab/notebook';
import { DuckDBSidebarWidget } from './sidebar.js';
import { CommIntrospectClient } from './introspectClient.js';
import type { IIntrospectClient } from './introspectClient.js';
import type { Kernel } from '@jupyterlab/services';

const sidebarPlugin: JupyterFrontEndPlugin<void> = {
  id: '@hugr-lab/duckdb-explorer:sidebar',
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

    let currentClient: IIntrospectClient | null = null;
    let currentKernel: Kernel.IKernelConnection | null = null;

    const connectToKernel = () => {
      const panel = notebookTracker.currentWidget;
      if (!panel) {
        disposeClient();
        sidebar.setClient(null);
        return;
      }

      const sessionContext = panel.sessionContext;
      if (!sessionContext?.session?.kernel) return;

      const kernel = sessionContext.session.kernel;

      // Avoid reconnecting to the same kernel.
      if (currentKernel === kernel && currentClient) return;

      disposeClient();

      try {
        currentKernel = kernel;
        const client = new CommIntrospectClient(kernel);
        currentClient = client;
        sidebar.setClient(client);
        console.log('[DuckDB Explorer] Connected via comm');

        // Broadcast base URL for other extensions (e.g., perspective-viewer).
        kernel.requestKernelInfo().then((reply) => {
          if (reply) {
            const content = reply.content as any;
            const baseUrl = content?.hugr_base_url;
            if (baseUrl) {
              document.dispatchEvent(
                new CustomEvent('hugr:base-url-update', { detail: { baseUrl } }),
              );
            }
          }
        }).catch(() => {
          // Kernel may not support hugr_base_url.
        });
      } catch {
        // Kernel may not be ready yet.
      }
    };

    const disposeClient = () => {
      if (currentClient?.dispose) {
        currentClient.dispose();
      }
      currentClient = null;
      currentKernel = null;
    };

    notebookTracker.currentChanged.connect(() => {
      connectToKernel();
    });

    notebookTracker.widgetAdded.connect((_sender, panel) => {
      const onStatusChanged = () => {
        if (panel === notebookTracker.currentWidget) {
          connectToKernel();
        }
      };
      panel.sessionContext.statusChanged.connect(onStatusChanged);
      panel.disposed.connect(() => {
        panel.sessionContext.statusChanged.disconnect(onStatusChanged);
        if (panel === notebookTracker.currentWidget) {
          disposeClient();
          sidebar.setClient(null);
        }
      });
    });
  },
};

export default sidebarPlugin;
