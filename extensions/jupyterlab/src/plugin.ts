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
import { MainAreaWidget } from '@jupyterlab/apputils';
import { DuckDBSidebarWidget } from './sidebar.js';
import { IntrospectClient } from './introspectClient.js';
import { PerspectiveTabWidget } from './perspectiveTab.js';
import { JsonTabWidget } from './jsonTab.js';
import { initSpoolProxy, setNotebookDir } from './spoolUrl.js';

/** Extract directory part from a notebook path. */
function notebookDirFromPath(path: string): string {
  const parts = path.split('/');
  parts.pop(); // remove filename
  return parts.length > 0 ? parts.join('/') : '.';
}

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

    // Initialize spool proxy for JupyterLab/Hub (provides auth + routing)
    const settings = app.serviceManager.serverSettings;
    initSpoolProxy({ baseUrl: settings.baseUrl, token: settings.token });

    document.addEventListener('hugr:open-in-tab', ((e: CustomEvent) => {
      const { arrowUrl, title, geometryColumns, tileSources } = e.detail;
      const content = new PerspectiveTabWidget(arrowUrl, title, geometryColumns, tileSources);
      const widget = new MainAreaWidget({ content });
      widget.title.label = title || 'Result';
      widget.title.closable = true;
      app.shell.add(widget, 'main');
    }) as EventListener);

    document.addEventListener('hugr:open-json-in-tab', ((e: CustomEvent) => {
      const { data, title } = e.detail;
      const content = new JsonTabWidget(data, title);
      const widget = new MainAreaWidget({ content });
      widget.title.label = title || 'JSON';
      widget.title.closable = true;
      app.shell.add(widget, 'main');
    }) as EventListener);

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
          // Broadcast base URL for widget.ts Arrow URL rebuilding
          document.dispatchEvent(new CustomEvent('hugr:base-url-update', { detail: { baseUrl } }));
        }
      } catch {
        // Kernel may not support hugr_base_url.
      }
    };

    // Track notebook directory for spool pin/unpin.
    // Set from every event that touches a notebook — by the time
    // widget.ts calls getNotebookDir(), it must be non-null.
    const syncDir = () => {
      const panel = notebookTracker.currentWidget;
      if (panel?.context?.path) {
        setNotebookDir(notebookDirFromPath(panel.context.path));
      }
    };

    notebookTracker.currentChanged.connect(() => {
      syncDir();
      void tryDiscover();
    });

    notebookTracker.widgetAdded.connect((_sender, panel) => {
      // Set dir from this specific panel immediately
      if (panel.context?.path) {
        setNotebookDir(notebookDirFromPath(panel.context.path));
      }

      // Also set dir when context path becomes available (async load)
      panel.context.ready.then(() => {
        if (panel.context.path) {
          setNotebookDir(notebookDirFromPath(panel.context.path));
        }
      }).catch(() => {});

      const onStatusChanged = () => {
        syncDir();
        void tryDiscover();
      };
      panel.sessionContext.statusChanged.connect(onStatusChanged);
      panel.disposed.connect(() => {
        panel.sessionContext.statusChanged.disconnect(onStatusChanged);
      });
    });

    // Set dir now if a notebook is already open
    syncDir();
  },
};

export default sidebarPlugin;
