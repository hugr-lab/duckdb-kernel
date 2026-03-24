/**
 * JupyterLab application plugin: Perspective viewer support.
 *
 * Discovers the kernel's base_url, initializes spool proxy,
 * handles "Open in Tab" commands, and tracks notebook directory.
 */

import {
  JupyterFrontEnd,
  JupyterFrontEndPlugin,
} from '@jupyterlab/application';
import { INotebookTracker } from '@jupyterlab/notebook';
import { MainAreaWidget } from '@jupyterlab/apputils';
import { PerspectiveTabWidget } from './perspectiveTab.js';
import { JsonTabWidget } from './jsonTab.js';
import { initSpoolProxy, setNotebookDir } from './spoolUrl.js';

/** Extract directory part from a notebook path. */
function notebookDirFromPath(path: string): string {
  const parts = path.split('/');
  parts.pop(); // remove filename
  return parts.length > 0 ? parts.join('/') : '.';
}

const perspectivePlugin: JupyterFrontEndPlugin<void> = {
  id: '@hugr-lab/perspective-viewer:sidebar',
  autoStart: true,
  requires: [INotebookTracker],
  activate: (
    app: JupyterFrontEnd,
    notebookTracker: INotebookTracker,
  ) => {
    console.log('[Perspective Viewer] Plugin activated');

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
          // Broadcast base URL for widget.ts Arrow URL rebuilding
          document.dispatchEvent(new CustomEvent('hugr:base-url-update', { detail: { baseUrl } }));
        }
      } catch {
        // Kernel may not support hugr_base_url.
      }
    };

    // Track notebook directory for spool pin/unpin.
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
      if (panel.context?.path) {
        setNotebookDir(notebookDirFromPath(panel.context.path));
      }

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

    syncDir();
  },
};

export default perspectivePlugin;
