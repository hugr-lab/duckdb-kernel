/**
 * JupyterLab MIME renderer extension for HUGR result metadata.
 *
 * Intercepts `application/vnd.hugr.result+json` output from the DuckDB
 * kernel and renders an interactive Perspective viewer.
 */

import { IRenderMime } from '@jupyterlab/rendermime-interfaces';
import { HugrResultWidget } from './widget.js';

const MIME_TYPE = 'application/vnd.hugr.result+json';

const rendererFactory: IRenderMime.IRendererFactory = {
  safe: true,
  mimeTypes: [MIME_TYPE],
  createRenderer: (options: IRenderMime.IRendererOptions) =>
    new HugrResultWidget(options),
};

const extension: IRenderMime.IExtension = {
  id: '@hugr-lab/perspective-viewer:plugin',
  rendererFactory,
  rank: 0,
  dataType: 'json',
};

export default extension;
