/**
 * JSON viewer widget for opening JSON data in a JupyterLab main area tab.
 */

import { Widget } from '@lumino/widgets';
import { buildJsonRawView, buildJsonTree } from '@hugr-lab/perspective-core';

export class JsonTabWidget extends Widget {
  constructor(data: any, title?: string) {
    super();
    this.addClass('hugr-json-tab');
    this.node.style.width = '100%';
    this.node.style.height = '100%';
    this.node.style.overflow = 'auto';
    this.node.style.padding = '16px';
    this.node.style.boxSizing = 'border-box';

    // Toolbar
    const toolbar = document.createElement('div');
    toolbar.style.cssText = 'margin-bottom: 8px; display: flex; gap: 8px;';

    const treeBtn = document.createElement('button');
    treeBtn.className = 'hugr-json-raw-btn hugr-json-raw-btn-active';
    treeBtn.textContent = 'Tree';
    toolbar.appendChild(treeBtn);

    const rawBtn = document.createElement('button');
    rawBtn.className = 'hugr-json-raw-btn';
    rawBtn.textContent = 'Raw';
    toolbar.appendChild(rawBtn);

    this.node.appendChild(toolbar);

    // Tree view
    const tree = document.createElement('div');
    tree.className = 'hugr-json-tree';
    buildJsonTree(data, tree, true);
    this.node.appendChild(tree);

    // Raw view
    const rawWrap = document.createElement('div');
    rawWrap.className = 'hugr-json-raw';
    rawWrap.style.display = 'none';
    buildJsonRawView(data, rawWrap);
    this.node.appendChild(rawWrap);

    let mode: 'tree' | 'raw' = 'tree';
    const setMode = (m: 'tree' | 'raw') => {
      mode = m;
      tree.style.display = m === 'tree' ? '' : 'none';
      rawWrap.style.display = m === 'raw' ? '' : 'none';
      treeBtn.classList.toggle('hugr-json-raw-btn-active', m === 'tree');
      rawBtn.classList.toggle('hugr-json-raw-btn-active', m === 'raw');
    };
    treeBtn.addEventListener('click', () => setMode('tree'));
    rawBtn.addEventListener('click', () => setMode('raw'));
  }
}
