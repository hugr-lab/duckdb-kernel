/**
 * JSON viewer widget for opening JSON data in a JupyterLab main area tab.
 */

import { Widget } from '@lumino/widgets';
import { buildJsonRawView } from './widget';

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
    this._buildJsonTree(data, tree, true);
    this.node.appendChild(tree);

    // Raw view with syntax highlighting, line numbers, folding, bracket matching
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

  private _buildJsonTree(data: any, parent: HTMLElement, expanded: boolean): void {
    if (data === null || data === undefined) {
      const val = document.createElement('span');
      val.className = 'hugr-json-null';
      val.textContent = 'null';
      parent.appendChild(val);
      return;
    }

    if (Array.isArray(data)) {
      if (data.length === 0) {
        const val = document.createElement('span');
        val.className = 'hugr-json-bracket';
        val.textContent = '[]';
        parent.appendChild(val);
        return;
      }
      this._buildCollapsible(data, parent, expanded, true);
      return;
    }

    if (typeof data === 'object') {
      const keys = Object.keys(data);
      if (keys.length === 0) {
        const val = document.createElement('span');
        val.className = 'hugr-json-bracket';
        val.textContent = '{}';
        parent.appendChild(val);
        return;
      }
      this._buildCollapsible(data, parent, expanded, false);
      return;
    }

    const val = document.createElement('span');
    if (typeof data === 'string') {
      val.className = 'hugr-json-string';
      val.textContent = JSON.stringify(data);
    } else if (typeof data === 'number') {
      val.className = 'hugr-json-number';
      val.textContent = String(data);
    } else if (typeof data === 'boolean') {
      val.className = 'hugr-json-bool';
      val.textContent = String(data);
    } else {
      val.textContent = String(data);
    }
    parent.appendChild(val);
  }

  private _buildCollapsible(data: any, parent: HTMLElement, expanded: boolean, isArray: boolean): void {
    const count = isArray ? data.length : Object.keys(data).length;

    const row = document.createElement('div');
    row.className = 'hugr-json-row';

    const toggle = document.createElement('span');
    toggle.className = 'hugr-json-toggle';
    toggle.textContent = expanded ? '\u25BC' : '\u25B6';
    row.appendChild(toggle);

    const summary = document.createElement('span');
    summary.className = 'hugr-json-summary';
    summary.textContent = isArray ? `Array(${count})` : `{${count} keys}`;
    row.appendChild(summary);

    parent.appendChild(row);

    const children = document.createElement('div');
    children.className = 'hugr-json-children';
    children.style.display = expanded ? '' : 'none';

    if (isArray) {
      for (let i = 0; i < data.length; i++) {
        const entry = document.createElement('div');
        entry.className = 'hugr-json-entry';
        const key = document.createElement('span');
        key.className = 'hugr-json-index';
        key.textContent = `${i}: `;
        entry.appendChild(key);
        this._buildJsonTree(data[i], entry, false);
        children.appendChild(entry);
      }
    } else {
      for (const [k, v] of Object.entries(data)) {
        const entry = document.createElement('div');
        entry.className = 'hugr-json-entry';
        const key = document.createElement('span');
        key.className = 'hugr-json-key';
        key.textContent = `${k}: `;
        entry.appendChild(key);
        this._buildJsonTree(v, entry, false);
        children.appendChild(entry);
      }
    }

    parent.appendChild(children);

    const doToggle = () => {
      const isOpen = children.style.display !== 'none';
      children.style.display = isOpen ? 'none' : '';
      toggle.textContent = isOpen ? '\u25B6' : '\u25BC';
    };
    toggle.addEventListener('click', doToggle);
    summary.addEventListener('click', doToggle);
  }
}
