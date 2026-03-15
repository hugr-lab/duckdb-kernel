/**
 * JupyterLab sidebar widget for DuckDB database explorer.
 *
 * Accordion sections with icons:
 *   SESSION / CATALOG / EXTENSIONS / SECRETS / SETTINGS / MEMORY / RESULT FILES
 *
 * Catalog is a tree; other sections are table views.
 * Tables/Views get an ℹ button → modal with DESCRIBE / SUMMARIZE tabs.
 */

import { Widget } from '@lumino/widgets';
import { LabIcon } from '@jupyterlab/ui-components';
import { IntrospectClient } from './introspectClient.js';

/* ========== icons ========== */

const duckdbIcon = new LabIcon({
  name: '@hugr-lab/perspective-viewer:duckdb-icon',
  svgstr:
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 500 500">' +
    '<path fill="#1a1a1a" d="M250,500C112,500,0,388,0,250S111.4,0,250,0,500,112,500,250,388,500,250,500Z"/>' +
    '<path fill="#fff100" d="M190.1,146.6c-56.8,0-103.4,46.6-103.4,103.4s46.6,103.4,103.4,103.4,103.4-46.6,103.4-103.4-46.6-103.4-103.4-103.4Z"/>' +
    '<path fill="#fff100" d="M376.1,212.8h-49.1v74.4h49.1c20.6,0,37.2-16.7,37.2-37.2s-16.7-37.2-37.2-37.2Z"/>' +
    '</svg>',
});

const SECTION_ICONS: Record<string, string> = {
  SESSION: '⚡', CATALOG: '🗄', EXTENSIONS: '🧩',
  SECRETS: '🔒', SETTINGS: '⚙', MEMORY: '📊', 'RESULT FILES': '📁',
};

const OBJ_ICONS: Record<string, { icon: string; cls: string }> = {
  database: { icon: '🗄', cls: 'hugr-icon-db' },
  schema:   { icon: '📂', cls: 'hugr-icon-schema' },
  table:    { icon: '⊞', cls: 'hugr-icon-table' },
  view:     { icon: '👁', cls: 'hugr-icon-view' },
  function: { icon: 'ƒ', cls: 'hugr-icon-func' },
  index:    { icon: '🔑', cls: 'hugr-icon-index' },
};

function colIcon(dataType: string): { icon: string; cls: string } {
  const dt = (dataType || '').toUpperCase();
  if (/^(BIG|SMALL|TINY|U?HUGE)?INT|INTEGER|UBIGINT|USMALLINT|UTINYINT/.test(dt))
    return { icon: '#', cls: 'hugr-col-int' };
  if (/DOUBLE|FLOAT|REAL|DECIMAL|NUMERIC/.test(dt))
    return { icon: '.0', cls: 'hugr-col-float' };
  if (/VARCHAR|TEXT|CHAR|STRING|BLOB|UUID/.test(dt))
    return { icon: 'Aa', cls: 'hugr-col-text' };
  if (/BOOL/.test(dt)) return { icon: '◉', cls: 'hugr-col-bool' };
  if (/DATE|TIME|TIMESTAMP|INTERVAL/.test(dt)) return { icon: '◷', cls: 'hugr-col-date' };
  if (/LIST|ARRAY/.test(dt)) return { icon: '[]', cls: 'hugr-col-list' };
  if (/STRUCT|MAP|UNION/.test(dt)) return { icon: '{}', cls: 'hugr-col-struct' };
  return { icon: '◆', cls: 'hugr-col-other' };
}

/* ========== helpers ========== */

function formatBytes(bytes: unknown): string {
  const n = Number(bytes);
  if (!Number.isFinite(n) || n === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB'];
  let i = 0; let val = n;
  while (val >= 1024 && i < units.length - 1) { val /= 1024; i++; }
  return `${val.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

function esc(text: unknown): string {
  const el = document.createElement('span');
  el.textContent = String(text ?? '');
  return el.innerHTML;
}

/* ========== expandable node factory ========== */

interface ExpandableOpts {
  objType?: string;
  label: string;
  description?: string;
  loader: (container: HTMLElement) => Promise<void>;
  showRefresh?: boolean;
}

/**
 * Creates a tree node with integrated expand/collapse, lazy loading,
 * and optional refresh button.  All in one handler — no dual listeners.
 */
function createExpandableNode(opts: ExpandableOpts): HTMLElement {
  const item = document.createElement('div');
  item.className = 'hugr-tree-item';

  const row = document.createElement('div');
  row.className = 'hugr-tree-row';

  const toggle = document.createElement('span');
  toggle.className = 'hugr-tree-toggle';
  toggle.textContent = '▶';
  row.appendChild(toggle);

  // Object icon
  if (opts.objType) {
    const oi = OBJ_ICONS[opts.objType];
    if (oi) {
      const ic = document.createElement('span');
      ic.className = `hugr-obj-icon ${oi.cls}`;
      ic.textContent = oi.icon;
      row.appendChild(ic);
    }
  }

  const labelEl = document.createElement('span');
  labelEl.className = 'hugr-tree-label';
  labelEl.textContent = opts.label;
  row.appendChild(labelEl);

  if (opts.description) {
    const desc = document.createElement('span');
    desc.className = 'hugr-tree-desc';
    desc.textContent = opts.description;
    row.appendChild(desc);
  }

  // Refresh button
  if (opts.showRefresh) {
    const btn = document.createElement('button');
    btn.className = 'hugr-refresh-btn hugr-refresh-btn-small';
    btn.textContent = '↻';
    btn.title = `Refresh ${opts.label}`;
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      // Collapse, clear, reload
      loaded = false;
      childrenEl.innerHTML = '';
      childrenEl.style.display = 'none';
      toggle.textContent = '▶';
      // Re-expand → triggers load
      doExpand();
    });
    row.appendChild(btn);
  }

  item.appendChild(row);

  const childrenEl = document.createElement('div');
  childrenEl.className = 'hugr-tree-children';
  childrenEl.style.display = 'none';
  item.appendChild(childrenEl);

  let loaded = false;

  const doExpand = () => {
    childrenEl.style.display = 'block';
    toggle.textContent = '▼';
    if (!loaded) {
      loaded = true;
      childrenEl.innerHTML = '<div class="hugr-loading">Loading…</div>';
      opts.loader(childrenEl)
        .then(() => {
          // Remove loading indicator (loader appends children alongside it)
          const ld = childrenEl.querySelector('.hugr-loading');
          if (ld) ld.remove();
          if (childrenEl.childElementCount === 0) {
            childrenEl.innerHTML = '<div class="hugr-empty">(empty)</div>';
          }
        })
        .catch((err: any) => {
          childrenEl.innerHTML = `<div class="hugr-error">${esc(err.message)}</div>`;
        });
    }
  };

  row.addEventListener('click', (e) => {
    // Skip if user clicked a button inside the row
    if ((e.target as HTMLElement).closest('button')) return;
    const isOpen = childrenEl.style.display !== 'none';
    if (isOpen) {
      childrenEl.style.display = 'none';
      toggle.textContent = '▶';
    } else {
      doExpand();
    }
  });

  return item;
}

/* ========== leaf node ========== */

function createLeafNode(
  objType: string,
  label: string,
  description?: string,
): HTMLElement {
  const item = document.createElement('div');
  item.className = 'hugr-tree-item';
  const row = document.createElement('div');
  row.className = 'hugr-tree-row hugr-tree-row-leaf';

  const oi = OBJ_ICONS[objType];
  if (oi) {
    const ic = document.createElement('span');
    ic.className = `hugr-obj-icon ${oi.cls}`;
    ic.textContent = oi.icon;
    row.appendChild(ic);
  }

  const labelEl = document.createElement('span');
  labelEl.className = 'hugr-tree-label';
  labelEl.textContent = label;
  row.appendChild(labelEl);

  if (description) {
    const desc = document.createElement('span');
    desc.className = 'hugr-tree-desc';
    desc.textContent = description;
    row.appendChild(desc);
  }

  item.appendChild(row);
  return item;
}

function createColumnNode(col: any): HTMLElement {
  const ci = colIcon(col.data_type || '');
  const isPK = col.column_default?.includes('nextval') ||
               col.is_primary === true || col.key === 'PRI';

  const item = document.createElement('div');
  item.className = 'hugr-tree-item';
  const row = document.createElement('div');
  row.className = 'hugr-tree-row hugr-tree-row-leaf';

  const iconEl = document.createElement('span');
  iconEl.className = `hugr-col-icon ${ci.cls}`;
  iconEl.textContent = ci.icon;
  row.appendChild(iconEl);

  if (isPK) {
    const pk = document.createElement('span');
    pk.className = 'hugr-pk-badge';
    pk.textContent = '🔑';
    pk.title = 'Primary Key';
    row.appendChild(pk);
  }

  const labelEl = document.createElement('span');
  labelEl.className = 'hugr-tree-label';
  labelEl.textContent = col.column_name;
  row.appendChild(labelEl);

  const desc = document.createElement('span');
  desc.className = 'hugr-tree-desc';
  desc.textContent =
    col.data_type + (col.is_nullable === 'YES' ? ' (nullable)' : '');
  row.appendChild(desc);

  item.appendChild(row);
  return item;
}

/* ========== modal ========== */

function showModal(title: string, content: HTMLElement): void {
  const overlay = document.createElement('div');
  overlay.className = 'hugr-modal-overlay';

  const dialog = document.createElement('div');
  dialog.className = 'hugr-modal';

  const header = document.createElement('div');
  header.className = 'hugr-modal-header';

  const titleEl = document.createElement('span');
  titleEl.className = 'hugr-modal-title';
  titleEl.textContent = title;

  const closeBtn = document.createElement('button');
  closeBtn.className = 'hugr-modal-close';
  closeBtn.textContent = '✕';
  closeBtn.addEventListener('click', () => overlay.remove());

  header.appendChild(titleEl);
  header.appendChild(closeBtn);

  const body = document.createElement('div');
  body.className = 'hugr-modal-body';
  body.appendChild(content);

  dialog.appendChild(header);
  dialog.appendChild(body);
  overlay.appendChild(dialog);

  overlay.addEventListener('click', (e) => {
    if (e.target === overlay) overlay.remove();
  });

  document.body.appendChild(overlay);
}

function buildAutoTable(data: any[]): HTMLElement {
  const wrapper = document.createElement('div');
  wrapper.className = 'hugr-modal-table-wrap';

  if (!data || data.length === 0) {
    wrapper.innerHTML = '<div class="hugr-empty">(no data)</div>';
    return wrapper;
  }

  const keys = Object.keys(data[0]);
  const table = document.createElement('table');
  table.className = 'hugr-table';

  const thead = document.createElement('thead');
  const headRow = document.createElement('tr');
  for (const k of keys) {
    const th = document.createElement('th');
    th.textContent = k;
    headRow.appendChild(th);
  }
  thead.appendChild(headRow);
  table.appendChild(thead);

  const tbody = document.createElement('tbody');
  for (const row of data) {
    const tr = document.createElement('tr');
    for (const k of keys) {
      const td = document.createElement('td');
      const v = row[k];
      td.textContent = v == null ? '' : String(v);
      td.title = v == null ? '' : String(v);
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
  table.appendChild(tbody);
  wrapper.appendChild(table);
  return wrapper;
}

/** Build a key-value view element from a data object. */
function buildKVView(data: Record<string, any>): HTMLElement {
  const el = document.createElement('div');
  el.className = 'hugr-kv-modal';

  for (const [key, val] of Object.entries(data)) {
    const row = document.createElement('div');
    row.className = 'hugr-kv hugr-kv-modal-row';

    const k = document.createElement('span');
    k.className = 'hugr-kv-key';
    k.textContent = key;

    const v = document.createElement('span');
    v.className = 'hugr-kv-val';
    v.textContent = val == null ? '' : String(val);
    v.title = val == null ? '' : String(val);

    row.appendChild(k);
    row.appendChild(v);
    el.appendChild(row);
  }
  return el;
}

/** Modal showing all key-value pairs of an object. */
function showKVModal(title: string, data: Record<string, any>): void {
  showModal(title, buildKVView(data));
}

/** Modal that loads data async, then shows key-value pairs. */
function showAsyncKVModal(title: string, loader: () => Promise<Record<string, any>>): void {
  const content = document.createElement('div');
  content.innerHTML = '<div class="hugr-loading">Loading…</div>';
  showModal(title, content);

  loader()
    .then((data) => content.replaceChildren(buildKVView(data)))
    .catch((err: any) => {
      content.innerHTML = `<div class="hugr-error">${esc(err.message)}</div>`;
    });
}

/* ========== main widget ========== */

export class DuckDBSidebarWidget extends Widget {
  private client: IntrospectClient | null = null;
  private sections: Map<string, HTMLElement> = new Map();

  constructor() {
    super();
    this.addClass('hugr-sidebar');
    this.id = 'duckdb-explorer-sidebar';
    this.title.icon = duckdbIcon;
    this.title.caption = 'DuckDB Explorer';
    this.title.closable = true;
    this.node.innerHTML =
      '<div class="hugr-sidebar-placeholder">No active DuckDB kernel</div>';
  }

  setClient(client: IntrospectClient | null): void {
    this.client = client;
    void this.refresh();
  }

  async refresh(): Promise<void> {
    if (!this.client) {
      this.node.innerHTML =
        '<div class="hugr-sidebar-placeholder">No active DuckDB kernel</div>';
      return;
    }

    this.sections.clear();
    const root = document.createElement('div');
    root.className = 'hugr-sidebar-root';

    const defs: [string, () => Promise<HTMLElement>][] = [
      ['SESSION', () => this.loadSession()],
      ['CATALOG', () => this.loadCatalog()],
      ['EXTENSIONS', () => this.loadExtensions()],
      ['SECRETS', () => this.loadSecrets()],
      ['SETTINGS', () => this.loadSettings()],
      ['MEMORY', () => this.loadMemory()],
      ['RESULT FILES', () => this.loadResultFiles()],
    ];

    for (const [name, loader] of defs) {
      root.appendChild(this.createSection(name, loader));
    }

    this.node.replaceChildren(root);
    this.toggleSection('SESSION');
    this.toggleSection('CATALOG');
  }

  /* ==================== accordion ==================== */

  private createSection(
    title: string,
    loader: () => Promise<HTMLElement>,
  ): HTMLElement {
    const section = document.createElement('div');
    section.className = 'hugr-section';

    const header = document.createElement('div');
    header.className = 'hugr-section-header';

    const toggle = document.createElement('span');
    toggle.className = 'hugr-section-toggle';
    toggle.textContent = '▶';

    const iconEl = document.createElement('span');
    iconEl.className = 'hugr-section-icon';
    iconEl.textContent = SECTION_ICONS[title] || '';

    const label = document.createElement('span');
    label.className = 'hugr-section-label';
    label.textContent = title;

    const refreshBtn = document.createElement('button');
    refreshBtn.className = 'hugr-refresh-btn';
    refreshBtn.textContent = '↻';
    refreshBtn.title = `Refresh ${title}`;
    refreshBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      void this.refreshSection(title, loader);
    });

    header.appendChild(toggle);
    header.appendChild(iconEl);
    header.appendChild(label);
    header.appendChild(refreshBtn);

    const body = document.createElement('div');
    body.className = 'hugr-section-body';
    body.style.display = 'none';

    header.addEventListener('click', (e) => {
      if ((e.target as HTMLElement).closest('button')) return;
      this.toggleSection(title);
    });

    section.appendChild(header);
    section.appendChild(body);

    this.sections.set(title, section);
    (section as any)._loader = loader;
    return section;
  }

  private toggleSection(title: string): void {
    const section = this.sections.get(title);
    if (!section) return;
    const body = section.querySelector('.hugr-section-body') as HTMLElement;
    const toggle = section.querySelector('.hugr-section-toggle') as HTMLElement;
    if (!body || !toggle) return;

    const isOpen = body.style.display !== 'none';
    if (isOpen) {
      body.style.display = 'none';
      toggle.textContent = '▶';
    } else {
      body.style.display = 'block';
      toggle.textContent = '▼';
      if (body.childElementCount === 0) {
        void this.refreshSection(title, (section as any)._loader);
      }
    }
  }

  private async refreshSection(
    title: string,
    loader: () => Promise<HTMLElement>,
  ): Promise<void> {
    const section = this.sections.get(title);
    if (!section) return;
    const body = section.querySelector('.hugr-section-body') as HTMLElement;
    if (!body) return;

    body.innerHTML = '<div class="hugr-loading">Loading…</div>';
    try {
      const content = await loader();
      body.replaceChildren(content);
    } catch (err: any) {
      body.innerHTML = `<div class="hugr-error">${esc(err.message)}</div>`;
    }
  }

  /* ==================== SESSION ==================== */

  private async loadSession(): Promise<HTMLElement> {
    const info = await this.client!.sessionInfo();
    const el = document.createElement('div');
    el.className = 'hugr-session-info';
    el.innerHTML = `
      <div class="hugr-kv"><span class="hugr-kv-key">DuckDB</span><span class="hugr-kv-val">${esc(info.duckdb_version)}</span></div>
      <div class="hugr-kv"><span class="hugr-kv-key">Session</span><span class="hugr-kv-val">${esc(info.session_id)}</span></div>
      <div class="hugr-kv"><span class="hugr-kv-key">Memory</span><span class="hugr-kv-val">${info.kernel_mem_mb} MB</span></div>
    `;
    return el;
  }

  /* ==================== CATALOG ==================== */

  private async loadCatalog(): Promise<HTMLElement> {
    const container = document.createElement('div');
    container.className = 'hugr-catalog-tree';

    const databases = await this.client!.databases();
    for (const db of databases) {
      container.appendChild(this.createCatalogDB(db));
    }

    // System Functions — top-level category, grouped by name
    container.appendChild(createExpandableNode({
      label: 'System Functions',
      showRefresh: true,
      loader: async (childrenEl) => {
        const fns = await this.client!.systemFunctions();
        const nodes = this.buildGroupedFunctions(fns);
        for (const n of nodes) childrenEl.appendChild(n);
      },
    }));

    if (databases.length === 0) {
      container.innerHTML = '<div class="hugr-empty">(no databases)</div>';
    }
    return container;
  }

  private createCatalogDB(db: any): HTMLElement {
    const desc = [db.type ?? '', db.readonly ? 'read-only' : '']
      .filter(Boolean).join(', ');
    const node = createExpandableNode({
      objType: 'database',
      label: db.database_name,
      description: desc ? `(${desc})` : undefined,
      showRefresh: true,
      loader: async (container) => {
        const schemas = await this.client!.schemas(db.database_name);
        for (const s of schemas) {
          container.appendChild(
            this.createCatalogSchema(db.database_name, s.schema_name),
          );
        }
      },
    });
    // Info button → modal with full duckdb_databases() info
    this.addAsyncInfoButton(node, `Database: ${db.database_name}`, async () => {
      const rows = await this.client!.databaseInfo(db.database_name);
      return rows[0] || db;
    });
    return node;
  }

  private createCatalogSchema(database: string, schema: string): HTMLElement {
    return createExpandableNode({
      objType: 'schema',
      label: schema,
      showRefresh: true,
      loader: async (container) => {
        container.appendChild(this.createCatalogCategory(
          'Tables', database, schema,
          () => this.loadCatalogTables(database, schema),
        ));
        container.appendChild(this.createCatalogCategory(
          'Views', database, schema,
          () => this.loadCatalogViews(database, schema),
        ));
        container.appendChild(this.createCatalogCategory(
          'Functions', database, schema,
          () => this.loadCatalogFunctions(schema),
        ));
        container.appendChild(this.createCatalogCategory(
          'Indexes', database, schema,
          () => this.loadCatalogIndexes(database, schema),
        ));
      },
    });
  }

  private createCatalogCategory(
    label: string,
    _database: string,
    _schema: string,
    loader: () => Promise<HTMLElement[]>,
  ): HTMLElement {
    return createExpandableNode({
      label,
      showRefresh: true,
      loader: async (container) => {
        const nodes = await loader();
        for (const n of nodes) container.appendChild(n);
      },
    });
  }

  /* --- catalog loaders --- */

  private async loadCatalogTables(
    database: string,
    schema: string,
  ): Promise<HTMLElement[]> {
    const tables = await this.client!.tables(database, schema);
    return tables.map((t: any) => {
      const node = createExpandableNode({
        objType: 'table',
        label: t.table_name,
        description: t.column_count != null ? `${t.column_count} cols` : undefined,
        loader: async (container) => {
          const cols = await this.client!.columns(database, schema, t.table_name);
          for (const c of cols) container.appendChild(createColumnNode(c));
        },
      });
      this.addInfoButton(node, t.table_name, database, schema, 'table');
      return node;
    });
  }

  private async loadCatalogViews(
    database: string,
    schema: string,
  ): Promise<HTMLElement[]> {
    const views = await this.client!.views(database, schema);
    return views.map((v: any) => {
      const node = createExpandableNode({
        objType: 'view',
        label: v.view_name,
        description: v.column_count != null ? `${v.column_count} cols` : undefined,
        loader: async (container) => {
          const cols = await this.client!.columns(database, schema, v.view_name);
          for (const c of cols) container.appendChild(createColumnNode(c));
        },
      });
      this.addInfoButton(node, v.view_name, database, schema, 'view');
      return node;
    });
  }

  private async loadCatalogFunctions(schema: string): Promise<HTMLElement[]> {
    const fns = await this.client!.functions(schema);
    return this.buildGroupedFunctions(fns);
  }

  private async loadCatalogIndexes(
    database: string,
    schema: string,
  ): Promise<HTMLElement[]> {
    const idxs = await this.client!.indexes(database, schema);
    return idxs.map((i: any) =>
      createLeafNode('index', i.index_name, i.is_unique ? 'unique' : ''),
    );
  }

  /* --- function grouping --- */

  /**
   * Group functions by name. Single overload → leaf with ℹ.
   * Multiple overloads → expandable node, children are overloads with ℹ.
   */
  private buildGroupedFunctions(fns: any[]): HTMLElement[] {
    // Group by function_name
    const groups = new Map<string, any[]>();
    for (const f of fns) {
      const name = f.function_name ?? '';
      if (!groups.has(name)) groups.set(name, []);
      groups.get(name)!.push(f);
    }

    const result: HTMLElement[] = [];
    for (const [name, overloads] of groups) {
      if (overloads.length === 1) {
        const f = overloads[0];
        const sig = this.formatFuncSignature(f);
        const node = createLeafNode('function', name, sig);
        this.addGenericInfoButton(node, `Function: ${name}`, f);
        result.push(node);
      } else {
        const node = createExpandableNode({
          objType: 'function',
          label: name,
          description: `${overloads.length} overloads`,
          loader: async (container) => {
            for (const f of overloads) {
              const sig = this.formatFuncSignature(f);
              const child = createLeafNode('function', sig, f.function_type ?? '');
              this.addGenericInfoButton(child, `${name}(${f.parameter_types ?? ''})`, f);
              container.appendChild(child);
            }
          },
        });
        result.push(node);
      }
    }
    return result;
  }

  private formatFuncSignature(f: any): string {
    const params = f.parameter_types ?? f.parameters ?? '';
    const ret = f.return_type ?? '';
    if (!params && !ret) return f.function_type ?? '';
    if (!params) return `→ ${ret}`;
    return `(${params}) → ${ret}`;
  }

  /* --- info button & modal --- */

  private addInfoButton(
    node: HTMLElement,
    name: string,
    database: string,
    schema: string,
    kind: 'table' | 'view',
  ): void {
    const row = node.querySelector('.hugr-tree-row') as HTMLElement;
    if (!row) return;

    const btn = document.createElement('button');
    btn.className = 'hugr-info-btn';
    btn.textContent = 'ℹ';
    btn.title = `Details: ${name}`;
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      void this.openDetailModal(name, database, schema, kind);
    });
    row.appendChild(btn);
  }

  private async openDetailModal(
    name: string,
    database: string,
    schema: string,
    kind: 'table' | 'view',
  ): Promise<void> {
    const content = document.createElement('div');
    content.className = 'hugr-modal-content';

    const tabBar = document.createElement('div');
    tabBar.className = 'hugr-modal-tabs';

    const infoTab = document.createElement('button');
    infoTab.className = 'hugr-modal-tab hugr-modal-tab-active';
    infoTab.textContent = 'INFO';

    const descTab = document.createElement('button');
    descTab.className = 'hugr-modal-tab';
    descTab.textContent = 'DESCRIBE';

    const sumTab = document.createElement('button');
    sumTab.className = 'hugr-modal-tab';
    sumTab.textContent = 'SUMMARIZE';

    tabBar.appendChild(infoTab);
    tabBar.appendChild(descTab);
    tabBar.appendChild(sumTab);
    content.appendChild(tabBar);

    const tabContent = document.createElement('div');
    tabContent.className = 'hugr-modal-tab-content';
    content.appendChild(tabContent);

    const kindLabel = kind === 'table' ? 'Table' : 'View';
    showModal(`${kindLabel}: ${database}.${schema}.${name}`, content);

    const allTabs = [infoTab, descTab, sumTab];
    const activateTab = (active: HTMLElement) => {
      for (const t of allTabs) t.classList.remove('hugr-modal-tab-active');
      active.classList.add('hugr-modal-tab-active');
    };

    const loadInfo = async () => {
      tabContent.innerHTML = '<div class="hugr-loading">Loading…</div>';
      try {
        const rows = kind === 'table'
          ? await this.client!.tableInfo(database, schema, name)
          : await this.client!.viewInfo(database, schema, name);
        if (rows.length > 0) {
          tabContent.replaceChildren(buildKVView(rows[0]));
        } else {
          tabContent.innerHTML = '<div class="hugr-empty">(no info)</div>';
        }
      } catch (err: any) {
        tabContent.innerHTML = `<div class="hugr-error">${esc(err.message)}</div>`;
      }
    };

    const loadDescribe = async () => {
      tabContent.innerHTML = '<div class="hugr-loading">Loading…</div>';
      try {
        const data = await this.client!.describe(database, schema, name);
        tabContent.replaceChildren(buildAutoTable(data));
      } catch (err: any) {
        tabContent.innerHTML = `<div class="hugr-error">${esc(err.message)}</div>`;
      }
    };

    const loadSummarize = async () => {
      tabContent.innerHTML = '<div class="hugr-loading">Loading…</div>';
      try {
        const data = await this.client!.summarize(database, schema, name);
        tabContent.replaceChildren(buildAutoTable(data));
      } catch (err: any) {
        tabContent.innerHTML = `<div class="hugr-error">${esc(err.message)}</div>`;
      }
    };

    infoTab.addEventListener('click', () => { activateTab(infoTab); void loadInfo(); });
    descTab.addEventListener('click', () => { activateTab(descTab); void loadDescribe(); });
    sumTab.addEventListener('click', () => { activateTab(sumTab); void loadSummarize(); });

    void loadInfo();
  }

  /* ==================== TABLE sections ==================== */

  private async loadExtensions(): Promise<HTMLElement> {
    const data = await this.client!.extensions();
    return this.buildTable(
      ['Name', 'Status', 'Version', 'Description'],
      data.map((e: any) => [
        e.extension_name,
        String(e.loaded) === 'true' ? '✅ loaded' : String(e.installed) === 'true' ? '📦 installed' : '○ available',
        e.extension_version ?? '',
        e.description ?? '',
      ]),
    );
  }

  private async loadSecrets(): Promise<HTMLElement> {
    const data = await this.client!.secrets();
    return this.buildTableWithInfo(
      ['Name', 'Type', 'Provider', 'Scope'],
      data.map((s: any) => ({
        cells: [s.name, s.type, s.provider, s.scope ?? ''],
        modalTitle: `Secret: ${s.name}`,
        modalData: s,
      })),
    );
  }

  private async loadSettings(): Promise<HTMLElement> {
    const data = await this.client!.settings();
    return this.buildTable(
      ['Name', 'Value', 'Type', 'Description'],
      data.map((s: any) => [
        s.name, String(s.value ?? ''), s.input_type ?? '', s.description ?? '',
      ]),
    );
  }

  private async loadMemory(): Promise<HTMLElement> {
    const data = await this.client!.memory();
    return this.buildTable(
      ['Tag', 'Memory', 'Temp Storage'],
      data.map((m: any) => [
        m.tag, formatBytes(m.memory_usage_bytes), formatBytes(m.temporary_storage_bytes),
      ]),
    );
  }

  private async loadResultFiles(): Promise<HTMLElement> {
    const data = await this.client!.spoolFiles();
    const wrapper = document.createElement('div');
    wrapper.className = 'hugr-table-wrapper';

    if (data.length === 0) {
      wrapper.innerHTML = '<div class="hugr-empty">(empty)</div>';
      return wrapper;
    }

    const table = document.createElement('table');
    table.className = 'hugr-table';

    const thead = document.createElement('thead');
    const headRow = document.createElement('tr');
    for (const h of ['', 'Query ID', 'Size', 'Created']) {
      const th = document.createElement('th');
      th.textContent = h;
      headRow.appendChild(th);
    }
    thead.appendChild(headRow);
    table.appendChild(thead);

    const tbody = document.createElement('tbody');
    for (const f of data) {
      const tr = document.createElement('tr');

      // Delete button cell
      const tdDel = document.createElement('td');
      tdDel.style.padding = '2px 4px';
      const delBtn = document.createElement('button');
      delBtn.className = 'hugr-delete-btn';
      delBtn.textContent = '✕';
      delBtn.title = `Delete ${f.query_id}`;
      delBtn.addEventListener('click', async () => {
        try {
          await this.client!.deleteSpoolFile(f.query_id);
          tr.remove();
        } catch (err: any) {
          delBtn.title = `Error: ${err.message}`;
        }
      });
      tdDel.appendChild(delBtn);
      tr.appendChild(tdDel);

      for (const cell of [f.query_id, formatBytes(f.size_bytes), f.created_at ?? '']) {
        const td = document.createElement('td');
        td.textContent = cell;
        td.title = cell;
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    wrapper.appendChild(table);
    return wrapper;
  }

  /* --- generic info buttons for tree nodes --- */

  private addGenericInfoButton(
    node: HTMLElement,
    title: string,
    data: Record<string, any>,
  ): void {
    const row = node.querySelector('.hugr-tree-row') as HTMLElement;
    if (!row) return;

    const btn = document.createElement('button');
    btn.className = 'hugr-info-btn';
    btn.textContent = 'ℹ';
    btn.title = title;
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      showKVModal(title, data);
    });
    row.appendChild(btn);
  }

  private addAsyncInfoButton(
    node: HTMLElement,
    title: string,
    loader: () => Promise<Record<string, any>>,
  ): void {
    const row = node.querySelector('.hugr-tree-row') as HTMLElement;
    if (!row) return;

    const btn = document.createElement('button');
    btn.className = 'hugr-info-btn';
    btn.textContent = 'ℹ';
    btn.title = title;
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      showAsyncKVModal(title, loader);
    });
    row.appendChild(btn);
  }

  /* ==================== table builder ==================== */

  /**
   * Table with an ℹ button per row that opens a key-value modal.
   */
  private buildTableWithInfo(
    headers: string[],
    rows: { cells: string[]; modalTitle: string; modalData: Record<string, any> }[],
  ): HTMLElement {
    const wrapper = document.createElement('div');
    wrapper.className = 'hugr-table-wrapper';

    if (rows.length === 0) {
      wrapper.innerHTML = '<div class="hugr-empty">(empty)</div>';
      return wrapper;
    }

    const table = document.createElement('table');
    table.className = 'hugr-table';

    const thead = document.createElement('thead');
    const headRow = document.createElement('tr');
    // Extra column for info button
    const thInfo = document.createElement('th');
    thInfo.style.width = '28px';
    headRow.appendChild(thInfo);
    for (const h of headers) {
      const th = document.createElement('th');
      th.textContent = h;
      headRow.appendChild(th);
    }
    thead.appendChild(headRow);
    table.appendChild(thead);

    const tbody = document.createElement('tbody');
    for (const row of rows) {
      const tr = document.createElement('tr');
      // Info button cell
      const tdBtn = document.createElement('td');
      tdBtn.style.padding = '2px 4px';
      const btn = document.createElement('button');
      btn.className = 'hugr-info-btn';
      btn.textContent = 'ℹ';
      btn.title = row.modalTitle;
      btn.addEventListener('click', () => showKVModal(row.modalTitle, row.modalData));
      tdBtn.appendChild(btn);
      tr.appendChild(tdBtn);

      for (const cell of row.cells) {
        const td = document.createElement('td');
        td.textContent = cell;
        td.title = cell;
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    wrapper.appendChild(table);
    return wrapper;
  }

  private buildTable(headers: string[], rows: string[][]): HTMLElement {
    const wrapper = document.createElement('div');
    wrapper.className = 'hugr-table-wrapper';

    if (rows.length === 0) {
      wrapper.innerHTML = '<div class="hugr-empty">(empty)</div>';
      return wrapper;
    }

    const table = document.createElement('table');
    table.className = 'hugr-table';

    const thead = document.createElement('thead');
    const headRow = document.createElement('tr');
    for (const h of headers) {
      const th = document.createElement('th');
      th.textContent = h;
      headRow.appendChild(th);
    }
    thead.appendChild(headRow);
    table.appendChild(thead);

    const tbody = document.createElement('tbody');
    for (const row of rows) {
      const tr = document.createElement('tr');
      for (const cell of row) {
        const td = document.createElement('td');
        td.textContent = cell;
        td.title = cell;
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    wrapper.appendChild(table);
    return wrapper;
  }
}
