/**
 * VS Code TreeDataProviders for DuckDB database explorer.
 *
 * Multiple providers, one per view section:
 * - SessionProvider: session info (flat)
 * - CatalogProvider: databases -> schemas -> tables/views/functions/indexes -> columns
 * - SystemFunctionsProvider: grouped system functions
 * - SimpleListProvider: generic flat list for extensions, secrets, settings, memory, result files
 */

import * as vscode from 'vscode';
import { IntrospectClient } from './introspectClient';

type NodeType =
  | 'session'
  | 'database'
  | 'schema'
  | 'category'
  | 'table'
  | 'view'
  | 'function'
  | 'function_group'
  | 'index'
  | 'column'
  | 'extension'
  | 'secret'
  | 'setting'
  | 'memory'
  | 'spool_file'
  | 'info';

interface TreeNode {
  type: NodeType;
  label: string;
  description?: string;
  tooltip?: string;
  category?: string;
  database?: string;
  schema?: string;
  table?: string;
  collapsible: boolean;
  iconId?: string;
  data?: any;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatBytes(bytes: unknown): string {
  const n = Number(bytes);
  if (!Number.isFinite(n) || n === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB'];
  let i = 0;
  let val = n;
  while (val >= 1024 && i < units.length - 1) { val /= 1024; i++; }
  return `${val.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

function formatFuncSig(f: any): string {
  const params = f.parameter_types ?? f.parameters ?? '';
  const ret = f.return_type ?? '';
  if (!params && !ret) return f.function_type ?? '';
  if (!params) return `\u2192 ${ret}`;
  return `(${params}) \u2192 ${ret}`;
}

function columnIcon(dataType: string): string {
  const dt = (dataType || '').toUpperCase();
  if (/^(BIG|SMALL|TINY|U?HUGE)?INT|INTEGER/.test(dt)) return 'symbol-number';
  if (/DOUBLE|FLOAT|REAL|DECIMAL|NUMERIC/.test(dt)) return 'symbol-number';
  if (/VARCHAR|TEXT|CHAR|STRING|UUID/.test(dt)) return 'symbol-string';
  if (/BOOL/.test(dt)) return 'symbol-boolean';
  if (/DATE|TIME|TIMESTAMP|INTERVAL/.test(dt)) return 'calendar';
  if (/BLOB/.test(dt)) return 'file-binary';
  if (/LIST|ARRAY/.test(dt)) return 'symbol-array';
  if (/STRUCT|MAP|UNION/.test(dt)) return 'symbol-struct';
  return 'symbol-field';
}

function noKernelItem(): TreeNode {
  return { type: 'info', label: 'No active DuckDB kernel', collapsible: false, iconId: 'info' };
}

function errorItem(err: any): TreeNode {
  const msg = err?.message ?? String(err);
  // Connection errors → show friendly placeholder instead of error
  if (msg.includes('ECONNREFUSED') || msg.includes('fetch failed') || msg.includes('Failed to fetch') || msg.includes('ENOTFOUND') || msg.includes('socket hang up')) {
    return { type: 'info', label: 'Run a query to connect', collapsible: false, iconId: 'info' };
  }
  return { type: 'info', label: `Error: ${msg}`, collapsible: false, iconId: 'error' };
}

function groupFunctions(fns: any[]): TreeNode[] {
  const groups = new Map<string, any[]>();
  for (const f of fns) {
    const name = f.function_name ?? '';
    if (!groups.has(name)) groups.set(name, []);
    groups.get(name)!.push(f);
  }

  const result: TreeNode[] = [];
  for (const [name, overloads] of groups) {
    if (overloads.length === 1) {
      const f = overloads[0];
      const sig = formatFuncSig(f);
      result.push({
        type: 'function',
        label: name,
        description: sig,
        tooltip: `${name}(${f.parameter_types ?? f.parameters ?? ''}) \u2192 ${f.return_type ?? ''}`,
        collapsible: false,
        iconId: 'symbol-method',
      });
    } else {
      const children: TreeNode[] = overloads.map((f: any) => ({
        type: 'function' as const,
        label: `(${f.parameter_types ?? f.parameters ?? ''}) \u2192 ${f.return_type ?? ''}`,
        description: f.function_type ?? '',
        tooltip: f.description ?? name,
        collapsible: false,
        iconId: 'symbol-method',
      }));
      result.push({
        type: 'function_group',
        label: name,
        description: `${overloads.length} overloads`,
        collapsible: true,
        iconId: 'symbol-method',
        data: children,
      });
    }
  }
  return result;
}

// ---------------------------------------------------------------------------
// Base tree item rendering (shared)
// ---------------------------------------------------------------------------

function toTreeItem(node: TreeNode): vscode.TreeItem {
  const item = new vscode.TreeItem(
    node.label,
    node.collapsible
      ? vscode.TreeItemCollapsibleState.Collapsed
      : vscode.TreeItemCollapsibleState.None,
  );
  if (node.description) item.description = node.description;
  if (node.tooltip) item.tooltip = node.tooltip;
  if (node.iconId) item.iconPath = new vscode.ThemeIcon(node.iconId);
  item.contextValue = node.type;
  return item;
}

// ---------------------------------------------------------------------------
// SessionProvider
// ---------------------------------------------------------------------------

export class SessionProvider implements vscode.TreeDataProvider<TreeNode> {
  private _onDidChangeTreeData = new vscode.EventEmitter<TreeNode | undefined | void>();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;
  private client: IntrospectClient | null = null;

  setClient(client: IntrospectClient | null): void {
    this.client = client;
    this._onDidChangeTreeData.fire();
  }

  refresh(): void {
    this._onDidChangeTreeData.fire();
  }

  getTreeItem(node: TreeNode): vscode.TreeItem {
    return toTreeItem(node);
  }

  async getChildren(): Promise<TreeNode[]> {
    if (!this.client) return [noKernelItem()];
    try {
      const info = await this.client.sessionInfo();
      return [
        {
          type: 'session',
          label: `DuckDB ${info.duckdb_version}`,
          description: `Session: ${info.session_id?.slice(0, 8)}\u2026`,
          tooltip: `Session ID: ${info.session_id}\nDuckDB: ${info.duckdb_version}\nMemory: ${info.kernel_mem_mb} MB`,
          collapsible: false,
          iconId: 'server-environment',
        },
      ];
    } catch (err: any) {
      return [errorItem(err)];
    }
  }
}

// ---------------------------------------------------------------------------
// CatalogProvider
// ---------------------------------------------------------------------------

export class CatalogProvider implements vscode.TreeDataProvider<TreeNode> {
  private _onDidChangeTreeData = new vscode.EventEmitter<TreeNode | undefined | void>();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;
  private client: IntrospectClient | null = null;
  private generation = 0;

  setClient(client: IntrospectClient | null): void {
    this.client = client;
    this.generation++;
    this._onDidChangeTreeData.fire();
  }

  refresh(): void {
    this.generation++;
    this._onDidChangeTreeData.fire();
  }

  refreshNode(_node?: TreeNode): void {
    this.generation++;
    this._onDidChangeTreeData.fire();
  }

  getTreeItem(node: TreeNode): vscode.TreeItem {
    const item = toTreeItem(node);
    // Include generation in the id so VS Code treats refreshed nodes as new
    // (forgets their expanded state → they collapse).
    if (node.collapsible) {
      const base = [node.type, node.database, node.schema, node.category, node.table, node.label]
        .filter(Boolean).join('/');
      item.id = `${base}#${this.generation}`;
    }
    return item;
  }

  async getChildren(element?: TreeNode): Promise<TreeNode[]> {
    if (!this.client) return [noKernelItem()];
    try {
      if (!element) return this.getDatabaseNodes();
      switch (element.type) {
        case 'database': return this.getSchemaNodes(element.database!);
        case 'schema': return this.getCategoryNodes(element.database!, element.schema!);
        case 'category': return this.getCategoryChildren(element);
        case 'table':
        case 'view': return this.getColumnNodes(element.database!, element.schema!, element.table!);
        case 'function_group': return (element.data as TreeNode[]) || [];
        default: return [];
      }
    } catch (err: any) {
      return [errorItem(err)];
    }
  }

  private async getDatabaseNodes(): Promise<TreeNode[]> {
    const databases = await this.client!.databases();
    return databases.map((db: any) => {
      const desc = [db.type ?? '', String(db.readonly) === 'true' ? 'read-only' : '']
        .filter(Boolean).join(', ');
      return {
        type: 'database' as const,
        label: db.database_name,
        description: desc || undefined,
        database: db.database_name,
        collapsible: true,
        iconId: 'database',
      };
    });
  }

  private async getSchemaNodes(database: string): Promise<TreeNode[]> {
    const schemas = await this.client!.schemas(database);
    return schemas.map((s: any) => ({
      type: 'schema' as const,
      label: s.schema_name,
      database,
      schema: s.schema_name,
      collapsible: true,
      iconId: 'symbol-namespace',
    }));
  }

  private getCategoryNodes(database: string, schema: string): TreeNode[] {
    return [
      { type: 'category', label: 'Tables', category: 'tables', database, schema, collapsible: true, iconId: 'symbol-class' },
      { type: 'category', label: 'Views', category: 'views', database, schema, collapsible: true, iconId: 'eye' },
      { type: 'category', label: 'Functions', category: 'functions', database, schema, collapsible: true, iconId: 'symbol-method' },
      { type: 'category', label: 'Indexes', category: 'indexes', database, schema, collapsible: true, iconId: 'list-tree' },
    ];
  }

  private async getCategoryChildren(node: TreeNode): Promise<TreeNode[]> {
    switch (node.category) {
      case 'tables': return this.getTableNodes(node.database!, node.schema!);
      case 'views': return this.getViewNodes(node.database!, node.schema!);
      case 'functions': return this.getFunctionNodes(node.schema!);
      case 'indexes': return this.getIndexNodes(node.database!, node.schema!);
      default: return [];
    }
  }

  private async getTableNodes(database: string, schema: string): Promise<TreeNode[]> {
    const tables = await this.client!.tables(database, schema);
    return tables.map((t: any) => ({
      type: 'table' as const,
      label: t.table_name,
      description: t.column_count != null ? `${t.column_count} cols` : undefined,
      database,
      schema,
      table: t.table_name,
      collapsible: true,
      iconId: 'symbol-class',
    }));
  }

  private async getViewNodes(database: string, schema: string): Promise<TreeNode[]> {
    const views = await this.client!.views(database, schema);
    return views.map((v: any) => ({
      type: 'view' as const,
      label: v.view_name,
      description: v.column_count != null ? `${v.column_count} cols` : undefined,
      tooltip: v.sql,
      database,
      schema,
      table: v.view_name,
      collapsible: true,
      iconId: 'eye',
    }));
  }

  private async getFunctionNodes(schema: string): Promise<TreeNode[]> {
    const fns = await this.client!.functions(schema);
    return groupFunctions(fns);
  }

  private async getIndexNodes(database: string, schema: string): Promise<TreeNode[]> {
    const indexes = await this.client!.indexes(database, schema);
    return indexes.map((idx: any) => ({
      type: 'index' as const,
      label: idx.index_name,
      description: idx.is_unique ? 'unique' : '',
      tooltip: idx.sql,
      collapsible: false,
      iconId: 'list-tree',
    }));
  }

  private async getColumnNodes(database: string, schema: string, table: string): Promise<TreeNode[]> {
    const cols = await this.client!.columns(database, schema, table);
    return cols.map((c: any) => {
      const iconId = columnIcon(c.data_type);
      return {
        type: 'column' as const,
        label: c.column_name,
        description: c.data_type + (c.is_nullable === 'YES' ? ' (nullable)' : ''),
        tooltip: `${c.column_name} ${c.data_type}${c.is_nullable === 'YES' ? ' (nullable)' : ''}${c.column_default ? ` default: ${c.column_default}` : ''}`,
        collapsible: false,
        iconId,
      };
    });
  }
}

// ---------------------------------------------------------------------------
// SystemFunctionsProvider
// ---------------------------------------------------------------------------

export class SystemFunctionsProvider implements vscode.TreeDataProvider<TreeNode> {
  private _onDidChangeTreeData = new vscode.EventEmitter<TreeNode | undefined | void>();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;
  private client: IntrospectClient | null = null;
  private generation = 0;

  setClient(client: IntrospectClient | null): void {
    this.client = client;
    this.generation++;
    this._onDidChangeTreeData.fire();
  }

  refresh(): void {
    this.generation++;
    this._onDidChangeTreeData.fire();
  }

  getTreeItem(node: TreeNode): vscode.TreeItem {
    const item = toTreeItem(node);
    if (node.collapsible) {
      item.id = `${node.type}/${node.label}#${this.generation}`;
    }
    return item;
  }

  async getChildren(element?: TreeNode): Promise<TreeNode[]> {
    if (!this.client) return [noKernelItem()];
    try {
      if (!element) {
        const fns = await this.client.systemFunctions();
        return groupFunctions(fns);
      }
      if (element.type === 'function_group') {
        return (element.data as TreeNode[]) || [];
      }
      return [];
    } catch (err: any) {
      return [errorItem(err)];
    }
  }
}

// ---------------------------------------------------------------------------
// SimpleListProvider - generic flat-list provider
// ---------------------------------------------------------------------------

type FetchFn = (client: IntrospectClient) => Promise<TreeNode[]>;

export class SimpleListProvider implements vscode.TreeDataProvider<TreeNode> {
  private _onDidChangeTreeData = new vscode.EventEmitter<TreeNode | undefined | void>();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;
  private client: IntrospectClient | null = null;
  private fetchItems: FetchFn;

  constructor(fetchItems: FetchFn) {
    this.fetchItems = fetchItems;
  }

  setClient(client: IntrospectClient | null): void {
    this.client = client;
    this._onDidChangeTreeData.fire();
  }

  refresh(): void {
    this._onDidChangeTreeData.fire();
  }

  getTreeItem(node: TreeNode): vscode.TreeItem {
    return toTreeItem(node);
  }

  async getChildren(): Promise<TreeNode[]> {
    if (!this.client) return [noKernelItem()];
    try {
      return await this.fetchItems(this.client);
    } catch (err: any) {
      return [errorItem(err)];
    }
  }
}

// ---------------------------------------------------------------------------
// Factory functions for SimpleListProvider fetch callbacks
// ---------------------------------------------------------------------------

export function extensionsFetch(client: IntrospectClient): Promise<TreeNode[]> {
  return client.extensions().then((exts) =>
    exts.map((e: any) => {
      const loaded = String(e.loaded) === 'true';
      const installed = String(e.installed) === 'true';
      return {
        type: 'extension' as const,
        label: e.extension_name,
        description: loaded ? 'loaded' : installed ? 'installed' : 'available',
        tooltip: `${e.description || e.extension_name}\nVersion: ${e.extension_version ?? 'n/a'}`,
        collapsible: false,
        iconId: loaded ? 'check' : installed ? 'package' : 'cloud',
        data: e,
      };
    }),
  );
}

export function secretsFetch(client: IntrospectClient): Promise<TreeNode[]> {
  return client.secrets().then((secrets) =>
    secrets.map((s: any) => ({
      type: 'secret' as const,
      label: s.name,
      description: `${s.type} (${s.provider})`,
      tooltip: `Scope: ${s.scope}\nPersistent: ${s.persistent ?? 'n/a'}\nStorage: ${s.storage ?? 'n/a'}`,
      collapsible: false,
      iconId: 'lock',
      data: s,
    })),
  );
}

export function settingsFetch(client: IntrospectClient): Promise<TreeNode[]> {
  return client.settings().then((settings) =>
    settings.map((s: any) => ({
      type: 'setting' as const,
      label: s.name,
      description: String(s.value ?? ''),
      tooltip: `${s.description || s.name}\nType: ${s.input_type ?? ''}\nScope: ${s.scope ?? ''}`,
      collapsible: false,
      iconId: 'gear',
    })),
  );
}

export function memoryFetch(client: IntrospectClient): Promise<TreeNode[]> {
  return client.memory().then((mem) =>
    mem.map((m: any) => ({
      type: 'memory' as const,
      label: m.tag,
      description: formatBytes(m.memory_usage_bytes),
      tooltip: `Memory: ${formatBytes(m.memory_usage_bytes)}\nTemp storage: ${formatBytes(m.temporary_storage_bytes)}`,
      collapsible: false,
      iconId: 'pulse',
    })),
  );
}

export function resultFilesFetch(client: IntrospectClient): Promise<TreeNode[]> {
  return client.spoolFiles().then((files) =>
    files.map((f: any) => ({
      type: 'spool_file' as const,
      label: f.query_id,
      description: formatBytes(f.size_bytes),
      tooltip: `Query: ${f.query_id}\nSize: ${formatBytes(f.size_bytes)}\nCreated: ${f.created_at}`,
      collapsible: false,
      iconId: 'file-binary',
      data: f,
    })),
  );
}
