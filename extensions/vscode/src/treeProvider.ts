/**
 * VS Code TreeDataProvider for DuckDB database explorer.
 *
 * Shows a lazy-loading tree: databases → schemas → tables/views/functions → columns.
 * Also shows session info, extensions, secrets, settings, memory, spool files.
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
  /** Category name for folder grouping (e.g. "Tables", "Views"). */
  category?: string;
  /** Context for child queries. */
  database?: string;
  schema?: string;
  table?: string;
  collapsible: boolean;
  iconId?: string;
  data?: any;
}

export class DuckDBTreeProvider implements vscode.TreeDataProvider<TreeNode> {
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

  async getChildren(element?: TreeNode): Promise<TreeNode[]> {
    if (!this.client) {
      return [{ type: 'info', label: 'No active DuckDB kernel', collapsible: false, iconId: 'info' }];
    }

    try {
      if (!element) {
        return this.getRootNodes();
      }

      switch (element.type) {
        case 'database':
          return this.getSchemaNodes(element.database!);
        case 'schema':
          return this.getCategoryNodes(element.database!, element.schema!);
        case 'category':
          return this.getCategoryChildren(element);
        case 'table':
        case 'view':
          return this.getColumnNodes(element.database!, element.schema!, element.table!);
        default:
          return [];
      }
    } catch (err: any) {
      return [{ type: 'info', label: `Error: ${err.message}`, collapsible: false, iconId: 'error' }];
    }
  }

  private async getRootNodes(): Promise<TreeNode[]> {
    const nodes: TreeNode[] = [];

    // Session info header.
    try {
      const info = await this.client!.sessionInfo();
      nodes.push({
        type: 'session',
        label: `DuckDB ${info.duckdb_version}`,
        description: `Session: ${info.session_id?.slice(0, 8)}…`,
        tooltip: `Session ID: ${info.session_id}\nDuckDB: ${info.duckdb_version}\nMemory: ${info.kernel_mem_mb} MB`,
        collapsible: false,
        iconId: 'server-environment',
      });
    } catch {
      // Skip if session info fails.
    }

    // Databases.
    const databases = await this.client!.databases();
    for (const db of databases) {
      nodes.push({
        type: 'database',
        label: db.database_name,
        description: db.readonly ? 'read-only' : undefined,
        database: db.database_name,
        collapsible: true,
        iconId: 'database',
      });
    }

    // Extensions section.
    nodes.push({
      type: 'category',
      label: 'Extensions',
      category: 'extensions',
      collapsible: true,
      iconId: 'extensions',
    });

    // Secrets section.
    nodes.push({
      type: 'category',
      label: 'Secrets',
      category: 'secrets',
      collapsible: true,
      iconId: 'lock',
    });

    // Settings section.
    nodes.push({
      type: 'category',
      label: 'Settings',
      category: 'settings',
      collapsible: true,
      iconId: 'gear',
    });

    // Memory section.
    nodes.push({
      type: 'category',
      label: 'Memory',
      category: 'memory',
      collapsible: true,
      iconId: 'pulse',
    });

    // Spool files section.
    nodes.push({
      type: 'category',
      label: 'Result Files',
      category: 'spool_files',
      collapsible: true,
      iconId: 'file-binary',
    });

    return nodes;
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
      case 'tables':
        return this.getTableNodes(node.database!, node.schema!);
      case 'views':
        return this.getViewNodes(node.database!, node.schema!);
      case 'functions':
        return this.getFunctionNodes(node.schema!);
      case 'indexes':
        return this.getIndexNodes(node.database!, node.schema!);
      case 'extensions':
        return this.getExtensionNodes();
      case 'secrets':
        return this.getSecretNodes();
      case 'settings':
        return this.getSettingNodes();
      case 'memory':
        return this.getMemoryNodes();
      case 'spool_files':
        return this.getSpoolNodes();
      default:
        return [];
    }
  }

  private async getTableNodes(database: string, schema: string): Promise<TreeNode[]> {
    const tables = await this.client!.tables(database, schema);
    return tables.map((t: any) => ({
      type: 'table' as const,
      label: t.table_name,
      description: `${t.column_count ?? ''} cols`,
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
      description: `${v.column_count ?? ''} cols`,
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
    return fns.map((f: any) => ({
      type: 'function' as const,
      label: f.function_name,
      description: f.function_type,
      tooltip: `${f.function_name}(${f.parameters ?? ''}) → ${f.return_type ?? ''}`,
      collapsible: false,
      iconId: 'symbol-method',
    }));
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
    return cols.map((c: any) => ({
      type: 'column' as const,
      label: c.column_name,
      description: c.data_type,
      tooltip: `${c.column_name} ${c.data_type}${c.is_nullable === 'YES' ? ' (nullable)' : ''}${c.column_default ? ` default: ${c.column_default}` : ''}`,
      collapsible: false,
      iconId: 'symbol-field',
    }));
  }

  private async getExtensionNodes(): Promise<TreeNode[]> {
    const exts = await this.client!.extensions();
    return exts.map((e: any) => ({
      type: 'extension' as const,
      label: e.extension_name,
      description: e.loaded ? 'loaded' : e.installed ? 'installed' : 'available',
      tooltip: e.description || e.extension_name,
      collapsible: false,
      iconId: e.loaded ? 'check' : e.installed ? 'package' : 'cloud',
    }));
  }

  private async getSecretNodes(): Promise<TreeNode[]> {
    const secrets = await this.client!.secrets();
    return secrets.map((s: any) => ({
      type: 'secret' as const,
      label: s.name,
      description: `${s.type} (${s.provider})`,
      tooltip: `Scope: ${s.scope}`,
      collapsible: false,
      iconId: 'lock',
    }));
  }

  private async getSettingNodes(): Promise<TreeNode[]> {
    const settings = await this.client!.settings();
    return settings.map((s: any) => ({
      type: 'setting' as const,
      label: s.name,
      description: String(s.value),
      tooltip: s.description || s.name,
      collapsible: false,
      iconId: 'gear',
    }));
  }

  private async getMemoryNodes(): Promise<TreeNode[]> {
    const mem = await this.client!.memory();
    return mem.map((m: any) => ({
      type: 'memory' as const,
      label: m.tag,
      description: formatBytes(m.memory_usage_bytes),
      tooltip: `Memory: ${formatBytes(m.memory_usage_bytes)}, Temp storage: ${formatBytes(m.temporary_storage_bytes)}`,
      collapsible: false,
      iconId: 'pulse',
    }));
  }

  private async getSpoolNodes(): Promise<TreeNode[]> {
    const files = await this.client!.spoolFiles();
    return files.map((f: any) => ({
      type: 'spool_file' as const,
      label: f.query_id.slice(0, 8) + '…',
      description: formatBytes(f.size_bytes),
      tooltip: `Query: ${f.query_id}\nSize: ${formatBytes(f.size_bytes)}\nCreated: ${f.created_at}`,
      collapsible: false,
      iconId: 'file-binary',
    }));
  }
}

function formatBytes(bytes: number): string {
  if (bytes == null || bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB'];
  let i = 0;
  let val = bytes;
  while (val >= 1024 && i < units.length - 1) {
    val /= 1024;
    i++;
  }
  return `${val.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}
