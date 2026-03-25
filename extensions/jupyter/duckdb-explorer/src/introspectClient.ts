/**
 * Introspect client interface and implementations.
 *
 * - IIntrospectClient: shared interface used by the sidebar
 * - IntrospectClient: HTTP transport (VS Code, standalone)
 * - CommIntrospectClient: Jupyter Comm transport (JupyterLab)
 */

import type { Kernel, KernelMessage } from '@jupyterlab/services';

export interface IntrospectResponse {
  type: string;
  data: any;
}

/**
 * Common interface for introspection clients.
 * Both HTTP and Comm implementations conform to this.
 */
export interface IIntrospectClient {
  databases(): Promise<any[]>;
  schemas(database?: string): Promise<any[]>;
  tables(database?: string, schema?: string): Promise<any[]>;
  views(database?: string, schema?: string): Promise<any[]>;
  columns(database: string, schema: string, table: string): Promise<any[]>;
  functions(schema?: string): Promise<any[]>;
  systemFunctions(): Promise<any[]>;
  indexes(database?: string, schema?: string, table?: string): Promise<any[]>;
  sessionInfo(): Promise<any>;
  extensions(): Promise<any[]>;
  secrets(): Promise<any[]>;
  settings(): Promise<any[]>;
  memory(): Promise<any[]>;
  spoolFiles(): Promise<any[]>;
  describe(database: string, schema: string, table: string): Promise<any[]>;
  summarize(database: string, schema: string, table: string): Promise<any[]>;
  tableInfo(database: string, schema: string, table: string): Promise<any[]>;
  viewInfo(database: string, schema: string, table: string): Promise<any[]>;
  databaseInfo(database: string): Promise<any[]>;
  deleteSpoolFile(queryId: string): Promise<void>;
  dispose?(): void;
}

/**
 * HTTP client for the kernel's /introspect endpoint.
 * Used by VS Code and as fallback.
 */
export class IntrospectClient implements IIntrospectClient {
  constructor(private baseUrl: string) {}

  async fetch(type: string, params?: Record<string, string>): Promise<IntrospectResponse> {
    const query = new URLSearchParams({ type, ...params });
    const url = `${this.baseUrl}/introspect?${query}`;

    const resp = await fetch(url);
    const body = await resp.json();

    if (!resp.ok) {
      throw new Error(body.error || `HTTP ${resp.status}`);
    }
    return body;
  }

  async databases(): Promise<any[]> {
    return (await this.fetch('databases')).data;
  }

  async schemas(database?: string): Promise<any[]> {
    return (await this.fetch('schemas', database ? { database } : undefined)).data;
  }

  async tables(database?: string, schema?: string): Promise<any[]> {
    const params: Record<string, string> = {};
    if (database) params.database = database;
    if (schema) params.schema = schema;
    return (await this.fetch('tables', params)).data;
  }

  async views(database?: string, schema?: string): Promise<any[]> {
    const params: Record<string, string> = {};
    if (database) params.database = database;
    if (schema) params.schema = schema;
    return (await this.fetch('views', params)).data;
  }

  async columns(database: string, schema: string, table: string): Promise<any[]> {
    return (await this.fetch('columns', { database, schema, table })).data;
  }

  async functions(schema?: string): Promise<any[]> {
    return (await this.fetch('functions', schema ? { schema } : undefined)).data;
  }

  async systemFunctions(): Promise<any[]> {
    return (await this.fetch('system_functions')).data;
  }

  async indexes(database?: string, schema?: string, table?: string): Promise<any[]> {
    const params: Record<string, string> = {};
    if (database) params.database = database;
    if (schema) params.schema = schema;
    if (table) params.table = table;
    return (await this.fetch('indexes', params)).data;
  }

  async sessionInfo(): Promise<any> {
    return (await this.fetch('session_info')).data;
  }

  async extensions(): Promise<any[]> {
    return (await this.fetch('extensions')).data;
  }

  async secrets(): Promise<any[]> {
    return (await this.fetch('secrets')).data;
  }

  async settings(): Promise<any[]> {
    return (await this.fetch('settings')).data;
  }

  async memory(): Promise<any[]> {
    return (await this.fetch('memory')).data;
  }

  async spoolFiles(): Promise<any[]> {
    return (await this.fetch('spool_files')).data;
  }

  async describe(database: string, schema: string, table: string): Promise<any[]> {
    return (await this.fetch('describe', { database, schema, table })).data;
  }

  async summarize(database: string, schema: string, table: string): Promise<any[]> {
    return (await this.fetch('summarize', { database, schema, table })).data;
  }

  async tableInfo(database: string, schema: string, table: string): Promise<any[]> {
    return (await this.fetch('table_info', { database, schema, table })).data;
  }

  async viewInfo(database: string, schema: string, table: string): Promise<any[]> {
    return (await this.fetch('view_info', { database, schema, table })).data;
  }

  async databaseInfo(database: string): Promise<any[]> {
    return (await this.fetch('database_info', { database })).data;
  }

  async deleteSpoolFile(queryId: string): Promise<void> {
    const url = `${this.baseUrl}/spool/delete?query_id=${encodeURIComponent(queryId)}`;
    const resp = await fetch(url, { method: 'DELETE' });
    if (!resp.ok) {
      const body = await resp.json();
      throw new Error(body.error || `HTTP ${resp.status}`);
    }
  }
}

/**
 * Jupyter Comm-based introspect client.
 * Uses the Jupyter Comm protocol to communicate with the kernel,
 * bypassing HTTP port accessibility issues in JupyterHub/Docker.
 */
export class CommIntrospectClient implements IIntrospectClient {
  private comm: Kernel.IComm;
  private pending = new Map<string, { resolve: (v: any) => void; reject: (e: Error) => void }>();
  private disposed = false;

  constructor(kernel: Kernel.IKernelConnection) {
    this.comm = kernel.createComm('duckdb.introspect');
    this.comm.onMsg = (msg: KernelMessage.ICommMsgMsg) => this._handleResponse(msg);
    this.comm.onClose = () => this._handleClose();
    this.comm.open();
  }

  private _request(type: string, params?: Record<string, string>): Promise<any> {
    if (this.disposed) {
      return Promise.reject(new Error('comm disposed'));
    }
    const requestId = crypto.randomUUID();
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pending.delete(requestId);
        reject(new Error(`introspect request timed out: ${type}`));
      }, 30000);
      this.pending.set(requestId, {
        resolve: (v: any) => { clearTimeout(timeout); resolve(v); },
        reject: (e: Error) => { clearTimeout(timeout); reject(e); },
      });
      this.comm.send({ request_id: requestId, type, ...params });
    });
  }

  private _handleResponse(msg: KernelMessage.ICommMsgMsg): void {
    const data = msg.content.data as any;
    if (!data || !data.request_id) return;
    const pending = this.pending.get(data.request_id);
    if (!pending) return;
    this.pending.delete(data.request_id);
    if (data.error) {
      pending.reject(new Error(data.error));
    } else {
      pending.resolve(data.data);
    }
  }

  private _handleClose(): void {
    this.disposed = true;
    for (const { reject } of this.pending.values()) {
      reject(new Error('comm closed'));
    }
    this.pending.clear();
  }

  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    try {
      this.comm.close();
    } catch {
      // Comm may already be closed.
    }
    for (const { reject } of this.pending.values()) {
      reject(new Error('comm disposed'));
    }
    this.pending.clear();
  }

  async databases(): Promise<any[]> {
    return this._request('databases');
  }

  async schemas(database?: string): Promise<any[]> {
    return this._request('schemas', database ? { database } : undefined);
  }

  async tables(database?: string, schema?: string): Promise<any[]> {
    const params: Record<string, string> = {};
    if (database) params.database = database;
    if (schema) params.schema = schema;
    return this._request('tables', params);
  }

  async views(database?: string, schema?: string): Promise<any[]> {
    const params: Record<string, string> = {};
    if (database) params.database = database;
    if (schema) params.schema = schema;
    return this._request('views', params);
  }

  async columns(database: string, schema: string, table: string): Promise<any[]> {
    return this._request('columns', { database, schema, table });
  }

  async functions(schema?: string): Promise<any[]> {
    return this._request('functions', schema ? { schema } : undefined);
  }

  async systemFunctions(): Promise<any[]> {
    return this._request('system_functions');
  }

  async indexes(database?: string, schema?: string, table?: string): Promise<any[]> {
    const params: Record<string, string> = {};
    if (database) params.database = database;
    if (schema) params.schema = schema;
    if (table) params.table = table;
    return this._request('indexes', params);
  }

  async sessionInfo(): Promise<any> {
    return this._request('session_info');
  }

  async extensions(): Promise<any[]> {
    return this._request('extensions');
  }

  async secrets(): Promise<any[]> {
    return this._request('secrets');
  }

  async settings(): Promise<any[]> {
    return this._request('settings');
  }

  async memory(): Promise<any[]> {
    return this._request('memory');
  }

  async spoolFiles(): Promise<any[]> {
    return this._request('spool_files');
  }

  async describe(database: string, schema: string, table: string): Promise<any[]> {
    return this._request('describe', { database, schema, table });
  }

  async summarize(database: string, schema: string, table: string): Promise<any[]> {
    return this._request('summarize', { database, schema, table });
  }

  async tableInfo(database: string, schema: string, table: string): Promise<any[]> {
    return this._request('table_info', { database, schema, table });
  }

  async viewInfo(database: string, schema: string, table: string): Promise<any[]> {
    return this._request('view_info', { database, schema, table });
  }

  async databaseInfo(database: string): Promise<any[]> {
    return this._request('database_info', { database });
  }

  async deleteSpoolFile(_queryId: string): Promise<void> {
    // Spool file management is HTTP-only (tied to ArrowServer).
    // This is a no-op via comm; the sidebar should hide spool controls
    // when using comm transport, but we provide a safe fallback.
    throw new Error('deleteSpoolFile is not available via comm transport');
  }
}
