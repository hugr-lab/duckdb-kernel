/**
 * HTTP client for the kernel's /introspect endpoint.
 * Browser-side (JupyterLab runs in browser, so uses fetch API).
 */

export interface IntrospectResponse {
  type: string;
  data: any;
}

export class IntrospectClient {
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
