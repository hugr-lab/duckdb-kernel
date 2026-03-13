/**
 * HTTP client for the kernel's /introspect endpoint.
 */

import * as http from 'http';

export interface IntrospectResponse {
  type: string;
  data: any;
}

export class IntrospectClient {
  constructor(private baseUrl: string) {}

  async fetch(type: string, params?: Record<string, string>): Promise<IntrospectResponse> {
    const query = new URLSearchParams({ type, ...params });
    const url = `${this.baseUrl}/introspect?${query}`;

    return new Promise((resolve, reject) => {
      http.get(url, (res) => {
        let body = '';
        res.on('data', (chunk: string) => { body += chunk; });
        res.on('end', () => {
          if (res.statusCode && res.statusCode >= 400) {
            try {
              const err = JSON.parse(body);
              reject(new Error(err.error || `HTTP ${res.statusCode}`));
            } catch {
              reject(new Error(`HTTP ${res.statusCode}: ${body}`));
            }
            return;
          }
          try {
            resolve(JSON.parse(body));
          } catch (e) {
            reject(new Error(`Invalid JSON response: ${body.slice(0, 200)}`));
          }
        });
        res.on('error', reject);
      }).on('error', reject);
    });
  }

  async databases(): Promise<any[]> {
    const resp = await this.fetch('databases');
    return resp.data;
  }

  async schemas(database?: string): Promise<any[]> {
    const resp = await this.fetch('schemas', database ? { database } : undefined);
    return resp.data;
  }

  async tables(database?: string, schema?: string): Promise<any[]> {
    const params: Record<string, string> = {};
    if (database) params.database = database;
    if (schema) params.schema = schema;
    const resp = await this.fetch('tables', params);
    return resp.data;
  }

  async views(database?: string, schema?: string): Promise<any[]> {
    const params: Record<string, string> = {};
    if (database) params.database = database;
    if (schema) params.schema = schema;
    const resp = await this.fetch('views', params);
    return resp.data;
  }

  async columns(database: string, schema: string, table: string): Promise<any[]> {
    const resp = await this.fetch('columns', { database, schema, table });
    return resp.data;
  }

  async functions(schema?: string): Promise<any[]> {
    const resp = await this.fetch('functions', schema ? { schema } : undefined);
    return resp.data;
  }

  async indexes(database?: string, schema?: string, table?: string): Promise<any[]> {
    const params: Record<string, string> = {};
    if (database) params.database = database;
    if (schema) params.schema = schema;
    if (table) params.table = table;
    const resp = await this.fetch('indexes', params);
    return resp.data;
  }

  async sessionInfo(): Promise<any> {
    const resp = await this.fetch('session_info');
    return resp.data;
  }

  async extensions(): Promise<any[]> {
    const resp = await this.fetch('extensions');
    return resp.data;
  }

  async secrets(): Promise<any[]> {
    const resp = await this.fetch('secrets');
    return resp.data;
  }

  async settings(): Promise<any[]> {
    const resp = await this.fetch('settings');
    return resp.data;
  }

  async memory(): Promise<any[]> {
    const resp = await this.fetch('memory');
    return resp.data;
  }

  async spoolFiles(): Promise<any[]> {
    const resp = await this.fetch('spool_files');
    return resp.data;
  }

  async describe(database: string, schema: string, table: string): Promise<any[]> {
    const resp = await this.fetch('describe', { database, schema, table });
    return resp.data;
  }

  async summarize(database: string, schema: string, table: string): Promise<any[]> {
    const resp = await this.fetch('summarize', { database, schema, table });
    return resp.data;
  }

  async tableInfo(database: string, schema: string, table: string): Promise<any[]> {
    const resp = await this.fetch('table_info', { database, schema, table });
    return resp.data;
  }

  async viewInfo(database: string, schema: string, table: string): Promise<any[]> {
    const resp = await this.fetch('view_info', { database, schema, table });
    return resp.data;
  }

  async databaseInfo(database: string): Promise<any[]> {
    const resp = await this.fetch('database_info', { database });
    return resp.data;
  }

  async systemFunctions(): Promise<any[]> {
    const resp = await this.fetch('system_functions');
    return resp.data;
  }

  async deleteSpoolFile(queryId: string): Promise<void> {
    const url = `${this.baseUrl}/spool/delete?query_id=${encodeURIComponent(queryId)}`;
    return new Promise((resolve, reject) => {
      const req = http.request(url, { method: 'DELETE' }, (res) => {
        let body = '';
        res.on('data', (chunk: string) => { body += chunk; });
        res.on('end', () => {
          if (res.statusCode && res.statusCode >= 400) {
            try {
              reject(new Error(JSON.parse(body).error));
            } catch {
              reject(new Error(`HTTP ${res.statusCode}`));
            }
            return;
          }
          resolve();
        });
        res.on('error', reject);
      });
      req.on('error', reject);
      req.end();
    });
  }
}
