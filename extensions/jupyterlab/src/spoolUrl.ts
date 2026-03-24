/**
 * Spool URL routing — determines whether to use spool proxy (JupyterLab/Hub)
 * or direct kernel Arrow HTTP server (VS Code).
 */

let _serverSettings: { baseUrl: string; token: string } | null = null;

/**
 * Initialize with ServerConnection settings from JupyterLab.
 * Called once from plugin.ts at activation.
 * If not called (VS Code), falls back to direct kernel URLs.
 */
export function initSpoolProxy(settings: { baseUrl: string; token: string }): void {
  _serverSettings = settings;
}

/**
 * Check if spool proxy is available (running inside JupyterLab/Hub).
 */
export function hasSpoolProxy(): boolean {
  return _serverSettings !== null && _serverSettings.baseUrl !== '';
}

/**
 * Build Arrow stream URL.
 *
 * JupyterLab: {baseUrl}hugr/spool/arrow/stream?q={queryId}&kernel_id={kernelId}
 * VS Code:    http://127.0.0.1:{port}/arrow/stream?q={queryId}
 */
export function buildArrowStreamUrl(
  queryId: string,
  opts?: {
    kernelBaseUrl?: string;
    kernelId?: string;
    geoarrow?: boolean;
    columns?: string[];
    limit?: number;
    total?: number;
  },
): string {
  const params = new URLSearchParams();
  params.set('q', queryId);

  if (opts?.geoarrow) params.set('geoarrow', '1');
  if (opts?.columns?.length) params.set('columns', opts.columns.join(','));
  if (opts?.limit) params.set('limit', String(opts.limit));
  if (opts?.total) params.set('total', String(opts.total));

  if (_serverSettings) {
    // JupyterLab/Hub — use spool proxy
    if (opts?.kernelId) params.set('kernel_id', opts.kernelId);
    return `${_serverSettings.baseUrl}hugr/spool/arrow/stream?${params}`;
  }

  // VS Code — direct to kernel
  if (opts?.kernelBaseUrl) {
    return `${opts.kernelBaseUrl}/arrow/stream?${params}`;
  }

  // Fallback
  return `/arrow/stream?${params}`;
}

/**
 * Build spool management URL (pin, unpin, is_pinned, delete).
 */
export function buildSpoolUrl(
  action: 'pin' | 'unpin' | 'is_pinned' | 'delete',
  queryId: string,
  opts?: { kernelBaseUrl?: string; kernelId?: string; dir?: string },
): string {
  const params = new URLSearchParams();
  params.set('q', queryId);

  if (opts?.kernelId) params.set('kernel_id', opts.kernelId);
  if (opts?.dir) params.set('dir', opts.dir);

  if (_serverSettings) {
    return `${_serverSettings.baseUrl}hugr/spool/${action}?${params}`;
  }

  if (opts?.kernelBaseUrl) {
    return `${opts.kernelBaseUrl}/spool/${action}?query_id=${queryId}`;
  }

  return `/spool/${action}?query_id=${queryId}`;
}

/**
 * Get fetch init with auth headers for spool requests.
 * JupyterLab: adds Authorization token header.
 * VS Code: plain fetch.
 */
export function getSpoolFetchInit(signal?: AbortSignal): RequestInit {
  const init: RequestInit = {};
  if (signal) init.signal = signal;

  if (_serverSettings?.token) {
    init.headers = {
      Authorization: `token ${_serverSettings.token}`,
    };
  }

  return init;
}
