/**
 * Arrow IPC streaming with length-prefix protocol.
 *
 * Each chunk is [4-byte LE length][Arrow IPC bytes].
 * Stream ends with a zero-length marker.
 */

import { concatBuffers } from './utils.js';

/**
 * Rebuild Arrow URL using the latest known kernel base URL.
 * Extracts queryID from old URL and builds new URL with current port.
 * Skips if URL is already a spool proxy URL.
 */
export function rebuildArrowUrl(oldUrl: string, getKernelBaseUrl: () => string | null): string {
  // Don't rewrite spool proxy URLs — they go through Jupyter server, not kernel
  if (oldUrl.includes('/hugr/spool/')) return oldUrl;
  const base = getKernelBaseUrl();
  if (!base) return oldUrl;
  try {
    const old = new URL(oldUrl);
    const q = old.searchParams.get('q');
    if (!q) return oldUrl;
    const rebuilt = new URL(`${base}${old.pathname}`);
    old.searchParams.forEach((v, k) => rebuilt.searchParams.set(k, v));
    return rebuilt.toString();
  } catch {
    return oldUrl;
  }
}

/**
 * Stream length-prefixed Arrow IPC chunks into a Perspective table.
 *
 * @param arrowUrl - URL to fetch Arrow IPC stream from
 * @param perspectiveWorker - Perspective worker instance
 * @param fetchInit - RequestInit (auth headers, signal, etc.)
 * @param signal - Optional AbortSignal for cancellation
 * @param retryRebuildUrl - Optional function to rebuild URL on connection error (retry logic)
 */
export async function streamArrowToTable(
  arrowUrl: string,
  perspectiveWorker: any,
  fetchInit: RequestInit,
  signal?: AbortSignal,
  retryRebuildUrl?: (url: string) => string,
): Promise<any> {
  // Try fetch, retry with rebuilt URL on connection error (port may have changed after reload)
  let response: Response | undefined;
  try {
    response = await fetch(arrowUrl, { ...fetchInit, signal });
  } catch {
    // Connection error — wait for kernel base URL discovery, then retry
    if (retryRebuildUrl) {
      for (let attempt = 0; attempt < 3; attempt++) {
        await new Promise(r => setTimeout(r, 1000 * (attempt + 1)));
        const rebuilt = retryRebuildUrl(arrowUrl);
        if (rebuilt !== arrowUrl) {
          try {
            response = await fetch(rebuilt, { ...fetchInit, signal });
            break;
          } catch { /* retry */ }
        }
      }
    }
    if (!response) {
      throw new Error(`Result unavailable. Re-run the cell to refresh.`);
    }
  }
  if (!response.ok) {
    throw new Error(`Failed to fetch Arrow data (HTTP ${response.status})`);
  }

  if (!response.body) {
    throw new Error('Response body is null');
  }

  const reader = response.body.getReader();
  let buffer = new Uint8Array(0);
  let table: any = null;

  try {
    while (true) {
      if (signal?.aborted) break;

      while (buffer.length < 4) {
        const { done, value } = await reader.read();
        if (done) return table;
        buffer = concatBuffers(buffer, value);
      }

      const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
      const chunkLen = view.getUint32(0, true);

      if (chunkLen === 0) break;

      while (buffer.length < 4 + chunkLen) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer = concatBuffers(buffer, value);
      }

      if (buffer.length < 4 + chunkLen) {
        break;
      }

      const chunk = buffer.slice(4, 4 + chunkLen);
      buffer = buffer.slice(4 + chunkLen);

      if (table === null) {
        table = await perspectiveWorker.table(chunk.buffer);
      } else {
        await table.update(chunk.buffer);
      }
    }
  } finally {
    reader.releaseLock();
  }

  return table;
}
