"use strict";
(self["webpackChunk_hugr_lab_perspective_viewer"] = self["webpackChunk_hugr_lab_perspective_viewer"] || []).push([["lib_index_js"],{

/***/ "./lib/index.js"
/*!**********************!*\
  !*** ./lib/index.js ***!
  \**********************/
(__unused_webpack_module, exports, __webpack_require__) {


/**
 * JupyterLab MIME renderer extension for HUGR result metadata.
 *
 * Intercepts `application/vnd.hugr.result+json` output from the DuckDB
 * kernel and renders an interactive Perspective viewer.
 */
Object.defineProperty(exports, "__esModule", ({ value: true }));
const widget_js_1 = __webpack_require__(/*! ./widget.js */ "./lib/widget.js");
const MIME_TYPE = 'application/vnd.hugr.result+json';
const rendererFactory = {
    safe: true,
    mimeTypes: [MIME_TYPE],
    createRenderer: (options) => new widget_js_1.HugrResultWidget(options),
};
const extension = {
    id: '@hugr-lab/perspective-viewer:plugin',
    rendererFactory,
    rank: 0,
    dataType: 'json',
};
exports["default"] = extension;


/***/ },

/***/ "./lib/widget.js"
/*!***********************!*\
  !*** ./lib/widget.js ***!
  \***********************/
(__unused_webpack_module, exports, __webpack_require__) {


/**
 * Perspective viewer widget for HUGR result metadata.
 *
 * Fetches Arrow IPC data from Go kernel's HTTP server and loads
 * it into Perspective in a single call to avoid WASM overhead
 * from many small table.update() calls.
 */
Object.defineProperty(exports, "__esModule", ({ value: true }));
exports.HugrResultWidget = void 0;
const widgets_1 = __webpack_require__(/*! @lumino/widgets */ "webpack/sharing/consume/default/@lumino/widgets");
const MIME_TYPE = 'application/vnd.hugr.result+json';
const STATIC_BASE = '/lab/extensions/@hugr-lab/perspective-viewer/static/perspective';
let _perspectiveReady = null;
function loadPerspective() {
    if (_perspectiveReady) {
        return _perspectiveReady;
    }
    _perspectiveReady = (async () => {
        const [perspective] = await Promise.all([
            import(/* webpackIgnore: true */ `${STATIC_BASE}/perspective.js`),
            import(/* webpackIgnore: true */ `${STATIC_BASE}/perspective-viewer.js`),
            import(/* webpackIgnore: true */ `${STATIC_BASE}/perspective-viewer-datagrid.js`),
            import(/* webpackIgnore: true */ `${STATIC_BASE}/perspective-viewer-d3fc.js`),
        ]);
        const themeHref = `${STATIC_BASE}/themes.css`;
        if (!document.querySelector(`link[href="${themeHref}"]`)) {
            const link = document.createElement('link');
            link.rel = 'stylesheet';
            link.href = themeHref;
            document.head.appendChild(link);
        }
        await customElements.whenDefined('perspective-viewer');
        return perspective;
    })();
    return _perspectiveReady;
}
function formatNumber(n) {
    return n.toLocaleString('en-US');
}
function parseTruncation(arrowUrl) {
    try {
        const url = new URL(arrowUrl);
        const limit = parseInt(url.searchParams.get('limit') ?? '0', 10);
        const total = parseInt(url.searchParams.get('total') ?? '0', 10);
        if (limit > 0 && total > limit) {
            return { truncated: true, limit, total };
        }
    }
    catch {
        // Ignore.
    }
    return { truncated: false, limit: 0, total: 0 };
}
function buildFullUrl(arrowUrl) {
    try {
        const url = new URL(arrowUrl);
        url.searchParams.delete('limit');
        url.searchParams.delete('total');
        return url.toString();
    }
    catch {
        return arrowUrl;
    }
}
/**
 * Stream length-prefixed Arrow IPC chunks from the Go server into a
 * Perspective table. Each chunk is [4-byte LE length][Arrow IPC bytes].
 * Stream ends with a zero-length marker.
 *
 * Creates the table from the first chunk, then updates with subsequent ones.
 * Supports AbortController to cancel mid-stream on dispose/reload.
 */
async function streamArrowToTable(arrowUrl, perspectiveWorker, signal) {
    const response = await fetch(arrowUrl, { signal });
    if (!response.ok) {
        throw new Error(`Failed to fetch Arrow data (HTTP ${response.status})`);
    }
    const reader = response.body.getReader();
    let buffer = new Uint8Array(0);
    let table = null;
    try {
        while (true) {
            if (signal?.aborted)
                break;
            // Ensure we have at least 4 bytes for the length prefix.
            while (buffer.length < 4) {
                const { done, value } = await reader.read();
                if (done)
                    return table;
                buffer = concatBuffers(buffer, value);
            }
            // Read chunk length (little-endian uint32).
            const view = new DataView(buffer.buffer, buffer.byteOffset, 4);
            const chunkLen = view.getUint32(0, true);
            // Zero length = end of stream.
            if (chunkLen === 0)
                break;
            // Read the full chunk.
            while (buffer.length < 4 + chunkLen) {
                const { done, value } = await reader.read();
                if (done)
                    break;
                buffer = concatBuffers(buffer, value);
            }
            // Extract the Arrow IPC chunk.
            const chunk = buffer.slice(4, 4 + chunkLen);
            buffer = buffer.slice(4 + chunkLen);
            // Feed to perspective.
            if (table === null) {
                table = await perspectiveWorker.table(chunk.buffer);
            }
            else {
                await table.update(chunk.buffer);
            }
        }
    }
    finally {
        reader.releaseLock();
    }
    return table;
}
function concatBuffers(a, b) {
    const result = new Uint8Array(a.length + b.length);
    result.set(a);
    result.set(b, a.length);
    return result;
}
class HugrResultWidget extends widgets_1.Widget {
    constructor(options) {
        super();
        this._viewer = null;
        this._table = null;
        this._client = null;
        this._abortController = null;
        this._mimeType = options.mimeType;
        this.addClass('hugr-result-viewer');
    }
    async renderModel(model) {
        const metadata = model.data[this._mimeType];
        if (!metadata || !metadata.arrow_url) {
            this._showError('No result metadata available.');
            return;
        }
        await this._loadViewer(metadata.arrow_url, metadata);
    }
    async _loadViewer(arrowUrl, metadata) {
        const truncation = parseTruncation(arrowUrl);
        this.node.innerHTML = '<div class="hugr-result-loading">Loading viewer...</div>';
        try {
            const perspective = await loadPerspective();
            await this._cleanup();
            this._abortController = new AbortController();
            this._client = await perspective.worker();
            this._table = await streamArrowToTable(arrowUrl, this._client, this._abortController.signal);
            // Build UI.
            this.node.innerHTML = '';
            // Status bar: rows info + kernel memory.
            const statusParts = [];
            if (truncation.truncated) {
                statusParts.push(`Showing ${formatNumber(truncation.limit)} of ${formatNumber(truncation.total)} rows`);
            }
            else if (metadata?.rows) {
                statusParts.push(`${formatNumber(metadata.rows)} rows`);
            }
            if (metadata?.kernel_mem_mb) {
                statusParts.push(`Kernel: ${formatNumber(metadata.kernel_mem_mb)} MB`);
            }
            if (statusParts.length > 0 || truncation.truncated) {
                const banner = document.createElement('div');
                banner.className = 'hugr-result-banner';
                banner.innerHTML = `<span>${statusParts.join(' · ')}</span>`;
                if (truncation.truncated) {
                    const btn = document.createElement('button');
                    btn.className = 'hugr-result-load-all';
                    btn.textContent = `Load all ${formatNumber(truncation.total)} rows`;
                    btn.addEventListener('click', () => {
                        btn.disabled = true;
                        btn.textContent = 'Loading...';
                        this._loadViewer(buildFullUrl(arrowUrl), metadata);
                    });
                    banner.appendChild(btn);
                }
                this.node.appendChild(banner);
            }
            const viewer = document.createElement('perspective-viewer');
            viewer.setAttribute('plugin', 'Datagrid');
            this.node.appendChild(viewer);
            this._viewer = viewer;
            await viewer.load(this._table);
        }
        catch (err) {
            const message = err?.message || 'Unknown error';
            this._showError(`Failed to initialize viewer: ${message}`);
        }
    }
    async _cleanup() {
        if (this._abortController) {
            this._abortController.abort();
            this._abortController = null;
        }
        if (this._viewer) {
            try {
                await this._viewer.delete();
            }
            catch {
                // Ignore cleanup errors.
            }
            this._viewer = null;
        }
        if (this._table) {
            try {
                await this._table.delete();
            }
            catch {
                // Ignore cleanup errors.
            }
            this._table = null;
        }
        this._client = null;
    }
    async dispose() {
        await this._cleanup();
        super.dispose();
    }
    _showError(message) {
        this.node.innerHTML = `<div class="hugr-result-error">${message}</div>`;
    }
}
exports.HugrResultWidget = HugrResultWidget;


/***/ }

}]);
//# sourceMappingURL=lib_index_js.85d6c0662cb87da2f4b6.js.map