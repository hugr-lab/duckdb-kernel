/**
 * Perspective loader — dynamically imports Perspective JS/WASM from configurable base URL.
 */

let _perspectiveReady: Promise<any> | null = null;

/**
 * Load Perspective viewer and plugins from the given static base URL.
 * Loads: perspective.js, perspective-viewer.js, perspective-viewer-datagrid.js, perspective-viewer-d3fc.js
 * Also injects themes.css and waits for custom element registration.
 *
 * @param staticBase - Base URL for Perspective assets (e.g., "/static/perspective")
 * @param registerMapPlugin - Optional callback to register the map plugin after Perspective loads
 */
export function loadPerspective(staticBase: string, registerMapPlugin?: () => Promise<void>): Promise<any> {
  if (_perspectiveReady) {
    return _perspectiveReady;
  }
  _perspectiveReady = (async () => {
    const [perspective] = await Promise.all([
      import(/* webpackIgnore: true */ `${staticBase}/perspective.js`),
      import(/* webpackIgnore: true */ `${staticBase}/perspective-viewer.js`),
      import(/* webpackIgnore: true */ `${staticBase}/perspective-viewer-datagrid.js`),
      import(/* webpackIgnore: true */ `${staticBase}/perspective-viewer-d3fc.js`),
    ]);
    const themeHref = `${staticBase}/themes.css`;
    if (!document.querySelector(`link[href="${themeHref}"]`)) {
      const link = document.createElement('link');
      link.rel = 'stylesheet';
      link.href = themeHref;
      document.head.appendChild(link);
    }

    await customElements.whenDefined('perspective-viewer');
    if (registerMapPlugin) {
      await registerMapPlugin();
    }
    return perspective;
  })();
  _perspectiveReady.catch(() => {
    _perspectiveReady = null;
  });
  return _perspectiveReady;
}

/**
 * Reset the Perspective loader state (for testing or re-initialization).
 */
export function resetPerspectiveLoader(): void {
  _perspectiveReady = null;
}
