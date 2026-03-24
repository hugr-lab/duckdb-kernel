/**
 * Standalone map plugin entry point for perspective panel webview.
 * Exposes window.__mapPluginReady promise that resolves when plugin is registered.
 */
import { registerMapPlugin } from '@hugr-lab/perspective-core';

(window as any).__mapPluginReady = (async () => {
  try {
    await customElements.whenDefined('perspective-viewer');
    await registerMapPlugin();
  } catch (e) {
    console.warn('[HUGR] Map plugin registration failed:', e);
  }
})();
