import * as esbuild from 'esbuild';

// Extension host (Node.js, CommonJS)
await esbuild.build({
  entryPoints: ['src/extension.ts'],
  bundle: true,
  outfile: 'out/extension.js',
  platform: 'node',
  format: 'cjs',
  external: ['vscode'],
  sourcemap: true,
});

// Renderer (webview, ESM)
await esbuild.build({
  entryPoints: ['src/renderer.ts'],
  bundle: true,
  outfile: 'out/renderer.js',
  platform: 'browser',
  format: 'esm',
  sourcemap: true,
});

// Map plugin bundle for perspective panel webview (IIFE, self-registering)
await esbuild.build({
  entryPoints: ['src/map-plugin-standalone.ts'],
  bundle: true,
  outfile: 'out/map-plugin.js',
  platform: 'browser',
  format: 'iife',
  sourcemap: true,
});

// Perspective panel webview bundle (IIFE, uses perspective-core)
await esbuild.build({
  entryPoints: ['src/perspective-panel-webview.ts'],
  bundle: true,
  outfile: 'out/perspective-panel.js',
  platform: 'browser',
  format: 'iife',
  sourcemap: true,
});
