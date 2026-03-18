import * as esbuild from 'esbuild';
import { cpSync, mkdirSync, existsSync } from 'fs';
import { resolve, dirname } from 'path';

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

// Copy perspective assets to out/perspective/ so renderer can load them locally
// instead of from kernel HTTP static files.
const perspOutDir = 'out/perspective';
mkdirSync(perspOutDir, { recursive: true });

const copyIfExists = (src, dest) => {
  if (existsSync(src)) {
    cpSync(src, dest, { recursive: true });
    console.log(`  Copied ${src} → ${dest}`);
  }
};

console.log('Copying perspective assets...');
// Viewer (JS + WASM + CSS)
copyIfExists('node_modules/@perspective-dev/viewer/dist/cdn/perspective-viewer.js', `${perspOutDir}/perspective-viewer.js`);
copyIfExists('node_modules/@perspective-dev/viewer/dist/wasm/perspective-viewer.wasm', `${perspOutDir}/perspective-viewer.wasm`);
copyIfExists('node_modules/@perspective-dev/viewer/dist/css/themes.css', `${perspOutDir}/themes.css`);
// Client (JS + WASM)
copyIfExists('node_modules/@perspective-dev/client/dist/cdn/perspective.js', `${perspOutDir}/perspective.js`);
copyIfExists('node_modules/@perspective-dev/client/dist/wasm/perspective-js.wasm', `${perspOutDir}/perspective-js.wasm`);
// Server (WASM + worker)
copyIfExists('node_modules/@perspective-dev/server/dist/wasm/perspective-server.wasm', `${perspOutDir}/perspective-server.wasm`);
copyIfExists('node_modules/@perspective-dev/server/dist/cdn/perspective-server.worker.js', `${perspOutDir}/perspective-server.worker.js`);
// Plugins
copyIfExists('node_modules/@perspective-dev/viewer-datagrid/dist/cdn/perspective-viewer-datagrid.js', `${perspOutDir}/perspective-viewer-datagrid.js`);
copyIfExists('node_modules/@perspective-dev/viewer-d3fc/dist/cdn/perspective-viewer-d3fc.js', `${perspOutDir}/perspective-viewer-d3fc.js`);
console.log('Done.');
