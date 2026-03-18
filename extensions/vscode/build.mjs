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
