const CopyPlugin = require('copy-webpack-plugin');
const TerserPlugin = require('terser-webpack-plugin');
const path = require('path');

const PSP = path.resolve(__dirname, 'node_modules', '@perspective-dev');

// Patch relative WASM paths in CDN JS files so all assets
// resolve from the same flat directory.
const PATCHES = {
  'perspective.js': [
    ['"../../../server/dist/wasm/perspective-server.wasm"', '"./perspective-server.wasm"'],
  ],
  'perspective-viewer.js': [
    ['"../wasm/perspective-viewer.wasm"', '"./perspective-viewer.wasm"'],
  ],
};

function patchContent(content, name) {
  const patches = PATCHES[name];
  if (!patches) return content;
  let text = content.toString();
  for (const [search, replace] of patches) {
    text = text.replace(search, replace);
  }
  return Buffer.from(text);
}

function pspAsset(from, name) {
  return {
    from: path.join(PSP, from),
    to: 'perspective/[name][ext]',
    transform: name && PATCHES[name]
      ? { transformer: (content) => patchContent(content, name) }
      : undefined,
  };
}

module.exports = {
  optimization: {
    minimizer: [
      new TerserPlugin({
        // Perspective CDN files are pre-built and contain syntax Terser cannot parse
        exclude: /perspective\//,
      }),
    ],
  },
  plugins: [
    new CopyPlugin({
      patterns: [
        // CDN JS modules (self-contained, no external chunk deps)
        pspAsset('client/dist/cdn/perspective.js', 'perspective.js'),
        pspAsset('client/dist/cdn/perspective-server.worker.js'),
        pspAsset('viewer/dist/cdn/perspective-viewer.js', 'perspective-viewer.js'),
        pspAsset('viewer-datagrid/dist/cdn/perspective-viewer-datagrid.js'),
        pspAsset('viewer-d3fc/dist/cdn/perspective-viewer-d3fc.js'),
        // WASM binaries
        pspAsset('client/dist/wasm/perspective-js.wasm'),
        pspAsset('server/dist/wasm/perspective-server.wasm'),
        pspAsset('viewer/dist/wasm/perspective-viewer.wasm'),
        // CSS themes
        pspAsset('viewer/dist/css/themes.css'),
      ],
    }),
  ],
  externals: {
    '@perspective-dev/client': 'perspective',
    '@perspective-dev/viewer': 'perspective',
    '@perspective-dev/server': 'perspective',
    '@perspective-dev/viewer-datagrid': 'perspective',
    '@perspective-dev/viewer-d3fc': 'perspective',
  },
};
