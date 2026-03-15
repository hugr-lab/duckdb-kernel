/**
 * Install or update the DuckDB kernel binary from GitHub Releases.
 *
 * Downloads the kernel binary and kernel.json, places them in the
 * platform-specific Jupyter kernels directory, and patches the path.
 */

import * as vscode from 'vscode';
import * as https from 'https';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

const REPO = 'hugr-lab/duckdb-kernel';
const KERNEL_NAME = 'duckdb';

interface GHRelease {
  tag_name: string;
  assets: { name: string; browser_download_url: string }[];
}

function kernelDir(): string {
  switch (process.platform) {
    case 'darwin':
      return path.join(os.homedir(), 'Library', 'Jupyter', 'kernels', KERNEL_NAME);
    case 'win32':
      return path.join(process.env.APPDATA ?? path.join(os.homedir(), 'AppData', 'Roaming'), 'jupyter', 'kernels', KERNEL_NAME);
    default:
      return path.join(os.homedir(), '.local', 'share', 'jupyter', 'kernels', KERNEL_NAME);
  }
}

function binaryName(): string {
  const osMap: Record<string, string> = { darwin: 'darwin', linux: 'linux', win32: 'windows' };
  const archMap: Record<string, string> = { x64: 'amd64', arm64: 'arm64' };
  const osName = osMap[process.platform] ?? process.platform;
  const arch = archMap[process.arch] ?? process.arch;
  const suffix = process.platform === 'win32' ? '.exe' : '';
  return `duckdb-kernel-${osName}-${arch}${suffix}`;
}

function httpsGetJson<T>(url: string): Promise<T> {
  return new Promise((resolve, reject) => {
    const get = (u: string) => {
      https.get(u, { headers: { 'User-Agent': 'vscode-hugr' } }, (res) => {
        if (res.statusCode && res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          get(res.headers.location);
          return;
        }
        if (res.statusCode !== 200) {
          reject(new Error(`HTTP ${res.statusCode}`));
          return;
        }
        let body = '';
        res.on('data', (chunk) => (body += chunk));
        res.on('end', () => {
          try { resolve(JSON.parse(body)); } catch (e) { reject(e); }
        });
        res.on('error', reject);
      }).on('error', reject);
    };
    get(url);
  });
}

function httpsDownload(url: string, dest: string, onProgress?: (pct: number) => void): Promise<void> {
  return new Promise((resolve, reject) => {
    const get = (u: string) => {
      https.get(u, { headers: { 'User-Agent': 'vscode-hugr' } }, (res) => {
        if (res.statusCode && res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          get(res.headers.location);
          return;
        }
        if (res.statusCode !== 200) {
          reject(new Error(`HTTP ${res.statusCode} downloading ${url}`));
          return;
        }
        const total = parseInt(res.headers['content-length'] ?? '0', 10);
        let downloaded = 0;
        const file = fs.createWriteStream(dest);
        res.on('data', (chunk: Buffer) => {
          downloaded += chunk.length;
          if (total > 0 && onProgress) onProgress(Math.round((downloaded / total) * 100));
        });
        res.pipe(file);
        file.on('finish', () => file.close(() => resolve()));
        file.on('error', reject);
        res.on('error', reject);
      }).on('error', reject);
    };
    get(url);
  });
}

export async function installKernel(log: vscode.OutputChannel): Promise<void> {
  await vscode.window.withProgress(
    { location: vscode.ProgressLocation.Notification, title: 'DuckDB Kernel', cancellable: false },
    async (progress) => {
      progress.report({ message: 'Fetching latest release...' });

      const release = await httpsGetJson<GHRelease>(
        `https://api.github.com/repos/${REPO}/releases/latest`,
      );
      const version = release.tag_name;
      log.appendLine(`Latest release: ${version}`);

      const bin = binaryName();
      const binAsset = release.assets.find((a) => a.name === bin);
      const jsonAsset = release.assets.find((a) => a.name === 'kernel.json');

      if (!binAsset) {
        throw new Error(`No binary found for this platform: ${bin}`);
      }
      if (!jsonAsset) {
        throw new Error('kernel.json not found in release assets');
      }

      const dir = kernelDir();
      fs.mkdirSync(dir, { recursive: true });

      const suffix = process.platform === 'win32' ? '.exe' : '';
      const binaryPath = path.join(dir, `duckdb-kernel${suffix}`);
      const jsonPath = path.join(dir, 'kernel.json');

      // Check if existing binary is a symlink (dev install via `make install`)
      try {
        const stat = fs.lstatSync(binaryPath);
        if (stat.isSymbolicLink()) {
          const target = fs.readlinkSync(binaryPath);
          const answer = await vscode.window.showWarningMessage(
            `A dev kernel is symlinked at ${binaryPath} → ${target}. Replace with release binary?`,
            'Replace', 'Cancel',
          );
          if (answer !== 'Replace') {
            log.appendLine('Install cancelled — dev symlink preserved');
            return;
          }
          fs.unlinkSync(binaryPath);
        }
      } catch {
        // File doesn't exist yet — fine
      }

      // Download binary
      progress.report({ message: `Downloading ${bin}...` });
      await httpsDownload(binAsset.browser_download_url, binaryPath, (pct) => {
        progress.report({ message: `Downloading ${bin}... ${pct}%` });
      });

      // Make executable on Unix
      if (process.platform !== 'win32') {
        fs.chmodSync(binaryPath, 0o755);
      }

      // Download kernel.json
      progress.report({ message: 'Downloading kernel.json...' });
      await httpsDownload(jsonAsset.browser_download_url, jsonPath);

      // Patch kernel.json with absolute path
      progress.report({ message: 'Configuring kernel...' });
      const spec = JSON.parse(fs.readFileSync(jsonPath, 'utf-8'));
      spec.argv[0] = binaryPath;
      fs.writeFileSync(jsonPath, JSON.stringify(spec, null, 2));

      // Download Perspective static files (optional)
      const staticAsset = release.assets.find((a) => a.name === 'perspective-static.tar.gz');
      if (staticAsset) {
        progress.report({ message: 'Downloading Perspective viewer...' });
        const tarPath = path.join(dir, 'perspective-static.tar.gz');
        try {
          await httpsDownload(staticAsset.browser_download_url, tarPath);
          // Extract into static/ subdirectory next to the binary.
          // The tar contains perspective/ at root, kernel looks for static/perspective/.
          const staticDir = path.join(dir, 'static');
          fs.mkdirSync(staticDir, { recursive: true });
          const { execSync } = await import('child_process');
          execSync(`tar -xzf "${tarPath}" -C "${staticDir}"`);
          fs.unlinkSync(tarPath);
          log.appendLine('Perspective static files installed');
        } catch (e: any) {
          log.appendLine(`Perspective static files skipped: ${e.message}`);
          // Non-fatal — viewer works without static files (just no Perspective)
        }
      }

      log.appendLine(`Kernel ${version} installed to ${dir}`);
      vscode.window.showInformationMessage(
        `DuckDB Kernel ${version} installed. Restart any open notebooks to use it.`,
      );
    },
  );
}
