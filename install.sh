#!/usr/bin/env bash
set -euo pipefail

REPO="hugr-lab/duckdb-kernel"
KERNEL_NAME="duckdb"

# Detect OS and arch
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

case "$ARCH" in
    x86_64)  ARCH="amd64" ;;
    aarch64) ARCH="arm64" ;;
    arm64)   ARCH="arm64" ;;
    *)       echo "Unsupported architecture: $ARCH" >&2; exit 1 ;;
esac

case "$OS" in
    linux|darwin) ;;
    mingw*|msys*|cygwin*) OS="windows" ;;
    *) echo "Unsupported OS: $OS" >&2; exit 1 ;;
esac

SUFFIX=""
if [ "$OS" = "windows" ]; then
    SUFFIX=".exe"
fi

BINARY="duckdb-kernel-${OS}-${ARCH}${SUFFIX}"

# Determine version
VERSION="${1:-latest}"
if [ "$VERSION" = "latest" ]; then
    echo "Fetching latest release..."
    VERSION=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | sed -E 's/.*"([^"]+)".*/\1/')
    if [ -z "$VERSION" ]; then
        echo "Failed to determine latest version" >&2
        exit 1
    fi
fi
echo "Installing duckdb-kernel ${VERSION} (${OS}/${ARCH})"

# Determine Jupyter kernel directory
if [ "$OS" = "darwin" ]; then
    KERNEL_DIR="${HOME}/Library/Jupyter/kernels/${KERNEL_NAME}"
elif [ "$OS" = "windows" ]; then
    KERNEL_DIR="${APPDATA}/jupyter/kernels/${KERNEL_NAME}"
else
    KERNEL_DIR="${HOME}/.local/share/jupyter/kernels/${KERNEL_NAME}"
fi

mkdir -p "$KERNEL_DIR"

# Download binary
DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${VERSION}/${BINARY}"
echo "Downloading ${DOWNLOAD_URL}..."
curl -fSL -o "${KERNEL_DIR}/duckdb-kernel${SUFFIX}" "$DOWNLOAD_URL"
chmod +x "${KERNEL_DIR}/duckdb-kernel${SUFFIX}"

# Download kernel.json
KERNEL_JSON_URL="https://github.com/${REPO}/releases/download/${VERSION}/kernel.json"
curl -fSL -o "${KERNEL_DIR}/kernel.json" "$KERNEL_JSON_URL"

# Patch kernel.json with absolute path
BINARY_PATH="${KERNEL_DIR}/duckdb-kernel${SUFFIX}"
if command -v python3 &>/dev/null; then
    KERNEL_JSON="${KERNEL_DIR}/kernel.json" KERNEL_BINARY="${BINARY_PATH}" python3 -c "
import json, os
path = os.environ['KERNEL_JSON']
with open(path) as f:
    spec = json.load(f)
spec['argv'][0] = os.environ['KERNEL_BINARY']
with open(path, 'w') as f:
    json.dump(spec, f, indent=2)
"
else
    sed -i'' -e "s|\"duckdb-kernel\"|\"${BINARY_PATH}\"|" "${KERNEL_DIR}/kernel.json"
fi

echo ""
echo "duckdb-kernel ${VERSION} installed to ${KERNEL_DIR}"
echo "Verify with: jupyter kernelspec list"
