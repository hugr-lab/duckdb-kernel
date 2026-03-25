BINARY := duckdb-kernel
BUILD_TAGS := duckdb_arrow
KERNEL_DIR := $(HOME)/Library/Jupyter/kernels/duckdb
PYTHON := .venv/bin/python

.PHONY: build install clean test \
	build-perspective-core build-jupyter build-vscode build-extensions \
	install-jupyterlab install-vscode copy-perspective

# --- Kernel ---

build:
	go build -tags $(BUILD_TAGS) -o $(BINARY) ./cmd/duckdb-kernel

install: build copy-perspective
	mkdir -p $(KERNEL_DIR)
	ln -sf $(CURDIR)/$(BINARY) $(KERNEL_DIR)/$(BINARY)
	cp kernel/kernel.json $(KERNEL_DIR)/kernel.json
	@sed -i'' -e 's|"duckdb-kernel"|"$(KERNEL_DIR)/$(BINARY)"|' $(KERNEL_DIR)/kernel.json
	@cp kernel/logo-32x32.png kernel/logo-64x64.png $(KERNEL_DIR)/ 2>/dev/null || true
	@ln -sfn $(CURDIR)/static $(KERNEL_DIR)/static
	@echo "Kernel installed to $(KERNEL_DIR)"

test: install
	@cd tests && pip install -q -r requirements.txt pytest && \
		python -m pytest integration_test.py -v

clean:
	rm -f $(BINARY)

# --- Shared core (must be built before jupyter and vscode) ---

build-perspective-core:
	cd extensions/perspective-core && npm install && npm run build

# --- JupyterLab extensions ---

build-jupyter: build-perspective-core
	cd extensions/jupyter && jlpm install
	cd extensions/jupyter/perspective-viewer && jlpm build:prod
	cd extensions/jupyter/duckdb-explorer && jlpm build:prod

install-jupyterlab: build-jupyter
	uv pip install -e extensions/jupyter/perspective-viewer/ --python $(PYTHON)
	uv pip install -e extensions/jupyter/duckdb-explorer/ --python $(PYTHON)

# --- VS Code extension ---

build-vscode: build-perspective-core
	cd extensions/vscode && npm install && npm run build

install-vscode: build-vscode copy-perspective
	@echo "VS Code extension built. Restart VS Code to pick up changes."
	@echo "Perspective static files at static/perspective/ (served by kernel)"

# --- Static files (Perspective JS/WASM for Go kernel HTTP server) ---

copy-perspective: build-jupyter
	@mkdir -p static/perspective
	@cp extensions/jupyter/perspective-viewer/hugr_perspective/labextension/static/perspective/* static/perspective/
	@echo "Perspective static files copied to static/perspective/"

# --- All extensions ---

build-extensions: build-jupyter build-vscode
