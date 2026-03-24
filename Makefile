BINARY := duckdb-kernel
BUILD_TAGS := duckdb_arrow
KERNEL_DIR := $(HOME)/Library/Jupyter/kernels/duckdb

.PHONY: build install clean test build-jupyterlab install-jupyterlab build-vscode build-extensions

build:
	go build -tags $(BUILD_TAGS) -o $(BINARY) ./cmd/duckdb-kernel

install: build copy-perspective
	mkdir -p $(KERNEL_DIR)
	ln -sf $(CURDIR)/$(BINARY) $(KERNEL_DIR)/$(BINARY)
	cp kernel/kernel.json $(KERNEL_DIR)/kernel.json
	@# Update kernel.json to use absolute path
	@sed -i'' -e 's|"duckdb-kernel"|"$(KERNEL_DIR)/$(BINARY)"|' $(KERNEL_DIR)/kernel.json
	@# Copy kernel logos
	@cp kernel/logo-32x32.png kernel/logo-64x64.png $(KERNEL_DIR)/ 2>/dev/null || true
	@# Symlink perspective static files next to binary
	@ln -sfn $(CURDIR)/static $(KERNEL_DIR)/static
	@echo "Kernel installed to $(KERNEL_DIR)"

copy-perspective:
	@mkdir -p static/perspective
	@cp extensions/jupyterlab/hugr_perspective/labextension/static/perspective/* static/perspective/ 2>/dev/null || true
	@echo "Perspective static files copied to static/perspective/"

test: install
	@cd tests && pip install -q -r requirements.txt pytest && \
		python -m pytest integration_test.py -v

build-jupyterlab:
	cd extensions/jupyterlab && jlpm install && jlpm build

install-jupyterlab: build-jupyterlab
	uv pip install -e extensions/jupyterlab/ --python .venv/bin/python

build-vscode:
	cd extensions/vscode && npm install && npm run build

build-extensions: build-jupyterlab build-vscode

clean:
	rm -f $(BINARY)
