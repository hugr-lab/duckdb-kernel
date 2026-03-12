BINARY := duckdb-kernel
BUILD_TAGS := duckdb_arrow
KERNEL_DIR := $(HOME)/Library/Jupyter/kernels/duckdb

.PHONY: build install clean test build-jupyterlab build-vscode build-extensions

build:
	go build -tags $(BUILD_TAGS) -o $(BINARY) ./cmd/duckdb-kernel

install: build
	mkdir -p $(KERNEL_DIR)
	ln -sf $(CURDIR)/$(BINARY) $(KERNEL_DIR)/$(BINARY)
	cp kernel/kernel.json $(KERNEL_DIR)/kernel.json
	@# Update kernel.json to use absolute path
	@sed -i'' -e 's|"duckdb-kernel"|"$(KERNEL_DIR)/$(BINARY)"|' $(KERNEL_DIR)/kernel.json
	@echo "Kernel installed to $(KERNEL_DIR)"

test: install
	@cd tests && pip install -q -r requirements.txt pytest && \
		python -m pytest integration_test.py -v

build-jupyterlab:
	cd extensions/jupyterlab && jlpm install && jlpm build

build-vscode:
	cd extensions/vscode && npm install && npm run build

build-extensions: build-jupyterlab build-vscode

clean:
	rm -f $(BINARY)
