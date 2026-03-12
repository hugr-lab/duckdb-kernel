BINARY := duckdb-kernel
BUILD_TAGS := duckdb_arrow
KERNEL_DIR := $(HOME)/.local/share/jupyter/kernels/duckdb

.PHONY: build install clean

build:
	go build -tags $(BUILD_TAGS) -o $(BINARY) ./cmd/duckdb-kernel

install: build
	mkdir -p $(KERNEL_DIR)
	cp $(BINARY) $(KERNEL_DIR)/$(BINARY)
	cp kernel/kernel.json $(KERNEL_DIR)/kernel.json
	@# Update kernel.json to use absolute path
	@sed -i'' -e 's|"duckdb-kernel"|"$(KERNEL_DIR)/$(BINARY)"|' $(KERNEL_DIR)/kernel.json
	@echo "Kernel installed to $(KERNEL_DIR)"

clean:
	rm -f $(BINARY)
