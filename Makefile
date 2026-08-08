BUILD_DIR ?= build
CMAKE_ARGS ?=

.DEFAULT_GOAL := all
.PHONY: all configure test clangd-db dev clean

configure:
	cmake -S . -B $(BUILD_DIR) \
		-DXRPC_BUILD_TESTS=ON \
		-DXRPC_BUILD_EXAMPLES=ON \
		-DXRPC_BUILD_TOOLS=ON \
		-DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
		$(CMAKE_ARGS)

all: configure
	cmake --build $(BUILD_DIR) --parallel

test: all
	ctest --test-dir $(BUILD_DIR)/tests \
		--output-on-failure \
		-LE external

clangd-db:
	@command -v compdb >/dev/null 2>&1 || { \
		echo "error: compdb not found"; \
		exit 1; \
	}
	@test -f "$(BUILD_DIR)/compile_commands.json" || { \
		echo "error: $(BUILD_DIR)/compile_commands.json not found; run 'make configure' first"; \
		exit 1; \
	}
	@tmp="$(BUILD_DIR)/compile_commands.json.tmp"; \
	compdb -p "$(BUILD_DIR)" list > "$$tmp" && \
	mv "$$tmp" "$(BUILD_DIR)/compile_commands.json"

dev: all
	$(MAKE) clangd-db

clean:
	rm -rf $(BUILD_DIR)

