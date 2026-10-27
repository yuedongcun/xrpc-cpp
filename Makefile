BUILD_DIR ?= build
RELEASE_BUILD_DIR ?= build-release
CMAKE_ARGS ?=
CLANG_FORMAT ?= clang-format-20
CLANG_TIDY ?= clang-tidy-20
RUN_CLANG_TIDY ?= run-clang-tidy-20
INSTALL_PREFIX ?= /usr/local

.DEFAULT_GOAL := all
.PHONY: all configure release install test format check-format check-tidy check-exceptions clangd-db dev clean

FORMAT_FILES = $(shell find include src -type f \( \
	-name '*.h' -o -name '*.hh' -o -name '*.hpp' -o \
	-name '*.c' -o -name '*.cc' -o -name '*.cpp' -o -name '*.cxx' \
\))

configure:
	cmake -S . -B $(BUILD_DIR) \
		-DXRPC_BUILD_TESTS=ON \
		-DXRPC_BUILD_EXAMPLES=ON \
		-DXRPC_BUILD_TOOLS=ON \
		-DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
		$(CMAKE_ARGS) \
		-DCMAKE_BUILD_TYPE=Debug

all: configure
	cmake --build $(BUILD_DIR) --parallel

install: all
	cmake --install $(BUILD_DIR) --prefix "$(INSTALL_PREFIX)"

release:
	cmake -S . -B $(RELEASE_BUILD_DIR) \
		-DXRPC_BUILD_TESTS=ON \
		-DXRPC_BUILD_EXAMPLES=ON \
		-DXRPC_BUILD_TOOLS=ON \
		-DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
		$(CMAKE_ARGS) \
		-DCMAKE_BUILD_TYPE=Release
	cmake --build $(RELEASE_BUILD_DIR) --parallel

test: all
	ctest --test-dir $(BUILD_DIR)/tests \
		--output-on-failure \
		-LE external

format:
	@command -v $(CLANG_FORMAT) >/dev/null 2>&1 || { \
		echo "error: $(CLANG_FORMAT) not found"; \
		exit 1; \
	}
	@$(CLANG_FORMAT) -i $(FORMAT_FILES)

check-format:
	@command -v $(CLANG_FORMAT) >/dev/null 2>&1 || { \
		echo "error: $(CLANG_FORMAT) not found"; \
		exit 1; \
	}
	@$(CLANG_FORMAT) --dry-run --Werror $(FORMAT_FILES)

check-exceptions:
	@python3 tools/check_exception_boundaries.py

check-tidy:
	@command -v $(CLANG_TIDY) >/dev/null 2>&1 || { \
		echo "error: $(CLANG_TIDY) not found"; \
		exit 1; \
	}
	@command -v $(RUN_CLANG_TIDY) >/dev/null 2>&1 || { \
		echo "error: $(RUN_CLANG_TIDY) not found"; \
		exit 1; \
	}
	@test -f "$(BUILD_DIR)/compile_commands.json" || { \
		echo "error: $(BUILD_DIR)/compile_commands.json not found; run 'make' first"; \
		exit 1; \
	}
	@$(RUN_CLANG_TIDY) \
		-clang-tidy-binary="$(CLANG_TIDY)" \
		-p "$(BUILD_DIR)" \
		-config-file="$(CURDIR)/.clang-tidy" \
		-quiet \
		-header-filter="$(CURDIR)/(include|src)/.*" \
		"$(CURDIR)/src/.*\\.cpp"

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
