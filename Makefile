BUILD_DIR ?= build
CMAKE_ARGS ?=

.DEFAULT_GOAL := all
.PHONY: all configure test ci clean

configure:
	cmake -S . -B $(BUILD_DIR) \
		-DXRPC_BUILD_TESTS=ON \
		-DXRPC_BUILD_EXAMPLES=ON \
		-DXRPC_BUILD_TOOLS=ON \
		$(CMAKE_ARGS)

all: configure
	cmake --build $(BUILD_DIR) --parallel

test: all
	ctest --test-dir $(BUILD_DIR)/tests \
		--output-on-failure \
		-LE "external|tooling"

ci:
	./tools/ci/local_ci.sh

clean:
	rm -rf $(BUILD_DIR)
