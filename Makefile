BUILD_DIR ?= build
XRPC_CXX ?= c++

.DEFAULT_GOAL := all

.PHONY: configure all test test-all test-unit test-runtime test-integration test-e2e test-tooling test-external ci format check-format check-clang-tidy clean clean-build reconfigure

configure:
	cmake -S . -B $(BUILD_DIR) \
		-DCMAKE_CXX_COMPILER=$(XRPC_CXX) \
		-DXRPC_BUILD_TESTS=ON \
		-DXRPC_BUILD_EXAMPLES=ON \
		-DXRPC_BUILD_TOOLS=OFF

clean-build:
	rm -rf $(BUILD_DIR)

all: configure
	cmake --build $(BUILD_DIR) --parallel

test: all
	ctest --test-dir $(BUILD_DIR)/tests --output-on-failure -LE "external|tooling"

test-all: all
	ctest --test-dir $(BUILD_DIR)/tests --output-on-failure

test-unit: all
	ctest --test-dir $(BUILD_DIR)/tests --output-on-failure -L unit

test-runtime: all
	ctest --test-dir $(BUILD_DIR)/tests --output-on-failure -L runtime

test-integration: all
	ctest --test-dir $(BUILD_DIR)/tests --output-on-failure -L integration -LE external

test-e2e: all
	ctest --test-dir $(BUILD_DIR)/tests --output-on-failure -L e2e -LE external

test-tooling: all
	ctest --test-dir $(BUILD_DIR)/tests --output-on-failure -L tooling

test-external: all
	XRPC_ENABLE_CONSUL_TESTS=1 ctest --test-dir $(BUILD_DIR)/tests --output-on-failure -L external

ci:
	./tools/ci/local_ci.sh

format: configure
	cmake --build $(BUILD_DIR) --target format

check-format: configure
	cmake --build $(BUILD_DIR) --target check-format

check-clang-tidy: configure
	cmake --build $(BUILD_DIR) --target check-clang-tidy

clean:
	cmake --build $(BUILD_DIR) --target clean
