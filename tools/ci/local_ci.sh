#!/usr/bin/env bash

set -Eeuo pipefail

usage() {
  cat <<'EOF'
Usage: tools/ci/local_ci.sh [options]

Run the local XRPC CI gate.

Options:
  --build-dir DIR     Build directory. Default: build-ci
  --compiler CXX      C++ compiler. Default: clang++-20
  --jobs N            Build parallelism. Default: nproc
  --full              Also run the v1 benchmark smoke.
  --tidy              Also run make check-clang-tidy.
  --no-tools          Do not build benchmark tools or run tool dry-run checks.
  --help              Show this help.

Default gate:
  configure + build + ctest + benchmark config validation + HA dry-runs + git diff --check

Notes:
  - This script checks the tree; it does not auto-format or auto-fix code.
  - Use "make format" locally before running CI if format checks fail.
  - Live Consul/Prometheus HA tests are not run by default.
EOF
}

log() {
  printf '\n==> %s\n' "$*"
}

run() {
  printf '+'
  printf ' %q' "$@"
  printf '\n'
  "$@"
}

run_shell() {
  printf '+ %s\n' "$*"
  /usr/bin/env bash -c "$*"
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'missing required command: %s\n' "$1" >&2
    exit 2
  fi
}

default_jobs() {
  if command -v nproc >/dev/null 2>&1; then
    nproc
    return
  fi
  printf '4\n'
}

repo_root() {
  local script_dir
  script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
  cd -- "${script_dir}/../.." && pwd
}

build_dir="build-ci"
compiler="clang++-20"
jobs="$(default_jobs)"
run_full=0
run_tidy=0
build_tools=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-dir)
      if [[ $# -lt 2 ]]; then
        printf '--build-dir requires an argument\n' >&2
        exit 2
      fi
      build_dir="$2"
      shift 2
      ;;
    --compiler)
      if [[ $# -lt 2 ]]; then
        printf '--compiler requires an argument\n' >&2
        exit 2
      fi
      compiler="$2"
      shift 2
      ;;
    --jobs)
      if [[ $# -lt 2 ]]; then
        printf '--jobs requires an argument\n' >&2
        exit 2
      fi
      jobs="$2"
      shift 2
      ;;
    --full)
      run_full=1
      shift
      ;;
    --tidy)
      run_tidy=1
      shift
      ;;
    --no-tools)
      build_tools=0
      shift
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      printf 'unknown option: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

case "$jobs" in
  ''|*[!0-9]*)
    printf '--jobs must be a positive integer\n' >&2
    exit 2
    ;;
  0)
    printf '--jobs must be greater than zero\n' >&2
    exit 2
    ;;
esac

root="$(repo_root)"
cd "$root"

require_command cmake
require_command ctest
require_command git
require_command "$compiler"

cmake_tools_flag="OFF"
if [[ "$build_tools" -eq 1 ]]; then
  cmake_tools_flag="ON"
fi

start_seconds="$(date +%s)"

log "configure"
run cmake -S . -B "$build_dir" \
  -DCMAKE_CXX_COMPILER="$compiler" \
  -DXRPC_BUILD_TESTS=ON \
  -DXRPC_BUILD_TOOLS="$cmake_tools_flag" \
  -DXRPC_BUILD_EXAMPLES=ON

log "build"
run cmake --build "$build_dir" --parallel "$jobs"

log "unit, runtime, integration, and e2e tests"
run ctest --test-dir "$build_dir/tests" --output-on-failure -LE "external|tooling"

if [[ "$build_tools" -eq 1 ]]; then
  log "benchmark config validation"
  run ./tools/benchmark/run_suite.py --config tools/benchmark/configs/v1_smoke.json --validate-config
  run ./tools/benchmark/run_suite.py --config tools/benchmark/configs/v1_server_capacity.json --validate-config

  log "HA script dry-runs"
  run ./tools/ha/consul_failover_smoke.py --dry-run --benchmark-build-dir "$build_dir"
  run ./tools/ha/consul_fault_injection.py --dry-run --benchmark-build-dir "$build_dir" --scenario=redundant-server-kill
  run ./tools/ha/consul_fault_injection.py --dry-run --benchmark-build-dir "$build_dir" --scenario=resolver-empty \
    --min-client-failures=1
  run ./tools/ha/consul_fault_injection.py --dry-run --benchmark-build-dir "$build_dir" --scenario=delayed-registration \
    --min-client-failures=1
else
  log "tool checks skipped by --no-tools"
fi

if [[ "$run_full" -eq 1 ]]; then
  if [[ "$build_tools" -ne 1 ]]; then
    printf '--full requires benchmark tools; remove --no-tools\n' >&2
    exit 2
  fi
  log "v1 benchmark smoke"
  run ./tools/benchmark/run_suite.py --config tools/benchmark/configs/v1_smoke.json --build
fi

if [[ "$run_tidy" -eq 1 ]]; then
  log "clang-tidy"
  run make BUILD_DIR="$build_dir" XRPC_CXX="$compiler" check-clang-tidy
fi

log "working tree whitespace check"
run git diff --check

elapsed_seconds="$(($(date +%s) - start_seconds))"
printf '\nlocal_ci_result=PASS elapsed_s=%s build_dir=%s full=%s tidy=%s tools=%s\n' \
  "$elapsed_seconds" "$build_dir" "$run_full" "$run_tidy" "$build_tools"
