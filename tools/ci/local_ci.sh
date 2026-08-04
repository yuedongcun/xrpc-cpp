#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/../.." && pwd)"
cd "${repo_root}"

make test
ctest --test-dir build/tests --output-on-failure -L tooling
git diff --check

printf '\nlocal_ci_result=PASS\n'
