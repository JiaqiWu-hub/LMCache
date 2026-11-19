#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
LMCACHE_ROOT="${LMCACHE_ROOT:-${SCRIPT_DIR}}"
PYTHON_BIN="${PYTHON_BIN:-/mooncake_test/venv/bin/python3}"
VLLM_START_SCRIPT="${VLLM_START_SCRIPT:-/mooncake_test/workdir/start_lmcache_vllm.sh}"

export PYTHONPATH="${LMCACHE_ROOT}${PYTHONPATH:+:${PYTHONPATH}}"

verify_module_path() {
  local module_name="$1"
  local expected_path="$2"
  local actual_path

  actual_path="$(
    "${PYTHON_BIN}" -c \
      'import importlib.util, pathlib, sys; spec = importlib.util.find_spec(sys.argv[1]); print(pathlib.Path(spec.origin).resolve())' \
      "${module_name}"
  )"

  echo "[latency] ${module_name}: ${actual_path}"
  if [[ "${actual_path}" != "${expected_path}" ]]; then
    echo "[latency] ERROR: expected ${expected_path}, got ${actual_path}" >&2
    exit 1
  fi
}

verify_module_path \
  "lmcache" \
  "${LMCACHE_ROOT}/lmcache/__init__.py"
verify_module_path \
  "lmcache.integration.vllm.lmcache_mp_connector" \
  "${LMCACHE_ROOT}/lmcache/integration/vllm/lmcache_mp_connector.py"

echo "[latency] PYTHONPATH verified"

exec "${VLLM_START_SCRIPT}"
