#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
LMCACHE_ROOT="${LMCACHE_ROOT:-${SCRIPT_DIR}}"
PYTHON_BIN="${PYTHON_BIN:-/mooncake_test/venv/bin/python3}"
LMCACHE_START_SCRIPT="${LMCACHE_START_SCRIPT:-/mooncake_test/workdir/start_lmcache.sh}"

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

  echo "[io-metrics] ${module_name}: ${actual_path}"
  if [[ "${actual_path}" != "${expected_path}" ]]; then
    echo "[io-metrics] ERROR: expected ${expected_path}, got ${actual_path}" >&2
    exit 1
  fi
}

verify_module_path "lmcache" "${LMCACHE_ROOT}/lmcache/__init__.py"
verify_module_path \
  "lmcache.v1.mp_observability.subscribers.metrics.l2_io" \
  "${LMCACHE_ROOT}/lmcache/v1/mp_observability/subscribers/metrics/l2_io.py"
verify_module_path \
  "lmcache.lmcache_fs" \
  "${LMCACHE_ROOT}/lmcache/lmcache_fs.cpython-312-x86_64-linux-gnu.so"

echo "[io-metrics] PYTHONPATH verified"

if [[ "${VERIFY_ONLY:-0}" == "1" ]]; then
  exit 0
fi

exec "${LMCACHE_START_SCRIPT}"
