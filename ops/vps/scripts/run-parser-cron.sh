#!/usr/bin/env bash
# One-shot parser run for cron-scheduled sources (run_all.py once).
# Usage: run-parser-cron.sh <slug>
set -euo pipefail
SLUG="${1:?slug required}"
BASE="/opt/scholarshiptop-parsers"
APP="${BASE}/app"
ENV_FILE="${BASE}/env/${SLUG}.env"
LOG_DIR="${BASE}/logs"
LOCK_FILE="/tmp/scholarshiptop-parser-${SLUG}.lock"
VENV_PY="${BASE}/venv/bin/python"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_FILE="${LOG_DIR}/${SLUG}-run-${TS}.log"

mkdir -p "${LOG_DIR}"
if [[ ! -f "${ENV_FILE}" ]]; then
  echo "missing ${ENV_FILE}" >&2
  exit 1
fi
if [[ ! -x "${VENV_PY}" ]]; then
  echo "missing venv python: ${VENV_PY}" >&2
  exit 1
fi

{
  echo "=== parser ${SLUG} run ${TS} ==="
} >"${LOG_FILE}"

set +e
flock -n "${LOCK_FILE}" nice -n 10 ionice -c2 -n7 bash -lc "
  set -a
  # shellcheck disable=SC1090
  source \"${ENV_FILE}\"
  set +a
  export PYTHONUNBUFFERED=1
  export PYTHONIOENCODING=utf-8
  cd \"${APP}\"
  exec \"${VENV_PY}\" -u run_all.py
" >>"${LOG_FILE}" 2>&1
ec=$?
{
  echo "=== exit_code=${ec} ==="
} >>"${LOG_FILE}"

echo "log=${LOG_FILE} exit=${ec}"
exit "${ec}"
