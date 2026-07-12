#!/usr/bin/env bash
# Long-running repeater for Online Railway parsers (systemd).
# Usage: run-parser-repeater.sh <slug>
set -euo pipefail
SLUG="${1:?slug required}"
BASE="/opt/scholarshiptop-parsers"
APP="${BASE}/app"
ENV_FILE="${BASE}/env/${SLUG}.env"
LOG_DIR="${BASE}/logs"
VENV_PY="${BASE}/venv/bin/python"
LOG_FILE="${LOG_DIR}/${SLUG}-repeater.log"

mkdir -p "${LOG_DIR}"
if [[ ! -f "${ENV_FILE}" ]]; then
  echo "missing ${ENV_FILE}" >&2
  exit 1
fi
if [[ ! -x "${VENV_PY}" ]]; then
  echo "missing venv python: ${VENV_PY}" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a
export PYTHONUNBUFFERED=1
export PYTHONIOENCODING=utf-8
export SOURCE_TIMEOUT_SECONDS="${SOURCE_TIMEOUT_SECONDS:-3600}"
cd "${APP}"
exec nice -n 10 ionice -c2 -n7 "${VENV_PY}" -u repeater.py >>"${LOG_FILE}" 2>&1
