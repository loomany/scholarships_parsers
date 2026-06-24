#!/usr/bin/env bash
# ScholarshipTop parsers — VPS bootstrap (run on VPS as root/ubuntu with sudo).
set -euo pipefail
BASE=/opt/scholarshiptop-parsers
APP="$BASE/app"
VENV="$BASE/venv"

sudo mkdir -p "$BASE/env" "$BASE/logs" "$BASE/scripts"
sudo chmod 700 "$BASE/env"

if [[ ! -d "$APP/.git" ]]; then
  sudo git clone https://github.com/loomany/scholarships_parsers.git "$APP"
fi

if [[ ! -x "$VENV/bin/python" ]]; then
  sudo python3 -m venv "$VENV"
  sudo "$VENV/bin/pip" install --upgrade pip
  sudo "$VENV/bin/pip" install -r "$APP/requirements.txt"
  sudo "$VENV/bin/playwright" install chromium
  sudo "$VENV/bin/playwright" install-deps chromium || true
fi

echo "bootstrap_ok app=$APP venv=$VENV"
