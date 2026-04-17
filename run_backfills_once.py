"""Run eligibility + structured taxonomy backfills (loads .env from this folder)."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

BASE = Path(__file__).resolve().parent


def main() -> None:
    load_dotenv(BASE / ".env")
    venv = BASE / ".venv" / "Scripts" / "python.exe"
    exe = str(venv) if venv.exists() else sys.executable
    os.environ["PYTHONUNBUFFERED"] = "1"
    for script in ("backfill_eligibility_and_levels.py", "backfill_structured_taxonomy.py"):
        print(f"=== {script} ===", flush=True)
        r = subprocess.run(
            [exe, str(BASE / script)],
            cwd=str(BASE),
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        if r.returncode != 0:
            sys.exit(r.returncode)
    print("=== all backfills finished ===", flush=True)


if __name__ == "__main__":
    main()
