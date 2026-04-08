#!/usr/bin/env python3
"""Universal repeater for running parser scripts in an endless loop."""

from __future__ import annotations

import gc
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

DEFAULT_SCRIPT = "run_all.py"
DEFAULT_SLEEP_HOURS = 2.0
LOG_PREFIX = "[REPEATER]"


def _get_sleep_hours() -> float:
    raw_value = os.getenv("REPEATER_SLEEP_HOURS", str(DEFAULT_SLEEP_HOURS)).strip()
    try:
        hours = float(raw_value)
    except ValueError:
        print(
            f"{LOG_PREFIX} Invalid REPEATER_SLEEP_HOURS='{raw_value}'. "
            f"Using default {DEFAULT_SLEEP_HOURS}h.",
            flush=True,
        )
        return DEFAULT_SLEEP_HOURS

    if hours <= 0:
        print(
            f"{LOG_PREFIX} Non-positive REPEATER_SLEEP_HOURS='{raw_value}'. "
            f"Using default {DEFAULT_SLEEP_HOURS}h.",
            flush=True,
        )
        return DEFAULT_SLEEP_HOURS

    return hours


def _format_next_run(sleep_seconds: float) -> str:
    next_run = datetime.now(timezone.utc) + timedelta(seconds=sleep_seconds)
    return next_run.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def _cleanup() -> None:
    gc.collect()

    cleanup_command = os.getenv("REPEATER_CLEANUP_COMMAND", "").strip()
    if not cleanup_command:
        return

    print(f"{LOG_PREFIX} Running cleanup command: {cleanup_command}", flush=True)
    try:
        subprocess.run(shlex.split(cleanup_command), check=True)
    except Exception as exc:  # noqa: BLE001 - cleanup must never stop the repeater
        print(f"{LOG_PREFIX} Cleanup command failed: {exc}", flush=True)


def main() -> None:
    script_to_execute = os.getenv("EXECUTE_SCRIPT", DEFAULT_SCRIPT).strip() or DEFAULT_SCRIPT
    sleep_hours = _get_sleep_hours()
    sleep_seconds = sleep_hours * 3600

    while True:
        print(f"{LOG_PREFIX} Starting execution of {script_to_execute}.", flush=True)

        try:
            subprocess.run([sys.executable, script_to_execute], check=True)
        except subprocess.CalledProcessError as exc:
            print(
                f"{LOG_PREFIX} Script failed with exit code {exc.returncode}: {script_to_execute}",
                flush=True,
            )
        except Exception as exc:  # noqa: BLE001 - repeater must stay alive
            print(f"{LOG_PREFIX} Unexpected error: {exc}", flush=True)
        finally:
            _cleanup()
            next_run_str = _format_next_run(sleep_seconds)
            print(f"{LOG_PREFIX} Execution finished. Next run at {next_run_str}.", flush=True)
            time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
