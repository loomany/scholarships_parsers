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
DEFAULT_POST_COMMAND_DELAY_SECONDS = 180.0
DEFAULT_CONTENT_HUB_REPROCESS_COMMAND = "npm run content:reprocess-articles -- --all-published"
LOG_PREFIX = "[REPEATER]"


def _get_sleep_hours() -> float:
    publication_interval_minutes = os.getenv("PUBLICATION_INTERVAL_MINUTES", "").strip()
    if publication_interval_minutes:
        try:
            parsed_minutes = float(publication_interval_minutes)
            if parsed_minutes > 0:
                return parsed_minutes / 60
        except ValueError:
            print(
                f"{LOG_PREFIX} Invalid PUBLICATION_INTERVAL_MINUTES='{publication_interval_minutes}'. "
                f"Fallback to REPEATER_SLEEP_HOURS.",
                flush=True,
            )

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


def _format_sleep_duration(sleep_seconds: float) -> str:
    hours = sleep_seconds / 3600
    if hours.is_integer():
        return f"{int(hours)}h"
    return f"{hours:.2f}h"


def _get_positive_float_env(var_name: str, default_value: float) -> float:
    raw_value = os.getenv(var_name, str(default_value)).strip()
    try:
        value = float(raw_value)
    except ValueError:
        print(
            f"{LOG_PREFIX} Invalid {var_name}='{raw_value}'. Using default {default_value}.",
            flush=True,
        )
        return default_value

    if value < 0:
        print(
            f"{LOG_PREFIX} Negative {var_name}='{raw_value}'. Using default {default_value}.",
            flush=True,
        )
        return default_value

    return value


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


def _run_post_command_if_needed() -> None:
    post_command = os.getenv("REPEATER_POST_COMMAND", "").strip()
    run_reprocess_on_start = os.getenv("RUN_REPROCESS_ON_START", "").strip().lower()
    if not post_command and run_reprocess_on_start == "false":
        post_command = DEFAULT_CONTENT_HUB_REPROCESS_COMMAND

    if not post_command:
        return

    delay_seconds = _get_positive_float_env(
        "REPEATER_POST_COMMAND_DELAY_SECONDS",
        DEFAULT_POST_COMMAND_DELAY_SECONDS,
    )
    if delay_seconds > 0:
        print(
            f"{LOG_PREFIX} Waiting {delay_seconds:.0f}s before REPEATER_POST_COMMAND.",
            flush=True,
        )
        time.sleep(delay_seconds)

    print(f"{LOG_PREFIX} Running post command: {post_command}", flush=True)
    subprocess.run(shlex.split(post_command), check=True)
    print(f"{LOG_PREFIX} Post command completed.", flush=True)


def main() -> None:
    script_to_execute = os.getenv("EXECUTE_SCRIPT", DEFAULT_SCRIPT).strip() or DEFAULT_SCRIPT
    sleep_hours = _get_sleep_hours()
    sleep_seconds = sleep_hours * 3600

    print(
        f"{LOG_PREFIX} Booted. Target script: {script_to_execute}. Sleep interval: {_format_sleep_duration(sleep_seconds)}.",
        flush=True,
    )

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
            try:
                _run_post_command_if_needed()
            except subprocess.CalledProcessError as exc:
                print(
                    f"{LOG_PREFIX} Post command failed with exit code {exc.returncode}.",
                    flush=True,
                )
            except Exception as exc:  # noqa: BLE001 - repeater must stay alive
                print(f"{LOG_PREFIX} Post command error: {exc}", flush=True)
            _cleanup()
            next_run_str = _format_next_run(sleep_seconds)
            print(f"{LOG_PREFIX} Execution finished. Next run at {next_run_str}.", flush=True)
            print(
                f"{LOG_PREFIX} Repeater is active. Sleeping for {_format_sleep_duration(sleep_seconds)}.",
                flush=True,
            )
            print(
                f"{LOG_PREFIX} Парсер завершен. Вернусь через {sleep_hours:g} часа(ов).",
                flush=True,
            )
            _sleep_with_heartbeat(sleep_seconds)


if __name__ == "__main__":
    main()
