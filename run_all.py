"""
Точка входа: load_dotenv до любых импортов sources.*, затем запуск по GlobalConfig.

Включение источника: PARSER_SOURCES содержит ключ + соответствующий *_ENABLED=1.
Решение «запускать или нет» — только здесь (парсеры не делают early return по ENABLED).
"""

from __future__ import annotations

import importlib
import multiprocessing as mp
import os
import sys
import traceback
from datetime import datetime, timezone

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _BASE_DIR)

from dotenv import load_dotenv

load_dotenv(os.path.join(_BASE_DIR, ".env"))
load_dotenv(os.path.join(_BASE_DIR, "..", ".env"))

# Потоковый вывод в консоль (Windows/redirect): лог виден по мере выполнения.
os.environ.setdefault("PYTHONUNBUFFERED", "1")

# Пользовательский режим: всегда показываем браузер при каждом запуске.
# Чтобы разрешить headless из .env, задайте RUN_ALL_ALLOW_HEADLESS=1.
if os.environ.get("RUN_ALL_ALLOW_HEADLESS", "").strip().lower() not in (
    "1",
    "true",
    "yes",
    "on",
):
    os.environ["BIGFUTURE_HEADLESS"] = "0"
    os.environ["BOLD_HEADLESS"] = "0"
    os.environ["SCHOLARSHIPS_COM_HEADLESS"] = "0"
    os.environ["MASTERSPORTAL_HEADLESS"] = "0"
# Держим окно открытым на протяжении выполнения, но не блокируем завершение бесконечным "hold open".
os.environ["BOLD_KEEP_BROWSER_OPEN"] = "0"
os.environ["SCHOLARSHIPS_COM_KEEP_BROWSER_OPEN"] = "0"

from config import (
    CANONICAL_SOURCE_KEYS,
    get_global_config,
    get_int,
    get_str,
    print_env_by_prefix,
    print_parser_config_summary,
    source_enabled,
)
from telegram_notify import send_report

_SOURCE_MODULES: dict[str, tuple[str, str]] = {
    "scholarship_america": ("Scholarship America", "sources.scholarship_america"),
    "simpler_grants_gov": ("Simpler.Grants.gov", "sources.simpler_grants_gov"),
    "bigfuture": ("BigFuture (College Board)", "sources.bigfuture"),
    "bold_org": ("Bold.org", "sources.bold_org"),
    "scholarships_com": ("Scholarships.com", "sources.scholarships_com"),
    "mastersportal": ("Mastersportal", "sources.mastersportal"),
    "scholars4dev": ("Scholars4Dev", "sources.scholars4dev"),
    "opportunitydesk": ("Opportunity Desk", "sources.opportunitydesk"),
    "scholarships360": ("Scholarships360", "sources.scholarships360"),
    "daad": ("DAAD", "sources.daad"),
    "iefa": ("IEFA.org", "sources.iefa"),
    "ed_gov_html": ("ED.gov Grants Page", "sources.ed_gov_html"),
    "uoregon_research_html": ("UOregon External Funding", "sources.uoregon_research_html"),
    "wemakescholars": ("WeMakeScholars", "sources.wemakescholars"),
    "oneyoungworld": ("One Young World", "sources.oneyoungworld"),
    "mina7portal": ("Mina7 Portal", "sources.mina7portal"),
}
# Каждый ключ — пакет с __init__.py, экспортирующим run (см. sources/<key>/parser.py).

_SOURCE_DOMAINS: dict[str, str] = {
    "scholarship_america": "scholarshipamerica.org",
    "simpler_grants_gov": "simpler.grants.gov",
    "bigfuture": "bigfuture.collegeboard.org",
    "bold_org": "bold.org",
    "scholarships_com": "scholarships.com",
    "mastersportal": "mastersportal.com",
    "scholars4dev": "scholars4dev.com",
    "opportunitydesk": "opportunitydesk.org",
    "scholarships360": "scholarships360.org",
    "daad": "daad.de",
    "iefa": "iefa.org",
    "ed_gov_html": "ed.gov",
    "uoregon_research_html": "research.uoregon.edu",
    "wemakescholars": "wemakescholars.com",
    "oneyoungworld": "oneyoungworld.com",
    "mina7portal": "mina7portal.com",
}


def _run_source_subprocess(modname: str, result_q: mp.Queue) -> None:
    try:
        mod = importlib.import_module(modname)
        mod.run()
        result_q.put({"ok": True, "error": ""})
    except Exception as e:
        result_q.put(
            {
                "ok": False,
                "error": str(e),
                "traceback": traceback.format_exc(),
            }
        )


def _ensure_utf8_stdio() -> None:
    """Windows cp1251 ломает box-drawing в print_parser_config_summary; UTF-8 безопаснее."""
    if sys.platform != "win32":
        return
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except Exception:
            pass


def _classify_manual_reason(error_text: str, timed_out: bool) -> str:
    if timed_out:
        return "timeout_or_stuck"
    t = (error_text or "").lower()
    if any(x in t for x in ("captcha", "cloudflare", "challenge", "access denied", "forbidden", "429")):
        return "captcha_or_blocked"
    if any(x in t for x in ("connection", "timed out", "dns", "ssl", "name resolution", "unreachable")):
        return "site_unavailable_or_ip_block"
    return "manual_review_needed"


def main() -> None:
    _ensure_utf8_stdio()
    started_at = datetime.now(timezone.utc)
    dbg_prefix = get_str("PARSER_DEBUG_ENV_PREFIX", "")
    if dbg_prefix:
        print_env_by_prefix(dbg_prefix)
        print("")

    gc = get_global_config()
    names = gc.resolved_source_keys()
    print_parser_config_summary(names)
    try:
        start_lines = [
            "PARSER STARTED",
            "Продукт: Сбор грантов из подключенных источников",
            f"Режим: {gc.parser_mode or 'sources'}",
            f"Источников в запуске: {len(names)}",
            *([f"- {x}" for x in names] or ["- none"]),
            "Браузер: открыт (headful) для BigFuture/Bold/Scholarships.com/Mastersportal, "
            "если не задан RUN_ALL_ALLOW_HEADLESS=1. IEFA — HTTP по умолчанию, только лог в терминале.",
            "",
            "Статус: выполняется...",
        ]
        send_report("\n".join(start_lines))
    except Exception as e:
        print(f"Telegram start report error: {e}")

    executed: list[str] = []
    skipped: list[str] = []
    failed: list[str] = []
    manual_check: list[str] = []
    source_timeout_seconds = max(60, get_int("SOURCE_TIMEOUT_SECONDS", 900))

    for key in names:
        if key not in _SOURCE_MODULES:
            print(
                f"Неизвестный источник в PARSER_SOURCES: {key!r}. "
                f"Канонические ключи: {list(CANONICAL_SOURCE_KEYS)}"
            )
            failed.append(f"{key} (unknown)")
            continue
        label, modname = _SOURCE_MODULES[key]
        if not source_enabled(key):
            print("")
            print(f"========== {label} ({key}) — ПРОПУСК: ENABLED=0 ==========")
            skipped.append(f"{label} ({key})")
            continue
        print("")
        print(f"========== {label} ({key}) ==========")
        domain = _SOURCE_DOMAINS.get(key, "unknown-domain")
        result_q: mp.Queue = mp.get_context("spawn").Queue()
        proc = mp.get_context("spawn").Process(
            target=_run_source_subprocess,
            args=(modname, result_q),
            daemon=True,
        )
        proc.start()
        proc.join(source_timeout_seconds)
        timed_out = False
        if proc.is_alive():
            timed_out = True
            proc.terminate()
            proc.join(5)
            err = f"Source exceeded timeout={source_timeout_seconds}s and was terminated."
            reason = _classify_manual_reason(err, timed_out=True)
            print(f"Ошибка источника {key}: {err}")
            failed.append(f"{label} ({key})")
            manual_check.append(f"- {domain} | {reason} | {label} ({key})")
            print(f"========== конец: {key} ==========")
            continue
        result = {"ok": proc.exitcode == 0, "error": ""}
        if not result_q.empty():
            result = result_q.get()
        if result.get("ok"):
            executed.append(f"{label} ({key})")
        else:
            err = str(result.get("error") or f"exit_code={proc.exitcode}")
            print(f"Ошибка источника {key}: {err}")
            tb = result.get("traceback")
            if tb:
                print(tb)
            failed.append(f"{label} ({key})")
            reason = _classify_manual_reason(err, timed_out=timed_out)
            manual_check.append(f"- {domain} | {reason} | {label} ({key})")
        print(f"========== конец: {key} ==========")

    print("")
    print("Готово.")

    finished_at = datetime.now(timezone.utc)
    elapsed = int((finished_at - started_at).total_seconds())
    report_lines = [
        "PARSER FINISHED",
        "Продукт: Сбор грантов из подключенных источников",
        f"Длительность: {elapsed}s",
        "",
        f"Успешно выполнено: {len(executed)}",
        *([f"- {x}" for x in executed] or ["- none"]),
        "",
        f"Пропущено: {len(skipped)}",
        *([f"- {x}" for x in skipped] or ["- none"]),
        "",
        f"С ошибками: {len(failed)}",
        *([f"- {x}" for x in failed] or ["- none"]),
        "",
        f"Ручная проверка доменов: {len(manual_check)}",
        *(manual_check or ["- none"]),
        "",
        "Браузер: headful для BigFuture/Bold/Scholarships.com/Mastersportal; IEFA — окно Chromium "
        "только при IEFA_VISIBLE_BROWSER=1 (иначе HTTP без окна). "
        "Лог: PYTHONUNBUFFERED / python -u.",
    ]
    try:
        send_report("\n".join(report_lines))
    except Exception as e:
        print(f"Telegram report error: {e}")


if __name__ == "__main__":
    main()
