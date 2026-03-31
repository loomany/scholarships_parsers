"""
Точка входа: load_dotenv до любых импортов sources.*, затем запуск по GlobalConfig.

Включение источника: PARSER_SOURCES содержит ключ + соответствующий *_ENABLED=1.
Решение «запускать или нет» — только здесь (парсеры не делают early return по ENABLED).
"""

from __future__ import annotations

import importlib
import os
import sys

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _BASE_DIR)

from dotenv import load_dotenv

load_dotenv(os.path.join(_BASE_DIR, ".env"))
load_dotenv(os.path.join(_BASE_DIR, "..", ".env"))

from config import (
    CANONICAL_SOURCE_KEYS,
    get_global_config,
    get_str,
    print_env_by_prefix,
    print_parser_config_summary,
    source_enabled,
)

_SOURCE_MODULES: dict[str, tuple[str, str]] = {
    "scholarship_america": ("Scholarship America", "sources.scholarship_america"),
    "simpler_grants_gov": ("Simpler.Grants.gov", "sources.simpler_grants_gov"),
    "bigfuture": ("BigFuture (College Board)", "sources.bigfuture"),
}
# Каждый ключ — пакет с __init__.py, экспортирующим run (см. sources/<key>/parser.py).


def main() -> None:
    dbg_prefix = get_str("PARSER_DEBUG_ENV_PREFIX", "")
    if dbg_prefix:
        print_env_by_prefix(dbg_prefix)
        print("")

    gc = get_global_config()
    names = gc.resolved_source_keys()
    print_parser_config_summary(names)

    for key in names:
        if key not in _SOURCE_MODULES:
            print(
                f"Неизвестный источник в PARSER_SOURCES: {key!r}. "
                f"Канонические ключи: {list(CANONICAL_SOURCE_KEYS)}"
            )
            continue
        label, modname = _SOURCE_MODULES[key]
        if not source_enabled(key):
            print("")
            print(f"========== {label} ({key}) — ПРОПУСК: ENABLED=0 ==========")
            continue
        print("")
        print(f"========== {label} ({key}) ==========")
        try:
            mod = importlib.import_module(modname)
            mod.run()
        except Exception as e:
            print(f"Ошибка источника {key}: {e}")
        print(f"========== конец: {key} ==========")

    print("")
    print("Готово.")


if __name__ == "__main__":
    main()
