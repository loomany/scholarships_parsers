"""
Точка входа: загрузка .env и запуск парсеров.

По умолчанию: только Scholarship America (как раньше).

Список источников — переменная окружения PARSER_SOURCES:
  - scholarship_america — только Scholarship America
  - simpler_grants_gov — только Simpler.Grants.gov (HTML /search, без API-ключа)
  - all — оба по очереди (ошибка одного источника не останавливает второй)
  - через запятую, например: scholarship_america,simpler_grants_gov
"""

from __future__ import annotations

import os
import sys

# Чтобы можно было запускать из любой папки: python parsers/run_all.py
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv

from sources.scholarship_america import run as run_scholarship_america
from sources.simpler_grants_gov import run as run_simpler_grants_gov


def _parse_parser_sources() -> list[str]:
    raw = (os.environ.get("PARSER_SOURCES") or "scholarship_america").strip().lower()
    if raw == "all":
        return ["scholarship_america", "simpler_grants_gov"]
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return parts or ["scholarship_america"]


def main() -> None:
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    load_dotenv(env_path)
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

    registry: dict[str, tuple[str, object]] = {
        "scholarship_america": ("Scholarship America", run_scholarship_america),
        "simpler_grants_gov": ("Simpler.Grants.gov", run_simpler_grants_gov),
    }

    names = _parse_parser_sources()
    for key in names:
        if key not in registry:
            print(f"Неизвестный источник в PARSER_SOURCES: {key!r}. Доступно: {sorted(registry)}")
            continue
        label, run_fn = registry[key]
        print("")
        print(f"========== {label} ({key}) ==========")
        try:
            run_fn()
        except Exception as e:
            print(f"Ошибка источника {key}: {e}")
        print(f"========== конец: {key} ==========")

    print("")
    print("Готово.")


if __name__ == "__main__":
    main()
