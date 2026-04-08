"""
Единый слой конфигурации парсеров из os.environ.

Загрузку .env выполняйте до импорта этого модуля (см. run_all.py; только корневой .env).
Пер-источниковые шаблоны: sources/<источник>/.env.example (не загружаются автоматически).
Значения кэшируются на процесс (lru_cache).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


def get_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def get_int(name: str, default: int = 0) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def get_str(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


# Алиасы токенов в PARSER_SOURCES → канонический ключ модуля
SOURCE_ALIASES: dict[str, str] = {
    "simpler": "simpler_grants_gov",
    "scholarship": "scholarship_america",
    "sa": "scholarship_america",
    "bf": "bigfuture",
}

CANONICAL_SOURCE_KEYS: tuple[str, ...] = (
    "scholarship_america",
    "simpler_grants_gov",
    "bigfuture",
)


@dataclass(frozen=True)
class GlobalConfig:
    parser_sources_raw: str
    target_new_items: int
    max_list_pages: int
    no_new_pages_stop: int
    skip_existing_on_list: bool
    use_title_fallback_known: bool
    discovery_mode: str

    @classmethod
    def load(cls) -> GlobalConfig:
        dm = get_str("DISCOVERY_MODE", "new_only") or "new_only"
        return cls(
            parser_sources_raw=get_str("PARSER_SOURCES", "scholarship_america"),
            target_new_items=get_int("TARGET_NEW_ITEMS", 50),
            max_list_pages=get_int("MAX_LIST_PAGES", 1000),
            no_new_pages_stop=get_int("NO_NEW_PAGES_STOP", 50),
            skip_existing_on_list=get_bool("SKIP_EXISTING_ON_LIST", True),
            use_title_fallback_known=get_bool("USE_TITLE_FALLBACK_KNOWN", False),
            discovery_mode=dm.lower(),
        )

    def resolved_source_keys(self) -> list[str]:
        raw = self.parser_sources_raw.lower().strip()
        if raw == "all":
            return list(CANONICAL_SOURCE_KEYS)
        out: list[str] = []
        for part in raw.split(","):
            t = part.strip().lower()
            if not t:
                continue
            out.append(SOURCE_ALIASES.get(t, t))
        return out or ["scholarship_america"]


@dataclass(frozen=True)
class ScholarshipAmericaConfig:
    enabled: bool
    max_records_debug: int
    detail_fetch: bool
    ai_enabled: bool
    ai_model: str
    ai_max_input_chars: int
    keyword: str

    @classmethod
    def load(cls) -> ScholarshipAmericaConfig:
        return cls(
            enabled=get_bool("SCHOLARSHIP_AMERICA_ENABLED", True),
            max_records_debug=get_int("SCHOLARSHIP_AMERICA_MAX_RECORDS_DEBUG", 0),
            detail_fetch=get_bool("SCHOLARSHIP_AMERICA_DETAIL_FETCH", True),
            ai_enabled=False,
            ai_model="",
            ai_max_input_chars=0,
            keyword="",
        )


@dataclass(frozen=True)
class SimplerConfig:
    enabled: bool
    max_records_debug: int
    detail_fetch: bool
    ai_enabled: bool
    ai_model: str
    ai_max_input_chars: int
    keyword: str
    include_extended_search: bool

    @classmethod
    def load(cls) -> SimplerConfig:
        enabled = get_bool(
            "SIMPLER_ENABLED",
            get_bool("SIMPLER_GRANTS_GOV_ENABLED", True),
        )
        return cls(
            enabled=enabled,
            max_records_debug=get_int("SIMPLER_MAX_RECORDS_DEBUG", 30),
            detail_fetch=get_bool("SIMPLER_DETAIL_FETCH", True),
            ai_enabled=get_bool("SIMPLER_AI_ENRICH_ENABLED", False),
            ai_model=get_str("SIMPLER_AI_MODEL", "gpt-4o-mini") or "gpt-4o-mini",
            ai_max_input_chars=max(2048, get_int("SIMPLER_AI_MAX_INPUT_CHARS", 24_000)),
            keyword="",
            include_extended_search=get_bool("SIMPLER_INCLUDE_EXTENDED_SEARCH", False),
        )


@dataclass(frozen=True)
class BigFutureConfig:
    enabled: bool
    max_records_debug: int
    detail_fetch: bool
    active_only: bool
    fast_prefilter_only: bool
    deep_pass_only: bool
    auto_pipeline: bool
    fast_max_pages: int
    deep_max_items: int
    min_amount_hint: int
    prefilter_store_path: str
    recheck_reject_days: int
    deep_include_review: bool
    ai_enabled: bool
    ai_model: str
    ai_max_input_chars: int
    keyword: str
    headless: bool
    force_http: bool
    timeout_ms: int

    @classmethod
    def load(cls) -> BigFutureConfig:
        return cls(
            enabled=get_bool("BIGFUTURE_ENABLED", False),
            max_records_debug=get_int("BIGFUTURE_MAX_RECORDS_DEBUG", 30),
            detail_fetch=get_bool("BIGFUTURE_DETAIL_FETCH", True),
            active_only=get_bool("BIGFUTURE_ACTIVE_ONLY", True),
            fast_prefilter_only=get_bool("BIGFUTURE_FAST_PREFILTER_ONLY", False),
            deep_pass_only=get_bool("BIGFUTURE_DEEP_PASS_ONLY", False),
            auto_pipeline=get_bool("BIGFUTURE_AUTO_PIPELINE", True),
            fast_max_pages=max(0, get_int("BIGFUTURE_FAST_MAX_PAGES", 0)),
            deep_max_items=max(0, get_int("BIGFUTURE_DEEP_MAX_ITEMS", 0)),
            min_amount_hint=max(0, get_int("BIGFUTURE_MIN_AMOUNT_HINT", 500)),
            prefilter_store_path=get_str("BIGFUTURE_PREFILTER_STORE_PATH", ""),
            recheck_reject_days=max(0, get_int("BIGFUTURE_RECHECK_REJECT_DAYS", 30)),
            deep_include_review=get_bool("BIGFUTURE_DEEP_INCLUDE_REVIEW", True),
            ai_enabled=get_bool("BIGFUTURE_AI_ENRICH_ENABLED", False),
            ai_model=get_str("BIGFUTURE_AI_MODEL", "gpt-4o-mini") or "gpt-4o-mini",
            ai_max_input_chars=max(2048, get_int("BIGFUTURE_AI_MAX_INPUT_CHARS", 24_000)),
            keyword=get_str("BIGFUTURE_KEYWORD", ""),
            headless=get_bool("BIGFUTURE_HEADLESS", True),
            force_http=get_bool("BIGFUTURE_FORCE_HTTP", False),
            timeout_ms=get_int("BIGFUTURE_TIMEOUT_MS", 120_000),
        )


@dataclass(frozen=True)
class ScholarshipsAiFinalConfig:
    """Единый финальный AI-слой перед upsert (все парсеры каталога)."""

    enabled: bool
    model: str
    max_input_chars: int
    write_seo: bool
    write_score_from_model: bool
    write_guidance: bool

    @classmethod
    def load(cls) -> ScholarshipsAiFinalConfig:
        return cls(
            enabled=get_bool("SCHOLARSHIP_AI_FINAL_ENABLED", False),
            model=get_str("SCHOLARSHIP_AI_MODEL", "gpt-4o-mini") or "gpt-4o-mini",
            max_input_chars=max(4096, get_int("SCHOLARSHIP_AI_MAX_INPUT_CHARS", 24_000)),
            write_seo=get_bool("SCHOLARSHIP_AI_WRITE_SEO", True),
            write_score_from_model=get_bool("SCHOLARSHIP_AI_WRITE_SCORE", True),
            write_guidance=get_bool("SCHOLARSHIP_AI_WRITE_GUIDANCE", True),
        )


@lru_cache(maxsize=1)
def get_global_config() -> GlobalConfig:
    return GlobalConfig.load()


@lru_cache(maxsize=1)
def get_scholarship_america_config() -> ScholarshipAmericaConfig:
    return ScholarshipAmericaConfig.load()


@lru_cache(maxsize=1)
def get_simpler_config() -> SimplerConfig:
    return SimplerConfig.load()


@lru_cache(maxsize=1)
def get_bigfuture_config() -> BigFutureConfig:
    return BigFutureConfig.load()


@lru_cache(maxsize=1)
def get_scholarships_ai_final_config() -> ScholarshipsAiFinalConfig:
    return ScholarshipsAiFinalConfig.load()


def source_enabled(canonical_key: str) -> bool:
    """Флаг ENABLED для канонического ключа (решает run_all, не парсер)."""
    if canonical_key == "scholarship_america":
        return get_scholarship_america_config().enabled
    if canonical_key == "simpler_grants_gov":
        return get_simpler_config().enabled
    if canonical_key == "bigfuture":
        return get_bigfuture_config().enabled
    return True


def print_env_by_prefix(prefix: str) -> None:
    """Отладка: переменные окружения, чьи имена начинаются с prefix (без учёта регистра)."""
    p = prefix.strip()
    if not p:
        return
    pl = p.lower()
    print(f"--- env keys starting with {prefix!r} ---")
    for k in sorted(os.environ.keys()):
        if k.lower().startswith(pl):
            print(f"  {k}")
    print("--- end ---")


def print_parser_config_summary(resolved_keys: list[str] | None = None) -> None:
    """Читаемая сводка: режим, таблица источников, global, лимиты, AI, секреты."""
    g = get_global_config()
    keys = resolved_keys if resolved_keys is not None else g.resolved_source_keys()
    sa = get_scholarship_america_config()
    sm = get_simpler_config()
    bf = get_bigfuture_config()
    ai_final = get_scholarships_ai_final_config()

    def onoff(x: bool) -> str:
        return "ВКЛ" if x else "ОТКЛ"

    print("")
    print("╔══════════════════════════════════════════════════════════════════╗")
    print("║                     PARSER CONFIG SUMMARY                        ║")
    print("╠══════════════════════════════════════════════════════════════════╣")
    print(f"║ PARSER_SOURCES raw: {g.parser_sources_raw!r}")
    print(f"║ resolved keys:      {keys!r}")
    print(
        "║ aliases: simpler→simpler_grants_gov, sa|scholarship→scholarship_america, bf→bigfuture; "
        "пакеты: sources.<canonical_key>/"
    )
    print("╠══════════════════════════════════════════════════════════════════╣")
    print("║ Источник              │ в списке │ ENABLED │ запуск?              ║")
    print("╠════════════════════════╪══════════╪═════════╪══════════════════════╣")
    for ck in CANONICAL_SOURCE_KEYS:
        in_list = "да" if ck in keys else "нет"
        en = source_enabled(ck)
        will = (ck in keys) and en
        print(
            f"║ {ck:22}│ {in_list:^8} │ {onoff(en):^7} │ "
            f"{'ДА' if will else 'ПРОПУСК':^20} ║"
        )
    print("╠══════════════════════════════════════════════════════════════════╣")
    print("║ GLOBAL RUN                                                       ║")
    print(f"║  TARGET_NEW_ITEMS={g.target_new_items}  MAX_LIST_PAGES={g.max_list_pages}  NO_NEW_PAGES_STOP={g.no_new_pages_stop}")
    print(f"║  SKIP_EXISTING_ON_LIST={g.skip_existing_on_list}  USE_TITLE_FALLBACK_KNOWN={g.use_title_fallback_known}")
    print(f"║  DISCOVERY_MODE={g.discovery_mode!r}")
    print("╠══════════════════════════════════════════════════════════════════╣")
    print("║ SCHOLARSHIP AMERICA                                              ║")
    print(
        f"║  MAX_RECORDS_DEBUG={sa.max_records_debug} (0=min(TARGET only))  DETAIL_FETCH={sa.detail_fetch}  AI={sa.ai_enabled}"
    )
    print("╠══════════════════════════════════════════════════════════════════╣")
    print("║ SIMPLER (grants.gov)                                             ║")
    print(
        f"║  MAX_RECORDS_DEBUG={sm.max_records_debug} (0=no cap)  DETAIL_FETCH={sm.detail_fetch}  EXT_SEARCH={sm.include_extended_search}"
    )
    print(
        f"║  AI_ENRICH={sm.ai_enabled}  MODEL={sm.ai_model!r}  AI_MAX_INPUT={sm.ai_max_input_chars}"
    )
    print("╠══════════════════════════════════════════════════════════════════╣")
    print("║ BIGFUTURE                                                        ║")
    print(
        f"║  AUTO_PIPELINE={bf.auto_pipeline}  FAST_MAX_PAGES={bf.fast_max_pages} (0=use MAX_LIST_PAGES)  "
        f"DEEP_MAX_ITEMS={bf.deep_max_items} (0=no extra cap)"
    )
    print(
        f"║  MAX_RECORDS_DEBUG={bf.max_records_debug} (0=no cap)  DETAIL_FETCH={bf.detail_fetch}  "
        f"ACTIVE_ONLY={bf.active_only}  HEADLESS={bf.headless}  FORCE_HTTP={bf.force_http}"
    )
    print(
        f"║  FAST_PREFILTER_ONLY={bf.fast_prefilter_only}  DEEP_PASS_ONLY={bf.deep_pass_only}  "
        f"MIN_AMOUNT_HINT={bf.min_amount_hint}  RECHECK_REJECT_DAYS={bf.recheck_reject_days}"
    )
    print(f"║  TIMEOUT_MS={bf.timeout_ms}  KEYWORD={'set' if bf.keyword else 'empty'}")
    print(
        f"║  AI_ENRICH={bf.ai_enabled}  MODEL={bf.ai_model!r}  AI_MAX_INPUT={bf.ai_max_input_chars}"
    )
    print("╠══════════════════════════════════════════════════════════════════╣")
    print("║ SCHOLARSHIP AI FINAL (все источники, перед upsert)               ║")
    print(
        f"║  ENABLED={onoff(ai_final.enabled)}  MODEL={ai_final.model!r}  "
        f"MAX_INPUT={ai_final.max_input_chars}"
    )
    print(
        f"║  WRITE_SEO={ai_final.write_seo}  WRITE_SCORE={ai_final.write_score_from_model}  "
        f"WRITE_GUIDANCE={ai_final.write_guidance}"
    )
    print("╠══════════════════════════════════════════════════════════════════╣")
    print("║ SECRETS (только факт наличия)                                    ║")
    print(f"║  SUPABASE_URL:           {bool(get_str('SUPABASE_URL'))}")
    print(f"║  SUPABASE_SERVICE_ROLE:  {bool(get_str('SUPABASE_SERVICE_ROLE_KEY'))}")
    print(f"║  OPENAI_API_KEY:         {bool(get_str('OPENAI_API_KEY'))}")
    print("╚══════════════════════════════════════════════════════════════════╝")
    print("")
