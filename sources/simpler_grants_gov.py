"""
Парсер Simpler.Grants.gov (HTML) → public.scholarships (Supabase).

Листинг: GET /search с фильтрами как у публичного UI (см. HHS/simpler-grants-gov
frontend searchQueryTypes / searchFilterTypes): status, fundingInstrument, eligibility,
category, closeDate, page; опционально q — доп. ключевые слова (SEARCH_QUERIES).
Деталь: GET страницы opportunity.
Student-relevant фильтр: passes_student_relevance_filter (title + snippet + full_text).

SAVE paths (только этот источник):
  A — matched_student_levels + positive_kw (scholarship/tuition/financial_aid/fellowship).
  B — (scholarship|fellowship) + eligibility_kw.
  C — (student_level ИЛИ eligibility: student/undergraduate/graduate/individual/high_school)
      и нет «жёсткого» gov/capital/research-блока; soft (nonprofit, IHE) не режет при C.
  Иначе skip (institutional только без strong edu, или skip_no_path).

Без Grants API и без API-ключа.

Дополнительно:
  SIMPLER_INCLUDE_EXTENDED_SEARCH=1 — добавить к поиску: undergraduate, graduate, college, fellowship
  SIMPLER_MAX_RECORDS_DEBUG — потолок успешных upsert для этого источника (по умолчанию 30).
    Установите 0 для снятия лимита и полного прогона.

  Опциональное AI-обогащение (не подключено к run(); см. ai_enrich_simpler_grant):
  SIMPLER_AI_ENRICH_ENABLED, SIMPLER_AI_MODEL, SIMPLER_AI_MAX_INPUT_CHARS; ключ API — OPENAI_API_KEY.

Остальные параметры — как у Scholarship America: TARGET_NEW_ITEMS, MAX_LIST_PAGES,
NO_NEW_PAGES_STOP, SKIP_EXISTING_ON_LIST, DISCOVERY_MODE, USE_TITLE_FALLBACK_KNOWN.
"""

from __future__ import annotations

import copy
import json
import os
import re
import sys
import time
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

_PARSER_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PARSER_ROOT not in sys.path:
    sys.path.insert(0, _PARSER_ROOT)

from business_filters import (
    MIN_LEAD_DAYS_BEFORE_DEADLINE,
    classify_business_deadline,
    has_meaningful_funding,
)
from normalize_scholarship import apply_normalization
from scholarship_db_columns import SCHOLARSHIP_UPSERT_BODY_KEYS
from sources.scholarship_america import parse_award_min_max, parse_deadline_date
from utils import (
    KnownScholarshipIndex,
    get_client,
    listing_is_known,
    load_known_scholarship_index,
    upsert_scholarship,
)

SCHOLARSHIP_TABLE_KEYS: tuple[str, ...] = SCHOLARSHIP_UPSERT_BODY_KEYS

SOURCE = "simpler_grants_gov"
DEFAULT_CURRENCY = "USD"
LIST_URL = "https://simpler.grants.gov/search"
SITE_ORIGIN = "https://simpler.grants.gov"

SEARCH_QUERIES_BASE: tuple[str, ...] = ("scholarship", "student")
SEARCH_QUERIES_EXTENDED: tuple[str, ...] = (
    "undergraduate",
    "graduate",
    "college",
    "fellowship",
)

# Параметры строки запроса листинга — как у simpler.grants.gov (camelCase в URL).
# Значения — как в frontend/src/constants/searchFilterOptions.ts (не сырой текст UI).
# «Open» в интерфейсе = posted; закрытие «Next 90 days» = closeDate=90.
_LIST_FILTER_PARAMS: dict[str, str] = {
    "status": "posted",
    "fundingInstrument": "grant",
    "eligibility": "individuals",
    "category": "education",
    "closeDate": "90",
}
# SPA в адресной строке использует query=…; для SSR-карточек в HTML надёжно работает q=.
_LIST_KEYWORD_QUERY_PARAM = "q"

_HTML_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# --- HTML extraction helpers (не подключены к fetch/parse; для последующего использования) ---

_UNWANTED_TAGS: tuple[str, ...] = (
    "script",
    "style",
    "noscript",
    "iframe",
    "object",
    "embed",
    "template",
)


def _decompose_unwanted(root: Any) -> None:
    """Удаляет из поддерева BeautifulSoup теги, не нужные для безопасного текста/HTML."""
    if root is None or not hasattr(root, "find_all"):
        return
    for name in _UNWANTED_TAGS:
        for el in list(root.find_all(name)):
            el.decompose()


def _absolutize_html_fragment(fragment: str | None, base_url: str) -> str:
    """
    Парсит HTML-фрагмент и делает относительные href/src абсолютными относительно base_url.
    Возвращает сериализованную строку (пустую, если фрагмент пустой).
    """
    if not fragment or not str(fragment).strip():
        return ""
    base = (base_url or "").strip()
    if not base:
        return str(fragment)
    soup = BeautifulSoup(str(fragment), "html.parser")
    for el in soup.find_all(True):
        if not getattr(el, "name", None):
            continue
        href = el.get("href")
        if href:
            hs = str(href).strip()
            hl = hs.lower()
            if not hl.startswith(
                ("http://", "https://", "mailto:", "tel:", "javascript:", "#")
            ):
                el["href"] = urljoin(base, hs)
        src = el.get("src")
        if src:
            ss = str(src).strip()
            if not ss.lower().startswith(("http://", "https://", "data:")):
                el["src"] = urljoin(base, ss)
    return str(soup)


def _safe_inner_html_from_tag(tag: Any) -> str:
    """Внутренний HTML узла bs4.Tag; для прочих типов — пустая строка."""
    if tag is None or not isinstance(tag, Tag):
        return ""
    try:
        return tag.decode_contents()
    except (AttributeError, TypeError):
        return ""


def extract_full_content_html_from_simpler(
    soup: BeautifulSoup,
    page_url: str,
) -> str | None:
    """
    main → article → body: копия без chrome, gov-баннеров и нежелательных тегов;
    абсолютные ссылки. Пусто → None (не '').
    """
    root = soup.find("main") or soup.find("article") or soup.find("body")
    if not root:
        return None

    clone = BeautifulSoup(str(root), "html.parser")

    for tag in list(clone.find_all(["nav", "header", "footer"])):
        tag.decompose()

    for sel in (".usa-banner", ".gov-banner"):
        for el in list(clone.select(sel)):
            el.decompose()

    _decompose_unwanted(clone)

    html = clone.decode_contents().strip()
    if not html:
        return None

    out = _absolutize_html_fragment(html, page_url)
    return out or None


_SUPPORT_CONTACT_PHONE_RES: tuple[re.Pattern[str], ...] = (
    re.compile(r"\+1\s*\(\s*\d{3}\s*\)\s*\d{3}\s*[-.\s]?\s*\d{4}"),
    re.compile(r"\+1\s*\d{3}\s*[-.\s]?\s*\d{3}\s*[-.\s]?\s*\d{4}"),
    re.compile(r"\(\s*\d{3}\s*\)\s*\d{3}\s*[-.\s]?\s*\d{4}"),
    re.compile(r"\b\d{3}\s*-\s*\d{3}\s*-\s*\d{4}\b"),
    re.compile(r"\b\d{10}\b"),
)


def extract_support_contacts_from_simpler(
    soup: BeautifulSoup,
) -> tuple[str | None, str | None]:
    """
    Первый mailto: и первый tel: внутри main/article/body; телефон иначе — regex по тексту того же блока.
    """
    from urllib.parse import unquote as _unquote

    root = soup.find("main") or soup.find("article") or soup.find("body")
    if not root:
        return None, None

    email: str | None = None
    for a in root.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        low = href.lower()
        if not low.startswith("mailto:"):
            continue
        raw = href[7:].split("?")[0].split("#")[0].strip()
        if not raw:
            continue
        cand = _unquote(raw).strip()
        if "@" in cand:
            email = cand
            break

    phone: str | None = None
    for a in root.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        low = href.lower()
        if not low.startswith("tel:"):
            continue
        raw = href[4:].split("?")[0].split("#")[0].strip()
        if raw:
            phone = re.sub(r"\s+", " ", _unquote(raw)).strip()
            break

    if phone is None:
        blob = (root.get_text(" ", strip=True) or "").strip()
        for pat in _SUPPORT_CONTACT_PHONE_RES:
            m = pat.search(blob)
            if m:
                phone = m.group(0).strip()
                break

    return email, phone


# --- Фильтрация релевантности (blob = title + snippet + detail full_text) ---

_STUDENT_LEVEL_SPECS: list[tuple[str, re.Pattern[str]]] = [
    ("high_school", re.compile(r"\bhigh\s+school\b", re.I)),
    ("secondary_school", re.compile(r"\bsecondary\s+school\b", re.I)),
    ("freshman", re.compile(r"\bfreshman\b", re.I)),
    ("sophomore", re.compile(r"\bsophomore\b", re.I)),
    ("junior", re.compile(r"\bjunior\b", re.I)),
    ("grade_9_12", re.compile(r"\b(?:12th|11th|10th|9th)\s+grade\b", re.I)),
    ("college_student", re.compile(r"\bcollege\s+student\b", re.I)),
    (
        "year_student_undergrad",
        re.compile(
            r"\b(?:first|second|third|fourth)[-\s]year\s+(?:student|undergraduate)\b",
            re.I,
        ),
    ),
    (
        "nth_year_college",
        re.compile(r"\b(?:1st|2nd|3rd|4th)\s+year\s+(?:student|college)\b", re.I),
    ),
    ("undergraduate", re.compile(r"\bundergraduate\b", re.I)),
    ("graduate_student", re.compile(r"\bgraduate\s+student\b", re.I)),
    ("masters_student", re.compile(r"\bmaster'?s\s+student\b", re.I)),
    ("doctoral_student", re.compile(r"\bdoctoral\s+student\b", re.I)),
    ("phd_student", re.compile(r"\bph\.?d\.?\s+student\b", re.I)),
    ("adult_learner", re.compile(r"\badult\s+learner\b", re.I)),
    ("non_traditional", re.compile(r"\bnon[-\s]?traditional\b", re.I)),
]

_POSITIVE_SPECS: list[tuple[str, re.Pattern[str]]] = [
    ("scholarship", re.compile(r"\bscholarships?\b", re.I)),
    ("fellowship", re.compile(r"\bfellowships?\b", re.I)),
    ("tuition", re.compile(r"\btuition\b", re.I)),
    ("financial_aid", re.compile(r"\bfinancial\s+aid\b", re.I)),
]

_ELIGIBILITY_SPECS: list[tuple[str, re.Pattern[str]]] = [
    ("student", re.compile(r"\bstudents?\b", re.I)),
    ("individual", re.compile(r"\bindividuals?\b", re.I)),
    ("undergraduate", re.compile(r"\bundergraduate\b", re.I)),
    ("graduate", re.compile(r"\bgraduate\b", re.I)),
]

# Для пути C: те же eligibility + явный high school в тексте (не только student-level spec).
_PATH_C_EXTRA_ELIGIBILITY: list[tuple[str, re.Pattern[str]]] = [
    ("high_school", re.compile(r"\bhigh\s+school\b", re.I)),
]

_HARD_INSTITUTIONAL_PHRASES: tuple[str, ...] = (
    "state government",
    "county government",
    "city government",
    "small business",
)
_SOFT_INSTITUTIONAL_PHRASES: tuple[str, ...] = (
    "institution of higher education",
    "nonprofit",
    "non-profit",
)

_INSTITUTIONAL_PHRASES: tuple[str, ...] = (
    "state government",
    "county government",
    "city government",
    "small business",
    "institution of higher education",
    "nonprofit",
    "non-profit",
)
_RESEARCH_NEGATIVE_PHRASES: tuple[str, ...] = (
    "research infrastructure",
    "research equipment",
    "research institution",
    "institutional research",
    "research capacity building",
)
_OTHER_INSTITUTIONAL_WORDS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("infrastructure", re.compile(r"\binfrastructure\b", re.I)),
    ("equipment", re.compile(r"\bequipment\b", re.I)),
    ("construction", re.compile(r"\bconstruction\b", re.I)),
)

_DEADLINE_LABEL_RE = re.compile(
    r"(?:application\s+)?deadline|closing\s+date|close\s+date|due\s+date|application\s+close",
    re.I,
)

_PLAIN_DEADLINE_RES: tuple[re.Pattern[str], ...] = (
    re.compile(
        r"(?i)(?:application\s+)?deadline\s*[:\-–]\s*"
        r"([A-Za-z]{3,9}\.?\s+\d{1,2},?\s+\d{4})",
    ),
    re.compile(
        r"(?i)closing\s+date\s*[:\-–]\s*([A-Za-z]{3,9}\.?\s+\d{1,2},?\s+\d{4})",
    ),
    re.compile(
        r"(?i)close\s+date\s*[:\-–]\s*([A-Za-z]{3,9}\.?\s+\d{1,2},?\s+\d{4})",
    ),
    re.compile(
        r"(?i)due\s+date\s*[:\-–]\s*([A-Za-z]{3,9}\.?\s+\d{1,2},?\s+\d{4})",
    ),
    re.compile(
        r"(?i)closing\s*[:\-–]\s*([A-Za-z]{3,9}\.?\s+\d{1,2},?\s+\d{4})",
    ),
    re.compile(
        r"(?i)closing\s*[:\-–]\s*(\d{1,2}/\d{1,2}/\d{4})",
    ),
    re.compile(
        r"(?i)closing\s*[:\-–]\s*(\d{4}-\d{2}-\d{2})",
    ),
    re.compile(
        r"(?i)(?:application\s+)?deadline\s*[:\-–]\s*(\d{1,2}/\d{1,2}/\d{4})",
    ),
    re.compile(
        r"(?i)(?:application\s+)?deadline\s*[:\-–]\s*(\d{4}-\d{2}-\d{2})",
    ),
    re.compile(
        r"(?i)\b(\d{1,2}/\d{1,2}/\d{4})\b(?=[^.]{0,40}(?:deadline|closing|due\s+date))",
    ),
)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


TARGET_NEW_ITEMS = _env_int("TARGET_NEW_ITEMS", 50)
MAX_LIST_PAGES = _env_int("MAX_LIST_PAGES", 1000)
NO_NEW_PAGES_STOP = _env_int("NO_NEW_PAGES_STOP", 50)
SKIP_EXISTING_ON_LIST = _env_bool("SKIP_EXISTING_ON_LIST", True)
USE_TITLE_FALLBACK_KNOWN = _env_bool("USE_TITLE_FALLBACK_KNOWN", False)
DISCOVERY_MODE = (os.environ.get("DISCOVERY_MODE") or "new_only").strip().lower() or "new_only"

# Отладочный потолок успешных upsert только для этого источника.
# SIMPLER_MAX_RECORDS_DEBUG=0 — без лимита (полный прогон).
MAX_SIMPLER_GRANTS_GOV_RECORDS_DEBUG = _env_int("SIMPLER_MAX_RECORDS_DEBUG", 30)


def simpler_ai_enrich_enabled() -> bool:
    """SIMPLER_AI_ENRICH_ENABLED=1|true|yes|on — разрешить вызовы ai_enrich_simpler_grant."""
    return _env_bool("SIMPLER_AI_ENRICH_ENABLED", False)


def simpler_ai_model() -> str:
    """SIMPLER_AI_MODEL (по умолчанию gpt-4o-mini)."""
    return (os.environ.get("SIMPLER_AI_MODEL") or "gpt-4o-mini").strip()


def simpler_ai_max_input_chars() -> int:
    """SIMPLER_AI_MAX_INPUT_CHARS — лимит символов на вход модели (по умолчанию 24000, минимум 2048)."""
    return max(2048, _env_int("SIMPLER_AI_MAX_INPUT_CHARS", 24_000))


def _empty_simpler_ai_enrich() -> dict[str, Any]:
    return {
        "short_summary": None,
        "eligibility_list": [],
        "key_requirements": [],
        "required_documents": [],
        "funding_amount_text": None,
        "deadline_text": None,
        "payout_method": None,
        "provider_name": None,
        "student_relevance": None,
        "confidence_score": None,
    }


_SIMPLER_AI_SYSTEM_PROMPT = """You are an analyst extracting structured facts from a U.S. federal grant \
or assistance opportunity record (Simpler.Grants.gov style). Use only the JSON excerpt provided; do not \
invent agencies, amounts, or deadlines not supported by the text. If unknown, use null for strings, \
empty arrays for lists, or null for numbers.

Return a single JSON object with exactly these keys:
- short_summary (string|null): 1–3 sentences, plain language.
- eligibility_list (array of strings): who may apply; empty array if unclear.
- key_requirements (array of strings): main application steps or criteria; empty if unclear.
- required_documents (array of strings): explicit document types mentioned; empty if none stated.
- funding_amount_text (string|null): human-readable award range or amount if stated.
- deadline_text (string|null): closing / due date text if stated.
- payout_method (string|null): how funds are delivered if inferable (e.g. reimbursement, direct to institution); else null.
- provider_name (string|null): awarding agency or organization name if clear.
- student_relevance (string|null): one of high, medium, low, none — how relevant this is to individual \
students (undergrad/grad), based only on the excerpt.
- confidence_score (number|null): 0.0–1.0 reflecting how well the excerpt supports your extractions; \
null if not applicable.

Output valid JSON only, no markdown."""


def build_ai_input_payload_for_simpler(record: dict[str, Any] | None) -> dict[str, Any]:
    """
    Собирает усечённый текстовый срез записи scholarship (после build_full_record / нормализации)
    для передачи в модель. Не вызывает сеть.
    """
    max_c = simpler_ai_max_input_chars()
    r = dict(record or {})
    order = (
        "title",
        "url",
        "source",
        "source_id",
        "provider_name",
        "award_amount_text",
        "deadline_text",
        "description",
        "eligibility_text",
        "requirements_text",
        "awards_text",
        "winner_payment_text",
        "category",
        "apply_url",
        "institutions_text",
        "state_territory_text",
    )
    excerpt: dict[str, str] = {}
    used = 0
    overhead_per_field = 12
    for key in order:
        val = r.get(key)
        if val is None:
            continue
        s = str(val).strip()
        if not s:
            continue
        budget = max_c - used - overhead_per_field - len(key)
        if budget < 80:
            break
        if len(s) > budget:
            s = s[: max(1, budget - 1)] + "…"
        excerpt[key] = s
        used += len(s) + overhead_per_field + len(key)
        if used >= max_c:
            break
    return {
        "source_parser": SOURCE,
        "purpose": "simpler_grants_gov_ai_enrich",
        "record_excerpt": excerpt,
    }


def _coerce_str_list(val: Any) -> list[str] | None:
    if val is None:
        return None
    if isinstance(val, list):
        out = [str(x).strip() for x in val if str(x).strip()]
        return out
    if isinstance(val, str) and val.strip():
        return [val.strip()]
    return None


def _normalize_simpler_ai_enrich_parsed(raw: dict[str, Any] | None) -> dict[str, Any]:
    base = _empty_simpler_ai_enrich()
    if not raw:
        return base
    base["short_summary"] = (
        str(raw["short_summary"]).strip() if raw.get("short_summary") is not None else None
    ) or None
    for lst_key in ("eligibility_list", "key_requirements", "required_documents"):
        coerced = _coerce_str_list(raw.get(lst_key))
        base[lst_key] = coerced if coerced is not None else []
    for sk in ("funding_amount_text", "deadline_text", "payout_method", "provider_name"):
        v = raw.get(sk)
        base[sk] = str(v).strip() if v is not None and str(v).strip() else None
    sr = raw.get("student_relevance")
    if sr is not None and str(sr).strip():
        base["student_relevance"] = str(sr).strip().lower()
    else:
        base["student_relevance"] = None
    cs = raw.get("confidence_score")
    if cs is None:
        base["confidence_score"] = None
    else:
        try:
            f = float(cs)
            if f != f:  # NaN
                base["confidence_score"] = None
            else:
                base["confidence_score"] = max(0.0, min(1.0, f))
        except (TypeError, ValueError):
            base["confidence_score"] = None
    return base


def ai_enrich_simpler_grant(record: dict[str, Any] | None) -> dict[str, Any]:
    """
    Опциональное обогащение через OpenAI (тот же OPENAI_API_KEY, что и в scripts Node).

    Если SIMPLER_AI_ENRICH_ENABLED выключен — без сети, возвращает dict с теми же ключами и null.
    Ошибки API: без исключения наружу, тот же «пустой» результат по ключам.
    """
    empty = _empty_simpler_ai_enrich()
    if not simpler_ai_enrich_enabled():
        return empty

    api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return empty

    try:
        from openai import OpenAI
    except ImportError:
        return empty

    user_json = json.dumps(
        build_ai_input_payload_for_simpler(record),
        ensure_ascii=False,
    )
    user_prompt = (
        "Extract structured fields from this opportunity record excerpt (JSON). "
        "Respond with one JSON object using only the schema from the system message.\n\n"
        f"{user_json}"
    )

    try:
        client = OpenAI(api_key=api_key)
        completion = client.chat.completions.create(
            model=simpler_ai_model(),
            temperature=0.2,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": _SIMPLER_AI_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
        )
        text = (completion.choices[0].message.content or "").strip()
        if not text:
            return empty
        parsed = json.loads(text)
        if not isinstance(parsed, dict):
            return empty
        return _normalize_simpler_ai_enrich_parsed(parsed)
    except Exception:
        return empty


# Пороги «слабого» текста (согласованы с parse_detail_from_html: описание < 24 — fallback).
_MERGE_AI_MIN_DESCRIPTION_LEN = 24
_MERGE_AI_MIN_ELIGIBILITY_LEN = 20
_MERGE_AI_MIN_REQUIREMENTS_LEN = 40


def _record_field_str(record: dict[str, Any], key: str) -> str:
    v = record.get(key)
    if v is None:
        return ""
    return str(v).strip()


def _is_description_weak_for_ai_merge(text: str | None) -> bool:
    t = (text or "").strip()
    if not t:
        return True
    return len(t) < _MERGE_AI_MIN_DESCRIPTION_LEN


def _is_eligibility_garbage_for_ai_merge(text: str | None) -> bool:
    t = strip_us_gov_boilerplate((text or "").strip())
    if not t:
        return True
    return len(t) < _MERGE_AI_MIN_ELIGIBILITY_LEN


def _is_requirements_weak_for_ai_merge(text: str | None) -> bool:
    cleaned = strip_requirements_fluff((text or "").strip() or None)
    if not cleaned:
        return True
    return len(cleaned.strip()) < _MERGE_AI_MIN_REQUIREMENTS_LEN


def _ai_join_lines(items: Any) -> str | None:
    lines = _coerce_str_list(items)
    if not lines:
        return None
    body = "\n".join(lines)
    return body if body.strip() else None


def _ensure_mutable_raw_data(record: dict[str, Any]) -> dict[str, Any]:
    rd = record.get("raw_data")
    if isinstance(rd, dict):
        return copy.deepcopy(rd)
    if isinstance(rd, str) and rd.strip():
        try:
            parsed = json.loads(rd)
            if isinstance(parsed, dict):
                return copy.deepcopy(parsed)
        except json.JSONDecodeError:
            pass
    return {}


def merge_ai_enrichment_into_record(
    record: dict[str, Any] | None,
    ai_data: dict[str, Any] | None,
) -> dict[str, Any]:
    """
    Аккуратно вливает поля из ai_enrich_simpler_grant в запись, не затирая сильные ручные значения.

    Всегда кладёт полный объект ai_data в record[\"raw_data\"][\"ai_enrichment\"] (JSON-safe копия).
    Не вызывает run/build_full_record/parse_detail.
    """
    out = dict(record or {})
    rd = _ensure_mutable_raw_data(out)
    if isinstance(ai_data, dict):
        rd["ai_enrichment"] = _json_safe(copy.deepcopy(ai_data))
    else:
        rd["ai_enrichment"] = _json_safe(ai_data)
    out["raw_data"] = rd

    ai = (
        _normalize_simpler_ai_enrich_parsed(ai_data)
        if isinstance(ai_data, dict)
        else _empty_simpler_ai_enrich()
    )

    if _is_description_weak_for_ai_merge(_record_field_str(out, "description")):
        summ = (ai.get("short_summary") or "").strip() if ai.get("short_summary") else ""
        if summ:
            out["description"] = summ

    if _is_eligibility_garbage_for_ai_merge(_record_field_str(out, "eligibility_text")):
        joined = _ai_join_lines(ai.get("eligibility_list"))
        if joined:
            out["eligibility_text"] = joined

    if _is_requirements_weak_for_ai_merge(_record_field_str(out, "requirements_text")):
        joined = _ai_join_lines(ai.get("key_requirements"))
        if joined:
            out["requirements_text"] = joined

    if not _record_field_str(out, "award_amount_text"):
        famt = (ai.get("funding_amount_text") or "").strip() if ai.get("funding_amount_text") else ""
        if famt:
            out["award_amount_text"] = famt

    if not _record_field_str(out, "deadline_text"):
        dlt = (ai.get("deadline_text") or "").strip() if ai.get("deadline_text") else ""
        if dlt:
            out["deadline_text"] = dlt

    if not _record_field_str(out, "winner_payment_text"):
        pay = (ai.get("payout_method") or "").strip() if ai.get("payout_method") else ""
        if pay:
            out["winner_payment_text"] = pay

    if not _record_field_str(out, "provider_name"):
        pn = (ai.get("provider_name") or "").strip() if ai.get("provider_name") else ""
        if pn:
            out["provider_name"] = pn

    return out


def ai_enrich_simpler_record_if_enabled(record: dict[str, Any]) -> dict[str, Any]:
    """
    Точка входа в pipeline: при включённом SIMPLER_AI_ENRICH_ENABLED — вызов модели и merge в запись.
    Ошибки не пробрасываются; исходная запись возвращается с опциональным raw_data[\"ai_enrichment_error\"].
    """
    if not simpler_ai_enrich_enabled():
        print("[SIMPLER AI] enrich skipped (disabled)")
        return record

    try:
        print("[SIMPLER AI] enrich start")
        ai_data = ai_enrich_simpler_grant(record)
        merged = merge_ai_enrichment_into_record(record, ai_data)
        print("[SIMPLER AI] enrich success")
        return merged
    except Exception as e:
        err_msg = f"{type(e).__name__}: {e}"
        print(f"[SIMPLER AI] enrich failed: {err_msg}")
        try:
            fallback = dict(record)
            rd = _ensure_mutable_raw_data(fallback)
            rd["ai_enrichment_error"] = _json_safe(err_msg)
            fallback["raw_data"] = rd
            return fallback
        except Exception:
            return record


_GOV_LINE_DROP_RES: tuple[re.Pattern[str], ...] = (
    re.compile(r"^skip to main content\.?$", re.I),
    re.compile(r"^menu$", re.I),
    re.compile(r"^close$", re.I),
    re.compile(
        r"^an official website of the united states government\.?$",
        re.I,
    ),
    re.compile(r"^here'?s how you know\.?$", re.I),
    re.compile(r"^official websites use \.gov\.?$", re.I),
    re.compile(r"^secure \.gov websites use https\.?$", re.I),
)
_GOV_SUBSTRINGS_REMOVE: tuple[str, ...] = (
    "skip to main content",
    "an official website of the united states government",
    "here's how you know",
    "heres how you know",
)
_REQUIREMENTS_FLUFF_RES: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bsee the official page for full details\.?", re.I),
    re.compile(r"\bcheck the official application for the final list\.?", re.I),
    re.compile(r"\bsee the official listing\.?", re.I),
    re.compile(r"\brefer to the official opportunity page\.?", re.I),
)


def strip_us_gov_boilerplate(text: str | None) -> str:
    text = (text or "").strip()
    if not text:
        return ""
    lines: list[str] = []
    for line in str(text).replace("\r", "").split("\n"):
        t = line.strip()
        if not t:
            continue
        if any(p.match(t) for p in _GOV_LINE_DROP_RES):
            continue
        tl = t.lower()
        if any(s in tl for s in _GOV_SUBSTRINGS_REMOVE):
            continue
        lines.append(t)
    out = " ".join(lines)
    for s in _GOV_SUBSTRINGS_REMOVE:
        out = re.sub(re.escape(s), " ", out, flags=re.I)
    return " ".join(out.split())


def strip_requirements_fluff(text: str | None) -> str | None:
    t0 = (text or "").strip()
    if not t0:
        return None
    t = t0
    for pat in _REQUIREMENTS_FLUFF_RES:
        t = pat.sub(" ", t)
    t = " ".join(t.split()).strip()
    return t or None


def _heading_bucket(name: str | None) -> str | None:
    if not name:
        return None
    t = re.sub(r"\s+", " ", name).strip().lower()
    if len(t) > 96:
        t = t[:96]
    if re.search(r"\b(synopsis|description|summary)\b", t):
        return "synopsis"
    if "eligibility" in t or "eligible" in t:
        return "eligibility"
    if re.search(
        r"\b(award|funding|estimated|program funding|cost sharing|payment)\b",
        t,
    ):
        return "award_payment"
    if re.search(r"\b(requirement|document|materials)\b", t):
        return "requirements"
    if "application" in t or "apply" in t:
        return "application"
    return None


def _extract_sections_from_main(main: Any) -> dict[str, list[str]]:
    buckets: dict[str, list[str]] = {
        "synopsis": [],
        "eligibility": [],
        "award_payment": [],
        "requirements": [],
        "application": [],
        "other": [],
    }
    cur = "other"
    for el in main.find_all(True):
        if not getattr(el, "name", None):
            continue
        if el.name in ("h1", "h2", "h3", "h4"):
            lab = (el.get_text(" ", strip=True) or "").strip()
            nb = _heading_bucket(lab)
            if nb:
                cur = nb
            continue
        if el.name == "p":
            if el.parent and getattr(el.parent, "name", None) == "li":
                continue
            txt = (el.get_text(" ", strip=True) or "").strip()
            if len(txt) >= 3:
                buckets[cur].append(txt)
            continue
        if el.name == "li":
            if el.find("p"):
                continue
            txt = (el.get_text(" ", strip=True) or "").strip()
            if len(txt) >= 3:
                buckets[cur].append(txt)
    return buckets


def _extract_sections_from_main_with_html(
    main: Any,
    page_url: str,
) -> dict[str, dict[str, list[str]]]:
    """
    Секции как в _extract_sections_from_main, плюс параллельные html-фрагменты (str(el)).
    page_url зарезервирован под последующий absolutize по фрагментам.
    """
    _ = page_url

    keys = (
        "synopsis",
        "eligibility",
        "award_payment",
        "requirements",
        "application",
        "other",
    )
    buckets: dict[str, dict[str, list[str]]] = {
        k: {"text": [], "html": []} for k in keys
    }
    cur = "other"
    for el in main.find_all(True):
        if not getattr(el, "name", None):
            continue
        if el.name in ("h1", "h2", "h3", "h4"):
            lab = (el.get_text(" ", strip=True) or "").strip()
            nb = _heading_bucket(lab)
            if nb:
                cur = nb
            continue
        if el.name == "p":
            if el.parent and getattr(el.parent, "name", None) == "li":
                continue
            txt = (el.get_text(" ", strip=True) or "").strip()
            if len(txt) < 3:
                continue
            buckets[cur]["text"].append(txt)
            buckets[cur]["html"].append(str(el))
            continue
        if el.name == "li":
            if el.find("p"):
                continue
            txt = (el.get_text(" ", strip=True) or "").strip()
            if len(txt) < 3:
                continue
            buckets[cur]["text"].append(txt)
            buckets[cur]["html"].append(str(el))
    return buckets


def _split_payment_award(parts: list[str]) -> tuple[str | None, str | None]:
    pay_bits: list[str] = []
    award_bits: list[str] = []
    pay_re = re.compile(
        r"cost sharing|matching|reimburs|paid to|payment|payout|recipient",
        re.I,
    )
    award_re = re.compile(
        r"\$|usd|award|ceiling|floor|funding|total program|expected number of award",
        re.I,
    )
    for p in parts:
        ps = (p or "").strip()
        if not ps:
            continue
        if pay_re.search(ps):
            pay_bits.append(ps)
        elif award_re.search(ps):
            award_bits.append(ps)
        else:
            award_bits.append(ps)
    aw = " ".join(award_bits).strip() or None
    pw = " ".join(pay_bits).strip() or None
    return aw, pw


def _first_apply_href(soup: BeautifulSoup, page_url: str) -> str | None:
    for a in soup.select('a[href*="grants.gov"], a[href*="apply"]'):
        href = (a.get("href") or "").strip()
        if not href or href.startswith("#"):
            continue
        full = urljoin(page_url, href)
        if full.lower().startswith(("http://", "https://")):
            return full
    return None


def _guess_agency_from_soup(soup: BeautifulSoup) -> str | None:
    for sel in ("[class*='agency']", "header", "main"):
        for el in soup.select(sel)[:12]:
            t = (el.get_text(" ", strip=True) or "").strip()
            if 8 < len(t) < 120 and re.search(
                r"department|agency|administration|foundation|bureau",
                t,
                re.I,
            ):
                return t
    return None


def _search_queries() -> tuple[str, ...]:
    q = list(SEARCH_QUERIES_BASE)
    if _env_bool("SIMPLER_INCLUDE_EXTENDED_SEARCH", False):
        q.extend(SEARCH_QUERIES_EXTENDED)
    seen: set[str] = set()
    out: list[str] = []
    for term in q:
        term = str(term).strip()
        if not term or term in seen:
            continue
        seen.add(term)
        out.append(term)
    return tuple(out)


def fetch_list_page(page: int, query: str) -> list[dict[str, Any]]:
    params: dict[str, str] = {
        **_LIST_FILTER_PARAMS,
        "page": str(max(1, int(page))),
    }
    q = (query or "").strip()
    if q:
        params[_LIST_KEYWORD_QUERY_PARAM] = q
    r = requests.get(
        LIST_URL,
        params=params,
        headers=_HTML_HEADERS,
        timeout=30,
    )
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    cards = soup.select("a[href^='/opportunity/']")
    results: list[dict[str, Any]] = []
    seen_href: set[str] = set()

    for a in cards:
        href = a.get("href")
        title = (a.get_text(strip=True) or "").strip()
        if not href or not title:
            continue
        href = str(href).strip()
        if href in seen_href:
            continue
        seen_href.add(href)

        full_url = urljoin(SITE_ORIGIN, href)
        tail = href.strip("/").split("/")[-1]
        if "?" in tail:
            tail = tail.split("?")[0]
        source_id = tail or ""

        results.append(
            {
                "title": title,
                "url": full_url,
                "source_id": source_id,
                "_list_extra": {"snippet": ""},
            }
        )

    return results


def extract_deadline_from_simpler_opportunity_soup(soup: BeautifulSoup) -> str | None:
    """
    Дедлайн с карточки opportunity: time[datetime], пары dt/dd, строки таблиц с label.
    Вызывать по полному soup до сильного обрезания контента.
    """
    for t in soup.select("time[datetime]"):
        dv = (t.get("datetime") or "").strip()
        if dv and re.match(r"\d{4}-\d{2}-\d{2}", dv):
            return dv[:10]
    for dt in soup.find_all("dt"):
        lab = (dt.get_text(" ", strip=True) or "").strip()
        if lab and _DEADLINE_LABEL_RE.search(lab):
            dd = dt.find_next_sibling("dd")
            if dd:
                val = (dd.get_text(" ", strip=True) or "").strip()
                if val and 2 < len(val) < 240:
                    return val
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        for i, c in enumerate(cells[:-1]):
            cell_lab = (c.get_text(" ", strip=True) or "").strip()
            if cell_lab and _DEADLINE_LABEL_RE.search(cell_lab):
                nxt = (cells[i + 1].get_text(" ", strip=True) or "").strip()
                if nxt and 2 < len(nxt) < 240:
                    return nxt
    return None


def extract_deadline_from_plain_text(text: str | None) -> str | None:
    """Резерв: метки deadline / closing date / due date в сплошном тексте страницы."""
    raw = (text or "").strip()
    if len(raw) < 8:
        return None
    compact = " ".join(raw.split())
    for pat in _PLAIN_DEADLINE_RES:
        m = pat.search(compact)
        if m:
            try:
                g = (m.group(1) or "").strip()
            except IndexError:
                continue
            if g and len(g) < 200:
                return g
    return None


def fetch_detail_html(url: str) -> dict[str, Any]:
    r = requests.get(url, headers=_HTML_HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    extracted_deadline = extract_deadline_from_simpler_opportunity_soup(soup)
    apply_resolved = _first_apply_href(soup, url) or url
    agency_guess = _guess_agency_from_soup(soup)

    support_email, support_phone = extract_support_contacts_from_simpler(soup)
    full_content_html = extract_full_content_html_from_simpler(soup, url)

    for tag in soup.find_all(["nav", "header", "footer"]):
        tag.decompose()
    for sel in (
        '[class*="usa-banner"]',
        '[class*="gov-banner"]',
        '[id*="gov-banner"]',
        '[aria-label*="official"]',
    ):
        for tag in soup.select(sel):
            tag.decompose()

    main = soup.find("main") or soup.find("article") or soup.body
    if not main:
        text_blocks = soup.select("p, li")
        full_text = " ".join(
            (el.get_text(" ", strip=True) or "").strip() for el in text_blocks
        )
        full_text = strip_us_gov_boilerplate(full_text) or full_text
        sections_html: dict[str, dict[str, list[str]]] = {
            "synopsis": {"text": [], "html": []},
            "eligibility": {"text": [], "html": []},
            "award_payment": {"text": [], "html": []},
            "requirements": {"text": [], "html": []},
            "application": {"text": [], "html": []},
            "other": {"text": [], "html": []},
        }
        return {
            "full_text": full_text,
            "_sections": {},
            "page_url": url,
            "_apply_url_resolved": apply_resolved,
            "_provider_name_guess": agency_guess,
            "_extracted_deadline_text": extracted_deadline,
            "_sections_html": sections_html,
            "_support_email": support_email,
            "_support_phone": support_phone,
            "_full_content_html": full_content_html,
        }

    sections = _extract_sections_from_main(main)
    sections_html = _extract_sections_from_main_with_html(main, url)
    raw_main_text = (main.get_text(" ", strip=True) or "").strip()
    full_text = strip_us_gov_boilerplate(raw_main_text) or raw_main_text

    return {
        "full_text": full_text,
        "_sections": sections,
        "page_url": url,
        "_apply_url_resolved": apply_resolved,
        "_provider_name_guess": agency_guess,
        "_extracted_deadline_text": extracted_deadline,
        "_soup_flags": {"had_main": True},
        "_sections_html": sections_html,
        "_support_email": support_email,
        "_support_phone": support_phone,
        "_full_content_html": full_content_html,
    }


def parse_list_item(card: dict[str, Any]) -> dict[str, Any]:
    extra = dict(card.get("_list_extra") or {})
    return {
        "title": card["title"],
        "url": card["url"],
        "source_id": card["source_id"],
        "award_amount_text": None,
        "deadline_text": None,
        "status_text": None,
        "institutions_text": None,
        "state_territory_text": None,
        "applicants_count": None,
        "credibility_score_text": None,
        "is_verified": False,
        "is_recurring": False,
        "requirements_count": None,
        "_list_extra": extra,
    }


def _combined_filter_blob(
    list_data: dict[str, Any],
    detail: dict[str, Any] | None,
) -> str:
    le = list_data.get("_list_extra") or {}
    parts = [
        str(list_data.get("title") or ""),
        str(le.get("snippet") or ""),
        str(detail.get("full_text") or "") if detail else "",
    ]
    return " ".join(parts).lower()


def _direct_funding_guard_blob(
    list_data: dict[str, Any],
    detail: dict[str, Any] | None,
) -> str:
    """Текст для post-relevance guard: листинг + full_text + все секции main."""
    parts = [_combined_filter_blob(list_data, detail)]
    if detail and isinstance(detail.get("_sections"), dict):
        sec = detail["_sections"]
        for key in (
            "synopsis",
            "eligibility",
            "requirements",
            "application",
            "award_payment",
            "other",
        ):
            for line in sec.get(key) or []:
                parts.append(str(line))
    return " ".join(parts).lower()


# Косвенные program / public-diplomacy / проектные сигналы (без голого "program").
_INDIRECT_PROGRAM_GUARD_SPECS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("alumni", re.compile(r"\balumni\b", re.I)),
    ("innovation fund", re.compile(r"innovation\s+fund", re.I)),
    ("public service", re.compile(r"public\s+service", re.I)),
    ("mission goals", re.compile(r"mission\s+goals", re.I)),
    ("foreign policy objectives", re.compile(r"foreign\s+policy\s+objectives", re.I)),
    ("embassy", re.compile(r"\bembassy\b", re.I)),
    ("bureau", re.compile(r"\bbureau\b", re.I)),
    ("public diplomacy", re.compile(r"public\s+diplomacy", re.I)),
    ("exchange alumni", re.compile(r"exchange\s+alumni", re.I)),
    ("teams of at least", re.compile(r"teams\s+of\s+at\s+least", re.I)),
    ("submit proposals", re.compile(r"submit\s+proposals", re.I)),
    ("annual funding opportunity", re.compile(r"annual\s+funding\s+opportunity", re.I)),
    ("nonprofit", re.compile(r"non-?profits?", re.I)),
    ("organizations", re.compile(r"\borganizations?\b", re.I)),
    ("project implementation", re.compile(r"project\s+implementation", re.I)),
    ("community engagement", re.compile(r"community\s+engagement", re.I)),
    ("mission objectives", re.compile(r"mission\s+objectives", re.I)),
    ("request for proposals", re.compile(r"request\s+for\s+proposals", re.I)),
    ("rfp", re.compile(r"\brfp\b", re.I)),
    ("program funding", re.compile(r"program\s+funding", re.I)),
)

# Признаки eligibility «вперёд организациям», без явного студенческого гранта.
_ORG_FORWARD_ELIGIBILITY_GUARD_SPECS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "eligible include organizations",
        re.compile(
            r"eligible\s+(?:applicants?\s+)?(?:include|are)\s+.{0,40}\borganizations?\b",
            re.I,
        ),
    ),
    (
        "nonprofits may apply",
        re.compile(
            r"non-?profits?\s+(?:and\s+)?(?:other\s+)?(?:entities\s+)?(?:may|can|are\s+eligible\s+to)\s+apply",
            re.I,
        ),
    ),
    (
        "teams of",
        re.compile(r"\bteams\s+of\b", re.I),
    ),
)

# Сильные сигналы прямого студенческого финансирования — при наличии guard не режет.
_DIRECT_STUDENT_FUNDING_GUARD_SPECS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("scholarship", re.compile(r"\bscholarships?\b", re.I)),
    ("fellowship", re.compile(r"\bfellowships?\b", re.I)),
    ("tuition", re.compile(r"\btuition\b", re.I)),
    ("financial aid", re.compile(r"financial\s+aid", re.I)),
    ("stipend", re.compile(r"\bstipends?\b", re.I)),
    ("academic support", re.compile(r"academic\s+support", re.I)),
    ("student award", re.compile(r"student\s+awards?\b", re.I)),
    ("undergraduate scholarship", re.compile(r"undergraduate\s+scholarships?\b", re.I)),
    ("graduate fellowship", re.compile(r"graduate\s+fellowships?\b", re.I)),
    (
        "paid to student",
        re.compile(r"paid\s+to\s+(?:the\s+)?students?\b", re.I),
    ),
    (
        "on behalf of student",
        re.compile(r"on\s+behalf\s+of\s+(?:the\s+)?students?\b", re.I),
    ),
    ("college expenses", re.compile(r"college\s+expenses", re.I)),
    ("educational expenses", re.compile(r"educational\s+expenses", re.I)),
    ("student applicant", re.compile(r"student\s+applicants?\b", re.I)),
    ("enrolled student", re.compile(r"enrolled\s+students?\b", re.I)),
    (
        "degree-seeking student",
        re.compile(r"degree[-\s]seeking\s+students?\b", re.I),
    ),
    ("direct support for students", re.compile(r"direct\s+support\s+for\s+students?\b", re.I)),
    ("student scholarship", re.compile(r"student\s+scholarships?\b", re.I)),
)


def passes_direct_student_funding_guard(
    list_data: dict[str, Any],
    detail: dict[str, Any] | None,
) -> tuple[bool, str, dict[str, Any]]:
    """
    Дополнительный слой после relevance: отсекать project / PD / alumni-heavy гранты
    без явных сигналов scholarship/fellowship/tuition/student aid.
    """
    blob_lc = _direct_funding_guard_blob(list_data, detail)
    indirect_hits = sorted(
        {lbl for lbl, pat in _INDIRECT_PROGRAM_GUARD_SPECS if pat.search(blob_lc)}
    )
    org_hits = sorted(
        {lbl for lbl, pat in _ORG_FORWARD_ELIGIBILITY_GUARD_SPECS if pat.search(blob_lc)}
    )
    direct_hits = sorted(
        {lbl for lbl, pat in _DIRECT_STUDENT_FUNDING_GUARD_SPECS if pat.search(blob_lc)}
    )
    diag: dict[str, Any] = {
        "matched_indirect_program_signals": indirect_hits,
        "matched_org_forward_eligibility_signals": org_hits,
        "matched_direct_student_funding_signals": direct_hits,
    }
    if direct_hits:
        return True, "ok: direct student funding signals", diag
    if not indirect_hits and not org_hits:
        return True, "ok: no indirect/program gate triggers", diag
    return (
        False,
        "skip: indirect project/program grant (not direct student funding)",
        diag,
    )


def _collect_negative_matches(blob_lc: str) -> list[str]:
    """Полный набор негативов для диагностики (hard + soft + research + patterns)."""
    out: list[str] = []
    for ph in _INSTITUTIONAL_PHRASES:
        if ph in blob_lc:
            out.append(ph)
    for ph in _RESEARCH_NEGATIVE_PHRASES:
        if ph in blob_lc:
            out.append(ph)
    for label, pat in _OTHER_INSTITUTIONAL_WORDS:
        if pat.search(blob_lc):
            out.append(label)
    return sorted(set(out))


def _collect_hard_institutional_matches(blob_lc: str) -> list[str]:
    out: list[str] = []
    for ph in _HARD_INSTITUTIONAL_PHRASES:
        if ph in blob_lc:
            out.append(ph)
    for ph in _RESEARCH_NEGATIVE_PHRASES:
        if ph in blob_lc:
            out.append(ph)
    for label, pat in _OTHER_INSTITUTIONAL_WORDS:
        if pat.search(blob_lc):
            out.append(label)
    return sorted(set(out))


def _collect_soft_institutional_matches(blob_lc: str) -> list[str]:
    out: list[str] = []
    for ph in _SOFT_INSTITUTIONAL_PHRASES:
        if ph in blob_lc:
            out.append(ph)
    return sorted(set(out))


def _matched_path_c_education(blob_lc: str) -> list[str]:
    labels: list[str] = []
    for lbl, pat in _ELIGIBILITY_SPECS:
        if pat.search(blob_lc):
            labels.append(lbl)
    for lbl, pat in _PATH_C_EXTRA_ELIGIBILITY:
        if pat.search(blob_lc):
            labels.append(lbl)
    return sorted(set(labels))


def build_filter_diagnostics(
    blob_lc: str,
    matched_search_query: str,
) -> dict[str, Any]:
    m_sl = [lbl for lbl, p in _STUDENT_LEVEL_SPECS if p.search(blob_lc)]
    m_pos = [lbl for lbl, p in _POSITIVE_SPECS if p.search(blob_lc)]
    m_elig = [lbl for lbl, p in _ELIGIBILITY_SPECS if p.search(blob_lc)]
    m_neg = _collect_negative_matches(blob_lc)
    m_path_c_edu = _matched_path_c_education(blob_lc)
    m_hard = _collect_hard_institutional_matches(blob_lc)
    m_soft = _collect_soft_institutional_matches(blob_lc)
    return {
        "matched_search_query": matched_search_query,
        "matched_student_levels": m_sl,
        "matched_positive_keywords": m_pos,
        "matched_eligibility_keywords": m_elig,
        "matched_negative_keywords": m_neg,
        "matched_path_c_education_keywords": m_path_c_edu,
        "matched_hard_institutional_keywords": m_hard,
        "matched_soft_institutional_keywords": m_soft,
        "filter_path": "",
    }


def passes_student_relevance_filter(
    list_data: dict[str, Any],
    detail: dict[str, Any] | None,
    *,
    matched_search_query: str = "",
) -> tuple[bool, str, dict[str, Any]]:
    blob_lc = _combined_filter_blob(list_data, detail)
    diag = build_filter_diagnostics(blob_lc, matched_search_query)

    sl = bool(diag["matched_student_levels"])
    pos: list[str] = list(diag["matched_positive_keywords"])
    pk = bool(pos)
    sf = any(k in pos for k in ("scholarship", "fellowship"))
    el = bool(diag["matched_eligibility_keywords"])
    path_c_edu = bool(diag["matched_path_c_education_keywords"])
    strong_edu = sl or path_c_edu
    hard_inst = bool(diag["matched_hard_institutional_keywords"])
    soft_inst = bool(diag["matched_soft_institutional_keywords"])
    broad_inst = hard_inst or soft_inst

    branch_a = sl and pk
    branch_b = sf and el

    if branch_a:
        diag["filter_path"] = "A+B" if branch_b else "A"
        if branch_b:
            return (
                True,
                "save: A (student_level + positive_kw) [also B]",
                diag,
            )
        return True, "save: A (student_level + positive_kw)", diag
    if branch_b:
        diag["filter_path"] = "B"
        return True, "save: B (scholarship/fellowship + eligibility)", diag

    # C: образовательная релевантность без обязательных слов scholarship/tuition;
    # soft nonprofit/IHE не блокируют, если нет hard gov/capital/research сигнала.
    if strong_edu and not hard_inst:
        diag["filter_path"] = "C"
        return (
            True,
            "save: C (student/education signal, no hard gov/org-capital block)",
            diag,
        )
    if strong_edu and hard_inst:
        diag["filter_path"] = "skip_hard_institutional"
        return (
            False,
            "skip: hard institutional (gov/capital/research) despite edu signal",
            diag,
        )
    if broad_inst:
        diag["filter_path"] = "skip_institutional"
        return (
            False,
            "skip: institutional/soft-negative (no A/B/C)",
            diag,
        )
    diag["filter_path"] = "skip_no_path"
    return False, "skip: no SAVE path (need A, B, or C)", diag


def passes_scholarship_filter(
    list_data: dict[str, Any],
    detail: dict[str, Any] | None,
) -> bool:
    ok, _, _ = passes_student_relevance_filter(list_data, detail)
    return ok


def parse_detail_from_html(
    detail: dict[str, Any] | None,
    page_url: str,
) -> dict[str, Any] | None:
    """Разметка под build_full_record: секции main + очистка .gov boilerplate."""
    if not detail:
        return None
    ft_raw = (detail.get("full_text") or "").strip()
    ft = strip_us_gov_boilerplate(ft_raw) or ft_raw
    ft = (ft or "").strip()

    sections: dict[str, list[str]] = dict(detail.get("_sections") or {})
    sections_html_raw = detail.get("_sections_html")
    sections_html: dict[str, Any] = (
        dict(sections_html_raw) if isinstance(sections_html_raw, dict) else {}
    )

    fch_raw = detail.get("_full_content_html")
    full_content_html: str | None = (
        fch_raw if isinstance(fch_raw, str) and fch_raw.strip() else None
    )

    def _dual_lists(key: str) -> tuple[list[str], list[str]]:
        b = sections_html.get(key)
        if not isinstance(b, dict):
            return [], []
        tx = b.get("text")
        hx = b.get("html")
        tl = list(tx) if isinstance(tx, list) else []
        hl = list(hx) if isinstance(hx, list) else []
        return tl, hl

    def _text_lines_nonempty(lines: list[str]) -> bool:
        return any((str(x) or "").strip() for x in lines)

    syn = " ".join(sections.get("synopsis") or []).strip()
    _, syn_html_list = _dual_lists("synopsis")
    syn_html_joined = "".join(syn_html_list).strip()
    description_html_abs = (
        _absolutize_html_fragment(syn_html_joined, page_url) or None
        if syn_html_joined
        else None
    )

    elig = " ".join(sections.get("eligibility") or []).strip()
    _, elig_html_list = _dual_lists("eligibility")
    elig_html_joined = "".join(elig_html_list).strip()
    eligibility_html_abs = (
        _absolutize_html_fragment(elig_html_joined, page_url) or None
        if elig_html_joined
        else None
    )

    award_parts = sections.get("award_payment") or []
    _, award_html_list = _dual_lists("award_payment")
    award_html_joined = "".join(award_html_list).strip()
    payment_html_abs = (
        _absolutize_html_fragment(award_html_joined, page_url) or None
        if award_html_joined
        else None
    )

    req_bucket_key: str | None = None
    for key in ("requirements", "application", "other"):
        lines = sections.get(key) or []
        if _text_lines_nonempty(list(lines) if isinstance(lines, list) else []):
            req_bucket_key = key
            break

    req_text_lines: list[str] = []
    req_html_parts: list[str] = []
    if req_bucket_key:
        raw_lines = sections.get(req_bucket_key) or []
        if isinstance(raw_lines, list):
            req_text_lines = [
                str(x).strip() for x in raw_lines if str(x).strip()
            ]
        _, req_html_parts = _dual_lists(req_bucket_key)

    requirements_text: str | None = (
        "\n".join(req_text_lines).strip() if req_text_lines else None
    )
    if requirements_text:
        requirements_text = strip_us_gov_boilerplate(requirements_text) or None
    requirements_text = strip_requirements_fluff(requirements_text)

    req_html_joined = "".join(req_html_parts).strip() if req_html_parts else ""
    requirements_html_abs = (
        _absolutize_html_fragment(req_html_joined, page_url) or None
        if req_html_joined
        else None
    )

    description = strip_us_gov_boilerplate(syn) if syn else None
    desc_needs_full_text = not description or len(description) < 24
    if desc_needs_full_text:
        description = ft if ft and len(ft) >= 24 else None
    if description:
        description = strip_us_gov_boilerplate(description)

    if (
        not description_html_abs
        and full_content_html
        and desc_needs_full_text
        and description
    ):
        description_html_abs = (
            _absolutize_html_fragment(full_content_html, page_url) or None
        )

    if (
        not requirements_html_abs
        and full_content_html
        and not (requirements_text or "").strip()
    ):
        requirements_html_abs = (
            _absolutize_html_fragment(full_content_html, page_url) or None
        )

    eligibility_text = strip_us_gov_boilerplate(elig) if elig else None

    awards_text, winner_payment_text = _split_payment_award(award_parts)
    if awards_text:
        awards_text = strip_us_gov_boilerplate(awards_text)
    if winner_payment_text:
        winner_payment_text = strip_us_gov_boilerplate(winner_payment_text)
    if not awards_text and not winner_payment_text and award_parts:
        joined = strip_us_gov_boilerplate(" ".join(award_parts))
        if joined:
            awards_text = joined

    provider_name = detail.get("_provider_name_guess")
    if isinstance(provider_name, str):
        provider_name = strip_us_gov_boilerplate(provider_name) or None
    else:
        provider_name = None

    apply_url = str(detail.get("_apply_url_resolved") or page_url).strip() or page_url

    dl_dom = (detail.get("_extracted_deadline_text") or "").strip()
    dl_plain = (
        (extract_deadline_from_plain_text(ft) or "").strip() if not dl_dom else ""
    )
    _parsed_deadline_text = (dl_dom or dl_plain).strip() or None

    support_email = detail.get("_support_email")
    support_phone = detail.get("_support_phone")

    return {
        "provider_name": provider_name,
        "provider_url": None,
        "provider_mission": None,
        "description": description,
        "description_html": description_html_abs,
        "requirements_text": requirements_text,
        "requirements_html": requirements_html_abs,
        "winner_payment_text": winner_payment_text,
        "payment_html": payment_html_abs,
        "apply_url": apply_url,
        "apply_button_text": "View on Simpler.Grants.gov",
        "application_status_text": None,
        "mark_started_available": False,
        "mark_submitted_available": False,
        "provider_social_facebook": None,
        "provider_social_instagram": None,
        "provider_social_linkedin": None,
        "category": None,
        "eligibility_text": eligibility_text,
        "eligibility_html": eligibility_html_abs,
        "awards_text": awards_text,
        "awards_html": payment_html_abs,
        "notification_text": None,
        "notification_html": None,
        "selection_criteria_text": None,
        "selection_criteria_html": None,
        "_support_email": support_email,
        "_support_phone": support_phone,
        "full_content_html": full_content_html,
        "full_text": ft or None,
        "_parsed_deadline_text": _parsed_deadline_text,
        "_detail_extra": {"page_url": page_url},
    }


def _count_req_lines(requirements_text: str | None) -> int | None:
    if not requirements_text or not requirements_text.strip():
        return None
    lines = [ln.strip() for ln in requirements_text.splitlines() if ln.strip()]
    return len(lines) if lines else None


def _json_safe(obj: Any) -> Any:
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, float) and (obj != obj):
        return None
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(x) for x in obj]
    return str(obj)


def build_full_record(
    list_data: dict[str, Any],
    detail: dict[str, Any] | None,
    detail_error: str | None,
) -> dict[str, Any]:
    d = dict(detail or {})
    list_extra = dict(list_data.get("_list_extra") or {})
    filter_diag = list_data.pop("_filter_diagnostics", None)

    support_email = d.pop("_support_email", None)
    support_phone = d.pop("_support_phone", None)
    d.pop("_detail_extra", None)
    d.pop("_extracted_deadline_text", None)
    parsed_deadline_from_detail = d.pop("_parsed_deadline_text", None)
    section_snapshot = {
        "eligibility": d.get("eligibility_text"),
        "eligibility_html": d.get("eligibility_html"),
        "awards": d.get("awards_text"),
        "awards_html": d.get("awards_html"),
        "notification": d.get("notification_text"),
        "notification_html": d.get("notification_html"),
        "selection_criteria": d.get("selection_criteria_text"),
        "selection_criteria_html": d.get("selection_criteria_html"),
        "description_html": d.get("description_html"),
        "payment_html": d.get("payment_html"),
        "requirements_html": d.get("requirements_html"),
        "full_content_html": d.get("full_content_html"),
    }

    raw_data: dict[str, Any] = {
        "source_parser": SOURCE,
        "list": {k: list_data.get(k) for k in list_data if not str(k).startswith("_")},
        "list_extra": list_extra,
        "detail": {k: v for k, v in d.items() if not str(k).startswith("_")},
        "sections": section_snapshot,
        "detail_error": detail_error,
    }
    if isinstance(filter_diag, dict) and filter_diag:
        raw_data["filter_diagnostics"] = filter_diag

    title = list_data.get("title") or "Untitled scholarship"
    url = list_data.get("url") or ""
    award_text = list_data.get("award_amount_text") or d.get("award_amount_text")
    deadline_text = list_data.get("deadline_text")
    if parsed_deadline_from_detail and str(parsed_deadline_from_detail).strip():
        deadline_text = str(parsed_deadline_from_detail).strip()

    amin, amax = parse_award_min_max(award_text)
    ddate = parse_deadline_date(deadline_text)

    req_text = d.get("requirements_text")
    req_n = list_data.get("requirements_count")
    if req_n is None:
        req_n = _count_req_lines(req_text)

    category = d.get("category")
    if not category and list_data.get("institutions_text"):
        category = list_data.get("institutions_text")

    status_text = list_data.get("status_text")
    institutions_text = list_data.get("institutions_text")
    state_territory_text = list_data.get("state_territory_text")

    record: dict[str, Any] = {
        "source": SOURCE,
        "source_id": list_data.get("source_id"),
        "url": url,
        "title": title,
        "provider_name": d.get("provider_name"),
        "provider_url": d.get("provider_url"),
        "provider_mission": d.get("provider_mission"),
        "award_amount_text": award_text,
        "award_amount_min": amin,
        "award_amount_max": amax,
        "currency": DEFAULT_CURRENCY,
        "deadline_text": deadline_text,
        "deadline_date": ddate,
        "requirements_count": req_n,
        "requirements_text": req_text,
        "applicants_count": list_data.get("applicants_count"),
        "credibility_score_text": list_data.get("credibility_score_text"),
        "is_verified": bool(list_data.get("is_verified")),
        "is_recurring": bool(list_data.get("is_recurring")),
        "winner_payment_text": d.get("winner_payment_text"),
        "description": d.get("description"),
        "description_html": d.get("description_html"),
        "provider_social_facebook": d.get("provider_social_facebook"),
        "provider_social_instagram": d.get("provider_social_instagram"),
        "provider_social_linkedin": d.get("provider_social_linkedin"),
        "apply_url": d.get("apply_url"),
        "apply_button_text": d.get("apply_button_text"),
        "application_status_text": d.get("application_status_text"),
        "mark_started_available": bool(d.get("mark_started_available")),
        "mark_submitted_available": bool(d.get("mark_submitted_available")),
        "status_text": status_text,
        "institutions_text": institutions_text,
        "state_territory_text": state_territory_text,
        "support_email": support_email,
        "support_phone": support_phone,
        "eligibility_text": d.get("eligibility_text"),
        "eligibility_html": d.get("eligibility_html"),
        "awards_text": d.get("awards_text"),
        "awards_html": d.get("awards_html"),
        "notification_text": d.get("notification_text"),
        "notification_html": d.get("notification_html"),
        "selection_criteria_text": d.get("selection_criteria_text"),
        "selection_criteria_html": d.get("selection_criteria_html"),
        "payment_html": d.get("payment_html"),
        "requirements_html": d.get("requirements_html"),
        "full_content_html": d.get("full_content_html"),
        "category": category,
        "tags": [],
        "is_active": True,
        "raw_data": _json_safe(raw_data),
    }

    apply_normalization(record)

    for k in SCHOLARSHIP_TABLE_KEYS:
        if k not in record:
            record[k] = None

    record["is_active"] = True
    record["currency"] = DEFAULT_CURRENCY
    record["source"] = SOURCE
    return record


# --- Debug: business filter skip (no parsed deadline) — только лог, без изменения логики ---

_DEADLINE_DEBUG_SNIP_RE = re.compile(
    r"(?i)\b(?:deadline|closing|close\s+date|due)\b",
)


def _full_text_deadline_debug_snippets(
    full_text: str | None,
    *,
    radius: int = 90,
    max_snips: int = 5,
    max_len: int = 320,
) -> list[str]:
    if not full_text:
        return []
    t = str(full_text)
    if not t.strip():
        return []
    out: list[str] = []
    for m in _DEADLINE_DEBUG_SNIP_RE.finditer(t):
        a = max(0, m.start() - radius)
        b = min(len(t), m.end() + radius)
        frag = re.sub(r"\s+", " ", t[a:b].replace("\n", " ")).strip()
        if frag:
            out.append(frag[:max_len])
        if len(out) >= max_snips:
            break
    return out


def _log_simpler_skip_no_parsed_deadline_debug(
    title: str,
    record: dict[str, Any],
    merged_detail: dict[str, Any] | None,
    detail: dict[str, Any] | None,
) -> None:
    print(f"  [deadline debug] title: {(title or '')[:220]!r}")
    print(f"  [deadline debug] record[deadline_text]: {record.get('deadline_text')!r}")
    print(f"  [deadline debug] record[deadline_date]: {record.get('deadline_date')!r}")
    md = merged_detail if isinstance(merged_detail, dict) else {}
    print(
        "  [deadline debug] merged_detail[_parsed_deadline_text]: "
        f"{md.get('_parsed_deadline_text')!r}"
    )
    det = detail if isinstance(detail, dict) else {}
    print(
        "  [deadline debug] detail[_extracted_deadline_text]: "
        f"{det.get('_extracted_deadline_text')!r}"
    )
    snips = _full_text_deadline_debug_snippets(det.get("full_text"))
    if snips:
        for i, s in enumerate(snips, 1):
            print(f"  [deadline debug] full_text snip#{i}: {s!r}")
    else:
        print("  [deadline debug] full_text snippets: (none — empty or no keyword hits)")


def run() -> None:
    stats: dict[str, int] = {
        "list_rows_seen": 0,
        "known_skipped": 0,
        "new_found": 0,
        "detail_fetched": 0,
        "scholarship_skipped": 0,
        "indirect_program_skipped": 0,
        "skip_no_funding": 0,
        "skip_no_deadline": 0,
        "skip_expired": 0,
        "skip_deadline_too_close": 0,
        "upsert_ok": 0,
        "upsert_failed": 0,
    }
    seen_urls_session: set[str] = set()
    stop_reason = ""
    use_skip = SKIP_EXISTING_ON_LIST and DISCOVERY_MODE == "new_only"

    cap_dbg = MAX_SIMPLER_GRANTS_GOV_RECORDS_DEBUG
    effective_target = (
        min(TARGET_NEW_ITEMS, cap_dbg) if cap_dbg > 0 else TARGET_NEW_ITEMS
    )

    search_queries = _search_queries()
    print(
        f"{SOURCE}: HTML discovery (TARGET_NEW_ITEMS={TARGET_NEW_ITEMS}, "
        f"effective_target_upserts={effective_target}, "
        f"SIMPLER_MAX_RECORDS_DEBUG={cap_dbg} (0=unlimited), "
        f"MAX_LIST_PAGES={MAX_LIST_PAGES}, NO_NEW_PAGES_STOP={NO_NEW_PAGES_STOP}, "
        f"SKIP_EXISTING_ON_LIST={SKIP_EXISTING_ON_LIST}, DISCOVERY_MODE={DISCOVERY_MODE!r}, "
        f"SEARCH_QUERIES={search_queries!r}, "
        f"list_filters={_LIST_FILTER_PARAMS!r}, "
        f"list_keyword_param={_LIST_KEYWORD_QUERY_PARAM!r}, "
        f"SIMPLER_INCLUDE_EXTENDED_SEARCH={_env_bool('SIMPLER_INCLUDE_EXTENDED_SEARCH', False)})"
    )

    idx: KnownScholarshipIndex
    if use_skip:
        try:
            idx = load_known_scholarship_index(get_client(), SOURCE)
            print(
                f"  known index: {len(idx.urls)} urls, {len(idx.source_ids)} source_ids, "
                f"{len(idx.slugs_lc)} slugs, {len(idx.titles_norm)} titles "
                f"(USE_TITLE_FALLBACK_KNOWN={USE_TITLE_FALLBACK_KNOWN})"
            )
        except Exception as e:
            print(f"  warning: could not load known index ({e}); continuing without skip")
            idx = KnownScholarshipIndex()
    else:
        idx = KnownScholarshipIndex()

    list_pages_loaded = 0
    for search_query in search_queries:
        if stats["upsert_ok"] >= effective_target:
            stop_reason = stop_reason or "reached effective_target_upserts"
            break

        page = 1
        consecutive_pages_no_new = 0
        while page <= MAX_LIST_PAGES:
            if stats["upsert_ok"] >= effective_target:
                stop_reason = stop_reason or "reached effective_target_upserts"
                break

            print(f"[list] query={search_query!r} page={page}")
            try:
                rows = fetch_list_page(page, search_query)
            except Exception as e:
                print(f"  list fetch failed: {e}")
                stop_reason = stop_reason or (
                    f"list fetch failed query={search_query!r} page={page}"
                )
                break

            list_pages_loaded += 1
            if not rows:
                print(f"  empty page for query {search_query!r}; next query")
                break

            new_on_this_page = 0

            for card_row in rows:
                if stats["upsert_ok"] >= effective_target:
                    break

                card = parse_list_item(card_row)

                stats["list_rows_seen"] += 1
                title = str(card.get("title") or "")
                detail_url = str(card.get("url") or "")

                if detail_url in seen_urls_session:
                    print(f"  row: {title[:70]} → duplicate URL this session, skip")
                    continue
                seen_urls_session.add(detail_url)

                known = bool(
                    use_skip
                    and listing_is_known(
                        card,
                        idx,
                        title_fallback=USE_TITLE_FALLBACK_KNOWN,
                    )
                )
                if known:
                    stats["known_skipped"] += 1
                    print(f"  row: {title[:70]} → known, skip")
                    continue

                stats["new_found"] += 1
                new_on_this_page += 1
                print(f"  row: {title[:70]} → new, fetching detail")

                detail: dict[str, Any] | None = None
                detail_error: str | None = None
                try:
                    detail = fetch_detail_html(detail_url)
                    stats["detail_fetched"] += 1
                    print("  detail OK")
                except Exception as e:
                    detail_error = str(e)
                    print(f"  detail failed: {e}")

                ok_save, filter_reason, filter_diag = passes_student_relevance_filter(
                    card,
                    detail,
                    matched_search_query=search_query,
                )
                if not ok_save:
                    stats["scholarship_skipped"] += 1
                    print(
                        f"  skip: relevance filter — {filter_reason} | "
                        f"path={filter_diag.get('filter_path')} "
                        f"levels={filter_diag.get('matched_student_levels')} "
                        f"path_c_edu={filter_diag.get('matched_path_c_education_keywords')} "
                        f"pos={filter_diag.get('matched_positive_keywords')} "
                        f"elig={filter_diag.get('matched_eligibility_keywords')} "
                        f"hard={filter_diag.get('matched_hard_institutional_keywords')} "
                        f"soft={filter_diag.get('matched_soft_institutional_keywords')} "
                        f"neg_all={filter_diag.get('matched_negative_keywords')}"
                    )
                    time.sleep(0.15)
                    continue

                ok_guard, guard_reason, guard_diag = passes_direct_student_funding_guard(
                    card,
                    detail,
                )
                if not ok_guard:
                    stats["indirect_program_skipped"] += 1
                    print(
                        "  skip: indirect project/program grant (not direct student funding) | "
                        f"indirect={guard_diag.get('matched_indirect_program_signals')} "
                        f"org_elig={guard_diag.get('matched_org_forward_eligibility_signals')} "
                        f"direct={guard_diag.get('matched_direct_student_funding_signals')}"
                    )
                    time.sleep(0.15)
                    continue

                card["_filter_diagnostics"] = filter_diag

                merged_detail = parse_detail_from_html(detail, detail_url)
                record = build_full_record(card, merged_detail, detail_error)
                record = ai_enrich_simpler_record_if_enabled(record)

                if not has_meaningful_funding(record):
                    stats["skip_no_funding"] += 1
                    print("  skip: business filter — no meaningful funding")
                    time.sleep(0.15)
                    continue

                dbiz = classify_business_deadline(record.get("deadline_date"))
                if dbiz != "ok":
                    if dbiz == "no_deadline":
                        stats["skip_no_deadline"] += 1
                        _log_simpler_skip_no_parsed_deadline_debug(
                            str(card.get("title") or record.get("title") or ""),
                            record,
                            merged_detail,
                            detail,
                        )
                        print("  skip: business filter — no parsed deadline")
                    elif dbiz == "expired":
                        stats["skip_expired"] += 1
                        print("  skip: business filter — deadline expired")
                    else:
                        stats["skip_deadline_too_close"] += 1
                        print(
                            "  skip: business filter — deadline too soon "
                            f"(need >= {MIN_LEAD_DAYS_BEFORE_DEADLINE} days)"
                        )
                    time.sleep(0.15)
                    continue

                try:
                    upsert_scholarship(record)
                    stats["upsert_ok"] += 1
                    print(
                        f"  upsert OK ({filter_reason}) "
                        f"({stats['upsert_ok']}/{effective_target})"
                    )
                except Exception as e:
                    stats["upsert_failed"] += 1
                    print(f"  upsert failed: {e}")

                time.sleep(0.15)

            if stats["upsert_ok"] >= effective_target:
                stop_reason = stop_reason or "reached effective_target_upserts"
                break

            if new_on_this_page == 0:
                if use_skip:
                    consecutive_pages_no_new += 1
                    if consecutive_pages_no_new >= NO_NEW_PAGES_STOP:
                        print(
                            f"  {NO_NEW_PAGES_STOP} consecutive pages with no new rows "
                            f"for query={search_query!r}; trying next query"
                        )
                        break
            else:
                consecutive_pages_no_new = 0

            page += 1

        if stop_reason and "list fetch failed" in stop_reason:
            break
        if stats["upsert_ok"] >= effective_target:
            break

    if not stop_reason:
        stop_reason = "ended (all search queries exhausted or page limits)"

    print("")
    print(f"processed list pages: {list_pages_loaded}")
    print(f"list rows seen: {stats['list_rows_seen']}")
    print(f"known skipped: {stats['known_skipped']}")
    print(f"new found: {stats['new_found']}")
    print(f"detail fetched: {stats['detail_fetched']}")
    print(f"relevance filter skipped: {stats['scholarship_skipped']}")
    print(
        "skip (indirect program / not direct student funding): "
        f"{stats['indirect_program_skipped']}"
    )
    print(f"skip (business): no funding: {stats['skip_no_funding']}")
    print(f"skip (business): no deadline: {stats['skip_no_deadline']}")
    print(f"skip (business): expired: {stats['skip_expired']}")
    print(
        "skip (business): deadline too close "
        f"(<{MIN_LEAD_DAYS_BEFORE_DEADLINE}d): {stats['skip_deadline_too_close']}"
    )
    print(f"upsert OK: {stats['upsert_ok']}")
    print(f"upsert failed: {stats['upsert_failed']}")
    print(f"stop reason: {stop_reason}")


if __name__ == "__main__":
    run()
