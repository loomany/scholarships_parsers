"""
Парсер BigFuture Scholarship Search (College Board) → public.scholarships (Supabase).

Листинг: direct HTTP к стабильному JSON API
(GET /scholarship-search bootstrap + POST scholarshipsearch-api.collegeboard.org/scholarships).
Деталь: SSR __NEXT_DATA__ на странице /scholarships/{programTitleSlug} (Playwright, опционально).

Конфигурация: config.BigFutureConfig + GlobalConfig; шаблон — README.md и .env.example в этом пакете.
  Включение — BIGFUTURE_ENABLED в run_all.
  Двухфазный pipeline: fast list-prefilter (sources.bigfuture.prefilter + JSON store) → deep
  (detail, business filters, AI, upsert). BIGFUTURE_AUTO_PIPELINE=1 (по умолчанию): обе фазы
  за один run() без переключения env. Ручной режим при AUTO=0: BIGFUTURE_FAST_PREFILTER_ONLY,
  BIGFUTURE_DEEP_PASS_ONLY, BIGFUTURE_PREFILTER_STORE_PATH, BIGFUTURE_MIN_AMOUNT_HINT,
  BIGFUTURE_RECHECK_REJECT_DAYS, BIGFUTURE_DEEP_INCLUDE_REVIEW.
  BIGFUTURE_HEADLESS, BIGFUTURE_TIMEOUT_MS, BIGFUTURE_MAX_RECORDS_DEBUG, BIGFUTURE_DETAIL_FETCH,
  BIGFUTURE_ACTIVE_ONLY (листинг: пропуск карточек с closeDate в прошлом до detail/AI),
  BIGFUTURE_KEYWORD, BIGFUTURE_AI_* — см. .env.example.
"""

from __future__ import annotations

import json
import os
import re
import traceback
import sys
import time
from datetime import date
from typing import Any, NamedTuple

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import Page, Playwright, sync_playwright

_PARSER_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if _PARSER_ROOT not in sys.path:
    sys.path.insert(0, _PARSER_ROOT)

from business_filters import (
    MIN_LEAD_DAYS_BEFORE_DEADLINE,
    classify_business_deadline,
    has_meaningful_funding,
)
from normalize_scholarship import apply_normalization
from scholarship_db_columns import (
    SCHOLARSHIP_RECORD_DEFAULT_KEYS,
    SCHOLARSHIP_UPSERT_BODY_KEYS,
)
from sources.scholarship_america.parser import parse_award_min_max, parse_deadline_date
from utils import (
    KnownScholarshipIndex,
    get_client,
    listing_is_known,
    load_known_scholarship_index,
    upsert_scholarship,
)

from config import BigFutureConfig, get_bigfuture_config, get_global_config
from sources.bigfuture.prefilter import (
    PREFILTER_PASS,
    PREFILTER_REJECT_DEADLINE,
    PREFILTER_REJECT_FUNDING,
    PREFILTER_REJECT_RELEVANCE,
    PREFILTER_REVIEW,
    BigFuturePrefilterStore,
    classify_fast_prefilter,
)
from sources.shared_ai_enrichment import (
    empty_ai_enrichment,
    ensure_mutable_raw_data,
    json_safe as _json_safe,
    merge_ai_enrichment_into_record,
    normalize_ai_enrichment_parsed,
)

SCHOLARSHIP_TABLE_KEYS: tuple[str, ...] = SCHOLARSHIP_UPSERT_BODY_KEYS

SOURCE = "bigfuture"
DEFAULT_CURRENCY = "USD"
SITE_ORIGIN = "https://bigfuture.collegeboard.org"
SEARCH_URL = f"{SITE_ORIGIN}/scholarship-search"
SCHOLARSHIPS_API = "https://scholarshipsearch-api.collegeboard.org/scholarships"
LIST_PAGE_SIZE = 15

_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
    re.DOTALL,
)


_gc = get_global_config()
_bfc = get_bigfuture_config()
BIGFUTURE_ENABLED = _bfc.enabled
BIGFUTURE_HEADLESS = _bfc.headless
BIGFUTURE_TIMEOUT_MS = _bfc.timeout_ms
BIGFUTURE_MAX_RECORDS_DEBUG = _bfc.max_records_debug
BIGFUTURE_DETAIL_FETCH = _bfc.detail_fetch
BIGFUTURE_ACTIVE_ONLY = _bfc.active_only
BIGFUTURE_KEYWORD = _bfc.keyword

TARGET_NEW_ITEMS = _gc.target_new_items
MAX_LIST_PAGES = _gc.max_list_pages
NO_NEW_PAGES_STOP = _gc.no_new_pages_stop
SKIP_EXISTING_ON_LIST = _gc.skip_existing_on_list
USE_TITLE_FALLBACK_KNOWN = _gc.use_title_fallback_known
DISCOVERY_MODE = _gc.discovery_mode


def bigfuture_ai_enrich_enabled() -> bool:
    return get_bigfuture_config().ai_enabled


def bigfuture_ai_model() -> str:
    return get_bigfuture_config().ai_model


def bigfuture_ai_max_input_chars() -> int:
    return get_bigfuture_config().ai_max_input_chars


_BIGFUTURE_AI_SYSTEM_PROMPT = """You are an analyst extracting structured facts from a college \
scholarship directory / scholarship catalog record (BigFuture, College Board style). The excerpt may \
include fields from a listing API and a detail page. Use only information present in the JSON excerpt; \
do not invent award amounts, deadlines, provider names, eligibility rules, or required documents that \
are not supported by the text. If unknown, use null for strings, empty arrays for lists, or null for numbers.

Return a single JSON object with exactly these keys:
- short_summary (string|null): 1–3 sentences, plain language.
- eligibility_list (array of strings): who may apply; empty array if unclear.
- key_requirements (array of strings): main application steps or criteria; empty if unclear.
- required_documents (array of strings): explicit document types mentioned; empty if none stated.
- funding_amount_text (string|null): human-readable award range or amount if stated.
- deadline_text (string|null): closing / due date text if stated.
- payout_method (string|null): how funds are delivered if inferable (e.g. paid to student, tuition credit); else null.
- provider_name (string|null): scholarship sponsor or organization name if clear.
- student_relevance (string|null): one of high, medium, low, none — for students seeking aid, based only on the excerpt.
- confidence_score (number|null): 0.0–1.0 reflecting how well the excerpt supports your extractions; null if not applicable.

Output valid JSON only, no markdown."""


def build_ai_input_payload_for_bigfuture(record: dict[str, Any] | None) -> dict[str, Any]:
    """
    Компактный excerpt для модели: основные поля записи + усечённый raw_data (карточка API, detail payload).
    """
    max_c = bigfuture_ai_max_input_chars()
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
        "full_content_html",
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

    if used < max_c - 200:
        rd_any = r.get("raw_data")
        rd: dict[str, Any] | None = None
        if isinstance(rd_any, dict):
            rd = rd_any
        elif isinstance(rd_any, str) and rd_any.strip():
            try:
                p = json.loads(rd_any)
                rd = p if isinstance(p, dict) else None
            except json.JSONDecodeError:
                rd = None
        if isinstance(rd, dict):
            slim: dict[str, Any] = {}
            le = rd.get("list_extra")
            if isinstance(le, dict):
                rc = le.get("raw_list_card")
                if isinstance(rc, dict):
                    slim["raw_list_card"] = rc
                nm = le.get("list_network_meta")
                if isinstance(nm, dict):
                    slim["list_network_meta"] = nm
            dp = rd.get("detail_raw_payload")
            if isinstance(dp, dict):
                slim["detail_raw_payload"] = dp
            if slim:
                blob = json.dumps(slim, ensure_ascii=False)
                budget = max_c - used - 24
                if budget > 120:
                    if len(blob) > budget:
                        blob = blob[: max(1, budget - 1)] + "…"
                    excerpt["raw_data_excerpt"] = blob
                    used += len(blob) + 24

    return {
        "source_parser": SOURCE,
        "purpose": "bigfuture_ai_enrich",
        "record_excerpt": excerpt,
    }


def ai_enrich_bigfuture_grant(record: dict[str, Any] | None) -> dict[str, Any]:
    """
    Обогащение через OpenAI (OPENAI_API_KEY). При выключенном BIGFUTURE_AI_ENRICH_ENABLED — пустая схема, без сети.
    """
    empty = empty_ai_enrichment()
    if not bigfuture_ai_enrich_enabled():
        return empty

    api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return empty

    try:
        from openai import OpenAI
    except ImportError:
        return empty

    user_json = json.dumps(
        build_ai_input_payload_for_bigfuture(record),
        ensure_ascii=False,
    )
    user_prompt = (
        "Extract structured fields from this scholarship catalog record excerpt (JSON). "
        "Respond with one JSON object using only the schema from the system message.\n\n"
        f"{user_json}"
    )

    try:
        client = OpenAI(api_key=api_key)
        completion = client.chat.completions.create(
            model=bigfuture_ai_model(),
            temperature=0.2,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": _BIGFUTURE_AI_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
        )
        text = (completion.choices[0].message.content or "").strip()
        if not text:
            return empty
        parsed = json.loads(text)
        if not isinstance(parsed, dict):
            return empty
        return normalize_ai_enrichment_parsed(parsed)
    except Exception:
        return empty


def ai_enrich_bigfuture_record_if_enabled(record: dict[str, Any]) -> dict[str, Any]:
    """
    Pipeline: при BIGFUTURE_AI_ENRICH_ENABLED — вызов модели и merge (без grants.gov preclean).
    """
    if not bigfuture_ai_enrich_enabled():
        print("[BIGFUTURE AI] enrich skipped (disabled)")
        return record

    try:
        print("[BIGFUTURE AI] enrich start")
        ai_data = ai_enrich_bigfuture_grant(record)
        merged = merge_ai_enrichment_into_record(record, ai_data)
        print("[BIGFUTURE AI] enrich success")
        return merged
    except Exception as e:
        err_msg = f"{type(e).__name__}: {e}"
        print(f"[BIGFUTURE AI] enrich failed: {err_msg}")
        try:
            fallback = dict(record)
            rd = ensure_mutable_raw_data(fallback)
            rd["ai_enrichment_error"] = _json_safe(err_msg)
            fallback["raw_data"] = rd
            return fallback
        except Exception:
            return record


_LIST_INCLUDE_FIELDS: list[str] = [
    "cbScholarshipId",
    "programTitleSlug",
    "programReferenceId",
    "programOrganizationName",
    "scholarshipMaximumAward",
    "programName",
    "openDate",
    "closeDate",
    "isMeritBased",
    "isNeedBased",
    "awardVerificationCriteriaDescription",
    "programSelfDescription",
    "eligibilityCriteriaDescription",
    "blurb",
]

_pw_holder: dict[str, Any] = {}
_list_http_holder: dict[str, Any] = {}


def _list_post_body(from_offset: int) -> dict[str, Any]:
    """
    Тело POST к scholarshipsearch-api.collegeboard.org/scholarships.
    Сервер, по проверкам, учитывает в criteria по сути только includeFields (и config);
    дополнительные ключи (activeOnly, range по closeDate и т.п.) не меняют totalHits.
    Отсев истёкших карточек — локально в fetch_list_page при BIGFUTURE_ACTIVE_ONLY.
    """
    return {
        "config": {"size": LIST_PAGE_SIZE, "from": from_offset},
        "criteria": {"includeFields": _LIST_INCLUDE_FIELDS},
    }


def _close_date_from_list_item(item: dict[str, Any]) -> date | None:
    """closeDate в list API обычно строка YYYY-MM-DD; иначе None."""
    raw = item.get("closeDate")
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def _list_item_expired_by_close_date(item: dict[str, Any], *, today: date | None = None) -> bool:
    d = _close_date_from_list_item(item)
    if d is None:
        return False
    t = today if today is not None else date.today()
    return d < t


def _scholarship_url(slug: str | None) -> str:
    s = (slug or "").strip()
    if not s:
        return SEARCH_URL
    return f"{SITE_ORIGIN}/scholarships/{s}"


def _format_award_text(raw: Any) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        if raw != raw:
            return None
        n = float(raw)
        if n.is_integer():
            return f"${int(n):,}"
        return f"${n:,.2f}"
    t = str(raw).strip()
    return t or None


def _extract_next_data_props(html: str) -> dict[str, Any] | None:
    m = _NEXT_DATA_RE.search(html)
    if not m:
        return None
    try:
        tree = json.loads(m.group(1))
    except json.JSONDecodeError:
        return None
    props = tree.get("props") or {}
    pp = props.get("pageProps") or {}
    data = pp.get("data")
    return data if isinstance(data, dict) else None


def _main_inner_html(soup: BeautifulSoup, page_url: str) -> str | None:
    root = soup.find("main") or soup.find("article") or soup.find("body")
    if not root:
        return None
    for tag in list(root.find_all(["script", "style", "noscript", "nav", "header", "footer"])):
        tag.decompose()
    html = root.decode_contents().strip()
    return html or None


def _start_playwright() -> tuple[Playwright, Any, Page]:
    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=BIGFUTURE_HEADLESS)
    page = browser.new_page()
    page.set_default_timeout(BIGFUTURE_TIMEOUT_MS)
    return pw, browser, page


def _close_playwright() -> None:
    page = _pw_holder.get("page")
    browser = _pw_holder.get("browser")
    pw = _pw_holder.get("pw")
    try:
        if page:
            page.close()
    except Exception:
        pass
    try:
        if browser:
            browser.close()
    except Exception:
        pass
    try:
        if pw:
            pw.stop()
    except Exception:
        pass
    _pw_holder.clear()


def _ensure_page() -> Page:
    if _pw_holder.get("page"):
        return _pw_holder["page"]
    pw, browser, page = _start_playwright()
    _pw_holder["pw"] = pw
    _pw_holder["browser"] = browser
    _pw_holder["page"] = page
    return page


def _close_list_http_session() -> None:
    sess = _list_http_holder.get("session")
    try:
        if sess:
            sess.close()
    except Exception:
        pass
    _list_http_holder.clear()


def _ensure_list_http_session() -> requests.Session:
    sess = _list_http_holder.get("session")
    if isinstance(sess, requests.Session):
        return sess
    sess = requests.Session()
    sess.headers.update(
        {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "origin": SITE_ORIGIN,
            "referer": SEARCH_URL,
            "user-agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            ),
        }
    )
    _list_http_holder["session"] = sess
    return sess


def _bootstrap_list_http_session(sess: requests.Session) -> None:
    if _list_http_holder.get("bootstrapped"):
        return
    r = sess.get(SEARCH_URL, timeout=BIGFUTURE_TIMEOUT_MS / 1000)
    r.raise_for_status()
    _list_http_holder["bootstrapped"] = True


def _post_scholarships_list(body: dict[str, Any]) -> dict[str, Any]:
    sess = _ensure_list_http_session()
    attempts = 3
    last_err: dict[str, Any] | None = None
    for attempt in range(1, attempts + 1):
        try:
            _bootstrap_list_http_session(sess)
            r = sess.post(
                SCHOLARSHIPS_API,
                json=body,
                timeout=BIGFUTURE_TIMEOUT_MS / 1000,
            )
            if r.status_code >= 400:
                txt = (r.text or "")[:2000]
                err_kind = "http_error"
                if r.status_code in (401, 403):
                    err_kind = "auth_or_bot_block"
                elif r.status_code in (429,):
                    err_kind = "rate_limit"
                last_err = {
                    "_parseError": True,
                    "kind": err_kind,
                    "attempt": attempt,
                    "status": r.status_code,
                    "text": txt,
                }
                if attempt < attempts:
                    time.sleep(0.8 * attempt)
                    continue
                return last_err
            try:
                payload = r.json()
            except Exception:
                txt = (r.text or "")[:2000]
                last_err = {
                    "_parseError": True,
                    "kind": "non_json_response",
                    "attempt": attempt,
                    "status": r.status_code,
                    "text": txt,
                }
                if attempt < attempts:
                    time.sleep(0.6 * attempt)
                    continue
                return last_err
            return payload if isinstance(payload, dict) else {"data": payload}
        except Exception as e:
            last_err = {
                "_parseError": True,
                "kind": "request_exception",
                "attempt": attempt,
                "status": None,
                "text": f"{type(e).__name__}: {e}",
            }
            if attempt < attempts:
                time.sleep(0.8 * attempt)
                continue
            return last_err
    return last_err or {"_parseError": True, "kind": "unknown"}


class BigFutureListPageResult(NamedTuple):
    """Результат одной страницы list API: строки после локальных фильтров + метаданные."""

    rows: list[dict[str, Any]]
    api_row_count: int
    all_usable_rows_expired: bool


def fetch_list_page(
    page: int,
    query: str,
    stats: dict[str, int] | None = None,
) -> BigFutureListPageResult:
    """
    page: 1-based индекс страницы листинга API (from = (page-1)*LIST_PAGE_SIZE).
    query: фильтр подстроки по полям карточки (как доп. узкий поиск); пусто — все карточки страницы.
    stats: если передан, инкрементируется skip_prefilter_expired при отсеве по closeDate.

    api_row_count — число элементов в ответе API (len(data)); 0 значит реальный конец выдачи.
    all_usable_rows_expired — все карточки с slug/title отсеяны только по closeDate (для лога).
    """
    empty = BigFutureListPageResult([], 0, False)
    from_offset = max(0, (max(1, int(page)) - 1) * LIST_PAGE_SIZE)
    payload = _list_post_body(from_offset)
    raw = _post_scholarships_list(payload)
    if not isinstance(raw, dict):
        return empty
    if raw.get("_parseError"):
        print(f"  [bigfuture] list API parse error: {raw!r}")
        return empty
    if raw.get("message") and "data" not in raw:
        print(f"  [bigfuture] list API message: {raw.get('message')!r}")
        return empty

    rows = raw.get("data")
    if not isinstance(rows, list):
        return empty

    api_row_count = len(rows)
    network_meta = {
        "api_url": SCHOLARSHIPS_API,
        "method": "POST",
        "request_config": payload.get("config"),
        "totalHits": raw.get("totalHits"),
        "from": raw.get("from"),
    }

    q = (query or "").strip().lower()
    out: list[dict[str, Any]] = []
    n_usable = 0
    n_expired_among_usable = 0
    for item in rows:
        if not isinstance(item, dict):
            continue
        slug = (item.get("programTitleSlug") or "").strip()
        title = (item.get("programName") or "").strip()
        if not slug and not title:
            continue
        n_usable += 1
        if BIGFUTURE_ACTIVE_ONLY and _list_item_expired_by_close_date(item):
            n_expired_among_usable += 1
            if stats is not None:
                stats["skip_prefilter_expired"] = stats.get("skip_prefilter_expired", 0) + 1
            cd = item.get("closeDate")
            label = (title or slug or "?")[:70]
            print(f"  skip: skip_prefilter_expired — {label} (closeDate={cd!r})")
            continue
        blob = " ".join(
            [
                title,
                (item.get("programOrganizationName") or "") + " "
                + (item.get("blurb") or ""),
            ]
        ).lower()
        if q and q not in blob:
            continue
        sid = (item.get("cbScholarshipId") or "").strip() or slug
        url = _scholarship_url(slug)
        out.append(
            {
                "title": title or slug or "Scholarship",
                "url": url,
                "source_id": sid,
                "award_amount_text": _format_award_text(item.get("scholarshipMaximumAward")),
                "deadline_text": (str(item.get("closeDate")).strip() if item.get("closeDate") else None),
                "status_text": None,
                "institutions_text": (item.get("programOrganizationName") or None),
                "state_territory_text": None,
                "applicants_count": None,
                "credibility_score_text": None,
                "is_verified": False,
                "is_recurring": False,
                "requirements_count": None,
                "_list_extra": {
                    "snippet": (item.get("blurb") or "")[:2000] or None,
                    "raw_list_card": item,
                    "list_network_meta": network_meta,
                },
            }
        )
    all_usable_expired = (
        api_row_count > 0
        and len(out) == 0
        and n_usable > 0
        and n_expired_among_usable == n_usable
    )
    return BigFutureListPageResult(out, api_row_count, all_usable_expired)


def parse_list_item(card: dict[str, Any]) -> dict[str, Any]:
    extra = dict(card.get("_list_extra") or {})
    return {
        "title": card["title"],
        "url": card["url"],
        "source_id": card["source_id"],
        "award_amount_text": card.get("award_amount_text"),
        "deadline_text": card.get("deadline_text"),
        "status_text": card.get("status_text"),
        "institutions_text": card.get("institutions_text"),
        "state_territory_text": card.get("state_territory_text"),
        "applicants_count": card.get("applicants_count"),
        "credibility_score_text": card.get("credibility_score_text"),
        "is_verified": bool(card.get("is_verified")),
        "is_recurring": bool(card.get("is_recurring")),
        "requirements_count": card.get("requirements_count"),
        "_list_extra": extra,
    }


def fetch_detail_html(url: str) -> dict[str, Any]:
    page = _ensure_page()
    page.goto(url, wait_until="networkidle", timeout=BIGFUTURE_TIMEOUT_MS)
    html = page.content()
    soup = BeautifulSoup(html, "html.parser")
    next_data = _extract_next_data_props(html)
    full_content_html = _main_inner_html(soup, url)

    parts: list[str] = []
    if isinstance(next_data, dict):
        ap = (next_data.get("aboutPara") or "").strip()
        if ap:
            parts.append(ap)
        elig = next_data.get("eligibilityCriteriaDescriptions")
        if isinstance(elig, list):
            parts.extend(str(x).strip() for x in elig if str(x).strip())
    full_text = " ".join(parts).strip() or (soup.get_text(" ", strip=True) or "").strip()

    return {
        "full_text": full_text,
        "_sections": {},
        "page_url": url,
        "_apply_url_resolved": (
            str(next_data.get("applicationUrl")).strip()
            if isinstance(next_data, dict) and next_data.get("applicationUrl")
            else url
        ),
        "_provider_name_guess": (
            next_data.get("programOrgName")
            if isinstance(next_data, dict)
            else None
        ),
        "_extracted_deadline_text": (
            str(next_data.get("scholarshipDeadline")).strip()
            if isinstance(next_data, dict) and next_data.get("scholarshipDeadline")
            else None
        ),
        "_next_detail": next_data,
        "_raw_detail_html": html,
        "_full_content_html": full_content_html,
    }


def _combined_filter_blob(
    list_data: dict[str, Any],
    detail: dict[str, Any] | None,
) -> str:
    le = list_data.get("_list_extra") or {}
    raw = le.get("raw_list_card") if isinstance(le, dict) else None
    raw_blob = json.dumps(raw, ensure_ascii=False) if isinstance(raw, dict) else ""
    parts = [
        str(list_data.get("title") or ""),
        str(le.get("snippet") or ""),
        raw_blob,
        str(detail.get("full_text") or "") if detail else "",
    ]
    if detail and isinstance(detail.get("_next_detail"), dict):
        parts.append(json.dumps(detail["_next_detail"], ensure_ascii=False))
    return " ".join(parts).lower()


# Мягкие исключения: каталог уже про стипендии; отсекаем редкие явные non-student кейсы.
_BF_HARD_SKIP: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("faculty_only", re.compile(r"\b(?:faculty|staff)\s+only\b", re.I)),
    ("employees_only", re.compile(r"\bemployees only\b", re.I)),
    ("employee_assistance", re.compile(r"\bemployee assistance program\b", re.I)),
)


def passes_bigfuture_relevance(
    list_data: dict[str, Any],
    detail: dict[str, Any] | None,
    *,
    matched_filter: str = "",
) -> tuple[bool, str, dict[str, Any]]:
    blob_lc = _combined_filter_blob(list_data, detail)
    hits = [lbl for lbl, pat in _BF_HARD_SKIP if pat.search(blob_lc)]
    diag: dict[str, Any] = {
        "matched_filter": matched_filter,
        "matched_hard_skips": hits,
        "filter_path": "",
    }
    if hits:
        diag["filter_path"] = "skip_hard"
        return False, f"skip: bigfuture hard negative ({', '.join(hits)})", diag
    diag["filter_path"] = "ok_default"
    return True, "save: BigFuture scholarship catalog", diag


def parse_detail_from_html(
    detail: dict[str, Any] | None,
    page_url: str,
    list_data: dict[str, Any],
) -> dict[str, Any] | None:
    """Алиас к parse_detail_from_payload (тот же контракт, что у других source)."""
    return parse_detail_from_payload(detail, page_url, list_data)


def parse_detail_from_payload(
    detail: dict[str, Any] | None,
    page_url: str,
    list_data: dict[str, Any],
) -> dict[str, Any] | None:
    """
    Нормализует деталь (или только листинг) в поля, ожидаемые build_full_record.
    """
    le = list_data.get("_list_extra") or {}
    raw_card = le.get("raw_list_card") if isinstance(le, dict) else None

    nd: dict[str, Any] | None = None
    if detail and isinstance(detail.get("_next_detail"), dict):
        nd = detail["_next_detail"]

    if nd:
        title = (nd.get("scholarshipName") or list_data.get("title") or "").strip()
        provider = (nd.get("programOrgName") or list_data.get("institutions_text") or None)
        about = (nd.get("aboutPara") or "").strip()
        elig_lines = nd.get("eligibilityCriteriaDescriptions")
        elig_text = None
        if isinstance(elig_lines, list):
            elig_text = "\n".join(str(x).strip() for x in elig_lines if str(x).strip())
        amount = _format_award_text(nd.get("amountDisplay") or nd.get("maxAmountFormat"))
        apply_url = (str(nd.get("applicationUrl")).strip() if nd.get("applicationUrl") else page_url)
        dl = (str(nd.get("scholarshipDeadline")).strip() if nd.get("scholarshipDeadline") else None)

        req_parts: list[str] = []
        v1 = nd.get("awardVerificationCriteriaDescription")
        if isinstance(v1, str) and v1.strip():
            req_parts.append(v1.strip())
        v2 = nd.get("programSelfDescription")
        if isinstance(v2, str) and v2.strip():
            req_parts.append(v2.strip())
        requirements_text = "\n\n".join(req_parts) if req_parts else None

        fch = detail.get("_full_content_html") if detail else None
        full_content_html = fch if isinstance(fch, str) and fch.strip() else None

        return {
            "provider_name": provider,
            "provider_url": (str(nd.get("programUrl")).strip() if nd.get("programUrl") else None),
            "provider_mission": None,
            "description": about or None,
            "description_html": None,
            "requirements_text": requirements_text,
            "requirements_html": None,
            "winner_payment_text": None,
            "payment_html": None,
            "apply_url": apply_url or page_url,
            "apply_button_text": "Apply on provider site",
            "application_status_text": nd.get("scholarshipStatus"),
            "mark_started_available": False,
            "mark_submitted_available": False,
            "provider_social_facebook": None,
            "provider_social_instagram": None,
            "provider_social_linkedin": None,
            "category": None,
            "eligibility_text": elig_text,
            "eligibility_html": None,
            "awards_text": amount,
            "awards_html": None,
            "notification_text": None,
            "notification_html": None,
            "selection_criteria_text": None,
            "selection_criteria_html": None,
            "_support_email": None,
            "_support_phone": None,
            "full_content_html": full_content_html,
            "full_text": (detail.get("full_text") if detail else None) or about,
            "_parsed_deadline_text": dl,
            "_detail_extra": {"page_url": page_url},
            "award_amount_text": amount or list_data.get("award_amount_text"),
        }

    # Только листинг
    if isinstance(raw_card, dict):
        blurb = (raw_card.get("blurb") or "").strip()
        elig = (raw_card.get("eligibilityCriteriaDescription") or "").strip()
        award = _format_award_text(raw_card.get("scholarshipMaximumAward"))
        dl = (
            str(raw_card.get("closeDate")).strip() if raw_card.get("closeDate") else None
        )
        return {
            "provider_name": raw_card.get("programOrganizationName"),
            "provider_url": None,
            "provider_mission": None,
            "description": blurb or None,
            "description_html": None,
            "requirements_text": elig or None,
            "requirements_html": None,
            "winner_payment_text": None,
            "payment_html": None,
            "apply_url": page_url,
            "apply_button_text": "View on BigFuture",
            "application_status_text": None,
            "mark_started_available": False,
            "mark_submitted_available": False,
            "provider_social_facebook": None,
            "provider_social_instagram": None,
            "provider_social_linkedin": None,
            "category": None,
            "eligibility_text": elig or None,
            "eligibility_html": None,
            "awards_text": award,
            "awards_html": None,
            "notification_text": None,
            "notification_html": None,
            "selection_criteria_text": None,
            "selection_criteria_html": None,
            "_support_email": None,
            "_support_phone": None,
            "full_content_html": None,
            "full_text": blurb,
            "_parsed_deadline_text": dl,
            "_detail_extra": {"page_url": page_url},
            "award_amount_text": award or list_data.get("award_amount_text"),
        }

    return {
        "provider_name": list_data.get("institutions_text"),
        "provider_url": None,
        "provider_mission": None,
        "description": (le.get("snippet") if isinstance(le, dict) else None),
        "description_html": None,
        "requirements_text": None,
        "requirements_html": None,
        "winner_payment_text": None,
        "payment_html": None,
        "apply_url": page_url,
        "apply_button_text": "View on BigFuture",
        "application_status_text": None,
        "mark_started_available": False,
        "mark_submitted_available": False,
        "provider_social_facebook": None,
        "provider_social_instagram": None,
        "provider_social_linkedin": None,
        "category": None,
        "eligibility_text": None,
        "eligibility_html": None,
        "awards_text": list_data.get("award_amount_text"),
        "awards_html": None,
        "notification_text": None,
        "notification_html": None,
        "selection_criteria_text": None,
        "selection_criteria_html": None,
        "_support_email": None,
        "_support_phone": None,
        "full_content_html": None,
        "full_text": None,
        "_parsed_deadline_text": list_data.get("deadline_text"),
        "_detail_extra": {"page_url": page_url},
        "award_amount_text": list_data.get("award_amount_text"),
    }


def _count_req_lines(requirements_text: str | None) -> int | None:
    if not requirements_text or not requirements_text.strip():
        return None
    lines = [ln.strip() for ln in requirements_text.splitlines() if ln.strip()]
    return len(lines) if lines else None


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

    raw_detail_for_store: Any = None
    if isinstance(d.get("_next_detail"), dict):
        raw_detail_for_store = d.get("_next_detail")
    raw_html = d.pop("_raw_detail_html", None)
    d.pop("_next_detail", None)

    raw_data: dict[str, Any] = {
        "source_parser": SOURCE,
        "list": {k: list_data.get(k) for k in list_data if not str(k).startswith("_")},
        "list_extra": {
            k: v
            for k, v in list_extra.items()
            if k in ("snippet", "raw_list_card", "list_network_meta")
        },
        "detail": {
            k: v
            for k, v in d.items()
            if not str(k).startswith("_") and k != "award_amount_text"
        },
        "detail_raw_payload": raw_detail_for_store,
        "detail_raw_html_preview": (raw_html[:50_000] if isinstance(raw_html, str) else None),
        "sections": section_snapshot,
        "detail_error": detail_error,
    }
    if isinstance(filter_diag, dict) and filter_diag:
        raw_data["filter_diagnostics"] = filter_diag

    title = list_data.get("title") or "Untitled scholarship"
    url = list_data.get("url") or ""
    award_text = d.get("award_amount_text") or list_data.get("award_amount_text")
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
        "status_text": list_data.get("status_text"),
        "institutions_text": list_data.get("institutions_text"),
        "state_territory_text": list_data.get("state_territory_text"),
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

    for k in SCHOLARSHIP_RECORD_DEFAULT_KEYS:
        if k not in record:
            record[k] = None

    record["is_active"] = True
    record["currency"] = DEFAULT_CURRENCY
    record["source"] = SOURCE
    return record


def _listing_queries() -> tuple[str, ...]:
    raw = BIGFUTURE_KEYWORD
    if not raw:
        return ("",)
    parts = [p.strip().lower() for p in re.split(r"[|,]", raw) if p.strip()]
    return tuple(parts) if parts else ("",)


def _bump_bigfuture_prefilter_stat(stats: dict[str, int], fst: str) -> None:
    if fst == PREFILTER_PASS:
        stats["prefilter_fast_pass"] += 1
    elif fst == PREFILTER_REJECT_DEADLINE:
        stats["prefilter_fast_reject_deadline"] += 1
    elif fst == PREFILTER_REJECT_FUNDING:
        stats["prefilter_fast_reject_funding"] += 1
    elif fst == PREFILTER_REJECT_RELEVANCE:
        stats["prefilter_fast_reject_relevance"] += 1
    elif fst == PREFILTER_REVIEW:
        stats["prefilter_fast_review"] += 1


def _bigfuture_run_list_prefilter_scan(
    bf: BigFutureConfig,
    store: BigFuturePrefilterStore,
    stats: dict[str, int],
    idx: KnownScholarshipIndex,
    use_skip: bool,
    effective_target: int,
    seen_list_urls: set[str],
    seen_urls_session: set[str],
    *,
    list_page_cap: int,
    session_deep_queue_target: list[tuple[dict[str, Any], str]] | None,
    count_deep_queued: bool,
    queries: tuple[str, ...],
) -> tuple[int, str]:
    """List/API scan + fast prefilter + store upsert. No detail, no AI, no upsert_scholarship."""
    list_pages_loaded = 0
    stop_reason = ""

    for filter_q in queries:
        if stats["upsert_ok"] >= effective_target:
            stop_reason = stop_reason or "reached effective_target_upserts"
            break

        page = 1
        consecutive_pages_no_new = 0
        while page <= list_page_cap:
            if stats["upsert_ok"] >= effective_target:
                stop_reason = stop_reason or "reached effective_target_upserts"
                break

            print(f"[list] filter={filter_q!r} page={page}")
            try:
                lp = fetch_list_page(page, filter_q, stats)
            except Exception as e:
                print(f"  list fetch failed: {e}")
                stop_reason = stop_reason or f"list fetch failed page={page}"
                break

            list_pages_loaded += 1
            rows = lp.rows
            if lp.api_row_count == 0:
                print(
                    "  empty page (API returned no cards); stop listing for this filter"
                )
                break

            if not rows:
                if lp.all_usable_rows_expired:
                    print(
                        "  page filtered out (all expired), continue paging"
                    )
                else:
                    print(
                        "  page filtered out (no rows after local filters), "
                        "continue paging"
                    )
                page += 1
                continue

            new_on_this_page = 0
            for card_row in rows:
                if stats["upsert_ok"] >= effective_target:
                    break

                stats["list_rows_seen"] += 1
                title = str(card_row.get("title") or "")
                detail_url = str(card_row.get("url") or "")

                if detail_url in seen_list_urls:
                    print(
                        f"  row: {title[:70]} → duplicate URL this session, skip"
                    )
                    continue
                seen_list_urls.add(detail_url)

                pre_card = parse_list_item(card_row)
                known = bool(
                    use_skip
                    and listing_is_known(
                        pre_card,
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
                print(f"  row: {title[:70]} → new")

                fst, freason, ahint, cdate = classify_fast_prefilter(
                    card_row,
                    min_amount_hint=bf.min_amount_hint,
                )
                store.upsert_from_card_row(
                    card_row,
                    prefilter_status=fst,
                    prefilter_reason=freason,
                    amount_hint=ahint,
                    close_date=cdate,
                )
                _bump_bigfuture_prefilter_stat(stats, fst)

                if fst in (
                    PREFILTER_REJECT_DEADLINE,
                    PREFILTER_REJECT_FUNDING,
                    PREFILTER_REJECT_RELEVANCE,
                ):
                    print(f"  fast prefilter: {fst} — {freason}")
                    time.sleep(0.02)
                    continue

                deep_ok = fst == PREFILTER_PASS or (
                    fst == PREFILTER_REVIEW and bf.deep_include_review
                )
                if not deep_ok:
                    print(f"  fast prefilter: {fst} — {freason}")
                    time.sleep(0.02)
                    continue

                print(f"  fast prefilter: {fst} → queued for deep")
                if count_deep_queued:
                    stats["deep_queued"] += 1
                if session_deep_queue_target is not None:
                    session_deep_queue_target.append((card_row, filter_q))
                time.sleep(0.02)

            if stats["upsert_ok"] >= effective_target:
                stop_reason = stop_reason or "reached effective_target_upserts"
                break

            if new_on_this_page == 0:
                if use_skip:
                    consecutive_pages_no_new += 1
                    if consecutive_pages_no_new >= NO_NEW_PAGES_STOP:
                        print(
                            f"  {NO_NEW_PAGES_STOP} consecutive pages with no new rows "
                            f"for filter={filter_q!r}; next filter or end"
                        )
                        break
            else:
                consecutive_pages_no_new = 0

            page += 1

        if stop_reason and "list fetch failed" in stop_reason:
            break
        if stats["upsert_ok"] >= effective_target:
            break

    return list_pages_loaded, stop_reason


def _run_bigfuture_deep_for_card(
    card_row: dict[str, Any],
    filter_q: str,
    *,
    stats: dict[str, int],
    idx: KnownScholarshipIndex,
    use_skip: bool,
    effective_target: int,
    seen_urls_session: set[str],
    count_new_found: bool,
) -> None:
    """Detail → relevance → build_full_record → business filters → AI → upsert."""
    if stats["upsert_ok"] >= effective_target:
        return

    card = parse_list_item(card_row)
    title = str(card.get("title") or "")
    detail_url = str(card.get("url") or "")

    if detail_url in seen_urls_session:
        print(f"  row: {title[:70]} → duplicate URL this session, skip")
        return
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
        return

    if count_new_found:
        stats["new_found"] += 1
        print(f"  row: {title[:70]} → new")

    detail: dict[str, Any] | None = None
    detail_error: str | None = None
    if BIGFUTURE_DETAIL_FETCH:
        try:
            detail = fetch_detail_html(detail_url)
            stats["detail_fetched"] += 1
            print("  detail OK")
        except Exception as e:
            detail_error = str(e)
            print(f"  detail failed: {e}")
    else:
        print("  detail skipped (BIGFUTURE_DETAIL_FETCH=0)")

    ok_save, filter_reason, filter_diag = passes_bigfuture_relevance(
        card,
        detail,
        matched_filter=filter_q,
    )
    if not ok_save:
        stats["relevance_skipped"] += 1
        print(f"  skip: relevance — {filter_reason}")
        time.sleep(0.12)
        return

    card["_filter_diagnostics"] = filter_diag
    merged_detail = parse_detail_from_html(
        detail,
        detail_url,
        card,
    )
    record = build_full_record(card, merged_detail, detail_error)

    if not has_meaningful_funding(record):
        stats["skip_no_funding"] += 1
        print("  skip: business filter — no meaningful funding")
        time.sleep(0.12)
        return

    dbiz = classify_business_deadline(record.get("deadline_date"))
    if dbiz != "ok":
        if dbiz == "no_deadline":
            stats["skip_no_deadline"] += 1
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
        time.sleep(0.12)
        return

    record = ai_enrich_bigfuture_record_if_enabled(record)

    try:
        upsert_scholarship(record)
        stats["upsert_ok"] += 1
        has_levels = bool(record.get("study_levels"))
        has_fos = bool(record.get("field_of_study"))
        has_cit = bool(record.get("citizenship_statuses"))
        if has_levels:
            stats["taxonomy_study_levels_non_empty"] += 1
        if has_fos:
            stats["taxonomy_field_of_study_non_empty"] += 1
        if has_cit:
            stats["taxonomy_citizenship_non_empty"] += 1
        if not (has_levels or has_fos or has_cit):
            stats["taxonomy_all_empty"] += 1
        print(
            f"  upsert OK ({filter_reason}) "
            f"({stats['upsert_ok']}/{effective_target})"
        )
    except Exception as e:
        stats["upsert_failed"] += 1
        print(f"  upsert failed: {e}")

    time.sleep(0.12)


def _print_bigfuture_run_summary(
    *,
    stats: dict[str, int],
    bf: BigFutureConfig,
    list_pages_loaded: int,
    stop_reason: str,
    show_list_requests: bool,
) -> None:
    print("")
    if show_list_requests:
        print(f"processed list requests: {list_pages_loaded}")
    print(f"list rows seen: {stats['list_rows_seen']}")
    print(f"known skipped: {stats['known_skipped']}")
    print(f"new found: {stats['new_found']}")
    print(
        f"fast prefilter: pass={stats['prefilter_fast_pass']} "
        f"reject_deadline={stats['prefilter_fast_reject_deadline']} "
        f"reject_funding={stats['prefilter_fast_reject_funding']} "
        f"reject_relevance={stats['prefilter_fast_reject_relevance']} "
        f"review={stats['prefilter_fast_review']}"
    )
    print(
        f"deep: queued={stats['deep_queued']} processed={stats['deep_processed']}"
    )
    print(f"detail fetched: {stats['detail_fetched']}")
    print(f"relevance skipped: {stats['relevance_skipped']}")
    print(f"skip (list prefilter, expired closeDate): {stats['skip_prefilter_expired']}")
    print(f"skip (business): no funding: {stats['skip_no_funding']}")
    print(f"skip (business): no deadline: {stats['skip_no_deadline']}")
    print(f"skip (business): expired: {stats['skip_expired']}")
    print(
        "skip (business): deadline too close "
        f"(<{MIN_LEAD_DAYS_BEFORE_DEADLINE}d): {stats['skip_deadline_too_close']}"
    )
    print(f"upsert OK: {stats['upsert_ok']}")
    print(f"upsert failed: {stats['upsert_failed']}")
    print("taxonomy coverage (on upserted rows):")
    print(f"  study_levels non-empty: {stats['taxonomy_study_levels_non_empty']}")
    print(f"  field_of_study non-empty: {stats['taxonomy_field_of_study_non_empty']}")
    print(f"  citizenship_statuses non-empty: {stats['taxonomy_citizenship_non_empty']}")
    print(f"  all three empty: {stats['taxonomy_all_empty']}")
    print(f"stop reason: {stop_reason}")


def _run_bigfuture_auto_pipeline(
    bf: BigFutureConfig,
    *,
    store_path: str,
    store: BigFuturePrefilterStore,
    stats: dict[str, int],
    idx: KnownScholarshipIndex,
    use_skip: bool,
    effective_target: int,
    queries: tuple[str, ...],
) -> None:
    print("[BigFuture] AUTO PIPELINE ENABLED")
    if bf.fast_prefilter_only or bf.deep_pass_only:
        print(
            "[BigFuture] note: AUTO_PIPELINE=1 — BIGFUTURE_FAST_PREFILTER_ONLY / "
            "BIGFUTURE_DEEP_PASS_ONLY are ignored for this run"
        )

    seen_list_urls: set[str] = set()
    seen_urls_session: set[str] = set()
    stop_reason = ""
    list_page_cap = bf.fast_max_pages if bf.fast_max_pages > 0 else MAX_LIST_PAGES

    print("[BigFuture] PHASE 1/2 — FAST PREFILTER START")
    print(
        f"  list page cap: {list_page_cap} "
        f"(BIGFUTURE_FAST_MAX_PAGES={bf.fast_max_pages}, 0 → MAX_LIST_PAGES={MAX_LIST_PAGES})"
    )

    phase1_ok = False
    list_pages_loaded = 0
    try:
        list_pages_loaded, stop_reason = _bigfuture_run_list_prefilter_scan(
            bf,
            store,
            stats,
            idx,
            use_skip,
            effective_target,
            seen_list_urls,
            seen_urls_session,
            list_page_cap=list_page_cap,
            session_deep_queue_target=None,
            count_deep_queued=False,
            queries=queries,
        )
        store.save()
        phase1_ok = True
    except Exception as e:
        print(f"[BigFuture] PHASE 1/2 — FAST PREFILTER failed: {e}")
        traceback.print_exc()

    print("[BigFuture] PHASE 1/2 — FAST PREFILTER END")
    print(f"  scanned pages: {list_pages_loaded}")
    print(f"  rows seen: {stats['list_rows_seen']}")
    print(f"  pass: {stats['prefilter_fast_pass']}")
    print(f"  reject_deadline: {stats['prefilter_fast_reject_deadline']}")
    print(f"  reject_funding: {stats['prefilter_fast_reject_funding']}")
    print(f"  reject_relevance: {stats['prefilter_fast_reject_relevance']}")
    print(f"  review: {stats['prefilter_fast_review']}")
    print(f"  store path: {store_path}")

    deep_skip_msg: str | None = None
    if not phase1_ok:
        deep_skip_msg = "Deep phase skipped: fast prefilter failed"
    elif not os.path.isfile(store_path):
        deep_skip_msg = "Deep phase skipped: prefilter store file missing after phase 1"
    else:
        candidates = list(
            store.iter_deep_candidates(
                recheck_reject_days=bf.recheck_reject_days,
                include_review=bf.deep_include_review,
            )
        )
        if bf.deep_max_items > 0:
            candidates = candidates[: bf.deep_max_items]
        n_queued = len(candidates)
        if n_queued == 0:
            deep_skip_msg = (
                "Deep phase skipped: no candidates from fast prefilter "
                "(empty shortlist / nothing eligible for deep)"
            )
        else:
            stats["deep_queued"] = n_queued
            d0 = stats["detail_fetched"]
            p0 = stats["deep_processed"]
            uo0 = stats["upsert_ok"]
            uf0 = stats["upsert_failed"]
            print(
                "[BigFuture] PHASE 2/2 — DEEP PASS START "
                f"({n_queued} candidate(s); BIGFUTURE_DEEP_MAX_ITEMS cap="
                f"{bf.deep_max_items or 'off'})"
            )
            for entry in candidates:
                if stats["upsert_ok"] >= effective_target:
                    stop_reason = stop_reason or "reached effective_target_upserts"
                    break
                snap = entry.get("card_row_snapshot")
                if not isinstance(snap, dict):
                    print("  skip: entry without card_row_snapshot")
                    continue
                stats["deep_processed"] += 1
                _run_bigfuture_deep_for_card(
                    snap,
                    "",
                    stats=stats,
                    idx=idx,
                    use_skip=use_skip,
                    effective_target=effective_target,
                    seen_urls_session=seen_urls_session,
                    count_new_found=True,
                )
            store.save()
            print("[BigFuture] PHASE 2/2 — DEEP PASS END")
            print(f"  queued: {n_queued}")
            print(f"  processed: {stats['deep_processed'] - p0}")
            print(f"  detail fetched: {stats['detail_fetched'] - d0}")
            print(f"  upsert OK: {stats['upsert_ok'] - uo0}")
            print(f"  upsert failed: {stats['upsert_failed'] - uf0}")

    if deep_skip_msg:
        print(f"[BigFuture] {deep_skip_msg}")

    if not stop_reason:
        stop_reason = "auto pipeline (phase 1–2) completed" if phase1_ok else "auto pipeline stopped"
    _print_bigfuture_run_summary(
        stats=stats,
        bf=bf,
        list_pages_loaded=list_pages_loaded,
        stop_reason=stop_reason,
        show_list_requests=True,
    )


def run() -> None:
    bf = get_bigfuture_config()
    if not bf.auto_pipeline and bf.fast_prefilter_only and bf.deep_pass_only:
        print(
            f"{SOURCE}: error: BIGFUTURE_FAST_PREFILTER_ONLY and BIGFUTURE_DEEP_PASS_ONLY "
            "cannot both be enabled"
        )
        return

    store_path = (bf.prefilter_store_path or "").strip() or os.path.join(
        _PARSER_ROOT,
        ".bigfuture_prefilter_store.json",
    )
    store = BigFuturePrefilterStore(store_path)
    store.load()

    stats: dict[str, int] = {
        "list_rows_seen": 0,
        "known_skipped": 0,
        "new_found": 0,
        "detail_fetched": 0,
        "relevance_skipped": 0,
        "skip_prefilter_expired": 0,
        "skip_no_funding": 0,
        "skip_no_deadline": 0,
        "skip_expired": 0,
        "skip_deadline_too_close": 0,
        "upsert_ok": 0,
        "upsert_failed": 0,
        "taxonomy_study_levels_non_empty": 0,
        "taxonomy_field_of_study_non_empty": 0,
        "taxonomy_citizenship_non_empty": 0,
        "taxonomy_all_empty": 0,
        "prefilter_fast_pass": 0,
        "prefilter_fast_reject_deadline": 0,
        "prefilter_fast_reject_funding": 0,
        "prefilter_fast_reject_relevance": 0,
        "prefilter_fast_review": 0,
        "deep_queued": 0,
        "deep_processed": 0,
    }
    seen_list_urls: set[str] = set()
    seen_urls_session: set[str] = set()
    stop_reason = ""
    use_skip = SKIP_EXISTING_ON_LIST and DISCOVERY_MODE == "new_only"
    list_pages_loaded = 0

    cap_dbg = BIGFUTURE_MAX_RECORDS_DEBUG
    effective_target = (
        min(TARGET_NEW_ITEMS, cap_dbg) if cap_dbg > 0 else TARGET_NEW_ITEMS
    )

    queries = _listing_queries()
    list_page_cap = bf.fast_max_pages if bf.fast_max_pages > 0 else MAX_LIST_PAGES
    print(
        f"{SOURCE}: direct HTTP list API + Playwright detail "
        f"(TARGET_NEW_ITEMS={TARGET_NEW_ITEMS}, effective_target_upserts={effective_target}, "
        f"BIGFUTURE_MAX_RECORDS_DEBUG={cap_dbg} (0=unlimited), "
        f"MAX_LIST_PAGES={MAX_LIST_PAGES}, LIST_PAGE_CAP={list_page_cap}, "
        f"NO_NEW_PAGES_STOP={NO_NEW_PAGES_STOP}, "
        f"DETAIL_FETCH={BIGFUTURE_DETAIL_FETCH}, ACTIVE_ONLY={BIGFUTURE_ACTIVE_ONLY}, "
        f"KEYWORD_FILTERS={queries!r}, "
        f"AI_ENRICH={bigfuture_ai_enrich_enabled()}, "
        f"AUTO_PIPELINE={bf.auto_pipeline}, "
        f"FAST_PREFILTER_ONLY={bf.fast_prefilter_only}, DEEP_PASS_ONLY={bf.deep_pass_only}, "
        f"FAST_MAX_PAGES={bf.fast_max_pages}, DEEP_MAX_ITEMS={bf.deep_max_items}, "
        f"MIN_AMOUNT_HINT={bf.min_amount_hint}, RECHECK_REJECT_D={bf.recheck_reject_days}, "
        f"PREFILTER_STORE={store_path!r})"
    )

    try:
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

        if bf.auto_pipeline:
            _run_bigfuture_auto_pipeline(
                bf,
                store_path=store_path,
                store=store,
                stats=stats,
                idx=idx,
                use_skip=use_skip,
                effective_target=effective_target,
                queries=queries,
            )
        else:
            session_deep_queue: list[tuple[dict[str, Any], str]] = []

            if bf.deep_pass_only:
                _ensure_page()
                candidates = list(
                    store.iter_deep_candidates(
                        recheck_reject_days=bf.recheck_reject_days,
                        include_review=bf.deep_include_review,
                    )
                )
                if bf.deep_max_items > 0:
                    candidates = candidates[: bf.deep_max_items]
                stats["deep_queued"] = len(candidates)
                print(f"[deep-only] {len(candidates)} store entries eligible for deep pass")
                for entry in candidates:
                    if stats["upsert_ok"] >= effective_target:
                        stop_reason = stop_reason or "reached effective_target_upserts"
                        break
                    snap = entry.get("card_row_snapshot")
                    if not isinstance(snap, dict):
                        print("  skip: deep-only entry without card_row_snapshot")
                        continue
                    stats["deep_processed"] += 1
                    _run_bigfuture_deep_for_card(
                        snap,
                        "",
                        stats=stats,
                        idx=idx,
                        use_skip=use_skip,
                        effective_target=effective_target,
                        seen_urls_session=seen_urls_session,
                        count_new_found=True,
                    )
                store.save()
            else:
                queue_target: list[tuple[dict[str, Any], str]] | None = (
                    None if bf.fast_prefilter_only else session_deep_queue
                )
                list_pages_loaded, stop_reason = _bigfuture_run_list_prefilter_scan(
                    bf,
                    store,
                    stats,
                    idx,
                    use_skip,
                    effective_target,
                    seen_list_urls,
                    seen_urls_session,
                    list_page_cap=list_page_cap,
                    session_deep_queue_target=queue_target,
                    count_deep_queued=True,
                    queries=queries,
                )

                store.save()

                if bf.fast_prefilter_only:
                    stop_reason = stop_reason or "fast prefilter only (no deep pass)"
                else:
                    print(f"\n[deep] processing {len(session_deep_queue)} queued card(s)")
                    for card_row, filter_q in session_deep_queue:
                        if stats["upsert_ok"] >= effective_target:
                            stop_reason = stop_reason or "reached effective_target_upserts"
                            break
                        stats["deep_processed"] += 1
                        _run_bigfuture_deep_for_card(
                            card_row,
                            filter_q,
                            stats=stats,
                            idx=idx,
                            use_skip=use_skip,
                            effective_target=effective_target,
                            seen_urls_session=seen_urls_session,
                            count_new_found=False,
                        )
                    store.save()

            if not stop_reason:
                stop_reason = "ended (filters / page limits)"

            _print_bigfuture_run_summary(
                stats=stats,
                bf=bf,
                list_pages_loaded=list_pages_loaded,
                stop_reason=stop_reason,
                show_list_requests=not bf.deep_pass_only,
            )
    finally:
        _close_list_http_session()
        _close_playwright()


if __name__ == "__main__":
    run()
