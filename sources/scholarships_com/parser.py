"""
Scholarships.com parser -> public.scholarships (Supabase).

Pipeline:
1) Open authenticated Scholarships.com pages (storage_state supported).
2) Discover workable routes and save crawl rules JSON.
3) Capture JSON/XHR responses and DOM listings.
4) Run the same business filters/normalization/upsert pipeline as other sources.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

_PARSER_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PARSER_ROOT not in sys.path:
    sys.path.insert(0, _PARSER_ROOT)

from dotenv import load_dotenv

load_dotenv(os.path.join(_PARSER_ROOT, ".env"))
load_dotenv(os.path.join(os.path.dirname(_PARSER_ROOT), ".env"))

from ai_monitoring import print_ai_session_summary, record_ai_skip, snapshot_ai_usage
from business_filters import (
    MIN_LEAD_DAYS_BEFORE_DEADLINE,
    classify_business_deadline,
    has_meaningful_funding,
)
from config import get_global_config
from international_signals import detect_international_signal
from normalize_scholarship import apply_normalization
from scholarship_db_columns import (
    SCHOLARSHIP_RECORD_DEFAULT_KEYS,
    SCHOLARSHIP_UPSERT_BODY_KEYS,
)
from sources.scholarship_america.parser import parse_award_min_max, parse_deadline_date
from sources.shared_ai_enrichment import json_safe as _json_safe
from sources.scholarships_com.prefilter import (
    PREFILTER_PASS,
    PREFILTER_REJECT_DEADLINE,
    PREFILTER_REJECT_FUNDING,
    PREFILTER_REJECT_KNOWN,
    PREFILTER_REJECT_MAPPING,
    ScholarshipsComPrefilterStore,
)
from utils import (
    KnownScholarshipIndex,
    get_client,
    listing_is_known,
    load_known_scholarship_index,
    upsert_scholarship,
)

SOURCE = "scholarships_com"
DEFAULT_CURRENCY = "USD"
SITE_ORIGIN = "https://www.scholarships.com"
MATCHES_URL = "https://www.scholarships.com/scholarshipmatches"
DIRECTORY_URL = "https://www.scholarships.com/financial-aid/college-scholarships/scholarship-directory"
SESSION_STATE_PATH = os.path.join(_PARSER_ROOT, "scholarships_com_session.json")
ROUTES_RULES_PATH = os.path.join(_PARSER_ROOT, ".scholarships_com_routes.json")
DISCOVERY_CHECKPOINT_PATH = os.path.join(_PARSER_ROOT, ".scholarships_com_discovery_checkpoint.json")
DETAIL_CHECKPOINT_PATH = os.path.join(_PARSER_ROOT, ".scholarships_com_detail_checkpoint.json")
PREFILTER_STORE_PATH_DEFAULT = os.path.join(_PARSER_ROOT, ".scholarships_com_prefilter_store.json")

_CAPTCHA_MARKERS: tuple[str, ...] = (
    "captcha",
    "recaptcha",
    "hcaptcha",
    "verify you are human",
    "verify you're human",
    "cloudflare",
    "challenge",
    "access denied",
    "forbidden",
    "unusual traffic",
)

_ROUTE_HINT_RE = re.compile(
    r"(scholarship|scholarships|grant|grants|financial-aid|scholarship-directory|matches)",
    re.I,
)
_GRANT_URL_RE = re.compile(
    r"/financial-aid/college-scholarships/scholarship-directory(?:/|$)|/scholarships/",
    re.I,
)
_INTERNATIONAL_STUDENT_RE = re.compile(
    r"("
    r"international\s+(?:undergraduate|graduate|degree[-\s]?seeking\s+)?students?"
    r"|foreign\s+students?"
    r"|non[-\s]?u\.?s\.?\s+citizens?"
    r"|not\s+(?:a\s+)?u\.?s\.?\s+citizen"
    r"|f[-\s]?1\s+visa"
    r"|student\s+visa"
    r"|students?\s+(?:from|outside)\s+(?:the\s+)?u\.?s\.?"
    r"|open\s+(?:in|to)\s+(?:the\s+)?u\.?s\.?/canada"
    r"|european\s+countries"
    r")",
    re.I,
)
_INTERNATIONAL_PRIORITY_SEEDS: tuple[str, ...] = (
    "https://www.scholarships.com/financial-aid/college-scholarships/scholarship-directory/academic-major/international-affairs",
    "https://www.scholarships.com/financial-aid/college-scholarships/scholarship-directory/academic-major/international-business",
    "https://www.scholarships.com/financial-aid/college-scholarships/scholarship-directory/student-organization/international-students-organization",
    "https://www.scholarships.com/scholarships/central-washington-university-international-student-scholarship",
    "https://www.scholarships.com/scholarships/international-loper-scholarship",
    "https://www.scholarships.com/scholarships/tocris-scholarship-program",
)


def _clean_scholarship_title(raw_title: Any) -> str | None:
    title = _clean_text(raw_title)
    if not title:
        return None
    # Remove common source suffixes like " - Scholarships.com".
    title = re.sub(r"\s*[-|/]\s*scholarships\.com\s*$", "", title, flags=re.I).strip()
    # Some pages can leak source words without domain.
    title = re.sub(r"\s*[-|/]\s*scholarships\s*$", "", title, flags=re.I).strip()
    return title or None


def _get_bool_env(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _get_int_env(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _get_str_env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


_gc = get_global_config()
TARGET_NEW_ITEMS = _gc.target_new_items
SKIP_EXISTING_ON_LIST = _gc.skip_existing_on_list
USE_TITLE_FALLBACK_KNOWN = _gc.use_title_fallback_known
DISCOVERY_MODE = _gc.discovery_mode

SCHOLARSHIPS_COM_ENABLED = _get_bool_env("SCHOLARSHIPS_COM_ENABLED", True)
SCHOLARSHIPS_COM_EMAIL = _get_str_env("SCHOLARSHIPS_COM_EMAIL")
SCHOLARSHIPS_COM_PASSWORD = _get_str_env("SCHOLARSHIPS_COM_PASSWORD")
SCHOLARSHIPS_COM_HEADLESS = _get_bool_env("SCHOLARSHIPS_COM_HEADLESS", True)
SCHOLARSHIPS_COM_TIMEOUT_MS = _get_int_env("SCHOLARSHIPS_COM_TIMEOUT_MS", 120_000)
SCHOLARSHIPS_COM_SCROLL_WAIT_MS = max(500, _get_int_env("SCHOLARSHIPS_COM_SCROLL_WAIT_MS", 1500))
SCHOLARSHIPS_COM_SCROLL_STEPS = max(0, _get_int_env("SCHOLARSHIPS_COM_SCROLL_STEPS", 6))
SCHOLARSHIPS_COM_MAX_DISCOVERY_PAGES = max(1, _get_int_env("SCHOLARSHIPS_COM_MAX_DISCOVERY_PAGES", 40))
SCHOLARSHIPS_COM_MAX_LISTING_PAGES = max(1, _get_int_env("SCHOLARSHIPS_COM_MAX_LISTING_PAGES", 120))
SCHOLARSHIPS_COM_MAX_DETAIL_PAGES = max(1, _get_int_env("SCHOLARSHIPS_COM_MAX_DETAIL_PAGES", 1500))
SCHOLARSHIPS_COM_AUTH_WAIT_SECONDS = max(30, _get_int_env("SCHOLARSHIPS_COM_AUTH_WAIT_SECONDS", 240))
SCHOLARSHIPS_COM_KEEP_BROWSER_OPEN = _get_bool_env("SCHOLARSHIPS_COM_KEEP_BROWSER_OPEN", False)
SCHOLARSHIPS_COM_MAX_RECORDS_DEBUG = max(0, _get_int_env("SCHOLARSHIPS_COM_MAX_RECORDS_DEBUG", 0))
SCHOLARSHIPS_COM_PREFILTER_STORE_PATH = _get_str_env(
    "SCHOLARSHIPS_COM_PREFILTER_STORE_PATH",
    PREFILTER_STORE_PATH_DEFAULT,
)
SCHOLARSHIPS_COM_FORCE_REFRESH = _get_bool_env("SCHOLARSHIPS_COM_FORCE_REFRESH", False)
SCHOLARSHIPS_COM_SKIP_DISCOVERY = _get_bool_env("SCHOLARSHIPS_COM_SKIP_DISCOVERY", False)
SCHOLARSHIPS_COM_DISCOVERY_CHECKPOINT = _get_bool_env("SCHOLARSHIPS_COM_DISCOVERY_CHECKPOINT", True)
SCHOLARSHIPS_COM_DETAIL_CHECKPOINT = _get_bool_env("SCHOLARSHIPS_COM_DETAIL_CHECKPOINT", True)
SCHOLARSHIPS_COM_RUN_MODE = (_get_str_env("SCHOLARSHIPS_COM_RUN_MODE", "full") or "full").lower()
SCHOLARSHIPS_COM_ONLY_INTERNATIONAL = _get_bool_env("SCHOLARSHIPS_COM_ONLY_INTERNATIONAL", False)
SCHOLARSHIPS_COM_DISCOVERY_BATCH_PAGES = max(
    0,
    _get_int_env("SCHOLARSHIPS_COM_DISCOVERY_BATCH_PAGES", 0),
)
SCHOLARSHIPS_COM_MAX_DETAIL_SCANS_PER_RUN = max(
    0,
    _get_int_env("SCHOLARSHIPS_COM_MAX_DETAIL_SCANS_PER_RUN", 0),
)
if SCHOLARSHIPS_COM_ONLY_INTERNATIONAL:
    ROUTES_RULES_PATH = _get_str_env(
        "SCHOLARSHIPS_COM_ROUTE_RULES_PATH",
        os.path.join(_PARSER_ROOT, ".scholarships_com_international_routes.json"),
    )
    DISCOVERY_CHECKPOINT_PATH = _get_str_env(
        "SCHOLARSHIPS_COM_DISCOVERY_CHECKPOINT_PATH",
        os.path.join(_PARSER_ROOT, ".scholarships_com_international_discovery_checkpoint.json"),
    )
    DETAIL_CHECKPOINT_PATH = _get_str_env(
        "SCHOLARSHIPS_COM_DETAIL_CHECKPOINT_PATH",
        os.path.join(_PARSER_ROOT, ".scholarships_com_international_detail_checkpoint.json"),
    )
    if SCHOLARSHIPS_COM_PREFILTER_STORE_PATH == PREFILTER_STORE_PATH_DEFAULT:
        SCHOLARSHIPS_COM_PREFILTER_STORE_PATH = os.path.join(
            _PARSER_ROOT,
            ".scholarships_com_international_prefilter_store.json",
        )
else:
    # "Full" run can still point at a large shared routes JSON and its own detail checkpoint
    # (e.g. same queue as international, different DETAIL_CHECKPOINT to avoid clobbering the intl worker).
    _rr_full = _get_str_env("SCHOLARSHIPS_COM_ROUTE_RULES_PATH")
    if _rr_full:
        ROUTES_RULES_PATH = _rr_full
    _cp_disc = _get_str_env("SCHOLARSHIPS_COM_DISCOVERY_CHECKPOINT_PATH")
    if _cp_disc:
        DISCOVERY_CHECKPOINT_PATH = _cp_disc
    _cp_det = _get_str_env("SCHOLARSHIPS_COM_DETAIL_CHECKPOINT_PATH")
    if _cp_det:
        DETAIL_CHECKPOINT_PATH = _cp_det


def _log(msg: str) -> None:
    print(msg, flush=True)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        out = " ".join(value.split()).strip()
        return out or None
    if isinstance(value, (int, float)) and value == value:
        return str(value)
    return None


def _strip_html(value: Any) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    text = re.sub(r"</(p|div|li|tr|h[1-6]|br)>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = " ".join(text.split()).strip()
    return text or None


def _to_absolute_url(value: str | None) -> str | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.startswith(("http://", "https://")):
        return raw
    if raw.startswith("//"):
        return f"https:{raw}"
    return urljoin(SITE_ORIGIN, raw)


def _is_login_or_block_page(page: Any) -> bool:
    current_url = ""
    try:
        current_url = str(page.url or "").lower()
    except Exception:
        current_url = ""

    # Logged-in signal: once we are on scholarshipmatches, treat auth as successful.
    # The site can keep hidden login inputs in DOM templates, so field checks are unreliable.
    if "scholarshipmatches" in current_url:
        return False

    parts = []
    try:
        parts.append(str(page.url or ""))
    except Exception:
        pass
    try:
        parts.append(str(page.title() or ""))
    except Exception:
        pass
    try:
        parts.append(str(page.content() or "")[:50_000])
    except Exception:
        pass
    blob = "\n".join(parts).lower()
    if any(m in blob for m in _CAPTCHA_MARKERS):
        return True
    if any(x in blob for x in ("sign in", "log in", "login", "create account")):
        return True
    return False


def _wait_for_manual_auth(page: Any) -> None:
    _log(
        f"{SOURCE}: please finish login/captcha manually in the opened browser. "
        f"waiting up to {SCHOLARSHIPS_COM_AUTH_WAIT_SECONDS}s..."
    )
    deadline = time.time() + SCHOLARSHIPS_COM_AUTH_WAIT_SECONDS
    while time.time() < deadline:
        if not _is_login_or_block_page(page):
            _log(f"{SOURCE}: manual auth confirmed, continuing crawl")
            return
        page.wait_for_timeout(1500)
    raise RuntimeError("Manual auth timeout: still blocked/login page after wait window.")


def _safe_fill_first(page: Any, selectors: tuple[str, ...], value: str) -> bool:
    for selector in selectors:
        try:
            node = page.query_selector(selector)
            if node is None:
                continue
            node.fill(value, timeout=900)
            return True
        except Exception:
            continue
    return False


def _safe_click_first(page: Any, selectors: tuple[str, ...]) -> bool:
    for selector in selectors:
        try:
            node = page.query_selector(selector)
            if node is None:
                continue
            node.click(timeout=900)
            return True
        except Exception:
            continue
    return False


def _attempt_auto_login(page: Any) -> bool:
    if not SCHOLARSHIPS_COM_EMAIL or not SCHOLARSHIPS_COM_PASSWORD:
        _log(f"{SOURCE}: auto-login skipped (credentials are empty)")
        return False
    _log(f"{SOURCE}: attempting auto-login on current page...")
    _safe_click_first(
        page,
        (
            'button:has-text("Student Log In")',
            'a:has-text("Student Log In")',
            'button:has-text("Log In")',
            'a:has-text("Log In")',
        ),
    )
    page.wait_for_timeout(500)
    email_ok = _safe_fill_first(
        page,
        (
            'input[type="email"]',
            'input[name="email"]',
            'input[id*="email" i]',
            'input[autocomplete="email"]',
            'input[placeholder*="email" i]',
        ),
        SCHOLARSHIPS_COM_EMAIL,
    )
    password_ok = _safe_fill_first(
        page,
        (
            'input[type="password"]',
            'input[name="password"]',
            'input[id*="password" i]',
            'input[autocomplete="current-password"]',
            'input[placeholder*="password" i]',
        ),
        SCHOLARSHIPS_COM_PASSWORD,
    )
    if not email_ok or not password_ok:
        _log(f"{SOURCE}: auto-login form fields not found (email_ok={email_ok}, password_ok={password_ok})")
        return False
    _log(f"{SOURCE}: auto-login fields filled, trying submit...")
    clicked = _safe_click_first(
        page,
        (
            'button[type="submit"]',
            'input[type="submit"]',
            'button:has-text("Sign In")',
            'button:has-text("Log In")',
            'a:has-text("Student Log In")',
            'button:has-text("Login")',
            'button:has-text("Continue")',
        ),
    )
    if clicked:
        _log(f"{SOURCE}: submit clicked, waiting for redirect...")
        page.wait_for_timeout(2500)
    else:
        _log(f"{SOURCE}: submit control not found, trying Enter/form submit fallback...")
        submitted = False
        try:
            pwd = page.query_selector('input[type="password"]')
            if pwd is not None:
                pwd.press("Enter", timeout=900)
                submitted = True
        except Exception:
            pass
        if not submitted:
            try:
                submitted = bool(
                    page.evaluate(
                        """() => {
                            const form = document.querySelector('form');
                            if (!form) return false;
                            if (typeof form.requestSubmit === 'function') form.requestSubmit();
                            else form.submit();
                            return true;
                        }"""
                    )
                )
            except Exception:
                submitted = False
        if submitted:
            _log(f"{SOURCE}: fallback submit sent, waiting for redirect...")
            page.wait_for_timeout(2500)
            return True
        _log(f"{SOURCE}: fallback submit failed, manual click may be required")
    return clicked


def _ensure_authenticated(context: Any, page: Any) -> None:
    _log(f"{SOURCE}: opening matches page for auth check...")
    page.goto(MATCHES_URL, wait_until="domcontentloaded")
    page.wait_for_timeout(1200)
    if _is_login_or_block_page(page):
        _log(f"{SOURCE}: login/challenge page detected")
        _attempt_auto_login(page)
        page.wait_for_timeout(1000)
        _wait_for_manual_auth(page)
    else:
        _log(f"{SOURCE}: session already authenticated")
    try:
        context.storage_state(path=SESSION_STATE_PATH)
        _log(f"{SOURCE}: saved session state -> {SESSION_STATE_PATH}")
    except Exception as exc:
        _log(f"{SOURCE}: warning: could not save session state ({exc})")


def _normalize_key(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(name or "").strip().lower())


def _iter_direct_values(obj: Any, keys: tuple[str, ...]) -> list[Any]:
    if not isinstance(obj, dict):
        return []
    wanted = {_normalize_key(k) for k in keys}
    out: list[Any] = []
    for key, value in obj.items():
        if _normalize_key(key) in wanted:
            out.append(value)
    return out


def _iter_recursive_values(obj: Any, keys: tuple[str, ...], max_depth: int = 5) -> list[Any]:
    if max_depth < 0:
        return []
    wanted = {_normalize_key(k) for k in keys}
    out: list[Any] = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            if _normalize_key(key) in wanted:
                out.append(value)
            if isinstance(value, (dict, list)):
                out.extend(_iter_recursive_values(value, keys, max_depth=max_depth - 1))
    elif isinstance(obj, list):
        for value in obj[:250]:
            if isinstance(value, (dict, list)):
                out.extend(_iter_recursive_values(value, keys, max_depth=max_depth - 1))
    return out


def _first_value(obj: Any, keys: tuple[str, ...]) -> Any:
    direct = _iter_direct_values(obj, keys)
    if direct:
        return direct[0]
    recursive = _iter_recursive_values(obj, keys)
    return recursive[0] if recursive else None


def _first_str(obj: Any, keys: tuple[str, ...]) -> str | None:
    values = _iter_direct_values(obj, keys) + _iter_recursive_values(obj, keys)
    for value in values:
        if isinstance(value, str):
            text = _clean_text(value)
            if text:
                return text
        elif isinstance(value, (int, float)) and value == value:
            return str(value)
    return None


def _slug_from_url(url: str | None) -> str | None:
    if not url:
        return None
    path = urlparse(url).path.strip("/")
    if not path:
        return None
    parts = [part for part in path.split("/") if part]
    return parts[-1] if parts else None


def _search_scholarship_url(node: Any, max_depth: int = 5) -> str | None:
    if max_depth < 0:
        return None
    if isinstance(node, str):
        text = node.strip()
        if _ROUTE_HINT_RE.search(text) and "/" in text:
            return _to_absolute_url(text)
        return None
    if isinstance(node, dict):
        for value in node.values():
            hit = _search_scholarship_url(value, max_depth=max_depth - 1)
            if hit:
                return hit
    elif isinstance(node, list):
        for value in node[:120]:
            hit = _search_scholarship_url(value, max_depth=max_depth - 1)
            if hit:
                return hit
    return None


def _candidate_url(item: dict[str, Any]) -> str | None:
    raw = _first_str(item, _URL_KEYS)
    url = _to_absolute_url(raw)
    if url:
        return url
    slug = _first_str(item, _SLUG_KEYS)
    if slug:
        return _to_absolute_url(f"/{slug.strip('/')}/")
    return _search_scholarship_url(item)


def _candidate_source_id(item: dict[str, Any], url: str | None) -> str | None:
    direct = _first_str(item, _ID_KEYS)
    if direct:
        return direct
    slug = _first_str(item, _SLUG_KEYS)
    if slug:
        return slug
    return _slug_from_url(url)


def _parse_iso_dateish(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        m = re.search(r"(\d{4}-\d{2}-\d{2})", raw)
        if m:
            return m.group(1)
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return parsed.date().isoformat()
        except ValueError:
            return None
    if isinstance(value, (int, float)) and value == value:
        try:
            num = float(value)
            if num > 10_000_000_000:
                num = num / 1000.0
            dt = datetime.fromtimestamp(num, tz=timezone.utc)
            return dt.date().isoformat()
        except Exception:
            return None
    return None


def _candidate_deadline_text(item: dict[str, Any]) -> str | None:
    text = _first_str(item, _DEADLINE_TEXT_KEYS)
    if text:
        return text
    date_like = _first_value(item, _DEADLINE_DATE_KEYS)
    return _parse_iso_dateish(date_like)


def _candidate_deadline_date(item: dict[str, Any], deadline_text: str | None) -> str | None:
    direct = _first_value(item, _DEADLINE_DATE_KEYS)
    iso = _parse_iso_dateish(direct)
    if iso:
        return iso
    return parse_deadline_date(deadline_text)


def _candidate_award_text(item: dict[str, Any]) -> str | None:
    text = _first_str(item, _AMOUNT_TEXT_KEYS)
    if text:
        return text
    value = _first_value(item, _AMOUNT_VALUE_KEYS)
    if value is None:
        return None
    if isinstance(value, (int, float)) and value == value:
        if value <= 0:
            return None
        return f"${int(value):,}" if float(value).is_integer() else f"${float(value):,.2f}"
    if isinstance(value, str):
        m = re.search(r"(\d[\d,]*(?:\.\d+)?)", value)
        if not m:
            return None
        raw = m.group(1).replace(",", "")
        try:
            num = float(raw)
            return f"${int(num):,}" if num.is_integer() else f"${num:,.2f}"
        except ValueError:
            return None
    return None


def _candidate_provider_name(item: dict[str, Any]) -> str | None:
    direct = _first_str(item, _PROVIDER_NAME_KEYS)
    if direct:
        return direct
    nested = _first_value(item, ("provider", "organization", "sponsor", "donor", "fundingSource"))
    if isinstance(nested, dict):
        return _first_str(nested, ("name", "title", "displayName"))
    return None


def _candidate_provider_url(item: dict[str, Any]) -> str | None:
    direct = _first_str(item, _PROVIDER_URL_KEYS)
    if direct:
        return _to_absolute_url(direct)
    nested = _first_value(item, ("provider", "organization", "sponsor", "donor"))
    if isinstance(nested, dict):
        return _to_absolute_url(_first_str(nested, ("url", "href", "website", "profileUrl")))
    return None


def _candidate_apply_url(item: dict[str, Any], fallback: str | None) -> str | None:
    vals = _iter_direct_values(item, _APPLY_URL_KEYS) + _iter_recursive_values(item, _APPLY_URL_KEYS)
    for value in vals:
        text = _clean_text(value)
        if not text:
            continue
        url = _to_absolute_url(text)
        if url:
            return url
    return fallback


def _looks_like_scholarship_obj(obj: Any) -> bool:
    if not isinstance(obj, dict):
        return False
    title = _first_str(obj, _TITLE_KEYS)
    if not title:
        return False
    url = _candidate_url(obj)
    source_id = _candidate_source_id(obj, url)
    signals = 0
    if title:
        signals += 2
    if url or source_id:
        signals += 1
    if (
        _first_str(obj, _AMOUNT_TEXT_KEYS)
        or _first_value(obj, _AMOUNT_VALUE_KEYS) is not None
        or _first_str(obj, _DEADLINE_TEXT_KEYS)
        or _first_value(obj, _DEADLINE_DATE_KEYS) is not None
        or _first_str(obj, _DESCRIPTION_KEYS)
    ):
        signals += 1
    return signals >= 4


def _extract_scholarship_candidates(node: Any, max_depth: int = 7) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if max_depth < 0:
        return out
    if isinstance(node, dict):
        if _looks_like_scholarship_obj(node):
            out.append(node)
        for value in node.values():
            if isinstance(value, (dict, list)):
                out.extend(_extract_scholarship_candidates(value, max_depth=max_depth - 1))
    elif isinstance(node, list):
        for value in node[:300]:
            if isinstance(value, dict) and _looks_like_scholarship_obj(value):
                out.append(value)
            if isinstance(value, (dict, list)):
                out.extend(_extract_scholarship_candidates(value, max_depth=max_depth - 1))
    return out


def _snapshot_hash(item: dict[str, Any]) -> str:
    blob = json.dumps(item, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.md5(blob.encode("utf-8")).hexdigest()


def _build_record(item: dict[str, Any], response_url: str) -> dict[str, Any] | None:
    title = _clean_scholarship_title(_first_str(item, _TITLE_KEYS))
    url = _candidate_url(item)
    if not title or not url:
        return None
    source_id = _candidate_source_id(item, url)
    award_amount_text = _candidate_award_text(item)
    award_amount_min, award_amount_max = parse_award_min_max(award_amount_text)
    deadline_text = _candidate_deadline_text(item)
    deadline_date = _candidate_deadline_date(item, deadline_text)

    description = _first_str(item, _DESCRIPTION_KEYS)
    eligibility_text = _first_str(item, _ELIGIBILITY_KEYS)
    requirements_text = _first_str(item, _REQUIREMENTS_KEYS)
    provider_name = _candidate_provider_name(item)
    provider_url = _candidate_provider_url(item)
    apply_url = _candidate_apply_url(item, url)
    status_text = _first_str(item, _STATUS_KEYS)

    tags: list[str] = []
    cat_val = _first_value(item, _CATEGORY_KEYS)
    if isinstance(cat_val, list):
        for token in cat_val:
            t = _clean_text(token)
            if t and t.lower() not in tags:
                tags.append(t.lower())
    else:
        t = _clean_text(cat_val)
        if t:
            tags.append(t.lower())

    record: dict[str, Any] = {
        "source": SOURCE,
        "source_id": source_id,
        "url": url,
        "title": title,
        "provider_name": provider_name,
        "provider_url": provider_url,
        "award_amount_text": award_amount_text,
        "award_amount_min": award_amount_min,
        "award_amount_max": award_amount_max,
        "currency": DEFAULT_CURRENCY,
        "deadline_text": deadline_text,
        "deadline_date": deadline_date,
        "description": _strip_html(description) or description,
        "eligibility_text": _strip_html(eligibility_text) or eligibility_text,
        "requirements_text": _strip_html(requirements_text) or requirements_text,
        "apply_url": apply_url,
        "apply_button_text": "Visit Website",
        "mark_started_available": True,
        "mark_submitted_available": True,
        "status_text": status_text,
        "official_source_name": "Scholarships.com",
        "category": tags[0] if tags else None,
        "tags": tags[:20],
        "is_active": True,
        "is_recurring": False,
        "raw_data": _json_safe(
            {
                "captured_at": _now_iso(),
                "response_url": response_url,
                "raw_item": item,
            }
        ),
    }
    apply_normalization(record)
    for key in SCHOLARSHIP_RECORD_DEFAULT_KEYS:
        if key not in record:
            record[key] = None
    record["source"] = SOURCE
    record["currency"] = DEFAULT_CURRENCY
    record["is_indexable"] = True
    record["is_verified"] = bool(record.get("is_verified"))
    return record


class _CaptureState:
    def __init__(self) -> None:
        self.json_responses_seen = 0
        self.candidate_items_seen = 0
        self.captured: list[tuple[dict[str, Any], str]] = []
        self.identities: set[str] = set()
        self.detail_urls: set[str] = set()
        self.route_candidates: set[str] = set()


def _capture_identity(item: dict[str, Any], response_url: str) -> str:
    url = _candidate_url(item) or ""
    source_id = _candidate_source_id(item, url) or ""
    title = _first_str(item, _TITLE_KEYS) or ""
    return " | ".join([source_id, url, title, response_url])


def _response_handler_factory(state: _CaptureState):
    def _handler(response: Any) -> None:
        content_type = (response.headers.get("content-type") or "").lower()
        url = str(response.url or "")
        url_low = url.lower()
        if response.status >= 400:
            return
        if "json" not in content_type and not any(x in url_low for x in ("/api/", "graphql", "search", "scholar")):
            return
        try:
            payload = response.json()
        except Exception:
            return
        state.json_responses_seen += 1
        candidates = _extract_scholarship_candidates(payload)
        if not candidates:
            return
        for item in candidates:
            identity = _capture_identity(item, url)
            if identity in state.identities:
                continue
            state.identities.add(identity)
            state.captured.append((item, url))
            state.candidate_items_seen += 1
            c_url = _candidate_url(item)
            if c_url and _ROUTE_HINT_RE.search(c_url):
                state.detail_urls.add(c_url)
    return _handler


def _domain_is_same_site(url: str) -> bool:
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        return False
    return host.endswith("scholarships.com")


def _is_grant_route(url: str) -> bool:
    if not url:
        return False
    if not _domain_is_same_site(url):
        return False
    low = url.lower()
    if "/about-us/" in low or "/college-search/" in low or "/educators/" in low:
        return False
    return bool(_GRANT_URL_RE.search(low) or "scholarshipmatches" in low)


def _unique_grant_urls(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in items:
        u = str(raw or "").split("#", 1)[0].strip()
        if not u or u in seen or not _is_grant_route(u):
            continue
        seen.add(u)
        out.append(u)
    return out


def _has_international_student_signal(record: dict[str, Any], item: dict[str, Any]) -> bool:
    pieces: list[str] = []
    for key in (
        "title",
        "url",
        "source_id",
        "description",
        "eligibility_text",
        "requirements_text",
        "award_amount_text",
        "deadline_text",
    ):
        value = record.get(key)
        if value:
            pieces.append(str(value))
    try:
        pieces.append(json.dumps(item, ensure_ascii=False)[:80_000])
    except Exception:
        pass
    blob = "\n".join(pieces)
    return bool(_INTERNATIONAL_STUDENT_RE.search(blob))


def _collect_routes_from_dom(page: Any, state: _CaptureState) -> None:
    try:
        anchors = page.eval_on_selector_all(
            "a[href]",
            "els => els.map(e => e.getAttribute('href')).filter(Boolean)",
        )
    except Exception:
        return
    for href in anchors[:500]:
        url = _to_absolute_url(str(href))
        if not url or not _is_grant_route(url):
            continue
        if _ROUTE_HINT_RE.search(url):
            state.route_candidates.add(url.split("#", 1)[0])
        if re.search(r"/scholarships/", url, re.I):
            state.detail_urls.add(url.split("#", 1)[0])


def _scroll_page(page: Any, steps: int) -> None:
    for _ in range(steps):
        try:
            page.mouse.wheel(0, 5000)
        except Exception:
            pass
        page.wait_for_timeout(SCHOLARSHIPS_COM_SCROLL_WAIT_MS)


def _discover_routes(page: Any, state: _CaptureState) -> dict[str, Any]:
    seeds = [
        MATCHES_URL,
        f"{MATCHES_URL}?sortOrder=duedate&sortDirection=desc",
        f"{MATCHES_URL}?sortOrder=maxvalue&sortDirection=desc",
        DIRECTORY_URL,
    ]
    if SCHOLARSHIPS_COM_ONLY_INTERNATIONAL:
        seeds = _unique_grant_urls(list(_INTERNATIONAL_PRIORITY_SEEDS) + seeds)
    front_seeds = list(seeds)
    front_seed_set = set(front_seeds)

    def _load_discovery_checkpoint() -> tuple[set[str], list[str]]:
        if not SCHOLARSHIPS_COM_DISCOVERY_CHECKPOINT or not os.path.isfile(DISCOVERY_CHECKPOINT_PATH):
            return set(), []
        try:
            with open(DISCOVERY_CHECKPOINT_PATH, encoding="utf-8") as f:
                payload = json.load(f)
            if not isinstance(payload, dict):
                return set(), []
            visited_rows = payload.get("visited")
            queue_rows = payload.get("queue")
            visited = set(_unique_grant_urls(visited_rows if isinstance(visited_rows, list) else []))
            queue = _unique_grant_urls(queue_rows if isinstance(queue_rows, list) else [])
            return visited, queue
        except Exception:
            return set(), []

    def _save_discovery_checkpoint(visited_now: set[str], queue_now: list[str]) -> None:
        if not SCHOLARSHIPS_COM_DISCOVERY_CHECKPOINT:
            return
        payload = {
            "source": SOURCE,
            "updated_at": _now_iso(),
            "visited": sorted(visited_now),
            "queue": _unique_grant_urls(queue_now),
            "max_discovery_pages": SCHOLARSHIPS_COM_MAX_DISCOVERY_PAGES,
        }
        try:
            with open(DISCOVERY_CHECKPOINT_PATH, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def _clear_discovery_checkpoint() -> None:
        if not SCHOLARSHIPS_COM_DISCOVERY_CHECKPOINT:
            return
        try:
            if os.path.exists(DISCOVERY_CHECKPOINT_PATH):
                os.remove(DISCOVERY_CHECKPOINT_PATH)
        except Exception:
            pass

    visited, resumed_queue = _load_discovery_checkpoint()
    # Always prepend fresh-front URLs so brand-new grants near the top are picked first.
    visited -= front_seed_set
    queue = _unique_grant_urls(front_seeds + resumed_queue + seeds)
    if resumed_queue:
        _log(
            f"{SOURCE}: discovery resume loaded: visited={len(visited)} queue={len(resumed_queue)}"
        )

    save_tick = 0
    discovery_interrupted = False
    visits_this_run = 0
    while queue and len(visited) < SCHOLARSHIPS_COM_MAX_DISCOVERY_PAGES:
        if (
            SCHOLARSHIPS_COM_DISCOVERY_BATCH_PAGES > 0
            and visits_this_run >= SCHOLARSHIPS_COM_DISCOVERY_BATCH_PAGES
        ):
            _log(
                f"{SOURCE}: discovery batch limit reached="
                f"{SCHOLARSHIPS_COM_DISCOVERY_BATCH_PAGES}"
            )
            break
        url = queue.pop(0)
        force_front = url in front_seed_set
        if url in visited and not force_front:
            continue
        visited.add(url)
        visits_this_run += 1
        _log(f"{SOURCE}: discovery visit {len(visited)} -> {url}")
        try:
            page.goto(url, wait_until="domcontentloaded")
            page.wait_for_timeout(1000)
            _scroll_page(page, steps=min(4, SCHOLARSHIPS_COM_SCROLL_STEPS))
            _collect_routes_from_dom(page, state)
        except Exception as exc:
            _log(f"{SOURCE}: discovery warning for {url}: {exc}")
            if _is_driver_connection_closed(exc):
                _log(f"{SOURCE}: browser driver connection lost during discovery; finishing with checkpoint")
                _save_discovery_checkpoint(visited, queue)
                discovery_interrupted = True
                break
        for route in sorted(state.route_candidates):
            if route not in visited and route not in queue and len(queue) < 300:
                queue.append(route)
        save_tick += 1
        if save_tick == 5:
            _save_discovery_checkpoint(visited, queue)
            save_tick = 0
    discovery_complete = not queue or len(visited) >= SCHOLARSHIPS_COM_MAX_DISCOVERY_PAGES
    previous_rules = _load_route_rules()
    previous_routes = previous_rules.get("discovered_routes")
    previous_details = previous_rules.get("detail_urls")
    merged_routes = set(
        u for u in (previous_routes if isinstance(previous_routes, list) else [])
        if isinstance(u, str) and _is_grant_route(u)
    )
    merged_routes.update(u for u in state.route_candidates if _is_grant_route(u))
    merged_details = set(
        u for u in (previous_details if isinstance(previous_details, list) else [])
        if isinstance(u, str) and _is_grant_route(u)
    )
    merged_details.update(u for u in state.detail_urls if _is_grant_route(u))
    route_rules = {
        "source": SOURCE,
        "captured_at": _now_iso(),
        "parser_type": "BROWSER",
        "seed_urls": seeds,
        "discovered_routes": sorted(merged_routes)[:5000],
        "detail_urls": sorted(merged_details)[:20000],
        "json_responses_seen": state.json_responses_seen,
        "captured_candidates": state.candidate_items_seen,
        "discovery_complete": discovery_complete,
        "discovery_interrupted": discovery_interrupted,
        "discovery_visited_total": len(visited),
        "discovery_queue_remaining": len(queue),
    }
    try:
        with open(ROUTES_RULES_PATH, "w", encoding="utf-8") as f:
            json.dump(route_rules, f, ensure_ascii=False, indent=2)
        _log(f"{SOURCE}: route rules saved -> {ROUTES_RULES_PATH}")
        if discovery_complete:
            _clear_discovery_checkpoint()
        else:
            _save_discovery_checkpoint(visited, queue)
    except OSError as exc:
        _log(f"{SOURCE}: warning: could not save route rules ({exc})")
    return route_rules


def _load_route_rules() -> dict[str, Any]:
    if not os.path.isfile(ROUTES_RULES_PATH):
        return {}
    try:
        with open(ROUTES_RULES_PATH, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _persist_route_rules_after_listing(route_rules: dict[str, Any], state: _CaptureState) -> None:
    """
    On collect, listing capture fills state.detail_urls / route_candidates; merge into
    route_rules and save so a parallel `process` run (or a later run) can open detail pages.
    """
    if not route_rules:
        return
    merged_routes: set[str] = set()
    for u in route_rules.get("discovered_routes") or []:
        if isinstance(u, str) and _is_grant_route(u):
            merged_routes.add(u.split("#", 1)[0])
    for u in state.route_candidates:
        if _is_grant_route(str(u)):
            merged_routes.add(str(u).split("#", 1)[0])
    route_rules["discovered_routes"] = sorted(merged_routes)[:5000]

    merged_details: set[str] = set()
    for u in route_rules.get("detail_urls") or []:
        if isinstance(u, str) and _is_grant_route(u):
            merged_details.add(u.split("#", 1)[0])
    for u in state.detail_urls:
        if _is_grant_route(str(u)):
            merged_details.add(str(u).split("#", 1)[0])
    route_rules["detail_urls"] = sorted(merged_details)[:20000]
    route_rules["captured_at"] = _now_iso()
    route_rules["source"] = SOURCE
    if "parser_type" not in route_rules:
        route_rules["parser_type"] = "BROWSER"
    try:
        with open(ROUTES_RULES_PATH, "w", encoding="utf-8") as f:
            json.dump(route_rules, f, ensure_ascii=False, indent=2)
        _log(
            f"{SOURCE}: route rules updated after listing (details={len(merged_details)}) -> "
            f"{ROUTES_RULES_PATH}"
        )
    except OSError as exc:
        _log(f"{SOURCE}: warning: could not save route rules after listing ({exc})")


def _run_listing_capture(page: Any, state: _CaptureState, route_rules: dict[str, Any]) -> bool:
    routes = route_rules.get("discovered_routes")
    if not isinstance(routes, list) or not routes:
        routes = [MATCHES_URL, DIRECTORY_URL]
    listing_routes = [str(u) for u in routes if isinstance(u, str) and _is_grant_route(str(u))]
    listing_routes = listing_routes[:SCHOLARSHIPS_COM_MAX_LISTING_PAGES]
    driver_lost = False
    for idx, url in enumerate(listing_routes, start=1):
        _log(f"{SOURCE}: listing capture {idx}/{len(listing_routes)} -> {url}")
        try:
            page.goto(url, wait_until="domcontentloaded")
            page.wait_for_timeout(900)
            _scroll_page(page, SCHOLARSHIPS_COM_SCROLL_STEPS)
            _collect_routes_from_dom(page, state)
            if idx % 40 == 0:
                _persist_route_rules_after_listing(route_rules, state)
        except Exception as exc:
            _log(f"{SOURCE}: listing route failed: {url} -> {exc}")
            if _is_driver_connection_closed(exc):
                _log(f"{SOURCE}: browser driver connection lost during listing capture; stopping this phase")
                driver_lost = True
                break
    _persist_route_rules_after_listing(route_rules, state)
    return driver_lost


def _load_detail_checkpoint() -> set[str]:
    if not SCHOLARSHIPS_COM_DETAIL_CHECKPOINT or not os.path.isfile(DETAIL_CHECKPOINT_PATH):
        return set()
    try:
        with open(DETAIL_CHECKPOINT_PATH, encoding="utf-8") as f:
            payload = json.load(f)
        if not isinstance(payload, dict):
            return set()
        processed = payload.get("processed_urls")
        return set(_unique_grant_urls(processed if isinstance(processed, list) else []))
    except Exception:
        return set()


def _save_detail_checkpoint(processed_urls: set[str]) -> None:
    if not SCHOLARSHIPS_COM_DETAIL_CHECKPOINT:
        return
    payload = {
        "source": SOURCE,
        "updated_at": _now_iso(),
        "processed_urls": sorted(processed_urls),
    }
    try:
        with open(DETAIL_CHECKPOINT_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _crawl_detail_urls(
    page: Any,
    state: _CaptureState,
    route_rules: dict[str, Any],
    *,
    known_idx: KnownScholarshipIndex | None = None,
    use_known_skip: bool = False,
    stats: dict[str, int] | None = None,
) -> bool:
    def _merge_disk_detail_urls() -> set[str]:
        out: set[str] = set()
        on_disk = _load_route_rules()
        if not isinstance(on_disk, dict):
            return out
        for row in (on_disk.get("detail_urls") or []):
            if isinstance(row, str) and _is_grant_route(row):
                out.add(row.split("#", 1)[0])
        return out

    detail_urls: set[str] = set()
    for key in ("detail_urls",):
        value = route_rules.get(key)
        if isinstance(value, list):
            for row in value:
                if isinstance(row, str) and _is_grant_route(row):
                    detail_urls.add(row.split("#", 1)[0])
    detail_urls |= _merge_disk_detail_urls()
    detail_urls.update(
        str(u).split("#", 1)[0] for u in state.detail_urls if _is_grant_route(str(u))
    )
    processed_urls = _load_detail_checkpoint()
    known_skipped = 0
    detail_list: list[str] = []
    for url in sorted(detail_urls):
        if url in processed_urls:
            continue
        if use_known_skip and known_idx is not None and _detail_url_is_known(url, known_idx):
            known_skipped += 1
            continue
        detail_list.append(url)
    if stats is not None:
        stats["detail_known_skipped"] += known_skipped
    if known_skipped > 0:
        _log(f"{SOURCE}: detail urls skipped by known index={known_skipped}")
    detail_list = detail_list[:SCHOLARSHIPS_COM_MAX_DETAIL_PAGES]
    max_per = SCHOLARSHIPS_COM_MAX_DETAIL_SCANS_PER_RUN
    driver_lost = False
    detail_idx = 0
    scans = 0
    queued: set[str] = set(detail_list)

    def _append_new_from_disk() -> int:
        added = 0
        for u in _merge_disk_detail_urls():
            if u in processed_urls or u in queued:
                continue
            if use_known_skip and known_idx is not None and _detail_url_is_known(u, known_idx):
                continue
            if len(detail_list) >= 40_000:
                break
            detail_list.append(u)
            queued.add(u)
            added += 1
        for u in state.detail_urls:
            u0 = str(u).split("#", 1)[0]
            if not _is_grant_route(u0) or u0 in processed_urls or u0 in queued:
                continue
            if use_known_skip and known_idx is not None and _detail_url_is_known(u0, known_idx):
                continue
            if len(detail_list) >= 40_000:
                break
            detail_list.append(u0)
            queued.add(u0)
            added += 1
        if added:
            _log(
                f"{SOURCE}: detail queue +{added} (parallel listing feed), queue_len={len(detail_list)}"
            )
        return added

    while detail_idx < len(detail_list) and scans < SCHOLARSHIPS_COM_MAX_DETAIL_PAGES:
        if max_per > 0 and scans >= max_per:
            break
        if scans > 0 and scans % 25 == 0 and max_per == 0:
            _append_new_from_disk()
        url = detail_list[detail_idx]
        detail_idx += 1
        scans += 1
        _log(f"{SOURCE}: detail scan {scans}/{len(detail_list)} -> {url}")
        try:
            page.goto(url, wait_until="domcontentloaded")
            page.wait_for_timeout(600)
            _collect_routes_from_dom(page, state)
            title = _clean_scholarship_title(page.title()) or ""
            page_text = ""
            try:
                page_text = str(page.inner_text("body") or "")
            except Exception:
                page_text = ""
            amount_text: str | None = None
            deadline_text: str | None = None
            amount_m = re.search(r"Amount:\s*([^\n\r]+)", page_text, re.I)
            if amount_m:
                amount_text = _clean_text(amount_m.group(1))
            deadline_m = re.search(r"Deadline:\s*([^\n\r]+)", page_text, re.I)
            if deadline_m:
                deadline_text = _clean_text(deadline_m.group(1))
            if not amount_text:
                m2 = re.search(r"\$\s?\d[\d,]*(?:\.\d{2})?", page_text)
                if m2:
                    amount_text = _clean_text(m2.group(0))
            if not deadline_text:
                m3 = re.search(
                    r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}",
                    page_text,
                    re.I,
                )
                if m3:
                    deadline_text = _clean_text(m3.group(0))
            # Turn a detail page into a candidate row so it enters the same pipeline.
            row = {
                "title": title,
                "name": title,
                "url": url,
                "link": url,
                "amount": amount_text,
                "awardAmountText": amount_text,
                "deadline": deadline_text,
                "deadlineText": deadline_text,
                "description": _clean_text(
                    page.eval_on_selector("meta[name='description']", "el => el.content").strip()
                    if page.locator("meta[name='description']").count() > 0
                    else ""
                ),
                "pageText": _clean_text(page_text[:50_000]),
            }
            identity = _capture_identity(row, url)
            if identity not in state.identities:
                state.identities.add(identity)
                state.captured.append((row, url))
                state.candidate_items_seen += 1
            processed_urls.add(url)
            if scans == 1 or scans % 10 == 0:
                _save_detail_checkpoint(processed_urls)
        except Exception as exc:
            if _is_driver_connection_closed(exc):
                _log(f"{SOURCE}: browser driver connection lost during detail scan; stopping this phase")
                driver_lost = True
                break
            continue
    _save_detail_checkpoint(processed_urls)
    return driver_lost


def _record_identity(record: dict[str, Any]) -> str:
    return " | ".join(
        [
            str(record.get("source_id") or "").strip(),
            str(record.get("url") or "").strip(),
            str(record.get("title") or "").strip(),
        ]
    )


def _detail_url_is_known(url: str, idx: KnownScholarshipIndex) -> bool:
    if not url:
        return False
    u = str(url).strip()
    if not u:
        return False
    if u in idx.urls:
        return True
    slug = _slug_from_url(u)
    if slug and slug.lower() in idx.slugs_lc:
        return True
    return False


def _is_driver_connection_closed(exc: Exception) -> bool:
    msg = str(exc or "").lower()
    return "connection closed while reading from the driver" in msg


def run() -> None:
    if not SCHOLARSHIPS_COM_ENABLED:
        _log(f"{SOURCE}: disabled via SCHOLARSHIPS_COM_ENABLED=0")
        return
    from playwright.sync_api import sync_playwright

    if SCHOLARSHIPS_COM_ONLY_INTERNATIONAL:
        _log(
            f"{SOURCE}: international-only mode enabled; store={SCHOLARSHIPS_COM_PREFILTER_STORE_PATH}"
        )
    ai_usage_start = snapshot_ai_usage()
    store = ScholarshipsComPrefilterStore(SCHOLARSHIPS_COM_PREFILTER_STORE_PATH)
    store.load()
    state = _CaptureState()
    effective_target = (
        min(TARGET_NEW_ITEMS, SCHOLARSHIPS_COM_MAX_RECORDS_DEBUG)
        if SCHOLARSHIPS_COM_MAX_RECORDS_DEBUG > 0
        else TARGET_NEW_ITEMS
    )
    use_skip = (
        (SKIP_EXISTING_ON_LIST and DISCOVERY_MODE == "new_only")
        or SCHOLARSHIPS_COM_RUN_MODE == "process"
    ) and not SCHOLARSHIPS_COM_FORCE_REFRESH

    idx: KnownScholarshipIndex
    if use_skip:
        try:
            idx = load_known_scholarship_index(get_client(), SOURCE)
            _log(
                f"{SOURCE}: known index loaded: urls={len(idx.urls)} source_ids={len(idx.source_ids)} "
                f"slugs={len(idx.slugs_lc)} titles={len(idx.titles_norm)}"
            )
        except Exception as exc:
            _log(f"{SOURCE}: warning: failed to load known index ({exc})")
            idx = KnownScholarshipIndex()
    else:
        idx = KnownScholarshipIndex()

    stats: dict[str, int] = {
        "captured_candidates": 0,
        "prefilter_pass": 0,
        "prefilter_reject_known": 0,
        "prefilter_reject_mapping": 0,
        "prefilter_reject_funding": 0,
        "prefilter_reject_deadline": 0,
        "deep_candidates": 0,
        "known_skipped": 0,
        "mapped_skipped": 0,
        "skip_no_funding": 0,
        "skip_no_deadline": 0,
        "skip_expired": 0,
        "skip_deadline_too_close": 0,
        "upsert_ok": 0,
        "upsert_failed": 0,
        "detail_known_skipped": 0,
    }
    seen_records_session: set[str] = set()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=SCHOLARSHIPS_COM_HEADLESS)
        context_kwargs: dict[str, Any] = {}
        if os.path.exists(SESSION_STATE_PATH):
            context_kwargs["storage_state"] = SESSION_STATE_PATH
            _log(f"{SOURCE}: using saved session -> {SESSION_STATE_PATH}")
        context = browser.new_context(**context_kwargs)
        page = context.new_page()
        page.set_default_timeout(SCHOLARSHIPS_COM_TIMEOUT_MS)
        page.on("response", _response_handler_factory(state))
        driver_lost = False
        try:
            _ensure_authenticated(context, page)
            if SCHOLARSHIPS_COM_RUN_MODE == "process":
                _log(f"{SOURCE}: process mode, loading saved route rules")
                route_rules = _load_route_rules()
                if not route_rules:
                    _log(f"{SOURCE}: no route rules found; run collect mode first")
                    raise SystemExit(1)
            elif SCHOLARSHIPS_COM_SKIP_DISCOVERY:
                _log(f"{SOURCE}: skipping discovery, loading saved route rules")
                route_rules = _load_route_rules()
            else:
                route_rules = _discover_routes(page, state)
                if not route_rules.get("discovered_routes"):
                    route_rules = _load_route_rules()
            if not route_rules:
                fallback_routes = list(_INTERNATIONAL_PRIORITY_SEEDS) if SCHOLARSHIPS_COM_ONLY_INTERNATIONAL else [MATCHES_URL, DIRECTORY_URL]
                route_rules = {"discovered_routes": fallback_routes, "detail_urls": []}
            if bool(route_rules.get("discovery_interrupted", False)):
                driver_lost = True
            if SCHOLARSHIPS_COM_RUN_MODE != "process" and not driver_lost:
                driver_lost = _run_listing_capture(page, state, route_rules)
            if SCHOLARSHIPS_COM_RUN_MODE == "collect":
                _persist_route_rules_after_listing(route_rules, state)
                route_count = len(route_rules.get("discovered_routes") or [])
                detail_count = len(route_rules.get("detail_urls") or []) + len(state.detail_urls)
                _log(
                    f"{SOURCE}: collect mode complete: routes={route_count}, detail_urls={detail_count}"
                )
                if driver_lost:
                    _log(f"{SOURCE}: collect interrupted by browser driver; restart will resume")
                    raise SystemExit(2)
                return
            if not driver_lost:
                driver_lost = _crawl_detail_urls(
                    page,
                    state,
                    route_rules,
                    known_idx=idx,
                    use_known_skip=use_skip,
                    stats=stats,
                )
            _log(
                f"{SOURCE}: captured JSON responses={state.json_responses_seen}, "
                f"candidates={state.candidate_items_seen}"
            )
            total_captured = len(state.captured)
            for idx_entry, (item, response_url) in enumerate(state.captured, start=1):
                stats["captured_candidates"] += 1
                if idx_entry == 1 or idx_entry % 50 == 0 or idx_entry == total_captured:
                    _log(
                        f"{SOURCE}: prefilter progress {idx_entry}/{total_captured} "
                        f"(pass={stats['prefilter_pass']} deadline_reject={stats['prefilter_reject_deadline']})"
                    )
                record = _build_record(item, response_url)
                if not record:
                    stats["mapped_skipped"] += 1
                    stats["prefilter_reject_mapping"] += 1
                    store.upsert_candidate(
                        source_id=_candidate_source_id(item, _candidate_url(item)),
                        url=_candidate_url(item),
                        title=_first_str(item, _TITLE_KEYS),
                        response_url=response_url,
                        snapshot_hash=_snapshot_hash(item),
                        prefilter_status=PREFILTER_REJECT_MAPPING,
                        prefilter_reason="build_record_failed",
                        item_snapshot=item,
                    )
                    continue
                if SCHOLARSHIPS_COM_ONLY_INTERNATIONAL and not _has_international_student_signal(record, item):
                    stats["mapped_skipped"] += 1
                    stats["prefilter_reject_mapping"] += 1
                    store.upsert_candidate(
                        source_id=record.get("source_id"),
                        url=record.get("url"),
                        title=record.get("title"),
                        response_url=response_url,
                        snapshot_hash=_snapshot_hash(item),
                        prefilter_status=PREFILTER_REJECT_MAPPING,
                        prefilter_reason="not_international_student",
                        item_snapshot=item,
                    )
                    continue
                if use_skip and listing_is_known(record, idx, title_fallback=USE_TITLE_FALLBACK_KNOWN):
                    stats["known_skipped"] += 1
                    stats["prefilter_reject_known"] += 1
                    record_ai_skip()
                    store.upsert_candidate(
                        source_id=record.get("source_id"),
                        url=record.get("url"),
                        title=record.get("title"),
                        response_url=response_url,
                        snapshot_hash=_snapshot_hash(item),
                        prefilter_status=PREFILTER_REJECT_KNOWN,
                        prefilter_reason="known_in_db",
                        item_snapshot=item,
                    )
                    continue
                if not has_meaningful_funding(record):
                    intl_signal = detect_international_signal(
                        record.get("title"),
                        record.get("url"),
                        record.get("eligibility_text"),
                        record.get("requirements_text"),
                        record.get("description"),
                        record.get("tags"),
                        item,
                    )
                    if intl_signal:
                        stats["prefilter_pass"] += 1
                        store.upsert_candidate(
                            source_id=record.get("source_id"),
                            url=record.get("url"),
                            title=record.get("title"),
                            response_url=response_url,
                            snapshot_hash=_snapshot_hash(item),
                            prefilter_status=PREFILTER_PASS,
                            prefilter_reason=f"international_funding_override:{intl_signal}",
                            item_snapshot=item,
                        )
                        continue
                    stats["prefilter_reject_funding"] += 1
                    store.upsert_candidate(
                        source_id=record.get("source_id"),
                        url=record.get("url"),
                        title=record.get("title"),
                        response_url=response_url,
                        snapshot_hash=_snapshot_hash(item),
                        prefilter_status=PREFILTER_REJECT_FUNDING,
                        prefilter_reason="no_meaningful_funding",
                        item_snapshot=item,
                    )
                    continue
                dbiz = classify_business_deadline(record.get("deadline_date"))
                if dbiz != "ok":
                    stats["prefilter_reject_deadline"] += 1
                    store.upsert_candidate(
                        source_id=record.get("source_id"),
                        url=record.get("url"),
                        title=record.get("title"),
                        response_url=response_url,
                        snapshot_hash=_snapshot_hash(item),
                        prefilter_status=PREFILTER_REJECT_DEADLINE,
                        prefilter_reason=dbiz,
                        item_snapshot=item,
                    )
                    continue
                stats["prefilter_pass"] += 1
                store.upsert_candidate(
                    source_id=record.get("source_id"),
                    url=record.get("url"),
                    title=record.get("title"),
                    response_url=response_url,
                    snapshot_hash=_snapshot_hash(item),
                    prefilter_status=PREFILTER_PASS,
                    prefilter_reason="",
                    item_snapshot=item,
                )

            store.save()
            deep_candidates = store.iter_deep_candidates()
            stats["deep_candidates"] = len(deep_candidates)
            _log(f"{SOURCE}: deep candidates queued={stats['deep_candidates']}")

            for deep_idx, entry in enumerate(deep_candidates, start=1):
                if deep_idx == 1 or deep_idx % 10 == 0:
                    _log(
                        f"{SOURCE}: deep progress {deep_idx}/{len(deep_candidates)} "
                        f"(upsert_ok={stats['upsert_ok']}, upsert_failed={stats['upsert_failed']})"
                    )
                item = entry.get("item_snapshot")
                response_url = str(entry.get("response_url") or "")
                if not isinstance(item, dict):
                    continue
                record = _build_record(item, response_url)
                if not record:
                    continue
                if SCHOLARSHIPS_COM_ONLY_INTERNATIONAL and not _has_international_student_signal(record, item):
                    continue
                rid = _record_identity(record)
                if rid in seen_records_session:
                    continue
                seen_records_session.add(rid)
                try:
                    # hard guard for unknown columns before upsert
                    unknown = set(record) - set(SCHOLARSHIP_UPSERT_BODY_KEYS) - {"id"}
                    if unknown:
                        raise ValueError(f"unknown keys in record: {sorted(unknown)}")
                    upsert_scholarship(record)
                    stats["upsert_ok"] += 1
                    store.mark_processed(entry)
                    if effective_target > 0 and stats["upsert_ok"] >= effective_target:
                        _log(f"{SOURCE}: reached effective target={effective_target}")
                        break
                except Exception as exc:
                    stats["upsert_failed"] += 1
                    _log(f"{SOURCE}: upsert failed for '{record.get('title')}' -> {exc}")

            store.save()
            _log(f"captured candidates: {stats['captured_candidates']}")
            _log(f"prefilter pass: {stats['prefilter_pass']}")
            _log(f"prefilter reject known: {stats['prefilter_reject_known']}")
            _log(f"prefilter reject mapping: {stats['prefilter_reject_mapping']}")
            _log(f"prefilter reject funding: {stats['prefilter_reject_funding']}")
            _log(f"prefilter reject deadline: {stats['prefilter_reject_deadline']}")
            _log(f"upsert OK: {stats['upsert_ok']}")
            _log(f"upsert failed: {stats['upsert_failed']}")
            _log(f"detail known skipped: {stats['detail_known_skipped']}")
            _log(
                f"skip deadline too close (<{MIN_LEAD_DAYS_BEFORE_DEADLINE}d): "
                f"{stats['skip_deadline_too_close']}"
            )
            print_ai_session_summary(
                SOURCE,
                processed=stats["captured_candidates"],
                new_found=stats["upsert_ok"],
                start=ai_usage_start,
            )
            if driver_lost and (effective_target <= 0 or stats["upsert_ok"] < effective_target):
                _log(f"{SOURCE}: browser driver was lost; exiting non-zero so watchdog restarts")
                raise SystemExit(2)
            if SCHOLARSHIPS_COM_KEEP_BROWSER_OPEN and not SCHOLARSHIPS_COM_HEADLESS:
                _log(f"{SOURCE}: keeping browser open; stop process when done.")
                try:
                    while True:
                        page.wait_for_timeout(1000)
                except KeyboardInterrupt:
                    _log(f"{SOURCE}: browser hold interrupted.")
        finally:
            try:
                context.close()
            except Exception as exc:
                _log(f"{SOURCE}: context close warning: {exc}")
            try:
                browser.close()
            except Exception as exc:
                _log(f"{SOURCE}: browser close warning: {exc}")


_TITLE_KEYS: tuple[str, ...] = (
    "title",
    "name",
    "scholarshipTitle",
    "displayTitle",
    "seoTitle",
    "opportunityTitle",
)
_URL_KEYS: tuple[str, ...] = (
    "url",
    "href",
    "permalink",
    "canonicalUrl",
    "shareUrl",
    "publicUrl",
    "path",
    "link",
)
_SLUG_KEYS: tuple[str, ...] = (
    "slug",
    "scholarshipSlug",
    "seoSlug",
    "publicSlug",
)
_ID_KEYS: tuple[str, ...] = (
    "id",
    "uuid",
    "scholarshipId",
    "listingId",
    "opportunityId",
)
_AMOUNT_TEXT_KEYS: tuple[str, ...] = (
    "awardAmountText",
    "amountText",
    "awardText",
    "scholarshipAmountText",
    "amountFormatted",
    "amount",
)
_AMOUNT_VALUE_KEYS: tuple[str, ...] = (
    "awardAmount",
    "amountValue",
    "scholarshipAmount",
    "awardValue",
    "maxAmount",
    "minimumAmount",
)
_DEADLINE_TEXT_KEYS: tuple[str, ...] = (
    "deadlineText",
    "deadline",
    "applicationDeadline",
    "closeDateText",
    "dueDate",
)
_DEADLINE_DATE_KEYS: tuple[str, ...] = (
    "deadlineDate",
    "deadlineAt",
    "applicationDeadlineAt",
    "deadline",
    "closeDate",
    "endDate",
    "expiresAt",
    "dueDate",
)
_DESCRIPTION_KEYS: tuple[str, ...] = (
    "description",
    "summary",
    "shortDescription",
    "snippet",
    "about",
    "body",
)
_ELIGIBILITY_KEYS: tuple[str, ...] = (
    "eligibility",
    "eligibilityText",
    "eligibilityDescription",
    "whoCanApply",
)
_REQUIREMENTS_KEYS: tuple[str, ...] = (
    "requirements",
    "requirementsText",
    "applicationRequirements",
    "essayPrompt",
    "instructions",
)
_PROVIDER_NAME_KEYS: tuple[str, ...] = (
    "providerName",
    "sponsorName",
    "donorName",
    "organizationName",
    "foundationName",
    "hostName",
)
_PROVIDER_URL_KEYS: tuple[str, ...] = (
    "providerUrl",
    "sponsorUrl",
    "donorUrl",
    "organizationUrl",
    "hostUrl",
)
_STATUS_KEYS: tuple[str, ...] = (
    "status",
    "applicationStatus",
    "scholarshipStatus",
)
_APPLY_URL_KEYS: tuple[str, ...] = (
    "applyUrl",
    "applicationUrl",
    "ctaUrl",
    "externalUrl",
    "link",
)
_CATEGORY_KEYS: tuple[str, ...] = (
    "category",
    "categories",
    "tags",
    "topics",
    "majors",
    "interests",
)


if __name__ == "__main__":
    run()

