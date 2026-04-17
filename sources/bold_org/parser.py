"""
Bold.org scholarship parser -> public.scholarships (Supabase).

Flow:
1. Log in with Playwright using BOLD_EMAIL / BOLD_PASSWORD.
2. Open scholarship pages.
3. Intercept JSON responses with page.on("response", ...).
4. Extract scholarship-like objects from API payloads.
5. Map them into the shared scholarship record shape.
6. Normalize, filter, and upsert.

This parser is intentionally resilient to small payload shape changes. There is no
single hardcoded endpoint; instead it listens to JSON responses from Bold pages and
searches recursively for scholarship-like objects.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import hashlib
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

from ai_monitoring import print_ai_session_summary, record_ai_skip, snapshot_ai_usage

_PARSER_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if _PARSER_ROOT not in sys.path:
    sys.path.insert(0, _PARSER_ROOT)

# has_meaningful_funding: award_signals + business_filters (общий для всех источников).
from business_filters import (
    MIN_LEAD_DAYS_BEFORE_DEADLINE,
    classify_business_deadline,
    has_meaningful_funding,
)
from config import get_global_config
from normalize_scholarship import apply_normalization
from scholarship_db_columns import (
    SCHOLARSHIP_RECORD_DEFAULT_KEYS,
    SCHOLARSHIP_UPSERT_BODY_KEYS,
)
from sources.scholarship_america.parser import parse_award_min_max, parse_deadline_date
from sources.shared_ai_enrichment import json_safe as _json_safe
from sources.bold_org.prefilter import (
    BoldPrefilterStore,
    PREFILTER_PASS,
    PREFILTER_REJECT_DEADLINE,
    PREFILTER_REJECT_FUNDING,
    PREFILTER_REJECT_KNOWN,
    PREFILTER_REJECT_MAPPING,
)
from utils import (
    KnownScholarshipIndex,
    get_client,
    listing_is_known,
    load_known_scholarship_index,
    upsert_scholarship,
)

SCHOLARSHIP_TABLE_KEYS: tuple[str, ...] = SCHOLARSHIP_UPSERT_BODY_KEYS

SOURCE = "bold_org"
DEFAULT_CURRENCY = "USD"
SITE_ORIGIN = "https://bold.org"
APP_ORIGIN = "https://app.bold.org"
LOGIN_URL = f"{APP_ORIGIN}/scholarships/"
PUBLIC_SCHOLARSHIPS_URL = f"{SITE_ORIGIN}/scholarships/"
APP_SCHOLARSHIPS_URL = f"{APP_ORIGIN}/scholarships/"
SESSION_STATE_PATH = os.path.join(_PARSER_ROOT, "bold_session.json")
CAPTCHA_SCREENSHOT_PATH = os.path.join(_PARSER_ROOT, "bold_captcha_detected.png")
CAPTCHA_HTML_PATH = os.path.join(_PARSER_ROOT, "bold_captcha_detected.html")

_CAPTCHA_MARKERS: tuple[str, ...] = (
    "captcha",
    "recaptcha",
    "hcaptcha",
    "verify you are human",
    "verify you're human",
    "human verification",
    "unusual traffic",
    "press and hold",
    "cloudflare",
    "challenge",
    "cf-challenge",
)

_gc = get_global_config()
TARGET_NEW_ITEMS = _gc.target_new_items
SKIP_EXISTING_ON_LIST = _gc.skip_existing_on_list
USE_TITLE_FALLBACK_KNOWN = _gc.use_title_fallback_known
DISCOVERY_MODE = _gc.discovery_mode


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


BOLD_EMAIL = _get_str_env("BOLD_EMAIL")
BOLD_PASSWORD = _get_str_env("BOLD_PASSWORD")
BOLD_HEADLESS = _get_bool_env("BOLD_HEADLESS", True)
BOLD_TIMEOUT_MS = _get_int_env("BOLD_TIMEOUT_MS", 120_000)
BOLD_SCROLL_STEPS = max(0, _get_int_env("BOLD_SCROLL_STEPS", 0))
BOLD_SCROLL_WAIT_MS = max(250, _get_int_env("BOLD_SCROLL_WAIT_MS", 1750))
BOLD_NO_NEW_ROUNDS_STOP = max(1, _get_int_env("BOLD_NO_NEW_ROUNDS_STOP", 4))
BOLD_RECOVERY_SCROLL_ROUNDS = max(0, _get_int_env("BOLD_RECOVERY_SCROLL_ROUNDS", 3))
BOLD_RECOVERY_WAIT_MS = max(500, _get_int_env("BOLD_RECOVERY_WAIT_MS", 5000))
BOLD_POST_LOGIN_WAIT_MS = max(500, _get_int_env("BOLD_POST_LOGIN_WAIT_MS", 2500))
BOLD_MAX_RECORDS_DEBUG = max(0, _get_int_env("BOLD_ORG_MAX_RECORDS_DEBUG", 0))
BOLD_KEEP_BROWSER_OPEN = _get_bool_env("BOLD_KEEP_BROWSER_OPEN", True)
BOLD_PREFILTER_STORE_PATH = _get_str_env(
    "BOLD_PREFILTER_STORE_PATH",
    os.path.join(_PARSER_ROOT, ".bold_prefilter_store.json"),
)
# Temporary refresh mode for backfilling older Bold rows with improved mapping.
# Set BOLD_FORCE_REFRESH=0 later to restore skip-existing behavior.
BOLD_FORCE_REFRESH = _get_bool_env("BOLD_FORCE_REFRESH", True)


def _normalize_key(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(name or "").strip().lower())


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _log(message: str) -> None:
    print(message, flush=True)


def _read_page_title(page: Any) -> str:
    try:
        return str(page.title() or "")
    except Exception:
        return ""


def _read_page_html(page: Any) -> str:
    try:
        return str(page.content() or "")
    except Exception:
        return ""


def _challenge_signal_text(page: Any) -> str:
    parts = [
        str(getattr(page, "url", "") or ""),
        _read_page_title(page),
        _read_page_html(page)[:50_000],
    ]
    return "\n".join(parts).lower()


def _detect_captcha_or_challenge(page: Any) -> str | None:
    text = _challenge_signal_text(page)
    for marker in _CAPTCHA_MARKERS:
        if marker in text:
            return marker
    return None


def _save_challenge_artifacts(page: Any, marker: str) -> None:
    try:
        page.screenshot(path=CAPTCHA_SCREENSHOT_PATH, full_page=True)
        _log(f"{SOURCE}: challenge screenshot saved -> {CAPTCHA_SCREENSHOT_PATH}")
    except Exception as exc:
        _log(f"{SOURCE}: warning: could not save challenge screenshot ({exc})")
    try:
        with open(CAPTCHA_HTML_PATH, "w", encoding="utf-8") as f:
            f.write(_read_page_html(page))
        _log(f"{SOURCE}: challenge html saved -> {CAPTCHA_HTML_PATH}")
    except Exception as exc:
        _log(f"{SOURCE}: warning: could not save challenge html ({exc})")
    _log(
        f"{SOURCE}: CAPTCHA_DETECTED marker={marker!r} "
        f"url={getattr(page, 'url', '')!r} title={_read_page_title(page)!r}"
    )


def _raise_if_challenge_detected(page: Any, *, phase: str) -> None:
    marker = _detect_captcha_or_challenge(page)
    if not marker:
        return
    _save_challenge_artifacts(page, marker)
    raise RuntimeError(
        f"Bold.org anti-bot challenge detected during {phase} "
        f"(marker={marker!r}, url={getattr(page, 'url', '')!r})"
    )


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


def _join_nonempty(parts: list[str | None], *, sep: str = "\n\n") -> str | None:
    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        text = _clean_text(part)
        if not text:
            continue
        if text in seen:
            continue
        seen.add(text)
        out.append(text)
    if not out:
        return None
    return sep.join(out)


def _bold_study_levels(item: dict[str, Any]) -> list[str]:
    raw = item.get("educationLevel")
    if not isinstance(raw, list):
        return []
    mapping = {
        "_highschool": "high_school_senior",
        "_undergraduate": "college_1",
        "_graduate": "graduate_student",
    }
    out: list[str] = []
    seen: set[str] = set()
    for token in raw:
        key = _normalize_key(token)
        mapped = mapping.get(key)
        if mapped and mapped not in seen:
            seen.add(mapped)
            out.append(mapped)
    return out


def _bold_eligibility_lines(item: dict[str, Any]) -> list[str]:
    raw = item.get("eligibility")
    if raw is None:
        return []
    lines: list[str] = []
    seen: set[str] = set()
    if isinstance(raw, list):
        for row in raw:
            if not isinstance(row, dict):
                continue
            label = _clean_text(row.get("label"))
            value = _clean_text(row.get("value"))
            text = None
            if label and value:
                text = f"{label}: {value}"
            elif value:
                text = value
            elif label:
                text = label
            if text and text not in seen:
                seen.add(text)
                lines.append(text)
    else:
        text = _clean_text(raw)
        if text:
            lines.append(text)
    return lines


def _bold_group_tags(item: dict[str, Any]) -> list[str]:
    raw = item.get("groups")
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for row in raw:
        if not isinstance(row, dict):
            continue
        for candidate in (
            _clean_text(row.get("name")),
            _clean_text(row.get("slug")),
            _clean_text(row.get("parentScholarshipGroupName")),
            _clean_text(row.get("parentScholarshipGroupSlug")),
            _clean_text(row.get("scholarshipCategorySlug")),
        ):
            if not candidate:
                continue
            normalized = candidate.strip().lower()
            if normalized not in seen:
                seen.add(normalized)
                out.append(normalized)
    return out


def _bold_essay_requirements(item: dict[str, Any]) -> str | None:
    essay = item.get("essay")
    if not isinstance(essay, dict):
        return None
    topic_html = essay.get("topic")
    topic_text = _strip_html(topic_html)
    min_len = essay.get("minLength")
    max_len = essay.get("maxLength")
    parts: list[str] = []
    if topic_text:
        parts.append(f"Essay prompt: {topic_text}")
    limits: list[str] = []
    if min_len is not None and str(min_len).strip():
        limits.append(f"min length {min_len}")
    if max_len is not None and str(max_len).strip():
        limits.append(f"max length {max_len}")
    if limits:
        parts.append("Essay requirements: " + ", ".join(limits))
    return "\n".join(parts).strip() or None


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
        for value in obj[:200]:
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


def _flatten_strings(value: Any, *, limit: int = 20) -> list[str]:
    out: list[str] = []

    def walk(node: Any) -> None:
        if len(out) >= limit:
            return
        if isinstance(node, str):
            text = _clean_text(node)
            if text:
                out.append(text)
            return
        if isinstance(node, (int, float)) and node == node:
            out.append(str(node))
            return
        if isinstance(node, list):
            for child in node:
                walk(child)
                if len(out) >= limit:
                    return
            return
        if isinstance(node, dict):
            for child in node.values():
                walk(child)
                if len(out) >= limit:
                    return

    walk(value)
    return out


def _string_list(obj: Any, keys: tuple[str, ...], *, limit: int = 20) -> list[str]:
    values = _iter_direct_values(obj, keys) + _iter_recursive_values(obj, keys)
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        for item in _flatten_strings(value, limit=limit):
            if item not in seen:
                seen.add(item)
                out.append(item)
            if len(out) >= limit:
                return out
    return out


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


def _slug_from_url(url: str | None) -> str | None:
    if not url:
        return None
    path = urlparse(url).path.strip("/")
    if not path:
        return None
    parts = [part for part in path.split("/") if part]
    if not parts:
        return None
    return parts[-1]


def _search_scholarship_url(node: Any, max_depth: int = 5) -> str | None:
    if max_depth < 0:
        return None
    if isinstance(node, str):
        text = node.strip()
        if "/scholarships/" in text or text.startswith("/scholarships/"):
            return _to_absolute_url(text)
        return None
    if isinstance(node, dict):
        for value in node.values():
            hit = _search_scholarship_url(value, max_depth=max_depth - 1)
            if hit:
                return hit
    elif isinstance(node, list):
        for value in node[:100]:
            hit = _search_scholarship_url(value, max_depth=max_depth - 1)
            if hit:
                return hit
    return None


def _format_usd(value: float | int | None) -> str | None:
    if value is None:
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    if num <= 0:
        return None
    if num.is_integer():
        return f"${int(num):,}"
    return f"${num:,.2f}"


def _parse_numeric_amount(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)) and value == value:
        num = float(value)
        return num if num > 0 else None
    if isinstance(value, str):
        cleaned = value.replace(",", "").strip()
        match = re.search(r"(\d+(?:\.\d+)?)", cleaned)
        if not match:
            return None
        try:
            num = float(match.group(1))
        except ValueError:
            return None
        return num if num > 0 else None
    return None


_DOCUMENT_URL_RE = re.compile(
    r"https?://[^\s'\"<>]+",
    re.I,
)


def _is_document_url(url: str) -> bool:
    text = (url or "").strip()
    if not text:
        return False
    lower = text.lower()
    lower_no_query = lower.split("?", 1)[0].split("#", 1)[0]
    if lower_no_query.endswith(".pdf"):
        return True
    if "drive.google.com/" in lower:
        return True
    if "docs.google.com/" in lower:
        return True
    return False


def _extract_document_urls_from_text(value: str) -> list[str]:
    text = (value or "").strip()
    if not text:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for match in _DOCUMENT_URL_RE.finditer(text):
        url = match.group(0).strip().rstrip("),.;")
        if not _is_document_url(url):
            continue
        if url not in seen:
            seen.add(url)
            out.append(url)
    if _is_document_url(text) and text not in seen:
        out.append(text)
    return out


def _collect_document_urls(node: Any, out: list[str], seen: set[str]) -> None:
    if isinstance(node, str):
        for url in _extract_document_urls_from_text(node):
            if url not in seen:
                seen.add(url)
                out.append(url)
        return
    if isinstance(node, dict):
        for value in node.values():
            _collect_document_urls(value, out, seen)
        return
    if isinstance(node, list):
        for value in node:
            _collect_document_urls(value, out, seen)


def _extract_document_url_items(node: Any) -> list[dict[str, str]]:
    urls: list[str] = []
    seen: set[str] = set()
    _collect_document_urls(node, urls, seen)
    return [{"title": "Document", "url": url} for url in urls]


def _parse_iso_dateish(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        match = re.search(r"(\d{4}-\d{2}-\d{2})", raw)
        if match:
            return match.group(1)
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
        except (OverflowError, OSError, ValueError):
            return None
    return None


def _first_nested_name(obj: Any, container_keys: tuple[str, ...]) -> str | None:
    values = _iter_direct_values(obj, container_keys) + _iter_recursive_values(obj, container_keys)
    for value in values:
        if isinstance(value, dict):
            name = _first_str(value, ("name", "title", "displayName"))
            if name:
                return name
    return None


def _looks_like_scholarship_obj(obj: Any) -> bool:
    if not isinstance(obj, dict):
        return False
    title = _first_str(obj, _TITLE_KEYS)
    if not title:
        return False
    url = _candidate_url(obj)
    source_id = _candidate_source_id(obj, url)
    signal_count = 0
    if title:
        signal_count += 2
    if url or source_id:
        signal_count += 1
    if (
        _first_str(obj, _AMOUNT_TEXT_KEYS)
        or _first_value(obj, _AMOUNT_VALUE_KEYS) is not None
        or _first_str(obj, _DEADLINE_TEXT_KEYS)
        or _first_value(obj, _DEADLINE_DATE_KEYS) is not None
        or _first_str(obj, _DESCRIPTION_KEYS)
        or _first_str(obj, _STATUS_KEYS)
    ):
        signal_count += 1
    return signal_count >= 4


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
        for value in node[:250]:
            if isinstance(value, dict) and _looks_like_scholarship_obj(value):
                out.append(value)
            if isinstance(value, (dict, list)):
                out.extend(_extract_scholarship_candidates(value, max_depth=max_depth - 1))
    return out


def _candidate_url(item: dict[str, Any]) -> str | None:
    raw = _first_str(item, _URL_KEYS)
    url = _to_absolute_url(raw)
    if url:
        return url
    slug = _first_str(item, _SLUG_KEYS)
    if slug:
        return f"{SITE_ORIGIN}/scholarships/{slug.strip('/').strip()}/"
    return _search_scholarship_url(item)


def _candidate_source_id(item: dict[str, Any], url: str | None) -> str | None:
    direct = _first_str(item, _ID_KEYS)
    if direct:
        return direct
    slug = _first_str(item, _SLUG_KEYS)
    if slug:
        return slug
    return _slug_from_url(url)


def _candidate_deadline_text(item: dict[str, Any]) -> str | None:
    text = _first_str(item, _DEADLINE_TEXT_KEYS)
    if text:
        return text
    date_like = _first_value(item, _DEADLINE_DATE_KEYS)
    iso = _parse_iso_dateish(date_like)
    return iso


def _candidate_deadline_date(item: dict[str, Any], deadline_text: str | None) -> str | None:
    direct = _first_value(item, _DEADLINE_DATE_KEYS)
    iso = _parse_iso_dateish(direct)
    if iso:
        return iso
    return parse_deadline_date(deadline_text)


def _fast_prefilter_deadline(item: dict[str, Any]) -> tuple[str | None, str]:
    deadline_text = _candidate_deadline_text(item)
    deadline_date = _candidate_deadline_date(item, deadline_text)
    return deadline_date, classify_business_deadline(deadline_date)


def _candidate_award_text(item: dict[str, Any]) -> str | None:
    text = _first_str(item, _AMOUNT_TEXT_KEYS)
    if text:
        return text
    numeric = _parse_numeric_amount(_first_value(item, _AMOUNT_VALUE_KEYS))
    return _format_usd(numeric)


def _candidate_provider_name(item: dict[str, Any]) -> str | None:
    direct = _first_str(item, _PROVIDER_NAME_KEYS)
    if direct:
        return direct
    return _first_nested_name(item, ("provider", "donor", "organization", "sponsor", "fund", "owner"))


def _candidate_provider_url(item: dict[str, Any]) -> str | None:
    direct = _first_str(item, _PROVIDER_URL_KEYS)
    if direct:
        return _to_absolute_url(direct)
    nested = _first_value(item, ("provider", "donor", "organization", "sponsor"))
    if isinstance(nested, dict):
        return _to_absolute_url(_first_str(nested, ("url", "href", "profileUrl")))
    return None


def _candidate_external_apply_url(item: dict[str, Any]) -> str | None:
    """
    Prefer a non-Bold destination when the payload exposes a direct sponsor/apply link.
    This intentionally checks both top-level and nested payloads such as raw_list_card.link.
    """
    candidate_keys = ("link",) + _APPLY_URL_KEYS
    values = _iter_direct_values(item, candidate_keys) + _iter_recursive_values(
        item,
        candidate_keys,
    )
    for value in values:
        text = _clean_text(value)
        if not text:
            continue
        url = _to_absolute_url(text)
        if url and not _is_bold_url(url):
            return url
    return None


def _is_bold_url(value: str | None) -> bool:
    if not value:
        return False
    try:
        host = (urlparse(str(value)).netloc or "").lower()
    except Exception:
        return False
    return host.endswith("bold.org")


def _candidate_is_active(item: dict[str, Any], status_text: str | None) -> bool | None:
    raw = _first_value(item, _IS_ACTIVE_KEYS)
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)) and raw == raw:
        return bool(raw)
    if isinstance(raw, str):
        low = raw.strip().lower()
        if low in {"open", "active", "available", "true", "1"}:
            return True
        if low in {"closed", "expired", "inactive", "false", "0"}:
            return False
    low_status = (status_text or "").strip().lower()
    if low_status in {"open", "active", "available"}:
        return True
    if low_status in {"closed", "expired", "inactive"}:
        return False
    return None


def _record_identity(record: dict[str, Any]) -> str:
    return " | ".join(
        [
            str(record.get("source_id") or "").strip(),
            str(record.get("url") or "").strip(),
            str(record.get("title") or "").strip(),
        ]
    )


def _snapshot_hash(item: dict[str, Any]) -> str:
    blob = json.dumps(item, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.md5(blob.encode("utf-8")).hexdigest()


def enforce_required_defaults(record: dict[str, Any]) -> dict[str, Any]:
    """
    Central place for Bold-specific technical defaults required by DB constraints.

    Keep this intentionally conservative: only force fields that are effectively
    technical availability/indexability flags or other safe non-null defaults.

    Current technical defaults:
    - source, currency
    - is_active
    - mark_started_available
    - mark_submitted_available
    - is_indexable
    - is_recurring
    - is_verified
    """
    out = dict(record)
    out["source"] = SOURCE
    out["currency"] = DEFAULT_CURRENCY
    out["is_active"] = True
    out["mark_started_available"] = True
    out["mark_submitted_available"] = True
    out["is_indexable"] = True
    out["is_recurring"] = bool(out.get("is_recurring"))
    out["is_verified"] = bool(out.get("is_verified"))
    if (
        not _clean_text(out.get("provider_url"))
        and _clean_text(out.get("apply_url"))
        and not _is_bold_url(out.get("apply_url"))
    ):
        out["provider_url"] = out.get("apply_url")
    out["apply_button_text"] = "Visit Website"
    return out


def _safe_click_first(page: Any, selectors: tuple[str, ...]) -> bool:
    for selector in selectors:
        locator = page.locator(selector)
        try:
            if locator.count() < 1:
                continue
            locator.first.click()
            return True
        except Exception:
            continue
    return False


def _safe_fill_first(page: Any, selectors: tuple[str, ...], value: str) -> bool:
    for selector in selectors:
        locator = page.locator(selector)
        try:
            if locator.count() < 1:
                continue
            locator.first.fill(value)
            return True
        except Exception:
            continue
    return False


def _dismiss_cookie_banner(page: Any) -> bool:
    selectors = (
        'button:has-text("Accept only essential")',
        'button:has-text("Accept essential")',
        'button:has-text("Accept all")',
        'button:has-text("I agree")',
        'button:has-text("Got it")',
        '[aria-label*="cookie" i] button',
        '[id*="cookie" i] button',
        '[class*="cookie" i] button',
    )
    clicked = _safe_click_first(page, selectors)
    if clicked:
        page.wait_for_timeout(500)
    return clicked


def _login_submit_locator(page: Any) -> Any | None:
    candidates = (
        page.get_by_role(
            "button",
            name=re.compile(r"log in|sign in|submit|continue", re.IGNORECASE),
        ),
        page.get_by_role(
            "link",
            name=re.compile(r"log in|sign in|submit|continue", re.IGNORECASE),
        ),
        page.locator('button[type="submit"]'),
        page.locator('input[type="submit"]'),
        page.locator('button:has-text("Sign in")'),
        page.locator('button:has-text("Log in")'),
        page.locator('button:has-text("Continue")'),
    )
    for locator in candidates:
        try:
            if locator.count() < 1:
                continue
            return locator.first
        except Exception:
            continue
    return None


def _login_complete(page: Any) -> bool:
    """
    Heuristic check: Bold may redirect to different authenticated pages, so we treat
    disappearance of login inputs and headings as a successful sign-in signal too.
    """
    try:
        current_url = (page.url or "").lower()
    except Exception:
        current_url = ""

    if current_url and not any(
        token in current_url
        for token in (
            "/reset-password",
            "/register/",
        )
    ):
        if any(
            token in current_url
            for token in (
                "/dashboard",
                "/profile",
                "/account",
                "/applicant",
                "/scholarships/",
            )
        ):
            try:
                password_fields = page.locator('input[type="password"]').count()
            except Exception:
                password_fields = 0
            if password_fields == 0:
                return True

    checks = (
        ('input[type="email"]', 0),
        ('input[type="password"]', 0),
    )
    fields_gone = True
    for selector, expected in checks:
        try:
            if page.locator(selector).count() != expected:
                fields_gone = False
                break
        except Exception:
            continue
    if fields_gone:
        return True

    try:
        login_heading = page.get_by_role(
            "heading",
            name=re.compile(r"sign in|log in", re.IGNORECASE),
        )
        if login_heading.count() == 0:
            return True
    except Exception:
        pass

    return False


def _wait_for_login_complete(page: Any, timeout_ms: int) -> bool:
    deadline = time.time() + (max(1000, timeout_ms) / 1000.0)
    while time.time() < deadline:
        if _login_complete(page):
            return True
        page.wait_for_timeout(500)
    return _login_complete(page)


def _login(page: Any) -> None:
    if not BOLD_EMAIL or not BOLD_PASSWORD:
        raise RuntimeError("Set BOLD_EMAIL and BOLD_PASSWORD before running sources.bold_org")

    page.goto(LOGIN_URL, wait_until="domcontentloaded")
    page.wait_for_timeout(800)
    _raise_if_challenge_detected(page, phase="login_page_open")

    email_ok = _safe_fill_first(
        page,
        (
            'input[type="email"]',
            'input[name="email"]',
            'input[autocomplete="email"]',
            'input[placeholder*="Email" i]',
        ),
        BOLD_EMAIL,
    )
    password_ok = _safe_fill_first(
        page,
        (
            'input[type="password"]',
            'input[name="password"]',
            'input[autocomplete="current-password"]',
            'input[placeholder*="Password" i]',
        ),
        BOLD_PASSWORD,
    )
    if not email_ok or not password_ok:
        raise RuntimeError("Could not find Bold.org login form fields")

    submit = _login_submit_locator(page)
    if submit is not None:
        try:
            submit.click()
        except Exception:
            submit = None

    if submit is None:
        print("Пожалуйста, нажмите кнопку входа вручную в браузере...")
        page.wait_for_timeout(30_000)
    else:
        page.wait_for_timeout(BOLD_POST_LOGIN_WAIT_MS)

    try:
        page.wait_for_load_state("networkidle", timeout=15_000)
    except Exception:
        pass
    _raise_if_challenge_detected(page, phase="post_login_submit")

    if not _wait_for_login_complete(page, timeout_ms=30_000):
        raise RuntimeError(
            "Bold.org login did not complete after automatic/manual submit wait"
        )
    _raise_if_challenge_detected(page, phase="login_complete_check")

    try:
        page.context.storage_state(path=SESSION_STATE_PATH)
        print(f"{SOURCE}: saved session state -> {SESSION_STATE_PATH}")
    except Exception as exc:
        print(f"{SOURCE}: warning: could not save session state ({exc})")


def _human_scroll(page: Any) -> None:
    distances = (2200, 2600, 1800, -1400, 2400, -900)
    for dist in distances:
        try:
            page.mouse.wheel(0, dist)
        except Exception:
            pass
        page.wait_for_timeout(900)


def _page_scroll_height(page: Any) -> int | None:
    try:
        return int(page.evaluate("() => document.body ? document.body.scrollHeight : 0"))
    except Exception:
        return None


def _run_recovery_scroll(page: Any, state: "_CaptureState", *, page_idx: int, step: int) -> bool:
    before_count = state.candidate_items_seen
    for recovery_idx in range(1, BOLD_RECOVERY_SCROLL_ROUNDS + 1):
        _dismiss_cookie_banner(page)
        try:
            page.mouse.wheel(0, 7000)
            page.wait_for_timeout(700)
            page.mouse.wheel(0, -1200)
            page.wait_for_timeout(500)
            page.mouse.wheel(0, 8000)
        except Exception:
            pass
        page.wait_for_timeout(BOLD_RECOVERY_WAIT_MS)
        delta = state.candidate_items_seen - before_count
        _log(
            f"{SOURCE}: recovery scroll {recovery_idx}/{BOLD_RECOVERY_SCROLL_ROUNDS} "
            f"after step {step} on page {page_idx}/2 -> "
            f"new_candidates={delta} total={state.candidate_items_seen}"
        )
        if delta > 0:
            return True
    return False


def _visit_scholarship_pages(page: Any, state: "_CaptureState") -> None:
    urls = (PUBLIC_SCHOLARSHIPS_URL, APP_SCHOLARSHIPS_URL)
    for page_idx, url in enumerate(urls, start=1):
        _log(f"{SOURCE}: visit page {page_idx}/{len(urls)} -> {url}")
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(1000)
        _raise_if_challenge_detected(page, phase=f"page_open:{url}")
        if _dismiss_cookie_banner(page):
            _log(f"{SOURCE}: cookie banner dismissed")
        _human_scroll(page)
        no_new_rounds = 0
        start_candidates = state.candidate_items_seen
        step = 0
        while True:
            if BOLD_SCROLL_STEPS > 0 and step >= BOLD_SCROLL_STEPS:
                _log(
                    f"{SOURCE}: reached scroll cap {BOLD_SCROLL_STEPS} "
                    f"on page {page_idx}/{len(urls)}"
                )
                break
            step += 1
            before_count = state.candidate_items_seen
            before_height = _page_scroll_height(page)
            _dismiss_cookie_banner(page)
            clicked_more = _safe_click_first(
                page,
                (
                    'button:has-text("Load more")',
                    'button:has-text("See more")',
                    'button:has-text("Show more")',
                ),
            )
            try:
                page.mouse.wheel(0, 5000)
            except Exception:
                pass
            page.wait_for_timeout(BOLD_SCROLL_WAIT_MS)
            after_count = state.candidate_items_seen
            after_height = _page_scroll_height(page)
            delta = after_count - before_count
            height_changed = (
                before_height is not None
                and after_height is not None
                and after_height > before_height
            )
            if delta > 0:
                no_new_rounds = 0
            else:
                no_new_rounds += 1
            _log(
                f"{SOURCE}: scroll step {step} on page {page_idx}/{len(urls)} "
                f"-> new_candidates={delta} total={after_count} "
                f"clicked_more={clicked_more} height_changed={height_changed} "
                f"no_new_rounds={no_new_rounds}/{BOLD_NO_NEW_ROUNDS_STOP}"
            )
            if no_new_rounds >= BOLD_NO_NEW_ROUNDS_STOP:
                recovered = False
                if BOLD_RECOVERY_SCROLL_ROUNDS > 0:
                    _log(
                        f"{SOURCE}: no new candidates for {no_new_rounds} rounds; "
                        f"starting recovery scrolls on page {page_idx}/{len(urls)}"
                    )
                    recovered = _run_recovery_scroll(
                        page,
                        state,
                        page_idx=page_idx,
                        step=step,
                    )
                if recovered:
                    no_new_rounds = 0
                    _log(
                        f"{SOURCE}: recovery scroll found more candidates; "
                        f"continuing page {page_idx}/{len(urls)}"
                    )
                    continue
                _log(
                    f"{SOURCE}: stop scrolling page {page_idx}/{len(urls)} "
                    f"after {no_new_rounds} rounds without new candidates"
                )
                break
        _log(
            f"{SOURCE}: page {page_idx}/{len(urls)} done -> "
            f"new candidates on page={state.candidate_items_seen - start_candidates}, "
            f"total candidates={state.candidate_items_seen}"
        )


def _build_record(item: dict[str, Any], response_url: str) -> dict[str, Any] | None:
    source_id = str(item.get("id")) if item.get("id") is not None else None
    title = _clean_text(item.get("name"))
    url = _to_absolute_url(_clean_text(item.get("link")))
    if not url:
        slug = _clean_text(item.get("slug"))
        if slug:
            url = f"{SITE_ORIGIN}/scholarships/{slug.strip('/')}/"
    if not title or not url:
        return None

    raw_amount = (
        item.get("amount")
        if item.get("amount") is not None
        else item.get("totalAwardAmount")
        if item.get("totalAwardAmount") is not None
        else item.get("fundingRequestAmount")
    )
    award_amount_text = None
    if raw_amount is not None and str(raw_amount).strip() != "":
        amount_str = str(raw_amount).strip()
        award_amount_text = amount_str if amount_str.startswith("$") else f"${amount_str}"
    award_amount_min, award_amount_max = parse_award_min_max(award_amount_text)
    deadline_raw = item.get("endDate") if item.get("endDate") else item.get("deadline")
    deadline_text = _clean_text(deadline_raw)
    deadline_date = _parse_iso_dateish(deadline_raw) or _candidate_deadline_date(
        item,
        deadline_text,
    )
    description_short = _clean_text(item.get("description"))
    content_html = _clean_text(item.get("content"))
    content_text = _strip_html(content_html)
    eligibility_lines = _bold_eligibility_lines(item)
    eligibility_text = "\n".join(eligibility_lines) if eligibility_lines else None
    essay_requirements = _bold_essay_requirements(item)
    donor = item.get("donor") if isinstance(item.get("donor"), dict) else {}
    donor_mission = _clean_text(donor.get("mission")) if donor else None
    description = _join_nonempty(
        [
            description_short,
            content_text,
        ]
    )
    if not eligibility_text:
        eligibility_text = _join_nonempty(
            [
                content_text,
                description_short,
            ]
        )
    requirements_text = _join_nonempty(
        [
            essay_requirements,
            eligibility_text,
            content_text,
        ]
    )
    donor = item.get("donor") if isinstance(item.get("donor"), dict) else {}
    donor_first = _clean_text(donor.get("firstName")) if donor else None
    donor_last = _clean_text(donor.get("lastName")) if donor else None
    donor_name = " ".join(part for part in (donor_first, donor_last) if part).strip() or None
    funded_by = _clean_text(item.get("fundedBy"))
    provider_name = funded_by or donor_name
    external_apply_url = _candidate_external_apply_url(item)
    apply_url = external_apply_url or url
    provider_url = _candidate_provider_url(item)
    if provider_url and _is_bold_url(provider_url):
        provider_url = None
    if not provider_url and external_apply_url:
        provider_url = apply_url
    provider_is_external = bool(provider_url and not _is_bold_url(provider_url))
    apply_is_external = bool(apply_url and not _is_bold_url(apply_url))
    status_text = _clean_text(item.get("status"))
    is_active = _candidate_is_active(item, status_text)
    recurrency_text = _clean_text(item.get("recurrency"))
    is_recurring = bool(recurrency_text)
    category = _clean_text(item.get("category"))
    tags: list[str] = []
    if category:
        tags.append(category.strip().lower())
    for token in _bold_group_tags(item):
        if token not in tags:
            tags.append(token)
        if len(tags) >= 20:
            break
    study_levels = _bold_study_levels(item)
    number_of_awards = item.get("numberOfAwards")
    applicants_count = item.get("numberOfApplicants")
    education_level_raw = item.get("educationLevel")

    raw_data = {
        k: v
        for k, v in item.items()
        if k not in {"winners", "finalists", "cycles", "resources"}
    }
    raw_data["url"] = apply_url
    document_urls = _extract_document_url_items(raw_data)

    if not status_text and is_active is not None:
        status_text = "Open" if is_active else "Closed"

    record: dict[str, Any] = {
        "source": SOURCE,
        "source_id": source_id,
        "url": url,
        "title": title,
        "provider_name": provider_name,
        "provider_url": provider_url,
        "provider_mission": donor_mission,
        "award_amount_text": award_amount_text,
        "award_amount_min": award_amount_min,
        "award_amount_max": award_amount_max,
        "currency": DEFAULT_CURRENCY,
        "deadline_text": deadline_text,
        "deadline_date": deadline_date,
        "description": description,
        "description_html": content_html,
        "eligibility_text": eligibility_text,
        "requirements_text": requirements_text,
        "apply_url": apply_url,
        "apply_button_text": "Visit Website",
        "mark_started_available": True,
        "mark_submitted_available": True,
        "status_text": status_text,
        "number_of_awards": number_of_awards,
        "applicants_count": applicants_count,
        "study_levels": study_levels,
        "category": category,
        "official_source_name": "Bold.org",
        "tags": tags,
        "document_urls": document_urls,
        "is_active": True if is_active is None else bool(is_active),
        "is_recurring": is_recurring,
        "raw_data": _json_safe(
            {
                "captured_at": _now_iso(),
                "response_url": response_url,
                "raw_list_card": raw_data,
                "education_level_raw": education_level_raw,
                "eligibility_lines": eligibility_lines,
                "essay_requirements_text": essay_requirements,
                "recurrency_text": recurrency_text,
                "provider_name_source": "fundedBy" if funded_by else "donor" if donor_name else None,
                "provider_is_external": provider_is_external,
                "apply_is_external": apply_is_external,
            }
        ),
    }

    apply_normalization(record)

    for key in SCHOLARSHIP_RECORD_DEFAULT_KEYS:
        if key not in record:
            record[key] = None

    return enforce_required_defaults(record)


def _build_listing_preview(item: dict[str, Any]) -> dict[str, Any] | None:
    """
    Минимальная карточка для раннего skip через known index.

    В Bold-парсере нет отдельного detail-fetch на каждый грант, но этот preview
    позволяет отсеять уже известные записи до полного build_record / normalization.
    """
    title = _clean_text(item.get("name"))
    url = _to_absolute_url(_clean_text(item.get("link")))
    if not url:
        slug = _clean_text(item.get("slug"))
        if slug:
            url = f"{SITE_ORIGIN}/scholarships/{slug.strip('/')}/"
    if not title or not url:
        return None
    return {
        "source": SOURCE,
        "source_id": str(item.get("id")) if item.get("id") is not None else _slug_from_url(url),
        "url": url,
        "title": title,
    }


class _CaptureState:
    def __init__(self) -> None:
        self.response_urls_seen = 0
        self.json_responses_seen = 0
        self.candidate_items_seen = 0
        self.captured: list[tuple[dict[str, Any], str]] = []
        self.identities: set[str] = set()


def _capture_identity(item: dict[str, Any], response_url: str) -> str:
    url = _candidate_url(item) or ""
    source_id = _candidate_source_id(item, url) or ""
    title = _first_str(item, _TITLE_KEYS) or ""
    return " | ".join([source_id, url, title, response_url])


def _response_handler_factory(state: _CaptureState):
    def _handler(response: Any) -> None:
        state.response_urls_seen += 1
        url_low = (response.url or "").lower()
        content_type = (response.headers.get("content-type") or "").lower()
        if response.status >= 400:
            return
        if "json" not in content_type and not any(
            token in url_low for token in ("graphql", "/api/", "scholar", "search")
        ):
            return
        try:
            payload = response.json()
        except Exception:
            return
        state.json_responses_seen += 1
        if isinstance(payload, dict):
            payload_json = payload.get("json")
            if isinstance(payload_json, dict):
                data_rows = payload_json.get("data")
                if isinstance(data_rows, list):
                    payload = data_rows
        candidates = _extract_scholarship_candidates(payload)
        if not candidates:
            return
        new_added = 0
        for item in candidates:
            identity = _capture_identity(item, response.url)
            if identity in state.identities:
                continue
            state.identities.add(identity)
            state.captured.append((item, response.url))
            state.candidate_items_seen += 1
            new_added += 1
        if new_added:
            _log(
                f"{SOURCE}: candidates +{new_added} from {response.url} "
                f"(total={state.candidate_items_seen})"
            )

    return _handler


def run() -> None:
    from playwright.sync_api import sync_playwright

    ai_usage_start = snapshot_ai_usage()
    store = BoldPrefilterStore(BOLD_PREFILTER_STORE_PATH)
    store.load()
    effective_target = (
        min(TARGET_NEW_ITEMS, BOLD_MAX_RECORDS_DEBUG)
        if BOLD_MAX_RECORDS_DEBUG > 0
        else TARGET_NEW_ITEMS
    )
    use_skip = (
        SKIP_EXISTING_ON_LIST
        and DISCOVERY_MODE == "new_only"
        and not BOLD_FORCE_REFRESH
    )

    print(
        f"{SOURCE}: Playwright JSON capture "
        f"(TARGET_NEW_ITEMS={TARGET_NEW_ITEMS}, "
        f"effective_target_upserts={effective_target}, "
        f"BOLD_HEADLESS={BOLD_HEADLESS}, "
        f"BOLD_TIMEOUT_MS={BOLD_TIMEOUT_MS}, "
        f"BOLD_FORCE_REFRESH={BOLD_FORCE_REFRESH}, "
        f"SKIP_EXISTING_ON_LIST={SKIP_EXISTING_ON_LIST}, "
        f"DISCOVERY_MODE={DISCOVERY_MODE!r})"
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
        except Exception as exc:
            print(f"  warning: could not load known index ({exc}); continuing without skip")
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
    }
    seen_records_session: set[str] = set()
    state = _CaptureState()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=BOLD_HEADLESS)
        context_kwargs: dict[str, Any] = {}
        if os.path.exists(SESSION_STATE_PATH):
            context_kwargs["storage_state"] = SESSION_STATE_PATH
            _log(f"{SOURCE}: using saved session state -> {SESSION_STATE_PATH}")
        context = browser.new_context(**context_kwargs)
        page = context.new_page()
        page.set_default_timeout(BOLD_TIMEOUT_MS)
        page.on("response", _response_handler_factory(state))

        try:
            if context_kwargs.get("storage_state"):
                page.goto(APP_SCHOLARSHIPS_URL, wait_until="domcontentloaded")
                page.wait_for_timeout(1200)
                if not _login_complete(page):
                    _log(f"{SOURCE}: saved session invalid; performing fresh login")
                    _login(page)
            else:
                _login(page)
            _visit_scholarship_pages(page, state)
            page.wait_for_timeout(1500)
            if not state.captured:
                _log(f"{SOURCE}: no candidates yet, waiting extra 10s for late JSON...")
                page.wait_for_timeout(10_000)
            if not state.captured:
                raise RuntimeError(
                    "No scholarship-like JSON payloads were captured from Bold.org. "
                    "Open DevTools/network once, confirm the live API response shape, and "
                    "adjust the TODO key lists in sources/bold_org/parser.py if needed."
                )

            _log(
                f"  captured JSON responses: {state.json_responses_seen}, "
                f"candidate items: {state.candidate_items_seen}"
            )

            total_captured = len(state.captured)
            for idx_entry, (item, response_url) in enumerate(state.captured, start=1):
                stats["captured_candidates"] += 1
                if idx_entry == 1 or idx_entry % 50 == 0 or idx_entry == total_captured:
                    _log(
                        f"{SOURCE}: prefilter progress {idx_entry}/{total_captured} "
                        f"(pass={stats['prefilter_pass']} "
                        f"deadline_reject={stats['prefilter_reject_deadline']} "
                        f"funding_reject={stats['prefilter_reject_funding']} "
                        f"known_reject={stats['prefilter_reject_known']})"
                    )
                preview = _build_listing_preview(item)
                prefilter_status = PREFILTER_PASS
                prefilter_reason = ""

                if use_skip and preview is not None:
                    known = bool(
                        listing_is_known(
                            preview,
                            idx,
                            title_fallback=USE_TITLE_FALLBACK_KNOWN,
                        )
                    )
                    if known:
                        stats["known_skipped"] += 1
                        stats["prefilter_reject_known"] += 1
                        record_ai_skip()
                        prefilter_status = PREFILTER_REJECT_KNOWN
                        prefilter_reason = "known_in_db"
                        store.upsert_candidate(
                            source_id=preview.get("source_id"),
                            url=preview.get("url"),
                            title=preview.get("title"),
                            response_url=response_url,
                            snapshot_hash=_snapshot_hash(item),
                            prefilter_status=prefilter_status,
                            prefilter_reason=prefilter_reason,
                            item_snapshot=item,
                        )
                        _log(
                            f"  [SKIP] Grant ID: {preview.get('source_id') or '?'} already exists. AI skipped."
                        )
                        continue

                fast_deadline_date, fast_dbiz = _fast_prefilter_deadline(item)
                if fast_dbiz != "ok":
                    stats["prefilter_reject_deadline"] += 1
                    title = preview.get("title") if preview else _clean_text(item.get("name"))
                    fast_url = preview.get("url") if preview else _candidate_url(item)
                    fast_source_id = (
                        preview.get("source_id")
                        if preview
                        else _candidate_source_id(item, fast_url)
                    )
                    store.upsert_candidate(
                        source_id=fast_source_id,
                        url=fast_url,
                        title=title,
                        response_url=response_url,
                        snapshot_hash=_snapshot_hash(item),
                        prefilter_status=PREFILTER_REJECT_DEADLINE,
                        prefilter_reason=f"early_deadline:{fast_dbiz}:{fast_deadline_date or ''}",
                        item_snapshot=item,
                    )
                    continue

                record = _build_record(item, response_url)
                if not record:
                    stats["mapped_skipped"] += 1
                    stats["prefilter_reject_mapping"] += 1
                    title = preview.get("title") if preview else _clean_text(item.get("name"))
                    store.upsert_candidate(
                        source_id=preview.get("source_id") if preview else item.get("id"),
                        url=preview.get("url") if preview else _candidate_url(item),
                        title=title,
                        response_url=response_url,
                        snapshot_hash=_snapshot_hash(item),
                        prefilter_status=PREFILTER_REJECT_MAPPING,
                        prefilter_reason="build_record_failed",
                        item_snapshot=item,
                    )
                    continue

                if not has_meaningful_funding(record):
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
                        prefilter_reason=str(dbiz),
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
            _log(f"  bold prefilter store: {BOLD_PREFILTER_STORE_PATH}")
            _log(f"  deep candidates queued: {stats['deep_candidates']}")

            total_deep = len(deep_candidates)
            for deep_idx, entry in enumerate(deep_candidates, start=1):
                if deep_idx == 1 or deep_idx % 25 == 0 or deep_idx == total_deep:
                    _log(
                        f"{SOURCE}: deep progress {deep_idx}/{total_deep} "
                        f"(upsert_ok={stats['upsert_ok']} upsert_failed={stats['upsert_failed']})"
                    )
                item = entry.get("item_snapshot")
                response_url = str(entry.get("response_url") or "")
                if not isinstance(item, dict):
                    continue
                record = _build_record(item, response_url)
                if not record:
                    continue
                record_id = _record_identity(record)
                if record_id in seen_records_session:
                    continue
                seen_records_session.add(record_id)

                try:
                    upsert_scholarship(record)
                    stats["upsert_ok"] += 1
                    store.mark_processed(entry)
                    _log(
                        f"  upsert OK ({stats['upsert_ok']}/{effective_target}): {record['title'][:80]}"
                    )
                    if effective_target > 0 and stats["upsert_ok"] >= effective_target:
                        _log(
                            f"{SOURCE}: reached effective_target_upserts={effective_target}, stopping deep pass"
                        )
                        break
                except Exception as exc:
                    stats["upsert_failed"] += 1
                    _log(f"  upsert failed: {record['title'][:80]} -> {exc}")
                time.sleep(0.03)

            store.save()

            _log("")
            _log(f"captured candidates: {stats['captured_candidates']}")
            _log(f"prefilter pass: {stats['prefilter_pass']}")
            _log(f"prefilter reject known: {stats['prefilter_reject_known']}")
            _log(f"prefilter reject mapping: {stats['prefilter_reject_mapping']}")
            _log(f"prefilter reject funding: {stats['prefilter_reject_funding']}")
            _log(f"prefilter reject deadline: {stats['prefilter_reject_deadline']}")
            _log(f"deep candidates: {stats['deep_candidates']}")
            _log(f"known skipped: {stats['known_skipped']}")
            _log(f"mapping skipped: {stats['mapped_skipped']}")
            _log(f"skip no funding: {stats['skip_no_funding']}")
            _log(f"skip no deadline: {stats['skip_no_deadline']}")
            _log(f"skip expired: {stats['skip_expired']}")
            _log(
                f"skip deadline too close (<{MIN_LEAD_DAYS_BEFORE_DEADLINE}d): {stats['skip_deadline_too_close']}"
            )
            _log(f"upsert OK: {stats['upsert_ok']}")
            _log(f"upsert failed: {stats['upsert_failed']}")
            print_ai_session_summary(
                SOURCE,
                processed=stats["captured_candidates"],
                new_found=stats["upsert_ok"],
                start=ai_usage_start,
            )

            if BOLD_KEEP_BROWSER_OPEN and not BOLD_HEADLESS:
                _log(f"{SOURCE}: keeping browser open; stop the process when you are done viewing it.")
                try:
                    while True:
                        page.wait_for_timeout(1000)
                except KeyboardInterrupt:
                    _log(f"{SOURCE}: browser hold interrupted, closing browser.")
        finally:
            context.close()
            browser.close()

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
)
_AMOUNT_VALUE_KEYS: tuple[str, ...] = (
    "awardAmount",
    "amount",
    "scholarshipAmount",
    "awardValue",
    "amountValue",
)
_DEADLINE_TEXT_KEYS: tuple[str, ...] = (
    "deadlineText",
    "deadline",
    "applicationDeadline",
    "closeDateText",
)
_DEADLINE_DATE_KEYS: tuple[str, ...] = (
    "deadlineDate",
    "deadlineAt",
    "applicationDeadlineAt",
    "deadline",
    "closeDate",
    "endDate",
    "expiresAt",
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
_IS_ACTIVE_KEYS: tuple[str, ...] = (
    "isActive",
    "active",
    "isOpen",
    "open",
)
_APPLY_URL_KEYS: tuple[str, ...] = (
    "applyUrl",
    "applicationUrl",
    "ctaUrl",
    "externalUrl",
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
