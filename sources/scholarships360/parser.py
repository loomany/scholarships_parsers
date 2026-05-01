"""Scholarships360 parser -> public.scholarships (Supabase)."""

from __future__ import annotations

import hashlib
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag

_PARSER_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PARSER_ROOT not in sys.path:
    sys.path.insert(0, _PARSER_ROOT)

from business_filters import classify_business_deadline, has_meaningful_funding
from config import get_global_config
from normalize_scholarship import apply_normalization
from scholarship_db_columns import SCHOLARSHIP_RECORD_DEFAULT_KEYS, SCHOLARSHIP_UPSERT_BODY_KEYS
from sources.scholarship_america.parser import parse_award_min_max
from utils import KnownScholarshipIndex, get_client, listing_is_known, load_known_scholarship_index, upsert_scholarship

SOURCE = "scholarships360"
SITE_ORIGIN = "https://scholarships360.org"
SEARCH_URL = f"{SITE_ORIGIN}/scholarships/search/"
AJAX_URL = f"{SITE_ORIGIN}/wp-admin/admin-ajax.php"
DEFAULT_CURRENCY = "USD"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

_gc = get_global_config()
TARGET_NEW_ITEMS = _gc.target_new_items
MAX_LIST_PAGES = _gc.max_list_pages
NO_NEW_PAGES_STOP = _gc.no_new_pages_stop
SKIP_EXISTING_ON_LIST = _gc.skip_existing_on_list
USE_TITLE_FALLBACK_KNOWN = _gc.use_title_fallback_known


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


SCHOLARSHIPS360_ENABLED = _get_bool_env("SCHOLARSHIPS360_ENABLED", True)
SCHOLARSHIPS360_DETAIL_FETCH = _get_bool_env("SCHOLARSHIPS360_DETAIL_FETCH", True)
SCHOLARSHIPS360_REQUEST_DELAY_MS = max(0, _get_int_env("SCHOLARSHIPS360_REQUEST_DELAY_MS", 800))
SCHOLARSHIPS360_TIMEOUT_SECONDS = max(10, _get_int_env("SCHOLARSHIPS360_TIMEOUT_SECONDS", 45))
SCHOLARSHIPS360_MAX_RECORDS_DEBUG = max(0, _get_int_env("SCHOLARSHIPS360_MAX_RECORDS_DEBUG", 0))


def _log(message: str) -> None:
    print(message, flush=True)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()
    return text or None


def _clean_title(value: Any) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    text = re.sub(
        r"\s+This scholarship has been verified by the scholarship providing organization\.?\s*",
        " ",
        text,
        flags=re.I,
    )
    return _clean_text(text)


def _fetch(url: str) -> str:
    if SCHOLARSHIPS360_REQUEST_DELAY_MS:
        time.sleep(SCHOLARSHIPS360_REQUEST_DELAY_MS / 1000.0)
    response = requests.get(url, headers=HEADERS, timeout=SCHOLARSHIPS360_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.text


def _fetch_listing_page(page_idx: int) -> tuple[str, str]:
    if page_idx <= 1:
        return _fetch(SEARCH_URL), SEARCH_URL
    if SCHOLARSHIPS360_REQUEST_DELAY_MS:
        time.sleep(SCHOLARSHIPS360_REQUEST_DELAY_MS / 1000.0)
    response = requests.post(
        AJAX_URL,
        data={
            "action": "ajax_edu_filter_level",
            "page": str(page_idx),
            "searchTerm": "",
            "sidebar_academic_interest": "",
            "sidebar_state": "",
            "sidebar_grade": "",
            "sidebar_background": "",
            "sidebar_sort": "newness",
        },
        headers={**HEADERS, "Referer": SEARCH_URL},
        timeout=SCHOLARSHIPS360_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    template = payload.get("template") if isinstance(payload, dict) else None
    return str(template or ""), f"{SEARCH_URL}?current_page={page_idx}"


def _canonical_url(url: str) -> str:
    parsed = urlparse(url)
    clean = parsed._replace(fragment="", query="").geturl()
    return clean.rstrip("/") + "/"


def _source_id_from_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    if path:
        return path.split("/")[-1]
    return hashlib.md5(url.encode("utf-8")).hexdigest()[:16]


def _extract_card_value(text: str, label: str) -> str | None:
    pattern = rf"\b{re.escape(label)}\s+(.+?)(?:\bOffered by\b|\bExclusive\b|\b\d+\s+award|\bMultiple awards|\bDeadline\b|\bNext Deadline\b|\bGrade Level\b|$)"
    match = re.search(pattern, text, re.I)
    return _clean_text(match.group(1)) if match else None


def _extract_deadline_text(text: str) -> str | None:
    match = re.search(r"\b(?:Next\s+)?Deadline\s+([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})", text, re.I)
    return _clean_text(match.group(1)) if match else None


def _extract_award_text(text: str) -> str | None:
    match = re.search(r"\b((?:\d+\s+award|Multiple awards)[^$]{0,40}\$[\d,]+(?:\s*(?:each|per year|/ year|up to)?[^A-Z]{0,60})?)", text, re.I)
    if match:
        return _clean_text(match.group(1))
    match = re.search(r"\b(\$[\d,]+(?:\s*(?:scholarship|award))?)", text, re.I)
    return _clean_text(match.group(1)) if match else None


def _extract_listing_items(html: str, page_url: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for card in soup.select(".re-scholarship-card"):
        if not isinstance(card, Tag):
            continue
        link = card.select_one("h4 a[href]")
        if link is None:
            link = card.find("a", href=re.compile(r"/scholarships/search/"))
        if link is None:
            continue
        href = str(link.get("href") or "")
        url = _canonical_url(urljoin(page_url, href))
        if not re.search(r"/scholarships/search/[^/]+/$", url) or url in seen:
            continue
        title = _clean_title(link.get_text(" ", strip=True))
        if not title:
            continue
        text = _clean_text(card.get_text(" ", strip=True)) or ""
        seen.add(url)
        items.append(
            {
                "source_id": _source_id_from_url(url),
                "url": url,
                "title": title,
                "listing_text": text,
                "provider_name": _extract_card_value(text, "Offered by"),
                "award_amount_text": _extract_award_text(text),
                "deadline_text": _extract_deadline_text(text),
                "grade_level_text": _extract_card_value(text, "Grade Level"),
            }
        )
    return items


def _extract_section(lines: list[str], heading_re: str, stop_res: tuple[str, ...]) -> str | None:
    out: list[str] = []
    active = False
    for line in lines:
        if re.search(heading_re, line, re.I):
            active = True
            continue
        if active and any(re.search(pat, line, re.I) for pat in stop_res):
            break
        if active:
            out.append(line)
    return _clean_text("\n".join(out))


def _extract_detail(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    title = _clean_title(soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else None)
    text = soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    joined = "\n".join(lines)
    provider = None
    provider_match = re.search(r"\bOffered by\s*\n?(.+?)(?:\n|Exclusive|Reviewed by)", joined, re.I | re.S)
    if provider_match:
        provider = _clean_text(provider_match.group(1))
    award_match = re.search(r"\baward worth(?:\s+up to)?\s*\n?(\$[\d,]+)", joined, re.I)
    award_text = f"award worth {award_match.group(1)}" if award_match else None
    deadline_text = None
    deadline_match = re.search(r"\b(?:Application Deadline:|Next Deadline)\s*\n?([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})", joined, re.I)
    if deadline_match:
        deadline_text = _clean_text(deadline_match.group(1))
    grade_level = _extract_section(lines, r"^Grade level$|^Grade Level$", (r"^Open Date$", r"^Next Deadline$", r"^Winner Announcement$", r"^Scholarship Overview$"))
    overview = _extract_section(lines, r"^Scholarship Overview$", (r"^About ", r"^Eligibility information$", r"^Scholarship Interest Index$", r"^Application information$"))
    eligibility = _extract_section(lines, r"^Eligibility information$", (r"^Scholarship Interest Index$", r"^Application information$", r"^Scholarship FAQ$"))
    application = _extract_section(lines, r"^Application information$", (r"^RECENT SCHOLARSHIPS360 WINNERS$", r"^Scholarship FAQ$", r"^Discover similar scholarships"))
    citizenship = _extract_section(lines, r"^Citizenship Status$", (r"^State Residency$", r"^County Residency$", r"^Gender$", r"^Minimum GPA$", r"^Scholarship Interest Index$"))
    apply_url = None
    for a in soup.find_all("a", href=True):
        href = str(a.get("href") or "")
        label = a.get_text(" ", strip=True).lower()
        if "app.scholarships360.org" in href and ("apply" in label or "confirm eligibility" in label or "dashboard/scholarships" in href):
            apply_url = href
            break
    return {
        "title": title,
        "provider_name": provider,
        "award_amount_text": award_text,
        "deadline_text": deadline_text,
        "grade_level_text": grade_level,
        "overview": overview,
        "eligibility_text": eligibility,
        "application_text": application,
        "citizenship_text": citizenship,
        "apply_url": apply_url,
        "body_text": joined,
        "full_content_html": html[:150_000],
    }


_MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


def _parse_deadline_date(deadline_text: str | None) -> str | None:
    text = _clean_text(deadline_text)
    if not text:
        return None
    match = re.search(r"\b([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})\b", text, re.I)
    if not match:
        return None
    mon = _MONTHS.get(match.group(1).lower())
    if not mon:
        return None
    try:
        return datetime(int(match.group(3)), mon, int(match.group(2))).date().isoformat()
    except ValueError:
        return None


def _build_record(list_data: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
    title = detail.get("title") or list_data.get("title")
    provider_name = detail.get("provider_name") or list_data.get("provider_name") or "Scholarships360"
    award_text = detail.get("award_amount_text") or list_data.get("award_amount_text")
    amin, amax = parse_award_min_max(award_text)
    deadline_text = detail.get("deadline_text") or list_data.get("deadline_text")
    deadline_date = _parse_deadline_date(deadline_text)
    overview = detail.get("overview") or list_data.get("listing_text") or title
    eligibility = "\n".join(
        str(x)
        for x in (
            detail.get("eligibility_text"),
            f"Citizenship Status: {detail.get('citizenship_text')}" if detail.get("citizenship_text") else None,
            f"Grade Level: {detail.get('grade_level_text') or list_data.get('grade_level_text')}" if (detail.get("grade_level_text") or list_data.get("grade_level_text")) else None,
        )
        if x
    )
    requirements_text = "\n".join(str(x) for x in (eligibility, detail.get("application_text")) if x)
    record: dict[str, Any] = {
        "source": SOURCE,
        "source_id": list_data.get("source_id"),
        "url": list_data.get("url"),
        "title": title,
        "provider_name": provider_name,
        "award_amount_text": award_text,
        "award_amount_min": amin,
        "award_amount_max": amax,
        "currency": DEFAULT_CURRENCY,
        "deadline_text": deadline_text,
        "deadline_date": deadline_date,
        "description": overview,
        "eligibility_text": eligibility or overview,
        "requirements_text": requirements_text or eligibility or overview,
        "apply_url": detail.get("apply_url") or list_data.get("url"),
        "apply_button_text": "Apply / Confirm eligibility",
        "mark_started_available": True,
        "mark_submitted_available": True,
        "official_source_name": "Scholarships360",
        "is_active": True,
        "is_recurring": False,
        "full_content_html": detail.get("full_content_html"),
        "tags": ["scholarships360"],
        "raw_data": {
            "captured_at": _now_iso(),
            "listing": list_data,
            "detail_preview": {k: v for k, v in detail.items() if k not in {"full_content_html", "body_text"}},
        },
    }
    apply_normalization(record)
    for key in SCHOLARSHIP_RECORD_DEFAULT_KEYS:
        if key not in record:
            record[key] = None
    record["source"] = SOURCE
    record["is_active"] = True
    return record


def run() -> None:
    if not SCHOLARSHIPS360_ENABLED:
        _log(f"{SOURCE}: disabled via SCHOLARSHIPS360_ENABLED=0")
        return
    idx = KnownScholarshipIndex()
    if SKIP_EXISTING_ON_LIST:
        try:
            idx = load_known_scholarship_index(get_client(), SOURCE)
            _log(
                f"{SOURCE}: known index loaded: urls={len(idx.urls)} source_ids={len(idx.source_ids)} "
                f"slugs={len(idx.slugs_lc)} titles={len(idx.titles_norm)}"
            )
        except Exception as exc:
            _log(f"{SOURCE}: warning: failed to load known index ({exc})")
    stats = {"listing_seen": 0, "known_skipped": 0, "skip_no_funding": 0, "skip_deadline": 0, "upsert_ok": 0, "upsert_failed": 0}
    success_rows: list[dict[str, str]] = []
    seen: set[str] = set()
    no_new_pages = 0
    for page_idx in range(1, max(1, MAX_LIST_PAGES) + 1):
        html, page_url = _fetch_listing_page(page_idx)
        items = _extract_listing_items(html, page_url)
        _log(f"{SOURCE}: search page {page_idx}/{MAX_LIST_PAGES}: candidates={len(items)}")
        if not items:
            break
        new_on_page = 0
        stop = False
        for item in items:
            url = str(item.get("url") or "")
            if not url or url in seen:
                continue
            seen.add(url)
            stats["listing_seen"] += 1
            preview = {"source": SOURCE, "source_id": item.get("source_id"), "url": url, "title": item.get("title")}
            if SKIP_EXISTING_ON_LIST and listing_is_known(preview, idx, title_fallback=USE_TITLE_FALLBACK_KNOWN):
                stats["known_skipped"] += 1
                continue
            new_on_page += 1
            detail = _extract_detail(_fetch(url)) if SCHOLARSHIPS360_DETAIL_FETCH else {}
            record = _build_record(item, detail)
            if not has_meaningful_funding(record):
                stats["skip_no_funding"] += 1
                _log(f"{SOURCE}: skip no funding: {record.get('title')}")
                continue
            dbiz = classify_business_deadline(record.get("deadline_date"))
            if dbiz != "ok":
                stats["skip_deadline"] += 1
                _log(f"{SOURCE}: skip deadline {dbiz}: {record.get('title')}")
                continue
            unknown = set(record) - set(SCHOLARSHIP_UPSERT_BODY_KEYS) - {"id"}
            if unknown:
                raise ValueError(f"unknown keys in record: {sorted(unknown)}")
            try:
                row = upsert_scholarship(record)
                stats["upsert_ok"] += 1
                success_rows.append({"title": str(record.get("title") or ""), "url": str(record.get("url") or ""), "slug": str(row.get("slug") or record.get("slug") or "")})
                _log(f"{SOURCE}: upsert OK #{stats['upsert_ok']}: {record.get('title')} | slug={success_rows[-1]['slug']}")
            except Exception as exc:
                stats["upsert_failed"] += 1
                _log(f"{SOURCE}: upsert failed for {record.get('title')!r}: {exc}")
            if TARGET_NEW_ITEMS > 0 and stats["upsert_ok"] >= TARGET_NEW_ITEMS:
                _log(f"{SOURCE}: reached TARGET_NEW_ITEMS={TARGET_NEW_ITEMS}")
                stop = True
                break
            if SCHOLARSHIPS360_MAX_RECORDS_DEBUG > 0 and stats["listing_seen"] >= SCHOLARSHIPS360_MAX_RECORDS_DEBUG:
                _log(f"{SOURCE}: reached debug cap={SCHOLARSHIPS360_MAX_RECORDS_DEBUG}")
                stop = True
                break
        if stop:
            break
        if new_on_page == 0:
            no_new_pages += 1
            _log(f"{SOURCE}: page {page_idx}: no new listings ({no_new_pages}/{NO_NEW_PAGES_STOP})")
            if NO_NEW_PAGES_STOP > 0 and no_new_pages >= NO_NEW_PAGES_STOP:
                break
        else:
            no_new_pages = 0
    _log(f"{SOURCE}: success rows: {success_rows}")
    _log(f"{SOURCE}: done {stats}")


if __name__ == "__main__":
    run()
