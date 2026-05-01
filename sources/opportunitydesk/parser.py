"""OpportunityDesk scholarship parser -> public.scholarships (Supabase)."""

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

SOURCE = "opportunitydesk"
SITE_ORIGIN = "https://opportunitydesk.org"
START_URL = f"{SITE_ORIGIN}/category/fellowships-and-scholarships/?PageSpeed=noscript"
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


OPPORTUNITYDESK_ENABLED = _get_bool_env("OPPORTUNITYDESK_ENABLED", True)
OPPORTUNITYDESK_DETAIL_FETCH = _get_bool_env("OPPORTUNITYDESK_DETAIL_FETCH", True)
OPPORTUNITYDESK_REQUEST_DELAY_MS = max(0, _get_int_env("OPPORTUNITYDESK_REQUEST_DELAY_MS", 800))
OPPORTUNITYDESK_TIMEOUT_SECONDS = max(10, _get_int_env("OPPORTUNITYDESK_TIMEOUT_SECONDS", 45))
OPPORTUNITYDESK_MAX_RECORDS_DEBUG = max(0, _get_int_env("OPPORTUNITYDESK_MAX_RECORDS_DEBUG", 0))


def _log(message: str) -> None:
    print(message, flush=True)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()
    return text or None


def _fetch(url: str) -> str:
    if OPPORTUNITYDESK_REQUEST_DELAY_MS:
        time.sleep(OPPORTUNITYDESK_REQUEST_DELAY_MS / 1000.0)
    response = requests.get(url, headers=HEADERS, timeout=OPPORTUNITYDESK_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.text


def _page_url(page_idx: int) -> str:
    if page_idx <= 1:
        return START_URL
    return f"{SITE_ORIGIN}/category/fellowships-and-scholarships/page/{page_idx}/?PageSpeed=noscript"


def _source_id_from_url(url: str) -> str:
    path = urlparse(url).path
    match = re.search(r"/(\d{4})/(\d{2})/(\d{2})/([^/]+)/", path)
    if match:
        return "-".join(match.groups())
    return hashlib.md5(url.encode("utf-8")).hexdigest()[:16]


def _is_digest_title(title: str) -> bool:
    return bool(
        re.search(
            r"\b(\d+\s+(fully|open)|curated|compiled|latest|jobs|internships|opportunities currently open)\b",
            title,
            re.I,
        )
    )


def _looks_like_scholarship(title: str, text: str) -> bool:
    blob = f"{title} {text}"
    if _is_digest_title(title):
        return False
    if re.search(r"\b(jobs?|internships?|consultancies|volunteer roles|competition|conference)\b", title, re.I):
        return False
    return bool(
        re.search(r"\bDeadline:\s*", blob, re.I)
        and re.search(r"\b(scholarship|scholars program|studentship|tuition|fully-funded|fully funded|fellowship)\b", blob, re.I)
    )


def _extract_listing_items(html: str, page_url: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for link in soup.find_all("a", href=True):
        if not isinstance(link, Tag):
            continue
        href = str(link.get("href") or "")
        url = urljoin(page_url, href)
        if "opportunitydesk.org" not in url or not re.search(r"/\d{4}/\d{2}/\d{2}/[^/]+/?", url):
            continue
        title = _clean_text(link.get_text(" ", strip=True) or link.get("title"))
        if not title or not href:
            continue
        if len(title) < 12 or title.lower() in {"read more", "continue reading"}:
            continue
        if url in seen:
            continue

        summary = ""
        ancestor: Any = link
        for _ in range(5):
            ancestor = ancestor.parent if isinstance(ancestor, Tag) else None
            if not isinstance(ancestor, Tag):
                break
            text = ancestor.get_text(" ", strip=True)
            if "Deadline:" in text and len(text) > len(title) + 20:
                summary = text
                break
        if not summary:
            block_parts: list[str] = []
            node: Any = link.find_parent(["h2", "h3", "h4"]) or link
            for _ in range(8):
                node = node.find_next_sibling()
                if node is None or (isinstance(node, Tag) and node.name in {"h2", "h3", "h4"}):
                    break
                if isinstance(node, Tag):
                    block_parts.append(node.get_text(" ", strip=True))
            summary = _clean_text(" ".join(block_parts)) or ""
        blob = f"{title} {summary}"
        if not _looks_like_scholarship(title, blob):
            continue
        seen.add(url)
        items.append(
            {
                "source_id": _source_id_from_url(url),
                "url": url,
                "title": title,
                "listing_text": summary,
                "deadline_text": _extract_deadline_text(blob),
            }
        )
    return items


def _extract_deadline_text(text: str) -> str | None:
    match = re.search(r"\bDeadline:\s*([^.\n]+)", text, re.I)
    return _clean_text(match.group(1)) if match else None


def _extract_detail(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    title = _clean_text((soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else None))
    text = soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    joined = "\n".join(lines)
    sections: dict[str, list[str]] = {"benefits": [], "eligibility": [], "application": []}
    current: str | None = None
    for line in lines:
        key = line.strip().lower()
        if key in {"benefits", "benefit"}:
            current = "benefits"
            continue
        if key in {"eligibility", "eligibilities"}:
            current = "eligibility"
            continue
        if key in {"application", "how to apply"}:
            current = "application"
            continue
        if current and key in {"previous article", "next article"}:
            current = None
        if current:
            sections[current].append(line)
    apply_url = None
    for a in soup.find_all("a", href=True):
        label = a.get_text(" ", strip=True).lower()
        href = str(a.get("href") or "")
        if "click here to apply" in label or label in {"apply", "apply now"}:
            apply_url = href
            break
    info_url = None
    for a in soup.find_all("a", href=True):
        label = a.get_text(" ", strip=True).lower()
        href = str(a.get("href") or "")
        if "for more information" in joined.lower() and href.startswith("http") and "opportunitydesk.org" not in href:
            info_url = href
    return {
        "title": title,
        "body_text": joined,
        "deadline_text": _extract_deadline_text(joined),
        "benefits_text": _clean_text("\n".join(sections["benefits"])),
        "eligibility_text": _clean_text("\n".join(sections["eligibility"])),
        "application_text": _clean_text("\n".join(sections["application"])),
        "apply_url": apply_url or info_url,
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
    if not text or re.search(r"\b(ongoing|varies|unspecified)\b", text, re.I):
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


def _provider_from_title(title: str | None) -> str:
    text = title or ""
    match = re.search(r"\bat\s+(.+?)(?:\s+\d{4}|\s+\(|$)", text, re.I)
    if match:
        return _clean_text(match.group(1)) or "Opportunity Desk"
    match = re.search(r"\bby\s+(.+?)(?:\s+\d{4}|\s+\(|$)", text, re.I)
    if match:
        return _clean_text(match.group(1)) or "Opportunity Desk"
    return "Opportunity Desk"


def _build_record(list_data: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
    title = detail.get("title") or list_data.get("title")
    deadline_text = detail.get("deadline_text") or list_data.get("deadline_text")
    benefits = detail.get("benefits_text")
    body = detail.get("body_text") or list_data.get("listing_text") or ""
    award_text = benefits or ("Fully-funded" if re.search(r"fully[-\s]?funded", body, re.I) else None)
    amin, amax = parse_award_min_max(award_text)
    deadline_date = _parse_deadline_date(deadline_text)
    description = _clean_text(body[:5000]) or list_data.get("listing_text") or title
    eligibility_text = detail.get("eligibility_text") or description
    requirements_text = "\n".join(
        str(x)
        for x in (
            eligibility_text,
            detail.get("application_text"),
        )
        if x
    )
    record: dict[str, Any] = {
        "source": SOURCE,
        "source_id": list_data.get("source_id"),
        "url": list_data.get("url"),
        "title": title,
        "provider_name": _provider_from_title(title),
        "award_amount_text": award_text,
        "award_amount_min": amin,
        "award_amount_max": amax,
        "currency": DEFAULT_CURRENCY,
        "deadline_text": deadline_text,
        "deadline_date": deadline_date,
        "description": description,
        "eligibility_text": eligibility_text,
        "requirements_text": requirements_text or eligibility_text,
        "apply_url": detail.get("apply_url") or list_data.get("url"),
        "apply_button_text": "Apply / More information",
        "mark_started_available": True,
        "mark_submitted_available": True,
        "official_source_name": "Opportunity Desk",
        "is_active": True,
        "is_recurring": False,
        "full_content_html": detail.get("full_content_html"),
        "tags": ["opportunitydesk"],
        "raw_data": {
            "captured_at": _now_iso(),
            "listing": list_data,
            "detail_preview": {
                k: v for k, v in detail.items() if k not in {"full_content_html", "body_text"}
            },
            "detail_body_preview": body[:20_000],
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
    if not OPPORTUNITYDESK_ENABLED:
        _log(f"{SOURCE}: disabled via OPPORTUNITYDESK_ENABLED=0")
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
    stats = {
        "listing_seen": 0,
        "known_skipped": 0,
        "skip_no_funding": 0,
        "skip_deadline": 0,
        "upsert_ok": 0,
        "upsert_failed": 0,
    }
    success_urls: list[str] = []
    seen_urls: set[str] = set()
    for page_idx in range(1, max(1, MAX_LIST_PAGES) + 1):
        page_url = _page_url(page_idx)
        _log(f"{SOURCE}: listing page {page_idx}/{MAX_LIST_PAGES}: {page_url}")
        html = _fetch(page_url)
        items = _extract_listing_items(html, page_url)
        _log(f"{SOURCE}: listing page {page_idx}: candidates={len(items)}")
        if not items:
            break
        for item in items:
            url = str(item.get("url") or "")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            stats["listing_seen"] += 1
            preview = {
                "source": SOURCE,
                "source_id": item.get("source_id"),
                "url": url,
                "title": item.get("title"),
            }
            if SKIP_EXISTING_ON_LIST and listing_is_known(preview, idx, title_fallback=USE_TITLE_FALLBACK_KNOWN):
                stats["known_skipped"] += 1
                continue
            detail = _extract_detail(_fetch(url)) if OPPORTUNITYDESK_DETAIL_FETCH else {}
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
                upsert_scholarship(record)
                stats["upsert_ok"] += 1
                success_urls.append(str(record.get("url")))
                _log(f"{SOURCE}: upsert OK #{stats['upsert_ok']}: {record.get('title')} | {record.get('url')}")
            except Exception as exc:
                stats["upsert_failed"] += 1
                _log(f"{SOURCE}: upsert failed for {record.get('title')!r}: {exc}")
            if TARGET_NEW_ITEMS > 0 and stats["upsert_ok"] >= TARGET_NEW_ITEMS:
                _log(f"{SOURCE}: reached TARGET_NEW_ITEMS={TARGET_NEW_ITEMS}")
                _log(f"{SOURCE}: success urls: {success_urls}")
                _log(f"{SOURCE}: done {stats}")
                return
            if OPPORTUNITYDESK_MAX_RECORDS_DEBUG > 0 and stats["listing_seen"] >= OPPORTUNITYDESK_MAX_RECORDS_DEBUG:
                _log(f"{SOURCE}: reached debug cap={OPPORTUNITYDESK_MAX_RECORDS_DEBUG}")
                _log(f"{SOURCE}: success urls: {success_urls}")
                _log(f"{SOURCE}: done {stats}")
                return
    _log(f"{SOURCE}: success urls: {success_urls}")
    _log(f"{SOURCE}: done {stats}")


if __name__ == "__main__":
    run()
