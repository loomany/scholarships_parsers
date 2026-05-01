"""Scholars4Dev scholarship parser -> public.scholarships (Supabase)."""

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

from business_filters import MIN_LEAD_DAYS_BEFORE_DEADLINE, classify_business_deadline, has_meaningful_funding
from config import get_global_config
from normalize_scholarship import apply_normalization
from scholarship_db_columns import SCHOLARSHIP_RECORD_DEFAULT_KEYS, SCHOLARSHIP_UPSERT_BODY_KEYS
from sources.scholarship_america.parser import parse_award_min_max
from utils import KnownScholarshipIndex, get_client, listing_is_known, load_known_scholarship_index, upsert_scholarship

SOURCE = "scholars4dev"
SITE_ORIGIN = "https://www.scholars4dev.com"
START_URL = f"{SITE_ORIGIN}/"
SITEMAP_URL = f"{SITE_ORIGIN}/sitemap/"
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


def _get_int_env(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _get_bool_env(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


SCHOLARS4DEV_ENABLED = _get_bool_env("SCHOLARS4DEV_ENABLED", True)
SCHOLARS4DEV_DETAIL_FETCH = _get_bool_env("SCHOLARS4DEV_DETAIL_FETCH", True)
SCHOLARS4DEV_REQUEST_DELAY_MS = max(0, _get_int_env("SCHOLARS4DEV_REQUEST_DELAY_MS", 600))
SCHOLARS4DEV_TIMEOUT_SECONDS = max(10, _get_int_env("SCHOLARS4DEV_TIMEOUT_SECONDS", 45))
SCHOLARS4DEV_MAX_RECORDS_DEBUG = max(0, _get_int_env("SCHOLARS4DEV_MAX_RECORDS_DEBUG", 0))
SCHOLARS4DEV_SITEMAP_DISCOVERY = _get_bool_env("SCHOLARS4DEV_SITEMAP_DISCOVERY", True)
SCHOLARS4DEV_LIST_PAGE_DISCOVERY = _get_bool_env("SCHOLARS4DEV_LIST_PAGE_DISCOVERY", True)
SCHOLARS4DEV_CATEGORY_PAGE_LIMIT = max(1, _get_int_env("SCHOLARS4DEV_CATEGORY_PAGE_LIMIT", MAX_LIST_PAGES))


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
    if SCHOLARS4DEV_REQUEST_DELAY_MS:
        time.sleep(SCHOLARS4DEV_REQUEST_DELAY_MS / 1000.0)
    response = requests.get(url, headers=HEADERS, timeout=SCHOLARS4DEV_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.text


def _page_url(page_idx: int) -> str:
    return START_URL if page_idx <= 1 else f"{SITE_ORIGIN}/page/{page_idx}/"


def _category_page_url(base_url: str, page_idx: int) -> str:
    return base_url if page_idx <= 1 else f"{base_url.rstrip('/')}/page/{page_idx}/"


def _canonical_url(url: str) -> str:
    parsed = urlparse(url)
    without_fragment = parsed._replace(fragment="").geturl()
    return without_fragment.rstrip("/") + "/"


def _source_id_from_url(url: str) -> str:
    parsed = urlparse(url)
    match = re.search(r"/(\d+)/", parsed.path)
    if match:
        return match.group(1)
    return hashlib.md5(url.encode("utf-8")).hexdigest()[:16]


def _is_internal_article_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.netloc.endswith("scholars4dev.com") and bool(re.search(r"/\d+/", parsed.path))


def _make_stub_item(url: str, title: str | None = None, discovery_source: str | None = None) -> dict[str, Any]:
    clean_url = _canonical_url(url)
    return {
        "source_id": _source_id_from_url(clean_url),
        "url": clean_url,
        "title": title or clean_url,
        "listing_text": None,
        "deadline_text": None,
        "study_in_text": None,
        "discovery_source": discovery_source,
    }


def _has_next_page(html: str, next_page_idx: int) -> bool:
    return f"/page/{next_page_idx}/" in html


def _looks_like_single_scholarship(text: str) -> bool:
    return bool(
        re.search(r"\bDeadline:\s*", text, re.I)
        and re.search(r"\bStudy in:\s*", text, re.I)
        and not re.search(r"\bTop\s+\d+|\d+\+\s+International Scholarships|Scholarships to Watch", text, re.I)
    )


def _extract_listing_items(html: str, page_url: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for h2 in soup.find_all("h2"):
        if not isinstance(h2, Tag):
            continue
        link = h2.find("a", href=True)
        title = _clean_text(link.get_text(" ", strip=True) if link else h2.get_text(" ", strip=True))
        href = str(link.get("href")) if link else ""
        if not title or not href:
            continue
        url = _canonical_url(urljoin(page_url, href))
        if url in seen:
            continue
        block_parts: list[str] = []
        node: Any = h2
        for _ in range(8):
            node = node.find_next_sibling()
            if node is None or (isinstance(node, Tag) and node.name == "h2"):
                break
            if isinstance(node, Tag):
                block_parts.append(node.get_text(" ", strip=True))
        summary = _clean_text(" ".join(block_parts)) or ""
        blob = f"{title} {summary}"
        if not _looks_like_single_scholarship(blob):
            continue
        seen.add(url)
        items.append(
            {
                "source_id": _source_id_from_url(url),
                "url": url,
                "title": title,
                "listing_text": summary,
                "deadline_text": _extract_inline_value(blob, "Deadline"),
                "study_in_text": _extract_study_in(blob),
            }
        )
    return items


def _extract_article_links(html: str, page_url: str, discovery_source: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = str(a.get("href") or "")
        url = _canonical_url(urljoin(page_url, href))
        if url in seen or not _is_internal_article_url(url):
            continue
        title = _clean_text(a.get_text(" ", strip=True))
        if not title:
            continue
        seen.add(url)
        items.append(_make_stub_item(url, title, discovery_source))
    return items


def _extract_sitemap_links(html: str, page_url: str) -> tuple[list[tuple[str, str]], list[dict[str, Any]], list[tuple[str, str]]]:
    soup = BeautifulSoup(html, "html.parser")
    category_links: list[tuple[str, str]] = []
    upcoming_items: list[dict[str, Any]] = []
    list_pages: list[tuple[str, str]] = []
    seen_categories: set[str] = set()
    seen_upcoming: set[str] = set()
    seen_lists: set[str] = set()
    current_section: str | None = None

    for node in soup.find_all(["h2", "a"]):
        if not isinstance(node, Tag):
            continue
        if node.name == "h2":
            current_section = _clean_text(node.get_text(" ", strip=True))
            continue
        href = str(node.get("href") or "")
        title = _clean_text(node.get_text(" ", strip=True))
        if not href or not title:
            continue
        url = _canonical_url(urljoin(page_url, href))
        if "/category/" in url or "/tag/" in url:
            section_lc = (current_section or "").lower()
            is_sitemap_category_section = (
                section_lc.startswith("search by ")
                or section_lc in {"search by category", "search by country of origin"}
            )
            if is_sitemap_category_section and url not in seen_categories:
                category_links.append((title, url))
                seen_categories.add(url)
            continue
        if not _is_internal_article_url(url):
            continue
        section_lc = (current_section or "").lower()
        if "upcoming scholarships" in section_lc:
            if url not in seen_upcoming:
                upcoming_items.append(_make_stub_item(url, title, "sitemap_upcoming"))
                seen_upcoming.add(url)
        elif "lists of scholarships" in section_lc and SCHOLARS4DEV_LIST_PAGE_DISCOVERY:
            if url not in seen_lists:
                list_pages.append((title, url))
                seen_lists.add(url)
    return category_links, upcoming_items, list_pages


def _extract_inline_value(text: str, label: str) -> str | None:
    match = re.search(
        rf"\b{re.escape(label)}:\s*([\s\S]*?)(?:\bStudy in:|\bCourse starts|\bNext course starts|$)",
        text,
        re.I,
    )
    return _clean_text(match.group(1)) if match else None


def _extract_study_in(text: str) -> str | None:
    match = re.search(r"\bStudy in:\s*([\s\S]*?)(?:\bCourse starts|\bNext course starts|$)", text, re.I)
    return _clean_text(match.group(1)) if match else None


def _extract_detail_sections(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    title = _clean_text((soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else None))
    text = soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    data: dict[str, Any] = {"title": title, "body_text": "\n".join(lines)}

    label_map = {
        "brief description": "description",
        "host institution(s)": "host_institutions",
        "level/field(s) of study": "field_of_study",
        "number of scholarships": "number_of_awards_text",
        "target group": "target_group",
        "scholarship value/inclusions/duration": "award_amount_text",
        "eligibility": "eligibility_text",
        "application instructions": "application_instructions",
        "website": "website_text",
    }
    current_key: str | None = None
    buckets: dict[str, list[str]] = {v: [] for v in label_map.values()}
    for line in lines:
        normalized = line.rstrip(":").strip().lower()
        if normalized in label_map:
            current_key = label_map[normalized]
            continue
        if current_key:
            if normalized.startswith("disclaimer"):
                current_key = None
                continue
            buckets[current_key].append(line)
    for key, vals in buckets.items():
        val = _clean_text("\n".join(vals))
        if val:
            data[key] = val

    link = None
    for a in soup.find_all("a", href=True):
        label = a.get_text(" ", strip=True)
        href = str(a.get("href") or "")
        if "official scholarship website" in label.lower() or "official website" in label.lower():
            link = href
            break
    data["official_url"] = link
    data["full_content_html"] = html[:150_000]
    return data


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
    if not text or re.search(r"\b(varies|not specified|admissions deadline)\b", text, re.I):
        return None
    matches = list(
        re.finditer(
            r"\b(\d{1,2})\s+([A-Za-z]{3,9})(?:[a-z]*)[\s,/-]*(\d{4})\b",
            text,
            re.I,
        )
    )
    if not matches:
        matches = list(
            re.finditer(
                r"\b([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})\b",
                text,
                re.I,
            )
        )
        parsed: list[datetime] = []
        for m in matches:
            mon = _MONTHS.get(m.group(1).lower())
            if mon:
                try:
                    parsed.append(datetime(int(m.group(3)), mon, int(m.group(2))))
                except ValueError:
                    pass
        if parsed:
            return max(parsed).date().isoformat()
    parsed = []
    for m in matches:
        mon = _MONTHS.get(m.group(2).lower())
        if mon:
            try:
                parsed.append(datetime(int(m.group(3)), mon, int(m.group(1))))
            except ValueError:
                pass
    if parsed:
        return max(parsed).date().isoformat()

    yearless = list(re.finditer(r"\b(\d{1,2})\s+([A-Za-z]{3,9})(?:[a-z]*)\b", text, re.I))
    if not yearless:
        yearless = list(re.finditer(r"\b([A-Za-z]{3,9})\s+(\d{1,2})\b", text, re.I))

    today = datetime.now(timezone.utc).date()
    parsed_yearless: list[datetime] = []
    for m in yearless:
        if m.group(1).isdigit():
            day = int(m.group(1))
            mon = _MONTHS.get(m.group(2).lower())
        else:
            day = int(m.group(2))
            mon = _MONTHS.get(m.group(1).lower())
        if not mon:
            continue
        for year in (today.year, today.year + 1):
            try:
                candidate = datetime(year, mon, day)
            except ValueError:
                continue
            if candidate.date() >= today:
                parsed_yearless.append(candidate)
                break
    return max(parsed_yearless).date().isoformat() if parsed_yearless else None


def _build_record(list_data: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
    title = detail.get("title") or list_data.get("title")
    provider_name = None
    body = detail.get("body_text") or list_data.get("listing_text") or ""
    provider_match = re.search(r"\|\s*\n?(.+?)\s+(?:Bachelors|Masters|PhD|Undergraduate|Postgraduate)", body)
    if provider_match:
        provider_name = _clean_text(provider_match.group(1))
    if not provider_name:
        provider_name = _clean_text((detail.get("host_institutions") or "").split("\n", 1)[0]) or "Scholars4Dev"

    award_text = detail.get("award_amount_text")
    if not award_text:
        award_text = list_data.get("listing_text")
    amin, amax = parse_award_min_max(award_text)
    deadline_text = list_data.get("deadline_text") or _extract_inline_value(body, "Deadline")
    deadline_date = _parse_deadline_date(deadline_text)
    study_in = list_data.get("study_in_text") or _extract_study_in(body)
    description = detail.get("description") or list_data.get("listing_text") or title
    eligibility_parts = [
        f"Study in: {study_in}" if study_in else None,
        f"Target group: {detail.get('target_group')}" if detail.get("target_group") else None,
        detail.get("eligibility_text"),
    ]
    eligibility_text = "\n".join(str(x) for x in eligibility_parts if x)
    requirements_text = "\n".join(
        str(x)
        for x in (
            eligibility_text,
            detail.get("application_instructions"),
        )
        if x
    )
    apply_url = detail.get("official_url") or list_data.get("url")

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
        "description": description,
        "eligibility_text": eligibility_text or description,
        "requirements_text": requirements_text or eligibility_text or description,
        "apply_url": apply_url,
        "apply_button_text": "Official Scholarship Website",
        "mark_started_available": True,
        "mark_submitted_available": True,
        "official_source_name": "Scholars4Dev",
        "is_active": True,
        "is_recurring": bool(deadline_text and re.search(r"\bannual\b", deadline_text, re.I)),
        "field_of_study": None,
        "full_content_html": detail.get("full_content_html"),
        "tags": ["scholars4dev"],
        "raw_data": {
            "captured_at": _now_iso(),
            "listing": list_data,
            "detail": {k: v for k, v in detail.items() if k != "full_content_html"},
        },
    }
    apply_normalization(record)
    for key in SCHOLARSHIP_RECORD_DEFAULT_KEYS:
        if key not in record:
            record[key] = None
    record["source"] = SOURCE
    record["is_active"] = True
    return record


def _process_item(
    item: dict[str, Any],
    idx: KnownScholarshipIndex,
    stats: dict[str, int],
    seen_urls: set[str],
) -> bool:
    url = str(item.get("url") or "")
    if not url:
        return False
    url = _canonical_url(url)
    item["url"] = url
    if url in seen_urls:
        stats["duplicate_skipped"] += 1
        return False
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
        return _reached_debug_cap(stats)
    detail = _extract_detail_sections(_fetch(url)) if SCHOLARS4DEV_DETAIL_FETCH else {}
    record = _build_record(item, detail)
    if not has_meaningful_funding(record):
        stats["skip_no_funding"] += 1
        _log(f"{SOURCE}: skip no funding: {record.get('title')}")
        return _reached_debug_cap(stats)
    dbiz = classify_business_deadline(record.get("deadline_date"))
    if dbiz != "ok":
        stats["skip_deadline"] += 1
        _log(f"{SOURCE}: skip deadline {dbiz}: {record.get('title')}")
        return _reached_debug_cap(stats)
    unknown = set(record) - set(SCHOLARSHIP_UPSERT_BODY_KEYS) - {"id"}
    if unknown:
        raise ValueError(f"unknown keys in record: {sorted(unknown)}")
    try:
        upsert_scholarship(record)
        stats["upsert_ok"] += 1
        _log(f"{SOURCE}: upsert OK #{stats['upsert_ok']}: {record.get('title')}")
    except Exception as exc:
        stats["upsert_failed"] += 1
        _log(f"{SOURCE}: upsert failed for {record.get('title')!r}: {exc}")
    if TARGET_NEW_ITEMS > 0 and stats["upsert_ok"] >= TARGET_NEW_ITEMS:
        _log(f"{SOURCE}: reached TARGET_NEW_ITEMS={TARGET_NEW_ITEMS}")
        return True
    return _reached_debug_cap(stats)


def _reached_debug_cap(stats: dict[str, int]) -> bool:
    if SCHOLARS4DEV_MAX_RECORDS_DEBUG > 0 and stats["listing_seen"] >= SCHOLARS4DEV_MAX_RECORDS_DEBUG:
        _log(f"{SOURCE}: reached debug cap={SCHOLARS4DEV_MAX_RECORDS_DEBUG}")
        return True
    return False


def _walk_main_listing_pages(
    idx: KnownScholarshipIndex,
    stats: dict[str, int],
    seen_urls: set[str],
) -> bool:
    max_pages = max(1, MAX_LIST_PAGES)
    for page_idx in range(1, max_pages + 1):
        page_url = _page_url(page_idx)
        _log(f"{SOURCE}: listing page {page_idx}/{max_pages}: {page_url}")
        try:
            html = _fetch(page_url)
        except Exception as exc:
            stats["discovery_errors"] += 1
            _log(f"{SOURCE}: listing fetch failed {page_url}: {exc}")
            break
        items = _extract_listing_items(html, page_url)
        stats["discovery_pages"] += 1
        _log(f"{SOURCE}: listing page {page_idx}: candidates={len(items)}")
        if not items:
            break
        for item in items:
            if _process_item(item, idx, stats, seen_urls):
                return True
    return False


def _walk_sitemap_discovery(
    idx: KnownScholarshipIndex,
    stats: dict[str, int],
    seen_urls: set[str],
) -> bool:
    if not SCHOLARS4DEV_SITEMAP_DISCOVERY:
        return False
    try:
        sitemap_html = _fetch(SITEMAP_URL)
    except Exception as exc:
        stats["discovery_errors"] += 1
        _log(f"{SOURCE}: sitemap fetch failed {SITEMAP_URL}: {exc}")
        return False
    category_links, upcoming_items, list_pages = _extract_sitemap_links(sitemap_html, SITEMAP_URL)
    _log(
        f"{SOURCE}: sitemap discovery: categories={len(category_links)} "
        f"upcoming={len(upcoming_items)} list_pages={len(list_pages)}"
    )

    for item in upcoming_items:
        if _process_item(item, idx, stats, seen_urls):
            return True

    if SCHOLARS4DEV_LIST_PAGE_DISCOVERY:
        for list_idx, (title, list_url) in enumerate(list_pages, 1):
            _log(f"{SOURCE}: sitemap list page {list_idx}/{len(list_pages)}: {title} {list_url}")
            try:
                html = _fetch(list_url)
            except Exception as exc:
                stats["discovery_errors"] += 1
                _log(f"{SOURCE}: list page fetch failed {list_url}: {exc}")
                continue
            stats["discovery_pages"] += 1
            items = [
                item
                for item in _extract_article_links(html, list_url, "sitemap_list_page")
                if item.get("url") != list_url
            ]
            _log(f"{SOURCE}: sitemap list page candidates={len(items)}")
            for item in items:
                if _process_item(item, idx, stats, seen_urls):
                    return True

    page_limit = max(1, SCHOLARS4DEV_CATEGORY_PAGE_LIMIT)
    for category_idx, (title, category_url) in enumerate(category_links, 1):
        _log(f"{SOURCE}: category {category_idx}/{len(category_links)}: {title} {category_url}")
        for page_idx in range(1, page_limit + 1):
            page_url = _category_page_url(category_url, page_idx)
            try:
                html = _fetch(page_url)
            except Exception as exc:
                stats["discovery_errors"] += 1
                _log(f"{SOURCE}: category fetch failed {page_url}: {exc}")
                break
            stats["discovery_pages"] += 1
            items = _extract_listing_items(html, page_url)
            _log(f"{SOURCE}: category page {page_idx}: candidates={len(items)}")
            if not items:
                break
            for item in items:
                item["discovery_source"] = "sitemap_category"
                if _process_item(item, idx, stats, seen_urls):
                    return True
            if not _has_next_page(html, page_idx + 1):
                break
    return False


def run() -> None:
    if not SCHOLARS4DEV_ENABLED:
        _log(f"{SOURCE}: disabled via SCHOLARS4DEV_ENABLED=0")
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
        "duplicate_skipped": 0,
        "discovery_pages": 0,
        "discovery_errors": 0,
    }
    seen_urls: set[str] = set()
    if _walk_sitemap_discovery(idx, stats, seen_urls):
        _log(f"{SOURCE}: done {stats}")
        return
    if _walk_main_listing_pages(idx, stats, seen_urls):
        _log(f"{SOURCE}: done {stats}")
        return
    _log(f"{SOURCE}: done {stats}")


if __name__ == "__main__":
    run()
