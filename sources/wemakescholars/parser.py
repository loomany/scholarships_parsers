"""
WeMakeScholars aggregator -> public.scholarships (Supabase).

Обход только URL без query string (соответствие robots.txt: Disallow: /*?*).

Discovery: статические hub-ссылки + BFS по hub-кандидатам в пределах MAX_LIST_PAGES.
Детали: GET /scholarship/{slug}, разбор article.more-about-scholarship.
"""

from __future__ import annotations

import os
import re
import sys
import time
from collections import deque
from datetime import date, datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag

_PARSER_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PARSER_ROOT not in sys.path:
    sys.path.insert(0, _PARSER_ROOT)

from dotenv import load_dotenv

load_dotenv(os.path.join(_PARSER_ROOT, ".env"))
load_dotenv(os.path.join(os.path.dirname(_PARSER_ROOT), ".env"))

from business_filters import classify_business_deadline, has_meaningful_funding
from calendar import monthrange
from config import get_global_config
from normalize_scholarship import apply_normalization
from scholarship_db_columns import SCHOLARSHIP_RECORD_DEFAULT_KEYS, SCHOLARSHIP_UPSERT_BODY_KEYS
from sources.scholarship_america.parser import parse_award_min_max, parse_deadline_date
from utils import KnownScholarshipIndex, get_client, listing_is_known, load_known_scholarship_index, upsert_scholarship

SOURCE = "wemakescholars"
SITE_ORIGIN = "https://www.wemakescholars.com"
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


WEMAKE_SCHOLARS_ENABLED = _get_bool_env("WEMAKE_SCHOLARS_ENABLED", False)
WEMAKE_SCHOLARS_DETAIL_FETCH = _get_bool_env("WEMAKE_SCHOLARS_DETAIL_FETCH", True)
WEMAKE_SCHOLARS_REQUEST_DELAY_MS = max(0, _get_int_env("WEMAKE_SCHOLARS_REQUEST_DELAY_MS", 600))
WEMAKE_SCHOLARS_TIMEOUT_SECONDS = max(10, _get_int_env("WEMAKE_SCHOLARS_TIMEOUT_SECONDS", 45))
WEMAKE_SCHOLARS_MAX_RECORDS_DEBUG = max(0, _get_int_env("WEMAKE_SCHOLARS_MAX_RECORDS_DEBUG", 0))


def _hub_pages_cap() -> int:
    """
    Лимит GET к hub-страницам (не к карточкам /scholarship/).
    Если WEMAKE_SCHOLARS_MAX_HUB_PAGES не задан — используется GlobalConfig.MAX_LIST_PAGES.
    Если WEMAKE_SCHOLARS_MAX_HUB_PAGES=0 — большой безопасный потолок до исчерпания очереди BFS.
    """
    raw = os.getenv("WEMAKE_SCHOLARS_MAX_HUB_PAGES")
    if raw is None or not str(raw).strip():
        return max(1, MAX_LIST_PAGES)
    try:
        v = int(str(raw).strip())
    except ValueError:
        return max(1, MAX_LIST_PAGES)
    if v == 0:
        return 9_999_999
    return max(1, v)


HUB_PAGES_CAP = _hub_pages_cap()

_MONTHS_FULL = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
_MONTH_ABBREV = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

_DEFAULT_HUB_SEEDS: tuple[str, ...] = (
    f"{SITE_ORIGIN}/scholarship",
    f"{SITE_ORIGIN}/masters-scholarships-for-international-students",
    f"{SITE_ORIGIN}/bachelors-scholarships-for-international-students",
    f"{SITE_ORIGIN}/mba-scholarships-for-international-students",
    f"{SITE_ORIGIN}/medicine-scholarships-for-international-students",
    f"{SITE_ORIGIN}/phd-scholarships-for-international-students",
    f"{SITE_ORIGIN}/post-doc-scholarships-for-international-students",
    f"{SITE_ORIGIN}/scholarships-to-study-in-united-states-for-international-students",
    f"{SITE_ORIGIN}/scholarships-to-study-in-united-kingdom-for-international-students",
    f"{SITE_ORIGIN}/scholarships-to-study-in-germany-for-international-students",
    f"{SITE_ORIGIN}/scholarships-to-study-in-australia-for-international-students",
    f"{SITE_ORIGIN}/scholarships-to-study-in-ireland-for-international-students",
    f"{SITE_ORIGIN}/scholarships-to-study-in-canada-for-international-students",
    f"{SITE_ORIGIN}/scholarships-to-study-in-france-for-international-students",
    f"{SITE_ORIGIN}/scholarships-to-study-in-netherlands-for-international-students",
    f"{SITE_ORIGIN}/scholarships-to-study-in-new-zealand-for-international-students",
    f"{SITE_ORIGIN}/computer-sciences-and-information-technology-scholarships-for-international-students-to-study-abroad",
    f"{SITE_ORIGIN}/mechanical-engineering-scholarships-for-international-students-to-study-abroad",
    f"{SITE_ORIGIN}/public-health-scholarships-for-international-students-to-study-abroad",
    f"{SITE_ORIGIN}/aviation-or-related-scholarships-for-international-students-to-study-abroad",
    f"{SITE_ORIGIN}/university/duke-university/scholarships",
    f"{SITE_ORIGIN}/university/northeastern-university/scholarships",
    f"{SITE_ORIGIN}/university/university-of-birmingham/scholarships",
    f"{SITE_ORIGIN}/university/arizona-state-university-asu/scholarships",
)


def _log(message: str) -> None:
    print(message, flush=True)


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()
    return text or None


def _norm_url(url: str) -> str:
    u = url.split("#")[0].strip()
    if u.endswith("/"):
        u = u[:-1]
    return u


def _wms_host(netloc: str) -> bool:
    n = (netloc or "").lower()
    return n == "www.wemakescholars.com" or n == "wemakescholars.com"


def _is_scholarship_detail_path(path: str) -> bool:
    path = path.rstrip("/") or "/"
    if not path.startswith("/scholarship/"):
        return False
    rest = path[len("/scholarship/") :]
    if not rest or "/" in rest:
        return False
    slug = rest.lower()
    if slug in {"search", "index"}:
        return False
    return True


def _is_allowed_hub_path(path: str) -> bool:
    path = (path.rstrip("/") or "/").lower()
    if not path.startswith("/"):
        return False
    if path.startswith("/blog"):
        return False
    if path.startswith("/education-loan"):
        return False
    if path.startswith("/signup") or path.startswith("/login") or path.startswith("/report"):
        return False
    if _is_scholarship_detail_path(path):
        return False
    if path == "/scholarship":
        return True
    if "-scholarships-for-international-students" in path:
        return True
    if "scholarships-to-study-in-" in path and path.endswith("-international-students"):
        return True
    if re.match(r"^/university/[^/]+/scholarships$", path):
        return True
    if re.match(r"^/trust-foundation/[^/]+/scholarships$", path):
        return True
    if path.endswith("-to-study-abroad") and "scholarship" in path:
        return True
    return False


def _detail_slug(url: str) -> str | None:
    parsed = urlparse(url)
    if not _wms_host(parsed.netloc):
        return None
    path = parsed.path.rstrip("/") or "/"
    if not _is_scholarship_detail_path(path):
        return None
    return path.split("/")[-1]


def _fetch(url: str) -> str:
    if "?" in url:
        raise ValueError(f"{SOURCE}: refusing URL with query string (robots): {url}")
    if WEMAKE_SCHOLARS_REQUEST_DELAY_MS:
        time.sleep(WEMAKE_SCHOLARS_REQUEST_DELAY_MS / 1000.0)
    response = requests.get(url, headers=HEADERS, timeout=WEMAKE_SCHOLARS_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.text


def _parse_hub_links(html: str, page_url: str) -> tuple[set[str], set[str]]:
    soup = BeautifulSoup(html, "html.parser")
    details: set[str] = set()
    hubs: set[str] = set()
    for link in soup.find_all("a", href=True):
        if not isinstance(link, Tag):
            continue
        href = str(link.get("href") or "").strip()
        if not href or href.startswith("#") or "?" in href:
            continue
        abs_url = _norm_url(urljoin(page_url, href))
        parsed = urlparse(abs_url)
        if parsed.scheme not in {"http", "https"}:
            continue
        if not _wms_host(parsed.netloc):
            continue
        path = parsed.path or "/"
        if _is_scholarship_detail_path(path):
            details.add(abs_url)
        elif _is_allowed_hub_path(path):
            hubs.add(abs_url)
    return details, hubs


def _parse_deadline_date_wms(deadline_text: str | None) -> str | None:
    if not deadline_text:
        return None
    s = deadline_text.strip()
    if re.search(r"\b(rolling|tba|tbc|varies|ongoing)\b", s, re.I):
        return None
    iso = parse_deadline_date(s)
    if iso:
        return iso
    m = re.search(
        r"\b(\d{1,2})\s+([A-Za-z]{3,12})\s*,?\s*(\d{4})\b",
        s,
        re.I,
    )
    if not m:
        return None
    day, mon_s, year = int(m.group(1)), m.group(2).lower(), int(m.group(3))
    mon = _MONTHS_FULL.get(mon_s) or _MONTH_ABBREV.get(mon_s[:3])
    if not mon:
        return None
    last = monthrange(year, mon)[1]
    d = min(day, last)
    try:
        return date(year, mon, d).isoformat()
    except ValueError:
        return None


def _currency_hint(award_text: str | None, body: str) -> str:
    blob = f"{award_text or ''} {body}"
    u = blob.upper()
    if "INR" in u or "LAKH" in u or "RUPEE" in u:
        return "INR"
    if "€" in blob or "EUR" in u:
        return "EUR"
    if "£" in blob or "GBP" in u:
        return "GBP"
    if "CAD" in u or "CAN$" in u:
        return "CAD"
    if "AUD" in u:
        return "AUD"
    return DEFAULT_CURRENCY


def _extract_detail(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    title = _clean_text(h1.get_text(" ", strip=True) if h1 else None)

    art = soup.select_one("article.more-about-scholarship") or soup.find("article")
    if art is None:
        art = soup.body

    deadline_text = None
    award_amount_text = None
    provider_name = None
    degree_text = None
    lis_meta: list[str] = []

    if art:
        for li in art.find_all("li"):
            t = _clean_text(li.get_text(" ", strip=True))
            if not t:
                continue
            low = t.lower()
            if low.startswith("deadline:"):
                deadline_text = _clean_text(t.split(":", 1)[1])
                lis_meta.append(t)
            elif low.startswith("scholarship value:"):
                award_amount_text = _clean_text(t.split(":", 1)[1])
                lis_meta.append(t)
            elif low.startswith("provided by:"):
                provider_name = _clean_text(t.split(":", 1)[1])
                lis_meta.append(t)
            elif low.startswith("degree:"):
                degree_text = _clean_text(t.split(":", 1)[1])
                lis_meta.append(t)

    apply_url = None
    if art:
        for a in art.find_all("a", href=True):
            href = str(a.get("href") or "").strip()
            if not href.startswith("http") or "wemakescholars.com" in href:
                continue
            lab = (_clean_text(a.get_text(" ", strip=True)) or "").lower()
            if lab in {"here", "apply", "apply now", "website", "official website"} or "apply" in lab:
                apply_url = href
                break
        if not apply_url:
            for a in art.find_all("a", href=True):
                href = str(a.get("href") or "").strip()
                if href.startswith("http") and "wemakescholars.com" not in href:
                    apply_url = href
                    break

    paragraphs: list[str] = []
    if art:
        for p in art.find_all("p"):
            txt = _clean_text(p.get_text(" ", strip=True))
            if txt:
                paragraphs.append(txt)

    description = _clean_text("\n\n".join(paragraphs))

    eligibility_lines: list[str] = []
    if art:
        meta_low = tuple(x.lower() for x in lis_meta)
        for li in art.find_all("li"):
            t = _clean_text(li.get_text(" ", strip=True))
            if not t:
                continue
            low = t.lower()
            if any(low.startswith(p) for p in ("degree:", "provided by:", "deadline:", "scholarship value:")):
                continue
            eligibility_lines.append(t)

    eligibility_text = _clean_text("\n".join(eligibility_lines)) or description

    amin, amax = parse_award_min_max(award_amount_text or "")
    blob_for_curr = "\n".join(x for x in (award_amount_text, description, eligibility_text) if x)
    currency = _currency_hint(award_amount_text, blob_for_curr)

    return {
        "title": title,
        "deadline_text": deadline_text,
        "award_amount_text": award_amount_text,
        "provider_name_detail": provider_name,
        "degree_text": degree_text,
        "apply_url": apply_url,
        "description": description or title,
        "eligibility_text": eligibility_text,
        "award_amount_min": amin,
        "award_amount_max": amax,
        "currency": currency,
        "full_content_html": html[:150_000],
    }


def _build_record(url: str, detail: dict[str, Any]) -> dict[str, Any]:
    slug = _detail_slug(url) or url
    title = detail.get("title") or slug
    deadline_text = detail.get("deadline_text")
    deadline_date = _parse_deadline_date_wms(deadline_text)

    eligibility_text = detail.get("eligibility_text") or detail.get("description")
    provider = detail.get("provider_name_detail") or "WeMakeScholars"
    reqs = eligibility_text

    record: dict[str, Any] = {
        "source": SOURCE,
        "source_id": slug,
        "url": url,
        "title": title,
        "provider_name": provider,
        "award_amount_text": detail.get("award_amount_text"),
        "award_amount_min": detail.get("award_amount_min"),
        "award_amount_max": detail.get("award_amount_max"),
        "currency": detail.get("currency") or DEFAULT_CURRENCY,
        "deadline_text": deadline_text,
        "deadline_date": deadline_date,
        "description": detail.get("description") or title,
        "eligibility_text": eligibility_text,
        "requirements_text": reqs or eligibility_text,
        "apply_url": detail.get("apply_url") or url,
        "apply_button_text": "Apply",
        "mark_started_available": True,
        "mark_submitted_available": True,
        "official_source_name": "WeMakeScholars",
        "is_active": True,
        "is_recurring": False,
        "full_content_html": detail.get("full_content_html"),
        "tags": ["wemakescholars"],
        "raw_data": {
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "detail_slug": slug,
            "degree_text": detail.get("degree_text"),
        },
    }

    apply_normalization(record)
    for key in SCHOLARSHIP_RECORD_DEFAULT_KEYS:
        if key not in record:
            record[key] = None
    record["source"] = SOURCE
    record["is_active"] = True
    return record


def _extra_hub_seeds_from_env() -> list[str]:
    raw = (os.getenv("WEMAKE_SCHOLARS_EXTRA_HUBS") or "").strip()
    if not raw:
        return []
    out: list[str] = []
    for part in raw.split(","):
        u = part.strip()
        if u and "?" not in u:
            out.append(_norm_url(u))
    return out


def run() -> None:
    if not WEMAKE_SCHOLARS_ENABLED:
        _log(f"{SOURCE}: disabled via WEMAKE_SCHOLARS_ENABLED=0")
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

    seeds_raw = {_norm_url(u) for u in (*_DEFAULT_HUB_SEEDS, *_extra_hub_seeds_from_env())}
    main_listing = _norm_url(f"{SITE_ORIGIN}/scholarship")
    seeds_ordered = [main_listing] + sorted(s for s in seeds_raw if s != main_listing)
    hub_queue: deque[str] = deque(seeds_ordered)
    hubs_seen: set[str] = set()
    detail_urls_ordered: list[str] = []

    hubs_fetched = 0
    stats = {
        "hubs_fetched": 0,
        "listing_seen": 0,
        "known_skipped": 0,
        "skip_no_funding": 0,
        "skip_deadline": 0,
        "upsert_ok": 0,
        "upsert_failed": 0,
    }
    success_urls: list[str] = []

    while hub_queue and hubs_fetched < HUB_PAGES_CAP:
        hub_url = hub_queue.popleft()
        hub_url = _norm_url(hub_url)
        if hub_url in hubs_seen:
            continue
        hubs_seen.add(hub_url)
        hubs_fetched += 1
        stats["hubs_fetched"] = hubs_fetched
        _log(f"{SOURCE}: hub {hubs_fetched}/{HUB_PAGES_CAP}: {hub_url}")
        try:
            html = _fetch(hub_url)
        except Exception as exc:
            _log(f"{SOURCE}: hub fetch failed {hub_url!r}: {exc}")
            continue
        detail_set, hub_set = _parse_hub_links(html, hub_url)
        for du in sorted(detail_set):
            if du not in detail_urls_ordered:
                detail_urls_ordered.append(du)
        for hu in hub_set:
            hu = _norm_url(hu)
            if hu not in hubs_seen and hu not in hub_queue:
                hub_queue.append(hu)

        if TARGET_NEW_ITEMS > 0:
            need_pool = max(60, TARGET_NEW_ITEMS * 10)
            if len(detail_urls_ordered) >= need_pool:
                _log(
                    f"{SOURCE}: stopping hub crawl early ({len(detail_urls_ordered)} detail URLs collected, "
                    f"need_pool>={need_pool} for TARGET_NEW_ITEMS={TARGET_NEW_ITEMS})"
                )
                break

    seen_detail: set[str] = set()
    for url in detail_urls_ordered:
        url = _norm_url(url)
        if url in seen_detail:
            continue
        seen_detail.add(url)
        stats["listing_seen"] += 1
        slug = _detail_slug(url)
        preview = {
            "source": SOURCE,
            "source_id": slug,
            "url": url,
            "title": None,
        }
        if SKIP_EXISTING_ON_LIST and listing_is_known(preview, idx, title_fallback=USE_TITLE_FALLBACK_KNOWN):
            stats["known_skipped"] += 1
            continue

        if not WEMAKE_SCHOLARS_DETAIL_FETCH:
            continue
        try:
            dhtml = _fetch(url)
            detail = _extract_detail(dhtml)
        except Exception as exc:
            _log(f"{SOURCE}: detail fetch failed {url!r}: {exc}")
            continue

        preview["title"] = detail.get("title")
        record = _build_record(url, detail)

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

        if WEMAKE_SCHOLARS_MAX_RECORDS_DEBUG > 0 and stats["listing_seen"] >= WEMAKE_SCHOLARS_MAX_RECORDS_DEBUG:
            _log(f"{SOURCE}: reached debug cap={WEMAKE_SCHOLARS_MAX_RECORDS_DEBUG}")
            break

    _log(f"{SOURCE}: success urls: {success_urls}")
    _log(f"{SOURCE}: done {stats}")


if __name__ == "__main__":
    run()
