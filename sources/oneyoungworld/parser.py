"""One Young World scholarships -> public.scholarships (Supabase)."""

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
from bs4 import BeautifulSoup
from dotenv import load_dotenv

_PARSER_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PARSER_ROOT not in sys.path:
    sys.path.insert(0, _PARSER_ROOT)

load_dotenv(os.path.join(_PARSER_ROOT, ".env"))
load_dotenv(os.path.join(os.path.dirname(_PARSER_ROOT), ".env"))

from business_filters import classify_business_deadline, has_meaningful_funding
from config import get_global_config
from normalize_scholarship import apply_normalization
from scholarship_db_columns import SCHOLARSHIP_RECORD_DEFAULT_KEYS, SCHOLARSHIP_UPSERT_BODY_KEYS
from sources.scholarship_america.parser import parse_award_min_max, parse_deadline_date
from utils import KnownScholarshipIndex, get_client, listing_is_known, load_known_scholarship_index, upsert_scholarship

SOURCE = "oneyoungworld"
SITE_ORIGIN = "https://www.oneyoungworld.com"
LISTING_URL = f"{SITE_ORIGIN}/scholarships"
DEFAULT_CURRENCY = "USD"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

_gc = get_global_config()
TARGET_NEW_ITEMS = _gc.target_new_items
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


ONEYOUNGWORLD_ENABLED = _get_bool_env("ONEYOUNGWORLD_ENABLED", False)
ONEYOUNGWORLD_DETAIL_FETCH = _get_bool_env("ONEYOUNGWORLD_DETAIL_FETCH", True)
ONEYOUNGWORLD_REQUEST_DELAY_MS = max(0, _get_int_env("ONEYOUNGWORLD_REQUEST_DELAY_MS", 800))
ONEYOUNGWORLD_TIMEOUT_SECONDS = max(10, _get_int_env("ONEYOUNGWORLD_TIMEOUT_SECONDS", 45))
ONEYOUNGWORLD_MAX_RECORDS_DEBUG = max(0, _get_int_env("ONEYOUNGWORLD_MAX_RECORDS_DEBUG", 0))


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
    if ONEYOUNGWORLD_REQUEST_DELAY_MS:
        time.sleep(ONEYOUNGWORLD_REQUEST_DELAY_MS / 1000.0)
    response = requests.get(url, headers=HEADERS, timeout=ONEYOUNGWORLD_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.text


def _source_id_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/").split("/")
    if len(path) >= 2 and path[-2] == "scholarship":
        return path[-1][:120]
    return hashlib.md5(url.encode("utf-8")).hexdigest()[:16]


def _path_slug_from_oyw_url(url: str | None) -> str | None:
    if not url:
        return None
    parts = [p for p in urlparse(url).path.strip("/").split("/") if p]
    if len(parts) >= 2 and parts[0].lower() == "scholarship" and parts[1]:
        return parts[1][:120].strip()
    return None


def _normalize_deadline_phrase_for_sa(raw: str) -> str:
    """'5 May 2026' -> 'May 5, 2026' для parse_deadline_date."""
    s = (raw or "").strip()
    m = re.match(r"^(\d{1,2})\s+([A-Za-z]{3,9})\s+(\d{4})\s*$", s)
    if m:
        return f"{m.group(2)} {int(m.group(1))}, {m.group(3)}"
    return s


def _extract_deadline_line(text: str) -> str | None:
    blob = text or ""
    for pat in (
        r"(?is)application\s+deadline\s*:\s*([^\n]+)",
        r"(?is)registration\s+deadline\s*:\s*([^\n]+)",
        r"(?is)\bdeadline\s*:\s*([^\n]+)",
    ):
        m = re.search(pat, blob)
        if m:
            hit = _clean_text(m.group(1))
            if hit and len(hit) >= 8:
                return hit
    return None


def _main_content_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    root = soup.find("main") or soup.find("article") or soup.body
    return root.get_text("\n", strip=True) if root else soup.get_text("\n", strip=True)


def _extract_listings(html: str, page_url: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for a in soup.find_all("a", href=True):
        href = str(a.get("href") or "").strip()
        if "/scholarship/" not in href:
            continue
        full = urljoin(page_url, href)
        parsed = urlparse(full)
        if "oneyoungworld.com" not in parsed.netloc.lower():
            continue
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) < 2 or parts[0].lower() != "scholarship":
            continue
        slug_seg = parts[1].strip()
        if not slug_seg or slug_seg.endswith(".pdf"):
            continue
        canon = f"{SITE_ORIGIN}/scholarship/{slug_seg}".split("#")[0]
        if canon in seen:
            continue
        raw = _clean_text(a.get_text(" ", strip=True)) or ""
        title = raw
        deadline_hint_listing: str | None = None
        split_m = re.search(r"(?i)\s+registration\s+deadline\s*:\s*", raw)
        if split_m:
            title = raw[: split_m.start()].strip()
            deadline_hint_listing = raw[split_m.end() :].strip()
        if not title or len(title) < 12:
            continue
        seen.add(canon)
        out.append(
            {
                "source_id": slug_seg[:120],
                "url": canon,
                "title": title,
                "deadline_text_listing": deadline_hint_listing,
            }
        )
    return out


def _extract_apply_url(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    best: str | None = None
    for node in soup.find_all("a", href=True):
        href = str(node.get("href") or "").strip()
        label = (node.get_text(" ", strip=True) or "").strip().lower()
        if href.startswith("https://apply.oneyoungworld.com/"):
            if "apply" in label or not best:
                best = href
    return best


def _extract_detail(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    title = _clean_text(h1.get_text(" ", strip=True) if h1 else None)
    body_text = _main_content_text(html)
    deadline_text = _extract_deadline_line(body_text)
    apply_url = _extract_apply_url(html)
    main_el = soup.find("main") or soup.find("article")
    full_html_snip = str(main_el)[:180_000] if main_el else html[:180_000]
    return {
        "title": title,
        "body_text": body_text,
        "deadline_text_detail": deadline_text,
        "apply_url": apply_url,
        "full_content_html": full_html_snip,
    }


def _deadline_iso(listing_deadline: str | None, detail_deadline: str | None) -> str | None:
    for cand in (detail_deadline, listing_deadline):
        if not cand:
            continue
        normed = _normalize_deadline_phrase_for_sa(cand)
        d = parse_deadline_date(normed) or parse_deadline_date(cand)
        if d:
            return d
    return None


def _build_record(list_row: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
    title = detail.get("title") or list_row.get("title")
    body = detail.get("body_text") or ""
    deadline_detail = detail.get("deadline_text_detail")
    deadline_text = deadline_detail or list_row.get("deadline_text_listing")
    deadline_date = _deadline_iso(list_row.get("deadline_text_listing"), deadline_detail)
    if re.search(r"travel|accommodation|meals|summit|fully[-\s]?funded|\$[\d,]+", body, re.I):
        award_stub = (
            "Full scholarship covering One Young World Summit access, "
            "travel, accommodation and meals where stated on the programme page."
        )
    else:
        award_stub = (
            "Full scholarship — One Young World programme (see eligibility and benefits on the official page)."
        )
    amin, amax = parse_award_min_max(award_stub)
    desc = _clean_text(body[:8000]) or title
    apply_url = detail.get("apply_url") or list_row.get("url")
    record: dict[str, Any] = {
        "source": SOURCE,
        "source_id": list_row.get("source_id"),
        "url": list_row.get("url"),
        "title": title,
        "provider_name": "One Young World",
        "award_amount_text": award_stub,
        "award_amount_min": amin,
        "award_amount_max": amax,
        "currency": DEFAULT_CURRENCY,
        "deadline_text": _clean_text(deadline_text),
        "deadline_date": deadline_date,
        "description": desc,
        "eligibility_text": desc,
        "requirements_text": desc,
        "apply_url": apply_url,
        "apply_button_text": "Apply",
        "mark_started_available": True,
        "mark_submitted_available": True,
        "official_source_name": "One Young World",
        "provider_url": SITE_ORIGIN,
        "is_active": True,
        "is_recurring": bool(deadline_text and re.search(r"\bannual\b", deadline_text, re.I)),
        "tags": ["oneyoungworld", "scholarship"],
        "raw_data": {
            "captured_at": _now_iso(),
            "listing": list_row,
            "detail_keys": list(detail.keys()),
        },
        "full_content_html": detail.get("full_content_html"),
    }
    apply_normalization(record)
    slug_path = _path_slug_from_oyw_url(list_row.get("url"))
    if slug_path:
        record["slug"] = slug_path
    for key in SCHOLARSHIP_RECORD_DEFAULT_KEYS:
        if key not in record:
            record[key] = None
    record["source"] = SOURCE
    record["is_active"] = True
    return record


def run() -> None:
    if not ONEYOUNGWORLD_ENABLED:
        _log(f"{SOURCE}: OFF (ONEYOUNGWORLD_ENABLED=0)")
        return

    idx = KnownScholarshipIndex()
    if SKIP_EXISTING_ON_LIST:
        try:
            idx = load_known_scholarship_index(get_client(), SOURCE)
            _log(
                f"{SOURCE}: known urls={len(idx.urls)} ids={len(idx.source_ids)} "
                f"titles={len(idx.titles_norm)}"
            )
        except Exception as exc:
            _log(f"{SOURCE}: known index warning: {exc}")

    stats: dict[str, int] = {
        "listing_seen": 0,
        "known_skipped": 0,
        "skip_no_funding": 0,
        "skip_deadline": 0,
        "upsert_ok": 0,
        "upsert_failed": 0,
        "detail_fetch_failed": 0,
    }
    success_rows: list[dict[str, str]] = []

    _log(f"{SOURCE}: fetching listing {LISTING_URL}")
    try:
        html = _fetch(LISTING_URL)
    except requests.RequestException as exc:
        _log(f"{SOURCE}: listing failed: {exc}")
        return

    items = _extract_listings(html, LISTING_URL)
    _log(f"{SOURCE}: listing candidates={len(items)}")

    seen_urls: set[str] = set()
    for item in items:
        url = str(item.get("url") or "")
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        preview = {"source": SOURCE, "source_id": item.get("source_id"), "url": url, "title": item.get("title")}
        if SKIP_EXISTING_ON_LIST and listing_is_known(preview, idx, title_fallback=USE_TITLE_FALLBACK_KNOWN):
            stats["known_skipped"] += 1
            continue

        stats["listing_seen"] += 1

        detail: dict[str, Any] = {}
        if ONEYOUNGWORLD_DETAIL_FETCH:
            try:
                detail = _extract_detail(_fetch(url))
            except requests.RequestException as exc:
                stats["detail_fetch_failed"] += 1
                _log(f"{SOURCE}: detail fetch failed {url}: {exc}")
                detail = {}

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
            success_rows.append({"public_url": str(record.get("url") or "").strip()})
            _log(f"{SOURCE}: upsert OK #{stats['upsert_ok']}: {record.get('title')} | {record.get('url')}")
        except Exception as exc:
            stats["upsert_failed"] += 1
            _log(f"{SOURCE}: upsert failed {record.get('title')!r}: {exc}")

        if TARGET_NEW_ITEMS > 0 and stats["upsert_ok"] >= TARGET_NEW_ITEMS:
            _log(f"{SOURCE}: TARGET_NEW_ITEMS reached ({TARGET_NEW_ITEMS})")
            break
        if ONEYOUNGWORLD_MAX_RECORDS_DEBUG > 0 and stats["listing_seen"] >= ONEYOUNGWORLD_MAX_RECORDS_DEBUG:
            break

    # Подтягиваем slug из БД для ссылок на каталог пользователя (если Supabase доступен).
    client = None
    try:
        client = get_client()
    except Exception:
        client = None
    if client:
        res = (
            client.table("scholarships")
            .select("title, slug, url")
            .eq("source", SOURCE)
            .order("updated_at", desc=True)
            .limit(max(25, TARGET_NEW_ITEMS or 25))
            .execute()
        )
        slug_by_url = {(r.get("url") or ""): r.get("slug") for r in (res.data or []) if isinstance(r, dict)}
        title_by_url = {(r.get("url") or ""): r.get("title") for r in (res.data or []) if isinstance(r, dict)}
        base = (
            os.getenv("SITE_URL")
            or os.getenv("NEXT_PUBLIC_SITE_URL")
            or os.getenv("APP_URL")
            or os.getenv("FRONTEND_URL")
            or ""
        ).strip().rstrip("/")
        for row in success_rows:
            pu = row.get("public_url") or ""
            sl = slug_by_url.get(pu)
            row["slug"] = str(sl or "")
            row["title"] = str(title_by_url.get(pu) or "")
            row["catalog_url"] = f"{base}/scholarships/{sl}" if (base and sl) else ""

    _log(f"{SOURCE}: --- public catalog URLs (YOUR site) ---")
    for row in success_rows:
        if row.get("catalog_url"):
            _log(row["catalog_url"])
        elif row.get("slug"):
            _log(f"slug={row['slug']} (add SITE_URL or NEXT_PUBLIC_SITE_URL to .env for full URL)")
        else:
            _log(f"{SOURCE}: no slug yet — source URL: {row.get('public_url')}")
    _log(f"{SOURCE}: done {stats}")


if __name__ == "__main__":
    run()
