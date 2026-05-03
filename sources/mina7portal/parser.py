"""Mina7 Portal Scholarships / Grants -> public.scholarships (Supabase)."""

from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import date, datetime, timezone
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

SOURCE = "mina7portal"
SITE_HOST = "mina7portal.com"
SITE_ORIGIN = f"https://{SITE_HOST}"
DEFAULT_CURRENCY = "USD"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

_gc = get_global_config()
TARGET_NEW_ITEMS = _gc.target_new_items
MAX_LIST_PAGES = _gc.max_list_pages
SKIP_EXISTING_ON_LIST = _gc.skip_existing_on_list
USE_TITLE_FALLBACK_KNOWN = _gc.use_title_fallback_known
NO_NEW_PAGES_STOP = _gc.no_new_pages_stop


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


MINA7PORTAL_ENABLED = _get_bool_env("MINA7PORTAL_ENABLED", False)
MINA7PORTAL_DETAIL_FETCH = _get_bool_env("MINA7PORTAL_DETAIL_FETCH", True)
MINA7PORTAL_REQUEST_DELAY_MS = max(0, _get_int_env("MINA7PORTAL_REQUEST_DELAY_MS", 800))
MINA7PORTAL_TIMEOUT_SECONDS = max(15, _get_int_env("MINA7PORTAL_TIMEOUT_SECONDS", 55))
MINA7PORTAL_MAX_RECORDS_DEBUG = max(0, _get_int_env("MINA7PORTAL_MAX_RECORDS_DEBUG", 0))
MINA7PORTAL_SKIP_NOISY_JOBS = _get_bool_env("MINA7PORTAL_SKIP_NOISY_JOBS", True)

MINA7_LOCALE = (os.getenv("MINA7PORTAL_LOCALE") or os.getenv("MINA7_LOCALE") or "en").strip().lower().replace("/", "") or "en"

_NOISY_TITLE_RE = re.compile(
    r"\b(open\s+pool|postdoctoral\s+scholar|postdoc\s+fellowship\s+vacancy|"
    r"research\s+associate,?\s+postdoctoral|consultancy\s+vacancy|"
    r"temporary\s+lecturer|principal\s+investigator\s+vacancy)\b",
    re.I,
)


def _types_from_env() -> list[str]:
    raw = (
        os.getenv("MINA7PORTAL_OPPORTUNITY_TYPES")
        or os.getenv("MINA7_OPPORTUNITY_TYPES")
        or "grants,scholarships"
    )
    out: list[str] = []
    for part in raw.split(","):
        p = re.sub(r"[^a-z0-9\-]+", "", part.strip().lower())
        if p and p not in out:
            out.append(p)
    return out or ["grants", "scholarships"]


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
    if MINA7PORTAL_REQUEST_DELAY_MS:
        time.sleep(MINA7PORTAL_REQUEST_DELAY_MS / 1000.0)
    response = requests.get(url, headers=HEADERS, timeout=MINA7PORTAL_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.text


def _listing_url(type_slug: str, page_idx: int) -> str:
    base = f"{SITE_ORIGIN}/{MINA7_LOCALE}/opportunity-type/{type_slug.strip().lower()}"
    if page_idx <= 1:
        return base
    return f"{base}?page={page_idx}"


def _canonical_opportunity_url(href: str, page_url: str) -> str | None:
    href = str(href or "").strip()
    full = urljoin(page_url, href)
    parsed = urlparse(full)
    if parsed.netloc.lower().removeprefix("www.") != SITE_HOST:
        return None
    path = parsed.path.strip("/").split("/")
    if (
        len(path) >= 3
        and path[0].lower() == MINA7_LOCALE
        and path[1].lower() == "opportunity"
        and path[2]
    ):
        seg = path[2][:200]
        if not seg or seg.endswith(".jpg"):
            return None
        return f"{SITE_ORIGIN}/{MINA7_LOCALE}/opportunity/{seg}"
    return None


def _extract_list_candidates(html: str, page_url: str, type_slug: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for a in soup.find_all("a", href=True):
        canon = _canonical_opportunity_url(str(a.get("href") or ""), page_url)
        if not canon or canon in seen:
            continue
        title = _clean_text(a.get_text(" ", strip=True))
        if not title or len(title) < 14:
            continue
        if MINA7PORTAL_SKIP_NOISY_JOBS and _NOISY_TITLE_RE.search(title):
            continue
        seen.add(canon)
        seg = urlparse(canon).path.strip("/").split("/")[-1]
        out.append(
            {
                "source_id": seg[:120],
                "url": canon,
                "title": title,
                "listing_type": type_slug,
            }
        )
    return out


def _parse_json_ld_primary(html: str) -> dict[str, Any] | None:
    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script", type=re.compile(r"ld\s*\+\s*json", re.I)):
        raw = (script.string or script.get_text() or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        blobs = data if isinstance(data, list) else [data]
        for d in blobs:
            if isinstance(d, dict) and d.get("applicationDeadline"):
                return d
        for d in blobs:
            if isinstance(d, dict) and d.get("headline"):
                return d
        for d in blobs:
            if isinstance(d, dict) and isinstance(d.get("name"), str):
                return d
    return None


def _org_name(blob: Any) -> str | None:
    if isinstance(blob, dict):
        n = blob.get("name")
        if isinstance(n, str) and n.strip():
            return n.strip()
        if blob.get("@type") == "Organization":
            alt = blob.get("alternateName")
            if isinstance(alt, str) and alt.strip():
                return alt.strip()
    return None


def _iso_date_from_deadline(value: Any) -> str | None:
    if not value:
        return None
    s = str(value).strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}", s):
        try:
            return date.fromisoformat(s[:10]).isoformat()
        except ValueError:
            return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.date().isoformat()
    except ValueError:
        pass
    n = parse_deadline_date(s)
    if n:
        return n
    m = re.match(r"^(\d{1,2})\s+([A-Za-z]{3,9})\s+(\d{4})\s*$", s)
    if m:
        n2 = parse_deadline_date(f"{m.group(2)} {int(m.group(1))}, {m.group(3)}")
        return n2
    return None


def _funding_hint_from_dom(html: str) -> str | None:
    lines = [ln.strip() for ln in BeautifulSoup(html, "html.parser").get_text("\n").split("\n") if ln.strip()]
    for i, ln in enumerate(lines):
        if ln.lower() == "opportunity funding" and i + 1 < len(lines):
            return _clean_text(lines[i + 1])
    return None


def _detail_body_fragment(html: str) -> tuple[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    root = soup.find("main") or soup.find("article") or soup.body
    if root:
        return root.get_text("\n", strip=True), str(root)[:170_000]
    return soup.get_text("\n", strip=True), html[:170_000]


def _extract_detail(html: str, page_url: str) -> dict[str, Any]:
    ld = _parse_json_ld_primary(html) or {}
    title = _clean_text(ld.get("headline")) or None
    h1 = BeautifulSoup(html, "html.parser").find("h1")
    title = title or _clean_text(h1.get_text(" ", strip=True)) if h1 else title
    body_text, fc_html = _detail_body_fragment(html)
    deadline_raw = ld.get("applicationDeadline") or ld.get("expires")
    deadline_iso = _iso_date_from_deadline(deadline_raw)
    deadline_text = None
    lines = [ln.strip() for ln in body_text.split("\n") if ln.strip()]
    for i, ln in enumerate(lines):
        if ln == "Deadline" and i + 1 < len(lines):
            deadline_text = _clean_text(lines[i + 1])
            break
    if deadline_iso:
        dl_src = deadline_text or deadline_iso
    else:
        dl_src = deadline_text
        deadline_iso = _iso_date_from_deadline(deadline_text) if deadline_text else None
    funding = _funding_hint_from_dom(html) or ""
    pub = ld.get("datePublished") or ld.get("dateCreated")
    prov = _org_name(ld.get("provider")) or _org_name(ld.get("publisher")) or "Mina7 Portal"

    canon = _clean_text(ld.get("url")) or page_url

    apply_url = canon
    for a in BeautifulSoup(html, "html.parser").find_all("a", href=True):
        h = str(a.get("href") or "").strip()
        if not h.startswith("http"):
            continue
        host = urlparse(h).netloc.lower().replace("www.", "")
        if host == SITE_HOST or host.endswith("instagram.com"):
            continue
        if host.endswith("facebook.com") or host.endswith("twitter.com"):
            continue
        if host.endswith("t.me"):
            continue
        if "linkedin.com" in host:
            continue
        lbl = (a.get_text() or "").strip().upper()
        if any(x in lbl for x in ("APPLY", "OFFICIAL", "WEBSITE", "MORE INFO", "SOURCE")):
            apply_url = h.split("#")[0]
            break

    return {
        "title": title,
        "provider_name_hint": prov,
        "deadline_iso": deadline_iso,
        "deadline_text": _clean_text(dl_src if isinstance(dl_src, str) else str(deadline_iso or "")),
        "award_hint": funding,
        "description": body_text[:12_000] if body_text else None,
        "apply_url_preferred": apply_url,
        "canonical_url_record": canon,
        "publisher_date": pub,
        "full_content_html": fc_html,
    }


def _build_record(list_row: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
    title = detail.get("title") or list_row.get("title")
    body = detail.get("description") or ""
    deadline_date = detail.get("deadline_iso")
    deadline_text = detail.get("deadline_text")
    lt = str(list_row.get("listing_type") or "").lower()

    ah = detail.get("award_hint") or ""
    blob_txt = " ".join(
        str(x) for x in (title, body[:4000], ah) if x
    )
    if not ah and re.search(r"fully\s+funded|partial\s+funding|grant|tuition|stipend|scholarship", blob_txt, re.I):
        ah = ah or "Funding details on listing (see eligibility and official source)."
    if not ah and "grant" in lt:
        ah = "Grant opportunity — see programme page for funding amount and eligibility."
    elif not ah and "scholarship" in lt:
        ah = "Scholarship opportunity — see programme page for award value and eligibility."
    if not ah:
        ah = "See opportunity page for funding and award details."

    amin, amax = parse_award_min_max(ah)
    slug_seg = urlparse(list_row["url"]).path.strip("/").split("/")[-1]

    desc = _clean_text(body[:8000]) or title
    rec: dict[str, Any] = {
        "source": SOURCE,
        "source_id": list_row.get("source_id"),
        "url": list_row.get("url"),
        "title": title,
        "provider_name": detail.get("provider_name_hint") or "Mina7 Portal",
        "award_amount_text": ah,
        "award_amount_min": amin,
        "award_amount_max": amax,
        "currency": DEFAULT_CURRENCY,
        "deadline_text": deadline_text,
        "deadline_date": deadline_date,
        "description": desc,
        "eligibility_text": desc,
        "requirements_text": desc,
        "apply_url": detail.get("apply_url_preferred") or list_row.get("url"),
        "apply_button_text": "View / Apply",
        "mark_started_available": True,
        "mark_submitted_available": True,
        "official_source_name": "Mina7 Portal",
        "provider_url": SITE_ORIGIN,
        "is_active": True,
        "is_recurring": bool(deadline_text and re.search(r"\bannual\b", deadline_text + " " + (body or ""), re.I)),
        "tags": ["mina7portal", str(list_row.get("listing_type") or "grant")[:40]],
        "raw_data": {
            "captured_at": _now_iso(),
            "listing": list_row,
            "json_ld_partial": {
                k: v
                for k, v in (detail.items())
                if k not in {"description", "full_content_html"}
            },
        },
        "full_content_html": detail.get("full_content_html"),
    }
    apply_normalization(rec)
    if slug_seg:
        rec["slug"] = slug_seg[:120]
    for key in SCHOLARSHIP_RECORD_DEFAULT_KEYS:
        if key not in rec:
            rec[key] = None
    rec["source"] = SOURCE
    rec["is_active"] = True
    return rec


def _walk_type_slug(
    type_slug: str,
    idx: KnownScholarshipIndex,
    stats: dict[str, int],
    seen_urls: set[str],
) -> bool:
    no_new_pages = 0
    type_slug = type_slug.strip().lower()
    for page_idx in range(1, max(1, MAX_LIST_PAGES) + 1):
        page_url = _listing_url(type_slug, page_idx)
        _log(f"{SOURCE}: [{type_slug}] page {page_idx}/{MAX_LIST_PAGES} {page_url}")
        try:
            html = _fetch(page_url)
        except requests.RequestException as exc:
            stats["discovery_errors"] = stats.get("discovery_errors", 0) + 1
            _log(f"{SOURCE}: listing fetch failed: {exc}")
            break
        stats["discovery_pages"] = stats.get("discovery_pages", 0) + 1
        candidates = _extract_list_candidates(html, page_url, type_slug)
        if not candidates:
            _log(f"{SOURCE}: [{type_slug}] empty listing — stop this type")
            break
        new_on_page = 0
        for item in candidates:
            url = str(item.get("url") or "").strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            preview = {
                "source": SOURCE,
                "source_id": item.get("source_id"),
                "url": url,
                "title": item.get("title"),
            }
            if SKIP_EXISTING_ON_LIST and listing_is_known(preview, idx, title_fallback=USE_TITLE_FALLBACK_KNOWN):
                stats["known_skipped"] += 1
                continue
            new_on_page += 1

            stats["listing_seen"] += 1

            detail: dict[str, Any]
            if MINA7PORTAL_DETAIL_FETCH:
                try:
                    detail = _extract_detail(_fetch(url), url)
                except requests.RequestException as exc:
                    stats["detail_failed"] = stats.get("detail_failed", 0) + 1
                    _log(f"{SOURCE}: detail fetch failed {url}: {exc}")
                    continue
            else:
                detail = {"title": item.get("title"), "description": None, "deadline_iso": None, "deadline_text": None,
                          "award_hint": "", "provider_name_hint": "Mina7 Portal",
                          "apply_url_preferred": url, "canonical_url_record": url, "full_content_html": None}

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
                raise ValueError(f"unknown keys: {sorted(unknown)}")
            try:
                upsert_scholarship(record)
                stats["upsert_ok"] += 1
                _log(f"{SOURCE}: upsert OK #{stats['upsert_ok']}: {record.get('title')}")
            except Exception as exc:
                stats["upsert_failed"] += 1
                _log(f"{SOURCE}: upsert failed: {exc}")

            if TARGET_NEW_ITEMS > 0 and stats["upsert_ok"] >= TARGET_NEW_ITEMS:
                _log(f"{SOURCE}: TARGET_NEW_ITEMS reached ({TARGET_NEW_ITEMS})")
                return True
            if MINA7PORTAL_MAX_RECORDS_DEBUG > 0 and stats["listing_seen"] >= MINA7PORTAL_MAX_RECORDS_DEBUG:
                return True

        if new_on_page == 0:
            no_new_pages += 1
            _log(f"{SOURCE}: [{type_slug}] no NEW on page ({no_new_pages}/{NO_NEW_PAGES_STOP})")
            if NO_NEW_PAGES_STOP > 0 and no_new_pages >= NO_NEW_PAGES_STOP:
                break
        else:
            no_new_pages = 0
    return False


def run() -> None:
    if not MINA7PORTAL_ENABLED:
        _log(f"{SOURCE}: OFF (MINA7PORTAL_ENABLED=0)")
        return

    idx = KnownScholarshipIndex()
    if SKIP_EXISTING_ON_LIST:
        try:
            idx = load_known_scholarship_index(get_client(), SOURCE)
            _log(f"{SOURCE}: known urls={len(idx.urls)} ids={len(idx.source_ids)} titles={len(idx.titles_norm)}")
        except Exception as exc:
            _log(f"{SOURCE}: known index warning: {exc}")

    stats: dict[str, int] = {
        "listing_seen": 0,
        "known_skipped": 0,
        "skip_no_funding": 0,
        "skip_deadline": 0,
        "upsert_ok": 0,
        "upsert_failed": 0,
        "discovery_pages": 0,
        "discovery_errors": 0,
        "detail_failed": 0,
    }
    seen_urls: set[str] = set()
    kinds = _types_from_env()

    for t_slug in kinds:
        if _walk_type_slug(t_slug, idx, stats, seen_urls):
            break

    client = None
    try:
        client = get_client()
    except Exception:
        client = None
    if stats["upsert_ok"] and client:
        base = (
            os.getenv("SITE_URL")
            or os.getenv("NEXT_PUBLIC_SITE_URL")
            or os.getenv("APP_URL")
            or os.getenv("FRONTEND_URL")
            or ""
        ).strip().rstrip("/")
        _log(f"{SOURCE}: --- public catalog (set SITE_URL for full URLs) upserts={stats['upsert_ok']}")
        if base:
            res = (
                client.table("scholarships")
                .select("slug,url")
                .eq("source", SOURCE)
                .order("updated_at", desc=True)
                .limit(max(40, TARGET_NEW_ITEMS or 20))
                .execute()
            )
            for row in res.data or []:
                slug = row.get("slug")
                if slug:
                    _log(f"{base}/scholarships/{slug}")

    _log(f"{SOURCE}: done {stats}")


if __name__ == "__main__":
    run()
