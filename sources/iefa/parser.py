"""
IEFA.org (International Education Financial Aid) -> public.scholarships.

По умолчанию загрузка через HTTP (requests), без окна браузера.
Опционально IEFA_VISIBLE_BROWSER=1 — тот же контент через видимый Chromium (Playwright).

Собирает страны: host_country_names / applicant_country_names / country_summary
и синхронизирует public.catalog_countries.
"""

from __future__ import annotations

import os
import re
import sys
import time
from collections.abc import Callable
from contextlib import contextmanager
from datetime import datetime, timezone
from html import unescape
from typing import Any
from urllib.parse import urljoin

_PARSER_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PARSER_ROOT not in sys.path:
    sys.path.insert(0, _PARSER_ROOT)

from dotenv import load_dotenv

load_dotenv(os.path.join(_PARSER_ROOT, ".env"))
load_dotenv(os.path.join(os.path.dirname(_PARSER_ROOT), ".env"))

import requests
from bs4 import BeautifulSoup

from ai_monitoring import print_ai_session_summary, record_ai_skip, snapshot_ai_usage
from business_filters import classify_business_deadline, has_meaningful_funding
from config import get_global_config
from international_signals import detect_international_signal
from normalize_scholarship import apply_normalization
from scholarship_db_columns import SCHOLARSHIP_RECORD_DEFAULT_KEYS, SCHOLARSHIP_UPSERT_BODY_KEYS
from sources.scholarship_america.parser import parse_award_min_max, parse_deadline_date
from sources.shared_ai_enrichment import json_safe as _json_safe
from utils import (
    KnownScholarshipIndex,
    get_client,
    listing_is_known,
    load_known_scholarship_index,
    upsert_scholarship,
)

SOURCE = "iefa"
COUNTRY_SOURCE = "iefa"
SITE_ORIGIN = "https://www.iefa.org"
LIST_URL = f"{SITE_ORIGIN}/scholarships"
DEFAULT_CURRENCY = "USD"

_gc = get_global_config()
TARGET_NEW_ITEMS = _gc.target_new_items
SKIP_EXISTING_ON_LIST = _gc.skip_existing_on_list
USE_TITLE_FALLBACK_KNOWN = _gc.use_title_fallback_known
DISCOVERY_MODE = _gc.discovery_mode

_HTTP_TIMEOUT = max(15, int((os.getenv("IEFA_HTTP_TIMEOUT") or "45").strip() or "45"))
_REQUEST_DELAY_MS = max(0, int((os.getenv("IEFA_REQUEST_DELAY_MS") or "450").strip() or "450"))
_PER_PAGE = max(5, min(5000, int((os.getenv("IEFA_PER_PAGE") or "40").strip() or "40")))
_MAX_PAGES = max(0, int((os.getenv("IEFA_MAX_PAGES") or "0").strip() or "0"))
_MAX_RECORDS_DEBUG = max(0, int((os.getenv("IEFA_MAX_RECORDS_DEBUG") or "0").strip() or "0"))
_ONLY_INTERNATIONAL = (os.getenv("IEFA_ONLY_INTERNATIONAL") or "").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
_STRICT_DEADLINE = (os.getenv("IEFA_STRICT_DEADLINE") or "").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
_FORCE_REFRESH = (os.getenv("IEFA_FORCE_REFRESH") or "").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
_PROGRESS_EVERY = max(0, int((os.getenv("IEFA_PROGRESS_EVERY") or "5").strip() or "5"))
# Открыть реальное окно Chromium и грузить те же URL, что и HTTP-парсер (медленнее, но наглядно).
_VISIBLE_BROWSER = (os.getenv("IEFA_VISIBLE_BROWSER") or "").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)

_SESSION = requests.Session()
_SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
)

_LIST_PATH_RE = re.compile(r"^/scholarships/(\d+)/([^/?#]+)/?", re.I)


def _log(msg: str) -> None:
    print(msg, flush=True)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sleep_polite() -> None:
    if _REQUEST_DELAY_MS:
        time.sleep(_REQUEST_DELAY_MS / 1000.0)


def _fetch(url: str) -> str:
    last_err: Exception | None = None
    for attempt in range(3):
        try:
            r = _SESSION.get(url, timeout=_HTTP_TIMEOUT)
            r.raise_for_status()
            return r.text
        except Exception as exc:
            last_err = exc
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"IEFA fetch failed for {url!r}: {last_err}")


@contextmanager
def _playwright_fetcher():
    """Chromium: headless=False по умолчанию, если включён IEFA_VISIBLE_BROWSER."""
    from playwright.sync_api import sync_playwright

    default_headless = "0" if _VISIBLE_BROWSER else "1"
    headless = (os.getenv("IEFA_HEADLESS") or default_headless).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    timeout_ms = int(max(30, _HTTP_TIMEOUT)) * 1000
    _log(f"{SOURCE}: Playwright Chromium (headless={headless}); видно окно только при headless=0")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(timeout_ms)

        def fetch_html(url: str) -> str:
            last_err: Exception | None = None
            for attempt in range(3):
                try:
                    page.goto(url, wait_until="domcontentloaded")
                    return page.content()
                except Exception as exc:
                    last_err = exc
                    time.sleep(1.5 * (attempt + 1))
            raise RuntimeError(f"IEFA Playwright fetch failed for {url!r}: {last_err}")

        try:
            yield fetch_html
        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass


def _country_slug(label: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "-", (label or "").strip().lower()).strip("-")
    return s or "unknown"


def _join_labels(parts: list[str]) -> str:
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    if len(parts) == 2:
        return f"{parts[0]} and {parts[1]}"
    return ", ".join(parts[:-1]) + f", and {parts[-1]}"


def _parse_country_blob(raw: str | None) -> tuple[list[str], bool]:
    """
    Returns (labels, unrestricted).
    Unrestricted / world / empty host means unrestricted=True with empty list.
    """
    if raw is None:
        return [], True
    t = str(raw).strip()
    if not t:
        return [], True
    tl = t.lower()
    if tl in ("unrestricted", "world", "any", "all"):
        return [], True
    blob = t.replace(" and ", ", ")
    blob = re.sub(r"\s+and\s+", ", ", blob, flags=re.I)
    parts: list[str] = []
    for chunk in re.split(r"[,;/]|(?<!\d)\s+and\s+(?!\d)", blob):
        s = chunk.strip()
        if not s or s.lower() in ("and", "or"):
            continue
        if s not in parts:
            parts.append(s)
    return parts, False


def _extract_country_labels_from_listing_html(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    labels: list[str] = []
    seen: set[str] = set()
    for name in ("AwardSearch[locations]", "AwardSearch[details]"):
        sel = soup.find("select", attrs={"name": name})
        if not sel:
            continue
        for opt in sel.find_all("option"):
            val = opt.get("value") or ""
            text = opt.get_text(" ", strip=True)
            if not text or text.lower().startswith("where are"):
                continue
            if val in ("", "0", "all"):
                continue
            key = text.lower()
            if key in seen:
                continue
            seen.add(key)
            labels.append(text)
    return labels


def _sync_catalog_country_labels(labels: list[str]) -> int:
    if not labels:
        return 0
    try:
        client = get_client()
    except Exception as exc:
        _log(f"{SOURCE}: skip catalog_countries sync (no Supabase client): {exc}")
        return 0
    now = _now_iso()
    rows: list[dict[str, Any]] = []
    seen_slug: set[str] = set()
    for label in labels:
        slug = _country_slug(label)
        if slug in seen_slug:
            continue
        seen_slug.add(slug)
        rows.append(
            {
                "source": COUNTRY_SOURCE,
                "slug": slug,
                "display_name": label,
                "last_seen_at": now,
            }
        )
    n_ok = 0
    batch = 120
    for i in range(0, len(rows), batch):
        chunk = rows[i : i + batch]
        try:
            client.table("catalog_countries").upsert(chunk, on_conflict="source,slug").execute()
            n_ok += len(chunk)
        except Exception as exc:
            _log(f"{SOURCE}: catalog_countries upsert batch failed: {exc}")
    return n_ok


def _parse_total_items(html: str) -> int | None:
    """
    Листинг показывает, например: Showing <b>1-40</b> of <b>1,565</b> items.
    Раньше был <strong>; на мобильных/релизах тег может меняться — ловим оба и запятые в числе.
    """
    patterns = (
        r"of\s*<(?:strong|b)>\s*([\d,\s]+)\s*</(?:strong|b)>\s*items",
        r"of\s+([\d,]+)\s+items",
    )
    for pat in patterns:
        m = re.search(pat, html, re.I)
        if not m:
            continue
        raw = re.sub(r"[\s,]", "", m.group(1))
        if not raw.isdigit():
            continue
        try:
            return int(raw)
        except ValueError:
            continue
    return None


def _parse_last_page_hint_from_pager(html: str) -> int | None:
    """Максимальный номер page= в ссылках пагинации на этой странице (часто 1..10, не вся лента)."""
    nums: list[int] = []
    for m in re.finditer(r"scholarships\?page=(\d+)", html, re.I):
        try:
            nums.append(int(m.group(1)))
        except ValueError:
            continue
    return max(nums) if nums else None


def _listing_items(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    by_id: dict[str, dict[str, Any]] = {}
    for a in soup.select('a[href^="/scholarships/"]'):
        href = unescape(a.get("href") or "")
        m = _LIST_PATH_RE.match(href.split("?")[0])
        if not m:
            continue
        sid = m.group(1)
        slug_part = m.group(2)
        if sid in by_id:
            continue
        title_hint = a.get_text(" ", strip=True) or ""
        path = f"/scholarships/{sid}/{slug_part}"
        by_id[sid] = {
            "source_id": sid,
            "path": path,
            "title_hint": title_hint,
            "url": urljoin(SITE_ORIGIN, path),
        }
    return list(by_id.values())


def _next_div_text(node: Any) -> str:
    sib = node.find_next_sibling()
    if sib is None:
        return ""
    if sib.name in ("div", "p", "section"):
        return sib.get_text("\n", strip=True)
    return sib.get_text(" ", strip=True)


def _detail_table_map(soup: BeautifulSoup) -> dict[str, str]:
    for table in soup.find_all("table"):
        rows: dict[str, str] = {}
        for tr in table.find_all("tr"):
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
            if len(cells) >= 2:
                rows[cells[0]] = cells[1]
            elif len(cells) == 1:
                rows[cells[0]] = ""
        if "Nationality Required" in rows or "Host Countries" in rows:
            return rows
    return {}


def _parse_detail(html: str, url: str, source_id: str) -> dict[str, Any] | None:
    soup = BeautifulSoup(html, "html.parser")
    og = soup.find("meta", property="og:title")
    title = None
    if og and og.get("content"):
        raw_t = str(og["content"]).split("::")[0].strip()
        if raw_t:
            title = raw_t
    if not title:
        h1 = soup.find("h1")
        title = h1.get_text(" ", strip=True) if h1 else None
    if not title:
        return None

    sponsor = None
    for p in soup.find_all("p"):
        t = p.get_text(" ", strip=True)
        if t.lower().startswith("sponsor:"):
            sponsor = t.split(":", 1)[1].strip() or None
            break

    deadline_text = None
    fields_text = None
    award_amount_raw = None
    for h4 in soup.find_all("h4"):
        label = h4.get_text(" ", strip=True).lower()
        block = _next_div_text(h4)
        if "deadline" in label:
            deadline_text = block or None
        elif "field" in label and "study" in label:
            fields_text = block or None
        elif "award" in label and "amount" in label:
            award_amount_raw = block or None

    desc_parts: list[str] = []
    other_criteria_parts: list[str] = []
    for h2 in soup.find_all("h2"):
        ht = h2.get_text(" ", strip=True).lower()
        if "scholarship description" in ht:
            txt = _next_div_text(h2)
            if txt:
                desc_parts.append(txt)
        elif "other criteria" in ht:
            txt = _next_div_text(h2)
            if txt:
                other_criteria_parts.append(txt)

    tmap = _detail_table_map(soup)
    nat_raw = tmap.get("Nationality Required") or ""
    host_raw = tmap.get("Host Countries") or ""
    num_awards_raw = tmap.get("Number of Awards") or ""

    applicant_countries, applicant_unrestricted = _parse_country_blob(nat_raw)
    host_countries, host_unrestricted = _parse_country_blob(host_raw)

    number_of_awards: int | None = None
    if num_awards_raw:
        m = re.search(r"(\d+)", num_awards_raw)
        if m:
            try:
                number_of_awards = int(m.group(1))
            except ValueError:
                number_of_awards = None

    award_amount_text = award_amount_raw
    award_amount_min, award_amount_max = parse_award_min_max(award_amount_text)
    # Полная дата часто только в «Other criteria» / описании; иначе deadline_date=None и всё проходит при IEFA_STRICT_DEADLINE=0.
    description_body = "\n\n".join(desc_parts + other_criteria_parts) if (
        desc_parts or other_criteria_parts
    ) else ""
    deadline_date: str | None = None
    for blob in (
        [deadline_text] if deadline_text else []
    ) + other_criteria_parts + desc_parts:
        if not blob:
            continue
        deadline_date = parse_deadline_date(blob)
        if deadline_date:
            break

    nat_line = (
        "Unrestricted"
        if applicant_unrestricted
        else (nat_raw.strip() or _join_labels(applicant_countries))
    )
    host_line = (
        "Unrestricted" if host_unrestricted else (host_raw.strip() or _join_labels(host_countries))
    )
    country_summary = f"Applicants: {nat_line} | Study location: {host_line}"

    elig_head = (
        f"Nationality (IEFA): {nat_line}\n"
        f"Host countries (IEFA): {host_line}\n\n"
    )
    eligibility_text = elig_head + description_body
    requirements_text = description_body or eligibility_text

    tags = ["iefa"]
    if host_unrestricted and applicant_unrestricted:
        tags.append("international")

    return {
        "source_id": source_id,
        "url": url,
        "title": title,
        "provider_name": sponsor,
        "award_amount_text": award_amount_text,
        "award_amount_min": award_amount_min,
        "award_amount_max": award_amount_max,
        "currency": DEFAULT_CURRENCY,
        "deadline_text": deadline_text,
        "deadline_date": deadline_date,
        "description": description_body or eligibility_text,
        "eligibility_text": eligibility_text,
        "requirements_text": requirements_text,
        "apply_url": url,
        "apply_button_text": "View on IEFA",
        "mark_started_available": True,
        "mark_submitted_available": True,
        "official_source_name": "IEFA",
        "tags": tags[:24],
        "is_active": True,
        "is_recurring": False,
        "number_of_awards": number_of_awards,
        "host_country_names": host_countries,
        "applicant_country_names": applicant_countries,
        "country_summary": country_summary,
        "nationality_raw": nat_raw,
        "host_raw": host_raw,
        "applicant_unrestricted": applicant_unrestricted,
        "host_unrestricted": host_unrestricted,
        "fields_text": fields_text,
        "html_excerpt": html[:120_000],
    }


def _deadline_allowed(deadline_date: Any) -> tuple[bool, str]:
    dbiz = classify_business_deadline(deadline_date)
    if dbiz == "ok":
        return True, dbiz
    if dbiz in ("expired", "too_close"):
        return False, dbiz
    # no_deadline (rolling / descriptive deadlines on IEFA)
    if _STRICT_DEADLINE:
        return False, dbiz
    return True, f"{dbiz}_iefa_relaxed"


def _build_record(detail: dict[str, Any]) -> dict[str, Any]:
    fields_blob = detail.pop("fields_text", None)
    nationality_raw = detail.pop("nationality_raw", "")
    host_raw = detail.pop("host_raw", "")
    applicant_unrestricted = detail.pop("applicant_unrestricted", True)
    host_unrestricted = detail.pop("host_unrestricted", True)
    html_excerpt = detail.pop("html_excerpt", "")

    record: dict[str, Any] = {
        "source": SOURCE,
        "source_id": detail["source_id"],
        "url": detail["url"],
        "title": detail["title"],
        "provider_name": detail.get("provider_name"),
        "award_amount_text": detail.get("award_amount_text"),
        "award_amount_min": detail.get("award_amount_min"),
        "award_amount_max": detail.get("award_amount_max"),
        "currency": DEFAULT_CURRENCY,
        "deadline_text": detail.get("deadline_text"),
        "deadline_date": detail.get("deadline_date"),
        "description": detail.get("description"),
        "eligibility_text": detail.get("eligibility_text"),
        "requirements_text": detail.get("requirements_text"),
        "apply_url": detail.get("apply_url"),
        "apply_button_text": detail.get("apply_button_text"),
        "mark_started_available": True,
        "mark_submitted_available": True,
        "official_source_name": detail.get("official_source_name"),
        "tags": detail.get("tags"),
        "is_active": True,
        "is_recurring": False,
        "number_of_awards": detail.get("number_of_awards"),
        "host_country_names": detail.get("host_country_names") or [],
        "applicant_country_names": detail.get("applicant_country_names") or [],
        "country_summary": detail.get("country_summary"),
    }
    if fields_blob and str(fields_blob).strip():
        fos = str(fields_blob).strip()
        record["category"] = fos[:200] if len(fos) > 200 else fos

    apply_normalization(record)

    rd_base = {
        "captured_at": _now_iso(),
        "nationality_raw": nationality_raw,
        "host_countries_raw": host_raw,
        "applicant_unrestricted": applicant_unrestricted,
        "host_unrestricted": host_unrestricted,
        "fields_of_study_text": fields_blob,
        "html_excerpt_len": len(html_excerpt or ""),
    }
    prior = record.get("raw_data")
    if isinstance(prior, dict):
        rd = dict(prior)
    else:
        rd = {}
    rd["iefa"] = rd_base
    record["raw_data"] = _json_safe(rd)

    for key in SCHOLARSHIP_RECORD_DEFAULT_KEYS:
        if key not in record:
            record[key] = None

    record["source"] = SOURCE
    record["currency"] = DEFAULT_CURRENCY
    record["is_indexable"] = True
    record["is_verified"] = bool(record.get("is_verified"))

    unknown = set(record.keys()) - set(SCHOLARSHIP_UPSERT_BODY_KEYS) - {"id"}
    if unknown:
        raise ValueError(f"IEFA: unknown record keys: {sorted(unknown)}")

    return record


def _run_iefa_scrape(fetch_html: Callable[[str], str], ai_usage_start: Any) -> None:
    effective_target = (
        min(TARGET_NEW_ITEMS, _MAX_RECORDS_DEBUG) if _MAX_RECORDS_DEBUG > 0 else TARGET_NEW_ITEMS
    )
    if _MAX_RECORDS_DEBUG > 0:
        _log(
            f"{SOURCE}: IEFA_MAX_RECORDS_DEBUG={_MAX_RECORDS_DEBUG} "
            f"-> stop after {effective_target} successful upsert(s) (not full catalog)"
        )
    use_skip = (
        (SKIP_EXISTING_ON_LIST and DISCOVERY_MODE == "new_only") and not _FORCE_REFRESH
    )
    _log(
        f"{SOURCE}: skip existing in DB: {use_skip} (SKIP_EXISTING_ON_LIST + DISCOVERY_MODE; "
        f"IEFA_FORCE_REFRESH={_FORCE_REFRESH})"
    )

    idx: KnownScholarshipIndex
    if use_skip:
        try:
            idx = load_known_scholarship_index(get_client(), SOURCE)
            _log(
                f"{SOURCE}: known index loaded: urls={len(idx.urls)} "
                f"source_ids={len(idx.source_ids)}"
            )
        except Exception as exc:
            _log(f"{SOURCE}: warning: known index failed ({exc})")
            idx = KnownScholarshipIndex()
    else:
        idx = KnownScholarshipIndex()

    stats: dict[str, int] = {
        "pages": 0,
        "list_items": 0,
        "detail_ok": 0,
        "skipped_known": 0,
        "skip_funding": 0,
        "skip_deadline": 0,
        "skip_international": 0,
        "upsert_ok": 0,
        "upsert_failed": 0,
        "countries_synced": 0,
    }

    _log(f"{SOURCE}: fetching listing page 1 (per-page={_PER_PAGE})")
    first_html = fetch_html(f"{LIST_URL}?page=1&per-page={_PER_PAGE}")
    total_items = _parse_total_items(first_html)
    labels = _extract_country_labels_from_listing_html(first_html)
    n_c = _sync_catalog_country_labels(labels)
    stats["countries_synced"] += n_c
    _log(f"{SOURCE}: catalog country labels synced (batch rows) ~ {n_c}; unique labels={len(labels)}")

    total_pages = 1
    if total_items:
        total_pages = max(1, (total_items + _PER_PAGE - 1) // _PER_PAGE)
    cap_pages = total_pages
    if _MAX_PAGES > 0:
        cap_pages = min(cap_pages, _MAX_PAGES)
    if _gc.max_list_pages > 0:
        cap_pages = min(cap_pages, _gc.max_list_pages)
    unknown_total = total_items is None
    pager_hint = _parse_last_page_hint_from_pager(first_html)
    if unknown_total:
        cap_pages = max(cap_pages, _gc.max_list_pages or _MAX_PAGES or 500)
    _log(
        f"{SOURCE}: total_items={total_items} -> total_pages={total_pages}; "
        f"pager_links_max_page={pager_hint}; cap_pages={cap_pages}; "
        f"unknown_total={unknown_total}"
    )

    seen_session: set[str] = set()
    page = 0
    while True:
        page += 1
        if page > cap_pages:
            break
        stats["pages"] += 1
        html = first_html if page == 1 else fetch_html(f"{LIST_URL}?page={page}&per-page={_PER_PAGE}")
        items = _listing_items(html)
        stats["list_items"] += len(items)
        _log(f"{SOURCE}: page {page} - {len(items)} scholarships on page")

        if unknown_total and not items:
            _log(f"{SOURCE}: empty listing page {page}; stop pagination")
            break

        n_on_page = len(items)
        for slot_i, it in enumerate(items, start=1):
            sid = str(it["source_id"])
            url = str(it["url"])
            if sid in seen_session:
                continue
            seen_session.add(sid)

            preview = {"source": SOURCE, "source_id": sid, "url": url, "title": it.get("title_hint")}
            if use_skip and listing_is_known(preview, idx, title_fallback=USE_TITLE_FALLBACK_KNOWN):
                stats["skipped_known"] += 1
                record_ai_skip()
                if _PROGRESS_EVERY and (
                    slot_i % _PROGRESS_EVERY == 0 or slot_i == n_on_page
                ):
                    _log(
                        f"{SOURCE}: page {page} row {slot_i}/{n_on_page} "
                        f"(already in DB) skipped_known={stats['skipped_known']} "
                        f"upsert_ok={stats['upsert_ok']}"
                    )
                continue

            try:
                dhtml = fetch_html(url)
                detail = _parse_detail(dhtml, url, sid)
                if not detail:
                    continue
                stats["detail_ok"] += 1
                record = _build_record(detail)
            except Exception as exc:
                stats["upsert_failed"] += 1
                _log(f"{SOURCE}: detail/build failed {url} -> {exc}")
                _sleep_polite()
                if _PROGRESS_EVERY and (
                    slot_i % _PROGRESS_EVERY == 0 or slot_i == n_on_page
                ):
                    _log(
                        f"{SOURCE}: page {page} row {slot_i}/{n_on_page} "
                        f"detail_ok={stats['detail_ok']} upsert_ok={stats['upsert_ok']} "
                        f"upsert_failed={stats['upsert_failed']}"
                    )
                continue

            if not has_meaningful_funding(record):
                intl = detect_international_signal(
                    record.get("title"),
                    record.get("url"),
                    record.get("eligibility_text"),
                    record.get("requirements_text"),
                    record.get("description"),
                    record.get("country_summary"),
                    record.get("tags"),
                )
                if not intl:
                    stats["skip_funding"] += 1
                    record_ai_skip()
                    _sleep_polite()
                    if _PROGRESS_EVERY and (
                        slot_i % _PROGRESS_EVERY == 0 or slot_i == n_on_page
                    ):
                        _log(
                            f"{SOURCE}: page {page} row {slot_i}/{n_on_page} "
                            f"skip_funding={stats['skip_funding']} upsert_ok={stats['upsert_ok']}"
                        )
                    continue

            ok_d, _ = _deadline_allowed(record.get("deadline_date"))
            if not ok_d:
                stats["skip_deadline"] += 1
                record_ai_skip()
                _sleep_polite()
                if _PROGRESS_EVERY and (
                    slot_i % _PROGRESS_EVERY == 0 or slot_i == n_on_page
                ):
                    _log(
                        f"{SOURCE}: page {page} row {slot_i}/{n_on_page} "
                        f"skip_deadline={stats['skip_deadline']} upsert_ok={stats['upsert_ok']}"
                    )
                continue

            if _ONLY_INTERNATIONAL:
                intl = detect_international_signal(
                    record.get("title"),
                    record.get("url"),
                    record.get("eligibility_text"),
                    record.get("requirements_text"),
                    record.get("description"),
                    record.get("country_summary"),
                    record.get("tags"),
                )
                if not intl:
                    stats["skip_international"] += 1
                    record_ai_skip()
                    _sleep_polite()
                    if _PROGRESS_EVERY and (
                        slot_i % _PROGRESS_EVERY == 0 or slot_i == n_on_page
                    ):
                        _log(
                            f"{SOURCE}: page {page} row {slot_i}/{n_on_page} "
                            f"skip_international={stats['skip_international']} "
                            f"upsert_ok={stats['upsert_ok']}"
                        )
                    continue

            merge_labels: list[str] = []
            merge_labels.extend(record.get("host_country_names") or [])
            merge_labels.extend(record.get("applicant_country_names") or [])
            stats["countries_synced"] += _sync_catalog_country_labels(merge_labels)

            try:
                upsert_scholarship(record)
                stats["upsert_ok"] += 1
            except Exception as exc:
                stats["upsert_failed"] += 1
                _log(f"{SOURCE}: upsert failed {record.get('title')!r} -> {exc}")

            _sleep_polite()

            if _PROGRESS_EVERY and (slot_i % _PROGRESS_EVERY == 0 or slot_i == n_on_page):
                _log(
                    f"{SOURCE}: page {page} row {slot_i}/{n_on_page} "
                    f"skipped_known={stats['skipped_known']} detail_ok={stats['detail_ok']} "
                    f"upsert_ok={stats['upsert_ok']} skip_funding={stats['skip_funding']} "
                    f"skip_deadline={stats['skip_deadline']} upsert_failed={stats['upsert_failed']}"
                )

            if effective_target > 0 and stats["upsert_ok"] >= effective_target:
                cap_reason = (
                    f"IEFA_MAX_RECORDS_DEBUG={_MAX_RECORDS_DEBUG}"
                    if _MAX_RECORDS_DEBUG > 0
                    else f"TARGET_NEW_ITEMS={TARGET_NEW_ITEMS}"
                )
                _log(f"{SOURCE}: reached upsert cap ({effective_target}) via {cap_reason}")
                break
        if effective_target > 0 and stats["upsert_ok"] >= effective_target:
            break
        _sleep_polite()

    _log(
        f"{SOURCE}: done pages={stats['pages']} list_items={stats['list_items']} "
        f"detail_ok={stats['detail_ok']} skipped_known={stats['skipped_known']} "
        f"skip_funding={stats['skip_funding']} skip_deadline={stats['skip_deadline']} "
        f"skip_international={stats['skip_international']} "
        f"upsert_ok={stats['upsert_ok']} upsert_failed={stats['upsert_failed']} "
        f"countries_upserts~{stats['countries_synced']}"
    )
    print_ai_session_summary(
        SOURCE,
        processed=stats["detail_ok"],
        new_found=stats["upsert_ok"],
        start=ai_usage_start,
    )


def run() -> None:
    enabled = (os.getenv("IEFA_ENABLED") or "").strip().lower() in ("1", "true", "yes", "on")
    if not enabled:
        _log(f"{SOURCE}: disabled (set IEFA_ENABLED=1 to run)")
        return

    ai_usage_start = snapshot_ai_usage()
    if _VISIBLE_BROWSER:
        _log(
            f"{SOURCE}: IEFA_VISIBLE_BROWSER=1 — открывается Chromium; "
            f"HTTP-режим без окна: выключите флаг в .env"
        )
        with _playwright_fetcher() as fetch_html:
            _run_iefa_scrape(fetch_html, ai_usage_start)
    else:
        _log(
            f"{SOURCE}: HTTP-режим (requests), окна браузера нет. "
            f"Чтобы видеть Chromium: IEFA_VISIBLE_BROWSER=1"
        )
        _run_iefa_scrape(_fetch, ai_usage_start)


if __name__ == "__main__":
    run()
