"""DAAD scholarship database parser -> public.scholarships (Supabase)."""

from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests
from bs4 import BeautifulSoup

_PARSER_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PARSER_ROOT not in sys.path:
    sys.path.insert(0, _PARSER_ROOT)

from business_filters import classify_business_deadline, has_meaningful_funding
from config import get_global_config
from country_eligibility import country_codes_from_labels
from normalize_scholarship import apply_normalization
from scholarship_db_columns import SCHOLARSHIP_RECORD_DEFAULT_KEYS, SCHOLARSHIP_UPSERT_BODY_KEYS
from sources.scholarship_america.parser import parse_award_min_max
from utils import KnownScholarshipIndex, get_client, listing_is_known, load_known_scholarship_index, upsert_scholarship

SOURCE = "daad"
SITE_ORIGIN = "https://www2.daad.de"
ALT_SITE_ORIGIN = "https://www.daad.de"
DB_URL = f"{SITE_ORIGIN}/deutschland/stipendium/datenbank/en/21148-scholarship-database/"
DATA_BASE = f"{SITE_ORIGIN}/bundles/daadstipendiendatenbanklsh/data/a/js"
ALT_DATA_BASE = f"{ALT_SITE_ORIGIN}/bundles/daadstipendiendatenbanklsh/data/a/js"
DEFAULT_CURRENCY = "EUR"
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


DAAD_ENABLED = _get_bool_env("DAAD_ENABLED", True)
DAAD_DETAIL_FETCH = _get_bool_env("DAAD_DETAIL_FETCH", True)
DAAD_REQUEST_DELAY_MS = max(0, _get_int_env("DAAD_REQUEST_DELAY_MS", 500))
DAAD_TIMEOUT_SECONDS = max(10, _get_int_env("DAAD_TIMEOUT_SECONDS", 45))
DAAD_RETRY_ATTEMPTS = max(1, _get_int_env("DAAD_RETRY_ATTEMPTS", 4))
DAAD_RETRY_BACKOFF_SECONDS = max(1, _get_int_env("DAAD_RETRY_BACKOFF_SECONDS", 5))
DAAD_ONLY_DAAD_FUNDED = _get_bool_env("DAAD_ONLY_DAAD_FUNDED", False)
DAAD_MAX_RECORDS_DEBUG = max(0, _get_int_env("DAAD_MAX_RECORDS_DEBUG", 0))


def _log(message: str) -> None:
    print(message, flush=True)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = BeautifulSoup(str(value).replace("\xa0", " "), "html.parser").get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _fetch(url: str) -> str:
    last_exc: Exception | None = None
    for attempt in range(1, DAAD_RETRY_ATTEMPTS + 1):
        if DAAD_REQUEST_DELAY_MS:
            time.sleep(DAAD_REQUEST_DELAY_MS / 1000.0)
        try:
            response = requests.get(url, headers=HEADERS, timeout=DAAD_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            last_exc = exc
            if attempt >= DAAD_RETRY_ATTEMPTS:
                break
            wait_s = DAAD_RETRY_BACKOFF_SECONDS * attempt
            _log(f"{SOURCE}: fetch retry {attempt}/{DAAD_RETRY_ATTEMPTS} after {type(exc).__name__}: {url} (sleep {wait_s}s)")
            time.sleep(wait_s)
    if last_exc:
        raise last_exc
    raise RuntimeError(f"Failed to fetch {url}")


def _load_taffy(name: str) -> list[dict[str, Any]]:
    last_exc: Exception | None = None
    for base_url in (DATA_BASE, ALT_DATA_BASE):
        try:
            text = _fetch(f"{base_url}/{name}.js")
            break
        except requests.RequestException as exc:
            last_exc = exc
            _log(f"{SOURCE}: data host failed for {name}.js: {base_url} ({type(exc).__name__})")
    else:
        if last_exc:
            raise last_exc
        raise RuntimeError(f"Cannot fetch DAAD data file {name}.js")
    start = text.find("[")
    end = text.rfind("]")
    if start < 0 or end <= start:
        raise ValueError(f"Cannot parse DAAD data file {name}.js")
    data = json.loads(text[start : end + 1])
    return [item for item in data if isinstance(item, dict)]


def _detail_url(sap_progid: Any) -> str:
    return f"{DB_URL}?detail={sap_progid}"


def _source_id(item: dict[str, Any]) -> str:
    return str(item.get("sapProgid") or item.get("sapObjid") or item.get("id") or "").strip()


def _extract_sections(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    h2 = soup.find("h2")
    title = _clean_text(h2.get_text(" ", strip=True) if h2 else soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else None)
    lines = [ln.strip() for ln in soup.get_text("\n", strip=True).splitlines() if ln.strip()]
    joined = "\n".join(lines)

    def section(start_re: str, stop_res: tuple[str, ...]) -> str | None:
        out: list[str] = []
        active = False
        for line in lines:
            if re.search(start_re, line, re.I):
                active = True
                continue
            if active and any(re.search(pat, line, re.I) for pat in stop_res):
                break
            if active:
                out.append(line)
        return _clean_text("\n".join(out))

    return {
        "title": title,
        "objective": section(r"^Objective$", (r"^Who can apply\?$", r"^What can be funded\?$", r"^Duration of the funding$")),
        "who_can_apply": section(r"^Who can apply\?$", (r"^What can be funded\?$", r"^Duration of the funding$", r"^Value$")),
        "what_can_be_funded": section(r"^What can be funded\?$", (r"^Duration of the funding$", r"^Value$", r"^Selection$")),
        "duration": section(r"^Duration of the funding$", (r"^Value$", r"^Selection$", r"^Application requirements$")),
        "value": section(r"^Value$", (r"^Selection$", r"^Application requirements$", r"^What requirements must be met\?$")),
        "selection": section(r"^Selection$", (r"^Application requirements$", r"^What requirements must be met\?$", r"^Application Procedure$")),
        "requirements": section(r"^What requirements must be met\?$", (r"^Language skills$", r"^Application Procedure$", r"^Application deadline$")),
        "language_skills": section(r"^Language skills$", (r"^Application Procedure$", r"^Application deadline$", r"^Application documents$")),
        "application_deadline": section(r"^Application deadline$", (r"^Application documents$", r"^Please note$", r"^Contact and Consulting$")),
        "application_documents": section(r"^Application documents$", (r"^Please note$", r"^Contact and Consulting$", r"^Submitting an application$")),
        "contact": section(r"^Contact and Consulting$", (r"^Submitting an application$", r"^Funded by:$", r"^Please select your status")),
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


def _parse_date(text: str) -> str | None:
    text = _clean_text(text) or ""
    for match in re.finditer(r"\b(\d{1,2})\.(\d{1,2})\.(20\d{2})\b", text):
        try:
            return datetime(int(match.group(3)), int(match.group(2)), int(match.group(1))).date().isoformat()
        except ValueError:
            continue
    for match in re.finditer(r"\b(\d{1,2})\s+([A-Za-z]{3,9})\s+(20\d{2})\b", text, re.I):
        mon = _MONTHS.get(match.group(2).lower())
        if mon:
            return datetime(int(match.group(3)), mon, int(match.group(1))).date().isoformat()
    for match in re.finditer(r"\b([A-Za-z]{3,9})\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(20\d{2})\b", text, re.I):
        mon = _MONTHS.get(match.group(1).lower())
        if mon:
            return datetime(int(match.group(3)), mon, int(match.group(2))).date().isoformat()
    return None


def _deadline_info(sap_progid: Any, deadline_by_id: dict[int, dict[str, Any]]) -> tuple[str | None, str | None]:
    try:
        rec = deadline_by_id.get(int(sap_progid))
    except (TypeError, ValueError):
        rec = None
    if not rec:
        return None, None
    texts: list[str] = []
    general = rec.get("general")
    if isinstance(general, dict):
        texts.append(str(general.get("en") or ""))
    countries = rec.get("countries")
    if isinstance(countries, dict):
        for rows in countries.values():
            if isinstance(rows, list):
                for row in rows:
                    if isinstance(row, dict) and row.get("en"):
                        texts.append(str(row.get("en")))
    cleaned = [_clean_text(t) for t in texts]
    cleaned = [t for t in cleaned if t]
    future_dates = [d for d in (_parse_date(t) for t in cleaned) if d and classify_business_deadline(d) == "ok"]
    deadline_date = min(future_dates) if future_dates else None
    if deadline_date:
        sample = next((t for t in cleaned if deadline_date[:4] in t or deadline_date[5:7] in t), None)
        return sample or f"Application deadline: {deadline_date}", deadline_date
    return (cleaned[0] if cleaned else None), None


def _country_names(ids: list[Any], origin_by_id: dict[int, str]) -> list[str]:
    names: list[str] = []
    for cid in ids or []:
        try:
            name = origin_by_id.get(int(cid))
        except (TypeError, ValueError):
            name = None
        if name and name not in names:
            names.append(name)
    return names


def _award_text(detail: dict[str, Any]) -> str:
    value = detail.get("value")
    if value:
        return str(value)
    return "DAAD scholarship funding; value varies by programme"


def _build_record(
    item: dict[str, Any],
    detail: dict[str, Any],
    deadline_by_id: dict[int, dict[str, Any]],
    origin_by_id: dict[int, str],
) -> dict[str, Any]:
    sid = _source_id(item)
    title = detail.get("title") or item.get("nameEn") or item.get("programmnameEn") or "DAAD scholarship"
    title = re.sub(r"\s+•\s+DAAD$", "", str(title)).strip()
    url = _detail_url(item.get("sapProgid") or item.get("sapObjid"))
    deadline_text, deadline_date = _deadline_info(item.get("sapProgid"), deadline_by_id)
    applicant_country_names = _country_names(list(item.get("origin") or []), origin_by_id)
    applicant_country_codes = country_codes_from_labels(applicant_country_names)
    value_text = _award_text(detail)
    amin, amax = parse_award_min_max(value_text)
    description = "\n".join(
        str(x)
        for x in (detail.get("objective"), detail.get("what_can_be_funded"), detail.get("duration"))
        if x
    )
    eligibility = "\n".join(
        str(x)
        for x in (detail.get("who_can_apply"), detail.get("requirements"), detail.get("language_skills"))
        if x
    )
    requirements = "\n".join(
        str(x)
        for x in (eligibility, detail.get("application_documents"))
        if x
    )
    record: dict[str, Any] = {
        "source": SOURCE,
        "source_id": sid,
        "url": url,
        "title": title,
        "provider_name": "DAAD" if item.get("isDaad") else "DAAD scholarship database",
        "provider_url": "https://www.daad.de/en/",
        "award_amount_text": value_text,
        "award_amount_min": amin,
        "award_amount_max": amax,
        "currency": DEFAULT_CURRENCY,
        "deadline_text": deadline_text or detail.get("application_deadline") or "Application deadlines vary by country/programme.",
        "deadline_date": deadline_date,
        "description": description or detail.get("body_text") or title,
        "eligibility_text": eligibility or detail.get("who_can_apply") or description or title,
        "requirements_text": requirements or eligibility or description or title,
        "awards_text": value_text,
        "selection_criteria_text": detail.get("selection"),
        "apply_url": url,
        "apply_button_text": "View DAAD programme",
        "mark_started_available": True,
        "mark_submitted_available": True,
        "official_source_name": "DAAD",
        "is_verified": bool(item.get("isDaad")),
        "is_active": True,
        "is_recurring": True,
        "host_country_names": ["Germany"],
        "applicant_country_names": applicant_country_names,
        "applicant_country_codes": applicant_country_codes,
        "host_country_codes": ["DE"],
        "country_eligibility_notes": [
            "DAAD database country/origin eligibility was mapped from the programme listing.",
        ],
        "full_content_html": detail.get("full_content_html"),
        "tags": ["daad", "germany"],
        "raw_data": {
            "captured_at": _now_iso(),
            "listing": item,
            "deadline": deadline_by_id.get(int(item.get("sapProgid") or 0)) if str(item.get("sapProgid") or "").isdigit() else None,
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
    if not DAAD_ENABLED:
        _log(f"{SOURCE}: disabled via DAAD_ENABLED=0")
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

    scholarships = _load_taffy("scholarships")
    deadlines = _load_taffy("deadlines")
    origins = _load_taffy("origin")
    deadline_by_id = {int(x["id"]): x for x in deadlines if str(x.get("id") or "").isdigit()}
    origin_by_id = {int(x["id"]): str(x.get("nameEn") or "") for x in origins if str(x.get("id") or "").isdigit()}
    if DAAD_ONLY_DAAD_FUNDED:
        scholarships = [x for x in scholarships if int(x.get("isDaad") or 0) == 1]
    scholarships = sorted(scholarships, key=lambda x: (0 if int(x.get("isDaad") or 0) == 1 else 1, str(x.get("nameEn") or "")))
    if MAX_LIST_PAGES > 0:
        # DAAD data is one static list; interpret pages as soft batches of 25.
        scholarships = scholarships[: MAX_LIST_PAGES * 25]

    stats = {"listing_seen": 0, "known_skipped": 0, "skip_no_funding": 0, "skip_deadline": 0, "upsert_ok": 0, "upsert_failed": 0}
    success_rows: list[dict[str, str]] = []
    _log(f"{SOURCE}: candidates={len(scholarships)}")
    for item in scholarships:
        sid = _source_id(item)
        if not sid:
            continue
        preview = {"source": SOURCE, "source_id": sid, "url": _detail_url(item.get("sapProgid")), "title": item.get("nameEn")}
        stats["listing_seen"] += 1
        if SKIP_EXISTING_ON_LIST and listing_is_known(preview, idx, title_fallback=USE_TITLE_FALLBACK_KNOWN):
            stats["known_skipped"] += 1
            continue
        detail = _extract_sections(_fetch(preview["url"])) if DAAD_DETAIL_FETCH else {}
        record = _build_record(item, detail, deadline_by_id, origin_by_id)
        if not has_meaningful_funding(record):
            stats["skip_no_funding"] += 1
            _log(f"{SOURCE}: skip no funding: {record.get('title')}")
            continue
        dbiz = classify_business_deadline(record.get("deadline_date"))
        if dbiz in {"expired", "too_close"}:
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
            break
        if DAAD_MAX_RECORDS_DEBUG > 0 and stats["listing_seen"] >= DAAD_MAX_RECORDS_DEBUG:
            _log(f"{SOURCE}: reached debug cap={DAAD_MAX_RECORDS_DEBUG}")
            break
    _log(f"{SOURCE}: success rows: {success_rows}")
    _log(f"{SOURCE}: done {stats}")


if __name__ == "__main__":
    run()
