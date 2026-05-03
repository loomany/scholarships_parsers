"""Appily (my.appily.com) scholarship search -> public.scholarships (Supabase).

Playwright opens the authenticated search UI. You complete login/password in the
browser, press Enter in the terminal (TTY), then the parser scrolls/collects JSON
from network responses and upserts normalized records — same ergonomics pattern as
sources.bold_org manual auth gate.
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

from business_filters import classify_business_deadline, has_meaningful_funding
from config import get_global_config
from normalize_scholarship import apply_normalization
from scholarship_db_columns import SCHOLARSHIP_RECORD_DEFAULT_KEYS, SCHOLARSHIP_UPSERT_BODY_KEYS
from sources.scholarship_america.parser import parse_award_min_max, parse_deadline_date
from utils import KnownScholarshipIndex, get_client, listing_is_known, load_known_scholarship_index, upsert_scholarship

SOURCE = "appily"
DEFAULT_CURRENCY = "USD"
ORIGIN_MY = "https://my.appily.com"
SCHOLARSHIP_SEARCH_URL = f"{ORIGIN_MY}/scholarship-search"
SESSION_STATE_PATH = os.path.join(_PARSER_ROOT, "appily_session.json")

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


APPILY_ENABLED = _get_bool_env("APPILY_ENABLED", False)
APPILY_HEADLESS = _get_bool_env("APPILY_HEADLESS", False)
APPILY_REQUIRE_MANUAL_AUTH = _get_bool_env("APPILY_REQUIRE_MANUAL_AUTH", True)
APPILY_TIMEOUT_MS = _get_int_env("APPILY_TIMEOUT_MS", 120_000)
APPILY_SCROLL_STEPS = max(0, _get_int_env("APPILY_SCROLL_STEPS", 24))
APPILY_SCROLL_WAIT_MS = max(250, _get_int_env("APPILY_SCROLL_WAIT_MS", 2200))
APPILY_NO_NEW_ROUNDS_STOP = max(1, _get_int_env("APPILY_NO_NEW_ROUNDS_STOP", 8))
APPILY_AUTH_WAIT_SECONDS = max(60, _get_int_env("APPILY_AUTH_WAIT_SECONDS", 900))
APPILY_MAX_RECORDS_DEBUG = max(0, _get_int_env("APPILY_MAX_RECORDS_DEBUG", 0))
APPILY_DOM_FALLBACK = _get_bool_env("APPILY_DOM_FALLBACK", True)


TITLE_KEYS = ("title", "name", "scholarshipname", "scholarshiptitle", "displayname")
URL_KEYS = ("url", "link", "href", "detailurl", "scholarshipurl", "canonicalurl", "externalurl")
ID_KEYS = ("id", "scholarshipid", "scholarship_id", "scholarshipid")
AWARD_KEYS = ("amount", "awardamount", "scholarshipamount", "award", "awardvalue", "value")
DEADLINE_KEYS = ("deadline", "enddate", "applicationdeadline", "duedate", "expirationdate")


def _log(msg: str) -> None:
    print(msg, flush=True)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()
    return text or None


def _normalize_key(key: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(key or "").strip().lower())


def _iter_recursive_values(obj: Any, wanted_normalized: frozenset[str], max_depth: int = 7) -> list[Any]:
    if max_depth < 0:
        return []
    out: list[Any] = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            if _normalize_key(str(key)) in wanted_normalized:
                out.append(value)
            if isinstance(value, (dict, list)):
                out.extend(_iter_recursive_values(value, wanted_normalized, max_depth=max_depth - 1))
    elif isinstance(obj, list):
        for value in obj[:300]:
            if isinstance(value, (dict, list)):
                out.extend(_iter_recursive_values(value, wanted_normalized, max_depth=max_depth - 1))
    return out


def _first_str(obj: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    nk = frozenset(_normalize_key(k) for k in keys)
    for key, value in obj.items():
        if _normalize_key(str(key)) not in nk:
            continue
        if isinstance(value, str):
            t = _clean_text(value)
            if t:
                return t
    vals = _iter_recursive_values(obj, nk)
    for value in vals:
        if isinstance(value, str):
            t = _clean_text(value)
            if t:
                return t
    return None


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
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).date().isoformat()
        except ValueError:
            return None
    if isinstance(value, (int, float)) and value == value:
        try:
            return datetime.fromtimestamp(float(value) / (1000.0 if value > 1e12 else 1.0), tz=timezone.utc).date().isoformat()
        except (OverflowError, OSError, ValueError):
            return None
    return None


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
    return urljoin(f"{ORIGIN_MY}/", raw.lstrip("/"))


def _candidate_url(item: dict[str, Any]) -> str | None:
    for key in URL_KEYS:
        u = _to_absolute_url(_clean_text(item.get(key)))
        if u and "appily.com" in u.lower():
            return u
    for key in URL_KEYS:
        u = _to_absolute_url(_clean_text(item.get(key)))
        if u:
            return u
    blob = json.dumps(item, default=str)[:8000]
    for m in re.finditer(r"https?://[^\s\"'<>]+", blob, re.I):
        u = m.group(0).rstrip("),.;")
        if "appily.com" in u.lower() or "/scholarship" in u.lower():
            return u
    return None


def _candidate_source_id(item: dict[str, Any], url: str | None) -> str | None:
    for key in ID_KEYS:
        v = item.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    if url:
        return hashlib.sha256(url.encode("utf-8")).hexdigest()[:24]
    return None


def _looks_like_scholarship_obj(obj: dict[str, Any]) -> bool:
    title = _first_str(obj, TITLE_KEYS)
    if not title or len(title) < 4:
        return False
    url = _candidate_url(obj)
    if not url:
        return False
    sid = _candidate_source_id(obj, url)
    desc = _clean_text(obj.get("description")) or _clean_text(obj.get("summary")) or ""
    has_meta = bool(
        sid
        or _first_str(obj, AWARD_KEYS)
        or _first_str(obj, DEADLINE_KEYS)
        or _parse_iso_dateish(obj.get("endDate") or obj.get("deadline") or obj.get("applicationDeadline"))
        or len(desc) > 24
    )
    return has_meta


def _extract_scholarship_candidates(node: Any, max_depth: int = 8) -> list[dict[str, Any]]:
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
        for value in node[:400]:
            if isinstance(value, dict) and _looks_like_scholarship_obj(value):
                out.append(value)
            elif isinstance(value, (dict, list)):
                out.extend(_extract_scholarship_candidates(value, max_depth=max_depth - 1))
    return out


def _build_record(item: dict[str, Any], response_url: str) -> dict[str, Any] | None:
    title = _first_str(item, TITLE_KEYS)
    url = _candidate_url(item)
    if not title or not url:
        return None

    sid = _candidate_source_id(item, url)
    award_raw = (
        _first_str(item, AWARD_KEYS)
        or _clean_text(item.get("maxAward"))
        or _clean_text(item.get("awardText"))
    )
    award_amount_text = award_raw if award_raw and "$" in award_raw else (
        f"${award_raw.replace('$', '').strip()}" if award_raw and re.search(r"\d", award_raw) else award_raw or "See Appily listing for award details."
    )
    amin, amax = parse_award_min_max(award_amount_text or "")

    deadline_raw = None
    for k in DEADLINE_KEYS:
        deadline_raw = item.get(k)
        if deadline_raw is not None:
            break
    deadline_text = _clean_text(deadline_raw) if isinstance(deadline_raw, str) else None
    deadline_date = _parse_iso_dateish(deadline_raw) or parse_deadline_date(deadline_text)

    description = (
        _clean_text(item.get("description"))
        or _clean_text(item.get("shortDescription"))
        or _clean_text(item.get("summary"))
        or title
    )
    eligibility = (
        _clean_text(item.get("eligibility"))
        or _clean_text(item.get("eligibilityRequirements"))
        or description
    )
    provider = (
        _clean_text(item.get("provider"))
        or _clean_text(item.get("sponsor"))
        or _clean_text(item.get("organizationName"))
        or "Appily scholarship database"
    )

    slug = urlparse(url).path.strip("/").split("/")[-1] if url else None

    record: dict[str, Any] = {
        "source": SOURCE,
        "source_id": sid,
        "url": url,
        "title": title,
        "provider_name": provider,
        "provider_url": ORIGIN_MY,
        "award_amount_text": award_amount_text,
        "award_amount_min": amin,
        "award_amount_max": amax,
        "currency": DEFAULT_CURRENCY,
        "deadline_text": deadline_text,
        "deadline_date": deadline_date,
        "description": description,
        "eligibility_text": eligibility,
        "requirements_text": eligibility,
        "apply_url": url,
        "apply_button_text": "Open on Appily",
        "mark_started_available": True,
        "mark_submitted_available": True,
        "official_source_name": "Appily",
        "is_active": True,
        "is_recurring": False,
        "tags": ["appily"],
        "raw_data": {"captured_at": _now_iso(), "network_from": response_url, "snapshot": item},
        "full_content_html": None,
    }
    apply_normalization(record)
    if slug:
        record["slug"] = str(slug)[:120]
    for key in SCHOLARSHIP_RECORD_DEFAULT_KEYS:
        if key not in record:
            record[key] = None
    record["source"] = SOURCE
    record["is_active"] = True
    return record


class _CaptureState:
    def __init__(self) -> None:
        self.json_seen = 0
        self.candidate_items_seen = 0
        self.captured: list[tuple[dict[str, Any], str]] = []
        self.identities: set[str] = set()


def _identity(item: dict[str, Any], response_url: str) -> str:
    return " | ".join(
        [_candidate_source_id(item, _candidate_url(item)) or "", _candidate_url(item) or "", response_url[:200]],
    )


def _response_handler_factory(state: _CaptureState):
    def _handler(response: Any) -> None:
        raw_url = response.url or ""
        if "appily.com" not in raw_url.lower():
            return
        if response.status >= 400:
            return
        ct = (response.headers.get("content-type") or "").lower()
        url_low = raw_url.lower()
        if "json" not in ct and not any(
            t in url_low for t in ("graphql", "/api/", "scholar", "search", "eligible", "match")
        ):
            return
        try:
            payload = response.json()
        except Exception:
            return
        state.json_seen += 1
        candidates = _extract_scholarship_candidates(payload)
        if not candidates:
            return
        new_added = 0
        for item in candidates:
            ident = _identity(item, response.url)
            if ident in state.identities:
                continue
            state.identities.add(ident)
            state.captured.append((item, response.url))
            state.candidate_items_seen += 1
            new_added += 1
        if new_added:
            _log(f"{SOURCE}: captured +{new_added} scholarship-like objects from {raw_url[:120]}")

    return _handler


def _safe_click_first(page: Any, selectors: tuple[str, ...]) -> bool:
    for selector in selectors:
        loc = page.locator(selector)
        try:
            if loc.count() < 1:
                continue
            loc.first.click()
            return True
        except Exception:
            continue
    return False


def _page_scroll_height(page: Any) -> int | None:
    try:
        return int(page.evaluate("() => document.body.scrollHeight") or 0)
    except Exception:
        return None


def _save_session(page: Any) -> None:
    try:
        page.context.storage_state(path=SESSION_STATE_PATH)
        _log(f"{SOURCE}: saved session -> {SESSION_STATE_PATH}")
    except Exception as exc:
        _log(f"{SOURCE}: warning: session save failed: {exc}")


def _manual_auth_gate(page: Any) -> None:
    page.goto(SCHOLARSHIP_SEARCH_URL, wait_until="domcontentloaded")
    page.wait_for_timeout(1500)
    _log(f"{SOURCE}: page loaded — sign in manually in Chromium, then Enter in this terminal.")

    selectors = (
        'button:has-text("Accept")',
        'button:has-text("I agree")',
        'button:has-text("Got it")',
        '[aria-label*="accept" i]',
    )
    if _safe_click_first(page, selectors):
        page.wait_for_timeout(800)

    _log(f"{SOURCE}: LOG IN MANUALLY in the Chromium window until you see scholarship search/results.")
    if sys.stdin is not None and sys.stdin.isatty():
        _log(f'{SOURCE}: when ready, switch to THIS terminal and press Enter to start capture + scrolling...')
        try:
            input()
        except EOFError:
            pass
    else:
        _log(f"{SOURCE}: non-TTY stdin; waiting up to {APPILY_AUTH_WAIT_SECONDS}s for manual login...")
        page.wait_for_timeout(APPILY_AUTH_WAIT_SECONDS * 1000)

    _save_session(page)


def _dom_fallback_collect(page: Any, state: _CaptureState) -> None:
    if not APPILY_DOM_FALLBACK:
        return
    try:
        rows: list[dict[str, str]] = page.evaluate(
            """() => {
              const out = [];
              const seen = new Set();
              document.querySelectorAll('a[href*="scholarship"]').forEach(a => {
                const href = (a.href || '').split('#')[0];
                const t = (a.innerText || '').trim().replace(/\\s+/g, ' ').slice(0, 280);
                if (!href.includes('appily.com') || t.length < 4) return;
                const key = href + '|' + t;
                if (seen.has(key)) return;
                seen.add(key);
                out.push({ url: href, title: t });
              });
              return out;
            }"""
        )
    except Exception as exc:
        _log(f"{SOURCE}: DOM fallback skipped: {exc}")
        return

    for row in rows or []:
        url = row.get("url") or ""
        title = row.get("title") or ""
        if not url or not title:
            continue
        item = {"title": title, "url": url, "link": url, "description": title}
        ident = _identity(item, "dom_fallback")
        if ident in state.identities:
            continue
        state.identities.add(ident)
        state.captured.append((item, SCHOLARSHIP_SEARCH_URL))
        state.candidate_items_seen += 1
    _log(f"{SOURCE}: DOM fallback queued {len(rows or [])} link(s)")


def _scroll_collect(page: Any, state: _CaptureState) -> None:
    _log(f"{SOURCE}: search page scrolling (steps up to {APPILY_SCROLL_STEPS})...")
    try:
        current = (page.url or "").lower()
    except Exception:
        current = ""
    if SCHOLARSHIP_SEARCH_URL.lower() not in current:
        page.goto(SCHOLARSHIP_SEARCH_URL, wait_until="domcontentloaded")
    page.wait_for_timeout(2500)

    no_new = 0
    before_total = len(state.captured)
    for step in range(1, APPILY_SCROLL_STEPS + 1):
        before = state.candidate_items_seen
        h0 = _page_scroll_height(page)
        _safe_click_first(
            page,
            (
                'button:has-text("Load more")',
                'button:has-text("Show more")',
                'button:has-text("See more")',
                '[data-testid*="load" i]',
            ),
        )
        try:
            page.mouse.wheel(0, 6000)
        except Exception:
            pass
        page.wait_for_timeout(APPILY_SCROLL_WAIT_MS)
        delta = state.candidate_items_seen - before
        h1 = _page_scroll_height(page)
        grows = h0 is not None and h1 is not None and h1 > h0
        _log(f"{SOURCE}: scroll {step}/{APPILY_SCROLL_STEPS} new_json={delta} total={state.candidate_items_seen} height_grows={grows}")
        if delta > 0 or grows:
            no_new = 0
        else:
            no_new += 1
        if no_new >= APPILY_NO_NEW_ROUNDS_STOP:
            _log(f"{SOURCE}: stopping scroll after {no_new} idle rounds")
            break

    if len(state.captured) <= before_total and APPILY_DOM_FALLBACK:
        _dom_fallback_collect(page, state)


def run() -> None:
    if not APPILY_ENABLED:
        _log(f"{SOURCE}: OFF (set APPILY_ENABLED=1 to run)")
        return

    _log(f"{SOURCE}: loading Playwright (first run may be slow)...")
    from playwright.sync_api import sync_playwright

    use_skip = SKIP_EXISTING_ON_LIST and DISCOVERY_MODE == "new_only"
    idx = KnownScholarshipIndex()
    if use_skip:
        try:
            idx = load_known_scholarship_index(get_client(), SOURCE)
            _log(f"{SOURCE}: known index urls={len(idx.urls)} ids={len(idx.source_ids)} titles={len(idx.titles_norm)}")
        except Exception as exc:
            _log(f"{SOURCE}: known index warning: {exc}")

    stats: dict[str, int] = {
        "captured": 0,
        "known_skipped": 0,
        "skip_no_funding": 0,
        "skip_deadline": 0,
        "skip_map": 0,
        "upsert_ok": 0,
        "upsert_failed": 0,
    }
    effective_target = (
        min(TARGET_NEW_ITEMS, APPILY_MAX_RECORDS_DEBUG)
        if APPILY_MAX_RECORDS_DEBUG > 0
        else TARGET_NEW_ITEMS
    )
    seen_session: set[str] = set()
    cap = _CaptureState()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=APPILY_HEADLESS)
        kw: dict[str, Any] = {}
        if os.path.isfile(SESSION_STATE_PATH):
            kw["storage_state"] = SESSION_STATE_PATH
            _log(f"{SOURCE}: loading saved session -> {SESSION_STATE_PATH}")
        context = browser.new_context(**kw)
        page = context.new_page()
        page.set_default_timeout(APPILY_TIMEOUT_MS)
        page.on("response", _response_handler_factory(cap))

        try:
            if APPILY_REQUIRE_MANUAL_AUTH:
                _manual_auth_gate(page)
            else:
                page.goto(SCHOLARSHIP_SEARCH_URL, wait_until="domcontentloaded")
                page.wait_for_timeout(1800)
            _scroll_collect(page, cap)

            if not cap.captured:
                _log(f"{SOURCE}: no records yet; extra wait 15s for late JSON...")
                page.wait_for_timeout(15_000)
                _dom_fallback_collect(page, cap)

            if not cap.captured:
                raise RuntimeError(
                    f"No scholarship data captured from Appily after manual login and scroll — "
                    f"toggle APPILY_DOM_FALLBACK=1, increase APPILY_SCROLL_STEPS, "
                    f"or record Network responses and extend URL/json heuristics in sources/appily/parser.py"
                )

            _log(f"{SOURCE}: payloads json_responses_seen={cap.json_seen} candidates={cap.candidate_items_seen}")

            total = len(cap.captured)
            for i, (item, response_url) in enumerate(cap.captured, start=1):
                stats["captured"] += 1
                rec = _build_record(item, response_url)
                if not rec:
                    stats["skip_map"] += 1
                    continue

                rid = "|".join([str(rec.get("source_id") or ""), str(rec.get("url") or ""), str(rec.get("title") or "")])
                if rid in seen_session:
                    continue
                seen_session.add(rid)

                preview = {
                    "source": SOURCE,
                    "source_id": rec.get("source_id"),
                    "url": rec.get("url"),
                    "title": rec.get("title"),
                }
                if use_skip and listing_is_known(preview, idx, title_fallback=USE_TITLE_FALLBACK_KNOWN):
                    stats["known_skipped"] += 1
                    continue

                if not has_meaningful_funding(rec):
                    stats["skip_no_funding"] += 1
                    _log(f"{SOURCE}: skip no funding: {rec.get('title')}")
                    continue
                dbiz = classify_business_deadline(rec.get("deadline_date"))
                if dbiz != "ok":
                    stats["skip_deadline"] += 1
                    _log(f"{SOURCE}: skip deadline {dbiz}: {rec.get('title')}")
                    continue

                unknown = set(rec) - set(SCHOLARSHIP_UPSERT_BODY_KEYS) - {"id"}
                if unknown:
                    raise ValueError(f"unknown keys: {sorted(unknown)}")
                try:
                    upsert_scholarship(rec)
                    stats["upsert_ok"] += 1
                    _log(f"{SOURCE}: upsert OK #{stats['upsert_ok']}: {rec.get('title')}")
                except Exception as exc:
                    stats["upsert_failed"] += 1
                    _log(f"{SOURCE}: upsert failed: {exc}")

                if effective_target > 0 and stats["upsert_ok"] >= effective_target:
                    _log(f"{SOURCE}: TARGET_NEW_ITEMS reached ({effective_target})")
                    break
                if i == 1 or i % 40 == 0 or i == total:
                    _log(f"{SOURCE}: progress {i}/{total} upserts={stats['upsert_ok']}")

        finally:
            try:
                _save_session(page)
            except Exception:
                pass
            browser.close()

    _log(f"{SOURCE}: done {stats}")


if __name__ == "__main__":
    run()
