"""Mastersportal scholarship parser -> public.scholarships (Supabase).

Opens a visible browser, waits for the operator to finish login manually, then
crawls the Mastersportal scholarship search similarly to the Scholarships.com
pipeline: listing cards first, detail pages second, then shared filters/upsert.
"""

from __future__ import annotations

import hashlib
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

_PARSER_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PARSER_ROOT not in sys.path:
    sys.path.insert(0, _PARSER_ROOT)

from business_filters import MIN_LEAD_DAYS_BEFORE_DEADLINE, classify_business_deadline, has_meaningful_funding
from config import get_global_config
from normalize_scholarship import apply_normalization
from scholarship_db_columns import SCHOLARSHIP_RECORD_DEFAULT_KEYS, SCHOLARSHIP_UPSERT_BODY_KEYS
from sources.scholarship_america.parser import parse_award_min_max
from utils import KnownScholarshipIndex, get_client, listing_is_known, load_known_scholarship_index, upsert_scholarship

SOURCE = "mastersportal"
SITE_ORIGIN = "https://www.mastersportal.com"
SEARCH_URL = "https://www.mastersportal.com/search/scholarships/master"
SESSION_STATE_PATH = os.path.join(_PARSER_ROOT, "mastersportal_session.json")
USER_DATA_DIR = os.path.join(_PARSER_ROOT, ".mastersportal_browser_profile")
DEFAULT_CURRENCY = "EUR"

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


MASTERSPORTAL_HEADLESS = _get_bool_env("MASTERSPORTAL_HEADLESS", False)
MASTERSPORTAL_TIMEOUT_MS = max(30_000, _get_int_env("MASTERSPORTAL_TIMEOUT_MS", 120_000))
MASTERSPORTAL_AUTH_WAIT_SECONDS = max(30, _get_int_env("MASTERSPORTAL_AUTH_WAIT_SECONDS", 900))
MASTERSPORTAL_DETAIL_FETCH = _get_bool_env("MASTERSPORTAL_DETAIL_FETCH", False)
MASTERSPORTAL_KEEP_BROWSER_OPEN = _get_bool_env("MASTERSPORTAL_KEEP_BROWSER_OPEN", True)
MASTERSPORTAL_MAX_RECORDS_DEBUG = max(0, _get_int_env("MASTERSPORTAL_MAX_RECORDS_DEBUG", 0))
MASTERSPORTAL_DETAIL_DELAY_MS = max(0, _get_int_env("MASTERSPORTAL_DETAIL_DELAY_MS", 10_000))
MASTERSPORTAL_LISTING_DELAY_MS = max(0, _get_int_env("MASTERSPORTAL_LISTING_DELAY_MS", 2500))
MASTERSPORTAL_USE_PERSISTENT_PROFILE = _get_bool_env("MASTERSPORTAL_USE_PERSISTENT_PROFILE", True)
MASTERSPORTAL_BROWSER_CHANNEL = (os.getenv("MASTERSPORTAL_BROWSER_CHANNEL") or "chrome").strip()
MASTERSPORTAL_DIRECT_PAGE_GOTO = _get_bool_env("MASTERSPORTAL_DIRECT_PAGE_GOTO", False)


class MastersportalBlockedError(RuntimeError):
    pass


def _log(message: str) -> None:
    print(message, flush=True)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()
    return text or None


def _abs_url(value: Any) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    return urljoin(SITE_ORIGIN, text)


def _listing_url(page_number: int) -> str:
    if page_number <= 1:
        return SEARCH_URL
    parsed = urlparse(SEARCH_URL)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["page"] = str(page_number)
    return urlunparse(parsed._replace(query=urlencode(query)))


def _source_id_from_url(url: str) -> str:
    m = re.search(r"/scholarships/(\d+)/", url)
    if m:
        return m.group(1)
    return hashlib.md5(url.encode("utf-8")).hexdigest()[:16]


def _parse_deadline_date(deadline_text: str | None) -> str | None:
    text = _clean_text(deadline_text)
    if not text or re.search(r"not\s+specified", text, re.I):
        return None
    for fmt in ("%d %b %Y", "%d %B %Y", "%B %d, %Y", "%b %d, %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text[:40], fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _save_session_state(context: Any) -> None:
    try:
        context.storage_state(path=SESSION_STATE_PATH)
        _log(f"{SOURCE}: saved session state -> {SESSION_STATE_PATH}")
    except Exception as exc:
        _log(f"{SOURCE}: warning: could not save session state ({exc})")


def _is_blocked_page(page: Any) -> bool:
    parts: list[str] = []
    for getter in (
        lambda: page.url,
        lambda: page.title(),
        lambda: page.inner_text("body", timeout=1500),
    ):
        try:
            parts.append(str(getter() or ""))
        except Exception:
            pass
    blob = "\n".join(parts).lower()
    return any(
        marker in blob
        for marker in (
            "sorry, you have been blocked",
            "you are unable to access mastersportal.com",
            "cloudflare ray id",
            "access denied",
        )
    )


def _raise_if_blocked(page: Any, phase: str) -> None:
    if _is_blocked_page(page):
        raise MastersportalBlockedError(
            f"{SOURCE}: blocked by Mastersportal/Cloudflare during {phase}; "
            "browser will stay open; solve/check it manually, then stop and rerun."
        )


def _manual_auth_gate(context: Any, page: Any) -> None:
    page.goto(SEARCH_URL, wait_until="domcontentloaded")
    page.wait_for_timeout(1500)
    _raise_if_blocked(page, "manual auth open")

    _log("")
    _log("=" * 72)
    _log(f"{SOURCE}: browser is open and parser is PAUSED.")
    _log(f"{SOURCE}: finish login/close popups manually in the browser.")
    _log(f"{SOURCE}: ONLY AFTER THAT press Enter in this terminal to start parsing.")
    _log("=" * 72)
    try:
        input()
    except EOFError:
        _log(
            f"{SOURCE}: stdin is not interactive; waiting "
            f"{MASTERSPORTAL_AUTH_WAIT_SECONDS}s before parsing"
        )
        time.sleep(MASTERSPORTAL_AUTH_WAIT_SECONDS)
    _raise_if_blocked(page, "manual auth confirm")
    _save_session_state(context)


def _extract_listing_cards(page: Any) -> list[dict[str, Any]]:
    cards = page.locator("a.ScholarshipCard")
    try:
        count = cards.count()
    except Exception:
        count = 0
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for i in range(count):
        card = cards.nth(i)
        try:
            data = card.evaluate(
                """(el) => {
                    const q = (sel) => {
                        const node = el.querySelector(sel);
                        return node ? (node.innerText || node.textContent || '').trim() : '';
                    };
                    const money = el.querySelector('[data-currency][data-amount]');
                    const href = el.getAttribute('href') || '';
                    return {
                        href,
                        title: el.getAttribute('title') || q('.ScholarshipName'),
                        providerName: q('.ProviderDetails .Name'),
                        providerType: q('.ScholarshipProvider'),
                        amountText: money
                            ? `${money.getAttribute('data-amount') || ''} ${money.getAttribute('data-currency') || ''}`.trim()
                            : q('.ScholarshipCardQuickFacts .QFValue'),
                        amountDisplay: q('.ScholarshipCardQuickFacts .QFValue'),
                        currency: money ? (money.getAttribute('data-currency') || '') : '',
                        deadlineText: (() => {
                            const facts = Array.from(el.querySelectorAll('.ScholarshipCardQuickFacts > div'));
                            for (const fact of facts) {
                                const label = fact.querySelector('.QFLabel');
                                if (label && /deadline/i.test(label.innerText || label.textContent || '')) {
                                    const val = fact.querySelector('.QFValue');
                                    return val ? (val.innerText || val.textContent || '').trim() : '';
                                }
                            }
                            return '';
                        })(),
                        basis: q('.ApplicationBasis'),
                        locationText: q('.ProviderDetails .Location'),
                        text: (el.innerText || el.textContent || '').trim(),
                    };
                }"""
            )
        except Exception as exc:
            _log(f"{SOURCE}: card extract failed idx={i}: {exc}")
            continue
        url = _abs_url(data.get("href"))
        title = _clean_text(data.get("title"))
        if not url or not title or url in seen:
            continue
        seen.add(url)
        out.append(
            {
                "source_id": _source_id_from_url(url),
                "url": url,
                "title": title,
                "provider_name": _clean_text(data.get("providerName")),
                "provider_type": _clean_text(data.get("providerType")),
                "award_amount_text": _clean_text(data.get("amountText") or data.get("amountDisplay")),
                "award_amount_display": _clean_text(data.get("amountDisplay")),
                "currency": (_clean_text(data.get("currency")) or DEFAULT_CURRENCY).upper(),
                "deadline_text": _clean_text(data.get("deadlineText")),
                "basis": _clean_text(data.get("basis")),
                "location_text": _clean_text(data.get("locationText")),
                "card_text": _clean_text(data.get("text")),
            }
        )
    return out


def _page_position_text(page: Any) -> str | None:
    try:
        body = page.inner_text("body", timeout=2500)
    except Exception:
        return None
    match = re.search(r"\b(\d{1,5})\s+of\s+(\d{1,5})\b", body, re.I)
    if not match:
        return None
    return f"{match.group(1)} of {match.group(2)}"


def _extract_about_from_detail_text(text: str) -> str | None:
    marker = "About"
    if marker not in text:
        return _clean_text(text[:3000])
    rest = text.split(marker, 1)[1]
    for stop in ("Eligible Programmes", "Scholarship Details", "Master Programmes", "Our partners"):
        if stop in rest:
            rest = rest.split(stop, 1)[0]
            break
    return _clean_text(rest[:5000])


def _fetch_detail(detail_page: Any, url: str) -> dict[str, Any]:
    if MASTERSPORTAL_DETAIL_DELAY_MS:
        detail_page.wait_for_timeout(MASTERSPORTAL_DETAIL_DELAY_MS)
    detail_page.goto(url, wait_until="domcontentloaded")
    detail_page.wait_for_timeout(1200)
    _raise_if_blocked(detail_page, f"detail fetch {url}")
    try:
        text = detail_page.inner_text("body", timeout=7000)
    except Exception:
        text = ""
    try:
        title = detail_page.title()
    except Exception:
        title = ""
    try:
        html = detail_page.content()
    except Exception:
        html = ""
    return {
        "page_title": _clean_text(title),
        "body_text": text,
        "about": _extract_about_from_detail_text(text),
        "full_content_html": html[:150_000] if html else None,
    }


def _build_record(list_data: dict[str, Any], detail: dict[str, Any] | None) -> dict[str, Any]:
    detail = detail or {}
    award_text = list_data.get("award_amount_text")
    amin, amax = parse_award_min_max(award_text)
    deadline_text = list_data.get("deadline_text")
    deadline_date = _parse_deadline_date(deadline_text)
    description = detail.get("about") or list_data.get("card_text") or list_data.get("title")
    eligibility_bits = [
        list_data.get("basis"),
        list_data.get("location_text"),
        detail.get("about"),
    ]
    eligibility_text = "\n".join(str(x) for x in eligibility_bits if x)
    provider_name = list_data.get("provider_name") or "Mastersportal"
    tags = ["mastersportal", "masters"]
    if list_data.get("basis"):
        tags.append(str(list_data["basis"]).strip().lower())

    record: dict[str, Any] = {
        "source": SOURCE,
        "source_id": list_data.get("source_id"),
        "url": list_data.get("url"),
        "title": list_data.get("title"),
        "provider_name": provider_name,
        "award_amount_text": award_text,
        "award_amount_min": amin,
        "award_amount_max": amax,
        "currency": list_data.get("currency") or DEFAULT_CURRENCY,
        "deadline_text": deadline_text,
        "deadline_date": deadline_date,
        "description": description,
        "eligibility_text": eligibility_text or description,
        "requirements_text": eligibility_text or description,
        "apply_url": list_data.get("url"),
        "apply_button_text": "View on Mastersportal",
        "mark_started_available": True,
        "mark_submitted_available": True,
        "status_text": "Open",
        "official_source_name": "Mastersportal",
        "tags": tags[:20],
        "is_active": True,
        "is_recurring": False,
        "full_content_html": detail.get("full_content_html"),
        "raw_data": {
            "captured_at": _now_iso(),
            "listing": list_data,
            "detail_page_title": detail.get("page_title"),
            "detail_body_preview": (detail.get("body_text") or "")[:20_000],
        },
    }
    apply_normalization(record)
    for key in SCHOLARSHIP_RECORD_DEFAULT_KEYS:
        if key not in record:
            record[key] = None
    record["source"] = SOURCE
    record["is_active"] = True
    return record


def _click_next(page: Any) -> bool:
    try:
        page.evaluate("() => window.scrollTo(0, document.body ? document.body.scrollHeight : 0)")
        page.wait_for_timeout(1200)
    except Exception:
        pass
    candidates = (
        page.get_by_role("link", name=re.compile(r"^\s*Next\s*$", re.I)),
        page.get_by_role("button", name=re.compile(r"^\s*Next\s*$", re.I)),
        page.get_by_label(re.compile(r"\bnext\b", re.I)),
        page.locator('a:has-text("Next")'),
        page.locator('button:has-text("Next")'),
        page.locator('[aria-label*="Next" i]'),
        page.locator('[class*="next" i]'),
    )
    for locator in candidates:
        try:
            if locator.count() < 1:
                continue
            _log(f"{SOURCE}: clicking visible Next control")
            locator.first.scroll_into_view_if_needed(timeout=2000)
            page.wait_for_timeout(500)
            locator.first.click(timeout=5000)
            page.wait_for_load_state("domcontentloaded", timeout=20_000)
            page.wait_for_timeout(2500)
            return True
        except Exception:
            continue
    try:
        clicked = bool(
            page.evaluate(
                """() => {
                    const els = Array.from(document.querySelectorAll('a,button,[role="button"]'));
                    const target = els.find((el) => {
                        const text = (el.innerText || el.textContent || '').trim();
                        const aria = (el.getAttribute('aria-label') || '').trim();
                        return /^next$/i.test(text) || /next/i.test(aria);
                    });
                    if (!target) return false;
                    target.scrollIntoView({block: 'center', inline: 'center'});
                    target.click();
                    return true;
                }"""
            )
        )
        if clicked:
            _log(f"{SOURCE}: clicked Next via DOM fallback")
            page.wait_for_load_state("domcontentloaded", timeout=20_000)
            page.wait_for_timeout(2500)
            return True
    except Exception:
        pass
    return False


def _ensure_first_listing_page(page: Any) -> None:
    current = ""
    try:
        current = str(page.url or "")
    except Exception:
        current = ""
    if "/search/scholarships/master" in current and "page=" not in current:
        _log(f"{SOURCE}: using current listing page after manual gate")
        page.wait_for_timeout(1200)
        _raise_if_blocked(page, "listing page 1")
        return
    _log(f"{SOURCE}: opening listing page 1: {SEARCH_URL}")
    page.goto(SEARCH_URL, wait_until="domcontentloaded")
    page.wait_for_timeout(2500)
    _raise_if_blocked(page, "listing page 1")


def _advance_listing_page(page: Any, page_idx: int) -> bool:
    if MASTERSPORTAL_LISTING_DELAY_MS:
        _log(f"{SOURCE}: waiting {MASTERSPORTAL_LISTING_DELAY_MS}ms before next listing page")
        page.wait_for_timeout(MASTERSPORTAL_LISTING_DELAY_MS)
    _log(f"{SOURCE}: advancing to listing page {page_idx} via Next")
    if _click_next(page):
        _raise_if_blocked(page, f"listing page {page_idx}")
        return True
    if not MASTERSPORTAL_DIRECT_PAGE_GOTO:
        _log(f"{SOURCE}: Next control not found; direct page goto disabled")
        return False
    url = _listing_url(page_idx)
    _log(f"{SOURCE}: Next not found; direct fallback to {url}")
    page.goto(url, wait_until="domcontentloaded")
    page.wait_for_timeout(2500)
    _raise_if_blocked(page, f"listing page {page_idx}")
    return True


def _new_browser_context(pw: Any) -> Any:
    context_kwargs: dict[str, Any] = {
        "viewport": {"width": 1365, "height": 900},
        "locale": "en-US",
        "timezone_id": "Asia/Almaty",
        "user_agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    }
    launch_kwargs: dict[str, Any] = {
        "headless": MASTERSPORTAL_HEADLESS,
        "args": [
            "--disable-blink-features=AutomationControlled",
            "--start-maximized",
        ],
    }
    if MASTERSPORTAL_BROWSER_CHANNEL:
        launch_kwargs["channel"] = MASTERSPORTAL_BROWSER_CHANNEL

    if MASTERSPORTAL_USE_PERSISTENT_PROFILE:
        os.makedirs(USER_DATA_DIR, exist_ok=True)
        try:
            _log(
                f"{SOURCE}: launching persistent browser profile -> {USER_DATA_DIR} "
                f"(channel={MASTERSPORTAL_BROWSER_CHANNEL or 'default'})"
            )
            return pw.chromium.launch_persistent_context(
                USER_DATA_DIR,
                **launch_kwargs,
                **context_kwargs,
            )
        except Exception as exc:
            _log(f"{SOURCE}: persistent Chrome launch failed ({exc}); falling back to bundled Chromium")
            launch_kwargs.pop("channel", None)
            return pw.chromium.launch_persistent_context(
                USER_DATA_DIR,
                **launch_kwargs,
                **context_kwargs,
            )

    try:
        browser = pw.chromium.launch(**launch_kwargs)
    except Exception as exc:
        _log(f"{SOURCE}: Chrome launch failed ({exc}); falling back to bundled Chromium")
        launch_kwargs.pop("channel", None)
        browser = pw.chromium.launch(**launch_kwargs)
    context = browser.new_context(**context_kwargs)
    if os.path.exists(SESSION_STATE_PATH):
        _log(f"{SOURCE}: storage_state is ignored in non-persistent fallback; using profile is preferred")
    return context


def run() -> None:
    from playwright.sync_api import sync_playwright

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
    max_pages = max(1, MAX_LIST_PAGES)
    if MASTERSPORTAL_MAX_RECORDS_DEBUG > 0:
        max_pages = min(max_pages, 3)

    with sync_playwright() as pw:
        context = _new_browser_context(pw)
        page = context.new_page()
        page.set_default_timeout(MASTERSPORTAL_TIMEOUT_MS)
        detail_page = context.new_page() if MASTERSPORTAL_DETAIL_FETCH else None
        try:
            _manual_auth_gate(context, page)
            no_new_pages = 0
            seen_urls: set[str] = set()
            for page_idx in range(1, max_pages + 1):
                if page_idx == 1:
                    _ensure_first_listing_page(page)
                elif not _advance_listing_page(page, page_idx):
                    break
                cards = _extract_listing_cards(page)
                pos = _page_position_text(page)
                _log(
                    f"{SOURCE}: listing page {page_idx}/{max_pages}: "
                    f"cards={len(cards)} position={pos or 'unknown'}"
                )
                new_on_page = 0
                for list_data in cards:
                    url = str(list_data.get("url") or "")
                    if not url or url in seen_urls:
                        continue
                    seen_urls.add(url)
                    new_on_page += 1
                    stats["listing_seen"] += 1
                    preview = {
                        "source": SOURCE,
                        "source_id": list_data.get("source_id"),
                        "url": url,
                        "title": list_data.get("title"),
                    }
                    if SKIP_EXISTING_ON_LIST and listing_is_known(preview, idx, title_fallback=USE_TITLE_FALLBACK_KNOWN):
                        stats["known_skipped"] += 1
                        continue
                    detail = _fetch_detail(detail_page, url) if detail_page is not None else {}
                    record = _build_record(list_data, detail)
                    if not has_meaningful_funding(record):
                        stats["skip_no_funding"] += 1
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
                        _log(f"{SOURCE}: upsert OK #{stats['upsert_ok']}: {record.get('title')}")
                    except Exception as exc:
                        stats["upsert_failed"] += 1
                        _log(f"{SOURCE}: upsert failed for {record.get('title')!r}: {exc}")
                    if TARGET_NEW_ITEMS > 0 and stats["upsert_ok"] >= TARGET_NEW_ITEMS:
                        _log(f"{SOURCE}: reached TARGET_NEW_ITEMS={TARGET_NEW_ITEMS}")
                        return
                    if MASTERSPORTAL_MAX_RECORDS_DEBUG > 0 and stats["listing_seen"] >= MASTERSPORTAL_MAX_RECORDS_DEBUG:
                        _log(f"{SOURCE}: reached debug record cap={MASTERSPORTAL_MAX_RECORDS_DEBUG}")
                        return
                if new_on_page <= 0:
                    no_new_pages += 1
                else:
                    no_new_pages = 0
                if NO_NEW_PAGES_STOP > 0 and no_new_pages >= NO_NEW_PAGES_STOP:
                    _log(f"{SOURCE}: stopping after {no_new_pages} no-new pages")
                    break
                if len(cards) <= 0:
                    _log(f"{SOURCE}: no cards on listing page {page_idx}; stopping")
                    break
                if pos:
                    m_pos = re.match(r"(\d+)\s+of\s+(\d+)", pos)
                    if m_pos and int(m_pos.group(1)) >= int(m_pos.group(2)):
                        _log(f"{SOURCE}: reached last listing page ({pos})")
                        break
        finally:
            _log(
                f"{SOURCE}: done listing_seen={stats['listing_seen']} known_skipped={stats['known_skipped']} "
                f"skip_no_funding={stats['skip_no_funding']} skip_deadline={stats['skip_deadline']} "
                f"upsert_ok={stats['upsert_ok']} upsert_failed={stats['upsert_failed']}"
            )
            _save_session_state(context)
            if MASTERSPORTAL_KEEP_BROWSER_OPEN:
                _log(f"{SOURCE}: keeping browser open; press Ctrl+C to close")
                try:
                    while True:
                        page.wait_for_timeout(60_000)
                except Exception:
                    pass
            context.close()


if __name__ == "__main__":
    run()
