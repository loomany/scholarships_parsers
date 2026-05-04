"""
Unigo.com — листинги /scholarships/* через Playwright (Cloudflare), детальные страницы → Supabase.

Паттерн близок к Scholarships.com: браузер для сессии; дальше HTML + общий upsert.
AJAX с action=get_notifications — не каталог; при UNIGO_LOG_AJAX_ACTIONS логируем другие action.
"""

from __future__ import annotations

import hashlib
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, unquote_plus, urlparse, urlunparse

_PARSER_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PARSER_ROOT not in sys.path:
    sys.path.insert(0, _PARSER_ROOT)

from dotenv import load_dotenv

load_dotenv(os.path.join(_PARSER_ROOT, ".env"))
load_dotenv(os.path.join(os.path.dirname(_PARSER_ROOT), ".env"))

from bs4 import BeautifulSoup, Tag

from ai_monitoring import print_ai_session_summary, record_ai_skip, snapshot_ai_usage
from business_filters import (
    MIN_LEAD_DAYS_BEFORE_DEADLINE,
    classify_business_deadline,
    has_meaningful_funding,
)
from config import get_global_config
from normalize_scholarship import apply_normalization
from scholarship_db_columns import SCHOLARSHIP_RECORD_DEFAULT_KEYS
from sources.scholarship_america.parser import parse_award_min_max, parse_deadline_date
from utils import (
    KnownScholarshipIndex,
    get_client,
    listing_is_known,
    load_known_scholarship_index,
    upsert_scholarship,
)

SOURCE = "unigo"
SITE_ORIGIN = "https://www.unigo.com"
DEFAULT_CURRENCY = "USD"
SESSION_STATE_PATH = os.path.join(_PARSER_ROOT, "unigo_session.json")


def _get_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def _get_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


UNIGO_ENABLED = _get_bool("UNIGO_ENABLED", False)
UNIGO_HEADLESS = _get_bool("UNIGO_HEADLESS", False)
UNIGO_REQUIRE_MANUAL_START = _get_bool("UNIGO_REQUIRE_MANUAL_START", False)
UNIGO_LOG_AJAX_ACTIONS = _get_bool("UNIGO_LOG_AJAX_ACTIONS", False)
UNIGO_DEBUG_LINKS = _get_bool("UNIGO_DEBUG_LINKS", False)
# Обход Cloudflare: системный Chrome + постоянный профиль (cookies / cf_clearance между запусками)
UNIGO_USE_PERSISTENT_PROFILE = _get_bool("UNIGO_USE_PERSISTENT_PROFILE", True)
UNIGO_PLAYWRIGHT_CHANNEL = (os.getenv("UNIGO_PLAYWRIGHT_CHANNEL") or "chrome").strip()
UNIGO_BROWSER_PROFILE_DIR = (
    os.getenv("UNIGO_BROWSER_PROFILE_DIR") or ""
).strip() or os.path.join(_PARSER_ROOT, ".unigo_browser_profile")
UNIGO_SAVE_STORAGE_STATE_PATH = (
    os.getenv("UNIGO_SAVE_STORAGE_STATE_PATH") or SESSION_STATE_PATH
).strip()
UNIGO_CDP_URL = (os.getenv("UNIGO_CDP_URL") or "").strip()
UNIGO_CDP_USE_FIRST_TAB = _get_bool("UNIGO_CDP_USE_FIRST_TAB", True)

DESKTOP_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

_gc = get_global_config()
TARGET_NEW_ITEMS = _gc.target_new_items
SKIP_EXISTING_ON_LIST = _gc.skip_existing_on_list
USE_TITLE_FALLBACK_KNOWN = _gc.use_title_fallback_known
DISCOVERY_MODE = _gc.discovery_mode

SCROLL_ROUNDS = max(1, min(30, _get_int("UNIGO_SCROLL_ROUNDS", 4)))
SCROLL_WAIT_MS = max(200, min(60000, _get_int("UNIGO_SCROLL_WAIT_MS", 1200)))
NAV_DELAY_MS = max(0, min(15000, _get_int("UNIGO_NAV_DELAY_MS", 800)))
TIMEOUT_MS = max(15000, min(240000, _get_int("UNIGO_TIMEOUT_MS", 90000)))
# UNIGO_MAX_DISCOVERED: >0 верхний предел очереди детальных URL (max 20000); <=0 без лимита.
_md_raw = _get_int("UNIGO_MAX_DISCOVERED", 800)
MAX_DISCOVERED_LIMIT: int | None
if _md_raw <= 0:
    MAX_DISCOVERED_LIMIT = None
else:
    MAX_DISCOVERED_LIMIT = max(10, min(20000, _md_raw))
UNIGO_BY_MAJOR_EXPAND = _get_bool("UNIGO_BY_MAJOR_EXPAND", True)
# После hub /scholarships/by-major: сколько листингов …/by-major/<slug>; <=0 без лимита (все найденные).
_bmc_raw = _get_int("UNIGO_BY_MAJOR_MAX_CATEGORIES", 120)
BY_MAJOR_CATEGORY_LIMIT: int | None
if _bmc_raw <= 0:
    BY_MAJOR_CATEGORY_LIMIT = None
else:
    BY_MAJOR_CATEGORY_LIMIT = max(1, min(500, _bmc_raw))


def _discovery_queue_full(n: int) -> bool:
    return MAX_DISCOVERED_LIMIT is not None and n >= MAX_DISCOVERED_LIMIT


def _by_major_cat_full(n: int) -> bool:
    return BY_MAJOR_CATEGORY_LIMIT is not None and n >= BY_MAJOR_CATEGORY_LIMIT

_DEFAULT_SEEDS: tuple[str, ...] = (
    f"{SITE_ORIGIN}/scholarships",
    f"{SITE_ORIGIN}/scholarships/by-major",
    f"{SITE_ORIGIN}/scholarships/weird/scholarships-for-redheads",
    f"{SITE_ORIGIN}/scholarships/high-school-students",
    f"{SITE_ORIGIN}/scholarships/undergraduate-students",
    f"{SITE_ORIGIN}/scholarships/graduate-students",
    f"{SITE_ORIGIN}/scholarships/grants-for-college",
)


def _log(msg: str) -> None:
    print(msg, flush=True)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_path() -> str:
    return (os.getenv("UNIGO_SESSION_PATH") or SESSION_STATE_PATH).strip() or SESSION_STATE_PATH


def _seed_urls() -> list[str]:
    raw = (os.getenv("UNIGO_SEED_URLS") or "").strip()
    if raw:
        return [x.strip() for x in raw.split(",") if x.strip()]
    return list(_DEFAULT_SEEDS)


def _normalize_unigo_url(href: str) -> str | None:
    if not href or href.startswith("#") or href.lower().startswith("javascript:"):
        return None
    p = urlparse(href)
    host = (p.netloc or "").lower()
    hn = host[4:] if host.startswith("www.") else host
    if hn != "unigo.com" and not hn.endswith(".unigo.com"):
        return None
    path = (p.path or "/").rstrip("/") or "/"
    if not path.startswith("/scholarships"):
        return None
    return urlunparse(("https", "www.unigo.com", path, "", "", ""))


def _path_segments(url: str) -> list[str]:
    path = urlparse(url).path.strip("/")
    return [s for s in path.split("/") if s]


def _looks_like_detail_url(url: str) -> bool:
    parts = _path_segments(url)
    if len(parts) < 2 or parts[0] != "scholarships":
        return False
    if len(parts) == 2:
        return False
    # /scholarships/by-major/nursing → листинг по специальности, не карточка гранта.
    if len(parts) == 3 and parts[1] == "by-major":
        return False
    skip_second = {
        "match",
        "by-college",
        "our-scholarships",
    }
    if len(parts) >= 2 and parts[1] in skip_second:
        return True
    last = parts[-1].lower()
    if len(parts) == 3 and last.endswith("-scholarships"):
        return False
    return True


def _is_by_major_hub_url(url: str) -> bool:
    p = urlparse(url).path.rstrip("/") or "/"
    return p == "/scholarships/by-major"


def _looks_like_by_major_category_url(url: str) -> bool:
    parts = _path_segments(url)
    return len(parts) == 3 and parts[0] == "scholarships" and parts[1] == "by-major"


def _collect_major_category_urls_from_dom(page: Any) -> list[str]:
    try:
        raw: list[str] = page.evaluate(
            """() => Array.from(document.querySelectorAll('a[href]'))
                .map(a => a.href)
                .filter(Boolean)"""
        )
    except Exception:
        return []
    out: list[str] = []
    for h in raw:
        n = _normalize_unigo_url(h)
        if n and _looks_like_by_major_category_url(n):
            out.append(n)
    return out


def _source_id_for_url(url: str) -> str:
    canon = url.split("#")[0].rstrip("/")
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()[:16]


def _cloudflare_challenge_html(html: str) -> bool:
    h = html[:8000].lower()
    return "checking your browser" in h or "just a moment" in h or "__cf_chl" in html


def _ajax_action_from_post_data(post_data: str | None) -> str | None:
    if not post_data:
        return None
    try:
        pairs = parse_qsl(post_data, keep_blank_values=True)
    except Exception:
        return None
    for k, v in pairs:
        if k == "action":
            return unquote_plus(v)
    return None


def _is_cloudflare_blocked(page: Any) -> bool:
    try:
        return _cloudflare_challenge_html(page.content())
    except Exception:
        return False


def _settle_network(page: Any) -> None:
    """Дождаться затихания сети (частично помогает с ленивыми блоками после CF)."""
    try:
        page.wait_for_load_state("networkidle", timeout=12000)
    except Exception:
        pass


def _same_page_path(page: Any, target_url: str) -> bool:
    """Только точное совпадение пути (без ?utm), чтобы не пропускать переход /scholarships -> /scholarships/...."""
    try:
        a = urlparse(page.url).path.rstrip("/")
        b = urlparse(target_url).path.rstrip("/")
        return bool(b) and a == b
    except Exception:
        return False


def _safe_goto(page: Any, url: str) -> None:
    """Избежать падения interrupted / ERR_ABORTED (редиректы, вкладка соревнуется с навигацией)."""
    retriable = (
        "interrupted by another navigation",
        "net::err_aborted",
    )
    for attempt in range(1, 4):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
            return
        except Exception as exc:
            low = str(exc).lower()
            if attempt < 3 and any(n in low for n in retriable):
                _log(f"{SOURCE}: goto retry {attempt}/3 ({url[:72]}…) — {low[:80]}")
                page.wait_for_timeout(1500)
                continue
            raise exc


def _scroll_page(page: Any) -> None:
    for _ in range(SCROLL_ROUNDS):
        try:
            page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        except Exception:
            break
        page.wait_for_timeout(SCROLL_WAIT_MS)


def _collect_hrefs_from_dom(page: Any) -> list[str]:
    try:
        raw: list[str] = page.evaluate(
            """() => Array.from(document.querySelectorAll('a[href]'))
                .map(a => a.href)
                .filter(Boolean)"""
        )
    except Exception:
        return []
    if UNIGO_DEBUG_LINKS:
        sch_any = sum(
            1
            for h in raw
            if "unigo.com" in str(h).lower() and "/scholarships" in str(h).lower()
        )
        _log(f"{SOURCE}: DEBUG anchor hrefs total={len(raw)} unigo/scholarships~={sch_any}")
    out: list[str] = []
    for h in raw:
        n = _normalize_unigo_url(h)
        if n and _looks_like_detail_url(n):
            out.append(n)
    if UNIGO_DEBUG_LINKS:
        sample = sorted(set(out))[:15]
        _log(f"{SOURCE}: DEBUG detail-candidate urls={len(set(out))} sample={sample!r}")
    return out


def _meta_content(soup: BeautifulSoup, *, prop: str | None = None, name: str | None = None) -> str | None:
    if prop:
        tag = soup.find("meta", attrs={"property": prop})
    else:
        tag = soup.find("meta", attrs={"name": name})
    if not tag or not isinstance(tag, Tag):
        return None
    c = tag.get("content")
    s = str(c).strip() if c else ""
    return s or None


def _main_text_blob(soup: BeautifulSoup) -> str:
    root = soup.select_one("article") or soup.select_one("main") or soup.body
    if not root:
        return ""
    parts: list[str] = []
    for el in root.find_all(["p", "li", "h2", "h3"]):
        if isinstance(el, Tag):
            t = el.get_text(" ", strip=True)
            if t and len(t) > 2:
                parts.append(t)
    return "\n".join(parts)[:12000]


def _extract_deadline_hint(blob: str) -> str | None:
    for pat in (
        r"(?:Deadline|Due date|Apply by|Applications close)\s*[:\s]+\s*([^\n]+)",
        r"(?:deadline|due)\s+is\s+([^\n]+)",
    ):
        m = re.search(pat, blob, re.I)
        if m:
            line = m.group(1).strip()
            if len(line) > 3:
                return line[:500]
    return None


def _extract_award_hint(blob: str, title: str) -> str | None:
    blob2 = f"{title}\n{blob}"
    m = re.search(r"(?:Award|Amount|Prize|Value)\s*[:\s]+\s*([^\n]+)", blob2, re.I)
    if m:
        return m.group(1).strip()[:500]
    m2 = re.search(r"\$[\d,]+(?:\.\d{2})?(?:\s*[-–]\s*\$[\d,]+(?:\.\d{2})?)?", blob2)
    if m2:
        return m2.group(0).strip()
    return None


def _first_external_apply(soup: BeautifulSoup, page_url: str) -> str | None:
    for a in soup.find_all("a", href=True):
        if not isinstance(a, Tag):
            continue
        href = str(a.get("href") or "").strip()
        if not href.startswith("http"):
            continue
        if "unigo.com" in urlparse(href).netloc.lower():
            continue
        lab = a.get_text(" ", strip=True).lower()
        if any(x in lab for x in ("apply", "learn more", "website", "visit", "more info")) or not lab:
            return href
    return None


def _parse_detail(html: str, url: str) -> dict[str, Any] | None:
    if _cloudflare_challenge_html(html):
        return None
    soup = BeautifulSoup(html, "html.parser")
    title = None
    h1 = soup.find("h1")
    if isinstance(h1, Tag):
        title = h1.get_text(" ", strip=True)
    if not title:
        title = _meta_content(soup, prop="og:title") or _meta_content(soup, name="twitter:title")
    title = (title or "").strip() or None
    if not title or len(title) < 5:
        return None
    low = title.lower()
    if low.startswith("unigo") and "scholarship" in low and len(title) < 40:
        return None

    blob = _main_text_blob(soup)
    meta_desc = _meta_content(soup, name="description") or _meta_content(soup, prop="og:description")
    body = "\n".join(x for x in (meta_desc, blob) if x).strip()
    deadline_text = _extract_deadline_hint(body) or _extract_deadline_hint(meta_desc or "")
    award_text = _extract_award_hint(body, title)
    amin, amax = parse_award_min_max(award_text)
    deadline_date = parse_deadline_date(deadline_text)

    apply_url = _first_external_apply(soup, url) or url
    provider = None
    m = re.search(r"(?:Provider|Sponsor|Organization)\s*[:\s]+\s*([^\n]+)", body, re.I)
    if m:
        provider = m.group(1).strip()[:300]

    full_html = None
    main = soup.select_one("article") or soup.select_one("main")
    if isinstance(main, Tag):
        full_html = str(main)[:150_000]

    return {
        "title": title,
        "provider_name": provider or "Unigo listing",
        "award_amount_text": award_text,
        "award_amount_min": amin,
        "award_amount_max": amax,
        "deadline_text": deadline_text,
        "deadline_date": deadline_date,
        "description": body[:8000] or title,
        "eligibility_text": body[:8000] or title,
        "requirements_text": body[:8000] or title,
        "apply_url": apply_url,
        "full_content_html": full_html,
    }


def _build_record(detail: dict[str, Any], url: str) -> dict[str, Any]:
    record: dict[str, Any] = {
        "source": SOURCE,
        "source_id": _source_id_for_url(url),
        "url": url,
        "title": detail.get("title"),
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
        "apply_url": detail.get("apply_url") or url,
        "mark_started_available": True,
        "mark_submitted_available": True,
        "full_content_html": detail.get("full_content_html"),
        "raw_data": {"detail_url": url, "parser": "unigo_playwright_v1"},
        "is_active": True,
        "is_recurring": False,
    }
    apply_normalization(record)
    for k in SCHOLARSHIP_RECORD_DEFAULT_KEYS:
        if k not in record:
            record[k] = None
    record["source"] = SOURCE
    record["is_active"] = True
    return record


def run() -> None:
    if not UNIGO_ENABLED:
        _log(f"{SOURCE}: disabled (set UNIGO_ENABLED=1)")
        return

    from playwright.sync_api import sync_playwright

    ai_usage_start = snapshot_ai_usage()
    use_skip = (SKIP_EXISTING_ON_LIST and DISCOVERY_MODE == "new_only") or False
    idx: KnownScholarshipIndex
    if use_skip:
        try:
            idx = load_known_scholarship_index(get_client(), SOURCE)
            _log(
                f"{SOURCE}: known index urls={len(idx.urls)} source_ids={len(idx.source_ids)} "
                f"slugs={len(idx.slugs_lc)}"
            )
        except Exception as exc:
            _log(f"{SOURCE}: known index load failed ({exc}); continuing empty")
            idx = KnownScholarshipIndex()
    else:
        idx = KnownScholarshipIndex()

    seeds = _seed_urls()
    session = _session_path()
    discovered: set[str] = set()
    stats = {"known_skipped": 0, "upsert_ok": 0, "upsert_failed": 0, "skip_parse": 0, "skip_funding": 0, "skip_deadline": 0}
    processed = 0

    def on_response(resp: Any) -> None:
        if not UNIGO_LOG_AJAX_ACTIONS:
            return
        try:
            if "admin-ajax.php" not in (resp.url or ""):
                return
            req = resp.request
            if (req.method or "").upper() != "POST":
                return
            action = _ajax_action_from_post_data(req.post_data)
            if action and action != "get_notifications":
                _log(f"{SOURCE}: ajax action={action!r} status={resp.status}")
        except Exception:
            pass

    browser: Any = None
    attached_via_cdp = False
    with sync_playwright() as pw:
        context_kw: dict[str, Any] = {
            "viewport": {"width": 1365, "height": 900},
            "locale": "en-US",
            "user_agent": DESKTOP_USER_AGENT,
        }
        launch_kw: dict[str, Any] = {
            "headless": UNIGO_HEADLESS,
            "ignore_default_args": ["--enable-automation"],
            "args": ["--disable-blink-features=AutomationControlled"],
        }
        channel = UNIGO_PLAYWRIGHT_CHANNEL
        if channel:
            launch_kw["channel"] = channel

        if UNIGO_CDP_URL:
            attached_via_cdp = True
            _log(
                f"{SOURCE}: CDP -> ваш Chrome ({UNIGO_CDP_URL}); окно без баннера "
                "`автоматизированное ПО`, если Chrome запущен вручную с remote-debugging-port."
            )
            try:
                browser = pw.chromium.connect_over_cdp(UNIGO_CDP_URL)
            except Exception as exc:
                _log(
                    f"{SOURCE}: CDP connect failed ({exc}). Частые причины:\n"
                    f"   1) Chrome не запущен с флагом: chrome.exe --remote-debugging-port=9222\n"
                    f"   2) На Windows лучше UNIGO_CDP_URL=http://127.0.0.1:9222 (не localhost).\n"
                    f"   3) Порт занят другим приложением или Chrome закрыли до запуска парсера."
                )
                raise
            ctxs = getattr(browser, "contexts", None) or []
            if not ctxs:
                try:
                    browser.close()
                except Exception:
                    pass
                raise RuntimeError(
                    f"{SOURCE}: нет контекста в Chrome по CDP. Окно Chrome должно быть уже открыто."
                )
            context = ctxs[0]
            if context.pages and UNIGO_CDP_USE_FIRST_TAB:
                page = context.pages[0]
                _log(
                    f"{SOURCE}: берём вашу первую вкладку (сейчас: {page.url}). "
                    "Можно заранее открыть Unigo и пройти Cloudflare, затем Enter в терминале."
                )
            else:
                page = context.new_page()

        elif UNIGO_USE_PERSISTENT_PROFILE:
            os.makedirs(UNIGO_BROWSER_PROFILE_DIR, exist_ok=True)
            _log(
                f"{SOURCE}: persistent profile -> {UNIGO_BROWSER_PROFILE_DIR} "
                f"(channel={channel or 'bundled chromium'}; см. README Cloudflare)"
            )
            try:
                context = pw.chromium.launch_persistent_context(
                    UNIGO_BROWSER_PROFILE_DIR,
                    **launch_kw,
                    **context_kw,
                )
            except Exception as exc:
                _log(f"{SOURCE}: persistent+channel launch failed ({exc}); retry without channel")
                launch_kw.pop("channel", None)
                context = pw.chromium.launch_persistent_context(
                    UNIGO_BROWSER_PROFILE_DIR,
                    **launch_kw,
                    **context_kw,
                )
            page = context.pages[0] if context.pages else context.new_page()
        else:
            try:
                browser = pw.chromium.launch(**launch_kw)
            except Exception as exc:
                _log(f"{SOURCE}: chromium.launch failed ({exc}); retry without channel")
                launch_kw.pop("channel", None)
                browser = pw.chromium.launch(**launch_kw)
            ctx_new: dict[str, Any] = dict(context_kw)
            if os.path.isfile(session):
                ctx_new["storage_state"] = session
                _log(f"{SOURCE}: storage_state={session}")
            context = browser.new_context(**ctx_new)
            page = context.new_page()

        page.set_default_timeout(TIMEOUT_MS)
        page.on("response", on_response)

        try:
            first = seeds[0] if seeds else f"{SITE_ORIGIN}/scholarships"
            _log(f"{SOURCE}: goto {first}")
            if not (_same_page_path(page, first)):
                _safe_goto(page, first)
            _settle_network(page)
            if UNIGO_REQUIRE_MANUAL_START or _is_cloudflare_blocked(page):
                if not UNIGO_REQUIRE_MANUAL_START and _is_cloudflare_blocked(page):
                    _log(
                        f"{SOURCE}: похоже на страницу Cloudflare — включите UNIGO_REQUIRE_MANUAL_START=1 "
                        "или пройдите проверку в открывшемся окне (Chrome + persistent профиль)."
                    )
                _log(
                    f"{SOURCE}: открой/обнови страницу если нужно, пройди Cloudflare-капчу, "
                    "дождись нормального сайта — затем Enter в этом терминале…"
                )
                try:
                    input()
                except EOFError:
                    pass
                if not _same_page_path(page, first):
                    _safe_goto(page, first)
                else:
                    _log(f"{SOURCE}: та же вкладка уже на {urlparse(first).path} — второй goto пропущен")
                _settle_network(page)
                if _is_cloudflare_blocked(page):
                    _log(
                        f"{SOURCE}: после Enter всё ещё Cloudflare — выполните проверку в браузере, "
                        "затем снова Enter…"
                    )
                    try:
                        input()
                    except EOFError:
                        pass
                    if not _same_page_path(page, first):
                        _safe_goto(page, first)
                    _settle_network(page)

            if _get_bool("UNIGO_SAVE_STORAGE_STATE", False):
                try:
                    context.storage_state(path=UNIGO_SAVE_STORAGE_STATE_PATH)
                    _log(f"{SOURCE}: saved storage_state -> {UNIGO_SAVE_STORAGE_STATE_PATH}")
                except Exception as exc:
                    _log(f"{SOURCE}: UNIGO_SAVE_STORAGE_STATE не удалось: {exc}")

            dc_cap = MAX_DISCOVERED_LIMIT if MAX_DISCOVERED_LIMIT is not None else "∞"
            mc_cap = BY_MAJOR_CATEGORY_LIMIT if BY_MAJOR_CATEGORY_LIMIT is not None else "∞"
            _log(
                f"{SOURCE}: run caps TARGET_NEW_ITEMS={TARGET_NEW_ITEMS} "
                f"(0=без предела по upsert) discovery_queue={dc_cap} by_major_pages={mc_cap}"
            )

            by_major_categories: set[str] = set()
            for seed in seeds:
                _log(f"{SOURCE}: seed listing {seed}")
                try:
                    _safe_goto(page, seed)
                except Exception as exc:
                    _log(f"{SOURCE}: seed goto failed {exc}")
                    continue
                _settle_network(page)
                _scroll_page(page)
                for u in _collect_hrefs_from_dom(page):
                    if _discovery_queue_full(len(discovered)):
                        break
                    discovered.add(u)
                if UNIGO_BY_MAJOR_EXPAND and _is_by_major_hub_url(seed):
                    for u in sorted(set(_collect_major_category_urls_from_dom(page))):
                        if _by_major_cat_full(len(by_major_categories)):
                            break
                        by_major_categories.add(u)
                if NAV_DELAY_MS:
                    page.wait_for_timeout(NAV_DELAY_MS)

            if UNIGO_BY_MAJOR_EXPAND and by_major_categories:
                _log(
                    f"{SOURCE}: by-major expand → {len(by_major_categories)} category pages "
                    f"(cap={BY_MAJOR_CATEGORY_LIMIT if BY_MAJOR_CATEGORY_LIMIT is not None else '∞'})"
                )
                for maj in sorted(by_major_categories):
                    if _discovery_queue_full(len(discovered)):
                        break
                    _log(f"{SOURCE}: by-major listing {maj}")
                    try:
                        _safe_goto(page, maj)
                    except Exception as exc:
                        _log(f"{SOURCE}: by-major goto failed {maj} ({exc})")
                        continue
                    _settle_network(page)
                    _scroll_page(page)
                    before = len(discovered)
                    for u in _collect_hrefs_from_dom(page):
                        if _discovery_queue_full(len(discovered)):
                            break
                        discovered.add(u)
                    if UNIGO_DEBUG_LINKS:
                        _log(f"{SOURCE}: DEBUG by-major picks +{len(discovered) - before} urls from {maj}")
                    if NAV_DELAY_MS:
                        page.wait_for_timeout(NAV_DELAY_MS)

            _log(f"{SOURCE}: discovered detail urls={len(discovered)}")
            if not discovered:
                _log(
                    f"{SOURCE}: no detail links — возможен Cloudflare/пустая выдача. "
                    "Поставьте UNIGO_REQUIRE_MANUAL_START=1 или UNIGO_DEBUG_LINKS=1 и снова запустите."
                )
            todo = sorted(discovered)

            for pos, url in enumerate(todo):
                # TARGET_NEW_ITEMS>0 — стоп после N успешных upsert; 0 — обойти всю очередь.
                if TARGET_NEW_ITEMS > 0 and stats["upsert_ok"] >= TARGET_NEW_ITEMS:
                    _log(f"{SOURCE}: TARGET_NEW_ITEMS={TARGET_NEW_ITEMS} reached, stopping detail pass")
                    break
                sid = _source_id_for_url(url)
                listing_probe = {"url": url, "source_id": sid, "title": ""}
                if use_skip and listing_is_known(
                    listing_probe, idx, title_fallback=USE_TITLE_FALLBACK_KNOWN
                ):
                    stats["known_skipped"] += 1
                    if (stats["known_skipped"] % 100) == 0:
                        _log(f"{SOURCE}: known_skipped={stats['known_skipped']} (still scanning queue)")
                    continue

                processed += 1
                udisp = url if len(url) <= 110 else url[:107] + "…"
                _log(f"{SOURCE}: [{pos + 1}/{len(todo)}] fetch #{processed}: {udisp}")
                try:
                    _safe_goto(page, url)
                    if NAV_DELAY_MS:
                        page.wait_for_timeout(NAV_DELAY_MS)
                    html = page.content()
                except Exception as exc:
                    _log(f"{SOURCE}: detail fetch failed {url} ({exc})")
                    stats["skip_parse"] += 1
                    continue

                detail = _parse_detail(html, url)
                if not detail:
                    stats["skip_parse"] += 1
                    continue

                record = _build_record(detail, url)
                if not has_meaningful_funding(record):
                    stats["skip_funding"] += 1
                    record_ai_skip()
                    continue
                dbiz = classify_business_deadline(record.get("deadline_date"))
                if dbiz != "ok":
                    stats["skip_deadline"] += 1
                    record_ai_skip()
                    continue

                try:
                    upsert_scholarship(record)
                    stats["upsert_ok"] += 1
                    idx.urls.add(url)
                    idx.source_ids.add(sid)
                    _log(f"{SOURCE}: upsert_ok {stats['upsert_ok']} {url}")
                except Exception as exc:
                    stats["upsert_failed"] += 1
                    _log(f"{SOURCE}: upsert_failed {url} ({exc})")

        finally:
            if attached_via_cdp and browser is not None:
                try:
                    browser.close()
                except Exception:
                    pass
            else:
                try:
                    context.close()
                except Exception:
                    pass
                if browser is not None:
                    try:
                        browser.close()
                    except Exception:
                        pass

    print_ai_session_summary(
        SOURCE,
        processed=processed,
        new_found=stats["upsert_ok"],
        start=ai_usage_start,
    )
    _log(
        f"{SOURCE}: done known_skipped={stats['known_skipped']} upsert_ok={stats['upsert_ok']} "
        f"failed={stats['upsert_failed']} skip_parse={stats['skip_parse']} "
        f"skip_funding={stats['skip_funding']} skip_deadline={stats['skip_deadline']} "
        f"(MIN_LEAD_DAYS_BEFORE_DEADLINE={MIN_LEAD_DAYS_BEFORE_DEADLINE})"
    )


if __name__ == "__main__":
    run()
