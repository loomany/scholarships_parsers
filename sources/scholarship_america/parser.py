"""
Парсер Scholarship America → public.scholarships (Supabase).

Режим new_only (по умолчанию): на листинге пропускаем уже известные строки (без detail),
листаем страницы, пока не сохранено TARGET_NEW_ITEMS новых или не сработал stop.

Остановка (раньше MAX_LIST_PAGES, если сработало условие):
  — набрано TARGET_NEW_ITEMS успешных upsert;
  — подряд NO_NEW_PAGES_STOP страниц листинга без ни одной новой карточки (в режиме skip;
    дефолт 50 — чтобы не резать обход при ~40 страницах, где «пустые» страницы идут кусками);
  — достигнут верхний предел MAX_LIST_PAGES (только safety ceiling, не целевая глубина).

Конфигурация: config.py + корневой .env; шаблон переменных — README.md и .env.example в этом пакете.
  Включение источника — только в run_all (*_ENABLED).
  SCHOLARSHIP_AMERICA_MAX_RECORDS_DEBUG, SCHOLARSHIP_AMERICA_DETAIL_FETCH,
  TARGET_NEW_ITEMS, MAX_LIST_PAGES, … — см. GlobalConfig / ScholarshipAmericaConfig.
"""

from __future__ import annotations

import os
import re
import sys
import time
from copy import copy
from calendar import monthrange
from datetime import date, datetime
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, NavigableString, Tag

_PARSER_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if _PARSER_ROOT not in sys.path:
    sys.path.insert(0, _PARSER_ROOT)

from business_filters import (
    MIN_LEAD_DAYS_BEFORE_DEADLINE,
    classify_business_deadline,
    has_meaningful_funding,
)
from normalize_scholarship import apply_normalization
from scholarship_db_columns import (
    SCHOLARSHIP_RECORD_DEFAULT_KEYS,
    SCHOLARSHIP_UPSERT_BODY_KEYS,
)
from utils import (
    KnownScholarshipIndex,
    get_client,
    listing_is_known,
    load_known_scholarship_index,
    upsert_scholarship,
)

from config import get_global_config, get_scholarship_america_config

_gc = get_global_config()
_sac = get_scholarship_america_config()
TARGET_NEW_ITEMS = _gc.target_new_items
MAX_LIST_PAGES = _gc.max_list_pages
NO_NEW_PAGES_STOP = _gc.no_new_pages_stop
SKIP_EXISTING_ON_LIST = _gc.skip_existing_on_list
USE_TITLE_FALLBACK_KNOWN = _gc.use_title_fallback_known
DISCOVERY_MODE = _gc.discovery_mode

# 0 = без отдельного потолка (лимит только TARGET_NEW_ITEMS); >0 = min(TARGET, cap) успешных upsert.
SCHOLARSHIP_AMERICA_MAX_RECORDS_DEBUG = _sac.max_records_debug
SCHOLARSHIP_AMERICA_DETAIL_FETCH = _sac.detail_fetch

BROWSE_URL = "https://scholarshipamerica.org/students/browse-scholarships/"
BASE_HOST = "https://scholarshipamerica.org"
SOURCE = "scholarship_america"
DEFAULT_CURRENCY = "USD"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

SCHOLARSHIP_TABLE_KEYS: tuple[str, ...] = SCHOLARSHIP_UPSERT_BODY_KEYS


def _browse_url_for_page(page: int) -> str:
    base = BROWSE_URL.split("?")[0].rstrip("/") + "/"
    if page <= 1:
        return base
    return f"{base}?_paged={page}"


def _get_soup(url: str) -> BeautifulSoup:
    resp = requests.get(url, headers=HEADERS, timeout=45)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def _text_or_none(el: BeautifulSoup | Tag | None) -> str | None:
    if el is None:
        return None
    t = el.get_text("\n", strip=True)
    return t if t else None


def _abs_url(href: str | None) -> str | None:
    if not href or not str(href).strip():
        return None
    return urljoin(BASE_HOST, str(href).strip())


def _slug_from_scholarship_url(url: str) -> str | None:
    path = urlparse(url).path.strip("/").split("/")
    if len(path) >= 2 and path[-2] == "scholarship":
        return path[-1] or None
    return path[-1] if path else None


def _listing_detail_after_strong(article: Tag, label: str) -> str | None:
    label_l = label.strip().lower()
    for li in article.select("ul.mgpb-listing-item__scholarship-details li"):
        strong = li.find("strong")
        if not strong:
            continue
        if strong.get_text(strip=True).lower() != label_l:
            continue
        nxt = strong.find_next_sibling("span")
        if nxt:
            t = nxt.get_text(strip=True)
            return t if t else None
        rest = li.get_text(" ", strip=True)
        key = strong.get_text(strip=True)
        if rest.lower().startswith(key.lower()):
            rest = rest[len(key) :].strip()
        return rest if rest else None
    return None


def _listing_li_plain_texts(article: Tag) -> list[str]:
    out: list[str] = []
    for li in article.select("ul.mgpb-listing-item__scholarship-details li"):
        t = li.get_text(" ", strip=True)
        if t:
            out.append(t)
    return out


def _listing_status_text(article: Tag) -> str | None:
    """Open / Closed: первая строка без strong или явная метка."""
    for li in article.select("ul.mgpb-listing-item__scholarship-details li"):
        if li.find("strong"):
            continue
        t = li.get_text(strip=True)
        if not t:
            continue
        tl = t.lower()
        if tl in ("open", "closed"):
            return t
    for t in _listing_li_plain_texts(article):
        tl = t.lower()
        if tl == "open" or tl.startswith("open "):
            return "Open"
        if tl == "closed" or tl.startswith("closed"):
            return "Closed"
    return None


def _parse_int_loose(s: str | None) -> int | None:
    if not s:
        return None
    m = re.search(r"(\d[\d,]*)", s.replace(",", ""))
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except ValueError:
        return None


def _article_looks_open(article: Tag) -> bool:
    st = _listing_status_text(article)
    if st and st.lower() == "open":
        return True
    if st and st.lower() == "closed":
        return False
    blob = article.get_text(" ", strip=True).lower()
    return bool(re.search(r"\bopen\b", blob) and "closed" not in blob[:100])


def parse_listing_card(article: Tag) -> dict[str, Any] | None:
    heading = article.select_one("a.mgpb-listing-item__heading")
    if not heading or not heading.get("href"):
        return None
    title = heading.get_text(strip=True)
    if not title:
        return None
    url = _abs_url(heading.get("href"))
    if not url:
        return None

    award = _listing_detail_after_strong(article, "Award Amount")
    deadline = _listing_detail_after_strong(article, "Deadline")
    applicants_raw = _listing_detail_after_strong(article, "Applicants")
    credibility = _listing_detail_after_strong(article, "Credibility")
    req_count_li = _listing_detail_after_strong(article, "Requirements")
    institutions = _listing_detail_after_strong(article, "Institutions")
    state_territory = _listing_detail_after_strong(article, "State/Territory")
    status_text = _listing_status_text(article)

    blob = article.get_text(" ", strip=True).lower()
    is_verified = "verified" in blob and (
        "scholarship america" in blob or "extremely safe" in blob
    )
    is_recurring = "recurring" in blob or "reapply" in blob

    source_id = _slug_from_scholarship_url(url)
    li_texts = _listing_li_plain_texts(article)
    snippet_el = article.select_one(
        ".mgpb-listing-item__description, .mgpb-listing-item__excerpt, p"
    )
    list_snippet = _text_or_none(snippet_el)

    return {
        "title": title,
        "url": url,
        "award_amount_text": award,
        "deadline_text": deadline,
        "source_id": source_id,
        "status_text": status_text,
        "institutions_text": institutions,
        "state_territory_text": state_territory,
        "applicants_count": _parse_int_loose(applicants_raw),
        "credibility_score_text": credibility,
        "is_verified": bool(is_verified),
        "is_recurring": bool(is_recurring),
        "requirements_count": _parse_int_loose(req_count_li),
        "_list_extra": {
            "li_lines": li_texts,
            "snippet": list_snippet,
            "status_open_guess": _article_looks_open(article),
        },
    }


def clean_requirements_text(raw: str | None) -> str | None:
    if not raw or not str(raw).strip():
        return None
    junk = {
        "|",
        ".",
        "not",
        "a",
        "A",
        "—",
        "-",
        "•",
        "·",
        "▪",
    }
    lines: list[str] = []
    for line in str(raw).replace("\r", "\n").split("\n"):
        s = line.strip()
        if not s:
            continue
        if s in junk:
            continue
        if len(s) <= 1 and s.isalpha():
            continue
        if re.match(r"^[\s|.\-–—•·▪]+$", s):
            continue
        lines.append(s)
    if not lines:
        return None
    return "\n".join(lines)


def _requirements_block_tag(soup: BeautifulSoup) -> Tag | None:
    """Entire #requirements region (headings, nested lists, notes, mailto)."""
    block = soup.select_one("#requirements")
    return block if isinstance(block, Tag) else None


_UNWANTED_TAGS = frozenset(
    {
        "script",
        "style",
        "svg",
        "iframe",
        "object",
        "embed",
        "form",
        "button",
        "noscript",
    }
)


def _decompose_unwanted(root: Tag) -> None:
    for bad in root.find_all(list(_UNWANTED_TAGS)):
        bad.decompose()


def _absolutize_html_fragment(fragment: str, base_url: str) -> str:
    if not fragment or not str(fragment).strip():
        return fragment
    wrapped = f"<div class='sa-frag-root'>{fragment}</div>"
    s = BeautifulSoup(wrapped, "html.parser")
    root = s.select_one(".sa-frag-root")
    if not root:
        return fragment
    for a in root.find_all("a", href=True):
        href = str(a.get("href") or "").strip()
        if href.startswith(("mailto:", "tel:", "#")):
            continue
        a["href"] = urljoin(base_url, href)
        full = str(a["href"])
        if full.startswith("http"):
            a["target"] = "_blank"
            a["rel"] = ["noopener", "noreferrer"]
    return root.decode_contents()


def _safe_inner_html_from_tag(tag: Tag | None) -> str | None:
    if tag is None:
        return None
    t = copy(tag)
    _decompose_unwanted(t)
    inner = t.decode_contents().strip()
    return inner if inner else None


def _requirements_html_and_plain(
    soup: BeautifulSoup, page_url: str
) -> tuple[str | None, str | None]:
    block = _requirements_block_tag(soup)
    if not block:
        return None, None
    inner = _safe_inner_html_from_tag(block)
    html_out = (
        _absolutize_html_fragment(inner, page_url) if inner else None
    )
    raw_plain = block.get_text("\n", strip=True)
    plain = clean_requirements_text(raw_plain)
    return html_out, plain


def _guess_provider_from_intro(soup: BeautifulSoup) -> str | None:
    first_p = soup.select_one(".scholarship-content__intro-text p")
    if not first_p:
        return None
    text = first_p.get_text(strip=True)
    m = re.match(r"^The\s+(.+?)\s+has\s+established\b", text, flags=re.I)
    if m:
        return m.group(1).strip()
    return None


def _first_http_link_outside_sa(intro: Tag | None) -> str | None:
    if intro is None:
        return None
    for a in intro.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href.startswith("http"):
            continue
        hlow = href.lower()
        if "scholarshipamerica.org" in hlow or "saiapply.org" in hlow:
            continue
        return href
    return None


def _payment_subsection_pair(soup: BeautifulSoup) -> tuple[str | None, str | None]:
    for h in soup.find_all(re.compile(r"^h[1-6]$", re.I)):
        t = h.get_text(strip=True)
        if re.search(r"payment\s+of\s+scholarships", t, re.I):
            plain, html = _harvest_section_after_heading_pair(h, "payment")
            return (plain or None, html or None)
    return None, None


def _social_from_soup(soup: BeautifulSoup) -> dict[str, str | None]:
    fb = ig = li_url = None
    for a in soup.select('a[href*="facebook.com"]'):
        fb = _abs_url(a.get("href"))
        if fb:
            break
    for a in soup.select('a[href*="instagram.com"]'):
        ig = _abs_url(a.get("href"))
        if ig:
            break
    for a in soup.select('a[href*="linkedin.com"]'):
        li_url = _abs_url(a.get("href"))
        if li_url:
            break
    return {"facebook": fb, "instagram": ig, "linkedin": li_url}


def _mark_buttons(soup: BeautifulSoup) -> tuple[bool, bool]:
    html = soup.get_text(" ", strip=True).lower()
    started = bool(
        re.search(r"mark\s+as\s+started", html)
        or re.search(r"started\s*[—\-]", html)
    )
    submitted = bool(
        re.search(r"mark\s+as\s+submitted", html)
        or re.search(r"submitted\s*[—\-]", html)
    )
    return started, submitted


def _scholarship_body_root(soup: BeautifulSoup) -> Tag | None:
    for sel in (
        ".scholarship-content .mgpb-wysiwyg__wysiwyg",
        ".scholarship-content",
        "article.scholarship",
        "main article",
    ):
        el = soup.select_one(sel)
        if el:
            return el
    return soup.select_one("main") or soup.body


def _scholarship_full_content_outer(soup: BeautifulSoup) -> Tag | None:
    """Prefer outer column wrapper so intro + all sections stay in one snapshot."""
    el = soup.select_one(".scholarship-content")
    if isinstance(el, Tag):
        return el
    return _scholarship_body_root(soup)


def extract_full_content_html(soup: BeautifulSoup, page_url: str) -> str | None:
    root = _scholarship_full_content_outer(soup)
    if root is None:
        return None
    t = copy(root)
    _decompose_unwanted(t)
    inner = t.decode_contents().strip()
    if not inner:
        return None
    return _absolutize_html_fragment(inner, page_url)


def _major_section_key_from_heading_tag(tag: Tag) -> str | None:
    """Next major SA section — stop harvesting when another starts (not inner h3/h4)."""
    if not isinstance(tag, Tag) or not tag.name:
        return None
    if not re.match(r"^h[1-6]$", tag.name, re.I):
        return None
    t = tag.get_text(strip=True)
    tl = t.lower().strip()
    if tl == "eligibility":
        return "eligibility"
    if tl == "awards":
        return "awards"
    if tl == "notification":
        return "notification"
    if "selection" in tl and "recipient" in tl:
        return "selection_criteria"
    if re.search(r"payment\s+of\s+scholarships", t, re.I):
        return "payment"
    if tl == "requirements":
        return "requirements"
    return None


def _harvest_section_after_heading_pair(
    h: Tag, current_section_key: str
) -> tuple[str, str]:
    text_chunks: list[str] = []
    html_chunks: list[str] = []
    for sib in h.find_next_siblings():
        if isinstance(sib, Tag):
            nxt = _major_section_key_from_heading_tag(sib)
            if nxt is not None and nxt != current_section_key:
                break
        if isinstance(sib, Tag):
            txt = sib.get_text("\n", strip=True)
            if txt:
                text_chunks.append(txt)
            html_chunks.append(str(sib))
        elif isinstance(sib, NavigableString) and str(sib).strip():
            text_chunks.append(str(sib).strip())
    text = "\n\n".join(text_chunks).strip()
    html = "".join(html_chunks).strip()
    return text, html


def extract_wysiwyg_sections_full(
    soup: BeautifulSoup, page_url: str
) -> dict[str, str | None]:
    """Секции по h2/h3: параллельно plain-text (summary) и HTML-фрагмент."""
    out: dict[str, str | None] = {
        "eligibility_text": None,
        "eligibility_html": None,
        "awards_text": None,
        "awards_html": None,
        "notification_text": None,
        "notification_html": None,
        "selection_criteria_text": None,
        "selection_criteria_html": None,
    }
    root = _scholarship_body_root(soup)
    if not root:
        return out

    patterns: list[tuple[str, str, re.Pattern[str]]] = [
        ("eligibility", "eligibility_text", re.compile(r"^eligibility$", re.I)),
        ("awards", "awards_text", re.compile(r"^awards$", re.I)),
        (
            "notification",
            "notification_text",
            re.compile(r"^notification$", re.I),
        ),
        (
            "selection_criteria",
            "selection_criteria_text",
            re.compile(r"selection\s+of\s+recipients", re.I),
        ),
    ]

    for hx in root.find_all(["h2", "h3", "h4"]):
        title = hx.get_text(strip=True)
        if not title:
            continue
        for section_key, text_key, pat in patterns:
            if out[text_key] is not None:
                continue
            if pat.search(title):
                body_txt, body_html = _harvest_section_after_heading_pair(
                    hx, section_key
                )
                if body_txt:
                    out[text_key] = body_txt
                hk = text_key.replace("_text", "_html")
                if body_html:
                    out[hk] = _absolutize_html_fragment(body_html, page_url)
                break
    return out


def extract_support_contacts(soup: BeautifulSoup) -> tuple[str | None, str | None]:
    email = phone = None
    for a in soup.select('a[href^="mailto:"]'):
        href = (a.get("href") or "").strip()
        m = re.match(r"mailto:([^?&]+)", href, re.I)
        if m:
            email = m.group(1).strip()
            break
    for a in soup.select('a[href^="tel:"]'):
        href = (a.get("href") or "").strip()
        m = re.match(r"tel:([^?&]+)", href, re.I)
        if m:
            phone = re.sub(r"\s+", " ", m.group(1).strip())
            break
    if phone is None:
        blob = soup.select_one("#requirements, .scholarship-content, main")
        if blob:
            text = blob.get_text("\n", strip=True)
            m2 = re.search(
                r"\(?\d{3}\)\s*\d{3}[-.\s]?\d{4}|\b\d{3}[-.\s]\d{3}[-.\s]\d{4}\b",
                text,
            )
            if m2:
                phone = m2.group(0).strip()
    return email, phone


def parse_detail_page(soup: BeautifulSoup, page_url: str) -> dict[str, Any]:
    sections = extract_wysiwyg_sections_full(soup, page_url)
    req_html, req_plain = _requirements_html_and_plain(soup, page_url)
    pay_text, pay_html = _payment_subsection_pair(soup)

    out: dict[str, Any] = {
        "provider_name": None,
        "provider_url": None,
        "provider_mission": None,
        "description": None,
        "description_html": None,
        "requirements_text": req_plain,
        "requirements_html": req_html,
        "winner_payment_text": pay_text,
        "payment_html": (
            _absolutize_html_fragment(pay_html, page_url) if pay_html else None
        ),
        "apply_url": None,
        "apply_button_text": None,
        "application_status_text": None,
        "mark_started_available": False,
        "mark_submitted_available": False,
        "provider_social_facebook": None,
        "provider_social_instagram": None,
        "provider_social_linkedin": None,
        "category": None,
        "eligibility_text": sections["eligibility_text"],
        "eligibility_html": sections["eligibility_html"],
        "awards_text": sections["awards_text"],
        "awards_html": sections["awards_html"],
        "notification_text": sections["notification_text"],
        "notification_html": sections["notification_html"],
        "selection_criteria_text": sections["selection_criteria_text"],
        "selection_criteria_html": sections["selection_criteria_html"],
    }

    intro = soup.select_one(".scholarship-content__intro-text")
    out["description"] = _text_or_none(intro)
    intro_inner = _safe_inner_html_from_tag(intro)
    out["description_html"] = (
        _absolutize_html_fragment(intro_inner, page_url) if intro_inner else None
    )

    hero = soup.select_one(".mgpb-hero__description.wysiwyg")
    out["provider_mission"] = _text_or_none(hero)

    guessed = _guess_provider_from_intro(soup)
    if guessed:
        out["provider_name"] = guessed

    purl = _first_http_link_outside_sa(intro)
    if purl:
        out["provider_url"] = purl

    apply_a = soup.select_one("a.mgpb-application-banner__link")
    if apply_a and apply_a.get("href"):
        out["apply_url"] = _abs_url(apply_a.get("href"))
        out["apply_button_text"] = apply_a.get_text(strip=True) or None

    status_el = soup.select_one(
        ".mgpb-application-banner__status, .scholarship-content__status, .mgpb-hero__eyebrow"
    )
    if status_el:
        out["application_status_text"] = status_el.get_text(" ", strip=True) or None
    if out["application_status_text"] is None:
        hero_top = soup.select_one(".mgpb-hero")
        if hero_top:
            for p in hero_top.find_all("p"):
                tx = p.get_text(" ", strip=True)
                if re.search(r"applications\s+(open|closed)", tx, re.I):
                    out["application_status_text"] = tx
                    break

    soc = _social_from_soup(soup)
    out["provider_social_facebook"] = soc["facebook"]
    out["provider_social_instagram"] = soc["instagram"]
    out["provider_social_linkedin"] = soc["linkedin"]

    ms, mb = _mark_buttons(soup)
    out["mark_started_available"] = ms
    out["mark_submitted_available"] = mb

    crumb = soup.select_one(".breadcrumb, .breadcrumbs, nav[aria-label='Breadcrumb']")
    if crumb:
        out["category"] = crumb.get_text(" > ", strip=True) or None
    if out["category"] is None:
        og_t = soup.select_one('meta[property="article:section"]')
        if og_t and og_t.get("content"):
            out["category"] = og_t["content"].strip()

    em, ph = extract_support_contacts(soup)
    out["_support_email"] = em
    out["_support_phone"] = ph

    out["full_content_html"] = extract_full_content_html(soup, page_url)

    out["_detail_extra"] = {"page_url": page_url}
    return out


_MONEY_NUM_RE = re.compile(
    r"(?:\$|USD\s*)?\s*([\d]{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)",
    re.I,
)


def parse_award_min_max(award_amount_text: str | None) -> tuple[float | None, float | None]:
    if not award_amount_text:
        return None, None
    t = award_amount_text.strip()
    if not t or re.search(r"\bTBD\b|\bN/?A\b|varies", t, re.I):
        return None, None
    nums: list[float] = []
    for m in _MONEY_NUM_RE.finditer(t):
        raw = m.group(1).replace(",", "")
        try:
            nums.append(float(raw))
        except ValueError:
            continue
    if not nums:
        return None, None
    upper = t.upper()
    if len(nums) >= 2:
        return min(nums), max(nums)
    if re.search(r"up\s+to", upper):
        return None, nums[0]
    return nums[0], nums[0]


_MONTHS = (
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
)


def parse_deadline_date(deadline_text: str | None) -> str | None:
    if not deadline_text:
        return None
    s = deadline_text.strip()
    m = re.search(
        r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s*(\d{4})\b",
        s,
        re.I,
    )
    if m:
        mon_name, d, y = m.group(1), int(m.group(2)), int(m.group(3))
        mon = _MONTHS.index(mon_name.lower()) + 1
        last = monthrange(y, mon)[1]
        day = min(d, last)
        try:
            return date(y, mon, day).isoformat()
        except ValueError:
            return None
    m2 = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", s)
    if m2:
        mm, dd, yy = int(m2.group(1)), int(m2.group(2)), int(m2.group(3))
        try:
            return date(yy, mm, dd).isoformat()
        except ValueError:
            return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%B %d, %Y", "%b %d, %Y"):
        try:
            dt = datetime.strptime(s[: min(len(s), 40)].strip(), fmt)
            return dt.date().isoformat()
        except ValueError:
            continue
    return None


def _count_req_lines(requirements_text: str | None) -> int | None:
    if not requirements_text or not requirements_text.strip():
        return None
    lines = [ln.strip() for ln in requirements_text.splitlines() if ln.strip()]
    return len(lines) if lines else None


def _json_safe(obj: Any) -> Any:
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, float) and (obj != obj):
        return None
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(x) for x in obj]
    return str(obj)


def build_full_record(
    list_data: dict[str, Any],
    detail: dict[str, Any] | None,
    detail_error: str | None,
) -> dict[str, Any]:
    d = dict(detail or {})
    list_extra = dict(list_data.get("_list_extra") or {})

    support_email = d.pop("_support_email", None)
    support_phone = d.pop("_support_phone", None)
    d.pop("_detail_extra", None)
    section_snapshot = {
        "eligibility": d.get("eligibility_text"),
        "eligibility_html": d.get("eligibility_html"),
        "awards": d.get("awards_text"),
        "awards_html": d.get("awards_html"),
        "notification": d.get("notification_text"),
        "notification_html": d.get("notification_html"),
        "selection_criteria": d.get("selection_criteria_text"),
        "selection_criteria_html": d.get("selection_criteria_html"),
        "description_html": d.get("description_html"),
        "payment_html": d.get("payment_html"),
        "requirements_html": d.get("requirements_html"),
        "full_content_html": d.get("full_content_html"),
    }

    raw_data: dict[str, Any] = {
        "source_parser": SOURCE,
        "list": {k: list_data.get(k) for k in list_data if not str(k).startswith("_")},
        "list_extra": list_extra,
        "detail": {k: v for k, v in d.items() if not str(k).startswith("_")},
        "sections": section_snapshot,
        "detail_error": detail_error,
    }

    title = list_data.get("title") or "Untitled scholarship"
    url = list_data.get("url") or ""
    award_text = list_data.get("award_amount_text")
    deadline_text = list_data.get("deadline_text")

    amin, amax = parse_award_min_max(award_text)
    ddate = parse_deadline_date(deadline_text)

    req_text = d.get("requirements_text")
    req_n = list_data.get("requirements_count")
    if req_n is None:
        req_n = _count_req_lines(req_text)

    category = d.get("category")
    if not category and list_data.get("institutions_text"):
        category = list_data.get("institutions_text")

    status_text = list_data.get("status_text")
    institutions_text = list_data.get("institutions_text")
    state_territory_text = list_data.get("state_territory_text")

    record: dict[str, Any] = {
        "source": SOURCE,
        "source_id": list_data.get("source_id"),
        "url": url,
        "title": title,
        "provider_name": d.get("provider_name"),
        "provider_url": d.get("provider_url"),
        "provider_mission": d.get("provider_mission"),
        "award_amount_text": award_text,
        "award_amount_min": amin,
        "award_amount_max": amax,
        "currency": DEFAULT_CURRENCY,
        "deadline_text": deadline_text,
        "deadline_date": ddate,
        "requirements_count": req_n,
        "requirements_text": req_text,
        "applicants_count": list_data.get("applicants_count"),
        "credibility_score_text": list_data.get("credibility_score_text"),
        "is_verified": bool(list_data.get("is_verified")),
        "is_recurring": bool(list_data.get("is_recurring")),
        "winner_payment_text": d.get("winner_payment_text"),
        "description": d.get("description"),
        "description_html": d.get("description_html"),
        "provider_social_facebook": d.get("provider_social_facebook"),
        "provider_social_instagram": d.get("provider_social_instagram"),
        "provider_social_linkedin": d.get("provider_social_linkedin"),
        "apply_url": d.get("apply_url"),
        "apply_button_text": d.get("apply_button_text"),
        "application_status_text": d.get("application_status_text"),
        "mark_started_available": bool(d.get("mark_started_available")),
        "mark_submitted_available": bool(d.get("mark_submitted_available")),
        "status_text": status_text,
        "institutions_text": institutions_text,
        "state_territory_text": state_territory_text,
        "support_email": support_email,
        "support_phone": support_phone,
        "eligibility_text": d.get("eligibility_text"),
        "eligibility_html": d.get("eligibility_html"),
        "awards_text": d.get("awards_text"),
        "awards_html": d.get("awards_html"),
        "notification_text": d.get("notification_text"),
        "notification_html": d.get("notification_html"),
        "selection_criteria_text": d.get("selection_criteria_text"),
        "selection_criteria_html": d.get("selection_criteria_html"),
        "payment_html": d.get("payment_html"),
        "requirements_html": d.get("requirements_html"),
        "full_content_html": d.get("full_content_html"),
        "category": category,
        "tags": [],
        "is_active": True,
        "raw_data": _json_safe(raw_data),
    }

    apply_normalization(record)

    for k in SCHOLARSHIP_RECORD_DEFAULT_KEYS:
        if k not in record:
            record[k] = None

    record["is_active"] = True
    record["currency"] = DEFAULT_CURRENCY
    record["source"] = SOURCE
    return record


def _summarize_record(r: dict[str, Any]) -> str:
    keys = (
        "title",
        "status_text",
        "institutions_text",
        "eligibility_text",
        "requirements_text",
        "apply_url",
    )
    parts = []
    for k in keys:
        v = r.get(k)
        if v is not None and v != "" and v != []:
            parts.append(f"{k}={repr(str(v))[:100]}")
    return "; ".join(parts) if parts else "(defaults/null)"


def run() -> None:
    cap_dbg = SCHOLARSHIP_AMERICA_MAX_RECORDS_DEBUG
    effective_target = (
        min(TARGET_NEW_ITEMS, cap_dbg) if cap_dbg > 0 else TARGET_NEW_ITEMS
    )

    stats: dict[str, int] = {
        "list_cards_seen": 0,
        "known_skipped": 0,
        "new_found": 0,
        "detail_fetched": 0,
        "skip_no_funding": 0,
        "skip_no_deadline": 0,
        "skip_expired": 0,
        "skip_deadline_too_close": 0,
        "upsert_ok": 0,
        "upsert_failed": 0,
        "taxonomy_study_levels_non_empty": 0,
        "taxonomy_field_of_study_non_empty": 0,
        "taxonomy_citizenship_non_empty": 0,
        "taxonomy_all_empty": 0,
    }
    seen_urls_session: set[str] = set()
    consecutive_pages_no_new = 0
    stop_reason = ""
    use_skip = SKIP_EXISTING_ON_LIST and DISCOVERY_MODE == "new_only"

    print(
        f"{SOURCE}: discovery (TARGET_NEW_ITEMS={TARGET_NEW_ITEMS}, "
        f"effective_target_upserts={effective_target}, "
        f"SCHOLARSHIP_AMERICA_MAX_RECORDS_DEBUG={cap_dbg} (0=use TARGET only), "
        f"DETAIL_FETCH={SCHOLARSHIP_AMERICA_DETAIL_FETCH}, "
        f"MAX_LIST_PAGES={MAX_LIST_PAGES}, NO_NEW_PAGES_STOP={NO_NEW_PAGES_STOP}, "
        f"SKIP_EXISTING_ON_LIST={SKIP_EXISTING_ON_LIST}, DISCOVERY_MODE={DISCOVERY_MODE!r})"
    )

    idx: KnownScholarshipIndex
    if use_skip:
        try:
            idx = load_known_scholarship_index(get_client(), SOURCE)
            print(
                f"  known index: {len(idx.urls)} urls, {len(idx.source_ids)} source_ids, "
                f"{len(idx.slugs_lc)} slugs, {len(idx.titles_norm)} titles "
                f"(USE_TITLE_FALLBACK_KNOWN={USE_TITLE_FALLBACK_KNOWN})"
            )
        except Exception as e:
            print(f"  warning: could not load known index ({e}); continuing without skip")
            idx = KnownScholarshipIndex()
    else:
        idx = KnownScholarshipIndex()

    page = 1
    list_pages_loaded = 0
    while page <= MAX_LIST_PAGES:
        if stats["upsert_ok"] >= effective_target:
            stop_reason = stop_reason or "reached effective_target_upserts"
            break

        list_url = _browse_url_for_page(page)
        print(f"[list] page {page} {list_url}")
        try:
            soup = _get_soup(list_url)
        except Exception as e:
            print(f"  list fetch failed: {e}")
            stop_reason = stop_reason or f"list fetch failed on page {page}"
            break

        list_pages_loaded += 1
        articles = soup.select("article.mgpb-listing-item")
        if not articles:
            print("  no listing cards; stopping")
            stop_reason = stop_reason or f"no cards on page {page}"
            break

        new_on_this_page = 0

        for article in articles:
            if stats["upsert_ok"] >= effective_target:
                break

            card = parse_listing_card(article)
            if not card:
                continue

            stats["list_cards_seen"] += 1
            title = str(card.get("title") or "")
            detail_url = str(card.get("url") or "")

            if detail_url in seen_urls_session:
                print(f"  card: {title[:70]} → duplicate URL this session, skip")
                continue
            seen_urls_session.add(detail_url)

            known = bool(
                use_skip
                and listing_is_known(
                    card,
                    idx,
                    title_fallback=USE_TITLE_FALLBACK_KNOWN,
                )
            )
            if known:
                stats["known_skipped"] += 1
                print(f"  card: {title[:70]} → known, skip")
                continue

            stats["new_found"] += 1
            new_on_this_page += 1
            print(f"  card: {title[:70]} → new")

            detail: dict[str, Any] | None = None
            detail_error: str | None = None
            if SCHOLARSHIP_AMERICA_DETAIL_FETCH:
                print("  fetching detail…")
                try:
                    dsoup = _get_soup(detail_url)
                    detail = parse_detail_page(dsoup, detail_url)
                    stats["detail_fetched"] += 1
                    print("  detail OK")
                except Exception as e:
                    detail_error = str(e)
                    print(f"  detail failed: {e}")
            else:
                print("  detail skipped (SCHOLARSHIP_AMERICA_DETAIL_FETCH=0)")

            record = build_full_record(card, detail, detail_error)

            if not has_meaningful_funding(record):
                stats["skip_no_funding"] += 1
                print("  skip: business filter — no meaningful funding")
                continue

            dbiz = classify_business_deadline(record.get("deadline_date"))
            if dbiz != "ok":
                if dbiz == "no_deadline":
                    stats["skip_no_deadline"] += 1
                    print("  skip: business filter — no parsed deadline")
                elif dbiz == "expired":
                    stats["skip_expired"] += 1
                    print("  skip: business filter — deadline expired")
                else:
                    stats["skip_deadline_too_close"] += 1
                    print(
                        "  skip: business filter — deadline too soon "
                        f"(need >= {MIN_LEAD_DAYS_BEFORE_DEADLINE} days)"
                    )
                continue

            try:
                upsert_scholarship(record)
                stats["upsert_ok"] += 1
                has_levels = bool(record.get("study_levels"))
                has_fos = bool(record.get("field_of_study"))
                has_cit = bool(record.get("citizenship_statuses"))
                if has_levels:
                    stats["taxonomy_study_levels_non_empty"] += 1
                if has_fos:
                    stats["taxonomy_field_of_study_non_empty"] += 1
                if has_cit:
                    stats["taxonomy_citizenship_non_empty"] += 1
                if not (has_levels or has_fos or has_cit):
                    stats["taxonomy_all_empty"] += 1
                print(f"  upsert OK ({stats['upsert_ok']}/{effective_target})")
            except Exception as e:
                stats["upsert_failed"] += 1
                print(f"  upsert failed: {e}")

        if new_on_this_page == 0:
            if use_skip:
                consecutive_pages_no_new += 1
                if consecutive_pages_no_new >= NO_NEW_PAGES_STOP:
                    stop_reason = (
                        stop_reason
                        or f"{NO_NEW_PAGES_STOP} consecutive pages with no new scholarships"
                    )
                    break
        else:
            consecutive_pages_no_new = 0

        if stats["upsert_ok"] >= effective_target:
            stop_reason = stop_reason or "reached effective_target_upserts"
            break

        page += 1

    if not stop_reason:
        if page > MAX_LIST_PAGES:
            stop_reason = "reached MAX_LIST_PAGES (safety ceiling)"
        else:
            stop_reason = "ended"

    print("")
    print(f"processed list pages: {list_pages_loaded}")
    print(f"list cards seen: {stats['list_cards_seen']}")
    print(f"known skipped: {stats['known_skipped']}")
    print(f"new found: {stats['new_found']}")
    print(f"detail fetched: {stats['detail_fetched']}")
    print(f"skip (business): no funding: {stats['skip_no_funding']}")
    print(f"skip (business): no deadline: {stats['skip_no_deadline']}")
    print(f"skip (business): expired: {stats['skip_expired']}")
    print(
        "skip (business): deadline too close "
        f"(<{MIN_LEAD_DAYS_BEFORE_DEADLINE}d): {stats['skip_deadline_too_close']}"
    )
    print(f"upsert OK: {stats['upsert_ok']}")
    print(f"upsert failed: {stats['upsert_failed']}")
    print("taxonomy coverage (on upserted rows):")
    print(f"  study_levels non-empty: {stats['taxonomy_study_levels_non_empty']}")
    print(f"  field_of_study non-empty: {stats['taxonomy_field_of_study_non_empty']}")
    print(f"  citizenship_statuses non-empty: {stats['taxonomy_citizenship_non_empty']}")
    print(f"  all three empty: {stats['taxonomy_all_empty']}")
    print(f"stop reason: {stop_reason}")


if __name__ == "__main__":
    run()
