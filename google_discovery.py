from __future__ import annotations

import csv
import json
import os
import re
from dataclasses import asdict, dataclass
from datetime import timezone, datetime
from pathlib import Path
from urllib.parse import parse_qs, quote_plus, urljoin, urlparse
from glob import glob

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from playwright._impl._errors import Error as PlaywrightError, TargetClosedError
from playwright.sync_api import sync_playwright

try:
    from scholarships_parsers.business_filters import classify_business_deadline, has_meaningful_funding
    from scholarships_parsers.sources.scholarship_america.parser import parse_deadline_date
    from scholarships_parsers.telegram_notify import send_report
except ModuleNotFoundError:
    from business_filters import classify_business_deadline, has_meaningful_funding
    from sources.scholarship_america.parser import parse_deadline_date
    from telegram_notify import send_report

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
load_dotenv(BASE_DIR.parent / ".env")


@dataclass
class SiteProfile:
    domain: str
    grant_url: str
    source_url: str
    source_category: str
    relevance_score: int
    decision: str
    reason: str
    parser_type: str
    qualified: bool
    qualified_grants_count: int
    rejected_reason: str
    access_mode: str
    registration_required: bool
    api_detected: bool
    blocked_or_captcha: bool
    reachable: bool
    note: str


def _clean_domain(url: str) -> str:
    host = urlparse(url).netloc.lower()
    host = host.replace("www.", "")
    return host


def _is_google_url(url: str) -> bool:
    h = _clean_domain(url)
    return (
        h.endswith("google.com")
        or h.endswith("gstatic.com")
        or h.endswith("google.kz")
    )


def _is_noise_domain(domain: str) -> bool:
    d = (domain or "").lower()
    block_suffixes = (
        "google.com",
        "google.kz",
        "gstatic.com",
        "youtube.com",
        "facebook.com",
        "instagram.com",
        "tiktok.com",
        "linkedin.com",
        "indeed.com",
    )
    block_contains = (
        "doubleclick",
        "adservice",
        "googlesyndication",
    )
    if any(d == s or d.endswith("." + s) for s in block_suffixes):
        return True
    return any(x in d for x in block_contains)


def _categorize_source(domain: str, html_lower: str) -> str:
    d = (domain or "").lower()
    if d.endswith(".gov") or d.endswith(".mil"):
        return "gov"
    if any(x in d for x in (".edu", "college", "university")):
        return "college_university"
    if any(x in html_lower for x in ("foundation", "nonprofit", "non-profit", "501(c)(3)")):
        return "foundation_nonprofit"
    if any(x in d for x in ("grants", "scholarship", "instrumentl", "grantwatch")):
        return "aggregator"
    return "other"


def _compute_relevance(domain: str, html_lower: str, url: str) -> tuple[int, str]:
    score = 0
    reasons: list[str] = []
    positive = (
        ("grant", 12, "ключевое слово grant"),
        ("scholarship", 12, "ключевое слово scholarship"),
        ("funding opportunity", 10, "упоминание funding opportunity"),
        ("eligibility", 8, "есть eligibility"),
        ("deadline", 8, "есть deadline"),
        ("apply", 6, "есть apply"),
        ("financial aid", 8, "есть financial aid"),
    )
    for key, pts, why in positive:
        if key in html_lower:
            score += pts
            reasons.append(why)
    if any(x in url.lower() for x in ("/grants", "/scholarships", "/funding", "/financial-aid")):
        score += 10
        reasons.append("url похож на страницу программы")
    if ("grants.gov" in domain) or (domain.endswith(".gov") and ("grant" in html_lower or "scholarship" in html_lower)):
        score += 10
        reasons.append("профильный госисточник грантов")
    category = _categorize_source(domain, html_lower)
    if category == "gov":
        score += 20
        reasons.append("госисточник")
    elif category == "college_university":
        score += 20
        reasons.append("источник колледжа/университета")
    elif category == "foundation_nonprofit":
        score += 12
        reasons.append("фонд/некоммерческий источник")
    elif category == "aggregator":
        score += 5
        reasons.append("агрегатор")

    negatives = (
        ("jobs", -20, "страница про вакансии/jobs"),
        ("careers", -20, "страница про careers"),
        ("crm", -12, "признаки CRM/SaaS"),
        ("demo", -8, "демо/маркетинговая страница"),
        ("book a demo", -10, "маркетинг вместо каталога"),
        ("pricing", -8, "pricing-страница"),
    )
    for key, pts, why in negatives:
        if key in html_lower:
            score += pts
            reasons.append(why)

    # Balanced: для вузовских страниц с явными признаками финансирования поднимаем приоритет.
    if ("scholarship" in html_lower and "financial aid" in html_lower) or (
        "grant" in html_lower and "eligibility" in html_lower and "apply" in html_lower
    ):
        score += 12
        reasons.append("комбинация признаков реальной grant/scholarship страницы")

    # Антишум для нецелевых международных коммерческих лендингов.
    if any(x in domain for x in ("ue-germany", "germany")):
        score -= 15
        reasons.append("нерелевантный международный коммерческий источник")

    score = max(0, min(100, score))
    if not reasons:
        reasons = ["нет явных сигналов"]
    return score, "; ".join(reasons[:4])


def _decision_for_score(score: int) -> str:
    if score >= 60:
        return "keep"
    if score >= 40:
        return "review"
    return "drop"


def _classify_parser_type(
    html_lower: str,
    grant_url: str,
    api_detected: bool,
    registration_required: bool,
    blocked_or_captcha: bool,
) -> str:
    if api_detected:
        return "API"
    browser_markers = (
        "captcha",
        "cloudflare",
        "verify you are human",
        "sign in",
        "log in",
        "register",
        "__next",
        "nuxt",
        "webpack",
        "javascript required",
    )
    if registration_required or blocked_or_captcha or any(m in html_lower for m in browser_markers):
        return "BROWSER"
    if any(k in grant_url.lower() for k in ("/api", "openapi", "swagger", "/graphql")):
        return "API"
    return "HTML"


def _card_listing_quality(html: str, source_url: str, domain: str) -> tuple[bool, int]:
    lower = (html or "").lower()
    soup = BeautifulSoup(html or "", "html.parser")
    base_host = _clean_domain(source_url or f"https://{domain}")
    internal_drilldowns: set[str] = set()
    path_hints = (
        "/scholarship/",
        "/scholarships/",
        "/opportunity/",
        "/opportunities/",
        "/grant/",
        "/grants/",
        "/funding-opportunity",
        "/search-results-detail/",
    )
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href or href.startswith(("#", "javascript:")):
            continue
        full = urljoin(source_url, href)
        if _clean_domain(full) != base_host:
            continue
        path = (urlparse(full).path or "").lower()
        if len(path) < 3:
            continue
        if any(h in path for h in path_hints):
            internal_drilldowns.add(full.split("#", 1)[0])
            continue
        txt = a.get_text(" ", strip=True).lower()
        if any(k in txt for k in ("scholarship", "grant", "fellowship", "opportunity")) and path.count("/") >= 2:
            internal_drilldowns.add(full.split("#", 1)[0])

    cards_count = len(internal_drilldowns)
    list_markers = (
        ("award amount" in lower or "estimated total program funding" in lower)
        and ("deadline" in lower or "close date" in lower or "applications are due" in lower)
        and ("open" in lower or "closed" in lower or "status" in lower)
    )
    cards_ok = cards_count >= 3 or (cards_count >= 2 and list_markers)
    return cards_ok, cards_count


def _extract_deadline_hint(text: str) -> str | None:
    source = text or ""
    pats = (
        r"(?i)(?:deadline|close date|closing date|applications? (?:are )?due)\s*[:\-]?\s*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
        r"(?i)(?:deadline|close date|closing date|applications? (?:are )?due)\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{4})",
        r"(?i)([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
    )
    for pat in pats:
        m = re.search(pat, source)
        if m:
            return (m.group(1) or "").strip()
    return None


def _normalize_result_href(href: str) -> str:
    h = (href or "").strip()
    if not h:
        return ""
    if h.startswith("/url?") or "google.com/url?" in h:
        parsed = urlparse(("https://www.google.com" + h) if h.startswith("/url?") else h)
        q = parse_qs(parsed.query).get("q", [])
        return (q[0] or "").strip() if q else ""
    return h


def _looks_like_google_captcha(html: str, title: str) -> bool:
    t = (title or "").lower()
    h = (html or "").lower()
    markers = (
        "unusual traffic",
        "recaptcha",
        "i'm not a robot",
        "я не робот",
        "/sorry/index",
        "determine that it's really you",
        "подозрительный трафик",
    )
    text = f"{t}\n{h}"
    return any(m in text for m in markers)


def _extract_candidates_from_page(page: any) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    anchors = page.query_selector_all("a[href]")
    for a in anchors:
        href = _normalize_result_href(a.get_attribute("href") or "")
        if not href.startswith("http"):
            continue
        if _is_google_url(href):
            continue
        d = _clean_domain(href)
        if not d or d in seen:
            continue
        if _is_noise_domain(d):
            continue
        seen.add(d)
        out.append((d, href))
    if out:
        return out
    # Fallback: Google result cards often keep target links near h3 anchors.
    h3_anchors = page.query_selector_all("h3")
    for h3 in h3_anchors:
        parent_link = h3.evaluate_handle("node => node.closest('a')")
        try:
            href = _normalize_result_href(parent_link.get_property("href").json_value() or "")
        except Exception:
            href = ""
        if not href.startswith("http"):
            continue
        if _is_google_url(href):
            continue
        d = _clean_domain(href)
        if not d or d in seen:
            continue
        if _is_noise_domain(d):
            continue
        seen.add(d)
        out.append((d, href))
    return out


def _safe_extract_candidates(page: any) -> list[tuple[str, str]]:
    for _ in range(3):
        try:
            return _extract_candidates_from_page(page)
        except PlaywrightError as exc:
            if "Execution context was destroyed" in str(exc):
                page.wait_for_timeout(1000)
                continue
            raise
    return []


def _load_seen_domains(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return {str(x).strip().lower() for x in payload if str(x).strip()}
    except Exception:
        return set()
    return set()


def _load_recent_html_targets(limit: int) -> list[tuple[str, str]]:
    files = sorted(glob(str(BASE_DIR / "discovery_outputs" / "sources_discovery_*.json")), reverse=True)
    if not files:
        return []
    try:
        payload = json.loads(Path(files[0]).read_text(encoding="utf-8"))
    except Exception:
        return []
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for row in payload:
        if not isinstance(row, dict):
            continue
        if str(row.get("parser_type", "")).upper() != "HTML":
            continue
        if not bool(row.get("qualified", False)):
            continue
        d = str(row.get("domain", "")).strip().lower()
        u = str(row.get("grant_url") or row.get("source_url") or f"https://{d}").strip()
        if not d or d in seen:
            continue
        seen.add(d)
        out.append((d, u))
        if len(out) >= limit:
            break
    return out


def _save_seen_domains(path: Path, domains: set[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(sorted(domains), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _default_google_starts() -> list[int]:
    raw = (os.environ.get("DISCOVERY_GOOGLE_STARTS") or "0,10,20,30,40,50,60,70").strip()
    out: list[int] = []
    for part in raw.split(","):
        p = part.strip()
        if p.isdigit():
            out.append(int(p))
    return out if out else [0]


def discover_domains(
    limit: int,
    queries: list[str],
    skip_seen: bool,
    seen_path: Path,
    *,
    google_starts: list[int] | None = None,
) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    seen: set[str] = set()
    historical_seen = _load_seen_domains(seen_path) if skip_seen else set()
    starts = google_starts if google_starts is not None else _default_google_starts()
    manual_wait_sec = int((os.environ.get("DISCOVERY_CAPTCHA_WAIT_SECONDS") or "90").strip())
    captcha_retries = int((os.environ.get("DISCOVERY_CAPTCHA_RETRIES") or "3").strip())
    captcha_poll_sec = int((os.environ.get("DISCOVERY_CAPTCHA_POLL_SECONDS") or "3").strip())
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        page = browser.new_page()
        page.set_default_timeout(45000)

        def _reopen_page_if_closed() -> None:
            nonlocal browser, page
            try:
                _ = page.url
            except Exception:
                try:
                    browser.close()
                except Exception:
                    pass
                browser = pw.chromium.launch(headless=False)
                page = browser.new_page()
                page.set_default_timeout(45000)
                print("Browser/page was closed manually. Reopened and continuing.")

        try:
            for q in queries:
                for start in starts:
                    print(f"[query] {q} (start={start})")
                    _reopen_page_if_closed()
                    search_url = (
                        f"https://www.google.com/search?q={quote_plus(q)}&num=20&start={start}"
                    )
                    try:
                        page.goto(search_url, wait_until="domcontentloaded")
                    except TargetClosedError:
                        _reopen_page_if_closed()
                        page.goto(search_url, wait_until="domcontentloaded")
                    page.wait_for_timeout(1500)
                    current_candidates = _safe_extract_candidates(page)
                    if not current_candidates:
                        content = page.content()
                        title = page.title()
                        if _looks_like_google_captcha(content, title):
                            print(
                                f"Google CAPTCHA detected for query [{q}] start={start}. "
                                f"Please solve it manually in the open browser. "
                                f"Waiting {manual_wait_sec}s (retries: {captcha_retries})."
                            )
                            solved = False
                            for attempt in range(captcha_retries):
                                waited = 0
                                while waited < manual_wait_sec:
                                    page.wait_for_timeout(captcha_poll_sec * 1000)
                                    waited += captcha_poll_sec
                                    current_candidates = _safe_extract_candidates(page)
                                    if current_candidates:
                                        solved = True
                                        print("CAPTCHA seems solved. Continuing discovery.")
                                        break
                                if solved:
                                    break
                                print(
                                    f"Still no results after manual wait ({attempt + 1}/{captcha_retries}). "
                                    "If CAPTCHA is visible, complete it and wait for auto-continue."
                                )
                            if not solved and not current_candidates:
                                print(
                                    "CAPTCHA not solved in time for this page. "
                                    "Skipping this query offset."
                                )
                    print(f"[query] domains extracted: {len(current_candidates)}")
                    for d, href in current_candidates:
                        if d in seen:
                            continue
                        if skip_seen and d in historical_seen:
                            print(f"[skip seen] {d}")
                            continue
                        seen.add(d)
                        candidates.append((d, href))
                        print(f"[found] {d} (total={len(candidates)}/{limit})")
                        if len(candidates) >= limit:
                            historical_seen.update(seen)
                            if skip_seen:
                                _save_seen_domains(seen_path, historical_seen)
                            return candidates
            historical_seen.update(seen)
            if skip_seen:
                _save_seen_domains(seen_path, historical_seen)
            return candidates
        finally:
            browser.close()
    return candidates


def profile_domain(domain: str, candidate_url: str) -> SiteProfile:
    base = f"https://{domain}"
    source_url = candidate_url or base
    try:
        r = requests.get(source_url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        text = r.text[:150000]
        lower = text.lower()
        reachable = r.ok
    except Exception as e:
        return SiteProfile(
            domain=domain,
            grant_url=source_url,
            source_url=source_url,
            source_category="other",
            relevance_score=0,
            decision="drop",
            reason="недоступен сайт",
            parser_type="BROWSER",
            qualified=False,
            qualified_grants_count=0,
            rejected_reason="unreachable",
            access_mode="BLOCKED_OR_RISKY",
            registration_required=False,
            api_detected=False,
            blocked_or_captcha=True,
            reachable=False,
            note=f"unreachable: {e}",
        )

    blocked = any(x in lower for x in ("captcha", "cloudflare", "access denied", "forbidden", "verify you are human"))
    registration = any(x in lower for x in ("sign in", "log in", "register", "create account"))

    api_detected = False
    for endpoint in ("/api", "/openapi.json", "/swagger", "/docs/api", "/rss"):
        try:
            rr = requests.get(base + endpoint, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            if rr.status_code < 400:
                api_detected = True
                break
        except Exception:
            continue

    if blocked:
        mode = "BLOCKED_OR_RISKY"
    elif api_detected:
        mode = "API_AVAILABLE"
    elif registration:
        mode = "ACCOUNT_REQUIRED"
    else:
        mode = "PARSABLE_PUBLIC"

    soup = BeautifulSoup(text, "html.parser")
    title = (soup.title.text.strip() if soup.title and soup.title.text else "")[:80]
    category = _categorize_source(domain, lower)
    score, reason = _compute_relevance(domain, lower, source_url)
    decision = _decision_for_score(score)
    parser_type = _classify_parser_type(
        html_lower=lower,
        grant_url=source_url,
        api_detected=api_detected,
        registration_required=registration,
        blocked_or_captcha=blocked,
    )
    deadline_hint = _extract_deadline_hint(text)
    deadline_date = parse_deadline_date(deadline_hint)
    funding_probe = {
        "award_amount_text": "",
        "awards_text": lower[:4000],
        "winner_payment_text": "",
        "provider_name": " ".join([domain, category]),
        "is_verified": category in ("gov", "college_university", "foundation_nonprofit"),
    }
    funding_ok = has_meaningful_funding(funding_probe)
    dbiz = classify_business_deadline(deadline_date)
    cards_ok, cards_count = _card_listing_quality(text, source_url, domain)
    qualified = funding_ok and (dbiz == "ok") and cards_ok
    rejected_reason = ""
    qualified_grants_count = cards_count if qualified else 0
    if not qualified:
        if not funding_ok:
            rejected_reason = "no_meaningful_funding_signal"
        elif dbiz != "ok":
            rejected_reason = f"deadline_{dbiz}"
        elif not cards_ok:
            rejected_reason = "no_card_listing_drilldown"
        decision = "drop"
    note = title or "ok"
    return SiteProfile(
        domain=domain,
        grant_url=source_url,
        source_url=source_url,
        source_category=category,
        relevance_score=score,
        decision=decision,
        reason=reason,
        parser_type=parser_type,
        qualified=qualified,
        qualified_grants_count=qualified_grants_count,
        rejected_reason=rejected_reason,
        access_mode=mode,
        registration_required=registration,
        api_detected=api_detected,
        blocked_or_captcha=blocked,
        reachable=reachable,
        note=note,
    )


_EXTRA_UNTIL_QUERIES: tuple[str, ...] = (
    "scholarship search browse opportunities United States",
    "financial aid scholarship listing site:.org",
    "fellowship graduate students USA apply",
    "undergraduate scholarship database open",
    "college grants scholarships listing site:.edu",
    "external funding opportunities scholarships site:.edu",
)


def discover_profiles_until_approved(
    need: int,
    queries: list[str],
    skip_seen: bool,
    seen_path: Path,
    *,
    batch_size: int,
    max_rounds: int,
    google_starts: list[int] | None,
) -> list[SiteProfile]:
    """
    Repeatedly run Google discovery + profiling until `need` qualified sites
    or max_rounds exhausted. Uses pagination (DISCOVERY_GOOGLE_STARTS) and
    widens queries with _EXTRA_UNTIL_QUERIES each round.
    """
    profiled: set[str] = set()
    profiles: list[SiteProfile] = []
    approved: list[SiteProfile] = []
    extras = list(_EXTRA_UNTIL_QUERIES)
    for round_ix in range(max_rounds):
        if len(approved) >= need:
            break
        n_extra = min(round_ix + 1, len(extras))
        qround: list[str] = []
        for q in list(queries) + extras[:n_extra]:
            if q and q not in qround:
                qround.append(q)
        found = discover_domains(
            limit=batch_size,
            queries=qround,
            skip_seen=skip_seen,
            seen_path=seen_path,
            google_starts=google_starts,
        )
        new_pairs = [(d, h) for d, h in found if d not in profiled]
        print(
            f"[until_approved round={round_ix + 1}/{max_rounds}] "
            f"new_domains={len(new_pairs)} approved={len(approved)}/{need}",
            flush=True,
        )
        for d, href in new_pairs:
            profiled.add(d)
            p = profile_domain(d, href)
            profiles.append(p)
            if p.qualified:
                approved.append(p)
                print(
                    f"[approved {len(approved)}/{need}] {p.domain} | {p.grant_url}",
                    flush=True,
                )
                if len(approved) >= need:
                    break
    return profiles


def main() -> None:
    target = int((os.environ.get("DISCOVERY_TARGET_SITES") or "5").strip())
    until_approved = int((os.environ.get("DISCOVERY_UNTIL_APPROVED") or "0").strip() or "0")
    until_batch = int((os.environ.get("DISCOVERY_UNTIL_BATCH") or "14").strip() or "14")
    until_max_rounds = int((os.environ.get("DISCOVERY_UNTIL_MAX_ROUNDS") or "35").strip() or "35")
    skip_seen = (os.environ.get("DISCOVERY_SKIP_SEEN", "1").strip().lower() in ("1", "true", "yes", "on"))
    seen_store_path = Path((os.environ.get("DISCOVERY_SEEN_STORE_PATH") or str(BASE_DIR / "discovery_outputs" / "seen_domains.json")).strip())
    raw_queries = (os.environ.get("DISCOVERY_GOOGLE_QUERIES") or "").strip()
    html_target_mode = (os.environ.get("DISCOVERY_HTML_TARGET_MODE") or "").strip().lower()
    if raw_queries:
        queries = [q.strip() for q in raw_queries.split("|") if q.strip()]
    else:
        queries = [
            "site:.edu scholarships grants students",
            "university grant opportunities united states",
            "foundation grants united states apply",
            "site:.gov grants state opportunities USA",
        ]

    started = datetime.now(timezone.utc)
    plan_line = (
        f"Цель: до {until_approved} approved сайтов (карточки+правила)"
        if until_approved > 0
        else f"План: найти {target} сайтов"
    )
    send_report(
        "\n".join(
            [
                "DISCOVERY STARTED",
                "Продукт: Поиск грантовых сайтов (Google, США)",
                plan_line,
                f"Поисковых запросов (база): {len(queries)}",
                f"Антидубль (история): {'вкл' if skip_seen else 'выкл'}",
                "Браузер: открыт (headful)",
                "",
                "Формат финального отчёта по каждому сайту:",
                "домен | парсер (HTML / API / BROWSER) | подходит да/нет | ссылка",
                "",
                "Статус: выполняется...",
            ]
        )
    )

    if html_target_mode == "recent_html":
        found = _load_recent_html_targets(limit=max(1, target))
        print(f"[target mode] recent_html -> loaded {len(found)} targets from latest discovery file")
        profiles = [profile_domain(d, href) for d, href in found]
    elif until_approved > 0:
        profiles = discover_profiles_until_approved(
            until_approved,
            queries,
            skip_seen,
            seen_store_path,
            batch_size=max(until_batch, until_approved * 6),
            max_rounds=until_max_rounds,
            google_starts=_default_google_starts(),
        )
    else:
        found = discover_domains(
            limit=target,
            queries=queries,
            skip_seen=skip_seen,
            seen_path=seen_store_path,
        )
        profiles = [profile_domain(d, href) for d, href in found]

    out_dir = BASE_DIR / "discovery_outputs"
    out_dir.mkdir(exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    json_path = out_dir / f"sources_discovery_{ts}.json"
    csv_path = out_dir / f"sources_discovery_{ts}.csv"

    with json_path.open("w", encoding="utf-8") as f:
        json.dump([asdict(x) for x in profiles], f, ensure_ascii=False, indent=2)

    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "domain",
                "grant_url",
                "source_url",
                "source_category",
                "relevance_score",
                "decision",
                "reason",
                "parser_type",
                "qualified",
                "qualified_grants_count",
                "rejected_reason",
                "access_mode",
                "registration_required",
                "api_detected",
                "blocked_or_captcha",
                "reachable",
                "note",
            ],
        )
        writer.writeheader()
        for p in profiles:
            writer.writerow(asdict(p))

    counts: dict[str, int] = {}
    decision_counts: dict[str, int] = {}
    parser_counts: dict[str, int] = {}
    approved = [p for p in profiles if p.qualified]
    rejected = [p for p in profiles if not p.qualified]
    for p in profiles:
        counts[p.access_mode] = counts.get(p.access_mode, 0) + 1
        decision_counts[p.decision] = decision_counts.get(p.decision, 0) + 1
        parser_counts[p.parser_type] = parser_counts.get(p.parser_type, 0) + 1

    mode_map = {
        "PARSABLE_PUBLIC": "Публичный парсинг",
        "ACCOUNT_REQUIRED": "Нужна регистрация",
        "PORTAL_ONLY": "Только через портал",
        "API_AVAILABLE": "Есть API/фид",
        "BLOCKED_OR_RISKY": "Блок/CAPTCHA/риск",
    }
    kept = [p for p in profiles if p.decision == "keep"]
    review = [p for p in profiles if p.decision == "review"]
    approved_path = out_dir / f"approved_sources_{ts}.json"
    rejected_path = out_dir / f"rejected_sources_{ts}.json"
    approved_path.write_text(
        json.dumps([asdict(x) for x in approved], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    rejected_path.write_text(
        json.dumps([asdict(x) for x in rejected], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    lines = [
        "DISCOVERY FINISHED",
        "Продукт: Поиск грантовых сайтов (Google, США)",
        f"Найдено сайтов: {len(profiles)}",
        f"Успешно доступных: {sum(1 for p in profiles if p.reachable)}",
        f"Approved sources: {len(approved)}",
        f"Rejected sources: {len(rejected)}",
        "",
        "Approved (под парсер грантов):",
        *(
            [f"- {p.domain} | парсер={p.parser_type} | {p.grant_url}" for p in approved]
            or ["- нет"]
        ),
        "",
        "Полный список проверенных сайтов (домен | парсер HTML/API/BROWSER | подходит | ссылка):",
        *(
            [
                f"- {p.domain} | {p.parser_type} | "
                f"{'подходит' if p.qualified else 'нет'} | {p.grant_url}"
                for p in profiles
            ]
            or ["- нет"]
        ),
        "",
        "Решения качества (balanced):",
        *[f"- {k}: {v}" for k, v in sorted(decision_counts.items())],
        "",
        "Разбивка по типам доступа:",
        *[f"- {mode_map.get(k, k)}: {v}" for k, v in sorted(counts.items())],
        "",
        "Разбивка по parser_type:",
        *[f"- {k}: {v}" for k, v in sorted(parser_counts.items())],
        "",
        "Rejected (первые 10, с типом парсера):",
        *(
            [
                f"- {p.domain} | парсер={p.parser_type} | {p.rejected_reason or '—'}"
                for p in rejected[:10]
            ]
            or ["- нет"]
        ),
        "",
        "Keep (готово к работе):",
        *(
            [
                f"- {p.domain} | парсер={p.parser_type} | score={p.relevance_score} | {p.grant_url}"
                for p in kept
            ]
            or ["- нет"]
        ),
        "",
        "Review (проверить руками):",
        *(
            [
                f"- {p.domain} | парсер={p.parser_type} | score={p.relevance_score} | {p.grant_url} | {p.reason}"
                for p in review
            ]
            or ["- нет"]
        ),
    ]
    send_report("\n".join(lines))
    print("\n".join(lines))


if __name__ == "__main__":
    main()
